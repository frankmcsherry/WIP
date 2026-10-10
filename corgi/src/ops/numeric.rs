//! The numeric layer over the core — the first stacked vocabulary on `OpLike`.
//! `NumOp` embeds the whole core `Op` (structure and comparison) via `Core`
//! and adds arithmetic via `Arith`. The same `Graph`/`eval_graph`/`shape_of`
//! machinery runs it unchanged; the core never learns arithmetic.
//!
//! Two leaves carry numbers. `Int` is an integer: arithmetic on it means integer arithmetic, and
//! it is exact within `i64` (past that it wraps, the one documented edge for now; truncating
//! division, `x / 0 = 0` and `x % 0 = x`, so nothing fails on data). `Float` is an `f64`, with IEEE
//! arithmetic. The plain ops (`add`, `mul`, ..) take either, two of one kind; a mix is a shape
//! error. Integers a program wants to treat as 64-bit words (hashing, bit banging) use the `_b64`
//! verbs, the shifts and the bitwise ops ([`BitOp`], [`ShiftOp`]): each takes the low 64 bits of
//! its operands, does the `u64` operation, and reads the result back as an `i64`.

use super::cmp::CmpOp;
use super::core::Op;
use super::text::TextOp;
use crate::graph::{Graph, OpLike};
use crate::value::{f64_key, f64_of_key, Prim, Scalar, Value};
use std::sync::Arc;

/// the named monoid reductions — `List<Int> -> Int` or `List<Float> -> Float` per row, each a
/// one-pass horizontal fold (the fast paths a general `fold` over the same monoid would be ~20x
/// slower than). A Float sum or product adds or multiplies in row order, so it rounds as the `fold`
/// does. `Min`/`Max` go by value, and an empty row's is 0 (a program that wants another default
/// tests `len` and `select`s it); `All`/`Any` are the mask AND/OR, written as bytes, over Ints only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Red {
    Add, // `fold_add` (sum) / `scan_add` (prefix sum)
    Mul, // `fold_mul` (product) / `scan_mul`
    Min,
    Max,
    All,
    Any,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div, // Int: truncating, x/0 = 0, MIN/-1 wraps. Float: IEEE division.
    Rem, // INTEGER-ONLY (the float remainder has no caller). `x % 0 = x`: a total definition, so the
         // lane body needs no branch out and callers that guard the divisor pay nothing. It is the
         // "no reduction" reading of a zero modulus, which is what DDIR's `hash(0, ..)` means.
    // NB: lane-wise min/max are NOT here — they're order ops, so they live in `cmp` as
    // `CmpOp::Min`/`Max`.
}

/// integers as 64-bit words: `add_b64`, `sub_b64`, `mul_b64` are the low 64 bits of the sum,
/// difference and product (read back as an `i64`), and `and`, `or`, `xor` are two's complement
/// bitwise, which keeps every result in the `i64` range. Two byte leaves `and`, `or` and `xor` to a
/// byte leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BitOp {
    AddB64,
    SubB64,
    MulB64,
    And,
    Or,
    Xor,
}

/// shifts by a constant, each on the integer as its 64-bit word: `ShlB64` drops the bits shifted
/// past 64, `ShrB64` fills with zeros (a logical shift), and the rotates move bits around the
/// word. A shift by 64 or more leaves no bits; a rotate turns by `k mod 64`. (There is no
/// integer shift: dividing by a power of two is `div`, which runs as a shift.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ShiftOp {
    ShlB64,
    ShrB64,
    RotlB64,
    RotrB64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp),            // (X, X) -> X   two Ints or two Floats
    BinImm(BinOp, Scalar), // X -> X   `x op c` with a constant `c` of x's kind. One pass over `x`,
                           // in place when it is uniquely owned; no column of `c` is built.
    Bits(BitOp),           // (Int, Int) -> Int
    BitsImm(BitOp, i64),   // Int -> Int   `x op c`
    Shift(ShiftOp, u32),   // Int -> Int
    Neg,                   // X -> X   negate an Int (wrapping at the edge) or a Float
    ToFloat,               // Int -> Float   the nearest `f64`
    Reduce(Red),           // List<X> -> X          per-row monoid reduction (sum/prod/min/max/all/any), X Int or Float
    Scan(Red),             // List<X> -> List<X>    per-row inclusive monoid PREFIX scan. The monoid
                           // fast path for `scan` with a monoid body: one in-place pass, where the
                           // general `FoldScan` re-evals the body per element (catastrophic on one long
                           // row — see performance.md). `Reduce` is its drop-the-prefix sibling.
}

/// apply a binary lane op `f` in place, writing into whichever operand buffer we uniquely own.
/// Both lanes are read before the store, so EITHER side is a valid destination (Sub included:
/// `f` is `x - y` regardless of where it lands). `get_mut` (not `make_mut`) tests uniqueness
/// without cloning, so a shared LHS falls through to a unique RHS; only when both are shared do we allocate.
fn bin_into<T: Copy>(mut a: Arc<Vec<T>>, mut b: Arc<Vec<T>>, f: impl Fn(T, T) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut a) {
        for (x, &y) in dst.iter_mut().zip(b.iter()) { *x = f(*x, y); }
        a
    } else if let Some(dst) = Arc::get_mut(&mut b) {
        for (&x, y) in a.iter().zip(dst.iter_mut()) { *y = f(x, *y); }
        b
    } else {
        Arc::new(a.iter().zip(b.iter()).map(|(&x, &y)| f(x, y)).collect())
    }
}

/// apply a unary lane op `f` in place when the operand is uniquely owned, else fresh. (A binary op
/// against a constant is this with the constant captured.)
fn map_into<T: Copy>(mut a: Arc<Vec<T>>, f: impl Fn(T) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut a) {
        for x in dst.iter_mut() { *x = f(*x); }
        a
    } else {
        Arc::new(a.iter().map(|&x| f(x)).collect())
    }
}

/// `f(own, other)` lane by lane, into `own`'s buffer when it is uniquely held, else fresh.
fn zip_into<T: Copy, U: Copy>(mut own: Arc<Vec<T>>, other: &[U], f: impl Fn(T, U) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut own) {
        for (x, &y) in dst.iter_mut().zip(other) { *x = f(*x, y); }
        own
    } else {
        Arc::new(own.iter().zip(other).map(|(&x, &y)| f(x, y)).collect())
    }
}

/// a binary integer kernel: `f` on each pair, written as `i64`s — into an `i64` operand's buffer
/// when one is uniquely held. A byte operand is read where it lies, each element widened as it is
/// read, never as a column of its own.
fn int_pairs(a: Prim, b: Prim, f: impl Fn(i64, i64) -> i64) -> Prim {
    match (a, b) {
        (Prim::I64(x), Prim::I64(y)) => Prim::I64(bin_into(x, y, f)),
        (Prim::I64(x), Prim::U8(y)) => Prim::I64(zip_into(x, &y, |p, q: u8| f(p, q as i64))),
        (Prim::U8(x), Prim::I64(y)) => Prim::I64(zip_into(y, &x, |q, p: u8| f(p as i64, q))),
        (Prim::U8(x), Prim::U8(y)) => {
            Prim::I64(Arc::new(x.iter().zip(y.iter()).map(|(&p, &q)| f(p as i64, q as i64)).collect()))
        }
        _ => unreachable!("int_pairs: integer leaves, checked by the caller"),
    }
}

/// a unary integer kernel: `f` on each element, written as `i64`s — in place in a uniquely held
/// `i64` leaf; a byte leaf is read where it lies.
fn int_map(a: Prim, f: impl Fn(i64) -> i64) -> Prim {
    match a {
        Prim::I64(x) => Prim::I64(map_into(x, f)),
        Prim::U8(x) => Prim::I64(Arc::new(x.iter().map(|&p| f(p as i64)).collect())),
        Prim::F64(_) => unreachable!("int_map: an integer leaf, checked by the caller"),
    }
}

/// `$apply(args.., body)` with the integer lane body of the `BinOp` `$op`: exact within `i64`,
/// wrapping past it; truncating division with `x / 0 = 0` and `x % 0 = x`. The op is matched
/// ONCE, above the loop, so each arm is one concrete closure the loop inlines and vectorizes.
macro_rules! int_body {
    ($op:expr, $apply:ident($($arg:expr),*)) => {
        match $op {
            BinOp::Add => $apply($($arg,)* |x: i64, y: i64| x.wrapping_add(y)),
            BinOp::Sub => $apply($($arg,)* |x: i64, y: i64| x.wrapping_sub(y)),
            BinOp::Mul => $apply($($arg,)* |x: i64, y: i64| x.wrapping_mul(y)),
            BinOp::Div => $apply($($arg,)* |x: i64, y: i64| if y == 0 { 0 } else { x.wrapping_div(y) }),
            BinOp::Rem => $apply($($arg,)* |x: i64, y: i64| if y == 0 { x } else { x.wrapping_rem(y) }),
        }
    };
}

/// `$apply(args.., body)` with the float lane body of `$op`, on total-order keys (decode, IEEE op,
/// encode); `rem` is integer-only. Matched once, as `int_body`.
macro_rules! float_body {
    ($op:expr, $apply:ident($($arg:expr),*)) => {
        match $op {
            BinOp::Add => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) + f64_of_key(y))),
            BinOp::Sub => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) - f64_of_key(y))),
            BinOp::Mul => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) * f64_of_key(y))),
            BinOp::Div => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) / f64_of_key(y))),
            BinOp::Rem => return Err("rem is integer-only".into()),
        }
    };
}

/// `$apply(args.., body)` with the lane body of the bitwise op `$op` on two `i64`s.
macro_rules! bit_body {
    ($op:expr, $apply:ident($($arg:expr),*)) => {
        match $op {
            BitOp::AddB64 => $apply($($arg,)* |x: i64, y: i64| x.wrapping_add(y)),
            BitOp::SubB64 => $apply($($arg,)* |x: i64, y: i64| x.wrapping_sub(y)),
            BitOp::MulB64 => $apply($($arg,)* |x: i64, y: i64| x.wrapping_mul(y)),
            BitOp::And => $apply($($arg,)* |x: i64, y: i64| x & y),
            BitOp::Or => $apply($($arg,)* |x: i64, y: i64| x | y),
            BitOp::Xor => $apply($($arg,)* |x: i64, y: i64| x ^ y),
        }
    };
}

/// the bitwise ops that keep two bytes a byte, each applied with its byte lane body, or `None`. The
/// body is written into each arm rather than returned as a function pointer, which would be a call
/// per byte.
macro_rules! byte_bits {
    ($op:expr, $apply:ident($($arg:expr),*)) => {
        match $op {
            BitOp::And => Some($apply($($arg,)* |x: u8, y: u8| x & y)),
            BitOp::Or => Some($apply($($arg,)* |x: u8, y: u8| x | y)),
            BitOp::Xor => Some($apply($($arg,)* |x: u8, y: u8| x ^ y)),
            _ => None,
        }
    };
}

/// `x op c` over bytes, in place when `x` is uniquely owned.
fn byte_imm(x: Arc<Vec<u8>>, c: u8, f: impl Fn(u8, u8) -> u8) -> Arc<Vec<u8>> {
    map_into(x, move |x| f(x, c))
}

/// the error for two leaves of different kinds.
fn mixed(op: impl std::fmt::Debug, a: &Prim, b: &Prim) -> String {
    let kind = |p: &Prim| if p.is_int() { "Int" } else { "Float" };
    format!("{op:?}: an {} and a {}", kind(a), kind(b))
}

/// a binary op on two leaves of one kind.
fn bin_eval(op: BinOp, a: Prim, b: Prim) -> Result<Prim, String> {
    Ok(match (a, b) {
        (Prim::F64(x), Prim::F64(y)) => Prim::F64(float_body!(op, bin_into(x, y))),
        (a, b) if a.is_int() && b.is_int() => int_body!(op, int_pairs(a, b)),
        (a, b) => return Err(mixed(op, &a, &b)),
    })
}

/// `x op c` for a constant `c` of `x`'s kind.
fn imm_eval(op: BinOp, a: Prim, c: Scalar) -> Result<Prim, String> {
    fn with<T: Copy>(f: impl Fn(T, T) -> T, c: T) -> impl Fn(T) -> T {
        move |x| f(x, c)
    }
    fn int_imm(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, with(f, c))
    }
    fn float_imm(a: Arc<Vec<u64>>, k: u64, f: impl Fn(u64, u64) -> u64) -> Prim {
        Prim::F64(map_into(a, with(f, k)))
    }
    Ok(match (a, c) {
        (Prim::F64(x), Scalar::Float(k)) => float_body!(op, float_imm(x, k)),
        // by a power of two, `div` and `rem` are shifts: a negative dividend is biased by `c - 1`
        // first, so the quotient still rounds toward zero (and the remainder takes its sign).
        (a, Scalar::Int(c)) if a.is_int() && c > 1 && c.count_ones() == 1 && matches!(op, BinOp::Div | BinOp::Rem) => {
            let k = c.trailing_zeros();
            match op {
                BinOp::Div => int_map(a, move |x| (x + ((x >> 63) & (c - 1))) >> k),
                _ => int_map(a, move |x| x - (((x + ((x >> 63) & (c - 1))) >> k) << k)),
            }
        }
        (a, Scalar::Int(c)) if a.is_int() => int_body!(op, int_imm(a, c)),
        (a, c) => return Err(format!("{op:?}: {} with the constant {c:?}", if a.is_int() { "an Int" } else { "a Float" })),
    })
}

/// a bitwise op on two integer leaves. Two byte leaves `and`, `or` and `xor` to bytes.
fn bits_eval(op: BitOp, a: Prim, b: Prim) -> Result<Prim, String> {
    if !(a.is_int() && b.is_int()) {
        return Err(mixed(op, &a, &b));
    }
    let (a, b) = match (a, b) {
        (Prim::U8(x), Prim::U8(y)) if matches!(op, BitOp::And | BitOp::Or | BitOp::Xor) => {
            return Ok(Prim::U8(byte_bits!(op, bin_into(x, y)).expect("a byte op")));
        }
        ab => ab,
    };
    Ok(bit_body!(op, int_pairs(a, b)))
}

/// `x op c`, bitwise, for an integer leaf. A byte leaf against a byte constant `and`s, `or`s and
/// `xor`s to bytes (`c and 223`, the case fold of text).
fn bits_imm(op: BitOp, a: Prim, c: i64) -> Result<Prim, String> {
    fn int_imm(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, move |x| f(x, c))
    }
    if !a.is_int() {
        return Err(format!("{op:?}: a Float"));
    }
    let a = match a {
        Prim::U8(x) if matches!(op, BitOp::And | BitOp::Or | BitOp::Xor) && (0..=255).contains(&c) => {
            return Ok(Prim::U8(byte_bits!(op, byte_imm(x, c as u8)).expect("a byte op")));
        }
        a => a,
    };
    Ok(bit_body!(op, int_imm(a, c)))
}

/// a shift by `k` of an integer leaf, the op matched once above the loop.
fn shift_eval(op: ShiftOp, a: Prim, k: u32) -> Result<Prim, String> {
    if !a.is_int() {
        return Err(format!("{op:?}: a Float"));
    }
    Ok(match op {
        ShiftOp::ShlB64 if k >= 64 => int_map(a, |_| 0),
        ShiftOp::ShrB64 if k >= 64 => int_map(a, |_| 0),
        ShiftOp::ShlB64 => int_map(a, move |x| ((x as u64) << k) as i64),
        ShiftOp::ShrB64 => int_map(a, move |x| ((x as u64) >> k) as i64),
        ShiftOp::RotlB64 => int_map(a, move |x| (x as u64).rotate_left(k % 64) as i64),
        ShiftOp::RotrB64 => int_map(a, move |x| (x as u64).rotate_right(k % 64) as i64),
    })
}

/// each row's reduction, reading the values at their storage (a byte leaf is not widened first).
/// Sums and products wrap at the `i64` edge, as `add` and `mul` do. An empty row's sum is 0, its
/// product 1, its minimum and maximum 0.
fn reduce_rows<T: Copy + Into<i64>>(bounds: &crate::value::Bounds, xs: &[T], r: Red) -> Value {
    let mut start = 0;
    let rows = bounds.ends().map(|end| {
        let row = &xs[start..end];
        start = end;
        row
    });
    match r {
        Red::Add => Value::i64(rows.map(|s| s.iter().fold(0i64, |a, &x| a.wrapping_add(x.into()))).collect()),
        Red::Mul => Value::i64(rows.map(|s| s.iter().fold(1i64, |a, &x| a.wrapping_mul(x.into()))).collect()),
        Red::Min => Value::i64(rows.map(|s| s.iter().map(|&x| x.into()).min().unwrap_or(0)).collect()),
        Red::Max => Value::i64(rows.map(|s| s.iter().map(|&x| x.into()).max().unwrap_or(0)).collect()),
        Red::All => Value::u8(rows.map(|s| s.iter().all(|&x| x.into() != 0) as u8).collect()),
        Red::Any => Value::u8(rows.map(|s| s.iter().any(|&x| x.into() != 0) as u8).collect()),
    }
}

/// each row's reduction of Floats, held as their order keys. A sum is the fold of `add` from 0.0 in
/// row order, and a product the fold of `mul` from 1.0, so each rounds as that fold does. The least
/// and greatest compare the keys, which is the order `min` and `max` use; an empty row's are 0.0.
/// `fold_all` and `fold_any` read a mask, which is an Int.
fn reduce_floats(bounds: &crate::value::Bounds, ks: &[u64], r: Red) -> Result<Value, String> {
    let mut start = 0;
    let rows = bounds.ends().map(|end| {
        let row = &ks[start..end];
        start = end;
        row
    });
    let zero = f64_key(0.0);
    let out: Vec<u64> = match r {
        Red::Add => rows.map(|s| f64_key(s.iter().fold(0.0, |a, &k| a + f64_of_key(k)))).collect(),
        Red::Mul => rows.map(|s| f64_key(s.iter().fold(1.0, |a, &k| a * f64_of_key(k)))).collect(),
        Red::Min => rows.map(|s| s.iter().copied().min().unwrap_or(zero)).collect(),
        Red::Max => rows.map(|s| s.iter().copied().max().unwrap_or(zero)).collect(),
        Red::All | Red::Any => return Err("fold_all and fold_any read a mask, which is an Int, not a Float".into()),
    };
    Ok(Value::Prim(Prim::F64(Arc::new(out))))
}

/// each row's inclusive prefix under `step`, written over `xs` in place, starting from `id` in every
/// row. The recurrence is sequential within a row, so this is one pass over memory, not a
/// vectorizable one.
fn prefix_rows<T: Copy>(bounds: &crate::value::Bounds, xs: &mut [T], id: T, step: impl Fn(T, T) -> T) {
    let mut start = 0;
    for end in bounds.ends() {
        let mut a = id;
        for slot in &mut xs[start..end] {
            a = step(a, *slot);
            *slot = a;
        }
        start = end;
    }
}

impl ArithOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            ArithOp::Bin(op) => {
                let (a, b) = input.into_pair("binary arith")?;
                let (pa, pb) = (a.into_prim("binary arith lhs")?, b.into_prim("binary arith rhs")?);
                assert_eq!(pa.len(), pb.len(), "binary arith: operands at different strata");
                Value::Prim(bin_eval(*op, pa, pb)?)
            }
            ArithOp::BinImm(op, c) => Value::Prim(imm_eval(*op, input.into_prim("arith with a constant")?, *c)?),
            ArithOp::Bits(op) => {
                let (a, b) = input.into_pair("bitwise")?;
                let (pa, pb) = (a.into_prim("bitwise lhs")?, b.into_prim("bitwise rhs")?);
                assert_eq!(pa.len(), pb.len(), "bitwise: operands at different strata");
                Value::Prim(bits_eval(*op, pa, pb)?)
            }
            ArithOp::BitsImm(op, c) => Value::Prim(bits_imm(*op, input.into_prim("bitwise with a constant")?, *c)?),
            ArithOp::Shift(op, k) => Value::Prim(shift_eval(*op, input.into_prim("shift")?, *k)?),
            ArithOp::Neg => Value::Prim(match input.into_prim("neg")? {
                Prim::F64(v) => Prim::F64(map_into(v, |k| f64_key(-f64_of_key(k)))),
                p => int_map(p, |x: i64| x.wrapping_neg()),
            }),
            ArithOp::ToFloat => {
                let xs = input.as_i64("to_float")?;
                Value::f64(xs.iter().map(|&x| x as f64).collect())
            }
            ArithOp::Reduce(r) => {
                let (bounds, vals) = input.into_list("reduce")?;
                match vals.into_prim("reduce values")? {
                    Prim::U8(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::I64(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::F64(ks) => reduce_floats(&bounds, &ks, *r)?,
                }
            }
            ArithOp::Scan(r) => {
                let (bounds, vals) = input.into_list("scan")?;
                let out = match vals.into_prim("scan values")? {
                    // Floats as their keys: a running sum or product decodes, adds in row order and
                    // encodes; a running least or greatest compares the keys.
                    Prim::F64(ks) => {
                        let mut ks = Arc::unwrap_or_clone(ks);
                        let float = |f: fn(f64, f64) -> f64| move |a: u64, k: u64| f64_key(f(f64_of_key(a), f64_of_key(k)));
                        match r {
                            Red::Add => prefix_rows(&bounds, &mut ks, f64_key(0.0), float(|a, x| a + x)),
                            Red::Mul => prefix_rows(&bounds, &mut ks, f64_key(1.0), float(|a, x| a * x)),
                            Red::Min => prefix_rows(&bounds, &mut ks, u64::MAX, u64::min),
                            Red::Max => prefix_rows(&bounds, &mut ks, u64::MIN, u64::max),
                            Red::All | Red::Any => return Err("scan_all and scan_any read a mask, which is an Int, not a Float".into()),
                        }
                        Value::Prim(Prim::F64(Arc::new(ks)))
                    }
                    ints => {
                        let mut xs = Value::Prim(ints).into_i64("scan values")?; // owned: the prefix is written in place
                        match r {
                            Red::Add => prefix_rows(&bounds, &mut xs, 0, i64::wrapping_add),
                            Red::Mul => prefix_rows(&bounds, &mut xs, 1, i64::wrapping_mul),
                            Red::Min => prefix_rows(&bounds, &mut xs, i64::MAX, i64::min),
                            Red::Max => prefix_rows(&bounds, &mut xs, i64::MIN, i64::max),
                            Red::All => prefix_rows(&bounds, &mut xs, 1, |a, x| a & (x != 0) as i64), // all nonzero so far
                            Red::Any => prefix_rows(&bounds, &mut xs, 0, |a, x| a | (x != 0) as i64), // any nonzero so far
                        }
                        if matches!(r, Red::All | Red::Any) { Value::u8(xs.iter().map(|&x| x as u8).collect()) } else { Value::i64(xs) }
                    }
                };
                Value::List(bounds, Box::new(out))
            }
        })
    }
}

/// the standard vocabulary: the core (structural) ops plus the `cmp` (comparison/order),
/// `arith`, and `text` buckets — the layer the `ml` surface and the optimizer are typed at.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NumOp {
    Core(Op<NumOp>),
    Cmp(CmpOp),
    Arith(ArithOp),
    Text(TextOp),
    /// A kernel supplied from outside corgi (`ops::host`).
    Host(super::host::HostOp),
}

impl OpLike for NumOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        #[cfg(feature = "profile")]
        return crate::explain::profile::time(|| profile_key(self), || self.dispatch(input));
        #[cfg(not(feature = "profile"))]
        self.dispatch(input)
    }
    fn children(&self) -> Vec<&Graph<NumOp>> {
        match self {
            NumOp::Core(c) => c.children(), // core bodies are Graph<NumOp>
            NumOp::Cmp(_) | NumOp::Arith(_) | NumOp::Text(_) | NumOp::Host(_) => Vec::new(),
        }
    }
}

/// the name an op is profiled under: `explain`'s, with constants dropped so they aggregate.
#[cfg(feature = "profile")]
fn profile_key(op: &NumOp) -> String {
    match op {
        NumOp::Core(Op::Lit(_)) => "lit".into(),
        _ => crate::explain::op_name(op),
    }
}

impl NumOp {
    fn dispatch(&self, input: Value) -> Result<Value, String> {
        match self {
            NumOp::Core(c) => c.eval(input),
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
            NumOp::Text(t) => t.eval(input),
            NumOp::Host(h) => h.eval(input),
        }
    }
}

// ergonomic embedding: `b.add(Field(1), …)` / `b.add(SortBy, …)` work without wrapping.
impl From<Op<NumOp>> for NumOp {
    fn from(o: Op<NumOp>) -> Self {
        NumOp::Core(o)
    }
}
impl From<CmpOp> for NumOp {
    fn from(c: CmpOp) -> Self {
        NumOp::Cmp(c)
    }
}
impl From<ArithOp> for NumOp {
    fn from(a: ArithOp) -> Self {
        NumOp::Arith(a)
    }
}
impl From<TextOp> for NumOp {
    fn from(t: TextOp) -> Self {
        NumOp::Text(t)
    }
}
