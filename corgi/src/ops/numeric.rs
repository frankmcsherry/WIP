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
    ImmBin(Scalar, BinOp), // X -> X   `c op x`: the constant on the left, for the ops where the
                           // side matters (`(100, d) sub`); one pass, as `BinImm`.
    Bits(BitOp),           // (Int, Int) -> Int
    BitsImm(BitOp, i64),   // Int -> Int   `x op c`
    ImmBits(i64, BitOp),   // Int -> Int   `c op x`, for `sub_b64`
    Shift(ShiftOp, u32),   // Int -> Int
    Neg,                   // X -> X   negate an Int (wrapping at the edge) or a Float
    ToFloat,               // Int -> Float   the nearest `f64`
    Reduce(Red),           // List<X> -> X          per-row monoid reduction (sum/prod/min/max/all/any), X Int or Float
    IntScatter(Red),       // (List<Int>, List<X>) -> List<(Int, X)>   per row, one (key, total) per
                           // distinct Int key, in key order: the monoid reduction of the key's values,
                           // in element order. X is a leaf or a product of leaves, reduced field by
                           // field. `group` and a fold, with no rows moved.
    HashScatter(Red),      // (List<K>, List<X>) -> List<(K, X)>   the same for keys of any shape, in the
                           // order of the keys' hashes (keys that share a hash, in key order)
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

/// division of an `i64` by a constant `d` with no divide: the multiplier `m` and shift `s` that make
/// `x / d` a multiply-high, a shift and a sign correction (Granlund and Montgomery; Hacker's
/// Delight, figure 10-1). Truncating, as `wrapping_div`, for `2 <= |d|` and `d != i64::MIN`. Found
/// once per op; a divide per element is what it replaces.
#[derive(Clone, Copy)]
struct Divisor {
    d: i64,
    m: i64,
    s: u32,
    fix: i64, // `x` added (1) or taken (-1) from the high half: `m` came out with the wrong sign for `d`
}

impl Divisor {
    fn new(d: i64) -> Divisor {
        debug_assert!(d.unsigned_abs() >= 2 && d != i64::MIN);
        let ad = d.unsigned_abs();
        let two63 = 1u64 << 63;
        let t = two63 + ((d as u64) >> 63);
        let anc = t - 1 - t % ad; // |nc|, the largest multiple of `d`, less one, below 2^63
        let mut p = 63;
        let (mut q1, mut r1) = (two63 / anc, two63 % anc); // 2^p / |nc|
        let (mut q2, mut r2) = (two63 / ad, two63 % ad); // 2^p / |d|
        loop {
            p += 1;
            (q1, r1) = (q1.wrapping_mul(2), r1.wrapping_mul(2));
            if r1 >= anc {
                (q1, r1) = (q1.wrapping_add(1), r1 - anc);
            }
            (q2, r2) = (q2.wrapping_mul(2), r2.wrapping_mul(2));
            if r2 >= ad {
                (q2, r2) = (q2.wrapping_add(1), r2 - ad);
            }
            let delta = ad - r2;
            if !(q1 < delta || (q1 == delta && r1 == 0)) {
                break;
            }
        }
        let m = q2.wrapping_add(1) as i64;
        let m = if d < 0 { m.wrapping_neg() } else { m };
        let fix = if d > 0 && m < 0 { 1 } else if d < 0 && m > 0 { -1 } else { 0 };
        Divisor { d, m, s: p - 64, fix }
    }

    /// `x / d`, with the high half's correction `FIX` (this divisor's `fix`) fixed per loop.
    #[inline]
    fn div<const FIX: i64>(self, x: i64) -> i64 {
        let q = ((self.m as i128 * x as i128) >> 64) as i64;
        let q = match FIX {
            1 => q.wrapping_add(x),
            -1 => q.wrapping_sub(x),
            _ => q,
        } >> self.s;
        q + ((q as u64) >> 63) as i64 // toward zero: a negative quotient rounds up
    }

    /// `f(self, x)` over the leaf `a`, with `f` the division `div` monomorphized for this
    /// divisor's correction, so the loop holds no test of it.
    fn map(self, a: Prim, rem: bool) -> Prim {
        let d = self.d;
        macro_rules! go {
            ($fix:literal) => {
                if rem {
                    int_map(a, move |x| x.wrapping_sub(self.div::<$fix>(x).wrapping_mul(d)))
                } else {
                    int_map(a, move |x| self.div::<$fix>(x))
                }
            };
        }
        match self.fix {
            1 => go!(1),
            -1 => go!(-1),
            _ => go!(0),
        }
    }
}

/// `x op c` for a constant `c` of `x`'s kind, or `c op x` when the constant is on the `left`.
fn imm_eval(op: BinOp, a: Prim, c: Scalar, left: bool) -> Result<Prim, String> {
    fn int_imm(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, move |x| f(x, c))
    }
    fn int_imm_left(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, move |x| f(c, x))
    }
    fn float_imm(a: Arc<Vec<u64>>, k: u64, f: impl Fn(u64, u64) -> u64) -> Prim {
        Prim::F64(map_into(a, move |x| f(x, k)))
    }
    fn float_imm_left(a: Arc<Vec<u64>>, k: u64, f: impl Fn(u64, u64) -> u64) -> Prim {
        Prim::F64(map_into(a, move |x| f(k, x)))
    }
    Ok(match (a, c) {
        (Prim::F64(x), Scalar::Float(k)) if left => float_body!(op, float_imm_left(x, k)),
        (a, Scalar::Int(c)) if left && a.is_int() => int_body!(op, int_imm_left(a, c)),
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
        // by any other constant but 0, 1, -1 and the least `i64`, a multiply-high (see `Divisor`)
        (a, Scalar::Int(c)) if a.is_int() && c.unsigned_abs() >= 2 && c != i64::MIN && matches!(op, BinOp::Div | BinOp::Rem) => {
            Divisor::new(c).map(a, matches!(op, BinOp::Rem))
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

/// `x op c`, bitwise, for an integer leaf, or `c op x` when the constant is on the `left`. A byte
/// leaf against a byte constant `and`s, `or`s and `xor`s to bytes (`c and 223`, the case fold of
/// text).
fn bits_imm(op: BitOp, a: Prim, c: i64, left: bool) -> Result<Prim, String> {
    fn int_imm(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, move |x| f(x, c))
    }
    fn int_imm_left(a: Prim, c: i64, f: impl Fn(i64, i64) -> i64) -> Prim {
        int_map(a, move |x| f(c, x))
    }
    if !a.is_int() {
        return Err(format!("{op:?}: a Float"));
    }
    if left {
        return Ok(bit_body!(op, int_imm_left(a, c)));
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
    match r {
        Red::Add => Value::i64(per_row(bounds, xs, |s| s.iter().fold(0i64, |a, &x| a.wrapping_add(x.into())))),
        Red::Mul => Value::i64(per_row(bounds, xs, |s| s.iter().fold(1i64, |a, &x| a.wrapping_mul(x.into())))),
        Red::Min => Value::i64(per_row(bounds, xs, |s| s.iter().map(|&x| x.into()).min().unwrap_or(0))),
        Red::Max => Value::i64(per_row(bounds, xs, |s| s.iter().map(|&x| x.into()).max().unwrap_or(0))),
        Red::All => Value::u8(per_row(bounds, xs, |s| s.iter().all(|&x| x.into() != 0) as u8)),
        Red::Any => Value::u8(per_row(bounds, xs, |s| s.iter().any(|&x| x.into() != 0) as u8)),
    }
}

/// `f` of each row's slice of `xs`; a stride's rows are its fixed-size chunks.
fn per_row<T, R>(bounds: &crate::value::Bounds, xs: &[T], f: impl FnMut(&[T]) -> R) -> Vec<R> {
    match bounds {
        crate::value::Bounds::Stride(k, _) if *k > 0 => xs.chunks_exact(*k).map(f).collect(),
        _ => {
            let mut start = 0;
            bounds.ends().map(|end| { let row = &xs[start..end]; start = end; row }).map(f).collect()
        }
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

#[cfg(test)]
mod scatter_tests;

/// The values of a keyed reduction: a leaf, or a product of leaves, reduced field by field.
struct Fields {
    fields: Vec<Value>,
    tuple: bool,
}

impl Fields {
    fn new(vals: Value, r: Red, who: &str) -> Result<Fields, String> {
        let (fields, tuple) = match vals {
            Value::Prod(fs) => (fs, true),
            v => (vec![v], false),
        };
        for f in &fields {
            match f {
                Value::Prim(p) if !p.is_int() && matches!(r, Red::All | Red::Any) => {
                    return Err(format!("{who}: `all` and `any` read a mask, which is an Int, not a Float"));
                }
                Value::Prim(_) => {}
                other => {
                    return Err(format!("{who} values: expected leaves or a product of them, got {}", crate::shape::shape_of_value(other)));
                }
            }
        }
        Ok(Fields { fields, tuple })
    }

    /// each field's totals, by number, appended to `outs`.
    fn reduce(&self, at: std::ops::Range<usize>, slots: &[u32], g: usize, r: Red, outs: &mut [Vec<u64>], table: &mut Vec<u64>) {
        for (f, out) in self.fields.iter().zip(outs.iter_mut()) {
            reduce_field(f, at.clone(), slots, g, r, out, table);
        }
    }

    /// the result: each row's `(key, total)` pairs.
    fn finish(self, ends: Vec<usize>, keys: Value, outs: Vec<Vec<u64>>, r: Red) -> Value {
        let mut cols = Vec::with_capacity(self.fields.len());
        for (f, out) in self.fields.iter().zip(outs) {
            cols.push(match f {
                Value::Prim(Prim::F64(_)) => Value::Prim(Prim::F64(Arc::new(out))),
                Value::Prim(_) if matches!(r, Red::All | Red::Any) => Value::u8(out.into_iter().map(|x| x as u8).collect()),
                _ => Value::i64(out.into_iter().map(|x| x as i64).collect()),
            });
        }
        let vals = if self.tuple { Value::Prod(cols) } else { cols.pop().expect("one field") };
        Value::List(ends.into(), Box::new(Value::Prod(vec![keys, vals])))
    }
}

/// `(keys, values) int_scatter_<r>`, row by row, for Int keys: one `(key, total)` pair per distinct
/// key, in key order, `total` the reduction under `r` of the key's values in element order. A row's
/// keys and values pair up by position (the longer list's extra elements dropped).
///
/// This is `hash_scatter` and then a sort of each row's pairs by key, except where every row's keys
/// span a range not much wider than the row. There a table over the range numbers each row's keys
/// in key order (which keys occur, then a running count over those), and each field is reduced into
/// the numbered slots, one pass each.
fn int_scatter_rows(kb: &crate::value::Bounds, keys: &Value, vb: &crate::value::Bounds, vals: Value, r: Red) -> Result<Value, String> {
    let rows = kb.len();
    if vb.len() != rows {
        return Err(format!("int_scatter: {rows} rows of keys, {} rows of values", vb.len()));
    }
    let ints = keys.as_i64("int_scatter keys")?;
    let at: Vec<(usize, usize, usize)> = (0..rows)
        .map(|q| {
            let ((ks, ke), (vs, ve)) = (kb.span(q), vb.span(q));
            (ks, vs, (ke - ks).min(ve - vs))
        })
        .collect();
    // each row's least key and the width of the range its keys span
    let ranges: Vec<(i64, i128)> = at
        .iter()
        .map(|&(ks, _, len)| {
            let (lo, hi) = ints[ks..ks + len].iter().fold((i64::MAX, i64::MIN), |(l, h), &x| (l.min(x), h.max(x)));
            (lo, if len == 0 { 0 } else { hi as i128 - lo as i128 + 1 })
        })
        .collect();
    if at.iter().zip(&ranges).any(|(&(_, _, len), &(_, width))| width > (2 * len + 4096) as i128) {
        return Ok(by_key(hash_scatter_rows(kb, keys, vb, vals, r, &crate::hash::hash, true)?));
    }
    let fields = Fields::new(vals, r, "int_scatter")?;
    let mut out_keys: Vec<i64> = Vec::new();
    let mut outs: Vec<Vec<u64>> = vec![Vec::new(); fields.fields.len()];
    let mut ends = Vec::with_capacity(rows);
    let mut num = Numbering::default();
    let mut table = Vec::new();
    for (&(ks, vs, len), &(lo, width)) in at.iter().zip(&ranges) {
        let g = num.by_range(&ints[ks..ks + len], lo, width as usize, &mut out_keys);
        fields.reduce(vs..vs + len, &num.slots, g, r, &mut outs, &mut table);
        ends.push(out_keys.len());
    }
    Ok(fields.finish(ends, Value::i64(out_keys), outs, r))
}

/// each row's `(key, total)` pairs, sorted by key.
fn by_key(pairs: Value) -> Value {
    let Value::List(ends, kv) = pairs else { unreachable!("a list of pairs") };
    let Value::Prod(mut kv) = *kv else { unreachable!("pairs") };
    let totals = kv.pop().expect("totals");
    let labels = if ends.len() == 1 { Vec::new() } else { crate::ops::cmp::order::segment_labels(&ends) };
    let (perm, _, keys) = crate::ops::cmp::sort::sort_values(&labels, &kv[0]);
    Value::List(ends, Box::new(Value::Prod(vec![keys, crate::engine::gather(&totals, &perm)])))
}

/// `(keys, values) hash_scatter_<r>`, row by row, for keys of any shape: one `(key, total)` pair per
/// distinct key, in the order of the keys' hashes (as unsigned words; keys that share a hash, in key
/// order), `total` the reduction under `r` of the key's values in element order. A row's keys and
/// values pair up by position (the longer list's extra elements dropped). The keys come out copied.
///
/// Each key is hashed, and the row's distinct hashes are numbered `0..g` in hash order by hash
/// tables (see [`Numbering`]). Then every key is compared with the first key that got its number,
/// in place: when all agree, no two distinct keys share a hash and the numbering is exact. When some
/// disagree, the row is numbered by sorting its keys by `(hash, key)` instead. Each field is then
/// reduced into `g` slots, one pass each. `exact` says that equal hashes mean equal keys, so no key
/// need be compared: corgi's hash of a leaf mixes its word by a bijection.
fn hash_scatter_rows(kb: &crate::value::Bounds, keys: &Value, vb: &crate::value::Bounds, vals: Value, r: Red, hash: &dyn Fn(&Value) -> Vec<u64>, exact: bool) -> Result<Value, String> {
    let rows = kb.len();
    if vb.len() != rows {
        return Err(format!("hash_scatter: {rows} rows of keys, {} rows of values", vb.len()));
    }
    let fields = Fields::new(vals, r, "hash_scatter")?;
    let hashes = hash(keys);
    let mut kept: Vec<usize> = Vec::new(); // the position of each distinct key, in output order
    let mut outs: Vec<Vec<u64>> = vec![Vec::new(); fields.fields.len()];
    let mut ends = Vec::with_capacity(rows);
    let mut num = Numbering::default();
    let mut table = Vec::new();
    for q in 0..rows {
        let ((ks, ke), (vs, ve)) = (kb.span(q), vb.span(q));
        let len = (ke - ks).min(ve - vs);
        let row = &hashes[ks..ks + len];
        let g = num.words(row);
        // the first position with each number, and whether every key equals its number's first
        let start = kept.len();
        kept.resize(start + g, usize::MAX);
        for (k, &s) in num.slots.iter().enumerate() {
            if kept[start + s as usize] == usize::MAX {
                kept[start + s as usize] = ks + k;
            }
        }
        let firsts = &kept[start..];
        let slots = &num.slots;
        let g = if exact || equal_to_firsts(keys, ks, len, &|k| firsts[slots[k] as usize]) {
            g
        } else {
            // two distinct keys share a hash: number the row by sorting its keys by (hash, key), the
            // hash's top bit flipped so the signed order of the words is their unsigned order
            kept.truncate(start);
            let at: Vec<usize> = (ks..ks + len).collect();
            let words = Value::i64(row.iter().map(|&h| (h ^ (1 << 63)) as i64).collect());
            let pairs = Value::Prod(vec![words, crate::engine::gather(keys, &at)]);
            let (order, runs, _) = crate::ops::cmp::sort::sort_values(&[], &pairs);
            let mut g = 0;
            for (j, (&k, &run)) in order.iter().zip(&runs).enumerate() {
                if j == 0 || run != runs[j - 1] {
                    kept.push(ks + k);
                    g += 1;
                }
                num.slots[k] = (g - 1) as u32;
            }
            g
        };
        fields.reduce(vs..vs + len, &num.slots, g, r, &mut outs, &mut table);
        ends.push(kept.len());
    }
    Ok(fields.finish(ends, crate::engine::gather(keys, &kept), outs, r))
}

/// whether each of the `len` keys from position `ks` equals the key at `first(k)` (`k` counted from
/// `ks`), compared in place: leaves by value, products field by field, lists of leaves by length then
/// elements. Other shapes go to the general comparator.
fn equal_to_firsts(keys: &Value, ks: usize, len: usize, first: &dyn Fn(usize) -> usize) -> bool {
    match keys {
        Value::Prim(Prim::I64(xs)) => (0..len).all(|k| xs[ks + k] == xs[first(k)]),
        Value::Prim(Prim::U8(xs)) => (0..len).all(|k| xs[ks + k] == xs[first(k)]),
        Value::Prim(Prim::F64(xs)) => (0..len).all(|k| xs[ks + k] == xs[first(k)]),
        Value::Unit(_) => true,
        Value::Prod(fields) => fields.iter().all(|f| equal_to_firsts(f, ks, len, first)),
        Value::List(bounds, vals) if matches!(**vals, Value::Prim(_)) => {
            fn same<T: PartialEq>(v: &[T], a: (usize, usize), b: (usize, usize)) -> bool {
                a.1 - a.0 == b.1 - b.0 && v[a.0..a.1] == v[b.0..b.1]
            }
            let span = |k: usize| (bounds.span(ks + k), bounds.span(first(k)));
            match &**vals {
                Value::Prim(Prim::U8(v)) => (0..len).all(|k| { let (a, b) = span(k); same(v, a, b) }),
                Value::Prim(Prim::I64(v)) => (0..len).all(|k| { let (a, b) = span(k); same(v, a, b) }),
                Value::Prim(Prim::F64(v)) => (0..len).all(|k| { let (a, b) = span(k); same(v, a, b) }),
                _ => unreachable!("a list of leaves"),
            }
        }
        other => {
            let at: Vec<usize> = (ks..ks + len).collect();
            let reps: Vec<usize> = (0..len).map(first).collect();
            crate::ops::cmp::order::compare_idx(other, other, &at, &reps).iter().all(|&c| c == 0)
        }
    }
}

/// The numbering step of a keyed reduction: a row's distinct ids numbered `0..g` in ascending order,
/// each element's number in `slots`. The buffers are kept from row to row, so a column of short rows
/// allocates nothing per row.
#[derive(Default)]
struct Numbering {
    slots: Vec<u32>,
    seen: Vec<bool>,
    rank: Vec<u32>,
    table: ProbeTable,
    order: Vec<u32>,
    parted: Vec<u64>,
    local: Vec<u32>,
}

impl Numbering {
    /// keys in a range `span` wide from `lo`: which occur, then a running count over those.
    fn by_range(&mut self, keys: &[i64], lo: i64, span: usize, distinct: &mut Vec<i64>) -> usize {
        self.seen.clear();
        self.seen.resize(span, false);
        for &k in keys {
            self.seen[(k - lo) as usize] = true;
        }
        self.rank.clear();
        let mut g = 0u32;
        for (j, &s) in self.seen.iter().enumerate() {
            self.rank.push(g);
            if s {
                distinct.push(lo + j as i64);
                g += 1;
            }
        }
        let rank = &self.rank;
        self.slots.clear();
        self.slots.extend(keys.iter().map(|&k| rank[(k - lo) as usize]));
        g as usize
    }

    /// number 64-bit words in ascending (unsigned) order. While the distinct words fit a hash table
    /// that stays in cache, one table numbers them as first seen; a row longer than that whose sample
    /// of 4096 words holds many distinct ones, or whose table fills, is partitioned by the words' top
    /// bits instead (a counting pass that keeps the order across partitions, the words themselves laid
    /// out in partition order), and each partition is numbered by its own table.
    fn words(&mut self, words: &[u64]) -> usize {
        const ONE_TABLE: u32 = 17; // 128K entries, about 1.5 MB with their numbers
        let n = words.len();
        self.slots.clear();
        self.slots.resize(n, 0);
        if n == 0 {
            return 0;
        }
        if n <= 1 << (ONE_TABLE - 1) {
            let bits = (2 * n).next_power_of_two().trailing_zeros().max(4);
            return self.table.number(words, bits, 0, &mut self.slots).expect("a table twice the row");
        }
        let step = n / 4096;
        let mut sample: Vec<u64> = words.iter().step_by(step).copied().collect();
        sample.sort_unstable();
        sample.dedup();
        if sample.len() <= 1024 {
            if let Some(g) = self.table.number(words, ONE_TABLE, 0, &mut self.slots) {
                return g;
            }
        }
        // partitions of about 16K words, each numbered by a table twice its size
        let bits = (n / 16384).next_power_of_two().trailing_zeros().clamp(1, 16);
        let part = |w: u64| (w >> (64 - bits)) as usize;
        let mut starts = vec![0usize; (1 << bits) + 1];
        for &w in words {
            starts[part(w) + 1] += 1;
        }
        for p in 0..1 << bits {
            starts[p + 1] += starts[p];
        }
        let mut fill = starts.clone();
        self.order.resize(n, 0);
        self.parted.resize(n, 0);
        for (k, &w) in words.iter().enumerate() {
            let at = &mut fill[part(w)];
            self.order[*at] = k as u32;
            self.parted[*at] = w;
            *at += 1;
        }
        let biggest = starts.windows(2).map(|w| w[1] - w[0]).max().unwrap_or(0);
        let table_bits = (2 * biggest).next_power_of_two().trailing_zeros().max(4);
        self.local.resize(n, 0);
        let mut g = 0;
        for w in starts.windows(2) {
            let (lo, hi) = (w[0], w[1]);
            g += self.table.number(&self.parted[lo..hi], table_bits, g, &mut self.local[lo..hi]).expect("a table twice the partition");
        }
        for (&k, &s) in self.order.iter().zip(&self.local) {
            self.slots[k as usize] = s;
        }
        g
    }
}

/// An open-addressing table from a word to its number, reused across rows and partitions.
#[derive(Default)]
struct ProbeTable {
    keys: Vec<u64>,
    nums: Vec<u32>,
    used: Vec<usize>, // the entries filled, to clear them
    seen: Vec<u64>,   // the distinct words, as first seen
}

impl ProbeTable {
    /// number `words`, writing `base + rank` (each word's rank among the distinct ones, ascending)
    /// to `out`, using a table of `2^bits` entries. `None`, with the table cleared, when the distinct
    /// words would fill more than half of it.
    fn number(&mut self, words: &[u64], bits: u32, base: usize, out: &mut [u32]) -> Option<usize> {
        let size = 1usize << bits;
        if self.keys.len() < size {
            self.keys.resize(size, 0);
            self.nums.resize(size, u32::MAX);
        }
        let (mask, limit) = (size - 1, size / 2);
        for (j, &w) in words.iter().enumerate() {
            let mut i = (w.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize;
            out[j] = loop {
                if self.nums[i] == u32::MAX {
                    if self.seen.len() == limit {
                        self.clear();
                        return None;
                    }
                    self.keys[i] = w;
                    self.nums[i] = self.seen.len() as u32;
                    self.used.push(i);
                    self.seen.push(w);
                    break self.nums[i];
                }
                if self.keys[i] == w {
                    break self.nums[i];
                }
                i = (i + 1) & mask;
            };
        }
        // first-seen numbers to ranks in ascending order
        let mut order: Vec<u32> = (0..self.seen.len() as u32).collect();
        order.sort_unstable_by_key(|&j| self.seen[j as usize]);
        let mut rank = vec![0u32; self.seen.len()];
        for (r, &j) in order.iter().enumerate() {
            rank[j as usize] = (base + r) as u32;
        }
        for n in out[..words.len()].iter_mut() {
            *n = rank[*n as usize];
        }
        let g = self.seen.len();
        self.clear();
        Some(g)
    }

    fn clear(&mut self) {
        for &i in &self.used {
            self.nums[i] = u32::MAX;
        }
        self.used.clear();
        self.seen.clear();
    }
}

/// one field's totals, `g` of them, appended to `out`: each element of `at` reduced under `r` into
/// the slot `slots` names, in `table`.
fn reduce_field(f: &Value, at: std::ops::Range<usize>, slots: &[u32], g: usize, r: Red, out: &mut Vec<u64>, table: &mut Vec<u64>) {
    match f {
        Value::Prim(Prim::I64(xs)) => {
            let xs = &xs[at];
            reduce_ints(slots, g, |k| xs[k], r, table)
        }
        Value::Prim(Prim::U8(xs)) => {
            let xs = &xs[at];
            reduce_ints(slots, g, |k| xs[k] as i64, r, table)
        }
        Value::Prim(Prim::F64(ks)) => {
            let (unit, step) = float_monoid(r);
            table.clear();
            table.resize(g, unit);
            for (&s, &k) in slots.iter().zip(&ks[at]) {
                table[s as usize] = step(table[s as usize], k);
            }
        }
        _ => unreachable!("scatter fields are checked"),
    }
    out.extend_from_slice(table);
}

/// a Float's monoid on keys: its unit key and its step (a sum or product decodes, operates and
/// encodes; a least or greatest compares the keys).
fn float_monoid(r: Red) -> (u64, fn(u64, u64) -> u64) {
    match r {
        Red::Add => (f64_key(0.0), |a, k| f64_key(f64_of_key(a) + f64_of_key(k))),
        Red::Mul => (f64_key(1.0), |a, k| f64_key(f64_of_key(a) * f64_of_key(k))),
        Red::Min => (u64::MAX, u64::min),
        _ => (u64::MIN, u64::max),
    }
}

/// integer values (element `k`'s is `value(k)`) reduced under `r` into `g` slots by `slots`, each
/// monoid its own loop so its step inlines, the totals left in `acc` as `i64` bits. A table of at
/// most `SMALL` slots is held `COPIES` times when the row is long enough to repay it.
fn reduce_ints(slots: &[u32], g: usize, value: impl Fn(usize) -> i64 + Copy, r: Red, acc: &mut Vec<u64>) {
    fn go(slots: &[u32], g: usize, value: impl Fn(usize) -> i64, unit: i64, step: impl Fn(i64, i64) -> i64, out: &mut Vec<u64>) {
        const COPIES: usize = 8;
        const SMALL: usize = 4096;
        out.clear();
        if g > 0 && g <= SMALL && slots.len() >= COPIES * g {
            let mut acc = vec![unit; COPIES * g];
            for (k, &s) in slots.iter().enumerate() {
                let at = (k & (COPIES - 1)) * g + s as usize;
                acc[at] = step(acc[at], value(k));
            }
            let (first, rest) = acc.split_at_mut(g);
            for copy in rest.chunks_exact(g) {
                for (a, &b) in first.iter_mut().zip(copy) {
                    *a = step(*a, b);
                }
            }
            out.extend(acc[..g].iter().map(|&x| x as u64));
        } else {
            out.resize(g, unit as u64);
            for (k, &s) in slots.iter().enumerate() {
                let a = &mut out[s as usize];
                *a = step(*a as i64, value(k)) as u64;
            }
        }
    }
    let x = value;
    match r {
        Red::Add => go(slots, g, x, 0, i64::wrapping_add, acc),
        Red::Mul => go(slots, g, x, 1, i64::wrapping_mul, acc),
        Red::Min => go(slots, g, x, i64::MAX, i64::min, acc),
        Red::Max => go(slots, g, x, i64::MIN, i64::max, acc),
        Red::All => go(slots, g, x, 1, |a, x| a & (x != 0) as i64, acc),
        Red::Any => go(slots, g, x, 0, |a, x| a | (x != 0) as i64, acc),
    }
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
            ArithOp::BinImm(op, c) => Value::Prim(imm_eval(*op, input.into_prim("arith with a constant")?, *c, false)?),
            ArithOp::ImmBin(c, op) => Value::Prim(imm_eval(*op, input.into_prim("arith with a constant")?, *c, true)?),
            ArithOp::Bits(op) => {
                let (a, b) = input.into_pair("bitwise")?;
                let (pa, pb) = (a.into_prim("bitwise lhs")?, b.into_prim("bitwise rhs")?);
                assert_eq!(pa.len(), pb.len(), "bitwise: operands at different strata");
                Value::Prim(bits_eval(*op, pa, pb)?)
            }
            ArithOp::BitsImm(op, c) => Value::Prim(bits_imm(*op, input.into_prim("bitwise with a constant")?, *c, false)?),
            ArithOp::ImmBits(c, op) => Value::Prim(bits_imm(*op, input.into_prim("bitwise with a constant")?, *c, true)?),
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
            ArithOp::IntScatter(r) => {
                let (keys, vals) = input.into_pair("int_scatter")?;
                let (kb, keys) = keys.into_list("int_scatter keys")?;
                let (vb, vals) = vals.into_list("int_scatter values")?;
                int_scatter_rows(&kb, &keys, &vb, vals, *r)?
            }
            ArithOp::HashScatter(r) => {
                let (keys, vals) = input.into_pair("hash_scatter")?;
                let (kb, keys) = keys.into_list("hash_scatter keys")?;
                let (vb, vals) = vals.into_list("hash_scatter values")?;
                let exact = matches!(keys, Value::Prim(_));
                hash_scatter_rows(&kb, &keys, &vb, vals, *r, &crate::hash::hash, exact)?
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
