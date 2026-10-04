//! The numeric layer over the core — the first stacked vocabulary on `OpLike`.
//! `NumOp` embeds the whole core `Op` (structure and comparison) via `Core`
//! and adds arithmetic via `Arith`. The same `Graph`/`eval_graph`/`shape_of`
//! machinery runs it unchanged; the core never learns arithmetic.
//!
//! Arithmetic is the (op × kind × width) GRID — `Bin(op, kind, bits)` / `Neg(kind, bits)`,
//! macro-generated over the widths. A leaf is always `Prim::Uw`; `Kind::U` reads the bytes
//! as the value (native wrapping ops), `Kind::I` reads them as an order-preserving *swizzled*
//! signed value (XOR the top bit — `enc_i64` generalized per width). All interpretation lives
//! here; the shape-checker sees plain leaf ops, never the kinds.

use super::cmp::CmpOp;
use super::core::Op;
use super::text::TextOp;
use crate::graph::{Graph, OpLike};

use crate::value::{Bounds, Prim, Value};
use std::sync::Arc;

/// order-preserving encode/decode for signed 64-bit integers.
pub fn enc_i64(x: i64) -> u64 {
    (x as u64) ^ (1 << 63)
}
pub fn dec_i64(u: u64) -> i64 {
    (u ^ (1 << 63)) as i64
}

/// a typed scalar literal: the value `n` encoded for `kind` at `width` — raw for `U`, sign-swizzled
/// for `I` (the order-preserving form the leaf stores). The surface `lit_<k><w> N` lowers to
/// `Op::Lit` of this.
pub(crate) fn lit_value(kind: Kind, width: u32, n: u64) -> Value {
    let raw = match width {
        8 => Prim::U8(Arc::new(vec![n as u8])),
        16 => Prim::U16(Arc::new(vec![n as u16])),
        32 => Prim::U32(Arc::new(vec![n as u32])),
        64 => Prim::U64(Arc::new(vec![n])),
        _ => panic!("lit: unsupported width {width}"),
    };
    Value::Prim(if matches!(kind, Kind::I) { raw.xor_signbit() } else { raw })
}

/// the named monoid reductions — `List<U64> -> U64` per row, each a one-pass SIMD-friendly horizontal
/// fold (the fast paths a general `fold` over the same monoid would be ~20x slower than). `Min`/`Max`
/// are kind-blind (the order-preserving bytes make them correct for signed/float too); `Sum`/`Prod`
/// are unsigned; `All`/`Any` are the 0/1-mask AND/OR.
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
    Div, // Integer: truncating, x/0 = 0, signed MIN/-1 wraps. Float: IEEE division.
    Rem, // INTEGER-ONLY (the float remainder has no caller). `x % 0 = x`: a total definition, so the
         // lane body needs no branch out and callers that guard the divisor pay nothing. It is the
         // "no reduction" reading of a zero modulus, which is what DDIR's `hash(0, ..)` means.
    // NB: lane-wise min/max are NOT here — they're kind-blind order ops (byte min/max on the
    // order-preserving leaf needs no deswizzle), so they live in `cmp` as `CmpOp::Min`/`Max`.
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    U, // unsigned: the bytes ARE the value
    I, // signed: the bytes are an order-preserving swizzle of the value
    F, // float (32/64 only): the bytes are the IEEE bits under the TOTAL-order swizzle. Arithmetic is
       // IEEE (NaN/inf propagate, div-by-zero -> inf/NaN, no panic); ordering/equality is total, NOT
       // IEEE — NaN is orderable (sorts to the top) and equals itself bit-for-bit, -0 != +0. (See NOTES.)
}

/// IEEE-bits <-> total-order encoding for f32 (and f64 below): negatives flip all bits, non-negatives
/// flip just the sign bit, so the unsigned byte order is the float total order (`f64::total_cmp`). The
/// kind-blind comparator then sorts/compares floats correctly with no special case.
pub(crate) fn enc_f32(f: f32) -> u32 {
    let b = f.to_bits();
    if b >> 31 == 1 { !b } else { b ^ (1 << 31) }
}
fn dec_f32(u: u32) -> f32 {
    f32::from_bits(if u >> 31 == 1 { u ^ (1 << 31) } else { !u })
}
pub(crate) fn enc_f64(f: f64) -> u64 {
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
}
fn dec_f64(u: u64) -> f64 {
    f64::from_bits(if u >> 63 == 1 { u ^ (1 << 63) } else { !u })
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp, Kind, u32), // binary leaf arithmetic at a bit-width
    BinImm(BinOp, Kind, u32, u64), // the same with a constant right operand: `x op c`, where `c` is the
                           // constant's stored bits at that width (its kind's encoding). One pass over
                           // `x`, in place when it is uniquely owned; no column of `c` is built.
    Neg(Kind, u32),        // unary negate
    ToSigned,              // leaf -> leaf  XOR the sign bit (any width): unsigned <-> signed encoding,
                           // the kind-conversion `signed` (an involution; how a column enters Kind::I)
    ToFloat(u32),          // U-int leaf -> float leaf (w in {32,64}): each unsigned int -> the float of
                           // the same width, total-order encoded. `to_f32`/`to_f64`: how iota becomes floats.
    Shr(u32),              // U64 -> U64   x >> k  (= ÷ 2^k; the SIMD-vectorizable divide, USHR)
    And(u64),              // U64 -> U64   x & m   (= mod 2^k with m = 2^k-1; the SIMD modulo, AND)
    Reduce(Red, Kind, u32), // List<X> -> X      per-row monoid reduction (sum/prod/min/max/all/any),
                           // in row order. Sum and product are at a kind and width, as `Bin` is
                           // (`fold_add` is the u64 sum, `fold_add_f64` the f64 one, which adds in
                           // row order and so matches a fold of `add_f64` bit for bit); min, max, all
                           // and any read the stored order, so they ignore the kind and take any width.
    Scan(Red, Kind, u32),  // List<X> -> List<X>  per-row inclusive monoid PREFIX scan of the same
                           // monoids. The fast path for `scan` with a monoid body: one in-place pass,
                           // where the general `FoldScan` re-evals the body per element (catastrophic
                           // on one long row — see performance.md). `Reduce` is its drop-the-prefix
                           // sibling.
}

// deswizzle the order-preserving signed encoding (XOR the top bit `m`), apply a native wrapping op,
// reswizzle. `m` is a per-width constant. This is the `Kind::I` lane body, factored so the grid's
// six (kind × op) arms each stay a one-line lane map. `wrapping_*` are inherent on every uN/iN, so
// no `num_traits` dependency.
macro_rules! swiz {
    ($u:ty, $i:ty, $x:ident, $y:ident, $op:ident) => {{
        let m = !(<$u>::MAX >> 1);
        ((($x ^ m) as $i).$op(($y ^ m) as $i) as $u) ^ m
    }};
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

/// apply a binary lane op `f` against the constant `c`, in place when `a` is uniquely owned, else
/// fresh. The immediate sibling of `bin_into`: `f(x, c)` is exactly what `bin_into` computes when
/// every row of the right operand is `c`.
fn imm_into<T: Copy>(mut a: Arc<Vec<T>>, c: T, f: impl Fn(T, T) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut a) {
        for x in dst.iter_mut() { *x = f(*x, c); }
        a
    } else {
        Arc::new(a.iter().map(|&x| f(x, c)).collect())
    }
}

/// the integer (kind × op) lane bodies, written once: `$apply($($arg),*, body)` for the body of the
/// cell `($kind, $op)` at unsigned type `$u` and signed type `$i`. `int_bin` applies them to two
/// columns and `int_imm` to a column and a constant, so the two can't disagree.
macro_rules! int_arms {
    ($u:ty, $i:ty, $kind:expr, $op:expr, $apply:ident($($arg:expr),*)) => {
        match ($kind, $op) {
            (Kind::U, BinOp::Add) => $apply($($arg,)* |x: $u, y: $u| x.wrapping_add(y)),
            (Kind::U, BinOp::Sub) => $apply($($arg,)* |x: $u, y: $u| x.wrapping_sub(y)),
            (Kind::U, BinOp::Mul) => $apply($($arg,)* |x: $u, y: $u| x.wrapping_mul(y)),
            (Kind::I, BinOp::Add) => $apply($($arg,)* |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_add)),
            (Kind::I, BinOp::Sub) => $apply($($arg,)* |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_sub)),
            (Kind::I, BinOp::Mul) => $apply($($arg,)* |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_mul)),
            (Kind::U, BinOp::Rem) => $apply($($arg,)* |x: $u, y: $u| if y == 0 { x } else { x % y }),
            // `wrapping_rem` for the MIN % -1 overflow; the zero divisor is the total `x % 0 = x`.
            (Kind::I, BinOp::Rem) => $apply($($arg,)* |x: $u, y: $u| {
                let m = !(<$u>::MAX >> 1);
                if (y ^ m) as $i == 0 { x } else { swiz!($u, $i, x, y, wrapping_rem) }
            }),
            (Kind::U, BinOp::Div) => $apply($($arg,)* |x: $u, y: $u| if y == 0 { 0 } else { x / y }),
            (Kind::I, BinOp::Div) => $apply($($arg,)* |x: $u, y: $u| {
                let m = !(<$u>::MAX >> 1);
                if (y ^ m) as $i == 0 { m } else { swiz!($u, $i, x, y, wrapping_div) }
            }),
            // float is dispatched by `bin_eval`/`imm_eval` before reaching here.
            (Kind::F, _) => unreachable!("int arithmetic: float dispatched by bin_eval/imm_eval"),
        }
    };
}

/// apply a unary lane op `f` in place when the operand is uniquely owned, else fresh.
fn neg_into<T: Copy>(mut a: Arc<Vec<T>>, f: impl Fn(T) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut a) {
        for x in dst.iter_mut() { *x = f(*x); }
        a
    } else {
        Arc::new(a.iter().map(|&x| f(x)).collect())
    }
}

// list the widths ONCE; generate the per-width binary/unary leaf arithmetic. Mirrors `prim!`.
// The (kind × op) dispatch is HOISTED ABOVE the lane loop: each arm matches once, picks ONE concrete
// closure, then makes a single tight pass — no per-element branch to keep the vectorizer out.
// `Kind::U` is native wrapping; `Kind::I` deswizzles/reswizzles via `swiz!`.
macro_rules! grid {
    ($($V:ident => $u:ty : $i:ty),+ $(,)?) => {
        fn int_bin(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
            match (a, b) {
                $( (Prim::$V(av), Prim::$V(bv)) => Prim::$V(int_arms!($u, $i, kind, op, bin_into(av, bv))), )+
                _ => panic!("arith: operand width mismatch"),
            }
        }

        // `c` is the constant's stored bits; at a narrow width they fit (the front end checks).
        #[allow(clippy::unnecessary_cast)]
        fn int_imm(op: BinOp, kind: Kind, a: Prim, c: u64) -> Prim {
            match a {
                $( Prim::$V(av) => Prim::$V(int_arms!($u, $i, kind, op, imm_into(av, c as $u))), )+
            }
        }

        fn int_neg(kind: Kind, a: Prim) -> Prim {
            match a {
                $( Prim::$V(av) => Prim::$V(match kind {
                    Kind::U => neg_into(av, |x: $u| x.wrapping_neg()),
                    Kind::I => neg_into(av, |x: $u| {
                        let m = !(<$u>::MAX >> 1);
                        (((x ^ m) as $i).wrapping_neg() as $u) ^ m
                    }),
                    Kind::F => unreachable!("int_neg: float dispatched by neg_eval"),
                }), )+
            }
        }
    };
}
grid! { U8 => u8:i8, U16 => u16:i16, U32 => u32:i32, U64 => u64:i64 }

/// the binary leaf op, dispatching `Kind::F` to the float path (32/64 only) and `U`/`I` to the macro
/// grid. `eval` has already rejected float at widths 8/16, so the fallthroughs panic.
fn bin_eval(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
    match kind {
        Kind::F => float_bin(op, a, b),
        _ => int_bin(op, kind, a, b),
    }
}

/// `x op c` with `c` the constant's stored bits, dispatching float to `float_imm`.
fn imm_eval(op: BinOp, kind: Kind, a: Prim, c: u64) -> Prim {
    match kind {
        Kind::F => float_imm(op, a, c),
        _ => int_imm(op, kind, a, c),
    }
}

fn neg_eval(kind: Kind, a: Prim) -> Prim {
    match kind {
        Kind::F => match a {
            Prim::U32(v) => Prim::U32(neg_into(v, |u| enc_f32(-dec_f32(u)))),
            Prim::U64(v) => Prim::U64(neg_into(v, |u| enc_f64(-dec_f64(u)))),
            _ => panic!("float neg expects f32/f64"),
        },
        _ => int_neg(kind, a),
    }
}

/// IEEE float arithmetic on the total-order-encoded leaf: deswizzle both operands, apply the native
/// op (NaN/inf propagate, div-by-zero -> inf/NaN — no panic), re-encode. `min`/`max` use IEEE's
/// (NaN-skipping) float min/max; the *ordering* used by sort/`Rel` is the total order, separately.
fn float_bin(op: BinOp, a: Prim, b: Prim) -> Prim {
    macro_rules! f { ($V:ident, $dec:ident, $enc:ident, $av:ident, $bv:ident) => {
        Prim::$V(bin_into($av, $bv, |x, y| { let (x, y) = ($dec(x), $dec(y)); $enc(match op {
            BinOp::Add => x + y, BinOp::Sub => x - y, BinOp::Mul => x * y, BinOp::Div => x / y,
            BinOp::Rem => unreachable!("float Rem is rejected before dispatch"),
        })}))
    }}
    match (a, b) {
        (Prim::U32(av), Prim::U32(bv)) => f!(U32, dec_f32, enc_f32, av, bv),
        (Prim::U64(av), Prim::U64(bv)) => f!(U64, dec_f64, enc_f64, av, bv),
        _ => panic!("float arith expects f32/f64 (width 32/64)"),
    }
}

/// float `x op c` on the encoded leaf: the constant decodes once, each lane as in `float_bin`.
fn float_imm(op: BinOp, a: Prim, c: u64) -> Prim {
    macro_rules! f { ($V:ident, $dec:ident, $enc:ident, $av:ident, $c:expr) => {{
        let y = $dec($c);
        Prim::$V(imm_into($av, $c, |x, _| { let x = $dec(x); $enc(match op {
            BinOp::Add => x + y, BinOp::Sub => x - y, BinOp::Mul => x * y, BinOp::Div => x / y,
            BinOp::Rem => unreachable!("float Rem is rejected before dispatch"),
        })}))
    }}}
    match a {
        Prim::U32(av) => f!(U32, dec_f32, enc_f32, av, c as u32),
        Prim::U64(av) => f!(U64, dec_f64, enc_f64, av, c),
        _ => panic!("float arith expects f32/f64 (width 32/64)"),
    }
}

impl ArithOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            ArithOp::Bin(op, kind, w) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float arith only at width 32/64, got {w}"));
                }
                if matches!(op, BinOp::Rem) && matches!(kind, Kind::F) {
                    return Err("rem is integer-only".into());
                }
                let (a, b) = input.into_pair("binary arith")?;
                let (pa, pb) = (a.into_prim("binary arith lhs")?, b.into_prim("binary arith rhs")?);
                if pa.bits() != *w || pb.bits() != *w {
                    return Err(format!("binary arith expects (U{w}, U{w}), got (U{}, U{})", pa.bits(), pb.bits()));
                }
                assert_eq!(pa.len(), pb.len(), "binary arith: operands at different strata");
                Value::Prim(bin_eval(*op, *kind, pa, pb))
            }
            ArithOp::BinImm(op, kind, w, c) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float arith only at width 32/64, got {w}"));
                }
                if matches!(op, BinOp::Rem) && matches!(kind, Kind::F) {
                    return Err("rem is integer-only".into());
                }
                let p = input.into_prim("arith with a constant")?;
                if p.bits() != *w {
                    return Err(format!("arith with a U{w} constant expects U{w}, got U{}", p.bits()));
                }
                Value::Prim(imm_eval(*op, *kind, p, *c))
            }
            ArithOp::Neg(kind, w) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float neg only at width 32/64, got {w}"));
                }
                let p = input.into_prim("Neg")?;
                if p.bits() != *w {
                    return Err(format!("Neg expects U{w}, got U{}", p.bits()));
                }
                Value::Prim(neg_eval(*kind, p))
            }
            ArithOp::ToSigned => Value::Prim(input.into_prim("signed")?.xor_signbit()),
            ArithOp::ToFloat(w) => Value::Prim(match (w, input.into_prim("to_float")?) {
                (32, Prim::U32(v)) => Prim::U32(neg_into(v, |x| enc_f32(x as f32))),
                (64, Prim::U64(v)) => Prim::U64(neg_into(v, |x| enc_f64(x as f64))),
                (w, p) => return Err(format!("to_float expects a U{w} leaf (w in 32/64), got U{}", p.bits())),
            }),
            // in place when uniquely owned. Both vectorize (vector shift / vector AND) — the SIMD forms of
            // divide / modulo by a power of two, which general integer div/mod lack on NEON.
            ArithOp::Shr(k) => {
                let mut xs = input.into_u64("Shr")?;
                xs.iter_mut().for_each(|x| *x >>= *k);
                Value::u64(xs)
            }
            ArithOp::And(m) => {
                let mut xs = input.into_u64("And")?;
                xs.iter_mut().for_each(|x| *x &= *m);
                Value::u64(xs)
            }
            ArithOp::Reduce(r, kind, w) => {
                let (bounds, vals) = input.into_list("reduce")?;
                Value::Prim(monoid_rows(&bounds, vals.into_prim("reduce values")?, *r, *kind, *w, false)?)
            }
            ArithOp::Scan(r, kind, w) => {
                let (bounds, vals) = input.into_list("scan")?;
                let prefixes = monoid_rows(&bounds, vals.into_prim("scan values")?, *r, *kind, *w, true)?;
                Value::List(bounds, Box::new(Value::Prim(prefixes)))
            }
        })
    }

}

/// Each row of `xs` reduced by the monoid `(id, f)` in row order (`scan` false: one value per row,
/// the identity for an empty row, reading `xs` in place), or replaced by its inclusive prefixes
/// (`scan` true: written over `xs`, which is copied first only if it is shared). The monoid works on
/// decoded values (`dec`, `enc` between the stored bits and them), so a running total stays decoded
/// and only what is stored is encoded.
fn fold_rows<T: Copy, A: Copy>(
    bounds: &Bounds,
    xs: Arc<Vec<T>>,
    scan: bool,
    id: A,
    dec: impl Fn(T) -> A,
    f: impl Fn(A, A) -> A,
    enc: impl Fn(A) -> T,
) -> Vec<T> {
    let mut start = 0;
    if !scan {
        let mut out = Vec::with_capacity(bounds.len());
        for end in bounds.ends() {
            out.push(enc(xs[start..end].iter().fold(id, |a, &x| f(a, dec(x)))));
            start = end;
        }
        return out;
    }
    let mut xs = Arc::unwrap_or_clone(xs);
    for end in bounds.ends() {
        let mut acc = id;
        for slot in &mut xs[start..end] {
            acc = f(acc, dec(*slot));
            *slot = enc(acc);
        }
        start = end;
    }
    xs
}

/// A monoid reduction or inclusive scan of each row of `p`, on its stored bits. Sum and product
/// combine at `kind` (unsigned wrapping; signed wrapping through the order-preserving encoding; float
/// in IEEE arithmetic) and need values `w` bits wide. Min and max compare the stored bits, which is
/// the value's order for every kind, and all and any test them against zero; those four take any
/// width.
fn monoid_rows(bounds: &Bounds, p: Prim, r: Red, kind: Kind, w: u32, scan: bool) -> Result<Prim, String> {
    let typed = matches!(r, Red::Add | Red::Mul);
    if typed && p.bits() != w {
        return Err(format!("a {kind:?}{w} sum or product expects U{w} values, got U{}", p.bits()));
    }
    fn same<T>(x: T) -> T {
        x
    }
    macro_rules! ints {
        ($v:expr, $V:ident, $u:ty, $i:ty) => {{
            let xs = $v;
            let m: $u = !(<$u>::MAX >> 1); // the sign bit, which the signed encoding flips
            Prim::$V(Arc::new(match (r, kind) {
                // signed: the order-preserving encoding flips the sign bit; the total is kept decoded
                (Red::Add, Kind::I) => fold_rows(bounds, xs, scan, 0 as $i, |x: $u| (x ^ m) as $i, <$i>::wrapping_add, |a: $i| a as $u ^ m),
                (Red::Mul, Kind::I) => fold_rows(bounds, xs, scan, 1 as $i, |x: $u| (x ^ m) as $i, <$i>::wrapping_mul, |a: $i| a as $u ^ m),
                // integer sums and products wrap (the totality invariant), so reducing raw
                // two's-complement differences (a negative one is a large u64) gives the right sum.
                (Red::Add, _) => fold_rows(bounds, xs, scan, 0, same, <$u>::wrapping_add, same),
                (Red::Mul, _) => fold_rows(bounds, xs, scan, 1, same, <$u>::wrapping_mul, same),
                (Red::Min, _) => fold_rows(bounds, xs, scan, <$u>::MAX, same, <$u>::min, same),
                (Red::Max, _) => fold_rows(bounds, xs, scan, 0, same, <$u>::max, same),
                (Red::All, _) => fold_rows(bounds, xs, scan, 1, same, |a: $u, x: $u| a & (x != 0) as $u, same),
                (Red::Any, _) => fold_rows(bounds, xs, scan, 0, same, |a: $u, x: $u| a | (x != 0) as $u, same),
            }))
        }};
    }
    macro_rules! floats {
        ($v:expr, $V:ident, $u:ty, $dec:ident, $enc:ident) => {{
            let xs = $v;
            Prim::$V(Arc::new(match r {
                Red::Add => fold_rows(bounds, xs, scan, 0.0, $dec, |a, x| a + x, $enc),
                _ => fold_rows(bounds, xs, scan, 1.0, $dec, |a, x| a * x, $enc),
            }))
        }};
    }
    Ok(match p {
        Prim::U32(v) if typed && kind == Kind::F => floats!(v, U32, u32, dec_f32, enc_f32),
        Prim::U64(v) if typed && kind == Kind::F => floats!(v, U64, u64, dec_f64, enc_f64),
        _ if typed && kind == Kind::F => return Err(format!("float sums and products only at width 32/64, got {w}")),
        Prim::U8(v) => ints!(v, U8, u8, i8),
        Prim::U16(v) => ints!(v, U16, u16, i16),
        Prim::U32(v) => ints!(v, U32, u32, i32),
        Prim::U64(v) => ints!(v, U64, u64, i64),
    })
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
