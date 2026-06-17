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
use crate::shape::Shape;
use crate::value::{Prim, Value};
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
pub fn lit_value(kind: Kind, width: u32, n: u64) -> Value {
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
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Red {
    Add, // `reduce_add` (sum)
    Mul, // `reduce_mul` (product)
    Min,
    Max,
    All,
    Any,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div, // FLOAT-ONLY (integer div deferred: no NEON op, div-by-zero would panic). x/0 -> ±inf, 0/0 -> NaN.
    // NB: lane-wise min/max are NOT here — they're kind-blind order ops (byte min/max on the
    // order-preserving leaf needs no deswizzle), so they live in `cmp` as `CmpOp::Min`/`Max`.
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
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
fn enc_f32(f: f32) -> u32 {
    let b = f.to_bits();
    if b >> 31 == 1 { !b } else { b ^ (1 << 31) }
}
fn dec_f32(u: u32) -> f32 {
    f32::from_bits(if u >> 31 == 1 { u ^ (1 << 31) } else { !u })
}
fn enc_f64(f: f64) -> u64 {
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
}
fn dec_f64(u: u64) -> f64 {
    f64::from_bits(if u >> 63 == 1 { u ^ (1 << 63) } else { !u })
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp, Kind, u32), // binary leaf arithmetic at a bit-width
    Neg(Kind, u32),        // unary negate
    ToSigned,              // leaf -> leaf  XOR the sign bit (any width): unsigned <-> signed encoding,
                           // the kind-conversion `signed` (an involution; how a column enters Kind::I)
    ToFloat(u32),          // U-int leaf -> float leaf (w in {32,64}): each unsigned int -> the float of
                           // the same width, total-order encoded. `to_f32`/`to_f64`: how iota becomes floats.
    AddU64(u64),           // U64 -> U64   x + c   (sugar)
    Shr(u32),              // U64 -> U64   x >> k  (= ÷ 2^k; the SIMD-vectorizable divide, USHR)
    And(u64),              // U64 -> U64   x & m   (= mod 2^k with m = 2^k-1; the SIMD modulo, AND)
    Reduce(Red),           // List<U64> -> U64      per-row monoid reduction (sum/prod/min/max/all/any)
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
                $( (Prim::$V(av), Prim::$V(bv)) => Prim::$V(match (kind, op) {
                    (Kind::U, BinOp::Add) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_add(y)),
                    (Kind::U, BinOp::Sub) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_sub(y)),
                    (Kind::U, BinOp::Mul) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_mul(y)),
                    (Kind::I, BinOp::Add) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_add)),
                    (Kind::I, BinOp::Sub) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_sub)),
                    (Kind::I, BinOp::Mul) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_mul)),
                    // integer division is deferred; the judge rejects it, so this is never reached.
                    (Kind::U, BinOp::Div) | (Kind::I, BinOp::Div) => {
                        unreachable!("integer Div is rejected by judge")
                    }
                    // float is dispatched by `bin_eval` before reaching here.
                    (Kind::F, _) => unreachable!("int_bin: float dispatched by bin_eval"),
                }), )+
                _ => panic!("arith: operand width mismatch"),
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
/// grid. The judge has already rejected float at widths 8/16 and integer `Div`, so the fallthroughs panic.
fn bin_eval(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
    match kind {
        Kind::F => float_bin(op, a, b),
        _ => int_bin(op, kind, a, b),
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
        })}))
    }}
    match (a, b) {
        (Prim::U32(av), Prim::U32(bv)) => f!(U32, dec_f32, enc_f32, av, bv),
        (Prim::U64(av), Prim::U64(bv)) => f!(U64, dec_f64, enc_f64, av, bv),
        _ => panic!("float arith expects f32/f64 (width 32/64)"),
    }
}

impl ArithOp {
    fn eval(&self, input: Value) -> Value {
        match self {
            ArithOp::Bin(op, kind, _) => {
                let (a, b) = input.into_pair("binary arith");
                let (pa, pb) = (a.into_prim("binary arith lhs"), b.into_prim("binary arith rhs"));
                assert_eq!(pa.len(), pb.len(), "binary arith: operands at different strata");
                Value::Prim(bin_eval(*op, *kind, pa, pb))
            }
            ArithOp::Neg(kind, _) => Value::Prim(neg_eval(*kind, input.into_prim("Neg"))),
            ArithOp::ToSigned => Value::Prim(input.into_prim("signed").xor_signbit()),
            ArithOp::ToFloat(w) => Value::Prim(match (w, input.into_prim("to_float")) {
                (32, Prim::U32(v)) => Prim::U32(neg_into(v, |x| enc_f32(x as f32))),
                (64, Prim::U64(v)) => Prim::U64(neg_into(v, |x| enc_f64(x as f64))),
                _ => panic!("to_float: width/leaf mismatch"),
            }),
            ArithOp::AddU64(c) => {
                // in place when uniquely owned: `into_u64` moves the buffer out at refcount 1, else clones.
                let mut xs = input.into_u64("AddU64");
                xs.iter_mut().for_each(|x| *x = x.wrapping_add(*c));
                Value::u64(xs)
            }
            // in place, like AddU64. Both vectorize (vector shift / vector AND) — the SIMD forms of
            // divide / modulo by a power of two, which general integer div/mod lack on NEON.
            ArithOp::Shr(k) => {
                let mut xs = input.into_u64("Shr");
                xs.iter_mut().for_each(|x| *x >>= *k);
                Value::u64(xs)
            }
            ArithOp::And(m) => {
                let mut xs = input.into_u64("And");
                xs.iter_mut().for_each(|x| *x &= *m);
                Value::u64(xs)
            }
            ArithOp::Reduce(r) => {
                let (bounds, vals) = input.into_list("reduce");
                let xs = vals.into_u64("reduce values");
                let mut out = Vec::with_capacity(bounds.len());
                let mut start = 0;
                for end in bounds {
                    let s = &xs[start..end]; // empty row -> the monoid identity
                    out.push(match r {
                        Red::Add => s.iter().sum(),
                        Red::Mul => s.iter().product(),
                        Red::Min => s.iter().copied().min().unwrap_or(u64::MAX),
                        Red::Max => s.iter().copied().max().unwrap_or(0),
                        Red::All => s.iter().all(|&x| x != 0) as u64,
                        Red::Any => s.iter().any(|&x| x != 0) as u64,
                    });
                    start = end;
                }
                Value::u64(out)
            }
        }
    }

    /// shape rule: kind-blind, leaf widths only.
    fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        Ok(match self {
            ArithOp::Bin(op, kind, w) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float arith only at width 32/64, got {w}"));
                }
                if matches!(op, BinOp::Div) && !matches!(kind, Kind::F) {
                    return Err("integer div is deferred — div is float-only (use div_f32/div_f64)".into());
                }
                match input {
                    Prod(ts) if ts.len() == 2 && ts[0] == Prim(*w) && ts[1] == Prim(*w) => Prim(*w),
                    _ => return Err(format!("binary arith expects (U{w}, U{w}), got {input}")),
                }
            }
            ArithOp::Neg(kind, w) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float neg only at width 32/64, got {w}"));
                }
                match input {
                    Prim(b) if b == w => Prim(*w),
                    _ => return Err(format!("Neg expects U{w}, got {input}")),
                }
            }
            ArithOp::ToSigned => match input {
                Prim(w) => Prim(*w),
                _ => return Err(format!("signed expects a leaf, got {input}")),
            },
            ArithOp::ToFloat(w) => match input {
                Prim(b) if b == w && matches!(w, 32 | 64) => Prim(*w),
                _ => return Err(format!("to_float expects a U{w} leaf (w in 32/64), got {input}")),
            },
            ArithOp::AddU64(_) | ArithOp::Shr(_) | ArithOp::And(_) => match input {
                Prim(64) => Prim(64),
                _ => return Err(format!("U64-constant arith expects U64, got {input}")),
            },
            ArithOp::Reduce(_) => match input {
                List(t) if **t == Prim(64) => Prim(64),
                _ => return Err(format!("reduce expects List<U64>, got {input}")),
            },
        })
    }
}

/// the standard vocabulary: the core (structural) ops plus the `cmp` (comparison/order),
/// `arith`, and `text` buckets — the layer the `ml` surface and the optimizer are typed at.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum NumOp {
    Core(Op<NumOp>),
    Cmp(CmpOp),
    Arith(ArithOp),
    Text(TextOp),
}

impl OpLike for NumOp {
    fn eval(&self, input: Value) -> Value {
        match self {
            NumOp::Core(c) => c.eval(input),
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
            NumOp::Text(t) => t.eval(input),
        }
    }
    fn judge(&self, input: &Shape) -> Result<Shape, String> {
        match self {
            NumOp::Core(c) => c.judge(input),
            NumOp::Cmp(c) => c.judge(input),
            NumOp::Arith(a) => a.judge(input),
            NumOp::Text(t) => t.judge(input),
        }
    }
    fn children(&self) -> Vec<&Graph<NumOp>> {
        match self {
            NumOp::Core(c) => c.children(), // core bodies are Graph<NumOp>
            NumOp::Cmp(_) | NumOp::Arith(_) | NumOp::Text(_) => Vec::new(),
        }
    }
}

// ergonomic embedding: `b.add(Field(1), …)` / `b.add(SortList, …)` work without wrapping.
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
