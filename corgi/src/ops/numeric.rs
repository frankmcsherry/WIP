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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Min, // lane-wise minimum — order-sensitive, so kind-aware (a SIMD-LCD reduction's binary core)
    Max, // lane-wise maximum
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    U, // unsigned: the bytes ARE the value
    I, // signed: the bytes are an order-preserving swizzle of the value
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp, Kind, u32), // binary leaf arithmetic at a bit-width
    Neg(Kind, u32),        // unary negate
    AddU64(u64),           // U64 -> U64   x + c   (sugar)
    Shr(u32),              // U64 -> U64   x >> k  (= ÷ 2^k; the SIMD-vectorizable divide, USHR)
    And(u64),              // U64 -> U64   x & m   (= mod 2^k with m = 2^k-1; the SIMD modulo, AND)
    ReduceSum,             // List<U64> -> U64      (unsigned reduce)
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
        fn bin_eval(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
            match (a, b) {
                $( (Prim::$V(av), Prim::$V(bv)) => Prim::$V(match (kind, op) {
                    (Kind::U, BinOp::Add) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_add(y)),
                    (Kind::U, BinOp::Sub) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_sub(y)),
                    (Kind::U, BinOp::Mul) => bin_into(av, bv, |x: $u, y: $u| x.wrapping_mul(y)),
                    (Kind::I, BinOp::Add) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_add)),
                    (Kind::I, BinOp::Sub) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_sub)),
                    (Kind::I, BinOp::Mul) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_mul)),
                    // min/max are total (no overflow). Unsigned is native; signed picks via the swizzle —
                    // and since the swizzle is ORDER-PRESERVING, the deswizzle is unnecessary, but `swiz!`
                    // keeps the grid uniform and the result is identical.
                    (Kind::U, BinOp::Min) => bin_into(av, bv, |x: $u, y: $u| x.min(y)),
                    (Kind::U, BinOp::Max) => bin_into(av, bv, |x: $u, y: $u| x.max(y)),
                    (Kind::I, BinOp::Min) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, min)),
                    (Kind::I, BinOp::Max) => bin_into(av, bv, |x: $u, y: $u| swiz!($u, $i, x, y, max)),
                }), )+
                _ => panic!("arith: operand width mismatch"),
            }
        }

        fn neg_eval(kind: Kind, a: Prim) -> Prim {
            match a {
                $( Prim::$V(av) => Prim::$V(match kind {
                    Kind::U => neg_into(av, |x: $u| x.wrapping_neg()),
                    Kind::I => neg_into(av, |x: $u| {
                        let m = !(<$u>::MAX >> 1);
                        (((x ^ m) as $i).wrapping_neg() as $u) ^ m
                    }),
                }), )+
            }
        }
    };
}
grid! { U8 => u8:i8, U16 => u16:i16, U32 => u32:i32, U64 => u64:i64 }

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
            ArithOp::ReduceSum => {
                let (bounds, vals) = input.into_list("ReduceSum");
                let xs = vals.into_u64("ReduceSum values");
                let mut out = Vec::with_capacity(bounds.len());
                let mut start = 0;
                for end in bounds {
                    out.push(xs[start..end].iter().sum());
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
            ArithOp::Bin(_, _, w) => match input {
                Prod(ts) if ts.len() == 2 && ts[0] == Prim(*w) && ts[1] == Prim(*w) => Prim(*w),
                _ => return Err(format!("binary arith expects (U{w}, U{w}), got {input}")),
            },
            ArithOp::Neg(_, w) => match input {
                Prim(b) if b == w => Prim(*w),
                _ => return Err(format!("Neg expects U{w}, got {input}")),
            },
            ArithOp::AddU64(_) | ArithOp::Shr(_) | ArithOp::And(_) => match input {
                Prim(64) => Prim(64),
                _ => return Err(format!("U64-constant arith expects U64, got {input}")),
            },
            ArithOp::ReduceSum => match input {
                List(t) if **t == Prim(64) => Prim(64),
                _ => return Err(format!("ReduceSum expects List<U64>, got {input}")),
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
