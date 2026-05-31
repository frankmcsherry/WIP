//! The numeric layer over the core — the first stacked vocabulary on `OpLike`.
//! `NumOp` embeds the whole core `Op` (now structure + comparison only) via `Core`
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
    AddU64(u64),           // U64 -> U64   x + c   (kept sugar)
    Shr(u32),              // U64 -> U64   x >> k  (= ÷ 2^k; the SIMD-vectorizable divide, USHR)
    And(u64),              // U64 -> U64   x & m   (= mod 2^k with m = 2^k-1; the SIMD modulo, AND)
    ReduceSum,             // List<U64> -> U64      (unsigned reduce)
}

// op-match without a `num_traits` dependency: `wrapping_*` are inherent on every uN/iN.
macro_rules! wrap {
    ($op:expr, $x:expr, $y:expr) => {
        match $op {
            BinOp::Add => $x.wrapping_add($y),
            BinOp::Sub => $x.wrapping_sub($y),
            BinOp::Mul => $x.wrapping_mul($y),
        }
    };
}

// list the widths ONCE; generate the per-width binary/unary leaf arithmetic. `Kind::I`
// deswizzles (XOR the top bit, `as iN`), operates natively, reswizzles. Mirrors `prim!`.
macro_rules! grid {
    ($($V:ident => $u:ty : $i:ty),+ $(,)?) => {
        fn bin_eval(op: BinOp, kind: Kind, a: &Prim, b: &Prim) -> Prim {
            match (a, b) {
                $( (Prim::$V(av), Prim::$V(bv)) => Prim::$V(Arc::new(
                    av.iter().zip(bv.iter()).map(|(&x, &y)| match kind {
                        Kind::U => wrap!(op, x, y),
                        Kind::I => {
                            let m = !(<$u>::MAX >> 1); // the top bit = the swizzle
                            (wrap!(op, (x ^ m) as $i, (y ^ m) as $i) as $u) ^ m
                        }
                    }).collect(),
                )), )+
                _ => panic!("arith: operand width mismatch"),
            }
        }

        fn neg_eval(kind: Kind, a: &Prim) -> Prim {
            match a {
                $( Prim::$V(av) => Prim::$V(Arc::new(
                    av.iter().map(|&x| match kind {
                        Kind::U => x.wrapping_neg(),
                        Kind::I => {
                            let m = !(<$u>::MAX >> 1);
                            (((x ^ m) as $i).wrapping_neg() as $u) ^ m
                        }
                    }).collect(),
                )), )+
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
                Value::Prim(bin_eval(*op, *kind, &pa, &pb))
            }
            ArithOp::Neg(kind, _) => Value::Prim(neg_eval(*kind, &input.into_prim("Neg"))),
            ArithOp::AddU64(c) => {
                // in place when the input is uniquely owned: move-on-last-use leaves the leaf at
                // refcount 1, so `into_u64` MOVES the buffer out; we mutate it and re-wrap, with no
                // fresh output column (no allocation, no write-allocate). Shared input falls back to
                // a clone inside `into_u64`, so this stays correct either way.
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

/// the numeric layer: the core (structural) vocabulary plus the `cmp` (comparison/order) and `arith` buckets.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum NumOp {
    Core(Op<NumOp>),
    Cmp(CmpOp),
    Arith(ArithOp),
}

impl OpLike for NumOp {
    fn eval(&self, input: Value) -> Value {
        match self {
            NumOp::Core(c) => c.eval(input),
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
        }
    }
    fn judge(&self, input: &Shape) -> Result<Shape, String> {
        match self {
            NumOp::Core(c) => c.judge(input),
            NumOp::Cmp(c) => c.judge(input),
            NumOp::Arith(a) => a.judge(input),
        }
    }
    fn children(&self) -> Vec<&Graph<NumOp>> {
        match self {
            NumOp::Core(c) => c.children(), // core bodies ARE Graph<NumOp> now
            NumOp::Cmp(_) | NumOp::Arith(_) => Vec::new(),
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
