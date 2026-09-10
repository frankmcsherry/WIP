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
    Add, // `fold_add` (sum) / `scan_add` (prefix sum)
    Mul, // `fold_mul` (product) / `scan_mul`
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
    Div, // Integer: truncating, x/0 = 0, signed MIN/-1 wraps. Float: IEEE division.
    Rem, // INTEGER-ONLY (the float remainder has no caller). `x % 0 = x`: a total definition, so the
         // lane body needs no branch out and callers that guard the divisor pay nothing. It is the
         // "no reduction" reading of a zero modulus, which is what DDIR's `hash(0, ..)` means.
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
    Scan(Red),             // List<U64> -> List<U64>  per-row inclusive monoid PREFIX scan. The monoid
                           // fast path for `scan` with a monoid body: one in-place pass, where the
                           // general `FoldScan` re-evals the body per element (catastrophic on one long
                           // row — see perf-gaps.md). `Reduce` is its drop-the-prefix sibling.
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
                    (Kind::U, BinOp::Rem) => bin_into(av, bv, |x: $u, y: $u| if y == 0 { x } else { x % y }),
                    // `wrapping_rem` for the MIN % -1 overflow; the zero divisor is the total `x % 0 = x`.
                    (Kind::I, BinOp::Rem) => bin_into(av, bv, |x: $u, y: $u| {
                        let m = !(<$u>::MAX >> 1);
                        if (y ^ m) as $i == 0 { x } else { swiz!($u, $i, x, y, wrapping_rem) }
                    }),
                    (Kind::U, BinOp::Div) => bin_into(av, bv, |x: $u, y: $u| if y == 0 { 0 } else { x / y }),
                    (Kind::I, BinOp::Div) => bin_into(av, bv, |x: $u, y: $u| {
                        let m = !(<$u>::MAX >> 1);
                        if (y ^ m) as $i == 0 { m } else { swiz!($u, $i, x, y, wrapping_div) }
                    }),
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
/// grid. `eval` has already rejected float at widths 8/16, so the fallthroughs panic.
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
            BinOp::Rem => unreachable!("float Rem is rejected before dispatch"),
        })}))
    }}
    match (a, b) {
        (Prim::U32(av), Prim::U32(bv)) => f!(U32, dec_f32, enc_f32, av, bv),
        (Prim::U64(av), Prim::U64(bv)) => f!(U64, dec_f64, enc_f64, av, bv),
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
            ArithOp::AddU64(c) => {
                // in place when uniquely owned: `into_u64` moves the buffer out at refcount 1, else clones.
                let mut xs = input.into_u64("AddU64")?;
                xs.iter_mut().for_each(|x| *x = x.wrapping_add(*c));
                Value::u64(xs)
            }
            // in place, like AddU64. Both vectorize (vector shift / vector AND) — the SIMD forms of
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
            ArithOp::Reduce(r) => {
                let (bounds, vals) = input.into_list("reduce")?;
                let xs = vals.as_u64("reduce values")?;
                let mut out = Vec::with_capacity(bounds.len());
                let mut start = 0;
                for end in bounds.ends() {
                    let s = &xs[start..end]; // empty row -> the monoid identity
                    out.push(match r {
                        // Wrapping, to match the Scan sibling (prefix!) and the Kind::U BinOp add — so
                        // reducing raw two's-complement diffs (a negative diff is a large u64) yields
                        // the correct i64 sum instead of a checked-overflow panic in debug.
                        Red::Add => s.iter().fold(0u64, |a, &x| a.wrapping_add(x)),
                        Red::Mul => s.iter().fold(1u64, |a, &x| a.wrapping_mul(x)),
                        Red::Min => s.iter().copied().min().unwrap_or(u64::MAX),
                        Red::Max => s.iter().copied().max().unwrap_or(0),
                        Red::All => s.iter().all(|&x| x != 0) as u64,
                        Red::Any => s.iter().any(|&x| x != 0) as u64,
                    });
                    start = end;
                }
                Value::u64(out)
            }
            ArithOp::Scan(r) => {
                let (bounds, vals) = input.into_list("scan")?;
                let mut xs = vals.into_u64("scan values")?; // owned -> inclusive prefix written in place
                // one monomorphic loop per monoid (no per-element dispatch); the recurrence is
                // sequential within a row, so this is a single memory pass, not a vectorizable one.
                macro_rules! prefix {
                    ($id:expr, $a:ident, $x:ident => $comb:expr) => {{
                        let mut start = 0;
                        for end in bounds.ends() {
                            let mut $a = $id;
                            for slot in &mut xs[start..end] {
                                let $x = *slot;
                                $a = $comb;
                                *slot = $a;
                            }
                            start = end;
                        }
                    }};
                }
                match r {
                    // integer Add/Mul wrap (the totality invariant); identities seed each row.
                    Red::Add => prefix!(0u64, a, x => a.wrapping_add(x)),
                    Red::Mul => prefix!(1u64, a, x => a.wrapping_mul(x)),
                    Red::Min => prefix!(u64::MAX, a, x => a.min(x)),
                    Red::Max => prefix!(0u64, a, x => a.max(x)),
                    Red::All => prefix!(1u64, a, x => a & (x != 0) as u64), // running "all nonzero so far"
                    Red::Any => prefix!(0u64, a, x => a | (x != 0) as u64), // running "any nonzero so far"
                }
                Value::List(bounds, Box::new(Value::u64(xs)))
            }
        })
    }

}

/// A `Fold` body recognized as a PRODUCT OF MONOIDS, and the reductions it becomes.
///
/// `fold ((acc, x) -> ((acc.0, x) add, acc.1 add_u64 1))` computes a sum and a count. Each field is
/// updated by an associative op from a contribution that never reads the accumulator, so the whole
/// fold is `seed_i ⊕ reduce_i(list)`: one pass per field, where the lockstep fold runs the body once
/// per ROUND, and on one long row a round is one element.
///
/// This lives in the numeric layer, not the core: whether an op is a monoid is a numeric question,
/// and `Op<L>` is deliberately blind to it. It is a physical choice made at eval time, like the
/// `strided` fast path, not an optimizer rewrite. Declining is always safe: the caller runs the
/// lockstep fold, which computes the same thing. Only `Add`/`Mul` at `Kind::U` width 64 and
/// `Min`/`Max` count as monoids here; `Sub` is not associative, and bitwise `And`/`Or` are not the
/// `All`/`Any` reductions except on 0/1 columns, which nothing here proves.
mod monoid_fold {
    use super::{ArithOp, BinOp, Kind, NumOp, Red};
    use crate::graph::{Graph, NodeKind, OpLike};
    use crate::ops::cmp::CmpOp;
    use crate::ops::core::Op;
    use crate::value::{Prim, Value};

    /// What a field contributes per element: a body node's column, or a constant. The `count`
    /// idiom is `acc.i add_u64 1`, a constant that does not depend on the element at all.
    enum Contribution {
        Node(usize),
        Const(u64),
    }

    /// How the body names the accumulator it updates.
    enum Accum {
        /// `Field(0)` of `Input`: a scalar accumulator, `fold(xs, 0, acc + x)`. The common shape;
        /// it is what a `sum` over a collected list lowers to when the surface has no reducer.
        Bare,
        /// `Field(j)` of `Field(0)` of `Input`: a product accumulator, one field per monoid.
        Fields,
    }

    pub(super) struct MonoidFold {
        accum: Accum,
        fields: Vec<(Red, Contribution)>,
        /// body nodes the element side needs, marked in graph order.
        needed: Vec<bool>,
    }

    /// The monoid an update op names, if it names one.
    fn monoid_of(kind: &NumOp) -> Option<Red> {
        match kind {
            NumOp::Arith(ArithOp::Bin(BinOp::Add, Kind::U, 64)) => Some(Red::Add),
            NumOp::Arith(ArithOp::Bin(BinOp::Mul, Kind::U, 64)) => Some(Red::Mul),
            NumOp::Arith(ArithOp::AddU64(_)) => Some(Red::Add),
            NumOp::Cmp(CmpOp::Min) => Some(Red::Min),
            NumOp::Cmp(CmpOp::Max) => Some(Red::Max),
            _ => None,
        }
    }

    /// Is `node` exactly `Field(0)` of `Input`, the accumulator itself?
    fn is_acc(g: &Graph<NumOp>, node: usize) -> bool {
        let n = &g.nodes[node];
        matches!(&n.kind, NodeKind::Op(NumOp::Core(Op::Field(0))))
            && matches!(g.nodes[n.inputs[0]].kind, NodeKind::Input)
    }

    /// Is `node` exactly `Field(j)` of the accumulator?
    fn is_acc_field(g: &Graph<NumOp>, node: usize, j: usize) -> bool {
        let outer = &g.nodes[node];
        matches!(&outer.kind, NodeKind::Op(NumOp::Core(Op::Field(i))) if *i == j)
            && is_acc(g, outer.inputs[0])
    }

    /// Does `node` name the accumulator this body updates: the whole of it, or its field `j`?
    fn names_acc(g: &Graph<NumOp>, node: usize, accum: &Accum, j: usize) -> bool {
        match accum {
            Accum::Bare => is_acc(g, node),
            Accum::Fields => is_acc_field(g, node, j),
        }
    }

    /// The constant a `Lit` node broadcasts, if it is a one-row `u64` leaf. A `Lit` reads its
    /// input only for its length, so a literal never depends on the accumulator, whatever it is
    /// anchored to; this is how `acc.1 mul 2`, which the surface spells as a lit-pair, is a
    /// constant contribution.
    fn literal_u64(g: &Graph<NumOp>, node: usize) -> Option<u64> {
        match &g.nodes[node].kind {
            NodeKind::Op(NumOp::Core(Op::Lit(Value::Prim(Prim::U64(v))))) if v.len() == 1 => Some(v[0]),
            _ => None,
        }
    }

    /// Mark `root` and everything it reads. `None` if any of it reads the accumulator, which is
    /// exactly "some node reads `Input` other than through `Field(1)`", since `Field(1)` of the
    /// body's pair is the element.
    fn mark_elem_side(g: &Graph<NumOp>, root: usize, needed: &mut [bool]) -> Option<()> {
        let mut stack = vec![root];
        while let Some(i) = stack.pop() {
            if std::mem::replace(&mut needed[i], true) {
                continue;
            }
            let node = &g.nodes[i];
            if matches!(node.kind, NodeKind::Input) {
                continue;
            }
            for &e in &node.inputs {
                if matches!(g.nodes[e].kind, NodeKind::Input)
                    && !matches!(&node.kind, NodeKind::Op(NumOp::Core(Op::Field(1))))
                {
                    return None; // reads the pair itself, so it can see the accumulator
                }
                stack.push(e);
            }
        }
        Some(())
    }

    impl MonoidFold {
        /// Recognize the body, or decline.
        pub(super) fn recognize(g: &Graph<NumOp>) -> Option<MonoidFold> {
            let out = &g.nodes[g.output];
            // a `Tuple` output updates each accumulator FIELD; anything else updates the
            // accumulator itself, which is the scalar `fold(xs, 0, acc + x)` a `sum` lowers to.
            let (accum, updates) = match &out.kind {
                NodeKind::Tuple => (Accum::Fields, out.inputs.clone()),
                _ => (Accum::Bare, vec![g.output]),
            };
            let mut fields = Vec::with_capacity(updates.len());
            let mut needed = vec![false; g.nodes.len()];
            for (j, &upd) in updates.iter().enumerate() {
                let node = &g.nodes[upd];
                let NodeKind::Op(op) = &node.kind else { return None };
                let red = monoid_of(op)?;
                let contribution = match op {
                    // the immediate form: `acc.j add_u64 c`, whose operand IS the accumulator field.
                    NumOp::Arith(ArithOp::AddU64(c)) => {
                        if !names_acc(g, node.inputs[0], &accum, j) {
                            return None;
                        }
                        Contribution::Const(*c)
                    }
                    // the pair form: `(acc.j, e) <op>`, where `e` never reads the accumulator.
                    _ => {
                        let pair = &g.nodes[node.inputs[0]];
                        let NodeKind::Tuple = pair.kind else { return None };
                        let [acc, elem] = pair.inputs[..] else { return None };
                        if !names_acc(g, acc, &accum, j) {
                            return None;
                        }
                        match literal_u64(g, elem) {
                            Some(c) => Contribution::Const(c),
                            None => {
                                mark_elem_side(g, elem, &mut needed)?;
                                Contribution::Node(elem)
                            }
                        }
                    }
                };
                fields.push((red, contribution));
            }
            Some(MonoidFold { accum, fields, needed })
        }

        /// `(seed, list) -> seed ⊕ reduce(list)`, per field.
        ///
        /// Returns `None` where the plan does not fit the input: a seed that is not a `U64` leaf
        /// per field (the recognized cells compute at 64 bits, so any other seed would change the
        /// result's shape), or a contribution that is not a `U64` column, or any error on the way.
        /// The caller then runs the lockstep fold, which is the definition and reports the shape
        /// error in its own words, so recognition never decides whether a program types.
        pub(super) fn eval(&self, g: &Graph<NumOp>, input: Value) -> Option<Value> {
            fn is_u64(v: &Value) -> bool {
                matches!(v, Value::Prim(Prim::U64(_)))
            }
            let Value::Prod(pair) = &input else { return None };
            let [seed, Value::List(..)] = &pair[..] else { return None };
            let fits = match (&self.accum, seed) {
                (Accum::Bare, s) => is_u64(s),
                (Accum::Fields, Value::Prod(fs)) => fs.len() == self.fields.len() && fs.iter().all(is_u64),
                _ => false,
            };
            if !fits {
                return None;
            }
            let (seed, list) = input.into_pair("Fold").ok()?;
            let seeds = match self.accum {
                Accum::Bare => vec![seed],
                Accum::Fields => seed.into_prod("Fold seed").ok()?,
            };
            let (bounds, vals) = list.into_list("Fold list").ok()?;
            let total = vals.len();

            // the element side, evaluated ONCE over every element. The accumulator slot is a unit
            // column of the same length: `Field(0)` of the pair is unreachable by construction (see
            // `mark_elem_side`), so nothing can look at it, and it costs two words.
            let mut vals_at: Vec<Option<Value>> = vec![None; g.nodes.len()];
            let arg = Value::Prod(vec![Value::Unit(total), vals]);
            for (i, node) in g.nodes.iter().enumerate() {
                if !self.needed[i] {
                    continue;
                }
                let v = match &node.kind {
                    NodeKind::Input => arg.clone(),
                    NodeKind::Tuple => Value::Prod(
                        node.inputs.iter().map(|&e| vals_at[e].clone().expect("marked in order")).collect(),
                    ),
                    NodeKind::Op(o) => o.eval(vals_at[node.inputs[0]].clone().expect("marked in order")).ok()?,
                };
                vals_at[i] = Some(v);
            }

            let mut out = Vec::with_capacity(seeds.len());
            for (s, (red, c)) in seeds.into_iter().zip(&self.fields) {
                let column = match c {
                    Contribution::Node(n) => vals_at[*n].clone().expect("marked"),
                    // a constant contribution still reduces per row: `count` is `Add` over a
                    // column of ones, i.e. the row length, and `Min`/`Max` of a constant is it.
                    Contribution::Const(k) => Value::u64(vec![*k; total]),
                };
                if !is_u64(&column) {
                    return None;
                }
                let reduced = ArithOp::Reduce(*red).eval(Value::List(bounds.clone(), Box::new(column))).ok()?;
                // seed ⊕ reduction: associativity is what makes the split legal, and every
                // monoid here is commutative, so the order of the two does not matter.
                let combine: NumOp = match red {
                    Red::Add => ArithOp::Bin(BinOp::Add, Kind::U, 64).into(),
                    Red::Mul => ArithOp::Bin(BinOp::Mul, Kind::U, 64).into(),
                    Red::Min => CmpOp::Min.into(),
                    Red::Max => CmpOp::Max.into(),
                    _ => unreachable!("monoid_of yields only Add/Mul/Min/Max"),
                };
                out.push(combine.eval(Value::Prod(vec![s, reduced])).ok()?);
            }
            Some(match self.accum {
                Accum::Bare => out.into_iter().next().expect("one field"),
                Accum::Fields => Value::Prod(out),
            })
        }
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
    fn eval(&self, input: Value) -> Result<Value, String> {
        match self {
            NumOp::Core(c) => {
                // a `Fold` whose body is a product of monoids becomes one reduction per field
                // (see `monoid_fold`), recognized here because the core is numeric-blind. A plan
                // that does not fit the input declines, and the lockstep fold below is the definition.
                if let Op::Fold(body) = c {
                    if let Some(plan) = monoid_fold::MonoidFold::recognize(body) {
                        if let Some(out) = plan.eval(body, input.clone()) {
                            return Ok(out);
                        }
                    }
                }
                c.eval(input)
            }
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
            NumOp::Text(t) => t.eval(input),
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

#[cfg(test)]
mod monoid_fold_tests {
    //! The product-of-monoids fast path must compute EXACTLY what the lockstep fold computes, and
    //! must decline every body it does not understand. Each case runs both paths on the same input
    //! and compares: `NumOp::eval` is the intercepted one, `Op::eval` the core's lockstep. It also
    //! asserts which path the body was expected to take, so a case that silently declines cannot
    //! pass by accident.

    use super::monoid_fold::MonoidFold;
    use super::NumOp;
    use crate::graph::{Graph, NodeKind, OpLike};
    use crate::ops::core::Op;
    use crate::value::{Bounds, Value};

    /// the body of the (single) `fold` in a surface program, so the cases exercise the real
    /// lowering rather than a hand-built graph.
    fn body_of(src: &str) -> Graph<NumOp> {
        let g = crate::parse_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"));
        for n in &g.nodes {
            if let NodeKind::Op(NumOp::Core(Op::Fold(b))) = &n.kind {
                return (**b).clone();
            }
        }
        panic!("no fold in {src}");
    }

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }

    /// run both paths on `(seed, list)` and require them to agree, including on the error.
    fn agree(src: &str, recognized: bool, seed: Value, list: Value) {
        let body = body_of(src);
        assert_eq!(MonoidFold::recognize(&body).is_some(), recognized, "{src}: recognition");
        let input = Value::Prod(vec![seed, list]);
        let fast = NumOp::Core(Op::Fold(Box::new(body.clone()))).eval(input.clone());
        let slow = Op::<NumOp>::Fold(Box::new(body)).eval(input);
        assert_eq!(fast, slow, "{src}");
    }

    /// the inputs each case is checked over: one long row, several rows, an empty row, an empty
    /// list, a single element, a strided partition, and a non-zero seed.
    fn lists() -> Vec<(Value, Value)> {
        let seed2 = |n: usize| Value::Prod(vec![u(&vec![0; n]), u(&vec![0; n])]);
        vec![
            (seed2(1), Value::List(vec![6].into(), Box::new(u(&[3, 1, 4, 1, 5, 9])))),
            (seed2(3), Value::List(vec![2, 2, 5].into(), Box::new(u(&[7, 2, 8, 1, 6])))),
            (seed2(3), Value::List(vec![0, 3, 3].into(), Box::new(u(&[4, 5, 6])))),
            (seed2(1), Value::List(vec![0].into(), Box::new(u(&[])))),
            (seed2(1), Value::List(vec![1].into(), Box::new(u(&[42])))),
            (seed2(3), Value::List(Bounds::Stride(2, 3), Box::new(u(&[9, 1, 8, 2, 7, 3])))),
            (
                Value::Prod(vec![u(&[100]), u(&[7])]),
                Value::List(vec![4].into(), Box::new(u(&[1, 2, 3, 4]))),
            ),
        ]
    }

    #[test]
    fn recognized_bodies_agree_with_the_lockstep_fold() {
        let cases = [
            // C5: sum and count. The count's contribution is a CONSTANT, not the element.
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) add, acc.1 add_u64 1))",
            // min and max, from the `cmp` bucket rather than the arithmetic grid
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) min, (acc.1, x) max))",
            // a contribution that is an EXPRESSION of the element, not the element
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x mul 2 add_u64 1) add, (acc.1, x) max))",
            // product and sum together; the element expression is shared between the two fields
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x add_u64 1) mul, (acc.1, x add_u64 1) add))",
            // both fields constant: neither contribution depends on the element at all. `mul 2`
            // is the lit-pair spelling, and the literal is anchored to the accumulator field.
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> (acc.0 add_u64 3, acc.1 mul 2))",
        ];
        for src in cases {
            for (seed, list) in lists() {
                agree(src, true, seed, list);
            }
        }
    }

    /// The SCALAR accumulator, `fold(xs, 0, acc + x)`: the body's output is the update itself
    /// rather than a `Tuple` of them.
    #[test]
    fn a_scalar_accumulator_agrees_with_the_lockstep_fold() {
        let cases = [
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, x) add)", true),
            ("let s = input lit 1 in (s, input) fold ((acc, x) -> (acc, x) mul)", true),
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, x) max)", true),
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, x mul 3 add_u64 1) add)", true),
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> acc add_u64 1)", true),
            // not associative, and the element side reads the accumulator: both must decline.
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, x) sub)", false),
            ("let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, acc) add)", false),
        ];
        let lists = [
            Value::List(vec![6].into(), Box::new(u(&[3, 1, 4, 1, 5, 9]))),
            Value::List(vec![2, 2, 5].into(), Box::new(u(&[7, 2, 8, 1, 6]))),
            Value::List(vec![0, 3, 3].into(), Box::new(u(&[4, 5, 6]))),
            Value::List(vec![0].into(), Box::new(u(&[]))),
            Value::List(Bounds::Stride(2, 3), Box::new(u(&[9, 1, 8, 2, 7, 3]))),
        ];
        for (src, recognized) in cases {
            for list in &lists {
                let rows = list.len();
                let seed = u(&vec![if src.contains("lit 1") { 1 } else { 0 }; rows]);
                agree(src, recognized, seed, list.clone());
            }
        }
    }

    #[test]
    fn unrecognized_bodies_fall_through_unchanged() {
        let cases = [
            // `sub` is not associative, so the split would be wrong
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) sub, (acc.1, x) add))",
            // the element side READS the accumulator: `acc.1` is not a function of x
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, acc.1) add, (acc.1, x) add))",
            // field 0 is updated from acc.1: the wrong field, so the updates are not independent
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.1, x) add, (acc.1, x) add))",
            // one field is a monoid and the other is not: the whole body declines
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) mul, (acc.1, x) sub))",
            // the accumulator is threaded whole rather than field by field
            "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) add, (acc.0, x) add))",
        ];
        for src in cases {
            for (seed, list) in lists() {
                agree(src, false, seed, list);
            }
        }
    }

    /// The plan inspects the BODY; the seed's shape is a separate question, and a seed the
    /// recognized cells cannot compute at must fall back rather than change the answer. The same
    /// holds for an element column of the wrong width: both paths report the lockstep path's error.
    #[test]
    fn a_seed_or_element_the_cells_cannot_hold_falls_back() {
        let src = "let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) add, acc.1 add_u64 1))";
        let list = Value::List(vec![3].into(), Box::new(u(&[1, 2, 3])));
        // a u32 field: the cells compute at 64, so the result would not be the seed's shape.
        agree(src, true, Value::Prod(vec![Value::u32(vec![0]), u(&[0])]), list.clone());
        // the wrong number of fields.
        agree(src, true, Value::Prod(vec![u(&[0])]), list.clone());
        // narrow elements under a u64 accumulator: a width error on both paths, in the same words.
        let narrow = Value::List(vec![3].into(), Box::new(Value::u8(vec![1, 2, 3])));
        agree(src, true, Value::Prod(vec![u(&[0]), u(&[0])]), narrow);
        // ...and the same for a SCALAR accumulator, whose seed is the leaf itself.
        let bare = "let s = input lit 0 in (s, input) fold ((acc, x) -> (acc, x) add)";
        agree(bare, true, Value::u32(vec![0]), list.clone());
        agree(bare, true, Value::Prod(vec![u(&[0])]), list);
    }
}
