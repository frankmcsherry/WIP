//! corgi — a minimal columnar, **single-input** term-graph IR.
//!
//! Every semantic op is a unary `T0 -> T1`, evaluated as `eval(Value) -> Value` —
//! the "1:1 map" taken literally. There is no `arity()`: a node's shape requirement
//! lives in its input's type, which the typer needs anyway.
//!
//! The leaf is a width-tagged `Prim` column (`u8`/`u16`/`u32`/`u64`); signed and float are
//! KINDS the numeric layer encodes onto it, so the core stays kind-blind. Booleans use the
//! idiom `0 = false, nonzero = true` (a mask is just a leaf) — no `Bool` leaf. `Unit` is
//! deferred until JSON `null` needs it.
//!
//! Structural nodes (the only arity != 1 nodes):
//!   * `Input` — arity 0, the stratum root (reads the parameter)
//!   * `Tuple` — arity N, the sole fan-in (collect edges into a product)
//!
//! Everything else is a unary op via [`op::Op::eval`], including `Lit` (a constant
//! element filled to its input's length — anchored to a stratum) and the two
//! closed-body ops `MapList` / `MapSum` (they recurse into [`graph::eval_graph`]).
//!
//! Layers: [`value`] (the data) → [`engine`] (`gather`/`concat` + index gen) →
//! [`ops`] (the vocabulary; `ops::cmp` carries its own `compare_idx`/structural-order
//! and discrimination-sort engine) → [`graph`] (the IR + evaluator).

pub(crate) mod effect;
pub(crate) mod engine;
pub(crate) mod frontend;
pub(crate) mod graph;
pub(crate) mod hash;
pub(crate) mod ops;
pub(crate) mod optimize;
pub(crate) mod shape;
pub(crate) mod value;

pub use effect::{effect_eval_graph, eval_try, is_total, EffectValues, FailValues};
pub use frontend::{parse_ml, Program};
pub use graph::{eval_graph, shape_of, Builder, Graph, OpLike};
pub use hash::hash;
pub use ops::{dec_i64, enc_i64, ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred, Red, TextOp};
pub use optimize::{cancel_isos, cse, dce, fuse_maps, optimize, peephole};
pub use shape::{shape_of_value, Shape};
pub use value::{show, Bounds, Value};

/// Arrangement-substrate support: row-level primitives for using corgi columns directly as a
/// differential-dataflow batch (merge/sort/gather/compare over flat columns), without decoding
/// to rows. Thin public wrappers over the internal `engine`/`ops::cmp::order` machinery. Added
/// for the dd-corgi backend spike (Route B: cursor-less corgi arrangement).
pub mod arrange {
    use crate::value::{Bounds, Value};
    use std::cmp::Ordering;

    /// Batched equal-range probe (`find` as a single-row batch): for each row of `needles` (any
    /// order), the `[lo, hi)` range of that key in the SORTED `haystack` column. Returns `(lo, hi)`
    /// Vecs aligned with `needles`; `hi > lo` is the membership test, `lo..hi` the matching positions.
    /// O(|needles| · log|haystack|) — the multi-record replacement for a per-pair merge-join /
    /// semijoin scan.
    pub fn find_ranges(needles: &Value, haystack: &Value) -> (Vec<usize>, Vec<usize>) {
        let (n, h) = (needles.len(), haystack.len());
        let needle_list = Value::List(Bounds::Offsets(vec![n]), Box::new(needles.clone()));
        let hay_list = Value::List(Bounds::Offsets(vec![h]), Box::new(haystack.clone()));
        let out = crate::ops::cmp::CmpOp::Find.eval(Value::Prod(vec![needle_list, hay_list]));
        let (_b, vals) = out.into_list("find_ranges");
        let (lo, hi) = vals.into_pair("find_ranges lo/hi");
        let lo = lo.into_u64("find_ranges lo").into_iter().map(|x| x as usize).collect();
        let hi = hi.into_u64("find_ranges hi").into_iter().map(|x| x as usize).collect();
        (lo, hi)
    }

    /// Select/reorder rows of a single columnar `Value` by index.
    pub fn gather(v: &Value, idx: &[usize]) -> Value { crate::engine::gather(v, idx) }
    /// Multi-source gather: output row `i` = row `off[i]` of source `srcs[tags[i]]` (all same shape).
    /// Builds a merged batch's columns by interleaving two sorted inputs without a concat.
    pub fn gather_lanes(srcs: &[Option<&Value>], tags: &[usize], off: &[usize]) -> Value {
        crate::engine::gather_lanes(srcs, tags, off)
    }
    /// Structural compare of row `i` of `a` vs row `j` of `b` (same shape). Build a sort by
    /// `indices.sort_by(|&i,&j| compare_at(kv, i, kv, j))`; merge two sorted batches with it.
    pub fn compare_at(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
        crate::ops::cmp::order::compare_at(a, i, b, j)
    }

    /// Multi-record argsort: the permutation that sorts *all* of `v`'s rows by structural order, in
    /// one columnar discrimination pass — the batched replacement for driving `sort_by(compare_at)`
    /// per pair.
    pub fn sort_perm(v: &Value) -> Vec<usize> {
        crate::ops::cmp::order::sort_blocks(&vec![0u64; v.len()], v).0
    }

    /// Batched structural compare: `out[k]` = sign of row `ia[k]` of `a` vs row `ib[k]` of `b` (all
    /// pairs in one pass). Use `ia=0..n-1`, `ib=1..n` to flag adjacent-equal runs after a sort.
    pub fn compare_idx(a: &Value, b: &Value, ia: &[usize], ib: &[usize]) -> Vec<i8> {
        crate::ops::cmp::order::compare_idx(a, b, ia, ib)
    }
}
