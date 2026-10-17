//! corgi — a minimal columnar, **single-input** term-graph IR.
//!
//! Every semantic op is a unary `T0 -> T1`, evaluated as `eval(Value) -> Value` —
//! the "1:1 map" taken literally. There is no `arity()`: a node's shape requirement
//! lives in its input's type, which the typer needs anyway.
//!
//! A leaf holds integers (`Int`) or floats (`Float`, an `f64`). An integer's storage is the
//! engine's choice — bytes for text, masks and tags, `i64` otherwise — and never its value.
//! Booleans use the idiom `0 = false, nonzero = true` (a mask is an integer leaf, stored as
//! bytes) — no `Bool` leaf.
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

pub mod bytes;
pub(crate) mod engine;
pub mod explain;
pub(crate) mod frontend;
pub(crate) mod graph;
pub(crate) mod hash;
pub(crate) mod ops;
pub(crate) mod optimize;
pub(crate) mod shape;
pub(crate) mod value;

pub use frontend::{parse_ml, parse_ml_in, Program, Scope};
pub use graph::{eval_graph, shape_of, Builder, Graph, OpLike};
pub use hash::hash;
pub use ops::host::{HostKernel, HostOp};
pub use ops::{ArithOp, BinOp, BitOp, CmpOp, NumOp, Op, Pattern, Pred, Red, ShiftOp, TextOp};
pub use optimize::{cancel_isos, cse, dce, fuse_maps, immediates, optimize, peephole};
pub use shape::{shape_of_value, Shape};
pub use value::{show, Bounds, Scalar, Tags, Value};

/// Arrangement-substrate support: row-level primitives for using corgi columns directly as a
/// differential-dataflow batch (merge/sort/gather/compare over flat columns), without decoding
/// to rows. Thin public wrappers over the internal `engine`/`ops::cmp::order` machinery. Added
/// for the dd-corgi backend spike (Route B: cursor-less corgi arrangement).
pub mod arrange {
    use crate::value::{Bounds, Prim, Value};
    use std::cmp::Ordering;

    /// A row borrowed from a pool of columns, as `(column, row)`.
    pub type RowRef = (usize, usize);

    /// A column's integers as `i64`s, if it is an integer leaf — peeling single-field products,
    /// which order identically to the field they wrap. `None` for any other shape: whether a
    /// column is a bare integer is a question about its shape, never its storage.
    ///
    /// Borrowed where the integers are held as `i64`s, widened where they are held narrower.
    /// Without it every leaf inspection from outside corgi has to `gather(..).into_i64(..)` or
    /// clone, because a shared column's `Arc` cannot be unwrapped: callers pay a full column copy
    /// to look at values they only read.
    pub fn leaf_slice(v: &Value) -> Option<std::borrow::Cow<'_, [i64]>> {
        match v {
            Value::Prim(_) => v.as_i64("leaf_slice").ok(),
            Value::Prod(fs) if fs.len() == 1 => leaf_slice(&fs[0]),
            _ => None,
        }
    }

    /// Prepare borrowed comparison lanes for a column made of native `i64` leaves, nested
    /// products and units. Lanes follow structural field order; units contribute no lanes.
    ///
    /// Returns `None` for any other storage or shape, including integers held as bytes. Unlike
    /// [`leaf_slice`], this never widens or materializes payload to test whether it can be borrowed.
    /// Only the small vector of lane descriptors is allocated, once for the whole column.
    pub fn borrowed_int_lanes(v: &Value) -> Option<Vec<&[i64]>> {
        fn walk<'a>(v: &'a Value, lanes: &mut Vec<&'a [i64]>) -> bool {
            match v {
                Value::Prim(Prim::I64(xs)) => { lanes.push(xs); true }
                Value::Prod(fields) => fields.iter().all(|field| walk(field, lanes)),
                Value::Unit(_) => true,
                _ => false,
            }
        }
        let mut lanes = Vec::new();
        walk(v, &mut lanes).then_some(lanes)
    }

    /// Select/reorder rows of a single columnar `Value` by index.
    pub fn gather(v: &Value, idx: &[usize]) -> Value { crate::engine::gather(v, idx) }
    /// Multi-source gather: output row `i` = row `off[i]` of source `srcs[tags[i]]` (all same shape).
    /// Builds a merged batch's columns by interleaving two sorted inputs without a concat.
    pub fn gather_lanes(srcs: &[Option<&Value>], tags: &[usize], off: &[usize]) -> Value {
        crate::engine::gather_lanes(srcs, tags, off)
    }
    /// Structural comparison of one isolated row pair (same shape). For batches, prefer
    /// [`equal_pairs`], [`sort_blocks`] or [`survey_groups`] to calls per row.
    pub fn compare_at(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
        crate::ops::cmp::order::compare_at(a, i, b, j)
    }

    /// Equality of row pairs borrowed from several columns. Each coordinate is `(column, row)`;
    /// each pair must have matching logical shapes. Results follow the order of `pairs`.
    ///
    /// Only coordinate indices are regrouped, by source-column pair. Each group uses one bulk
    /// structural descent; payload columns are neither gathered nor converted to rows. This lets
    /// a host compare a whole window of keys or output candidates without scalar calls per key.
    pub fn equal_pairs(columns: &[&Value], pairs: &[(RowRef, RowRef)]) -> Vec<bool> {
        let order = pair_order(pairs);
        let mut result = vec![false; pairs.len()];
        let (mut left, mut right) = (Vec::new(), Vec::new());
        for group in order.chunk_by(|&a, &b| (pairs[a].0.0, pairs[a].1.0) == (pairs[b].0.0, pairs[b].1.0)) {
            let (a, b) = (pairs[group[0]].0.0, pairs[group[0]].1.0);
            left.clear();
            right.clear();
            let (lo, ro) = (pairs[group[0]].0.1, pairs[group[0]].1.1);
            let dense = group.iter().enumerate().all(|(offset, &i)| pairs[i].0.1 == lo + offset && pairs[i].1.1 == ro + offset);
            let equal = if dense {
                crate::ops::cmp::order::equal_ranges(columns[a], columns[b], lo, ro, group.len())
            } else {
                left.extend(group.iter().map(|&i| pairs[i].0.1));
                right.extend(group.iter().map(|&i| pairs[i].1.1));
                crate::ops::cmp::order::equal_idx(columns[a], columns[b], &left, &right)
            };
            for (&i, sign) in group.iter().zip(equal) { result[i] = sign == 0; }
        }
        result
    }

    /// Group requests by source columns, retaining request order within each group. Already
    /// grouped requests, including the single-column-pair case, need no sort.
    fn pair_order(pairs: &[(RowRef, RowRef)]) -> Vec<usize> {
        let mut order: Vec<usize> = (0..pairs.len()).collect();
        let columns = |i: usize| (pairs[i].0.0, pairs[i].1.0);
        if order.windows(2).any(|w| columns(w[0]) > columns(w[1])) {
            order.sort_unstable_by_key(|&i| (pairs[i].0.0, pairs[i].1.0, i));
        }
        order
    }

    /// Multi-record argsort: the permutation that sorts *all* of `v`'s rows by structural order, in
    /// one columnar discrimination pass — the batched replacement for driving `sort_by(compare_at)`
    /// per pair.
    pub fn sort_perm(v: &Value) -> Vec<usize> {
        crate::ops::cmp::sort::sort_blocks(&[], v).0
    }

    /// Segmented (discrimination) argsort: the multi-block generalization of [`sort_perm`]. Given
    /// per-row `labels` marking segments (non-decreasing — segment `s` is the maximal run of rows
    /// sharing a label; empty for one segment), return `(perm, refined_labels)` where `perm` sorts `v`'s rows WITHIN each
    /// label block by corgi structural order (stable, so ties keep input order), and `refined_labels`
    /// further splits each block by equal value (two rows share a refined label iff they shared a
    /// `labels` value AND are structurally equal). `sort_perm(v)` is exactly the single-block case
    /// `sort_blocks(&[0; n], v).0`.
    ///
    /// This is the load-bearing segmented primitive for the dd backend: within a segment `[lo, hi)`
    /// (a run of one input label), `perm[lo]` is the segment's structural
    /// ARGMIN (its minimum row's original position) and `perm[lo..hi]` is the segment's sorted order —
    /// so argmin/argmax/first-per-segment and per-segment sorted order fall out while keeping the row
    /// positions the caller indexed by.
    pub fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
        crate::ops::cmp::sort::sort_blocks(labels, v)
    }

    /// Per-element segment labels from a `List`'s row `Bounds`: element of row `r` gets label `r`.
    /// This is the seed for a segmented [`sort_blocks`] that sorts within each list row while keeping
    /// the rows contiguous and in outer-row order. `Offsets` and the equivalent `Stride` produce the
    /// same labels (labels depend only on the partition, not its encoding).
    pub fn segment_labels(bounds: &Bounds) -> Vec<u64> {
        crate::ops::cmp::order::segment_labels(bounds)
    }

    pub use crate::ops::cmp::survey::GroupRun;

    /// Survey the mutual interleaving of two structurally-sorted columns `a` and `b`, a class at a
    /// time: a match is the maximal equal class on
    /// BOTH sides ([`GroupRun::Both`]), so a caller carrying per-row payloads (times, diffs)
    /// consolidates the whole class in one place; the leading leaf decides the interleaving with
    /// one gallop and every level below refines all the classes it left equal at once.
    pub fn survey_groups(a: &Value, b: &Value) -> Vec<GroupRun> {
        crate::ops::cmp::survey::survey_groups(a, b)
    }

    /// Segment ends of the maximal equal-value runs in a structurally-sorted column `keys`:
    /// `out[g]` is the exclusive end of group `g` (group `g` is `out[g-1]..out[g]`, implicit
    /// `out[-1] = 0`), and `out.last() == keys.len()`. The single-column equal-key boundaries that
    /// complement [`survey_groups`].
    pub fn group_bounds(keys: &Value) -> Vec<usize> {
        crate::ops::cmp::order::group_bounds(keys)
    }

    #[cfg(test)]
    mod hash_tests;

    #[cfg(test)]
    mod order_tests;
}
