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

pub mod bytes;
pub(crate) mod effect;
pub(crate) mod engine;
pub(crate) mod frontend;
pub(crate) mod graph;
pub(crate) mod hash;
pub(crate) mod ops;
pub(crate) mod optimize;
pub(crate) mod shape;
pub(crate) mod value;

pub use effect::{is_total, lower_effects};
pub use frontend::{parse_ml, Program};
pub use graph::{eval_graph, shape_of, Builder, Graph, OpLike};
pub use hash::hash;
pub use ops::host::{HostKernel, HostOp};
pub use ops::{dec_i64, enc_i64, ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred, Red, TextOp};
pub use optimize::{cancel_isos, cse, dce, fuse_maps, immediates, optimize, peephole};
pub use shape::{shape_of_value, Shape};
pub use value::{show, Bounds, Tags, Value};

/// Arrangement-substrate support: row-level primitives for using corgi columns directly as a
/// differential-dataflow batch (merge/sort/gather/compare over flat columns), without decoding
/// to rows. Thin public wrappers over the internal `engine`/`ops::cmp::order` machinery. Added
/// for the dd-corgi backend spike (Route B: cursor-less corgi arrangement).
pub mod arrange {
    use crate::value::{Bounds, Value};
    use std::cmp::Ordering;

    /// Borrow a column's `u64` leaf, if it is one — peeling single-field products, which
    /// order identically to the field they wrap.
    ///
    /// The zero-copy read. Without it every leaf inspection from outside corgi has to
    /// `gather(..).into_u64(..)` or clone, because a shared column's `Arc` cannot be
    /// unwrapped: callers pay a full column copy to look at values they only read.
    pub fn leaf_slice(v: &Value) -> Option<&[u64]> {
        match v {
            Value::Prim(crate::value::Prim::U64(xs)) => Some(&xs[..]),
            Value::Prod(fs) if fs.len() == 1 => leaf_slice(&fs[0]),
            _ => None,
        }
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
    mod hash_tests {
        use crate::value::{Bounds, Value};

        /// ONE id function: the `hash` op's column is the `hash` function's ids, wrapped. There
        /// were once two folds with different constants that disagreed about whether a `u8 5` and
        /// a `u64 5` are the same value — exactly the question the boundary id exists to answer —
        /// so this is pinned to keep a second one from quietly reappearing.
        #[test]
        fn the_hash_op_is_the_hash_function() {
            let shapes = [
                Value::u64(vec![5, 7, 5]),
                Value::Prod(vec![Value::u8(vec![1, 2, 3]), Value::u32(vec![9, 9, 8])]),
                Value::sum(vec![0, 1, 0], vec![Value::u16(vec![4, 6]), Value::u64(vec![7])]),
                Value::List(Bounds::offsets(vec![1, 1, 4]), Box::new(Value::u8(vec![3, 4, 5, 6]))),
                Value::Unit(3),
            ];
            for v in shapes {
                let op = crate::ops::Op::<crate::NumOp>::Hash.eval(v.clone()).unwrap();
                assert_eq!(crate::hash::hash(&v), op.into_u64("hash op").unwrap());
            }
        }
    }

    #[cfg(test)]
    mod order_tests {
        use super::{gather, group_bounds, segment_labels, sort_blocks};
        use crate::value::{Bounds, Value};

        #[test]
        fn sort_blocks_segmented_stable_argmin() {
            // two segments (labels [0,0,0, 1,1,1]) over rows; segment 0 has duplicate 3s (stability),
            // segment 1 has duplicate 5s. Segments must stay contiguous, sort within, argmin at start.
            let labels = vec![0u64, 0, 0, 1, 1, 1];
            let v = Value::u64(vec![3, 1, 3, 5, 2, 5]);
            let (perm, _refined) = sort_blocks(&labels, &v);

            // segment 0 occupies output [0,3), segment 1 [3,6); each perm entry stays in its segment's
            // index range (rows contiguous, in segment order).
            for &p in &perm[0..3] {
                assert!(p < 3, "segment 0 pulled a row from segment 1");
            }
            for &p in &perm[3..6] {
                assert!((3..6).contains(&p), "segment 1 pulled a row from segment 0");
            }

            // sorted WITHIN each segment.
            let sorted = gather(&v, &perm).into_u64("sorted").unwrap();
            assert_eq!(&sorted[0..3], &[1, 3, 3]);
            assert_eq!(&sorted[3..6], &[2, 5, 5]);

            // perm[segment_start] is the segment's argmin (original position of its minimum).
            assert_eq!(perm[0], 1); // min of [3,1,3] is at index 1
            assert_eq!(perm[3], 4); // min of [5,2,5] is at index 4

            // stability: the two equal 3s (indices 0 and 2) keep input order.
            assert_eq!(&perm[1..3], &[0, 2]);
        }

        #[test]
        fn segment_labels_offsets_and_stride_agree() {
            // Offsets([2,4,6]) and the equivalent Stride(2,3) describe the same 3-row partition
            // (rows of width 2), so per-element segment labels are identical.
            let off = Bounds::offsets(vec![2, 4, 6]);
            let stride = Bounds::Stride(2, 3);
            assert_eq!(segment_labels(&off), vec![0, 0, 1, 1, 2, 2]);
            assert_eq!(segment_labels(&off), segment_labels(&stride));
        }

        #[test]
        fn roundtrip_matches_sortlist_op() {
            // build List<u64> with ragged rows, segmented-sort it via the arrange surface, and check
            // it reproduces exactly what the ML `sort` op (CmpOp::SortList) produces on the same list.
            let bounds = Bounds::offsets(vec![3, 3, 6]); // rows [3,1,2], [], [5,0,4]
            let vals = Value::u64(vec![3, 1, 2, 5, 0, 4]);
            let list = Value::List(bounds.clone(), Box::new(vals.clone()));

            // arrange surface: seed segment labels from bounds, segmented-sort, gather by perm.
            let labels = segment_labels(&bounds);
            let (perm, _refined) = sort_blocks(&labels, &vals);
            let ours = Value::List(bounds.clone(), Box::new(gather(&vals, &perm)));

            // the ML op.
            let theirs = crate::ops::cmp::CmpOp::SortList.eval(list).unwrap();
            assert_eq!(ours, theirs);
        }

        #[test]
        fn group_bounds_runs() {
            // exclusive ends of equal-value runs: [1,1,2,3,3,3] → groups [0,2),[2,3),[3,6).
            assert_eq!(group_bounds(&Value::u64(vec![1, 1, 2, 3, 3, 3])), vec![2, 3, 6]);
            // all distinct → one end per row; all equal → a single group; empty → no ends.
            assert_eq!(group_bounds(&Value::u64(vec![1, 2, 3])), vec![1, 2, 3]);
            assert_eq!(group_bounds(&Value::u64(vec![4, 4, 4])), vec![3]);
            assert!(group_bounds(&Value::u64(vec![])).is_empty());
            // agrees with run_layout's ends over the same value column read as its own labels.
            let keys = Value::u64(vec![10, 10, 20, 20, 20, 30]);
            let labels = vec![0u64, 0, 1, 1, 1, 2];
            assert_eq!(group_bounds(&keys), crate::ops::cmp::order::run_layout(&labels).0);
        }
    }
}
