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
pub use ops::{dec_i64, enc_i64, ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred, Red, TextOp};
pub use optimize::{cancel_isos, cse, dce, fuse_maps, optimize, peephole};
pub use shape::{shape_of_value, Shape};
pub use value::{show, Bounds, Value};

/// Arrangement-substrate support: row-level primitives for using corgi columns directly as a
/// differential-dataflow batch (merge/sort/gather/compare over flat columns), without decoding
/// to rows. Thin public wrappers over the internal `engine`/`ops::cmp::order` machinery. Added
/// for the dd-corgi backend spike (Route B: cursor-less corgi arrangement).
pub mod arrange {
    use crate::value::{Bounds, Prim, Value};
    use std::cmp::Ordering;

    /// Batched equal-range probe (`find` as a single-row batch): for each row of `needles` (any
    /// order), the `[lo, hi)` range of that key in the SORTED `haystack` column. Returns `(lo, hi)`
    /// Vecs aligned with `needles`; `hi > lo` is the membership test, `lo..hi` the matching positions.
    /// O(|needles| · log|haystack|) — the multi-record replacement for a per-pair merge-join /
    /// semijoin scan.
    pub fn find_ranges(needles: &Value, haystack: &Value) -> (Vec<usize>, Vec<usize>) {
        // Fast path: both sides one `U64` leaf — integer keys and hashes, i.e. the
        // overwhelmingly common probe. A native binary search per needle, with no
        // structural dispatch: the generic path below re-inspects the column's shape at
        // every comparison step of every probe, which profiling of an incremental
        // differential round attributed 21% of its time to.
        // NB single-field products peel: `Prod([X])` orders exactly as `X`, and callers
        // routinely wrap a scalar key in a 1-tuple, so matching only a bare leaf would
        // silently miss the common case.
        fn as_u64_leaf(v: &Value) -> Option<&[u64]> {
            match v {
                Value::Prim(crate::value::Prim::U64(xs)) => Some(&xs[..]),
                Value::Prod(fs) if fs.len() == 1 => as_u64_leaf(&fs[0]),
                _ => None,
            }
        }
        if let (Some(ns), Some(hs)) = (as_u64_leaf(needles), as_u64_leaf(haystack)) {
            let (mut lo, mut hi) = (Vec::with_capacity(ns.len()), Vec::with_capacity(ns.len()));
            for n in ns.iter() {
                let l = hs.partition_point(|x| x < n);
                let h = l + hs[l..].partition_point(|x| x == n);
                lo.push(l);
                hi.push(h);
            }
            return (lo, hi);
        }
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
        crate::ops::cmp::order::sort_blocks(&vec![0u64; v.len()], v).0
    }

    /// Batched structural compare: `out[k]` = sign of row `ia[k]` of `a` vs row `ib[k]` of `b` (all
    /// pairs in one pass). Use `ia=0..n-1`, `ib=1..n` to flag adjacent-equal runs after a sort.
    pub fn compare_idx(a: &Value, b: &Value, ia: &[usize], ib: &[usize]) -> Vec<i8> {
        crate::ops::cmp::order::compare_idx(a, b, ia, ib)
    }

    /// Segmented (discrimination) argsort: the multi-block generalization of [`sort_perm`]. Given
    /// per-row `labels` marking segments (non-decreasing — segment `s` is the maximal run of rows
    /// sharing a label), return `(perm, refined_labels)` where `perm` sorts `v`'s rows WITHIN each
    /// label block by corgi structural order (stable, so ties keep input order), and `refined_labels`
    /// further splits each block by equal value (two rows share a refined label iff they shared a
    /// `labels` value AND are structurally equal). `sort_perm(v)` is exactly the single-block case
    /// `sort_blocks(&[0; n], v).0`.
    ///
    /// This is the load-bearing segmented primitive for the dd backend: within a segment `[lo, hi)`
    /// (a run of one input label, read via [`run_layout`]), `perm[lo]` is the segment's structural
    /// ARGMIN (its minimum row's original position) and `perm[lo..hi]` is the segment's sorted order —
    /// so argmin/argmax/first-per-segment and per-segment sorted order fall out while keeping the row
    /// positions the caller indexed by.
    pub fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
        crate::ops::cmp::order::sort_blocks(labels, v)
    }

    /// Per-element segment labels from a `List`'s row `Bounds`: element of row `r` gets label `r`.
    /// This is the seed for a segmented [`sort_blocks`] that sorts within each list row while keeping
    /// the rows contiguous and in outer-row order. `Offsets` and the equivalent `Stride` produce the
    /// same labels (labels depend only on the partition, not its encoding).
    pub fn segment_labels(bounds: &Bounds) -> Vec<u64> {
        crate::ops::cmp::order::segment_labels(bounds)
    }

    /// The run structure of a non-decreasing `labels` vector (e.g. either side of [`sort_blocks`]):
    /// `(ends, firsts)`, where `ends[i]` is the exclusive end and `firsts[i]` the first index of the
    /// `i`th maximal equal-label run. Reads segment/group boundaries back out — segment `i` occupies
    /// `firsts[i]..ends[i]`, and `firsts` are the per-segment representative positions.
    pub fn run_layout(labels: &[u64]) -> (Vec<usize>, Vec<usize>) {
        crate::ops::cmp::order::run_layout(labels)
    }

    pub use crate::ops::cmp::order::Run;

    /// Survey the mutual interleaving of two structurally-sorted columns `a` and `b` as a sequence
    /// of [`Run`]s — maximal ranges exclusive to one side, single matched pairs common to both — in
    /// one bidirectional zig-zag gallop rather than a per-pair two-pointer. The bidirectional
    /// generalization of [`find_ranges`] (the one-directional needle-into-haystack gallop), and the
    /// merge kernel a `CorgiChunk` bulk-`gather`s its ranges from: the corgi/Rust boundary is crossed
    /// once per *range*, not once per *row*, and corgi owns no times — the caller drives the lattice
    /// consolidation off the returned runs. See [`Run`] for the coverage/sortedness guarantees.
    pub fn survey(a: &Value, b: &Value) -> Vec<Run> {
        crate::ops::cmp::order::survey(a, b)
    }

    /// Segment ends of the maximal equal-value runs in a structurally-sorted column `keys`:
    /// `out[g]` is the exclusive end of group `g` (group `g` is `out[g-1]..out[g]`, implicit
    /// `out[-1] = 0`), and `out.last() == keys.len()`. The single-column equal-key boundaries that
    /// complement a [`survey`]; the `Value`-column analogue of [`run_layout`]'s `ends`.
    pub fn group_bounds(keys: &Value) -> Vec<usize> {
        crate::ops::cmp::order::group_bounds(keys)
    }

    // --- structural per-row content hash ---------------------------------------------------------
    //
    // A deterministic, position-independent u64 per row. The finalizer is splitmix64 and the
    // fold `mix` is order-sensitive, so `Prod`/`List` element order matters while row POSITION,
    // column capacity, and Arc identity do not. Distinct per-constructor seeds keep e.g. a `Unit`
    // from colliding with a `Prim` that happens to carry the same bytes.

    /// splitmix64 finalizer — a fast, seed-free avalanche used to mix leaf bytes and combine
    /// child hashes reproducibly across processes (no `RandomState`).
    #[inline]
    fn splitmix64(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// order-sensitive combine of an accumulator with the next child/leaf hash.
    #[inline]
    fn mix(h: u64, x: u64) -> u64 {
        splitmix64(h ^ splitmix64(x))
    }

    // per-constructor seeds (arbitrary distinct constants; only their distinctness matters).
    const SEED_PRIM: u64 = 0x0000_0000_5052_494D; // "PRIM"
    const SEED_PROD: u64 = 0x0000_0000_5052_4F44; // "PROD"
    const SEED_SUM: u64 = 0x0000_0000_0053_554D; //  "SUM"
    const SEED_LIST: u64 = 0x0000_0000_4C49_5354; // "LIST"
    const SEED_UNIT: u64 = 0x0000_0000_554E_4954; // "UNIT"

    /// Structural content hash: `out[r]` is a stable u64 hash of row `r` of `v`, computed
    /// columnar over the whole column in one pass. Kind-blind/structural: equal-by-`Value`-
    /// equality rows hash equal, and the hash is a deterministic function of row CONTENT
    /// (not of position or batch), so ids computed here coincide across the output->input
    /// boundary of a dataflow operator. Returns a Vec<u64> aligned with `v`'s rows.
    pub fn hash_rows(v: &Value) -> Vec<u64> {
        match v {
            // Leaf: mix the width tag into the seed (so different widths of the same numeric
            // value need not collide), then avalanche each row's bytes read straight from the Vec.
            Value::Prim(p) => {
                let seed = SEED_PRIM.wrapping_add(p.bits() as u64);
                match p {
                    Prim::U8(xs) => xs.iter().map(|&x| mix(seed, x as u64)).collect(),
                    Prim::U16(xs) => xs.iter().map(|&x| mix(seed, x as u64)).collect(),
                    Prim::U32(xs) => xs.iter().map(|&x| mix(seed, x as u64)).collect(),
                    Prim::U64(xs) => xs.iter().map(|&x| mix(seed, x)).collect(),
                }
            }
            // Product: combine the child hashes of the row across all columns, in column order.
            Value::Prod(cols) => {
                let n = v.len();
                let child: Vec<Vec<u64>> = cols.iter().map(hash_rows).collect();
                (0..n)
                    .map(|r| child.iter().fold(SEED_PROD, |h, c| mix(h, c[r])))
                    .collect()
            }
            // Sum: mix the row's tag, then the committed lane's payload hash at the within-variant
            // offset. A `None` lane holds no rows, so it is never indexed.
            Value::Sum(tags, offset, variants) => {
                let tags = tags.usize_vec();
                let lanes: Vec<Option<Vec<u64>>> =
                    variants.iter().map(|o| o.as_ref().map(hash_rows)).collect();
                tags.iter()
                    .zip(offset.iter())
                    .map(|(&t, &o)| {
                        let payload = lanes[t]
                            .as_ref()
                            .expect("hash_rows: row committed to a ⊥ lane")[o];
                        mix(mix(SEED_SUM, t as u64), payload)
                    })
                    .collect()
            }
            // List: mix the row's element count, then the row's flattened element hashes in order.
            Value::List(bounds, vals) => {
                let elem = hash_rows(vals);
                (0..bounds.len())
                    .map(|r| {
                        let (s, e) = bounds.span(r);
                        let mut h = mix(SEED_LIST, (e - s) as u64);
                        for k in s..e {
                            h = mix(h, elem[k]);
                        }
                        h
                    })
                    .collect()
            }
            // Unit: every row is identical content, so a single constant.
            Value::Unit(n) => vec![splitmix64(SEED_UNIT); *n],
        }
    }

    #[cfg(test)]
    mod hash_tests {
        use super::hash_rows;
        use crate::arrange::gather;
        use crate::value::{Bounds, Value};
        use std::sync::Arc;

        #[test]
        fn equal_and_single_diff() {
            let a = Value::u32(vec![10, 20, 30, 40]);
            let b = Value::u32(vec![10, 20, 30, 40]);
            assert_eq!(hash_rows(&a), hash_rows(&b));

            let c = Value::u32(vec![10, 20, 99, 40]);
            let ha = hash_rows(&a);
            let hc = hash_rows(&c);
            assert_eq!(ha[0], hc[0]);
            assert_eq!(ha[1], hc[1]);
            assert_ne!(ha[2], hc[2]); // exactly the differing row changed
            assert_eq!(ha[3], hc[3]);
        }

        #[test]
        fn structural_stride_vs_offsets() {
            // same logical list two ways: Stride(2,3) vs the equivalent Offsets([2,4,6]).
            let vals = Value::u16(vec![1, 2, 3, 4, 5, 6]);
            let strided = Value::List(Bounds::Stride(2, 3), Box::new(vals.clone()));
            let offsets = Value::List(Bounds::Offsets(vec![2, 4, 6]), Box::new(vals));
            assert_eq!(strided, offsets); // PartialEq-equal by the partition
            assert_eq!(hash_rows(&strided), hash_rows(&offsets));
        }

        #[test]
        fn ignores_arc_identity_and_capacity() {
            // distinct Arcs, and an over-capacity backing Vec, must not change the hash.
            let a = Value::u64(vec![7, 8, 9]);
            let mut backing = Vec::with_capacity(64);
            backing.extend_from_slice(&[7, 8, 9]);
            let b = Value::Prim(crate::value::Prim::U64(Arc::new(backing)));
            assert_eq!(hash_rows(&a), hash_rows(&b));
        }

        #[test]
        fn covers_prim_prod_sum_list_unit() {
            // Prod of a leaf and a nested Prod.
            let leaf = Value::u8(vec![1, 2, 3]);
            let nested = Value::Prod(vec![
                Value::u16(vec![10, 20, 30]),
                Value::u32(vec![100, 200, 300]),
            ]);
            let prod = Value::Prod(vec![leaf, nested]);
            let hp = hash_rows(&prod);
            assert_eq!(hp.len(), 3);
            assert!(hp[0] != hp[1] && hp[1] != hp[2]);

            // Sum with a ⊥/None lane (variant 0 uncommitted) and a multi-arm committed shape.
            // tags: rows go to lane 1 (u16) and lane 2 (u32).
            let s = Value::sum_opt(
                vec![1, 2, 1, 2],
                vec![
                    None,
                    Some(Value::u16(vec![5, 7])),
                    Some(Value::u32(vec![9, 11])),
                ],
            );
            let hs = hash_rows(&s);
            assert_eq!(hs.len(), 4);
            // rows 0 and 2 both land in lane 1 but carry different payloads → different hashes.
            assert_ne!(hs[0], hs[2]);

            // List with varying row widths.
            let list = Value::List(
                Bounds::Offsets(vec![2, 2, 5]), // rows of width 2, 0, 3
                Box::new(Value::u8(vec![1, 2, 3, 4, 5])),
            );
            let hl = hash_rows(&list);
            assert_eq!(hl.len(), 3);
            assert_ne!(hl[0], hl[1]); // width-2 vs empty row differ
            assert_ne!(hl[1], hl[2]);

            // Unit: all rows identical.
            let unit = Value::Unit(4);
            let hu = hash_rows(&unit);
            assert_eq!(hu.len(), 4);
            assert!(hu.iter().all(|&x| x == hu[0]));
        }

        #[test]
        fn empty_row_distinct_from_singleton_zero() {
            // a width-0 list row must not hash like a row holding a single 0 element.
            let list = Value::List(
                Bounds::Offsets(vec![0, 1]),
                Box::new(Value::u8(vec![0])),
            );
            let h = hash_rows(&list);
            assert_ne!(h[0], h[1]);
        }

        #[test]
        fn permutation_is_position_independent() {
            // hash_rows(gather(v, perm))[k] == hash_rows(v)[perm[k]].
            let v = Value::Prod(vec![
                Value::u32(vec![3, 1, 4, 1, 5, 9]),
                Value::u16(vec![30, 10, 40, 10, 50, 90]),
            ]);
            let perm = vec![5usize, 0, 3, 2, 1, 4];
            let base = hash_rows(&v);
            let permuted = hash_rows(&gather(&v, &perm));
            for (k, &p) in perm.iter().enumerate() {
                assert_eq!(permuted[k], base[p]);
            }
        }
    }

    #[cfg(test)]
    mod order_tests {
        use super::{
            compare_at, gather, group_bounds, run_layout, segment_labels, sort_blocks, survey, Run,
        };
        use crate::value::{Bounds, Value};
        use std::cmp::Ordering;

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
            let sorted = gather(&v, &perm).into_u64("sorted");
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
            let off = Bounds::Offsets(vec![2, 4, 6]);
            let stride = Bounds::Stride(2, 3);
            assert_eq!(segment_labels(&off), vec![0, 0, 1, 1, 2, 2]);
            assert_eq!(segment_labels(&off), segment_labels(&stride));
        }

        #[test]
        fn run_layout_simple() {
            // labels [0,0,1,2,2] → 3 runs: [0,2), [2,3), [3,5).
            let (ends, firsts) = run_layout(&[0, 0, 1, 2, 2]);
            assert_eq!(ends, vec![2, 3, 5]);
            assert_eq!(firsts, vec![0, 2, 3]);
        }

        #[test]
        fn roundtrip_matches_sortlist_op() {
            // build List<u64> with ragged rows, segmented-sort it via the arrange surface, and check
            // it reproduces exactly what the ML `sort` op (CmpOp::SortList) produces on the same list.
            let bounds = Bounds::Offsets(vec![3, 3, 6]); // rows [3,1,2], [], [5,0,4]
            let vals = Value::u64(vec![3, 1, 2, 5, 0, 4]);
            let list = Value::List(bounds.clone(), Box::new(vals.clone()));

            // arrange surface: seed segment labels from bounds, segmented-sort, gather by perm.
            let labels = segment_labels(&bounds);
            let (perm, _refined) = sort_blocks(&labels, &vals);
            let ours = Value::List(bounds.clone(), Box::new(gather(&vals, &perm)));

            // the ML op.
            let theirs = crate::ops::cmp::CmpOp::SortList.eval(list);
            assert_eq!(ours, theirs);
        }

        // --- survey / group_bounds ------------------------------------------------------------

        /// The obvious O(n+m) oracle: the same zig-zag two-pointer as [`survey`] but with a linear
        /// scan in place of the gallop, so it produces the identical `Run` structure element for
        /// element — a direct check that galloping changes only cost, not the reported interleaving.
        fn survey_naive(a: &Value, b: &Value) -> Vec<Run> {
            let (na, nb) = (a.len(), b.len());
            let (mut i, mut j) = (0usize, 0usize);
            let mut out = Vec::new();
            while i < na && j < nb {
                match compare_at(a, i, b, j) {
                    Ordering::Less => {
                        let start = i;
                        while i < na && compare_at(a, i, b, j) == Ordering::Less {
                            i += 1;
                        }
                        out.push(Run::A(start, i));
                    }
                    Ordering::Equal => {
                        out.push(Run::Both(i, j));
                        i += 1;
                        j += 1;
                    }
                    Ordering::Greater => {
                        let start = j;
                        while j < nb && compare_at(b, j, a, i) == Ordering::Less {
                            j += 1;
                        }
                        out.push(Run::B(start, j));
                    }
                }
            }
            if i < na {
                out.push(Run::A(i, na));
            }
            if j < nb {
                out.push(Run::B(j, nb));
            }
            out
        }

        /// Check every guarantee [`survey`] promises, plus exact agreement with the linear oracle.
        fn check_survey(a: &Value, b: &Value) {
            let runs = survey(a, b);
            assert_eq!(runs, survey_naive(a, b), "gallop must match the linear scan");

            // 1. coverage: the A ranges and every Both.ia partition 0..a.len() in order (B likewise).
            let (mut ai, mut bi) = (0usize, 0usize);
            for r in &runs {
                match *r {
                    Run::A(lo, hi) => {
                        assert_eq!(lo, ai, "gap/overlap in a coverage");
                        assert!(lo < hi, "empty A run");
                        ai = hi;
                    }
                    Run::B(lo, hi) => {
                        assert_eq!(lo, bi, "gap/overlap in b coverage");
                        assert!(lo < hi, "empty B run");
                        bi = hi;
                    }
                    Run::Both(ia, ib) => {
                        assert_eq!(ia, ai, "gap/overlap at a match (a side)");
                        assert_eq!(ib, bi, "gap/overlap at a match (b side)");
                        // 3. a match really is structurally equal.
                        assert_eq!(compare_at(a, ia, b, ib), Ordering::Equal, "unequal Both");
                        ai += 1;
                        bi += 1;
                    }
                }
            }
            assert_eq!(ai, a.len(), "a not fully covered");
            assert_eq!(bi, b.len(), "b not fully covered");

            // 2. sortedness: expanding the runs to their rows yields a non-decreasing merge. Each
            // emitted row is described as (side, index); consecutive rows must be <= structurally.
            let mut rows: Vec<(bool, usize)> = Vec::new(); // (from_a, index)
            for r in &runs {
                match *r {
                    Run::A(lo, hi) => rows.extend((lo..hi).map(|k| (true, k))),
                    Run::B(lo, hi) => rows.extend((lo..hi).map(|k| (false, k))),
                    Run::Both(ia, _) => rows.push((true, ia)),
                }
            }
            for w in rows.windows(2) {
                let side = |t: (bool, usize)| if t.0 { a } else { b };
                let ord = compare_at(side(w[0]), w[0].1, side(w[1]), w[1].1);
                assert_ne!(ord, Ordering::Greater, "merged sequence out of order");
            }
        }

        #[test]
        fn survey_disjoint_and_interleaved() {
            // fully disjoint, a all below b: one A run then one B run.
            assert_eq!(
                survey(&Value::u64(vec![1, 2, 3]), &Value::u64(vec![4, 5, 6])),
                vec![Run::A(0, 3), Run::B(0, 3)]
            );
            // strict interleave 1,2,3,4: alternating singleton runs, no matches.
            assert_eq!(
                survey(&Value::u64(vec![1, 3]), &Value::u64(vec![2, 4])),
                vec![Run::A(0, 1), Run::B(0, 1), Run::A(1, 2), Run::B(1, 2)]
            );
            check_survey(&Value::u64(vec![1, 2, 3]), &Value::u64(vec![4, 5, 6]));
            check_survey(&Value::u64(vec![1, 3]), &Value::u64(vec![2, 4]));
        }

        #[test]
        fn survey_matches_and_gallop() {
            // shared keys become Both; the long below-pivot stretches are what the gallop skips.
            let a = Value::u64(vec![1, 2, 3, 4, 5]);
            let b = Value::u64(vec![3, 5, 7]);
            assert_eq!(
                survey(&a, &b),
                vec![Run::A(0, 2), Run::Both(2, 0), Run::A(3, 4), Run::Both(4, 1), Run::B(2, 3)]
            );
            check_survey(&a, &b);
        }

        #[test]
        fn survey_duplicates_and_edges() {
            // duplicate on one side after a match falls through as a follow-on A run.
            check_survey(&Value::u64(vec![5, 5, 5]), &Value::u64(vec![5]));
            check_survey(&Value::u64(vec![5]), &Value::u64(vec![5, 5, 5]));
            // all equal, equal lengths: three Both pairs.
            assert_eq!(
                survey(&Value::u64(vec![7, 7]), &Value::u64(vec![7, 7])),
                vec![Run::Both(0, 0), Run::Both(1, 1)]
            );
            // empty inputs.
            assert_eq!(survey(&Value::u64(vec![]), &Value::u64(vec![1, 2])), vec![Run::B(0, 2)]);
            assert_eq!(survey(&Value::u64(vec![1, 2]), &Value::u64(vec![])), vec![Run::A(0, 2)]);
            assert!(survey(&Value::u64(vec![]), &Value::u64(vec![])).is_empty());
        }

        #[test]
        fn survey_over_product_columns() {
            // structural (key, val) rows: lexicographic order, some rows shared across the two runs.
            let a = Value::Prod(vec![
                Value::u64(vec![1, 1, 2, 3]),
                Value::u32(vec![10, 20, 10, 10]),
            ]);
            let b = Value::Prod(vec![
                Value::u64(vec![1, 2, 2, 4]),
                Value::u32(vec![20, 10, 30, 10]),
            ]);
            // (1,20) and (2,10) are shared → two Both pairs; the rest interleave by lex order.
            check_survey(&a, &b);
            let runs = survey(&a, &b);
            let both: Vec<_> = runs.iter().filter(|r| matches!(r, Run::Both(..))).collect();
            assert_eq!(both, vec![&Run::Both(1, 0), &Run::Both(2, 1)]);
        }

        #[test]
        fn survey_at_scale_matches_oracle() {
            // two sorted runs drawn from an overlapping key space (deterministic LCG, no rng dep),
            // exercising long gallop stretches and scattered matches against the linear oracle.
            let mk = |seed: u64, n: usize, modulus: u64| {
                let mut s = seed;
                let mut xs: Vec<u64> = (0..n)
                    .map(|_| {
                        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        (s >> 33) % modulus
                    })
                    .collect();
                xs.sort_unstable();
                Value::u64(xs)
            };
            check_survey(&mk(1, 500, 300), &mk(2, 500, 300)); // dense overlap, many Both + dups
            check_survey(&mk(3, 800, 5000), &mk(4, 200, 5000)); // sparse, lopsided sizes
            check_survey(&mk(5, 1, 10), &mk(6, 1000, 10)); // single-element vs large
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
            assert_eq!(group_bounds(&keys), run_layout(&labels).0);
        }
    }
}

#[cfg(test)]
mod find_ranges_fast_path {
    use crate::Value;

    /// The `U64` fast path in `arrange::find_ranges` must agree with the generic
    /// structural path exactly, including absent needles (`lo == hi`), duplicate runs,
    /// unsorted needles, and empty inputs.
    #[test]
    fn agrees_with_generic() {
        // Generic path, forced by wrapping the leaves in a 1-field product (same order,
        // same equal-classes, but not the `Prim::U64`/`Prim::U64` pattern).
        fn generic(needles: &[u64], hay: &[u64]) -> (Vec<usize>, Vec<usize>) {
            // Two fields: not peelable to a leaf, so this takes the structural path.
            let n = Value::Prod(vec![Value::u64(needles.to_vec()), Value::u64(vec![0; needles.len()])]);
            let h = Value::Prod(vec![Value::u64(hay.to_vec()), Value::u64(vec![0; hay.len()])]);
            crate::arrange::find_ranges(&n, &h)
        }
        fn fast(needles: &[u64], hay: &[u64]) -> (Vec<usize>, Vec<usize>) {
            crate::arrange::find_ranges(&Value::u64(needles.to_vec()), &Value::u64(hay.to_vec()))
        }

        let mut state = 0x243f6a8885a308d3u64;
        let mut rng = move || { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state };
        for case in 0..200 {
            // Small value range so duplicates and absences are common.
            let hlen = (rng() % 24) as usize;
            let mut hay: Vec<u64> = (0..hlen).map(|_| rng() % 8).collect();
            hay.sort();
            let nlen = (rng() % 8) as usize;
            let needles: Vec<u64> = (0..nlen).map(|_| rng() % 10).collect(); // unsorted, may miss
            assert_eq!(fast(&needles, &hay), generic(&needles, &hay), "case {case}: needles={needles:?} hay={hay:?}");
        }
        // Degenerate shapes.
        assert_eq!(fast(&[], &[1, 2]), generic(&[], &[1, 2]));
        assert_eq!(fast(&[1, 2], &[]), generic(&[1, 2], &[]));
        assert_eq!(fast(&[u64::MAX], &[0, u64::MAX]), generic(&[u64::MAX], &[0, u64::MAX]));
        // The 1-tuple wrapping DDIR actually uses must reach the fast path and agree.
        let wrap = |xs: &[u64]| Value::Prod(vec![Value::u64(xs.to_vec())]);
        assert_eq!(
            crate::arrange::find_ranges(&wrap(&[1, 5, 9]), &wrap(&[1, 1, 5, 7])),
            generic(&[1, 5, 9], &[1, 1, 5, 7]),
        );
    }
}
