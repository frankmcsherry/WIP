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
    use crate::value::{Bounds, Prim, Value};
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
}
