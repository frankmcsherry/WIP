//! The comparison/order op bucket. Two leaf compares — `Rel` (two columns → mask) and `Gt` (a column
//! vs a constant, the immediate-form sugar) — plus the list ops `SortList`/`DedupList`/`GroupKey`
//! (discrimination via `sort_blocks`/`run_layout`) and `Find` (batched binary search via `compare_idx`). All are
//! kind-blind: they read the stored bytes, correct for unsigned and order-preserving signed alike. A
//! flat enum (no sub-graphs); `NumOp` embeds it as the `Cmp` bucket alongside `Core`/`Arith`.
//! The structural-order engine these ops reduce to is the private [`order`] submodule.

pub(crate) mod order;

use crate::engine::gather;
use order::{
    compare_cols, compare_idx, known_sorted, run_firsts, run_layout, runs_per_row, segment_labels,
    sort_blocks, sorted_signs,
};
use crate::shape::{same, shape_of_value};
use crate::value::Value;

/// a relational predicate for the leaf compare-to-mask op [`CmpOp::Rel`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Pred {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Pred {
    /// does this predicate hold for a lane's comparison sign (`-1`/`0`/`+1` for `<`/`=`/`>`)?
    fn test(self, o: i8) -> bool {
        match self {
            Pred::Eq => o == 0,
            Pred::Ne => o != 0,
            Pred::Lt => o < 0,
            Pred::Le => o <= 0,
            Pred::Gt => o > 0,
            Pred::Ge => o >= 0,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Rel(Pred), // (X, X) -> U64 mask   lane-wise compare of two equal-width leaf columns (kind-blind)
    RelImm(Pred, u64), // X -> U64 mask   the IMMEDIATE form of `Rel`: compare a leaf column against
               // a constant, given in the leaf's stored form. Kind-blind and any width, where the
               // pair form `(x, x lit c) <pred>` has to broadcast an n-element constant column and
               // build a product to say the same thing. (This was `Gt(u64)`, a U64-only threshold.)
    Min,       // (X, X) -> X   lane-wise minimum (kind-blind byte min; order op, no deswizzle)
    Max,       // (X, X) -> X   lane-wise maximum
    SortList,  // List<X> -> List<X>   structural order
    DedupList, // List<X> -> List<X>   distinct, per row (sorted)
    GroupKey,  // List<(K,V)> -> List<(K, List<V>)>   group by key, per row (sorted)
    Find,      // (needle:List<X>, haystack:List<X>) -> List<(lo,hi)>  equal_range / needle elem
}

impl CmpOp {
    pub(crate) fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            CmpOp::Rel(pred) => {
                let (a, b) = input.into_pair("Rel")?;
                same(&shape_of_value(&a), &shape_of_value(&b)).map_err(|e| format!("Rel: {e}"))?;
                assert_eq!(a.len(), b.len(), "Rel: operands at different strata");
                let mask = match (&a, &b) {
                    // leaf pair: the vectorized lane compare. Resolve the predicate to its three
                    // order-flags ONCE here (sign `-1`/`0`/`+1`), so `rel`'s lane loop is branchless.
                    (Value::Prim(pa), Value::Prim(pb)) =>
                        pa.rel(pb, pred.test(-1), pred.test(0), pred.test(1)),
                    // any other shape: the bulk structural comparator — one descent per type level,
                    // linear (the Sum arm computes within-offsets in bulk, not a per-lane rescan).
                    _ => compare_cols(&a, &b).iter().map(|&o| pred.test(o) as u64).collect(),
                };
                Value::u64(mask)
            }

            CmpOp::Min | CmpOp::Max => {
                let take_max = matches!(self, CmpOp::Max);
                let (a, b) = input.into_pair("min/max")?;
                let (pa, pb) = (a.into_prim("min/max lhs")?, b.into_prim("min/max rhs")?);
                if pa.bits() != pb.bits() {
                    return Err(format!("min/max expects two equal-width leaves, got U{} and U{}", pa.bits(), pb.bits()));
                }
                assert_eq!(pa.len(), pb.len(), "min/max: operands at different strata");
                Value::Prim(pa.lane_pick(pb, take_max))
            }

            CmpOp::RelImm(pred, c) => {
                let p = input.into_prim("relational immediate")?;
                // the constant is the leaf's STORED form, so it has to fit the leaf's width —
                // truncating it silently would compare against a different value than written.
                if p.bits() < 64 && *c >= (1u64 << p.bits()) {
                    return Err(format!("{pred:?} {c}: constant does not fit a U{} leaf", p.bits()));
                }
                Value::u64(p.rel_imm(*c, pred.test(-1), pred.test(0), pred.test(1)))
            }

            CmpOp::SortList => {
                let (bounds, vals) = input.into_list("SortList")?;
                // Already ordered: the permutation is the identity, so the discrimination pass and
                // the gather after it would together copy the column to reproduce it. See
                // `sorted_signs` for why asking is worth it — a dataflow's batches arrive sorted.
                if known_sorted(&bounds, &vals) {
                    return Ok(Value::List(bounds, Box::new(vals)));
                }
                // A bare leaf has no companion column to carry, so the VALUES can be sorted
                // directly: no permutation to build, no indirection through one, and no gather
                // after. Equal leaf elements are indistinguishable, so stability is unobservable.
                if let Value::Prim(mut p) = vals {
                    p.sort_rows(&bounds);
                    return Ok(Value::List(bounds, Box::new(Value::Prim(p))));
                }
                let (perm, _) = sort_blocks(&segment_labels(&bounds), &vals);
                Value::List(bounds, Box::new(gather(&vals, &perm)))
            }

            CmpOp::DedupList => {
                // distinct, per row: discriminate, then keep one representative per run.
                let (bounds, vals) = input.into_list("DedupList")?;
                // Already ordered: the runs are the adjacent-equal spans, which the sortedness
                // check has just computed. The permutation would be the identity, so the run
                // firsts ARE the representatives — no discrimination pass at all.
                if let Some(signs) = sorted_signs(&bounds, &vals) {
                    let firsts = run_firsts(&bounds, &signs);
                    let nb = runs_per_row(&bounds, &firsts);
                    return Ok(Value::List(nb.into(), Box::new(gather(&vals, &firsts))));
                }
                // as in `SortList`: a leaf payload sorts by value, and the distinct elements are
                // then one compacting pass over the sorted rows.
                if let Value::Prim(mut p) = vals {
                    p.sort_rows(&bounds);
                    let nb = p.dedup_sorted_rows(&bounds);
                    return Ok(Value::List(nb.into(), Box::new(Value::Prim(p))));
                }
                let (perm, labels) = sort_blocks(&segment_labels(&bounds), &vals);
                let (_ends, firsts) = run_layout(&labels);
                let idx: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
                // outer bounds: cumulative distinct count per row (runs never cross rows).
                let nb = runs_per_row(&bounds, &firsts);
                Value::List(nb.into(), Box::new(gather(&vals, &idx)))
            }

            CmpOp::GroupKey => {
                // group by key, per row: discriminate by K (stable → V keeps order); the
                // K-runs are the groups, and each run's V-span is its inner list.
                let (bounds, vals) = input.into_list("GroupKey")?;
                let (k_col, v_col) = vals.into_pair("GroupKey values")?;
                // Keys already ordered: the permutation is the identity, so the values need no
                // gather at all (a stable sort would leave them exactly where they are), and the
                // key runs are the adjacent-equal spans the check just computed.
                if let Some(signs) = sorted_signs(&bounds, &k_col) {
                    let firsts = run_firsts(&bounds, &signs);
                    // exclusive ends of the runs: each run ends where the next begins, and the
                    // last at the end of the column.
                    let mut ends: Vec<usize> = Vec::with_capacity(firsts.len());
                    if let Some(rest) = firsts.get(1..) {
                        ends.extend_from_slice(rest);
                        ends.push(k_col.len());
                    }
                    let keys = gather(&k_col, &firsts);
                    let inner = Value::List(ends.into(), Box::new(v_col));
                    let no = runs_per_row(&bounds, &firsts);
                    return Ok(Value::List(
                        no.into(),
                        Box::new(Value::Prod(vec![keys, inner])),
                    ));
                }
                let (perm, klabels) = sort_blocks(&segment_labels(&bounds), &k_col);
                let v_sorted = gather(&v_col, &perm);
                let (ends, firsts) = run_layout(&klabels);
                // the representatives compose: reading `perm` at the run starts is the same index
                // as sorting the whole key column and then subsetting it (as `DedupList` does).
                let reps: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
                let keys = gather(&k_col, &reps);
                let inner = Value::List(ends.into(), Box::new(v_sorted));
                // outer bounds: cumulative #groups per row.
                let no = runs_per_row(&bounds, &firsts);
                Value::List(no.into(), Box::new(Value::Prod(vec![keys, inner])))
            }

            // for each needle element, equal_range it in the matching haystack row (batched binary
            // search, see `batched_bound`). Output shaped like `needle`, each (lo,hi) relative to its row.
            CmpOp::Find => {
                let (needle, haystack) = input.into_pair("Find")?;
                let (nb, nvals) = needle.into_list("Find needle")?;
                let (hb, hvals) = haystack.into_list("Find haystack")?;
                same(&shape_of_value(&nvals), &shape_of_value(&hvals)).map_err(|e| format!("Find: {e}"))?;
                assert_eq!(nb.len(), hb.len(), "Find: needle/haystack row count");
                let n = nvals.len();
                // each needle element's haystack-row window [lo,hi). The window's start is also the
                // row base the answer is relative to; the search moves `lo`, so the base is rewalked
                // off the bounds at the end rather than kept as a third copy of the same column.
                let (mut lo, mut hi) = (vec![0usize; n], vec![0usize; n]);
                let (mut ns, mut hs) = (0, 0);
                for r in 0..nb.len() {
                    let (ne, he) = (nb.end(r), hb.end(r));
                    for k in ns..ne {
                        lo[k] = hs;
                        hi[k] = he;
                    }
                    ns = ne;
                    hs = he;
                }
                // lower = first haystack pos NOT less than the needle; upper = first GREATER. Same
                // batched search, different tie rule on `haystack[mid] vs needle`.
                let mut lower = (lo.clone(), hi.clone());
                let mut upper = (lo, hi);
                batched_bound(&hvals, &nvals, &mut lower.0, &mut lower.1, |o| o < 0);
                batched_bound(&hvals, &nvals, &mut upper.0, &mut upper.1, |o| o <= 0);
                // row-relative: subtract each element's haystack row start, rewalked here.
                let (mut lo_c, mut hi_c) = (Vec::with_capacity(n), Vec::with_capacity(n));
                let (mut ns, mut hs) = (0, 0);
                for r in 0..nb.len() {
                    let (ne, he) = (nb.end(r), hb.end(r));
                    for k in ns..ne {
                        lo_c.push((lower.0[k] - hs) as u64);
                        hi_c.push((upper.0[k] - hs) as u64);
                    }
                    ns = ne;
                    hs = he;
                }
                Value::List(nb, Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])))
            }
        })
    }

}

/// one batched lower/upper-bound search: every needle element advances its window `[lo,hi)` in
/// lockstep until it collapses, one `compare_idx` per round comparing `haystack[mid]` to its needle
/// element. `go_right(sign)` is the tie rule (`sign` is haystack-vs-needle, `-1`/`0`/`+1`): lower bound
/// steps right on `< 0`, upper bound on `<= 0`. Rounds = ⌈log₂ max-span⌉; each is linear in the live
/// needles. No gather and no whole-row compare — `compare_idx` pushes the (mid, needle) index pairs down.
fn batched_bound(
    hvals: &Value,
    nvals: &Value,
    lo: &mut [usize],
    hi: &mut [usize],
    go_right: impl Fn(i8) -> bool,
) {
    // The live needle set only shrinks: seed it once and compact in place each round, so a
    // round's work tracks the ACTIVE needles, not all of them (the full rescan per round was
    // ~8% of a join-heavy profile). `active` doubles as the needle indices into `nvals`.
    let mut active: Vec<usize> = (0..lo.len()).filter(|&k| lo[k] < hi[k]).collect();
    let mut mids: Vec<usize> = Vec::with_capacity(active.len());
    while !active.is_empty() {
        mids.clear();
        mids.extend(active.iter().map(|&k| (lo[k] + hi[k]) / 2));
        let ord = compare_idx(hvals, nvals, &mids, &active);
        let mut w = 0usize;
        for t in 0..active.len() {
            let k = active[t];
            if go_right(ord[t]) {
                lo[k] = mids[t] + 1;
            } else {
                hi[k] = mids[t];
            }
            if lo[k] < hi[k] {
                active[w] = k;
                w += 1;
            }
        }
        active.truncate(w);
    }
}

#[cfg(test)]
mod sorted_fast_paths {
    //! The `sort`/`dedup`/`group` fast paths for an already-ordered column must produce EXACTLY
    //! what the discrimination path produces. The references below are the op bodies as they were
    //! before the fast paths existed, so this is a direct A/B rather than a restatement of the
    //! intent — delete a fast path and these still pass; break one and they fail.

    use super::order::{run_layout, runs_per_row, segment_labels, sort_blocks};
    use super::CmpOp;
    use crate::engine::gather;
    use crate::value::{Bounds, Value};

    fn ref_sort(v: Value) -> Value {
        let (bounds, vals) = v.into_list("ref").unwrap();
        let (perm, _) = sort_blocks(&segment_labels(&bounds), &vals);
        Value::List(bounds, Box::new(gather(&vals, &perm)))
    }
    fn ref_dedup(v: Value) -> Value {
        let (bounds, vals) = v.into_list("ref").unwrap();
        let (perm, labels) = sort_blocks(&segment_labels(&bounds), &vals);
        let (_ends, firsts) = run_layout(&labels);
        let idx: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
        let nb = runs_per_row(&bounds, &firsts);
        Value::List(nb.into(), Box::new(gather(&vals, &idx)))
    }
    fn ref_group(v: Value) -> Value {
        let (bounds, vals) = v.into_list("ref").unwrap();
        let (k_col, v_col) = vals.into_pair("ref").unwrap();
        let (perm, klabels) = sort_blocks(&segment_labels(&bounds), &k_col);
        let v_sorted = gather(&v_col, &perm);
        let (ends, firsts) = run_layout(&klabels);
        let reps: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
        let keys = gather(&k_col, &reps);
        let inner = Value::List(ends.into(), Box::new(v_sorted));
        let no = runs_per_row(&bounds, &firsts);
        Value::List(no.into(), Box::new(Value::Prod(vec![keys, inner])))
    }

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }
    fn list(ends: Vec<usize>, vals: Value) -> Value {
        Value::List(ends.into(), Box::new(vals))
    }

    /// every shape the fast paths can meet, in an ALREADY SORTED state (so the fast path fires)
    /// and in an unsorted state (so it does not, and the reference path is exercised too).
    fn cases() -> Vec<Value> {
        vec![
            // one row, sorted, with duplicate runs at the start, middle and end.
            list(vec![8], u(&[1, 1, 2, 5, 5, 5, 9, 9])),
            // one row, unsorted.
            list(vec![8], u(&[5, 1, 9, 2, 5, 1, 9, 5])),
            // several rows: sorted within each, but a row's last element exceeds the next's first —
            // the case a whole-column sortedness scan would get wrong.
            list(vec![3, 6, 9], u(&[7, 8, 9, 1, 2, 3, 4, 4, 6])),
            // ragged rows, including empty ones at the front, middle and end.
            list(vec![0, 2, 2, 5, 5], u(&[3, 3, 1, 2, 2])),
            // a compound key, sorted lexicographically (the leading field ties in places).
            list(vec![5], Value::Prod(vec![u(&[1, 1, 1, 2, 2]), u(&[10, 10, 20, 5, 7])])),
            // a compound key whose LEADING field is sorted but whose second is not — the cheap
            // reject passes and the structural check has to catch it.
            list(vec![4], Value::Prod(vec![u(&[1, 1, 2, 2]), u(&[20, 10, 5, 7])])),
            // narrow leaves.
            list(vec![6], Value::u8(vec![0, 0, 3, 3, 3, 255])),
            // a sum column, sorted by tag then payload.
            list(vec![5], Value::sum(vec![0, 0, 1, 1, 1], vec![u(&[4, 4]), u(&[1, 2, 2])])),
            // a list-of-lists, sorted length-first.
            list(
                vec![4],
                list(vec![0, 1, 3, 3], u(&[7, 2, 9])),
            ),
            // unit rows: all equal, so one run.
            list(vec![4], Value::Unit(4)),
            // degenerate: an empty column, and a single element.
            list(vec![0], u(&[])),
            list(vec![1], u(&[42])),
            // a strided partition, sorted (the `Bounds::Stride` representation, not `Offsets`).
            Value::List(Bounds::Stride(2, 3), Box::new(u(&[1, 4, 2, 2, 5, 9]))),
        ]
    }

    /// a deterministic LCG column, as the benches use — big enough to take the radix path (blocks
    /// over 32 elements) at every leaf width, with a small value range so duplicate runs are dense.
    fn scrambled(n: usize, modulus: u64) -> Vec<u64> {
        (0..n as u64)
            .map(|i| {
                (i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407) >> 32)
                    % modulus
            })
            .collect()
    }

    /// Leaf columns past the insertion-sort threshold, at every width and in several row layouts —
    /// the sizes that reach the radix, which the hand-written cases above are all too small for.
    fn radix_cases() -> Vec<Value> {
        let mut out = Vec::new();
        for &n in &[33usize, 100, 5000] {
            for &modulus in &[4u64, 1000, u32::MAX as u64 + 1] {
                let xs = scrambled(n, modulus);
                out.push(list(vec![n], u(&xs)));
                // the same values split across ragged rows, so each row sorts independently.
                let ends = vec![n / 4, n / 4, n / 2, n];
                out.push(list(ends, u(&xs)));
                // narrower leaves take a different width arm and fewer radix passes.
                out.push(list(vec![n], Value::u8(xs.iter().map(|&x| x as u8).collect())));
                out.push(list(vec![n], Value::u16(xs.iter().map(|&x| x as u16).collect())));
                out.push(list(vec![n], Value::u32(xs.iter().map(|&x| x as u32).collect())));
            }
        }
        // every value equal (the all-zero-significant-bits early return) and a single element.
        out.push(list(vec![64], u(&vec![7; 64])));
        out.push(list(vec![64], u(&vec![0; 64])));
        out
    }

    #[test]
    fn sort_matches_the_discrimination_path() {
        for v in cases().into_iter().chain(radix_cases()) {
            assert_eq!(
                CmpOp::SortList.eval(v.clone()).unwrap(),
                ref_sort(v.clone()),
                "sort disagreed on {}",
                crate::value::show(&v)
            );
        }
    }

    #[test]
    fn dedup_matches_the_discrimination_path() {
        for v in cases().into_iter().chain(radix_cases()) {
            assert_eq!(
                CmpOp::DedupList.eval(v.clone()).unwrap(),
                ref_dedup(v.clone()),
                "dedup disagreed on {}",
                crate::value::show(&v)
            );
        }
    }

    #[test]
    fn group_matches_the_discrimination_path() {
        // (K, V) pairs: sorted by K with duplicate keys (so the V order within a group is the
        // thing at risk), sorted by K in several rows, and unsorted.
        let pairs = |ends: Vec<usize>, ks: &[u64], vs: &[u64]| {
            list(ends, Value::Prod(vec![u(ks), u(vs)]))
        };
        let cases = [
            pairs(vec![6], &[1, 1, 2, 3, 3, 3], &[10, 11, 20, 30, 31, 32]),
            pairs(vec![3, 6], &[5, 5, 9, 1, 2, 2], &[1, 2, 3, 4, 5, 6]),
            pairs(vec![6], &[3, 1, 2, 1, 3, 2], &[10, 11, 20, 30, 31, 32]),
            pairs(vec![0, 3], &[7, 7, 8], &[1, 2, 3]),
            pairs(vec![0], &[], &[]),
        ];
        for v in cases {
            assert_eq!(
                CmpOp::GroupKey.eval(v.clone()).unwrap(),
                ref_group(v.clone()),
                "group disagreed on {}",
                crate::value::show(&v)
            );
        }
    }
}
