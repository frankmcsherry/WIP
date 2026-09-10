//! The comparison/order op bucket. Two leaf compares — `Rel` (two columns → mask) and `Gt` (a column
//! vs a constant, the immediate-form sugar) — plus the list ops `SortList`/`DedupList`/`GroupKey`
//! (discrimination via `sort_blocks`/`run_layout`) and `Find` (batched binary search via `compare_idx`). All are
//! kind-blind: they read the stored bytes, correct for unsigned and order-preserving signed alike. A
//! flat enum (no sub-graphs); `NumOp` embeds it as the `Cmp` bucket alongside `Core`/`Arith`.
//! The structural-order engine these ops reduce to is the private [`order`] submodule.

pub(crate) mod order;
pub(crate) mod sort;
pub(crate) mod survey;

use crate::engine::gather;
use order::{compare_adjacent, compare_cols, compare_idx, run_layout, runs_per_row, segment_labels};
use sort::{contains_list, sort_blocks, sort_values, sort_values_only};
use crate::shape::{same, shape_of_value};
use crate::value::{Bounds, Prim, Value};
use survey::{find_groups, find_sorted};

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
    Gt(u64),   // X -> U64 mask    (x > c) as 0/1   — the column-vs-immediate sugar form
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

            CmpOp::Gt(c) => {
                let xs = input.as_u64("Gt")?;
                Value::u64(xs.iter().map(|&x| (x > *c) as u64).collect())
            }

            // the sort produces the sorted column itself; nothing is gathered afterwards.
            CmpOp::SortList => {
                let (bounds, vals) = input.into_list("SortList")?;
                let (_, sorted) = sort_values_only(&row_labels(&bounds), &vals);
                Value::List(bounds, Box::new(sorted))
            }

            CmpOp::DedupList => {
                // distinct, per row: sort, then keep one representative per run.
                let (bounds, vals) = input.into_list("DedupList")?;
                let (kept, _ends, firsts, _perm) = representatives(&row_labels(&bounds), &vals, false);
                // outer bounds: cumulative distinct count per row (runs never cross rows).
                let nb = runs_per_row(&bounds, &firsts);
                Value::List(nb.into(), Box::new(kept))
            }

            CmpOp::GroupKey => {
                // group by key, per row: sort by K (stable → V keeps order); the K-runs are the
                // groups, and each run's V-span is its inner list. The payload follows the
                // permutation; the keys are one representative per run.
                let (bounds, vals) = input.into_list("GroupKey")?;
                let (k_col, v_col) = vals.into_pair("GroupKey values")?;
                let (keys, ends, firsts, perm) = representatives(&row_labels(&bounds), &k_col, true);
                let v_sorted = gather(&v_col, &perm);
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
                // A needle that is itself in order is MERGED into the haystack instead of
                // searched per probe: the shape a join has, both sides sorted, and the difference
                // between `|needle| * log|haystack|` comparisons and `|needle| + |haystack|`. A
                // leaf pair takes the direct walk; any other shape the level walk of the survey.
                // Asking costs one pass over the needle, which exits at the first inversion.
                let (lo_c, hi_c) = if rows_sorted(&nb, &nvals) {
                    match find_sorted(&nb, &nvals, &hb, &hvals) {
                        Some(walk) => walk,
                        None => find_groups(&nb, &nvals, &hb, &hvals),
                    }
                } else {
                    find_search(&nb, &nvals, &hb, &hvals)
                };
                Value::List(nb, Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])))
            }
        })
    }

}

/// Is every row of `bounds` non-decreasing in structural order? A leaf is scanned directly,
/// exiting at the first inversion, so an unordered leaf costs a few loads for the question; any
/// other shape takes one adjacent structural compare, a pass over the column.
fn rows_sorted(bounds: &Bounds, vals: &Value) -> bool {
    fn scan<T: Ord>(bounds: &Bounds, v: &[T]) -> bool {
        let mut start = 0;
        for end in bounds.ends() {
            if v[start..end].windows(2).any(|w| w[0] > w[1]) {
                return false;
            }
            start = end;
        }
        true
    }
    match vals {
        Value::Prim(Prim::U8(v)) => scan(bounds, v),
        Value::Prim(Prim::U16(v)) => scan(bounds, v),
        Value::Prim(Prim::U32(v)) => scan(bounds, v),
        Value::Prim(Prim::U64(v)) => scan(bounds, v),
        _ => {
            // `signs[k]` orders element `k` against `k + 1`; a row boundary is not a comparison.
            let signs = compare_adjacent(vals);
            let mut start = 0;
            for end in bounds.ends() {
                if end > start && signs[start..end - 1].iter().any(|&o| o > 0) {
                    return false;
                }
                start = end;
            }
            true
        }
    }
}

/// Every needle element's `[lo, hi)` in its haystack row by the batched binary search: each
/// element's window `[lo, hi)` halves per round until it collapses, one `compare_idx` per round
/// (see [`batched_bound`]). The form for a needle in no particular order.
fn find_search(nb: &Bounds, nvals: &Value, hb: &Bounds, hvals: &Value) -> (Vec<u64>, Vec<u64>) {
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
    batched_bound(hvals, nvals, &mut lower.0, &mut lower.1, |o| o < 0);
    batched_bound(hvals, nvals, &mut upper.0, &mut upper.1, |o| o <= 0);
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
    (lo_c, hi_c)
}

/// The labels for a per-row sort: each element its row, or none at all when there is one row.
fn row_labels(bounds: &Bounds) -> Vec<u64> {
    if bounds.len() == 1 { Vec::new() } else { segment_labels(bounds) }
}

/// Sort within `labels`' blocks and keep one row per run of equal rows: `(kept, run ends, run
/// starts, the sort's permutation)`, the permutation empty unless `with_perm`. A leaf or a
/// product of leaves comes straight out of the sort, sorted, and the runs are read off it in
/// ascending order; a shape with a `List` in it has a sorted form that is itself a gather of
/// every element, so there the kept rows alone are gathered from the source.
fn representatives(labels: &[u64], v: &Value, with_perm: bool) -> (Value, Vec<usize>, Vec<usize>, Vec<usize>) {
    if contains_list(v) {
        let (perm, refined) = sort_blocks(labels, v);
        let (ends, firsts) = run_layout(&refined);
        let idx: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
        (gather(v, &idx), ends, firsts, perm)
    } else if with_perm {
        let (perm, refined, sorted) = sort_values(labels, v);
        let (ends, firsts) = run_layout(&refined);
        (gather(&sorted, &firsts), ends, firsts, perm)
    } else {
        let (refined, sorted) = sort_values_only(labels, v);
        let (ends, firsts) = run_layout(&refined);
        (gather(&sorted, &firsts), ends, firsts, Vec::new())
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
mod merged_find {
    use super::CmpOp;
    use crate::value::Value;

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }
    fn list(ends: Vec<usize>, vals: Value) -> Value {
        Value::List(ends.into(), Box::new(vals))
    }

    /// The merged `find` (a sorted needle walked into the haystack) must answer exactly what the
    /// per-probe search answers, including absent needles, duplicate runs on either side, empty
    /// rows, needles outside the haystack's range on both ends, and needles at every leaf width.
    #[test]
    fn merged_find_matches_the_per_probe_search() {
        /// the batched-search path: a two-field product orders exactly as its first field but is
        /// not a leaf, so it never takes the merge.
        fn reference(nb: Vec<usize>, needles: &[u64], hb: Vec<usize>, hay: &[u64]) -> Value {
            let pad = |xs: &[u64]| Value::Prod(vec![u(xs), Value::u64(vec![0; xs.len()])]);
            CmpOp::Find.eval(Value::Prod(vec![list(nb, pad(needles)), list(hb, pad(hay))])).unwrap()
        }
        fn merged(nb: Vec<usize>, needles: &[u64], hb: Vec<usize>, hay: &[u64]) -> Value {
            CmpOp::Find.eval(Value::Prod(vec![list(nb, u(needles)), list(hb, u(hay))])).unwrap()
        }
        fn merged_u16(nb: Vec<usize>, needles: &[u64], hb: Vec<usize>, hay: &[u64]) -> Value {
            let narrow = |xs: &[u64]| Value::u16(xs.iter().map(|&x| x as u16).collect());
            CmpOp::Find.eval(Value::Prod(vec![list(nb, narrow(needles)), list(hb, narrow(hay))])).unwrap()
        }
        /// (needle row ends, needle values, haystack row ends, haystack values)
        type Case = (Vec<usize>, Vec<u64>, Vec<usize>, Vec<u64>);
        let cases: Vec<Case> = vec![
            // dense hits, misses at both ends, duplicate runs on both sides
            (vec![6], vec![0, 1, 1, 3, 7, 9], vec![8], vec![1, 1, 2, 3, 3, 3, 5, 8]),
            // needle entirely below / above the haystack
            (vec![2], vec![0, 0], vec![3], vec![5, 6, 7]),
            (vec![2], vec![9, 9], vec![3], vec![5, 6, 7]),
            // several rows, each with its own range; the needle is sorted within each row only
            (vec![2, 4], vec![1, 5, 2, 2], vec![3, 7], vec![1, 5, 5, 0, 2, 2, 9]),
            // empty needle row, empty haystack row
            (vec![0, 2], vec![3, 4], vec![2, 2], vec![3, 4]),
            (vec![2, 2], vec![3, 4], vec![0, 2], vec![3, 4]),
            // everything empty
            (vec![0], vec![], vec![0], vec![]),
            // every needle equal, every haystack element equal
            (vec![3], vec![4, 4, 4], vec![4], vec![4, 4, 4, 4]),
            // an UNSORTED needle row: the search, not the merge, and the same answer
            (vec![4], vec![9, 1, 5, 1], vec![4], vec![1, 1, 5, 8]),
        ];
        for (nb, needles, hb, hay) in cases {
            let want = reference(nb.clone(), &needles, hb.clone(), &hay);
            assert_eq!(merged(nb.clone(), &needles, hb.clone(), &hay), want, "needles={needles:?} hay={hay:?}");
            assert_eq!(merged_u16(nb, &needles, hb, &hay), want, "u16 needles={needles:?} hay={hay:?}");
        }
        // at scale: a sorted needle over an overlapping key space, against the same reference.
        let mut hay: Vec<u64> = (0..2000u64).map(|i| (i * 7) % 900).collect();
        hay.sort_unstable();
        let mut needles: Vec<u64> = (0..500u64).map(|i| (i * 13) % 1000).collect();
        needles.sort_unstable();
        assert_eq!(
            merged(vec![needles.len()], &needles, vec![hay.len()], &hay),
            reference(vec![needles.len()], &needles, vec![hay.len()], &hay),
        );
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// a random column of `rows` rows over a small value space, so that the two sides share rows,
    /// nesting up to `depth` levels below the top.
    fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
        if depth == 0 {
            return Value::u64((0..rows).map(|_| rng.below(4) as u64).collect());
        }
        match rng.below(6) {
            0 => Value::u8((0..rows).map(|_| rng.below(3) as u8).collect()),
            1 => Value::Prod((0..1 + rng.below(3)).map(|_| random_value(rng, rows, depth - 1)).collect()),
            2 => {
                let arity = 1 + rng.below(3);
                let tags: Vec<usize> = (0..rows).map(|_| rng.below(arity)).collect();
                let lanes = (0..arity)
                    .map(|t| random_value(rng, tags.iter().filter(|&&x| x == t).count(), depth - 1))
                    .collect();
                Value::sum(tags, lanes)
            }
            3 => {
                let mut ends = Vec::with_capacity(rows);
                let mut total = 0;
                for _ in 0..rows {
                    total += rng.below(3);
                    ends.push(total);
                }
                Value::List(ends.into(), Box::new(random_value(rng, total, depth - 1)))
            }
            4 => Value::List(crate::value::Bounds::Stride(2, rows), Box::new(random_value(rng, rows * 2, depth - 1))),
            _ => Value::Unit(rows),
        }
    }

    /// ragged row ends over `total` elements.
    fn row_ends(rng: &mut Rng, rows: usize, total: usize) -> Vec<usize> {
        let mut cuts: Vec<usize> = (0..rows.saturating_sub(1)).map(|_| rng.below(total + 1)).collect();
        cuts.sort_unstable();
        cuts.push(total);
        cuts
    }

    /// The level walk answers exactly what the batched search answers, on random shapes at every
    /// nesting, with ragged and empty rows on both sides, needles present and absent.
    #[test]
    fn structured_find_matches_the_search_on_random_shapes() {
        use super::super::cmp::sort::sort_values;
        use super::super::cmp::order::segment_labels;
        use super::{find_groups, find_search};
        for seed in 1..200u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let rows = 1 + rng.below(4);
            let (nn, nh) = (rng.below(12), rng.below(24));
            // one draw for both sides, so that they share rows, then each side sorted per row.
            let both = random_value(&mut rng, nn + nh, 3);
            let needles = crate::engine::gather(&both, &(0..nn).collect::<Vec<_>>());
            let hay = crate::engine::gather(&both, &(nn..nn + nh).collect::<Vec<_>>());
            let nb: crate::value::Bounds = row_ends(&mut rng, rows, nn).into();
            let hb: crate::value::Bounds = row_ends(&mut rng, rows, nh).into();
            let (_, _, needles) = sort_values(&segment_labels(&nb), &needles);
            let (_, _, hay) = sort_values(&segment_labels(&hb), &hay);
            let want = find_search(&nb, &needles, &hb, &hay);
            assert_eq!(find_groups(&nb, &needles, &hb, &hay), want, "seed {seed}:\n{}\n{}", crate::value::show(&needles), crate::value::show(&hay));
            // and the op itself takes the merge for these, since the needle rows are in order.
            let out = CmpOp::Find.eval(Value::Prod(vec![Value::List(nb.clone(), Box::new(needles)), Value::List(hb, Box::new(hay))])).unwrap();
            let (_, pair) = out.into_list("find").unwrap();
            let (lo, hi) = pair.into_pair("find").unwrap();
            assert_eq!((lo.into_u64("lo").unwrap(), hi.into_u64("hi").unwrap()), want, "seed {seed}");
        }
    }
}
