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
use order::{compare_cols, compare_idx, run_layout, runs_per_row, segment_labels};
use sort::{contains_list, sort_blocks, sort_values, sort_values_only};
use crate::shape::{same, shape_of_value};
use crate::value::{Bounds, Prim, Value};

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
                // keys already in order within every row (an output emitted in trie order, a
                // sorted column): the groups are the runs of equal adjacent keys, no sort, and the
                // payload stays where it is. One structural pass over adjacent pairs decides.
                let (keys, ends, firsts, v_sorted) = match sorted_runs(&bounds, &k_col) {
                    Some((firsts, ends)) => (gather(&k_col, &firsts), ends, firsts, v_col),
                    None => {
                        let (keys, ends, firsts, perm) = representatives(&row_labels(&bounds), &k_col, true);
                        (keys, ends, firsts, gather(&v_col, &perm))
                    }
                };
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
                // the haystack may be a referenced list (captured or sliced by reference): rows are
                // read through `span`, and the search indexes the payload absolutely, so no copy.
                let (hb, hvals) = haystack.into_rows("Find haystack")?;
                same(&shape_of_value(&nvals), &shape_of_value(&hvals)).map_err(|e| format!("Find: {e}"))?;
                assert_eq!(nb.len(), hb.len(), "Find: needle/haystack row count");
                let n = nvals.len();
                // each needle element's haystack-row window [lo,hi). The window's start is also the
                // row base the answer is relative to; the search moves `lo`, so the base is rewalked
                // off the bounds at the end rather than kept as a third copy of the same column.
                let (mut lo, mut hi) = (vec![0usize; n], vec![0usize; n]);
                for r in 0..nb.len() {
                    let (ns, ne) = nb.span(r);
                    let (hs, he) = hb.span(r);
                    for k in ns..ne {
                        lo[k] = hs;
                        hi[k] = he;
                    }
                }
                // lower = first haystack pos NOT less than the needle; upper = first GREATER. Same
                // batched search, different tie rule on `haystack[mid] vs needle`.
                let mut lower = (lo.clone(), hi.clone());
                let mut upper = (lo, hi);
                // Leaf keys: sorted needles merge (gallop) from the previous needle's bound; the
                // upper bound is resolved by scanning the equal run, the batched search finishing
                // any long run. Product keys: LAYERED, one field at a time — field 0 narrows every
                // needle's window to its equal class by the same merge, and each later field
                // narrows within that class by the batched search (windows are now per needle and
                // small). Column-independent, width-generic, no tuple is ever compared as a tuple.
                let leaf_fields = leaf_fields(&hvals, &nvals);
                match leaf_fields.as_deref() {
                    Some([(hp, np)]) => {
                        let merged = hp.merge_lower(np, &nb, &mut lower.0, &mut lower.1);
                        if !merged {
                            batched_bound(&hvals, &nvals, &mut lower.0, &mut lower.1, |o| o < 0);
                        }
                        hp.run_ends(np, &lower.0, &mut upper.0, &mut upper.1);
                        batched_bound(&hvals, &nvals, &mut upper.0, &mut upper.1, |o| o <= 0);
                    }
                    Some(fields) => {
                        layered_find(fields, &nb, &mut lower, &mut upper);
                    }
                    None => {
                        batched_bound(&hvals, &nvals, &mut lower.0, &mut lower.1, |o| o < 0);
                        batched_bound(&hvals, &nvals, &mut upper.0, &mut upper.1, |o| o <= 0);
                    }
                }
                // row-relative: subtract each element's haystack row start, rewalked here.
                let (mut lo_c, mut hi_c) = (Vec::with_capacity(n), Vec::with_capacity(n));
                for r in 0..nb.len() {
                    let (ns, ne) = nb.span(r);
                    let (hs, _) = hb.span(r);
                    for k in ns..ne {
                        lo_c.push((lower.0[k] - hs) as u64);
                        hi_c.push((upper.0[k] - hs) as u64);
                    }
                }
                Value::List(nb, Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])))
            }
        })
    }

}

/// The labels for a per-row sort: each element its row, or none at all when there is one row.
/// if `keys` is non-decreasing within every row of `bounds`, its runs of equal keys as
/// `(run starts, run ends)`; `None` as soon as a descent is found.
fn sorted_runs(bounds: &Bounds, keys: &Value) -> Option<(Vec<usize>, Vec<usize>)> {
    let n = keys.len();
    if n == 0 {
        return Some((Vec::new(), Vec::new()));
    }
    // adjacent pairs (k-1, k) for k in 1..n, compared structurally in one pass
    let ia: Vec<usize> = (1..n).collect();
    let ib: Vec<usize> = (0..n - 1).collect();
    let ord = compare_idx(keys, keys, &ia, &ib); // sign of keys[k] vs keys[k-1]
    let mut firsts = Vec::new();
    let mut ends = Vec::new();
    for r in 0..bounds.len() {
        let (s, e) = bounds.span(r);
        if s == e {
            continue;
        }
        firsts.push(s);
        for k in s + 1..e {
            match ord[k - 1] {
                o if o < 0 => return None,
                0 => {}
                _ => {
                    ends.push(k);
                    firsts.push(k);
                }
            }
        }
        ends.push(e);
    }
    Some((firsts, ends))
}

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

/// a leaf key, or a product of leaf keys, as (haystack field, needle field) pairs — the fields a
/// layered search walks. `None` for any other shape (a nested product, a list, a sum), which takes
/// the structural comparator.
fn leaf_fields<'a>(h: &'a Value, n: &'a Value) -> Option<Vec<(&'a Prim, &'a Prim)>> {
    match (h, n) {
        (Value::Prim(hp), Value::Prim(np)) => Some(vec![(hp, np)]),
        (Value::Prod(hc), Value::Prod(nc)) if hc.len() == nc.len() && !hc.is_empty() => hc
            .iter()
            .zip(nc)
            .map(|(a, b)| match (a, b) {
                (Value::Prim(hp), Value::Prim(np)) => Some((hp, np)),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

/// equal-range search of product keys, one field at a time — datatoad's layered intersection.
/// `lower`/`upper` arrive as each needle's haystack-row window and leave as its equal range on the
/// whole key. Field 0 is a galloping merge over the sorted needle rows; every later field is the
/// same merge over the CLASSES the previous fields left equal: needles with the same window are a
/// class, their values on this field are sorted within it (the needles are in structural order), and
/// the class's window is the haystack range equal on the prefix. Each field is one leaf column
/// against one leaf column at whatever width it has; nothing compares a tuple. Needle rows that are
/// not sorted fall back to the per-needle batched search.
fn layered_find(fields: &[(&Prim, &Prim)], nb: &Bounds, lower: &mut (Vec<usize>, Vec<usize>), upper: &mut (Vec<usize>, Vec<usize>)) {
    let n = lower.0.len();
    let mut rows: Bounds = nb.clone();
    for (f, &(hf, nf)) in fields.iter().enumerate() {
        if f > 0 {
            // the classes: maximal runs of needles with the same [lower, upper) window
            let mut ends = Vec::new();
            for k in 0..n {
                if k + 1 == n || lower.0[k + 1] != lower.0[k] || upper.0[k + 1] != upper.0[k] {
                    ends.push(k + 1);
                }
            }
            rows = Bounds::offsets(ends);
            // every needle's window is its class's prefix range
            for k in 0..n {
                lower.1[k] = upper.0[k];
                upper.1[k] = upper.0[k];
                upper.0[k] = lower.0[k];
            }
        }
        if !hf.merge_lower(nf, &rows, &mut lower.0, &mut lower.1) {
            batched_bound_leaf(hf, nf, &mut lower.0, &mut lower.1, &|o| o < 0);
        }
        // the upper bound, once per RUN of equal needles: consecutive needles of one class with the
        // same lower bound have the same value on this field (both sides are sorted), so the run's
        // head searches and the rest copy — the prefix sharing of a trie, on the needle side.
        let mut heads: Vec<usize> = Vec::new();
        let mut run_of: Vec<usize> = vec![0; n];
        let eq_prev = nf.eq_prev();
        let mut row = 0usize; // the class of needle k, walked in order
        for k in 0..n {
            while rows.span(row).1 <= k {
                row += 1;
            }
            let same = k > 0 && eq_prev[k] && rows.span(row).0 <= k - 1;
            if !same {
                heads.push(k);
            }
            run_of[k] = heads.len() - 1;
        }
        // upper window per head: [lower bound, class upper); resolve by scanning the equal run,
        // then the batched search for whatever is left
        let mut active = heads.clone();
        for &k in &heads {
            upper.0[k] = lower.0[k];
        }
        hf.run_ends_at(nf, &heads, &mut upper.0, &mut upper.1);
        active.retain(|&k| upper.0[k] < upper.1[k]);
        hf.batched_bound(nf, &mut upper.0, &mut upper.1, &mut active, &|o| o <= 0);
        for k in 0..n {
            let h = heads[run_of[k]];
            upper.0[k] = upper.0[h];
        }
    }
}

/// the batched search on one leaf field, every needle keeping its own window
fn batched_bound_leaf(h: &Prim, n: &Prim, lo: &mut [usize], hi: &mut [usize], go_right: &dyn Fn(i8) -> bool) {
    let mut active: Vec<usize> = (0..lo.len()).filter(|&k| lo[k] < hi[k]).collect();
    if active.is_empty() {
        return;
    }
    h.batched_bound(n, lo, hi, &mut active, go_right);
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
    if active.is_empty() {
        return;
    }
    // leaf columns: compare and update in one loop, no per-round comparison vector (that vector
    // was a third of a join-heavy profile: one allocation and fill per round, forty rounds deep).
    if let (Value::Prim(hp), Value::Prim(np)) = (hvals, nvals) {
        hp.batched_bound(np, lo, hi, &mut active, &go_right);
        return;
    }
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
