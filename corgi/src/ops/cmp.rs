//! The comparison/order op bucket. The leaf compare `Rel` (two columns → mask; `RelImm` when one side
//! is a constant) — plus the list ops `SortBy` (the sort's own output; `sort`, `dedup` and `group`
//! are words over it), `SortLimit`, `Adjacent` and `Find` (a search per needle on leaves, `search`;
//! a batched binary search via `compare_idx` otherwise). All are
//! kind-blind: they read the stored bytes, correct for unsigned and order-preserving signed alike. A
//! flat enum (no sub-graphs); `NumOp` embeds it as the `Cmp` bucket alongside `Core`/`Arith`.
//! The structural-order engine these ops reduce to is the private [`order`] submodule.

pub(crate) mod order;
pub(crate) mod search;
pub(crate) mod sort;
pub(crate) mod survey;

use crate::engine::gather;
use order::{compare_adjacent, compare_cols, compare_idx, equal_cols, segment_labels};
use sort::{sort_blocks, sort_values, sort_values_only};
use crate::shape::{same, shape_of_value};
use crate::value::{Bounds, Scalar, Value};
use search::find_leaf;
use std::hint::select_unpredictable;

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Rel(Pred), // (X, X) -> Int mask   compare row by row in structural order: leaves lane-wise by
               // value, lists/products/sums as `sort` orders them. The mask is 0/1, held as bytes.
    RelImm(Pred, Scalar), // X -> Int mask   `x pred c`, `c` a constant of x's kind
    Min,       // (X, X) -> X   lane-wise minimum, by value
    Max,       // (X, X) -> X   lane-wise maximum
    MinImm(Scalar), // X -> X   lane-wise min with a constant, in place
    MaxImm(Scalar), // X -> X   lane-wise max with a constant
    SortBy,    // List<(K,V)> -> List<(K,V,Int)>   stable order by K alone, V carried along (a Unit
               // V carries nothing), and each element's run of equal keys (numbered densely)
    SortLimit(usize), // List<X> -> List<X>   the first k of each row in structural order (`sort`,
               // then take k), sorting only what can reach the first k: see `sort_limit`
    Adjacent,  // List<X> -> List<Int>   1 where an element differs from the one before it in its
               // row, and at each row's first element: where runs of equal elements start
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
                    // equality needs no order: lists of different lengths differ unread.
                    _ if matches!(pred, Pred::Eq | Pred::Ne) => equal_cols(&a, &b).iter().map(|&o| pred.test(o) as u8).collect(),
                    _ => compare_cols(&a, &b).iter().map(|&o| pred.test(o) as u8).collect(),
                };
                Value::u8(mask)
            }

            CmpOp::Min | CmpOp::Max => {
                let take_max = matches!(self, CmpOp::Max);
                let (a, b) = input.into_pair("min/max")?;
                same(&shape_of_value(&a), &shape_of_value(&b)).map_err(|e| format!("min/max: {e}"))?;
                let (pa, pb) = (a.into_prim("min/max lhs")?, b.into_prim("min/max rhs")?);
                assert_eq!(pa.len(), pb.len(), "min/max: operands at different strata");
                Value::Prim(pa.lane_pick(pb, take_max))
            }

            CmpOp::RelImm(pred, c) => {
                let p = input.into_prim("compare with a constant")?;
                if !c.kind_of(&p) {
                    return Err(format!("compare with a constant: {} against {c:?}", shape_of_value(&Value::Prim(p))));
                }
                Value::u8(p.rel_imm(*c, pred.test(-1), pred.test(0), pred.test(1)))
            }

            CmpOp::MinImm(c) | CmpOp::MaxImm(c) => {
                let p = input.into_prim("min/max with a constant")?;
                if !c.kind_of(&p) {
                    return Err(format!("min/max with a constant: {} against {c:?}", shape_of_value(&Value::Prim(p))));
                }
                Value::Prim(p.pick_imm(*c, matches!(self, CmpOp::MaxImm(..))))
            }

            // the sort moves the keys; the payload follows the permutation, unless there is none.
            CmpOp::SortBy => {
                let (bounds, vals) = input.into_list("SortBy")?;
                let (k, v) = vals.into_pair("SortBy elements")?;
                let labels = row_labels(&bounds);
                let (sk, sv, refined) = if let Value::Unit(n) = v {
                    let (refined, sk) = sort_values_only(&labels, &k);
                    (sk, Value::Unit(n), refined)
                } else {
                    let (perm, refined, sk) = sort_values(&labels, &k);
                    (sk, gather(&v, &perm), refined)
                };
                // the runs the sort found, as it found them: each element's run, numbered densely
                // over the whole column (a run never spans two rows)
                Value::List(bounds, Box::new(Value::Prod(vec![sk, sv, Value::upto(refined.len(), refined.into_iter().map(|r| r as usize))])))
            }

            CmpOp::SortLimit(k) => {
                let (bounds, vals) = input.into_list("SortLimit")?;
                sort_limit(&bounds, &vals, *k)
            }

            // one structural compare of each element with the next, over the whole column; a row's
            // first element starts a run whatever it follows.
            CmpOp::Adjacent => {
                let (bounds, vals) = input.into_list("Adjacent")?;
                let mut mask = vec![1u8; vals.len()];
                for (k, s) in compare_adjacent(&vals).into_iter().enumerate() {
                    mask[k + 1] = (s != 0) as u8;
                }
                for r in 0..bounds.len() {
                    let (s, e) = bounds.span(r);
                    if s < e {
                        mask[s] = 1;
                    }
                }
                Value::List(bounds, Box::new(Value::u8(mask)))
            }

            // for each needle element, equal_range it in the matching haystack row: leaves by a
            // search per needle (`search`), anything else by the batched binary search
            // (`batched_bound`). Output shaped like `needle`, each (lo,hi) relative to its row.
            CmpOp::Find => {
                let (needle, haystack) = input.into_pair("Find")?;
                let (nb, nvals) = needle.into_list("Find needle")?;
                // the haystack may be a referenced list (captured or sliced by reference): rows are
                // read through `span`, and the search indexes the payload absolutely, so no copy.
                let (hb, hvals) = haystack.rows_of("Find haystack")?;
                same(&shape_of_value(&nvals), &shape_of_value(hvals)).map_err(|e| format!("Find: {e}"))?;
                assert_eq!(nb.len(), hb.len(), "Find: needle/haystack row count");
                // Leaves: a search per needle (a walk for a dense row of needles in order; a
                // branch-free binary search, sixteen needles at a time, otherwise). See `search`.
                if let Some((lo_c, hi_c)) = find_leaf(&nb, &nvals, hb, hvals) {
                    return Ok(Value::List(nb, Box::new(Value::Prod(vec![Value::i64(lo_c), Value::i64(hi_c)]))));
                }
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
                batched_bound(hvals, &nvals, &mut lower.0, &mut lower.1, |o| o < 0);
                batched_bound(hvals, &nvals, &mut upper.0, &mut upper.1, |o| o <= 0);
                // row-relative: subtract each element's haystack row start, rewalked here.
                let (mut lo_c, mut hi_c) = (Vec::with_capacity(n), Vec::with_capacity(n));
                for r in 0..nb.len() {
                    let (ns, ne) = nb.span(r);
                    let (hs, _) = hb.span(r);
                    for k in ns..ne {
                        lo_c.push((lower.0[k] - hs) as i64);
                        hi_c.push((upper.0[k] - hs) as i64);
                    }
                }
                Value::List(nb, Box::new(Value::Prod(vec![Value::i64(lo_c), Value::i64(hi_c)])))
            }
        })
    }

}

/// one level of the structural order of a column's rows: a column sorted whole, or a list's elements,
/// position by position (each position its own level, made only for the rows still in play).
enum Level {
    Col(Value),
    Elems(Value),
}

/// the levels whose lexicographic order is the structural order of `v`'s rows, most significant
/// first: a product's fields' in turn, a list's elements by position and then its length (lists
/// order lexicographically), anything else itself.
fn order_levels(v: &Value, out: &mut Vec<Level>) {
    match v {
        Value::Prod(fields) if !fields.is_empty() => fields.iter().for_each(|f| order_levels(f, out)),
        Value::List(inner, vals) => {
            // the elements by position, a row past its end reading zero (the least value of any
            // shape), then the length: a proper prefix ties its padded rows and comes first.
            out.push(Level::Elems(v.clone()));
            out.push(Level::Col(Value::upto(vals.len(), (0..inner.len()).map(|i| { let (s, e) = inner.span(i); e - s }))));
        }
        other => out.push(Level::Col(other.clone())),
    }
}

/// the rows still in play, in order so far: `idx` (rows of the column), `labels` (equal where the
/// levels so far tie), `ends` (each row's end in `idx`).
struct InPlay {
    idx: Vec<usize>,
    labels: Vec<u64>,
    ends: Vec<usize>,
}

impl InPlay {
    /// sort the rows in play by `col` (one value per row in play) within their ties, then keep each
    /// row's first `k` and the whole run of ties at the `k`-th.
    fn level(&mut self, col: &Value, k: usize) {
        let (perm, refined) = sort_blocks(&self.labels, col);
        let sorted: Vec<usize> = perm.iter().map(|&p| self.idx[p]).collect();
        let (mut keep, mut lab, mut ends) = (Vec::new(), Vec::new(), Vec::with_capacity(self.ends.len()));
        let mut s = 0;
        for &e in &self.ends {
            let mut stop = e.min(s + k);
            while stop > s && stop < e && refined[stop] == refined[stop - 1] {
                stop += 1; // the run of ties at the k-th position comes whole
            }
            keep.extend_from_slice(&sorted[s..stop]);
            lab.extend_from_slice(&refined[s..stop]);
            ends.push(keep.len());
            s = e;
        }
        (self.idx, self.labels, self.ends) = (keep, lab, ends);
    }
    /// no ties left: later levels cannot change the order.
    fn settled(&self) -> bool {
        if self.labels.is_empty() {
            return self.idx.len() <= 1; // no labels: one block
        }
        self.labels.windows(2).all(|w| w[0] != w[1])
    }
    /// positions `p - 1` and `p` are in one block.
    fn tied(&self, p: usize) -> bool {
        self.labels.is_empty() || self.labels[p] == self.labels[p - 1]
    }
}

/// `sort` then the first `k` of each row, sorting only what can still reach the first `k`. The order's
/// levels (see `order_levels`) are sorted one at a time, each within the ties the levels before it
/// left. After each level a row keeps its first `k` positions and the whole run of ties at the
/// `k`-th: nothing past that run can reach the first `k`, so the later levels sort only what is kept.
/// A list's elements are levels position by position, an MSD radix sort that stops where the rows in
/// play stop tying. Not a word over `sort_by` yet: the levels come from the key's type, which the
/// lowering does not know, and a list key's levels are data (dev/indexed-sort.md).
fn sort_limit(bounds: &Bounds, vals: &Value, k: usize) -> Value {
    let mut levels = Vec::new();
    order_levels(vals, &mut levels);
    let mut play = InPlay { idx: (0..vals.len()).collect(), labels: row_labels(bounds), ends: bounds.ends().collect() };
    for (l, level) in levels.iter().enumerate() {
        if l > 0 && play.settled() {
            break;
        }
        match level {
            Level::Col(c) if l == 0 => play.level(c, k),
            Level::Col(c) => play.level(&gather(c, &play.idx), k),
            Level::Elems(list) => {
                let Value::List(inner, elems) = list else { unreachable!("a list level is a List") };
                // bytes go eight at a time, packed big-endian into one u64 level; a short row pads
                // with zeros, and ties the padding leaves are the length level's to break.
                // Eight bytes every tied run agrees on (a shared prefix) change nothing: no sort.
                if let Ok(bytes) = elems.as_u8("bytes") {
                    for j in (0..).step_by(8) {
                        if play.settled() {
                            break;
                        }
                        let mut any = false;
                        let col: Vec<u64> = play.idx.iter().map(|&r| {
                            let (s, e) = inner.span(r);
                            let mut word = 0u64;
                            for b in 0..8 {
                                let at = s + j + b;
                                word = (word << 8) | if at < e { any = true; bytes[at] as u64 } else { 0 };
                            }
                            word
                        }).collect();
                        if !any {
                            break;
                        }
                        let splits = (1..col.len()).any(|p| play.tied(p) && col[p] != col[p - 1]);
                        if splits {
                            // the words order unsigned: as `i64`s, each with its top bit flipped
                            play.level(&Value::i64(col.iter().map(|&w| (w ^ (1 << 63)) as i64).collect()), k);
                        }
                    }
                    continue;
                }
                for j in 0.. {
                    if play.settled() {
                        break;
                    }
                    // element j of each row in play. A row that has ended comes first (a proper
                    // prefix), so whether a row has element j is a level of its own, made when some
                    // row in play has ended; under it, an ended row's zero ties only with its kind.
                    let (mut any, mut ended) = (false, false);
                    let mut at: Vec<usize> = play.idx.iter().map(|&r| {
                        let (s, e) = inner.span(r);
                        if s + j < e { any = true; s + j } else { ended = true; usize::MAX }
                    }).collect();
                    if !any {
                        break;
                    }
                    if ended {
                        play.level(&Value::u8(at.iter().map(|&p| (p != usize::MAX) as u8).collect()), k);
                        // that level reordered (and may have cut) the rows in play: read them again
                        at = play.idx.iter().map(|&r| {
                            let (s, e) = inner.span(r);
                            if s + j < e { s + j } else { usize::MAX }
                        }).collect();
                    }
                    let col = crate::engine::gather_or_zero(elems, &at).expect("a list's elements have a zero");
                    play.level(&col, k);
                }
            }
        }
    }
    // every level sorted: each row's first k positions are its answer
    let (mut take, mut out_ends, mut s) = (Vec::new(), Vec::with_capacity(play.ends.len()), 0);
    for &e in &play.ends {
        take.extend_from_slice(&play.idx[s..e.min(s + k)]);
        out_ends.push(take.len());
        s = e;
    }
    Value::List(out_ends.into(), Box::new(gather(vals, &take)))
}

/// The labels for a per-row sort: each element its row, or none at all when there is one row.
fn row_labels(bounds: &Bounds) -> Vec<u64> {
    if bounds.len() == 1 { Vec::new() } else { segment_labels(bounds) }
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
        // Branch-free: which way each window moves is as good as random, and a branch on it was
        // the largest single cost of this loop. Every needle is written back, and the cursor
        // advances past it only while its window is open.
        let mut w = 0usize;
        for t in 0..active.len() {
            let (k, mid) = (active[t], mids[t]);
            let right = go_right(ord[t]);
            let (l, h) = (select_unpredictable(right, mid + 1, lo[k]), select_unpredictable(right, hi[k], mid));
            (lo[k], hi[k]) = (l, h);
            active[w] = k;
            w += (l < h) as usize;
        }
        active.truncate(w);
    }
}
