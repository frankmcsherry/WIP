//! The comparison/order op bucket. Two leaf compares — `Rel` (two columns → mask) and `Gt` (a column
//! vs a constant, the immediate-form sugar) — plus the list ops `SortList`/`DedupList`/`GroupKey`
//! (discrimination via `sort_blocks`/`run_layout`) and `Find` (batched binary search via `compare_idx`). All are
//! kind-blind: they read the stored bytes, correct for unsigned and order-preserving signed alike. A
//! flat enum (no sub-graphs); `NumOp` embeds it as the `Cmp` bucket alongside `Core`/`Arith`.
//! The structural-order engine these ops reduce to is the private [`order`] submodule.

mod order;

use crate::engine::gather;
use order::{compare_cols, compare_idx, run_layout, runs_per_row, segment_labels, sort_blocks};
use crate::shape::{join, Shape};
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
    Gt(u64),   // X -> U64 mask    (x > c) as 0/1   — the column-vs-immediate sugar form
    SortList,  // List<X> -> List<X>   structural order
    DedupList, // List<X> -> List<X>   distinct, per row (sorted)
    GroupKey,  // List<(K,V)> -> List<(K, List<V>)>   group by key, per row (sorted)
    Find,      // (needle:List<X>, haystack:List<X>) -> List<(lo,hi)>  equal_range / needle elem
}

impl CmpOp {
    pub(crate) fn eval(&self, input: Value) -> Value {
        match self {
            CmpOp::Rel(pred) => {
                let (a, b) = input.into_pair("Rel");
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

            CmpOp::Gt(c) => {
                let xs = input.into_u64("Gt");
                Value::u64(xs.iter().map(|&x| (x > *c) as u64).collect())
            }

            CmpOp::SortList => {
                let (bounds, vals) = input.into_list("SortList");
                let (perm, _) = sort_blocks(&segment_labels(&bounds), &vals);
                Value::List(bounds, Box::new(gather(&vals, &perm)))
            }

            CmpOp::DedupList => {
                // distinct, per row: discriminate, then keep one representative per run.
                let (bounds, vals) = input.into_list("DedupList");
                let (perm, labels) = sort_blocks(&segment_labels(&bounds), &vals);
                let (_ends, firsts) = run_layout(&labels);
                let idx: Vec<usize> = firsts.iter().map(|&f| perm[f]).collect();
                // outer bounds: cumulative distinct count per row (runs never cross rows).
                let nb = runs_per_row(&bounds, &firsts);
                Value::List(nb, Box::new(gather(&vals, &idx)))
            }

            CmpOp::GroupKey => {
                // group by key, per row: discriminate by K (stable → V keeps order); the
                // K-runs are the groups, and each run's V-span is its inner list.
                let (bounds, vals) = input.into_list("GroupKey");
                let (k_col, v_col) = vals.into_pair("GroupKey values");
                let (perm, klabels) = sort_blocks(&segment_labels(&bounds), &k_col);
                let k_sorted = gather(&k_col, &perm);
                let v_sorted = gather(&v_col, &perm);
                let (ends, firsts) = run_layout(&klabels);
                let keys = gather(&k_sorted, &firsts);
                let inner = Value::List(ends, Box::new(v_sorted));
                // outer bounds: cumulative #groups per row.
                let no = runs_per_row(&bounds, &firsts);
                Value::List(no, Box::new(Value::Prod(vec![keys, inner])))
            }

            // for each needle element, equal_range it in the matching haystack row (batched binary
            // search, see `batched_bound`). Output shaped like `needle`, each (lo,hi) relative to its row.
            CmpOp::Find => {
                let (needle, haystack) = input.into_pair("Find");
                let (nb, nvals) = needle.into_list("Find needle");
                let (hb, hvals) = haystack.into_list("Find haystack");
                assert_eq!(nb.len(), hb.len(), "Find: needle/haystack row count");
                let n = nvals.len();
                // each needle element's haystack-row window [lo,hi) and its row start (row-relative answer).
                let (mut lo, mut hi, mut base) = (vec![0usize; n], vec![0usize; n], vec![0usize; n]);
                let (mut ns, mut hs) = (0, 0);
                for r in 0..nb.len() {
                    let (ne, he) = (nb[r], hb[r]);
                    for k in ns..ne {
                        lo[k] = hs;
                        hi[k] = he;
                        base[k] = hs;
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
                let lo_c: Vec<u64> = lower.0.iter().zip(&base).map(|(&p, &b)| (p - b) as u64).collect();
                let hi_c: Vec<u64> = upper.0.iter().zip(&base).map(|(&p, &b)| (p - b) as u64).collect();
                Value::List(nb, Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])))
            }
        }
    }

    /// the type-level shadow of `eval` — kind-blind, structural, exactly like `core::Op::judge`.
    pub(crate) fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        let err = |what: &str| Err(format!("{what}, got {input}"));
        Ok(match self {
            // operands must JOIN (not equal), mirroring Find: a ⊥-laned operand (e.g. an
            // inject'd probe) compares against a committed one; eval never reads a ⊥ lane
            // (a row's tag only names a committed lane), so the comparator is total here.
            CmpOp::Rel(_) => match input {
                Prod(ts) if ts.len() == 2 => {
                    join(&ts[0], &ts[1]).map_err(|e| format!("Rel: {e}"))?;
                    Prim(64)
                }
                _ => return err("Rel expects a pair of unifiable shapes"),
            },
            CmpOp::Gt(_) => match input {
                Prim(64) => Prim(64),
                _ => return err("Gt expects U64"),
            },
            CmpOp::SortList | CmpOp::DedupList => match input {
                List(_) => input.clone(),
                _ => return err("sort/dedup expects a list"),
            },
            CmpOp::GroupKey => match input {
                List(inner) => match inner.as_ref() {
                    Prod(ts) if ts.len() == 2 => {
                        List(Box::new(Prod(vec![ts[0].clone(), List(Box::new(ts[1].clone()))])))
                    }
                    _ => return err("GroupKey expects List<(K,V)>"),
                },
                _ => return err("GroupKey expects a list"),
            },
            CmpOp::Find => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    // needle/haystack elements must JOIN, not equal: a ⊥ lane (e.g. an inject'd
                    // probe) adopts the committed sibling's shape, as in `Unwrap`. Eval is total
                    // over joined operands — a row's tag only ever names a committed lane, so
                    // `compare_idx` never reads a ⊥ lane.
                    (List(a), List(b)) => {
                        join(a, b).map_err(|e| format!("Find: {e}"))?;
                        List(Box::new(Prod(vec![Prim(64), Prim(64)])))
                    }
                    _ => return err("Find expects (List<X>, List<X>)"),
                },
                _ => return err("Find expects a pair"),
            },
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
    loop {
        // live needles and their probe midpoints; the active list doubles as the needle indices
        // (needle element k lives at index k in `nvals`).
        let (mut active, mut mids) = (Vec::new(), Vec::new());
        for (k, (&l, &h)) in lo.iter().zip(hi.iter()).enumerate() {
            if l < h {
                active.push(k);
                mids.push((l + h) / 2);
            }
        }
        if active.is_empty() {
            break;
        }
        let ord = compare_idx(hvals, nvals, &mids, &active);
        for (t, &k) in active.iter().enumerate() {
            if go_right(ord[t]) {
                lo[k] = mids[t] + 1;
            } else {
                hi[k] = mids[t];
            }
        }
    }
}
