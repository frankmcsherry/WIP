//! The comparison/order op bucket. Two leaf compares — `Rel` (two columns → mask) and `Gt` (a column
//! vs a constant, the immediate-form sugar) — plus the list ops `SortList`/`DedupList`/`GroupKey`
//! (discrimination via `sort_blocks`/`run_layout`) and `Find` (binary search via `compare2`). All are
//! kind-blind: they read the stored bytes, correct for unsigned and order-preserving signed alike. A
//! flat enum (no sub-graphs); `NumOp` embeds it as the `Cmp` bucket alongside `Core`/`Arith`.

use crate::cmp::{compare2, run_layout, segment_labels, sort_blocks};
use crate::engine::gather;
use crate::shape::Shape;
use crate::value::Value;
use std::cmp::Ordering;

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
    /// does this predicate hold for a lane's `Ordering`?
    fn test(self, o: Ordering) -> bool {
        use Ordering::*;
        match self {
            Pred::Eq => o == Equal,
            Pred::Ne => o != Equal,
            Pred::Lt => o == Less,
            Pred::Le => o != Greater,
            Pred::Gt => o == Greater,
            Pred::Ge => o != Less,
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
    pub fn eval(&self, input: Value) -> Value {
        match self {
            CmpOp::Rel(pred) => {
                let (a, b) = input.into_pair("Rel");
                assert_eq!(a.len(), b.len(), "Rel: operands at different strata");
                let mask = match (&a, &b) {
                    // leaf pair: the vectorized lane compare (SIMD-friendly).
                    (Value::Prim(pa), Value::Prim(pb)) => pa.rel(pb, |o| pred.test(o)),
                    // any other shape: the same STRUCTURAL comparator sort/find use, per row.
                    _ => (0..a.len()).map(|i| pred.test(compare2(&a, i, &b, i)) as u64).collect(),
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
                let mut nb = Vec::with_capacity(bounds.len());
                let mut g = 0;
                for &row_end in &bounds {
                    while g < firsts.len() && firsts[g] < row_end {
                        g += 1;
                    }
                    nb.push(g);
                }
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
                let mut no = Vec::with_capacity(bounds.len());
                let mut g = 0;
                for &row_end in &bounds {
                    while g < firsts.len() && firsts[g] < row_end {
                        g += 1;
                    }
                    no.push(g);
                }
                Value::List(no, Box::new(Value::Prod(vec![keys, inner])))
            }

            // for each needle element, equal_range it in the matching haystack row; output is shaped like
            // `needle`, each (lo,hi) RELATIVE to its haystack row.
            CmpOp::Find => {
                let (needle, haystack) = input.into_pair("Find");
                let (nb, nvals) = needle.into_list("Find needle");
                let (hb, hvals) = haystack.into_list("Find haystack");
                assert_eq!(nb.len(), hb.len(), "Find: needle/haystack row count");
                let mut lo_c = Vec::new();
                let mut hi_c = Vec::new();
                let (mut ns, mut hs) = (0, 0);
                for r in 0..nb.len() {
                    let (ne, he) = (nb[r], hb[r]);
                    for i in ns..ne {
                        // lower = first j in [hs,he) with haystack[j] >= needle[i]
                        let lower = {
                            let (mut a, mut b) = (hs, he);
                            while a < b {
                                let m = (a + b) / 2;
                                if compare2(&hvals, m, &nvals, i) == Ordering::Less {
                                    a = m + 1;
                                } else {
                                    b = m;
                                }
                            }
                            a
                        };
                        // upper = first j with haystack[j] > needle[i]
                        let upper = {
                            let (mut a, mut b) = (hs, he);
                            while a < b {
                                let m = (a + b) / 2;
                                if compare2(&hvals, m, &nvals, i) != Ordering::Greater {
                                    a = m + 1;
                                } else {
                                    b = m;
                                }
                            }
                            a
                        };
                        lo_c.push((lower - hs) as u64);
                        hi_c.push((upper - hs) as u64);
                    }
                    ns = ne;
                    hs = he;
                }
                Value::List(nb, Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])))
            }
        }
    }

    /// the type-level shadow of `eval` — kind-blind, structural, exactly like `core::Op::judge`.
    pub fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        let err = |what: &str| Err(format!("{what}, got {input}"));
        Ok(match self {
            CmpOp::Rel(_) => match input {
                Prod(ts) if ts.len() == 2 && ts[0] == ts[1] => Prim(64),
                _ => return err("Rel expects (X, X) of equal shape"),
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
                    (List(a), List(b)) if a == b => List(Box::new(Prod(vec![Prim(64), Prim(64)]))),
                    (List(_), List(_)) => return err("Find needle/haystack element types differ"),
                    _ => return err("Find expects (List<X>, List<X>)"),
                },
                _ => return err("Find expects a pair"),
            },
        })
    }
}
