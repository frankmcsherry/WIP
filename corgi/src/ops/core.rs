//! The operator vocabulary. Every *semantic* op is a unary `T0 -> T1` evaluated by
//! `eval`; the input's type carries the shape requirement (no `arity()`). The two
//! structural nodes (`Input`, `Tuple`) are handled by the evaluator, not here.

use crate::engine::{
    expand_ranges, fill, filter_mask, gather, gather_lanes, owner_ids, split_by_mask,
};
use crate::graph::{eval_graph, shape_of, Graph, OpLike};
use crate::shape::{join, shape_of_value, Shape};
use crate::value::Value;

/// the core op vocabulary: structure only — comparison/order is the `cmp` bucket (`ops::cmp`) and
/// arithmetic the `numeric` layer. Generic over `L`, the layer used for body sub-graphs, so a higher layer's
/// `map` bodies can use the higher vocabulary. `Op<L>` is not itself `OpLike` — the layer enum is (e.g.
/// `NumOp`), delegating to these inherent methods.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Op<L> {
    Lit(Value),     // a constant element, filled to the input's length (anchored)
    Cast(u32),      // leaf -> leaf     re-width to N bits (low bytes / zero-pad), kind-blind
    // Tuple ops
    Field(usize),   // (.., X_i, ..) -> X_i
    Broadcast,      // (X, List<Y>)   -> List<(X,Y)>
    Partition,      // (X, U64-mask) -> Sum{zero-rows, nonzero-rows}   (the boolean 2-way; = Branch(2))
    Branch(usize),  // (X, U64-tags) -> Sum{X × n}   data-driven N-way partition: row i -> variant tags[i]
    // List ops
    Transpose,      // List<(X,Y,..)> -> (List<X>, List<Y>, ..)
    Filter,         // (List<X>, List<U64-mask>) -> List<X>
    Slices,         // (List<(lo,hi)>, haystack:List<T>) -> List<List<T>>   materialize each range
    Flatten,        // List<List<X>> -> (List<(lo,hi)>, List<X>)  destructure: ranges + flat values
    Enlist,         // X -> List<X>   each element its own length-1 list (list-monad unit)
    Iota,           // U64 -> List<U64>   per row [0,1,…,n-1] — a List-introducer / in-language data generator
    // Sum ops
    Unwrap,                         // homogeneous Sum -> payload
    Inject(usize, usize),           // X -> Sum{tag:X, ⊥..}  put X in variant `tag` of `arity` lanes; the
                                    // others are ⊥ (empty, shape uncommitted, pinned later by `join`).
    MapList(Box<Graph<L>>),         // closed body on a list's values (in the *layer* L)
    MapSum(Vec<(usize, Graph<L>)>), // closed bodies on chosen sum variants; unlisted variants pass
                                    // through. The Vec breaks the type recursion, so no Box. A variadic
                                    // match — disjoint indices keep the arms independent (the optimizer
                                    // relies on this; `judge` rejects duplicates).
}

impl<L: OpLike> Op<L> {
    pub(crate) fn eval(&self, input: Value) -> Value {
        match self {
            Op::Lit(v) => fill(v, input.len()),

            Op::Field(i) => {
                let mut cols = input.into_prod("Field");
                cols.swap_remove(*i)
            }

            Op::Transpose => {
                let (bounds, vals) = input.into_list("Transpose");
                let cols = vals.into_prod("Transpose values");
                Value::Prod(
                    cols.into_iter()
                        .map(|c| Value::List(bounds.clone(), Box::new(c)))
                        .collect(),
                )
            }

            Op::Broadcast => {
                let (x, list) = input.into_pair("Broadcast");
                let (bounds, y) = list.into_list("Broadcast list");
                let idx = owner_ids(&bounds);
                Value::List(bounds, Box::new(Value::Prod(vec![gather(&x, &idx), y])))
            }

            Op::Cast(bits) => match input {
                Value::Prim(p) => Value::Prim(p.cast(*bits)),
                _ => panic!("Cast: expected a leaf"),
            },

            Op::Filter => {
                let (data, mask) = input.into_pair("Filter");
                let (bounds, vals) = data.into_list("Filter data");
                let (mb, mv) = mask.into_list("Filter mask");
                assert_eq!(bounds, mb, "Filter: data/mask bounds differ");
                let m = mv.into_u64("Filter mask");
                let (idx, nb) = filter_mask(&bounds, &m);
                Value::List(nb, Box::new(gather(&vals, &idx)))
            }

            Op::Partition => {
                let (data, mask) = input.into_pair("Partition");
                let m = mask.into_u64("Partition mask");
                assert_eq!(data.len(), m.len());
                let tags = m.iter().map(|&b| (b != 0) as usize).collect();
                let (idx_f, idx_t) = split_by_mask(&m);
                Value::sum(tags, vec![gather(&data, &idx_f), gather(&data, &idx_t)])
            }

            // N-way partition: the discriminant `tags` routes each row of `data` to its variant. The
            // tags ARE the sum's tag column; each variant gathers its rows in order (so the implicit
            // within-variant offset matches `Value::sum`).
            Op::Branch(n) => {
                let (data, tags_v) = input.into_pair("Branch");
                let tags = tags_v.into_u64("Branch tags");
                assert_eq!(data.len(), tags.len(), "Branch: payload/discriminant length");
                let mut groups: Vec<Vec<usize>> = vec![Vec::new(); *n];
                for (i, &t) in tags.iter().enumerate() {
                    assert!((t as usize) < *n, "Branch: tag {t} out of range (n={n})");
                    groups[t as usize].push(i);
                }
                let variants = groups.iter().map(|idx| gather(&data, idx)).collect();
                Value::sum(tags.iter().map(|&t| t as usize).collect(), variants)
            }

            Op::Unwrap => {
                // each row's payload, read straight from its variant by the carried within-offset —
                // the fused inverse of `Inject` (no `concat(variants)` temporary).
                let (tags, offset, variants) = input.into_sum("Unwrap");
                let refs: Vec<Option<&Value>> = variants.iter().map(|o| o.as_ref()).collect();
                gather_lanes(&refs, &tags, &offset)
            }

            // sum introduction: every row goes to variant `tag` (a constant tag run), the
            // payload column fills that lane, the others are zero-row. The unary dual of `tuple`.
            Op::Inject(tag, arity) => {
                let n = input.len();
                let mut variants = vec![None; *arity];
                variants[*tag] = Some(input);
                Value::sum_opt(vec![*tag; n], variants)
            }

            Op::MapList(body) => {
                let (bounds, inner) = input.into_list("MapList");
                Value::List(bounds, Box::new(eval_graph(body, inner)))
            }

            Op::MapSum(arms) => {
                let (tags, _offset, mut variants) = input.into_sum("MapSum");
                for (k, body) in arms {
                    // take the lane so the body's `Input` owns it (refcount 1 ⇒ in-place). A ⊥ lane
                    // (`None`) has no rows and a deferred shape, so the body can't run on it — leave it
                    // ⊥ (the judge defers its type the same way).
                    let Some(lane) = variants[*k].take() else { continue };
                    let lane_len = lane.len();
                    let res = eval_graph(body, lane);
                    assert_eq!(res.len(), lane_len, "MapSum changed a variant's length");
                    variants[*k] = Some(res);
                }
                Value::sum_opt(tags, variants)
            }

            // materialize: replace each (lo,hi) range with the haystack-row slice it
            // names. List<(lo,hi)> -> List<List<T>>; reuses `gather`. A list-introducer.
            Op::Slices => {
                let (lohi, haystack) = input.into_pair("Slices");
                let (lb, lvals) = lohi.into_list("Slices ranges");
                let (hb, hvals) = haystack.into_list("Slices haystack");
                let (lo, hi) = lvals.into_pair("Slices lo_hi");
                let (lo_c, hi_c) = (lo.into_u64("Slices lo"), hi.into_u64("Slices hi"));
                assert_eq!(lb.len(), hb.len(), "Slices: row count");
                let (idx, inner_bounds) = expand_ranges(&lb, &lo_c, &hi_c, &hb);
                let inner = Value::List(inner_bounds, Box::new(gather(&hvals, &idx)));
                Value::List(lb, Box::new(inner))
            }

            // DESTRUCTURE one list layer: return the per-inner-list ranges (relative to
            // each top row's flattened span) AND the one-level-flattened values. Both
            // outputs are lists at the SAME top stratum, so they bundle as a Prod, and
            // `Slices` is the exact inverse — hence MapList(MapList(b)) == Flatten; b; Slices.
            Op::Flatten => {
                let (ob, inner) = input.into_list("Flatten");
                let (ib, vals) = inner.into_list("Flatten inner");
                let new_ob: Vec<usize> =
                    ob.iter().map(|&e| if e == 0 { 0 } else { ib[e - 1] }).collect();
                let mut lo_c = Vec::with_capacity(ib.len());
                let mut hi_c = Vec::with_capacity(ib.len());
                let mut prev = 0;
                for &e in &ob {
                    let base = if prev == 0 { 0 } else { ib[prev - 1] }; // top row's flat start
                    for kk in prev..e {
                        let g_lo = if kk == 0 { 0 } else { ib[kk - 1] };
                        lo_c.push((g_lo - base) as u64);
                        hi_c.push((ib[kk] - base) as u64);
                    }
                    prev = e;
                }
                let ranges = Value::List(
                    ob,
                    Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])),
                );
                let flat = Value::List(new_ob, Box::new(vals));
                Value::Prod(vec![ranges, flat])
            }

            // wrap each element in its own length-1 list (the list-monad unit). Values
            // unchanged; bounds become [1,2,..,n]. `Flatten` of an `Enlist` is identity.
            Op::Enlist => {
                let n = input.len();
                Value::List((1..=n).collect(), Box::new(input))
            }

            // generate a range per row: element n_i becomes the list [0,1,…,n_i-1]. Cardinality
            // lands inside the new List (SEQ stays 1:1). Lets a program build its own input data.
            Op::Iota => {
                let ns = input.into_u64("Iota");
                let mut bounds = Vec::with_capacity(ns.len());
                let mut vals = Vec::new();
                let mut end = 0usize;
                for &n in &ns {
                    vals.extend(0..n);
                    end += n as usize;
                    bounds.push(end);
                }
                Value::List(bounds, Box::new(Value::u64(vals)))
            }
        }
    }

    /// the type-level shadow of `eval`: given the input's shape, return the output's
    /// shape (or a structural error). Pattern-matches the input exactly like `eval`,
    /// so adding an op means one rule here and one in `eval`. `Input`/`Tuple` are
    /// handled by `graph::shape_of`, the analogue of `eval_graph`.
    pub(crate) fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        let err = |what: &str| Err(format!("{what}, got {input}"));
        Ok(match self {
            Op::Lit(v) => shape_of_value(v),

            Op::Field(i) => match input {
                Prod(ts) if *i < ts.len() => ts[*i].clone(),
                _ => return err(&format!("Field({i}) expects a product with > {i} fields")),
            },

            Op::Transpose => match input {
                List(inner) => match inner.as_ref() {
                    Prod(ts) => Prod(ts.iter().map(|t| List(Box::new(t.clone()))).collect()),
                    _ => return err("Transpose expects List<product>"),
                },
                _ => return err("Transpose expects a list"),
            },

            Op::Broadcast => match input {
                Prod(ts) if ts.len() == 2 => match &ts[1] {
                    List(y) => List(Box::new(Prod(vec![ts[0].clone(), (**y).clone()]))),
                    _ => return err("Broadcast expects (X, List<Y>)"),
                },
                _ => return err("Broadcast expects a pair"),
            },

            Op::Partition => match input {
                Prod(ts) if ts.len() == 2 && ts[1] == Prim(64) => {
                    Sum(vec![Some(ts[0].clone()), Some(ts[0].clone())])
                }
                _ => return err("Partition expects (X, U64-mask)"),
            },

            Op::Branch(n) => match input {
                Prod(ts) if ts.len() == 2 && ts[1] == Prim(64) => Sum(vec![Some(ts[0].clone()); *n]),
                _ => return err("Branch expects (X, U64-tags)"),
            },

            Op::Cast(bits) => match input {
                Prim(_) => Prim(*bits),
                _ => return err("Cast expects a leaf"),
            },

            Op::Filter => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(x), List(m)) if **m == Prim(64) => List(x.clone()),
                    _ => return err("Filter expects (List<X>, List<U64>)"),
                },
                _ => return err("Filter expects a pair"),
            },

            Op::Slices => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(lh), List(t)) if **lh == Prod(vec![Prim(64), Prim(64)]) => {
                        List(Box::new(List(t.clone())))
                    }
                    _ => return err("Slices expects (List<(U64,U64)>, List<T>)"),
                },
                _ => return err("Slices expects a pair"),
            },

            Op::Flatten => match input {
                List(inner) => match inner.as_ref() {
                    List(x) => Prod(vec![List(Box::new(Prod(vec![Prim(64), Prim(64)]))), List(x.clone())]),
                    _ => return err("Flatten expects List<List<X>>"),
                },
                _ => return err("Flatten expects a list"),
            },

            Op::Enlist => List(Box::new(input.clone())),

            Op::Iota => match input {
                Prim(64) => List(Box::new(Prim(64))),
                _ => return err("Iota expects U64"),
            },

            // homogeneous up to `⊥`: fold the committed (`Some`) lanes by `join`, so `⊥` lanes adopt the
            // concrete payload and a genuine clash of two concrete payloads is the type error.
            Op::Unwrap => match input {
                Sum(ts) => {
                    let mut present = ts.iter().flatten();
                    let first = present
                        .next()
                        .ok_or_else(|| format!("Unwrap: sum has no committed variant, got {input}"))?
                        .clone();
                    present.try_fold(first, |a, t| join(&a, t))?
                }
                _ => return err("Unwrap expects a sum"),
            },

            // sum intro: the input lands in variant `tag`; the other `arity - 1` lanes are `⊥` (`None`,
            // shape uncommitted) and adopt their type wherever the sum is later merged.
            Op::Inject(tag, arity) => {
                if *tag >= *arity {
                    return err(&format!("Inject: tag {tag} out of range for arity {arity}"));
                }
                let mut variants = vec![None; *arity];
                variants[*tag] = Some(input.clone());
                Sum(variants)
            }

            Op::MapList(body) => match input {
                List(x) => List(Box::new(shape_of(body, x)?)),
                _ => return err("MapList expects a list"),
            },

            Op::MapSum(arms) => match input {
                Sum(ts) => {
                    let mut out = ts.clone();
                    for (i, (k, body)) in arms.iter().enumerate() {
                        if *k >= ts.len() {
                            return err(&format!("MapSum: no variant {k}"));
                        }
                        // disjoint indices keep the arms independent (so they commute).
                        if arms[..i].iter().any(|(j, _)| j == k) {
                            return err(&format!("MapSum: duplicate variant {k}"));
                        }
                        // a ⊥ (`None`) lane can't be shaped through the body — defer it (stays ⊥).
                        out[*k] = match &ts[*k] {
                            Some(s) => Some(shape_of(body, s)?),
                            None => None,
                        };
                    }
                    Sum(out)
                }
                _ => return err("MapSum expects a sum"),
            },

        })
    }

    pub(crate) fn children(&self) -> Vec<&Graph<L>> {
        match self {
            Op::MapList(b) => vec![b],
            Op::MapSum(arms) => arms.iter().map(|(_, b)| b).collect(),
            _ => Vec::new(),
        }
    }
}
