//! The failure family: partiality as ORDINARY DATA in the pure vocabulary.
//!
//! A fallible column is `Fail<T> = Sum{ Ok: T | Err: Unit }` — lane 0 the Ok payload (packed in row
//! order), lane 1 a length-carrying unit (no error payload; the failing-node breadcrumb is deferred).
//! Every op here is a plain `T0 -> T1` that `eval` runs and `judge` types like any other; there is no
//! second evaluator and no second typer. Three kinds of op:
//!
//!   * the `Try*` producers — the total per-row forms of the partial kernels (`get`/`gather`/`branch`/
//!     `zip`/`slices`/`filter`/`chunk`): a row that would have tripped the kernel's assert lands in Err.
//!   * `Lift` (`X -> Fail<X>`, all Ok) and `Squash` (`Fail<Fail<T>> -> Fail<T>`, the monad join).
//!   * the `Hoist*` distributive laws — Fail commuted out through each structural functor:
//!     `HoistProd` `(Fail<A>, Fail<B>, ..) -> Fail<(A, B, ..)>` (a row errs if ANY field errs),
//!     `HoistList` `List<Fail<T>> -> Fail<List<T>>` (a row errs if ANY element errs),
//!     `HoistSum` `Sum{.. Fail<A> ..} -> Fail<Sum{.. A ..}>` (a row errs iff its own lane errs on it).
//!
//! The surface never writes `Lift`/`Squash`/`Hoist*`: [`crate::effect::lower_effects`] inserts them, so a
//! program written against pure values runs on the Ok lane of whatever fails upstream. The layout is
//! what `try` reveals — `try` is the identity on values and a marker to the totality query.

use crate::engine::gather;
use crate::graph::OpLike;
use crate::shape::Shape;
use crate::value::{Bounds, Prim, Value};
use std::sync::Arc;

// --- the representation --------------------------------------------------------------------------

/// build a `Fail<T>` from a per-row error mask and the packed Ok lane (`ok.len()` = the Ok count).
pub(crate) fn fail(err: &[bool], ok: Value) -> Value {
    let mut tags = Vec::with_capacity(err.len());
    let mut off = Vec::with_capacity(err.len());
    let (mut n_ok, mut n_err) = (0usize, 0usize);
    for &e in err {
        if e {
            tags.push(1u8);
            off.push(n_err);
            n_err += 1;
        } else {
            tags.push(0u8);
            off.push(n_ok);
            n_ok += 1;
        }
    }
    debug_assert_eq!(ok.len(), n_ok, "fail: Ok lane length disagrees with the mask");
    Value::Sum(Prim::U8(Arc::new(tags)), off, vec![Some(ok), Some(Value::Unit(n_err))])
}

/// destructure a `Fail<T>` into its error mask and packed Ok lane.
pub(crate) fn into_fail(v: Value, who: &str) -> (Vec<bool>, Value) {
    match v {
        Value::Sum(Prim::U8(tags), _off, mut lanes) if lanes.len() == 2 => {
            let err = tags.iter().map(|&t| t != 0).collect();
            let ok = lanes[0].take().unwrap_or_else(|| panic!("{who}: Fail with a ⊥ Ok lane"));
            (err, ok)
        }
        _ => panic!("{who}: expected a Fail (Sum{{T | Unit}})"),
    }
}

/// the shape of `Fail<T>`.
pub(crate) fn fail_shape(t: Shape) -> Shape {
    Shape::Sum(vec![Some(t), Some(Shape::Unit)])
}

/// the payload shape of a `Fail<T>` (a two-lane sum whose Err lane is `Unit` or uncommitted).
pub(crate) fn unfail(s: &Shape, who: &str) -> Result<Shape, String> {
    match s {
        Shape::Sum(ls) if ls.len() == 2 && matches!(ls[1], Some(Shape::Unit) | None) => match &ls[0] {
            Some(t) => Ok(t.clone()),
            None => Err(format!("{who}: Fail with a ⊥ Ok lane, got {s}")),
        },
        _ => Err(format!("{who}: expected a Fail (Sum{{T | Unit}}), got {s}")),
    }
}

/// positions in a packed Ok lane of the rows `keep` (each of which must be Ok in `err`).
fn ranks(err: &[bool], keep: &[usize]) -> Vec<usize> {
    let mut out = Vec::with_capacity(keep.len());
    let (mut rank, mut next) = (0usize, 0usize);
    for (r, &e) in err.iter().enumerate() {
        if next < keep.len() && keep[next] == r {
            debug_assert!(!e, "ranks: keeping an Err row");
            out.push(rank);
            next += 1;
        }
        if !e {
            rank += 1;
        }
    }
    out
}

// --- Lift / Squash -------------------------------------------------------------------------------

/// `X -> Fail<X>`: every row Ok.
pub(crate) fn lift(v: Value) -> Value {
    let n = v.len();
    Value::Sum(Prim::U8(Arc::new(vec![0u8; n])), (0..n).collect(), vec![Some(v), Some(Value::Unit(0))])
}

/// `Fail<Fail<T>> -> Fail<T>`: a row is Ok iff Ok at both levels; the inner Ok lane passes through.
pub(crate) fn squash(v: Value) -> Value {
    let (outer_err, inner) = into_fail(v, "Squash");
    let (inner_err, ok) = into_fail(inner, "Squash inner");
    let mut inner = inner_err.iter();
    let err: Vec<bool> = outer_err.iter().map(|&oe| oe || *inner.next().unwrap()).collect();
    fail(&err, ok)
}

// --- the distributive laws -----------------------------------------------------------------------

/// `(Fail<A>, Fail<B>, ..) -> Fail<(A, B, ..)>`: a row errs if ANY field errs; the survivors carry
/// the product of the fields' Ok values (each field's packed lane read at the survivor's rank).
pub(crate) fn hoist_prod(input: Value) -> Value {
    let fields: Vec<(Vec<bool>, Value)> =
        input.into_prod("HoistProd").into_iter().map(|f| into_fail(f, "HoistProd field")).collect();
    let n = fields.first().map_or(0, |(e, _)| e.len());
    let mut err = vec![false; n];
    for (e, _) in &fields {
        for (r, &x) in e.iter().enumerate() {
            err[r] |= x;
        }
    }
    let keep: Vec<usize> = (0..n).filter(|&r| !err[r]).collect();
    let cols = fields
        .into_iter()
        .map(|(e, ok)| if e == err { ok } else { gather(&ok, &ranks(&e, &keep)) }) // no drop: pass through
        .collect();
    fail(&err, Value::Prod(cols))
}

/// `List<Fail<T>> -> Fail<List<T>>`: a row errs if ANY element errs; the survivors carry their whole
/// list of Ok values (consecutive in the packed lane, so an all-Ok column needs no gather).
pub(crate) fn hoist_list(input: Value) -> Value {
    let (bounds, elems) = input.into_list("HoistList");
    let (elem_err, ok) = into_fail(elems, "HoistList element");
    if !elem_err.iter().any(|&e| e) {
        return lift(Value::List(bounds, Box::new(ok)));
    }
    let mut err = Vec::with_capacity(bounds.len());
    let mut keep = Vec::new();
    let mut ok_bounds = Vec::new();
    let (mut rank, mut start) = (0usize, 0usize);
    for end in bounds.ends() {
        let n_ok = elem_err[start..end].iter().filter(|&&e| !e).count();
        if n_ok == end - start {
            err.push(false);
            keep.extend(rank..rank + n_ok);
            ok_bounds.push(keep.len());
        } else {
            err.push(true);
        }
        rank += n_ok;
        start = end;
    }
    fail(&err, Value::List(ok_bounds.into(), Box::new(gather(&ok, &keep))))
}

/// `Sum{.. Fail<A> ..} -> Fail<Sum{.. A ..}>` for the lanes listed in `fallible` (the others are pure
/// and pass through): a row errs iff its own lane errs on it; the survivors re-tag over the lanes'
/// Ok values, whose packed order is already the survivors' order.
pub(crate) fn hoist_sum(fallible: &[usize], input: Value) -> Value {
    let (tags, off, lanes) = input.into_sum("HoistSum");
    let mut errs: Vec<Option<Vec<bool>>> = vec![None; lanes.len()];
    let mut new_lanes: Vec<Option<Value>> = Vec::with_capacity(lanes.len());
    for (k, lane) in lanes.into_iter().enumerate() {
        if fallible.contains(&k) {
            match lane {
                Some(l) => {
                    let (e, ok) = into_fail(l, "HoistSum lane");
                    errs[k] = Some(e);
                    new_lanes.push(Some(ok));
                }
                None => new_lanes.push(None), // a ⊥ lane holds no rows — nothing can err
            }
        } else {
            new_lanes.push(lane);
        }
    }
    let err: Vec<bool> =
        tags.iter().zip(&off).map(|(&t, &o)| errs[t].as_ref().is_some_and(|e| e[o])).collect();
    let ok_tags: Vec<usize> = tags.iter().zip(&err).filter(|(_, &e)| !e).map(|(&t, _)| t).collect();
    fail(&err, Value::sum_opt(ok_tags, new_lanes))
}

// --- the total per-row producers -----------------------------------------------------------------

/// `(idx:U64, haystack:List<T>) -> Fail<T>`: row r's element `idx[r]`, Err if out of that row's range.
pub(crate) fn try_get(input: Value) -> Value {
    let (idx, haystack) = input.into_pair("TryGet");
    let idxs = idx.into_u64("TryGet index");
    let (hb, hvals) = haystack.into_list("TryGet haystack");
    assert_eq!(idxs.len(), hb.len(), "TryGet: index/haystack row count");
    let mut err = Vec::with_capacity(idxs.len());
    let mut abs = Vec::new();
    let mut hs = 0;
    for (r, he) in hb.ends().enumerate() {
        let x = idxs[r] as usize;
        if x < he - hs {
            err.push(false);
            abs.push(hs + x);
        } else {
            err.push(true);
        }
        hs = he;
    }
    fail(&err, gather(&hvals, &abs))
}

/// `(idx:List<U64>, haystack:List<T>) -> Fail<List<T>>`: per row, all-or-nothing over its indices.
pub(crate) fn try_gather(input: Value) -> Value {
    let (idx, haystack) = input.into_pair("TryGather");
    let (ib, ivals) = idx.into_list("TryGather indices");
    let (hb, hvals) = haystack.into_list("TryGather haystack");
    assert_eq!(ib.len(), hb.len(), "TryGather: indices/haystack row count");
    let idxs = ivals.into_u64("TryGather indices");
    let mut err = Vec::with_capacity(ib.len());
    let mut abs = Vec::new();
    let mut bounds = Vec::new();
    let (mut is, mut hs) = (0usize, 0usize);
    for r in 0..ib.len() {
        let (ie, he) = (ib.end(r), hb.end(r));
        let rowlen = he - hs;
        if idxs[is..ie].iter().all(|&x| (x as usize) < rowlen) {
            err.push(false);
            abs.extend(idxs[is..ie].iter().map(|&x| hs + x as usize));
            bounds.push(abs.len());
        } else {
            err.push(true);
        }
        is = ie;
        hs = he;
    }
    fail(&err, Value::List(bounds.into(), Box::new(gather(&hvals, &abs))))
}

/// `(ranges:List<(lo,hi)>, haystack:List<T>) -> Fail<List<List<T>>>`: per row, every range must
/// satisfy `lo <= hi <= rowlen`.
pub(crate) fn try_slices(input: Value) -> Value {
    let (lohi, haystack) = input.into_pair("TrySlices");
    let (lb, lvals) = lohi.into_list("TrySlices ranges");
    let (hb, hvals) = haystack.into_list("TrySlices haystack");
    assert_eq!(lb.len(), hb.len(), "TrySlices: row count");
    let (lo, hi) = lvals.into_pair("TrySlices lo_hi");
    let (lo_c, hi_c) = (lo.into_u64("TrySlices lo"), hi.into_u64("TrySlices hi"));
    let mut err = Vec::with_capacity(lb.len());
    let mut abs = Vec::new();
    let mut inner = Vec::new();
    let mut outer = Vec::new();
    let (mut ls, mut hs) = (0usize, 0usize);
    for r in 0..lb.len() {
        let (le, he) = (lb.end(r), hb.end(r));
        let rowlen = he - hs;
        let row_ok = (ls..le).all(|k| {
            let (l, h) = (lo_c[k] as usize, hi_c[k] as usize);
            l <= h && h <= rowlen
        });
        if row_ok {
            err.push(false);
            for k in ls..le {
                abs.extend((lo_c[k] as usize..hi_c[k] as usize).map(|p| hs + p));
                inner.push(abs.len());
            }
            outer.push(inner.len());
        } else {
            err.push(true);
        }
        ls = le;
        hs = he;
    }
    let mats = Value::List(inner.into(), Box::new(gather(&hvals, &abs)));
    fail(&err, Value::List(outer.into(), Box::new(mats)))
}

/// `(data:List<X>, mask:List<U64>) -> Fail<List<X>>`: per row, data and mask must agree in length.
pub(crate) fn try_filter(input: Value) -> Value {
    let (data, mask) = input.into_pair("TryFilter");
    let (db, dvals) = data.into_list("TryFilter data");
    let (mb, mvals) = mask.into_list("TryFilter mask");
    assert_eq!(db.len(), mb.len(), "TryFilter: row count");
    let m = mvals.into_u64("TryFilter mask");
    let mut err = Vec::with_capacity(db.len());
    let mut idx = Vec::new();
    let mut bounds = Vec::new();
    let (mut ds, mut ms) = (0usize, 0usize);
    for r in 0..db.len() {
        let (de, me) = (db.end(r), mb.end(r));
        if de - ds == me - ms {
            err.push(false);
            idx.extend((0..de - ds).filter(|&k| m[ms + k] != 0).map(|k| ds + k));
            bounds.push(idx.len());
        } else {
            err.push(true);
        }
        ds = de;
        ms = me;
    }
    fail(&err, Value::List(bounds.into(), Box::new(gather(&dvals, &idx))))
}

/// `List<X> -> Fail<List<List<X>>>`: per row, the length must divide by `k`; Ok rows re-partition into
/// a `Stride(k)` inner list with no value movement beyond the Ok-row gather.
pub(crate) fn try_chunk(k: usize, input: Value) -> Value {
    let (bounds, vals) = input.into_list("TryChunk");
    let mut err = Vec::with_capacity(bounds.len());
    let mut keep = Vec::new();
    let mut outer = Vec::new();
    let (mut total_sub, mut prev) = (0usize, 0usize);
    for end in bounds.ends() {
        let len = end - prev;
        if len % k == 0 {
            err.push(false);
            keep.extend(prev..end);
            total_sub += len / k;
            outer.push(total_sub);
        } else {
            err.push(true);
        }
        prev = end;
    }
    let inner = Value::List(Bounds::Stride(k, total_sub), Box::new(gather(&vals, &keep)));
    fail(&err, Value::List(outer.into(), Box::new(inner)))
}

/// `(X, tags:U64) -> Fail<Sum{X × n}>`: the demux; a tag `>= n` errs its row.
pub(crate) fn try_branch(n: usize, input: Value) -> Value {
    let (data, tags_v) = input.into_pair("TryBranch");
    let tags = tags_v.into_u64("TryBranch tags");
    assert_eq!(data.len(), tags.len(), "TryBranch: payload/discriminant length");
    assert!(n <= 256, "TryBranch: arity {n} exceeds the u8 tag width");
    let mut err = Vec::with_capacity(tags.len());
    let (mut ok_tags, mut ok_off) = (Vec::with_capacity(tags.len()), Vec::with_capacity(tags.len()));
    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, &t) in tags.iter().enumerate() {
        let t = t as usize;
        if t < n {
            err.push(false);
            ok_tags.push(t as u8);
            ok_off.push(groups[t].len()); // the within-variant offset: the lane's size on arrival
            groups[t].push(i);
        } else {
            err.push(true);
        }
    }
    let variants = groups.iter().map(|idx| Some(gather(&data, idx))).collect();
    fail(&err, Value::Sum(Prim::U8(Arc::new(ok_tags)), ok_off, variants))
}

/// `(List<X>, List<Y>) -> Fail<List<(X, Y)>>`: per row, the two inner lists must agree in length.
pub(crate) fn try_zip(input: Value) -> Value {
    let (lx, ly) = input.into_pair("TryZip");
    let (bx, vx) = lx.into_list("TryZip lhs");
    let (by, vy) = ly.into_list("TryZip rhs");
    assert_eq!(bx.len(), by.len(), "TryZip: row count");
    let mut err = Vec::with_capacity(bx.len());
    let (mut ok_outer, mut ok_x, mut ok_y) = (Vec::new(), Vec::new(), Vec::new());
    let (mut sx, mut sy) = (0, 0);
    for r in 0..bx.len() {
        let (ex, ey) = (bx.end(r), by.end(r));
        if ex - sx == ey - sy {
            err.push(false);
            ok_x.extend(sx..ex);
            ok_y.extend(sy..ey);
            ok_outer.push(ok_x.len());
        } else {
            err.push(true);
        }
        sx = ex;
        sy = ey;
    }
    let pairs = Value::Prod(vec![gather(&vx, &ok_x), gather(&vy, &ok_y)]);
    fail(&err, Value::List(ok_outer.into(), Box::new(pairs)))
}

// --- the judges ----------------------------------------------------------------------------------

/// the shape rules for the family, in the same order as the evals above. `None` = not a fail-family op.
pub(crate) fn judge<L: OpLike>(op: &super::core::Op<L>, input: &Shape) -> Option<Result<Shape, String>> {
    use super::core::Op;
    use Shape::*;
    let err = |what: &str| Err(format!("{what}, got {input}"));
    Some(match op {
        Op::Lift => Ok(fail_shape(input.clone())),
        Op::Squash => unfail(input, "Squash").and_then(|t| unfail(&t, "Squash inner")).map(fail_shape),
        Op::HoistProd => match input {
            Prod(ts) => ts.iter().map(|t| unfail(t, "HoistProd field")).collect::<Result<Vec<_>, _>>()
                .map(|ts| fail_shape(Prod(ts))),
            _ => err("HoistProd expects a product of Fails"),
        },
        Op::HoistList => match input {
            List(t) => unfail(t, "HoistList element").map(|t| fail_shape(List(Box::new(t)))),
            _ => err("HoistList expects a list of Fails"),
        },
        Op::HoistSum(fallible) => match input {
            Sum(ls) => (|| {
                let mut out = ls.clone();
                for &k in fallible {
                    let Some(lane) = ls.get(k) else { return err(&format!("HoistSum: no lane {k}")) };
                    out[k] = match lane {
                        Some(s) => Some(unfail(s, "HoistSum lane")?),
                        None => None,
                    };
                }
                Ok(fail_shape(Sum(out)))
            })(),
            _ => err("HoistSum expects a sum"),
        },
        Op::TryGet => match input {
            Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                (Prim(64), List(t)) => Ok(fail_shape((**t).clone())),
                _ => err("TryGet expects (U64, List<T>)"),
            },
            _ => err("TryGet expects a pair"),
        },
        Op::TryGather => match input {
            Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                (List(i), List(t)) if **i == Prim(64) => Ok(fail_shape(List(t.clone()))),
                _ => err("TryGather expects (List<U64>, List<T>)"),
            },
            _ => err("TryGather expects a pair"),
        },
        Op::TrySlices => match input {
            Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                (List(lh), List(t)) if **lh == Prod(vec![Prim(64), Prim(64)]) => {
                    Ok(fail_shape(List(Box::new(List(t.clone())))))
                }
                _ => err("TrySlices expects (List<(U64,U64)>, List<T>)"),
            },
            _ => err("TrySlices expects a pair"),
        },
        Op::TryFilter => match input {
            Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                (List(x), List(m)) if **m == Prim(64) => Ok(fail_shape(List(x.clone()))),
                _ => err("TryFilter expects (List<X>, List<U64>)"),
            },
            _ => err("TryFilter expects a pair"),
        },
        Op::TryChunk(k) => match input {
            List(inner) if *k > 0 => Ok(fail_shape(List(Box::new(List(inner.clone()))))),
            List(_) => err("TryChunk width must be positive"),
            _ => err("TryChunk expects a list"),
        },
        Op::TryBranch(n) => match input {
            Prod(ts) if ts.len() == 2 && ts[1] == Prim(64) => {
                if *n > 256 {
                    err(&format!("TryBranch: arity {n} exceeds the u8 tag width"))
                } else {
                    Ok(fail_shape(Sum(vec![Some(ts[0].clone()); *n])))
                }
            }
            _ => err("TryBranch expects (X, U64-tags)"),
        },
        Op::TryZip => match input {
            Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                (List(x), List(y)) => {
                    Ok(fail_shape(List(Box::new(Prod(vec![(**x).clone(), (**y).clone()])))))
                }
                _ => err("TryZip expects (List<X>, List<Y>)"),
            },
            _ => err("TryZip expects a pair of lists"),
        },
        _ => return None,
    })
}

/// the evals for the family, dispatched from `Op::eval`. `None` = not a fail-family op.
pub(crate) fn eval<L: OpLike>(op: &super::core::Op<L>, input: Value) -> Result<Value, Value> {
    use super::core::Op;
    Ok(match op {
        Op::Lift => lift(input),
        Op::Squash => squash(input),
        Op::HoistProd => hoist_prod(input),
        Op::HoistList => hoist_list(input),
        Op::HoistSum(fallible) => hoist_sum(fallible, input),
        Op::TryGet => try_get(input),
        Op::TryGather => try_gather(input),
        Op::TrySlices => try_slices(input),
        Op::TryFilter => try_filter(input),
        Op::TryChunk(k) => try_chunk(*k, input),
        Op::TryBranch(n) => try_branch(*n, input),
        Op::TryZip => try_zip(input),
        _ => return Err(input),
    })
}
