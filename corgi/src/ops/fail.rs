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
use crate::shape::shape_of_value;
use crate::value::{Bounds, Prim, Tags, Value};
use std::sync::Arc;

// --- the representation --------------------------------------------------------------------------

/// build a `Fail<T>` from a per-row error mask and the packed Ok lane (`ok.len()` = the Ok count).
///
/// A mask with no errors set is the CONSTANT assignment — two words, no tag column and no offset
/// column — which is the same value [`lift`] builds and the common case for every producer here.
pub(crate) fn fail(err: &[bool], ok: Value) -> Value {
    if !err.iter().any(|&e| e) {
        debug_assert_eq!(ok.len(), err.len(), "fail: Ok lane length disagrees with the mask");
        return Value::sum_tagged(Tags::constant(0, err.len()), vec![ok, Value::Unit(0)]);
    }
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
    Value::sum_tagged(Tags::column(Prim::U8(Arc::new(tags)), off), vec![ok, Value::Unit(n_err)])
}

/// did NO row fail? O(1) — the Err lane is a length-carrying `Unit`, so the failure count is a
/// field read, not a mask to materialise and scan. Every consumer below asks this first, because
/// "nothing has failed yet" is the state a fallible pipeline spends most of its time in.
pub(crate) fn no_errors(v: &Value) -> bool {
    matches!(v, Value::Sum(_, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(0)))
}

/// destructure a `Fail<T>` into its error mask and packed Ok lane; anything else is the shape error.
pub(crate) fn into_fail(v: Value, who: &str) -> Result<(Vec<bool>, Value), String> {
    match v {
        Value::Sum(tags, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_)) => {
            let err = tags.tags_iter().map(|t| t != 0).collect();
            Ok((err, lanes.into_iter().next().unwrap()))
        }
        other => Err(format!("{who}: expected a Fail (Sum{{T | Unit}}), got {}", shape_of_value(&other))),
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

/// `X -> Fail<X>`: every row Ok. Two words: the assignment is constant, so neither the tag column
/// nor the offset column exists. `lower_effects` inserts one of these per pure field of a mixed
/// tuple and per element of a fallible fold body — the latter once per ROUND — so its cost is the
/// difference between free and 9 bytes a row each time.
pub(crate) fn lift(v: Value) -> Value {
    let n = v.len();
    Value::sum_tagged(Tags::constant(0, n), vec![v, Value::Unit(0)])
}

/// `Fail<Fail<T>> -> Fail<T>`: a row is Ok iff Ok at both levels; the inner Ok lane passes through.
pub(crate) fn squash(v: Value) -> Result<Value, String> {
    // nothing failed at the outer level: the inner `Fail<T>` already IS the answer, mask and all.
    if no_errors(&v) {
        let (_, inner) = into_fail(v, "Squash")?;
        return into_fail(inner, "Squash inner").map(|(e, ok)| fail(&e, ok));
    }
    let (outer_err, inner) = into_fail(v, "Squash")?;
    // nothing failed at the inner level: the result's mask is the outer's, unchanged.
    if no_errors(&inner) {
        let (_, ok) = into_fail(inner, "Squash inner")?;
        return Ok(fail(&outer_err, ok));
    }
    let (inner_err, ok) = into_fail(inner, "Squash inner")?;
    let mut inner = inner_err.iter();
    let err: Vec<bool> = outer_err.iter().map(|&oe| oe || *inner.next().unwrap()).collect();
    Ok(fail(&err, ok))
}

// --- the distributive laws -----------------------------------------------------------------------

/// `(Fail<A>, Fail<B>, ..) -> Fail<(A, B, ..)>`: a row errs if ANY field errs; the survivors carry
/// the product of the fields' Ok values (each field's packed lane read at the survivor's rank).
pub(crate) fn hoist_prod(input: Value) -> Result<Value, String> {
    // no field failed anywhere: the product of the Ok lanes is the answer, all rows Ok. This is
    // the shape `lower_effects` emits for a tuple whose fields are all `Lift`s, and for every
    // round of a fallible fold body in which nothing has yet gone wrong.
    if let Value::Prod(fs) = &input {
        if fs.iter().all(no_errors) {
            let oks: Result<Vec<Value>, String> = input
                .into_prod("HoistProd")?
                .into_iter()
                .map(|f| into_fail(f, "HoistProd field").map(|(_, ok)| ok))
                .collect();
            return Ok(lift(Value::Prod(oks?)));
        }
    }
    let fields: Vec<(Vec<bool>, Value)> = input
        .into_prod("HoistProd")?
        .into_iter()
        .map(|f| into_fail(f, "HoistProd field"))
        .collect::<Result<_, _>>()?;
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
    Ok(fail(&err, Value::Prod(cols)))
}

/// `List<Fail<T>> -> Fail<List<T>>`: a row errs if ANY element errs; the survivors carry their whole
/// list of Ok values (consecutive in the packed lane, so an all-Ok column needs no gather).
pub(crate) fn hoist_list(input: Value) -> Result<Value, String> {
    let (bounds, elems) = input.into_list("HoistList")?;
    // no element failed: the list of Ok values is the answer, and asking costs a field read rather
    // than a mask to materialise and scan.
    if no_errors(&elems) {
        let (_, ok) = into_fail(elems, "HoistList element")?;
        return Ok(lift(Value::List(bounds, Box::new(ok))));
    }
    let (elem_err, ok) = into_fail(elems, "HoistList element")?;
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
    Ok(fail(&err, Value::List(ok_bounds.into(), Box::new(gather(&ok, &keep)))))
}

/// `Sum{.. Fail<A> ..} -> Fail<Sum{.. A ..}>` for the lanes listed in `fallible` (the others are pure
/// and pass through): a row errs iff its own lane errs on it; the survivors re-tag over the lanes'
/// Ok values, whose packed order is already the survivors' order.
pub(crate) fn hoist_sum(fallible: &[usize], input: Value) -> Result<Value, String> {
    let (tags, lanes) = input.into_sum("HoistSum")?;
    if let Some(k) = fallible.iter().find(|&&k| k >= lanes.len()) {
        return Err(format!("HoistSum: no lane {k}"));
    }
    let mut errs: Vec<Option<Vec<bool>>> = vec![None; lanes.len()];
    let mut new_lanes: Vec<Value> = Vec::with_capacity(lanes.len());
    for (k, lane) in lanes.into_iter().enumerate() {
        if fallible.contains(&k) {
            let (e, ok) = into_fail(lane, "HoistSum lane")?;
            errs[k] = Some(e);
            new_lanes.push(ok);
        } else {
            new_lanes.push(lane);
        }
    }
    let err: Vec<bool> = (0..tags.len())
        .map(|r| errs[tags.tag_at(r)].as_ref().is_some_and(|e| e[tags.offset_at(r)]))
        .collect();
    let ok_tags: Vec<usize> =
        tags.tags_iter().zip(&err).filter(|(_, &e)| !e).map(|(t, _)| t).collect();
    Ok(fail(&err, Value::sum(ok_tags, new_lanes)))
}

// --- the total per-row producers -----------------------------------------------------------------

/// `(idx:U64, haystack:List<T>) -> Fail<T>`: row r's element `idx[r]`, Err if out of that row's range.
pub(crate) fn try_get(input: Value) -> Result<Value, String> {
    let (idx, haystack) = input.into_pair("TryGet")?;
    let idxs = idx.as_u64("TryGet index")?;
    let (hb, hvals) = haystack.into_list("TryGet haystack")?;
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
    Ok(fail(&err, gather(&hvals, &abs)))
}

/// `(idx:List<U64>, haystack:List<T>) -> Fail<List<T>>`: per row, all-or-nothing over its indices.
pub(crate) fn try_gather(input: Value) -> Result<Value, String> {
    let (idx, haystack) = input.into_pair("TryGather")?;
    let (ib, ivals) = idx.into_list("TryGather indices")?;
    let (hb, hvals) = haystack.into_list("TryGather haystack")?;
    assert_eq!(ib.len(), hb.len(), "TryGather: indices/haystack row count");
    if ib.len() == 1 && hb.len() == 1 {
        if let Value::Prim(p) = &hvals {
            // the one path that CONSUMES the indices: it validates and gathers in that buffer.
            let idxs = ivals.into_u64("TryGather indices")?;
            // One row over a leaf: validate and gather in the index buffer itself (an identity
            // gather reuses the haystack leaf), and recover the uniform `Stride` form of the bounds
            // without another allocation. The one-row primitive gather is the pointer-chase kernel.
            let ib = ib.compact();
            return Ok(match p.gather_u64_checked_owned(idxs, hb.end(0)) {
                Some(g) => fail(&[false], Value::List(ib, Box::new(Value::Prim(g)))),
                None => fail(&[true], Value::List(Bounds::offsets(Vec::new()), Box::new(Value::Prim(p.gather(&[]))))),
            });
        }
    }
    let idxs = ivals.as_u64("TryGather indices")?;
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
    Ok(fail(&err, Value::List(bounds.into(), Box::new(gather(&hvals, &abs)))))
}

/// `(ranges:List<(lo,hi)>, haystack:List<T>) -> Fail<List<List<T>>>`: per row, every range must
/// satisfy `lo <= hi <= rowlen`.
pub(crate) fn try_slices(input: Value) -> Result<Value, String> {
    let (lohi, haystack) = input.into_pair("TrySlices")?;
    let (lb, lvals) = lohi.into_list("TrySlices ranges")?;
    let (hb, hvals) = haystack.into_list("TrySlices haystack")?;
    assert_eq!(lb.len(), hb.len(), "TrySlices: row count");
    let (lo, hi) = lvals.into_pair("TrySlices lo_hi")?;
    let (lo_c, hi_c) = (lo.as_u64("TrySlices lo")?, hi.as_u64("TrySlices hi")?);
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
    Ok(fail(&err, Value::List(outer.into(), Box::new(mats))))
}

/// `(data:List<X>, mask:List<U64>) -> Fail<List<X>>`: per row, data and mask must agree in length.
pub(crate) fn try_filter(input: Value) -> Result<Value, String> {
    let (data, mask) = input.into_pair("TryFilter")?;
    let (db, dvals) = data.into_list("TryFilter data")?;
    let (mb, mvals) = mask.into_list("TryFilter mask")?;
    assert_eq!(db.len(), mb.len(), "TryFilter: row count");
    let m = mvals.as_u64("TryFilter mask")?;
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
    Ok(fail(&err, Value::List(bounds.into(), Box::new(gather(&dvals, &idx)))))
}

/// `List<X> -> Fail<List<List<X>>>`: per row, the length must divide by `k`; Ok rows re-partition into
/// a `Stride(k)` inner list with no value movement beyond the Ok-row gather.
pub(crate) fn try_chunk(k: usize, input: Value) -> Result<Value, String> {
    if k == 0 {
        return Err("TryChunk width must be positive".into());
    }
    let (bounds, vals) = input.into_list("TryChunk")?;
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
    Ok(fail(&err, Value::List(outer.into(), Box::new(inner))))
}

/// `(X, tags:U64) -> Fail<Sum{X × n}>`: the demux; a tag `>= n` errs its row.
pub(crate) fn try_branch(n: usize, input: Value) -> Result<Value, String> {
    let (data, tags_v) = input.into_pair("TryBranch")?;
    let tags = tags_v.as_u64("TryBranch tags")?;
    assert_eq!(data.len(), tags.len(), "TryBranch: payload/discriminant length");
    if n > 256 {
        return Err(format!("TryBranch: arity {n} exceeds the u8 tag width"));
    }
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
    let variants = groups.iter().map(|idx| gather(&data, idx)).collect();
    Ok(fail(&err, Value::sum_tagged(Tags::column(Prim::U8(Arc::new(ok_tags)), ok_off), variants)))
}

/// `(List<X>, List<Y>) -> Fail<List<(X, Y)>>`: per row, the two inner lists must agree in length.
pub(crate) fn try_zip(input: Value) -> Result<Value, String> {
    let (lx, ly) = input.into_pair("TryZip")?;
    let (bx, vx) = lx.into_list("TryZip lhs")?;
    let (by, vy) = ly.into_list("TryZip rhs")?;
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
    Ok(fail(&err, Value::List(ok_outer.into(), Box::new(pairs))))
}

/// is this op one of the family (dispatched to [`eval`] by `Op::eval`)?
pub(crate) fn is_family<L: OpLike>(op: &super::core::Op<L>) -> bool {
    use super::core::Op;
    matches!(
        op,
        Op::Lift | Op::Squash | Op::HoistProd | Op::HoistList | Op::HoistSum(_) | Op::TryGet | Op::TryGather
            | Op::TrySlices | Op::TryFilter | Op::TryChunk(_) | Op::TryBranch(_) | Op::TryZip
    )
}

/// the evals for the family, dispatched from `Op::eval`; `Err` is the shape error, as everywhere.
pub(crate) fn eval<L: OpLike>(op: &super::core::Op<L>, input: Value) -> Result<Value, String> {
    use super::core::Op;
    match op {
        Op::Lift => Ok(lift(input)),
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
        _ => unreachable!("not a failure-family op"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn one_row(idx: Vec<u64>, hay: &Arc<Vec<u64>>) -> Value {
        Value::Prod(vec![
            Value::List(vec![idx.len()].into(), Box::new(Value::u64(idx))),
            Value::List(vec![hay.len()].into(), Box::new(Value::Prim(Prim::U64(hay.clone())))),
        ])
    }

    /// The state a fallible pipeline spends most of its time in: a `Fail` column that CAN fail but
    /// has not. Its assignment is constant, so it carries no discriminant column and no offset
    /// column, and "did anything fail" is a field read rather than a mask to build and scan.
    #[test]
    fn a_fail_that_has_not_failed_carries_no_witness_columns() {
        let hay = Arc::new(vec![10, 20, 30]);
        let ok = try_gather(one_row(vec![2, 0, 1], &hay)).unwrap();
        assert!(no_errors(&ok));
        let Value::Sum(tags, _) = &ok else { panic!("a Fail is a Sum") };
        assert_eq!(tags.const_tag(), Some(0), "no failures: one tag throughout");

        // ...and it survives the plumbing `lower_effects` wraps around it. `Lift` is constant by
        // construction, and hoisting a product of un-failed columns stays constant.
        let lifted = lift(Value::u64(vec![1, 2, 3]));
        assert!(no_errors(&lifted));
        let paired = hoist_prod(Value::Prod(vec![lifted, lift(Value::u64(vec![4, 5, 6]))])).unwrap();
        assert!(no_errors(&paired));
        let Value::Sum(tags, _) = &paired else { panic!("a Fail is a Sum") };
        assert_eq!(tags.const_tag(), Some(0), "hoisting un-failed fields stays constant");
    }

    /// A row that DOES fail forces the general assignment — the two forms have to agree on what
    /// they mean, so the mask read back is the same either way.
    #[test]
    fn a_failure_forces_the_general_assignment() {
        let hay = Arc::new(vec![10, 20, 30]);
        let bad = try_gather(one_row(vec![7], &hay)).unwrap();
        assert!(!no_errors(&bad));
        let (err, _) = into_fail(bad, "t").unwrap();
        assert_eq!(err, vec![true]);
    }

    #[test]
    fn one_row_identity_gather_reuses_the_haystack_leaf() {
        let hay = Arc::new(vec![10, 20, 30]);
        let (err, ok) = into_fail(try_gather(one_row(vec![0, 1, 2], &hay)).unwrap(), "t").unwrap();
        assert_eq!(err, vec![false]);
        let (_, vals) = ok.into_list("identity gather result").unwrap();
        let Prim::U64(out) = vals.into_prim("identity gather result").unwrap() else { panic!("expected U64") };
        assert!(Arc::ptr_eq(&out, &hay));
    }

    #[test]
    fn one_row_u64_gather_discards_a_partially_rewritten_failure() {
        let hay = Arc::new(vec![10, 20, 30]);
        let (err, ok) = into_fail(try_gather(one_row(vec![1, 3, 0], &hay)).unwrap(), "t").unwrap();
        assert_eq!(err, vec![true]);
        assert_eq!(ok.len(), 0);
        assert_eq!(hay.as_slice(), &[10, 20, 30]);
    }

    #[test]
    fn one_row_nonidentity_u64_gather_returns_values_and_normalizes_bounds() {
        let hay = Arc::new(vec![10, 20, 30]);
        let input = Value::Prod(vec![
            Value::List(Bounds::offsets(vec![3]), Box::new(Value::u64(vec![2, 0, 1]))),
            Value::List(vec![3].into(), Box::new(Value::Prim(Prim::U64(hay.clone())))),
        ]);
        let (err, ok) = into_fail(try_gather(input).unwrap(), "t").unwrap();
        assert_eq!(err, vec![false]);
        let (bounds, vals) = ok.into_list("nonidentity gather result").unwrap();
        assert_eq!(bounds.strided(), Some(3));
        let Prim::U64(out) = vals.into_prim("nonidentity gather result").unwrap() else { panic!("expected U64") };
        assert_eq!(out.as_slice(), &[30, 10, 20]);
        assert!(!Arc::ptr_eq(&out, &hay));
    }

    #[test]
    #[should_panic(expected = "Gather: index 3 out of row 0's bounds")]
    fn raw_one_row_primitive_gather_still_panics_on_an_invalid_index() {
        use crate::ops::{NumOp, Op};
        use crate::graph::OpLike;
        let _ = NumOp::Core(Op::Gather).eval(Value::Prod(vec![
            Value::List(vec![2].into(), Box::new(Value::u64(vec![0, 3]))),
            Value::List(vec![2].into(), Box::new(Value::u64(vec![10, 20]))),
        ]));
    }
}
