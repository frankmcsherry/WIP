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
    let (tags, ok, _) = fail_parts(v, who)?;
    Ok((tags.tags_iter().map(|t| t != 0).collect(), ok))
}

/// Read the assignment and error count without expanding a row-sized mask. Callers that only
/// need the Ok payload, or can retain an existing assignment, use this instead of `into_fail`.
fn fail_parts(v: Value, who: &str) -> Result<(Tags, Value, usize), String> {
    match v {
        Value::Sum(tags, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_)) => {
            let Value::Unit(errors) = lanes[1] else { unreachable!() };
            Ok((tags, lanes.into_iter().next().unwrap(), errors))
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
    let (tags, inner, errors) = fail_parts(v, "Squash")?;
    // nothing failed at the outer level: the inner `Fail<T>` already IS the answer, mask and all.
    if errors == 0 {
        if matches!(&inner, Value::Sum(_, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_))) {
            return Ok(inner);
        }
        // Preserve shape validation even on an empty/all-Ok input.
        return Err(format!("Squash inner: expected a Fail (Sum{{T | Unit}}), got {}", shape_of_value(&inner)));
    }
    // nothing failed at the inner level: the result's mask is the outer's, unchanged.
    if no_errors(&inner) {
        let (_, ok, _) = fail_parts(inner, "Squash inner")?;
        return Ok(Value::sum_tagged(tags, vec![ok, Value::Unit(errors)]));
    }
    let (inner_err, ok) = into_fail(inner, "Squash inner")?;
    let mut inner = inner_err.iter();
    let err: Vec<bool> = tags.tags_iter().map(|t| t != 0 || *inner.next().unwrap()).collect();
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
                .map(|f| fail_parts(f, "HoistProd field").map(|(_, ok, _)| ok))
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
        let (_, ok, _) = fail_parts(elems, "HoistList element")?;
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
    if fallible.iter().all(|&k| no_errors(&lanes[k])) {
        let lanes = lanes.into_iter().enumerate().map(|(k, lane)| {
            if fallible.contains(&k) {
                fail_parts(lane, "HoistSum lane").map(|(_, ok, _)| ok)
            } else { Ok(lane) }
        }).collect::<Result<_, _>>()?;
        return Ok(lift(Value::sum_tagged(tags, lanes)));
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
//
// ONE shape, factored once (`per_row_try`): a mask pass — descriptor and leaf reads, no data
// movement — then the BASE partial kernel does the real work. When no row failed (the state a
// fallible pipeline spends most of its time in) the base op runs on the WHOLE input: the mask has
// pre-proven its asserts, and its fast paths all apply — TryZip's Ok lane is Zip's zero-copy
// rewrap, TryChunk's is Chunk's descriptor-only re-partition, neither of which the old fused
// loops here could reach. Only when a row HAS failed are the Ok rows gathered out first; the rare
// case pays the extra pass. The Try tier therefore re-implements no kernel: each partial op is
// the single implementation of its access pattern, serving both tiers — improving one improves
// both, and a fast path added to a base op (a `Gather` stride path, say) reaches `TryGather` for
// free. (The one exception, test-pinned: `try_gather`'s one-row leaf path keeps the identity
// check that reuses the haystack buffer, which the base one-row path does not attempt.)

/// The `Try*` shape: mask, then the base partial kernel — whole input when clean, Ok subset
/// otherwise. A fallible op that does not fit this shape is a design smell.
fn per_row_try<L: OpLike>(
    err: &[bool],
    base: &super::core::Op<L>,
    whole: Value,
) -> Result<Value, String> {
    if !err.iter().any(|&e| e) {
        return base.eval(whole).map(lift);
    }
    let keep: Vec<usize> = (0..err.len()).filter(|&r| !err[r]).collect();
    let ok = base.eval(gather(&whole, &keep))?;
    Ok(fail(err, ok))
}

/// borrow a pair's fields (the mask pass reads; `per_row_try` consumes the whole later).
fn pair_of<'a>(v: &'a Value, who: &str) -> Result<(&'a Value, &'a Value), String> {
    match v {
        Value::Prod(fs) if fs.len() == 2 => Ok((&fs[0], &fs[1])),
        other => Err(format!("{who}: expected a pair, got {}", shape_of_value(other))),
    }
}

/// borrow a list's bounds and payload.
fn list_of<'a>(v: &'a Value, who: &str) -> Result<(&'a Bounds, &'a Value), String> {
    match v {
        Value::List(b, vals) => Ok((b, vals)),
        other => Err(format!("{who}: expected a list, got {}", shape_of_value(other))),
    }
}

/// `(idx:U64, haystack:List<T>) -> Fail<T>`: row r's element `idx[r]`, Err if out of that row's range.
pub(crate) fn try_get<L: OpLike>(input: Value) -> Result<Value, String> {
    let mut err = Vec::new();
    {
        let (idx, haystack) = pair_of(&input, "TryGet")?;
        let idxs = idx.as_u64("TryGet index")?;
        let (hb, _) = list_of(haystack, "TryGet haystack")?;
        assert_eq!(idxs.len(), hb.len(), "TryGet: index/haystack row count");
        let mut hs = 0;
        for (r, he) in hb.ends().enumerate() {
            err.push(idxs[r] as usize >= he - hs);
            hs = he;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Get, input)
}

/// `(idx:List<U64>, haystack:List<T>) -> Fail<List<T>>`: per row, all-or-nothing over its indices.
pub(crate) fn try_gather<L: OpLike>(input: Value) -> Result<Value, String> {
    let one_row_leaf = {
        let (idx, haystack) = pair_of(&input, "TryGather")?;
        let (ib, _) = list_of(idx, "TryGather indices")?;
        let (hb, hvals) = list_of(haystack, "TryGather haystack")?;
        assert_eq!(ib.len(), hb.len(), "TryGather: indices/haystack row count");
        ib.len() == 1 && matches!(hvals, Value::Prim(_))
    };
    if one_row_leaf {
        // One row over a leaf: validate and gather in the index buffer itself (an identity
        // gather reuses the haystack leaf), and recover the uniform `Stride` form of the bounds
        // without another allocation. The one-row primitive gather is the pointer-chase kernel.
        let (idx, haystack) = input.into_pair("TryGather")?;
        let (ib, ivals) = idx.into_list("TryGather indices")?;
        let (hb, hvals) = haystack.into_list("TryGather haystack")?;
        let Value::Prim(p) = &hvals else { unreachable!("checked above") };
        let idxs = ivals.into_u64("TryGather indices")?;
        let ib = ib.compact();
        return Ok(match p.gather_u64_checked_owned(idxs, hb.end(0)) {
            Some(g) => fail(&[false], Value::List(ib, Box::new(Value::Prim(g)))),
            None => fail(&[true], Value::List(Bounds::offsets(Vec::new()), Box::new(Value::Prim(p.gather(&[]))))),
        });
    }
    let mut err = Vec::new();
    {
        let (idx, haystack) = pair_of(&input, "TryGather")?;
        let (ib, ivals) = list_of(idx, "TryGather indices")?;
        let (hb, _) = list_of(haystack, "TryGather haystack")?;
        let idxs = ivals.as_u64("TryGather indices")?;
        let (mut is, mut hs) = (0usize, 0usize);
        for r in 0..ib.len() {
            let (ie, he) = (ib.end(r), hb.end(r));
            let rowlen = (he - hs) as u64;
            err.push(!idxs[is..ie].iter().all(|&x| x < rowlen));
            is = ie;
            hs = he;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Gather, input)
}

/// `(ranges:List<(lo,hi)>, haystack:List<T>) -> Fail<List<List<T>>>`: per row, every range must
/// satisfy `lo <= hi <= rowlen`.
pub(crate) fn try_slices<L: OpLike>(input: Value) -> Result<Value, String> {
    let mut err = Vec::new();
    {
        let (lohi, haystack) = pair_of(&input, "TrySlices")?;
        let (lb, lvals) = list_of(lohi, "TrySlices ranges")?;
        let (hb, _) = list_of(haystack, "TrySlices haystack")?;
        assert_eq!(lb.len(), hb.len(), "TrySlices: row count");
        let (lo, hi) = pair_of(lvals, "TrySlices lo_hi")?;
        let (lo_c, hi_c) = (lo.as_u64("TrySlices lo")?, hi.as_u64("TrySlices hi")?);
        let (mut ls, mut hs) = (0usize, 0usize);
        for r in 0..lb.len() {
            let (le, he) = (lb.end(r), hb.end(r));
            let rowlen = he - hs;
            err.push(!(ls..le).all(|k| {
                let (l, h) = (lo_c[k] as usize, hi_c[k] as usize);
                l <= h && h <= rowlen
            }));
            ls = le;
            hs = he;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Slices, input)
}

/// `(data:List<X>, mask:List<U64>) -> Fail<List<X>>`: per row, data and mask must agree in length.
pub(crate) fn try_filter<L: OpLike>(input: Value) -> Result<Value, String> {
    let mut err = Vec::new();
    {
        let (data, mask) = pair_of(&input, "TryFilter")?;
        let (db, _) = list_of(data, "TryFilter data")?;
        let (mb, _) = list_of(mask, "TryFilter mask")?;
        assert_eq!(db.len(), mb.len(), "TryFilter: row count");
        let (mut ds, mut ms) = (0usize, 0usize);
        for r in 0..db.len() {
            let (de, me) = (db.end(r), mb.end(r));
            err.push(de - ds != me - ms);
            ds = de;
            ms = me;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Filter, input)
}

/// `List<X> -> Fail<List<List<X>>>`: per row, the length must divide by `k`.
pub(crate) fn try_chunk<L: OpLike>(k: usize, input: Value) -> Result<Value, String> {
    if k == 0 {
        return Err("TryChunk width must be positive".into());
    }
    let mut err = Vec::new();
    {
        let (bounds, _) = list_of(&input, "TryChunk")?;
        let mut prev = 0;
        for end in bounds.ends() {
            err.push((end - prev) % k != 0);
            prev = end;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Chunk(k), input)
}

/// `(X, tags:U64) -> Fail<Sum{X × n}>`: the demux; a tag `>= n` errs its row.
pub(crate) fn try_branch<L: OpLike>(n: usize, input: Value) -> Result<Value, String> {
    if n > 256 {
        return Err(format!("TryBranch: arity {n} exceeds the u8 tag width"));
    }
    let mut err = Vec::new();
    {
        let (data, tags_v) = pair_of(&input, "TryBranch")?;
        let tags = tags_v.as_u64("TryBranch tags")?;
        assert_eq!(data.len(), tags.len(), "TryBranch: payload/discriminant length");
        err.extend(tags.iter().map(|&t| t as usize >= n));
    }
    per_row_try(&err, &super::core::Op::<L>::Branch(n), input)
}

/// `(List<X>, List<Y>) -> Fail<List<(X, Y)>>`: per row, the two inner lists must agree in length.
pub(crate) fn try_zip<L: OpLike>(input: Value) -> Result<Value, String> {
    let mut err = Vec::new();
    {
        let (lx, ly) = pair_of(&input, "TryZip")?;
        let (bx, _) = list_of(lx, "TryZip lhs")?;
        let (by, _) = list_of(ly, "TryZip rhs")?;
        assert_eq!(bx.len(), by.len(), "TryZip: row count");
        let (mut sx, mut sy) = (0usize, 0usize);
        for r in 0..bx.len() {
            let (ex, ey) = (bx.end(r), by.end(r));
            err.push(ex - sx != ey - sy);
            sx = ex;
            sy = ey;
        }
    }
    per_row_try(&err, &super::core::Op::<L>::Zip, input)
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
        Op::TryGet => try_get::<L>(input),
        Op::TryGather => try_gather::<L>(input),
        Op::TrySlices => try_slices::<L>(input),
        Op::TryFilter => try_filter::<L>(input),
        Op::TryChunk(k) => try_chunk::<L>(*k, input),
        Op::TryBranch(n) => try_branch::<L>(*n, input),
        Op::TryZip => try_zip::<L>(input),
        _ => unreachable!("not a failure-family op"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn squash_matches_rowwise_failures() {
        // Each row succeeds, fails outside, or fails inside. Include empty,
        // all-Ok, all-Err and interleaved columns on both fast and general paths.
        for n in 0..=4 {
            for mut pattern in 0..3usize.pow(n) {
                let mut outer = Vec::new();
                let mut inner = Vec::new();
                let mut errors = Vec::new();
                let mut values = Vec::new();
                for row in 0..n {
                    let state = pattern % 3;
                    pattern /= 3;
                    outer.push(state == 1);
                    if state != 1 { inner.push(state == 2); }
                    errors.push(state != 0);
                    if state == 0 { values.push(row as u64); }
                }
                let ok = Value::u64(values);
                let nested = fail(&outer, fail(&inner, ok.clone()));
                assert_eq!(squash(nested).unwrap(), fail(&errors, ok));
            }
        }
    }

    #[test]
    fn hoist_sum_preserves_pure_lanes_and_packed_order() {
        for mask in 0..8 {
            let err: Vec<_> = (0..3).map(|i| mask & (1 << i) != 0).collect();
            let ok = Value::u64((0..3).filter(|&i| !err[i]).map(|i| 10 + i as u64).collect());
            let pure = Value::u8(vec![20, 21]);
            let input = Value::sum(vec![0, 1, 0, 1, 0], vec![fail(&err, ok.clone()), pure.clone()]);
            let errors = [err[0], false, err[1], false, err[2]];
            let tags = [0, 1, 0, 1, 0].into_iter().zip(errors)
                .filter_map(|(t, e)| (!e).then_some(t)).collect();
            let expected = fail(&errors, Value::sum(tags, vec![ok, pure]));
            assert_eq!(hoist_sum(&[0], input).unwrap(), expected);
        }
        let pure = Value::sum(vec![0, 1, 0], vec![Value::u64(vec![1, 2]), Value::Unit(1)]);
        assert_eq!(hoist_sum(&[], pure.clone()).unwrap(), lift(pure));
        let empty = Value::sum(vec![], vec![lift(Value::u64(vec![])), Value::Unit(0)]);
        let expected = lift(Value::sum(vec![], vec![Value::u64(vec![]), Value::Unit(0)]));
        assert_eq!(hoist_sum(&[0], empty).unwrap(), expected);
    }

    #[test]
    fn fast_paths_keep_shape_errors_even_on_empty_columns() {
        for n in [0, 2] {
            assert!(squash(Value::Unit(n)).unwrap_err().contains("Squash:"));
            assert!(squash(lift(Value::Unit(n))).unwrap_err().contains("Squash inner:"));
            let bad = Value::sum(vec![0; n], vec![Value::Unit(n)]);
            assert!(hoist_sum(&[0], bad.clone()).unwrap_err().contains("HoistSum lane:"));
            assert!(hoist_sum(&[1], bad).unwrap_err().contains("HoistSum: no lane 1"));
        }
    }

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
        let ok = try_gather::<crate::ops::NumOp>(one_row(vec![2, 0, 1], &hay)).unwrap();
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
        let bad = try_gather::<crate::ops::NumOp>(one_row(vec![7], &hay)).unwrap();
        assert!(!no_errors(&bad));
        let (err, _) = into_fail(bad, "t").unwrap();
        assert_eq!(err, vec![true]);
    }

    #[test]
    fn one_row_identity_gather_reuses_the_haystack_leaf() {
        let hay = Arc::new(vec![10, 20, 30]);
        let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(one_row(vec![0, 1, 2], &hay)).unwrap(), "t").unwrap();
        assert_eq!(err, vec![false]);
        let (_, vals) = ok.into_list("identity gather result").unwrap();
        let Prim::U64(out) = vals.into_prim("identity gather result").unwrap() else { panic!("expected U64") };
        assert!(Arc::ptr_eq(&out, &hay));
    }

    #[test]
    fn one_row_u64_gather_discards_a_partially_rewritten_failure() {
        let hay = Arc::new(vec![10, 20, 30]);
        let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(one_row(vec![1, 3, 0], &hay)).unwrap(), "t").unwrap();
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
        let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(input).unwrap(), "t").unwrap();
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
