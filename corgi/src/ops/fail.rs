//! The failure family: failure as ORDINARY DATA in the pure vocabulary.
//!
//! A fallible column is `Fail<T> = Sum{ Ok: T | Err: Unit }` — lane 0 the Ok payload (packed in row
//! order), lane 1 a length-carrying unit (no error payload; the failing-node breadcrumb is deferred).
//! Every op here is a plain `T0 -> T1` that `eval` runs and `judge` types like any other; there is no
//! second evaluator and no second typer. Three kinds of op:
//!
//!   * the `Try*` producers — the checked forms of the lossy kernels (`gather`/`zip`/`chunk`): a row
//!     the kernel would read zeros for, truncate or cut short lands in Err.
//!   * `Lift` (`X -> Fail<X>`, all Ok) and `Squash` (`Fail<Fail<T>> -> Fail<T>`, the monad join).
//!   * the `Hoist*` distributive laws — Fail commuted out through each structural functor:
//!     `HoistProd` `(Fail<A>, Fail<B>, ..) -> Fail<(A, B, ..)>` (a row errs if ANY field errs),
//!     `HoistList` `List<Fail<T>> -> Fail<List<T>>` (a row errs if ANY element errs),
//!     `HoistSum` `Sum{.. Fail<A> ..} -> Fail<Sum{.. A ..}>` (a row errs iff its own lane errs on it).
//!
//! The surface never writes `Lift`/`Squash`/`Hoist*`: [`crate::effect::lower_effects`] inserts them, so a
//! program written against pure values runs on the Ok lane of whatever fails upstream. The layout is
//! what `try` reveals — `try` is the identity on values and a marker to the lowering.

use crate::engine::{gather, index_plan, Owners};
use crate::graph::OpLike;
use crate::shape::shape_of_value;
use crate::value::{Bounds, Prim, Rows, Tags, Value};
use std::sync::Arc;

// --- the representation --------------------------------------------------------------------------

/// build a `Fail<T>` from a per-row error mask and the packed Ok lane (`ok.len()` = the Ok count).
///
/// A mask with no errors set is the CONSTANT assignment — two words, no tag column and no offset
/// column — which is the same value [`lift`] builds and the common case for every producer here.
pub(crate) fn fail(err: &[bool], ok: Value) -> Value {
    if !err.iter().any(|&e| e) {
        debug_assert_eq!(ok.len(), err.len(), "fail: Ok lane length disagrees with the mask");
        return Value::sum_tagged(Tags::Const(0, err.len()), vec![ok, Value::Unit(0)]);
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

/// Does this value have the `Fail<T> = Sum{T | Unit}` shape?
fn is_fail(v: &Value) -> bool {
    matches!(v, Value::Sum(_, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_)))
}

/// did NO row fail? O(1) — the Err lane is a length-carrying `Unit`, so the failure count is a
/// field read, not a mask to materialise and scan. Every consumer below asks this first, because
/// "nothing has failed yet" is the state a fallible pipeline spends most of its time in.
pub(crate) fn no_errors(v: &Value) -> bool {
    is_fail(v) && matches!(v, Value::Sum(_, lanes) if lanes[1].is_empty())
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
        Value::Sum(tags, lanes) if is_fail(&v) => {
            let Value::Unit(errors) = lanes[1] else { unreachable!() };
            let ok = lanes.into_iter().next().unwrap();
            debug_assert_eq!(ok.len() + errors, tags.len(), "Fail: lane lengths disagree with the assignment");
            Ok((tags, ok, errors))
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
    Value::sum_tagged(Tags::Const(0, n), vec![v, Value::Unit(0)])
}

/// `Fail<Fail<T>> -> Fail<T>`: a row is Ok iff Ok at both levels; the inner Ok lane passes through.
pub(crate) fn squash(v: Value) -> Result<Value, String> {
    let (tags, inner, errors) = fail_parts(v, "Squash")?;
    // nothing failed at the outer level: the inner `Fail<T>` already IS the answer, mask and all.
    // Keep even a noncanonical all-Ok column assignment as-is; compaction belongs to producers.
    // A malformed inner shape falls through to `into_fail` for the shared shape error.
    if errors == 0 && is_fail(&inner) {
        return Ok(inner);
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

// --- the checked producers -----------------------------------------------------------------------
//
// ONE shape, factored once (`per_row_try`): a mask pass — descriptor and leaf reads, no data
// movement — then the BASE kernel does the real work. When no row failed (the state a fallible
// pipeline spends most of its time in) the base op runs on the WHOLE input: the mask has proven
// it loses nothing, and its fast paths all apply — TryZip's Ok lane is Zip's zero-copy
// rewrap, TryChunk's is Chunk's descriptor-only re-partition, neither of which the old fused
// loops here could reach. Only when a row HAS failed are the Ok rows gathered out first; the rare
// case pays the extra pass. The Try tier therefore re-implements no kernel: each base op is
// the single implementation of its access pattern, serving both tiers — improving one improves
// both, and a fast path added to a base op (a `Gather` stride path, say) reaches `TryGather` for
// free. (The one exception, test-pinned: `try_gather`'s one-row leaf path keeps the identity
// check that reuses the haystack buffer, which the base one-row path does not attempt.)

/// The `Try*` shape: mask, then the base kernel — whole input when clean, Ok subset
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

/// `(idx:P, haystack:List<T>) -> Fail<P[T]>`: per row, all-or-nothing over its positions (`P` any
/// shape whose leaves are integer positions, as for `Gather`).
pub(crate) fn try_gather<L: OpLike>(input: Value) -> Result<Value, String> {
    let one_row_leaf = {
        let (idx, haystack) = pair_of(&input, "TryGather")?;
        let (hb, hvals) = haystack.rows_of("TryGather haystack")?;
        assert_eq!(idx.len(), hb.len(), "TryGather: indices/haystack row count");
        // the leaf fast path indexes the payload directly, so row 0 must BE the payload (a
        // partition); a referenced haystack takes the row-relative path.
        matches!(idx, Value::List(ib, ivals) if ib.len() == 1 && matches!(**ivals, Value::Prim(Prim::I64(_))))
            && matches!(hvals, Value::Prim(_))
            && matches!(hb, Rows::Part(_))
    };
    if one_row_leaf {
        // One row over a leaf: validate and gather in the index buffer itself (an identity
        // gather reuses the haystack leaf), and recover the uniform `Stride` form of the bounds
        // without another allocation. The one-row primitive gather is the pointer-chase kernel.
        let (idx, haystack) = input.into_pair("TryGather")?;
        let (ib, ivals) = idx.into_list("TryGather indices")?;
        let (hb, hvals) = haystack.into_list("TryGather haystack")?;
        let Value::Prim(p) = &hvals else { unreachable!("checked above") };
        let idxs = ivals.into_words("TryGather indices")?;
        let ib = ib.compact();
        return Ok(match p.gather_words_checked_owned(idxs, hb.end(0)) {
            Some(g) => fail(&[false], Value::List(ib, Box::new(Value::Prim(g)))),
            None => fail(&[true], Value::List(Bounds::offsets(Vec::new()), Box::new(Value::Prim(p.gather(&[]))))),
        });
    }
    // one pass checks each row and resolves its positions, so a clean input is gathered from them
    // without reading the indices again; a failing row takes the general path, which re-reads them.
    let (plan, ok) = {
        let (idx, haystack) = pair_of(&input, "TryGather")?;
        let (hb, _) = haystack.rows_of("TryGather haystack")?;
        let mut ok = vec![true; idx.len()];
        (index_plan(idx, &Owners::Identity, hb, &mut ok)?, ok)
    };
    if !ok.contains(&false) {
        let (_, haystack) = input.into_pair("TryGather")?;
        let (_, hvals) = haystack.rows_of("TryGather haystack")?;
        return Ok(lift(plan.fill(hvals)));
    }
    let err: Vec<bool> = ok.into_iter().map(|o| !o).collect();
    per_row_try(&err, &super::core::Op::<L>::Gather, input)
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

/// `(List<X>, List<Y>) -> Fail<List<(X, Y)>>`: per row, the two inner lists must agree in length.
pub(crate) fn try_zip<L: OpLike>(input: Value) -> Result<Value, String> {
    let mut err = Vec::new();
    {
        let (lx, ly) = pair_of(&input, "TryZip")?;
        let (bx, _) = list_of(lx, "TryZip lhs")?;
        let (by, _) = list_of(ly, "TryZip rhs")?;
        assert_eq!(bx.len(), by.len(), "TryZip: row count");
        // two lists from one source share their bounds (`(xs, xs map f)`), which compares in O(1)
        // and settles every row at once.
        if bx == by {
            return super::core::Op::<L>::Zip.eval(input).map(lift);
        }
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
        Op::Lift | Op::Squash | Op::HoistProd | Op::HoistList | Op::HoistSum(_) | Op::TryGather
            | Op::TryChunk(_) | Op::TryZip
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
        Op::TryGather => try_gather::<L>(input),
        Op::TryChunk(k) => try_chunk::<L>(*k, input),
        Op::TryZip => try_zip::<L>(input),
        _ => unreachable!("not a failure-family op"),
    }
}

#[cfg(test)]
mod tests;
