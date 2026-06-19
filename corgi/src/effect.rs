//! The effect layer — corgi's error model as a TYPE, not a structural tag buried in `Value`.
//!
//! A column lives in one of two statically-known regimes: `Pure(Value)` (no error possible) or
//! `Fail(FailValues)` (a per-row Ok-or-Error). Ops are likewise `Pure` (operate on a `Value`,
//! unchanged from the pure engine) or `Fail` (the total per-row checks — `get`/`gather`/`branch`/
//! `zip`/`filter`/`slices` — which introduce errors).
//!
//! `effect_eval` is the whole error monad in ONE place, as two matches:
//!   1. on the CONTAINER — `Pure` or `Fail`;
//!   2. the op's vectorized eval on a plain `Value` (the whole column when `Pure`, the `Ok` lane when
//!      `Fail`), FOLLOWED — for a `Fail` input — by a re-interleave that merges the op's output back
//!      against the pre-existing error mask. The merge is a row-position routing (pre-Error rows stay
//!      Error; pre-Ok rows take the op's result), so the result is always a FLAT `FailValues` — no
//!      nesting, hence no `JoinFail`. It is fast exactly when it should be: no prior errors -> the
//!      op's output verbatim; no new errors -> place the values, mask untouched.
//!
//! Pure ops never see an error; the pure engine is untouched. Totality is then a syntactic property
//! of the op tags (a program is total iff it produces a `Pure` column), proved by a separate typer.

use crate::engine::gather;
use crate::graph::{Graph, NodeKind, OpLike};
use crate::ops::core::{init_active, scatter};
use crate::ops::{NumOp, Op};
use crate::value::Value;

/// a fallible column: per original row, Ok or Error, with the Ok rows' values packed in row order
/// (`None` = no Ok rows). `Error` carries no payload (the failing-node breadcrumb is deferred).
#[derive(Clone, Debug)]
pub struct FailValues {
    /// per original row: `true` = Error.
    pub err: Vec<bool>,
    /// the Ok rows' values, in row order (`len == err.iter().filter(|e| !**e).count()`); `None` if
    /// every row errored (no values to carry).
    pub ok: Option<Value>,
}

/// a column in the pure or the potentially-errored regime — the regime is part of the type, so a
/// pure op never has to inspect structure to discover it could be looking at an error.
#[derive(Clone, Debug)]
pub enum EffectValues {
    Pure(Value),
    Fail(FailValues),
}

/// does this op introduce errors (a `FailOp`)? The total per-row checks: `get`/`gather`/`branch`/
/// `zip`. (`filter`/`slices` have no total kernel at this base yet — same pattern, added next.)
fn is_fail(op: &NumOp) -> bool {
    matches!(
        op,
        NumOp::Core(
            Op::GetTry
                | Op::Gather
                | Op::TryBranch(_)
                | Op::TryZip
                | Op::Slices
                | Op::Filter
                | Op::Chunk(_)
        )
    )
}

/// run a `FailOp` on a pure `Value`, producing a `FailValues` (err mask + Ok lane). The error-bearing
/// `Sum` the total kernels build is transient — it never escapes this layer.
fn fail_eval(op: &NumOp, v: Value) -> FailValues {
    match op {
        NumOp::Core(Op::Gather) => gather_rows(v), // per-row all-or-nothing (the kernel is per-element)
        NumOp::Core(Op::Slices) => slices_rows(v), // per-row: every (lo,hi) in its haystack row
        NumOp::Core(Op::Filter) => filter_rows(v), // per-row: data/mask lengths agree
        NumOp::Core(Op::Chunk(k)) => chunk_rows(v, *k), // per-row: length divisible by k
        NumOp::Core(Op::TryBranch(_)) => demux(op.eval(v)), // n+1-lane: re-tag 1..=n to the demux Sum
        // generic 2-lane `Sum{ Err:lane0 | Ok:lane1 }` — `get` (GetTry), `zip` (TryZip).
        _ => {
            let (tags, _off, mut lanes) = op.eval(v).into_sum("fail_eval");
            FailValues { err: tags.iter().map(|&t| t == 0).collect(), ok: lanes.remove(1) }
        }
    }
}

/// per-row `gather`: `(idx:List<U64>, haystack:List<T>)` -> for each row, Ok with the gathered list if
/// EVERY index is in range, else Error (all-or-nothing, the row's `?`/traverse).
fn gather_rows(input: Value) -> FailValues {
    let (idx, haystack) = input.into_pair("gather");
    let (ib, ivals) = idx.into_list("gather indices");
    let (hb, hvals) = haystack.into_list("gather haystack");
    let idxs = ivals.into_u64("gather indices");
    let mut err = Vec::with_capacity(ib.len());
    let mut abs = Vec::new(); // absolute haystack positions for the Ok rows' elements
    let mut bounds = Vec::new(); // cumulative element count per Ok row (the Ok list's bounds)
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
    let ok = (!bounds.is_empty()).then(|| Value::List(bounds.into(), Box::new(gather(&hvals, &abs))));
    FailValues { err, ok }
}

/// per-row `slices`: `(ranges:List<(lo,hi)>, haystack:List<T>)` -> for each row, Ok with the
/// materialized `List<List<T>>` if EVERY `(lo,hi)` satisfies `lo <= hi <= rowlen`, else Error.
fn slices_rows(input: Value) -> FailValues {
    let (lohi, haystack) = input.into_pair("slices");
    let (lb, lvals) = lohi.into_list("slices ranges");
    let (hb, hvals) = haystack.into_list("slices haystack");
    let (lo, hi) = lvals.into_pair("slices lo_hi");
    let (lo_c, hi_c) = (lo.into_u64("slices lo"), hi.into_u64("slices hi"));
    let mut err = Vec::with_capacity(lb.len());
    let mut abs = Vec::new(); // absolute haystack positions for the Ok rows' slices
    let mut inner = Vec::new(); // per range, cumulative element count (the inner List bounds)
    let mut outer = Vec::new(); // per Ok row, cumulative range count (the outer List bounds)
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
    let ok = (!outer.is_empty()).then(|| {
        let mats = Value::List(inner.into(), Box::new(gather(&hvals, &abs)));
        Value::List(outer.into(), Box::new(mats))
    });
    FailValues { err, ok }
}

/// per-row `filter`: `(data:List<X>, mask:List<U64>)` -> for each row, Ok with the mask-nonzero
/// elements if the data and mask rows have EQUAL length, else Error.
fn filter_rows(input: Value) -> FailValues {
    let (data, mask) = input.into_pair("filter");
    let (db, dvals) = data.into_list("filter data");
    let (mb, mvals) = mask.into_list("filter mask");
    let m = mvals.into_u64("filter mask");
    let mut err = Vec::with_capacity(db.len());
    let mut idx = Vec::new(); // kept absolute positions in `dvals`
    let mut bounds = Vec::new(); // per Ok row, cumulative kept count
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
    let ok = (!bounds.is_empty()).then(|| Value::List(bounds.into(), Box::new(gather(&dvals, &idx))));
    FailValues { err, ok }
}

/// per-row `chunk`: `List<X> -> List<List<X>>`, partitioning each row into `k`-wide sub-rows. A row
/// whose length isn't divisible by `k` is Error (all-or-nothing); Ok rows re-partition with no value
/// movement (the inner list is a `Stride(k)`), exactly like the pure kernel — over the Ok rows only.
fn chunk_rows(input: Value, k: usize) -> FailValues {
    let (bounds, vals) = input.into_list("chunk");
    let mut err = Vec::with_capacity(bounds.len());
    let mut keep = Vec::new(); // value positions of the Ok rows
    let mut outer = Vec::new(); // per Ok row, cumulative sub-row count
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
    let ok = (!outer.is_empty()).then(|| {
        let inner = Value::List(crate::value::Bounds::Stride(k, total_sub), Box::new(gather(&vals, &keep)));
        Value::List(outer.into(), Box::new(inner))
    });
    FailValues { err, ok }
}

/// project `TryBranch`'s `Sum{ Oob:lane0 | X×n: lanes 1..=n }`: `Oob` rows are Error; the rest re-tag
/// `1..=n` down to `0..n-1`, so the Ok lane is the demux `Sum{X×n}` (a real ADT, no error trace).
fn demux(sum: Value) -> FailValues {
    let (tags, _off, lanes) = sum.into_sum("branch");
    let err = tags.iter().map(|&t| t == 0).collect();
    let ok_tags: Vec<usize> = tags.iter().filter(|&&t| t != 0).map(|&t| t - 1).collect();
    let x_lanes: Vec<Option<Value>> = lanes.into_iter().skip(1).collect();
    let ok = (!ok_tags.is_empty()).then(|| Value::sum_opt(ok_tags, x_lanes));
    FailValues { err, ok }
}

/// the error monad, in one place (see the module header for the two-match structure).
pub fn effect_eval(op: &NumOp, ev: EffectValues) -> EffectValues {
    use EffectValues::*;
    match ev {
        // pure column: run the op; a FailOp turns it fallible, a pure op keeps it pure.
        Pure(v) if is_fail(op) => Fail(fail_eval(op, v)),
        Pure(v) => Pure(op.eval(v)),
        // fallible column: operate on the Ok lane, then re-interleave (FailOp) or just lift (pure op).
        Fail(FailValues { err, ok }) if is_fail(op) => {
            Fail(reinterleave(err, ok.map(|v| fail_eval(op, v))))
        }
        Fail(FailValues { err, ok }) => Fail(FailValues { err, ok: ok.map(|v| op.eval(v)) }),
    }
}

/// merge a `FailOp`'s output (computed over the pre-`Ok` rows, in order) back against the pre-existing
/// error mask: a row is Error if it was already Error OR the op just errored on it; the surviving Ok
/// values are exactly the op's Ok values. Flat — never nests.
fn reinterleave(pre: Vec<bool>, out: Option<FailValues>) -> FailValues {
    let Some(FailValues { err: out_err, ok }) = out else {
        return FailValues { err: pre, ok: None }; // there were no Ok rows to run on
    };
    let mut err = Vec::with_capacity(pre.len());
    let mut j = 0; // index into the pre-Ok rows, which is what `out_err` is indexed by
    for &pe in &pre {
        err.push(pe || { let e = out_err[j]; j += 1; e });
    }
    FailValues { err, ok }
}

/// evaluate a graph over `EffectValues` — the effect-aware sibling of `eval_graph`. `Tuple` composes
/// its fields: all pure -> a pure product; any fallible -> hoist (the applicative `Sequence`).
pub fn effect_eval_graph(g: &Graph<NumOp>, arg: EffectValues) -> EffectValues {
    let mut uses = vec![0usize; g.nodes.len()];
    for node in &g.nodes {
        for &i in &node.inputs {
            uses[i] += 1;
        }
    }
    uses[g.output] += 1;
    let take = |vals: &mut Vec<Option<EffectValues>>, uses: &mut [usize], i: usize| -> EffectValues {
        uses[i] -= 1;
        if uses[i] == 0 { vals[i].take().unwrap() } else { vals[i].as_ref().unwrap().clone() }
    };
    let mut arg = Some(arg);
    let mut vals: Vec<Option<EffectValues>> = Vec::with_capacity(g.nodes.len());
    for node in &g.nodes {
        let v = match &node.kind {
            NodeKind::Input => arg.take().expect("graph has more than one Input node"),
            NodeKind::Tuple => {
                let fields = node.inputs.iter().map(|&i| take(&mut vals, &mut uses, i)).collect();
                hoist_tuple(fields)
            }
            NodeKind::Op(NumOp::Core(Op::MapList(body))) => {
                eval_maplist(body, take(&mut vals, &mut uses, node.inputs[0]))
            }
            NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
                eval_mapsum(arms, take(&mut vals, &mut uses, node.inputs[0]))
            }
            NodeKind::Op(NumOp::Core(Op::Fold(body))) => {
                eval_fold(body, take(&mut vals, &mut uses, node.inputs[0]))
            }
            // FoldScan with a PURE body goes through the generic pure path; a FALLIBLE body would need
            // error-dropping in its per-round output stitch too — same pattern as `eval_fold`, but
            // unexercised, so guarded loudly rather than left to leak a raw Sum through the pure eval.
            NodeKind::Op(op @ NumOp::Core(Op::FoldScan(body))) => {
                assert!(
                    !graph_is_fail(body, false),
                    "fallible FoldScan body: not yet supported by the effect layer (add it like eval_fold)"
                );
                effect_eval(op, take(&mut vals, &mut uses, node.inputs[0]))
            }
            NodeKind::Op(NumOp::Core(Op::Try)) => match take(&mut vals, &mut uses, node.inputs[0]) {
                EffectValues::Fail(fv) => EffectValues::Pure(eval_try(fv)), // reify Fail -> pure Sum
                pure => pure,                                               // TRY on a pure value: no-op
            },
            NodeKind::Op(o) => effect_eval(o, take(&mut vals, &mut uses, node.inputs[0])),
        };
        vals.push(Some(v));
    }
    vals[g.output].take().unwrap()
}

/// the effect-TYPER: whether the graph's output column is in the `Fail` regime, propagated over the
/// op tags exactly as `effect_eval` propagates the regime at runtime. A `FailOp` produces `Fail`; a
/// `Tuple` is `Fail` if any field is; a body-bearing op is `Fail` if its body can fail (sound: bodies
/// run on the pure Ok lane, so they start `Pure`); `Try` discharges to `Pure`; every other op
/// preserves its input's regime. This replaces `Shape::Fail` + the threading pass.
pub fn graph_is_fail(g: &Graph<NumOp>, input_fail: bool) -> bool {
    let mut fail: Vec<bool> = Vec::with_capacity(g.nodes.len());
    for node in &g.nodes {
        let f = match &node.kind {
            NodeKind::Input => input_fail,
            NodeKind::Tuple => node.inputs.iter().any(|&i| fail[i]),
            NodeKind::Op(NumOp::Core(Op::Try)) => false, // TRY always discharges to Pure
            NodeKind::Op(NumOp::Core(Op::MapList(body))) => {
                fail[node.inputs[0]] || graph_is_fail(body, false)
            }
            NodeKind::Op(NumOp::Core(Op::Fold(body) | Op::FoldScan(body))) => {
                fail[node.inputs[0]] || graph_is_fail(body, false)
            }
            NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
                fail[node.inputs[0]] || arms.iter().any(|(_, b)| graph_is_fail(b, false))
            }
            NodeKind::Op(o) if is_fail(o) => true, // a FailOp introduces Fail regardless of input
            NodeKind::Op(_) => fail[node.inputs[0]], // a pure op preserves the regime
        };
        fail.push(f);
    }
    fail[g.output]
}

/// a program is TOTAL iff, on a pure input, its output column types `Pure` — i.e. every `FailOp` it
/// uses is discharged by a `TRY`. Syntactic, read straight off the op tags.
pub fn is_total(g: &Graph<NumOp>) -> bool {
    !graph_is_fail(g, false)
}

/// `TRY`'s semantics: reveal a `Fail` column as a pure `Sum{ T | Unit }` — Ok values at lane 0,
/// error-as-`Unit` at lane 1 — for the program to `match`. The one transition from `Fail` back to
/// `Pure`: it reifies the at-arm's-length effect into ordinary, matchable data.
pub fn eval_try(fv: FailValues) -> Value {
    let n_err = fv.err.iter().filter(|&&e| e).count();
    let tags: Vec<usize> = fv.err.iter().map(|&e| e as usize).collect(); // 0 = Ok, 1 = Err
    Value::sum_opt(tags, vec![fv.ok, Some(Value::Unit(n_err))])
}

/// `MapList` over `EffectValues`: map the body over the list's elements, then — if the body is
/// fallible — `Traverse` the per-element errors up to per-row (all-or-nothing). A pure body is the
/// ordinary map. (A FALLIBLE outer list is deferred — same shape, next increment.)
fn eval_maplist(body: &Graph<NumOp>, input: EffectValues) -> EffectValues {
    match input {
        EffectValues::Pure(v) => {
            let (bounds, elems) = v.into_list("map");
            match effect_eval_graph(body, EffectValues::Pure(elems)) {
                EffectValues::Pure(new) => EffectValues::Pure(Value::List(bounds, Box::new(new))),
                EffectValues::Fail(fv) => EffectValues::Fail(traverse(bounds, fv)),
            }
        }
        // a FALLIBLE outer list: map the body over the Ok rows' lists, keeping the outer errors. A
        // pure body lifts; a fallible body Traverses (per Ok row) then re-interleaves its new errors
        // with the outer mask — the same flat merge, never nested.
        EffectValues::Fail(FailValues { err, ok }) => {
            let Some(list) = ok else {
                return EffectValues::Fail(FailValues { err, ok: None }); // no Ok rows to map
            };
            let (bounds, elems) = list.into_list("map");
            match effect_eval_graph(body, EffectValues::Pure(elems)) {
                EffectValues::Pure(new) => {
                    EffectValues::Fail(FailValues { err, ok: Some(Value::List(bounds, Box::new(new))) })
                }
                EffectValues::Fail(belem) => {
                    EffectValues::Fail(reinterleave(err, Some(traverse(bounds, belem))))
                }
            }
        }
    }
}

/// `MapSum` (match / map_variant) over `EffectValues`: run each listed arm's body over its lane via
/// the effect layer. All arms pure -> the same Sum with mapped lanes. A FALLIBLE arm hoists its
/// errored rows OUT (the Sum sibling of `Traverse`): a row errors iff its arm errored on it, and the
/// Ok lane is the Sum rebuilt over the survivors. A `Fail` input lifts over its Ok lane, then
/// re-interleaves any arm errors with the outer mask.
fn eval_mapsum(arms: &[(usize, Graph<NumOp>)], input: EffectValues) -> EffectValues {
    match input {
        EffectValues::Pure(v) => mapsum_over_sum(arms, v),
        EffectValues::Fail(FailValues { err, ok: None }) => {
            EffectValues::Fail(FailValues { err, ok: None })
        }
        EffectValues::Fail(FailValues { err, ok: Some(v) }) => match mapsum_over_sum(arms, v) {
            EffectValues::Pure(s) => EffectValues::Fail(FailValues { err, ok: Some(s) }),
            EffectValues::Fail(inner) => EffectValues::Fail(reinterleave(err, Some(inner))),
        },
    }
}

/// run the arms over a Sum value (the core of `eval_mapsum`).
fn mapsum_over_sum(arms: &[(usize, Graph<NumOp>)], v: Value) -> EffectValues {
    let (tags, off, lanes) = v.into_sum("map_variant");
    let mut arm: Vec<Option<EffectValues>> = vec![None; lanes.len()]; // run result per listed lane
    for (k, body) in arms {
        if let Some(Some(lane)) = lanes.get(*k) {
            arm[*k] = Some(effect_eval_graph(body, EffectValues::Pure(lane.clone())));
        }
    }
    if !arm.iter().any(|a| matches!(a, Some(EffectValues::Fail(_)))) {
        // every arm pure: rebuild the Sum, each run lane replaced by its mapped value.
        let new: Vec<Option<Value>> = lanes
            .into_iter()
            .enumerate()
            .map(|(k, orig)| match arm[k].take() {
                Some(EffectValues::Pure(w)) => Some(w),
                _ => orig,
            })
            .collect();
        return EffectValues::Pure(Value::sum_opt(tags, new));
    }
    // a fallible arm: a row errors iff its arm errored on it (at its within-lane offset); the Ok lane
    // is the Sum rebuilt over the survivors (a fallible arm's lane becomes its packed Ok values).
    let err: Vec<bool> = tags
        .iter()
        .zip(&off)
        .map(|(&t, &o)| matches!(&arm[t], Some(EffectValues::Fail(fv)) if fv.err[o]))
        .collect();
    let new_tags: Vec<usize> =
        tags.iter().zip(&err).filter(|(_, &e)| !e).map(|(&t, _)| t).collect();
    let new_lanes: Vec<Option<Value>> = lanes
        .into_iter()
        .enumerate()
        .map(|(k, orig)| match arm[k].take() {
            Some(EffectValues::Pure(w)) => Some(w),
            Some(EffectValues::Fail(fv)) => fv.ok,
            None => orig,
        })
        .collect();
    let ok = (!new_tags.is_empty()).then(|| Value::sum_opt(new_tags, new_lanes));
    EffectValues::Fail(FailValues { err, ok })
}

/// `Fold` over `EffectValues`: the same ragged `active`-worklist loop as the pure fold, but the body
/// runs through the effect layer each round. A row whose body errors on some round is dead — it drops
/// from the worklist (no further rounds) and surfaces as a row Error; its accumulator is discarded.
/// All-or-nothing per row, like `Traverse`. A pure body never errors -> the result is `Pure`.
fn eval_fold(body: &Graph<NumOp>, input: EffectValues) -> EffectValues {
    match input {
        EffectValues::Pure(v) => fold_over(body, v),
        EffectValues::Fail(FailValues { err, ok: None }) => {
            EffectValues::Fail(FailValues { err, ok: None })
        }
        EffectValues::Fail(FailValues { err, ok: Some(v) }) => match fold_over(body, v) {
            EffectValues::Pure(r) => EffectValues::Fail(FailValues { err, ok: Some(r) }),
            EffectValues::Fail(inner) => EffectValues::Fail(reinterleave(err, Some(inner))),
        },
    }
}

/// the effect-aware fold loop (the core of `eval_fold`); mirrors `Op::Fold`'s eval round-for-round.
fn fold_over(body: &Graph<NumOp>, v: Value) -> EffectValues {
    let (seed, list) = v.into_pair("fold");
    let (bounds, vals) = list.into_list("fold list");
    let n = seed.len();
    let mut acc = seed;
    let mut err = vec![false; n]; // per row: errored on some round
    let mut active = init_active(&bounds);
    let (mut t, mut any_fail) = (0usize, false);
    while !active.is_empty() {
        let rows: Vec<usize> = active.iter().map(|&(r, _, _)| r).collect();
        let elem: Vec<usize> = active.iter().map(|&(_, s, _)| s + t).collect();
        let acc_active = gather(&acc, &rows);
        let elt = gather(&vals, &elem);
        match effect_eval_graph(body, EffectValues::Pure(Value::Prod(vec![acc_active, elt]))) {
            EffectValues::Pure(updated) => acc = scatter(acc, &rows, updated),
            EffectValues::Fail(FailValues { err: berr, ok: bok }) => {
                any_fail = true;
                let ok_rows: Vec<usize> =
                    rows.iter().zip(&berr).filter(|(_, &e)| !e).map(|(&r, _)| r).collect();
                for (&r, &e) in rows.iter().zip(&berr) {
                    if e {
                        err[r] = true; // this row is dead from here on
                    }
                }
                if let Some(bok) = bok {
                    acc = scatter(acc, &ok_rows, bok);
                }
            }
        }
        t += 1;
        active.retain(|&(r, _, len)| !err[r] && len > t); // drop dead rows and rows that ran out
    }
    if !any_fail {
        return EffectValues::Pure(acc);
    }
    let ok_rows: Vec<usize> = (0..n).filter(|&r| !err[r]).collect();
    let ok = (!ok_rows.is_empty()).then(|| gather(&acc, &ok_rows));
    EffectValues::Fail(FailValues { err, ok })
}

/// the applicative `Traverse` for lists: hoist a body's PER-ELEMENT errors to PER-ROW. A row (one
/// inner list) is Ok iff every element is Ok; surviving rows carry their full list of Ok values. The
/// list sibling of `hoist_tuple` — where fallibility that lands INSIDE a list gets pushed out to the
/// row, since the two-container model has no `Fail` buried in a `Value`.
fn traverse(bounds: crate::value::Bounds, fv: FailValues) -> FailValues {
    let FailValues { err: elem_err, ok: elem_ok } = fv;
    let mut row_err = Vec::with_capacity(bounds.len());
    let mut keep = Vec::new(); // positions into elem_ok kept (the fully-Ok rows' elements)
    let mut ok_bounds = Vec::new();
    let (mut rank, mut start) = (0usize, 0usize); // rank = count of Ok elements before the current row
    for r in 0..bounds.len() {
        let end = bounds.end(r);
        let n_ok = elem_err[start..end].iter().filter(|&&e| !e).count();
        if n_ok == end - start {
            // a fully-Ok row: its elements are consecutive in `elem_ok` at rank..rank+rowlen.
            row_err.push(false);
            keep.extend(rank..rank + (end - start));
            ok_bounds.push(keep.len());
        } else {
            row_err.push(true);
        }
        rank += n_ok;
        start = end;
    }
    let ok = match elem_ok {
        Some(v) if !ok_bounds.is_empty() => {
            Some(Value::List(ok_bounds.into(), Box::new(gather(&v, &keep))))
        }
        _ => None,
    };
    FailValues { err: row_err, ok }
}

/// the row count of a field (every field of a tuple shares it).
fn field_len(f: &EffectValues) -> usize {
    match f {
        EffectValues::Pure(v) => v.len(),
        EffectValues::Fail(fv) => fv.err.len(),
    }
}

/// compose a tuple of `EffectValues`: all pure -> a pure product (the common case); any fallible ->
/// the applicative `Sequence` — a row errors if ANY field does, and the surviving rows (where EVERY
/// field is Ok) carry the product of the fields' Ok values. The multi-field analog of `reinterleave`.
fn hoist_tuple(fields: Vec<EffectValues>) -> EffectValues {
    use EffectValues::*;
    if fields.iter().all(|f| matches!(f, Pure(_))) {
        let vs = fields.into_iter().map(|f| match f { Pure(v) => v, Fail(_) => unreachable!() }).collect();
        return Pure(Value::Prod(vs));
    }
    let n = field_len(&fields[0]);
    // a row survives iff every field is Ok there.
    let mut keep_row = vec![true; n];
    for f in &fields {
        if let Fail(fv) = f {
            for (r, &e) in fv.err.iter().enumerate() {
                keep_row[r] &= !e;
            }
        }
    }
    let err: Vec<bool> = keep_row.iter().map(|&k| !k).collect();
    let keep: Vec<usize> = (0..n).filter(|&r| keep_row[r]).collect();
    if keep.is_empty() {
        return Fail(FailValues { err, ok: None });
    }
    if keep.len() == n {
        // no row was actually dropped (the fallible fields carried no errors): the product is just the
        // fields at full length — skip the re-gather, which would COPY a large shared `Pure` field (the
        // haystack in a chained gather) on every call even though nothing failed.
        let cols = fields
            .into_iter()
            .map(|f| match f {
                Pure(v) => v,
                Fail(fv) => fv.ok.expect("no dropped rows implies a full Ok lane"),
            })
            .collect();
        return Fail(FailValues { err, ok: Some(Value::Prod(cols)) });
    }
    // gather each field's values at the surviving rows (a pure field is indexed by row directly; a
    // fallible field's Ok lane is packed over ITS Ok rows, so index it by rank-among-Ok-rows).
    let cols = fields
        .iter()
        .map(|f| match f {
            Pure(v) => gather(v, &keep),
            Fail(fv) => {
                let ok = fv.ok.as_ref().expect("a surviving row implies this field has Ok rows");
                let mut idx = Vec::with_capacity(keep.len());
                let (mut rank, mut next) = (0usize, 0usize);
                for r in 0..n {
                    if next < keep.len() && keep[next] == r {
                        idx.push(rank); // r's position within this field's packed Ok lane
                        next += 1;
                    }
                    if !fv.err[r] {
                        rank += 1;
                    }
                }
                gather(ok, &idx)
            }
        })
        .collect();
    Fail(FailValues { err, ok: Some(Value::Prod(cols)) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Builder;
    use crate::ops::ArithOp;

    #[test]
    fn get_then_lifted_add() {
        // (idx, haystack) GetTry -> Found/Oob per row; then `add 1` (pure) lifts over the Found values.
        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let g = b.add(NumOp::Core(Op::GetTry), vec![inp]);
        let a = b.add(NumOp::Arith(ArithOp::AddU64(1)), vec![g]);
        let graph = b.finish(a);

        // row 0: haystack[0][0] = 10 -> Found; row 1: haystack[1][5] out of range -> Oob.
        let input = Value::Prod(vec![
            Value::u64(vec![0, 5]),
            Value::List(vec![3, 4].into(), Box::new(Value::u64(vec![10, 20, 30, 40]))),
        ]);
        match effect_eval_graph(&graph, EffectValues::Pure(input)) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, true]); // Found, Oob
                assert_eq!(crate::value::show(&ok.unwrap()), "[11]"); // 10 + 1, lifted over Ok
            }
            EffectValues::Pure(_) => panic!("a FailOp must yield a Fail column"),
        }
    }

    #[test]
    fn tuple_hoist_sequences_a_fallible_field() {
        // field 0: Fail over 3 rows (Ok, Err, Ok) carrying [100, 300]; field 1: Pure [7, 8, 9].
        // Sequence drops row 1 (field 0 errored) and products the survivors: ([100,300], [7,9]).
        let a = EffectValues::Fail(FailValues {
            err: vec![false, true, false],
            ok: Some(Value::u64(vec![100, 300])),
        });
        let b = EffectValues::Pure(Value::u64(vec![7, 8, 9]));
        match hoist_tuple(vec![a, b]) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, true, false]);
                assert_eq!(crate::value::show(&ok.unwrap()), "([100, 300], [7, 9])");
            }
            EffectValues::Pure(_) => panic!("a fallible field must hoist to a Fail column"),
        }
    }

    #[test]
    fn gather_is_per_row_all_or_nothing() {
        // row 0: indices [0,2] both in range -> Ok [10,30]; row 1: index 5 out of range -> Error.
        let input = Value::Prod(vec![
            Value::List(vec![2, 5].into(), Box::new(Value::u64(vec![0, 2, 0, 1, 5]))),
            Value::List(vec![3, 4].into(), Box::new(Value::u64(vec![10, 20, 30, 40]))),
        ]);
        match effect_eval(&NumOp::Core(Op::Gather), EffectValues::Pure(input)) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, true]);
                assert_eq!(crate::value::show(&ok.unwrap()), "List ends=[2] <[10, 30]>");
            }
            EffectValues::Pure(_) => panic!("gather is a FailOp"),
        }
    }

    #[test]
    fn chunk_is_per_row_divisibility() {
        // chunk k=2 over [[0,1,2,3],[4,5,6]]: row 0 (len 4) -> Ok [[0,1],[2,3]]; row 1 (len 3) -> Error.
        let input = Value::List(vec![4, 7].into(), Box::new(Value::u64(vec![0, 1, 2, 3, 4, 5, 6])));
        match effect_eval(&NumOp::Core(Op::Chunk(2)), EffectValues::Pure(input)) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, true]);
                assert_eq!(crate::value::show(&ok.unwrap()), "List ends=[2] <List ends=[2, 4] <[0, 1, 2, 3]>>");
            }
            EffectValues::Pure(_) => panic!("chunk is a FailOp"),
        }
    }

    #[test]
    fn branch_demux_oob_is_error() {
        // tags [0,1,2] with branch 2: 0->lane0, 1->lane1, 2 out of range -> Error.
        let input = Value::Prod(vec![Value::u64(vec![100, 200, 300]), Value::u64(vec![0, 1, 2])]);
        match effect_eval(&NumOp::Core(Op::TryBranch(2)), EffectValues::Pure(input)) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, false, true]); // row 2 had tag 2 >= 2
                // the Ok lane is the demux Sum{X|X} over the in-range rows: tag0=[100], tag1=[200].
                assert_eq!(crate::value::show(&ok.unwrap()), "Sum tags=[0, 1] [[100], [200]]");
            }
            EffectValues::Pure(_) => panic!("branch is a FailOp"),
        }
    }

    #[test]
    fn fold_with_fallible_body_errors_per_row() {
        // body (acc, elem) -> get the elem-th element of the singleton list [acc]: Found(acc) if
        // elem==0, else Oob. A row folds OK iff every element is 0; any nonzero element errors it.
        let mut bb: Builder<NumOp> = Builder::default();
        let bi = bb.input();
        let acc = bb.add(NumOp::Core(Op::Field(0)), vec![bi]);
        let elem = bb.add(NumOp::Core(Op::Field(1)), vec![bi]);
        let lst = bb.add(NumOp::Core(Op::Enlist), vec![acc]);
        let pair = bb.tuple(vec![elem, lst]);
        let g = bb.add(NumOp::Core(Op::GetTry), vec![pair]);
        let body = bb.finish(g);

        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let f = b.add(NumOp::Core(Op::Fold(Box::new(body))), vec![inp]);
        let graph = b.finish(f);

        // seed [5,5]; row0 list [0,0] folds OK (acc stays 5), row1 [0,1] errors at round 1 (elem 1).
        let input = Value::Prod(vec![
            Value::u64(vec![5, 5]),
            Value::List(vec![2, 4].into(), Box::new(Value::u64(vec![0, 0, 0, 1]))),
        ]);
        match effect_eval_graph(&graph, EffectValues::Pure(input)) {
            EffectValues::Fail(FailValues { err, ok }) => {
                assert_eq!(err, vec![false, true]);
                assert_eq!(crate::value::show(&ok.unwrap()), "[5]");
            }
            EffectValues::Pure(_) => panic!("a fallible fold body makes the fold partial"),
        }
    }

    #[test]
    fn traverse_collapses_per_element_to_per_row() {
        // 2 rows: row0 = [Ok,Ok] (10,20), row1 = [Ok,Err] (30 ok, 4th element errored).
        // Traverse: row0 fully Ok -> keep [10,20]; row1 has an error -> the whole row drops.
        let fv = FailValues {
            err: vec![false, false, false, true],
            ok: Some(Value::u64(vec![10, 20, 30])),
        };
        let out = traverse(vec![2, 4].into(), fv);
        assert_eq!(out.err, vec![false, true]);
        assert_eq!(crate::value::show(&out.ok.unwrap()), "List ends=[2] <[10, 20]>");
    }

    #[test]
    fn try_reveals_fail_as_option_sum() {
        // get -> Found/Oob, then TRY reveals it as a pure Sum{ T | Unit } (Ok lane 0, Err = Unit).
        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let g = b.add(NumOp::Core(Op::GetTry), vec![inp]);
        let t = b.add(NumOp::Core(Op::Try), vec![g]);
        let graph = b.finish(t);
        let input = Value::Prod(vec![
            Value::u64(vec![0, 5]),
            Value::List(vec![3, 4].into(), Box::new(Value::u64(vec![10, 20, 30, 40]))),
        ]);
        match effect_eval_graph(&graph, EffectValues::Pure(input)) {
            EffectValues::Pure(v) => {
                // row 0 Found(10) -> tag 0; row 1 Oob -> tag 1 (Unit). A matchable Option.
                assert_eq!(crate::value::show(&v), "Sum tags=[0, 1] [[10], ()x1]");
            }
            EffectValues::Fail(_) => panic!("TRY moves Fail -> Pure"),
        }
    }

    #[test]
    fn typer_tracks_totality() {
        // `get` alone is partial (output Fail); wrapping it in TRY makes it total (output Pure).
        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let g = b.add(NumOp::Core(Op::GetTry), vec![inp]);
        let partial = b.finish(g);
        assert!(!is_total(&partial), "a bare FailOp is partial");

        let mut b2: Builder<NumOp> = Builder::default();
        let inp2 = b2.input();
        let g2 = b2.add(NumOp::Core(Op::GetTry), vec![inp2]);
        let t2 = b2.add(NumOp::Core(Op::Try), vec![g2]);
        assert!(is_total(&b2.finish(t2)), "TRY discharges the effect");

        // a FailOp inside a map body makes the whole program partial (Traverse propagates it).
        let mut bb: Builder<NumOp> = Builder::default();
        let bi = bb.input();
        let bg = bb.add(NumOp::Core(Op::GetTry), vec![bi]);
        let body = bb.finish(bg);
        let mut bm: Builder<NumOp> = Builder::default();
        let mi = bm.input();
        let mm = bm.add(NumOp::Core(Op::MapList(Box::new(body))), vec![mi]);
        assert!(!is_total(&bm.finish(mm)), "a fallible map body is partial");
    }

    #[test]
    fn maplist_over_fallible_list_lifts_and_keeps_errors() {
        // map (x -> x add 1) over a FALLIBLE list (row0 Ok [10,30], row1 Error): the pure body lifts
        // over the Ok rows and the outer error mask is preserved.
        let mut bb: Builder<NumOp> = Builder::default();
        let bi = bb.input();
        let ba = bb.add(NumOp::Arith(ArithOp::AddU64(1)), vec![bi]);
        let body = bb.finish(ba);
        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let m = b.add(NumOp::Core(Op::MapList(Box::new(body))), vec![inp]);
        let graph = b.finish(m);
        let input = EffectValues::Fail(FailValues {
            err: vec![false, true],
            ok: Some(Value::List(vec![2].into(), Box::new(Value::u64(vec![10, 30])))),
        });
        match effect_eval_graph(&graph, input) {
            EffectValues::Fail(fv) => {
                assert_eq!(fv.err, vec![false, true]);
                assert_eq!(crate::value::show(&fv.ok.unwrap()), "List ends=[2] <[11, 31]>");
            }
            EffectValues::Pure(_) => panic!("a fallible input list stays Fail"),
        }
    }

    #[test]
    fn maplist_pure_body_is_the_ordinary_map() {
        // map (x -> x add 1) over [[1,2],[3]] stays pure: [[2,3],[4]].
        let mut bb: Builder<NumOp> = Builder::default();
        let bi = bb.input();
        let ba = bb.add(NumOp::Arith(ArithOp::AddU64(1)), vec![bi]);
        let body = bb.finish(ba);
        let mut b: Builder<NumOp> = Builder::default();
        let inp = b.input();
        let m = b.add(NumOp::Core(Op::MapList(Box::new(body))), vec![inp]);
        let graph = b.finish(m);
        let input = Value::List(vec![2, 3].into(), Box::new(Value::u64(vec![1, 2, 3])));
        match effect_eval_graph(&graph, EffectValues::Pure(input)) {
            EffectValues::Pure(v) => assert_eq!(crate::value::show(&v), "List ends=[2, 3] <[2, 3, 4]>"),
            EffectValues::Fail(_) => panic!("a pure body keeps the map pure"),
        }
    }
}
