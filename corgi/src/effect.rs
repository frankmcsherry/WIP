//! The effect layer as a REWRITE: failure is threaded through a program by inserting ordinary ops,
//! not by a second evaluator.
//!
//! A program is written against pure values. Some of its ops (`gather`, `zip`, `chunk`, and the
//! forms over `gather`: `get`, `head`, `slices`) are checked: they report a row that doesn't fit as
//! an error, and their output is `Fail<T> = Sum{T | Unit}` (see [`crate::ops::fail`]). Everything
//! downstream of one still expects `T`. [`lower_effects`] makes the program well-typed:
//!
//!   * a pure op fed a `Fail<T>` becomes `MapSum([(0, op)])` — it runs on the packed Ok lane, the Err
//!     rows pass through untouched;
//!   * a fallible op fed a `Fail<T>` does the same and then `Squash`es the nested `Fail<Fail<U>>`;
//!   * a `Tuple` with a fallible field `Lift`s its pure fields and `HoistProd`s (a row errs if any field
//!     errs); a `MapList`/`MapSum`/`Fold`/`FoldScan` whose BODY fails hoists the body's per-element
//!     errors out with `HoistList`/`HoistSum` (per row, all-or-nothing);
//!   * `try` is erased — its input already IS the `Sum{T | Unit}` the program goes on to match.
//!
//! The lowered graph is in the pure vocabulary, so `eval_graph` runs it, `shape_of` types it, and the
//! optimizer sees through it. A failure no `try` takes up reaches the output, which is then a
//! `Fail<T>`; the typer shows it there.

use crate::graph::{Builder, Graph, NodeKind};
use crate::ops::{NumOp, Op};

/// does this op introduce failure (its output is a `Fail<..>`)?
fn is_fail_op(op: &NumOp) -> bool {
    matches!(
        op,
        NumOp::Core(
            Op::TryGather
                | Op::TryZip
                | Op::TryChunk(_)
        )
    )
}

/// the regime of every node: `true` where the column is a `Fail<..>` the program has not yet taken up.
/// A fallible op introduces it, `Tuple` is fallible if any field is, a body-bearing op if its body is,
/// `try` discharges it, and every other op preserves its input's regime.
fn regimes(g: &Graph<NumOp>, input_fail: bool) -> Vec<bool> {
    let mut fail: Vec<bool> = Vec::with_capacity(g.nodes.len());
    for node in &g.nodes {
        let f = match &node.kind {
            NodeKind::Input => input_fail,
            NodeKind::Tuple => node.inputs.iter().any(|&i| fail[i]),
            NodeKind::Op(NumOp::Core(Op::Try)) => false,
            NodeKind::Op(NumOp::Core(Op::MapList(body) | Op::Fold(body) | Op::FoldScan(body))) => {
                fail[node.inputs[0]] || graph_is_fail(body, false)
            }
            NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
                fail[node.inputs[0]] || arms.iter().any(|(_, b)| graph_is_fail(b, false))
            }
            NodeKind::Op(o) if is_fail_op(o) => true,
            NodeKind::Op(_) => fail[node.inputs[0]],
        };
        fail.push(f);
    }
    fail
}

/// whether the graph's output column is fallible, given its input's regime.
pub(crate) fn graph_is_fail(g: &Graph<NumOp>, input_fail: bool) -> bool {
    regimes(g, input_fail)[g.output]
}

/// a one-parameter graph from a builder closure: `Input`, then whatever `body` adds on it.
fn single(body: impl FnOnce(&mut Builder<NumOp>, usize) -> usize) -> Graph<NumOp> {
    let mut b = Builder::default();
    let x = b.input();
    let out = body(&mut b, x);
    b.finish(out)
}

/// `MapSum([(0, body)])`: run `body` on the Ok lane of a `Fail<..>` at node `x`.
fn on_ok(b: &mut Builder<NumOp>, x: usize, body: Graph<NumOp>) -> usize {
    b.add(Op::MapSum(vec![(0, body)]), vec![x])
}

/// lower `g` into the pure vocabulary (see the module header). `input_fail` says whether the graph's
/// parameter is itself a `Fail<..>` — always `false` at the top and for bodies, whose parameters are
/// the packed Ok lane / the pure elements.
fn lower(g: &Graph<NumOp>, input_fail: bool) -> Graph<NumOp> {
    let fail = regimes(g, input_fail);
    let mut b = Builder::default();
    let mut map: Vec<usize> = Vec::with_capacity(g.nodes.len());
    for (i, node) in g.nodes.iter().enumerate() {
        let id = match &node.kind {
            NodeKind::Input => b.input(),
            NodeKind::Tuple => {
                let any_fail = node.inputs.iter().any(|&j| fail[j]);
                let ins: Vec<usize> = node
                    .inputs
                    .iter()
                    .map(|&j| if any_fail && !fail[j] { b.add(Op::Lift, vec![map[j]]) } else { map[j] })
                    .collect();
                let t = b.tuple(ins);
                if any_fail { b.add(Op::HoistProd, vec![t]) } else { t }
            }
            NodeKind::Op(op) => {
                let x = map[node.inputs[0]];
                let in_fail = fail[node.inputs[0]];
                match op {
                    NumOp::Core(Op::Try) => x,
                    NumOp::Core(Op::MapList(body)) => {
                        let bf = graph_is_fail(body, false);
                        let body = lower(body, false);
                        let inner = move |b: &mut Builder<NumOp>, x: usize| {
                            let m = b.add(Op::MapList(Box::new(body)), vec![x]);
                            if bf { b.add(Op::HoistList, vec![m]) } else { m }
                        };
                        wrap(&mut b, x, in_fail, bf, inner)
                    }
                    NumOp::Core(Op::MapSum(arms)) => {
                        let fallible: Vec<usize> =
                            arms.iter().filter(|(_, a)| graph_is_fail(a, false)).map(|(k, _)| *k).collect();
                        let arms: Vec<(usize, Graph<NumOp>)> =
                            arms.iter().map(|(k, a)| (*k, lower(a, false))).collect();
                        let bf = !fallible.is_empty();
                        let inner = move |b: &mut Builder<NumOp>, x: usize| {
                            let m = b.add(Op::MapSum(arms), vec![x]);
                            if bf { b.add(Op::HoistSum(fallible), vec![m]) } else { m }
                        };
                        wrap(&mut b, x, in_fail, bf, inner)
                    }
                    NumOp::Core(Op::Fold(body)) => {
                        let bf = graph_is_fail(body, false);
                        let body = lower(body, false);
                        let inner = move |b: &mut Builder<NumOp>, x: usize| {
                            if !bf {
                                return b.add(Op::Fold(Box::new(body)), vec![x]);
                            }
                            // accumulate a `Fail<B>`: each round hoists (acc, elem) into the Ok lane,
                            // runs the body there, and squashes — a row that errs stays Err.
                            let step = single(|b, p| {
                                let h = hoist_pair(b, p);
                                let m = on_ok(b, h, body);
                                b.add(Op::Squash, vec![m])
                            });
                            let seeded = lift_seed(b, x);
                            b.add(Op::Fold(Box::new(step)), vec![seeded])
                        };
                        wrap(&mut b, x, in_fail, bf, inner)
                    }
                    NumOp::Core(Op::FoldScan(body)) => {
                        let bf = graph_is_fail(body, false);
                        let body = lower(body, false);
                        let inner = move |b: &mut Builder<NumOp>, x: usize| {
                            if !bf {
                                return b.add(Op::FoldScan(Box::new(body)), vec![x]);
                            }
                            // state `Fail<T>`, per-element output `Fail<R>`: as for `Fold`, then split the
                            // body's `Fail<(T, R)>` into its two fallible halves.
                            let step = single(|b, p| {
                                let h = hoist_pair(b, p);
                                let m = on_ok(b, h, body);
                                let s = b.add(Op::Squash, vec![m]);
                                let st = on_ok(b, s, single(|b, y| b.add(Op::Field(0), vec![y])));
                                let r = on_ok(b, s, single(|b, y| b.add(Op::Field(1), vec![y])));
                                b.tuple(vec![st, r])
                            });
                            let seeded = lift_seed(b, x);
                            let fs = b.add(Op::FoldScan(Box::new(step)), vec![seeded]);
                            // (Fail<T>, List<Fail<R>>) -> Fail<(T, List<R>)>
                            let st = b.add(Op::Field(0), vec![fs]);
                            let outs = b.add(Op::Field(1), vec![fs]);
                            let hl = b.add(Op::HoistList, vec![outs]);
                            let t = b.tuple(vec![st, hl]);
                            b.add(Op::HoistProd, vec![t])
                        };
                        wrap(&mut b, x, in_fail, bf, inner)
                    }
                    op => {
                        let op = op.clone();
                        let introduces = is_fail_op(&op);
                        let inner = move |b: &mut Builder<NumOp>, x: usize| b.add(op, vec![x]);
                        wrap(&mut b, x, in_fail, introduces, inner)
                    }
                }
            }
        };
        debug_assert_eq!(map.len(), i);
        map.push(id);
    }
    b.finish(map[g.output])
}

/// place `inner` (an op or op sequence on a pure input) at `x`: directly when `x` is pure, else on the
/// Ok lane — and `Squash` afterwards when `inner` itself yields a `Fail<..>` (so `Fail` never nests).
fn wrap(
    b: &mut Builder<NumOp>,
    x: usize,
    in_fail: bool,
    yields_fail: bool,
    inner: impl FnOnce(&mut Builder<NumOp>, usize) -> usize,
) -> usize {
    if !in_fail {
        return inner(b, x);
    }
    let m = on_ok(b, x, single(inner));
    if yields_fail { b.add(Op::Squash, vec![m]) } else { m }
}

/// `(Fail<B>, A) -> Fail<(B, A)>` at node `p`: lift the pure element and hoist the pair.
fn hoist_pair(b: &mut Builder<NumOp>, p: usize) -> usize {
    let acc = b.add(Op::Field(0), vec![p]);
    let elem = b.add(Op::Field(1), vec![p]);
    let elem = b.add(Op::Lift, vec![elem]);
    let t = b.tuple(vec![acc, elem]);
    b.add(Op::HoistProd, vec![t])
}

/// `(B, List<A>) -> (Fail<B>, List<A>)` at node `x`: lift the fold seed into the failure regime.
fn lift_seed(b: &mut Builder<NumOp>, x: usize) -> usize {
    let seed = b.add(Op::Field(0), vec![x]);
    let list = b.add(Op::Field(1), vec![x]);
    let seed = b.add(Op::Lift, vec![seed]);
    b.tuple(vec![seed, list])
}

/// rewrite a program so every op downstream of a fallible one runs on that column's Ok lane. The
/// result is in the pure vocabulary: `eval_graph` runs it, `shape_of` types it. Its output is the
/// original's, wrapped as `Fail<T> = Sum{T | Unit}` wherever the original was un-`try`'d fallible.
pub fn lower_effects(g: &Graph<NumOp>) -> Graph<NumOp> {
    lower(g, false)
}

#[cfg(test)]
mod tests;
