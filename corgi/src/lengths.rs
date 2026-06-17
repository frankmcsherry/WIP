//! The length/stratum checker — Class-A size agreement, the static half of the totality guarantee.
//!
//! It proves the structural preconditions the evaluator would otherwise panic on. There are only two:
//! `Zip` (all columns must share bounds) and `Filter` (data and mask must share bounds). Everything
//! else is automatic — corgi's SEQ level is 1:1, so within one scope every value has the same row
//! count; the only thing that can disagree is a LIST's *inner bounds*, and only Zip/Filter require
//! those equal.
//!
//! Method: **Futhark-style sized types, not dependent types.** Each `List` carries an opaque bounds
//! TOKEN (an equality variable, never an arithmetic term). Bounds-preserving ops propagate the token,
//! bounds-creating ops mint a fresh one, and Zip/Filter accept only when the relevant tokens are
//! identical — otherwise a STATIC error. No dynamic check is inserted, so there is no runtime size
//! failure (the deliberate divergence from Futhark, which aborts): a program either proves out or is
//! rejected. The analysis is conservative — when provenance is unclear it mints fresh, which is sound
//! (it can over-reject a safe-but-unproven program; the fix is `let`-sharing the source or a `try_`
//! op). Soundness rests on a handful of preserve-rules that mirror `eval` exactly (MapList/Transpose/
//! Field reuse the input bounds verbatim); every other op mints fresh via the typer's result shape.

use crate::graph::{Graph, NodeKind, OpLike};
use crate::ops::{NumOp, Op};
use crate::shape::Shape;

/// a `Shape` with a bounds token at each `List` — the identity used to prove agreement.
#[derive(Clone)]
enum BShape {
    Prim(u32),
    Prod(Vec<BShape>),
    Sum(Vec<Option<BShape>>),
    List(u32, Box<BShape>),
    Unit, // no bounds to track
}

impl BShape {
    fn to_shape(&self) -> Shape {
        match self {
            BShape::Prim(w) => Shape::Prim(*w),
            BShape::Prod(fs) => Shape::Prod(fs.iter().map(BShape::to_shape).collect()),
            BShape::Sum(ls) => {
                Shape::Sum(ls.iter().map(|o| o.as_ref().map(BShape::to_shape)).collect())
            }
            BShape::List(_, i) => Shape::List(Box::new(i.to_shape())),
            BShape::Unit => Shape::Unit,
        }
    }
}

/// decorate a `Shape` with all-fresh bounds tokens (every list's bounds an unrelated identity).
fn fresh(s: &Shape, c: &mut u32) -> BShape {
    match s {
        Shape::Prim(w) => BShape::Prim(*w),
        Shape::Prod(fs) => BShape::Prod(fs.iter().map(|f| fresh(f, c)).collect()),
        Shape::Sum(ls) => BShape::Sum(ls.iter().map(|o| o.as_ref().map(|s| fresh(s, c))).collect()),
        Shape::List(i) => {
            let t = *c;
            *c += 1;
            BShape::List(t, Box::new(fresh(i, c)))
        }
        Shape::Unit => BShape::Unit,
    }
}

/// any op not given a bounds-preserving rule: its bounds are unrelated to the input's, so decorate the
/// typer's result shape with fresh tokens. Sound (fresh never proves a false equality).
fn fresh_out(op: &NumOp, inb: &BShape, c: &mut u32) -> Result<BShape, String> {
    Ok(fresh(&op.judge(&inb.to_shape())?, c))
}

/// prove a graph's `Zip`/`Filter` bounds preconditions for the given input shape. `Ok(())` means no
/// well-typed run can hit a bounds panic; `Err` names the unprovable site.
pub fn check_lengths(g: &Graph<NumOp>, input: &Shape) -> Result<(), String> {
    let mut c = 0u32;
    let inb = fresh(input, &mut c);
    check_graph(g, inb, &mut c).map(|_| ())
}

/// the per-graph walker — the analogue of `shape_of`, carrying bounds tokens and recursing into bodies.
fn check_graph(g: &Graph<NumOp>, input: BShape, c: &mut u32) -> Result<BShape, String> {
    let mut bs: Vec<BShape> = Vec::with_capacity(g.nodes.len());
    for node in &g.nodes {
        let b = match &node.kind {
            NodeKind::Input => input.clone(),
            NodeKind::Tuple => {
                BShape::Prod(node.inputs.iter().map(|&i| bs[i].clone()).collect())
            }
            NodeKind::Op(op) => bshape_op(op, &bs[node.inputs[0]], c)?,
        };
        bs.push(b);
    }
    Ok(bs[g.output].clone())
}

fn bshape_op(op: &NumOp, inb: &BShape, c: &mut u32) -> Result<BShape, String> {
    match op {
        // --- the bounds-preserving rules (each mirrors `eval` reusing the input bounds verbatim) ---
        NumOp::Core(Op::Field(i)) => match inb {
            BShape::Prod(fs) => Ok(fs[*i].clone()),
            _ => fresh_out(op, inb, c),
        },
        // List<(X,Y,..)> -> (List<X>, List<Y>, ..) all sharing the input list's bounds.
        NumOp::Core(Op::Transpose) => match inb {
            BShape::List(t, inner) => match &**inner {
                BShape::Prod(fs) => Ok(BShape::Prod(
                    fs.iter().map(|f| BShape::List(*t, Box::new(f.clone()))).collect(),
                )),
                _ => fresh_out(op, inb, c),
            },
            _ => fresh_out(op, inb, c),
        },
        // MapList reuses the input list's bounds; recurse the body for its own Zip/Filter sites.
        NumOp::Core(Op::MapList(body)) => match inb {
            BShape::List(t, inner) => {
                let out = check_graph(body, (**inner).clone(), c)?;
                Ok(BShape::List(*t, Box::new(out)))
            }
            _ => fresh_out(op, inb, c),
        },

        // --- the two CHECK sites ---
        NumOp::Core(Op::Zip) => match inb {
            BShape::Prod(fs) if !fs.is_empty() => {
                let mut tok: Option<u32> = None;
                let mut inners = Vec::with_capacity(fs.len());
                for f in fs {
                    let BShape::List(t, inner) = f else { return fresh_out(op, inb, c) };
                    match tok {
                        None => tok = Some(*t),
                        Some(p) if p != *t => {
                            return Err("zip: column bounds not proven equal \
                                 (share a source via `let`/transpose, or use try_zip)"
                                .into())
                        }
                        Some(_) => {}
                    }
                    inners.push((**inner).clone());
                }
                Ok(BShape::List(tok.unwrap(), Box::new(BShape::Prod(inners))))
            }
            _ => fresh_out(op, inb, c),
        },
        NumOp::Core(Op::Filter) => match inb {
            BShape::Prod(fs) if fs.len() == 2 => match (&fs[0], &fs[1]) {
                (BShape::List(td, x), BShape::List(tm, _)) => {
                    if td != tm {
                        return Err("filter: data/mask bounds not proven equal \
                             (derive the mask from the data, or zip them first)"
                            .into());
                    }
                    let t = *c;
                    *c += 1;
                    Ok(BShape::List(t, x.clone())) // filtering re-segments: fresh bounds
                }
                _ => fresh_out(op, inb, c),
            },
            _ => fresh_out(op, inb, c),
        },

        // --- body-bearing ops: their result bounds are fresh, but their bodies must be checked too ---
        NumOp::Core(Op::MapSum(arms)) => {
            if let BShape::Sum(lanes) = inb {
                for (k, body) in arms {
                    if let Some(Some(lane)) = lanes.get(*k) {
                        check_graph(body, lane.clone(), c)?;
                    }
                }
            }
            fresh_out(op, inb, c)
        }
        NumOp::Core(Op::Fold(body)) | NumOp::Core(Op::FoldScan(body)) => {
            if let BShape::Prod(fs) = inb {
                if let [seed, BShape::List(_, a)] = fs.as_slice() {
                    // the accumulator is the FED-BACK output (not the seed) past round 0, so its bounds
                    // can differ from the seed's; thread FRESH tokens for it (the fixpoint back-edge —
                    // same rule as `ranges`). A body's self-derived equalities still hold (shared fresh
                    // token); only a zip/filter that leans on the seed's bounds for the per-round
                    // accumulator is rejected — which is correct, those bounds can change each round.
                    let acc = fresh(&seed.to_shape(), c);
                    check_graph(body, BShape::Prod(vec![acc, (**a).clone()]), c)?;
                }
            }
            fresh_out(op, inb, c)
        }

        _ => fresh_out(op, inb, c),
    }
}
