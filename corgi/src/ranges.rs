//! The range checker — Class-B value agreement for `branch`, the second half of the totality gate.
//!
//! `branch k` panics on a tag >= k; this proves it can't, by interval abstract interpretation. Each
//! `U64` leaf carries a value range `[lo,hi]` (or ⊤ = unknown); range-producing ops set it (`and m`
//! -> `[0,m]`, a comparison -> `[0,1]`, `shr`/`add_u64` shift it), the structural ops thread it
//! (Field/Tuple/MapList/Zip/Transpose), and at a `branch k` the tag's range must prove `<= k-1` —
//! else a STATIC error (use `try_branch`, or mask the tag with `and`). Like the length checker this is
//! a reject, not a dynamic check, so no runtime failure; and conservative — an unbounded range is ⊤,
//! which proves nothing (sound). It is NOT dependent types: ranges are an auxiliary lattice, never in
//! the shape.
//!
//! Scope: `branch` only. `gather`/`slices` out-of-bounds is also Class-B but its bound is a
//! data-dependent length, not a constant, so interval AI can't prove it — `index` (total) is the
//! answer there, not a range proof.

use crate::graph::{Graph, NodeKind, OpLike};
use crate::ops::{ArithOp, CmpOp, NumOp, Op};
use crate::shape::Shape;

/// inclusive value range of a `U64` leaf; `None` is ⊤ (unknown).
type Range = Option<(u64, u64)>;

/// a `Shape` carrying a value range at each leaf — the abstract value the pass threads.
#[derive(Clone)]
enum RShape {
    Prim(u32, Range),
    Prod(Vec<RShape>),
    Sum(Vec<Option<RShape>>),
    List(Box<RShape>),
    Unit, // no leaf to range
}

impl RShape {
    fn to_shape(&self) -> Shape {
        match self {
            RShape::Prim(w, _) => Shape::Prim(*w),
            RShape::Prod(fs) => Shape::Prod(fs.iter().map(RShape::to_shape).collect()),
            RShape::Sum(ls) => {
                Shape::Sum(ls.iter().map(|o| o.as_ref().map(RShape::to_shape)).collect())
            }
            RShape::List(i) => Shape::List(Box::new(i.to_shape())),
            RShape::Unit => Shape::Unit,
        }
    }
    /// the range if this is a leaf, else ⊤.
    fn range(&self) -> Range {
        match self {
            RShape::Prim(_, r) => *r,
            _ => None,
        }
    }
}

/// a shape with every leaf at ⊤ — what we know about an op's result by default (and about the input).
fn top(s: &Shape) -> RShape {
    match s {
        Shape::Prim(w) => RShape::Prim(*w, None),
        Shape::Prod(fs) => RShape::Prod(fs.iter().map(top).collect()),
        Shape::Sum(ls) => RShape::Sum(ls.iter().map(|o| o.as_ref().map(top)).collect()),
        Shape::List(i) => RShape::List(Box::new(top(i))),
        Shape::Unit => RShape::Unit,
    }
}

fn top_out(op: &NumOp, inr: &RShape) -> Result<RShape, String> {
    Ok(top(&op.judge(&inr.to_shape())?))
}

/// a tag range proves `< k` iff it is bounded with `hi < k`.
fn proves_lt(r: Range, k: usize) -> bool {
    matches!(r, Some((_, hi)) if (hi as u128) < k as u128)
}

/// prove every `branch k` in the graph routes only in-range tags, for the given input shape.
pub fn check_ranges(g: &Graph<NumOp>, input: &Shape) -> Result<(), String> {
    rgraph(g, top(input)).map(|_| ())
}

fn rgraph(g: &Graph<NumOp>, input: RShape) -> Result<RShape, String> {
    let mut rs: Vec<RShape> = Vec::with_capacity(g.nodes.len());
    for node in &g.nodes {
        let r = match &node.kind {
            NodeKind::Input => input.clone(),
            NodeKind::Tuple => {
                RShape::Prod(node.inputs.iter().map(|&i| rs[i].clone()).collect())
            }
            NodeKind::Op(op) => rshape_op(op, &rs[node.inputs[0]])?,
        };
        rs.push(r);
    }
    Ok(rs[g.output].clone())
}

fn rshape_op(op: &NumOp, inr: &RShape) -> Result<RShape, String> {
    match op {
        // --- the gate ---
        NumOp::Core(Op::Branch(k)) => match inr {
            RShape::Prod(fs) if fs.len() == 2 => {
                if !proves_lt(fs[1].range(), *k) {
                    return Err(format!(
                        "branch {k}: tag not proven < {k} (mask it with `and {}`, or use try_branch {k})",
                        k - 1
                    ));
                }
                Ok(RShape::Sum(vec![Some(fs[0].clone()); *k]))
            }
            _ => top_out(op, inr),
        },

        // --- range producers (U64 leaf -> a bounded leaf) ---
        NumOp::Arith(ArithOp::And(m)) => Ok(RShape::Prim(64, Some((0, *m)))),
        NumOp::Cmp(CmpOp::Gt(_)) => Ok(RShape::Prim(64, Some((0, 1)))),
        NumOp::Cmp(CmpOp::Rel(_)) => Ok(RShape::Prim(64, Some((0, 1)))),
        NumOp::Arith(ArithOp::Shr(s)) => {
            Ok(RShape::Prim(64, inr.range().map(|(lo, hi)| (lo >> *s, hi >> *s))))
        }
        NumOp::Arith(ArithOp::AddU64(c)) => Ok(RShape::Prim(
            64,
            inr.range().and_then(|(lo, hi)| Some((lo.checked_add(*c)?, hi.checked_add(*c)?))),
        )),

        // --- structural threading (ranges flow through unchanged) ---
        NumOp::Core(Op::Field(i)) => match inr {
            RShape::Prod(fs) => Ok(fs[*i].clone()),
            _ => top_out(op, inr),
        },
        NumOp::Core(Op::Transpose) => match inr {
            RShape::List(inner) => match &**inner {
                RShape::Prod(fs) => Ok(RShape::Prod(
                    fs.iter().map(|f| RShape::List(Box::new(f.clone()))).collect(),
                )),
                _ => top_out(op, inr),
            },
            _ => top_out(op, inr),
        },
        NumOp::Core(Op::Zip) => match inr {
            RShape::Prod(fs) if !fs.is_empty() => {
                let inners = fs
                    .iter()
                    .map(|f| match f {
                        RShape::List(i) => Ok((**i).clone()),
                        _ => Err(()),
                    })
                    .collect::<Result<Vec<_>, _>>();
                match inners {
                    Ok(es) => Ok(RShape::List(Box::new(RShape::Prod(es)))),
                    Err(()) => top_out(op, inr),
                }
            }
            _ => top_out(op, inr),
        },
        NumOp::Core(Op::MapList(body)) => match inr {
            RShape::List(inner) => Ok(RShape::List(Box::new(rgraph(body, (**inner).clone())?))),
            _ => top_out(op, inr),
        },

        // --- body-bearing ops: thread bodies (nested branches must be proven too), result ⊤ ---
        NumOp::Core(Op::MapSum(arms)) => {
            if let RShape::Sum(lanes) = inr {
                for (k, body) in arms {
                    if let Some(Some(lane)) = lanes.get(*k) {
                        rgraph(body, lane.clone())?;
                    }
                }
            }
            top_out(op, inr)
        }
        NumOp::Core(Op::Fold(body)) | NumOp::Core(Op::Scan(body)) | NumOp::Core(Op::FoldScan(body)) => {
            if let RShape::Prod(fs) = inr {
                if let [seed, RShape::List(a)] = fs.as_slice() {
                    rgraph(body, RShape::Prod(vec![seed.clone(), (**a).clone()]))?;
                }
            }
            top_out(op, inr)
        }

        _ => top_out(op, inr),
    }
}
