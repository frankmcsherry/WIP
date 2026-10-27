//! Backward requests in the original type's field positions.
//! Analysis checks the whole graph and unions readers without building a graph.
//! Unknown operators keep their full operands. Row-count-only requests skip work.

use crate::graph::{node_shapes, Graph, NodeKind};
use crate::{NumOp, Op, Shape};

/// The part of a value needed by a consumer.
/// Container variants retain their structure while describing demand on their contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Demand {
    None,
    All,
    Prod(Vec<Demand>),
    List(Box<Demand>),
    Sum(Vec<Demand>),
    Ref(Box<Demand>),
}

impl Demand {
    /// Union two demands on the same original shape.
    /// A structural mismatch returns an error without changing `self`.
    /// Use `union_for_shape` to also validate both demands against the original shape.
    pub fn merge(&mut self, other: Demand) -> Result<(), String> {
        if !self.compatible(&other) {
            return Err("demands have different structures".into());
        }
        self.merge_unchecked(other);
        Ok(())
    }

    /// Union two demands against one original shape and return the resulting projection shape.
    pub fn union_for_shape(original: &Shape, left: &Demand, right: &Demand) -> Result<(Demand, Shape), String> {
        left.projected_shape(original)?;
        right.projected_shape(original)?;
        let mut merged = left.clone();
        merged.merge(right.clone())?;
        let merged = merged.canonical(original);
        let projected = merged.projected_shape(original)?;
        Ok((merged, projected))
    }

    /// Compact the requested fields, retaining original order and container structure.
    /// `All` preserves unit fields and one-field products; `None` needs only row count.
    pub fn projected_shape(&self, original: &Shape) -> Result<Shape, String> {
        match (original, self) {
            (_, Demand::All) => Ok(original.clone()),
            (_, Demand::None) => Ok(Shape::Unit),
            (Shape::Prod(fields), Demand::Prod(needs)) if fields.len() == needs.len() => {
                let kept = fields.iter().zip(needs).filter(|(_, need)| **need != Demand::None)
                    .map(|(shape, need)| need.projected_shape(shape)).collect::<Result<Vec<_>, _>>()?;
                Ok(if kept.is_empty() { Shape::Unit } else { Shape::Prod(kept) })
            }
            (Shape::List(element), Demand::List(need)) => Ok(Shape::List(Box::new(need.projected_shape(element)?))),
            (Shape::Ref(list), Demand::Ref(need)) => {
                let list = need.projected_shape(list)?;
                if !matches!(list, Shape::List(_)) { return Err("reference request must retain list bounds".into()); }
                Ok(Shape::Ref(Box::new(list)))
            }
            (Shape::Sum(lanes), Demand::Sum(needs)) if lanes.len() == needs.len() => {
                Ok(Shape::Sum(lanes.iter().zip(needs).map(|(shape, need)| need.projected_shape(shape)).collect::<Result<_, _>>()?))
            }
            _ => Err(format!("demand {self:?} does not match shape {original}")),
        }
    }

    /// Compact a value at this request, using its original field positions.
    /// Unread product fields are dropped; list bounds, sum tags and reference
    /// rows are retained. Leaves keep their buffers. A shape mismatch is an error.
    pub fn project(&self, value: crate::Value) -> Result<crate::Value, String> {
        use crate::Value;
        match (self, value) {
            (Demand::All, value) => Ok(value),
            (Demand::None, value) => Ok(Value::Unit(value.len())),
            (Demand::Prod(needs), Value::Prod(fields)) if needs.len() == fields.len() => {
                let rows = fields.first().map_or(0, Value::len);
                let kept = needs.iter().zip(fields).filter(|(need, _)| **need != Demand::None)
                    .map(|(need, value)| need.project(value)).collect::<Result<Vec<_>, _>>()?;
                Ok(if kept.is_empty() { Value::Unit(rows) } else { Value::Prod(kept) })
            }
            (Demand::List(need), Value::List(bounds, values)) => {
                Ok(Value::List(bounds, Box::new(need.project(*values)?)))
            }
            (Demand::Sum(needs), Value::Sum(tags, lanes)) if needs.len() == lanes.len() => {
                let lanes = needs.iter().zip(lanes).map(|(need, value)| need.project(value)).collect::<Result<_, _>>()?;
                Ok(Value::Sum(tags, lanes))
            }
            (Demand::Ref(need), Value::Ref(list, rows)) => {
                let list = need.project(std::sync::Arc::unwrap_or_clone(list))?;
                if !matches!(list, Value::List(..)) { return Err("reference request must retain list bounds".into()); }
                Ok(Value::Ref(std::sync::Arc::new(list), rows))
            }
            (need, value) => Err(format!("demand {need:?} does not match shape {}", crate::shape_of_value(&value))),
        }
    }


    /// Normalize equivalent identity requests without changing product presence.
    /// None always stays None, including on unit. A retained unit field is All,
    /// which is distinct from dropping it. Explicit products of unread fields
    /// also retain their slot when nested, so they must not collapse to None.
    pub(crate) fn canonical(self, shape: &Shape) -> Demand {
        let demand = match (self, shape) {
            (Demand::Prod(ds), Shape::Prod(fs)) => Demand::Prod(ds.into_iter().zip(fs).map(|(d, s)| d.canonical(s)).collect()),
            (Demand::Sum(ds), Shape::Sum(fs)) => Demand::Sum(ds.into_iter().zip(fs).map(|(d, s)| d.canonical(s)).collect()),
            (Demand::List(d), Shape::List(s)) => Demand::List(Box::new(d.canonical(s))),
            (Demand::Ref(d), Shape::Ref(s)) => Demand::Ref(Box::new(d.canonical(s))),
            (demand, _) => demand,
        };
        match &demand {
            Demand::Prod(ds) if !ds.is_empty() && ds.iter().all(|d| *d == Demand::All) => Demand::All,
            Demand::Sum(ds) if ds.iter().all(|d| *d == Demand::All) => Demand::All,
            Demand::List(d) | Demand::Ref(d) if **d == Demand::All => Demand::All,
            _ => demand,
        }
    }

    /// Identity in original coordinates, preserving every product slot.
    pub(crate) fn is_all(&self, shape: &Shape) -> bool {
        self.clone().canonical(shape) == Demand::All
    }

    fn compatible(&self, other: &Demand) -> bool {
        match (self, other) {
            (Demand::None | Demand::All, _) | (_, Demand::None | Demand::All) => true,
            (Demand::Prod(left), Demand::Prod(right)) | (Demand::Sum(left), Demand::Sum(right)) => {
                left.len() == right.len() && left.iter().zip(right).all(|(l, r)| l.compatible(r))
            }
            (Demand::List(left), Demand::List(right)) | (Demand::Ref(left), Demand::Ref(right)) => left.compatible(right),
            _ => false,
        }
    }

    pub(crate) fn merge_unchecked(&mut self, other: Demand) {
        match (&mut *self, other) {
            (Demand::All, _) | (_, Demand::None) => {}
            (here, Demand::All) => *here = Demand::All,
            (here @ Demand::None, other) => *here = other,
            (Demand::Prod(left), Demand::Prod(right)) => {
                assert_eq!(left.len(), right.len());
                for (l, r) in left.iter_mut().zip(right) {
                    l.merge_unchecked(r);
                }
            }
            (Demand::List(left), Demand::List(right)) => left.merge_unchecked(*right),
            (Demand::Ref(left), Demand::Ref(right)) => left.merge_unchecked(*right),
            (Demand::Sum(left), Demand::Sum(right)) => {
                assert_eq!(left.len(), right.len());
                for (l, r) in left.iter_mut().zip(right) {
                    l.merge_unchecked(r);
                }
            }
            _ => unreachable!("demands for one node have different structures"),
        }
    }

    pub(crate) fn fields(self, count: usize) -> Vec<Demand> {
        match self {
            Demand::None => vec![Demand::None; count],
            Demand::All => vec![Demand::All; count],
            Demand::Prod(fields) => {
                assert_eq!(fields.len(), count);
                fields
            }
            _ => unreachable!("a tuple demand must be for a product"),
        }
    }

    pub(crate) fn list_element(self) -> Demand {
        match self {
            Demand::None | Demand::All => self,
            Demand::List(element) => *element,
            _ => unreachable!("a list demand must be for a list"),
        }
    }

    pub(crate) fn sum_lanes(self, count: usize) -> Vec<Demand> {
        match self {
            Demand::None => vec![Demand::None; count],
            Demand::All => vec![Demand::All; count],
            Demand::Sum(lanes) => {
                assert_eq!(lanes.len(), count);
                lanes
            }
            _ => unreachable!("a sum demand must be for a sum"),
        }
    }
}

/// Undo the shape change made by `Ref`: top-level lists become references, while products and sums pass through.
pub(crate) fn before_ref(shape: &Shape, demand: Demand) -> Demand {
    match (shape, demand) {
        (_, Demand::All) => Demand::All,
        (_, Demand::None) => Demand::None,
        (Shape::List(_), Demand::Ref(list)) => *list,
        (Shape::Ref(_), demand @ Demand::Ref(_)) => demand,
        (Shape::Prod(fields), Demand::Prod(needs)) => {
            assert_eq!(fields.len(), needs.len());
            Demand::Prod(fields.iter().zip(needs).map(|(shape, need)| before_ref(shape, need)).collect())
        }
        (Shape::Sum(lanes), Demand::Sum(needs)) => {
            assert_eq!(lanes.len(), needs.len());
            Demand::Sum(lanes.iter().zip(needs).map(|(shape, need)| before_ref(shape, need)).collect())
        }
        _ => unreachable!("demand does not match Ref output shape"),
    }
}

/// Requests at original node and field positions, unioned over all readers.
/// Identity requests are canonicalized to All; None stays None even on unit.
/// Body results and shapes are retained for the forward rebuild.
#[derive(Debug)]
pub struct DemandAnalysis {
    pub(crate) input: Demand,
    pub(crate) output: Shape,
    pub(crate) demands: Vec<Demand>,
    pub(crate) shapes: Vec<Shape>,
    pub(crate) live: Vec<bool>,
    pub(crate) bodies: Vec<Vec<(usize, DemandAnalysis)>>,
}

impl DemandAnalysis {
    pub fn input_demand(&self) -> &Demand { &self.input }
    pub fn output_shape(&self) -> &Shape { &self.output }
    pub fn node_demands(&self) -> &[Demand] { &self.demands }
}

/// Check the entire graph, then find its requests without constructing a graph.
/// Unread computations contribute no demand. Remaining patterns keep their full input;
/// call `compile` first to remove them and expose narrower requests.
pub fn analyze_demand(
    graph: &Graph<NumOp>,
    input: &Shape,
    output_demand: &Demand,
) -> Result<DemandAnalysis, String> {
    analyze(graph, input, output_demand, true)
}

fn analyze(
    graph: &Graph<NumOp>, input: &Shape, output_demand: &Demand, incoming: bool,
) -> Result<DemandAnalysis, String> {
    let shapes = node_shapes(graph, input)?;
    let output = shapes[graph.output].clone();
    output_demand.projected_shape(&output)?;
    let mut bodies = (0..graph.nodes.len()).map(|_| Vec::new()).collect::<Vec<_>>();

    let mut demands = vec![Demand::None; graph.nodes.len()];
    demands[graph.output] = output_demand.clone();
    // An unused node must not contribute demand, even if its fallback rule would demand All.
    let mut live = vec![false; graph.nodes.len()];
    live[graph.output] = true;
    for (id, node) in graph.nodes.iter().enumerate().rev() {
        if !live[id] {
            continue;
        }
        demands[id] = demands[id].clone().canonical(&shapes[id]);
        let demand = demands[id].clone();
        // Every value in this body has a row-count anchor. No payload computation
        // is needed when only that count is requested.
        if demand.projected_shape(&shapes[id])? == Shape::Unit {
            continue;
        }
        for &edge in &node.inputs { live[edge] = true; }
        match &node.kind {
            NodeKind::Input => {
                // Body inputs may be references made by an upstream graph node.
                // Only the public boundary must keep an incoming arena whole.
                if incoming { demands[id] = keep_references(&shapes[id], demand).canonical(&shapes[id]); }
            }
            NodeKind::Tuple => {
                for (&edge, field) in node.inputs.iter().zip(demand.fields(node.inputs.len())) {
                    demands[edge].merge_unchecked(field);
                }
            }
            NodeKind::Op(NumOp::Core(Op::Field(field))) => {
                let Shape::Prod(fields) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let mut needs = vec![Demand::None; fields.len()];
                needs[*field] = demand;
                demands[node.inputs[0]].merge_unchecked(Demand::Prod(needs));
            }
            NodeKind::Op(NumOp::Core(Op::Unit | Op::Lit(_))) => {}
            NodeKind::Op(NumOp::Core(Op::Enlist)) => {
                demands[node.inputs[0]].merge_unchecked(demand.list_element());
            }
            NodeKind::Op(NumOp::Core(Op::Ref)) => {
                demands[node.inputs[0]].merge_unchecked(before_ref(&shapes[node.inputs[0]], demand));
            }
            NodeKind::Op(NumOp::Core(Op::Len)) => {
                let need = match &shapes[node.inputs[0]] {
                    Shape::List(_) => Demand::List(Box::new(Demand::None)),
                    Shape::Ref(_) => Demand::Ref(Box::new(Demand::List(Box::new(Demand::None)))),
                    _ => unreachable!("Len input is a list or a reference to a list"),
                };
                demands[node.inputs[0]].merge_unchecked(need);
            }
            NodeKind::Op(NumOp::Core(Op::MapList(body))) => {
                let Shape::List(element) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let analysis = analyze(body, element, &demand.list_element(), false)?;
                demands[node.inputs[0]].merge_unchecked(Demand::List(Box::new(analysis.input.clone())));
                bodies[id].push((0, analysis));
            }
            NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
                let Shape::Sum(lanes) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let mut needs = demand.sum_lanes(lanes.len());
                for (tag, body) in arms {
                    let analysis = analyze(body, &lanes[*tag], &needs[*tag], false)?;
                    needs[*tag] = analysis.input.clone();
                    bodies[id].push((*tag, analysis));
                }
                demands[node.inputs[0]].merge_unchecked(Demand::Sum(needs));
            }
            NodeKind::Op(_) => demands[node.inputs[0]].merge_unchecked(Demand::All),
        }
    }

    let input = graph.nodes.iter().position(|node| matches!(node.kind, NodeKind::Input))
        .map(|id| demands[id].clone()).unwrap_or(Demand::None);
    Ok(DemandAnalysis { input, output, demands, shapes, live, bodies })
}

/// Incoming references have no field-access operation on their arenas.
fn keep_references(shape: &Shape, need: Demand) -> Demand {
    match (shape, need) {
        (_, Demand::None) => Demand::None,
        (Shape::Ref(_), _) => Demand::All,
        (Shape::Prod(fs), Demand::Prod(ds)) => Demand::Prod(fs.iter().zip(ds).map(|(s,d)| keep_references(s,d)).collect()),
        (Shape::List(s), Demand::List(d)) => Demand::List(Box::new(keep_references(s,*d))),
        (Shape::Sum(fs), Demand::Sum(ds)) => Demand::Sum(fs.iter().zip(ds).map(|(s,d)| keep_references(s,d)).collect()),
        (_, need) => need,
    }
}
