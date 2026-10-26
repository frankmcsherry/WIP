//! Input-demand analysis. This module builds no graphs and performs no rewriting.
//! Unread payloads need only row counts. Container demands retain bounds, tags and references.
//! Backward rules union demands at shared uses; unknown operations demand their complete input.
//! Shape validation remains the type checker's responsibility.

use crate::graph::{node_shapes, Graph, NodeKind};
use crate::ops::{NumOp, Op, Pattern};
use crate::shape::Shape;

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

fn pattern_requirement(pattern: &Pattern) -> Demand {
    match pattern {
        Pattern::Any => Demand::None,
        Pattern::Tuple(fields) if fields.is_empty() => Demand::None,
        Pattern::Tuple(fields) => Demand::Prod(fields.iter().map(pattern_requirement).collect()),
        Pattern::List(element) => Demand::List(Box::new(pattern_requirement(element))),
        Pattern::Sum(lanes) => Demand::Sum(lanes.iter().map(pattern_requirement).collect()),
        Pattern::Ref(list) => {
            let need = match pattern_requirement(list) {
                Demand::None => Demand::List(Box::new(Demand::None)),
                need => need,
            };
            Demand::Ref(Box::new(need))
        }
        _ => Demand::All,
    }
}

/// The input shape a `Project` target requires, with every erased subtree reduced to row count.
fn shape_requirement(shape: &Shape) -> Demand {
    match shape {
        Shape::Unit => Demand::None,
        Shape::Int | Shape::Float => Demand::All,
        Shape::Prod(fields) => Demand::Prod(fields.iter().map(shape_requirement).collect()),
        Shape::List(element) => Demand::List(Box::new(shape_requirement(element))),
        Shape::Sum(lanes) => Demand::Sum(lanes.iter().map(shape_requirement).collect()),
        Shape::Ref(list) => Demand::Ref(Box::new(shape_requirement(list))),
    }
}

impl Demand {
    /// The demand represented by a projection target, where `Unit` erases a subtree.
    pub fn from_projection(target: &Shape) -> Self {
        shape_requirement(target)
    }

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
        left.to_shape(original)?;
        right.to_shape(original)?;
        let mut merged = left.clone();
        merged.merge(right.clone())?;
        let projected = merged.to_shape(original)?;
        Ok((merged, projected))
    }

    /// Apply this demand to the original shape, replacing unread subtrees with `Unit`.
    pub fn to_shape(&self, original: &Shape) -> Result<Shape, String> {
        projected_shape(original, self)
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

    fn merge_unchecked(&mut self, other: Demand) {
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

    fn fields(self, count: usize) -> Vec<Demand> {
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

    fn list_element(self) -> Demand {
        match self {
            Demand::None | Demand::All => self,
            Demand::List(element) => *element,
            _ => unreachable!("a list demand must be for a list"),
        }
    }

    fn sum_lanes(self, count: usize) -> Vec<Demand> {
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
fn before_ref(shape: &Shape, demand: Demand) -> Demand {
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

/// Gather replaces every integer in its position tree with one haystack element.
/// Different leaves can observe different parts of that element, so combine their demands.
fn gathered_element_demand(positions: &Shape, demand: Demand) -> Demand {
    match positions {
        Shape::Int => demand,
        Shape::Unit => Demand::None,
        Shape::Prod(fields) => fields.iter().zip(demand.fields(fields.len()))
            .fold(Demand::None, |mut need, (field, demand)| {
                need.merge_unchecked(gathered_element_demand(field, demand));
                need
            }),
        Shape::List(element) => gathered_element_demand(element, demand.list_element()),
        Shape::Sum(lanes) => lanes.iter().zip(demand.sum_lanes(lanes.len()))
            .fold(Demand::None, |mut need, (lane, demand)| {
                need.merge_unchecked(gathered_element_demand(lane, demand));
                need
            }),
        _ => unreachable!("Gather positions contain only integers and containers"),
    }
}

/// Payload projections commute with these operations, including Gather's zero on a miss.
/// Keep the witnesses that decide which rows move, even when no payload is needed.
pub(crate) fn movement_demand(op: &Op<NumOp>, input: &Shape, demand: Demand) -> Option<Demand> {
    Some(match op {
        Op::Gather => {
            let Shape::Prod(pair) = input else { unreachable!() };
            let element = gathered_element_demand(&pair[0], demand);
            let list = Demand::List(Box::new(element));
            let haystack = if matches!(pair[1], Shape::Ref(_)) { Demand::Ref(Box::new(list)) } else { list };
            Demand::Prod(vec![Demand::All, haystack])
        }
        Op::Filter => Demand::List(Box::new(Demand::Prod(vec![Demand::All, demand.list_element()]))),
        Op::CapList => {
            let mut pair = demand.list_element().fields(2);
            let element = pair.pop().unwrap();
            Demand::Prod(vec![pair.pop().unwrap(), Demand::List(Box::new(element))])
        }
        Op::Select => Demand::Prod(vec![Demand::All, demand.clone(), demand]),
        _ => return None,
    })
}

pub(crate) fn input_demand(
    graph: &Graph<NumOp>,
    input: &Shape,
    output_demand: Demand,
) -> Result<(Demand, Shape), String> {
    let shapes = node_shapes(graph, input)?;
    let output = shapes[graph.output].clone();

    let mut demands = vec![Demand::None; graph.nodes.len()];
    demands[graph.output] = output_demand;
    // An unused node must not contribute demand, even if its fallback rule would demand All.
    let mut live = vec![false; graph.nodes.len()];
    live[graph.output] = true;
    for (id, node) in graph.nodes.iter().enumerate().rev() {
        if !live[id] {
            continue;
        }
        for &edge in &node.inputs {
            live[edge] = true;
        }
        let demand = demands[id].clone();
        match &node.kind {
            NodeKind::Input => {}
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
            NodeKind::Op(NumOp::Core(Op::Pattern(pattern))) => {
                let mut need = demand;
                need.merge_unchecked(pattern_requirement(pattern));
                demands[node.inputs[0]].merge_unchecked(need);
            }
            NodeKind::Op(NumOp::Core(Op::Project(target))) => {
                demands[node.inputs[0]].merge_unchecked(shape_requirement(target));
            }
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
                let (need, _) = input_demand(body, element, demand.list_element())?;
                demands[node.inputs[0]].merge_unchecked(Demand::List(Box::new(need)));
            }
            NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
                let Shape::Sum(lanes) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let mut needs = demand.sum_lanes(lanes.len());
                for (tag, body) in arms {
                    let (need, _) = input_demand(body, &lanes[*tag], needs[*tag].clone())?;
                    needs[*tag] = need;
                }
                demands[node.inputs[0]].merge_unchecked(Demand::Sum(needs));
            }
            NodeKind::Op(NumOp::Core(op)) if matches!(op, Op::Gather | Op::Filter | Op::CapList | Op::Select) => {
                let source = node.inputs[0];
                demands[source].merge_unchecked(movement_demand(op, &shapes[source], demand).unwrap());
            }
            NodeKind::Op(_) => demands[node.inputs[0]].merge_unchecked(Demand::All),
        }
    }

    let need = graph.nodes.iter().position(|node| matches!(node.kind, NodeKind::Input))
        .map(|node| demands[node].clone()).unwrap_or(Demand::None);
    Ok((need, output))
}

fn projected_shape(shape: &Shape, demand: &Demand) -> Result<Shape, String> {
    match (shape, demand) {
        (_, Demand::All) => Ok(shape.clone()),
        (_, Demand::None) => Ok(Shape::Unit),
        (Shape::Prod(fields), Demand::Prod(needs)) => {
            if fields.len() != needs.len() {
                return Err("product demand has the wrong number of fields".into());
            }
            Ok(Shape::Prod(
                fields
                    .iter()
                    .zip(needs)
                    .map(|(shape, need)| projected_shape(shape, need))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Shape::List(element), Demand::List(need)) => {
            Ok(Shape::List(Box::new(projected_shape(element, need)?)))
        }
        (Shape::Ref(list), Demand::Ref(need)) => {
            Ok(Shape::Ref(Box::new(projected_shape(list, need)?)))
        }
        (Shape::Sum(lanes), Demand::Sum(needs)) => {
            if lanes.len() != needs.len() {
                return Err("sum demand has the wrong number of lanes".into());
            }
            Ok(Shape::Sum(
                lanes
                    .iter()
                    .zip(needs)
                    .map(|(shape, need)| projected_shape(shape, need))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        _ => Err(format!("demand {demand:?} does not match shape {shape}")),
    }
}
