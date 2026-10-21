//! Find input columns that a graph cannot observe, and erase their payloads.
//!
//! A demand describes the part of a value needed by a graph's output.
//! `None` needs only row count, `All` needs the value, and container demands keep their structure with demands for their contents.
//! Backward propagation splits tuple demands, selects fields, follows `Enlist` and `Ref`, and enters the closed bodies of list and sum maps.
//! Demands from multiple uses meet by union.
//! Reachability is tracked separately because a live value needed only for its row count also has `None` demand.
//! Operations without a proved propagation rule demand their whole input.
//! A host kernel therefore demands all columns explicitly passed to it, while surrounding tuple and field wiring can discard other source columns.
//! `Len` demands list bounds but not their payload, including when it reads referenced rows.
//! Appending `Project` to an expression states which output fields matter, so the same pass can return its input demand.
//!
//! A projection replaces `None` subtrees with `Unit` while retaining product positions, list bounds, sum tags, and reference row numbers.
//! `Op::Project` consumes a column and reuses retained buffers, so its work depends on shape structure rather than row count.
//! The final typecheck rejects a projected shape that the original graph cannot accept.
//! The propagation rules establish value preservation; matching output shapes alone would not establish it.
//! The route toward syntactic minimality is an exact backward rule for each operation, followed by union at shared nodes.
//! No rule inspects row data or infers algebraic identities.
//! This pass does not yet claim a minimal demand under all Corgi operations.

use crate::graph::{shape_of, Builder, Graph, NodeKind, OpLike};
use crate::ops::{NumOp, Op, Pattern};
use crate::shape::{shape_of_value, Shape};
use crate::value::Value;

#[derive(Clone)]
enum Demand {
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
    fn merge(&mut self, other: Demand) {
        match (&mut *self, other) {
            (Demand::All, _) | (_, Demand::None) => {}
            (here, Demand::All) => *here = Demand::All,
            (here @ Demand::None, other) => *here = other,
            (Demand::Prod(left), Demand::Prod(right)) => {
                assert_eq!(left.len(), right.len());
                for (l, r) in left.iter_mut().zip(right) {
                    l.merge(r);
                }
            }
            (Demand::List(left), Demand::List(right)) => left.merge(*right),
            (Demand::Ref(left), Demand::Ref(right)) => left.merge(*right),
            (Demand::Sum(left), Demand::Sum(right)) => {
                assert_eq!(left.len(), right.len());
                for (l, r) in left.iter_mut().zip(right) {
                    l.merge(r);
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

/// An input operation that replaces unread payloads with `()`, if any exist.
///
/// `graph` must typecheck on `input`.
/// The returned graph accepts `input` and produces a simpler shape on which `graph` still typechecks and has the same output for every value.
/// Other operations are treated as reading their whole input until their demand rules are known.
pub fn demand_projection(
    graph: &Graph<NumOp>,
    input: &Shape,
) -> Result<Option<Graph<NumOp>>, String> {
    let (need, output) = input_demand(graph, input, Demand::All)?;
    let projected = projected_shape(input, &need);
    if projected == *input {
        return Ok(None);
    }
    let mut builder = Builder::default();
    let root = builder.input();
    let result = builder.add(Op::Project(projected.clone()), vec![root]);
    let projection = builder.finish(result);
    // An op may have a shape constraint beyond the values it reads.
    // Refuse the projection unless the subject still typechecks and keeps its original output shape.
    if shape_of(graph, &projected).as_ref() != Ok(&output) {
        return Ok(None);
    }
    Ok(Some(projection))
}

fn input_demand(
    graph: &Graph<NumOp>,
    input: &Shape,
    output_demand: Demand,
) -> Result<(Demand, Shape), String> {
    // Keep each intermediate shape: a Field needs the arity of its own input, not the root's.
    let mut values: Vec<Value> = Vec::with_capacity(graph.nodes.len());
    for node in &graph.nodes {
        let value = match &node.kind {
            NodeKind::Input => Value::empty(input),
            NodeKind::Tuple => {
                Value::Prod(node.inputs.iter().map(|&i| values[i].clone()).collect())
            }
            NodeKind::Op(op) => op.eval(values[node.inputs[0]].clone())?,
        };
        values.push(value);
    }
    let shapes: Vec<_> = values.iter().map(shape_of_value).collect();
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
                    demands[edge].merge(field);
                }
            }
            NodeKind::Op(NumOp::Core(Op::Field(field))) => {
                let Shape::Prod(fields) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let mut needs = vec![Demand::None; fields.len()];
                needs[*field] = demand;
                demands[node.inputs[0]].merge(Demand::Prod(needs));
            }
            NodeKind::Op(NumOp::Core(Op::Unit | Op::Lit(_))) => {}
            NodeKind::Op(NumOp::Core(Op::Pattern(pattern))) => {
                let mut need = demand;
                need.merge(pattern_requirement(pattern));
                demands[node.inputs[0]].merge(need);
            }
            NodeKind::Op(NumOp::Core(Op::Project(target))) => {
                demands[node.inputs[0]].merge(shape_requirement(target));
            }
            NodeKind::Op(NumOp::Core(Op::Enlist)) => {
                demands[node.inputs[0]].merge(demand.list_element());
            }
            NodeKind::Op(NumOp::Core(Op::Ref)) => {
                demands[node.inputs[0]].merge(before_ref(&shapes[node.inputs[0]], demand));
            }
            NodeKind::Op(NumOp::Core(Op::Len)) => {
                let need = match &shapes[node.inputs[0]] {
                    Shape::List(_) => Demand::List(Box::new(Demand::None)),
                    Shape::Ref(_) => Demand::Ref(Box::new(Demand::List(Box::new(Demand::None)))),
                    _ => unreachable!("Len input is a list or a reference to a list"),
                };
                demands[node.inputs[0]].merge(need);
            }
            NodeKind::Op(NumOp::Core(Op::MapList(body))) => {
                let Shape::List(element) = &shapes[node.inputs[0]] else {
                    unreachable!()
                };
                let (need, _) = input_demand(body, element, demand.list_element())?;
                demands[node.inputs[0]].merge(Demand::List(Box::new(need)));
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
                demands[node.inputs[0]].merge(Demand::Sum(needs));
            }
            NodeKind::Op(_) => demands[node.inputs[0]].merge(Demand::All),
        }
    }

    let input_node = graph
        .nodes
        .iter()
        .position(|node| matches!(node.kind, NodeKind::Input))
        .ok_or("graph has no input")?;
    Ok((demands[input_node].clone(), output))
}

fn projected_shape(shape: &Shape, demand: &Demand) -> Shape {
    match (shape, demand) {
        (_, Demand::All) => shape.clone(),
        (_, Demand::None) => Shape::Unit,
        (Shape::Prod(fields), Demand::Prod(needs)) => {
            assert_eq!(fields.len(), needs.len());
            Shape::Prod(
                fields
                    .iter()
                    .zip(needs)
                    .map(|(shape, need)| projected_shape(shape, need))
                    .collect(),
            )
        }
        (Shape::List(element), Demand::List(need)) => {
            Shape::List(Box::new(projected_shape(element, need)))
        }
        (Shape::Ref(list), Demand::Ref(need)) => {
            Shape::Ref(Box::new(projected_shape(list, need)))
        }
        (Shape::Sum(lanes), Demand::Sum(needs)) => {
            assert_eq!(lanes.len(), needs.len());
            Shape::Sum(
                lanes
                    .iter()
                    .zip(needs)
                    .map(|(shape, need)| projected_shape(shape, need))
                    .collect(),
            )
        }
        _ => unreachable!(),
    }
}
