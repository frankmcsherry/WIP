//! Build each producer once at the union of its readers' requests.
//! Readers use field access in that producer's compact type.
use crate::demand::{before_ref, DemandAnalysis};
use crate::graph::NodeKind;
use crate::{Builder, Demand, Graph, NumOp, Op, Shape};

/// Read `wanted` from a value stored at `available`, both in original coordinates.
/// An existing reference cannot be narrowed by field access: report what remains.
fn project(
    b: &mut Builder<NumOp>, source: usize, shape: &Shape,
    available: &Demand, wanted: &Demand,
) -> Result<(usize, Demand), String> {
    let available = available.clone().canonical(shape);
    let wanted = wanted.clone().canonical(shape);
    let (value, actual) = project_inner(b, source, shape, &available, &wanted)?;
    Ok((value, actual.canonical(shape)))
}

fn project_inner(
    b: &mut Builder<NumOp>, source: usize, shape: &Shape,
    available: &Demand, wanted: &Demand,
) -> Result<(usize, Demand), String> {
    if available.is_all(shape) && wanted.is_all(shape) { return Ok((source, Demand::All)); }
    if available == wanted || *wanted == Demand::All {
        return Ok((source, available.clone()));
    }
    if *wanted == Demand::None {
        return Ok((b.add(Op::Unit, vec![source]), Demand::None));
    }
    match (shape, wanted) {
        (Shape::Prod(fields), Demand::Prod(needs)) => {
            let have = available.clone().fields(fields.len());
            let mut index = 0;
            let mut kept = Vec::new();
            let mut actual = vec![Demand::None; fields.len()];
            for (i, ((shape, have), need)) in fields.iter().zip(&have).zip(needs).enumerate() {
                if *need != Demand::None {
                    let field = b.add(Op::Field(index), vec![source]);
                    let (field, need) = project(b, field, shape, have, need)?;
                    kept.push(field);
                    actual[i] = need;
                }
                if *have != Demand::None { index += 1; }
            }
            let result = if kept.is_empty() { b.add(Op::Unit, vec![source]) } else { b.tuple(kept) };
            Ok((result, Demand::Prod(actual)))
        }
        (Shape::List(element), Demand::List(need)) => {
            let mut body = Builder::default();
            let x = body.input();
            let (out, actual) = project(&mut body, x, element, &available.clone().list_element(), need)?;
            Ok((b.add(Op::MapList(Box::new(body.finish(out))), vec![source]), Demand::List(Box::new(actual))))
        }
        (Shape::Sum(lanes), Demand::Sum(needs)) => {
            let have = available.clone().sum_lanes(lanes.len());
            let mut arms = Vec::new();
            let mut actual = Vec::new();
            for (tag, ((shape, have), need)) in lanes.iter().zip(&have).zip(needs).enumerate() {
                let mut body = Builder::default();
                let x = body.input();
                let (out, need) = project(&mut body, x, shape, have, need)?;
                if out != x { arms.push((tag, body.finish(out))); }
                actual.push(need);
            }
            let result = if arms.is_empty() { source } else { b.add(Op::MapSum(arms), vec![source]) };
            Ok((result, Demand::Sum(actual)))
        }
        (Shape::Ref(_), Demand::Ref(_)) => Ok((source, available.clone())),
        _ => Err(format!("cannot read {wanted:?} from {available:?} at {shape}")),
    }
}

fn after_ref(shape: &Shape, need: Demand) -> Demand {
    match (shape, need) {
        (Shape::List(_), need @ Demand::List(_)) => Demand::Ref(Box::new(need)),
        (Shape::Prod(fs), Demand::Prod(ds)) => Demand::Prod(fs.iter().zip(ds).map(|(s,d)| after_ref(s,d)).collect()),
        (Shape::Sum(fs), Demand::Sum(ds)) => Demand::Sum(fs.iter().zip(ds).map(|(s,d)| after_ref(s,d)).collect()),
        (_, need) => need,
    }
}

/// A kernel's fixed operand slots remain present even when their payload is unit.
fn operands(
    b: &mut Builder<NumOp>, source: usize, shape: &Shape, have: &Demand, needs: &[Demand],
) -> Result<(Vec<usize>, Vec<Demand>), String> {
    let Shape::Prod(fields) = shape else { unreachable!("checked kernel operands") };
    let ds = have.clone().fields(fields.len());
    let mut index = 0;
    let mut nodes = Vec::new();
    let mut actual = Vec::new();
    for ((shape, producer), need) in fields.iter().zip(&ds).zip(needs) {
        let (node, d) = if *need == Demand::None {
            (b.add(Op::Unit, vec![source]), Demand::None)
        } else {
            let field = b.add(Op::Field(index), vec![source]);
            project(b, field, shape, producer, need)?
        };
        nodes.push(node); actual.push(d);
        index += usize::from(*producer != Demand::None);
    }
    Ok((nodes, actual))
}

// Gather retains the position tree even when its payload becomes unit. Its
// fixed product positions are separate from the compact product inside a leaf.
fn gather_result(
    b: &mut Builder<NumOp>, source: usize, positions: &Shape, element: &Shape,
    have: &Demand, need: &Demand,
) -> Result<(usize, Demand), String> {
    if *need == Demand::None { return Ok((b.add(Op::Unit, vec![source]), Demand::None)); }
    match positions {
        Shape::Int => project(b, source, element, have, need),
        Shape::Unit => Ok((source, need.clone())),
        Shape::Prod(fields) => {
            let needs = need.clone().fields(fields.len());
            let mut kept = Vec::new();
            let mut ds = vec![Demand::None; fields.len()];
            for (i, (shape, need)) in fields.iter().zip(&needs).enumerate() {
                if *need != Demand::None {
                    let field = b.add(Op::Field(i), vec![source]);
                    let (value, d) = gather_result(b, field, shape, element, have, need)?;
                    kept.push(value); ds[i] = d;
                }
            }
            let value = if kept.is_empty() && *need != Demand::All { b.add(Op::Unit, vec![source]) } else { b.tuple(kept) };
            Ok((value, if *need == Demand::All { Demand::All } else { Demand::Prod(ds) }))
        }
        Shape::List(positions) => {
            let mut body = Builder::default(); let x = body.input();
            let (out, d) = gather_result(&mut body, x, positions, element, have, &need.clone().list_element())?;
            Ok((b.add(Op::MapList(Box::new(body.finish(out))), vec![source]), Demand::List(Box::new(d))))
        }
        Shape::Sum(lanes) => {
            let needs = need.clone().sum_lanes(lanes.len());
            let mut arms = Vec::new(); let mut ds = Vec::new();
            for (tag, (shape, need)) in lanes.iter().zip(&needs).enumerate() {
                let mut body = Builder::default(); let x = body.input();
                let (out, d) = gather_result(&mut body, x, shape, element, have, need)?;
                if out != x { arms.push((tag, body.finish(out))); }
                ds.push(d);
            }
            Ok((b.add(Op::MapSum(arms), vec![source]), Demand::Sum(ds)))
        }
        _ => unreachable!("checked position tree"),
    }
}

fn movement(
    b: &mut Builder<NumOp>, op: Op<NumOp>, source: usize, input: &Shape,
    output: &Shape, have: &Demand, need: &Demand,
) -> Result<(usize, Demand), String> {
    use Demand::{All, None as Unread, List, Prod};
    let wanted = crate::demand::movement_demand(&op, input, need.clone()).unwrap();
    if have.is_all(input) && need.is_all(output) && wanted.is_all(input) {
        return Ok((b.add(op, vec![source]), All));
    }
    match op {
        Op::Filter if need.clone().list_element() != Unread => {
            let (source, d) = project(b, source, input, have, &wanted)?;
            let payload = d.list_element().fields(2).pop().unwrap();
            Ok((b.add(Op::Filter, vec![source]), List(Box::new(payload))))
        }
        Op::Filter => {
            let Shape::List(element) = input else { unreachable!() };
            let mut body = Builder::default(); let x = body.input();
            let (fields, ds) = operands(&mut body, x, element, &have.clone().list_element(),
                &[All, need.clone().list_element()])?;
            let pair = body.tuple(fields);
            let value = b.add(Op::MapList(Box::new(body.finish(pair))), vec![source]);
            let value = b.add(Op::Filter, vec![value]);
            Ok((value, List(Box::new(ds[1].clone()))))
        }
        Op::CapList => {
            let (fields, ds) = operands(b, source, input, have, &wanted.fields(2))?;
            let pair = b.tuple(fields);
            let value = b.add(Op::CapList, vec![pair]);
            let Shape::Prod(shapes) = input else { unreachable!() };
            let Shape::List(element) = &shapes[1] else { unreachable!() };
            let children = vec![ds[0].clone(), ds[1].clone().list_element()];
            let kernel_shape = Shape::List(Box::new(Shape::Prod(vec![
                children[0].projected_shape(&shapes[0])?, children[1].projected_shape(element)?,
            ])));
            let lane = need.clone().list_element();
            let local = if lane == Unread { Unread } else {
                Prod(lane.clone().fields(2).iter().map(|d| if *d == Unread { Unread } else { All }).collect())
            };
            let (value, projected) = project(b, value, &kernel_shape, &All, &List(Box::new(local)))?;
            let projected = projected.list_element();
            let actual = if projected == Unread { Unread } else {
                Prod(projected.fields(2).into_iter().zip(children)
                    .map(|(kept, child)| if kept == Unread { Unread } else { child }).collect())
            };
            Ok((value, List(Box::new(actual))))
        }
        Op::Select => {
            let (fields, ds) = operands(b, source, input, have, &wanted.fields(3))?;
            debug_assert_eq!(ds[1], ds[2], "Select alternatives must retain the same demand");
            let args = b.tuple(fields);
            let value = b.add(Op::Select, vec![args]);
            project(b, value, output, &ds[1], need)
        }
        Op::Gather => {
            let (fields, ds) = operands(b, source, input, have, &wanted.fields(2))?;
            let pair = b.tuple(fields);
            let value = b.add(Op::Gather, vec![pair]);
            let Shape::Prod(shapes) = input else { unreachable!() };
            let list = match &ds[1] { Demand::Ref(d) => (**d).clone(), d => d.clone() };
            let haystack = if let Shape::Ref(s) = &shapes[1] { &**s } else { &shapes[1] };
            let Shape::List(element) = haystack else { unreachable!() };
            gather_result(b, value, &shapes[0], element, &list.list_element(), need)
        }
        Op::Flatten => {
            let (source, ds) = project(b, source, input, have, &wanted)?;
            let value = b.add(Op::Flatten, vec![source]);
            let element = ds.list_element().list_element();
            let Shape::Prod(shapes) = output else { unreachable!() };
            let values = List(Box::new(element));
            let kernel_shape = Shape::Prod(vec![shapes[0].clone(), values.projected_shape(&shapes[1])?]);
            let needs = need.clone().fields(2);
            let local = Prod(vec![needs[0].clone(), if needs[1] == Unread { Unread } else { All }]);
            let (value, projected) = project(b, value, &kernel_shape, &All, &local)?;
            let mut actual = projected.fields(2);
            if actual[1] != Unread { actual[1] = values; }
            Ok((value, Prod(actual)))
        }
        _ => unreachable!("movement rule"),
    }
}

pub(crate) fn rewrite_demand(
    graph: Graph<NumOp>, analysis: &DemandAnalysis, provided_input: &Demand,
) -> Result<(Graph<NumOp>, Demand), String> {
    let mut b = Builder::default();
    let root = b.input();
    let mut built = vec![root; graph.nodes.len()];
    let mut actual = vec![Demand::None; graph.nodes.len()];
    let mut unit = None;
    for (id, node) in graph.nodes.into_iter().enumerate() {
        let need = &analysis.demands[id];
        if !analysis.live[id] && !matches!(node.kind, NodeKind::Input) { continue; }
        if need.projected_shape(&analysis.shapes[id])? == Shape::Unit {
            actual[id] = need.clone();
            built[id] = *unit.get_or_insert_with(|| b.add(Op::Unit, vec![root]));
            continue;
        }
        let result = match node.kind {
            NodeKind::Input => (root, provided_input.clone()),
            NodeKind::Tuple => {
                let needs = need.clone().fields(node.inputs.len());
                let mut fields = Vec::new();
                let mut ds = vec![Demand::None; needs.len()];
                for (i, (&edge, need)) in node.inputs.iter().zip(&needs).enumerate() {
                    if *need != Demand::None {
                        let (field, d) = project(&mut b, built[edge], &analysis.shapes[edge], &actual[edge], need)?;
                        fields.push(field); ds[i] = d;
                    }
                }
                let out = if fields.is_empty() && *need != Demand::All { b.add(Op::Unit, vec![root]) } else { b.tuple(fields) };
                (out, if *need == Demand::All { Demand::All } else { Demand::Prod(ds) })
            }
            NodeKind::Op(op) => {
                let edge = node.inputs[0];
                let source = built[edge];
                let shape = &analysis.shapes[edge];
                let have = &actual[edge];
                match op {
                    NumOp::Core(Op::Field(field)) => {
                        let Shape::Prod(fields) = shape else { unreachable!() };
                        let ds = have.clone().fields(fields.len());
                        let index = ds[..field].iter().filter(|d| **d != Demand::None).count();
                        let field_node = b.add(Op::Field(index), vec![source]);
                        project(&mut b, field_node, &fields[field], &ds[field], need)?
                    }
                    NumOp::Core(Op::Unit) => (b.add(Op::Unit, vec![root]), need.clone()),
                    NumOp::Core(Op::Lit(value)) => (b.add(Op::Lit(need.project(value)?), vec![root]), need.clone()),
                    NumOp::Core(Op::Enlist) => {
                        let (value, d) = project(&mut b, source, shape, have, &need.clone().list_element())?;
                        (b.add(Op::Enlist, vec![value]), Demand::List(Box::new(d)))
                    }
                    NumOp::Core(Op::Ref) => {
                        let (value, d) = project(&mut b, source, shape, have, &before_ref(shape, need.clone()))?;
                        (b.add(Op::Ref, vec![value]), after_ref(shape, d))
                    }
                    NumOp::Core(Op::Len) => (b.add(Op::Len, vec![source]), Demand::All),
                    NumOp::Core(Op::MapList(body)) => {
                        let a = &analysis.bodies[id][0].1;
                        let wanted = Demand::List(Box::new(a.input.clone()));
                        let (value, d) = project(&mut b, source, shape, have, &wanted)?;
                        let (body, output) = rewrite_demand(*body, a, &d.list_element())?;
                        (b.add(Op::MapList(Box::new(body)), vec![value]), Demand::List(Box::new(output)))
                    }
                    NumOp::Core(Op::MapSum(arms)) => {
                        let Shape::Sum(lanes) = shape else { unreachable!() };
                        let mut needs = need.clone().sum_lanes(lanes.len());
                        for (tag, a) in &analysis.bodies[id] { needs[*tag] = a.input.clone(); }
                        let (value, d) = project(&mut b, source, shape, have, &Demand::Sum(needs))?;
                        let mut ds = d.sum_lanes(lanes.len());
                        let mut new_arms = Vec::new();
                        for ((tag, body), (_, a)) in arms.into_iter().zip(&analysis.bodies[id]) {
                            let (body, output) = rewrite_demand(body, a, &ds[tag])?;
                            ds[tag] = output; new_arms.push((tag, body));
                        }
                        (b.add(Op::MapSum(new_arms), vec![value]), Demand::Sum(ds))
                    }
                    NumOp::Core(op @ (Op::Filter | Op::CapList | Op::Select | Op::Gather | Op::Flatten)) => {
                        movement(&mut b, op, source, shape, &analysis.shapes[id], have, need)?
                    }
                    op => {
                        let result = b.add(op, vec![source]);
                        project(&mut b, result, &analysis.shapes[id], &Demand::All, need)?
                    }
                }
            }
        };
        built[id] = result.0;
        actual[id] = result.1.canonical(&analysis.shapes[id]);
    }
    Ok((b.finish(built[graph.output]), actual[graph.output].clone()))
}
