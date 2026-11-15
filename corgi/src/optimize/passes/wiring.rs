//! Check the original graph while simplifying its typed wiring.
use crate::graph::{Graph, Node, NodeKind, OpLike};
use crate::ops::{NumOp, Op};
use crate::value::Value;

pub(in crate::optimize) fn typed_wiring(g: Graph<NumOp>, input: &crate::Shape) -> Result<Graph<NumOp>, String> {
    Ok(checked(g, Value::empty(input))?.0)
}

fn check_body(g: &mut Graph<NumOp>, input: Value) -> Result<Value, String> {
    let owned = Graph { nodes: std::mem::take(&mut g.nodes), output: g.output };
    let (graph, output) = checked(owned, input)?;
    *g = graph;
    Ok(output)
}

fn checked(g: Graph<NumOp>, input: Value) -> Result<(Graph<NumOp>, Value), String> {
    let output = g.output;
    let mut values: Vec<Value> = Vec::with_capacity(g.nodes.len());
    let mut built: Vec<Node<NumOp>> = Vec::with_capacity(g.nodes.len());
    let mut remap = vec![0; g.nodes.len()];
    let mut origin = Vec::with_capacity(g.nodes.len());
    let mut root = None;
    for (old, mut node) in g.nodes.into_iter().enumerate() {
        // Keep the original edges for checking, even when a rewrite makes their values dead.
        let value = match &mut node.kind {
            NodeKind::Input => input.clone(),
            NodeKind::Tuple if node.inputs.is_empty() => Value::Unit(0),
            NodeKind::Tuple => Value::Prod(node.inputs.iter().map(|&e| values[e].clone()).collect()),
            NodeKind::Op(op) => {
                let arg = &values[node.inputs[0]];
                let mut checked_bodies = Vec::new();
                match (op, arg) {
                    (NumOp::Core(Op::MapList(b)), Value::List(_, xs)) => checked_bodies.push(check_body(b, *xs.clone())?),
                    (NumOp::Core(Op::MapSum(arms)), Value::Sum(_, lanes)) => {
                        for (tag, b) in arms {
                            if let Some(lane) = lanes.get(*tag) { checked_bodies.push(check_body(b, lane.clone())?); }
                        }
                    }
                    (NumOp::Core(Op::Fold(b) | Op::FoldScan(b)), Value::Prod(pair)) if pair.len() == 2 => {
                        if let Value::List(_, xs) = &pair[1] {
                            checked_bodies.push(check_body(b, Value::Prod(vec![pair[0].clone(), *xs.clone()]))?);
                        }
                    }
                    _ => {}
                }
                let NodeKind::Op(op) = &node.kind else { unreachable!() };
                let mut checked_bodies = checked_bodies.into_iter();
                match op {
                    NumOp::Core(op) => op.eval_with(arg.clone(), &mut |_, _| {
                        Ok(checked_bodies.next().expect("one empty evaluation per body"))
                    })?,
                    op => op.eval(arg.clone())?,
                }
            }
        };
        values.push(value);
        for e in &mut node.inputs { *e = remap[*e]; }
        let redirect = match &node.kind {
            NodeKind::Op(NumOp::Core(Op::Pattern(_))) => Some(node.inputs[0]),
            NodeKind::Op(NumOp::Core(Op::Field(i))) => {
                let source = &built[node.inputs[0]];
                matches!(source.kind, NodeKind::Tuple).then(|| source.inputs[*i])
            }
            NodeKind::Tuple if !node.inputs.is_empty() => {
                // Unit positions can come from any producer; prefer the other fields' source.
                let parent = node.inputs.iter()
                    .filter(|&&e| !matches!(values[origin[e]], Value::Unit(_)))
                    .chain(node.inputs.iter()).find_map(|&e| {
                    matches!(built[e].kind, NodeKind::Op(NumOp::Core(Op::Field(_))))
                        .then(|| built[e].inputs[0])
                });
                let eta = |p: usize| {
                    let Value::Prod(fields) = &values[origin[p]] else { return false };
                    fields.len() == node.inputs.len() && node.inputs.iter().enumerate().all(|(i, &e)| {
                        matches!(built[e].kind, NodeKind::Op(NumOp::Core(Op::Field(j)))
                            if i == j && built[e].inputs[0] == p)
                        || (matches!(fields[i], Value::Unit(_)) && matches!(values[origin[e]], Value::Unit(_)))
                    })
                };
                parent.filter(|&p| eta(p)).or_else(|| root.filter(|&p| eta(p)))
            }
            NodeKind::Op(NumOp::Core(Op::MapList(b)))
                if matches!(b.nodes[b.output].kind, NodeKind::Input) => Some(node.inputs[0]),
            _ => None,
        };
        remap[old] = if let Some(id) = redirect { id } else {
            let id = built.len();
            if matches!(node.kind, NodeKind::Input) { root = Some(id); }
            built.push(node);
            origin.push(old);
            id
        };
    }
    // Eta must see the fields before known-unit fields become unit producers.
    for (node, &old) in built.iter_mut().zip(&origin) {
        if matches!(node.kind, NodeKind::Op(NumOp::Core(Op::Field(_)))) && matches!(values[old], Value::Unit(_)) {
            node.kind = NodeKind::Op(NumOp::Core(Op::Unit));
            node.inputs[0] = root.unwrap_or(node.inputs[0]);
        }
    }
    let g = Graph { nodes: built, output: remap[g.output] };
    Ok((g, values.swap_remove(output)))
}
