//! Optimizer passes over `Graph<NumOp>`. Each preserves evaluation. Structural passes
//! (`cse`/`dce`) are vocabulary-agnostic in spirit; the semantic peephole pattern-
//! matches core `Field`/`Tuple`, so it reaches through `NumOp::Core`.

use crate::graph::{Graph, Node, NodeKind};
use crate::ops::{NumOp, Op};
use std::collections::HashMap;

/// recurse a body-bearing core op's sub-graphs through a pass.
fn map_kind(kind: &NodeKind<NumOp>, f: fn(&Graph<NumOp>) -> Graph<NumOp>) -> NodeKind<NumOp> {
    match kind {
        NodeKind::Op(NumOp::Core(Op::MapList(b))) => {
            NodeKind::Op(NumOp::Core(Op::MapList(Box::new(f(b)))))
        }
        NodeKind::Op(NumOp::Core(Op::Fold(b))) => {
            NodeKind::Op(NumOp::Core(Op::Fold(Box::new(f(b)))))
        }
        NodeKind::Op(NumOp::Core(Op::Scan(b))) => {
            NodeKind::Op(NumOp::Core(Op::Scan(Box::new(f(b)))))
        }
        NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
            NodeKind::Op(NumOp::Core(Op::MapSum(arms.iter().map(|(k, b)| (*k, f(b))).collect())))
        }
        other => other.clone(),
    }
}

/// hash-consing: fold structurally-identical nodes into one.
pub fn cse(g: &Graph<NumOp>) -> Graph<NumOp> {
    let mut new_nodes: Vec<Node<NumOp>> = Vec::new();
    let mut remap = vec![0usize; g.nodes.len()];
    let mut seen: HashMap<Node<NumOp>, usize> = HashMap::new();
    for (old, node) in g.nodes.iter().enumerate() {
        let inputs: Vec<usize> = node.inputs.iter().map(|&i| remap[i]).collect();
        let canon = Node { kind: map_kind(&node.kind, cse), inputs };
        let id = match seen.get(&canon) {
            Some(&id) => id,
            None => {
                let id = new_nodes.len();
                new_nodes.push(canon.clone());
                seen.insert(canon, id);
                id
            }
        };
        remap[old] = id;
    }
    Graph { nodes: new_nodes, output: remap[g.output] }
}

/// dead-node elimination: keep only nodes reachable from the output.
pub fn dce(g: &Graph<NumOp>) -> Graph<NumOp> {
    let mut live = vec![false; g.nodes.len()];
    let mut stack = vec![g.output];
    while let Some(i) = stack.pop() {
        if std::mem::replace(&mut live[i], true) {
            continue;
        }
        stack.extend(g.nodes[i].inputs.iter().copied());
    }
    let mut remap = vec![0usize; g.nodes.len()];
    let mut new_nodes = Vec::new();
    for (old, node) in g.nodes.iter().enumerate() {
        if !live[old] {
            continue;
        }
        let inputs = node.inputs.iter().map(|&i| remap[i]).collect();
        remap[old] = new_nodes.len();
        new_nodes.push(Node { kind: map_kind(&node.kind, dce), inputs });
    }
    Graph { nodes: new_nodes, output: remap[g.output] }
}

/// peephole: `Field(i)` applied to a `Tuple` is just the tuple's i-th input.
pub fn peephole(g: &Graph<NumOp>) -> Graph<NumOp> {
    let mut remap = vec![0usize; g.nodes.len()];
    let mut new_nodes: Vec<Node<NumOp>> = Vec::new();
    for (old, node) in g.nodes.iter().enumerate() {
        let inputs: Vec<usize> = node.inputs.iter().map(|&i| remap[i]).collect();
        if let NodeKind::Op(NumOp::Core(Op::Field(i))) = &node.kind {
            let src = inputs[0];
            if matches!(new_nodes[src].kind, NodeKind::Tuple) {
                remap[old] = new_nodes[src].inputs[*i];
                continue;
            }
        }
        remap[old] = new_nodes.len();
        new_nodes.push(Node { kind: map_kind(&node.kind, peephole), inputs });
    }
    Graph { nodes: new_nodes, output: remap[g.output] }
}

/// run the passes together (peephole exposes dead tuples → cse → dce sweeps).
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    dce(&cse(&peephole(g)))
}
