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
        NodeKind::Op(NumOp::Core(Op::FoldScan(b))) => {
            NodeKind::Op(NumOp::Core(Op::FoldScan(Box::new(f(b)))))
        }
        NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => {
            NodeKind::Op(NumOp::Core(Op::MapSum(arms.iter().map(|(k, b)| (*k, f(b))).collect())))
        }
        other => other.clone(),
    }
}

/// the outcome of a rewrite rule at one node (see [`rewrite_graph`]); `None` (the common case) rebuilds
/// the node verbatim.
enum Rewrite {
    Redirect(usize),      // this node already exists (an earlier built id) — point consumers there
    Replace(Node<NumOp>), // emit this node instead of the default `{ kind, inputs }`
}

/// the shared single-pass rewriter that `peephole`/`cancel_isos`/`fuse_maps` are each just a `rule` of:
/// walk nodes in order, remap every edge to its already-rebuilt id, recurse each body via `map_kind`,
/// then consult `rule` (which sees the body-recursed `kind`, the remapped `inputs`, the nodes `built`
/// so far, and the original `node`). No hit rebuilds verbatim. (`cse`/`dce` carry cross-node state — a
/// dedup map, a reachability mark — so they stay bespoke.)
fn rewrite_graph(
    g: &Graph<NumOp>,
    recurse: fn(&Graph<NumOp>) -> Graph<NumOp>,
    rule: impl Fn(&NodeKind<NumOp>, &[usize], &[Node<NumOp>], &Node<NumOp>) -> Option<Rewrite>,
) -> Graph<NumOp> {
    let mut remap = vec![0usize; g.nodes.len()];
    let mut built: Vec<Node<NumOp>> = Vec::new();
    for (old, node) in g.nodes.iter().enumerate() {
        let inputs: Vec<usize> = node.inputs.iter().map(|&i| remap[i]).collect();
        let kind = map_kind(&node.kind, recurse);
        remap[old] = match rule(&kind, &inputs, &built, node) {
            Some(Rewrite::Redirect(r)) => r,
            Some(Rewrite::Replace(n)) => {
                built.push(n);
                built.len() - 1
            }
            None => {
                built.push(Node { kind, inputs });
                built.len() - 1
            }
        };
    }
    Graph { nodes: built, output: remap[g.output] }
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
    rewrite_graph(g, peephole, |kind, inputs, built, _| {
        if let NodeKind::Op(NumOp::Core(Op::Field(i))) = kind {
            if matches!(built[inputs[0]].kind, NodeKind::Tuple) {
                return Some(Rewrite::Redirect(built[inputs[0]].inputs[*i]));
            }
        }
        None
    })
}

/// inline `g1: A -> B` into `g2: B -> C`, returning `g1 ; g2 : A -> C`. `g2`'s `Input` is replaced by
/// `g1`'s output; its other nodes are appended with edges remapped. Body sub-graphs are self-contained
/// (their `Input` is local), so they copy verbatim — only top-level edges shift.
fn compose(g1: &Graph<NumOp>, g2: &Graph<NumOp>) -> Graph<NumOp> {
    let mut nodes = g1.nodes.clone();
    let mut remap = vec![0usize; g2.nodes.len()];
    for (i, node) in g2.nodes.iter().enumerate() {
        match &node.kind {
            NodeKind::Input => remap[i] = g1.output, // g2's parameter becomes g1's result
            _ => {
                let inputs = node.inputs.iter().map(|&e| remap[e]).collect();
                remap[i] = nodes.len();
                nodes.push(Node { kind: node.kind.clone(), inputs });
            }
        }
    }
    Graph { nodes, output: remap[g2.output] }
}

/// map fusion — the memory-bound lever: two passes over a list become one. `MapList(b1)` feeding
/// `MapList(b2)`, where the first is consumed ONLY by the second, fuse to `MapList(b1 ; b2)` — the
/// intermediate `List<Y>` is never materialized (MapList preserves bounds, so the body composition is
/// exact). Recurses into bodies first, so inner chains fuse before outer ones.
pub fn fuse_maps(g: &Graph<NumOp>) -> Graph<NumOp> {
    let mut uses = vec![0usize; g.nodes.len()];
    for n in &g.nodes {
        for &e in &n.inputs {
            uses[e] += 1;
        }
    }
    uses[g.output] += 1;
    rewrite_graph(g, fuse_maps, |kind, inputs, built, node| {
        if let NodeKind::Op(NumOp::Core(Op::MapList(b2))) = kind {
            // fuse only when the producer is a MapList used NOWHERE else (else fusing duplicates work).
            if uses[node.inputs[0]] == 1 {
                if let NodeKind::Op(NumOp::Core(Op::MapList(b1))) = &built[inputs[0]].kind {
                    return Some(Rewrite::Replace(Node {
                        kind: NodeKind::Op(NumOp::Core(Op::MapList(Box::new(compose(b1, b2))))),
                        inputs: vec![built[inputs[0]].inputs[0]],
                    }));
                }
            }
        }
        None
    })
}

/// adjacent structural isos that are exact inverses (`Zip∘Transpose`, `Weave∘Unweave`, and their
/// mirrors) cancel: `outer(inner(x)) = x`, so the consumer reads `inner`'s input directly. Sound
/// regardless of fan-out — only this edge is redirected; a still-shared `inner` survives for dce.
fn is_inverse(outer: &Op<NumOp>, inner: &Op<NumOp>) -> bool {
    use Op::*;
    matches!(
        (outer, inner),
        (Zip, Transpose) | (Transpose, Zip) | (Weave, Unweave) | (Unweave, Weave)
    )
}

/// cancel adjacent inverse isos (see [`is_inverse`]). The iso analogue of `peephole`'s Field-of-Tuple.
pub fn cancel_isos(g: &Graph<NumOp>) -> Graph<NumOp> {
    rewrite_graph(g, cancel_isos, |kind, inputs, built, _| {
        if let NodeKind::Op(NumOp::Core(outer)) = kind {
            if let NodeKind::Op(NumOp::Core(inner)) = &built[inputs[0]].kind {
                if is_inverse(outer, inner) {
                    return Some(Rewrite::Redirect(built[inputs[0]].inputs[0]));
                }
            }
        }
        None
    })
}

/// run the passes together: peephole and iso-cancellation expose dead/foldable structure, map fusion
/// collapses adjacent list passes, then cse → dce sweep.
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    dce(&cse(&fuse_maps(&cancel_isos(&peephole(g)))))
}
