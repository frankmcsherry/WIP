//! Optimizer passes over `Graph<NumOp>`. Each preserves evaluation. Structural passes
//! (`cse`/`dce`) are vocabulary-agnostic in spirit; the semantic peephole pattern-
//! matches core `Field`/`Tuple`, so it reaches through `NumOp::Core`.

use crate::graph::{Graph, Node, NodeKind};
use crate::ops::{ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred};
use crate::value::Value;
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
    match (outer, inner) {
        (Zip, Transpose) | (Transpose, Zip) | (Weave, Unweave) | (Unweave, Weave) => true,
        // `unwrap(inject x) = x`, the Sum-side pair. Only when every declared lane has one shape:
        // otherwise `Unwrap` is a shape ERROR on this sum, and cancelling would turn a program the
        // typer rejects into one it accepts.
        (Unwrap, Inject(_, shapes)) => shapes.windows(2).all(|w| w[0] == w[1]),
        _ => false,
    }
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

/// constant operands become immediates: a binary op whose input is a pair with a one-row leaf
/// literal in it runs as one op carrying the constant (`ArithOp::BinImm`, `CmpOp::RelImm`,
/// `CmpOp::MinImm`/`MaxImm`) on the other side, so `(x, 4u64) add` builds no column of 4s. Exact,
/// because the immediate kernels share the pair kernels' lane bodies and take the literal's stored
/// bits. A literal on the left converts where the order of operands doesn't matter (integer
/// `add`/`mul`, `eq`, `ne`, `min`, `max`) and for the other comparisons, which flip; `(c, x) sub`,
/// `div` and `rem`, and float `(c, x) add`/`mul` (whose NaN payloads can depend on the order), stay
/// pairs. A width that disagrees with the op stays a pair too, so it fails where it did. The pair and
/// the literal remain while anything else reads them; [`dce`] sweeps them otherwise. `Program` runs
/// this, then `dce`, on every program; the ML notation has no spelling of its own for these ops.
pub fn immediates(g: &Graph<NumOp>) -> Graph<NumOp> {
    rewrite_graph(g, immediates, |kind, inputs, built, _| {
        let NodeKind::Op(op) = kind else { return None };
        let pair = &built[*inputs.first()?];
        if !matches!(pair.kind, NodeKind::Tuple) || pair.inputs.len() != 2 {
            return None;
        }
        let literal = |i: usize| match &built[pair.inputs[i]].kind {
            NodeKind::Op(NumOp::Core(Op::Lit(Value::Prim(p)))) if p.len() == 1 => Some((p.bits(), p.usize_at(0) as u64)),
            _ => None,
        };
        let (x, (w, c), left) = match (literal(1), literal(0)) {
            (Some(l), _) => (pair.inputs[0], l, false),
            (None, Some(l)) => (pair.inputs[1], l, true),
            (None, None) => return None,
        };
        let flip = |p: Pred| match p {
            Pred::Lt => Pred::Gt,
            Pred::Le => Pred::Ge,
            Pred::Gt => Pred::Lt,
            Pred::Ge => Pred::Le,
            same => same,
        };
        let imm = match op {
            NumOp::Arith(ArithOp::Bin(b, k, bw)) => {
                let either_side = matches!(b, BinOp::Add | BinOp::Mul) && !matches!(k, Kind::F);
                if *bw != w || (left && !either_side) {
                    return None;
                }
                NumOp::Arith(ArithOp::BinImm(*b, *k, *bw, c))
            }
            NumOp::Cmp(CmpOp::Rel(p)) => NumOp::Cmp(CmpOp::RelImm(if left { flip(*p) } else { *p }, w, c)),
            NumOp::Cmp(CmpOp::Min) => NumOp::Cmp(CmpOp::MinImm(w, c)),
            NumOp::Cmp(CmpOp::Max) => NumOp::Cmp(CmpOp::MaxImm(w, c)),
            _ => return None,
        };
        Some(Rewrite::Replace(Node { kind: NodeKind::Op(imm), inputs: vec![x] }))
    })
}

/// run the passes together: peephole and iso-cancellation expose dead/foldable structure, map fusion
/// collapses adjacent list passes, then cse → dce sweep.
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    dce(&cse(&fuse_maps(&immediates(&cancel_isos(&peephole(g))))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::parse_ml;

    /// does any node of `g`, or of any body inside it, satisfy `f`?
    fn any_node(g: &Graph<NumOp>, f: &dyn Fn(&NodeKind<NumOp>) -> bool) -> bool {
        g.nodes.iter().any(|n| {
            f(&n.kind)
                || match &n.kind {
                    NodeKind::Op(NumOp::Core(Op::MapList(b) | Op::Fold(b) | Op::FoldScan(b))) => any_node(b, f),
                    NodeKind::Op(NumOp::Core(Op::MapSum(arms))) => arms.iter().any(|(_, b)| any_node(b, f)),
                    _ => false,
                }
        })
    }

    #[test]
    fn immediates_reach_into_bodies() {
        for src in [
            "input iota map (x -> (x, 1u64) sub)",
            "(input, input iota) fold ((a, x) -> ((a, x) add, 3u64) mul)",
            "input iota map (x -> (x, x and 1) branch 2 match (0 (e -> (e, 100u64) add), 1 (o -> o)))",
        ] {
            let g = dce(&immediates(&parse_ml(src).unwrap()));
            let lit = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Core(Op::Lit(_))));
            let imm = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Arith(ArithOp::BinImm(..))));
            assert!(!any_node(&g, &lit), "{src}: a literal is left");
            assert!(any_node(&g, &imm), "{src}: no immediate");
        }
    }
}
