//! Optimizer passes over `Graph<NumOp>`. Each preserves evaluation. Structural passes
//! (`cse`/`dce`) are vocabulary-agnostic in spirit; the semantic peephole pattern-
//! matches core `Field`/`Tuple`, so it reaches through `NumOp::Core`.

use crate::graph::{Builder, Graph, Node, NodeKind, OpLike};
use crate::ops::{ArithOp, BinOp, BitOp, CmpOp, NumOp, Op, Pattern, Pred};
use crate::value::{Scalar, Value};
use std::collections::HashMap;

pub(crate) mod demand;
mod payload;
pub use payload::compact_filter_payloads;

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
/// so far, and the original `node`). A rule may append nodes when it inlines a body. No hit rebuilds
/// verbatim. (`cse`/`dce` carry cross-node state — a dedup map, a reachability mark — so they stay
/// bespoke.)
fn rewrite_graph(
    g: &Graph<NumOp>,
    recurse: fn(&Graph<NumOp>) -> Graph<NumOp>,
    rule: impl Fn(&NodeKind<NumOp>, &[usize], &mut Vec<Node<NumOp>>, &Node<NumOp>) -> Option<Rewrite>,
) -> Graph<NumOp> {
    let mut remap = vec![0usize; g.nodes.len()];
    let mut built: Vec<Node<NumOp>> = Vec::new();
    for (old, node) in g.nodes.iter().enumerate() {
        let inputs: Vec<usize> = node.inputs.iter().map(|&i| remap[i]).collect();
        let kind = map_kind(&node.kind, recurse);
        remap[old] = match rule(&kind, &inputs, &mut built, node) {
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

/// peephole: `Field(i)` applied to a `Tuple` is just the tuple's i-th input, and a pattern of n
/// names or `_`s applied to a `Tuple` of n inputs is the tuple.
pub fn peephole(g: &Graph<NumOp>) -> Graph<NumOp> {
    rewrite_graph(g, peephole, |kind, inputs, built, _| {
        match kind {
            NodeKind::Op(NumOp::Core(Op::Field(i))) if matches!(built[inputs[0]].kind, NodeKind::Tuple) => {
                Some(Rewrite::Redirect(built[inputs[0]].inputs[*i]))
            }
            NodeKind::Op(NumOp::Core(Op::Pattern(Pattern::Tuple(ps))))
                if matches!(built[inputs[0]].kind, NodeKind::Tuple)
                    && built[inputs[0]].inputs.len() == ps.len()
                    && ps.iter().all(|p| *p == Pattern::Any) =>
            {
                Some(Rewrite::Redirect(inputs[0]))
            }
            _ => None,
        }
    })
}

/// `g1: A -> B` then `g2: B -> C`, as one graph `A -> C`: `g2`'s `Input` is `g1`'s output.
fn compose(g1: &Graph<NumOp>, g2: &Graph<NumOp>) -> Graph<NumOp> {
    let mut b = Builder::default();
    let x = b.input();
    let mid = b.graft(g1, x);
    let out = b.graft(g2, mid);
    b.finish(out)
}

/// Map fusion: `MapList(b1)` feeding `MapList(b2)`, where the first is consumed ONLY by the second,
/// fuse to `MapList(b1 ; b2)` — the intermediate list wrapper and interpreter setup disappear. The bodies already run on whole
/// columns; fusion alone does not eliminate their intermediate leaf buffers. Simplify the composed
/// body too, so fields discarded by the second map can remove computations in the first.
/// A map of `Enlist(x)` runs its body directly on `x`, then enlists the result. Both body calls see
/// the very same column, including on empty input. Recurses into bodies first.
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
                        kind: NodeKind::Op(NumOp::Core(Op::MapList(Box::new(dce(&peephole(&compose(b1, b2))))))),
                        inputs: vec![built[inputs[0]].inputs[0]],
                    }));
                }
                if matches!(built[inputs[0]].kind, NodeKind::Op(NumOp::Core(Op::Enlist))) {
                    let source = built[inputs[0]].inputs[0];
                    let mut remap = Vec::with_capacity(b2.nodes.len());
                    for n in &b2.nodes {
                        let id = if matches!(n.kind, NodeKind::Input) {
                            source
                        } else {
                            let inputs = n.inputs.iter().map(|&i| remap[i]).collect();
                            built.push(Node { kind: n.kind.clone(), inputs });
                            built.len() - 1
                        };
                        remap.push(id);
                    }
                    return Some(Rewrite::Replace(Node {
                        kind: NodeKind::Op(NumOp::Core(Op::Enlist)),
                        inputs: vec![remap[b2.output]],
                    }));
                }
            }
        }
        None
    })
}

/// Adjacent structural isos that are exact inverses (`Zip∘Transpose`, `Weave∘Unweave`, and
/// `Unweave∘Weave`) cancel: `outer(inner(x)) = x`, so the consumer reads `inner`'s input directly.
/// Sound regardless of fan-out — only this edge is redirected; a still-shared `inner` survives for
/// dce.
fn is_inverse(outer: &Op<NumOp>, inner: &Op<NumOp>) -> bool {
    use Op::*;
    match (outer, inner) {
        // Zip can discard the ends of unequal rows. Transpose after Zip therefore is not
        // an inverse unless the bounds were known to agree before Zip.
        (Zip, Transpose) | (Weave, Unweave) | (Unweave, Weave) => true,
        // `unwrap(inject x) = x`, the Sum-side pair. Only when every lane has one shape: otherwise
        // `Unwrap` is a shape ERROR on this sum, and cancelling would turn a program the typer
        // rejects into one it accepts. A lane taking its shape from the payload is known only then,
        // so it agrees with the others only when there are none.
        (Unwrap, Inject(_, lanes)) => {
            lanes.len() == 1 || (lanes.iter().all(Option::is_some) && lanes.windows(2).all(|w| w[0] == w[1]))
        }
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
/// literal in it runs as one op carrying the constant (`ArithOp::BinImm`, `ArithOp::BitsImm`,
/// `CmpOp::RelImm`, `CmpOp::MinImm`/`MaxImm`) on the other side, so `(x, 4) add` builds no column
/// of 4s. Exact, because the immediate kernels share the pair kernels' lane bodies. A literal on
/// the left becomes the same immediate where the order of operands doesn't matter (integer
/// `add`/`mul`, the bitwise ops but `sub_b64`, `eq`, `ne`, `min`, `max`), the flipped one for the
/// other comparisons, and otherwise an immediate that keeps the constant on the left
/// (`ArithOp::ImmBin`, `ArithOp::ImmBits`): `(100, d) sub`, `div`, `rem`, `sub_b64`, and float
/// `add`/`mul`, whose NaN payloads can depend on the order. A constant whose kind disagrees with
/// the op still fails where it did. The pair and the literal remain while anything else reads
/// them; [`dce`] sweeps them otherwise. `Program` runs this, then `dce`, on every program; the ML
/// notation has no spelling of its own for these ops.
pub fn immediates(g: &Graph<NumOp>) -> Graph<NumOp> {
    rewrite_graph(g, immediates, |kind, inputs, built, _| {
        let NodeKind::Op(op) = kind else { return None };
        let pair = &built[*inputs.first()?];
        if !matches!(pair.kind, NodeKind::Tuple) || pair.inputs.len() != 2 {
            return None;
        }
        // a list of leaves, one row: `eq` and `ne` take it as a constant list
        if let NumOp::Cmp(CmpOp::Rel(p @ (Pred::Eq | Pred::Ne))) = op {
            let list = |i: usize| match &built[pair.inputs[i]].kind {
                NodeKind::Op(NumOp::Core(Op::Lit(Value::List(b, vals)))) if b.len() == 1 && b.end(0) == vals.len() => {
                    match &**vals { Value::Prim(c) => Some(c.clone()), _ => None }
                }
                _ => None,
            };
            let found = list(1).map(|c| (pair.inputs[0], c)).or_else(|| list(0).map(|c| (pair.inputs[1], c)));
            if let Some((x, c)) = found {
                return Some(Rewrite::Replace(Node { kind: NodeKind::Op(NumOp::Cmp(CmpOp::ListImm(*p, c))), inputs: vec![x] }));
            }
        }
        let literal = |i: usize| match &built[pair.inputs[i]].kind {
            NodeKind::Op(NumOp::Core(Op::Lit(Value::Prim(p)))) if p.len() == 1 => Some(Scalar::of(p, 0)),
            _ => None,
        };
        let (x, c, left) = match (literal(1), literal(0)) {
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
            NumOp::Arith(ArithOp::Bin(b)) => {
                let either_side = matches!(b, BinOp::Add | BinOp::Mul) && matches!(c, Scalar::Int(_));
                if left && !either_side {
                    NumOp::Arith(ArithOp::ImmBin(c, *b))
                } else {
                    NumOp::Arith(ArithOp::BinImm(*b, c))
                }
            }
            NumOp::Arith(ArithOp::Bits(b)) => {
                let Scalar::Int(c) = c else { return None };
                if left && matches!(b, BitOp::SubB64) {
                    NumOp::Arith(ArithOp::ImmBits(c, *b))
                } else {
                    NumOp::Arith(ArithOp::BitsImm(*b, c))
                }
            }
            NumOp::Cmp(CmpOp::Rel(p)) => NumOp::Cmp(CmpOp::RelImm(if left { flip(*p) } else { *p }, c)),
            NumOp::Cmp(CmpOp::Min) => NumOp::Cmp(CmpOp::MinImm(c)),
            NumOp::Cmp(CmpOp::Max) => NumOp::Cmp(CmpOp::MaxImm(c)),
            _ => return None,
        };
        Some(Rewrite::Replace(Node { kind: NodeKind::Op(imm), inputs: vec![x] }))
    })
}

/// Evaluate scalar numeric kernels on literals, retaining their row-count anchors.
/// This is an optimizer pass, separate from immediate-kernel lowering.
pub fn fold_scalar(g: &Graph<NumOp>) -> Graph<NumOp> {
    rewrite_graph(g, fold_scalar, |kind, inputs, built, _| {
        let NodeKind::Op(op) = kind else { return None };
        fold_literal(op, inputs[0], built).map(Rewrite::Replace)
    })
}

/// Fold only one scalar lane of a numeric kernel. Never materialize a generated list or call a
/// host kernel at compile time. Keep the literal's original row-count anchor, including inside a
/// map or an empty sum lane. A shape error leaves the original operation in place.
fn fold_literal(op: &NumOp, input: usize, built: &[Node<NumOp>]) -> Option<Node<NumOp>> {
    match op {
        NumOp::Arith(ArithOp::BinImm(..) | ArithOp::ImmBin(..) | ArithOp::BitsImm(..)
            | ArithOp::ImmBits(..) | ArithOp::Shift(..) | ArithOp::Neg | ArithOp::ToFloat)
        | NumOp::Cmp(CmpOp::RelImm(..) | CmpOp::MinImm(..) | CmpOp::MaxImm(..)) => {}
        _ => return None,
    }
    let NodeKind::Op(NumOp::Core(Op::Lit(value @ Value::Prim(p)))) = &built[input].kind else { return None };
    if p.len() != 1 { return None; }
    let folded = op.eval(value.clone()).ok()?;
    Some(Node { kind: NodeKind::Op(NumOp::Core(Op::Lit(folded))), inputs: built[input].inputs.clone() })
}

/// Repeat only one-way structural cleanups. Equality is the ordinary graph equality; the cap
/// bounds compile work even if a future cleanup accidentally stops converging.
pub fn cleanup(g: &Graph<NumOp>) -> Graph<NumOp> {
    let mut graph = g.clone();
    for _ in 0..8 {
        let next = dce(&fold_scalar(&immediates(&cancel_isos(&peephole(&graph)))));
        if next == graph { break; }
        graph = next;
    }
    graph
}

/// Remove shape guards only after checking the complete graph and its closed bodies.
/// The result is checked again so erasing guards cannot change the output shape.
pub fn remove_checked_patterns(g: &Graph<NumOp>, input: &crate::Shape) -> Result<Graph<NumOp>, String> {
    fn strip(g: &Graph<NumOp>) -> Graph<NumOp> {
        rewrite_graph(g, strip, |kind, inputs, _, _| {
            matches!(kind, NodeKind::Op(NumOp::Core(Op::Pattern(_))))
                .then(|| Rewrite::Redirect(inputs[0]))
        })
    }
    let output = crate::shape_of(g, input)?;
    let graph = dce(&strip(g));
    if crate::shape_of(&graph, input)? != output {
        return Err("removing checked patterns changed the output shape".into());
    }
    Ok(graph)
}

#[cfg(test)]
mod tests;
