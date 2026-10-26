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
        "input iota map (x -> (x, 1) sub)",
        "(input, input iota) fold ((a, x) -> ((a, x) add, 3) mul)",
        "input iota map (x -> (x, x and 1) branch 2 match (0 (e -> (e, 100) add), 1 (o -> o)))",
    ] {
        let g = dce(&immediates(&parse_ml(src).unwrap()));
        let lit = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Core(Op::Lit(_))));
        let imm = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Arith(ArithOp::BinImm(..))));
        assert!(!any_node(&g, &lit), "{src}: a literal is left");
        assert!(any_node(&g, &imm), "{src}: no immediate");
    }
}

/// an immediate keeps every bit of its literal, where `usize` is 32 bits too (WebAssembly).
#[test]
fn immediates_keep_wide_literals() {
    let big = (3i64 << 46) + 5;
    let g = dce(&immediates(&parse_ml(&format!("input map (x -> (x, {big}) add)")).unwrap()));
    let wide = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Arith(ArithOp::BinImm(_, crate::Scalar::Int(c)))) if *c == big);
    assert!(any_node(&g, &wide), "the immediate lost its high bits");
}
