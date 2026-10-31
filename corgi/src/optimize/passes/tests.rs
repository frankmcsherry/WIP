use super::*;
use crate::frontend::parse_ml;

#[test]
fn range_gather_preserves_padding_empty_ranges_and_recursive_payloads() {
    use crate::{Bounds, Tags};
    use std::sync::Arc;
    let ints = Value::i64((0..21).collect());
    let lists = Value::List(Bounds::Stride(1, 21), Box::new(ints.clone()));
    let payloads = vec![
        ints.clone(), Value::u8((0..21).collect()), Value::f64((0..21).map(|i| i as f64).collect()),
        Value::Unit(21), Value::Prod(vec![ints.clone(), lists.clone()]), lists.clone(),
        Value::Sum(Tags::Const(0, 21), vec![ints, Value::Unit(0)]),
        Value::Ref(Arc::new(lists), Arc::new((0..21).collect())),
    ];
    let graph = parse_ml("input map (((lo, hi), xs) -> ((lo, hi) range, xs) gather)").unwrap();
    let fused = crate::optimize(&graph);
    assert!(any_node(&fused, &|k| matches!(k, NodeKind::Op(NumOp::Core(Op::GatherRange)))));
    assert!(!any_node(&fused, &|k| matches!(k, NodeKind::Op(NumOp::Core(Op::Range)))));
    for payload in payloads {
        let data = Value::List(vec![0, 1, 3, 3, 7, 10, 21].into(), Box::new(payload));
        for data in [data.clone(), Value::Ref(Arc::new(data), Arc::new(vec![6, 0, 3, 2, 1, 5, 4]))] {
        let input = Value::List(Bounds::Stride(7, 1), Box::new(Value::Prod(vec![
            Value::Prod(vec![Value::i64(vec![-3, -1, 0, 1, 4, 3, 5]), Value::i64(vec![2, -3, 4, 8, 4, 1, 9])]), data,
        ])));
        assert_eq!(crate::eval_graph(&fused, input.clone()), crate::eval_graph(&graph, input.clone()));
        let shape = crate::shape_of_value(&input);
        assert_eq!(crate::shape_of(&fused, &shape), crate::shape_of(&graph, &shape));
        }
    }
}

#[test]
fn range_gather_requires_a_private_range_and_pair() {
    for src in [
        "let at = (0, 3) range in ((at, input) gather, at)",
        "let pair = ((0, 3) range, input) in (pair gather, pair)",
    ] {
        let graph = crate::optimize(&parse_ml(src).unwrap());
        assert!(!any_node(&graph, &|k| matches!(k, NodeKind::Op(NumOp::Core(Op::GatherRange)))));
    }
    // Insertion before a second fusion must not confuse original and rebuilt node indices.
    let graph = crate::optimize(&parse_ml("(((0, 3) range, input) gather, ((1, 4) range, input) gather)").unwrap());
    assert_eq!(graph.nodes.iter().filter(|n| matches!(n.kind, NodeKind::Op(NumOp::Core(Op::GatherRange)))).count(), 2);
}

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
        let g = dce(immediates(parse_ml(src).unwrap()));
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
    let g = dce(immediates(parse_ml(&format!("input map (x -> (x, {big}) add)")).unwrap()));
    let wide = |k: &NodeKind<NumOp>| matches!(k, NodeKind::Op(NumOp::Arith(ArithOp::BinImm(_, crate::Scalar::Int(c)))) if *c == big);
    assert!(any_node(&g, &wide), "the immediate lost its high bits");
}

#[test]
fn checked_patterns_include_dead_code_and_nested_bodies() {
    use crate::{eval_graph, parse_ml, shape_of, Shape, Value};
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    b.add(Op::Pattern(Pattern::List(Box::new(Pattern::Any))), vec![x]);
    assert!(crate::compile(b.finish(x), &Shape::Int).is_err());

    let g = parse_ml("input map ((a, b) -> (a, b))").unwrap();
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let clean = crate::compile(g.clone(), &shape).unwrap();
    fn no_patterns(g: &Graph<NumOp>) {
        for node in g.nodes() {
            if let crate::NodeRef::Op(op, _) = node {
                assert!(!matches!(op, NumOp::Core(Op::Pattern(_))));
                for child in op.children() { no_patterns(child); }
            }
        }
    }
    assert_eq!(clean.input(), &shape);
    no_patterns(clean.graph());
    assert_eq!(shape_of(clean.graph(), &shape), shape_of(&g, &shape));
    let input = Value::List(vec![0, 2].into(), Box::new(Value::Prod(vec![
        Value::i64(vec![3, 4]), Value::i64(vec![5, 6]),
    ])));
    assert_eq!(eval_graph(clean.graph(), input.clone()), eval_graph(&g, input));
}
