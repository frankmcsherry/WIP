use super::*;
use crate::{eval_graph, parse_ml, shape_of_value, Bounds};

fn specialized(input: Value) -> Value {
    let graph = parse_ml("input flatten .1").unwrap();
    let compiled = crate::compile(graph.clone(), &shape_of_value(&input)).unwrap();
    assert!(matches!(compiled.graph.nodes[compiled.graph.output].kind,
        NodeKind::Op(NumOp::Core(Op::FlattenValues))));
    assert!(!compiled.graph.nodes.iter().any(|n|
        matches!(n.kind, NodeKind::Op(NumOp::Core(Op::Flatten)))));
    let result = eval_graph(compiled.graph(), input.clone());
    assert_eq!(result, eval_graph(&graph, input));
    result
}

#[test]
fn empty_outer_rows() {
    for ends in [vec![], vec![0, 0]] {
        let input = Value::List(ends.clone().into(), Box::new(Value::List(
            Vec::new().into(), Box::new(Value::i64(vec![])),
        )));
        let result = specialized(input);
        assert_eq!(result, Value::List(ends.into(), Box::new(Value::i64(vec![]))));
    }
}

#[test]
fn empty_inner_rows() {
    let input = Value::List(vec![2, 3, 3].into(), Box::new(Value::List(
        vec![0, 0, 2].into(), Box::new(Value::i64(vec![7, 8])),
    )));
    assert_eq!(specialized(input), Value::List(vec![0, 2, 2].into(), Box::new(Value::i64(vec![7, 8]))));
}

#[test]
fn nested_payloads_keep_their_storage() {
    let leaf = Value::i64(vec![1, 2, 3]);
    let payload = Value::Prod(vec![leaf.clone(), Value::List(
        vec![0, 2, 3].into(), Box::new(Value::i64(vec![10, 20, 30])),
    )]);
    let input = Value::List(vec![1, 1, 3].into(), Box::new(Value::List(
        vec![0, 1, 3].into(), Box::new(payload.clone()),
    )));
    let result = specialized(input);
    assert_eq!(result, Value::List(vec![0, 0, 3].into(), Box::new(payload)));
    let Value::List(_, values) = result else { panic!("list") };
    let Value::Prod(fields) = *values else { panic!("product") };
    let (Value::Prim(crate::value::Prim::I64(a)), Value::Prim(crate::value::Prim::I64(b))) =
        (&leaf, &fields[0]) else { panic!("leaves") };
    assert!(std::sync::Arc::ptr_eq(a, b));
}

#[test]
fn shared_flatten_keeps_both_outputs() {
    let input = Value::List(Bounds::Stride(1, 2), Box::new(Value::List(
        vec![0, 2].into(), Box::new(Value::i64(vec![7, 8])),
    )));
    let graph = parse_ml("let flat = input flatten in (flat.0, flat.1)").unwrap();
    let compiled = crate::compile(graph.clone(), &shape_of_value(&input)).unwrap();
    assert!(!compiled.graph.nodes.iter().any(|n|
        matches!(n.kind, NodeKind::Op(NumOp::Core(Op::FlattenValues)))));
    assert_eq!(eval_graph(compiled.graph(), input.clone()), eval_graph(&graph, input));
}

#[test]
fn values_only_flatten_still_passes_payload_demand() {
    let input = Value::List(Bounds::Stride(1, 2), Box::new(Value::List(
        Bounds::Stride(1, 2), Box::new(Value::Prod(vec![Value::i64(vec![1,2]), Value::i64(vec![10,20])])),
    )));
    let graph = parse_ml("input flatten .1 map (p -> p.0)").unwrap();
    let compiled = crate::compile(graph.clone(), &shape_of_value(&input)).unwrap();
    let result = crate::narrow(compiled, &crate::Demand::All).unwrap();
    use crate::Demand::{All, None, List, Prod};
    assert_eq!(result.input, List(Box::new(List(Box::new(Prod(vec![All, None]))))));
    assert_eq!(eval_graph(&result.graph, result.input.project(input.clone()).unwrap()), eval_graph(&graph, input));
}

#[test]
fn constant_row_anchors_do_not_keep_ranges() {
    let input = Value::List(Bounds::Stride(1, 2), Box::new(Value::List(
        vec![0, 2].into(), Box::new(Value::i64(vec![7, 8])),
    )));
    let graph = parse_ml("let (ranges,flat)=input flatten in (flat,1)").unwrap();
    let compiled = crate::compile(graph.clone(), &shape_of_value(&input)).unwrap();
    assert!(!compiled.graph.nodes.iter().any(|n| matches!(n.kind, NodeKind::Op(NumOp::Core(Op::Flatten)))));
    assert_eq!(eval_graph(compiled.graph(), input.clone()), eval_graph(&graph, input));
}

#[test]
fn new_values_operations_share_and_are_idempotent() {
    let mut b = Builder::default(); let x = b.input();
    let values = b.add(Op::FlattenValues, vec![x]);
    let flat = b.add(Op::Flatten, vec![x]); let second = b.add(Op::Field(1), vec![flat]);
    let out = b.tuple(vec![values, second]);
    let shape = crate::Shape::List(Box::new(crate::Shape::List(Box::new(crate::Shape::Int))));
    let compiled = crate::compile(b.finish(out), &shape).unwrap();
    assert_eq!(compiled.graph.nodes.iter().filter(|n| matches!(n.kind, NodeKind::Op(NumOp::Core(Op::FlattenValues)))).count(), 1);
    assert_eq!(crate::compile(compiled.graph.clone(), &shape).unwrap().graph(), compiled.graph());
}
