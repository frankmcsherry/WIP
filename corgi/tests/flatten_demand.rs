//! Flatten preserves both levels of list bounds while forwarding payload demand.
use corgi::{demand_projection, eval_graph, optimize_with_demand, parse_ml, shape_of, shape_of_value,
    Graph, NumOp, Shape, Value};

fn list(ends: Vec<usize>, values: Value) -> Value {
    Value::List(ends.into(), Box::new(values))
}

fn nested(payload: Value) -> Value {
    // Empty outer rows, empty inner lists, and duplicate payloads exercise bounds.
    list(vec![0, 2, 2, 4], list(vec![0, 2, 2, 5], payload))
}

fn pair() -> Value {
    Value::Prod(vec![Value::i64(vec![91, 92, 93, 94, 95]), Value::i64(vec![7, 7, 8, 7, 8])])
}

fn checked(graph: &Graph<NumOp>, input: Value) -> Graph<NumOp> {
    let shape = shape_of_value(&input);
    let optimized = optimize_with_demand(graph, &shape).unwrap().0;
    assert_eq!(shape_of(&optimized, &shape), shape_of(graph, &shape));
    assert_eq!(eval_graph(&optimized, input.clone()), eval_graph(graph, input));
    assert_eq!(eval_graph(&optimized, Value::empty(&shape)), eval_graph(graph, Value::empty(&shape)));
    optimized
}


#[test]
fn ranges_only_erase_payload_but_keep_inner_empty_lists() {
    let graph = parse_ml("input flatten .0").unwrap();
    let input = nested(pair());
    let shape = shape_of_value(&input);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), Shape::List(Box::new(Shape::List(Box::new(Shape::Unit)))));
    assert_eq!(eval_graph(&graph, eval_graph(&projection, input.clone())), eval_graph(&graph, input.clone()));
    checked(&graph, input);
}

