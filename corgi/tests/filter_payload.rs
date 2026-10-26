use corgi::{eval_graph, parse_ml, shape_of, shape_of_value, Graph, NodeRef, NumOp, Op, OpLike, Shape, Value};
use corgi::optimize_with_demand;

fn list(ends: Vec<usize>, value: Value) -> Value { Value::List(ends.into(), Box::new(value)) }
fn checked(g: &Graph<NumOp>, value: Value) -> Graph<NumOp> {
    let shape = shape_of_value(&value);
    let result = optimize_with_demand(g, &shape).unwrap().0;
    assert_eq!(shape_of(g, &shape), shape_of(&result, &shape));
    assert_eq!(eval_graph(g, value.clone()), eval_graph(&result, value));
    assert_eq!(eval_graph(g, Value::empty(&shape)), eval_graph(&result, Value::empty(&shape)));
    assert_eq!(optimize_with_demand(&result, &shape).unwrap().0, result);
    result
}
fn filter_payload(g: &Graph<NumOp>, input: &Shape) -> Shape {
    let mut values: Vec<Value> = Vec::new();
    for node in g.nodes() {
        let value = match node {
            NodeRef::Input => Value::empty(input),
            NodeRef::Tuple(xs) => Value::Prod(xs.iter().map(|&i| values[i].clone()).collect()),
            NodeRef::Op(op, source) => {
                if matches!(op, NumOp::Core(Op::Filter)) {
                    let Shape::List(pair) = shape_of_value(&values[source]) else { panic!() };
                    let Shape::Prod(pair) = *pair else { panic!() };
                    return pair[1].clone();
                }
                op.eval(values[source].clone()).unwrap()
            }
        };
        values.push(value);
    }
    panic!("missing filter")
}

#[test]
fn duplicate_fields_stay_shared_after_filter() {
    let g = parse_ml("input filter map (x -> (x.0, x.0))").unwrap();
    let payload = Shape::Prod(vec![Shape::Int; 8]);
    let value = list(vec![3], Value::Prod(vec![Value::i64(vec![1, 0, 1]), Value::Prod((0..8).map(|i| Value::i64(vec![i; 3])).collect())]));
    let shape = shape_of_value(&value);
    let result = checked(&g, value);
    assert_eq!(filter_payload(&result, &shape), payload);
}

#[test]
fn shared_payload_producer_stays_whole() {
    let g = parse_ml("let xs = input map (x -> (x.0, (x.1, x.1, x.1))) in (xs, xs filter map (x -> x.0))").unwrap();
    let input = list(vec![0, 3], Value::Prod(vec![Value::i64(vec![0, 1, 1]), Value::i64(vec![7, 8, 9])]));
    let shape = shape_of_value(&input);
    let result = checked(&g, input);
    assert_eq!(filter_payload(&result, &shape), Shape::Prod(vec![Shape::Int; 3]));
}
