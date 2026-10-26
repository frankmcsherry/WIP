//! Producer pruning leaves input payload omission to the separate demand manifest.
use corgi::{eval_graph, optimize_with_demand, parse_ml, shape_of, shape_of_value, Builder, Graph, NodeRef, NumOp, Op, OpLike, Shape, Value};

fn list(ends: Vec<usize>, values: Value) -> Value {
    Value::List(ends.into(), Box::new(values))
}

fn pair(n: usize) -> Value {
    Value::Prod(vec![Value::i64((0..n).map(|i| i as i64 + 10).collect()),
        list((1..=n).map(|i| i * 2).collect(), Value::i64(vec![99; n * 2]))])
}

fn gather_input() -> Value {
    Value::Prod(vec![Value::i64(vec![0, -1, 7, 0]), list(vec![2, 2, 3, 5], pair(5))])
}

fn checked(graph: &Graph<NumOp>, input: Value) -> Graph<NumOp> {
    let shape = shape_of_value(&input);
    let result = optimize_with_demand(graph, &shape).unwrap().0;
    assert_no_projects(&result);
    assert_eq!(shape_of(&result, &shape), shape_of(graph, &shape));
    assert_eq!(eval_graph(graph, input.clone()), eval_graph(&result, input));
    assert_eq!(eval_graph(graph, Value::empty(&shape)), eval_graph(&result, Value::empty(&shape)));
    result
}

fn movements(graph: &Graph<NumOp>, input: &Shape) -> Vec<Shape> {
    let mut values = Vec::<Value>::new();
    let mut result = Vec::new();
    for node in graph.nodes() {
        values.push(match node {
            NodeRef::Input => Value::empty(input),
            NodeRef::Tuple(fields) => Value::Prod(fields.iter().map(|&i| values[i].clone()).collect()),
            NodeRef::Op(op, source) => {
                if matches!(op, NumOp::Core(Op::Gather | Op::Filter | Op::CapList | Op::Select)) {
                    result.push(shape_of_value(&values[source]));
                }
                match (op, shape_of_value(&values[source])) {
                    (NumOp::Core(Op::MapList(body)), Shape::List(element)) => result.extend(movements(body, &element)),
                    (NumOp::Core(Op::MapSum(arms)), Shape::Sum(lanes)) => {
                        for (tag, body) in arms { result.extend(movements(body, &lanes[*tag])); }
                    }
                    (NumOp::Core(Op::Fold(body) | Op::FoldScan(body)), Shape::Prod(pair)) => {
                        let Shape::List(element) = &pair[1] else { panic!("fold list") };
                        result.extend(movements(body, &Shape::Prod(vec![pair[0].clone(), (**element).clone()])));
                    }
                    _ => {}
                }
                op.eval(values[source].clone()).unwrap()
            }
        });
    }
    result
}

fn assert_no_projects(graph: &Graph<NumOp>) {
    for node in graph.nodes() {
        if let NodeRef::Op(NumOp::Core(op), _) = node {
            match op {
                Op::Project(_) => panic!("default optimization inserted a projection"),
                Op::MapList(body) | Op::Fold(body) | Op::FoldScan(body) => assert_no_projects(body),
                Op::MapSum(arms) => for (_, body) in arms { assert_no_projects(body); },
                _ => {},
            }
        }
    }
}

#[test]
fn shared_movements_are_never_duplicated() {
    for source in [
        "let x = input gather in (x, x.0)",
        "let x = input gather in (x.0, x.1)",
        "let x = input gather .0 in (x, x)",
        "let x = (input gather .0, 1) add in (x, x)",
    ] {
        let graph = parse_ml(source).unwrap();
        let input = gather_input();
        let shape = shape_of_value(&input);
        let result = checked(&graph, input);
        assert_eq!(movements(&result, &shape).len(), 1, "{source}");
    }
}

#[test]
fn referenced_haystack_preserves_misses_and_row_numbers() {
    let graph = parse_ml("(input.0, input.1 ref) gather .0").unwrap();
    let input = gather_input();
    let shape = shape_of_value(&input);
    let result = checked(&graph, input);
    assert_eq!(movements(&result, &shape), movements(&graph, &shape));
    assert_no_projects(&result);
}

#[test]
fn checked_parse_failures_survive_select_pruning() {
    let graph = parse_ml("let p = input.1 parse_int in (input.0, (p, input.0), (p, input.0)) select .0").unwrap();
    let strings = ["42", "", "9x", "9223372036854775808"];
    let mut bytes = Vec::new();
    let mut ends = Vec::new();
    for s in strings { bytes.extend(s.bytes().map(i64::from)); ends.push(bytes.len()); }
    checked(&graph, Value::Prod(vec![Value::i64(vec![0, 1, 0, 1]), list(ends, Value::i64(bytes))]));
}

#[test]
fn fold_bodies_prune_movement_without_changing_feedback_shape() {
    for scan in [false, true] {
        let body = parse_ml(if scan {
            "let next = (input.0, input.1 gather .0) add in (next, input.0)"
        } else {
            "(input.0, input.1 gather .0) add"
        }).unwrap();
        let mut builder = Builder::<NumOp>::default();
        let input = builder.input();
        let output = builder.add(if scan { Op::FoldScan(Box::new(body)) } else { Op::Fold(Box::new(body)) }, vec![input]);
        let graph = builder.finish(output);
        let input = Value::Prod(vec![Value::i64(vec![100, 200, 300]), list(vec![0, 1, 4], gather_input())]);
        let shape = shape_of_value(&input);
        let result = checked(&graph, input);
        assert_eq!(movements(&result, &shape), movements(&graph, &shape));
    assert_no_projects(&result);
    }
}

