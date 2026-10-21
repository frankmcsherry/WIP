use corgi::{
    demand_projection, eval_graph, parse_ml, shape_of, Bounds, Builder, NumOp, Op, Pattern, Shape,
    Value,
};
use std::sync::Arc;

#[test]
fn join_projection_erases_unread_input_fields() {
    let graph = parse_ml("(input.0.0, input.2.1)").unwrap();
    let shape = Shape::Prod(vec![
        Shape::Prod(vec![Shape::Int, Shape::Int]),
        Shape::Int,
        Shape::Prod(vec![Shape::Int, Shape::Int]),
    ]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(projection.node_count(), 2);
    let projected = shape_of(&projection, &shape).unwrap();
    assert_eq!(
        projected,
        Shape::Prod(vec![
            Shape::Prod(vec![Shape::Int, Shape::Unit]),
            Shape::Unit,
            Shape::Prod(vec![Shape::Unit, Shape::Int]),
        ])
    );
    assert_eq!(shape_of(&graph, &projected), shape_of(&graph, &shape));

    let input = Value::Prod(vec![
        Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]),
        Value::i64(vec![5, 6]),
        Value::Prod(vec![Value::i64(vec![7, 8]), Value::i64(vec![9, 10])]),
    ]);
    let original_ptr = match &input {
        Value::Prod(fields) => match &fields[0] {
            Value::Prod(key) => corgi::arrange::borrowed_i64(&key[0]).unwrap().as_ptr(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    let expected = eval_graph(&graph, input.clone());
    let projected_input = eval_graph(&projection, input);
    let retained_ptr = match &projected_input {
        Value::Prod(fields) => match &fields[0] {
            Value::Prod(key) => corgi::arrange::borrowed_i64(&key[0]).unwrap().as_ptr(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    assert_eq!(retained_ptr, original_ptr);
    let actual = eval_graph(&graph, projected_input);
    assert_eq!(actual, expected);
}

#[test]
fn shared_uses_union_their_demands() {
    let graph = parse_ml("(input.0, input.1)").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Unit])
    );
}

#[test]
fn output_projection_drives_input_demand() {
    let subject = parse_ml("(input.0, input.1)").unwrap();
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let result = builder.graft(&subject, input);
    let output = builder.add(
        Op::Project(Shape::Prod(vec![Shape::Int, Shape::Unit])),
        vec![result],
    );
    let graph = builder.finish(output);
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit, Shape::Unit])
    );
    let input = Value::Prod(vec![
        Value::i64(vec![1, 2]),
        Value::i64(vec![3, 4]),
        Value::i64(vec![5, 6]),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn tuple_pattern_keeps_its_shape_check() {
    let graph = parse_ml("let (x, _) = input in x").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit])
    );
    let input = Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn eager_unused_fields_do_not_keep_their_values() {
    // DDIR's projection compiler creates all environment fields before compiling the terms.
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let _unused = builder.add(Op::Field(0), vec![input]);
    let used = builder.add(Op::Field(1), vec![input]);
    let graph = builder.finish(used);
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Unit, Shape::Int])
    );
}

#[test]
fn dead_opaque_use_does_not_demand_shared_input() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let _unused = builder.add(Op::Hash, vec![input]);
    let used = builder.add(Op::Field(0), vec![input]);
    let graph = builder.finish(used);
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit])
    );
}

#[test]
fn unknown_operations_keep_their_whole_input() {
    let graph = parse_ml("input hash").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    assert!(demand_projection(&graph, &shape).unwrap().is_none());
}

#[test]
fn unknown_operations_only_keep_fields_passed_to_them() {
    let graph = parse_ml("input.0 hash").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit])
    );
    let input = Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn unused_input_keeps_only_its_length() {
    let graph = parse_ml("input unit").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), Shape::Unit);
    let input = Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn project_rejects_shape_changes_other_than_erasure() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let output = builder.add(Op::Project(Shape::Prod(vec![Shape::Float])), vec![input]);
    let graph = builder.finish(output);
    assert!(shape_of(&graph, &Shape::Prod(vec![Shape::Int])).is_err());
}

#[test]
fn list_map_reads_only_one_element_field() {
    let mut body = Builder::<NumOp>::default();
    let element = body.input();
    let first = body.add(Op::Field(0), vec![element]);
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let output = builder.add(Op::MapList(Box::new(body.finish(first))), vec![input]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])))
    );
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2, 3]),
            Value::i64(vec![4, 5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn list_maps_union_their_element_demands() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let maps: Vec<_> = (0..2)
        .map(|field| {
            let mut body = Builder::<NumOp>::default();
            let element = body.input();
            let selected = body.add(Op::Field(field), vec![element]);
            builder.add(Op::MapList(Box::new(body.finish(selected))), vec![input])
        })
        .collect();
    let output = builder.tuple(maps);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![
        Shape::Int,
        Shape::Int,
        Shape::Int,
    ])));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Prod(vec![
            Shape::Int,
            Shape::Int,
            Shape::Unit
        ])))
    );
    let input = Value::List(
        Bounds::offsets(vec![2]),
        Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2]),
            Value::i64(vec![3, 4]),
            Value::i64(vec![5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn enlist_passes_element_demand_back_to_its_input() {
    let graph = parse_ml("input enlist map (x -> x.0)").unwrap();
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit])
    );
    let input = Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn list_pattern_keeps_bounds_without_payload() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let checked = builder.add(
        Op::Pattern(Pattern::List(Box::new(Pattern::Any))),
        vec![input],
    );
    let output = builder.add(Op::Unit, vec![checked]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Int));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Unit))
    );
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::i64(vec![1, 2, 3])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn len_keeps_bounds_without_list_payload() {
    let graph = parse_ml("input len").unwrap();
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Unit))
    );
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2, 3]),
            Value::i64(vec![4, 5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn shared_list_unions_len_and_payload_demands() {
    let mut body = Builder::<NumOp>::default();
    let element = body.input();
    let first = body.add(Op::Field(0), vec![element]);
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let lengths = builder.add(Op::Len, vec![input]);
    let mapped = builder.add(Op::MapList(Box::new(body.finish(first))), vec![input]);
    let output = builder.tuple(vec![lengths, mapped]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])))
    );
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2, 3]),
            Value::i64(vec![4, 5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn len_projects_a_referenced_list_without_copying_rows() {
    let graph = parse_ml("input len").unwrap();
    let shape = Shape::Ref(Box::new(Shape::List(Box::new(Shape::Prod(vec![
        Shape::Int,
        Shape::Int,
    ])))));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Ref(Box::new(Shape::List(Box::new(Shape::Unit))))
    );
    let rows = Arc::new(vec![2, 0, 2]);
    let input = Value::Ref(
        Arc::new(Value::List(
            Bounds::offsets(vec![2, 3, 5]),
            Box::new(Value::Prod(vec![
                Value::i64(vec![1, 2, 3, 4, 5]),
                Value::i64(vec![6, 7, 8, 9, 10]),
            ])),
        )),
        rows.clone(),
    );
    let expected = eval_graph(&graph, input.clone());
    let projected = eval_graph(&projection, input);
    let Value::Ref(_, projected_rows) = &projected else {
        panic!("projection must retain the reference")
    };
    assert!(Arc::ptr_eq(&rows, projected_rows));
    assert_eq!(eval_graph(&graph, projected), expected);
}

#[test]
fn ref_passes_list_demand_through_a_product() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let referenced = builder.add(Op::Ref, vec![input]);
    let list = builder.add(Op::Field(1), vec![referenced]);
    let output = builder.add(Op::Len, vec![list]);
    let graph = builder.finish(output);
    let shape = Shape::Prod(vec![
        Shape::Int,
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int]))),
    ]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Prod(vec![Shape::Unit, Shape::List(Box::new(Shape::Unit))])
    );
    let input = Value::Prod(vec![
        Value::i64(vec![10, 20]),
        Value::List(
            Bounds::offsets(vec![2, 3]),
            Box::new(Value::Prod(vec![
                Value::i64(vec![1, 2, 3]),
                Value::i64(vec![4, 5, 6]),
            ])),
        ),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn shared_ref_unions_len_and_pattern_demands() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let referenced = builder.add(Op::Ref, vec![input]);
    let lengths = builder.add(Op::Len, vec![referenced]);
    let pattern = Pattern::Ref(Box::new(Pattern::List(Box::new(Pattern::Tuple(vec![
        Pattern::Int,
        Pattern::Any,
    ])))));
    let checked = builder.add(Op::Pattern(pattern), vec![referenced]);
    let checked = builder.add(Op::Unit, vec![checked]);
    let output = builder.tuple(vec![lengths, checked]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])))
    );
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2, 3]),
            Value::i64(vec![4, 5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}

#[test]
fn full_use_of_shared_ref_prevents_erasure() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let referenced = builder.add(Op::Ref, vec![input]);
    let lengths = builder.add(Op::Len, vec![referenced]);
    let output = builder.tuple(vec![lengths, referenced]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    assert!(demand_projection(&graph, &shape).unwrap().is_none());
}

#[test]
fn ref_pattern_requires_reference_shape_but_not_payload() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let pattern = Pattern::Ref(Box::new(Pattern::Any));
    let checked = builder.add(Op::Pattern(pattern), vec![input]);
    let output = builder.add(Op::Unit, vec![checked]);
    let graph = builder.finish(output);
    let shape = Shape::Ref(Box::new(Shape::List(Box::new(Shape::Int))));
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Ref(Box::new(Shape::List(Box::new(Shape::Unit))))
    );
}

#[test]
fn sum_map_discards_an_unread_lane() {
    let mut first_body = Builder::<NumOp>::default();
    let first_input = first_body.input();
    let first = first_body.add(Op::Field(0), vec![first_input]);
    let mut second_body = Builder::<NumOp>::default();
    let second_input = second_body.input();
    let second = second_body.add(Op::Unit, vec![second_input]);
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let output = builder.add(
        Op::MapSum(vec![
            (0, first_body.finish(first)),
            (1, second_body.finish(second)),
        ]),
        vec![input],
    );
    let graph = builder.finish(output);
    let shape = Shape::Sum(vec![Shape::Prod(vec![Shape::Int, Shape::Int]), Shape::Int]);
    let projection = demand_projection(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        shape_of(&projection, &shape).unwrap(),
        Shape::Sum(vec![
            Shape::Prod(vec![Shape::Int, Shape::Unit]),
            Shape::Unit
        ])
    );
    let input = Value::sum(
        vec![0, 1, 0],
        vec![
            Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![3, 4])]),
            Value::i64(vec![5]),
        ],
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&graph, eval_graph(&projection, input))
    );
}
