use corgi::{
    compact_unit_fields, compact_unit_input, demand_projection, demand_transform, eval_graph, parse_ml, shape_of, ArithOp, BinOp, Bounds,
    Builder, Demand, NumOp, Op, Pattern, Shape, Value,
};
use std::sync::Arc;

#[test]
fn unit_compaction_narrows_join_values_and_preserves_the_key() {
    let graph = parse_ml("(input.0, input.1.0, input.1.2.1, input.2)").unwrap();
    let key = Shape::Prod(vec![Shape::Int, Shape::Unit]);
    let left = Shape::Prod(vec![
        Shape::Int,
        Shape::Unit,
        Shape::Prod(vec![Shape::Unit, Shape::Int]),
        Shape::Prod(vec![Shape::Unit, Shape::Unit]),
    ]);
    let right = Shape::Prod(vec![Shape::Unit, Shape::Unit]);
    let input_shape = Shape::Prod(vec![key.clone(), left, right]);
    let compacted = compact_unit_fields(&graph, &input_shape, &[1, 2]).unwrap().unwrap();
    assert_eq!(
        compacted.input_shape,
        Shape::Prod(vec![
            key,
            Shape::Prod(vec![Shape::Int, Shape::Prod(vec![Shape::Int])]),
            Shape::Unit,
        ])
    );
    assert_eq!(shape_of(&compacted.input_projection, &input_shape).unwrap(), compacted.input_shape);
    assert_eq!(shape_of(&compacted.graph, &compacted.input_shape), shape_of(&graph, &input_shape));

    let n = 3;
    let input = Value::Prod(vec![
        Value::Prod(vec![Value::i64(vec![5, 6, 7]), Value::Unit(n)]),
        Value::Prod(vec![
            Value::i64(vec![1, 2, 3]),
            Value::Unit(n),
            Value::Prod(vec![Value::Unit(n), Value::i64(vec![8, 9, 10])]),
            Value::Prod(vec![Value::Unit(n), Value::Unit(n)]),
        ]),
        Value::Prod(vec![Value::Unit(n), Value::Unit(n)]),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input)),
    );
}

#[test]
fn unit_compaction_handles_repeated_uses_and_validates_selection() {
    let shape = Shape::Prod(vec![Shape::Int, Shape::Prod(vec![Shape::Unit, Shape::Int, Shape::Unit])]);
    let graph = parse_ml("(input.1.1, input.1.1)").unwrap();
    let compacted = compact_unit_fields(&graph, &shape, &[1]).unwrap().unwrap();
    assert_eq!(compacted.input_shape, Shape::Prod(vec![Shape::Int, Shape::Prod(vec![Shape::Int])]));
    let input = Value::Prod(vec![
        Value::i64(vec![1, 2]),
        Value::Prod(vec![Value::Unit(2), Value::i64(vec![3, 4]), Value::Unit(2)]),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input)),
    );
    assert!(compact_unit_fields(&graph, &shape, &[0]).unwrap().is_none());
    assert!(compact_unit_fields(&graph, &shape, &[2]).is_err());
}

#[test]
fn unit_compaction_restores_structure_before_hashing() {
    let shape = Shape::Prod(vec![Shape::Int, Shape::Prod(vec![Shape::Int, Shape::Unit])]);
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let value = builder.add(Op::Field(1), vec![root]);
    let hashed = builder.add(Op::Hash, vec![value]);
    let graph = builder.finish(hashed);
    let compacted = compact_unit_fields(&graph, &shape, &[1]).unwrap().unwrap();
    let input = Value::Prod(vec![
        Value::i64(vec![1, 2]),
        Value::Prod(vec![Value::i64(vec![3, 4]), Value::Unit(2)]),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input)),
    );
}

#[test]
fn unit_compaction_retargets_a_list_lambda() {
    let graph = parse_ml("input map (x -> x.0.0)").unwrap();
    let shape = Shape::List(Box::new(Shape::Prod(vec![
        Shape::Prod(vec![Shape::Int, Shape::Unit]),
        Shape::Int,
    ])));
    let compacted = compact_unit_input(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        compacted.input_shape,
        Shape::List(Box::new(Shape::Prod(vec![Shape::Prod(vec![Shape::Int]), Shape::Int])))
    );
    assert_eq!(compacted.graph.node_count(), graph.node_count());
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![
            Value::Prod(vec![Value::i64(vec![1, 2, 3]), Value::Unit(3)]),
            Value::i64(vec![4, 5, 6]),
        ])),
    );
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input)),
    );
}

#[test]
fn unit_compaction_preserves_sum_tags() {
    let graph = parse_ml("input").unwrap();
    let shape = Shape::Sum(vec![
        Shape::Prod(vec![Shape::Int, Shape::Unit]),
        Shape::Prod(vec![Shape::Unit, Shape::Int]),
    ]);
    let compacted = compact_unit_input(&graph, &shape).unwrap().unwrap();
    assert_eq!(
        compacted.input_shape,
        Shape::Sum(vec![Shape::Prod(vec![Shape::Int]), Shape::Prod(vec![Shape::Int])]),
    );
    let input = Value::sum(vec![0, 1, 0], vec![
        Value::Prod(vec![Value::i64(vec![1, 2]), Value::Unit(2)]),
        Value::Prod(vec![Value::Unit(1), Value::i64(vec![3])]),
    ]);
    assert_eq!(
        eval_graph(&graph, input.clone()),
        eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input)),
    );
}

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
fn public_demand_merge_unions_consumers_of_one_input() {
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int]);
    let first = demand_transform(&parse_ml("input.0").unwrap(), &shape, &Shape::Int).unwrap();
    let second = demand_transform(&parse_ml("input.1").unwrap(), &shape, &Shape::Int).unwrap();
    let (need, projected) = Demand::union_for_shape(&shape, &first.input_demand, &second.input_demand).unwrap();
    assert_eq!(projected, Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Unit]));
    assert_eq!(need.to_shape(&shape).unwrap(), projected);
    assert_eq!(shape_of(&first.graph, &projected).unwrap(), Shape::Int);
    assert_eq!(shape_of(&second.graph, &projected).unwrap(), Shape::Int);
}

#[test]
fn public_demand_merge_preserves_container_structure() {
    let original = Shape::Prod(vec![
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int]))),
        Shape::Sum(vec![Shape::Int, Shape::Int]),
        Shape::Ref(Box::new(Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int]))))),
    ]);
    let left = Shape::Prod(vec![
        Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit]))),
        Shape::Sum(vec![Shape::Int, Shape::Unit]),
        Shape::Ref(Box::new(Shape::List(Box::new(Shape::Prod(vec![Shape::Unit, Shape::Int]))))),
    ]);
    let right = Shape::Prod(vec![
        Shape::List(Box::new(Shape::Prod(vec![Shape::Unit, Shape::Int]))),
        Shape::Sum(vec![Shape::Unit, Shape::Int]),
        Shape::Ref(Box::new(Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit]))))),
    ]);
    let mut need = Demand::from_projection(&left);
    need.merge(Demand::from_projection(&right)).unwrap();
    assert_eq!(need.to_shape(&original).unwrap(), original);
}

#[test]
fn public_demand_merge_rejects_incompatible_structures() {
    let mut need = Demand::from_projection(&Shape::Prod(vec![Shape::Int]));
    let before = need.clone();
    assert!(need.merge(Demand::from_projection(&Shape::List(Box::new(Shape::Int)))).is_err());
    assert_eq!(need, before);
    assert!(need.to_shape(&Shape::Prod(vec![Shape::Int, Shape::Int])).is_err());
    assert!(Demand::union_for_shape(
        &Shape::Prod(vec![Shape::Int, Shape::Int]),
        &need,
        &Demand::None,
    ).is_err());
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
fn transform_prunes_add_before_projecting_its_input() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let first = builder.add(Op::Field(0), vec![input]);
    let second = builder.add(Op::Field(1), vec![input]);
    let one = builder.add(Op::Lit(Value::i64(vec![1])), vec![input]);
    let pair = builder.tuple(vec![second, one]);
    let add = builder.add(ArithOp::Bin(BinOp::Add), vec![pair]);
    let output = builder.tuple(vec![first, add]);
    let subject = builder.finish(output);
    let input_shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let output_projection = Shape::Prod(vec![Shape::Int, Shape::Unit]);

    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let result = builder.graft(&subject, input);
    let output = builder.add(Op::Project(output_projection.clone()), vec![result]);
    let unchanged = builder.finish(output);
    assert!(demand_projection(&unchanged, &input_shape).unwrap().is_none());

    let transformed = demand_transform(&subject, &input_shape, &output_projection).unwrap();
    assert!(transformed.graph.node_count() < subject.node_count());
    assert_eq!(shape_of(&transformed.graph, &input_shape).unwrap(), output_projection);
    let input_projection = transformed.input_projection.unwrap();
    assert_eq!(
        shape_of(&input_projection, &input_shape).unwrap(),
        Shape::Prod(vec![Shape::Int, Shape::Unit])
    );
    let input = Value::Prod(vec![Value::i64(vec![2, 3]), Value::i64(vec![4, 5])]);
    assert_eq!(
        eval_graph(&unchanged, input.clone()),
        eval_graph(&transformed.graph, eval_graph(&input_projection, input))
    );
}

fn computed_pair() -> corgi::Graph<NumOp> {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let first = builder.add(Op::Field(0), vec![input]);
    let second = builder.add(Op::Field(1), vec![input]);
    let one = builder.add(Op::Lit(Value::i64(vec![1])), vec![input]);
    let pair = builder.tuple(vec![second, one]);
    let add = builder.add(ArithOp::Bin(BinOp::Add), vec![pair]);
    let output = builder.tuple(vec![first, add]);
    builder.finish(output)
}

#[test]
fn transform_prunes_add_behind_field_of_tuple() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let pair = builder.graft(&computed_pair(), input);
    let selected = builder.add(Op::Field(0), vec![pair]);
    let graph = builder.finish(selected);
    let shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let transformed = demand_transform(&graph, &shape, &Shape::Int).unwrap();
    let projection = transformed.input_projection.unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), Shape::Prod(vec![Shape::Int, Shape::Unit]));
    assert!(transformed.graph.node_count() < graph.node_count());
    let input = Value::Prod(vec![Value::i64(vec![2, 3]), Value::i64(vec![4, 5])]);
    assert_eq!(eval_graph(&graph, input.clone()), eval_graph(&transformed.graph, eval_graph(&projection, input)));
}

#[test]
fn transform_prunes_add_inside_list_map() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let output = builder.add(Op::MapList(Box::new(computed_pair())), vec![input]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let target = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])));
    let transformed = demand_transform(&graph, &shape, &target).unwrap();
    let projection = transformed.input_projection.unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), target);
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![Value::i64(vec![1, 2, 3]), Value::i64(vec![4, 5, 6])])),
    );
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let projected = builder.add(Op::Project(target), vec![root]);
    let projector = builder.finish(projected);
    assert_eq!(
        eval_graph(&projector, eval_graph(&graph, input.clone())),
        eval_graph(&transformed.graph, eval_graph(&projection, input)),
    );
}

#[test]
fn transform_prunes_add_inside_sum_map() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let output = builder.add(Op::MapSum(vec![(0, computed_pair())]), vec![input]);
    let graph = builder.finish(output);
    let shape = Shape::Sum(vec![Shape::Prod(vec![Shape::Int, Shape::Int]), Shape::Int]);
    let target = Shape::Sum(vec![Shape::Prod(vec![Shape::Int, Shape::Unit]), Shape::Unit]);
    let transformed = demand_transform(&graph, &shape, &target).unwrap();
    let projection = transformed.input_projection.unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), target);
    let input = Value::sum(
        vec![0, 1, 0],
        vec![
            Value::Prod(vec![Value::i64(vec![1, 2]), Value::i64(vec![4, 5])]),
            Value::i64(vec![6]),
        ],
    );
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let projected = builder.add(Op::Project(target), vec![root]);
    let projector = builder.finish(projected);
    assert_eq!(
        eval_graph(&projector, eval_graph(&graph, input.clone())),
        eval_graph(&transformed.graph, eval_graph(&projection, input)),
    );
}

#[test]
fn transform_does_not_duplicate_a_shared_map() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let mapped = builder.add(Op::MapList(Box::new(computed_pair())), vec![input]);
    let output = builder.tuple(vec![mapped, mapped]);
    let graph = builder.finish(output);
    let element = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let list = Shape::List(Box::new(element.clone()));
    let projected_list = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])));
    let target = Shape::Prod(vec![list.clone(), projected_list.clone()]);
    let transformed = demand_transform(&graph, &list, &target).unwrap();
    assert!(transformed.input_projection.is_none());
    assert_eq!(transformed.input_demand, Demand::All);

    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let mapped = builder.add(Op::MapList(Box::new(computed_pair())), vec![input]);
    let projected = builder.add(Op::Project(projected_list), vec![mapped]);
    let output = builder.tuple(vec![mapped, projected]);
    assert_eq!(transformed.graph, builder.finish(output));
}

#[test]
fn transform_prunes_map_after_its_other_use_dies() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let mapped = builder.add(Op::MapList(Box::new(computed_pair())), vec![input]);
    let output = builder.tuple(vec![mapped, mapped]);
    let graph = builder.finish(output);
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
    let projected_list = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])));
    let target = Shape::Prod(vec![projected_list.clone(), Shape::Unit]);
    let transformed = demand_transform(&graph, &shape, &target).unwrap();
    let projection = transformed.input_projection.unwrap();
    assert_eq!(shape_of(&projection, &shape).unwrap(), projected_list);
    let input = Value::List(
        Bounds::offsets(vec![2, 3]),
        Box::new(Value::Prod(vec![Value::i64(vec![1, 2, 3]), Value::i64(vec![4, 5, 6])])),
    );
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let projected = builder.add(Op::Project(target), vec![root]);
    let projector = builder.finish(projected);
    assert_eq!(
        eval_graph(&projector, eval_graph(&graph, input.clone())),
        eval_graph(&transformed.graph, eval_graph(&projection, input)),
    );
}

#[test]
fn transform_preserves_a_shared_add_with_an_observed_use() {
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let second = builder.add(Op::Field(1), vec![input]);
    let one = builder.add(Op::Lit(Value::i64(vec![1])), vec![input]);
    let pair = builder.tuple(vec![second, one]);
    let add = builder.add(ArithOp::Bin(BinOp::Add), vec![pair]);
    let output = builder.tuple(vec![add, add]);
    let subject = builder.finish(output);
    let input_shape = Shape::Prod(vec![Shape::Int, Shape::Int]);
    let output_projection = Shape::Prod(vec![Shape::Unit, Shape::Int]);
    let transformed = demand_transform(&subject, &input_shape, &output_projection).unwrap();
    let input_projection = transformed.input_projection.unwrap();
    assert_eq!(
        shape_of(&input_projection, &input_shape).unwrap(),
        Shape::Prod(vec![Shape::Unit, Shape::Int])
    );
    let input = Value::Prod(vec![Value::i64(vec![2, 3]), Value::i64(vec![4, 5])]);
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let output = builder.add(Op::Project(output_projection), vec![root]);
    let projector = builder.finish(output);
    let expected = eval_graph(&projector, eval_graph(&subject, input.clone()));
    assert_eq!(
        expected,
        eval_graph(&transformed.graph, eval_graph(&input_projection, input))
    );
}

#[test]
fn transform_preserves_fieldless_tuple_length() {
    let mut builder = Builder::<NumOp>::default();
    let _input = builder.input();
    let output = builder.tuple(vec![]);
    let subject = builder.finish(output);
    let input_shape = Shape::Int;
    let transformed = demand_transform(&subject, &input_shape, &Shape::Unit).unwrap();
    let input = Value::i64(vec![1, 2]);
    let projected_input = match transformed.input_projection {
        Some(projection) => eval_graph(&projection, input.clone()),
        None => input.clone(),
    };
    let original = eval_graph(&subject, input);
    let expected = eval_graph(
        &{
            let mut builder = Builder::<NumOp>::default();
            let root = builder.input();
            let output = builder.add(Op::Project(Shape::Unit), vec![root]);
            builder.finish(output)
        },
        original,
    );
    assert_eq!(eval_graph(&transformed.graph, projected_input), expected);
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

struct DemandRng(u64);

impl DemandRng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn random_shape(rng: &mut DemandRng, depth: usize) -> Shape {
    if depth == 0 {
        return if rng.below(3) == 0 { Shape::Unit } else { Shape::Int };
    }
    match rng.below(5) {
        0 => Shape::Int,
        1 => Shape::Unit,
        2 => Shape::Prod((0..1 + rng.below(3)).map(|_| random_shape(rng, depth - 1)).collect()),
        3 => Shape::List(Box::new(random_shape(rng, depth - 1))),
        _ => Shape::Sum((0..2).map(|_| random_shape(rng, depth - 1)).collect()),
    }
}

fn random_value(rng: &mut DemandRng, shape: &Shape, rows: usize) -> Value {
    match shape {
        Shape::Int => Value::i64((0..rows).map(|_| rng.below(11) as i64 - 5).collect()),
        Shape::Float => Value::f64((0..rows).map(|_| f64::from_bits(rng.next())).collect()),
        Shape::Unit => Value::Unit(rows),
        Shape::Prod(fields) => Value::Prod(fields.iter().map(|field| random_value(rng, field, rows)).collect()),
        Shape::List(element) => {
            let mut total = 0;
            let ends = (0..rows).map(|_| { total += rng.below(4); total }).collect();
            Value::List(Bounds::offsets(ends), Box::new(random_value(rng, element, total)))
        }
        Shape::Sum(lanes) => {
            let tags: Vec<_> = (0..rows).map(|_| rng.below(lanes.len())).collect();
            let values = lanes
                .iter()
                .enumerate()
                .map(|(tag, lane)| random_value(rng, lane, tags.iter().filter(|&&t| t == tag).count()))
                .collect();
            Value::sum(tags, values)
        }
        Shape::Ref(list) => {
            let arena = random_value(rng, list, rows + 3);
            let named = (0..rows).map(|_| rng.below(rows + 3)).collect();
            Value::Ref(Arc::new(arena), Arc::new(named))
        }
    }
}

fn random_graph(
    builder: &mut Builder<NumOp>,
    input: usize,
    shape: &Shape,
    rng: &mut DemandRng,
    depth: usize,
) -> (usize, Shape) {
    if depth == 0 {
        return (input, shape.clone());
    }
    match rng.below(7) {
        0 => (input, shape.clone()),
        1 => (builder.add(Op::Unit, vec![input]), Shape::Unit),
        2 => (builder.add(Op::Hash, vec![input]), Shape::Int),
        3 => {
            let (left, left_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (right, right_shape) = if rng.below(3) == 0 {
                (left, left_shape.clone())
            } else {
                random_graph(builder, input, shape, rng, depth - 1)
            };
            (builder.tuple(vec![left, right]), Shape::Prod(vec![left_shape, right_shape]))
        }
        4 if matches!(shape, Shape::Prod(fields) if !fields.is_empty()) => {
            let Shape::Prod(fields) = shape else { unreachable!() };
            let field = rng.below(fields.len());
            let selected = builder.add(Op::Field(field), vec![input]);
            random_graph(builder, selected, &fields[field], rng, depth - 1)
        }
        5 if matches!(shape, Shape::List(_)) => {
            let Shape::List(element) = shape else { unreachable!() };
            if rng.below(2) == 0 {
                (builder.add(Op::Len, vec![input]), Shape::Int)
            } else {
                let mut body = Builder::default();
                let item = body.input();
                let (output, output_shape) = random_graph(&mut body, item, element, rng, depth - 1);
                (builder.add(Op::MapList(Box::new(body.finish(output))), vec![input]), Shape::List(Box::new(output_shape)))
            }
        }
        6 if matches!(shape, Shape::Sum(_)) => {
            let Shape::Sum(lanes) = shape else { unreachable!() };
            let mut arms = Vec::new();
            let mut output_shapes = Vec::new();
            for (tag, lane) in lanes.iter().enumerate() {
                let mut body = Builder::default();
                let item = body.input();
                let (output, output_shape) = random_graph(&mut body, item, lane, rng, depth - 1);
                arms.push((tag, body.finish(output)));
                output_shapes.push(output_shape);
            }
            (builder.add(Op::MapSum(arms), vec![input]), Shape::Sum(output_shapes))
        }
        _ => (input, shape.clone()),
    }
}

fn random_projection(rng: &mut DemandRng, shape: &Shape) -> Shape {
    if rng.below(4) == 0 {
        return Shape::Unit;
    }
    match shape {
        Shape::Prod(fields) => Shape::Prod(fields.iter().map(|field| random_projection(rng, field)).collect()),
        Shape::List(element) => Shape::List(Box::new(random_projection(rng, element))),
        Shape::Sum(lanes) => Shape::Sum(lanes.iter().map(|lane| random_projection(rng, lane)).collect()),
        Shape::Ref(list) => {
            let Shape::List(element) = &**list else { unreachable!() };
            Shape::Ref(Box::new(Shape::List(Box::new(random_projection(rng, element)))))
        }
        other => other.clone(),
    }
}

#[test]
fn random_demand_rewrites_preserve_values() {
    let mut projected_inputs = 0;
    let mut compacted_inputs = 0;
    for seed in 1..=200u64 {
        let mut rng = DemandRng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let shape = random_shape(&mut rng, 2);
        let rows = 1 + rng.below(20);
        let input = random_value(&mut rng, &shape, rows);
        let mut builder = Builder::default();
        let root = builder.input();
        let (output, output_shape) = random_graph(&mut builder, root, &shape, &mut rng, 4);
        let graph = builder.finish(output);
        assert_eq!(shape_of(&graph, &shape).unwrap(), output_shape, "seed {seed}");
        let target = random_projection(&mut rng, &output_shape);

        if let Some(projection) = demand_projection(&graph, &shape).unwrap() {
            assert_eq!(
                eval_graph(&graph, input.clone()),
                eval_graph(&graph, eval_graph(&projection, input.clone())),
                "unmodified graph seed {seed}",
            );
        }
        if let Some(compacted) = compact_unit_input(&graph, &shape).unwrap() {
            compacted_inputs += 1;
            assert_eq!(
                eval_graph(&graph, input.clone()),
                eval_graph(&compacted.graph, eval_graph(&compacted.input_projection, input.clone())),
                "compaction seed {seed}",
            );
        }

        let mut builder = Builder::default();
        let root = builder.input();
        let result = builder.graft(&graph, root);
        let projected = builder.add(Op::Project(target.clone()), vec![result]);
        let projected_graph = builder.finish(projected);
        let expected = eval_graph(&projected_graph, input.clone());

        if let Some(projection) = demand_projection(&projected_graph, &shape).unwrap() {
            projected_inputs += 1;
            assert_eq!(expected, eval_graph(&projected_graph, eval_graph(&projection, input.clone())), "projection seed {seed}");
        }
        let transformed = demand_transform(&graph, &shape, &target).unwrap();
        let projected_input = transformed.input_projection.map_or(input.clone(), |projection| eval_graph(&projection, input));
        assert_eq!(expected, eval_graph(&transformed.graph, projected_input), "transform seed {seed}");
    }
    assert!(projected_inputs > 0);
    assert!(compacted_inputs > 0);
}

#[test]
fn generated_movement_projections_preserve_values() {
    for seed in 1..=150u64 {
        let mut rng = DemandRng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let rows = (seed as usize) % 13; // includes empty and singleton batches
        let element = Shape::Prod(vec![
            Shape::Float, random_shape(&mut rng, 2), Shape::Int,
            Shape::Ref(Box::new(Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int]))))),
        ]);

        let positions = random_shape(&mut rng, 2);
        let list = Shape::List(Box::new(element.clone()));
        let haystack = if seed % 2 == 0 { Shape::Ref(Box::new(list)) } else { list };
        let input_shape = Shape::Prod(vec![positions, haystack]);
        let input = random_value(&mut rng, &input_shape, rows);
        let mut b = Builder::<NumOp>::default();
        let x = b.input();
        let out = b.add(Op::Gather, vec![x]);
        let output = shape_of(&b.finish(out), &input_shape).unwrap();
        let target = random_projection(&mut rng, &output);
        check_movement_projection(Op::Gather, input, target);

        let filter_shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, element.clone()])));
        let input = random_value(&mut rng, &filter_shape, rows);
        let target = Shape::List(Box::new(random_projection(&mut rng, &element)));
        check_movement_projection(Op::Filter, input, target);

        let capture_shape = Shape::Prod(vec![element.clone(), Shape::List(Box::new(element.clone()))]);
        let input = random_value(&mut rng, &capture_shape, rows);
        let target = Shape::List(Box::new(Shape::Prod(vec![
            random_projection(&mut rng, &element), random_projection(&mut rng, &element),
        ])));
        check_movement_projection(Op::CapList, input, target);

        let select_shape = Shape::Prod(vec![Shape::Int, element.clone(), element.clone()]);
        let input = random_value(&mut rng, &select_shape, rows);
        let target = random_projection(&mut rng, &element);
        check_movement_projection(Op::Select, input, target);
    }
}

fn check_movement_projection(op: Op<NumOp>, input: Value, target: Shape) {
    let shape = corgi::shape_of_value(&input);
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let result = builder.add(op, vec![root]);
    let graph = builder.finish(result);
    let mut builder = Builder::<NumOp>::default();
    let root = builder.input();
    let result = builder.graft(&graph, root);
    let projected = builder.add(Op::Project(target.clone()), vec![result]);
    let projected_graph = builder.finish(projected);
    let expected = eval_graph(&projected_graph, input.clone());
    if let Some(projection) = demand_projection(&projected_graph, &shape).unwrap() {
        assert_eq!(expected, eval_graph(&projected_graph, eval_graph(&projection, input.clone())));
    }
    let transformed = demand_transform(&graph, &shape, &target).unwrap();
    // The local rewrite itself must narrow movement; no boundary projection is required.
    assert_eq!(expected, eval_graph(&transformed.graph, input.clone()));
    let projected_input = transformed.input_projection.map_or(input.clone(), |p| eval_graph(&p, input));
    assert_eq!(expected, eval_graph(&transformed.graph, projected_input));
}
