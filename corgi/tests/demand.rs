use corgi::*;

#[test]
fn analysis_unions_readers_and_skips_row_count_computations() {
    use corgi::{analyze_demand, Demand};
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    let dead = b.add(Op::Hash, vec![x]);
    let a = b.add(Op::Field(0), vec![x]);
    let c = b.add(Op::Field(2), vec![x]);
    let constant = b.add(Op::Lit(Value::i64(vec![7])), vec![dead]);
    let output = b.tuple(vec![a, c, constant]);
    let graph = b.finish(output);
    let shape = Shape::Prod(vec![Shape::Int; 3]);
    let analysis = analyze_demand(&graph, &shape, &Demand::All).unwrap();
    assert_eq!(analysis.input_demand(), &Demand::Prod(vec![Demand::All, Demand::None, Demand::All]));
    assert_eq!(analysis.node_demands()[dead], Demand::None);
    assert_eq!(analysis.input_demand().projected_shape(&shape).unwrap(), Shape::Prod(vec![Shape::Int; 2]));

    let mut b = Builder::default();
    let x = b.input();
    let mapped = b.add(Op::MapList(Box::new(graph)), vec![x]);
    let graph = b.finish(mapped);
    let analysis = analyze_demand(&graph, &Shape::List(Box::new(shape)), &Demand::All).unwrap();
    assert_eq!(analysis.input_demand(), &Demand::List(Box::new(Demand::Prod(vec![Demand::All, Demand::None, Demand::All]))));
    assert!(analyze_demand(&graph, &Shape::Int, &Demand::None).is_err());
    let value = Value::Prod(vec![Value::Unit(3), Value::i64(vec![4,5,6])]);
    assert_eq!(Demand::Prod(vec![Demand::None, Demand::All]).project(value.clone()).unwrap(), Value::Prod(vec![Value::i64(vec![4,5,6])]));
    assert_eq!(Demand::Prod(vec![Demand::None; 2]).project(value.clone()).unwrap(), Value::Unit(3));
    for request in [Demand::Prod(vec![Demand::All]), Demand::List(Box::new(Demand::All)), Demand::Prod(vec![Demand::Sum(vec![]), Demand::All])] {
        assert!(request.project(value.clone()).is_err());
        assert!(request.projected_shape(&shape_of_value(&value)).is_err());
    }
    let reference = Value::Ref(std::sync::Arc::new(Value::List(vec![1,3].into(), Box::new(value))), std::sync::Arc::new(vec![1,0,1]));
    let request = Demand::Ref(Box::new(Demand::List(Box::new(Demand::Prod(vec![Demand::None,Demand::All])))));
    let projected = request.project(reference.clone()).unwrap();
    assert_eq!(shape_of_value(&projected), request.projected_shape(&shape_of_value(&reference)).unwrap());
    let Value::Ref(_, rows) = projected else { unreachable!() };
    assert_eq!(*rows, vec![1,0,1]);
    assert!(Demand::Ref(Box::new(Demand::None)).project(reference).is_err());
    let identity_shape = Shape::Prod(vec![Shape::Unit, Shape::List(Box::new(Shape::Int))]);
    let identity_request = Demand::Prod(vec![Demand::All, Demand::List(Box::new(Demand::All))]);
    let (identity, projected) = Demand::union_for_shape(&identity_shape, &identity_request, &Demand::None).unwrap();
    assert_eq!(identity, Demand::All);
    assert_eq!(projected, identity_shape);
    let (unread_unit, _) = Demand::union_for_shape(&Shape::Unit, &Demand::None, &Demand::None).unwrap();
    assert_eq!(unread_unit, Demand::None);
    let dropped_unit = Demand::Prod(vec![Demand::None, Demand::All]);
    assert_eq!(dropped_unit.projected_shape(&identity_shape).unwrap(), Shape::Prod(vec![Shape::List(Box::new(Shape::Int))]));

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
            let arena = random_value(rng, list, 3);
            let indices = (0..rows).map(|_| rng.below(3)).collect();
            Value::Ref(std::sync::Arc::new(arena), std::sync::Arc::new(indices))
        }
        _ => unreachable!("the test generator does not produce this shape"),
    }
}

fn as_list(builder: &mut Builder<NumOp>, node: usize, shape: Shape) -> (usize, Shape) {
    if let Shape::List(element) = shape { (node, *element) }
    else { (builder.add(Op::Enlist, vec![node]), shape) }
}

fn gathered_shape(positions: &Shape, element: &Shape) -> Shape {
    match positions {
        Shape::Int => element.clone(),
        Shape::Unit => Shape::Unit,
        Shape::Prod(fields) => Shape::Prod(fields.iter().map(|s| gathered_shape(s, element)).collect()),
        Shape::List(s) => Shape::List(Box::new(gathered_shape(s, element))),
        Shape::Sum(lanes) => Shape::Sum(lanes.iter().map(|s| gathered_shape(s, element)).collect()),
        _ => unreachable!("positions use integers and containers"),
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
    match rng.below(16) {
        0 => (input, shape.clone()),
        7 => (builder.tuple(vec![]), Shape::Unit),
        8 => (builder.add(Op::Lit(Value::Prod(vec![])), vec![input]), Shape::Unit),
        9 => {
            let (context, context_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (payload, payload_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (list, element) = as_list(builder, payload, payload_shape);
            let pair = builder.tuple(vec![context, list]);
            (builder.add(Op::CapList, vec![pair]), Shape::List(Box::new(Shape::Prod(vec![context_shape, element]))))
        }
        10 => {
            let (payload, payload_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (list, element) = as_list(builder, payload, payload_shape);
            let mut body = Builder::default();
            let item = body.input();
            let hash = body.add(Op::Hash, vec![item]);
            let mask = body.add(ArithOp::BitsImm(BitOp::And, 1), vec![hash]);
            let pair = body.tuple(vec![mask, item]);
            let pairs = builder.add(Op::MapList(Box::new(body.finish(pair))), vec![list]);
            (builder.add(Op::Filter, vec![pairs]), Shape::List(Box::new(element)))
        }
        11 => {
            let (payload, payload_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (mut list, element) = as_list(builder, payload, payload_shape);
            if rng.below(2) == 0 { list = builder.add(Op::Ref, vec![list]); }
            let positions = random_shape(rng, 2);
            let indices = random_value(rng, &positions, 1);
            let indices = builder.add(Op::Lit(indices), vec![input]);
            let pair = builder.tuple(vec![indices, list]);
            let output_shape = gathered_shape(&positions, &element);
            (builder.add(Op::Gather, vec![pair]), output_shape)
        }
        12 => {
            let (left, branch_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let right = if rng.below(3) == 0 { left } else {
                let value = random_value(rng, &branch_shape, 1);
                builder.add(Op::Lit(value), vec![input])
            };
            let hash = builder.add(Op::Hash, vec![input]);
            let mask = builder.add(ArithOp::BitsImm(BitOp::And, 1), vec![hash]);
            let args = builder.tuple(vec![mask, left, right]);
            (builder.add(Op::Select, vec![args]), branch_shape)
        }
        13 => {
            let (payload, payload_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (list, element) = as_list(builder, payload, payload_shape);
            let nested = builder.add(Op::Enlist, vec![list]);
            let ranges = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Int])));
            (builder.add(Op::Flatten, vec![nested]), Shape::Prod(vec![ranges, Shape::List(Box::new(element))]))
        }
        14 => {
            let (payload, payload_shape) = random_graph(builder, input, shape, rng, depth - 1);
            let (list, element) = as_list(builder, payload, payload_shape);
            (builder.add(Op::Ref, vec![list]), Shape::Ref(Box::new(Shape::List(Box::new(element)))))
        }
        15 => {
            let literal_shape = random_shape(rng, 2);
            let value = random_value(rng, &literal_shape, 1);
            (builder.add(Op::Lit(value), vec![input]), literal_shape)
        }
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


fn random_request(rng: &mut DemandRng, shape: &Shape) -> Demand {
    if rng.below(4) == 0 { return Demand::None; }
    match shape {
        Shape::Prod(fields) => Demand::Prod(fields.iter().map(|s| random_request(rng,s)).collect()),
        Shape::List(s) => Demand::List(Box::new(random_request(rng,s))),
        Shape::Ref(list) => {
            let Shape::List(element) = &**list else { unreachable!() };
            Demand::Ref(Box::new(Demand::List(Box::new(random_request(rng, element)))))
        }
        Shape::Sum(lanes) => Demand::Sum(lanes.iter().map(|s| random_request(rng,s)).collect()),
        _ => Demand::All,
    }
}

fn check(graph: &Graph<NumOp>, input: Value, request: &Demand) -> (Graph<NumOp>, Demand) {
    let shape = shape_of_value(&input);
    let original = eval_graph(graph, input.clone());
    let graph = compile(graph.clone(), &shape).unwrap();
    let Narrowed { graph: rewritten, input: need, output } = narrow(graph, request).unwrap();
    let original_shape = shape_of_value(&original);
    let (union, _) = Demand::union_for_shape(&original_shape, &output, request).unwrap();
    assert_eq!(union, output);
    let expected_shape = output.projected_shape(&original_shape).unwrap();
    let expected = output.project(original).unwrap();
    let narrowed = need.project(input).unwrap();
    assert_eq!(shape_of_value(&narrowed), need.projected_shape(&shape).unwrap());
    assert_eq!(shape_of(&rewritten, &shape_of_value(&narrowed)).unwrap(), expected_shape);
    assert_eq!(shape_of_value(&expected), expected_shape);
    assert_eq!(eval_graph(&rewritten, narrowed), expected);
    (rewritten, need)
}

fn random_coverage(graph: &Graph<NumOp>, present: &mut [bool; 8]) {
    for node in graph.nodes() {
        match node {
            NodeRef::Tuple([]) => present[7] = true,
            NodeRef::Op(op, _) => {
                match op {
                    NumOp::Core(Op::Gather) => present[0] = true,
                    NumOp::Core(Op::Filter) => present[1] = true,
                    NumOp::Core(Op::CapList) => present[2] = true,
                    NumOp::Core(Op::Select) => present[3] = true,
                    NumOp::Core(Op::Flatten) => present[4] = true,
                    NumOp::Core(Op::Ref) => present[5] = true,
                    NumOp::Core(Op::Lit(_)) => present[6] = true,
                    _ => {}
                }
                for body in op.children() { random_coverage(body, present); }
            }
            _ => {}
        }
    }
}

#[test]
fn random_demand_rewrites_preserve_values() {
    let cases = std::env::var("CORGI_DEMAND_CASES").map(|s| s.parse::<u64>().unwrap()).unwrap_or(200);
    let mut counts = [0; 8];
    for seed in 1..=cases {
        let mut rng = DemandRng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let shape = random_shape(&mut rng, 2);
        let rows = 1 + rng.below(20);
        let input = random_value(&mut rng, &shape, rows);
        let mut builder = Builder::default();
        let root = builder.input();
        let (output, output_shape) = random_graph(&mut builder, root, &shape, &mut rng, 4);
        let graph = builder.finish(output);
        let mut present = [false; 8];
        random_coverage(&graph, &mut present);
        for (count, present) in counts.iter_mut().zip(present) { *count += usize::from(present); }
        let request = random_request(&mut rng, &output_shape);
        check(&graph, Value::empty(&shape), &request);
        check(&graph, input.clone(), &Demand::All);
        check(&graph, input, &request);
    }
    println!("{cases} seeds; {} comparisons; graphs containing each op:", cases * 3);
    for (name, count) in ["Gather", "Filter", "CapList", "Select", "Flatten", "Ref", "Lit", "empty Tuple"].into_iter().zip(counts) {
        println!("{name}: {count}");
    }
}

#[test]
fn shared_maps_rebase_fields_and_unlisted_sum_lanes() {
    let graph = parse_ml("let p = input map (x -> (x.0 hash, x.1, x.2, x.3)) in (p map (x -> (x.1, x.3)), p map (x -> (x.2, x.3)))").unwrap();
    let input = Value::List(vec![0,2].into(),Box::new(Value::Prod(vec![Value::i64(vec![1,2]),Value::i64(vec![3,4]),Value::i64(vec![5,6]),Value::i64(vec![7,8])])));
    let (out, need) = check(&graph, input, &Demand::All);
    assert_eq!(need,Demand::List(Box::new(Demand::Prod(vec![Demand::None,Demand::All,Demand::All,Demand::All]))));
    assert!(!format!("{out:?}").contains("Hash"));
    assert_eq!(out.nodes().filter(|n| matches!(n,NodeRef::Op(NumOp::Core(Op::MapList(_)),_))).count(),3);

    let mut b = Builder::default(); let x = b.input();
    let out = b.add(Op::MapSum(vec![(0,parse_ml("(input.2, input.0)").unwrap())]),vec![x]);
    let graph = b.finish(out);
    let input = Value::sum(vec![0,1,0],vec![Value::Prod(vec![Value::i64(vec![1,2]),Value::i64(vec![3,4]),Value::i64(vec![5,6])]),Value::Prod(vec![Value::i64(vec![7]),Value::i64(vec![8])])]);
    check(&graph,input,&Demand::Sum(vec![Demand::Prod(vec![Demand::None,Demand::All]),Demand::Prod(vec![Demand::None,Demand::All])]));
}

#[test]
fn literal_anchors_and_reference_boundaries() {
    let mut body = Builder::default(); let x = body.input();
    let hash = body.add(Op::Hash,vec![x]);
    let lit = body.add(Op::Lit(Value::Prod(vec![Value::f64(vec![f64::from_bits(0x7ff8000000001234)]),Value::i64(vec![9])])),vec![hash]);
    let mut b = Builder::default(); let x = b.input();
    let body = body.finish(lit);
    let mapped = b.add(Op::MapList(Box::new(body.clone())),vec![x]);
    let graph = b.finish(mapped);
    let input = Value::List(vec![0,2,2].into(),Box::new(Value::i64(vec![1,2])));
    let (out,_) = check(&graph,input,&Demand::List(Box::new(Demand::Prod(vec![Demand::All,Demand::None]))));
    assert!(!format!("{out:?}").contains("Hash"));
    let mut b = Builder::default(); let x = b.input();
    let mapped = b.add(Op::MapSum(vec![(0, body)]),vec![x]);
    let graph = b.finish(mapped);
    for (tags, first, second) in [(vec![1,1],vec![],vec![4,5]), (vec![0,1,0],vec![2,3],vec![4])] {
        let input = Value::sum(tags,vec![Value::i64(first),Value::i64(second)]);
        check(&graph,input,&Demand::Sum(vec![Demand::Prod(vec![Demand::All,Demand::None]),Demand::All]));
    }
    for src in ["input ref len", "input len"] {
        let g = parse_ml(src).unwrap();
        let list = Value::List(vec![1,3].into(),Box::new(Value::Prod(vec![Value::i64(vec![1,2,3]),Value::i64(vec![4,5,6])])));
        let v = if src == "input len" { Value::Ref(std::sync::Arc::new(list),std::sync::Arc::new(vec![1,0,1])) } else {list};
        let (_,d) = check(&g,v,&Demand::All);
        assert_eq!(d, if src=="input len" {Demand::All}else{Demand::List(Box::new(Demand::None))});
    }
    // A final reference keeps its arena, including when nested in a requested field.
    let list = Value::List(vec![1,3].into(), Box::new(Value::Prod(vec![
        Value::i64(vec![1,2,3]), Value::i64(vec![4,5,6]),
    ])));
    let reference = Value::Ref(std::sync::Arc::new(list), std::sync::Arc::new(vec![1,0,1]));
    let request = Demand::Ref(Box::new(Demand::List(Box::new(Demand::Prod(vec![Demand::All,Demand::None])))));
    for nested in [false, true] {
        let (src, input, request, expected, need) = if nested {
            ("(input.1, input.0)", Value::Prod(vec![Value::i64(vec![7,8,9]), reference.clone()]),
                Demand::Prod(vec![request.clone(), Demand::None]), Value::Prod(vec![reference.clone()]),
                Demand::Prod(vec![Demand::None, Demand::All]))
        } else {
            ("input", reference.clone(), request.clone(), reference.clone(), Demand::All)
        };
        let shape = shape_of_value(&input);
        let graph = compile(parse_ml(src).unwrap(), &shape).unwrap();
        let Narrowed { graph: out, input: actual, output } = narrow(graph, &request).unwrap();
        assert_eq!(output, if nested { Demand::Prod(vec![Demand::All, Demand::None]) } else { Demand::All });
        assert_eq!(actual, need);
        assert_eq!(shape_of(&out, &actual.projected_shape(&shape).unwrap()).unwrap(), shape_of_value(&expected));
        assert_eq!(eval_graph(&out, actual.project(input).unwrap()), expected);
    }
    let graph = parse_ml("input map (x -> x ref) map (x -> x len)").unwrap();
    let input = Value::List(vec![0,2].into(),Box::new(Value::List(vec![1,2].into(),Box::new(Value::i64(vec![3,4])))));
    let (_,need) = check(&graph,input,&Demand::All);
    assert_eq!(need,Demand::List(Box::new(Demand::List(Box::new(Demand::None)))));
    let mut b=Builder::<NumOp>::default();b.input();let empty=b.tuple(vec![]);let lit=b.add(Op::Lit(Value::i64(vec![7])),vec![empty]);
    check(&b.finish(lit),Value::Unit(3),&Demand::All);
}

#[test]
fn movement_preserves_witnesses_and_projects_shared_payloads() {
    let pair = Value::Prod(vec![Value::i64(vec![10, 20, 30]), Value::i64(vec![40, 50, 60])]);
    let filtered_input = Value::List(vec![0, 2, 3].into(), Box::new(Value::Prod(vec![
        Value::i64(vec![1, 0, 1]), pair.clone(),
    ])));
    let filter = parse_ml("input filter").unwrap();
    let second_field = Demand::Prod(vec![Demand::None, Demand::All]);
    let second_payload = Demand::List(Box::new(second_field.clone()));
    check(&filter, filtered_input.clone(), &second_payload);
    check(&filter, Value::empty(&shape_of_value(&filtered_input)), &second_payload);
    let bounds_only = Demand::List(Box::new(Demand::None));
    check(&filter, filtered_input.clone(), &bounds_only);

    let rows = Value::List(vec![0, 2, 3].into(), Box::new(pair.clone()));
    let capture_input = Value::Prod(vec![pair.clone(), rows]);
    let capture = parse_ml("input cap_list").unwrap();
    let payload_without_context = Demand::List(Box::new(Demand::Prod(vec![Demand::None, second_field])));
    check(&capture, capture_input.clone(), &payload_without_context);
    check(&capture, Value::empty(&shape_of_value(&capture_input)), &payload_without_context);
    check(&capture, capture_input, &bounds_only);

    // Both operands are unit-shaped, but neither output field was requested.
    let mut builder = Builder::<NumOp>::default();
    let input = builder.input();
    let context = builder.add(Op::Unit, vec![input]);
    let empty = builder.tuple(vec![]);
    let list = builder.add(Op::Enlist, vec![empty]);
    let operands = builder.tuple(vec![context, list]);
    let captured = builder.add(Op::CapList, vec![operands]);
    let graph = builder.finish(captured);
    let unread_fields = Demand::List(Box::new(Demand::Prod(vec![Demand::None, Demand::None])));
    check(&graph, Value::Unit(3), &unread_fields);
    check(&graph, Value::Unit(0), &unread_fields);
    let compiled = compile(graph, &Shape::Unit).unwrap();
    let narrowed = narrow(compiled, &unread_fields).unwrap();
    assert_eq!(narrowed.output, unread_fields);
    assert_eq!(shape_of(&narrowed.graph, &Shape::Unit).unwrap(), Shape::List(Box::new(Shape::Unit)));

    let nested = Value::List(vec![0, 2].into(), Box::new(Value::List(vec![0, 3].into(), Box::new(pair))));
    let flatten = parse_ml("input flatten").unwrap();
    let ranges_only = Demand::Prod(vec![Demand::All, Demand::None]);
    check(&flatten, nested.clone(), &ranges_only);
    check(&flatten, Value::empty(&shape_of_value(&nested)), &ranges_only);
    let values_only = Demand::Prod(vec![Demand::None, second_payload]);
    check(&flatten, nested, &values_only);

    let shared = parse_ml("let f = input filter in (input, f map (x -> x.1))").unwrap();
    check(&shared, filtered_input, &Demand::All);

    // The success tag survives even when the parsed payload is not read.
    let select = parse_ml("(input.0, input.1 parse_int, input.2 parse_int) select").unwrap();
    let bytes = Value::List(vec![1, 2, 2].into(), Box::new(Value::u8(b"4x".to_vec())));
    let alternatives = Value::Prod(vec![Value::i64(vec![1, 0, 1]), bytes.clone(), bytes]);
    let tags_only = Demand::Sum(vec![Demand::None, Demand::None]);
    check(&select, alternatives, &tags_only);
}

#[test]
fn gather_references_misses_and_unequal_position_requests() {
    let arena = Value::List(vec![2, 2, 5].into(), Box::new(Value::Prod(vec![
        Value::i64(vec![1, 2, 3, 4, 5]),
        Value::i64(vec![11, 12, 13, 14, 15]),
        Value::i64(vec![21, 22, 23, 24, 25]),
    ])));
    let references = Value::Ref(std::sync::Arc::new(arena.clone()), std::sync::Arc::new(vec![2, 0, 2]));
    let positions = Value::Prod(vec![
        Value::i64(vec![1, -1, 9]),
        Value::List(vec![0, 2, 3].into(), Box::new(Value::i64(vec![0, 8, 2]))),
    ]);
    let first_field = Demand::Prod(vec![Demand::All, Demand::None, Demand::None]);
    let last_field = Demand::Prod(vec![Demand::None, Demand::None, Demand::All]);
    let unequal_requests = Demand::Prod(vec![first_field, Demand::List(Box::new(last_field))]);
    let positions_only = Demand::Prod(vec![Demand::None, Demand::List(Box::new(Demand::None))]);

    let gather_incoming = parse_ml("input gather").unwrap();
    let incoming = Value::Prod(vec![positions.clone(), references]);
    let (_, incoming_request) = check(&gather_incoming, incoming.clone(), &unequal_requests);
    assert_eq!(incoming_request, Demand::All);
    check(&gather_incoming, Value::empty(&shape_of_value(&incoming)), &unequal_requests);
    check(&gather_incoming, incoming, &positions_only);

    let gather_created = parse_ml("(input.0, input.1 ref) gather").unwrap();
    let created = Value::Prod(vec![positions, arena]);
    let (_, created_request) = check(&gather_created, created.clone(), &unequal_requests);
    let retained_payload = Demand::Prod(vec![Demand::All, Demand::None, Demand::All]);
    assert_eq!(created_request, Demand::Prod(vec![Demand::All, Demand::List(Box::new(retained_payload))]));
    check(&gather_created, Value::empty(&shape_of_value(&created)), &unequal_requests);
    check(&gather_created, created, &positions_only);

    // The returned reference keeps its arena. The other reader drops excess
    // fields after gathering from it, without copying the arena.
    let shared = parse_ml("let r = input.1 ref in (r, (input.0, r) gather map (x -> x.2))").unwrap();
    let input = Value::Prod(vec![
        Value::List(vec![1, 1, 2].into(), Box::new(Value::i64(vec![0, 1]))),
        Value::List(vec![1, 1, 2].into(), Box::new(Value::Prod(vec![
            Value::i64(vec![1, 2]), Value::i64(vec![3, 4]), Value::i64(vec![5, 6]),
        ]))),
    ]);
    check(&shared, input, &Demand::All);
}
