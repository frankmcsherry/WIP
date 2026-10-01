//! `Ref<List<T>>`: referenced list rows. `ref` takes them (nothing copied, through products and
//! sums), `clone` copies the rows out, `gather` (hence the capture family and literals) and the merges
//! move only spans, and the readers `get`/`gather`/`find`/`slices`/`len` accept a referenced list.
//! These tests pin that a referenced haystack answers exactly as the list it references, that the two
//! spellings of a capture — by value and by reference — agree, and that references stay references
//! (over one arena) through the ops that move rows.

use corgi::{eval_graph, lower_effects, parse_ml, shape_of_value, show, Value};

fn run(src: &str, input: Value) -> Value {
    let g = lower_effects(&parse_ml(src).unwrap_or_else(|e| panic!("parse {src:?}: {e}")));
    eval_graph(&g, input)
}

fn seed(n: u64) -> Value {
    Value::u64(vec![n])
}

/// a 3-row list over a 6-element payload
fn haystack() -> Value {
    Value::List(vec![2, 5, 6].into(), Box::new(Value::u64(vec![10, 11, 20, 21, 22, 30])))
}

#[test]
fn ref_clone_round_trips_and_shows_as_the_rows() {
    let h = haystack();
    let referenced = run("input ref", h.clone());
    assert_eq!(show(&referenced), format!("Ref <{}>", show(&h)));
    assert_eq!(run("input ref clone", h.clone()), h);
    // `ref` passes through a product: the list field is referenced, the scalar stays by value
    let p = Value::Prod(vec![Value::u64(vec![1, 2, 3]), h.clone()]);
    assert_eq!(shape_of_value(&run("input ref", p.clone())).to_string(), "(U64, Ref<List<U64>>)");
    assert_eq!(run("input ref clone", p.clone()), p);
    // and `clone` of a value with no references is the value
    assert_eq!(run("input clone", p.clone()), p);
}

/// `clone` is deep: the references `slices` hands out on a referenced haystack come back as the
/// lists `slices` copies out of a plain one.
#[test]
fn clone_removes_nested_references() {
    let ranges = Value::List(
        vec![2, 2, 3].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 1, 0]), Value::u64(vec![2, 2, 1])])),
    );
    let arg = Value::Prod(vec![ranges, haystack()]);
    let copied = run("let (r, h) = input in (r, h) slices", arg.clone());
    let cloned = run("let (r, h) = input in (r, h ref) slices clone", arg);
    assert_eq!(copied, cloned);
}

/// compare, sort, dedup and group read a referenced row as the list it names, and a sorted column of
/// references is still references.
#[test]
fn order_reads_through_references() {
    let ranges = Value::List(
        vec![3, 5].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![1, 0, 1, 0, 0]), Value::u64(vec![2, 1, 2, 2, 1])])),
    );
    let h = Value::List(vec![2, 4].into(), Box::new(Value::u64(vec![7, 3, 3, 1])));
    let arg = Value::Prod(vec![ranges, h]);
    for op in ["sort", "dedup", "map (s -> (s, s)) group", "map (s -> (s, s) lt)"] {
        let by_ref = run(&format!("let (r, h) = input in (r, h ref) slices {op}"), arg.clone());
        let by_val = run(&format!("let (r, h) = input in (r, h) slices {op}"), arg.clone());
        assert_eq!(corgi::hash(&by_ref), corgi::hash(&by_val), "{op}");
        assert_eq!(run("input clone", by_ref), by_val, "{op}");
    }
    let sorted = run("let (r, h) = input in (r, h ref) slices sort", arg);
    let Value::Sum(_, lanes) = &sorted else { panic!() };
    let Value::List(_, inner) = &lanes[0] else { panic!() };
    assert!(matches!(&**inner, Value::Ref(..)), "sorting references moves references");
}

/// merging reference columns over ONE arena moves spans only; over distinct arenas each arena
/// contributes the union of its picked spans, once (never once per reference, never its dead
/// elements), and either way the rows are the rows.
#[test]
fn merges_keep_references() {
    use corgi::arrange::gather_lanes;
    use std::sync::Arc;
    let arena = Arc::new(Value::u64(vec![1, 2, 3, 4]));
    let a = Value::Ref(arena.clone(), Arc::new(vec![(0, 4), (1, 2)]));
    let b = Value::Ref(arena.clone(), Arc::new(vec![(2, 4)]));
    let (tags, off) = ([1, 0, 0], [0, 1, 0]);
    let merged = gather_lanes(&[Some(&a), Some(&b)], &tags, &off);
    let Value::Ref(p, spans) = &merged else { panic!("a merge of references is references") };
    assert!(Arc::ptr_eq(p, &arena), "one arena: spans only");
    assert_eq!(**spans, vec![(2, 4), (1, 2), (0, 4)]);

    let other = Arc::new(Value::u64(vec![9, 8, 7, 6, 5]));
    let c = Value::Ref(other, Arc::new(vec![(0, 2), (0, 2), (1, 2)]));
    let merged = gather_lanes(&[Some(&a), Some(&c)], &[1, 0, 1, 0, 1], &[0, 0, 1, 1, 2]);
    let Value::Ref(p, _) = &merged else { panic!() };
    assert_eq!(p.len(), 6, "a's [0, 4) and c's [0, 2), each once; c's unreferenced [2, 5) left behind");
    let expect = Value::List(vec![2, 6, 8, 9, 10].into(), Box::new(Value::u64(vec![9, 8, 1, 2, 3, 4, 9, 8, 2, 8])));
    assert_eq!(run("input clone", merged), expect);
}

/// a fold whose state is a reference keeps it a reference into the same arena, round after round:
/// the state is overwritten span by span in place, never rebuilt from the rows it names.
#[test]
fn fold_state_stays_a_reference() {
    use std::sync::Arc;
    let arena = Arc::new(Value::u64(vec![5, 6, 7]));
    let seed = Value::Ref(arena.clone(), Arc::new(vec![(0, 3), (1, 2)]));
    let xs = Value::List(vec![3, 4].into(), Box::new(Value::u64(vec![0, 1, 0, 1])));
    let out = run(
        "let (s, xs) = input in (s, xs) fold ((acc, x) -> (x, acc, acc) select)",
        Value::Prod(vec![seed.clone(), xs]),
    );
    let Value::Ref(p, _) = &out else { panic!("the fold's state is still a reference") };
    assert!(Arc::ptr_eq(p, &arena));
    assert_eq!(out, seed);
}

/// the codec carries a reference column as its spans and its payload once: same shape, same rows.
#[test]
fn bytes_round_trip_a_reference() {
    use std::sync::Arc;
    let r = Value::Prod(vec![
        Value::u64(vec![1, 2]),
        Value::Ref(Arc::new(Value::u64(vec![10, 11, 12])), Arc::new(vec![(0, 3), (1, 2)])),
    ]);
    let mut buf = Vec::new();
    corgi::bytes::write_to(&r, &mut buf).unwrap();
    assert_eq!(buf.len(), corgi::bytes::length_in_bytes(&r));
    let (back, used) = corgi::bytes::read_from(&buf).unwrap();
    assert_eq!((back, used), (r, buf.len()));
    // a span past its payload is refused, not trusted
    let bad = Value::Ref(Arc::new(Value::u64(vec![10])), Arc::new(vec![(0, 2)]));
    let mut buf = Vec::new();
    corgi::bytes::write_to(&bad, &mut buf).unwrap();
    assert!(corgi::bytes::read_from(&buf).is_err());
}

/// every reader gives the same answer on `h ref` as on `h`.
#[test]
fn readers_agree_through_a_box() {
    let h = haystack();
    let idx = Value::u64(vec![1, 2, 0]);
    let lists = Value::List(vec![1, 3, 4].into(), Box::new(Value::u64(vec![1, 2, 0, 0])));
    let needles = Value::List(vec![1, 2, 3].into(), Box::new(Value::u64(vec![11, 21, 22, 5])));
    let ranges = Value::List(
        vec![1, 3, 4].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 0, 2, 0]), Value::u64(vec![2, 1, 3, 1])])),
    );
    for (name, lhs, by_value, by_ref) in [
        ("get", Some(idx), "let (i, h) = input in (i, h) get", "let (i, h) = input in (i, h ref) get"),
        ("gather", Some(lists), "let (i, h) = input in (i, h) gather", "let (i, h) = input in (i, h ref) gather"),
        ("find", Some(needles), "let (n, h) = input in (n, h) find", "let (n, h) = input in (n, h ref) find"),
        ("slices", Some(ranges), "let (r, h) = input in (r, h) slices", "let (r, h) = input in (r, h ref) slices"),
        ("len", None, "input len", "input ref len"),
    ] {
        let arg = match lhs {
            Some(l) => Value::Prod(vec![l, h.clone()]),
            None => h.clone(),
        };
        let a = run(by_value, arg.clone());
        let b = run(by_ref, arg);
        // `slices` on a ref returns references (its own test below); the rest are by value.
        if name == "slices" {
            assert_eq!(show(&a), show(&b).replace("Ref <", "").replacen(">>>", ">>", 1), "{name}");
        } else {
            assert_eq!(a, b, "{name}: referenced haystack disagrees with the list");
        }
    }
}

/// `slices` on a referenced haystack hands out references; on a list it copies. Same rows either way.
#[test]
fn slices_on_a_box_is_by_reference() {
    let h = haystack();
    let ranges = Value::List(
        vec![2, 2, 3].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 1, 0]), Value::u64(vec![2, 2, 1])])),
    );
    let copied = run("let (r, h) = input in (r, h) slices", Value::Prod(vec![ranges.clone(), h.clone()]));
    let referenced = run("let (r, h) = input in (r, h ref) slices", Value::Prod(vec![ranges, h]));
    let expect = "Sum tags=[0, 0, 0] [List ends=[2, 2, 3] <List ends=[2, 3, 4] <[10, 11, 11, 30]>>, ()x0]";
    assert_eq!(show(&copied), expect);
    assert_eq!(show(&referenced), expect.replace("<List ends=[2, 3, 4]", "<Ref <List ends=[2, 3, 4]").replace(">>, ()x0]", ">>>, ()x0]"));
    let Value::Sum(_, lanes) = &referenced else { panic!() };
    let Value::List(_, inner) = &lanes[0] else { panic!() };
    assert!(matches!(&**inner, Value::Ref(..)), "the inner rows are references");
}

/// the capture: a referenced list context is one reference per element and the body's `get` reads
/// through it — the same answer as the by-value capture and as the capture-free `gather`.
#[test]
fn cap_list_of_a_referenced_list_agrees_with_the_copy() {
    let by_ref = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in \
         (xs ref, ys) cap_list map ((c, y) -> (y, c) get)",
        seed(6),
    );
    let by_value = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in \
         (xs, ys) cap_list map ((c, y) -> (y, c) get)",
        seed(6),
    );
    let via_gather = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in (ys, xs) gather",
        seed(6),
    );
    assert_eq!(by_ref, by_value);
    assert_eq!(show(&by_ref), show(&via_gather));
    assert_eq!(show(&by_ref), "Sum tags=[0] [List ends=[6] <[0, 0, 1, 1, 2, 2]>, ()x0]");
}

/// `ref` of a product references its list fields, so `Field` is ordinary projection and a list
/// field comes back referenced; `clone` then copies just that field.
#[test]
fn field_of_a_referenced_product() {
    let out = run(
        "let xs = input iota in let p = (xs, xs map (y -> y shr 1)) in let b = p ref in (b.1 clone, b.0 clone)",
        seed(6),
    );
    let expect = run("let xs = input iota in (xs map (y -> y shr 1), xs)", seed(6));
    assert_eq!(out, expect);
    // and a list field of a referenced product comes back as a referenced LIST (spans), readable by `gather`
    let by_ref = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in let b = (xs, xs) ref in (ys, b.1) gather",
        seed(6),
    );
    let by_value = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in (ys, xs) gather",
        seed(6),
    );
    assert_eq!(by_ref, by_value);
    assert_eq!(show(&by_ref), "Sum tags=[0] [List ends=[6] <[0, 0, 1, 1, 2, 2]>, ()x0]");
}

/// a Box where a list is required is the shape error "clone first", not a silent copy.
#[test]
fn a_box_is_not_silently_materialized() {
    let g = parse_ml("input ref map (x -> x)").unwrap();
    let err = corgi::shape_of(&g, &corgi::Shape::List(Box::new(corgi::Shape::Prim(64))));
    assert!(err.is_err(), "map over a Box must be a shape error");
    let msg = err.unwrap_err();
    assert!(msg.contains("expected a list") && msg.contains("Ref<"), "{msg}");
}

/// the WCO step: per anchor, every element of the small side searches its anchor's range of a
/// shared adjacency held by reference. The ref spelling agrees with the copying one.
#[test]
fn wco_step_searches_through_references() {
    use std::sync::Arc;
    let anchors = 4;
    let adj_vals: Vec<u64> = (0..40).collect();
    let by_ref = Value::Ref(Arc::new(Value::u64(adj_vals)), Arc::new(vec![(0, 40); anchors]));
    let ranges = Value::List(
        vec![1, 2, 3, 4].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 10, 20, 30]), Value::u64(vec![10, 20, 30, 40])])),
    );
    let small = Value::List(vec![2, 4, 6, 8].into(), Box::new(Value::u64(vec![3, 9, 10, 15, 25, 29, 30, 99])));
    let prog = |adj: &str| {
        format!(
            "let (small, ranges, adj) = input in let hay = ((ranges len, 1u64) sub, (ranges, {adj}) slices) get in (small, hay) find"
        )
    };
    let arg = Value::Prod(vec![small, ranges, by_ref]);
    let a = run(&prog("adj"), arg.clone());
    let b = run(&prog("adj clone"), arg);
    assert_eq!(a, b);
    // row-relative (lo, hi) per needle: 3 -> (3,4), 9 -> (9,10) in [0,10); 10 -> (0,1), 15 -> (5,6) in
    // [10,20); 25 -> (5,6), 29 -> (9,10); 30 -> (0,1), 99 -> (10,10) (absent) in [30,40)
    assert_eq!(
        show(&a),
        "Sum tags=[0, 0, 0, 0] [List ends=[2, 4, 6, 8] <([3, 9, 0, 5, 5, 9, 0, 10], [4, 10, 1, 6, 6, 10, 1, 10])>, ()x0]"
    );
}

/// a fold whose state is rebuilt as a fresh reference every round, over ragged rows (lengths 1 and
/// n): each round merges the finished row's state with the running row's new one, over distinct
/// arenas. The merge keeps only the rows still referenced, so the state's arena holds the live
/// rows — O(n) total, as by value — rather than accumulating every round's arena (O(n²)).
#[test]
fn fold_state_across_arenas_holds_only_live_rows() {
    let n = 2000;
    let seed = Value::List(vec![1, 2].into(), Box::new(Value::u64(vec![0, 0])));
    let xs = Value::List(vec![1, 1 + n].into(), Box::new(Value::u64(vec![3; 1 + n])));
    let out = run(
        "let (s, xs) = input in (s ref, xs) fold ((acc, x) -> x iota ref)",
        Value::Prod(vec![seed, xs]),
    );
    let Value::Ref(payload, _) = &out else { panic!("the fold's state is still a reference") };
    assert!(payload.len() <= 6, "the state's arena holds {} elements for 2 rows of 3", payload.len());
    assert_eq!(show(&run("input clone", out)), "List ends=[3, 6] <[0, 1, 2, 0, 1, 2]>");
}
