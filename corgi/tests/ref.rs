//! `Ref<List<T>>`: referenced list rows. `ref` takes them (nothing copied, through products and
//! sums), `clone` copies the rows out, `gather` (hence the capture family and literals) and the merges
//! move only row numbers, and the readers `get`/`gather`/`find`/`slices`/`len` accept a referenced
//! list.
//! These tests pin that a referenced haystack answers exactly as the list it references, that the two
//! spellings of a capture — by value and by reference — agree, and that references stay references
//! (over one arena) through the ops that move rows.

use corgi::{eval_graph, lower_effects, parse_ml, shape_of_value, show, Value};

fn run(src: &str, input: Value) -> Value {
    let g = lower_effects(&parse_ml(src).unwrap_or_else(|e| panic!("parse {src:?}: {e}")));
    eval_graph(&g, input)
}

fn seed(n: i64) -> Value {
    Value::i64(vec![n])
}

/// a 3-row list over a 6-element payload
fn haystack() -> Value {
    Value::List(vec![2, 5, 6].into(), Box::new(Value::i64(vec![10, 11, 20, 21, 22, 30])))
}

#[test]
fn ref_clone_round_trips_and_shows_as_the_rows() {
    let h = haystack();
    let referenced = run("input ref", h.clone());
    assert_eq!(show(&referenced), format!("Ref <{}>", show(&h)));
    assert_eq!(run("input ref clone", h.clone()), h);
    // `ref` passes through a product: the list field is referenced, the scalar stays by value
    let p = Value::Prod(vec![Value::i64(vec![1, 2, 3]), h.clone()]);
    assert_eq!(shape_of_value(&run("input ref", p.clone())).to_string(), "(Int, Ref<List<Int>>)");
    assert_eq!(run("input ref clone", p.clone()), p);
    // and `clone` of a value with no references is the value
    assert_eq!(run("input clone", p.clone()), p);
}


/// a list of references (row 0 holds three, row 1 two) into one arena of three rows, some named
/// more than once, and the lists those references name, copied out.
fn sub_list_references() -> (Value, Value) {
    use std::sync::Arc;
    let arena = Arc::new(Value::List(vec![1, 2, 4].into(), Box::new(Value::i64(vec![3, 7, 3, 1]))));
    let rows = Arc::new(vec![0, 1, 0, 2, 0]);
    let by_ref = Value::List(vec![3, 5].into(), Box::new(Value::Ref(arena, rows)));
    let by_val = Value::List(
        vec![3, 5].into(),
        Box::new(Value::List(vec![1, 2, 3, 5, 6].into(), Box::new(Value::i64(vec![3, 7, 3, 3, 1, 3])))),
    );
    (by_ref, by_val)
}

/// `clone` is deep: nested references come back as the lists they name.
#[test]
fn clone_removes_nested_references() {
    let (by_ref, by_val) = sub_list_references();
    assert_eq!(run("input clone", by_ref), by_val);
}

/// compare, sort, dedup and group read a referenced row as the list it names, and a sorted column of
/// references is still references.
#[test]
fn order_reads_through_references() {
    let (by_ref, by_val) = sub_list_references();
    for op in ["sort", "dedup", "map (s -> (s, s)) group", "map (s -> (s, s) lt)"] {
        let r = run(&format!("input {op}"), by_ref.clone());
        let v = run(&format!("input {op}"), by_val.clone());
        assert_eq!(corgi::hash(&r), corgi::hash(&v), "{op}");
        assert_eq!(run("input clone", r), v, "{op}");
    }
    let sorted = run("input sort", by_ref);
    let Value::List(_, inner) = &sorted else { panic!() };
    assert!(matches!(&**inner, Value::Ref(..)), "sorting references moves references");
}

/// merging reference columns over ONE arena moves row numbers only; over distinct arenas each
/// arena contributes the rows the result names, once (never once per reference, never its rows
/// that nothing names), and either way the rows are the rows.
#[test]
fn merges_keep_references() {
    use corgi::arrange::gather_lanes;
    use std::sync::Arc;
    // rows [1], [2, 3], [4]
    let arena = Arc::new(Value::List(vec![1, 3, 4].into(), Box::new(Value::i64(vec![1, 2, 3, 4]))));
    let a = Value::Ref(arena.clone(), Arc::new(vec![0, 1]));
    let b = Value::Ref(arena.clone(), Arc::new(vec![2]));
    let (tags, off) = ([1, 0, 0], [0, 1, 0]);
    let merged = gather_lanes(&[Some(&a), Some(&b)], &tags, &off);
    let Value::Ref(p, rows) = &merged else { panic!("a merge of references is references") };
    assert!(Arc::ptr_eq(p, &arena), "one arena: row numbers only");
    assert_eq!(**rows, vec![2, 1, 0]);

    // rows [9, 8], [7], [6, 5]
    let other = Arc::new(Value::List(vec![2, 3, 5].into(), Box::new(Value::i64(vec![9, 8, 7, 6, 5]))));
    let c = Value::Ref(other, Arc::new(vec![0, 0, 1]));
    let merged = gather_lanes(&[Some(&a), Some(&c)], &[1, 0, 1, 0, 1], &[0, 0, 1, 1, 2]);
    let Value::Ref(p, _) = &merged else { panic!() };
    let Value::List(_, held) = &**p else { panic!("an arena is a list") };
    assert_eq!((p.len(), held.len()), (4, 6), "a's two rows and c's two, each once; c's unnamed [6, 5] left behind");
    let expect = Value::List(vec![2, 3, 5, 7, 8].into(), Box::new(Value::i64(vec![9, 8, 1, 9, 8, 2, 3, 7])));
    assert_eq!(run("input clone", merged), expect);
}

/// a fold whose state is a reference keeps it a reference into the same arena, round after round:
/// the state is overwritten row number by row number in place, never rebuilt from the rows it names.
#[test]
fn fold_state_stays_a_reference() {
    use std::sync::Arc;
    let arena = Arc::new(Value::List(vec![2, 3].into(), Box::new(Value::i64(vec![5, 6, 7]))));
    let seed = Value::Ref(arena.clone(), Arc::new(vec![0, 1]));
    let xs = Value::List(vec![3, 4].into(), Box::new(Value::i64(vec![0, 1, 0, 1])));
    let out = run(
        "let (s, xs) = input in (s, xs) fold ((acc, x) -> (x, acc, acc) select)",
        Value::Prod(vec![seed.clone(), xs]),
    );
    let Value::Ref(p, _) = &out else { panic!("the fold's state is still a reference") };
    assert!(Arc::ptr_eq(p, &arena));
    assert_eq!(out, seed);
}

/// the codec carries a reference column as its row numbers and its arena once: same shape, same
/// rows, including a reference to an empty row.
#[test]
fn bytes_round_trip_a_reference() {
    use std::sync::Arc;
    let arena = Arc::new(Value::List(vec![2, 3, 3].into(), Box::new(Value::i64(vec![10, 11, 12]))));
    let r = Value::Prod(vec![Value::i64(vec![1, 2, 3]), Value::Ref(arena.clone(), Arc::new(vec![1, 0, 2]))]);
    let mut buf = Vec::new();
    corgi::bytes::write_to(&r, &mut buf).unwrap();
    assert_eq!(buf.len(), corgi::bytes::length_in_bytes(&r));
    let (back, used) = corgi::bytes::read_from(&buf).unwrap();
    assert_eq!((back, used), (r, buf.len()));
    // a row past its arena, or an arena that is not a list, is refused, not trusted
    for bad in [
        Value::Ref(arena, Arc::new(vec![3])),
        Value::Ref(Arc::new(Value::i64(vec![10])), Arc::new(vec![0])),
    ] {
        let mut buf = Vec::new();
        corgi::bytes::write_to(&bad, &mut buf).unwrap();
        assert!(corgi::bytes::read_from(&buf).is_err());
    }
}

/// every reader gives the same answer on `h ref` as on `h`.
#[test]
fn readers_agree_through_a_box() {
    let h = haystack();
    let idx = Value::i64(vec![1, 2, 0]);
    let lists = Value::List(vec![1, 3, 4].into(), Box::new(Value::i64(vec![1, 2, 0, 0])));
    let needles = Value::List(vec![1, 2, 3].into(), Box::new(Value::i64(vec![11, 21, 22, 5])));
    let ranges = Value::List(
        vec![1, 3, 4].into(),
        Box::new(Value::Prod(vec![Value::i64(vec![0, 0, 2, 0]), Value::i64(vec![2, 1, 3, 1])])),
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
        assert_eq!(a, b, "{name}: referenced haystack disagrees with the list");
    }
}

#[test]
fn cap_list_of_a_referenced_list_agrees_with_the_copy() {
    let by_ref = run(
        "let xs = input iota in let ys = xs map (y -> (y, 2) div) in \
         (xs ref, ys) cap_list map ((c, y) -> (y, c) get)",
        seed(6),
    );
    let by_value = run(
        "let xs = input iota in let ys = xs map (y -> (y, 2) div) in \
         (xs, ys) cap_list map ((c, y) -> (y, c) get)",
        seed(6),
    );
    let via_gather = run(
        "let xs = input iota in let ys = xs map (y -> (y, 2) div) in (ys, xs) gather",
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
        "let xs = input iota in let p = (xs, xs map (y -> (y, 2) div)) in let b = p ref in (b.1 clone, b.0 clone)",
        seed(6),
    );
    let expect = run("let xs = input iota in (xs map (y -> (y, 2) div), xs)", seed(6));
    assert_eq!(out, expect);
    // and a list field of a referenced product comes back as a referenced LIST (spans), readable by `gather`
    let by_ref = run(
        "let xs = input iota in let ys = xs map (y -> (y, 2) div) in let b = (xs, xs) ref in (ys, b.1) gather",
        seed(6),
    );
    let by_value = run(
        "let xs = input iota in let ys = xs map (y -> (y, 2) div) in (ys, xs) gather",
        seed(6),
    );
    assert_eq!(by_ref, by_value);
    assert_eq!(show(&by_ref), "Sum tags=[0] [List ends=[6] <[0, 0, 1, 1, 2, 2]>, ()x0]");
}

/// a Box where a list is required is the shape error "clone first", not a silent copy.
#[test]
fn a_box_is_not_silently_materialized() {
    let g = parse_ml("input ref map (x -> x)").unwrap();
    let err = corgi::shape_of(&g, &corgi::Shape::List(Box::new(corgi::Shape::Int)));
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
    let adj_vals: Vec<i64> = (0..40).collect();
    let adj = Value::List(vec![adj_vals.len()].into(), Box::new(Value::i64(adj_vals)));
    let by_ref = Value::Ref(Arc::new(adj), Arc::new(vec![0; anchors]));
    let ranges = Value::List(
        vec![1, 2, 3, 4].into(),
        Box::new(Value::Prod(vec![Value::i64(vec![0, 10, 20, 30]), Value::i64(vec![10, 20, 30, 40])])),
    );
    let small = Value::List(vec![2, 4, 6, 8].into(), Box::new(Value::i64(vec![3, 9, 10, 15, 25, 29, 30, 99])));
    let prog = |adj: &str| {
        format!(
            "let (small, ranges, adj) = input in let hay = ((ranges len, 1) sub, (ranges, {adj}) slices) get in (small, hay) find"
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
    let seed = Value::List(vec![1, 2].into(), Box::new(Value::i64(vec![0, 0])));
    let xs = Value::List(vec![1, 1 + n].into(), Box::new(Value::i64(vec![3; 1 + n])));
    let out = run(
        "let (s, xs) = input in (s ref, xs) fold ((acc, x) -> x iota ref)",
        Value::Prod(vec![seed, xs]),
    );
    let Value::Ref(arena, _) = &out else { panic!("the fold's state is still a reference") };
    let Value::List(_, held) = &**arena else { panic!("an arena is a list") };
    assert!(held.len() <= 6, "the state's arena holds {} elements for 2 rows of 3", held.len());
    assert_eq!(show(&run("input clone", out)), "List ends=[3, 6] <[0, 1, 2, 0, 1, 2]>");
}
