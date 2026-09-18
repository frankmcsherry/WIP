//! `Bounds::Spans`: a list whose rows are references into a shared payload. The capture family
//! (`cap_list`, `lit`, `slices`) produces it; the span-aware readers (`get`, `gather`, `find`, `len`)
//! read through it; everything else sees a partition via `into_list`'s compaction. These tests pin
//! that a shared list is the same VALUE as its copied form at every one of those seams.

use corgi::{eval_graph, lower_effects, parse_ml, show, Bounds, Value};

fn run(src: &str, input: Value) -> Value {
    let g = lower_effects(&parse_ml(src).unwrap_or_else(|e| panic!("parse {src:?}: {e}")));
    eval_graph(&g, input)
}

fn seed(n: u64) -> Value {
    Value::u64(vec![n])
}

/// a list of 3 rows over a 6-element payload, once as a partition and once as spans referencing
/// the same rows in a different layout (repeated / out of order over a longer payload).
fn partition_and_shared() -> (Value, Value) {
    let part = Value::List(vec![2, 5, 6].into(), Box::new(Value::u64(vec![10, 11, 20, 21, 22, 30])));
    let shared = Value::List(
        Bounds::Spans(vec![(4, 6), (1, 4), (0, 1)]),
        Box::new(Value::u64(vec![30, 20, 21, 22, 10, 11, 99, 99])),
    );
    (part, shared)
}

#[test]
fn shared_and_partition_lists_are_one_value() {
    let (part, shared) = partition_and_shared();
    assert_eq!(part, shared);
    assert_eq!(show(&part), show(&shared));
    let hash = |v: &Value| {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    };
    assert_eq!(hash(&part), hash(&shared));
    // compaction is the partition
    let c = shared.clone().compact();
    assert!(matches!(&c, Value::List(b, _) if b.is_partition()));
    assert_eq!(c, part);
    // the accessors: `into_list` hands back a partition, `into_list_shared` the spans as stored.
    let (b, _) = shared.clone().into_list("test").unwrap();
    assert!(b.is_partition());
    let (b, _) = shared.clone().into_list_shared("test").unwrap();
    assert!(!b.is_partition());
}

/// every span-aware reader gives the same answer on the shared haystack as on the partition.
#[test]
fn readers_agree_through_spans() {
    let (part, shared) = partition_and_shared();
    let idx = Value::u64(vec![1, 2, 0]);
    let lists = Value::List(vec![1, 3, 4].into(), Box::new(Value::u64(vec![1, 2, 0, 0])));
    let needles = Value::List(vec![1, 2, 3].into(), Box::new(Value::u64(vec![11, 21, 22, 5])));
    let ranges = Value::List(
        vec![1, 3, 4].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 0, 2, 0]), Value::u64(vec![2, 1, 3, 1])])),
    );
    let pair = |lhs: Value| move |h: &Value| Value::Prod(vec![lhs.clone(), h.clone()]);
    let alone = |h: &Value| h.clone();
    type Arg = Box<dyn Fn(&Value) -> Value>;
    let cases: Vec<(&str, &str, Arg)> = vec![
        ("get", "let (i, h) = input in (i, h) get", Box::new(pair(idx))),
        ("gather", "let (i, h) = input in (i, h) gather", Box::new(pair(lists))),
        ("find", "let (n, h) = input in (n, h) find", Box::new(pair(needles))),
        ("slices", "let (r, h) = input in (r, h) slices", Box::new(pair(ranges))),
        ("len", "input len", Box::new(alone)),
        ("map", "input map (x -> x)", Box::new(alone)),
        ("sort", "input sort", Box::new(alone)),
        ("reduce", "input fold_add", Box::new(alone)),
    ];
    for (name, prog, arg) in cases {
        let a = run(prog, arg(&part));
        let b = run(prog, arg(&shared));
        assert_eq!(a, b, "{name}: shared haystack disagrees with the partition");
        assert_eq!(show(&a), show(&b), "{name}");
    }
}

/// `slices` shares the haystack: the inner list's rows are spans, and it still equals the copy.
#[test]
fn slices_shares_the_haystack() {
    let (part, _) = partition_and_shared();
    let ranges = Value::List(
        vec![2, 2, 3].into(),
        Box::new(Value::Prod(vec![Value::u64(vec![0, 1, 0]), Value::u64(vec![2, 2, 1])])),
    );
    let out = run("let (r, h) = input in (r, h) slices", Value::Prod(vec![ranges, part]));
    // Fail<List<List<T>>>: Ok lane 0 holds the list
    let Value::Sum(_, _, lanes) = &out else { panic!("expected the Fail sum") };
    let Value::List(_, inner) = &lanes[0] else { panic!("expected the outer list") };
    let Value::List(ib, _) = &**inner else { panic!("expected the inner list") };
    // row 0's ranges (0,2) and (1,2) sit at haystack row 0 = [0,2); row 2's (0,1) at row 2 = [5,6)
    assert_eq!(*ib, Bounds::Spans(vec![(0, 2), (1, 2), (5, 6)]));
    assert_eq!(
        show(&out),
        "Sum tags=[0, 0, 0] [List ends=[2, 2, 3] <List ends=[2, 3, 4] <[10, 11, 11, 30]>>, ()x0]"
    );
}

/// the capture: a List-shaped context becomes one span per element, and the body's `get` reads
/// through it — the same answer as the capture-free `gather` spelling.
#[test]
fn cap_list_captures_a_list_by_reference() {
    let via_capture = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in \
         (xs, ys) cap_list map ((c, y) -> (y, c) get)",
        seed(6),
    );
    let via_gather = run(
        "let xs = input iota in let ys = xs map (y -> y shr 1) in (ys, xs) gather",
        seed(6),
    );
    // both are Fail<List<U64>> after lowering; the Ok payload is [0,0,1,1,2,2]
    let payload = |v: &Value| match v {
        Value::Sum(_, _, lanes) => lanes[0].clone().compact(),
        other => other.clone(),
    };
    assert_eq!(payload(&via_capture), payload(&via_gather));
    assert_eq!(show(&payload(&via_capture)), "List ends=[6] <[0, 0, 1, 1, 2, 2]>");
}

/// the wire: a shared list round-trips as spans (form 2), not as a copy.
#[test]
fn bytes_round_trip_keeps_spans() {
    let (_, shared) = partition_and_shared();
    let mut buf = Vec::new();
    corgi::bytes::write_to(&shared, &mut buf).unwrap();
    assert_eq!(buf.len(), corgi::bytes::length_in_bytes(&shared));
    let (back, used) = corgi::bytes::read_from(&buf).unwrap();
    assert_eq!(used, buf.len());
    assert_eq!(back, shared);
    assert!(matches!(&back, Value::List(b, _) if !b.is_partition()));
}
