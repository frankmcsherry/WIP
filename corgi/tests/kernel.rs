//! Kernel-op tests: ops valid in the core but deliberately absent from the surface vocabulary.
//! `Weave` is Unweave's formal inverse, kept kernel-only because its inputs (a tag stream whose
//! per-row counts match a set of lane lengths) arise ONLY from Unweave — see the resolve-table note.
//! The round-trip law `weave(unweave x) = x` was corpus program 33; it moves here now that `weave`
//! is off the surface and can no longer be written as a `.col`.

use corgi::Op::*;
use corgi::{eval_graph, Builder, NumOp, Value};

#[test]
fn weave_unweaves_round_trip() {
    // a heterogeneous sum column in one list row: tags [0,1,0,1], lane 0 and lane 1 each Int.
    let inner =
        Value::sum(vec![0, 1, 0, 1], vec![Value::i64(vec![10, 30]), Value::i64(vec![20, 40])]);
    let x = Value::List(vec![4].into(), Box::new(inner));

    // Input -> Unweave -> Weave reconstructs the input exactly (the List⊗Sum iso, kernel side).
    let mut b = Builder::<NumOp>::default();
    let i = b.input();
    let u = b.add(Unweave, vec![i]);
    let w = b.add(Weave, vec![u]);
    let g = b.finish(w);

    assert_eq!(eval_graph(&g, x.clone()), x);
}

/// `Weave` reads its tags by value: tags held as `i64`s (as `iota` writes them) weave as the same
/// tags held as bytes do.
#[test]
fn weave_reads_tags_at_any_storage() {
    let weave = |tags: Value| {
        let lane = |xs: Vec<i64>| Value::List(vec![2].into(), Box::new(Value::i64(xs)));
        let mut b = Builder::<NumOp>::default();
        let i = b.input();
        let w = b.add(Weave, vec![i]);
        let g = b.finish(w);
        eval_graph(&g, Value::Prod(vec![Value::List(vec![4].into(), Box::new(tags)), lane(vec![10, 30]), lane(vec![20, 40])]))
    };
    let expect = Value::List(vec![4].into(), Box::new(Value::sum(vec![0, 1, 0, 1], vec![Value::i64(vec![10, 30]), Value::i64(vec![20, 40])])));
    assert_eq!(weave(Value::u8(vec![0, 1, 0, 1])), expect);
    assert_eq!(weave(Value::i64(vec![0, 1, 0, 1])), expect);
}

/// The stride fast path in the indexed sort's `List` arm must produce the SAME sort as the general structural
/// path. Build `n` equal-width byte records two ways — the inner list as a `Stride` (which diverts to
/// the packed-u64 leaf radix) vs the equivalent `Offsets` (the position-by-position structural sort) —
/// sort each, and require identical results. A silent wrong-order regression fails here.
#[test]
fn stride_sort_matches_offsets() {
    use corgi::Bounds;
    let (n, k) = (500usize, 8usize);
    let bytes: Vec<u8> = (0..n * k).map(|i| i.wrapping_mul(37).wrapping_add(11) as u8).collect();
    // one outer row of `n` width-`k` records; only the inner bounds representation differs.
    let one_row = |inner: Bounds| {
        Value::List(vec![n].into(), Box::new(Value::List(inner, Box::new(Value::u8(bytes.clone())))))
    };
    let strided = one_row(Bounds::Stride(k, n));
    let offsets = one_row(Bounds::offsets((1..=n).map(|r| r * k).collect()));

    let g = corgi::parse_ml("input sort").unwrap();

    assert_eq!(
        eval_graph(&g, strided),
        eval_graph(&g, offsets),
        "stride sort fast path diverged from the structural sort"
    );
}

/// the raw `Zip` and `Chunk` are total but lossy: rows whose columns disagree in length keep the
/// shortest, and a row that doesn't divide by `k` drops its remainder. (`zip` and `chunk` on the
/// surface report those rows as errors instead.)
#[test]
fn raw_zip_and_chunk_are_total() {
    let one = |op| {
        let mut b = Builder::<NumOp>::default();
        let i = b.input();
        let o = b.add(op, vec![i]);
        b.finish(o)
    };
    let a = Value::List(vec![2, 5].into(), Box::new(Value::i64(vec![1, 2, 3, 4, 5])));
    let b = Value::List(vec![1, 4].into(), Box::new(Value::i64(vec![10, 20, 30, 40])));
    let zipped = eval_graph(&one(Zip), Value::Prod(vec![a, b]));
    let expect = Value::List(vec![1, 4].into(), Box::new(Value::Prod(vec![Value::i64(vec![1, 3, 4, 5]), Value::i64(vec![10, 20, 30, 40])])));
    assert_eq!(zipped, expect);
    let rows = Value::List(vec![5, 9].into(), Box::new(Value::i64((0..9).collect())));
    let chunked = eval_graph(&one(Chunk(2)), rows);
    let expect = Value::List(vec![2, 4].into(), Box::new(Value::List(corgi::Bounds::Stride(2, 4), Box::new(Value::i64(vec![0, 1, 2, 3, 5, 6, 7, 8])))));
    assert_eq!(chunked, expect);
}
