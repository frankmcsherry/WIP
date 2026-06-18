//! Kernel-op tests: ops valid in the core but deliberately absent from the surface vocabulary.
//! `Weave` is Unweave's formal inverse, kept kernel-only because its inputs (a tag stream whose
//! per-row counts match a set of lane lengths) arise ONLY from Unweave — see the resolve-table note.
//! The round-trip law `weave(unweave x) = x` was corpus program 33; it moves here now that `weave`
//! is off the surface and can no longer be written as a `.col`.

use corgi::Op::*;
use corgi::{eval_graph, Builder, NumOp, Value};

#[test]
fn weave_unweaves_round_trip() {
    // a heterogeneous sum column in one list row: tags [0,1,0,1], lane 0 and lane 1 each U64.
    let inner =
        Value::sum(vec![0, 1, 0, 1], vec![Value::u64(vec![10, 30]), Value::u64(vec![20, 40])]);
    let x = Value::List(vec![4].into(), Box::new(inner));

    // Input -> Unweave -> Weave reconstructs the input exactly (the List⊗Sum iso, kernel side).
    let mut b = Builder::<NumOp>::default();
    let i = b.input();
    let u = b.add(Unweave, vec![i]);
    let w = b.add(Weave, vec![u]);
    let g = b.finish(w);

    assert_eq!(eval_graph(&g, x.clone()), x);
}

/// The stride fast path in `sort_list_blocks` must produce the SAME sort as the general structural
/// path. Build `n` equal-width byte records two ways — the inner list as a `Stride` (which diverts to
/// the packed-u64 leaf radix) vs the equivalent `Offsets` (the position-by-position structural sort) —
/// sort each, and require identical results. A silent wrong-order regression fails here.
#[test]
fn stride_sort_matches_offsets() {
    use corgi::{Bounds, CmpOp};
    let (n, k) = (500usize, 8usize);
    let bytes: Vec<u8> = (0..n * k).map(|i| i.wrapping_mul(37).wrapping_add(11) as u8).collect();
    // one outer row of `n` width-`k` records; only the inner bounds representation differs.
    let one_row = |inner: Bounds| {
        Value::List(vec![n].into(), Box::new(Value::List(inner, Box::new(Value::u8(bytes.clone())))))
    };
    let strided = one_row(Bounds::Stride(k, n));
    let offsets = one_row(Bounds::Offsets((1..=n).map(|r| r * k).collect()));

    let mut b = Builder::<NumOp>::default();
    let i = b.input();
    let s = b.add(CmpOp::SortList, vec![i]);
    let g = b.finish(s);

    assert_eq!(
        eval_graph(&g, strided),
        eval_graph(&g, offsets),
        "stride sort fast path diverged from the structural sort"
    );
}
