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
    let x = Value::List(vec![4], Box::new(inner));

    // Input -> Unweave -> Weave reconstructs the input exactly (the List⊗Sum iso, kernel side).
    let mut b = Builder::<NumOp>::default();
    let i = b.input();
    let u = b.add(Unweave, vec![i]);
    let w = b.add(Weave, vec![u]);
    let g = b.finish(w);

    assert_eq!(eval_graph(&g, x.clone()), x);
}
