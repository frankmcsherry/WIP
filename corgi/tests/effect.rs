//! End-to-end: a fallible `ml` program lowered into the pure vocabulary. Exercises frontend -> graph
//! -> `lower_effects` -> `eval_graph`.

use corgi::{show, Program, Shape, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

// The corpus-wide run lives in tests/corpus.rs (it runs EVERY program and types the lowered graph).
// This file keeps the focused end-to-end checks.

#[test]
fn head_fails_into_the_output_and_try_reveals_it() {
    // `input iota head`: at n=0 the row is the empty list, so `head` (get index 0) is out of range.
    let p = Program::compile_ml("input iota head").unwrap();
    assert_eq!(show(&p.run(u64(&[0]))), "Sum tags=[1] [[], ()x1]");
    assert_eq!(p.shape(&Shape::Prim(64)).unwrap().to_string(), "{U64 | ()}");

    // `… head try` takes the Fail up as a pure, matchable `Sum{ T | Unit }`: the same value.
    let pt = Program::compile_ml("input iota head try").unwrap();
    assert_eq!(show(&pt.run(u64(&[0]))), "Sum tags=[1] [[], ()x1]");
}
