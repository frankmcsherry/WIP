//! End-to-end: a fallible `ml` program lowered into the pure vocabulary — partial by default, total
//! under `try`. Exercises frontend -> graph -> `lower_effects` -> `eval_graph` + the totality query.

use corgi::{show, Program, Shape, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

// The corpus-wide run lives in tests/corpus.rs (it runs EVERY program through run_partial and types
// the lowered graph). This file keeps the focused end-to-end + totality checks.

#[test]
fn head_is_partial_then_total_under_try() {
    // `input iota head`: at n=0 the row is the empty list, so `head` (get index 0) is out of range.
    let p = Program::compile_ml("input iota head").unwrap();
    assert!(!p.is_total(), "a bare fallible stage (head) is partial");
    assert!(p.run(u64(&[0])).is_err(), "a partial program has no bare value");
    assert_eq!(show(&p.run_partial(u64(&[0]))), "Sum tags=[1] [[], ()x1]");
    assert_eq!(p.shape(&Shape::Prim(64)).unwrap().to_string(), "{U64 | ()}");

    // `… head try` takes the Fail up as a pure, matchable `Sum{ T | Unit }`, and is total.
    let pt = Program::compile_ml("input iota head try").unwrap();
    assert!(pt.is_total(), "try discharges the effect");
    assert_eq!(show(&pt.run(u64(&[0])).unwrap()), "Sum tags=[1] [[], ()x1]");
}
