//! End-to-end: a fallible `ml` program through the effect layer — partial by default, total under
//! `TRY`. Exercises frontend -> graph -> `effect_eval_graph` + the syntactic totality query.

use corgi::{show, EffectValues, Program, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

// The corpus-wide effect run lives in tests/corpus.rs now (it runs EVERY program through run_effect).
// This file keeps the focused end-to-end + totality checks.

#[test]
fn head_is_partial_then_total_under_try() {
    // `input iota head`: at n=0 the row is the empty list, so `head` (get index 0) is out of range.
    let p = Program::compile_ml("input iota head").unwrap();
    assert!(!p.is_total(), "a bare FailOp (head) is partial");
    match p.run_effect(u64(&[0])) {
        EffectValues::Fail(fv) => assert_eq!(fv.err, vec![true], "the empty row errors"),
        EffectValues::Pure(_) => panic!("a partial program yields a Fail column"),
    }

    // `… head try` reveals the Fail as a pure, matchable `Sum{ T | Unit }`, and is total.
    let pt = Program::compile_ml("input iota head try").unwrap();
    assert!(pt.is_total(), "TRY discharges the effect");
    match pt.run_effect(u64(&[0])) {
        EffectValues::Pure(v) => assert_eq!(show(&v), "Sum tags=[1] [[], ()x1]"),
        EffectValues::Fail(_) => panic!("TRY moves the column from Fail to Pure"),
    }
}
