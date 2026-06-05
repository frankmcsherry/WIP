//! ML front-end tests: same demos through `let` and juxtaposed stages, plus a check that `let`
//! sharing produces fewer nodes than the same join written with the shared subexpression inlined.

use corgi::{parse_ml, shape_of_value, show, Program, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

fn run_ml(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
    p.check();
    let out = p.run(arg.clone()).expect("shape error");
    let inferred = p.shape(&shape_of_value(arg)).expect("type error");
    assert_eq!(inferred, shape_of_value(&out), "typer disagrees with evaluator");
    show(&out)
}

fn sample() -> Value {
    Value::Prod(vec![
        u64(&[10, 20, 30]),
        Value::List(
            vec![2, 3, 6],
            Box::new(Value::Prod(vec![u64(&[1, 2, 3, 4, 5, 6]), u64(&[100, 200, 300, 400, 500, 600])])),
        ),
        Value::sum(vec![0, 1, 0], vec![u64(&[1111, 3333]), u64(&[2222])]),
    ])
}

#[test]
fn sum_scores_with_destructure() {
    let src = "let (subj, vals) = input.1 transpose in vals reduce_sum";
    assert_eq!(run_ml(src, &sample()), "[300, 300, 1500]");
}

#[test]
fn match_contact() {
    let src = "input.2 map_variant 1 (p -> p add_u64 1000000) unwrap";
    assert_eq!(run_ml(src, &sample()), "[1111, 1002222, 3333]");
}

#[test]
fn const_via_lit_lambda() {
    let src = "let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, v lit 1000) add)";
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[1100, 1200, 1300, 1400, 1500, 1600]>");
}

#[test]
fn juxtaposition_stops_at_let_in() {
    // the juxtaposed chain `input.1 transpose` must terminate at the `let` body's `in`, not read it
    // as an op; then a bare lambda maps over the result.
    let src = "let (subj, vals) = input.1 transpose in subj map (v -> v add_u64 1)";
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[2, 3, 4, 5, 6, 7]>");
}

// classify_high (partition+map_variant+unwrap) and the find/slices join are now self-generating in
// tests/generated.rs (`classify_a_generated_range`, `join_generated_keys`) — retired here as twins.

#[test]
fn let_sharing_beats_fanout_recompute() {
    // same join: `let t = transpose` shares the transpose once; inlining it fans out and recomputes.
    let shared = "let t = input.0 transpose in let r = (input.1, t.0) find in (r, t.1) slices";
    let inlined =
        "((input.1, input.0 transpose field 0) find, input.0 transpose field 1) slices";
    let shared_nodes = parse_ml(shared).unwrap().node_count();
    let inlined_nodes = parse_ml(inlined).unwrap().node_count();
    assert!(shared_nodes < inlined_nodes, "shared {shared_nodes} should be < inlined {inlined_nodes}");
}

#[test]
fn workhorse_products_sums_lists() {
    // ONE program that walks the whole data model on the `sample()` record-batch
    //   ( id:U64, scores:List<(subj,val)>, contact:Sum{Email|Phone} ):
    //   input.1 transpose        List<(subj,val)> -> (List<subj>, List<val>)   [list <-> product]
    //   scores.1 reduce_sum       List<val> -> one U64 per record (sum each row) [list -> scalar]
    //   (input.0, totals) add     pair the id column with the totals, add them   [product + arith]
    //   map_variant 0 (..) unwrap bump the Email variant, then flatten the sum    [sum navigation]
    //   (id_plus, contact)           bundle the two results into a product           [product build]
    let src = "let scores = input.1 transpose in \
               let totals = scores.1 reduce_sum in \
               let id_plus = (input.0, totals) add in \
               let contact = input.2 map_variant 0 (e -> e add_u64 1000000) unwrap in \
               (id_plus, contact)";
    assert_eq!(run_ml(src, &sample()), "([310, 320, 1530], [1001111, 2222, 1003333])");
}

#[test]
fn errors_are_reported() {
    assert!(parse_ml("let x = input in y").is_err()); // unbound y
    assert!(parse_ml("input bogus").is_err());
    assert!(parse_ml("let x = input").is_err()); // missing 'in'
}

#[test]
fn string_literal_broadcasts() {
    // a string literal as a stage is a constant List<U8>, broadcast to the value.
    assert_eq!(run_ml("input \"hi\"", &u64(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}
