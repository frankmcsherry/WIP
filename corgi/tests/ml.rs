//! ML front-end tests: same demos through `let` and juxtaposed stages, plus a check that `let`
//! sharing produces fewer nodes than the same join written with the shared subexpression inlined.

use corgi::{eval_try, parse_ml, show, EffectValues, Program, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

/// run through the effect layer and render: a total program shows its pure value; a partial program
/// (an un-`TRY`'d `FailOp`) shows its result TRY'd to a `Sum{T | Unit}`.
fn run_ml(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
    p.check();
    match p.run_effect(arg.clone()) {
        EffectValues::Pure(v) => show(&v),
        EffectValues::Fail(fv) => show(&eval_try(fv)),
    }
}

fn sample() -> Value {
    Value::Prod(vec![
        u64(&[10, 20, 30]),
        Value::List(
            vec![2, 3, 6].into(),
            Box::new(Value::Prod(vec![u64(&[1, 2, 3, 4, 5, 6]), u64(&[100, 200, 300, 400, 500, 600])])),
        ),
        Value::sum(vec![0, 1, 0], vec![u64(&[1111, 3333]), u64(&[2222])]),
    ])
}

#[test]
fn sum_scores_with_destructure() {
    let src = "let (subj, vals) = input.1 transpose in vals fold_add";
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

// classify_high (branch+map_variant+unwrap) and the find/slices join are now self-generating in
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
    //   scores.1 fold_add       List<val> -> one U64 per record (sum each row) [list -> scalar]
    //   (input.0, totals) add     pair the id column with the totals, add them   [product + arith]
    //   map_variant 0 (..) unwrap bump the Email variant, then flatten the sum    [sum navigation]
    //   (id_plus, contact)           bundle the two results into a product           [product build]
    let src = "let scores = input.1 transpose in \
               let totals = scores.1 fold_add in \
               let id_plus = (input.0, totals) add in \
               let contact = input.2 map_variant 0 (e -> e add_u64 1000000) unwrap in \
               (id_plus, contact)";
    assert_eq!(run_ml(src, &sample()), "([310, 320, 1530], [1001111, 2222, 1003333])");
}

#[test]
fn enum_names_resolve_and_erase() {
    // the declaration is a compile-time table: `Phone` resolves to tag 1 and erases, so this is
    // the same graph as `match_contact`.
    let src = "enum Contact = Email | Phone in \
               input.2 map_variant Phone (p -> p add_u64 1000000) unwrap";
    assert_eq!(run_ml(src, &sample()), "[1111, 1002222, 3333]");
}

#[test]
fn inject_by_name_carries_arity() {
    // `inject Email` reads both tag and arity off the declaration (vs `inject 0 2`).
    let src = "enum Contact = Email | Phone in input.0 inject Email unwrap";
    assert_eq!(run_ml(src, &sample()), "[10, 20, 30]");
}

#[test]
fn branch_by_enum_and_named_match_arms() {
    let src = "enum Size = Lo | Hi in \
               let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, v gt 300) branch Size match (Lo (l -> l), Hi (h -> h add 1)))";
    // `branch` is a FailOp now (demux Sum{Lo|Hi} with Oob in the err-mask), so the result is a Fail
    // column shown TRY'd; the match arms still align (the demux re-tags Lo=0, Hi=1) — Hi (>300) gets +1.
    assert_eq!(
        run_ml(src, &sample()),
        "Sum tags=[0, 0, 0] [List ends=[2, 3, 6] <[100, 200, 300, 401, 501, 601]>, ()x0]"
    );
}

#[test]
fn lambda_destructures_pairs() {
    // `(subj, val) -> …` in a lambda mirrors the `let` pattern: names for the pair, no `.0`/`.1`.
    let src = "input.1 map ((subj, val) -> val)";
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[100, 200, 300, 400, 500, 600]>");
}

#[test]
fn binary_immediate_is_the_lit_pair() {
    // `v add 1000` desugars to `(v, v lit 1000) add` — the same output as `const_via_lit_lambda`.
    let src = "let (subj, vals) = input.1 transpose in vals map (v -> v add 1000)";
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[1100, 1200, 1300, 1400, 1500, 1600]>");
}

#[test]
fn errors_are_reported() {
    assert!(parse_ml("let x = input in y").is_err()); // unbound y
    assert!(parse_ml("input bogus").is_err());
    assert!(parse_ml("let x = input").is_err()); // missing 'in'
    assert!(parse_ml("input inject Bogus").is_err()); // undeclared variant
    assert!(parse_ml("input branch Bogus").is_err()); // undeclared enum
    assert!(parse_ml("enum E = A | A in input").is_err()); // duplicate variant
    assert!(parse_ml("enum E = A in enum F = A in input").is_err()); // variant names are global
}

#[test]
fn string_literal_broadcasts() {
    // a string literal as a stage is a constant List<U8>, broadcast to the value.
    assert_eq!(run_ml("input \"hi\"", &u64(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}

#[test]
fn head_sugar_is_a_failop() {
    // `head` is the get FailOp: a non-empty row -> Found(first), an EMPTY row -> Oob — both carried in
    // the err-mask, shown TRY'd as Sum{T | Unit}. No panic, no total/unchecked split. (`input add_u64 1
    // iota` is [0..n+1); `input iota` at n=0 is the empty row.)
    assert_eq!(run_ml("input add_u64 1 iota head", &u64(&[3])), "Sum tags=[0] [[0], ()x0]");
    assert_eq!(run_ml("input iota head", &u64(&[0])), "Sum tags=[1] [[], ()x1]");
}
