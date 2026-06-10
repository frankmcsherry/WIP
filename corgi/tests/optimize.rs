//! Optimizer tests: passes must preserve evaluation AND shrink the graph where
//! there is redundancy (CSE on the fanout join, DCE on a dead `let`, peephole on
//! `Field` of a `Tuple`).

use corgi::{cse, dce, optimize, parse_ml, peephole, show, Graph, NumOp, Program, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

fn eval_str(g: &Graph<NumOp>, arg: &Value) -> String {
    let p = Program::from_graph(g.clone());
    p.check();
    show(&p.run(arg.clone()).expect("shape error"))
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

fn join_input() -> Value {
    Value::Prod(vec![
        Value::List(
            vec![6],
            Box::new(Value::Prod(vec![u64(&[1, 1, 2, 3, 3, 3]), u64(&[10, 11, 20, 30, 31, 32])])),
        ),
        Value::List(vec![4], Box::new(u64(&[2, 3, 5, 1]))),
    ])
}

// the join with the shared transpose INLINED (fanned out) vs. hand-shared with `let`.
const INLINED_JOIN: &str =
    "((input.1, input.0 transpose field 0) find, input.0 transpose field 1) slices";
const ML_JOIN: &str =
    "let t = input.0 transpose in let r = (input.1, t.0) find in (r, t.1) slices";

#[test]
fn cse_collapses_the_fanout_join() {
    let fanout = parse_ml(INLINED_JOIN).unwrap();
    let folded = cse(&fanout);
    // CSE merges the duplicated `transpose` down to the hand-shared `let` form.
    assert!(folded.node_count() < fanout.node_count());
    assert_eq!(folded.node_count(), parse_ml(ML_JOIN).unwrap().node_count());
    // and it preserves the result.
    assert_eq!(eval_str(&folded, &join_input()), eval_str(&fanout, &join_input()));
}

#[test]
fn dce_drops_a_dead_let() {
    // `x` (a transpose) is computed but the body ignores it.
    let g = parse_ml("let x = input.0 transpose in input.1").unwrap();
    let lean = dce(&g);
    assert!(lean.node_count() < g.node_count());
    assert_eq!(eval_str(&lean, &join_input()), eval_str(&g, &join_input()));
}

#[test]
fn peephole_field_of_tuple() {
    let g = parse_ml("(input.0, input.1).0").unwrap();
    let opt = optimize(&g); // peephole removes the Tuple+Field, dce sweeps the dead branch
    assert!(opt.node_count() < g.node_count());
    assert_eq!(eval_str(&opt, &sample()), eval_str(&parse_ml("input.0").unwrap(), &sample()));
}

type Case = (&'static str, fn() -> Value);

#[test]
fn optimize_preserves_eval_everywhere() {
    let cases: &[Case] = &[
        ("input.1 transpose field 1 reduce_sum", sample),
        ("(input.0, input.1 transpose field 1) cap_list map (p -> p add)", sample),
        ("input.2 map_variant 1 (h -> h add_u64 1000000) unwrap", sample),
        (INLINED_JOIN, join_input),
    ];
    for (src, mk) in cases {
        let g = parse_ml(src).unwrap();
        let arg = mk();
        assert_eq!(eval_str(&optimize(&g), &arg), eval_str(&g, &arg), "mismatch for: {src}");
    }
}

#[test]
fn peephole_also_runs_under_bodies() {
    // a Field-of-Tuple inside a map body should fold too.
    let g = parse_ml("input.1 map (p -> (p, p).0)").unwrap();
    let opt = peephole(&g);
    assert_eq!(eval_str(&opt, &sample()), eval_str(&g, &sample()));
}
