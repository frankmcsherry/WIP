//! Optimizer tests: passes must preserve evaluation AND shrink the graph where
//! there is redundancy (CSE on the fanout join, DCE on a dead `let`, peephole on
//! `Field` of a `Tuple`).

use corgi::{
    cancel_isos, cse, dce, fuse_maps, optimize, parse_ml, peephole, show, Builder, Graph, NumOp, Op,
    Program, Value,
};

fn int(xs: &[i64]) -> Value {
    Value::i64(xs.to_vec())
}

fn eval_str(g: &Graph<NumOp>, arg: &Value) -> String {
    let p = Program::from_graph(g.clone());
    show(&p.run(arg.clone()))
}

fn sample() -> Value {
    Value::Prod(vec![
        int(&[10, 20, 30]),
        Value::List(
            vec![2, 3, 6].into(),
            Box::new(Value::Prod(vec![int(&[1, 2, 3, 4, 5, 6]), int(&[100, 200, 300, 400, 500, 600])])),
        ),
        Value::sum(vec![0, 1, 0], vec![int(&[1111, 3333]), int(&[2222])]),
    ])
}

fn join_input() -> Value {
    Value::Prod(vec![
        Value::List(
            vec![6].into(),
            Box::new(Value::Prod(vec![int(&[1, 1, 2, 3, 3, 3]), int(&[10, 11, 20, 30, 31, 32])])),
        ),
        Value::List(vec![4].into(), Box::new(int(&[2, 3, 5, 1]))),
    ])
}

// the join with the shared transpose INLINED (fanned out) vs. hand-shared with `let`.
const INLINED_JOIN: &str =
    "((input.1, input.0 transpose .0) find, input.0 transpose .1) slices";
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
        ("input.1 transpose .1 fold_add", sample),
        ("(input.0, input.1 transpose .1) cap_list map (p -> p add)", sample),
        ("input.2 map_variant 1 (h -> (h, 1000000) add) unwrap", sample),
        // map fusion: a three-deep MapList chain must collapse without changing the result.
        ("input.1 transpose .1 map (x -> (x, 1) add) map (x -> (x, 2) div) map (x -> (x, 5) add)", sample),
        // iso cancellation under composition with a real op between the pair.
        ("input.1 transpose zip map (p -> p.0)", sample),
        (INLINED_JOIN, join_input),
    ];
    for (src, mk) in cases {
        let g = parse_ml(src).unwrap();
        let arg = mk();
        assert_eq!(eval_str(&optimize(&g), &arg), eval_str(&g, &arg), "mismatch for: {src}");
    }
}

#[test]
fn fuse_maps_collapses_adjacent_passes() {
    // two passes over the same list become one. The fused graph has fewer nodes (one MapList, one
    // composed body) and the same result.
    let g = parse_ml("input.1 transpose .1 map (x -> (x, 1) add) map (x -> (x, 10) add)").unwrap();
    let fused = dce(&fuse_maps(&g)); // fusion orphans the producer MapList; dce sweeps it
    assert!(fused.node_count() < g.node_count(), "fusion should drop the intermediate MapList node");
    assert_eq!(eval_str(&fused, &sample()), eval_str(&g, &sample()));
}

#[test]
fn cancel_isos_drops_inverse_pairs() {
    // Transpose then (PURE) Zip is the identity on List<(X,Y)>; cancellation removes both. Built with
    // `Op::Zip` directly: the SURFACE `zip` is now the `TryZip` FailOp (fallible — not a pure inverse,
    // so it can't cancel without optimizer demotion), while the optimizer still targets the pure op.
    let mut b: Builder<NumOp> = Builder::default();
    let inp = b.input();
    let f1 = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let t = b.add(NumOp::Core(Op::Transpose), vec![f1]);
    let z = b.add(NumOp::Core(Op::Zip), vec![t]);
    let g = b.finish(z);
    let opt = dce(&cancel_isos(&g));
    assert!(opt.node_count() < g.node_count(), "Zip∘Transpose should cancel");
    assert_eq!(eval_str(&opt, &sample()), eval_str(&g, &sample()));
}

#[test]
fn optimize_preserves_every_corpus_program() {
    // the strongest net: for each `.col` program, the optimized graph evaluates identically to the
    // original on the program's own seed. Catches any pass (fusion/iso/cse/dce/peephole) that drifts.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("programs");
    for entry in std::fs::read_dir(&dir).expect("programs/ dir") {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "col") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let (mut n, mut prog) = (8i64, String::new());
        for line in text.lines() {
            if let Some(r) = line.strip_prefix("# n =") {
                n = r.trim().parse().unwrap();
            } else if !line.trim_start().starts_with('#') && !line.trim().is_empty() {
                prog.push_str(line.trim());
                prog.push(' ');
            }
        }
        let who = path.file_name().unwrap().to_string_lossy().into_owned();
        let g = parse_ml(prog.trim()).unwrap_or_else(|e| panic!("{who}: {e}"));
        let arg = int(&[n]);
        assert_eq!(eval_str(&optimize(&g), &arg), eval_str(&g, &arg), "optimize drifted on {who}");
    }
}

#[test]
fn peephole_also_runs_under_bodies() {
    // a Field-of-Tuple inside a map body should fold too.
    let g = parse_ml("input.1 map (p -> (p, p).0)").unwrap();
    let opt = peephole(&g);
    assert_eq!(eval_str(&opt, &sample()), eval_str(&g, &sample()));
}
