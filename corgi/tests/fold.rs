//! `Fold` and `FoldScan` against a plain per-row reference: each row's state starts at its seed and
//! the body runs on that one row, one element at a time. The lockstep evaluation must agree row for
//! row, whatever the row lengths (none, all empty, one row, uniform as offsets or as a stride,
//! already longest-first, ragged, one row much longer than the rest) and whatever the state's shape
//! (a leaf, a tuple, a list, a sum, a sum of lists, references, unit, and a fallible body's `Fail`).

use corgi::{arrange, eval_graph, lower_effects, parse_ml, Bounds, Builder, Graph, NumOp, Op, OpLike, Shape, Value};

fn rng(seed: u64) -> impl FnMut() -> u64 {
    let mut s = seed | 1;
    move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    }
}

fn ends(lens: &[usize]) -> Vec<usize> {
    lens.iter().scan(0, |e, &l| { *e += l; Some(*e) }).collect()
}

/// the row-length patterns, each as the list's bounds.
fn patterns(next: &mut impl FnMut() -> u64) -> Vec<(&'static str, Bounds)> {
    let mut lens = |n: usize, lo: u64, hi: u64| -> Vec<usize> { (0..n).map(|_| (lo + next() % (hi - lo)) as usize).collect() };
    let ragged = lens(40, 0, 9);
    let full = lens(25, 1, 9);
    let mut descending = lens(12, 0, 7);
    descending.sort_by(|a, b| b.cmp(a));
    let one = lens(1, 0, 12);
    vec![
        ("no rows", Bounds::offsets(vec![])),
        ("all empty", Bounds::offsets(vec![0; 5])),
        ("all empty, stride", Bounds::Stride(0, 4)),
        ("one row", Bounds::offsets(ends(&one))),
        ("one row, long", Bounds::offsets(vec![37])),
        ("uniform, offsets", Bounds::offsets(ends(&[4; 7]))),
        ("uniform, stride", Bounds::Stride(4, 7)),
        ("longest first", Bounds::offsets(ends(&descending))),
        ("longest first, then empty rows", Bounds::offsets(ends(&[5, 3, 3, 1, 0, 0]))),
        ("uniform, then empty rows", Bounds::offsets(ends(&[4, 4, 4, 0, 0]))),
        ("shortest first", Bounds::offsets(ends(&(0..9).collect::<Vec<_>>()))),
        ("ragged", Bounds::offsets(ends(&ragged))),
        ("ragged, compacted", ends(&ragged).into()),
        ("ragged, no empty rows", Bounds::offsets(ends(&full))),
        ("one long row among short", Bounds::offsets(ends(&[2, 0, 30, 1, 3]))),
    ]
}

fn u64s(next: &mut impl FnMut() -> u64, n: usize, m: u64) -> Vec<u64> {
    (0..n).map(|_| next() % m).collect()
}

/// a `List<U64>` of `n` rows, each 0..3 elements.
fn short_lists(next: &mut impl FnMut() -> u64, n: usize) -> Value {
    let lens: Vec<usize> = (0..n).map(|_| (next() % 4) as usize).collect();
    let total = lens.iter().sum();
    Value::List(ends(&lens).into(), Box::new(Value::u64(u64s(next, total, 100))))
}

fn op(o: Op<NumOp>, v: Value) -> Value {
    NumOp::Core(o).eval(v).unwrap()
}

/// by value: references cloned out, so two columns naming the same rows compare equal.
fn norm(v: Value) -> Value {
    op(Op::Clone, v)
}

/// a two-lane sum over `n` rows with random tags; `lane(k)` builds a lane of `k` rows.
fn two_lanes(next: &mut impl FnMut() -> u64, n: usize, mut lane: impl FnMut(&mut dyn FnMut() -> u64, usize) -> Value) -> Value {
    let tags: Vec<usize> = (0..n).map(|_| (next() % 2) as usize).collect();
    let ones = tags.iter().filter(|&&t| t == 1).count();
    let lanes = vec![lane(next, n - ones), lane(next, ones)];
    Value::sum(tags, lanes)
}

fn fold_graph(body: &Graph<NumOp>, scan: bool) -> Graph<NumOp> {
    let mut b = Builder::default();
    let i = b.input();
    let body = Box::new(body.clone());
    let o = b.add(if scan { Op::FoldScan(body) } else { Op::Fold(body) }, vec![i]);
    b.finish(o)
}

/// the reference: per row, the body over one row at a time. The row's final state, and (for a
/// scan) its outputs in order.
fn per_row(body: &Graph<NumOp>, seed: &Value, list: &Value, scan: bool) -> Vec<(Value, Vec<Value>)> {
    let Value::List(bounds, vals) = list else { panic!("the fold's list") };
    let mut start = 0;
    bounds
        .to_vec()
        .into_iter()
        .enumerate()
        .map(|(r, end)| {
            let mut acc = arrange::gather(seed, &[r]);
            let mut outs = Vec::new();
            for p in start..end {
                let out = eval_graph(body, Value::Prod(vec![acc, arrange::gather(vals, &[p])]));
                if scan {
                    let [state, o]: [Value; 2] = out.into_prod("scan body").unwrap().try_into().unwrap();
                    acc = state;
                    outs.push(o);
                } else {
                    acc = out;
                }
            }
            start = end;
            (acc, outs)
        })
        .collect()
}

/// run `body` as a Fold and as a FoldScan over `list` from `seed`, and compare every row with the
/// per-row reference.
fn check(what: &str, body_src: &str, scan: bool, seed: &Value, list: &Value) {
    let body = parse_ml(body_src).unwrap_or_else(|e| panic!("{body_src}: {e}"));
    let out = eval_graph(&fold_graph(&body, scan), Value::Prod(vec![seed.clone(), list.clone()]));
    let expect = per_row(&body, seed, list, scan);
    let (state, outs) = if scan {
        let [s, o]: [Value; 2] = out.into_prod("FoldScan output").unwrap().try_into().unwrap();
        (s, Some(o))
    } else {
        (out, None)
    };
    assert_eq!(state.len(), expect.len(), "{what}: row count");
    for (r, (acc, routs)) in expect.into_iter().enumerate() {
        assert_eq!(norm(arrange::gather(&state, &[r])), norm(acc), "{what}: row {r}'s state");
        if let Some(o) = &outs {
            let got = norm(arrange::gather(o, &[r]));
            let Value::List(b, vals) = &got else { panic!("{what}: FoldScan output is a list") };
            assert_eq!(b.to_vec(), vec![routs.len()], "{what}: row {r}'s output count");
            if !routs.is_empty() {
                let srcs: Vec<Option<&Value>> = routs.iter().map(Some).collect();
                let want = arrange::gather_lanes(&srcs, &(0..routs.len()).collect::<Vec<_>>(), &vec![0; routs.len()]);
                assert_eq!(**vals, norm(want), "{what}: row {r}'s outputs");
            }
        }
    }
}

#[test]
fn fold_and_foldscan_match_a_per_row_reference() {
    let mut next = rng(0x9E37_79B9_7F4A_7C15);
    let pats = patterns(&mut next);
    for (pattern, bounds) in pats {
        let n = bounds.to_vec().len();
        let list = Value::List(bounds.clone(), Box::new(Value::u64(u64s(&mut next, bounds.to_vec().last().copied().unwrap_or(0), 10))));
        let refs = |next: &mut dyn FnMut() -> u64| {
            let mut g = || next();
            Value::Prod(vec![op(Op::Ref, short_lists(&mut g, n)), op(Op::Ref, short_lists(&mut g, n))])
        };
        // (state, seed, fold body, foldscan body): the scan body returns (state, output).
        let cases: Vec<(&str, Value, &str, &str)> = vec![
            (
                "a leaf",
                Value::u64(u64s(&mut next, n, 100)),
                "let (a, x) = input in ((a, 3u64) mul, x) add",
                "let (a, x) = input in let b = ((a, 3u64) mul, x) add in (b, (b, x))",
            ),
            (
                "a tuple",
                Value::Prod(vec![Value::u64(u64s(&mut next, n, 100)), Value::u64(u64s(&mut next, n, 100))]),
                "let ((s, c), x) = input in (((s, 3u64) mul, x) add, (c, 1u64) add)",
                "let ((s, c), x) = input in ((((s, 3u64) mul, x) add, (c, 1u64) add), s)",
            ),
            (
                "a growing list",
                short_lists(&mut next, n),
                "let (a, x) = input in (a, x enlist) append",
                "let (a, x) = input in let b = (a, x enlist) append in (b, b len)",
            ),
            (
                "a list mapped in place",
                short_lists(&mut next, n),
                "let (a, x) = input in (x, a) cap_list map ((x, v) -> ((v, 3u64) mul, x) add)",
                "let (a, x) = input in ((x, a) cap_list map ((x, v) -> ((v, 3u64) mul, x) add), a)",
            ),
            (
                "a sum",
                two_lanes(&mut next, n, |g, k| Value::u64((0..k).map(|_| g() % 100).collect())),
                "enum E = Even u64 | Odd u64 in let (a, x) = input in \
                 let v = ((a unwrap, 3u64) mul, x) add in (x and 1, v inject Odd, v inject Even) select",
                "enum E = Even u64 | Odd u64 in let (a, x) = input in \
                 let v = ((a unwrap, 3u64) mul, x) add in let s = (x and 1, v inject Odd, v inject Even) select in (s, (a, s))",
            ),
            (
                "a sum of lists",
                two_lanes(&mut next, n, |g, k| short_lists(&mut || g(), k)),
                "enum L = P List(u64) | Q List(u64) in let (a, x) = input in \
                 let v = (a unwrap, x enlist) append in (x and 1, v inject Q, v inject P) select",
                "enum L = P List(u64) | Q List(u64) in let (a, x) = input in \
                 let v = (a unwrap, x enlist) append in ((x and 1, v inject Q, v inject P) select, v)",
            ),
            (
                "references",
                refs(&mut next),
                "let ((p, q), x) = input in ((x and 1, q, p) select, (x and 1, p, q) select)",
                "let ((p, q), x) = input in (((x and 1, q, p) select, (x and 1, p, q) select), p)",
            ),
            (
                "unit",
                Value::Unit(n),
                "let (a, x) = input in a",
                "let (a, x) = input in (a, x)",
            ),
            (
                "a leaf, a list and a sum",
                Value::Prod(vec![
                    Value::u64(u64s(&mut next, n, 100)),
                    short_lists(&mut next, n),
                    two_lanes(&mut next, n, |g, k| Value::u64((0..k).map(|_| g() % 100).collect())),
                ]),
                "enum E = Even u64 | Odd u64 in let ((s, l, e), x) = input in \
                 (((s, 3u64) mul, x) add, (l, x enlist) append, (x and 1, s inject Odd, e unwrap inject Even) select)",
                "enum E = Even u64 | Odd u64 in let ((s, l, e), x) = input in \
                 ((((s, 3u64) mul, x) add, (l, x enlist) append, (x and 1, s inject Odd, e unwrap inject Even) select), l)",
            ),
        ];
        for (state, seed, fold, scan) in &cases {
            check(&format!("{pattern}, {state} (fold)"), fold, false, seed, &list);
            check(&format!("{pattern}, {state} (foldscan)"), scan, true, seed, &list);
        }
        // the elements themselves lists: each round gathers whole rows of the payload.
        let nested = Value::List(bounds.clone(), Box::new(short_lists(&mut next, bounds.to_vec().last().copied().unwrap_or(0))));
        let seed = Value::u64(u64s(&mut next, n, 100));
        check(&format!("{pattern}, list elements"), "let (a, x) = input in ((a, 3u64) mul, x fold_add) add", false, &seed, &nested);
        check(
            &format!("{pattern}, list elements (foldscan)"),
            "let (a, x) = input in let b = ((a, 3u64) mul, x fold_add) add in (b, x)",
            true,
            &seed,
            &nested,
        );
    }
}

/// A fallible body is lowered to a fold over `Fail<B>`: a row whose body errs at any element is an
/// error, and the others fold on. Checked against the loop written out in Rust.
#[test]
fn fallible_fold_bodies_err_their_rows_only() {
    let mut next = rng(0xD1B5_4A32_D192_ED03);
    let pats = patterns(&mut next);
    // the body reads element `x` of `[0..8)`, so any element 8 or 9 errs its row.
    let fold = lower_effects(&parse_ml("(input.0, input.1) fold ((a, x) -> ((x, 8u64 iota) get, (a, 3u64) mul) add)").unwrap());
    let scan = lower_effects(
        &parse_ml("(input.0, input.1) foldscan ((a, x) -> let b = ((x, 8u64 iota) get, (a, 3u64) mul) add in (b, b))").unwrap(),
    );
    let ok = |v: Value| Value::sum(vec![0], vec![v, Value::Unit(0)]);
    let err = |shape: &Shape| Value::sum(vec![1], vec![Value::empty(shape), Value::Unit(1)]);
    let scan_shape = Shape::Prod(vec![Shape::Prim(64), Shape::List(Box::new(Shape::Prim(64)))]);
    for (pattern, bounds) in pats {
        let n = bounds.to_vec().len();
        let ends = bounds.to_vec();
        let vals = u64s(&mut next, ends.last().copied().unwrap_or(0), 10);
        let seed = u64s(&mut next, n, 100);
        let input = Value::Prod(vec![Value::u64(seed.clone()), Value::List(bounds.clone(), Box::new(Value::u64(vals.clone())))]);
        let folded = eval_graph(&fold, input.clone());
        let scanned = eval_graph(&scan, input);
        let mut start = 0;
        for (r, &end) in ends.iter().enumerate() {
            let row = &vals[start..end];
            let steps: Vec<u64> = row
                .iter()
                .scan(seed[r], |a, &x| { *a = a.wrapping_mul(3).wrapping_add(x); Some(*a) })
                .collect();
            let failed = row.iter().any(|&x| x >= 8);
            let (want_fold, want_scan) = if failed {
                (err(&Shape::Prim(64)), err(&scan_shape))
            } else {
                let last = steps.last().copied().unwrap_or(seed[r]);
                (
                    ok(Value::u64(vec![last])),
                    ok(Value::Prod(vec![Value::u64(vec![last]), Value::List(vec![steps.len()].into(), Box::new(Value::u64(steps)))])),
                )
            };
            assert_eq!(arrange::gather(&folded, &[r]), want_fold, "{pattern}: fold row {r}");
            assert_eq!(arrange::gather(&scanned, &[r]), want_scan, "{pattern}: foldscan row {r}");
            start = end;
        }
    }
}
