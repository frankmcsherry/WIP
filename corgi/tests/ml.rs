//! ML front-end tests: same demos through `let` and juxtaposed stages, plus a check that `let`
//! sharing produces fewer nodes than the same join written with the shared subexpression inlined.

use corgi::{parse_ml, show, Program, Value};

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

/// run through the effect layer and render: a total program shows its pure value; a partial program
/// (an un-`TRY`'d `FailOp`) shows its result TRY'd to a `Sum{T | Unit}`.
fn run_ml(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
    p.check();
    show(&p.run_partial(arg.clone()))
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
fn const_in_lambda() {
    let src = "let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, 1000u64) add)";
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
        "((input.1, input.0 transpose .0) find, input.0 transpose .1) slices";
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
fn inject_by_name_carries_the_sum_shape() {
    // `inject Email` reads the tag AND the whole sum's lane shapes off the declaration, so the
    // Phone lane is built as an empty u64 column and `unwrap` typechecks.
    let src = "enum Contact = Email u64 | Phone u64 in input.0 inject Email unwrap";
    assert_eq!(run_ml(src, &sample()), "[10, 20, 30]");
    // a lane without a declared payload shape cannot be built empty.
    assert!(parse_ml("enum Contact = Email u64 | Phone in input.0 inject Email").is_err());
    // shapes nest: an enum names an earlier fully-shaped enum.
    let src = "enum Contact = Email u64 | Phone u64 in enum Card = Anon () | Known Contact in \
               input.0 inject Email inject Known";
    let p = Program::compile_ml(src).unwrap();
    assert_eq!(p.shape(&corgi::Shape::Prod(vec![corgi::Shape::Prim(64)])).unwrap().to_string(), "{() | {U64 | U64}}");
}

#[test]
fn branch_by_enum_and_named_match_arms() {
    let src = "enum Size = Lo | Hi in \
               let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, v gt 300) branch Size match (Lo (l -> l), Hi (h -> (h, 1u64) add)))";
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
    // a string literal is a constant List<U8>, one per row of its scope's input.
    assert_eq!(run_ml("\"hi\"", &u64(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}

#[test]
fn head_sugar_is_a_failop() {
    // `head` is the get FailOp: a non-empty row -> Found(first), an EMPTY row -> Oob — both carried in
    // the err-mask, shown TRY'd as Sum{T | Unit}. No panic, no total/unchecked split. (`input add_u64 1
    // iota` is [0..n+1); `input iota` at n=0 is the empty row.)
    assert_eq!(run_ml("input add_u64 1 iota head", &u64(&[3])), "Sum tags=[0] [[0], ()x0]");
    assert_eq!(run_ml("input iota head", &u64(&[0])), "Sum tags=[1] [[], ()x1]");
}

#[test]
fn typed_literals_are_expressions() {
    // A suffixed constant is an expression, filled to the length of its scope's input: here the
    // map body's, so it needs no anchor.
    assert_eq!(run_ml("input iota map (x -> (x, 100u64) add)", &u64(&[3])), "List ends=[3] <[100, 101, 102]>");
    // The suffix picks the encoding, so signed and float constants compare and compute correctly.
    assert_eq!(run_ml("((-3i64, 5i64) lt, (5i64, -3i64) lt)", &u64(&[0])), "([1], [0])");
    assert_eq!(run_ml("((-3i64, 5i64) add_i64, 2i64) eq", &u64(&[0])), "[1]");
    assert_eq!(run_ml("((0.5f64, 0.25f64) add_f64, 0.75f64) eq", &u64(&[0])), "[1]");
    assert_eq!(run_ml("((1.5e2f32, 2f32) div_f32, 75f32) eq", &u64(&[0])), "[1]");
    assert_eq!(run_ml("(255u8, 65535u16, 7u32)", &u64(&[0])), "([255], [65535], [7])");
    // A string is an expression too.
    assert_eq!(run_ml("(\"hi\", input) .0", &u64(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}

#[test]
fn typed_literals_are_checked() {
    let err = |src: &str| parse_ml(src).err().unwrap_or_else(|| panic!("{src} parsed"));
    assert!(err("(input, 256u8) add").contains("does not fit"), "{}", err("256u8"));
    assert!(err("(input, -1u64) add").contains("does not fit"));
    assert!(err("(input, 128i8) add").contains("does not fit"));
    assert!(err("(input, -3) add").contains("needs a type suffix"));
    assert!(err("(input, 1.5u64) add").contains("needs an f32 or f64 suffix"));
    assert!(err("(input, 5q8) add").contains("unknown literal suffix"));
    // The spellings typed literals and `.N` replaced are gone, each with a pointer to its replacement.
    assert!(err("input lit 5").contains("typed literal"));
    assert!(err("input lit_i64 5").contains("typed literal"));
    assert!(err("input sub 1").contains("(x, 1u64) sub"));
    assert!(err("input field 1").contains(".N"));
    assert!(err("input \"hi\"").contains("unexpected"));
    // Without a float suffix, `x.0.1` is still two projections.
    assert_eq!(run_ml("((input, (input, 7u64)), input).0.1.1", &u64(&[0])), "[7]");
}

#[test]
fn nested_patterns_and_wildcards() {
    let src = "let ((a, _), b) = ((input, (input, 1u64) add), (input, 10u64) add) in (a, b) add";
    assert_eq!(run_ml(src, &u64(&[5])), "[20]");
    let src = "input iota map (x -> ((x, x), x)) map (((a, _), c) -> (a, c) mul)";
    assert_eq!(run_ml(src, &u64(&[4])), "List ends=[4] <[0, 1, 4, 9]>");
}

#[test]
fn projection_after_any_stage() {
    // `flatten` returns (ranges, values); `.1` after the stage takes the values.
    assert_eq!(run_ml("input iota map (x -> x iota) flatten .1", &u64(&[4])), "List ends=[6] <[0, 0, 1, 0, 1, 2]>");
}

#[test]
fn comments_and_error_positions() {
    let src = "# the identity\ninput # stays as it is\n";
    assert_eq!(run_ml(src, &u64(&[3])), "[3]");
    let err = parse_ml("let x = input in\n  (x, y) add").err().unwrap();
    assert!(err.starts_with("2:7: unbound variable 'y'"), "{err}");
    let err = parse_ml("input\n  map (x ->").err().unwrap();
    assert!(err.starts_with("2:"), "{err}");
    let err = parse_ml("input ?").err().unwrap();
    assert!(err.starts_with("1:7: unexpected character"), "{err}");
}

/// Jaro-Winkler over bytes, written plainly: the reference the two example programs must match
/// bit for bit (it follows `strsim::jaro_winkler`, which agrees with it on ASCII).
fn jaro_winkler_reference(a: &[u8], b: &[u8]) -> f64 {
    let (la, lb) = (a.len(), b.len());
    if la == 0 && lb == 0 {
        return 1.0;
    }
    if la == 0 || lb == 0 {
        return 0.0;
    }
    let d = (la.max(lb) / 2).saturating_sub(1);
    let (mut fa, mut fb) = (vec![false; la], vec![false; lb]);
    let mut m = 0usize;
    for i in 0..la {
        for j in i.saturating_sub(d)..lb.min(i + d + 1) {
            if a[i] == b[j] && !fb[j] {
                fa[i] = true;
                fb[j] = true;
                m += 1;
                break;
            }
        }
    }
    if m == 0 {
        return 0.0;
    }
    let mut bs = (0..lb).filter(|&j| fb[j]);
    let t = (0..la).filter(|&i| fa[i]).filter(|&i| a[i] != b[bs.next().unwrap()]).count() / 2;
    let sim = ((m as f64 / la as f64) + (m as f64 / lb as f64) + ((m - t) as f64 / m as f64)) / 3.0;
    if sim > 0.7 {
        let p = a.iter().take(4).zip(b).take_while(|(x, y)| x == y).count();
        sim + 0.1 * p as f64 * (1.0 - sim)
    } else {
        sim
    }
}

#[test]
fn jaro_winkler_examples_match_the_reference() {
    // Short strings over five letters, so matches, transpositions and prefixes are all common.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut string = move || (0..next() % 16).map(|_| b'a' + (next() % 5) as u8).collect::<Vec<u8>>();
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = [("MARTHA", "MARHTA"), ("DIXON", "DICKSONX"), ("", ""), ("abc", "")]
        .iter()
        .map(|(a, b)| (a.as_bytes().to_vec(), b.as_bytes().to_vec()))
        .collect();
    pairs.extend((0..2000).map(|_| (string(), string())));
    let column = |xs: Vec<&Vec<u8>>| {
        let ends: Vec<usize> = xs.iter().scan(0, |e, x| { *e += x.len(); Some(*e) }).collect();
        Value::List(ends.into(), Box::new(Value::u8(xs.into_iter().flatten().copied().collect())))
    };
    let input = Value::Prod(vec![column(pairs.iter().map(|p| &p.0).collect()), column(pairs.iter().map(|p| &p.1).collect())]);
    let decode = |u: u64| f64::from_bits(if u >> 63 == 1 { u ^ (1 << 63) } else { !u });
    for src in [include_str!("../examples/jaro_winkler/direct.col"), include_str!("../examples/jaro_winkler/by_byte.col")] {
        let p = Program::compile_ml(src).expect("parse error");
        assert!(p.is_total());
        let out = p.run(input.clone()).unwrap().into_u64("similarity").unwrap();
        for ((a, b), u) in pairs.iter().zip(out) {
            assert_eq!(decode(u).to_bits(), jaro_winkler_reference(a, b).to_bits(), "{a:?} {b:?}");
        }
    }
}
