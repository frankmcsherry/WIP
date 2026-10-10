//! ML front-end tests: same demos through `let` and juxtaposed stages, plus a check that `let`
//! sharing produces fewer nodes than the same join written with the shared subexpression inlined.

use corgi::{parse_ml, show, Program, Value};

fn int(xs: &[i64]) -> Value {
    Value::i64(xs.to_vec())
}

/// run through `Program` and render.
fn run_ml(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
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

#[test]
fn sum_scores_with_destructure() {
    let src = "let (subj, vals) = input.1 transpose in vals fold_add";
    assert_eq!(run_ml(src, &sample()), "[300, 300, 1500]");
}

/// Parentheses build a tuple in an expression as they do in a pattern and a shape: `(x)` is a
/// one-field tuple, and a pattern `(x)` takes one apart.
#[test]
fn parentheses_around_one_expression_build_a_tuple() {
    let input = Value::Prod(vec![int(&[1, 2]), int(&[3, 4])]);
    let pairs = "let (src, dst) = input in ((src), (dst))";
    assert_eq!(Program::compile_ml(pairs).unwrap().run(input.clone()), Value::Prod(vec![Value::Prod(vec![int(&[1, 2])]), Value::Prod(vec![int(&[3, 4])])]));
    let round_trip = "let (x, y) = input in let ((a), (b)) = ((x), (y)) in (a, b) add";
    assert_eq!(run_ml(round_trip, &input), "[4, 6]");
}

/// `()` is the tuple with no fields, the unit, in an expression and in a pattern, as in a shape.
#[test]
fn empty_parentheses_are_the_unit() {
    let input = Value::Prod(vec![int(&[1, 2]), int(&[3, 4])]);
    let keyed = Program::compile_ml("let (k, v) = input in (k, ())").unwrap().run(input.clone());
    assert_eq!(keyed, Value::Prod(vec![int(&[1, 2]), Value::Unit(2)]));
    let unkeyed = "let ((a, b), ()) = (input, ()) in (a, b) add";
    assert_eq!(run_ml(unkeyed, &input), "[4, 6]");
}

#[test]
fn match_contact() {
    let src = "input.2 map_variant 1 (p -> (p, 1000000) add) unwrap";
    assert_eq!(run_ml(src, &sample()), "[1111, 1002222, 3333]");
}

#[test]
fn const_in_lambda() {
    let src = "let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, 1000) add)";
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[1100, 1200, 1300, 1400, 1500, 1600]>");
}

#[test]
fn juxtaposition_stops_at_let_in() {
    // the juxtaposed chain `input.1 transpose` must terminate at the `let` body's `in`, not read it
    // as an op; then a bare lambda maps over the result.
    let src = "let (subj, vals) = input.1 transpose in subj map (v -> (v, 1) add)";
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
    //   ( id:Int, scores:List<(subj,val)>, contact:Sum{Email|Phone} ):
    //   input.1 transpose        List<(subj,val)> -> (List<subj>, List<val>)   [list <-> product]
    //   scores.1 fold_add       List<val> -> one Int per record (sum each row) [list -> scalar]
    //   (input.0, totals) add     pair the id column with the totals, add them   [product + arith]
    //   map_variant 0 (..) unwrap bump the Email variant, then flatten the sum    [sum navigation]
    //   (id_plus, contact)           bundle the two results into a product           [product build]
    let src = "let scores = input.1 transpose in \
               let totals = scores.1 fold_add in \
               let id_plus = (input.0, totals) add in \
               let contact = input.2 map_variant 0 (e -> (e, 1000000) add) unwrap in \
               (id_plus, contact)";
    assert_eq!(run_ml(src, &sample()), "([310, 320, 1530], [1001111, 2222, 1003333])");
}

#[test]
fn enum_names_resolve_and_erase() {
    // the declaration is a compile-time table: `Phone` resolves to tag 1 and erases, so this is
    // the same graph as `match_contact`.
    let src = "enum Contact = Email | Phone in \
               input.2 map_variant Phone (p -> (p, 1000000) add) unwrap";
    assert_eq!(run_ml(src, &sample()), "[1111, 1002222, 3333]");
}

#[test]
fn inject_by_name_carries_the_sum_shape() {
    // `inject Email` reads the tag AND the whole sum's lane shapes off the declaration, so the
    // Phone lane is built as an empty Int column and `unwrap` typechecks.
    let src = "enum Contact = Email int | Phone int in input.0 inject Email unwrap";
    assert_eq!(run_ml(src, &sample()), "[10, 20, 30]");
    // a lane without a declared payload shape cannot be built empty.
    assert!(parse_ml("enum Contact = Email int | Phone in input.0 inject Email").is_err());
    // shapes nest: an enum names an earlier fully-shaped enum.
    let src = "enum Contact = Email int | Phone int in enum Card = Anon () | Known Contact in \
               input.0 inject Email inject Known";
    let p = Program::compile_ml(src).unwrap();
    assert_eq!(p.shape(&corgi::Shape::Prod(vec![corgi::Shape::Int])).unwrap().to_string(), "{() | {Int | Int}}");
}

#[test]
fn branch_by_enum_and_named_match_arms() {
    let src = "enum Size = Lo | Hi in \
               let (subj, vals) = input.1 transpose in \
               vals map (v -> (v, (v, 300) gt) branch Size match (Lo (l -> l), Hi (h -> (h, 1) add)))";
    // `branch` is total (the demux Sum{Lo|Hi}), and the match arms align: Hi (>300) gets +1.
    assert_eq!(run_ml(src, &sample()), "List ends=[2, 3, 6] <[100, 200, 300, 401, 501, 601]>");
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
    // a string literal is a constant List<Int> of bytes, one per row of its scope's input.
    assert_eq!(run_ml("\"hi\"", &int(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}

#[test]
fn head_and_the_checked_words() {
    // `head` is `get 0`, lossy: an EMPTY row reads the zero of the element's shape. Each `try_` word
    // puts a row its op would lose something on in lane 1 as `()`, and runs the op on the rest in
    // lane 0. (`(input, 1) add iota` is [0..n+1); `input iota` at n=0 is the empty row.)
    assert_eq!(run_ml("(input, 1) add iota head", &int(&[3])), "[0]");
    assert_eq!(run_ml("input iota head", &int(&[0])), "[0]");
    assert_eq!(run_ml("(0, input iota) try_get", &int(&[2, 0])), "Sum tags=[0, 1] [[0], ()x1]");
    // against a row of two: positions 2 and -1 are out, 1 is in; no positions at all is in, even
    // against an empty row.
    assert_eq!(run_ml("(input, 2 iota) try_get", &int(&[2, -1, 1])), "Sum tags=[1, 1, 0] [[1], ()x2]");
    assert_eq!(run_ml("(input enlist, 2 iota) try_gather", &int(&[2, -1, 1])), "Sum tags=[1, 1, 0] [List ends=[1] <[1]>, ()x2]");
    assert_eq!(run_ml("(0 iota, input iota) try_gather", &int(&[0, 2])), "Sum tags=[0, 0] [List ends=[0, 0] <[]>, ()x0]");
    assert_eq!(run_ml("input iota try_chunk 2", &int(&[4, 3])), "Sum tags=[0, 1] [List ends=[2] <List ends=[2, 4] <[0, 1, 2, 3]>>, ()x1]");
}

#[test]
fn literals_are_expressions() {
    // A constant is an expression, filled to the length of its scope's input: here the map body's,
    // so it needs no anchor.
    assert_eq!(run_ml("input iota map (x -> (x, 100) add)", &int(&[3])), "List ends=[3] <[100, 101, 102]>");
    // Negative and float constants compare and compute by value.
    assert_eq!(run_ml("((-3, 5) lt, (5, -3) lt)", &int(&[0])), "([1], [0])");
    assert_eq!(run_ml("((-3, 5) add, 2) eq", &int(&[0])), "[1]");
    assert_eq!(run_ml("((0.5, 0.25) add, 0.75) eq", &int(&[0])), "[1]");
    assert_eq!(run_ml("((1.5e2, 2.0) div, 75.0) eq", &int(&[0])), "[1]");
    assert_eq!(run_ml("(255, -65535, 0.5)", &int(&[0])), "([255], [-65535], [0.5])");
    // A string is an expression too.
    assert_eq!(run_ml("(\"hi\", input) .0", &int(&[0, 0])), "List ends=[2, 4] <[104, 105, 104, 105]>");
}

#[test]
fn literals_are_checked() {
    let err = |src: &str| parse_ml(src).err().unwrap_or_else(|| panic!("{src} parsed"));
    // A literal takes no suffix; the old typed literals point at the bare spelling.
    assert!(err("(input, 256u8) add").contains("takes no suffix"), "{}", err("256u8"));
    assert!(err("(input, 5u64) add").contains("so write 5"));
    assert!(err("(input, -3i64) add").contains("so write -3"));
    assert!(err("(input, 1.5f64) add").contains("so write 1.5"));
    assert!(err("(input, 5q8) add").contains("takes no suffix"));
    // An Int is an i64: a literal past either end does not fit.
    assert!(err("(input, 9223372036854775808) add").contains("does not fit"));
    assert!(err("(input, -9223372036854775809) add").contains("does not fit"));
    // The spellings literals and `.N` replaced are gone, each with a pointer to its replacement.
    assert!(err("input lit 5").contains("a constant is a literal"));
    assert!(err("input lit_i64 5").contains("a constant is a literal"));
    assert!(err("input sub 1").contains("(x, 1) sub"));
    assert!(err("input field 1").contains(".N"));
    assert!(err("input \"hi\"").contains("unexpected"));
    // A number right after a `.` is a projection, never a float: `x.0.1` is two of them.
    assert_eq!(run_ml("((input, (input, 7)), input).0.1.1", &int(&[0])), "[7]");
}

#[test]
fn retired_spellings_point_to_their_replacements() {
    let err = |src: &str| parse_ml(src).err().unwrap_or_else(|| panic!("{src} parsed"));
    // a typed op: integers have no width, and the plain op takes its kind from its operands.
    for op in ["add_u64", "sub_i32", "mul_u8", "div_f64", "rem_i64", "neg_i16"] {
        let e = err(&format!("input {op}"));
        assert!(e.contains("is retired") && e.contains("takes its kind from its operands"), "{op}: {e}");
    }
    assert!(err("input add_u64").contains("add_b64"));
    // the conversions that existed only because integers had widths and signs.
    assert!(err("input cast").contains("width is its storage"));
    assert!(err("input signed").contains("signed values already"));
    assert!(err("input to_f64").contains("use to_float"));
    assert!(err("input to_f32").contains("use to_float"));
    assert!(err("input parse_u64").contains("use parse_int"));
    // shape names: an integer is `int`, a float `float`.
    assert!(err("enum E = A u64 | B () in input inject A").contains("is retired: an integer is `int`"));
    assert!(err("enum E = A f64 | B () in input inject A").contains("a float `float`"));
}

#[test]
fn nested_patterns_and_wildcards() {
    let src = "let ((a, _), b) = ((input, (input, 1) add), (input, 10) add) in (a, b) add";
    assert_eq!(run_ml(src, &int(&[5])), "[20]");
    let src = "input iota map (x -> ((x, x), x)) map (((a, _), c) -> (a, c) mul)";
    assert_eq!(run_ml(src, &int(&[4])), "List ends=[4] <[0, 1, 4, 9]>");
}

#[test]
fn projection_after_any_stage() {
    // `flatten` returns (ranges, values); `.1` after the stage takes the values.
    assert_eq!(run_ml("input iota map (x -> x iota) flatten .1", &int(&[4])), "List ends=[6] <[0, 0, 1, 0, 1, 2]>");
}

#[test]
fn comments_and_error_positions() {
    let src = "# the identity\ninput # stays as it is\n";
    assert_eq!(run_ml(src, &int(&[3])), "[3]");
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
    for src in [include_str!("../algorithms/jaro_winkler_direct.col"), include_str!("../algorithms/jaro_winkler_by_byte.col")] {
        let p = Program::compile_ml(src).expect("parse error");
        let out = p.run(input.clone()).as_f64("similarity").unwrap();
        for ((a, b), x) in pairs.iter().zip(out) {
            assert_eq!(x.to_bits(), jaro_winkler_reference(a, b).to_bits(), "{a:?} {b:?}");
        }
    }
}
