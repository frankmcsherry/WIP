//! ML front-end tests: same demos through `let` and juxtaposed stages, plus a check that `let`
//! sharing produces fewer nodes than the same join written with the shared subexpression inlined.

use std::sync::Arc;

use corgi::{parse_ml, parse_ml_in, show, Builder, HostKernel, HostOp, NumOp, Program, Scope, Shape, Value};

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

/// `select` over lists of leaves takes each row whole from the side its mask picks, whichever
/// storage each side holds.
#[test]
fn select_picks_each_list_row() {
    let words = Value::List(vec![2, 2, 5].into(), Box::new(int(&[1, 2, 300, 4, 5])));
    let bytes = Value::List(vec![1, 3, 3].into(), Box::new(Value::u8(vec![7, 8, 9])));
    let input = Value::Prod(vec![Value::u8(vec![1, 0, 1]), words, bytes]);
    assert_eq!(run_ml("let (m, a, b) = input in (m, a, b) select", &input), "List ends=[2, 4, 7] <[1, 2, 8, 9, 300, 4, 5]>");
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
    // two enums may share a variant name, but then a use must say which
    assert!(parse_ml("enum E = A int in enum F = A int in input inject A").is_err());
}

#[test]
fn a_variant_name_two_enums_share_is_written_with_its_enum() {
    let both = "enum Foo = Bar int | Qux int in enum Baz = Bar () | Zap int in ";
    // `Foo::Bar` says which `Bar`; `Qux` is Foo's alone and needs no enum
    let src = format!("{both} input inject Foo::Bar match (Foo::Bar (x -> x), Qux (_ -> 0))");
    assert_eq!(run_ml(&src, &int(&[5])), "[5]");
    let p = Program::compile_ml(&format!("{both} input inject Baz::Zap")).unwrap();
    assert_eq!(p.shape(&Shape::Int).unwrap().to_string(), "{() | Int}");
    let err = parse_ml(&format!("{both} input inject Bar")).err().unwrap();
    assert!(err.ends_with("'Bar' is a variant of Foo and Baz: write Foo::Bar or Baz::Bar"), "{err}");
    assert!(parse_ml(&format!("{both} input inject Baz::Qux")).is_err()); // not Baz's
    assert!(parse_ml(&format!("{both} input inject Nope::Bar")).is_err());
}

/// `x -> 2x` over Ints, as a host kernel.
struct Double;
impl HostKernel for Double {
    fn name(&self) -> &str { "double" }
    fn input(&self) -> &Shape { &Shape::Int }
    fn output(&self) -> &Shape { &Shape::Int }
    fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(Value::i64(input.as_i64("double")?.iter().map(|x| 2 * x).collect()))
    }
}

#[test]
fn call_reaches_the_words_a_scope_binds() {
    let mut scope = Scope::default();
    // a host kernel is the graph of one node over the input
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    let doubled = b.add(NumOp::Host(HostOp(Arc::new(Double))), vec![x]);
    scope.word("double", b.finish(doubled)).unwrap();
    // and a word may be written in corgi
    scope.word("inc", parse_ml("(input, 1) add").unwrap()).unwrap();
    // inside a lambda body, one after the other
    let rows = Value::List(vec![2, 3].into(), Box::new(int(&[1, 2, 3])));
    let g = parse_ml_in("input map (x -> x call double call inc)", &scope).unwrap();
    assert_eq!(show(&Program::from_graph(g).run(rows)), "List ends=[2, 3] <[3, 5, 7]>");
    // only the scope's words, and only through `call`
    let err = parse_ml_in("input call triple", &scope).err().unwrap();
    assert!(err.starts_with("1:12: no word 'triple' in scope"), "{err}");
    assert!(parse_ml_in("input double", &scope).is_err());
    assert!(parse_ml("input call inc").is_err());
    assert!(scope.word("inc", parse_ml("input").unwrap()).is_err());
    assert!(scope.word("two words", parse_ml("input").unwrap()).is_err());
}

#[test]
fn a_scope_declares_enums_for_every_program() {
    let mut scope = Scope::default();
    scope.declare("enum Size = Small int | Big (int, int)").unwrap();
    // one program builds a Size and another matches it; neither declares it
    let small = Program::from_graph(parse_ml_in("input inject Small", &scope).unwrap());
    let big = Program::from_graph(parse_ml_in("input inject Big", &scope).unwrap());
    let total = parse_ml_in("input match (Small (n -> n), Big ((a, b) -> (a, b) add))", &scope).unwrap();
    let total = Program::from_graph(total);
    assert_eq!(show(&total.run(small.run(int(&[5])))), "[5]");
    assert_eq!(show(&total.run(big.run(Value::Prod(vec![int(&[3]), int(&[4])])))), "[7]");
    // the graph is the one a program declaring the enum itself would have
    let own = parse_ml("enum Size = Small int | Big (int, int) in input inject Small").unwrap();
    assert_eq!(own, parse_ml_in("input inject Small", &scope).unwrap());
    // the host reads the declaration back, and resolves names as a program does
    assert_eq!(scope.variant("Big"), Ok((1, "Size")));
    assert_eq!(scope.variants("Size").unwrap()[1], ("Big".to_string(), Some(Shape::Prod(vec![Shape::Int, Shape::Int]))));
    // a program, or a later declaration, may share a variant name with the scope; uses then say which
    assert!(parse_ml_in("enum Other = Small int in input inject Small", &scope).is_err());
    let own = parse_ml_in("enum Other = Small int in input inject Size::Small", &scope).unwrap();
    assert_eq!(own, parse_ml_in("input inject Small", &scope).unwrap());
    scope.declare("enum Fits = Small () | Tiny int").unwrap();
    assert!(scope.variant("Small").is_err());
    assert_eq!(scope.variant("Fits::Small"), Ok((0, "Fits")));
    // enum names are unique, and a failed declaration changes nothing
    assert!(scope.declare("enum Size = Huge int").is_err());
    assert!(scope.declare("enum Odd = Once int | Once ()").is_err());
    assert!(scope.variant("Once").is_err());
    // a declaration, not a program
    assert!(scope.declare("enum Fits = Tiny int in input").is_err());
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

/// rows of words, each row a `List` of strings.
fn word_rows(rows: &[&[&str]]) -> Value {
    let (mut bytes, mut word_ends, mut row_ends) = (Vec::new(), Vec::new(), Vec::new());
    for row in rows {
        for w in *row {
            bytes.extend(w.bytes());
            word_ends.push(bytes.len());
        }
        row_ends.push(word_ends.len());
    }
    Value::List(
        corgi::Bounds::offsets(row_ends),
        Box::new(Value::List(corgi::Bounds::offsets(word_ends), Box::new(Value::u8(bytes)))),
    )
}

#[test]
fn hash_group_finds_the_groups_group_does() {
    // the same groups as `group`, row by row, in another order; sorted, they agree
    let input = word_rows(&[&["b", "a", "c", "a", "b", "a", ""], &[], &["x", "x"]]);
    assert_eq!(
        run_ml("input map (w -> (w, ())) hash_group map ((k, rows) -> (k clone, rows len)) sort", &input),
        run_ml("input map (w -> (w, ())) group map ((k, rows) -> (k, rows len))", &input),
    );
    // a key comes out by reference: cloned, it is the key
    assert_eq!(
        run_ml("input map (w -> (w, ())) hash_group map ((k, _) -> k clone) sort", &input),
        run_ml("input dedup", &input),
    );
}

#[test]
fn all_and_any_of_written_conditions() {
    let input = word_rows(&[&["b", "a", "", "abc"]]);
    assert_eq!(run_ml("input map (w -> ((w len, 0) gt, (w len, 1) le, (w, \"a\") ne) all)", &input), "List ends=[4] <[1, 0, 0, 0]>");
    assert_eq!(run_ml("input map (w -> ((w len, 2) gt, (w, \"b\") eq) any)", &input), "List ends=[4] <[1, 0, 0, 1]>");
    // a condition alone is itself; `all` of something not written as a tuple is an error
    assert_eq!(run_ml("input map (w -> ((w len, 0) gt) all)", &input), run_ml("input map (w -> (w len, 0) gt)", &input));
    assert!(Program::compile_ml("input map (w -> w all)").is_err());
}

/// shape errors from typing `src` against `input`'s shape.
fn shape_err(src: &str, input: &Shape) -> String {
    Program::compile_ml(src).expect("parse error").shape(input).expect_err("typed")
}

/// A list literal of constants is one constant, as a string is: `"ab"` and `[97, 98]` build the
/// same graph, a single `Lit` filled to its scope's length.
#[test]
fn list_literal_is_one_constant() {
    assert_eq!(parse_ml("[97, 98, 99]").unwrap(), parse_ml("\"abc\"").unwrap());
    assert_eq!(parse_ml("[1, 2, 300]").unwrap().node_count(), 2); // the input, and the literal
    assert_eq!(run_ml("[1, 2, 300]", &int(&[5, 6])), "List ends=[3, 6] <[1, 2, 300, 1, 2, 300]>");
    // tuples, strings, negative numbers, floats, the unit, and lists, nested
    assert_eq!(
        run_ml("[(0, \"GET\", -1, 1.5, ()), (1, \"PUT\", 2, -0.5, ())]", &int(&[7])),
        "List ends=[2] <([0, 1], List ends=[3, 6] <[71, 69, 84, 80, 85, 84]>, [-1, 2], [1.5, -0.5], ()x2)>",
    );
    assert_eq!(run_ml("[[1, 2], [], [3]]", &int(&[7])), "List ends=[3] <List ends=[2, 2, 3] <[1, 2, 3]>>");
    // in a lambda, the literal is as long as the lambda's parameter
    assert_eq!(run_ml("input iota map (i -> ([10, 20], i enlist) append)", &int(&[2])), "List ends=[2] <List ends=[3, 6] <[10, 20, 0, 10, 20, 1]>>");
}

/// `[]` takes its shape from an ascription, or from the elements beside it; with neither it is an
/// error, as is an element that is not a constant.
#[test]
fn empty_list_literal_needs_its_shape() {
    assert_eq!(run_ml("[] : List((int, int))", &int(&[7, 8])), "List ends=[0, 0] <([], [])>");
    assert_eq!(Program::compile_ml("[] : List((int, int))").unwrap().shape(&Shape::Int).unwrap().to_string(), "List<(Int, Int)>");
    assert_eq!(run_ml("[([], 1), ([2], 3)]", &int(&[7])), "List ends=[2] <(List ends=[0, 1] <[2]>, [1, 3])>");
    assert_eq!(run_ml("[[] : List(int)]", &int(&[7])), "List ends=[1] <List ends=[0] <[]>>");
    let err = |src: &str| Program::compile_ml(src).err().unwrap_or_else(|| panic!("{src} parsed"));
    for hole in ["[]", "[[]]", "[] : List(_)"] {
        assert!(err(hole).contains("has a hole"), "{hole}: {}", err(hole));
    }
    assert!(err("[1, 2.5]").ends_with("a list literal's elements differ in shape: int and float"));
    assert!(err("[1] : List(float)").ends_with("a constant of shape List(int) is not List(float)"));
    for computed in ["[input]", "[(1, 2) add]"] {
        assert!(err(computed).contains("(a enlist, b enlist) append"), "{computed}: {}", err(computed));
    }
}

/// `: S` checks a value's shape, as a stage and on a pattern; `_` is a hole. A failed check is a
/// shape error when the program is typed.
#[test]
fn ascriptions_check_shapes() {
    let input = Value::Prod(vec![int(&[1, 2]), Value::List(vec![1, 3].into(), Box::new(int(&[4, 5, 6])))]);
    let shape = Shape::Prod(vec![Shape::Int, Shape::List(Box::new(Shape::Int))]);
    let typed = "let (k : int, vs : List(int)) = input in (k, vs fold_add : int) add : int";
    assert_eq!(Program::compile_ml(typed).unwrap().shape(&shape).unwrap(), Shape::Int);
    assert_eq!(run_ml(typed, &input), "[5, 13]");
    assert_eq!(run_ml("input : (int, List(_)) .1 len", &input), "[1, 2]");
    // the fold's signature, written out
    assert_eq!(run_ml("((0, 0), input.1) fold ((acc : (int, int), x : int) -> ((acc.0, x) add, (acc.1, 1) add))", &input), "([4, 11], [1, 2])");
    assert_eq!(shape_err("input .1 : List(float)", &shape), "in the pattern List(float), the pattern float wants a Float, not Int");
    assert_eq!(shape_err("let (k, vs) : (int, int) = input in k", &shape), "in the pattern (int, int), the pattern int wants an Int, not List<Int>");
    assert!(Program::compile_ml("let (k, vs) : (int, int, int) = input in k").is_err()); // the pattern itself has 2 fields
    // an enum whose variants declare no payloads is a sum of holes
    let tagged = "enum Opt = None | Some in (input.0, input.0) branch Opt : Opt";
    assert_eq!(run_ml(tagged, &input), run_ml("(input.0, input.0) branch 2", &input));
    assert_eq!(shape_err("enum Opt = None | Some | Many in (input.0, input.0) branch 2 : Opt", &shape), "the pattern {_ | _ | _} wants a sum of 3 lanes, not {Int | Int}");
}

/// `append` takes one or more lists, so a list of computed values is one append of enlisted
/// columns, as long as each row has its elements.
#[test]
fn append_takes_any_number_of_lists() {
    let input = Value::Prod(vec![int(&[1, 2]), int(&[300, 4])]);
    let three = "let (a, b) = input in (a enlist, b enlist, (a, b) add enlist) append";
    assert_eq!(run_ml(three, &input), "List ends=[3, 6] <[1, 300, 301, 2, 4, 6]>");
    // the same as the pairs it replaces
    let pairs = "let (a, b) = input in ((a enlist, b enlist) append, (a, b) add enlist) append";
    assert_eq!(run_ml(three, &input), run_ml(pairs, &input));
    // elements that are not leaves, and a single list
    assert_eq!(
        run_ml("let (a, b) = input in ((a, b) enlist, (b, a) enlist, ((a, b) add, 0) enlist) append", &input),
        "List ends=[3, 6] <([1, 300, 301, 2, 4, 6], [300, 1, 0, 4, 2, 0])>",
    );
    assert_eq!(run_ml("let (a, b) = input in (a iota) append", &input), run_ml("input.0 iota", &input));
    // lists held at different storages meet at the wider
    assert_eq!(run_ml("(\"ab\", 300 enlist, input.0 enlist) append", &input), "List ends=[4, 8] <[97, 98, 300, 1, 97, 98, 300, 2]>");
}

/// A closed program evaluated on one unit row is a constant: how a host can write a value in corgi.
#[test]
fn closed_program_on_one_row_is_a_value() {
    let p = Program::compile_ml("enum Opt = None () | Some int in ([(1, \"a\"), (2, \"bc\")], 7 inject Some)").unwrap();
    assert_eq!(show(&p.run(Value::Unit(1))), "(List ends=[2] <([1, 2], List ends=[1, 3] <[97, 98, 99]>)>, Sum tags=[1] [()x0, [7]])");
}

/// `inject` into a variant whose payload shape the enum leaves open fills it with the payload's
/// shape, so one `Option` serves every payload. The lanes it does not fill still need shapes.
#[test]
fn inject_fills_an_open_lane_with_its_payloads_shape() {
    let both = Program::compile_ml("enum Option = None () | Some in (3 inject Some, \"dog\" inject Some)").unwrap();
    assert_eq!(both.shape(&Shape::Unit).unwrap().to_string(), "({() | Int}, {() | List<Int>})");
    assert_eq!(show(&both.run(Value::Unit(1))), "(Sum tags=[1] [()x0, [3]], Sum tags=[1] [()x0, List ends=[3] <[100, 111, 103]>])");
    // matched by name and checked by ascription, whatever the payload
    assert_eq!(run_ml("enum Option = None () | Some in input inject Some : Option match (None (_ -> 0), Some (x -> (x, 1) add))", &int(&[1, 2])), "[2, 3]");
    // a declared lane is still checked, and the lanes not filled must be declared
    assert_eq!(shape_err("enum O = N () | S int in \"dog\" inject S", &Shape::Unit), "Inject: lane 1 is declared Int, got List<Int>");
    let none = Program::compile_ml("enum Option = None () | Some in () inject None").err().unwrap();
    assert!(none.ends_with("inject None: enum 'Option' declares no payload shape for 'Some', whose empty lane needs one"), "{none}");
}

/// Parentheses build a tuple, so `(e)` where `e` was meant is a one-field tuple; a shape error on
/// one that holds what was wanted says so.
#[test]
fn shape_errors_note_a_one_field_tuple() {
    let note = "(a one-field tuple: parentheses build a tuple, so `(e)` is not `e`)";
    for src in ["(([] : List(int)), [1]) append", "((input), 2) add", "((input), input) min", "(input) : int"] {
        let err = shape_err(src, &Shape::Int);
        assert!(err.ends_with(note), "{src}: {err}");
    }
    // a one-field tuple of something else is no such slip
    assert_eq!(shape_err("(input) fold_add", &Shape::Int), "reduce: expected a list, got (Int)");
}
