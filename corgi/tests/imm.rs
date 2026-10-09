//! Constant operands run as immediates: `Program` rewrites a binary op on a pair holding a literal,
//! `(x, c) op`, into one op that carries `c` (`corgi::immediates`). These check every arithmetic
//! and word op, the six comparisons and lane min/max against the same program evaluated as parsed
//! (pair and literal column, no rewrite), on random columns of each storage (Int as bytes, Int as
//! `i64`s, Float) and the constants at the edges of an Int, through the path that writes into an
//! operand it owns; then where the rewrite fires, and where it must not.

use corgi::{dce, eval_graph, immediates, lower_effects, parse_ml, Program, Value};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 ^ (self.0 >> 29)
    }
}

/// through `Program`, which rewrites pairs with a literal into immediates.
fn run(src: &str, input: Value) -> Value {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}")).run(input)
}

/// the program as parsed: pairs with a literal stay pairs.
fn run_pair(src: &str, input: Value) -> Value {
    eval_graph(&lower_effects(&parse_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"))), input)
}

/// nodes left once the rewrite and dead-node sweep have run: `input` plus one immediate op is 2.
fn nodes_after(src: &str) -> usize {
    dce(&immediates(&parse_ml(src).unwrap())).node_count()
}

/// the storages a leaf column comes in.
#[derive(Clone, Copy, Debug)]
enum Col {
    Bytes,
    I64,
    Float,
}

/// `n` random rows of `col`, plus its edge values. A float column holds real floats.
fn rows(col: Col, n: usize, rng: &mut Rng) -> Value {
    match col {
        Col::Bytes => {
            let mut xs: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
            xs.extend([0, 1, 127, 128, 255]);
            Value::u8(xs)
        }
        Col::I64 => {
            let mut xs: Vec<i64> = (0..n).map(|_| rng.next() as i64).collect();
            xs.extend([0, 1, -1, 255, 256, i64::MAX, i64::MIN]);
            Value::i64(xs)
        }
        Col::Float => {
            let mut xs: Vec<f64> = (0..n).map(|_| (rng.next() % 20001) as f64 / 10.0 - 1000.0).collect();
            xs.extend([0.0, -0.0, 1.0, -1.0, 1e300, f64::INFINITY]);
            Value::f64(xs)
        }
    }
}

/// a freshly allocated copy of a leaf column, so the immediate kernels, which write into an
/// operand they own, take that path.
fn fresh(v: &Value) -> Value {
    if let Ok(xs) = v.as_u8("fresh") {
        Value::u8(xs.to_vec())
    } else if let Ok(xs) = v.as_f64("fresh") {
        Value::f64(xs)
    } else {
        Value::i64(v.as_i64("fresh").unwrap().into_owned())
    }
}

/// the constants a column of `col` meets: for an Int, small ones, the byte edges and the ends of
/// an `i64`.
fn constants(col: Col) -> &'static [&'static str] {
    match col {
        Col::Float => &["0.0", "-0.0", "-2.5", "1e3"],
        _ => &["0", "1", "-1", "7", "255", "256", "9223372036854775807", "-9223372036854775808"],
    }
}

#[test]
fn arithmetic_with_a_constant_matches_the_pair_form() {
    let mut rng = Rng(5);
    for col in [Col::Bytes, Col::I64, Col::Float] {
        let ops: &[&str] = match col {
            Col::Float => &["add", "sub", "mul", "div"],
            _ => &["add", "sub", "mul", "div", "rem", "add_b64", "sub_b64", "mul_b64", "and", "or", "xor"],
        };
        for op in ops {
            for c in constants(col) {
                let xs = rows(col, 300, &mut rng);
                let src = format!("(input, {c}) {op}");
                assert_eq!(nodes_after(&src), 2, "{src}: not rewritten");
                assert_eq!(run(&src, fresh(&xs)), run_pair(&src, fresh(&xs)), "{src} on {col:?}");
            }
        }
    }
}

#[test]
fn comparisons_and_min_max_with_a_constant_match_the_pair_form() {
    let mut rng = Rng(7);
    for col in [Col::Bytes, Col::I64, Col::Float] {
        for c in constants(col) {
            let xs = rows(col, 300, &mut rng);
            // the literal on the right, and on the left, where comparisons flip
            for src in [
                format!("(input, {c}) eq"), format!("(input, {c}) ne"), format!("(input, {c}) lt"),
                format!("(input, {c}) le"), format!("({c}, input) lt"), format!("({c}, input) le"),
                format!("(input, {c}) min"), format!("(input, {c}) max"), format!("({c}, input) max"),
            ] {
                assert_eq!(nodes_after(&src), 2, "{src}: not rewritten");
                assert_eq!(run(&src, fresh(&xs)), run_pair(&src, fresh(&xs)), "{src} on {col:?}");
            }
        }
    }
}

#[test]
fn pairs_with_a_literal_become_immediates() {
    // a literal on the right, any op with an immediate form
    for src in ["(input, 4) add", "(input, 4) sub", "(input, 4) rem", "(input, 4) div", "(input, 4) lt", "(input, 4) max", "(input, 4) sub_b64"] {
        assert_eq!(nodes_after(src), 2, "{src}");
    }
    // on the left, where the order doesn't matter, and comparisons, which flip
    for src in ["(4, input) add", "(4, input) mul", "(4, input) eq", "(4, input) min", "(4, input) lt", "(4, input) le", "(4, input) xor"] {
        assert_eq!(nodes_after(src), 2, "{src}");
    }
    // stays a pair: order matters, float on the left, no literal, a list literal
    for src in ["(4, input) sub", "(4, input) rem", "(4, input) sub_b64", "(1.5, input) add", "(input, input) add", "(input, \"ab\") eq"] {
        assert!(nodes_after(src) > 2, "{src}");
    }
    // (that it reaches into bodies is a unit test in optimize.rs, where bodies can be read)
}

#[test]
fn the_rewrite_keeps_every_answer() {
    // left-literal and flipped comparisons against the pair as parsed; each column fresh
    let xs: Vec<i64> = (0..200).map(|i| (i * 7919) % 50 - 25).collect();
    for src in [
        "(4, input) add", "(4, input) mul", "(4, input) eq", "(4, input) ne",
        "(4, input) lt", "(4, input) le", "(4, input) min", "(4, input) max",
        "(4, input) xor", "(-8, input) and", "(3, input) mul_b64",
        "(input, 4) sub", "(input, 0) rem", "(input, 0) div",
        "input iota map (x -> ((x, 3) mul, 7) add) fold_add",
    ] {
        assert_eq!(run(src, Value::i64(xs.clone())), run_pair(src, Value::i64(xs.clone())), "{src}");
    }
    // a literal shared by two consumers, one of which can't take it as an immediate
    let src = "let c = 4 in ((input, c) add, (c, input) sub)";
    assert_eq!(run(src, Value::i64(xs.clone())), run_pair(src, Value::i64(xs)), "{src}");
}
