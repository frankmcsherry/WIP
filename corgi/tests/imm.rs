//! Constant operands run as immediates: `Program` rewrites a binary op on a pair holding a literal,
//! `(x, c) op`, into one op that carries `c` (`corgi::immediates`). These check every cell of the
//! arithmetic grid, the six comparisons and lane min/max against the same program evaluated as
//! parsed (pair and literal column, no rewrite), on random columns and the constants at the edges of
//! each width, through the path that writes into an operand it owns; then where the rewrite fires,
//! and where it must not.

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

fn enc_f64(f: f64) -> u64 {
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
}

/// `n` random rows at `width` (stored bits), plus the edge values of the width. A float column
/// holds real floats in their encoding.
fn rows(kind: char, width: u32, n: usize, rng: &mut Rng) -> Vec<u64> {
    if kind == 'f' {
        let mut xs: Vec<f64> = (0..n).map(|_| (rng.next() % 20001) as f64 / 10.0 - 1000.0).collect();
        xs.extend([0.0, -0.0, 1.0, -1.0, 1e300, f64::INFINITY]);
        return xs.into_iter().map(enc_f64).collect();
    }
    let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
    let mut xs: Vec<u64> = (0..n).map(|_| rng.next() & mask).collect();
    xs.extend([0, 1, mask, mask >> 1, (mask >> 1) + 1]);
    xs
}

/// a freshly allocated column of `xs` at `width`, so the immediate kernels, which write into an
/// operand they own, take that path.
fn column(width: u32, xs: &[u64]) -> Value {
    match width {
        8 => Value::u8(xs.iter().map(|&x| x as u8).collect()),
        16 => Value::u16(xs.iter().map(|&x| x as u16).collect()),
        32 => Value::u32(xs.iter().map(|&x| x as u32).collect()),
        _ => Value::u64(xs.to_vec()),
    }
}

fn constants(kind: char, width: u32) -> Vec<String> {
    match kind {
        'u' => {
            let max = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
            vec![format!("0u{width}"), format!("1u{width}"), format!("7u{width}"), format!("{max}u{width}")]
        }
        'i' => {
            let max = (1i128 << (width - 1)) - 1;
            let min = -(1i128 << (width - 1));
            vec![format!("0i{width}"), format!("-1i{width}"), format!("3i{width}"), format!("{max}i{width}"), format!("{min}i{width}")]
        }
        _ => vec![format!("0.0f{width}"), format!("-2.5f{width}"), format!("1e3f{width}")],
    }
}

#[test]
fn arithmetic_with_a_constant_matches_the_pair_form() {
    let mut rng = Rng(5);
    for (kind, widths) in [('u', vec![8, 16, 32]), ('i', vec![8, 16, 32, 64]), ('f', vec![64])] {
        for &w in &widths {
            for op in ["add", "sub", "mul", "div", "rem"] {
                if kind == 'f' && op == "rem" {
                    continue;
                }
                let src_of = |c: &str| format!("(input, {c}) {op}_{kind}{w}");
                for c in constants(kind, w) {
                    let xs = rows(kind, w, 300, &mut rng);
                    let src = src_of(&c);
                    assert_eq!(nodes_after(&src), 2, "{src}: not rewritten");
                    assert_eq!(run(&src, column(w, &xs)), run_pair(&src, column(w, &xs)), "{src}");
                }
            }
        }
    }
    // the u64 row of the grid is spelled without a suffix (`add_u64` names the older immediate)
    let mut rng = Rng(6);
    for op in ["add", "sub", "mul", "rem", "div_u64"] {
        for c in constants('u', 64) {
            let xs = rows('u', 64, 300, &mut rng);
            let src = format!("(input, {c}) {op}");
            assert_eq!(nodes_after(&src), 2, "{src}: not rewritten");
            assert_eq!(run(&src, column(64, &xs)), run_pair(&src, column(64, &xs)), "{src}");
        }
    }
}

#[test]
fn comparisons_and_min_max_with_a_constant_match_the_pair_form() {
    let mut rng = Rng(7);
    for w in [8, 16, 32, 64] {
        for c in constants('u', w).into_iter().chain(constants('i', w)) {
            let xs = rows('u', w, 300, &mut rng);
            // the literal on the right, and on the left, where comparisons flip
            for src in [
                format!("(input, {c}) eq"), format!("(input, {c}) ne"), format!("(input, {c}) lt"),
                format!("(input, {c}) le"), format!("({c}, input) lt"), format!("({c}, input) le"),
                format!("(input, {c}) min"), format!("(input, {c}) max"), format!("({c}, input) max"),
            ] {
                assert_eq!(nodes_after(&src), 2, "{src}: not rewritten");
                assert_eq!(run(&src, column(w, &xs)), run_pair(&src, column(w, &xs)), "{src}");
            }
        }
    }
}

#[test]
fn pairs_with_a_literal_become_immediates() {
    // a literal on the right, any op with an immediate form
    for src in ["(input, 4u64) add", "(input, 4u64) sub", "(input, 4u64) rem", "(input, 4i64) div_i64", "(input, 4u64) lt", "(input, 4u64) max"] {
        assert_eq!(nodes_after(src), 2, "{src}");
    }
    // on the left, where the order doesn't matter, and comparisons, which flip
    for src in ["(4u64, input) add", "(4u64, input) mul", "(4u64, input) eq", "(4u64, input) min", "(4u64, input) lt", "(4u64, input) le"] {
        assert_eq!(nodes_after(src), 2, "{src}");
    }
    // stays a pair: order matters, float on the left, a width that disagrees, no literal, a list literal
    for src in ["(4u64, input) sub", "(4u64, input) rem", "(1.5f64, input) add_f64", "(input, 4u32) add", "(input, input) add", "(input, \"ab\") eq"] {
        assert!(nodes_after(src) > 2, "{src}");
    }
    // (that it reaches into bodies is a unit test in optimize.rs, where bodies can be read)
}

#[test]
fn the_rewrite_keeps_every_answer() {
    // left-literal and flipped comparisons against the pair as parsed; each column fresh
    let xs: Vec<u64> = (0..200).map(|i| (i * 7919) % 50).collect();
    for src in [
        "(4u64, input) add", "(4u64, input) mul", "(4u64, input) eq", "(4u64, input) ne",
        "(4u64, input) lt", "(4u64, input) le", "(4u64, input) min", "(4u64, input) max",
        "(input, 4u64) sub", "(input, 0u64) rem", "(input, 0u64) div_u64",
        "input iota map (x -> ((x, 3u64) mul, 7u64) add) fold_add",
    ] {
        assert_eq!(run(src, Value::u64(xs.clone())), run_pair(src, Value::u64(xs.clone())), "{src}");
    }
    // a literal shared by two consumers, one of which can't take it as an immediate
    let src = "let c = 4u64 in ((input, c) add, (c, input) sub)";
    assert_eq!(run(src, Value::u64(xs.clone())), run_pair(src, Value::u64(xs)), "{src}");
}
