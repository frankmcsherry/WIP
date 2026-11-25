//! Text bucket tests: the awkward rows the corpus programs don't dwell on — empty rows,
//! adjacent delimiters, overflow — since `Split` and `ParseInt` claim totality over every
//! well-typed input, not over the happy corpus.

use corgi::{shape_of_value, show, Program, Shape, Value};

fn run(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
    let out = p.run(arg.clone());
    let inferred = p.shape(&shape_of_value(arg)).expect("type error");
    assert_eq!(inferred, shape_of_value(&out), "typer disagrees with evaluator");
    show(&out)
}

/// a column of strings: one row of bytes per `&str`.
fn strings(rows: &[&str]) -> Value {
    let mut ends = Vec::new();
    let mut bytes = Vec::new();
    for r in rows {
        bytes.extend_from_slice(r.as_bytes());
        ends.push(bytes.len());
    }
    Value::List(ends.into(), Box::new(Value::u8(bytes)))
}

#[test]
fn split_keeps_empty_pieces() {
    // n delimiters -> n+1 pieces: adjacent delimiters and bare ends yield empty pieces, and an
    // empty row is one empty piece — never zero.
    let arg = strings(&["a,,b", "", ",x,"]);
    assert_eq!(
        run("input split \",\"", &arg),
        "List ends=[3, 4, 7] <List ends=[1, 1, 2, 2, 2, 3, 3] <[97, 98, 120]>>"
    );
}

#[test]
fn parse_int_is_total() {
    // malformed rows (empty, non-digit, a sign) land in the Err lane, which counts them; i64::MAX
    // is well-formed. Nothing panics.
    let arg = strings(&["42", "", "9x", "-7", "9223372036854775807"]);
    assert_eq!(
        run("input parse_int", &arg),
        "Sum tags=[0, 1, 1, 1, 0] [[42, 9223372036854775807], ()x3]"
    );
}

#[test]
fn parse_int_overflow_is_err_not_wrap() {
    // i64::MAX + 1: the checked accumulate rejects it — an Err row, not a wrapped value.
    let arg = strings(&["9223372036854775808"]);
    let out = run("input parse_int", &arg);
    assert!(out.starts_with("Sum tags=[1]"), "expected an Err row, got {out}");
}

#[test]
fn text_ops_reject_non_strings() {
    // the judge wants List<Int>; a bare Int column is a located type error, not an eval panic.
    let p = Program::compile_ml("input parse_int").unwrap();
    assert!(p.shape(&Shape::Int).is_err());
    let p = Program::compile_ml("input split \",\"").unwrap();
    assert!(p.shape(&Shape::Int).is_err());
    // and the delimiter must be one byte.
    assert!(Program::compile_ml("input split \",,\"").is_err());
}

/// `split` against its definition, a byte at a time: rows of every length around the kernel's
/// word and short-row edges, delimiters from none to every byte, and text held as bytes and wide.
#[test]
fn split_matches_its_definition() {
    let mut state = 0x5eedu64;
    let mut below = |n: u64| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) % n
    };
    for one_in in [0u64, 1, 2, 6, 40, 80, 200, 2000] {
        for max in [0u64, 1, 3, 8, 15, 16, 17, 40, 300] {
            let (mut ends, mut bytes) = (Vec::new(), Vec::new());
            // the pieces, kept bytes and row ends, built as the definition says
            let (mut outer, mut inner, mut kept) = (Vec::new(), Vec::new(), Vec::new());
            for _ in 0..200 {
                for _ in 0..below(max + 1) {
                    let b = if one_in != 0 && below(one_in) == 0 { b',' } else { b'a' + below(26) as u8 };
                    bytes.push(b);
                    if b == b',' { inner.push(kept.len()) } else { kept.push(b) }
                }
                ends.push(bytes.len());
                inner.push(kept.len());
                outer.push(inner.len());
            }
            let split = Program::compile_ml("input split \",\"").unwrap();
            let wide = |xs: &[u8]| Value::i64(xs.iter().map(|&b| b as i64).collect());
            for (storage, text, want) in [("bytes", Value::u8(bytes.clone()), Value::u8(kept.clone())), ("wide", wide(&bytes), wide(&kept))] {
                let input = Value::List(ends.clone().into(), Box::new(text));
                let want = Value::List(outer.clone().into(), Box::new(Value::List(inner.clone().into(), Box::new(want))));
                assert_eq!(split.run(input), want, "rows of up to {max}, one in {one_in}, text as {storage}");
            }
        }
    }
}
