//! Text bucket tests: the awkward rows the corpus programs don't dwell on — empty rows,
//! adjacent delimiters, overflow — since `Split` and `ParseU64` claim totality over every
//! well-typed input, not over the happy corpus.

use corgi::{shape_of_value, show, Program, Shape, Value};

fn run(src: &str, arg: &Value) -> String {
    let p = Program::compile_ml(src).expect("parse error");
    p.check();
    let out = p.run(arg.clone()).expect("shape error");
    let inferred = p.shape(&shape_of_value(arg)).expect("type error");
    assert_eq!(inferred, shape_of_value(&out), "typer disagrees with evaluator");
    show(&out)
}

/// a column of strings: one `List<U8>` row per `&str`.
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
fn parse_u64_is_total() {
    // malformed rows (empty, non-digit) land in the Err lane with their original bytes; u64::MAX
    // is well-formed. Nothing panics.
    let arg = strings(&["42", "", "9x", "18446744073709551615"]);
    assert_eq!(
        run("input parse_u64", &arg),
        "Sum tags=[1, 0, 0, 1] [List ends=[0, 2] <[57, 120]>, [42, 18446744073709551615]]"
    );
}

#[test]
fn parse_u64_overflow_is_err_not_wrap() {
    // u64::MAX + 1: the checked accumulate rejects it — an Err row, not a wrapped value.
    let arg = strings(&["18446744073709551616"]);
    let out = run("input parse_u64", &arg);
    assert!(out.starts_with("Sum tags=[0]"), "expected an Err row, got {out}");
}

#[test]
fn text_ops_reject_non_strings() {
    // the judge wants List<U8>; a bare U64 column is a located type error, not an eval panic.
    let p = Program::compile_ml("input parse_u64").unwrap();
    assert!(p.shape(&Shape::Prim(64)).is_err());
    let p = Program::compile_ml("input split \",\"").unwrap();
    assert!(p.shape(&Shape::Prim(64)).is_err());
    // and the delimiter must be one byte.
    assert!(Program::compile_ml("input split \",,\"").is_err());
}
