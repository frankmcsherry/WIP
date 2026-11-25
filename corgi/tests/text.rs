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

/// every byte value, a string each: the reference is the standard library's ASCII case.
fn every_byte() -> (Vec<u8>, Value) {
    let bytes: Vec<u8> = (0..=255).collect();
    let rows = Value::List((1..=256).collect::<Vec<usize>>().into(), Box::new(Value::u8(bytes.clone())));
    (bytes, rows)
}

#[test]
fn case_moves_ascii_letters_only() {
    let (bytes, rows) = every_byte();
    for (word, want) in [("lower", bytes.to_ascii_lowercase()), ("upper", bytes.to_ascii_uppercase())] {
        let p = Program::compile_ml(&format!("input {word}")).unwrap();
        let (bounds, out) = p.run(rows.clone()).into_list(word).unwrap();
        assert_eq!(out, Value::u8(want), "{word} of every byte");
        assert_eq!(bounds.to_vec(), (1..=256).collect::<Vec<usize>>(), "{word} keeps the rows");
    }
    // rows of every length, some empty
    let arg = strings(&["", "Hello, World", "", "@AZ[`az{", "42"]);
    assert_eq!(run("input lower", &arg), show(&strings(&["", "hello, world", "", "@az[`az{", "42"])));
    assert_eq!(run("input upper", &arg), show(&strings(&["", "HELLO, WORLD", "", "@AZ[`AZ{", "42"])));
}

/// A column nothing else holds is rewritten in place, and one that is shared is copied: the answer
/// is the same, and the shared column is unchanged.
#[test]
fn case_leaves_a_shared_column_alone() {
    let p = Program::compile_ml("input upper").unwrap();
    let kept = strings(&["shared", "Column"]);
    let from_shared = p.run(kept.clone());
    assert_eq!(kept, strings(&["shared", "Column"]));
    assert_eq!(p.run(strings(&["shared", "Column"])), from_shared);
    assert_eq!(from_shared, strings(&["SHARED", "COLUMN"]));
}

/// Integers held wider than bytes follow the same rule, whatever else is among them.
#[test]
fn case_reads_integers_at_any_storage() {
    let wide = |xs: &[i64]| Value::List(vec![xs.len()].into(), Box::new(Value::i64(xs.to_vec())));
    let arg = wide(&[65, 90, 91, 97, 122, 123, -65, 1000, 65 + 256]);
    assert_eq!(Program::compile_ml("input lower").unwrap().run(arg.clone()), wide(&[97, 122, 91, 97, 122, 123, -65, 1000, 65 + 256]));
    assert_eq!(Program::compile_ml("input upper").unwrap().run(arg), wide(&[65, 90, 91, 65, 90, 123, -65, 1000, 65 + 256]));
}

/// The words apply to strings wherever they sit, and to nothing else.
#[test]
fn case_is_typed_as_text() {
    let arg = strings(&["Ab Cd", "eF"]);
    assert_eq!(run("input split \" \" map (w -> w upper)", &arg), run("input upper split \" \"", &arg));
    assert!(Program::compile_ml("input lower").unwrap().shape(&Shape::Int).is_err(), "an Int is not a string");
    // one kernel per word, so two programs' uses are the same op
    assert_eq!(corgi::parse_ml("input lower").unwrap(), corgi::parse_ml("input lower").unwrap());
    assert_ne!(corgi::parse_ml("input lower").unwrap(), corgi::parse_ml("input upper").unwrap());
}
