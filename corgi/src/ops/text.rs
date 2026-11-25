//! The text bucket. A string is a list of integers, its bytes (held as bytes) — the `"…"` literal
//! already constructs one — so there is no string type to add: the core never learns "text", and
//! the typer sees only `List<Int>` in and shapes out. Both ops are TOTAL over their input shape:
//! `Split` has no failure case (n delimiters make n+1 pieces), and `ParseInt` returns its failures
//! as a committed `Sum` lane rather than panicking on data — a malformed row is reachable by some
//! well-typed program, so it must be a value, not a crash.


use crate::ops::host::{HostKernel, HostOp};
use crate::shape::Shape;
use crate::value::{Bounds, Prim, Tags, Value};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TextOp {
    Split(u8), // List<Int> -> List<List<Int>>   split each row's bytes at the delimiter; adjacent
               // delimiters and bare ends yield empty pieces, the delimiter byte is dropped.
    ParseInt,  // List<Int> -> Sum{Int | Unit}   parse each row as decimal: lane 0 (Ok) holds the
               // value; lane 1 (Err) counts the rows that are empty, non-digit, or past `i64`.
}

/// decimal parse, total: `None` on empty, any non-digit, or a value past `i64::MAX`. Hand-rolled
/// (vs `str::parse`) to reject the `+`/`-` prefixes std accepts — digits only. Reads integers, so
/// text held at any storage parses.
fn parse_int(digits: impl Iterator<Item = i64>) -> Option<i64> {
    let mut acc: i64 = 0;
    let mut any = false;
    for d in digits {
        if !(b'0' as i64..=b'9' as i64).contains(&d) {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(d - b'0' as i64)?;
        any = true;
    }
    any.then_some(acc)
}

/// [`TextOp::ParseInt`] over one storage: each row's digits, or the row in lane 1.
fn parse_rows<T: Copy + Into<i64>>(ends: &Bounds, xs: &[T]) -> Value {
    // the tags are written only once a row fails (every row before it parsed); while
    // none has, the assignment is the constant one and costs nothing per row.
    let mut tags: Option<Vec<u8>> = None;
    let mut oks = Vec::with_capacity(ends.len());
    let mut errs = 0usize;
    let mut start = 0;
    for end in ends.ends() {
        match parse_int(xs[start..end].iter().map(|&x| x.into())) {
            Some(v) => {
                if let Some(t) = &mut tags {
                    t.push(0);
                }
                oks.push(v);
            }
            None => {
                tags.get_or_insert_with(|| vec![0; oks.len()]).push(1);
                errs += 1;
            }
        }
        start = end;
    }
    let rows = oks.len() + errs;
    let assignment = match tags {
        None if rows > 0 => Tags::Const(0, rows),
        tags => {
            let tags = tags.unwrap_or_default();
            let mut count = [0usize; 2];
            let off = tags.iter().map(|&t| { let p = count[t as usize]; count[t as usize] += 1; p }).collect();
            Tags::column(Prim::U8(Arc::new(tags)), off)
        }
    };
    Value::sum_tagged(assignment, vec![Value::i64(oks), Value::Unit(errs)])
}

/// [`TextOp::Split`] over one storage: `d` is the delimiter at that storage.
fn split<T: Copy + PartialEq>(ends: &Bounds, bytes: &[T], d: T) -> (Vec<T>, Vec<usize>, Vec<usize>) {
    // one pass over the flat buffer: non-delimiter bytes copy through, each delimiter (and each
    // row end) closes a piece, each row end closes the outer row.
    let mut out = Vec::with_capacity(bytes.len());
    let mut piece_ends = Vec::new();
    let mut outer_ends = Vec::with_capacity(ends.len());
    let mut start = 0;
    for end in ends.ends() {
        for &b in &bytes[start..end] {
            if b == d {
                piece_ends.push(out.len());
            } else {
                out.push(b);
            }
        }
        piece_ends.push(out.len());
        outer_ends.push(piece_ends.len());
        start = end;
    }
    (out, piece_ends, outer_ends)
}

impl TextOp {
    pub(crate) fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            TextOp::Split(d) => {
                let (ends, vals) = input.into_list("Split")?;
                let (out, piece_ends, outer_ends) = match &vals {
                    Value::Prim(Prim::U8(bytes)) => {
                        let (o, p, e) = split(&ends, bytes, *d);
                        (Value::u8(o), p, e)
                    }
                    _ => {
                        let xs = vals.as_i64("Split bytes")?;
                        let (o, p, e) = split(&ends, &xs, *d as i64);
                        (Value::i64(o), p, e)
                    }
                };
                Value::List(outer_ends.into(), Box::new(Value::List(piece_ends.into(), Box::new(out))))
            }
            TextOp::ParseInt => {
                let (ends, vals) = input.into_list("ParseInt")?;
                // text is bytes, read where they are; any other Int column is read as `i64`s
                match &vals {
                    Value::Prim(Prim::U8(bytes)) => parse_rows(&ends, bytes),
                    _ => parse_rows(&ends, &vals.as_i64("ParseInt bytes")?),
                }
            }
        })
    }
}

/// `lower` and `upper`: each ASCII letter of a string to that case, every other integer left as it
/// is. `List<Int> -> List<Int>`, the row bounds kept.
///
/// These are host kernels, not `TextOp` variants: a word that is one loop over a column needs no
/// arm in the typer, the optimizer or `eval`, and the next such word adds a kernel here and a name
/// in the front-end's table. One kernel per word is shared by every program, since a host op is
/// equal only to itself.
struct Case {
    name: &'static str,
    upper: bool,
    text: Shape,
}

impl HostKernel for Case {
    fn name(&self) -> &str { self.name }
    fn input(&self) -> &Shape { &self.text }
    fn output(&self) -> &Shape { &self.text }
    fn eval(&self, input: Value) -> Result<Value, String> {
        let (bounds, vals) = input.into_list(self.name)?;
        let out = match vals {
            // text is bytes: rewritten where it lies when nothing else holds it, else in one pass
            // into a new column
            Value::Prim(Prim::U8(bytes)) => Value::u8(match (Arc::try_unwrap(bytes), self.upper) {
                (Ok(mut own), true) => { own.make_ascii_uppercase(); own }
                (Ok(mut own), false) => { own.make_ascii_lowercase(); own }
                (Err(shared), true) => shared.to_ascii_uppercase(),
                (Err(shared), false) => shared.to_ascii_lowercase(),
            }),
            // integers held wider: the letters among them move, by the same rule
            other => {
                let (from, by) = if self.upper { (b'a' as i64, -32) } else { (b'A' as i64, 32) };
                Value::int(other.as_i64(self.name)?.iter().map(|&x| if (from..from + 26).contains(&x) { x + by } else { x }))
            }
        };
        Ok(Value::List(bounds, Box::new(out)))
    }
}

/// The host op behind the word `lower` (`upper` when `upper`).
pub(crate) fn case(upper: bool) -> HostOp {
    static WORDS: OnceLock<[Arc<dyn HostKernel>; 2]> = OnceLock::new();
    let words = WORDS.get_or_init(|| {
        let word = |name, upper| -> Arc<dyn HostKernel> { Arc::new(Case { name, upper, text: Shape::List(Box::new(Shape::Int)) }) };
        [word("lower", false), word("upper", true)]
    });
    HostOp(words[upper as usize].clone())
}
