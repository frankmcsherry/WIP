//! The text bucket: byte-leaf interpretations, as `arith` is the numeric-leaf interpretation.
//! A string is `List<U8>` — the `"…"` literal already constructs one — so there is no string
//! type to add: the core never learns "text", and the typer sees only `List<U8>` in and shapes
//! out. Both ops are TOTAL over their input shape: `Split` has no failure case (n delimiters
//! make n+1 pieces), and `ParseU64` returns its failures as a committed `Sum` lane rather than
//! panicking on data — a malformed row is reachable by some well-typed program, so it must be
//! a value, not a crash.


use crate::value::{Prim, Tags, Value};

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TextOp {
    Split(u8), // List<U8> -> List<List<U8>>   split each row's bytes at the delimiter; adjacent
               // delimiters and bare ends yield empty pieces, the delimiter byte is dropped.
    ParseU64,  // List<U8> -> Sum{U64 | Unit}   parse each row as decimal: lane 0 (Ok) holds the
               // value; lane 1 (Err) counts the rows that are empty, non-digit, or overflowing.
}

/// decimal parse, total: `None` on empty, any non-digit byte, or u64 overflow. Hand-rolled
/// (vs `str::parse`) to reject the `+`/`-` prefixes std accepts — digits only.
fn parse_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(acc)
}

impl TextOp {
    pub(crate) fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            TextOp::Split(d) => {
                let (ends, vals) = input.into_list("Split")?;
                let bytes = vals.as_u8("Split bytes")?;
                // one pass over the flat buffer: non-delimiter bytes copy through, each delimiter
                // (and each row end) closes a piece, each row end closes the outer row.
                let mut out = Vec::with_capacity(bytes.len());
                let mut piece_ends = Vec::new();
                let mut outer_ends = Vec::with_capacity(ends.len());
                let mut start = 0;
                for end in ends.ends() {
                    for &b in &bytes[start..end] {
                        if b == *d {
                            piece_ends.push(out.len());
                        } else {
                            out.push(b);
                        }
                    }
                    piece_ends.push(out.len());
                    outer_ends.push(piece_ends.len());
                    start = end;
                }
                Value::List(outer_ends.into(), Box::new(Value::List(piece_ends.into(), Box::new(Value::u8(out)))))
            }
            TextOp::ParseU64 => {
                let (ends, vals) = input.into_list("ParseU64")?;
                let bytes = vals.as_u8("ParseU64 bytes")?;
                // the tags are written only once a row fails (every row before it parsed); while
                // none has, the assignment is the constant one and costs nothing per row.
                let mut tags: Option<Vec<u8>> = None;
                let mut oks = Vec::with_capacity(ends.len());
                let mut errs = 0usize;
                let mut start = 0;
                for end in ends.ends() {
                    match parse_u64(&bytes[start..end]) {
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
                        Tags::column(Prim::U8(crate::pool::leaf(tags)), off)
                    }
                };
                Value::sum_tagged(assignment, vec![Value::u64(oks), Value::Unit(errs)])
            }
        })
    }
}
