//! The text bucket: byte-leaf interpretations, as `arith` is the numeric-leaf interpretation.
//! A string is `List<U8>` — the `"…"` literal already constructs one — so there is no string
//! type to add: the core never learns "text", and the typer sees only `List<U8>` in and shapes
//! out. Both ops are TOTAL over their input shape: `Split` has no failure case (n delimiters
//! make n+1 pieces), and `ParseU64` returns its failures as a committed `Sum` lane rather than
//! panicking on data — a malformed row is reachable by some well-typed program, so it must be
//! a value, not a crash.

use crate::shape::Shape;
use crate::value::Value;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TextOp {
    Split(u8), // List<U8> -> List<List<U8>>   split each row's bytes at the delimiter; adjacent
               // delimiters and bare ends yield empty pieces, the delimiter byte is dropped.
    ParseU64,  // List<U8> -> Sum{List<U8> | U64}   parse each row as decimal: lane 1 (Ok) holds
               // the value, lane 0 (Err) the original bytes — empty, non-digit, or overflowing.
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
    pub(crate) fn eval(&self, input: Value) -> Value {
        match self {
            TextOp::Split(d) => {
                let (ends, vals) = input.into_list("Split");
                let bytes = vals.into_u8("Split bytes");
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
                let (ends, vals) = input.into_list("ParseU64");
                let bytes = vals.into_u8("ParseU64 bytes");
                let mut tags = Vec::with_capacity(ends.len());
                let mut oks = Vec::new();
                let (mut err_ends, mut err_bytes) = (Vec::new(), Vec::new());
                let mut start = 0;
                for end in ends.ends() {
                    let row = &bytes[start..end];
                    match parse_u64(row) {
                        Some(v) => {
                            tags.push(1);
                            oks.push(v);
                        }
                        None => {
                            tags.push(0);
                            err_bytes.extend_from_slice(row);
                            err_ends.push(err_bytes.len());
                        }
                    }
                    start = end;
                }
                Value::sum(
                    tags,
                    vec![Value::List(err_ends.into(), Box::new(Value::u8(err_bytes))), Value::u64(oks)],
                )
            }
        }
    }

    pub(crate) fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        if !matches!(input, List(t) if **t == Prim(8)) {
            return Err(format!("text op expects List<U8>, got {input}"));
        }
        Ok(match self {
            TextOp::Split(_) => List(Box::new(input.clone())),
            TextOp::ParseU64 => Sum(vec![Some(input.clone()), Some(Prim(64))]),
        })
    }
}
