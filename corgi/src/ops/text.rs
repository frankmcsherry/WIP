//! The text bucket. A string is a list of integers, its bytes (held as bytes) — the `"…"` literal
//! already constructs one — so there is no string type to add: the core never learns "text", and
//! the typer sees only `List<Int>` in and shapes out. Both ops are TOTAL over their input shape:
//! `Split` has no failure case (n delimiters make n+1 pieces), and `ParseInt` returns its failures
//! as a committed `Sum` lane rather than panicking on data — a malformed row is reachable by some
//! well-typed program, so it must be a value, not a crash.


use crate::value::{Bounds, Prim, Scalar, Tags, Value};
use std::sync::Arc;

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
                // the pieces keep the text's storage, widened first if it can't hold the delimiter
                let d = Scalar::Int(*d as i64);
                let p = vals.into_prim("Split bytes")?;
                let p = p.to_hold(d).unwrap_or(p);
                macro_rules! go { ($($s:ident),*) => { match &p {
                    $( Prim::$s(xs) => { let (o, pe, oe) = split(&ends, xs, d.elem()); (Prim::$s(Arc::new(o)), pe, oe) } )*
                    Prim::F64(_) => return Err("Split bytes: expected Int, got Float".into()),
                } } }
                let (out, piece_ends, outer_ends) = go!(U8, I8, I16, I32, I64);
                Value::List(outer_ends.into(), Box::new(Value::List(piece_ends.into(), Box::new(Value::Prim(out)))))
            }
            TextOp::ParseInt => {
                let (ends, vals) = input.into_list("ParseInt")?;
                let xs = vals.as_i64("ParseInt bytes")?;
                // the tags are written only once a row fails (every row before it parsed); while
                // none has, the assignment is the constant one and costs nothing per row.
                let mut tags: Option<Vec<u8>> = None;
                let mut oks = Vec::with_capacity(ends.len());
                let mut errs = 0usize;
                let mut start = 0;
                for end in ends.ends() {
                    match parse_int(xs[start..end].iter().copied()) {
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
        })
    }
}
