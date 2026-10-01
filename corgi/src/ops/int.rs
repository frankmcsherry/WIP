//! The integer op bucket: the ops that make, convert and compute with `Int` columns. Structure,
//! order and row movement need no ops of their own: `gather`, `sort`, `find`, comparisons, `hash`
//! and the codec already take an `Int` leaf wherever they take a `Prim` one.
//!
//! The arithmetic chooses each result's frame from its operands' frames before reading a row, so
//! its loops carry no overflow check; see [`crate::int`] and `dev/integers.md`.

use crate::int::{self, int_bin, Int, IntBin};
use crate::value::{Prim, Tags, Value};

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum IntOp {
    /// `(Int, Int) -> Int`: `add_int`, `sub_int`, `mul_int`
    Bin(IntBin),
    /// `Int -> Int`: the same values at the tightest frame and the narrowest width (`narrow`)
    Narrow,
    /// `U<w> -> Int`: a leaf's values as integers, read unsigned (`to_int`) or from the signed
    /// encoding (`to_int_signed`). The signed encoding needs no work: its stored bits are the
    /// offsets of the frame based at `-2^(w-1)`.
    FromBits { signed: bool },
    /// `Int -> U64`: back to 64-bit bits; an error for a value outside `0..2^64` (`to_u64`)
    ToU64,
    /// `List<Sum{A|B|..}> -> (List<Int>, List<A>, List<B>, ..)`: `unweave` handing back the sum's
    /// tags as an integer column, not widened (`unweave_int`)
    Unweave,
    /// `List<Int> -> Int`: each row's sum (`fold_add_int`). Exact; the result is narrowed.
    FoldAdd,
}

impl IntOp {
    pub(crate) fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            IntOp::Bin(op) => {
                let (a, b) = input.into_pair("integer arithmetic")?;
                let (a, b) = (a.into_int("integer arithmetic lhs")?, b.into_int("integer arithmetic rhs")?);
                Value::Int(int_bin(*op, a, b)?)
            }
            IntOp::Narrow => Value::Int(input.as_int("narrow")?.narrow()),
            IntOp::FromBits { signed } => {
                let p = input.into_prim(if *signed { "to_int_signed" } else { "to_int" })?;
                let bits = p.bits();
                let c = match &p {
                    Prim::U8(v) => int::from_unsigned(v),
                    Prim::U16(v) => int::from_unsigned(v),
                    Prim::U32(v) => int::from_unsigned(v),
                    Prim::U64(v) => int::from_unsigned(v),
                };
                Value::Int(if *signed { c.shifted(-(1i128 << (bits - 1))) } else { c })
            }
            IntOp::ToU64 => Value::u64(int::to_u64s(input.as_int("to_u64")?)?),
            IntOp::Unweave => unweave(input)?,
            IntOp::FoldAdd => {
                let (bounds, vals) = input.into_list("fold_add_int")?;
                let c = vals.into_int("fold_add_int values")?;
                let mut sums = Vec::with_capacity(bounds.len());
                let mut start = 0;
                for end in bounds.ends() {
                    // the offsets' sum, then the base once per element: no per-row decode
                    let off: u128 = (start..end).map(|i| c.off(i) as u128).sum();
                    sums.push(c.base() * (end - start) as i128 + off as i128);
                    start = end;
                }
                Value::Int(Int::from_i128s(&sums).map_err(|e| format!("fold_add_int: {e}"))?)
            }
        })
    }
}

/// `unweave` with the tag list as an integer column. A one-tag sum's tags are a constant column
/// (nothing stored); a column of `u8` discriminants becomes an 8-bit integer column — one byte per
/// row copied, where `unweave` writes eight. Each lane gains only bounds, as in `unweave`.
fn unweave(input: Value) -> Result<Value, String> {
    let (bounds, vals) = input.into_list("unweave_int")?;
    let (tags, lanes) = vals.into_sum("unweave_int")?;
    let arity = lanes.len();
    // per lane, the running count of its elements at each row's end. One row owns every element,
    // so its counts are the lane lengths.
    let lane_bounds: Vec<Vec<usize>> = if bounds.len() == 1 {
        lanes.iter().map(|l| vec![l.len()]).collect()
    } else {
        let mut lb = vec![Vec::with_capacity(bounds.len()); arity];
        let mut counts = vec![0usize; arity];
        let mut start = 0;
        for end in bounds.ends() {
            match &tags {
                Tags::Column(Prim::U8(ts), _) => ts[start..end].iter().for_each(|&t| counts[t as usize] += 1),
                _ => (start..end).for_each(|i| counts[tags.tag_at(i)] += 1),
            }
            for (l, &c) in lb.iter_mut().zip(&counts) {
                l.push(c);
            }
            start = end;
        }
        lb
    };
    let tag_col = match &tags {
        Tags::Const(t, rows) => Int::constant(*t as i128, *rows),
        Tags::Column(Prim::U8(ts), _) => int::from_u8_tags(ts, arity),
        Tags::Column(..) => {
            let vals: Vec<i128> = tags.tags_iter().map(|t| t as i128).collect();
            Int::from_i128s(&vals)?
        }
    };
    let mut out = vec![Value::List(bounds, Box::new(Value::Int(tag_col)))];
    for (lane, lb) in lanes.into_iter().zip(lane_bounds) {
        out.push(Value::List(lb.into(), Box::new(lane)));
    }
    Ok(Value::Prod(out))
}
