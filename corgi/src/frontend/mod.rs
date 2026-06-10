//! Front-end: the surface syntax that lowers to the core `Graph`. The core stays agnostic and many
//! surfaces could target it; the one kept is the ML-flavoured `ml` expression language, concatenative
//! by juxtaposition — a value followed by its operator stages with no separator (`input iota`), `let`
//! is graph sharing, and `x -> body` is a closed body. The op-name table below is the whole vocabulary
//! it reaches.

pub(crate) mod ml;
pub(crate) mod program;

pub use ml::parse_ml;
pub use program::Program;

use crate::ops::{ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred, TextOp};
use crate::value::Value;

/// a string literal as a `List<U8>` value (one list of its UTF-8 bytes). `"…"` lowers to `Op::Lit`
/// of this, broadcasting it to the input's length like any constant.
pub(crate) fn str_value(bytes: Vec<u8>) -> Value {
    Value::List(vec![bytes.len()], Box::new(Value::u8(bytes)))
}

/// which op idents take a trailing numeric argument — i.e. where a number follows the name.
/// (`branch` also takes one but is parsed specially: its count may be an enum name.)
pub(crate) fn takes_num(name: &str) -> bool {
    matches!(name, "field" | "gt" | "lit" | "add_u64" | "shr" | "and" | "cast")
}

/// which op idents are pair-eating binaries that accept an optional immediate: `x sub 1` is sugar
/// for `(x, x lit 1) sub`. (`and`/`shr`/`gt`/`add_u64` above are the core's immediate KERNELS and
/// always take their number; these desugar at the surface and the core sees the lit-pair graph.)
pub(crate) fn pair_imm(name: &str) -> bool {
    matches!(name, "add" | "sub" | "mul" | "eq" | "ne" | "lt" | "le")
}

/// the op-name -> `NumOp` table the front-end lowers through. `map` / `map_variant` are NOT here:
/// they carry sub-graphs and are built by the surface itself.
pub(crate) fn resolve(name: &str, arg: Option<u64>) -> Result<NumOp, String> {
    let n = || arg.ok_or_else(|| format!("op '{name}' needs a numeric argument"));
    Ok(match name {
        "field" => Op::Field(n()? as usize).into(),
        "gt" => CmpOp::Gt(n()?).into(), // column vs immediate (the threshold-filter sugar)
        "cast" => Op::Cast(n()? as u32).into(),
        "lit" => Op::Lit(Value::u64(vec![n()?])).into(),
        "transpose" => Op::Transpose.into(),
        "zip" => Op::Zip.into(),         // Transpose's inverse: parallel lists -> list of products
        "unweave" => Op::Unweave.into(), // sum column -> (tags, lane lists)
        "weave" => Op::Weave.into(),     // (tags, lane lists) -> sum column
        "cap_list" => Op::CapList.into(), // capture: pair a context with every list element
        "cap_sum" => Op::CapSum.into(),   // capture: distribute a context into every sum lane
        "branch" => Op::Branch(n()? as usize).into(), // N-way partition by a discriminant column;
                                                      // `branch 2` on a 0/1 mask is the boolean split
        "filter" => Op::Filter.into(),
        "sort" => CmpOp::SortList.into(),
        "dedup" => CmpOp::DedupList.into(),
        "group" => CmpOp::GroupKey.into(),
        "find" => CmpOp::Find.into(),
        "slices" => Op::Slices.into(),
        "gather" => Op::Gather.into(), // row-relative point gather (indices, haystack)
        "flatten" => Op::Flatten.into(),
        "enlist" => Op::Enlist.into(),
        "head" => Op::Head.into(), // each row's first element (the stratum drop; empty row panics)
        "iota" => Op::Iota.into(),
        "unwrap" => Op::Unwrap.into(),
        // relational compares: two equal-width leaf columns -> 0/1 mask. `gt`/`ge` are these with
        // the operands swapped, and the column-vs-constant `gt N` is the separate sugar above.
        "eq" => CmpOp::Rel(Pred::Eq).into(),
        "ne" => CmpOp::Rel(Pred::Ne).into(),
        "lt" => CmpOp::Rel(Pred::Lt).into(),
        "le" => CmpOp::Rel(Pred::Le).into(),
        // numeric layer — the kind-blind front-end reaches the u64-unsigned row of the grid:
        "add" => ArithOp::Bin(BinOp::Add, Kind::U, 64).into(),
        "sub" => ArithOp::Bin(BinOp::Sub, Kind::U, 64).into(),
        "mul" => ArithOp::Bin(BinOp::Mul, Kind::U, 64).into(),
        "neg" => ArithOp::Neg(Kind::U, 64).into(),
        "add_u64" => ArithOp::AddU64(n()?).into(),
        "shr" => ArithOp::Shr(n()? as u32).into(), // x >> k  (divide by 2^k)
        "and" => ArithOp::And(n()?).into(),         // x & m   (mod 2^k via m = 2^k-1)
        "reduce_sum" => ArithOp::ReduceSum.into(),
        // text: the surface passes split's delimiter as a byte (parsed from a one-byte string).
        "split" => TextOp::Split(n()? as u8).into(),
        "parse_u64" => TextOp::ParseU64.into(),
        other => return Err(format!("unknown op '{other}'")),
    })
}
