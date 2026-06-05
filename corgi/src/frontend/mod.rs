//! Front-end: the surface syntax that lowers to the core `Graph`. The core stays agnostic and many
//! surfaces could target it; the one kept is the ML-flavoured `ml` expression language, concatenative
//! by juxtaposition — a value followed by its operator stages with no separator (`input iota`), `let`
//! is graph sharing, and `x -> body` is a closed body. The op-name table below is the whole vocabulary
//! it reaches.

pub(crate) mod ml;
pub(crate) mod program;

pub use ml::parse_ml;
pub use program::Program;

use crate::ops::{ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred};
use crate::value::Value;

/// a string literal as a `List<U8>` value (one list of its UTF-8 bytes). `"…"` lowers to `Op::Lit`
/// of this, broadcasting it to the input's length like any constant.
pub(crate) fn str_value(bytes: Vec<u8>) -> Value {
    Value::List(vec![bytes.len()], Box::new(Value::u8(bytes)))
}

/// which op idents take a trailing numeric argument — i.e. where a number follows the name.
pub(crate) fn takes_num(name: &str) -> bool {
    matches!(name, "field" | "gt" | "lit" | "add_u64" | "shr" | "and" | "cast" | "branch")
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
        "broadcast" => Op::Broadcast.into(),
        "partition" => Op::Partition.into(),
        "branch" => Op::Branch(n()? as usize).into(), // N-way partition by a discriminant column
        "filter" => Op::Filter.into(),
        "sort" => CmpOp::SortList.into(),
        "dedup" => CmpOp::DedupList.into(),
        "group" => CmpOp::GroupKey.into(),
        "find" => CmpOp::Find.into(),
        "slices" => Op::Slices.into(),
        "flatten" => Op::Flatten.into(),
        "enlist" => Op::Enlist.into(),
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
        other => return Err(format!("unknown op '{other}'")),
    })
}
