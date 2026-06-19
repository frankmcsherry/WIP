//! Front-end: the surface syntax that lowers to the core `Graph`. The core stays agnostic and many
//! surfaces could target it; the one kept is the ML-flavoured `ml` expression language, concatenative
//! by juxtaposition — a value followed by its operator stages with no separator (`input iota`), `let`
//! is graph sharing, and `x -> body` is a closed body. The op-name table below is the whole vocabulary
//! it reaches.

pub(crate) mod ml;
pub(crate) mod program;

pub use ml::parse_ml;
pub use program::Program;

use crate::ops::{ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred, Red, TextOp};
use crate::value::Value;

/// a string literal as a `List<U8>` value (one list of its UTF-8 bytes). `"…"` lowers to `Op::Lit`
/// of this, broadcasting it to the input's length like any constant.
pub(crate) fn str_value(bytes: Vec<u8>) -> Value {
    Value::List(vec![bytes.len()].into(), Box::new(Value::u8(bytes)))
}

/// which op idents take a trailing numeric argument — i.e. where a number follows the name.
/// (`branch` also takes one but is parsed specially: its count may be an enum name.)
pub(crate) fn takes_num(name: &str) -> bool {
    matches!(name, "field" | "gt" | "lit" | "add_u64" | "shr" | "and" | "cast" | "chunk")
        || name.starts_with("lit_") // typed literals `lit_<k><w> N`
}

/// parse a `<kind><width>` suffix like `i32` / `u8` / `f64` into `(Kind, width)`, validating the
/// width (and that floats are only 32/64). The basis for the typed-arithmetic surface (`add_i32`, …).
fn parse_kw(suf: &str) -> Option<(Kind, u32)> {
    let kind = match suf.as_bytes().first()? {
        b'u' => Kind::U,
        b'i' => Kind::I,
        b'f' => Kind::F,
        _ => return None,
    };
    let width: u32 = suf.get(1..)?.parse().ok()?;
    let ok = match kind {
        Kind::F => matches!(width, 32 | 64), // no f8/f16 (no native type); a kind that projects onto 32/64
        _ => matches!(width, 8 | 16 | 32 | 64),
    };
    ok.then_some((kind, width))
}

/// the typed-arithmetic surface: `<op>_<k><w>` (`add_i32`, `div_f64`, `neg_u8`, …) and `lit_<k><w> N`,
/// surfacing the (op × kind × width) grid. Returns `None` for a name that isn't a typed form, so
/// `resolve` falls through to its fixed table. Width/kind validity is enforced by `parse_kw`.
fn typed_arith(name: &str, arg: Option<u64>) -> Option<NumOp> {
    if let Some(suf) = name.strip_prefix("lit_") {
        let (k, w) = parse_kw(suf)?;
        // no float literal token: `lit_f32 3` would store the raw bits 3, not 3.0 (`lit_value` only
        // encodes integers). Reject `F` so it's an unknown op, not a silent NaN; the float path is
        // `lit_uN K to_fN` (a literal integer, then the documented encode).
        if matches!(k, Kind::F) {
            return None;
        }
        return Some(Op::Lit(crate::ops::lit_value(k, w, arg?)).into());
    }
    let (base, suf) = name.rsplit_once('_')?;
    let (k, w) = parse_kw(suf)?;
    let bin = |op| Some(ArithOp::Bin(op, k, w).into());
    match base {
        "add" => bin(BinOp::Add),
        "sub" => bin(BinOp::Sub),
        "mul" => bin(BinOp::Mul),
        "div" => bin(BinOp::Div),
        "neg" => Some(ArithOp::Neg(k, w).into()),
        // min/max take no kind/width suffix — they're kind-blind and width-inferred (`min`/`max`).
        _ => None,
    }
}

/// which op idents are pair-eating binaries that accept an optional immediate: `x sub 1` is sugar
/// for `(x, x lit 1) sub`. (`and`/`shr`/`gt`/`add_u64` above are the core's immediate KERNELS and
/// always take their number; these desugar at the surface and the core sees the lit-pair graph.)
pub(crate) fn pair_imm(name: &str) -> bool {
    matches!(name, "add" | "sub" | "mul" | "min" | "max" | "eq" | "ne" | "lt" | "le")
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
        // One name per fallible method — each routes through the effect layer as a FailOp; the old
        // `_try`/`_uns` tier split is retired (totality is `is_total`, not a name). The lone survivor,
        // `gather_try`, is kept only as the DISTINCT per-element gather (below), never as an alias.
        "zip" => Op::TryZip.into(),      // the zip FailOp: per-row Sum{Err | Ok} (length agreement)
        "unweave" => Op::Unweave.into(), // sum column -> (tags, lane lists)
        // NOTE: `weave` (Unweave's inverse) is intentionally NOT on the surface. Unlike the other
        // iso-inverses (Zip pairs any two columns; Slices materializes any ranges, incl. Find's),
        // Weave's input — a tag stream whose per-row counts match a set of lane lengths — arises ONLY
        // from Unweave; a free-standing Weave is either provably the Unweave-inverse or a bug, and its
        // precondition is a histogram relation no static analysis cheaply proves. So `Op::Weave` stays
        // a KERNEL op (the optimizer's `Weave(Unweave x)=x` round-trip; tested at Builder level), never
        // a verb. (A `try_weave` would only carry an unactionable "inconsistent columns" error.)
        "cap_list" => Op::CapList.into(), // capture: pair a context with every list element
        "cap_sum" => Op::CapSum.into(),   // capture: distribute a context into every sum lane
        "branch" => Op::TryBranch(n()? as usize).into(), // the branch FailOp: tag>=n -> Oob (err-mask),
                                                         // else the demux Sum{X×n}; `branch 2` is the split
        "filter" => Op::Filter.into(),
        "sort" => CmpOp::SortList.into(),
        "dedup" => CmpOp::DedupList.into(),
        "group" => CmpOp::GroupKey.into(),
        "find" => CmpOp::Find.into(),
        // point access — `get` (scalar, one index per row) and `gather` (vector, a list of indices) are
        // the per-row FailOps: an out-of-range index folds into the err-mask. `gather_try` is the DISTINCT
        // per-element gather (`List<Sum{Oob | Found}>`), reifying each element's miss as DATA rather than a
        // per-row effect — kept as its own verb. `head` is sugar for `get 0` (see ml.rs): an empty row is
        // `Oob`, so a total head needs no separate non-emptiness proof.
        "slices" => Op::Slices.into(),        // the slices FailOp: per-row range-in-bounds check
        "get" => Op::GetTry.into(),           // the get FailOp: (idx, haystack) -> per-row Found/Oob
        "gather" => Op::Gather.into(),        // the gather FailOp: per-row all-or-nothing
        "gather_try" => Op::GatherTry.into(), // DISTINCT per-element gather: List<Sum{Oob | Found}>
        "try" => Op::Try.into(), // handle a fallible stage: reveal its Fail as a pure Sum{T | Unit}
        "flatten" => Op::Flatten.into(),
        "enlist" => Op::Enlist.into(),
        "append" => Op::Append.into(), // (List<X>, List<X>) -> List<X>  row-wise concat (the list-monoid ⊕)
        "len" => Op::Len.into(),       // List<X> -> U64  per-row element count, read off the bounds
        "chunk" => Op::Chunk(n()? as usize).into(), // List<X> -> List<List<X>>  fixed k-wide records (Stride producer)
        "unit" => Op::Unit.into(), // X -> Unit (the None of Option = Sum{Unit | T})
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
        "min" => CmpOp::Min.into(), // kind-blind lane min/max — order ops, in `cmp` not the arith grid
        "max" => CmpOp::Max.into(),
        "neg" => ArithOp::Neg(Kind::U, 64).into(),
        // the typed grid (signed/float/narrow) is reached by suffix: `add_i32`, `div_f64`, `lit_u8 N`,
        // … — see `typed_arith`. Plus the two kind conversions:
        "signed" => ArithOp::ToSigned.into(), // unsigned <-> signed encoding (XOR sign bit; involution)
        "to_f32" => ArithOp::ToFloat(32).into(), // unsigned int -> f32 (how iota becomes floats)
        "to_f64" => ArithOp::ToFloat(64).into(),
        // branchless blend: (mask, then, else) -> picked column (the SIMD bitselect, see Op::Select)
        "select" => Op::Select.into(),
        "add_u64" => ArithOp::AddU64(n()?).into(),
        "shr" => ArithOp::Shr(n()? as u32).into(), // x >> k  (divide by 2^k)
        "and" => ArithOp::And(n()?).into(),         // x & m   (mod 2^k via m = 2^k-1)
        // named monoid reductions, each `fold_<binop>` (fold_add = sum, fold_mul = product):
        "fold_add" => ArithOp::Reduce(Red::Add).into(),
        "fold_mul" => ArithOp::Reduce(Red::Mul).into(),
        "fold_min" => ArithOp::Reduce(Red::Min).into(),
        "fold_max" => ArithOp::Reduce(Red::Max).into(),
        "fold_all" => ArithOp::Reduce(Red::All).into(), // 1 iff every element nonzero (mask AND)
        "fold_any" => ArithOp::Reduce(Red::Any).into(), // 1 iff any element nonzero (mask OR)
        // the inclusive-prefix monoid scans — `scan_<binop>`, the one-pass fast paths for `scan` with a
        // monoid body (the `fold_*` siblings that KEEP each prefix instead of dropping to the total).
        "scan_add" => ArithOp::Scan(Red::Add).into(),
        "scan_mul" => ArithOp::Scan(Red::Mul).into(),
        "scan_min" => ArithOp::Scan(Red::Min).into(),
        "scan_max" => ArithOp::Scan(Red::Max).into(), // the running maximum
        "scan_all" => ArithOp::Scan(Red::All).into(),
        "scan_any" => ArithOp::Scan(Red::Any).into(),
        // text: the surface passes split's delimiter as a byte (parsed from a one-byte string).
        "split" => TextOp::Split(n()? as u8).into(),
        "parse_u64" => TextOp::ParseU64.into(),
        // the typed-arithmetic grid by suffix (`add_i32`, `mul_f64`, `lit_u8 N`, …); falls through to
        // an error only if it's neither a fixed op above nor a well-formed typed form.
        other => return typed_arith(other, arg).ok_or_else(|| format!("unknown op '{other}'")),
    })
}
