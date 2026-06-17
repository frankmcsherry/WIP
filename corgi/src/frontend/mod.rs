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
    Value::List(vec![bytes.len()], Box::new(Value::u8(bytes)))
}

/// which op idents take a trailing numeric argument — i.e. where a number follows the name.
/// (`branch` also takes one but is parsed specially: its count may be an enum name.)
pub(crate) fn takes_num(name: &str) -> bool {
    matches!(name, "field" | "gt" | "lit" | "add_u64" | "shr" | "and" | "cast" | "branch_try")
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
        return Some(Op::Lit(crate::ops::lit_value(k, w, arg?)).into());
    }
    let (base, suf) = name.rsplit_once('_')?;
    let (k, w) = parse_kw(suf)?;
    let bin = |op| Some(ArithOp::Bin(op, k, w).into());
    match base {
        "add" => bin(BinOp::Add),
        "sub" => bin(BinOp::Sub),
        "mul" => bin(BinOp::Mul),
        "min" => bin(BinOp::Min),
        "max" => bin(BinOp::Max),
        "div" => bin(BinOp::Div),
        "neg" => Some(ArithOp::Neg(k, w).into()),
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
        // the three tiers — `foo` (checked: a static pass gates it), `foo_try` (total: an error lane),
        // `foo_uns` (unchecked: asserts the precondition, may panic — the kernel tier `check_total` flags).
        "zip" => Op::Zip.into(),         // checked by the length pass (columns share bounds)
        "zip_try" => Op::TryZip.into(),  // total Zip (pair): per-row Sum{Err | Ok}
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
        "branch" => Op::Branch(n()? as usize).into(), // checked by the range pass (tags < n);
                                                      // `branch 2` on a 0/1 mask is the boolean split
        "branch_try" => Op::TryBranch(n()? as usize).into(), // total Branch: tag>=n -> Oob:U64 lane 0
        "filter" => Op::Filter.into(),
        "sort" => CmpOp::SortList.into(),
        "dedup" => CmpOp::DedupList.into(),
        "group" => CmpOp::GroupKey.into(),
        "find" => CmpOp::Find.into(),
        // the UNCHECKED kernel tier — data-dependent bounds no static pass proves; `check_total` flags
        // these as outside the guaranteed-total subset. `index` (total) is gather's safe form; `index 0`
        // is head's; a total `slices_try` (range-index) is the future safe form for `slices_uns`.
        "slices_uns" => Op::Slices.into(), // (ranges, haystack) -> List<List<T>>  ranges asserted in-bounds
        "gather_uns" => Op::Gather.into(), // row-relative point gather; indices asserted in-bounds
        "index" => Op::Index.into(),   // total point index -> Sum{Oob:U64 | Found:T} (gather's safe form)
        "flatten" => Op::Flatten.into(),
        "enlist" => Op::Enlist.into(),
        "head_uns" => Op::Head.into(), // each row's first element; non-empty asserted (index 0 is safe)
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
        "min" => ArithOp::Bin(BinOp::Min, Kind::U, 64).into(),
        "max" => ArithOp::Bin(BinOp::Max, Kind::U, 64).into(),
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
        "reduce_sum" => ArithOp::Reduce(Red::Sum).into(),
        "reduce_prod" => ArithOp::Reduce(Red::Prod).into(),
        "reduce_min" => ArithOp::Reduce(Red::Min).into(),
        "reduce_max" => ArithOp::Reduce(Red::Max).into(),
        "reduce_all" => ArithOp::Reduce(Red::All).into(), // 1 iff every element nonzero (mask AND)
        "reduce_any" => ArithOp::Reduce(Red::Any).into(), // 1 iff any element nonzero (mask OR)
        // text: the surface passes split's delimiter as a byte (parsed from a one-byte string).
        "split" => TextOp::Split(n()? as u8).into(),
        "parse_u64" => TextOp::ParseU64.into(),
        // the typed-arithmetic grid by suffix (`add_i32`, `mul_f64`, `lit_u8 N`, …); falls through to
        // an error only if it's neither a fixed op above nor a well-formed typed form.
        other => return typed_arith(other, arg).ok_or_else(|| format!("unknown op '{other}'")),
    })
}
