//! Front-end: the surface syntax that lowers to the core `Graph`. The core stays agnostic and many
//! surfaces could target it; the one kept is the ML-flavoured `ml` expression language, concatenative
//! by juxtaposition — a value followed by its operator stages with no separator (`input iota`), `let`
//! is graph sharing, and `x -> body` is a closed body. The op-name table below is the whole vocabulary
//! it reaches.

pub(crate) mod ml;
pub(crate) mod program;

pub use ml::parse_ml;
pub use program::Program;

use crate::ops::{ArithOp, BinOp, BitOp, CmpOp, NumOp, Op, Pred, Red, ShiftOp, TextOp};
use crate::value::Value;

/// a string literal as a `List<Int>` value (one list of its UTF-8 bytes, held as bytes). `"…"`
/// lowers to `Op::Lit` of this, broadcasting it to the input's length like any constant.
pub(crate) fn str_value(bytes: Vec<u8>) -> Value {
    Value::List(vec![bytes.len()].into(), Box::new(Value::u8(bytes)))
}

/// which op idents take a trailing numeric argument — i.e. where a number follows the name.
/// (`branch` also takes one but is parsed specially: its count may be an enum name; `and` takes
/// one optionally: `x and 255` masks by a constant, `(x, y) and` is the pair form.)
pub(crate) fn takes_num(name: &str) -> bool {
    matches!(name, "shl_b64" | "shr_b64" | "rotl_b64" | "rotr_b64" | "chunk" | "try_chunk" | "sort_limit")
}

/// the spellings the integer change retired, each pointed at what replaces it: a typed op such as
/// `add_u64` or `div_f64` (integers have no width, and an op takes its kind from its operands), and
/// the conversions that existed only because integers had widths and signs.
pub(crate) fn retired(name: &str) -> Option<String> {
    let typed = name.rsplit_once('_').filter(|(base, suf)| {
        matches!(*base, "add" | "sub" | "mul" | "div" | "rem" | "neg")
            && matches!(*suf, "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64")
    });
    if let Some((base, _)) = typed {
        let word = if matches!(base, "add" | "sub" | "mul") { format!(" (`{base}_b64` for 64-bit word arithmetic)") } else { String::new() };
        return Some(format!("'{name}' is retired: integers have no width, and `{base}` takes its kind from its operands{word}"));
    }
    Some(match name {
        "cast" => "'cast' is retired: an integer's width is its storage, which the engine picks".into(),
        "signed" => "'signed' is retired: integers are signed values already".into(),
        "to_f32" | "to_f64" => format!("'{name}' is retired: use to_float"),
        "parse_u64" => "'parse_u64' is retired: use parse_int".into(),
        "gather_try" => "'gather_try' is retired: `gather` reads the zero of the element's shape past a row, and `try_get` checks one position".into(),
        "try" => "'try' is retired: the plain ops are lossy, and each checked op is its own word (try_get, try_gather, try_zip, try_chunk) returning Sum{T | ()}".into(),
        // two right shifts that agree on non-negative integers and differ on negative ones made a
        // wrong answer easy to write; the word shift and division are each spelled for what they are
        "shr" => "'shr' is retired: `shr_b64 k` shifts the 64-bit word (zeros in from the top), and `(x, 2^k) div` divides (toward zero)".into(),
        _ => return None,
    })
}

/// the op-name -> `NumOp` table the front-end lowers through. `map` / `map_variant` are NOT here:
/// they carry sub-graphs and are built by the surface itself.
pub(crate) fn resolve(name: &str, arg: Option<u64>) -> Result<NumOp, String> {
    let n = || arg.ok_or_else(|| format!("op '{name}' needs a numeric argument"));
    let k = || -> Result<u32, String> { n().map(|k| k.min(u32::MAX as u64) as u32) };
    Ok(match name {
        "transpose" => Op::Transpose.into(),
        // The ops that can lose something are lossy under their plain names: `zip` keeps the shorter
        // list's length, `gather`/`get` read the zero of the element's shape past a row, `chunk` drops
        // a short last piece. Each has a checked form, `try_zip` and so on, built in ml.rs
        // (`try_word`): a `Sum{T | ()}` whose lane 1 holds the rows that would have lost something.
        "zip" => Op::Zip.into(),         // per row: as long as the shorter list
        "unweave" => Op::Unweave.into(), // sum column -> (tags, lane lists)
        // NOTE: `weave` (Unweave's inverse) is intentionally NOT on the surface. Unlike the other
        // iso-inverses (Zip pairs any two columns; `slices` materializes any ranges, incl. Find's),
        // Weave's input — a tag stream whose per-row counts match a set of lane lengths — arises ONLY
        // from Unweave; a free-standing Weave is either provably the Unweave-inverse or a bug, and its
        // precondition is a histogram relation no static analysis cheaply proves. So `Op::Weave` stays
        // a KERNEL op (the optimizer's `Weave(Unweave x)=x` round-trip; tested at Builder level), never
        // a verb. (A `try_weave` would only carry an unactionable "inconsistent columns" error.)
        "cap_list" => Op::CapList.into(), // capture: pair a context with every list element
        "ref" => Op::Ref.into(),          // T -> Ref<T>: take references (a capture then costs one ref per element)
        "clone" => Op::Clone.into(),      // Ref<T> -> T: clone the referenced rows out
        "cap_sum" => Op::CapSum.into(),   // capture: distribute a context into every sum lane
        "branch" => Op::Branch(n()? as usize).into(), // the demux; a tag of n-1 or more goes to the last lane
        "filter" => Op::Filter.into(), // [(mask, x)] -> [x]: keep the x whose mask is nonzero; total
        // `sort`, `dedup` and `group` are words over `sort_by`, built in ml.rs (`sort_word`).
        "sort_by" => CmpOp::SortBy.into(), // [(k, v)] -> [(k, v, run)]: stable by k, v carried along, each run of equal k numbered
        "sort_limit" => CmpOp::SortLimit(n()? as usize).into(), // `sort`, then the first k of each row
        "adjacent" => CmpOp::Adjacent.into(), // List<X> -> List<Int>: 1 where a run of equal elements starts
        "cut" => Op::Cut.into(),              // [(mask, x)] -> [[x]]: a piece starts at each marked x
        "find" => CmpOp::Find.into(),
        // point access — `gather` (per row, positions of any shape into that row's list, each integer
        // leaf replaced by its element; a position past the row reads the zero of its shape). `get`
        // is the same op on one position per row; `head` (= get 0) and `slices` (= map(range);
        // gather) are built in ml.rs.
        "gather" => Op::Gather.into(),
        "get" => Op::Gather.into(),           // gather on one position per row: (i, list) -> list[i]
        "range" => Op::Range.into(),          // (lo, hi) -> [lo, hi), empty when lo >= hi
        "flatten" => Op::Flatten.into(),
        "enlist" => Op::Enlist.into(),
        "append" => Op::Append.into(), // (List<X>, List<X>) -> List<X>  row-wise concat (the list-monoid ⊕)
        "len" => Op::Len.into(),       // List<X> -> Int  per-row element count, read off the bounds
        "chunk" => Op::Chunk(n()? as usize).into(), // List<X> -> List<List<X>>  fixed k-wide records; a short last piece is dropped
        "unit" => Op::Unit.into(), // X -> Unit (the None of Option = Sum{Unit | T})
        "iota" => Op::Iota.into(),
        "unwrap" => Op::Unwrap.into(),
        "hash" => Op::Hash.into(), // X -> Int  stable structural content hash, all 64 bits (the boundary id fn)
        // relational compares: two columns of one shape -> 0/1 mask, in structural order (leaves by
        // value; lists, products and sums as `sort` orders them, so `(s, "MAIL") eq` compares
        // strings). A leaf constant on either side becomes an immediate (`optimize::immediates`),
        // so `(x, 2) gt` builds no column of 2s; a list constant is still filled per row.
        "eq" => CmpOp::Rel(Pred::Eq).into(),
        "ne" => CmpOp::Rel(Pred::Ne).into(),
        "lt" => CmpOp::Rel(Pred::Lt).into(),
        "le" => CmpOp::Rel(Pred::Le).into(),
        "gt" => CmpOp::Rel(Pred::Gt).into(),
        "ge" => CmpOp::Rel(Pred::Ge).into(),
        // arithmetic, on two Ints or two Floats:
        "add" => ArithOp::Bin(BinOp::Add).into(),
        "sub" => ArithOp::Bin(BinOp::Sub).into(),
        "mul" => ArithOp::Bin(BinOp::Mul).into(),
        "div" => ArithOp::Bin(BinOp::Div).into(),
        "rem" => ArithOp::Bin(BinOp::Rem).into(),
        "neg" => ArithOp::Neg.into(),
        "min" => CmpOp::Min.into(), // lane min/max — order ops, in `cmp` not arithmetic
        "max" => CmpOp::Max.into(),
        "to_float" => ArithOp::ToFloat.into(), // Int -> Float (how iota becomes floats)
        // branchless blend: (mask, then, else) -> picked column (the SIMD bitselect, see Op::Select)
        "select" => Op::Select.into(),
        // integers as 64-bit words, and bitwise:
        "and" => match arg {
            Some(m) => ArithOp::BitsImm(BitOp::And, m as i64).into(), // x & m   (mod 2^k via m = 2^k-1)
            None => ArithOp::Bits(BitOp::And).into(),
        },
        "or" => ArithOp::Bits(BitOp::Or).into(),
        "xor" => ArithOp::Bits(BitOp::Xor).into(),
        "add_b64" => ArithOp::Bits(BitOp::AddB64).into(),
        "sub_b64" => ArithOp::Bits(BitOp::SubB64).into(),
        "mul_b64" => ArithOp::Bits(BitOp::MulB64).into(),
        "shl_b64" => ArithOp::Shift(ShiftOp::ShlB64, k()?).into(),
        "shr_b64" => ArithOp::Shift(ShiftOp::ShrB64, k()?).into(), // the logical shift
        "rotl_b64" => ArithOp::Shift(ShiftOp::RotlB64, k()?).into(),
        "rotr_b64" => ArithOp::Shift(ShiftOp::RotrB64, k()?).into(),
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
        "parse_int" => TextOp::ParseInt.into(),
        other => return Err(retired(other).unwrap_or_else(|| format!("unknown op '{other}'"))),
    })
}
