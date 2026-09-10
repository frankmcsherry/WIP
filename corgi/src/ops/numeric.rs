//! The numeric layer over the core — the first stacked vocabulary on `OpLike`.
//! `NumOp` embeds the whole core `Op` (structure and comparison) via `Core`
//! and adds arithmetic via `Arith`. The same `Graph`/`eval_graph`/`shape_of`
//! machinery runs it unchanged; the core never learns arithmetic.
//!
//! Arithmetic is the (op × kind × width) GRID, in two forms driven from one table of lane
//! bodies (`mod grid`): `Bin(op, kind, bits)` eats a pair of columns, `BinImm(op, kind, bits, c)`
//! eats one column and a constant. A leaf is always `Prim::Uw`; `Kind::U` reads the bytes as
//! the value (native wrapping ops), `Kind::I` reads them as an order-preserving *swizzled*
//! signed value (XOR the top bit — `enc_i64` generalized per width). All interpretation lives
//! here; the shape-checker sees plain leaf ops, never the kinds.

use super::cmp::CmpOp;
use super::core::Op;
use super::text::TextOp;
use crate::graph::{Graph, OpLike};

use crate::value::{Prim, Value};
use std::sync::Arc;

/// order-preserving encode/decode for signed 64-bit integers.
pub fn enc_i64(x: i64) -> u64 {
    (x as u64) ^ (1 << 63)
}
pub fn dec_i64(u: u64) -> i64 {
    (u ^ (1 << 63)) as i64
}

/// a typed scalar literal: the value `n` encoded for `kind` at `width` — raw for `U`, sign-swizzled
/// for `I` (the order-preserving form the leaf stores). The surface `lit_<k><w> N` lowers to
/// `Op::Lit` of this.
pub fn lit_value(kind: Kind, width: u32, n: u64) -> Value {
    let raw = match width {
        8 => Prim::U8(Arc::new(vec![n as u8])),
        16 => Prim::U16(Arc::new(vec![n as u16])),
        32 => Prim::U32(Arc::new(vec![n as u32])),
        64 => Prim::U64(Arc::new(vec![n])),
        _ => panic!("lit: unsupported width {width}"),
    };
    Value::Prim(if matches!(kind, Kind::I) { raw.xor_signbit() } else { raw })
}

/// the named monoid reductions — `List<U64> -> U64` per row, each a one-pass SIMD-friendly horizontal
/// fold (the fast paths a general `fold` over the same monoid would be ~20x slower than). `Min`/`Max`
/// are kind-blind (the order-preserving bytes make them correct for signed/float too); `Sum`/`Prod`
/// are unsigned; `All`/`Any` are the 0/1-mask AND/OR.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Red {
    Add, // `fold_add` (sum) / `scan_add` (prefix sum)
    Mul, // `fold_mul` (product) / `scan_mul`
    Min,
    Max,
    All,
    Any,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div, // FLOAT-ONLY (integer div deferred: no NEON op, div-by-zero would panic). x/0 -> ±inf, 0/0 -> NaN.
    Rem, // INTEGER-ONLY (the float remainder has no caller). `x % 0 = x`: a total definition, so the
         // lane body needs no branch out and callers that guard the divisor pay nothing. It is the
         // "no reduction" reading of a zero modulus, which is what DDIR's `hash(0, ..)` means.
    // The bitwise family. UNSIGNED-ONLY: `Kind::I` and `Kind::F` store an order-preserving swizzle,
    // so a bit op on those bytes is not the bit op on the value; `eval` rejects them. Shifts are
    // TOTAL: the shift amount wraps modulo the width (`wrapping_shl`/`shr`), as `Rem` is total on
    // a zero divisor, so no lane body needs a branch out. `Shr` is the SIMD divide by 2^k and
    // `And` the SIMD modulo (2^k - 1).
    Shl,
    Shr,
    And,
    Or,
    Xor,
    // NB: lane-wise min/max are NOT here — they're kind-blind order ops (byte min/max on the
    // order-preserving leaf needs no deswizzle), so they live in `cmp` as `CmpOp::Min`/`Max`.
}

impl BinOp {
    /// the bitwise cells, which read stored bytes and so are unsigned-only.
    fn is_bitwise(self) -> bool {
        matches!(self, BinOp::Shl | BinOp::Shr | BinOp::And | BinOp::Or | BinOp::Xor)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    U, // unsigned: the bytes ARE the value
    I, // signed: the bytes are an order-preserving swizzle of the value
    F, // float (32/64 only): the bytes are the IEEE bits under the TOTAL-order swizzle. Arithmetic is
       // IEEE (NaN/inf propagate, div-by-zero -> inf/NaN, no panic); ordering/equality is total, NOT
       // IEEE — NaN is orderable (sorts to the top) and equals itself bit-for-bit, -0 != +0. (See NOTES.)
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp, Kind, u32), // binary leaf arithmetic at a bit-width
    Neg(Kind, u32),        // unary negate
    ToSigned,              // leaf -> leaf  XOR the sign bit (any width): unsigned <-> signed encoding,
                           // the kind-conversion `signed` (an involution; how a column enters Kind::I)
    ToFloat(u32),          // U-int leaf -> float leaf (w in {32,64}): each unsigned int -> the float of
                           // the same width, total-order encoded. `to_f32`/`to_f64`: how iota becomes floats.
    BinImm(BinOp, Kind, u32, u64), // leaf -> leaf   the IMMEDIATE column of the grid: `x <op> c` at a
                           // bit-width, for a constant right operand given in its STORED form (raw for
                           // `U`, sign-swizzled for `I`, total-order-encoded for `F` — what `lit_value`
                           // builds). One cell per (op, kind, width) exactly as `Bin`; a separate
                           // variant because it consumes a different shape: `Bin` eats a pair, this
                           // eats one column. The pair form `(x, x lit c) <op>` means the same thing,
                           // but `Lit` broadcasts an n-element constant column and `Tuple` builds a
                           // product, so `x mul 3` wrote a whole extra column per use.
    // The three U64-only immediates that predate the immediate axis. Each is now a spelling of
    // the `BinImm` cell at `(op, U, 64)` and evaluates through it; they stay in the enum so a host
    // that builds them keeps compiling. New graphs should build `BinImm`.
    AddU64(u64),           // U64 -> U64   x + c   (= `BinImm(Add, U, 64, c)`)
    Shr(u32),              // U64 -> U64   x >> k  (= `BinImm(Shr, U, 64, k)`)
    And(u64),              // U64 -> U64   x & m   (= `BinImm(And, U, 64, m)`)
    Reduce(Red),           // List<U64> -> U64      per-row monoid reduction (sum/prod/min/max/all/any)
    Scan(Red),             // List<U64> -> List<U64>  per-row inclusive monoid PREFIX scan. The monoid
                           // fast path for `scan` with a monoid body: one in-place pass, where the
                           // general `FoldScan` re-evals the body per element (catastrophic on one long
                           // row — see perf-gaps.md). `Reduce` is its drop-the-prefix sibling.
}

mod grid {
    //! The (op × kind × width) GRID: one table of lane bodies, and everything needed to reach a
    //! cell of it.
    //!
    //! A leaf is always `Prim::Uw`. `Kind::U` reads the stored bytes as the value (native wrapping
    //! ops); `Kind::I` reads them as an order-preserving SWIZZLE and deswizzles per lane; `Kind::F`
    //! reads them as IEEE bits under the total-order swizzle and is dispatched out to `float_bin`
    //! before the table. All of that interpretation lives here; the shape-checker above sees plain
    //! leaf ops, never the kinds.
    //!
    //! `cells!` is the table, written ONCE and driven two ways: `bin_into` walks a second column,
    //! `imm_into` walks a constant. `check_cell` is the list of (op, kind, width) combinations that
    //! do not exist, shared by both forms so they cannot drift about it.

    use super::{BinOp, Kind};
    use crate::value::Prim;
    use std::sync::Arc;

    /// IEEE-bits <-> total-order encoding for f32 (and f64 below): negatives flip all bits, non-negatives
    /// flip just the sign bit, so the unsigned byte order is the float total order (`f64::total_cmp`). The
    /// kind-blind comparator then sorts/compares floats correctly with no special case.
    pub(super) fn enc_f32(f: f32) -> u32 {
        let b = f.to_bits();
        if b >> 31 == 1 { !b } else { b ^ (1 << 31) }
    }
    fn dec_f32(u: u32) -> f32 {
        f32::from_bits(if u >> 31 == 1 { u ^ (1 << 31) } else { !u })
    }
    pub(super) fn enc_f64(f: f64) -> u64 {
        let b = f.to_bits();
        if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
    }
    fn dec_f64(u: u64) -> f64 {
        f64::from_bits(if u >> 63 == 1 { u ^ (1 << 63) } else { !u })
    }

    // deswizzle the order-preserving signed encoding (XOR the top bit `m`), apply a native wrapping op,
    // reswizzle. `m` is a per-width constant. This is the `Kind::I` lane body, factored so the grid's
    // (kind × op) arms each stay a one-line lane map. `wrapping_*` are inherent on every uN/iN, so
    // no `num_traits` dependency.
    macro_rules! swiz {
        ($u:ty, $i:ty, $x:ident, $y:ident, $op:ident) => {{
            let m = !(<$u>::MAX >> 1);
            ((($x ^ m) as $i).$op(($y ^ m) as $i) as $u) ^ m
        }};
    }

    /// apply a binary lane op `f` in place, writing into whichever operand buffer we uniquely own.
    /// Both lanes are read before the store, so EITHER side is a valid destination (Sub included:
    /// `f` is `x - y` regardless of where it lands). `get_mut` (not `make_mut`) tests uniqueness
    /// without cloning, so a shared LHS falls through to a unique RHS; only when both are shared do we allocate.
    fn bin_into<T: Copy>(mut a: Arc<Vec<T>>, mut b: Arc<Vec<T>>, f: impl Fn(T, T) -> T) -> Arc<Vec<T>> {
        if let Some(dst) = Arc::get_mut(&mut a) {
            for (x, &y) in dst.iter_mut().zip(b.iter()) { *x = f(*x, y); }
            a
        } else if let Some(dst) = Arc::get_mut(&mut b) {
            for (&x, y) in a.iter().zip(dst.iter_mut()) { *y = f(x, *y); }
            b
        } else {
            Arc::new(a.iter().zip(b.iter()).map(|(&x, &y)| f(x, y)).collect())
        }
    }

    /// apply a unary lane op `f` in place when the operand is uniquely owned, else fresh.
    pub(super) fn neg_into<T: Copy>(mut a: Arc<Vec<T>>, f: impl Fn(T) -> T) -> Arc<Vec<T>> {
        if let Some(dst) = Arc::get_mut(&mut a) {
            for x in dst.iter_mut() { *x = f(*x); }
            a
        } else {
            Arc::new(a.iter().map(|&x| f(x)).collect())
        }
    }

    /// apply a lane op `f` to a column and a CONSTANT right operand, in place when uniquely owned.
    /// The immediate sibling of `bin_into`, with the same signature shape so the grid's cells can be
    /// written once and driven either way.
    fn imm_into<T: Copy>(a: Arc<Vec<T>>, y: T, f: impl Fn(T, T) -> T) -> Arc<Vec<T>> {
        neg_into(a, |x| f(x, y))
    }

    /// The (kind × op) TABLE, written once and driven two ways.
    ///
    /// `$apply` names how the right operand is walked — `bin_into` over a second column, `imm_into`
    /// over a constant — and both take the same `|x, y|` lane body, so a cell exists in exactly one
    /// place no matter which form reaches it. The dispatch is HOISTED ABOVE the lane loop: one match
    /// picks one concrete closure, then a single tight pass, with no per-element branch to keep the
    /// vectorizer out. `Kind::U` is native wrapping; `Kind::I` deswizzles and reswizzles via `swiz!`.
    macro_rules! cells {
        ($apply:ident, $av:expr, $rhs:expr, $u:ty, $i:ty, $kind:expr, $op:expr) => {
            match ($kind, $op) {
                (Kind::U, BinOp::Add) => $apply($av, $rhs, |x: $u, y: $u| x.wrapping_add(y)),
                (Kind::U, BinOp::Sub) => $apply($av, $rhs, |x: $u, y: $u| x.wrapping_sub(y)),
                (Kind::U, BinOp::Mul) => $apply($av, $rhs, |x: $u, y: $u| x.wrapping_mul(y)),
                (Kind::I, BinOp::Add) => $apply($av, $rhs, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_add)),
                (Kind::I, BinOp::Sub) => $apply($av, $rhs, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_sub)),
                (Kind::I, BinOp::Mul) => $apply($av, $rhs, |x: $u, y: $u| swiz!($u, $i, x, y, wrapping_mul)),
                (Kind::U, BinOp::Rem) => $apply($av, $rhs, |x: $u, y: $u| if y == 0 { x } else { x % y }),
                // `wrapping_rem` for the MIN % -1 overflow; the zero divisor is the total `x % 0 = x`.
                (Kind::I, BinOp::Rem) => $apply($av, $rhs, |x: $u, y: $u| {
                    let m = !(<$u>::MAX >> 1);
                    if (y ^ m) as $i == 0 { x } else { swiz!($u, $i, x, y, wrapping_rem) }
                }),
                // bitwise: unsigned only (`check_cell` rejects I/F), shifts total via `wrapping_*`.
                (Kind::U, BinOp::Shl) => $apply($av, $rhs, |x: $u, y: $u| x.wrapping_shl(y as u32)),
                (Kind::U, BinOp::Shr) => $apply($av, $rhs, |x: $u, y: $u| x.wrapping_shr(y as u32)),
                (Kind::U, BinOp::And) => $apply($av, $rhs, |x: $u, y: $u| x & y),
                (Kind::U, BinOp::Or) => $apply($av, $rhs, |x: $u, y: $u| x | y),
                (Kind::U, BinOp::Xor) => $apply($av, $rhs, |x: $u, y: $u| x ^ y),
                (Kind::I, BinOp::Shl) | (Kind::I, BinOp::Shr) | (Kind::I, BinOp::And)
                | (Kind::I, BinOp::Or) | (Kind::I, BinOp::Xor) => {
                    unreachable!("bitwise ops are unsigned-only and rejected before dispatch")
                }
                // integer division is deferred; `check_cell` rejects it up front.
                (Kind::U, BinOp::Div) | (Kind::I, BinOp::Div) => {
                    unreachable!("integer Div is rejected before dispatch")
                }
                // float is dispatched by `bin_eval`/`bin_imm_eval` before reaching here.
                (Kind::F, _) => unreachable!("cells: float dispatched above"),
            }
        };
    }

    // list the widths ONCE; generate the per-width leaf arithmetic over the table above. Mirrors `prim!`.
    macro_rules! grid {
        ($($V:ident => $u:ty : $i:ty),+ $(,)?) => {
            /// the pair form: two columns of one width.
            fn int_bin(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
                match (a, b) {
                    $( (Prim::$V(av), Prim::$V(bv)) =>
                        Prim::$V(cells!(bin_into, av, bv, $u, $i, kind, op)), )+
                    _ => panic!("arith: operand width mismatch"),
                }
            }

            /// the immediate form: one column and a constant, given in the leaf's stored form. Same
            /// cells, no operand column — where the pair form's caller has to broadcast one first.
            fn int_bin_imm(op: BinOp, kind: Kind, a: Prim, c: u64) -> Prim {
                match a {
                    $( Prim::$V(av) =>
                        Prim::$V(cells!(imm_into, av, c as $u, $u, $i, kind, op)), )+
                }
            }

            fn int_neg(kind: Kind, a: Prim) -> Prim {
                match a {
                    $( Prim::$V(av) => Prim::$V(match kind {
                        Kind::U => neg_into(av, |x: $u| x.wrapping_neg()),
                        Kind::I => neg_into(av, |x: $u| {
                            let m = !(<$u>::MAX >> 1);
                            (((x ^ m) as $i).wrapping_neg() as $u) ^ m
                        }),
                        Kind::F => unreachable!("int_neg: float dispatched by neg_eval"),
                    }), )+
                }
            }
        };
    }
    grid! { U8 => u8:i8, U16 => u16:i16, U32 => u32:i32, U64 => u64:i64 }

    /// the binary leaf op, dispatching `Kind::F` to the float path (32/64 only) and `U`/`I` to the macro
    /// grid. `check_cell` has already rejected float at widths 8/16 and integer `Div`.
    pub(super) fn bin_eval(op: BinOp, kind: Kind, a: Prim, b: Prim) -> Prim {
        match kind {
            Kind::F => float_bin(op, a, b),
            _ => int_bin(op, kind, a, b),
        }
    }

    /// the immediate leaf op, dispatching `Kind::F` to the float path and `U`/`I` to the macro grid.
    pub(super) fn bin_imm_eval(op: BinOp, kind: Kind, a: Prim, c: u64) -> Prim {
        match kind {
            // a one-element-per-row operand column is the honest float path here: the float lane
            // bodies live in `float_bin`, and duplicating them for a constant would be a second
            // definition of IEEE semantics to keep in step. `Prim::repeat` is one fill, no index column.
            Kind::F => {
                let n = a.len();
                let rhs = match a.bits() {
                    32 => Prim::U32(Arc::new(vec![c as u32; n])),
                    _ => Prim::U64(Arc::new(vec![c; n])),
                };
                float_bin(op, a, rhs)
            }
            _ => int_bin_imm(op, kind, a, c),
        }
    }

    /// the (op, kind, width) cells that are not defined, shared by the pair and immediate forms so the
    /// two cannot drift. `Err` is the shape error the typer reports.
    pub(super) fn check_cell(op: BinOp, kind: Kind, w: u32) -> Result<(), String> {
        if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
            return Err(format!("float arith only at width 32/64, got {w}"));
        }
        if matches!(op, BinOp::Div) && !matches!(kind, Kind::F) {
            return Err("integer div is deferred — div is float-only (use div_f32/div_f64)".into());
        }
        if matches!(op, BinOp::Rem) && matches!(kind, Kind::F) {
            return Err("rem is integer-only".into());
        }
        if op.is_bitwise() && !matches!(kind, Kind::U) {
            return Err(
                "bitwise ops are unsigned-only: signed and float leaves store an order-preserving \
                 swizzle, so a bit op on those bytes is not the bit op on the value"
                    .into(),
            );
        }
        Ok(())
    }

    pub(super) fn neg_eval(kind: Kind, a: Prim) -> Prim {
        match kind {
            Kind::F => match a {
                Prim::U32(v) => Prim::U32(neg_into(v, |u| enc_f32(-dec_f32(u)))),
                Prim::U64(v) => Prim::U64(neg_into(v, |u| enc_f64(-dec_f64(u)))),
                _ => panic!("float neg expects f32/f64"),
            },
            _ => int_neg(kind, a),
        }
    }

    /// IEEE float arithmetic on the total-order-encoded leaf: deswizzle both operands, apply the native
    /// op (NaN/inf propagate, div-by-zero -> inf/NaN — no panic), re-encode. `min`/`max` use IEEE's
    /// (NaN-skipping) float min/max; the *ordering* used by sort/`Rel` is the total order, separately.
    fn float_bin(op: BinOp, a: Prim, b: Prim) -> Prim {
        macro_rules! f { ($V:ident, $dec:ident, $enc:ident, $av:ident, $bv:ident) => {
            Prim::$V(bin_into($av, $bv, |x, y| { let (x, y) = ($dec(x), $dec(y)); $enc(match op {
                BinOp::Add => x + y, BinOp::Sub => x - y, BinOp::Mul => x * y, BinOp::Div => x / y,
                BinOp::Rem => unreachable!("float Rem is rejected before dispatch"),
                _ => unreachable!("bitwise ops are unsigned-only and rejected before dispatch"),
            })}))
        }}
        match (a, b) {
            (Prim::U32(av), Prim::U32(bv)) => f!(U32, dec_f32, enc_f32, av, bv),
            (Prim::U64(av), Prim::U64(bv)) => f!(U64, dec_f64, enc_f64, av, bv),
            _ => panic!("float arith expects f32/f64 (width 32/64)"),
        }
    }
}

use grid::{bin_eval, bin_imm_eval, check_cell, enc_f32, enc_f64, neg_eval, neg_into};

/// the `U64` leaf the three legacy immediates take, or their shape error.
fn prim_u64(input: Value, who: &str) -> Result<Prim, String> {
    let p = input.into_prim(who)?;
    if p.bits() != 64 {
        return Err(format!("{who}: expected U64, got U{}", p.bits()));
    }
    Ok(p)
}

impl ArithOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            ArithOp::Bin(op, kind, w) => {
                check_cell(*op, *kind, *w)?;
                let (a, b) = input.into_pair("binary arith")?;
                let (pa, pb) = (a.into_prim("binary arith lhs")?, b.into_prim("binary arith rhs")?);
                if pa.bits() != *w || pb.bits() != *w {
                    return Err(format!("binary arith expects (U{w}, U{w}), got (U{}, U{})", pa.bits(), pb.bits()));
                }
                assert_eq!(pa.len(), pb.len(), "binary arith: operands at different strata");
                Value::Prim(bin_eval(*op, *kind, pa, pb))
            }
            ArithOp::Neg(kind, w) => {
                if matches!(kind, Kind::F) && !matches!(w, 32 | 64) {
                    return Err(format!("float neg only at width 32/64, got {w}"));
                }
                let p = input.into_prim("Neg")?;
                if p.bits() != *w {
                    return Err(format!("Neg expects U{w}, got U{}", p.bits()));
                }
                Value::Prim(neg_eval(*kind, p))
            }
            ArithOp::ToSigned => Value::Prim(input.into_prim("signed")?.xor_signbit()),
            ArithOp::ToFloat(w) => Value::Prim(match (w, input.into_prim("to_float")?) {
                (32, Prim::U32(v)) => Prim::U32(neg_into(v, |x| enc_f32(x as f32))),
                (64, Prim::U64(v)) => Prim::U64(neg_into(v, |x| enc_f64(x as f64))),
                (w, p) => return Err(format!("to_float expects a U{w} leaf (w in 32/64), got U{}", p.bits())),
            }),
            // the immediate cell: one in-place pass, no operand column. `Bin` and this share
            // `check_cell` so the two forms cannot disagree about which cells exist.
            ArithOp::BinImm(op, kind, w, c) => {
                check_cell(*op, *kind, *w)?;
                let p = input.into_prim("immediate arith")?;
                if p.bits() != *w {
                    return Err(format!("immediate arith expects U{w}, got U{}", p.bits()));
                }
                Value::Prim(bin_imm_eval(*op, *kind, p, *c))
            }
            // the three spellings that predate the immediate axis, each the `(op, U, 64)` cell.
            ArithOp::AddU64(c) => Value::Prim(bin_imm_eval(BinOp::Add, Kind::U, prim_u64(input, "AddU64")?, *c)),
            ArithOp::Shr(k) => Value::Prim(bin_imm_eval(BinOp::Shr, Kind::U, prim_u64(input, "Shr")?, *k as u64)),
            ArithOp::And(m) => Value::Prim(bin_imm_eval(BinOp::And, Kind::U, prim_u64(input, "And")?, *m)),
            ArithOp::Reduce(r) => {
                let (bounds, vals) = input.into_list("reduce")?;
                let xs = vals.as_u64("reduce values")?;
                let mut out = Vec::with_capacity(bounds.len());
                let mut start = 0;
                for end in bounds.ends() {
                    let s = &xs[start..end]; // empty row -> the monoid identity
                    out.push(match r {
                        // Wrapping, to match the Scan sibling (prefix!) and the Kind::U BinOp add — so
                        // reducing raw two's-complement diffs (a negative diff is a large u64) yields
                        // the correct i64 sum instead of a checked-overflow panic in debug.
                        Red::Add => s.iter().fold(0u64, |a, &x| a.wrapping_add(x)),
                        Red::Mul => s.iter().fold(1u64, |a, &x| a.wrapping_mul(x)),
                        Red::Min => s.iter().copied().min().unwrap_or(u64::MAX),
                        Red::Max => s.iter().copied().max().unwrap_or(0),
                        Red::All => s.iter().all(|&x| x != 0) as u64,
                        Red::Any => s.iter().any(|&x| x != 0) as u64,
                    });
                    start = end;
                }
                Value::u64(out)
            }
            ArithOp::Scan(r) => {
                let (bounds, vals) = input.into_list("scan")?;
                let mut xs = vals.into_u64("scan values")?; // owned -> inclusive prefix written in place
                // one monomorphic loop per monoid (no per-element dispatch); the recurrence is
                // sequential within a row, so this is a single memory pass, not a vectorizable one.
                macro_rules! prefix {
                    ($id:expr, $a:ident, $x:ident => $comb:expr) => {{
                        let mut start = 0;
                        for end in bounds.ends() {
                            let mut $a = $id;
                            for slot in &mut xs[start..end] {
                                let $x = *slot;
                                $a = $comb;
                                *slot = $a;
                            }
                            start = end;
                        }
                    }};
                }
                match r {
                    // integer Add/Mul wrap (the totality invariant); identities seed each row.
                    Red::Add => prefix!(0u64, a, x => a.wrapping_add(x)),
                    Red::Mul => prefix!(1u64, a, x => a.wrapping_mul(x)),
                    Red::Min => prefix!(u64::MAX, a, x => a.min(x)),
                    Red::Max => prefix!(0u64, a, x => a.max(x)),
                    Red::All => prefix!(1u64, a, x => a & (x != 0) as u64), // running "all nonzero so far"
                    Red::Any => prefix!(0u64, a, x => a | (x != 0) as u64), // running "any nonzero so far"
                }
                Value::List(bounds, Box::new(Value::u64(xs)))
            }
        })
    }

}

/// the standard vocabulary: the core (structural) ops plus the `cmp` (comparison/order),
/// `arith`, and `text` buckets — the layer the `ml` surface and the optimizer are typed at.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum NumOp {
    Core(Op<NumOp>),
    Cmp(CmpOp),
    Arith(ArithOp),
    Text(TextOp),
}

impl OpLike for NumOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        match self {
            NumOp::Core(c) => c.eval(input),
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
            NumOp::Text(t) => t.eval(input),
        }
    }
    fn children(&self) -> Vec<&Graph<NumOp>> {
        match self {
            NumOp::Core(c) => c.children(), // core bodies are Graph<NumOp>
            NumOp::Cmp(_) | NumOp::Arith(_) | NumOp::Text(_) => Vec::new(),
        }
    }
}

// ergonomic embedding: `b.add(Field(1), …)` / `b.add(SortList, …)` work without wrapping.
impl From<Op<NumOp>> for NumOp {
    fn from(o: Op<NumOp>) -> Self {
        NumOp::Core(o)
    }
}
impl From<CmpOp> for NumOp {
    fn from(c: CmpOp) -> Self {
        NumOp::Cmp(c)
    }
}
impl From<ArithOp> for NumOp {
    fn from(a: ArithOp) -> Self {
        NumOp::Arith(a)
    }
}
impl From<TextOp> for NumOp {
    fn from(t: TextOp) -> Self {
        NumOp::Text(t)
    }
}
