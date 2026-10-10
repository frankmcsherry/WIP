//! The numeric layer over the core — the first stacked vocabulary on `OpLike`.
//! `NumOp` embeds the whole core `Op` (structure and comparison) via `Core`
//! and adds arithmetic via `Arith`. The same `Graph`/`eval_graph`/`shape_of`
//! machinery runs it unchanged; the core never learns arithmetic.
//!
//! Two leaves carry numbers. `Int` is an integer: arithmetic on it means integer arithmetic, and
//! it is exact within `i64` (past that it wraps, the one documented edge for now; truncating
//! division, `x / 0 = 0` and `x % 0 = x`, so nothing fails on data). `Float` is an `f64`, with IEEE
//! arithmetic. The plain ops (`add`, `mul`, ..) take either, two of one kind; a mix is a shape
//! error. Integers a program wants to treat as 64-bit words (hashing, bit banging) use the `_b64`
//! verbs, the shifts and the bitwise ops ([`BitOp`], [`ShiftOp`]): each takes the low 64 bits of
//! its operands, does the `u64` operation, and reads the result back as an `i64`.

use super::cmp::CmpOp;
use super::core::Op;
use super::text::TextOp;
use crate::graph::{Graph, OpLike};
use crate::value::{f64_key, f64_of_key, Elem, Prim, Scalar, Storage, Value, TILE};
use std::sync::Arc;

/// the named monoid reductions — `List<Int> -> Int` per row, each a one-pass SIMD-friendly
/// horizontal fold (the fast paths a general `fold` over the same monoid would be ~20x slower than).
/// `Min`/`Max` go by value, and an empty row's is 0, the zero of an integer (a program that wants
/// another default tests `len` and `select`s it); `All`/`Any` are the mask AND/OR, written as bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Red {
    Add, // `fold_add` (sum) / `scan_add` (prefix sum)
    Mul, // `fold_mul` (product) / `scan_mul`
    Min,
    Max,
    All,
    Any,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div, // Int: truncating, x/0 = 0, MIN/-1 wraps. Float: IEEE division.
    Rem, // INTEGER-ONLY (the float remainder has no caller). `x % 0 = x`: a total definition, so the
         // lane body needs no branch out and callers that guard the divisor pay nothing. It is the
         // "no reduction" reading of a zero modulus, which is what DDIR's `hash(0, ..)` means.
    // NB: lane-wise min/max are NOT here — they're order ops, so they live in `cmp` as
    // `CmpOp::Min`/`Max`.
}

/// integers as 64-bit words: `add_b64`, `sub_b64`, `mul_b64` are the low 64 bits of the sum,
/// difference and product (read back as an `i64`), and `and`, `or`, `xor` are two's complement
/// bitwise, which keeps every result in the `i64` range. Two byte leaves `and`, `or` and `xor` to a
/// byte leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BitOp {
    AddB64,
    SubB64,
    MulB64,
    And,
    Or,
    Xor,
}

/// shifts by a constant, each on the integer as its 64-bit word: `ShlB64` drops the bits shifted
/// past 64, `ShrB64` fills with zeros (a logical shift), and the rotates move bits around the
/// word. A shift by 64 or more leaves no bits; a rotate turns by `k mod 64`. (There is no
/// integer shift: dividing by a power of two is `div`, which runs as a shift.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ShiftOp {
    ShlB64,
    ShrB64,
    RotlB64,
    RotrB64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Bin(BinOp),            // (X, X) -> X   two Ints or two Floats
    BinImm(BinOp, Scalar), // X -> X   `x op c` with a constant `c` of x's kind. One pass over `x`,
                           // in place when it is uniquely owned; no column of `c` is built.
    Bits(BitOp),           // (Int, Int) -> Int
    BitsImm(BitOp, i64),   // Int -> Int   `x op c`
    Shift(ShiftOp, u32),   // Int -> Int
    Neg,                   // X -> X   negate an Int (wrapping at the edge) or a Float
    ToFloat,               // Int -> Float   the nearest `f64`
    Reduce(Red),           // List<Int> -> Int      per-row monoid reduction (sum/prod/min/max/all/any)
    Scan(Red),             // List<Int> -> List<Int>  per-row inclusive monoid PREFIX scan. The monoid
                           // fast path for `scan` with a monoid body: one in-place pass, where the
                           // general `FoldScan` re-evals the body per element (catastrophic on one long
                           // row — see performance.md). `Reduce` is its drop-the-prefix sibling.
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

/// apply a unary lane op `f` in place when the operand is uniquely owned, else fresh. (A binary op
/// against a constant is this with the constant captured.)
fn map_into<T: Copy>(mut a: Arc<Vec<T>>, f: impl Fn(T) -> T) -> Arc<Vec<T>> {
    if let Some(dst) = Arc::get_mut(&mut a) {
        for x in dst.iter_mut() { *x = f(*x); }
        a
    } else {
        Arc::new(a.iter().map(|&x| f(x)).collect())
    }
}

/// An integer storage a kernel computes at: its lane type, and how a leaf held there is borrowed
/// and made. Each integer op's loop is compiled once per lane it computes at (`u8`, `i16`, `i32`,
/// `i64`; [`at_storage`]), reading its operands as that type (a tile at a time from any other
/// storage) and writing it: code grows with the ops times the lanes, never with pairs of storages.
/// Each op plans the storage it computes at from what its operands can hold ([`span`], [`plan`]),
/// so no result leaves it and no lane needs a check; only at `i64` does arithmetic wrap, as it
/// always has.
trait Lane: Elem {
    const STORAGE: Storage;
    /// the integer `x`, which the plan made sure fits.
    fn of(x: i64) -> Self;
    fn slice(p: &Prim) -> Option<&[Self]>;
    /// the leaf's rows to write over, when it is held at this storage and nothing else holds it.
    fn slice_mut(p: &mut Prim) -> Option<&mut [Self]>;
    fn wrap(v: Arc<Vec<Self>>) -> Prim;
    fn add(self, y: Self) -> Self;
    fn sub(self, y: Self) -> Self;
    fn mul(self, y: Self) -> Self;
    /// truncating, with `x / 0 = 0`.
    fn div(self, y: Self) -> Self;
    /// the remainder of the truncating division, with `x % 0 = x`.
    fn rem(self, y: Self) -> Self;
    fn and(self, y: Self) -> Self;
    fn or(self, y: Self) -> Self;
    fn xor(self, y: Self) -> Self;
    fn neg(self) -> Self;
    /// every bit set for a negative value, none otherwise.
    fn sign(self) -> Self;
    /// shifts by fewer bits than the lane has: arithmetic to the right for the signed lanes.
    fn shr(self, k: u32) -> Self;
    fn shl(self, k: u32) -> Self;
}

macro_rules! lane {
    ($($t:ty => $V:ident),+) => { $(
        impl Lane for $t {
            const STORAGE: Storage = Storage::$V;
            #[inline] fn of(x: i64) -> Self { x as $t }
            fn slice(p: &Prim) -> Option<&[Self]> { if let Prim::$V(v) = p { Some(v) } else { None } }
            fn slice_mut(p: &mut Prim) -> Option<&mut [Self]> { if let Prim::$V(v) = p { Arc::get_mut(v).map(|v| &mut v[..]) } else { None } }
            fn wrap(v: Arc<Vec<Self>>) -> Prim { Prim::$V(v) }
            #[inline] fn add(self, y: Self) -> Self { self.wrapping_add(y) }
            #[inline] fn sub(self, y: Self) -> Self { self.wrapping_sub(y) }
            #[inline] fn mul(self, y: Self) -> Self { self.wrapping_mul(y) }
            #[inline] fn div(self, y: Self) -> Self { if y == 0 { 0 } else { self.wrapping_div(y) } }
            #[inline] fn rem(self, y: Self) -> Self { if y == 0 { self } else { self.wrapping_rem(y) } }
            #[inline] fn and(self, y: Self) -> Self { self & y }
            #[inline] fn or(self, y: Self) -> Self { self | y }
            #[inline] fn xor(self, y: Self) -> Self { self ^ y }
            #[inline] fn neg(self) -> Self { self.wrapping_neg() }
            #[allow(unused_comparisons)]
            #[inline] fn sign(self) -> Self { if self < 0 { !0 } else { 0 } }
            #[inline] fn shr(self, k: u32) -> Self { self >> k }
            #[inline] fn shl(self, k: u32) -> Self { self << k }
        }
    )+ };
}
lane!(u8 => U8, i8 => I8, i16 => I16, i32 => I32, i64 => I64);

/// `$body` with `$C` the lane type an op planned at the storage `$s` computes at: the body compiled
/// once per lane. `i8` computes at `i16` (what it holds is stored back at `i8`), as small negative
/// values are rare enough not to pay for a lane of their own; a division or remainder, which
/// no vector unit does, computes at `i32` or `i64` (`wide`).
macro_rules! at_storage {
    ($s:expr, $C:ident => $body:expr) => {
        match $s {
            Storage::U8 => { type $C = u8; $body }
            Storage::I8 | Storage::I16 => { type $C = i16; $body }
            Storage::I32 => { type $C = i32; $body }
            Storage::I64 => { type $C = i64; $body }
        }
    };
    (wide $s:expr, $C:ident => $body:expr) => {
        match Storage::join($s, Storage::I32) {
            Storage::I64 => { type $C = i64; $body }
            _ => { type $C = i32; $body }
        }
    };
}

/// rows `at..at + n` of an integer leaf as `C`s: the leaf's own slice when it is held at `C`,
/// otherwise converted into `buf` (the plan made sure `C` holds every value).
fn tile<'a, C: Lane>(p: &'a Prim, at: usize, n: usize, buf: &'a mut Vec<C>) -> &'a [C] {
    fn read<S: Elem, C: Lane>(xs: &[S], buf: &mut Vec<C>) {
        buf.clear();
        buf.extend(xs.iter().map(|&x| C::of(x.word() as i64)));
    }
    if let Some(xs) = C::slice(p) {
        return &xs[at..at + n];
    }
    match p {
        Prim::U8(v) => read(&v[at..at + n], buf),
        Prim::I8(v) => read(&v[at..at + n], buf),
        Prim::I16(v) => read(&v[at..at + n], buf),
        Prim::I32(v) => read(&v[at..at + n], buf),
        Prim::I64(v) => read(&v[at..at + n], buf),
        Prim::F64(_) => unreachable!("an integer kernel reads integer leaves"),
    }
    buf
}

/// a column written at a storage a tile at a time, from tiles computed at one that holds it (the
/// plan made sure every value fits): what a kernel writes when it doesn't write in place.
struct Writer(Prim);

impl Writer {
    fn new(s: Storage, n: usize) -> Writer {
        Writer(match s {
            Storage::U8 => Prim::U8(Arc::new(Vec::with_capacity(n))),
            Storage::I8 => Prim::I8(Arc::new(Vec::with_capacity(n))),
            Storage::I16 => Prim::I16(Arc::new(Vec::with_capacity(n))),
            Storage::I32 => Prim::I32(Arc::new(Vec::with_capacity(n))),
            Storage::I64 => Prim::I64(Arc::new(Vec::with_capacity(n))),
        })
    }
    fn put<C: Lane>(&mut self, xs: &[C]) {
        fn put<C: Lane, O: Lane>(v: &mut Arc<Vec<O>>, xs: &[C]) {
            Arc::get_mut(v).expect("a writer's leaf is its own").extend(xs.iter().map(|&x| O::of(x.word() as i64)));
        }
        match &mut self.0 {
            Prim::U8(v) => put(v, xs),
            Prim::I8(v) => put(v, xs),
            Prim::I16(v) => put(v, xs),
            Prim::I32(v) => put(v, xs),
            Prim::I64(v) => put(v, xs),
            Prim::F64(_) => unreachable!("a writer holds integers"),
        }
    }
}

/// where a kernel's results go when they can't be written over its first operand: over the second
/// operand's rows, when it is held at the results' storage and nothing else holds it, or into a
/// column written at their storage.
enum Sink {
    OverB,
    New(Writer),
}

impl Sink {
    /// the sink for `n` results at `out`, computed at `C`, beside a second operand `b` (if any).
    fn new<C: Lane>(b: Option<&mut Prim>, out: Storage, n: usize) -> Sink {
        if out == C::STORAGE && b.is_some_and(|b| C::slice_mut(b).is_some()) {
            return Sink::OverB;
        }
        Sink::New(Writer::new(out, n))
    }
    /// the results for rows `at..`, out of line, as only the loop computing them is per op.
    #[inline(never)]
    fn put<C: Lane>(&mut self, b: Option<&mut Prim>, at: usize, cs: &[C]) {
        match self {
            Sink::OverB => {
                let b = C::slice_mut(b.expect("a second operand")).expect("an operand to write over");
                b[at..at + cs.len()].copy_from_slice(cs);
            }
            Sink::New(w) => w.put(cs),
        }
    }
    fn done(self, b: Option<Prim>) -> Prim {
        match self {
            Sink::OverB => b.expect("a second operand"),
            Sink::New(w) => w.0,
        }
    }
}

/// `f` over a tile in place: `xs[k] = f(xs[k], ys[k])`. The one loop compiled per op (and lane);
/// out of line, so both of `pairs`'s ways of reaching it share it.
#[inline(never)]
fn over<C: Lane>(xs: &mut [C], ys: &[C], f: &impl Fn(C, C) -> C) {
    xs.iter_mut().zip(ys).for_each(|(x, &y)| *x = f(*x, y));
}

/// `f` on each pair, computed at `C` a tile at a time (an operand held elsewhere is read as `C`),
/// and stored at `out`, which `C` holds. At `out = C`, over `a`'s own rows when nothing else holds
/// them, else into a new column; otherwise each tile of `a` is copied out, computed over, and put
/// ([`Sink`]).
fn pairs<C: Lane>(mut a: Prim, mut b: Prim, out: Storage, f: impl Fn(C, C) -> C) -> Prim {
    let n = a.len();
    let (mut ba, mut bb) = (Vec::new(), Vec::new());
    if out == C::STORAGE && C::slice_mut(&mut a).is_some() {
        for at in (0..n).step_by(TILE) {
            let m = TILE.min(n - at);
            let ys = tile(&b, at, m, &mut bb);
            over(&mut C::slice_mut(&mut a).expect("checked")[at..at + m], ys, &f);
        }
        return a;
    }
    if out == C::STORAGE && C::slice_mut(&mut b).is_none() {
        let mut v = Vec::with_capacity(n);
        for at in (0..n).step_by(TILE) {
            let m = TILE.min(n - at);
            let (xs, ys) = (tile(&a, at, m, &mut ba), tile(&b, at, m, &mut bb));
            v.extend(xs.iter().zip(ys).map(|(&x, &y)| f(x, y)));
        }
        return C::wrap(Arc::new(v));
    }
    let mut sink = Sink::new::<C>(Some(&mut b), out, n);
    let mut cs = Vec::with_capacity(TILE.min(n));
    for at in (0..n).step_by(TILE) {
        let m = TILE.min(n - at);
        cs.clear();
        cs.extend_from_slice(tile(&a, at, m, &mut ba));
        over(&mut cs, tile(&b, at, m, &mut bb), &f);
        sink.put(Some(&mut b), at, &cs);
    }
    sink.done(Some(b))
}

/// `f` over a tile in place, as `over` is for `pairs`.
#[inline(never)]
fn over1<C: Lane>(xs: &mut [C], f: &impl Fn(C) -> C) {
    xs.iter_mut().for_each(|x| *x = f(*x));
}

/// `f` on each element, computed and stored as `pairs` does.
fn map<C: Lane>(mut a: Prim, out: Storage, f: impl Fn(C) -> C) -> Prim {
    let n = a.len();
    if out == C::STORAGE {
        if let Some(xs) = C::slice_mut(&mut a) {
            over1(xs, &f);
            return a;
        }
        let (mut buf, mut v) = (Vec::new(), Vec::with_capacity(n));
        for at in (0..n).step_by(TILE) {
            let m = TILE.min(n - at);
            v.extend(tile(&a, at, m, &mut buf).iter().map(|&x| f(x)));
        }
        return C::wrap(Arc::new(v));
    }
    let mut sink = Sink::new::<C>(None, out, n);
    let (mut buf, mut cs) = (Vec::new(), Vec::with_capacity(TILE.min(n)));
    for at in (0..n).step_by(TILE) {
        let m = TILE.min(n - at);
        cs.clear();
        cs.extend_from_slice(tile(&a, at, m, &mut buf));
        over1(&mut cs, &f);
        sink.put(None, at, &cs);
    }
    sink.done(None)
}

/// the interval an op plans with for an operand: for a narrow leaf, the least and greatest of its
/// values, found in a pass at its own width (a leaf with no rows plans as 0); for an `i64` leaf,
/// all of `i64`, as a pass over it would cost about what the op does.
fn span(p: &Prim) -> (i128, i128) {
    if storage(p) == Storage::I64 {
        return (i64::MIN as i128, i64::MAX as i128);
    }
    p.int_range().map_or((0, 0), |(lo, hi)| (lo as i128, hi as i128))
}

/// the storage of an integer leaf.
fn storage(p: &Prim) -> Storage {
    p.storage().expect("an integer leaf")
}

/// the narrowest storage holding an interval of results: `i64` when the interval passes it, as
/// a result past `i64` wraps.
fn plan((lo, hi): (i128, i128)) -> Storage {
    if lo < i64::MIN as i128 || hi > i64::MAX as i128 {
        Storage::I64
    } else {
        Storage::holding(lo as i64, hi as i64)
    }
}

/// every value `x op y` can take for `x` and `y` in two intervals: the plan for the op's output.
/// A divisor's interval holds 0 unless it is one constant (a storage's always does), and dividing
/// by 0 gives 0 and leaves a remainder of `x`.
fn arith_range(op: BinOp, (a0, a1): (i128, i128), (b0, b1): (i128, i128)) -> (i128, i128) {
    match op {
        BinOp::Add => (a0 + b0, a1 + b1),
        BinOp::Sub => (a0 - b1, a1 - b0),
        BinOp::Mul => {
            let c = [a0 * b0, a0 * b1, a1 * b0, a1 * b1];
            (c.into_iter().min().unwrap(), c.into_iter().max().unwrap())
        }
        // a constant divisor divides the ends; any other keeps a quotient within the dividend's
        // magnitude, and its sign too unless the divisor can be negative
        BinOp::Div if b0 == b1 && b0 != 0 => {
            let (p, q) = (a0 / b0, a1 / b0);
            (p.min(q), p.max(q))
        }
        BinOp::Div if b0 == b1 => (0, 0),
        BinOp::Div if b0 < 0 => {
            let m = a1.max(-a0);
            (-m, m)
        }
        BinOp::Div => (a0.min(0), a1.max(0)),
        // a remainder has the dividend's sign, and is smaller than a constant divisor
        BinOp::Rem if b0 == b1 && b0 != 0 => {
            let m = b0.abs() - 1;
            (a0.max(-m).min(0), a1.min(m).max(0))
        }
        BinOp::Rem => (a0.min(0), a1.max(0)),
    }
}

/// a binary op on two integer leaves, computed at the storage that holds both operands and every
/// result, then stored at the result's own storage when that is narrower.
fn int_bin(op: BinOp, a: Prim, b: Prim) -> Prim {
    let (sa, sb) = (storage(&a), storage(&b));
    // a sum, difference or product with an `i64` is planned at `i64` whatever the other holds,
    // so the other isn't scanned
    let wide = (sa == Storage::I64 || sb == Storage::I64) && matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul);
    let out = if wide { Storage::I64 } else { plan(arith_range(op, span(&a), span(&b))) };
    let at = Storage::join(Storage::join(sa, sb), out);
    match op {
        BinOp::Add => at_storage!(at, C => pairs::<C>(a, b, out, C::add)),
        BinOp::Sub => at_storage!(at, C => pairs::<C>(a, b, out, C::sub)),
        BinOp::Mul => at_storage!(at, C => pairs::<C>(a, b, out, C::mul)),
        BinOp::Div => at_storage!(wide at, C => pairs::<C>(a, b, out, C::div)),
        BinOp::Rem => at_storage!(wide at, C => pairs::<C>(a, b, out, C::rem)),
    }
}

/// `x op c` for an integer leaf and an integer constant, planned as `int_bin`. By a power of two,
/// `div` and `rem` are shifts: a negative dividend is biased by `c - 1` first, so the quotient
/// still rounds toward zero (and the remainder takes its sign).
fn int_imm(op: BinOp, a: Prim, c: i64) -> Prim {
    let sa = storage(&a);
    let out = plan(arith_range(op, span(&a), (c as i128, c as i128)));
    let at = Storage::join(Storage::join(sa, Storage::holding(c, c)), out);
    let pow2 = c > 1 && c.count_ones() == 1;
    match op {
        BinOp::Div | BinOp::Rem if !pow2 => at_storage!(wide at, C => {
            let k = C::of(c);
            if op == BinOp::Div { map::<C>(a, out, move |x| x.div(k)) } else { map::<C>(a, out, move |x| x.rem(k)) }
        }),
        _ => at_storage!(at, C => {
            let (k, m, s) = (C::of(c), C::of(c.wrapping_sub(1)), c.trailing_zeros());
            match op {
                BinOp::Div => map::<C>(a, out, move |x| x.add(x.sign().and(m)).shr(s)),
                BinOp::Rem => map::<C>(a, out, move |x| x.sub(x.add(x.sign().and(m)).shr(s).shl(s))),
                BinOp::Add => map::<C>(a, out, move |x| x.add(k)),
                BinOp::Sub => map::<C>(a, out, move |x| x.sub(k)),
                _ => map::<C>(a, out, move |x| x.mul(k)),
            }
        }),
    }
}

/// `$apply(args.., body)` with the float lane body of `$op`, on total-order keys (decode, IEEE op,
/// encode); `rem` is integer-only. Matched once, as `int_body`.
macro_rules! float_body {
    ($op:expr, $apply:ident($($arg:expr),*)) => {
        match $op {
            BinOp::Add => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) + f64_of_key(y))),
            BinOp::Sub => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) - f64_of_key(y))),
            BinOp::Mul => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) * f64_of_key(y))),
            BinOp::Div => $apply($($arg,)* |x: u64, y: u64| f64_key(f64_of_key(x) / f64_of_key(y))),
            BinOp::Rem => return Err("rem is integer-only".into()),
        }
    };
}

/// the error for two leaves of different kinds.
fn mixed(op: impl std::fmt::Debug, a: &Prim, b: &Prim) -> String {
    let kind = |p: &Prim| if p.is_int() { "Int" } else { "Float" };
    format!("{op:?}: an {} and a {}", kind(a), kind(b))
}

/// a binary op on two leaves of one kind.
fn bin_eval(op: BinOp, a: Prim, b: Prim) -> Result<Prim, String> {
    Ok(match (a, b) {
        (Prim::F64(x), Prim::F64(y)) => Prim::F64(float_body!(op, bin_into(x, y))),
        (a, b) if a.is_int() && b.is_int() => int_bin(op, a, b),
        (a, b) => return Err(mixed(op, &a, &b)),
    })
}

/// `x op c` for a constant `c` of `x`'s kind.
fn imm_eval(op: BinOp, a: Prim, c: Scalar) -> Result<Prim, String> {
    Ok(match (a, c) {
        (Prim::F64(x), Scalar::Float(k)) => {
            fn float_imm(a: Arc<Vec<u64>>, k: u64, f: impl Fn(u64, u64) -> u64) -> Prim {
                Prim::F64(map_into(a, move |x| f(x, k)))
            }
            float_body!(op, float_imm(x, k))
        }
        (a, Scalar::Int(c)) if a.is_int() => int_imm(op, a, c),
        (a, c) => return Err(format!("{op:?}: {} with the constant {c:?}", if a.is_int() { "an Int" } else { "a Float" })),
    })
}

/// a bitwise op on two integer leaves. `and`, `or` and `xor` of two's complement integers stay
/// within the storage that holds both operands, and are computed there; the `_b64` verbs are
/// 64-bit words.
fn bits_eval(op: BitOp, a: Prim, b: Prim) -> Result<Prim, String> {
    if !(a.is_int() && b.is_int()) {
        return Err(mixed(op, &a, &b));
    }
    Ok(match op {
        BitOp::AddB64 => pairs::<i64>(a, b, Storage::I64, i64::add),
        BitOp::SubB64 => pairs::<i64>(a, b, Storage::I64, i64::sub),
        BitOp::MulB64 => pairs::<i64>(a, b, Storage::I64, i64::mul),
        _ => {
            let at = Storage::join(storage(&a), storage(&b));
            at_storage!(at, C => match op {
                BitOp::And => pairs::<C>(a, b, at, C::and),
                BitOp::Or => pairs::<C>(a, b, at, C::or),
                _ => pairs::<C>(a, b, at, C::xor),
            })
        }
    })
}

/// `x op c`, bitwise, for an integer leaf. Computed where `x` and `c` both fit; `x and c` for a
/// `c` that is not negative is from 0 to `c`, and stored there (`c and 223`, the case fold of
/// text, stays bytes; a hash `and 255` becomes bytes).
fn bits_imm(op: BitOp, a: Prim, c: i64) -> Result<Prim, String> {
    if !a.is_int() {
        return Err(format!("{op:?}: a Float"));
    }
    let sa = storage(&a);
    let at = Storage::join(sa, Storage::holding(c, c));
    let out = match op {
        BitOp::And if c >= 0 => {
            let (lo, hi) = span(&a);
            Storage::holding(0, if lo >= 0 { c.min(hi as i64) } else { c })
        }
        _ => at,
    };
    Ok(match op {
        BitOp::AddB64 => map::<i64>(a, Storage::I64, move |x| x.wrapping_add(c)),
        BitOp::SubB64 => map::<i64>(a, Storage::I64, move |x| x.wrapping_sub(c)),
        BitOp::MulB64 => map::<i64>(a, Storage::I64, move |x| x.wrapping_mul(c)),
        _ => at_storage!(at, C => {
            let k = C::of(c);
            match op {
                BitOp::And => map::<C>(a, out, move |x| x.and(k)),
                BitOp::Or => map::<C>(a, out, move |x| x.or(k)),
                _ => map::<C>(a, out, move |x| x.xor(k)),
            }
        }),
    })
}

/// a shift by `k` of an integer leaf, on its 64-bit word; a byte shifted right is a byte.
fn shift_eval(op: ShiftOp, a: Prim, k: u32) -> Result<Prim, String> {
    if !a.is_int() {
        return Err(format!("{op:?}: a Float"));
    }
    Ok(match op {
        ShiftOp::ShrB64 if storage(&a) == Storage::U8 => map::<u8>(a, Storage::U8, move |x| x.checked_shr(k).unwrap_or(0)),
        ShiftOp::ShlB64 if k >= 64 => map::<i64>(a, Storage::I64, |_| 0),
        ShiftOp::ShrB64 if k >= 64 => map::<i64>(a, Storage::I64, |_| 0),
        ShiftOp::ShlB64 => map::<i64>(a, Storage::I64, move |x| ((x as u64) << k) as i64),
        ShiftOp::ShrB64 => map::<i64>(a, Storage::I64, move |x| ((x as u64) >> k) as i64),
        ShiftOp::RotlB64 => map::<i64>(a, Storage::I64, move |x| (x as u64).rotate_left(k % 64) as i64),
        ShiftOp::RotrB64 => map::<i64>(a, Storage::I64, move |x| (x as u64).rotate_right(k % 64) as i64),
    })
}

/// the storage a row's sum or prefix sum is planned at: the longest row's length times what the
/// values' storage holds (a sum reads its values once, so a pass to find their least and greatest
/// would double it).
fn sum_storage(bounds: &crate::value::Bounds, s: Storage) -> Storage {
    let n = (0..bounds.len()).map(|r| { let (a, b) = bounds.span(r); b - a }).max().unwrap_or(0) as i128;
    let (lo, hi) = s.range();
    plan((n * lo as i128, n * hi as i128))
}

/// each row's reduction, reading the values at their storage (a narrow leaf is not widened first).
/// Sums and products wrap at the `i64` edge, as `add` and `mul` do. An empty row's sum is 0, its
/// product 1, its minimum and maximum 0. A minimum or maximum is one of the values, and is held at
/// their storage; a sum at the storage [`sum_storage`] plans.
fn reduce_rows<T: Lane>(bounds: &crate::value::Bounds, xs: &[T], r: Red) -> Prim {
    let mut start = 0;
    let rows = bounds.ends().map(|end| {
        let row = &xs[start..end];
        start = end;
        row
    });
    let int = |x: T| x.word() as i64;
    match r {
        Red::Add => {
            let sums = rows.map(|s| s.iter().fold(0i64, |a, &x| a.wrapping_add(int(x)))).collect();
            Prim::I64(Arc::new(sums)).to_storage(sum_storage(bounds, T::STORAGE))
        }
        Red::Mul => Prim::I64(Arc::new(rows.map(|s| s.iter().fold(1i64, |a, &x| a.wrapping_mul(int(x)))).collect())),
        Red::Min => T::wrap(Arc::new(rows.map(|s| s.iter().copied().min().unwrap_or_default()).collect())),
        Red::Max => T::wrap(Arc::new(rows.map(|s| s.iter().copied().max().unwrap_or_default()).collect())),
        Red::All => Prim::U8(Arc::new(rows.map(|s| s.iter().all(|&x| int(x) != 0) as u8).collect())),
        Red::Any => Prim::U8(Arc::new(rows.map(|s| s.iter().any(|&x| int(x) != 0) as u8).collect())),
    }
}

impl ArithOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        Ok(match self {
            ArithOp::Bin(op) => {
                let (a, b) = input.into_pair("binary arith")?;
                let (pa, pb) = (a.into_prim("binary arith lhs")?, b.into_prim("binary arith rhs")?);
                assert_eq!(pa.len(), pb.len(), "binary arith: operands at different strata");
                Value::Prim(bin_eval(*op, pa, pb)?)
            }
            ArithOp::BinImm(op, c) => Value::Prim(imm_eval(*op, input.into_prim("arith with a constant")?, *c)?),
            ArithOp::Bits(op) => {
                let (a, b) = input.into_pair("bitwise")?;
                let (pa, pb) = (a.into_prim("bitwise lhs")?, b.into_prim("bitwise rhs")?);
                assert_eq!(pa.len(), pb.len(), "bitwise: operands at different strata");
                Value::Prim(bits_eval(*op, pa, pb)?)
            }
            ArithOp::BitsImm(op, c) => Value::Prim(bits_imm(*op, input.into_prim("bitwise with a constant")?, *c)?),
            ArithOp::Shift(op, k) => Value::Prim(shift_eval(*op, input.into_prim("shift")?, *k)?),
            ArithOp::Neg => Value::Prim(match input.into_prim("neg")? {
                Prim::F64(v) => Prim::F64(map_into(v, |k| f64_key(-f64_of_key(k)))),
                p => {
                    let (lo, hi) = span(&p);
                    let out = plan((-hi, -lo));
                    let at = Storage::join(storage(&p), out);
                    at_storage!(at, C => map::<C>(p, out, C::neg))
                }
            }),
            ArithOp::ToFloat => {
                let xs = input.as_i64("to_float")?;
                Value::f64(xs.iter().map(|&x| x as f64).collect())
            }
            ArithOp::Reduce(r) => {
                let (bounds, vals) = input.into_list("reduce")?;
                Value::Prim(match vals.into_prim("reduce values")? {
                    Prim::U8(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::I8(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::I16(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::I32(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::I64(xs) => reduce_rows(&bounds, &xs, *r),
                    Prim::F64(_) => return Err("reduce: expected Int values, got Float".into()),
                })
            }
            ArithOp::Scan(r) => {
                let (bounds, vals) = input.into_list("scan")?;
                let s = storage(&vals.clone().into_prim("scan values")?);
                let mut xs = vals.into_i64("scan values")?; // owned -> inclusive prefix written in place
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
                    Red::Add => prefix!(0i64, a, x => a.wrapping_add(x)),
                    Red::Mul => prefix!(1i64, a, x => a.wrapping_mul(x)),
                    Red::Min => prefix!(i64::MAX, a, x => a.min(x)),
                    Red::Max => prefix!(i64::MIN, a, x => a.max(x)),
                    Red::All => prefix!(1i64, a, x => a & (x != 0) as i64), // running "all nonzero so far"
                    Red::Any => prefix!(0i64, a, x => a | (x != 0) as i64), // running "any nonzero so far"
                }
                // a running minimum or maximum is one of the values; a prefix sum is planned as a sum
                let out = match r {
                    Red::All | Red::Any => Storage::U8,
                    Red::Min | Red::Max => s,
                    Red::Add => sum_storage(&bounds, s),
                    Red::Mul => Storage::I64,
                };
                let out = Value::Prim(Prim::I64(Arc::new(xs)).to_storage(out));
                Value::List(bounds, Box::new(out))
            }
        })
    }
}

/// the standard vocabulary: the core (structural) ops plus the `cmp` (comparison/order),
/// `arith`, and `text` buckets — the layer the `ml` surface and the optimizer are typed at.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NumOp {
    Core(Op<NumOp>),
    Cmp(CmpOp),
    Arith(ArithOp),
    Text(TextOp),
    /// A kernel supplied from outside corgi (`ops::host`).
    Host(super::host::HostOp),
}

impl OpLike for NumOp {
    fn eval(&self, input: Value) -> Result<Value, String> {
        #[cfg(feature = "profile")]
        return crate::explain::profile::time(|| profile_key(self), || self.dispatch(input));
        #[cfg(not(feature = "profile"))]
        self.dispatch(input)
    }
    fn children(&self) -> Vec<&Graph<NumOp>> {
        match self {
            NumOp::Core(c) => c.children(), // core bodies are Graph<NumOp>
            NumOp::Cmp(_) | NumOp::Arith(_) | NumOp::Text(_) | NumOp::Host(_) => Vec::new(),
        }
    }
}

/// the name an op is profiled under: `explain`'s, with constants dropped so they aggregate.
#[cfg(feature = "profile")]
fn profile_key(op: &NumOp) -> String {
    match op {
        NumOp::Core(Op::Lit(_)) => "lit".into(),
        _ => crate::explain::op_name(op),
    }
}

impl NumOp {
    fn dispatch(&self, input: Value) -> Result<Value, String> {
        match self {
            NumOp::Core(c) => c.eval(input),
            NumOp::Cmp(c) => c.eval(input),
            NumOp::Arith(a) => a.eval(input),
            NumOp::Text(t) => t.eval(input),
            NumOp::Host(h) => h.eval(input),
        }
    }
}

// ergonomic embedding: `b.add(Field(1), …)` / `b.add(SortBy, …)` work without wrapping.
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

#[cfg(test)]
mod tests;
