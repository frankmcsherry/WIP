//! Integers as values: a column of integers whose width is how it is stored, not what it is.
//!
//! An [`Int`] column is a set of mathematical integers. It stores them in a *frame of reference*:
//! a per-column `base` (the value offset 0 stands for) plus one unsigned offset per row, at the
//! narrowest of 0, 8, 16, 32 or 64 bits the column was built to need. Signed and unsigned are the
//! same thing here — `[-3, 7]` is base -3 with offsets up to 10 — and offsets keep the order of the
//! values, so sort, `find` and comparison read the stored offsets directly, as they read a `Prim`'s
//! bytes. A column whose rows are all one value stores nothing at all (width 0).
//!
//! The frame also carries `span`, a bound every offset is known to stay within. Kernels that make
//! a new column from old ones (arithmetic, merges) choose the result's frame from their operands'
//! frames *before* touching a row, so nothing checks for overflow per element; a sort packs fields
//! into one key by their spans rather than by their storage widths.
//!
//! Storage is word-backed (`crate::words`): the offsets live in `u64` words and are read as a
//! slice at the column's width, and a column may be a window (`at`, `len`) of a larger buffer, which
//! is how a decoded message is read without being copied. The shape of every `Int` column is the
//! same, `Shape::Int`; the frame is invisible to the typer, to equality and to hashing.
//!
//! The values a column can hold: any `i128` values whose spread fits 64 bits (`top - base` at most
//! `u64::MAX`). A result that would spread wider is an error for now (`dev/integers.md`).

// The kernels below are written once and expanded at every lane width, so a cast or conversion to
// `u64` is a no-op in the 64-bit expansion only; clippy sees that one.
#![allow(clippy::unnecessary_cast, clippy::useless_conversion)]

use crate::words::{view, view_mut, words_for, Lane};
use std::sync::Arc;

/// the storage width of a column's offsets.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Width {
    W0,
    W8,
    W16,
    W32,
    W64,
}

impl Width {
    /// the narrowest width that holds every offset up to `span`.
    pub fn of(span: u64) -> Width {
        match span {
            0 => Width::W0,
            1..=0xff => Width::W8,
            0x100..=0xffff => Width::W16,
            0x1_0000..=0xffff_ffff => Width::W32,
            _ => Width::W64,
        }
    }
    pub fn bits(self) -> u32 {
        match self {
            Width::W0 => 0,
            Width::W8 => 8,
            Width::W16 => 16,
            Width::W32 => 32,
            Width::W64 => 64,
        }
    }
    /// the largest offset the width holds.
    pub fn max_offset(self) -> u64 {
        match self {
            Width::W64 => u64::MAX,
            w => (1u64 << w.bits()) - 1,
        }
    }
    fn from_bits(bits: u64) -> Option<Width> {
        Some(match bits {
            0 => Width::W0,
            8 => Width::W8,
            16 => Width::W16,
            32 => Width::W32,
            64 => Width::W64,
            _ => return None,
        })
    }
}

/// a column of integers in a frame of reference (see the module documentation).
#[derive(Clone)]
pub struct Int {
    base: i128,            // the value offset 0 stands for
    span: u64,             // every offset is at most this; `span <= width.max_offset()`
    width: Width,          // the stored lane width
    len: usize,            // rows
    words: Arc<Vec<u64>>,  // the backing words (empty for width 0)
    at: usize,             // row 0 is lane `at` of `words` viewed at `width`
}

/// a column's offsets as a slice at its stored width; `Zero` for a constant column.
pub(crate) enum View<'a> {
    Zero,
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

/// run `$body` with `$s` bound to the column's offset slice, at whichever width it is stored. The
/// body is expanded once per width, so it may be generic code over the lane type. A constant
/// column has no slice; callers handle it first.
macro_rules! with_view {
    ($int:expr, $s:ident => $body:expr) => {
        match $int.view() {
            View::U8($s) => $body,
            View::U16($s) => $body,
            View::U32($s) => $body,
            View::U64($s) => $body,
            View::Zero => unreachable!("with_view: a constant column has no storage"),
        }
    };
}

/// run `$body` with the type `$T` set to the lane type of width `$w` (not `W0`).
macro_rules! with_lane {
    ($w:expr, $T:ident => $body:expr) => {
        match $w {
            Width::W8 => {
                #[allow(dead_code)]
                type $T = u8;
                $body
            }
            Width::W16 => {
                #[allow(dead_code)]
                type $T = u16;
                $body
            }
            Width::W32 => {
                #[allow(dead_code)]
                type $T = u32;
                $body
            }
            Width::W64 => {
                #[allow(dead_code)]
                type $T = u64;
                $body
            }
            Width::W0 => unreachable!("with_lane: width 0 stores nothing"),
        }
    };
}

/// the bound a frame describes: `(base, top)` with `top = base + span`.
fn top(base: i128, span: u64) -> i128 {
    base + span as i128
}

/// the frame covering two frames, if its spread fits 64 bits.
fn union(a: (i128, u64), b: (i128, u64)) -> Option<(i128, u64)> {
    let base = a.0.min(b.0);
    let hi = top(a.0, a.1).max(top(b.0, b.1));
    u64::try_from(hi - base).ok().map(|span| (base, span))
}

/// the error a result whose values spread more than 64 bits reports.
fn too_wide(what: &str) -> String {
    format!("{what}: the integers spread over more than 2^64 values, which a column cannot hold yet")
}

/// the order of two values as `-1`/`0`/`+1`.
#[inline]
fn sign<T: Ord>(x: T, y: T) -> i8 {
    (x > y) as i8 - (x < y) as i8
}

/// fill a fresh word buffer with `n` lanes of `T` written by `f`.
fn alloc<T: Lane>(n: usize, f: impl FnOnce(&mut [T])) -> Arc<Vec<u64>> {
    let mut w = vec![0u64; words_for(n, T::BITS)];
    f(&mut view_mut::<T>(&mut w)[..n]);
    Arc::new(w)
}

impl Int {
    // ---- construction ---------------------------------------------------------------------

    /// `n` rows of the value `v`: stores nothing.
    pub fn constant(v: i128, n: usize) -> Int {
        Int { base: v, span: 0, width: Width::W0, len: n, words: Arc::new(Vec::new()), at: 0 }
    }

    /// the empty column.
    pub fn empty() -> Int {
        Int::constant(0, 0)
    }

    /// a column from offsets already written at `width`, in `words` from lane `at`.
    pub(crate) fn from_parts(base: i128, span: u64, width: Width, len: usize, words: Arc<Vec<u64>>, at: usize) -> Int {
        debug_assert!(span <= width.max_offset(), "Int: span {span} exceeds width {width:?}");
        debug_assert!(width == Width::W0 || (at + len) * width.bits() as usize <= words.len() * 64, "Int: window outside its words");
        Int { base, span, width, len, words, at }
    }

    /// host data, narrowed: one pass for the range and one to pack the offsets, written into the
    /// front of `v`'s own allocation (no new buffer). A column whose range needs all 64 bits and
    /// starts at 0 is adopted as it is.
    pub fn from_u64s(mut v: Vec<u64>) -> Int {
        let n = v.len();
        let Some((lo, hi)) = min_max(&v) else { return Int::empty() };
        let span = hi - lo;
        let width = Width::of(span);
        match width {
            Width::W0 => Int::constant(lo as i128, n),
            Width::W64 => {
                if lo != 0 {
                    v.iter_mut().for_each(|x| *x -= lo);
                }
                Int::from_parts(lo as i128, span, width, n, Arc::new(v), 0)
            }
            _ => {
                pack_in_place(&mut v, lo, width.bits());
                v.truncate(words_for(n, width.bits()));
                Int::from_parts(lo as i128, span, width, n, Arc::new(v), 0)
            }
        }
    }

    /// host data stored as it is, without narrowing: base 0, width 64, and a span that promises
    /// nothing. No pass and no copy; kernels that need a tighter frame compute it.
    pub fn adopt_u64s(v: Vec<u64>) -> Int {
        let n = v.len();
        Int::from_parts(0, u64::MAX, Width::W64, n, Arc::new(v), 0)
    }

    /// signed host data, narrowed.
    pub fn from_i64s(v: &[i64]) -> Int {
        let Some((lo, hi)) = v.iter().fold(None, |m: Option<(i64, i64)>, &x| {
            Some(m.map_or((x, x), |(a, b)| (a.min(x), b.max(x))))
        }) else {
            return Int::empty();
        };
        let span = (hi as i128 - lo as i128) as u64;
        Int::from_offsets(lo as i128, span, v.len(), |i| (v[i] as i128 - lo as i128) as u64)
    }

    /// any integers whose spread fits 64 bits, narrowed; the error otherwise.
    pub fn from_i128s(v: &[i128]) -> Result<Int, String> {
        let Some((lo, hi)) = v.iter().fold(None, |m: Option<(i128, i128)>, &x| {
            Some(m.map_or((x, x), |(a, b)| (a.min(x), b.max(x))))
        }) else {
            return Ok(Int::empty());
        };
        let span = u64::try_from(hi - lo).map_err(|_| too_wide("Int::from_i128s"))?;
        Ok(Int::from_offsets(lo, span, v.len(), |i| (v[i] - lo) as u64))
    }

    /// a column in the frame `(base, span)` at its narrowest width, row `i`'s offset being `off(i)`.
    fn from_offsets(base: i128, span: u64, n: usize, off: impl Fn(usize) -> u64) -> Int {
        let width = Width::of(span);
        if width == Width::W0 {
            return Int::constant(base, n);
        }
        let words = with_lane!(width, T => alloc::<T>(n, |o| o.iter_mut().enumerate().for_each(|(i, x)| *x = T::from_u64(off(i)))));
        Int::from_parts(base, span, width, n, words, 0)
    }

    // ---- reading ----------------------------------------------------------------------------

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn base(&self) -> i128 {
        self.base
    }
    pub fn span(&self) -> u64 {
        self.span
    }
    pub fn width(&self) -> Width {
        self.width
    }
    /// the value row 0's offset would have to exceed for a row to be out of the frame.
    fn frame(&self) -> (i128, u64) {
        (self.base, self.span)
    }
    /// do the two columns read their offsets the same way (same base, same lanes)?
    fn same_encoding(&self, other: &Int) -> bool {
        self.base == other.base && self.width == other.width
    }
    /// is the backing buffer shared with anything else (another column, a decoded message)?
    pub fn shares_buffer(&self, other: &Int) -> bool {
        Arc::ptr_eq(&self.words, &other.words)
    }
    /// the backing buffer's identity, for tests that check a decode viewed rather than copied.
    pub fn buffer_ptr(&self) -> *const u64 {
        self.words.as_ptr()
    }

    pub(crate) fn view(&self) -> View<'_> {
        let (a, n) = (self.at, self.len);
        match self.width {
            Width::W0 => View::Zero,
            Width::W8 => View::U8(&view::<u8>(&self.words)[a..a + n]),
            Width::W16 => View::U16(&view::<u16>(&self.words)[a..a + n]),
            Width::W32 => View::U32(&view::<u32>(&self.words)[a..a + n]),
            Width::W64 => View::U64(&view::<u64>(&self.words)[a..a + n]),
        }
    }

    /// row `i`'s offset.
    #[inline]
    pub(crate) fn off(&self, i: usize) -> u64 {
        match self.view() {
            View::Zero => {
                assert!(i < self.len, "Int: row {i} of {}", self.len);
                0
            }
            View::U8(s) => s[i] as u64,
            View::U16(s) => s[i] as u64,
            View::U32(s) => s[i] as u64,
            View::U64(s) => s[i],
        }
    }

    /// row `i`'s value.
    #[inline]
    pub fn get(&self, i: usize) -> i128 {
        self.base + self.off(i) as i128
    }

    /// every value, in row order.
    pub fn values(&self) -> Vec<i128> {
        (0..self.len).map(|i| self.get(i)).collect()
    }

    /// the least and greatest offsets actually present.
    fn offset_range(&self) -> Option<(u64, u64)> {
        if self.len == 0 {
            return None;
        }
        if self.width == Width::W0 {
            return Some((0, 0));
        }
        Some(with_view!(self, s => {
            let (lo, hi) = s.iter().fold((u64::MAX, 0u64), |(lo, hi), &x| (lo.min(x as u64), hi.max(x as u64)));
            (lo, hi)
        }))
    }

    // ---- re-encoding ------------------------------------------------------------------------

    /// the same values at the tightest frame and the narrowest width that holds it. Shares the
    /// storage when only the bound tightens; a pass otherwise. The `narrow` op.
    pub fn narrow(&self) -> Int {
        let Some((lo, hi)) = self.offset_range() else { return Int::empty() };
        let span = hi - lo;
        let width = Width::of(span);
        if lo == 0 && width == self.width {
            let mut out = self.clone();
            out.span = span;
            return out;
        }
        self.reencode(self.base + lo as i128, span, width)
    }

    /// the same values in the frame `(base, span)` at `width`. The frame must hold every value
    /// (a wider frame than the column's, or the tighter one `narrow` measured). No copy when the
    /// base and width already match.
    pub(crate) fn reencode(&self, base: i128, span: u64, width: Width) -> Int {
        debug_assert!((0..self.len).all(|i| (base..=top(base, span)).contains(&self.get(i))), "reencode: a value outside the frame");
        debug_assert!(span <= width.max_offset());
        if width == Width::W0 {
            return Int::constant(base, self.len);
        }
        if base == self.base && width == self.width {
            let mut out = self.clone();
            out.span = span;
            return out;
        }
        // the shift between the frames, modulo 2^64: negative when narrowing to a higher base, and
        // every shifted offset lands in `0..=span`, so the wrapping sum is exact.
        let delta = (self.base - base) as u64;
        let n = self.len;
        let words = with_lane!(width, D => {
            if self.width == Width::W0 {
                alloc::<D>(n, |o| o.fill(D::from_u64(delta)))
            } else {
                with_view!(self, s => alloc::<D>(n, |o| {
                    for (d, &x) in o.iter_mut().zip(s) {
                        *d = D::from_u64((x as u64).wrapping_add(delta));
                    }
                }))
            }
        });
        Int::from_parts(base, span, width, n, words, 0)
    }

    /// the same values with every one moved by `c`: a new base, the same storage. What adding a
    /// constant costs.
    pub fn shifted(mut self, c: i128) -> Int {
        self.base += c;
        self
    }

    /// two columns in one encoding, so that a kernel can compare or merge their offsets
    /// directly. The frame is the union of theirs; a side whose base and width already serve it
    /// is not copied. `None` when the union spreads wider than 64 bits even once both are
    /// narrowed.
    pub(crate) fn unify(a: &Int, b: &Int) -> Option<(Int, Int)> {
        if a.same_encoding(b) {
            let span = a.span.max(b.span);
            let (mut x, mut y) = (a.clone(), b.clone());
            x.span = span;
            y.span = span;
            return Some((x, y));
        }
        let (a, b) = match union(a.frame(), b.frame()) {
            Some(_) => (a.clone(), b.clone()),
            None => (a.narrow(), b.narrow()),
        };
        let (base, span) = union(a.frame(), b.frame())?;
        let need = Width::of(span);
        // prefer an encoding one side already has, so that side is not copied
        let width = if a.base == base && a.width >= need {
            a.width
        } else if b.base == base && b.width >= need {
            b.width
        } else {
            need
        };
        Some((a.reencode(base, span, width), b.reencode(base, span, width)))
    }

    /// many columns in one encoding (see [`Int::unify`]).
    pub(crate) fn unify_all(cols: &[&Int]) -> Option<Vec<Int>> {
        let frame = |cs: &[Int]| cs[1..].iter().try_fold(cs[0].frame(), |f, c| union(f, c.frame()));
        let mut owned: Vec<Int> = cols.iter().map(|&c| c.clone()).collect();
        if owned.is_empty() {
            return None;
        }
        let (base, span) = match frame(&owned) {
            Some(f) => f,
            None => {
                owned = owned.iter().map(Int::narrow).collect();
                frame(&owned)?
            }
        };
        let need = Width::of(span);
        let width = owned.iter().filter(|c| c.base == base && c.width >= need).map(|c| c.width).min().unwrap_or(need);
        Some(owned.iter().map(|c| c.reencode(base, span, width)).collect())
    }

    // ---- row movement -----------------------------------------------------------------------

    /// row `j` of the result is row `idx[j]`; same encoding.
    pub(crate) fn gather(&self, idx: &[usize]) -> Int {
        if self.width == Width::W0 {
            if let Some(&i) = idx.iter().find(|&&i| i >= self.len) {
                panic!("gather: row {i} of {}", self.len);
            }
            return Int::constant(self.base, idx.len());
        }
        let words = with_view!(self, s => alloc(idx.len(), |o| {
            for (d, &i) in o.iter_mut().zip(idx) {
                *d = s[i];
            }
        }));
        Int::from_parts(self.base, self.span, self.width, idx.len(), words, 0)
    }

    /// `n` copies of row `i`: a constant column, which stores nothing.
    pub(crate) fn repeat(&self, i: usize, n: usize) -> Int {
        if n == 0 {
            return Int::constant(self.base, 0);
        }
        Int::constant(self.get(i), n)
    }

    /// result row `k` is row `off[k]` of `srcs[tags[k]]`. The sources are brought to one encoding
    /// first (a source already in it is not copied).
    pub(crate) fn gather_lanes(srcs: &[&Int], tags: &[usize], off: &[usize]) -> Int {
        let cols = Int::unify_all(srcs).unwrap_or_else(|| panic!("{}", too_wide("gather_lanes")));
        let (base, span, width) = (cols[0].base, cols[0].span, cols[0].width);
        if width == Width::W0 {
            return Int::constant(base, tags.len());
        }
        let words = with_lane!(width, T => {
            let slices: Vec<&[T]> = cols.iter().map(lanes_of::<T>).collect();
            alloc::<T>(tags.len(), |o| {
                for (d, (&t, &k)) in o.iter_mut().zip(tags.iter().zip(off)) {
                    *d = slices[t][k];
                }
            })
        });
        Int::from_parts(base, span, width, tags.len(), words, 0)
    }

    /// row `i` from `self` where `pick[i]` is nonzero, else from `other`.
    pub(crate) fn blend(self, other: Int, pick: &[u64]) -> Int {
        let (a, b) = Int::unify(&self, &other).unwrap_or_else(|| panic!("{}", too_wide("select")));
        if a.width == Width::W0 {
            return a; // both constant at one base: every row is that value
        }
        let words = with_lane!(a.width, T => {
            let (x, y) = (lanes_of::<T>(&a), lanes_of::<T>(&b));
            alloc::<T>(a.len, |o| {
                for (i, d) in o.iter_mut().enumerate() {
                    *d = if pick[i] != 0 { x[i] } else { y[i] };
                }
            })
        });
        Int::from_parts(a.base, a.span, a.width, a.len, words, 0)
    }

    /// overwrite rows `active[p]` with `src`'s row `p`, re-encoding `self` first if `src`'s values
    /// fall outside its frame (the fold accumulator's update).
    pub(crate) fn scatter_into(&mut self, active: &[usize], src: &Int) {
        let (mut a, b) = Int::unify(self, src).unwrap_or_else(|| panic!("{}", too_wide("fold")));
        if a.width == Width::W0 {
            *self = a;
            return;
        }
        let n = a.len;
        if a.at != 0 {
            // a window of a shared buffer: take the rows into a buffer of their own first
            a.words = with_lane!(a.width, T => {
                let s = lanes_of::<T>(&a);
                alloc::<T>(n, |o| o.copy_from_slice(s))
            });
            a.at = 0;
        }
        with_lane!(a.width, T => {
            let src = lanes_of::<T>(&b);
            let w = Arc::make_mut(&mut a.words);
            let dst = &mut view_mut::<T>(w)[..n];
            for (p, &r) in active.iter().enumerate() {
                dst[r] = src[p];
            }
        });
        *self = a;
    }

    /// append columns end to end. (Test-only, as `Prim::concat` is: the `gather_lanes` oracle.)
    #[cfg(test)]
    pub(crate) fn concat(parts: &[&Int]) -> Int {
        let cols = Int::unify_all(parts).expect("concat: spread");
        let n: usize = cols.iter().map(|c| c.len).sum();
        let mut vals = Vec::with_capacity(n);
        for c in &cols {
            vals.extend(c.values());
        }
        Int::from_i128s(&vals).unwrap()
    }

    // ---- order --------------------------------------------------------------------------------

    /// the rows `index[..]`' offsets, widened to `u64` and appended to `out`: the sort's one
    /// indirect read.
    pub(crate) fn pull_u64(&self, index: &[usize], out: &mut Vec<u64>) {
        if self.width == Width::W0 {
            out.extend(index.iter().map(|_| 0u64));
            return;
        }
        with_view!(self, s => out.extend(index.iter().map(|&i| s[i] as u64)))
    }

    /// `keys[q] = (keys[q] << bits) | offset of row index[q]`, at the column's significant bits.
    pub(crate) fn pack_u64(&self, index: &[usize], keys: &mut [u64]) {
        let bits = self.key_bits();
        if bits == 0 {
            return;
        }
        with_view!(self, s => {
            for (k, &i) in keys.iter_mut().zip(index) {
                *k = (*k << bits) | s[i] as u64;
            }
        })
    }

    /// how many bits a sort key needs for this column's offsets: those of its span. A column
    /// built from narrow data takes fewer bits than its storage, and a constant takes none.
    pub(crate) fn key_bits(&self) -> u32 {
        64 - self.span.leading_zeros()
    }

    /// a column in this one's encoding holding the offsets `keys` yields (the sorted column).
    pub(crate) fn with_keys(&self, n: usize, keys: impl Iterator<Item = u64>) -> Int {
        if self.width == Width::W0 {
            return Int::constant(self.base, n);
        }
        let words = with_lane!(self.width, T => alloc::<T>(n, |o| {
            for (d, k) in o.iter_mut().zip(keys) {
                *d = T::from_u64(k);
            }
        }));
        Int::from_parts(self.base, self.span, self.width, n, words, 0)
    }

    /// the order of row `ia[k]` of `self` against row `ib[k]` of `other`, for every `k`. One
    /// encoding: the offsets, at their width. Otherwise the values.
    pub(crate) fn cmp_idx(&self, ia: &[usize], ib: &[usize], other: &Int) -> Vec<i8> {
        if self.same_encoding(other) {
            return match (self.view(), other.view()) {
                (View::Zero, View::Zero) => vec![0; ia.len()],
                (View::U8(a), View::U8(b)) => ia.iter().zip(ib).map(|(&i, &j)| sign(a[i], b[j])).collect(),
                (View::U16(a), View::U16(b)) => ia.iter().zip(ib).map(|(&i, &j)| sign(a[i], b[j])).collect(),
                (View::U32(a), View::U32(b)) => ia.iter().zip(ib).map(|(&i, &j)| sign(a[i], b[j])).collect(),
                (View::U64(a), View::U64(b)) => ia.iter().zip(ib).map(|(&i, &j)| sign(a[i], b[j])).collect(),
                _ => unreachable!("same encoding, same width"),
            };
        }
        ia.iter().zip(ib).map(|(&i, &j)| sign(self.get(i), other.get(j))).collect()
    }

    /// the order of row `k` of `self` against row `k + skew` of `other`, for `n` rows.
    pub(crate) fn cmp_dense(&self, other: &Int, n: usize, skew: usize) -> Vec<i8> {
        if self.same_encoding(other) {
            return match (self.view(), other.view()) {
                (View::Zero, View::Zero) => vec![0; n],
                (View::U8(a), View::U8(b)) => (0..n).map(|k| sign(a[k], b[k + skew])).collect(),
                (View::U16(a), View::U16(b)) => (0..n).map(|k| sign(a[k], b[k + skew])).collect(),
                (View::U32(a), View::U32(b)) => (0..n).map(|k| sign(a[k], b[k + skew])).collect(),
                (View::U64(a), View::U64(b)) => (0..n).map(|k| sign(a[k], b[k + skew])).collect(),
                _ => unreachable!("same encoding, same width"),
            };
        }
        (0..n).map(|k| sign(self.get(k), other.get(k + skew))).collect()
    }

    /// lane-wise relational compare to a 0/1 mask, the three order flags resolved by the caller.
    pub(crate) fn rel(&self, other: &Int, lt: bool, eq: bool, gt: bool) -> Vec<u64> {
        let test = move |o: i8| ((lt & (o < 0)) | (eq & (o == 0)) | (gt & (o > 0))) as u64;
        match Int::unify(self, other) {
            Some((a, _)) if a.width == Width::W0 => vec![test(0); a.len],
            Some((a, b)) => with_lane!(a.width, T => {
                let (x, y) = (lanes_of::<T>(&a), lanes_of::<T>(&b));
                x.iter().zip(y).map(|(p, q)| ((lt & (p < q)) | (eq & (p == q)) | (gt & (p > q))) as u64).collect()
            }),
            None => (0..self.len).map(|i| test(sign(self.get(i), other.get(i)))).collect(),
        }
    }

    /// `x > c` per row, as a 0/1 mask: one comparison against `c`'s offset in this frame.
    pub(crate) fn gt_const(&self, c: i128) -> Vec<u64> {
        let rel = c - self.base;
        if rel < 0 {
            return vec![1; self.len];
        }
        if rel > self.span as i128 || self.width == Width::W0 {
            return vec![(0 > rel) as u64; self.len];
        }
        let t = rel as u64;
        with_view!(self, s => s.iter().map(|&x| (x as u64 > t) as u64).collect())
    }

    /// lane-wise min (`take_max` false) or max of two columns.
    pub(crate) fn lane_pick(self, other: Int, take_max: bool) -> Int {
        let (a, b) = Int::unify(&self, &other).unwrap_or_else(|| panic!("{}", too_wide("min/max")));
        if a.width == Width::W0 {
            return a;
        }
        let words = with_lane!(a.width, T => {
            let (x, y) = (lanes_of::<T>(&a), lanes_of::<T>(&b));
            alloc::<T>(a.len, |o| {
                for (d, (&p, &q)) in o.iter_mut().zip(x.iter().zip(y)) {
                    *d = if take_max { p.max(q) } else { p.min(q) };
                }
            })
        });
        Int::from_parts(a.base, a.span, a.width, a.len, words, 0)
    }

    /// the needles `self` re-encoded in the haystack's encoding, for a search: each needle's
    /// offset, or `Below`/`Above` when its value lies outside the haystack's frame (it then
    /// equals nothing there, and its range is empty at the row's start or end). The haystack is
    /// never copied.
    pub(crate) fn needles_in(&self, hay: &Int) -> (Int, Vec<Place>) {
        let n = self.len;
        let mut places = vec![Place::In; n];
        let lo = hay.base;
        let hi = top(hay.base, hay.span);
        // the common case: the needles' frame already lies inside the haystack's
        if lo <= self.base && top(self.base, self.span) <= hi {
            return (self.reencode(hay.base, hay.span, hay.width), places);
        }
        let offs: Vec<u64> = (0..n)
            .map(|i| {
                let v = self.get(i);
                if v < lo {
                    places[i] = Place::Below;
                    0
                } else if v > hi {
                    places[i] = Place::Above;
                    0
                } else {
                    (v - lo) as u64
                }
            })
            .collect();
        let out = if hay.width == Width::W0 {
            Int::constant(hay.base, n)
        } else {
            let words = with_lane!(hay.width, T => alloc::<T>(n, |o| {
                for (d, &x) in o.iter_mut().zip(&offs) {
                    *d = T::from_u64(x);
                }
            }));
            Int::from_parts(hay.base, hay.span, hay.width, n, words, 0)
        };
        (out, places)
    }

    // ---- hashing and display ----------------------------------------------------------------

    /// one stable hash per row, by value: an integer in `0..2^64` hashes as the `u64` leaf of the
    /// same value does, whatever this column's encoding.
    pub(crate) fn hashes(&self) -> Vec<u64> {
        let mut out = vec![0u64; self.len];
        self.fold_hashes(&mut out, |_, h| h);
        out
    }

    /// fold each row's hash into `acc`.
    pub(crate) fn fold_hashes(&self, acc: &mut [u64], mut fold: impl FnMut(u64, u64) -> u64) {
        use crate::hash::mix64;
        let unsigned = self.base >= 0 && top(self.base, self.span) <= u64::MAX as i128;
        if unsigned && self.width != Width::W0 {
            let b = self.base as u64;
            with_view!(self, s => {
                for (a, &x) in acc.iter_mut().zip(s) {
                    *a = fold(*a, mix64(b + x as u64));
                }
            })
        } else {
            for (i, a) in acc.iter_mut().enumerate().take(self.len) {
                *a = fold(*a, hash_value(self.get(i)));
            }
        }
    }

    pub(crate) fn show(&self) -> String {
        format!("{:?}", self.values())
    }
}

/// one integer's hash: as a `u64` leaf's when it is in `0..2^64`, else mixed with its high bits.
pub(crate) fn hash_value(v: i128) -> u64 {
    use crate::hash::mix64;
    if (0..=u64::MAX as i128).contains(&v) {
        mix64(v as u64)
    } else {
        mix64((v as u64) ^ mix64((v >> 64) as u64 ^ 0x9e37_79b9_7f4a_7c15))
    }
}

/// where a needle lies relative to a haystack's frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Place {
    Below,
    In,
    Above,
}

/// a column's offsets as `T`s; the column must be stored at `T`'s width.
fn lanes_of<T: Lane>(c: &Int) -> &[T] {
    debug_assert_eq!(c.width.bits(), T::BITS);
    &view::<T>(&c.words)[c.at..c.at + c.len]
}

/// the least and greatest of `v`.
fn min_max(v: &[u64]) -> Option<(u64, u64)> {
    if v.is_empty() {
        return None;
    }
    Some(v.iter().fold((u64::MAX, 0u64), |(lo, hi), &x| (lo.min(x), hi.max(x))))
}

/// pack `v[i] - lo` at `bits` bits into the front of `v` itself, in the lane order a word view
/// reads them back in. Word `k` holds elements `per*k ..`, so it is written at or before the
/// first of them is read; the writes `m..per*m` and the reads they need, `per*m..per*per*m`, are
/// disjoint, so the pass runs in blocks of geometrically growing size, each a plain loop over
/// two separate slices (which vectorizes, where one loop over the aliased buffer would not).
fn pack_in_place(v: &mut [u64], lo: u64, bits: u32) {
    match bits {
        8 => pack_blocks::<8>(v, lo),
        16 => pack_blocks::<16>(v, lo),
        32 => pack_blocks::<32>(v, lo),
        _ => unreachable!("pack_in_place: {bits}-bit lanes"),
    }
}

fn pack_blocks<const B: u32>(v: &mut [u64], lo: u64) {
    let per = (64 / B) as usize;
    let n = v.len();
    let words = n.div_ceil(per);
    if words == 0 {
        return;
    }
    let shift = |j: usize| if cfg!(target_endian = "little") { B * j as u32 } else { 64 - B * (j as u32 + 1) };
    let pack = |src: &[u64]| src.iter().enumerate().fold(0u64, |w, (j, &x)| w | (x - lo) << shift(j));
    v[0] = pack(&v[..per.min(n)]);
    let mut m = 1;
    while m < words {
        let end = (per * m).min(words);
        let (head, tail) = v.split_at_mut(per * m);
        let full = (end - m).min(tail.len() / per);
        for (d, src) in head[m..m + full].iter_mut().zip(tail.chunks_exact(per)) {
            *d = pack(src);
        }
        if m + full < end {
            head[m + full] = pack(&tail[per * full..]);
        }
        m = end;
    }
}

// ---- arithmetic -------------------------------------------------------------------------------
//
// The policy (dev/integers.md): a result's frame comes from its operands' frames before any row is
// read — interval arithmetic on (base, span) — so the per-row loop has no overflow check and no
// branch. When the bound would spread past 64 bits, the operands are narrowed to their actual
// ranges and the bound recomputed; only if that still does not fit is the result computed exactly
// and, if its values genuinely spread past 64 bits, reported as an error.

/// which binary op, for the shared driver.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum IntBin {
    Add,
    Sub,
    Mul,
}

impl IntBin {
    fn name(self) -> &'static str {
        match self {
            IntBin::Add => "add_int",
            IntBin::Sub => "sub_int",
            IntBin::Mul => "mul_int",
        }
    }
    /// the exact value of one row.
    fn exact(self, x: i128, y: i128) -> Option<i128> {
        match self {
            IntBin::Add => x.checked_add(y),
            IntBin::Sub => x.checked_sub(y),
            IntBin::Mul => x.checked_mul(y),
        }
    }
}

/// the result frame of `op` over two operand frames, and the per-row offset formula's constants,
/// when the frame fits 64 bits. `Plan::Exact` when it does not (or a base is negative for `Mul`).
enum Plan {
    /// result base and span; `r = x + y`
    Add(i128, u64),
    /// result base and span; `r = x + (sb - y)`
    Sub(i128, u64, u64),
    /// result base and span; `r = ba*y + bb*x + x*y` (both bases non-negative)
    Mul(i128, u64, u64, u64),
    Exact,
}

fn plan(op: IntBin, a: (i128, u64), b: (i128, u64)) -> Plan {
    let ((ba, sa), (bb, sb)) = (a, b);
    let fits = |s: u128| u64::try_from(s).ok();
    match op {
        IntBin::Add => match (ba.checked_add(bb), fits(sa as u128 + sb as u128)) {
            (Some(base), Some(span)) => Plan::Add(base, span),
            _ => Plan::Exact,
        },
        IntBin::Sub => match (ba.checked_sub(bb).and_then(|d| d.checked_sub(sb as i128)), fits(sa as u128 + sb as u128)) {
            (Some(base), Some(span)) => Plan::Sub(base, span, sb),
            _ => Plan::Exact,
        },
        IntBin::Mul => {
            if ba < 0 || bb < 0 {
                return Plan::Exact;
            }
            let (ta, tb) = (top(ba, sa), top(bb, sb));
            match (ba.checked_mul(bb), ta.checked_mul(tb)) {
                (Some(base), Some(t)) => match (u64::try_from(t - base), u64::try_from(ba), u64::try_from(bb)) {
                    (Ok(span), Ok(ba), Ok(bb)) => Plan::Mul(base, span, ba, bb),
                    _ => Plan::Exact,
                },
                _ => Plan::Exact,
            }
        }
    }
}

/// `op` over two columns of one length.
pub fn int_bin(op: IntBin, a: Int, b: Int) -> Result<Int, String> {
    assert_eq!(a.len, b.len, "{}: operands at different strata", op.name());
    // a constant operand: add and subtract move the base and touch nothing.
    match (op, a.width, b.width) {
        (IntBin::Add, _, Width::W0) => return Ok(a.shifted(b.base)),
        (IntBin::Add, Width::W0, _) => return Ok(b.shifted(a.base)),
        (IntBin::Sub, _, Width::W0) => return Ok(a.shifted(-b.base)),
        _ => {}
    }
    let mut p = plan(op, a.frame(), b.frame());
    let (a, b) = if matches!(p, Plan::Exact) {
        // the bound was loose: narrow both to what they hold and plan again.
        let (a, b) = (a.narrow(), b.narrow());
        p = plan(op, a.frame(), b.frame());
        (a, b)
    } else {
        (a, b)
    };
    match p {
        Plan::Add(base, span) => Ok(lanes2(a, b, base, span, |x, y| x + y)),
        Plan::Sub(base, span, sb) => Ok(lanes2(a, b, base, span, move |x, y| x + (sb - y))),
        // both bases zero (unsigned data narrowed from 0): one widening multiply per row
        Plan::Mul(base, span, 0, 0) => Ok(lanes2(a, b, base, span, |x, y| x * y)),
        Plan::Mul(base, span, ba, bb) => Ok(lanes2(a, b, base, span, move |x, y| ba * y + bb * x + x * y)),
        Plan::Exact => {
            let vals: Option<Vec<i128>> = (0..a.len).map(|i| op.exact(a.get(i), b.get(i))).collect();
            let vals = vals.ok_or_else(|| too_wide(op.name()))?;
            Int::from_i128s(&vals).map_err(|_| too_wide(op.name()))
        }
    }
}

/// the dense binary kernel: `r = f(x, y)` per row, the result in the frame `(base, span)` at its
/// narrowest width (or the operands', if wider). The operands are brought to one storage width;
/// then one loop per (operand width, result width) pair, with no check and no branch. Writes into
/// the left operand's buffer when it is the only holder and the result keeps its width.
fn lanes2(a: Int, b: Int, base: i128, span: u64, f: impl Fn(u64, u64) -> u64 + Copy) -> Int {
    let n = a.len;
    // one operand width (the wider); a constant operand is stored at it for the loop's sake.
    let ow = a.width.max(b.width).max(Width::W8);
    let a = if a.width == ow { a } else { a.reencode(a.base, a.span, ow) };
    let b = if b.width == ow { b } else { b.reencode(b.base, b.span, ow) };
    let rw = Width::of(span).max(ow);
    if rw == Width::W0 {
        return Int::constant(base, n);
    }
    // in place: same width out as in, and nobody else holds `a`'s words.
    if rw == ow && a.at == 0 {
        let mut a = a;
        if let Some(w) = Arc::get_mut(&mut a.words) {
            with_lane!(ow, T => {
                let y = lanes_of::<T>(&b);
                for (d, &q) in view_mut::<T>(w)[..n].iter_mut().zip(y) {
                    *d = T::from_u64(f((*d).into(), q.into()));
                }
            });
            a.base = base;
            a.span = span;
            return a;
        }
        return lanes2_fresh(&a, &b, base, span, rw, f);
    }
    lanes2_fresh(&a, &b, base, span, rw, f)
}

fn lanes2_fresh(a: &Int, b: &Int, base: i128, span: u64, rw: Width, f: impl Fn(u64, u64) -> u64 + Copy) -> Int {
    let n = a.len;
    let words = with_lane!(a.width, S => {
        let (x, y) = (lanes_of::<S>(a), lanes_of::<S>(b));
        with_lane!(rw, R => alloc::<R>(n, |o| {
            for (d, (&p, &q)) in o.iter_mut().zip(x.iter().zip(y)) {
                *d = R::from_u64(f(p.into(), q.into()));
            }
        }))
    });
    Int::from_parts(base, span, rw, n, words, 0)
}

// ---- equality, hashing, debug: by value, never by encoding --------------------------------------

impl PartialEq for Int {
    fn eq(&self, other: &Int) -> bool {
        if self.len != other.len {
            return false;
        }
        if self.same_encoding(other) {
            return match (self.view(), other.view()) {
                (View::Zero, View::Zero) => true,
                (View::U8(a), View::U8(b)) => a == b,
                (View::U16(a), View::U16(b)) => a == b,
                (View::U32(a), View::U32(b)) => a == b,
                (View::U64(a), View::U64(b)) => a == b,
                _ => unreachable!(),
            };
        }
        (0..self.len).all(|i| self.get(i) == other.get(i))
    }
}
impl Eq for Int {}

impl std::hash::Hash for Int {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.len.hash(state);
        for i in 0..self.len {
            self.get(i).hash(state);
        }
    }
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int(base {}, span {}, {:?}, {:?})", self.base, self.span, self.width, self.values())
    }
}

// ---- the codec's leaf -------------------------------------------------------------------------

/// the words a codec writes for a leaf: base (two words), span, width bits, length, then the
/// offsets' bytes padded to a word. Kept here so the codec never sees the storage.
pub(crate) fn encoded_len(c: &Int) -> usize {
    40 + (c.len * c.width.bits() as usize / 8).div_ceil(8) * 8
}

pub(crate) fn write_int<W: std::io::Write>(c: &Int, writer: &mut W) -> std::io::Result<()> {
    let w = |writer: &mut W, x: u64| writer.write_all(&x.to_le_bytes());
    w(writer, c.base as u64)?;
    w(writer, (c.base >> 64) as u64)?;
    w(writer, c.span)?;
    w(writer, c.width.bits() as u64)?;
    w(writer, c.len as u64)?;
    if c.width == Width::W0 {
        return Ok(());
    }
    let nb = c.len * c.width.bits() as usize / 8;
    if cfg!(target_endian = "little") {
        let bytes = view::<u8>(&c.words);
        let from = c.at * c.width.bits() as usize / 8;
        writer.write_all(&bytes[from..from + nb])?;
    } else {
        for i in 0..c.len {
            let x = c.off(i).to_le_bytes();
            writer.write_all(&x[..c.width.bits() as usize / 8])?;
        }
    }
    let pad = nb.div_ceil(8) * 8 - nb;
    if pad > 0 {
        writer.write_all(&[0u8; 8][..pad])?;
    }
    Ok(())
}

/// the header of an encoded leaf, checked: `(base, span, width, len)`.
pub(crate) fn read_header(words: [u64; 5]) -> Result<(i128, u64, Width, usize), String> {
    let base = (words[0] as u128 | (words[1] as u128) << 64) as i128;
    let span = words[2];
    let width = Width::from_bits(words[3]).ok_or_else(|| format!("corgi::bytes: bad Int width {}", words[3]))?;
    if span > width.max_offset() {
        return Err(format!("corgi::bytes: Int span {span} exceeds its width {}", width.bits()));
    }
    if base.checked_add(span as i128).is_none() {
        return Err("corgi::bytes: Int frame overflows".into());
    }
    Ok((base, span, width, words[4] as usize))
}

/// an encoded leaf's offsets, copied into a fresh buffer; checks every offset is within `span`.
pub(crate) fn read_copied(base: i128, span: u64, width: Width, len: usize, payload: &[u8]) -> Result<Int, String> {
    if width == Width::W0 {
        return Ok(Int::constant(base, len));
    }
    let mut w = vec![0u64; words_for(len, width.bits())];
    let nb = len * width.bits() as usize / 8;
    if cfg!(target_endian = "little") {
        view_mut::<u8>(&mut w)[..nb].copy_from_slice(&payload[..nb]);
    } else {
        let size = width.bits() as usize / 8;
        with_lane!(width, T => {
            let dst = &mut view_mut::<T>(&mut w)[..len];
            for (d, c) in dst.iter_mut().zip(payload.chunks_exact(size)) {
                let mut b = [0u8; 8];
                b[..size].copy_from_slice(c);
                *d = T::from_u64(u64::from_le_bytes(b));
            }
        });
    }
    let c = Int::from_parts(base, span, width, len, Arc::new(w), 0);
    check_span(&c)?;
    Ok(c)
}

/// an encoded leaf viewed where it lies in the shared message `buf`, starting at byte `byte_at`
/// (word-aligned): nothing is copied. Checks every offset is within `span` (a read pass).
pub(crate) fn read_viewed(base: i128, span: u64, width: Width, len: usize, buf: &Arc<Vec<u64>>, byte_at: usize) -> Result<Int, String> {
    if width == Width::W0 {
        return Ok(Int::constant(base, len));
    }
    let at = byte_at / (width.bits() as usize / 8);
    let c = Int::from_parts(base, span, width, len, buf.clone(), at);
    check_span(&c)?;
    Ok(c)
}

fn check_span(c: &Int) -> Result<(), String> {
    if c.width == Width::W64 && c.span == u64::MAX {
        return Ok(());
    }
    let max = with_view!(c, s => s.iter().fold(0u64, |m, &x| m.max(x as u64)));
    if max > c.span {
        return Err(format!("corgi::bytes: Int offset {max} exceeds its declared span {}", c.span));
    }
    Ok(())
}

/// a column back to `u64`s, if every value is in `0..2^64`.
pub(crate) fn to_u64s(c: &Int) -> Result<Vec<u64>, String> {
    if c.base < 0 || top(c.base, c.span) > u64::MAX as i128 {
        // the frame allows values outside u64; check the actual ones
        let vals = c.values();
        return vals
            .iter()
            .map(|&v| u64::try_from(v).map_err(|_| format!("to_u64: {v} is not in 0..2^64")))
            .collect();
    }
    let b = c.base as u64;
    if c.width == Width::W0 {
        return Ok(vec![b; c.len]);
    }
    Ok(with_view!(c, s => s.iter().map(|&x| b + x as u64).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// a column of `n` values drawn from `[lo, lo + range)`, built every way there is.
    fn columns(rng: &mut Rng, n: usize, lo: i128, range: u64) -> Vec<Int> {
        let vals: Vec<i128> = (0..n).map(|_| lo + (if range == 0 { 0 } else { rng.next() % range }) as i128).collect();
        let mut out = vec![Int::from_i128s(&vals).unwrap()];
        if vals.iter().all(|&v| v >= 0 && v <= u64::MAX as i128) {
            let u: Vec<u64> = vals.iter().map(|&v| v as u64).collect();
            out.push(Int::from_u64s(u.clone()));
            out.push(Int::adopt_u64s(u));
        }
        // a loose frame: wider base below, wider span above
        let c = Int::from_i128s(&vals).unwrap();
        out.push(c.reencode(c.base - 3, c.span.saturating_add(1000).min(u64::MAX - 3), Width::W64));
        out
    }

    #[test]
    fn narrowing_picks_the_width_and_keeps_the_values() {
        let mut rng = Rng(5);
        for (lo, range, w) in [(0i128, 1u64, Width::W0), (7, 200, Width::W8), (-100, 60_000, Width::W16), (1 << 40, 1 << 20, Width::W32), (-(1 << 62), u64::MAX, Width::W64)] {
            for n in [0usize, 1, 7, 8, 9, 1000] {
                let vals: Vec<i128> = (0..n).map(|_| lo + (rng.next() % range) as i128).collect();
                let c = Int::from_i128s(&vals).unwrap();
                assert_eq!(c.values(), vals);
                if n > 1 {
                    assert!(c.width <= w, "{lo} {range}: {:?}", c.width);
                }
                if vals.iter().all(|&v| v >= 0) {
                    let u: Vec<u64> = vals.iter().map(|&v| v as u64).collect();
                    let d = Int::from_u64s(u.clone());
                    assert_eq!(d.values(), vals);
                    assert_eq!(d, c);
                    assert_eq!(Int::adopt_u64s(u).narrow().width, c.width);
                }
            }
        }
    }

    #[test]
    fn narrowing_in_place_reuses_the_allocation() {
        let v: Vec<u64> = (0..1000).map(|i| 5000 + i % 300).collect();
        let p = v.as_ptr();
        let c = Int::from_u64s(v);
        assert_eq!(c.width, Width::W16);
        assert_eq!(c.words.as_ptr(), p);
    }

    #[test]
    fn arithmetic_matches_i128() {
        let mut rng = Rng(9);
        let frames = [(0i128, 1u64), (0, 256), (-5, 100), (1000, 70_000), (-(1 << 40), 1 << 33), (0, u64::MAX), (i64::MIN as i128, u64::MAX / 2)];
        for &(la, ra) in &frames {
            for &(lb, rb) in &frames {
                let n = 37;
                let a0 = &columns(&mut Rng(rng.next() | 1), n, la, ra);
                let b0 = &columns(&mut Rng(rng.next() | 1), n, lb, rb);
                for a in a0 {
                    for b in b0 {
                        for op in [IntBin::Add, IntBin::Sub, IntBin::Mul] {
                            let want: Option<Vec<i128>> = (0..n).map(|i| op.exact(a.get(i), b.get(i))).collect();
                            let got = int_bin(op, a.clone(), b.clone());
                            let fits = want.as_ref().and_then(|w| Int::from_i128s(w).ok());
                            match (fits, got) {
                                (Some(w), Ok(g)) => {
                                    assert_eq!(g.values(), w.values(), "{op:?} {a:?} {b:?}");
                                    assert!(g.span <= g.width.max_offset());
                                    assert!((0..n).all(|i| g.off(i) <= g.span), "{op:?}: offset past span");
                                }
                                (None, Err(_)) => {}
                                (w, g) => panic!("{op:?} {a:?} {b:?}: want {w:?} got {g:?}"),
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn adding_a_constant_moves_only_the_base() {
        let c = Int::from_u64s((0..100).collect());
        let k = Int::constant(-7, 100);
        let r = int_bin(IntBin::Add, c.clone(), k).unwrap();
        assert!(r.shares_buffer(&c));
        assert_eq!(r.values(), (0..100).map(|i| i - 7).collect::<Vec<i128>>());
    }

    #[test]
    fn unify_compares_across_frames() {
        let mut rng = Rng(3);
        for _ in 0..50 {
            let a = &columns(&mut Rng(rng.next() | 1), 20, -50, 300)[0];
            let b = &columns(&mut Rng(rng.next() | 1), 20, 100, 70_000)[0];
            let idx: Vec<usize> = (0..20).collect();
            let want: Vec<i8> = (0..20).map(|i| sign(a.get(i), b.get(i))).collect();
            assert_eq!(a.cmp_idx(&idx, &idx, b), want);
            let (x, y) = Int::unify(a, b).unwrap();
            assert_eq!(x.cmp_idx(&idx, &idx, &y), want);
            assert_eq!(x.values(), a.values());
            assert_eq!(y.values(), b.values());
            assert_eq!(a.rel(b, true, false, false), want.iter().map(|&o| (o < 0) as u64).collect::<Vec<_>>());
        }
    }

    #[test]
    fn gather_lanes_merges_encodings() {
        let a = Int::from_i64s(&[-3, 4, 9]);
        let b = Int::from_u64s(vec![70_000, 1]);
        let c = Int::constant(5, 2);
        let out = Int::gather_lanes(&[&a, &b, &c], &[0, 1, 2, 0, 1], &[2, 0, 1, 0, 1]);
        assert_eq!(out.values(), vec![9, 70_000, 5, -3, 1]);
        assert_eq!(Int::concat(&[&a, &b, &c]).values(), vec![-3, 4, 9, 70_000, 1, 5, 5]);
    }

    #[test]
    fn needles_outside_the_frame_are_placed() {
        let hay = Int::from_u64s(vec![10, 20, 30]);
        let needles = Int::from_i64s(&[5, 10, 25, 31, -1]);
        let (n, places) = needles.needles_in(&hay);
        assert_eq!(places, vec![Place::Below, Place::In, Place::In, Place::Above, Place::Below]);
        assert!(n.same_encoding(&hay));
        assert_eq!(n.get(1), 10);
        assert_eq!(n.get(2), 25);
    }

    #[test]
    fn hashes_agree_with_u64_leaves() {
        let c = Int::from_i64s(&[3, 1000, 7]);
        let u = crate::value::Value::u64(vec![3, 1000, 7]);
        assert_eq!(c.hashes(), crate::hash::hash(&u));
    }
}
