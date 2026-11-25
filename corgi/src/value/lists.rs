//! SPIKE: two compact list partitions, as alternates to one `usize` end per row.
//!
//! `Lens` holds each row's length at the narrowest unsigned width that holds every length, plus the
//! start of every 64th row.
//! A row's start is that block start plus the lengths before it in its block.
//!
//! `Unary` holds one bit per element and one bit per row: each row is its length in one bits, then a
//! zero.
//! Row `i` ends at `select0(i) - i`.
//! A directory of zero counts per 512-bit block, and the block of every 256th zero, answer `select0`.
//!
//! Which form a non-uniform partition takes is chosen by `CORGI_LISTS` (`offsets`, `lens`, `unary`),
//! read once, so the whole crate can be run under each form.

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Form {
    Offsets,
    Lens,
    Unary,
}

/// the form a non-uniform partition takes, from `CORGI_LISTS`.
pub fn form() -> Form {
    static FORM: OnceLock<Form> = OnceLock::new();
    *FORM.get_or_init(|| match std::env::var("CORGI_LISTS").as_deref() {
        Ok("lens") => Form::Lens,
        Ok("unary") => Form::Unary,
        Ok("offsets") | Err(_) => Form::Offsets,
        Ok(other) => panic!("CORGI_LISTS={other}: expected offsets, lens or unary"),
    })
}

// --- Lens ---------------------------------------------------------------------------------------

/// rows per stored block start.
pub const LENS_BLOCK: usize = 64;

/// row lengths at one width: the narrowest that holds the longest row.
#[derive(Clone, Debug)]
pub enum Narrow {
    // behind an `Arc`, so `len` hands the lengths out as a leaf without a copy
    U8(std::sync::Arc<Vec<u8>>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

macro_rules! narrow {
    ($n:expr, $v:ident => $body:expr) => {
        match $n {
            Narrow::U8($v) => $body,
            Narrow::U16($v) => $body,
            Narrow::U32($v) => $body,
            Narrow::U64($v) => $body,
        }
    };
}

impl Narrow {
    /// lengths at the narrowest width that holds `max`.
    pub fn with_max(lens: impl ExactSizeIterator<Item = usize>, max: usize) -> Narrow {
        if max <= u8::MAX as usize {
            Narrow::U8(std::sync::Arc::new(lens.map(|l| l as u8).collect()))
        } else if max <= u16::MAX as usize {
            Narrow::U16(lens.map(|l| l as u16).collect())
        } else if max <= u32::MAX as usize {
            Narrow::U32(lens.map(|l| l as u32).collect())
        } else {
            Narrow::U64(lens.map(|l| l as u64).collect())
        }
    }
    pub fn len(&self) -> usize {
        narrow!(self, v => v.len())
    }
    #[inline]
    pub fn get(&self, i: usize) -> usize {
        narrow!(self, v => v[i] as usize)
    }
    /// the sum of lengths `a..b`.
    #[inline]
    pub fn sum(&self, a: usize, b: usize) -> usize {
        match self {
            // at most 63 terms of a byte fit a u16 lane, which the compiler vectorizes widest
            Narrow::U8(v) if b - a < 256 => v[a..b].iter().map(|&x| x as u16).sum::<u16>() as usize,
            Narrow::U8(v) => v[a..b].iter().map(|&x| x as usize).sum(),
            Narrow::U16(v) => v[a..b].iter().map(|&x| x as u32).sum::<u32>() as usize,
            Narrow::U32(v) => v[a..b].iter().map(|&x| x as usize).sum(),
            Narrow::U64(v) => v[a..b].iter().map(|&x| x as usize).sum(),
        }
    }
    pub fn bytes(&self) -> usize {
        narrow!(self, v => std::mem::size_of_val(&v[..]))
    }
    /// the lengths as an integer leaf: shared for bytes, widened to i64 otherwise (corgi has no
    /// u16 or u32 leaf yet).
    pub fn to_prim(&self) -> crate::value::Prim {
        match self {
            Narrow::U8(v) => crate::value::Prim::U8(v.clone()),
            other => crate::value::Prim::I64(std::sync::Arc::new(other.to_i64())),
        }
    }
    /// bytes per length.
    pub fn width(&self) -> usize {
        match self { Narrow::U8(_) => 1, Narrow::U16(_) => 2, Narrow::U32(_) => 4, Narrow::U64(_) => 8 }
    }
    pub fn to_i64(&self) -> Vec<i64> {
        narrow!(self, v => v.iter().map(|&x| x as i64).collect())
    }
    /// the same lengths, at whatever widths.
    pub fn same(&self, other: &Narrow) -> bool {
        match (self, other) {
            (Narrow::U8(x), Narrow::U8(y)) => x == y,
            (Narrow::U16(x), Narrow::U16(y)) => x == y,
            (Narrow::U32(x), Narrow::U32(y)) => x == y,
            (Narrow::U64(x), Narrow::U64(y)) => x == y,
            _ => self.len() == other.len() && (0..self.len()).all(|i| self.get(i) == other.get(i)),
        }
    }
    /// append a length, widening every length first if it does not fit.
    #[inline]
    pub fn push(&mut self, l: usize) {
        match self {
            Narrow::U8(v) if l <= u8::MAX as usize => std::sync::Arc::make_mut(v).push(l as u8),
            Narrow::U16(v) if l <= u16::MAX as usize => v.push(l as u16),
            Narrow::U32(v) if l <= u32::MAX as usize => v.push(l as u32),
            Narrow::U64(v) => v.push(l as u64),
            _ => self.widen(l),
        }
    }
    #[cold]
    fn widen(&mut self, l: usize) {
        let max = l.max(narrow!(self, v => v.iter().map(|&x| x as usize).max().unwrap_or(0)));
        let n = self.len();
        *self = Narrow::with_max((0..n).map(|i| self.get(i)), max);
        self.push(l);
    }
    /// append lengths `a..b` of `other`.
    pub fn extend_from(&mut self, other: &Narrow, a: usize, b: usize) {
        match (self, other) {
            (Narrow::U8(x), Narrow::U8(y)) => std::sync::Arc::make_mut(x).extend_from_slice(&y[a..b]),
            (Narrow::U16(x), Narrow::U16(y)) => x.extend_from_slice(&y[a..b]),
            (this, other) => (a..b).for_each(|i| this.push(other.get(i))),
        }
    }
}

/// a partition written one row at a time, in the form `CORGI_LISTS` names (SPIKE).
pub enum Builder {
    Ends(Vec<usize>, usize),
    Lens(Narrow),
    Unary(Vec<u64>, usize, usize),
}

impl Builder {
    pub fn new(rows: usize) -> Builder {
        match form() {
            Form::Offsets => Builder::Ends(Vec::with_capacity(rows), 0),
            Form::Lens => Builder::Lens(Narrow::U8(std::sync::Arc::new(Vec::with_capacity(rows)))),
            Form::Unary => Builder::Unary(Vec::with_capacity(rows / 32 + 1), 0, 0),
        }
    }
    #[inline]
    pub fn push(&mut self, l: usize) {
        match self {
            Builder::Ends(e, acc) => {
                *acc += l;
                e.push(*acc);
            }
            Builder::Lens(n) => n.push(l),
            Builder::Unary(words, pos, rows) => {
                // ones at pos..pos+l, then the zero
                let end = *pos + l + 1;
                words.resize(end.div_ceil(64), 0);
                let (mut p, z) = (*pos, *pos + l);
                while p < z {
                    let w = p / 64;
                    let take = (64 - p % 64).min(z - p);
                    let mask = if take == 64 { !0 } else { ((1u64 << take) - 1) << (p % 64) };
                    words[w] |= mask;
                    p += take;
                }
                *pos = end;
                *rows += 1;
            }
        }
    }
    /// append rows `s..e` of `b`.
    pub fn extend_rows(&mut self, b: &super::Bounds, s: usize, e: usize) {
        match (&mut *self, b) {
            (Builder::Lens(n), super::Bounds::Lens(l)) => n.extend_from(&l.lens, s, e),
            _ => {
                let mut prev = if s == 0 { 0 } else { b.end(s - 1) };
                for end in b.ends_from(s).take(e - s) {
                    self.push(end - prev);
                    prev = end;
                }
            }
        }
    }
    pub fn finish(self) -> super::Bounds {
        match self {
            Builder::Ends(e, _) => e.into(),
            Builder::Lens(n) => {
                let l = Lens::from_lens(n);
                super::Bounds::uniform_lens(&l).unwrap_or_else(|| super::Bounds::Lens(std::sync::Arc::new(l)))
            }
            Builder::Unary(mut words, pos, rows) => {
                let total = pos - rows;
                // ones past the last row
                if pos % 64 != 0 {
                    *words.last_mut().unwrap() |= !0u64 << (pos % 64);
                }
                let u = Unary::with_words(words, rows, total);
                // a uniform partition stays a stride
                if rows > 0 && total % rows == 0 && (0..rows).all(|i| u.end(i) == (i + 1) * (total / rows)) {
                    return super::Bounds::Stride(total / rows, rows);
                }
                super::Bounds::Unary(std::sync::Arc::new(u))
            }
        }
    }
}

/// where a lengths partition keeps its block starts (SPIKE, `CORGI_LENS_LAYOUT`): one start per 64
/// rows whatever the width (`fixed`), one per 64 bytes of lengths (`width`), or each start in the
/// same 64-byte line as its block's 56 bytes of lengths (`inline`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Fixed,
    Width,
    Inline,
}

pub fn layout() -> Layout {
    static LAYOUT: OnceLock<Layout> = OnceLock::new();
    *LAYOUT.get_or_init(|| match std::env::var("CORGI_LENS_LAYOUT").as_deref() {
        Ok("width") => Layout::Width,
        Ok("inline") => Layout::Inline,
        Ok("fixed") | Err(_) => Layout::Fixed,
        Ok(other) => panic!("CORGI_LENS_LAYOUT={other}: expected fixed, width or inline"),
    })
}

/// whether a block is summed whole under a mask (SPIKE, `CORGI_LENS_SUM=masked`).
pub fn masked() -> bool {
    static MASKED: OnceLock<bool> = OnceLock::new();
    *MASKED.get_or_init(|| std::env::var("CORGI_LENS_SUM").as_deref() == Ok("masked"))
}

/// the sum of `xs[lo..hi]`, reading all of them.
#[inline]
fn masked_sum<T: Copy + Into<u64>, const N: usize>(xs: &[T; N], lo: usize, hi: usize) -> usize {
    // u16 lanes for bytes (64 x 255 fits), u32 lanes for u16 (32 x 65535 fits): as many lanes per
    // NEON add as the sum allows
    if std::mem::size_of::<T>() == 1 && N <= 256 {
        let mut acc = [0u16; N];
        for k in 0..N {
            acc[k] = (xs[k].into() as u16) & (((lo <= k && k < hi) as u16).wrapping_neg());
        }
        return acc.iter().sum::<u16>() as usize;
    }
    if std::mem::size_of::<T>() == 2 {
        let mut acc = [0u32; N];
        for k in 0..N {
            acc[k] = (xs[k].into() as u32) & (((lo <= k && k < hi) as u32).wrapping_neg());
        }
        return acc.iter().sum::<u32>() as usize;
    }
    let mut acc = 0u64;
    for k in 0..N {
        acc += xs[k].into() & (((lo <= k && k < hi) as u64).wrapping_neg());
    }
    acc as usize
}

/// whether a batch of random reads touches its lines in a first pass (SPIKE, `CORGI_LENS_TOUCH=1`).
pub fn touch() -> bool {
    static TOUCH: OnceLock<bool> = OnceLock::new();
    *TOUCH.get_or_init(|| std::env::var("CORGI_LENS_TOUCH").as_deref() == Ok("1"))
}

/// whether random reads of a compact partition decode it to offsets first (SPIKE, `CORGI_DECODE=never`).
pub fn decode() -> bool {
    static DECODE: OnceLock<bool> = OnceLock::new();
    *DECODE.get_or_init(|| std::env::var("CORGI_DECODE").as_deref() != Ok("never"))
}

#[derive(Clone, Debug)]
pub struct Lens {
    pub lens: Narrow,
    /// rows per block: a start for each.
    block: usize,
    /// the start of row `b * block`, for each block `b` (not kept for `inline`).
    starts: Vec<usize>,
    /// `inline`: 64-byte lines at the lengths' width, each the block's start (as the first 8 bytes,
    /// little-endian) and then its `block` lengths; `pad` elements come first, so that the lines lie
    /// on cache lines.
    lines: Option<Narrow>,
    pad: usize,
    total: usize,
}

impl Lens {
    pub fn from_ends(ends: &[usize]) -> Lens {
        let max = ends.iter().scan(0, |p, &e| Some(e - std::mem::replace(p, e))).max().unwrap_or(0);
        let lens = ends.iter().scan(0, |p, &e| Some(e - std::mem::replace(p, e)));
        Lens::from_lens(Narrow::with_max(ExactLen(lens, ends.len()), max))
    }
    /// the partition with these row lengths: the block starts are one pass over them.
    pub fn from_lens(lens: Narrow) -> Lens {
        let n = lens.len();
        let w = lens.width();
        let block = match layout() {
            Layout::Fixed => LENS_BLOCK,
            Layout::Width => 64 / w,
            Layout::Inline => 56 / w,
        };
        let blocks = n.div_ceil(block);
        let (mut starts, mut lines, mut pad) = (Vec::new(), None, 0);
        let mut acc = 0;
        if layout() == Layout::Inline {
            fn build<T: Copy + Default, const N: usize>(lens: &Narrow, block: usize, blocks: usize, w: usize, cast: fn(u64) -> T) -> (Vec<T>, usize, usize) {
                let mut v = vec![T::default(); N * blocks + N];
                let pad = (64 - v.as_ptr() as usize % 64) % 64 / w;
                let (n, k0) = (lens.len(), 8 / w);
                let mut acc = 0;
                for b in 0..blocks {
                    let line = &mut v[pad + N * b..pad + N * (b + 1)];
                    for k in 0..k0 {
                        line[k] = cast(((acc as u64) >> (8 * w * k)) & (u64::MAX >> (64 - 8 * w)));
                    }
                    for (k, r) in (b * block..((b + 1) * block).min(n)).enumerate() {
                        line[k0 + k] = cast(lens.get(r) as u64);
                    }
                    acc += lens.sum(b * block, ((b + 1) * block).min(n));
                }
                (v, pad, acc)
            }
            let (l, p, a) = match &lens {
                Narrow::U8(_) => { let (v, p, a) = build::<u8, 64>(&lens, block, blocks, 1, |x| x as u8); (Narrow::U8(std::sync::Arc::new(v)), p, a) }
                Narrow::U16(_) => { let (v, p, a) = build::<u16, 32>(&lens, block, blocks, 2, |x| x as u16); (Narrow::U16(v), p, a) }
                Narrow::U32(_) => { let (v, p, a) = build::<u32, 16>(&lens, block, blocks, 4, |x| x as u32); (Narrow::U32(v), p, a) }
                Narrow::U64(_) => { let (v, p, a) = build::<u64, 8>(&lens, block, blocks, 8, |x| x); (Narrow::U64(v), p, a) }
            };
            (lines, pad, acc) = (Some(l), p, a);
        } else {
            starts.reserve(blocks);
            for b in 0..blocks {
                starts.push(acc);
                acc += lens.sum(b * block, ((b + 1) * block).min(n));
            }
        }
        Lens { lens, block, starts, lines, pad, total: acc }
    }
    pub fn len(&self) -> usize {
        self.lens.len()
    }
    pub fn total(&self) -> usize {
        self.total
    }
    #[inline]
    pub fn start(&self, i: usize) -> usize {
        if i == self.len() {
            return self.total;
        }
        let b = i / self.block;
        if self.lines.is_none() {
            // a whole block of 64 bytes, summed under a mask: the same work for every row, with no
            // branch on where in its block the row falls
            if masked() && self.block * self.lens.width() == 64 && (b + 1) * self.block <= self.len() {
                let j = i - b * self.block;
                return self.starts[b] + match &self.lens {
                    Narrow::U8(v) => masked_sum::<u8, 64>(v[b * 64..][..64].try_into().unwrap(), 0, j),
                    Narrow::U16(v) => masked_sum::<u16, 32>(v[b * 32..][..32].try_into().unwrap(), 0, j),
                    Narrow::U32(v) => masked_sum::<u32, 16>(v[b * 16..][..16].try_into().unwrap(), 0, j),
                    Narrow::U64(v) => masked_sum::<u64, 8>(v[b * 8..][..8].try_into().unwrap(), 0, j),
                };
            }
            return self.starts[b] + self.lens.sum(b * self.block, i);
        }
        let j = i - b * self.block;
        fn line<T: Copy + Into<u64>, const N: usize>(v: &[T], pad: usize, b: usize, j: usize, w: usize) -> usize {
            let line: &[T; N] = v[pad + N * b..][..N].try_into().unwrap();
            let k0 = 8 / w;
            let base = (0..k0).fold(0u64, |acc, k| acc | (line[k].into() << (8 * w * k))) as usize;
            base + if masked() { masked_sum::<T, N>(line, k0, k0 + j) } else { line[k0..k0 + j].iter().map(|&x| x.into() as usize).sum() }
        }
        match self.lines.as_ref().unwrap() {
            Narrow::U8(v) => line::<u8, 64>(v, self.pad, b, j, 1),
            Narrow::U16(v) => line::<u16, 32>(v, self.pad, b, j, 2),
            Narrow::U32(v) => line::<u32, 16>(v, self.pad, b, j, 4),
            Narrow::U64(v) => line::<u64, 8>(v, self.pad, b, j, 8),
        }
    }
    #[inline]
    pub fn span(&self, i: usize) -> (usize, usize) {
        let s = self.start(i);
        (s, s + self.lens.get(i))
    }
    /// the spans of rows `idx`, with the width and layout decided once for the batch (SPIKE: the
    /// `width` layout, a whole block summed under a mask; the last partial block read row by row).
    pub fn spans_at(&self, idx: &[usize], out: &mut Vec<(usize, usize)>) {
        out.clear();
        // `inline`: the start, the lengths before the row and its own length, all in one line
        if let Some(lines) = &self.lines {
            fn go<T: Copy + Into<u64>, const N: usize>(v: &[T], pad: usize, w: usize, idx: &[usize], out: &mut Vec<(usize, usize)>) {
                let (k0, block) = (8 / w, (64 - 8) / w);
                if touch() {
                    let mut seen = 0u64;
                    for chunk in idx.chunks(256) {
                        for &i in chunk {
                            seen ^= v[pad + N * (i / block)].into();
                        }
                        out.extend(chunk.iter().map(|&i| {
                            let (b, j) = (i / block, i % block);
                            let line: &[T; N] = v[pad + N * b..][..N].try_into().unwrap();
                            let base = (0..k0).fold(0u64, |acc, k| acc | (line[k].into() << (8 * w * k))) as usize;
                            let s = base + masked_sum::<T, N>(line, k0, k0 + j);
                            (s, s + line[k0 + j].into() as usize)
                        }));
                    }
                    std::hint::black_box(seen);
                    return;
                }
                out.extend(idx.iter().map(|&i| {
                    let (b, j) = (i / block, i % block);
                    let line: &[T; N] = v[pad + N * b..][..N].try_into().unwrap();
                    let base = (0..k0).fold(0u64, |acc, k| acc | (line[k].into() << (8 * w * k))) as usize;
                    let s = base + masked_sum::<T, N>(line, k0, k0 + j);
                    (s, s + line[k0 + j].into() as usize)
                }));
            }
            match lines {
                Narrow::U8(v) => go::<u8, 64>(v, self.pad, 1, idx, out),
                Narrow::U16(v) => go::<u16, 32>(v, self.pad, 2, idx, out),
                Narrow::U32(v) => go::<u32, 16>(v, self.pad, 4, idx, out),
                Narrow::U64(v) => go::<u64, 8>(v, self.pad, 8, idx, out),
            }
            return;
        }
        if self.block * self.lens.width() != 64 {
            out.extend(idx.iter().map(|&i| self.span(i)));
            return;
        }
        let full = self.len() / self.block * self.block;
        fn go<T: Copy + Into<u64>, const N: usize>(v: &[T], starts: &[usize], full: usize, idx: &[usize], out: &mut Vec<(usize, usize)>, slow: impl Fn(usize) -> (usize, usize)) {
            // first a pass of plain loads over a chunk of queries, which the core overlaps, so the
            // lines the sums read are in cache when the long pass reaches them
            if touch() {
                let mut seen = 0u64;
                for chunk in idx.chunks(256) {
                    for &i in chunk {
                        if i < full {
                            seen ^= v[i / N * N].into() ^ starts[i / N] as u64;
                        }
                    }
                    out.extend(chunk.iter().map(|&i| {
                        if i >= full {
                            return slow(i);
                        }
                        let b = i / N;
                        let s = starts[b] + masked_sum::<T, N>(v[b * N..][..N].try_into().unwrap(), 0, i - b * N);
                        (s, s + v[i].into() as usize)
                    }));
                }
                std::hint::black_box(seen);
                return;
            }
            out.extend(idx.iter().map(|&i| {
                if i >= full {
                    return slow(i);
                }
                let b = i / N;
                let s = starts[b] + masked_sum::<T, N>(v[b * N..][..N].try_into().unwrap(), 0, i - b * N);
                (s, s + v[i].into() as usize)
            }));
        }
        let slow = |i| self.span(i);
        match &self.lens {
            Narrow::U8(v) => go::<u8, 64>(v, &self.starts, full, idx, out, slow),
            Narrow::U16(v) => go::<u16, 32>(v, &self.starts, full, idx, out, slow),
            Narrow::U32(v) => go::<u32, 16>(v, &self.starts, full, idx, out, slow),
            Narrow::U64(v) => go::<u64, 8>(v, &self.starts, full, idx, out, slow),
        }
    }
    /// the bytes the partition holds; for `inline`, its lines, which hold the lengths (the
    /// contiguous copy it also keeps for in-order reads is not counted).
    pub fn bytes(&self) -> usize {
        match &self.lines {
            None => self.lens.bytes() + 8 * self.starts.len(),
            Some(l) => l.bytes() - 64,
        }
    }
}

/// an iterator with a known length, for `Narrow::with_max`.
struct ExactLen<I>(I, usize);
impl<I: Iterator> Iterator for ExactLen<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<I::Item> {
        let x = self.0.next()?;
        self.1 -= 1;
        Some(x)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.1, Some(self.1))
    }
}
impl<I: Iterator> ExactSizeIterator for ExactLen<I> {}

// --- Unary --------------------------------------------------------------------------------------

/// words per directory block (512 bits).
const BLOCK_WORDS: usize = 8;
/// rows (zeros) per select hint.
const HINT: usize = 256;

#[derive(Clone, Debug)]
pub struct Unary {
    /// bit `p` is bit `p % 64` of word `p / 64`; past the last row the bits are ones.
    words: Vec<u64>,
    rows: usize,
    total: usize,
    /// the zeros before each block, and one more entry for the end.
    zeros: Vec<usize>,
    /// the block holding zero `h * HINT`, for each `h`.
    hints: Vec<u32>,
}

impl Unary {
    pub fn from_ends(ends: &[usize]) -> Unary {
        Unary::from_lens(ends.len(), ends.last().copied().unwrap_or(0), ends.iter().scan(0, |p, &e| Some(e - std::mem::replace(p, e))))
    }
    /// the partition of `rows` rows and `total` elements with these lengths.
    pub fn from_lens(rows: usize, total: usize, lens: impl Iterator<Item = usize>) -> Unary {
        let bits = rows + total;
        let mut words = vec![!0u64; bits.div_ceil(64)];
        let mut pos = 0;
        for l in lens {
            pos += l;
            words[pos / 64] &= !(1 << (pos % 64));
            pos += 1;
        }
        debug_assert_eq!(pos, bits);
        Unary::with_words(words, rows, total)
    }
    fn with_words(words: Vec<u64>, rows: usize, total: usize) -> Unary {
        let blocks = words.len().div_ceil(BLOCK_WORDS);
        let mut zeros = Vec::with_capacity(blocks + 1);
        let mut hints = Vec::with_capacity(rows.div_ceil(HINT));
        let mut acc = 0;
        for b in 0..blocks {
            zeros.push(acc);
            let z: usize = words[b * BLOCK_WORDS..((b + 1) * BLOCK_WORDS).min(words.len())].iter().map(|w| w.count_zeros() as usize).sum();
            // every hint whose zero falls in this block
            while hints.len() * HINT < acc + z {
                hints.push(b as u32);
            }
            acc += z;
        }
        zeros.push(acc);
        debug_assert_eq!(acc, rows);
        Unary { words, rows, total, zeros, hints }
    }
    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn total(&self) -> usize {
        self.total
    }
    /// the bit position of zero `k`.
    #[inline]
    pub fn select0(&self, k: usize) -> usize {
        let h = k / HINT;
        let (mut lo, mut hi) = (self.hints[h] as usize, self.hints.get(h + 1).map_or(self.zeros.len() - 2, |&b| b as usize));
        // the last block in lo..=hi with at most k zeros before it
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            if self.zeros[mid] <= k { lo = mid } else { hi = mid - 1 }
        }
        let mut rem = k - self.zeros[lo];
        let mut w = lo * BLOCK_WORDS;
        loop {
            let x = !self.words[w];
            let z = x.count_ones() as usize;
            if rem < z {
                return w * 64 + select_in_word(x, rem);
            }
            rem -= z;
            w += 1;
        }
    }
    #[inline]
    pub fn end(&self, i: usize) -> usize {
        self.select0(i) - i
    }
    #[inline]
    pub fn span(&self, i: usize) -> (usize, usize) {
        let p = self.select0(i);
        if i == 0 {
            return (0, p);
        }
        // the previous zero, by a backward scan within this block; else its own select
        let w = p / 64;
        let below = !self.words[w] & ((1u64 << (p % 64)) - 1);
        if below != 0 {
            let q = w * 64 + 63 - below.leading_zeros() as usize;
            return (q + 1 - i, p - i);
        }
        let floor = w - w % BLOCK_WORDS;
        let mut v = w;
        while v > floor {
            v -= 1;
            let x = !self.words[v];
            if x != 0 {
                let q = v * 64 + 63 - x.leading_zeros() as usize;
                return (q + 1 - i, p - i);
            }
        }
        (self.select0(i - 1) + 1 - i, p - i)
    }
    pub fn bytes(&self) -> usize {
        8 * self.words.len() + 8 * self.zeros.len() + 4 * self.hints.len()
    }
    pub fn words(&self) -> &[u64] {
        &self.words
    }
}

/// the position of set bit `r` of `x` (which has more than `r`).
#[inline]
pub fn select_in_word(x: u64, r: usize) -> usize {
    // bytes first, then bits in the byte
    let mut r = r as u32;
    let mut base = 0;
    let mut x = x;
    loop {
        let c = (x & 0xff).count_ones();
        if r < c {
            break;
        }
        r -= c;
        x >>= 8;
        base += 8;
    }
    let mut b = x & 0xff;
    for _ in 0..r {
        b &= b - 1;
    }
    base + b.trailing_zeros() as usize
}

// --- in-order ends ------------------------------------------------------------------------------

/// the end of each row from row `row` on, in order, in one pass.
pub enum Ends<'a> {
    Slice(std::iter::Copied<std::slice::Iter<'a, usize>>),
    Stride { k: usize, i: usize, n: usize },
    Lens { lens: &'a Narrow, i: usize, acc: usize },
    Unary { words: &'a [u64], w: usize, x: u64, row: usize, rows: usize },
}

impl<'a> Ends<'a> {
    pub fn lens(l: &'a Lens, from: usize) -> Ends<'a> {
        Ends::Lens { lens: &l.lens, i: from, acc: l.start(from) }
    }
    pub fn unary(u: &'a Unary, from: usize) -> Ends<'a> {
        if from == u.rows {
            return Ends::Unary { words: &u.words, w: u.words.len(), x: 0, row: from, rows: from };
        }
        let p = u.select0(from);
        // the word holding zero `from`, with the zeros below it cleared
        let x = !u.words[p / 64] & (!0u64 << (p % 64));
        Ends::Unary { words: &u.words, w: p / 64, x, row: from, rows: u.rows }
    }
}

impl Iterator for Ends<'_> {
    type Item = usize;
    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            Ends::Slice(it) => it.next(),
            Ends::Stride { k, i, n } => {
                (*i < *n).then(|| {
                    *i += 1;
                    *i * *k
                })
            }
            Ends::Lens { lens, i, acc } => {
                (*i < lens.len()).then(|| {
                    *acc += lens.get(*i);
                    *i += 1;
                    *acc
                })
            }
            Ends::Unary { words, w, x, row, rows } => {
                if *row == *rows {
                    return None;
                }
                while *x == 0 {
                    *w += 1;
                    *x = !words[*w];
                }
                let p = *w * 64 + x.trailing_zeros() as usize;
                *x &= *x - 1;
                let e = p - *row;
                *row += 1;
                Some(e)
            }
        }
    }
    // the form decided once, outside the loop: `collect`, `for_each` and `sum` come here
    #[inline]
    fn fold<B, F: FnMut(B, usize) -> B>(self, init: B, mut f: F) -> B {
        match self {
            Ends::Slice(it) => it.fold(init, f),
            Ends::Stride { k, i, n } => (i + 1..=n).fold(init, |b, j| f(b, j * k)),
            Ends::Lens { lens, i, mut acc } => narrow!(lens, v => v[i..].iter().fold(init, |b, &l| {
                acc += l as usize;
                f(b, acc)
            })),
            Ends::Unary { words, mut w, mut x, mut row, rows } => {
                let mut b = init;
                while row < rows {
                    while x == 0 {
                        w += 1;
                        x = !words[w];
                    }
                    let p = w * 64 + x.trailing_zeros() as usize;
                    x &= x - 1;
                    b = f(b, p - row);
                    row += 1;
                }
                b
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match self {
            Ends::Slice(it) => it.len(),
            Ends::Stride { i, n, .. } => n - i,
            Ends::Lens { lens, i, .. } => lens.len() - i,
            Ends::Unary { row, rows, .. } => rows - row,
        };
        (n, Some(n))
    }
}
impl ExactSizeIterator for Ends<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(ends: &[usize]) {
        let l = Lens::from_ends(ends);
        let u = Unary::from_ends(ends);
        assert_eq!(l.len(), ends.len());
        assert_eq!(u.len(), ends.len());
        for i in 0..ends.len() {
            let want = (if i == 0 { 0 } else { ends[i - 1] }, ends[i]);
            assert_eq!(l.span(i), want, "lens row {i}");
            assert_eq!(u.span(i), want, "unary row {i}");
            assert_eq!(u.end(i), ends[i]);
        }
        for from in [0, ends.len() / 3, ends.len()] {
            assert_eq!(Ends::lens(&l, from).collect::<Vec<_>>(), ends[from..]);
            assert_eq!(Ends::unary(&u, from).collect::<Vec<_>>(), ends[from..]);
        }
    }

    #[test]
    fn spans_and_ends() {
        let mut seed = 7u64;
        let mut rnd = |m: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % m
        };
        check(&[]);
        check(&[0]);
        check(&[0, 0, 0]);
        check(&[5]);
        for (rows, m) in [(10, 3), (1000, 2), (5000, 20), (3000, 700), (300, 70000), (2000, 1)] {
            let mut acc = 0;
            let ends: Vec<usize> = (0..rows).map(|_| { acc += rnd(m) as usize; acc }).collect();
            check(&ends);
        }
    }

    #[test]
    fn select_word() {
        for x in [1u64, 0x8000_0000_0000_0000, 0xf0f0, !0, 0x1234_5678_9abc_def0] {
            let mut y = x;
            for r in 0..x.count_ones() as usize {
                assert_eq!(select_in_word(x, r), y.trailing_zeros() as usize);
                y &= y - 1;
            }
        }
    }
}
