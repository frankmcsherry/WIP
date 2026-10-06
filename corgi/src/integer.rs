//! Adaptive-integer / independent-preparation spike.
//!
//! One logical integer column, independent of physical width. Native lanes use
//! corgi's existing unsigned order keys, with base 0 or -2^(w-1). Packed bits
//! cover booleans; i128 is a checked escape, not a claim of arbitrary precision.
//! Binary operations choose ONE execution encoding, prepare each source
//! independently in bounded tiles, and run one kernel per execution width.

use crate::value::Prim;
use std::{cmp::Ordering, hash::{Hash, Hasher}, sync::Arc};

const TILE: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Frame { Zero, Biased }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Encoding {
    Bits,
    Native { width: u32, frame: Frame },
    Wide,
}

impl Encoding {
    pub fn width(self) -> u32 {
        match self { Self::Bits => 1, Self::Native { width, .. } => width, Self::Wide => 128 }
    }
    fn base(self) -> i128 {
        match self {
            Self::Native { width, frame: Frame::Biased } => -(1i128 << (width - 1)),
            _ => 0,
        }
    }
    fn valid(self) -> bool {
        !matches!(self, Self::Native { width, .. } if !matches!(width, 8 | 16 | 32 | 64))
    }
    fn contains(self, x: i128) -> bool {
        match self {
            Self::Bits => matches!(x, 0 | 1),
            Self::Wide => true,
            Self::Native { width, .. } => x.checked_sub(self.base())
                .is_some_and(|d| d >= 0 && d <= ((1i128 << width) - 1)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Storage {
    Bits { words: Arc<Vec<u64>>, len: usize },
    Native(Prim, Frame),
    Wide(Arc<Vec<i128>>),
}

/// Bounds may be conservative after arithmetic/gather. Keeping a wider layout
/// is allowed: neither identity nor logical shape depends on minimal packing.
#[derive(Clone, Debug)]
pub struct Integer {
    pub(crate) storage: Storage,
    range: Option<(i128, i128)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Binary { Add, Sub, Mul }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Plan {
    pub encoding: Encoding,
    pub direct_inputs: usize,
    pub prepared_inputs: usize,
}

fn bounds(xs: impl Iterator<Item = i128>) -> Option<(i128, i128)> {
    xs.fold(None, |r, x| Some(r.map_or((x, x), |(lo, hi)| (lo.min(x), hi.max(x)))))
}

fn union(a: Option<(i128, i128)>, b: Option<(i128, i128)>) -> Option<(i128, i128)> {
    match (a, b) {
        (Some((al, ah)), Some((bl, bh))) => Some((al.min(bl), ah.max(bh))),
        (a, b) => a.or(b),
    }
}

fn choose(range: Option<(i128, i128)>, bits: bool) -> Encoding {
    let (lo, hi) = range.unwrap_or((0, 0));
    if bits && lo >= 0 && hi <= 1 { return Encoding::Bits; }
    let frame = if lo < 0 { Frame::Biased } else { Frame::Zero };
    for width in [8, 16, 32, 64] {
        let enc = Encoding::Native { width, frame };
        if enc.contains(lo) && enc.contains(hi) { return enc; }
    }
    Encoding::Wide
}

fn result_range(op: Binary, a: (i128, i128), b: (i128, i128)) -> Option<(i128, i128)> {
    match op {
        Binary::Add => Some((a.0.checked_add(b.0)?, a.1.checked_add(b.1)?)),
        Binary::Sub => Some((a.0.checked_sub(b.1)?, a.1.checked_sub(b.0)?)),
        Binary::Mul => {
            let xs = [a.0.checked_mul(b.0)?, a.0.checked_mul(b.1)?,
                a.1.checked_mul(b.0)?, a.1.checked_mul(b.1)?];
            bounds(xs.into_iter())
        }
    }
}

impl Integer {
    pub(crate) fn from_storage(storage: Storage) -> Self {
        let mut col = Self { storage, range: None };
        col.range = bounds((0..col.len()).map(|i| col.at(i)));
        col
    }
    pub fn new(xs: Vec<i128>) -> Self {
        let range = bounds(xs.iter().copied());
        Self::encode(xs, choose(range, true), range)
    }

    /// Explicit storage selection, for experiments and compatible byte views.
    /// This is a checked packing request, never a numeric truncation.
    pub fn with_encoding(xs: Vec<i128>, encoding: Encoding) -> Result<Self, String> {
        if !encoding.valid() { return Err("integer: invalid native width".into()); }
        if xs.iter().any(|&x| !encoding.contains(x)) {
            return Err(format!("integer: values do not fit {encoding:?}"));
        }
        let range = bounds(xs.iter().copied());
        Ok(Self::encode(xs, encoding, range))
    }

    fn encode(xs: Vec<i128>, encoding: Encoding, range: Option<(i128, i128)>) -> Self {
        let storage = match encoding {
            Encoding::Bits => {
                let mut words = vec![0u64; xs.len().div_ceil(64)];
                for (i, &x) in xs.iter().enumerate() { words[i / 64] |= (x as u64) << (i % 64); }
                Storage::Bits { words: Arc::new(words), len: xs.len() }
            }
            Encoding::Wide => Storage::Wide(Arc::new(xs)),
            Encoding::Native { width, frame } => {
                let base = encoding.base();
                macro_rules! pack { ($v:ident, $t:ty) => {
                    Prim::$v(Arc::new(xs.into_iter().map(|x| (x - base) as $t).collect()))
                }; }
                let p = match width { 8 => pack!(U8, u8), 16 => pack!(U16, u16),
                    32 => pack!(U32, u32), 64 => pack!(U64, u64), _ => unreachable!() };
                Storage::Native(p, frame)
            }
        };
        Self { storage, range }
    }

    pub fn encoding(&self) -> Encoding {
        match &self.storage {
            Storage::Bits { .. } => Encoding::Bits,
            Storage::Native(p, frame) => Encoding::Native { width: p.bits(), frame: *frame },
            Storage::Wide(_) => Encoding::Wide,
        }
    }
    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Bits { len, .. } => *len, Storage::Native(p, _) => p.len(),
            Storage::Wide(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn payload_bytes(&self) -> usize {
        match &self.storage {
            Storage::Bits { words, .. } => words.len() * 8,
            Storage::Native(p, _) => p.len() * (p.bits() as usize / 8),
            Storage::Wide(v) => v.len() * 16,
        }
    }
    pub fn at(&self, i: usize) -> i128 {
        assert!(i < self.len(), "integer index out of bounds");
        match &self.storage {
            Storage::Bits { words, .. } => ((words[i / 64] >> (i % 64)) & 1) as i128,
            Storage::Wide(xs) => xs[i],
            Storage::Native(p, _) => {
                let x = match p { Prim::U8(v) => v[i] as i128, Prim::U16(v) => v[i] as i128,
                    Prim::U32(v) => v[i] as i128, Prim::U64(v) => v[i] as i128 };
                x + self.encoding().base()
            }
        }
    }
    pub fn to_vec(&self) -> Vec<i128> { (0..self.len()).map(|i| self.at(i)).collect() }
    pub fn compact(&self) -> Self { Self::new(self.to_vec()) }

    /// Byte import/export preserve the Arc for an already compatible buffer.
    pub fn from_bytes(bytes: Arc<Vec<u8>>) -> Self {
        let range = bounds(bytes.iter().map(|&x| x as i128));
        Self { storage: Storage::Native(Prim::U8(bytes), Frame::Zero), range }
    }
    pub fn to_bytes(&self) -> Result<Arc<Vec<u8>>, String> {
        if let Storage::Native(Prim::U8(xs), Frame::Zero) = &self.storage { return Ok(xs.clone()); }
        let mut bytes = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            bytes.push(u8::try_from(self.at(i)).map_err(|_| "integer: byte conversion requires 0..255")?);
        }
        Ok(Arc::new(bytes))
    }

    pub(crate) fn from_native(p: Prim, frame: Frame, source: &Self) -> Self {
        Self { storage: Storage::Native(p, frame), range: source.range }
    }
    pub(crate) fn native(&self) -> Option<(&Prim, Frame)> {
        if let Storage::Native(p, f) = &self.storage { Some((p, *f)) } else { None }
    }
    pub(crate) fn gather(&self, indices: &[usize]) -> Self {
        match &self.storage {
            Storage::Native(p, f) => Self::from_native(p.gather(indices), *f, self),
            _ => Self::new(indices.iter().map(|&i| self.at(i)).collect()),
        }
    }
    pub(crate) fn interleave(srcs: &[&Self], tags: &[usize], offsets: &[usize]) -> Self {
        Self::new(tags.iter().zip(offsets).map(|(&t, &i)| srcs[t].at(i)).collect())
    }

    pub fn compare_at(&self, i: usize, other: &Self, j: usize) -> Ordering {
        self.at(i).cmp(&other.at(j))
    }

    /// Canonical value hashes; positive native-width values retain the existing
    /// unsigned corgi hash. Other i128 values include the upper limb as well.
    pub fn hashes(&self) -> Vec<u64> {
        if let Storage::Native(p, Frame::Zero) = &self.storage { return p.hashes(); }
        fn h(x: i128) -> u64 {
            let lo = x as u64;
            let hi = (x >> 64) as u64;
            if hi == 0 { crate::hash::mix64(lo) }
            else { crate::hash::mix64(lo ^ crate::hash::mix64(hi ^ 0x517c_c1b7_2722_0a95)) }
        }
        // Unary readers factor decoding; no stored-width pair is dispatched.
        let reader = Prepared::<i128>::new(self, Encoding::Wide);
        let mut scratch = vec![0i128; TILE];
        let mut out = Vec::with_capacity(self.len());
        for start in (0..self.len()).step_by(TILE) {
            let n = TILE.min(self.len() - start);
            out.extend(reader.tile(start, &mut scratch[..n]).iter().map(|&x| h(x)));
        }
        out
    }

    pub fn binary_plan(&self, other: &Self, op: Binary) -> Result<Plan, String> {
        if self.len() != other.len() { return Err("integer: operands have different lengths".into()); }
        let output = match (self.range, other.range) {
            (Some(a), Some(b)) => result_range(op, a, b), _ => Some((0, 0)),
        };
        let encoding = if output.is_none() { Encoding::Wide }
            else { choose(union(union(self.range, other.range), output), false) };
        Ok(plan(&[self, other], encoding))
    }

    pub fn binary(&self, other: &Self, op: Binary) -> Result<Self, String> {
        let enc = self.binary_plan(other, op)?.encoding;
        let range = match (self.range, other.range) {
            (Some(a), Some(b)) => result_range(op, a, b), _ => None,
        };
        macro_rules! run { ($t:ty) => { binary_kernel::<$t>(self, other, op, enc, range) }; }
        match enc.width() { 8 => run!(u8), 16 => run!(u16), 32 => run!(u32),
            64 => run!(u64), 128 => run!(i128), _ => unreachable!() }
    }

    /// Explicit word arithmetic: truncate operands and results modulo 2^width.
    /// This has no relationship to whichever width stored the input integers.
    pub fn wrapping(&self, other: &Self, op: Binary, width: u32) -> Result<Self, String> {
        if self.len() != other.len() { return Err("integer: operands have different lengths".into()); }
        if !matches!(width, 8 | 16 | 32 | 64) { return Err("word arithmetic requires width 8/16/32/64".into()); }
        let enc = Encoding::Native { width, frame: Frame::Zero };
        let range = (!self.is_empty()).then_some((0, (1i128 << width) - 1));
        macro_rules! run { ($t:ty) => { binary_kernel::<$t>(self, other, op, enc, range) }; }
        match width { 8 => run!(u8), 16 => run!(u16), 32 => run!(u32), 64 => run!(u64), _ => unreachable!() }
    }

    pub fn compare(&self, other: &Self) -> Result<Vec<i8>, String> {
        if self.len() != other.len() { return Err("integer: operands have different lengths".into()); }
        let enc = choose(union(self.range, other.range), false);
        macro_rules! run { ($t:ty) => { compare_kernel::<$t>(self, other, enc) }; }
        Ok(match enc.width() { 8 => run!(u8), 16 => run!(u16), 32 => run!(u32),
            64 => run!(u64), 128 => run!(i128), _ => unreachable!() })
    }

    pub fn pick(&self, other: &Self, take_max: bool) -> Result<Self, String> {
        if self.len() != other.len() { return Err("integer: operands have different lengths".into()); }
        let range = union(self.range, other.range);
        let enc = choose(range, false);
        macro_rules! run { ($t:ty) => { pick_kernel::<$t>(self, other, take_max, enc, range) }; }
        Ok(match enc.width() { 8 => run!(u8), 16 => run!(u16), 32 => run!(u32),
            64 => run!(u64), 128 => run!(i128), _ => unreachable!() })
    }

    /// N-ary addition uses the SAME readers and five execution widths. Arity is
    /// a runtime list of operands, not another axis in a dispatch macro.
    pub fn sum(cols: &[&Self]) -> Result<Self, String> {
        let Some(first) = cols.first() else { return Err("integer sum needs an operand".into()); };
        if cols.iter().any(|c| c.len() != first.len()) { return Err("integer: operands have different lengths".into()); }
        let mut inputs = None;
        let mut output = Some((0i128, 0i128));
        for c in cols {
            inputs = union(inputs, c.range);
            if let Some(r) = c.range { output = output.and_then(|o| result_range(Binary::Add, o, r)); }
        }
        let enc = if output.is_none() { Encoding::Wide } else { choose(union(inputs, output), false) };
        macro_rules! run { ($t:ty) => { sum_kernel::<$t>(cols, enc, if first.is_empty() { None } else { output }) }; }
        match enc.width() { 8 => run!(u8), 16 => run!(u16), 32 => run!(u32),
            64 => run!(u64), 128 => run!(i128), _ => unreachable!() }
    }
}

fn plan(cols: &[&Integer], enc: Encoding) -> Plan {
    let direct_inputs = cols.iter().filter(|c| c.encoding() == enc).count();
    Plan { encoding: enc, direct_inputs, prepared_inputs: cols.len() - direct_inputs }
}

impl PartialEq for Integer {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && (0..self.len()).all(|i| self.at(i) == other.at(i))
    }
}
impl Eq for Integer {}
impl Hash for Integer {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.len().hash(h);
        for i in 0..self.len() { self.at(i).hash(h); }
    }
}

trait Lane: Copy + Ord + Default {
    fn from_i128(x: i128) -> Self;
    fn direct(col: &Integer, enc: Encoding) -> Option<&[Self]>;
    fn store(xs: Vec<Self>, enc: Encoding, range: Option<(i128, i128)>) -> Integer;
    fn apply(op: Binary, x: Self, y: Self, bias: Self) -> Option<Self>;
    fn append_binary(op: Binary, a: &[Self], b: &[Self], out: &mut Vec<Self>, bias: Self, start: usize) -> Result<(), String>;
}

macro_rules! lanes {
    ($($t:ty => $v:ident),+) => { $(
        impl Lane for $t {
            fn from_i128(x: i128) -> Self { x as Self }
            fn direct(col: &Integer, enc: Encoding) -> Option<&[Self]> {
                if col.encoding() != enc { return None; }
                match &col.storage { Storage::Native(Prim::$v(xs), _) => Some(xs), _ => None }
            }
            fn store(xs: Vec<Self>, enc: Encoding, range: Option<(i128, i128)>) -> Integer {
                let Encoding::Native { frame, .. } = enc else { unreachable!() };
                Integer { storage: Storage::Native(Prim::$v(Arc::new(xs)), frame), range }
            }
            fn apply(op: Binary, x: Self, y: Self, bias: Self) -> Option<Self> {
                // All low-bit arithmetic is unsigned. The planned range proves
                // the decoded result fits, including signed biased encodings.
                Some(match op {
                    Binary::Add => x.wrapping_add(y).wrapping_add(bias),
                    Binary::Sub => x.wrapping_sub(y).wrapping_add(bias),
                    Binary::Mul => (x ^ bias).wrapping_mul(y ^ bias) ^ bias,
                })
            }
            fn append_binary(op: Binary, a: &[Self], b: &[Self], out: &mut Vec<Self>, bias: Self, _: usize) -> Result<(), String> {
                // Dispatch ABOVE the element loop; extend's exact-size iterator
                // writes a contiguous run without a Vec::push guard per lane.
                match op {
                    Binary::Add => out.extend(a.iter().zip(b).map(|(&x, &y)| x.wrapping_add(y).wrapping_add(bias))),
                    Binary::Sub => out.extend(a.iter().zip(b).map(|(&x, &y)| x.wrapping_sub(y).wrapping_add(bias))),
                    Binary::Mul => out.extend(a.iter().zip(b).map(|(&x, &y)| (x ^ bias).wrapping_mul(y ^ bias) ^ bias)),
                }
                Ok(())
            }
        }
    )+ };
}
lanes!(u8 => U8, u16 => U16, u32 => U32, u64 => U64);

impl Lane for i128 {
    fn from_i128(x: i128) -> Self { x }
    fn direct(col: &Integer, enc: Encoding) -> Option<&[Self]> {
        if enc != Encoding::Wide { return None; }
        match &col.storage { Storage::Wide(xs) => Some(xs), _ => None }
    }
    fn store(xs: Vec<Self>, _: Encoding, _: Option<(i128, i128)>) -> Integer {
        let range = bounds(xs.iter().copied());
        Integer { storage: Storage::Wide(Arc::new(xs)), range }
    }
    fn apply(op: Binary, x: Self, y: Self, _: Self) -> Option<Self> {
        match op { Binary::Add => x.checked_add(y), Binary::Sub => x.checked_sub(y), Binary::Mul => x.checked_mul(y) }
    }
    fn append_binary(op: Binary, a: &[Self], b: &[Self], out: &mut Vec<Self>, _: Self, start: usize) -> Result<(), String> {
        for (j, (&x, &y)) in a.iter().zip(b).enumerate() {
            out.push(Self::apply(op, x, y, 0).ok_or_else(|| format!("integer {op:?}: i128 overflow at row {}", start + j))?);
        }
        Ok(())
    }
}

type Decode<T> = fn(&Integer, usize, &mut [T], i128);
struct Prepared<'a, T: Lane> {
    col: &'a Integer,
    direct: Option<&'a [T]>,
    decode: Decode<T>,
    shift: i128,
}

impl<'a, T: Lane> Prepared<'a, T> {
    fn new(col: &'a Integer, enc: Encoding) -> Self {
        macro_rules! reader { ($v:ident) => {{
            fn decode<T: Lane>(col: &Integer, start: usize, dst: &mut [T], shift: i128) {
                let Storage::Native(Prim::$v(xs), _) = &col.storage else { unreachable!() };
                for (d, &x) in dst.iter_mut().zip(&xs[start..]) { *d = T::from_i128(x as i128 + shift); }
            }
            decode::<T> as Decode<T>
        }}; }
        fn bit<T: Lane>(col: &Integer, start: usize, dst: &mut [T], shift: i128) {
            let Storage::Bits { words, .. } = &col.storage else { unreachable!() };
            for (j, d) in dst.iter_mut().enumerate() {
                let i = start + j;
                *d = T::from_i128(((words[i / 64] >> (i % 64)) & 1) as i128 + shift);
            }
        }
        fn wide<T: Lane>(col: &Integer, start: usize, dst: &mut [T], shift: i128) {
            let Storage::Wide(xs) = &col.storage else { unreachable!() };
            for (d, &x) in dst.iter_mut().zip(&xs[start..]) { *d = T::from_i128(x + shift); }
        }
        let decode = match &col.storage {
            Storage::Bits { .. } => bit::<T>, Storage::Wide(_) => wide::<T>,
            Storage::Native(p, _) => match p { Prim::U8(_) => reader!(U8), Prim::U16(_) => reader!(U16),
                Prim::U32(_) => reader!(U32), Prim::U64(_) => reader!(U64) },
        };
        Self { col, direct: T::direct(col, enc), decode, shift: col.encoding().base() - enc.base() }
    }
    fn tile<'b>(&'b self, start: usize, scratch: &'b mut [T]) -> &'b [T] {
        if let Some(xs) = self.direct { &xs[start..start + scratch.len()] }
        else { (self.decode)(self.col, start, scratch, self.shift); scratch }
    }
}

fn binary_kernel<T: Lane>(a: &Integer, b: &Integer, op: Binary, enc: Encoding,
    range: Option<(i128, i128)>) -> Result<Integer, String> {
    let (a, b) = (Prepared::<T>::new(a, enc), Prepared::<T>::new(b, enc));
    let mut sa = [T::default(); TILE];
    let mut sb = [T::default(); TILE];
    let mut out = Vec::with_capacity(a.col.len());
    let bias = T::from_i128(-enc.base());
    for start in (0..a.col.len()).step_by(TILE) {
        let n = TILE.min(a.col.len() - start);
        let av = a.tile(start, &mut sa[..n]);
        let bv = b.tile(start, &mut sb[..n]);
        T::append_binary(op, av, bv, &mut out, bias, start)?;
    }
    Ok(T::store(out, enc, range))
}

fn compare_kernel<T: Lane>(a: &Integer, b: &Integer, enc: Encoding) -> Vec<i8> {
    let (a, b) = (Prepared::<T>::new(a, enc), Prepared::<T>::new(b, enc));
    let mut sa = [T::default(); TILE];
    let mut sb = [T::default(); TILE];
    let mut out = Vec::with_capacity(a.col.len());
    for start in (0..a.col.len()).step_by(TILE) {
        let n = TILE.min(a.col.len() - start);
        out.extend(a.tile(start, &mut sa[..n]).iter().zip(b.tile(start, &mut sb[..n]))
            .map(|(&x, &y)| (x > y) as i8 - (x < y) as i8));
    }
    out
}

fn pick_kernel<T: Lane>(a: &Integer, b: &Integer, take_max: bool, enc: Encoding,
    range: Option<(i128, i128)>) -> Integer {
    let (a, b) = (Prepared::<T>::new(a, enc), Prepared::<T>::new(b, enc));
    let mut sa = [T::default(); TILE];
    let mut sb = [T::default(); TILE];
    let mut out = Vec::with_capacity(a.col.len());
    for start in (0..a.col.len()).step_by(TILE) {
        let n = TILE.min(a.col.len() - start);
        let av = a.tile(start, &mut sa[..n]);
        let bv = b.tile(start, &mut sb[..n]);
        if take_max { out.extend(av.iter().zip(bv).map(|(&x, &y)| x.max(y))); }
        else { out.extend(av.iter().zip(bv).map(|(&x, &y)| x.min(y))); }
    }
    T::store(out, enc, range)
}

fn sum_kernel<T: Lane>(cols: &[&Integer], enc: Encoding, range: Option<(i128, i128)>) -> Result<Integer, String> {
    let readers: Vec<_> = cols.iter().map(|c| Prepared::<T>::new(c, enc)).collect();
    let bias = T::from_i128(-enc.base());
    let mut scratch = [T::default(); TILE];
    let mut acc = [bias; TILE];
    let mut out = Vec::with_capacity(cols[0].len());
    for start in (0..cols[0].len()).step_by(TILE) {
        let n = TILE.min(cols[0].len() - start);
        acc[..n].fill(bias);
        for reader in &readers {
            for (j, (a, &x)) in acc[..n].iter_mut().zip(reader.tile(start, &mut scratch[..n])).enumerate() {
                *a = T::apply(Binary::Add, *a, x, bias)
                    .ok_or_else(|| format!("integer sum: i128 overflow at row {}", start + j))?;
            }
        }
        out.extend_from_slice(&acc[..n]);
    }
    Ok(T::store(out, enc, range))
}

/// Numeric vocabulary addition. Ordinary operations have no width parameter.
/// The checked i128 escape reports row failures through the existing effect
/// layer; the direct Rust column API returns Result. This is bounded arithmetic.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IntegerOp {
    Binary(Binary), Sum, Wrapping(Binary, u32), FromUnsigned, FromSigned, ToBytes, FromBytes,
}

impl IntegerOp {
    pub(crate) fn eval(&self, input: crate::Value) -> Result<crate::Value, String> {
        use crate::Value;
        // Return the existing Fail<T> representation. The effect rewrite lifts
        // downstream operations and handles nested lists and fold back-edges.
        fn success(v: Value) -> Value {
            let n = v.len();
            Value::sum_tagged(crate::Tags::constant(0, n), vec![v, Value::Unit(0)])
        }
        fn failures(xs: impl Iterator<Item = Option<i128>>) -> Value {
            let mut tags = Vec::new();
            let mut ok = Vec::new();
            let mut errors = 0;
            for x in xs { match x {
                Some(x) => { tags.push(0); ok.push(x); }
                None => { tags.push(1); errors += 1; }
            } }
            Value::sum(tags, vec![Value::integer(ok), Value::Unit(errors)])
        }
        match self {
            Self::Binary(op) | Self::Wrapping(op, _) => {
                let (a, b) = input.into_pair("integer binary")?;
                let (a, b) = (a.into_integer("integer lhs")?, b.into_integer("integer rhs")?);
                if a.len() != b.len() { return Err("integer: operands have different lengths".into()); }
                if let Self::Wrapping(_, width) = self { return Ok(Value::Int(a.wrapping(&b, *op, *width)?)); }
                match a.binary(&b, *op) {
                    Ok(out) => Ok(success(Value::Int(out))),
                    Err(_) => Ok(failures((0..a.len()).map(|i| <i128 as Lane>::apply(*op, a.at(i), b.at(i), 0)))),
                }
            }
            Self::Sum => {
                let cols = input.into_prod("integer sum")?;
                let ints: Vec<_> = cols.into_iter().map(|c| c.into_integer("integer sum")).collect::<Result<_, _>>()?;
                let refs = ints.iter().collect::<Vec<_>>();
                if refs.is_empty() { return Err("integer sum needs an operand".into()); }
                if refs.iter().any(|c| c.len() != refs[0].len()) { return Err("integer: operands have different lengths".into()); }
                match Integer::sum(&refs) {
                    Ok(out) => Ok(success(Value::Int(out))),
                    Err(_) => Ok(failures((0..refs[0].len()).map(|r| refs.iter().try_fold(0i128, |a, c| a.checked_add(c.at(r)))))),
                }
            }
            Self::FromBytes => {
                let p = input.into_prim("integer from bytes")?;
                let Prim::U8(bytes) = p else { return Err("integer from bytes requires U8".into()); };
                Ok(Value::Int(Integer::from_bytes(bytes)))
            }
            Self::ToBytes => {
                let i = input.into_integer("integer to bytes")?;
                match i.to_bytes() {
                    Ok(bytes) => Ok(success(Value::Prim(Prim::U8(bytes)))),
                    Err(_) => {
                        let mut tags = Vec::new(); let mut bytes = Vec::new(); let mut errors = 0;
                        for r in 0..i.len() { match u8::try_from(i.at(r)) {
                            Ok(b) => { tags.push(0); bytes.push(b); },
                            Err(_) => { tags.push(1); errors += 1; },
                        } }
                        Ok(Value::sum(tags, vec![Value::u8(bytes), Value::Unit(errors)]))
                    }
                }
            }
            Self::FromUnsigned | Self::FromSigned => {
                let p = input.into_prim("integer import")?;
                let frame = if matches!(self, Self::FromSigned) { Frame::Biased } else { Frame::Zero };
                let mut i = Integer { storage: Storage::Native(p, frame), range: None };
                i.range = bounds((0..i.len()).map(|r| i.at(r)));
                Ok(Value::Int(i))
            }
        }
    }
}
