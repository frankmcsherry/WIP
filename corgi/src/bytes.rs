//! Byte codec for a columnar [`Value`]: a whole column to and from a self-describing,
//! 8-byte-aligned byte string.
//!
//! This exists for the *distribution* boundary — shipping columns between processes — where the
//! sender has a `Value` and the receiver has bytes. The design constraints are the ones that make
//! a columnar exchange worth doing at all:
//!
//!   * **Per COLUMN, not per row.** A leaf's payload is written as one contiguous run of its
//!     stored bytes; the recursion costs one header per structural node, not per record. A
//!     million-row `u64` leaf is a header plus an 8 MB `write_all`.
//!   * **8-byte aligned throughout.** Every field occupies a whole number of 64-bit words, so a
//!     leaf payload always begins at an 8-aligned offset of an 8-aligned buffer. That is what lets
//!     a decoder read wide leaves as words rather than bytes.
//!   * **Self-describing.** The bytes carry the shape, exactly as a `Value` does: nothing outside
//!     needs to agree on a schema in advance, and [`read_from`] reconstructs the same `Value` the
//!     encoder held — same leaf widths, same `Bounds` encoding, same `⊥` lanes.
//!
//! The one thing the codec does NOT do is share buffers: a `Prim` is an `Arc<Vec<uN>>`, which owns
//! its allocation, so a decode must copy the payload into a fresh `Vec`. That copy is one memcpy
//! per leaf column and is the honest floor for this representation; borrowing bytes would take a
//! different leaf type, not a different codec.
//!
//! ```text
//! Value ::= Prim | Prod | Sum | List | Unit          (all quantities are u64 little-endian words)
//!   Prim  = 0, bits, len, payload[len * bits/8]      payload padded to a word boundary
//!   Prod  = 1, fields, Value*fields
//!   Sum   = 2, bits, len, payload[..],               the discriminant leaf, inline
//!               offsets, u64*offsets,                the carried within-variant offset per row
//!               lanes, (0 | 1, Value)*lanes          0 marks a `⊥` (uncommitted) lane
//!   List  = 3, 0, n, u64*n, Value                    `Offsets` form: one end offset per row
//!         | 3, 1, stride, rows, Value                `Stride` form: the uniform partition
//!   Unit  = 4, n
//! ```

use crate::value::{Bounds, Prim, Value};

/// Round a byte count up to a whole number of 64-bit words.
#[inline]
fn pad8(n: usize) -> usize { (n + 7) & !7 }

/// The exact number of bytes [`write_to`] emits for `v` — always a multiple of 8.
pub fn length_in_bytes(v: &Value) -> usize {
    match v {
        Value::Prim(p) => 24 + pad8(prim_payload_len(p)),
        Value::Prod(cols) => 16 + cols.iter().map(length_in_bytes).sum::<usize>(),
        Value::Sum(tags, offsets, lanes) => {
            24 + pad8(prim_payload_len(tags))          // the discriminant leaf, inline
                + 8 + 8 * offsets.len()                 // the within-variant offsets
                + 8                                     // the lane count
                + lanes.iter().map(|l| 8 + l.as_ref().map_or(0, length_in_bytes)).sum::<usize>()
        }
        Value::List(bounds, values) => 8 + bounds_len(bounds) + length_in_bytes(values),
        Value::Unit(_) => 16,
    }
}

/// Serialize `v`. The byte count matches [`length_in_bytes`] exactly.
pub fn write_to<W: std::io::Write>(v: &Value, writer: &mut W) -> std::io::Result<()> {
    match v {
        Value::Prim(p) => {
            word(writer, 0)?;
            write_prim(p, writer)
        }
        Value::Prod(cols) => {
            word(writer, 1)?;
            word(writer, cols.len() as u64)?;
            for c in cols { write_to(c, writer)?; }
            Ok(())
        }
        Value::Sum(tags, offsets, lanes) => {
            word(writer, 2)?;
            write_prim(tags, writer)?;
            word(writer, offsets.len() as u64)?;
            for &o in offsets { word(writer, o as u64)?; }
            word(writer, lanes.len() as u64)?;
            for lane in lanes {
                match lane {
                    None => word(writer, 0)?,
                    Some(v) => { word(writer, 1)?; write_to(v, writer)?; }
                }
            }
            Ok(())
        }
        Value::List(bounds, values) => {
            word(writer, 3)?;
            write_bounds(bounds, writer)?;
            write_to(values, writer)
        }
        Value::Unit(n) => {
            word(writer, 4)?;
            word(writer, *n as u64)
        }
    }
}

/// Deserialize a `Value` from the front of `bytes`, returning it and the number of bytes read.
///
/// `bytes` must be the output of [`write_to`] (possibly with trailing content, which is left
/// unread). Malformed input is reported rather than panicking, so a decoder can attribute a
/// corrupt or truncated message instead of dying inside the recursion.
pub fn read_from(bytes: &[u8]) -> Result<(Value, usize), String> {
    let mut r = Reader { bytes, at: 0 };
    let v = read_value(&mut r)?;
    Ok((v, r.at))
}

// --- encoding helpers ---------------------------------------------------------------------------

/// Write one 64-bit little-endian word — every field of the format is one or more of these.
#[inline]
fn word<W: std::io::Write>(writer: &mut W, x: u64) -> std::io::Result<()> {
    writer.write_all(&x.to_le_bytes())
}

/// The stored size of a leaf's payload, before word padding.
fn prim_payload_len(p: &Prim) -> usize {
    match p {
        Prim::U8(v) => v.len(),
        Prim::U16(v) => 2 * v.len(),
        Prim::U32(v) => 4 * v.len(),
        Prim::U64(v) => 8 * v.len(),
    }
}

/// A leaf as `bits, len, payload` — the shared body of `Prim` and of a `Sum`'s discriminant.
/// The payload goes out as ONE `write_all` per column (the point of the exercise) and is padded
/// with zeros to keep whatever follows word-aligned.
fn write_prim<W: std::io::Write>(p: &Prim, writer: &mut W) -> std::io::Result<()> {
    let (bits, len) = match p {
        Prim::U8(v) => (8u64, v.len()),
        Prim::U16(v) => (16, v.len()),
        Prim::U32(v) => (32, v.len()),
        Prim::U64(v) => (64, v.len()),
    };
    word(writer, bits)?;
    word(writer, len as u64)?;
    // A column of `uN` has no byte view without a cast, and corgi takes no dependencies, so the
    // widths above u8 go out through a word-at-a-time loop over a reusable stack buffer. This is
    // still a linear scan of the column with no per-row allocation or dispatch.
    match p {
        Prim::U8(v) => writer.write_all(v)?,
        Prim::U16(v) => write_le(writer, v.iter().map(|&x| x.to_le_bytes()))?,
        Prim::U32(v) => write_le(writer, v.iter().map(|&x| x.to_le_bytes()))?,
        Prim::U64(v) => write_le(writer, v.iter().map(|&x| x.to_le_bytes()))?,
    }
    let pad = pad8(prim_payload_len(p)) - prim_payload_len(p);
    if pad > 0 { writer.write_all(&[0u8; 8][..pad])?; }
    Ok(())
}

/// Write a run of fixed-width little-endian elements, buffered so the sink sees few large writes.
fn write_le<W: std::io::Write, const N: usize, I: Iterator<Item = [u8; N]>>(writer: &mut W, items: I) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    for item in items {
        buf.extend_from_slice(&item);
        if buf.len() >= 4096 {
            writer.write_all(&buf)?;
            buf.clear();
        }
    }
    if !buf.is_empty() { writer.write_all(&buf)?; }
    Ok(())
}

/// The byte count of an encoded [`Bounds`], form word included.
fn bounds_len(bounds: &Bounds) -> usize {
    match bounds {
        Bounds::Offsets(v) => 16 + 8 * v.len(),
        Bounds::Stride(..) => 24,
    }
}

/// A list's row partition, keeping the form it was in: a `Stride` is 16 bytes whatever its length,
/// which is the whole reason corgi tracks uniformity, and turning one into `Offsets` on the wire
/// would throw that away at exactly the moment it costs the most.
fn write_bounds<W: std::io::Write>(bounds: &Bounds, writer: &mut W) -> std::io::Result<()> {
    match bounds {
        Bounds::Offsets(v) => {
            word(writer, 0)?;
            word(writer, v.len() as u64)?;
            for &e in v { word(writer, e as u64)?; }
            Ok(())
        }
        Bounds::Stride(k, rows) => {
            word(writer, 1)?;
            word(writer, *k as u64)?;
            word(writer, *rows as u64)
        }
    }
}

// --- decoding -----------------------------------------------------------------------------------

/// A cursor over the encoded bytes. Every read is bounds-checked and reported as an error, so a
/// truncated or corrupt message cannot walk off the buffer or panic mid-recursion.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    /// Read one 64-bit little-endian word.
    fn word(&mut self) -> Result<u64, String> {
        let end = self.at + 8;
        if end > self.bytes.len() {
            return Err(format!("corgi::bytes: truncated at {} (need 8, have {})", self.at, self.bytes.len() - self.at));
        }
        let x = u64::from_le_bytes(self.bytes[self.at..end].try_into().unwrap());
        self.at = end;
        Ok(x)
    }
    /// Take `n` payload bytes and advance past their word padding.
    fn payload(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at + n;
        if end > self.bytes.len() {
            return Err(format!("corgi::bytes: truncated payload at {} (need {}, have {})", self.at, n, self.bytes.len() - self.at));
        }
        let slice = &self.bytes[self.at..end];
        self.at += pad8(n);
        if self.at > self.bytes.len() {
            return Err(format!("corgi::bytes: truncated padding at {}", end));
        }
        Ok(slice)
    }
    /// Read `n` words as a `Vec<usize>` — the offset/bounds vectors.
    fn words(&mut self, n: usize) -> Result<Vec<usize>, String> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.word()? as usize); }
        Ok(out)
    }
}

fn read_value(r: &mut Reader) -> Result<Value, String> {
    match r.word()? {
        0 => Ok(Value::Prim(read_prim(r)?)),
        1 => {
            let n = r.word()? as usize;
            let mut cols = Vec::with_capacity(n);
            for _ in 0..n { cols.push(read_value(r)?); }
            Ok(Value::Prod(cols))
        }
        2 => {
            let tags = read_prim(r)?;
            let n_offsets = r.word()? as usize;
            let offsets = r.words(n_offsets)?;
            let n_lanes = r.word()? as usize;
            let mut lanes = Vec::with_capacity(n_lanes);
            for _ in 0..n_lanes {
                lanes.push(match r.word()? {
                    0 => None,
                    1 => Some(read_value(r)?),
                    other => return Err(format!("corgi::bytes: bad lane marker {other}")),
                });
            }
            Ok(Value::Sum(tags, offsets, lanes))
        }
        3 => {
            let bounds = read_bounds(r)?;
            Ok(Value::List(bounds, Box::new(read_value(r)?)))
        }
        4 => Ok(Value::Unit(r.word()? as usize)),
        other => Err(format!("corgi::bytes: bad value tag {other}")),
    }
}

fn read_prim(r: &mut Reader) -> Result<Prim, String> {
    use std::sync::Arc;
    let bits = r.word()?;
    let len = r.word()? as usize;
    Ok(match bits {
        8 => Prim::U8(Arc::new(r.payload(len)?.to_vec())),
        16 => Prim::U16(Arc::new(read_le(r.payload(2 * len)?, u16::from_le_bytes))),
        32 => Prim::U32(Arc::new(read_le(r.payload(4 * len)?, u32::from_le_bytes))),
        64 => Prim::U64(Arc::new(read_le(r.payload(8 * len)?, u64::from_le_bytes))),
        other => return Err(format!("corgi::bytes: bad leaf width {other}")),
    })
}

/// Decode a payload of fixed-width little-endian elements into an owned column.
fn read_le<T, const N: usize>(bytes: &[u8], from_le: fn([u8; N]) -> T) -> Vec<T> {
    bytes.chunks_exact(N).map(|c| from_le(c.try_into().unwrap())).collect()
}

fn read_bounds(r: &mut Reader) -> Result<Bounds, String> {
    match r.word()? {
        0 => {
            let n = r.word()? as usize;
            // `Bounds::Offsets` directly, NOT `Bounds::from`: the encoder recorded which form the
            // sender held, and normalizing here would silently rewrite it. (The two compare equal
            // when they describe the same partition, so this is fidelity, not correctness.)
            Ok(Bounds::Offsets(r.words(n)?))
        }
        1 => {
            let stride = r.word()? as usize;
            let rows = r.word()? as usize;
            Ok(Bounds::Stride(stride, rows))
        }
        other => Err(format!("corgi::bytes: bad bounds form {other}")),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Encode, decode, and check both the value and the promised length.
    fn round_trip(v: &Value) {
        let mut buf = Vec::new();
        write_to(v, &mut buf).unwrap();
        assert_eq!(buf.len(), length_in_bytes(v), "length_in_bytes disagrees with write_to for {v:?}");
        assert_eq!(buf.len() % 8, 0, "encoding is not word-aligned for {v:?}");
        let (back, read) = read_from(&buf).unwrap();
        assert_eq!(read, buf.len(), "read_from consumed {read} of {} bytes", buf.len());
        assert_eq!(&back, v, "round trip changed the value");
    }

    /// One of each constructor, at each leaf width, including the empty and `⊥` cases.
    fn corpus() -> Vec<Value> {
        vec![
            Value::Unit(0),
            Value::Unit(7),
            Value::u8(vec![]),
            Value::u8(vec![1, 2, 3]),                       // an odd payload length, to exercise padding
            Value::u16(vec![1, 2, 3, 4, 5]),
            Value::u32(vec![7; 9]),
            Value::u64(vec![u64::MAX, 0, 12345]),
            Value::Prod(vec![]),
            Value::Prod(vec![Value::u64(vec![1, 2]), Value::u8(vec![3, 4])]),
            Value::List(Bounds::Offsets(vec![1, 1, 4]), Box::new(Value::u32(vec![9, 8, 7, 6]))),
            Value::List(Bounds::Stride(2, 3), Box::new(Value::u64(vec![1, 2, 3, 4, 5, 6]))),
            Value::Sum(Prim::U8(std::sync::Arc::new(vec![0, 1, 0])), vec![0, 0, 1],
                       vec![Some(Value::u64(vec![10, 20])), Some(Value::u16(vec![30]))]),
            // a `⊥` lane: uncommitted, holds no rows, and must survive as `None`
            Value::Sum(Prim::U8(std::sync::Arc::new(vec![0, 0])), vec![0, 1],
                       vec![Some(Value::u64(vec![1, 2])), None]),
            // nesting: the recursion has to keep alignment across every level
            Value::Prod(vec![
                Value::List(Bounds::Offsets(vec![2, 3]), Box::new(Value::Prod(vec![
                    Value::u8(vec![1, 2, 3]),
                    Value::u64(vec![4, 5, 6]),
                ]))),
                Value::Unit(2),
            ]),
        ]
    }

    #[test]
    fn round_trips() {
        for v in corpus() { round_trip(&v); }
    }

    #[test]
    fn round_trip_is_shape_preserving() {
        for v in corpus() {
            let mut buf = Vec::new();
            write_to(&v, &mut buf).unwrap();
            let (back, _) = read_from(&buf).unwrap();
            assert_eq!(crate::shape_of_value(&back), crate::shape_of_value(&v));
            assert_eq!(back.len(), v.len());
        }
    }

    /// Rows survive individually, not just in bulk: the decoded column hashes row-for-row like
    /// the original, which is the property the distribution boundary actually depends on.
    #[test]
    fn round_trip_preserves_row_hashes() {
        for v in corpus() {
            let mut buf = Vec::new();
            write_to(&v, &mut buf).unwrap();
            let (back, _) = read_from(&buf).unwrap();
            assert_eq!(crate::arrange::hash_rows(&back), crate::arrange::hash_rows(&v));
        }
    }

    /// A truncated message is an error, not a panic or an out-of-bounds read.
    #[test]
    fn truncation_is_an_error() {
        for v in corpus() {
            let mut buf = Vec::new();
            write_to(&v, &mut buf).unwrap();
            for cut in (0..buf.len()).step_by(8) {
                assert!(read_from(&buf[..cut]).is_err(), "decoding {cut} of {} bytes should fail", buf.len());
            }
        }
    }

    /// A leaf column goes out as its stored bytes plus a fixed header — the per-column,
    /// not per-row, cost the codec exists to deliver.
    #[test]
    fn wide_leaves_cost_their_payload() {
        let v = Value::u64((0..10_000u64).collect());
        assert_eq!(length_in_bytes(&v), 24 + 8 * 10_000);
    }

    /// A splitmix64 stream — deterministic, seedable, no dependency. Enough randomness to shake
    /// out shapes a hand-written corpus does not think of.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            crate::hash::mix64(self.0)
        }
        /// A value in `[0, n)`.
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// A well-formed `Value` of exactly `rows` rows, nested up to `depth` levels.
    ///
    /// Well-formed matters more than random here: a `Prod`'s fields agree on length, a `Sum`'s
    /// offsets are the running per-lane counts its tags imply, and a `List`'s bounds total its
    /// values' length. Generating malformed columns would test the codec against inputs the rest
    /// of corgi cannot produce.
    fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
        // At depth 0 only leaves, so recursion always terminates.
        let arms = if depth == 0 { 2 } else { 5 };
        match rng.below(arms) {
            0 => match rng.below(4) {
                0 => Value::u8((0..rows).map(|_| rng.next() as u8).collect()),
                1 => Value::u16((0..rows).map(|_| rng.next() as u16).collect()),
                2 => Value::u32((0..rows).map(|_| rng.next() as u32).collect()),
                _ => Value::u64((0..rows).map(|_| rng.next()).collect()),
            },
            1 => Value::Unit(rows),
            2 => {
                let fields = 1 + rng.below(3);
                Value::Prod((0..fields).map(|_| random_value(rng, rows, depth - 1)).collect())
            }
            3 => {
                // Lists: sometimes uniform (so `Stride` is exercised), sometimes ragged.
                let (bounds, total) = if rng.below(2) == 0 {
                    let stride = rng.below(3);
                    (Bounds::Stride(stride, rows), stride * rows)
                } else {
                    let mut ends = Vec::with_capacity(rows);
                    let mut acc = 0;
                    for _ in 0..rows {
                        acc += rng.below(3);
                        ends.push(acc);
                    }
                    (Bounds::Offsets(ends), acc)
                };
                Value::List(bounds, Box::new(random_value(rng, total, depth - 1)))
            }
            _ => {
                // Sums: pick a tag per row, count per lane, and leave some lanes uncommitted (`⊥`).
                let lanes = 1 + rng.below(3);
                let tags: Vec<usize> = (0..rows).map(|_| rng.below(lanes)).collect();
                let mut counts = vec![0usize; lanes];
                let offsets: Vec<usize> = tags
                    .iter()
                    .map(|&t| {
                        counts[t] += 1;
                        counts[t] - 1
                    })
                    .collect();
                let variants = counts
                    .iter()
                    .map(|&n| if n == 0 { None } else { Some(random_value(rng, n, depth - 1)) })
                    .collect();
                Value::Sum(Prim::U8(std::sync::Arc::new(tags.iter().map(|&t| t as u8).collect())), offsets, variants)
            }
        }
    }

    /// The general property, over shapes nobody wrote down: whatever the encoder was handed comes
    /// back, its promised length is its actual length, and the encoding stays word-aligned.
    #[test]
    fn round_trips_random_shapes() {
        let mut rng = Rng(0x5EED);
        for i in 0..400 {
            let rows = i % 7; // includes 0 — empty columns at every nesting depth
            let v = random_value(&mut rng, rows, 3);
            round_trip(&v);
        }
    }

    /// Row identity survives too, for the same random shapes: the decoded column hashes
    /// row-for-row like the original, which is what a distribution boundary depends on.
    #[test]
    fn random_shapes_preserve_row_hashes() {
        let mut rng = Rng(0xC0FFEE);
        for i in 0..400 {
            let v = random_value(&mut rng, 1 + i % 9, 3);
            let mut buf = Vec::new();
            write_to(&v, &mut buf).unwrap();
            let (back, _) = read_from(&buf).unwrap();
            assert_eq!(crate::arrange::hash_rows(&back), crate::arrange::hash_rows(&v), "{v:?}");
        }
    }

    /// Truncating a random shape is an error, never a panic or an out-of-bounds read.
    #[test]
    fn random_shapes_reject_truncation() {
        let mut rng = Rng(0xBADCAFE);
        for i in 0..100 {
            let v = random_value(&mut rng, 1 + i % 5, 3);
            let mut buf = Vec::new();
            write_to(&v, &mut buf).unwrap();
            for cut in (0..buf.len()).step_by(8) {
                assert!(read_from(&buf[..cut]).is_err(), "decoding {cut} of {} bytes should fail: {v:?}", buf.len());
            }
        }
    }
}
