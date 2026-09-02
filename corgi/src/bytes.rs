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
//!     encoder held — same leaf widths, same `Bounds` encoding.
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
//!               lanes, Value*lanes                     one column per variant (empty if unused)
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
                + lanes.iter().map(length_in_bytes).sum::<usize>()
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
            for lane in lanes { write_to(lane, writer)?; }
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
/// `bytes` is normally the output of [`write_to`] (trailing content is left unread), but it does
/// not have to be: these bytes came off a wire, so **any** byte string must produce a `Value` or an
/// `Err`, never a panic, an abort, or an unbounded allocation. What that guarantee covers:
///
/// * **Framing.** Every read is bounds-checked; every length is checked against the bytes that
///   remain before it is believed, so no size arithmetic can wrap and no reservation can exceed
///   what the buffer could possibly hold.
/// * **Depth.** The recursion is capped ([`MAX_DEPTH`]). A short message of nested headers cannot
///   exhaust the stack.
/// * **Structure.** The returned `Value` satisfies the invariants the rest of corgi indexes by: a
///   `Prod`'s fields agree on length, a `Sum`'s tags name its lanes and its offsets land
///   inside them, a `List`'s bounds are non-decreasing and stay within its values. So a decoded
///   column can be hashed, compared, gathered and sorted like any other.
///
/// What it does *not* cover, both by design:
///
/// * **Corruption that stays within those bounds.** A flipped bit in a leaf payload, or an offset
///   that still points somewhere valid, decodes to a well-formed `Value` holding the wrong data.
///   Detecting that is a checksum's job, one layer up.
/// * **Declared row counts.** The payload-free constructors name rows without spending bytes on
///   them: `Unit(n)` is sixteen bytes whatever `n` is, and a `Stride(k, rows)` is twenty-four. So a
///   short message can denote an enormous column, and `Value::len` does not reveal it — the `Unit`
///   may be nested inside a `Sum` lane or under a `List` whose own row count is small. Use
///   [`declared_rows`] before doing per-row work on bytes from a peer you do not trust.
///
///   This is not an artifact of the codec. A `Unit` is O(1) in memory too, and per-row work on one
///   is O(n) whether it arrived over a wire or was built in process; the codec only makes it
///   reachable from outside. The right place to close it is the layer that knows how many rows the
///   message is *supposed* to have — for the DDIR container that is the time column, and its
///   decoder checks all four columns agree.
pub fn read_from(bytes: &[u8]) -> Result<(Value, usize), String> {
    let mut r = Reader { bytes, at: 0, depth: 0 };
    let v = read_value(&mut r)?;
    Ok((v, r.at))
}

/// How deep the decoder will follow nested headers before giving up.
///
/// Each level costs 16 wire bytes, so without a cap a 16 MB message of one-field `Prod` headers
/// walks a million frames deep and takes the process down with a stack overflow — which is not
/// something a caller can catch.
///
/// 128 is chosen from both ends. Real corgi shapes are a handful of levels, so it is an order of
/// magnitude past anything a program produces. And it is measured, not guessed — every traversal of
/// a `Value` in corgi recurses, and each has its own cliff. Depth at which a one-field `Prod` chain
/// stops surviving, on the 2 MB stack `cargo test` gives a test thread:
///
/// ```text
///                     release   debug
///   read_from            3660     479     <- this decoder
///   hash_rows            4880    1164
///   shape_of_value      10983    1688
///   write_to            21966    3760
///   Drop / len          32949    8780
/// ```
///
/// So the cap is not really protecting the decoder — it is protecting everything the decoder hands
/// a value to, and `hash_rows` is the one that gives out first. 128 sits a factor of four under the
/// lowest number in that table. Raising it means redoing the measurement, on whichever traversal is
/// weakest at the time.
///
/// This is also the answer to "why not make the decode iterative and drop the cap?". An explicit
/// stack would move `read_from` off the bottom of that table, but only as far as the next row: the
/// derived `Drop` recurses, and so do `len`, `shape_of_value`, `hash_rows` and `PartialEq`. Lifting
/// the ceiling means making all of them iterative, which is a corgi-wide change with its own
/// payoff (arbitrarily deep shapes) — not something a codec can do on its own.
pub const MAX_DEPTH: usize = 128;

/// The largest row count declared anywhere in `v`, saturating.
///
/// `Value::len` reports the top column's rows, which is the wrong question for bytes off a wire:
/// the expensive column may be nested. `Sum(tags=[0], lanes=[Unit(2^61)])` has `len() == 1`, and
/// hashing it allocates 2^61 words. This walks the whole structure and reports the worst declared
/// count, so a consumer can refuse a message that claims more rows than it is willing to
/// materialize — the check [`read_from`] deliberately does not make on the caller's behalf, since
/// only the caller knows how many rows are plausible.
///
/// Cost is O(nodes), not O(rows) — it reads declarations, never payloads.
pub fn declared_rows(v: &Value) -> u64 {
    match v {
        Value::Prim(p) => prim_len(p) as u64,
        Value::Prod(cols) => cols.iter().map(declared_rows).max().unwrap_or(0),
        Value::Sum(tags, offsets, lanes) => (prim_len(tags).max(offsets.len()) as u64)
            .max(lanes.iter().map(declared_rows).max().unwrap_or(0)),
        Value::List(bounds, values) => (bounds_rows(bounds) as u64)
            .max(bounds_total(bounds))
            .max(declared_rows(values)),
        Value::Unit(n) => *n as u64,
    }
}

/// A partition's row count, without touching its values.
fn bounds_rows(bounds: &Bounds) -> usize {
    match bounds {
        Bounds::Offsets(v) => v.len(),
        Bounds::Stride(_, rows) => *rows,
    }
}

/// A partition's flattened element count, saturating — `Bounds::total` multiplies, and this is
/// reachable from outside on values the checks have not seen.
fn bounds_total(bounds: &Bounds) -> u64 {
    match bounds {
        Bounds::Offsets(v) => v.last().copied().unwrap_or(0) as u64,
        Bounds::Stride(k, rows) => (*k as u64).saturating_mul(*rows as u64),
    }
}

/// A leaf's element count. (`Prim::len` is crate-private and this module is the only outside-facing
/// reader of it.)
fn prim_len(p: &Prim) -> usize {
    match p {
        Prim::U8(v) => v.len(),
        Prim::U16(v) => v.len(),
        Prim::U32(v) => v.len(),
        Prim::U64(v) => v.len(),
    }
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

/// A cursor over the encoded bytes.
///
/// Two rules make the decoder total. **Nothing is sized before it is bounded**: a count read off
/// the wire is checked against the bytes that remain before it is multiplied, reserved, or looped
/// over, so no arithmetic wraps and no reservation exceeds what the buffer could hold. And the
/// recursion carries its own `depth`, so a header chain cannot outrun the stack.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    depth: usize,
}

impl<'a> Reader<'a> {
    /// Bytes not yet consumed — the ceiling on any length this message can legitimately claim.
    #[inline]
    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    /// Read one 64-bit little-endian word.
    fn word(&mut self) -> Result<u64, String> {
        if self.remaining() < 8 {
            return Err(format!("corgi::bytes: truncated at {} (need 8, have {})", self.at, self.remaining()));
        }
        let x = u64::from_le_bytes(self.bytes[self.at..self.at + 8].try_into().unwrap());
        self.at += 8;
        Ok(x)
    }

    /// Read a length word, rejecting anything the remaining bytes could not encode.
    ///
    /// `per` is the smallest number of bytes one element can occupy. Checking here rather than at
    /// the point of use is what keeps every later `n * width` and `with_capacity(n)` honest: a
    /// wire-supplied `u64::MAX` never reaches them to wrap or to reserve.
    fn count(&mut self, per: usize, what: &str) -> Result<usize, String> {
        let n = self.word()?;
        let max = (self.remaining() / per) as u64;
        if n > max {
            return Err(format!("corgi::bytes: {what} claims {n} but only {max} fit in the remaining {} bytes", self.remaining()));
        }
        Ok(n as usize)
    }

    /// Take `n` payload bytes and advance past their word padding.
    fn payload(&mut self, n: usize) -> Result<&'a [u8], String> {
        if n > self.remaining() {
            return Err(format!("corgi::bytes: truncated payload at {} (need {}, have {})", self.at, n, self.remaining()));
        }
        let slice = &self.bytes[self.at..self.at + n];
        let padded = pad8(n);
        if padded > self.remaining() {
            return Err(format!("corgi::bytes: truncated padding at {}", self.at + n));
        }
        self.at += padded;
        Ok(slice)
    }

    /// Read `n` words as a `Vec<usize>` — the offset/bounds vectors. `n` has already been bounded
    /// by [`count`](Self::count), so the reservation cannot exceed the buffer.
    fn words(&mut self, n: usize) -> Result<Vec<usize>, String> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.word()? as usize);
        }
        Ok(out)
    }

    /// Run `f` one level deeper, refusing to go past [`MAX_DEPTH`].
    fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T, String>) -> Result<T, String> {
        if self.depth >= MAX_DEPTH {
            return Err(format!("corgi::bytes: nesting deeper than {MAX_DEPTH} at byte {}", self.at));
        }
        self.depth += 1;
        let out = f(self);
        self.depth -= 1;
        out
    }
}

/// The smallest encoding of a whole `Value` is `Unit`: a tag word and a count word.
const MIN_VALUE_BYTES: usize = 16;

fn read_value(r: &mut Reader) -> Result<Value, String> {
    match r.word()? {
        0 => Ok(Value::Prim(read_prim(r)?)),
        1 => {
            let n = r.count(MIN_VALUE_BYTES, "product fields")?;
            let mut cols = Vec::with_capacity(n);
            for _ in 0..n {
                cols.push(r.nested(read_value)?);
            }
            // `Value::len` reads field 0, so fields that disagree on length would make the column
            // silently lie about how many rows it holds.
            if let Some(first) = cols.first() {
                let rows = first.len();
                if let Some(bad) = cols.iter().position(|c| c.len() != rows) {
                    return Err(format!("corgi::bytes: product field {bad} has {} rows, field 0 has {rows}", cols[bad].len()));
                }
            }
            Ok(Value::Prod(cols))
        }
        2 => {
            let tags = read_prim(r)?;
            let n_offsets = r.count(8, "sum offsets")?;
            let offsets = r.words(n_offsets)?;
            // The smallest value (a `Unit`) is two words, so that is the floor per lane.
            let n_lanes = r.count(16, "sum lanes")?;
            let mut lanes = Vec::with_capacity(n_lanes);
            for _ in 0..n_lanes {
                lanes.push(r.nested(read_value)?);
            }
            check_sum(&tags, &offsets, &lanes)?;
            Ok(Value::Sum(tags, offsets, lanes))
        }
        3 => {
            let bounds = read_bounds(r)?;
            let values = r.nested(read_value)?;
            check_list(&bounds, &values)?;
            Ok(Value::List(bounds, Box::new(values)))
        }
        4 => Ok(Value::Unit(r.word()? as usize)),
        other => Err(format!("corgi::bytes: bad value tag {other}")),
    }
}

/// The `Sum` invariants every reader indexes by: a u8 discriminant naming one of the lanes, and a
/// carried offset that lands inside it. Without these, `hash_rows` and the comparators index out
/// of bounds on a column the decoder handed them.
fn check_sum(tags: &Prim, offsets: &[usize], lanes: &[Value]) -> Result<(), String> {
    // `Value::sum` stores the discriminant as a u8 and asserts the arity fits it; a wider
    // discriminant off the wire would be a shape corgi cannot construct.
    if !matches!(tags, Prim::U8(_)) {
        return Err("corgi::bytes: sum discriminant must be a u8 leaf".into());
    }
    if lanes.len() > 256 {
        return Err(format!("corgi::bytes: {} sum lanes exceeds the u8 tag width", lanes.len()));
    }
    let tag_vec = match tags {
        Prim::U8(v) => v,
        _ => unreachable!("checked above"),
    };
    if offsets.len() != tag_vec.len() {
        return Err(format!("corgi::bytes: {} sum offsets for {} tags", offsets.len(), tag_vec.len()));
    }
    let lane_rows: Vec<usize> = lanes.iter().map(Value::len).collect();
    for (row, (&t, &o)) in tag_vec.iter().zip(offsets).enumerate() {
        match lane_rows.get(t as usize) {
            None => return Err(format!("corgi::bytes: row {row} has tag {t} but there are {} lanes", lanes.len())),
            Some(rows) if o >= *rows => {
                return Err(format!("corgi::bytes: row {row} offset {o} is outside lane {t} ({rows} rows)"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The `List` invariant: the partition has to stay inside the values it partitions, and it has to
/// be non-decreasing, or `Bounds::span` yields a reversed range and panics on the slice.
fn check_list(bounds: &Bounds, values: &Value) -> Result<(), String> {
    let rows = values.len();
    match bounds {
        Bounds::Offsets(ends) => {
            let mut prev = 0;
            for (i, &e) in ends.iter().enumerate() {
                if e < prev {
                    return Err(format!("corgi::bytes: list bound {i} = {e} is below its predecessor {prev}"));
                }
                prev = e;
            }
            if prev > rows {
                return Err(format!("corgi::bytes: list bounds reach {prev} over {rows} values"));
            }
        }
        Bounds::Stride(k, n) => {
            let total = k.checked_mul(*n).ok_or_else(|| format!("corgi::bytes: list stride {k} x {n} rows overflows"))?;
            if total > rows {
                return Err(format!("corgi::bytes: list stride {k} x {n} rows reaches {total} over {rows} values"));
            }
        }
    }
    Ok(())
}

fn read_prim(r: &mut Reader) -> Result<Prim, String> {
    use std::sync::Arc;
    let bits = r.word()?;
    // Bound the element count by the width BEFORE multiplying: a wire-supplied length near
    // `u64::MAX` would otherwise wrap `len * width` — to something small in release (a corrupt
    // header decoding "successfully" to an empty leaf, desyncing the frame) or to something huge
    // that walks off the buffer.
    let payload = match bits {
        8 => r.count(1, "u8 leaf")?,
        16 => 2 * r.count(2, "u16 leaf")?,
        32 => 4 * r.count(4, "u32 leaf")?,
        64 => 8 * r.count(8, "u64 leaf")?,
        other => return Err(format!("corgi::bytes: bad leaf width {other}")),
    };
    let bytes = r.payload(payload)?;
    Ok(match bits {
        8 => Prim::U8(Arc::new(bytes.to_vec())),
        16 => Prim::U16(Arc::new(read_le(bytes, u16::from_le_bytes))),
        32 => Prim::U32(Arc::new(read_le(bytes, u32::from_le_bytes))),
        _ => Prim::U64(Arc::new(read_le(bytes, u64::from_le_bytes))),
    })
}

/// Decode a payload of fixed-width little-endian elements into an owned column.
fn read_le<T, const N: usize>(bytes: &[u8], from_le: fn([u8; N]) -> T) -> Vec<T> {
    bytes.chunks_exact(N).map(|c| from_le(c.try_into().unwrap())).collect()
}

fn read_bounds(r: &mut Reader) -> Result<Bounds, String> {
    match r.word()? {
        0 => {
            let n = r.count(8, "list bounds")?;
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

    /// One of each constructor, at each leaf width, including the empty cases.
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
                       vec![Value::u64(vec![10, 20]), Value::u16(vec![30])]),
            // a lane no row uses: an empty column of its shape, which must survive as such
            Value::Sum(Prim::U8(std::sync::Arc::new(vec![0, 0])), vec![0, 1],
                       vec![Value::u64(vec![1, 2]), Value::u16(vec![])]),
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
                // Sums: pick a tag per row, count per lane; a lane no row picks is an empty column.
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
                    .map(|&n| random_value(rng, n, depth - 1))
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

    // --- adversarial input ----------------------------------------------------------------------
    //
    // Truncation is the easy malformed-input family: it shortens the buffer, so the bounds checks
    // catch it. The interesting families corrupt a length or a tag IN PLACE, leaving the buffer
    // exactly as long as the decoder expects — which is where wrapped arithmetic, wire-sized
    // reservations, unbounded recursion, and structurally impossible columns live.
    //
    // Several of these depend on the build profile (a debug overflow panic is a release wrap), so
    // run them both ways: `cargo test` and `cargo test --release`.

    /// Word values chosen to break size arithmetic: zero and small tags, the wrap-to-large
    /// maximum, the wrap-to-small `2^63` (doubling it is 0), and a value big enough to make any
    /// reservation fatal if it were believed.
    const NASTY: [u64; 8] = [0, 1, 2, 5, u64::MAX, 1 << 63, 1 << 61, 1 << 32];

    /// Use a decoded value the way a consumer would. This is the real assertion of the adversarial
    /// tests: not just that `read_from` returned, but that what it returned can be indexed,
    /// hashed and shaped without panicking — which is what "the decode validates structure" means.
    fn exercise(v: &Value) {
        let _ = crate::shape_of_value(v);
        let _ = v.len();
        // The payload-free constructors declare rows without spending bytes, so a mutated header
        // can legitimately name an enormous column — documented, and `declared_rows` is exactly
        // the guard a consumer is told to apply. Using it here is the test asserting that the
        // advice works: `Value::len` alone would not see a `Unit` nested in a `Sum` lane.
        if declared_rows(v) <= 10_000 {
            let _ = crate::arrange::hash_rows(v);
        }
    }

    /// Corrupt one word of a valid encoding, leaving the length alone, and the decoder must still
    /// return — `Err`, or an `Ok` whose value is safe to use. This is the family the truncation
    /// test cannot reach.
    #[test]
    fn mutated_headers_never_panic() {
        let mut rng = Rng(0xD15EA5E);
        let mut values = corpus();
        values.extend((0..40).map(|i| random_value(&mut rng, 1 + i % 5, 3)));
        for v in &values {
            let mut buf = Vec::new();
            write_to(v, &mut buf).unwrap();
            for word in 0..buf.len() / 8 {
                let original: [u8; 8] = buf[word * 8..word * 8 + 8].try_into().unwrap();
                for nasty in NASTY {
                    buf[word * 8..word * 8 + 8].copy_from_slice(&nasty.to_le_bytes());
                    if let Ok((decoded, read)) = read_from(&buf) {
                        assert!(read <= buf.len(), "reported {read} bytes read of {}", buf.len());
                        exercise(&decoded);
                    }
                }
                buf[word * 8..word * 8 + 8].copy_from_slice(&original);
            }
        }
    }

    /// A length near `u64::MAX` must be rejected on its face, not multiplied by a leaf width
    /// first. In debug that multiply panics; in release it wraps — to something small, which
    /// would decode "successfully" to an empty leaf and desync the frame for a framing consumer,
    /// or to something huge, which would walk off the buffer.
    #[test]
    fn leaf_lengths_are_bounded_before_they_are_scaled() {
        for bits in [8u64, 16, 32, 64] {
            for len in [u64::MAX, 1 << 63, 1 << 61, 1 << 32, 1000] {
                // tag = Prim, then the width and the claimed element count, and nothing after.
                let mut buf = Vec::new();
                for w in [0, bits, len] {
                    buf.extend_from_slice(&w.to_le_bytes());
                }
                assert!(
                    read_from(&buf).is_err(),
                    "a {bits}-bit leaf claiming {len} elements with no payload must be rejected"
                );
            }
        }
    }

    /// Field, lane and bound counts are reservations, so they must be bounded by what the
    /// remaining bytes could encode before they reach `Vec::with_capacity` — otherwise a
    /// sixteen-byte message asks for a multi-gigabyte allocation.
    #[test]
    fn wire_counts_do_not_become_reservations() {
        // (value tag, the words that precede the count, description)
        let cases: [(u64, &[u64], &str); 3] = [
            (1, &[], "product fields"),
            (3, &[0], "list bounds"),   // List, Offsets form, then the bound count
            (4, &[], "unit rows"),      // Unit's count is not a reservation; it must still not panic
        ];
        for (tag, prefix, what) in cases {
            for n in [u64::MAX, 1 << 61, 1 << 40, 1 << 30] {
                let mut buf = Vec::new();
                buf.extend_from_slice(&tag.to_le_bytes());
                for w in prefix {
                    buf.extend_from_slice(&w.to_le_bytes());
                }
                buf.extend_from_slice(&n.to_le_bytes());
                // A `Unit` legitimately declares rows in no bytes; everything else must be refused.
                let result = read_from(&buf);
                if tag == 4 {
                    assert!(result.is_ok(), "{what}: a unit row count is not a reservation");
                } else {
                    assert!(result.is_err(), "{what}: {n} must be refused, not reserved");
                }
            }
        }
    }

    /// A chain of nested headers costs sixteen bytes a level, so without a cap a small message
    /// walks the decoder off the stack — an abort the caller cannot catch, not an `Err`.
    #[test]
    fn nesting_is_capped() {
        // One-field products all the way down: `Prod, 1, Prod, 1, ...`. Deep enough that without
        // the cap this is the reported failure — a stack overflow and a process abort, not an
        // `Err` — so reverting the cap fails this test the way the bug actually behaves.
        let levels = 100_000;
        let mut buf = Vec::with_capacity(16 * levels + 16);
        for _ in 0..levels {
            buf.extend_from_slice(&1u64.to_le_bytes());
            buf.extend_from_slice(&1u64.to_le_bytes());
        }
        buf.extend_from_slice(&4u64.to_le_bytes()); // a Unit at the bottom
        buf.extend_from_slice(&0u64.to_le_bytes());
        let err = read_from(&buf).expect_err("nesting past the cap must be an error");
        assert!(err.contains("nesting"), "unexpected error: {err}");

        // And the cap is generous rather than tight: a shape well inside it still decodes.
        let mut deep = Value::Unit(3);
        for _ in 0..MAX_DEPTH / 2 {
            deep = Value::Prod(vec![deep]);
        }
        round_trip(&deep);
    }

    /// The structural invariants the rest of corgi indexes by. Each of these is a byte string the
    /// framing accepts and the structure must not: without the checks, the first two panic inside
    /// `hash_rows` on a column `read_from` handed back as valid.
    #[test]
    fn structurally_impossible_columns_are_refused() {
        /// Encode `v`, overwrite word `word` with `to`, and return the bytes — same length as a
        /// valid message, so only the structure is wrong.
        fn patched(v: &Value, word: usize, to: u64) -> Vec<u8> {
            let mut buf = Vec::new();
            write_to(v, &mut buf).unwrap();
            buf[word * 8..word * 8 + 8].copy_from_slice(&to.to_le_bytes());
            buf
        }

        // A sum whose tag names a lane that is not there. Words: [Sum][bits][len][tags payload]…
        // and the payload word carries the single u8 discriminant in its low byte.
        let bad_tag = patched(
            &Value::Sum(Prim::U8(std::sync::Arc::new(vec![0])), vec![0], vec![Value::u64(vec![7])]),
            3,
            5,
        );
        assert!(read_from(&bad_tag).is_err(), "a tag naming a missing lane must be refused");

        // A sum whose carried offset points past the end of the lane it names.
        let bad_offset = patched(
            &Value::Sum(Prim::U8(std::sync::Arc::new(vec![0])), vec![0], vec![Value::u64(vec![7])]),
            5, // [Sum][bits][len][tags][n_offsets][offsets[0]]
            9,
        );
        assert!(read_from(&bad_offset).is_err(), "an offset outside its lane must be refused");

        // A list whose partition reaches past its values. Words: [List][form][n][ends[0]]…
        let over_reach = patched(&Value::List(Bounds::Offsets(vec![2]), Box::new(Value::u64(vec![1, 2]))), 3, 10);
        assert!(read_from(&over_reach).is_err(), "bounds reaching past the values must be refused");

        // The `Stride` form of the same thing: three rows of two over a two-element leaf.
        let over_stride = patched(&Value::List(Bounds::Stride(2, 1), Box::new(Value::u64(vec![1, 2]))), 3, 3);
        assert!(read_from(&over_stride).is_err(), "a stride reaching past the values must be refused");

        // A product whose fields disagree on length — `Value::len` reads field 0, so the column
        // would silently lie about how many rows it holds. Words:
        // [Prod][2] [Prim][64][2][payload×2] [Prim][64][2][payload×2], so field 1's count is word 9.
        let ragged = patched(&Value::Prod(vec![Value::u64(vec![1, 2]), Value::u64(vec![3, 4])]), 9, 1);
        assert!(read_from(&ragged).is_err(), "a product with ragged fields must be refused");

        // A sum discriminant at a width corgi cannot construct (`sum_opt` stores u8 and asserts
        // the arity fits it), which would otherwise let a tag column carry more than 256 lanes.
        let wide_tags = patched(
            &Value::Sum(Prim::U8(std::sync::Arc::new(vec![0])), vec![0], vec![Value::u64(vec![7])]),
            1,
            64,
        );
        assert!(read_from(&wide_tags).is_err(), "a non-u8 sum discriminant must be refused");
    }

    /// `declared_rows` has to see what `Value::len` cannot, or the advice attached to it is
    /// useless: the expensive column is the nested one.
    #[test]
    fn declared_rows_sees_through_nesting() {
        let huge = 1usize << 40;

        // A one-row sum whose lane names a trillion rows.
        let hidden_in_a_lane = Value::Sum(
            Prim::U8(std::sync::Arc::new(vec![0])),
            vec![0],
            vec![Value::Unit(huge)],
        );
        assert_eq!(hidden_in_a_lane.len(), 1);
        assert_eq!(declared_rows(&hidden_in_a_lane), huge as u64);

        // A one-row list whose values do.
        let hidden_under_a_list = Value::List(Bounds::Offsets(vec![huge]), Box::new(Value::Unit(huge)));
        assert_eq!(hidden_under_a_list.len(), 1);
        assert_eq!(declared_rows(&hidden_under_a_list), huge as u64);

        // A stride multiplies rather than storing, so its total is where the size hides.
        let hidden_in_a_stride = Value::List(Bounds::Stride(huge, 2), Box::new(Value::Unit(2 * huge)));
        assert_eq!(hidden_in_a_stride.len(), 2);
        assert_eq!(declared_rows(&hidden_in_a_stride), 2 * huge as u64);

        // And it does not overflow on a stride that would.
        let overflowing = Value::List(Bounds::Stride(usize::MAX, usize::MAX), Box::new(Value::Unit(0)));
        assert_eq!(declared_rows(&overflowing), u64::MAX);

        // On ordinary columns it agrees with `len`.
        for v in corpus() {
            if matches!(v, Value::List(..)) {
                continue; // a list's values are legitimately longer than its rows
            }
            assert!(declared_rows(&v) >= v.len() as u64, "{v:?}");
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
