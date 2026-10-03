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
//!   Sum   = 2, 0, bits, len, payload[..],            `Column` form: the discriminant leaf, inline
//!               offsets, u64*offsets,                  the carried within-lane offset per row
//!               lanes, Value*lanes                     one column per variant (empty if unused)
//!         | 2, 1, tag, rows, lanes, Value*lanes       `Const` form: every row carries `tag`, so
//!                                                      neither witness column is on the wire
//!   List  = 3, 0, n, u64*n, Value                    `Offsets` form: one end offset per row
//!         | 3, 1, stride, rows, Value                `Stride` form: the uniform partition
//!   Unit  = 4, n
//!   Ref   = 5, n, (lo, hi)*n, Value                spans of the payload that follows: the arena
//!                                                    goes once, however many rows reference it
//! ```
//!
//! A `Ref` keeps its shape and its sharing across the wire: its payload is written whole, once,
//! whatever the spans name of it (to ship only the named rows, `clone` first). Two `Ref` columns
//! of one value that share an arena each carry a copy of it, and decode to separate arenas.

use crate::value::{Bounds, Prim, Tags, Value};

/// Round a byte count up to a whole number of 64-bit words.
#[inline]
fn pad8(n: usize) -> usize { (n + 7) & !7 }

/// The exact number of bytes [`write_to`] emits for `v` — always a multiple of 8.
pub fn length_in_bytes(v: &Value) -> usize {
    match v {
        Value::Prim(p) => 24 + pad8(prim_payload_len(p)),
        Value::Prod(cols) => 16 + cols.iter().map(length_in_bytes).sum::<usize>(),
        Value::Sum(tags, lanes) => {
            8 + tags_len(tags)                          // the Sum word, then the assignment
                + 8                                     // the lane count
                + lanes.iter().map(length_in_bytes).sum::<usize>()
        }
        Value::List(bounds, values) => 8 + bounds_len(bounds) + length_in_bytes(values),
        Value::Unit(_) => 16,
        Value::Ref(payload, spans) => 16 + 16 * spans.len() + length_in_bytes(payload),
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
        Value::Sum(tags, lanes) => {
            word(writer, 2)?;
            write_tags(tags, writer)?;
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
        Value::Ref(payload, spans) => {
            word(writer, 5)?;
            word(writer, spans.len() as u64)?;
            for &(lo, hi) in spans.iter() {
                word(writer, lo as u64)?;
                word(writer, hi as u64)?;
            }
            write_to(payload, writer)
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
/// * **Depth.** The recursion is capped at 128 levels. A short message of nested headers cannot
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
///   hash                 4880    1164
///   shape_of_value      10983    1688
///   write_to            21966    3760
///   Drop / len          32949    8780
/// ```
///
/// So the cap is not really protecting the decoder — it is protecting everything the decoder hands
/// a value to, and `hash` is the one that gives out first. 128 sits a factor of four under the
/// lowest number in that table. Raising it means redoing the measurement, on whichever traversal is
/// weakest at the time.
///
/// This is also the answer to "why not make the decode iterative and drop the cap?". An explicit
/// stack would move `read_from` off the bottom of that table, but only as far as the next row: the
/// derived `Drop` recurses, and so do `len`, `shape_of_value`, `hash` and `PartialEq`. Lifting
/// the ceiling means making all of them iterative, which is a corgi-wide change with its own
/// payoff (arbitrarily deep shapes) — not something a codec can do on its own.
pub(crate) const MAX_DEPTH: usize = 128;

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
        Value::Sum(tags, lanes) => (tags.len() as u64)
            .max(lanes.iter().map(declared_rows).max().unwrap_or(0)),
        Value::List(bounds, values) => (bounds_rows(bounds) as u64)
            .max(bounds_total(bounds))
            .max(declared_rows(values)),
        Value::Unit(n) => *n as u64,
        Value::Ref(payload, spans) => (spans.len() as u64).max(declared_rows(payload)),
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
            for &e in v.iter() { word(writer, e as u64)?; }
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
            let tags = read_tags(r)?;
            // The smallest value (a `Unit`) is two words, so that is the floor per lane.
            let n_lanes = r.count(16, "sum lanes")?;
            let mut lanes = Vec::with_capacity(n_lanes);
            for _ in 0..n_lanes {
                lanes.push(r.nested(read_value)?);
            }
            check_sum(&tags, &lanes)?;
            Ok(Value::Sum(tags, lanes))
        }
        3 => {
            let bounds = read_bounds(r)?;
            let values = r.nested(read_value)?;
            check_list(&bounds, &values)?;
            Ok(Value::List(bounds, Box::new(values)))
        }
        4 => Ok(Value::Unit(r.word()? as usize)),
        5 => {
            let n = r.count(16, "ref spans")?;
            let mut spans = Vec::with_capacity(n);
            for _ in 0..n {
                spans.push((r.word()? as usize, r.word()? as usize));
            }
            let payload = r.nested(read_value)?;
            let len = payload.len();
            if let Some(&(lo, hi)) = spans.iter().find(|&&(lo, hi)| lo > hi || hi > len) {
                return Err(format!("corgi::bytes: ref span ({lo}, {hi}) outside a payload of {len} values"));
            }
            Ok(Value::Ref(std::sync::Arc::new(payload), std::sync::Arc::new(spans)))
        }
        other => Err(format!("corgi::bytes: bad value tag {other}")),
    }
}

/// The `Sum` invariants every reader indexes by: a u8 discriminant naming one of the lanes, and a
/// carried offset that lands inside it. Without these, `hash` and the comparators index out
/// of bounds on a column the decoder handed them.
fn check_sum(tags: &Tags, lanes: &[Value]) -> Result<(), String> {
    if lanes.len() > 256 {
        return Err(format!("corgi::bytes: {} sum lanes exceeds the u8 tag width", lanes.len()));
    }
    let lane_rows: Vec<usize> = lanes.iter().map(Value::len).collect();
    for row in 0..tags.len() {
        let (t, o) = (tags.tag_at(row), tags.offset_at(row));
        match lane_rows.get(t) {
            None => return Err(format!("corgi::bytes: row {row} has tag {t} but there are {} lanes", lanes.len())),
            Some(rows) if o >= *rows => {
                return Err(format!("corgi::bytes: row {row} offset {o} is outside lane {t} ({rows} rows)"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The lane assignment, in the same two-form style as `Bounds`: `Const` is the uniform case and
/// costs three words whatever the row count, `Column` writes the discriminant leaf and the offsets.
/// The encoder records which form the sender held; normalizing on read would silently rewrite it.
fn read_tags(r: &mut Reader) -> Result<Tags, String> {
    match r.word()? {
        0 => {
            let tags = read_prim(r)?;
            if !matches!(tags, Prim::U8(_)) {
                // `Value::sum` stores the discriminant as a u8 and asserts the arity fits it; a
                // wider discriminant off the wire would be a shape corgi cannot construct.
                return Err("corgi::bytes: sum discriminant must be a u8 leaf".into());
            }
            let n_offsets = r.count(8, "sum offsets")?;
            let offsets = r.words(n_offsets)?;
            if offsets.len() != tags.len() {
                return Err(format!("corgi::bytes: {} sum offsets for {} tags", offsets.len(), tags.len()));
            }
            Ok(Tags::Column(tags, std::sync::Arc::new(offsets)))
        }
        1 => {
            let tag = r.word()? as usize;
            let rows = r.word()? as usize;
            Ok(Tags::Const(tag, rows))
        }
        other => Err(format!("corgi::bytes: bad sum tag form {other}")),
    }
}

fn write_tags<W: std::io::Write>(tags: &Tags, writer: &mut W) -> std::io::Result<()> {
    match tags {
        Tags::Column(t, offsets) => {
            word(writer, 0)?;
            write_prim(t, writer)?;
            word(writer, offsets.len() as u64)?;
            for &o in offsets.iter() { word(writer, o as u64)?; }
            Ok(())
        }
        Tags::Const(tag, rows) => {
            word(writer, 1)?;
            word(writer, *tag as u64)?;
            word(writer, *rows as u64)
        }
    }
}

/// The bytes `write_tags` emits.
fn tags_len(tags: &Tags) -> usize {
    match tags {
        // form, then the discriminant leaf (bits, len, payload), then the offsets (count, words).
        Tags::Column(t, offsets) => 8 + 16 + pad8(prim_payload_len(t)) + 8 + 8 * offsets.len(),
        Tags::Const(..) => 24, // form, tag, rows — whatever the row count
    }
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
            Ok(Bounds::offsets(r.words(n)?))
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
mod test;
