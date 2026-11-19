//! A compressed codec for a range of a column's rows, beside [`crate::bytes`]'s codec for whole columns.
//!
//! [`encode`] writes rows `rows` of a [`Value`] as a self-contained run of bytes, and [`decode`] reads one or more such runs back into one value, for everything or for a [`Demand`].
//! A host that cuts a column into pages encodes each page's rows, and decodes a run of pages at once into one set of columns.
//! [`encode_i64s`] and [`encode_u64s`] code plain integer slices the same way, for a host's columns that are not corgi values.
//! [`row_bytes`] gives each row's uncompressed bytes, by which a host can cut rows into ranges of about a size.
//!
//! **Parts.**
//! The rows are coded as parts, each a run of integers, in order of the column's shape:
//! - a leaf is one part, of its values;
//! - a product has its fields' parts;
//! - a list has a part of its rows' lengths, then its elements' parts;
//! - a sum has a part of its rows' tags, then each variant's parts, which hold the range's rows of that variant;
//! - references to list rows have the parts of a list holding the rows they reference, which travel by value;
//! - a unit has none.
//!
//! A part's integers are a leaf's elements as their order keys: bytes as they are, an `i64` as `x ^ 2^63`, which orders as the integers do, and a float as the total-order key the leaf stores, with no conversion.
//!
//! **Codecs.**
//! Each part is coded by the smallest of:
//! - raw: each value at its width, one byte for bytes and tags and eight otherwise;
//! - frame of reference: the least value, then each value less it, bit-packed at the width of the largest, which is zero bits for a constant run;
//! - dictionary: the distinct values by frame of reference, then each value's position among them, bit-packed;
//! - split, for eight-byte values: the high and the low 32 bits as two runs, each coded by the smallest of the three above.
//!
//! A part of no values takes no bytes.
//! Otherwise its first byte names its codec, so the bytes say how to read them.
//!
//! **Layout.**
//! A range records its row count, its shape with each leaf's storage, and the byte length of each part, so that a decode can skip the parts a demand does not read:
//!
//! ```text
//! range  = rows, layout, directory bytes, (part bytes)*parts, part*parts
//! layout = 0 | 1 | 2                    a leaf of bytes, of i64s, of floats
//!        | 3                            a unit
//!        | 4, fields, layout*fields     a product
//!        | 5, layout                    a list of the layout
//!        | 6, lanes, layout*lanes       a sum
//!        | 7, layout                    references to rows of a list of the layout
//! part   = (nothing)                    no values
//!        | 0, value*n                   raw
//!        | 1, least, bits, packed*n     frame of reference
//!        | 2, d, least, bits, packed*d, code bits, code*n
//!        | 3, part, part                split: the high halves, then the low halves
//! ints   = count, part                  an integer slice
//! ```
//!
//! Counts, lengths and `d` are LEB128 varints; `least` is a value's width in little-endian bytes; `bits` is one byte.
//! A part's value count is not stored: the rows, the list lengths and the sum tags before it determine it.
//!
//! The decoder reads bytes this module wrote.
//! Other bytes may panic, but cannot cause undefined behavior, as the module has no `unsafe`.

use std::ops::Range;
use std::sync::Arc;

use crate::demand::Demand;
use crate::shape::Shape;
use crate::value::{referenced, Bounds, Elem, Prim, Tags, Value};

/// Append rows `rows` of `value` to `out`, compressed.
///
/// Panics if `rows` reaches past the value's rows.
pub fn encode(value: &Value, rows: Range<usize>, out: &mut Vec<u8>) {
    assert!(rows.start <= rows.end && rows.end <= value.len(), "pack::encode: rows {rows:?} of {} rows", value.len());
    varint(rows.len(), out);
    Layout::of(value).write(out);
    let start = out.len();
    let mut writer = Writer { out, lengths: Vec::new(), words: Vec::new() };
    writer.value(value, std::slice::from_ref(&rows));
    // The directory goes before the parts, so it is written after them and rotated into place.
    let mut directory = Vec::new();
    for &len in &writer.lengths { varint(len, &mut directory); }
    let mut head = Vec::with_capacity(directory.len() + 2);
    varint(directory.len(), &mut head);
    head.extend_from_slice(&directory);
    out.extend_from_slice(&head);
    out[start..].rotate_right(head.len());
}

/// The rows of the encoded ranges `ranges`, in order, as one value projected by `demand`, as [`Demand::project`] would project their rows.
///
/// Each part is decoded straight into the result's columns, and the parts `demand` does not read are skipped.
/// Leaves keep their storage, except that an integer leaf held as bytes in one range and as `i64`s in another decodes as `i64`s.
/// References decode as references to a list holding the rows they referenced, in order.
/// It is an error if `demand` does not fit the ranges' shape, if the ranges differ in shape, or if there are no ranges.
pub fn decode(ranges: &[&[u8]], demand: &Demand) -> Result<Value, String> {
    let headers: Vec<(usize, Layout, Parts)> = ranges.iter().map(|bytes| header(bytes)).collect();
    let Some((_, first, _)) = headers.first() else { return Err("pack::decode: no ranges".into()) };
    if let Some((_, other, _)) = headers.iter().find(|(_, layout, _)| !layout.same_shape(first)) {
        return Err(format!("pack::decode: ranges of shapes {} and {}", first.shape(), other.shape()));
    }
    let mut sink = Sink::new(first, demand)?;
    sink.reserve(headers.iter().map(|(rows, _, _)| rows).sum());
    for (rows, layout, mut parts) in headers {
        sink.read(&layout, &mut parts, rows);
    }
    Ok(sink.finish())
}

/// Append `xs` to `out`, compressed, as signed integers: coded as a leaf of `i64`s is, ordered as `x ^ 2^63`.
pub fn encode_i64s(xs: &[i64], out: &mut Vec<u8>) { encode_slice(xs, out) }

/// Append `xs` to `out`, compressed, as unsigned integers.
pub fn encode_u64s(xs: &[u64], out: &mut Vec<u8>) { encode_slice(xs, out) }

/// Append the integers encoded by [`encode_i64s`] at the start of `bytes` to `out`.
pub fn decode_i64s(bytes: &[u8], out: &mut Vec<i64>) { decode_slice(bytes, out) }

/// Append the integers encoded by [`encode_u64s`] at the start of `bytes` to `out`.
pub fn decode_u64s(bytes: &[u8], out: &mut Vec<u64>) { decode_slice(bytes, out) }

/// The bytes each row of `value` takes uncompressed, for cutting rows into ranges of about a size.
///
/// A row takes its leaves' storage widths (one byte for bytes, eight otherwise), eight bytes per list row with offsets, and nine per sum row with a column of tags; a list of rows of one width and a sum of one variant take nothing for their bounds or tags.
/// A reference takes eight bytes and the row it references, which a range carries by value.
pub fn row_bytes(value: &Value) -> Vec<u64> {
    match value {
        Value::Prim(p) => vec![p.bits() as u64 / 8; p.len()],
        Value::Unit(rows) => vec![0; *rows],
        Value::Prod(fields) => {
            let mut fields = fields.iter().map(row_bytes);
            let mut total = fields.next().unwrap_or_default();
            for field in fields { for (t, f) in total.iter_mut().zip(field) { *t += f; } }
            total
        }
        Value::List(bounds, elements) => list_row_bytes(bounds, elements),
        Value::Sum(tags, lanes) => {
            let own = if tags.const_tag().is_some() { 0 } else { 9 };
            let lanes: Vec<Vec<u64>> = lanes.iter().map(row_bytes).collect();
            (0..tags.len()).map(|i| own + lanes[tags.tag_at(i)][tags.offset_at(i)]).collect()
        }
        Value::Ref(arena, rows) => {
            let (bounds, elements) = referenced(arena);
            let arena = list_row_bytes(bounds, elements);
            rows.iter().map(|&row| 8 + arena[row]).collect()
        }
    }
}

fn list_row_bytes(bounds: &Bounds, elements: &Value) -> Vec<u64> {
    let own = if bounds.strided().is_some() { 0 } else { 8 };
    let mut before = vec![0];
    for b in row_bytes(elements) { before.push(before.last().unwrap() + b); }
    (0..bounds.len()).map(|i| {
        let row = span(bounds, i);
        own + before[row.end] - before[row.start]
    }).collect()
}

fn encode_slice<T: Elem>(xs: &[T], out: &mut Vec<u8>) {
    varint(xs.len(), out);
    let words: Vec<u64> = xs.iter().map(|x| x.key()).collect();
    encode_run(&words, 8, out);
}

fn decode_slice<T: Elem>(bytes: &[u8], out: &mut Vec<T>) {
    let (n, at) = read_varint(bytes, 0);
    decode_run(&bytes[at..], n, 8, out, T::from_key);
}

// ---------------------------------------------------------------------------------------------
// Shapes as parts.

/// How a leaf holds its values: corgi's three storages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Storage {
    U8,
    I64,
    F64,
}

/// A column's shape with each leaf's storage, as a range records it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Layout {
    Leaf(Storage),
    Unit,
    Prod(Vec<Layout>),
    List(Box<Layout>),
    Sum(Vec<Layout>),
    /// References to rows of a list, by the layout of the list's elements.
    Ref(Box<Layout>),
}

const BYTES: u8 = 0;
const I64S: u8 = 1;
const FLOATS: u8 = 2;
const UNIT: u8 = 3;
const PROD: u8 = 4;
const LIST: u8 = 5;
const SUM: u8 = 6;
const REF: u8 = 7;

impl Layout {
    fn of(value: &Value) -> Layout {
        match value {
            Value::Prim(Prim::U8(_)) => Layout::Leaf(Storage::U8),
            Value::Prim(Prim::I64(_)) => Layout::Leaf(Storage::I64),
            Value::Prim(Prim::F64(_)) => Layout::Leaf(Storage::F64),
            Value::Unit(_) => Layout::Unit,
            Value::Prod(fields) => Layout::Prod(fields.iter().map(Layout::of).collect()),
            Value::List(_, elements) => Layout::List(Box::new(Layout::of(elements))),
            Value::Sum(_, lanes) => Layout::Sum(lanes.iter().map(Layout::of).collect()),
            Value::Ref(arena, _) => Layout::Ref(Box::new(Layout::of(referenced(arena).1))),
        }
    }

    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Layout::Leaf(Storage::U8) => out.push(BYTES),
            Layout::Leaf(Storage::I64) => out.push(I64S),
            Layout::Leaf(Storage::F64) => out.push(FLOATS),
            Layout::Unit => out.push(UNIT),
            Layout::Prod(fields) => {
                out.push(PROD);
                varint(fields.len(), out);
                for field in fields { field.write(out); }
            }
            Layout::List(elements) => {
                out.push(LIST);
                elements.write(out);
            }
            Layout::Sum(lanes) => {
                out.push(SUM);
                varint(lanes.len(), out);
                for lane in lanes { lane.write(out); }
            }
            Layout::Ref(elements) => {
                out.push(REF);
                elements.write(out);
            }
        }
    }

    /// The layout at `bytes[*at..]`, moving `*at` past it.
    fn read(bytes: &[u8], at: &mut usize) -> Layout {
        let tag = bytes[*at];
        *at += 1;
        let many = |at: &mut usize| {
            let (n, end) = read_varint(bytes, *at);
            *at = end;
            (0..n).map(|_| Layout::read(bytes, at)).collect()
        };
        match tag {
            BYTES => Layout::Leaf(Storage::U8),
            I64S => Layout::Leaf(Storage::I64),
            FLOATS => Layout::Leaf(Storage::F64),
            UNIT => Layout::Unit,
            PROD => Layout::Prod(many(at)),
            LIST => Layout::List(Box::new(Layout::read(bytes, at))),
            SUM => Layout::Sum(many(at)),
            REF => Layout::Ref(Box::new(Layout::read(bytes, at))),
            tag => panic!("pack: unknown layout {tag}"),
        }
    }

    /// The number of parts a range holds for this layout.
    fn parts(&self) -> usize {
        match self {
            Layout::Leaf(_) => 1,
            Layout::Unit => 0,
            Layout::Prod(fields) => fields.iter().map(Layout::parts).sum(),
            Layout::List(elements) | Layout::Ref(elements) => 1 + elements.parts(),
            Layout::Sum(lanes) => 1 + lanes.iter().map(Layout::parts).sum::<usize>(),
        }
    }

    /// The corgi shape: storages forgotten, as an integer is the same value whatever holds it.
    fn shape(&self) -> Shape {
        match self {
            Layout::Leaf(Storage::F64) => Shape::Float,
            Layout::Leaf(_) => Shape::Int,
            Layout::Unit => Shape::Unit,
            Layout::Prod(fields) => Shape::Prod(fields.iter().map(Layout::shape).collect()),
            Layout::List(elements) => Shape::List(Box::new(elements.shape())),
            Layout::Sum(lanes) => Shape::Sum(lanes.iter().map(Layout::shape).collect()),
            Layout::Ref(elements) => Shape::Ref(Box::new(Shape::List(Box::new(elements.shape())))),
        }
    }

    /// Whether two layouts have one shape, whatever their integer leaves' storages.
    fn same_shape(&self, other: &Layout) -> bool {
        let all = |a: &[Layout], b: &[Layout]| a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.same_shape(b));
        match (self, other) {
            (Layout::Leaf(a), Layout::Leaf(b)) => (*a == Storage::F64) == (*b == Storage::F64),
            (Layout::Unit, Layout::Unit) => true,
            (Layout::Prod(a), Layout::Prod(b)) | (Layout::Sum(a), Layout::Sum(b)) => all(a, b),
            (Layout::List(a), Layout::List(b)) | (Layout::Ref(a), Layout::Ref(b)) => a.same_shape(b),
            _ => false,
        }
    }
}

/// Add `range` to the end of `ranges`, joining it to the last when they meet.
fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if range.is_empty() { return; }
    match ranges.last_mut() {
        Some(last) if last.end == range.start => last.end = range.end,
        _ => ranges.push(range),
    }
}

/// The elements of row `i` of a list.
fn span(bounds: &Bounds, i: usize) -> Range<usize> {
    let (start, end) = bounds.span(i);
    start..end
}

// ---------------------------------------------------------------------------------------------
// Encoding.

/// Writes a range's parts, and remembers each one's length.
struct Writer<'a> {
    out: &'a mut Vec<u8>,
    lengths: Vec<usize>,
    /// The part being written, as words.
    words: Vec<u64>,
}

impl Writer<'_> {
    /// The parts of `value` for its rows `rows`, in order.
    fn value(&mut self, value: &Value, rows: &[Range<usize>]) {
        let all = || rows.iter().flat_map(Range::clone);
        match value {
            Value::Prim(Prim::U8(xs)) => self.leaf(xs, rows),
            Value::Prim(Prim::I64(xs)) => self.leaf(xs, rows),
            Value::Prim(Prim::F64(xs)) => self.leaf(xs, rows),
            Value::Unit(_) => {}
            Value::Prod(fields) => for field in fields { self.value(field, rows) },
            Value::List(bounds, elements) => {
                self.words.clear();
                self.words.extend(all().map(|i| span(bounds, i).len() as u64));
                self.part(8);
                let mut inner = Vec::new();
                for r in rows.iter().filter(|r| !r.is_empty()) {
                    push_range(&mut inner, span(bounds, r.start).start..span(bounds, r.end - 1).end);
                }
                self.value(elements, &inner);
            }
            Value::Sum(tags, lanes) => {
                self.words.clear();
                self.words.extend(all().map(|i| tags.tag_at(i) as u64));
                self.part(1);
                let mut inner = vec![Vec::new(); lanes.len()];
                match tags.const_tag() {
                    // One tag throughout: a row's offset in its lane is its row number.
                    Some(t) => inner[t] = rows.to_vec(),
                    None => for i in all() {
                        let at = tags.offset_at(i);
                        push_range(&mut inner[tags.tag_at(i)], at..at + 1);
                    },
                }
                for (lane, ranges) in lanes.iter().zip(&inner) { self.value(lane, ranges); }
            }
            Value::Ref(arena, refs) => {
                let (bounds, elements) = referenced(arena);
                self.words.clear();
                self.words.extend(all().map(|i| span(bounds, refs[i]).len() as u64));
                self.part(8);
                let mut inner = Vec::new();
                for i in all() { push_range(&mut inner, span(bounds, refs[i])); }
                self.value(elements, &inner);
            }
        }
    }

    /// A leaf's elements `rows`, as their order keys.
    fn leaf<T: Elem>(&mut self, xs: &[T], rows: &[Range<usize>]) {
        self.words.clear();
        for r in rows { self.words.extend(xs[r.clone()].iter().map(|x| x.key())); }
        self.part(T::BITS as usize / 8);
    }

    /// The words held, as a part of values `width` bytes wide.
    fn part(&mut self, width: usize) {
        let start = self.out.len();
        encode_run(&self.words, width, self.out);
        self.lengths.push(self.out.len() - start);
    }
}

// ---------------------------------------------------------------------------------------------
// Decoding.

/// A range's row count, layout, and parts.
fn header(bytes: &[u8]) -> (usize, Layout, Parts<'_>) {
    let (rows, mut at) = read_varint(bytes, 0);
    let layout = Layout::read(bytes, &mut at);
    let (len, at) = read_varint(bytes, at);
    let parts = Parts { directory: &bytes[at..at + len], data: &bytes[at + len..] };
    (rows, layout, parts)
}

/// A range's parts not yet read: their lengths, and their bytes.
struct Parts<'a> {
    directory: &'a [u8],
    data: &'a [u8],
}

impl<'a> Parts<'a> {
    /// The next part's bytes.
    fn next(&mut self) -> &'a [u8] {
        let (len, at) = read_varint(self.directory, 0);
        self.directory = &self.directory[at..];
        let (part, rest) = self.data.split_at(len);
        self.data = rest;
        part
    }

    /// Move past the next `parts` parts, unread.
    fn skip(&mut self, parts: usize) {
        for _ in 0..parts { self.next(); }
    }
}

/// A column being decoded from ranges, built from its layout and a demand on it.
enum Sink {
    /// A column the demand reads none of: only its row count.
    Rows(usize),
    U8(Vec<u8>),
    I64(Vec<i64>),
    /// Float total-order keys.
    F64(Vec<u64>),
    /// Each field's column, `None` where not demanded; the row count; and whether the demand compacts the product, as `Demand::Prod` does and `Demand::All` does not.
    Prod(Vec<Option<Sink>>, usize, bool),
    List(Ends, Box<Sink>),
    Sum(Discriminants, Vec<Sink>),
    /// The referenced rows' ends and elements.
    Ref(Ends, Box<Sink>),
}

impl Sink {
    /// The sink for `layout` under `demand`, or the error [`Demand::project`] would report.
    fn new(layout: &Layout, demand: &Demand) -> Result<Sink, String> {
        let all = |layouts: &[Layout]| layouts.iter().map(|l| Sink::new(l, &Demand::All)).collect::<Result<Vec<_>, _>>();
        Ok(match (layout, demand) {
            (_, Demand::None) | (Layout::Unit, Demand::All) => Sink::Rows(0),
            (Layout::Leaf(Storage::U8), Demand::All) => Sink::U8(Vec::new()),
            (Layout::Leaf(Storage::I64), Demand::All) => Sink::I64(Vec::new()),
            (Layout::Leaf(Storage::F64), Demand::All) => Sink::F64(Vec::new()),
            (Layout::Prod(fields), Demand::All) => Sink::Prod(all(fields)?.into_iter().map(Some).collect(), 0, false),
            (Layout::Prod(fields), Demand::Prod(needs)) if fields.len() == needs.len() => {
                let fields = fields.iter().zip(needs).map(|(f, need)| match need {
                    Demand::None => Ok(None),
                    need => Sink::new(f, need).map(Some),
                });
                Sink::Prod(fields.collect::<Result<_, _>>()?, 0, true)
            }
            (Layout::List(elements), Demand::All) => Sink::List(Ends::new(), Box::new(Sink::new(elements, demand)?)),
            (Layout::List(elements), Demand::List(need)) => Sink::List(Ends::new(), Box::new(Sink::new(elements, need)?)),
            (Layout::Sum(lanes), Demand::All) => Sink::Sum(Discriminants::new(), all(lanes)?),
            (Layout::Sum(lanes), Demand::Sum(needs)) if lanes.len() == needs.len() => {
                let lanes = lanes.iter().zip(needs).map(|(l, need)| Sink::new(l, need));
                Sink::Sum(Discriminants::new(), lanes.collect::<Result<_, _>>()?)
            }
            (Layout::Ref(elements), Demand::All) => Sink::Ref(Ends::new(), Box::new(Sink::new(elements, demand)?)),
            (Layout::Ref(elements), Demand::Ref(need)) => match &**need {
                Demand::All => Sink::Ref(Ends::new(), Box::new(Sink::new(elements, need)?)),
                Demand::List(need) => Sink::Ref(Ends::new(), Box::new(Sink::new(elements, need)?)),
                _ => return Err("reference request must retain list bounds".into()),
            },
            _ => return Err(format!("demand {demand:?} does not match shape {}", layout.shape())),
        })
    }

    /// Room for `rows` more rows in each leaf the sink reaches through products, so that a run of ranges fills each once, without regrowing it.
    fn reserve(&mut self, rows: usize) {
        match self {
            Sink::U8(xs) => xs.reserve(rows),
            Sink::I64(xs) => xs.reserve(rows),
            Sink::F64(keys) => keys.reserve(rows),
            Sink::Prod(fields, ..) => for field in fields.iter_mut().flatten() { field.reserve(rows) },
            _ => {}
        }
    }

    /// Append `n` rows from `parts`, whose next parts are those of `layout`, and move `parts` past them.
    fn read(&mut self, layout: &Layout, parts: &mut Parts, n: usize) {
        // Integers held as bytes here and as `i64`s in this range meet at `i64`s.
        if let (Sink::U8(xs), Layout::Leaf(Storage::I64)) = (&*self, layout) {
            *self = Sink::I64(xs.iter().map(|&x| x as i64).collect());
        }
        match (self, layout) {
            (Sink::Rows(rows), _) => {
                *rows += n;
                parts.skip(layout.parts());
            }
            (Sink::U8(xs), _) => { decode_run(parts.next(), n, 1, xs, u8::from_key); }
            (Sink::I64(xs), Layout::Leaf(Storage::U8)) => { decode_run(parts.next(), n, 1, xs, |k| k as i64); }
            (Sink::I64(xs), _) => { decode_run(parts.next(), n, 8, xs, i64::from_key); }
            (Sink::F64(keys), _) => { decode_run(parts.next(), n, 8, keys, u64::from_key); }
            (Sink::Prod(sinks, rows, _), Layout::Prod(fields)) => {
                *rows += n;
                for (sink, field) in sinks.iter_mut().zip(fields) {
                    match sink {
                        Some(sink) => sink.read(field, parts, n),
                        None => parts.skip(field.parts()),
                    }
                }
            }
            (Sink::List(ends, elements), Layout::List(inner)) | (Sink::Ref(ends, elements), Layout::Ref(inner)) => {
                let added = ends.read(parts.next(), n);
                elements.read(inner, parts, added);
            }
            (Sink::Sum(tags, sinks), Layout::Sum(lanes)) => {
                let counts = tags.read(parts.next(), n, lanes.len());
                for ((sink, lane), count) in sinks.iter_mut().zip(lanes).zip(counts) { sink.read(lane, parts, count); }
            }
            _ => unreachable!("a sink is built from a layout of the same shape"),
        }
    }

    fn finish(self) -> Value {
        match self {
            Sink::Rows(rows) => Value::Unit(rows),
            Sink::U8(xs) => Value::Prim(Prim::U8(Arc::new(xs))),
            Sink::I64(xs) => Value::Prim(Prim::I64(Arc::new(xs))),
            Sink::F64(keys) => Value::Prim(Prim::F64(Arc::new(keys))),
            Sink::Prod(sinks, rows, compact) => {
                let fields: Vec<Value> = sinks.into_iter().flatten().map(Sink::finish).collect();
                if compact && fields.is_empty() { Value::Unit(rows) } else { Value::Prod(fields) }
            }
            Sink::List(ends, elements) => Value::List(ends.finish(), Box::new(elements.finish())),
            Sink::Sum(tags, lanes) => {
                let lanes: Vec<Value> = lanes.into_iter().map(Sink::finish).collect();
                Value::Sum(tags.finish(lanes.len()), lanes)
            }
            Sink::Ref(ends, elements) => {
                let arena = Value::List(ends.finish(), Box::new(elements.finish()));
                let rows = arena.len();
                Value::Ref(Arc::new(arena), Arc::new((0..rows).collect()))
            }
        }
    }
}

/// A list's row ends being decoded: rows of one length while every range's lengths are constant, as a `Bounds::Stride`, and the ends otherwise.
enum Ends {
    Stride(usize, usize),
    Offsets(Vec<usize>),
}

impl Ends {
    fn new() -> Ends { Ends::Stride(0, 0) }

    /// Append `n` rows whose lengths are the part `part`, and return the number of elements they hold.
    fn read(&mut self, part: &[u8], n: usize) -> usize {
        if n == 0 { return 0; }
        if let (Ends::Stride(k, rows), Some(len)) = (&mut *self, constant(part, 8)) {
            if *rows == 0 || *k == len as usize {
                *k = len as usize;
                *rows += n;
                return n * *k;
            }
        }
        if let Ends::Stride(k, rows) = *self {
            *self = Ends::Offsets((1..=rows).map(|i| i * k).collect());
        }
        let Ends::Offsets(ends) = self else { unreachable!() };
        let before = ends.last().copied().unwrap_or(0);
        let mut total = before;
        decode_run(part, n, 8, ends, |len| { total += len as usize; total });
        total - before
    }

    /// The bounds, as `Bounds::from` would make them.
    fn finish(self) -> Bounds {
        match self {
            Ends::Stride(k, rows) if rows > 0 => Bounds::Stride(k, rows),
            Ends::Stride(..) => Bounds::from(Vec::new()),
            Ends::Offsets(ends) => Bounds::from(ends),
        }
    }
}

/// A sum's tags being decoded: one tag while every range's tags are that one, as a `Tags::Const`, and the tags otherwise.
enum Discriminants {
    Const(usize, usize),
    Column(Vec<u8>),
}

impl Discriminants {
    fn new() -> Discriminants { Discriminants::Const(0, 0) }

    /// Append `n` rows whose tags are the part `part`, and return the number of rows of each of `lanes` variants.
    fn read(&mut self, part: &[u8], n: usize, lanes: usize) -> Vec<usize> {
        let mut counts = vec![0; lanes];
        if n == 0 { return counts; }
        if let (Discriminants::Const(t, rows), Some(tag)) = (&mut *self, constant(part, 1)) {
            if *rows == 0 || *t == tag as usize {
                *t = tag as usize;
                *rows += n;
                counts[*t] = n;
                return counts;
            }
        }
        if let Discriminants::Const(t, rows) = *self {
            *self = Discriminants::Column(vec![t as u8; rows]);
        }
        let Discriminants::Column(tags) = self else { unreachable!() };
        let from = tags.len();
        decode_run(part, n, 1, tags, |t| t as u8);
        for &t in &tags[from..] { counts[t as usize] += 1; }
        counts
    }

    /// The lane assignment, as `Tags::column` would make it, with each row's offset within its lane.
    fn finish(self, lanes: usize) -> Tags {
        match self {
            Discriminants::Const(t, rows) => Tags::Const(t, rows),
            Discriminants::Column(tags) => {
                let mut cursor = vec![0; lanes];
                let offsets = tags.iter().map(|&t| { let at = cursor[t as usize]; cursor[t as usize] += 1; at }).collect();
                Tags::column(Prim::U8(Arc::new(tags)), offsets)
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Codecs on one run of integers.

const RAW: u8 = 0;
const FOR: u8 = 1;
const DICT: u8 = 2;
const SPLIT: u8 = 3;

/// The bits that hold `x`.
fn bits(x: u64) -> u32 { 64 - x.leading_zeros() }

/// The bytes of `n` values packed at `bits` bits.
fn packed(n: usize, bits: u32) -> usize { (n * bits as usize).div_ceil(8) }

/// A word from up to eight little-endian bytes.
fn word(bytes: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(b)
}

/// The value of every element of a part of values `width` bytes wide, when its coding says it is constant.
fn constant(part: &[u8], width: usize) -> Option<u64> {
    (part.first() == Some(&FOR) && part[1 + width] == 0).then(|| word(&part[1..1 + width]))
}

/// The coding chosen for a run.
enum Plan {
    Raw,
    /// The least value and the bits of the largest difference from it.
    For(u64, u32),
    /// The bits of the entries' frame of reference, the bits of a position, and the distinct values in order.
    Dict(u32, u32, Vec<u64>),
    /// The codings of the high and low halves, and the halves.
    Split(Box<Plan>, Box<Plan>, Vec<u64>, Vec<u64>),
}

impl Plan {
    /// The smallest coding of `words`, values `width` bytes wide; `split` allows splitting eight-byte values.
    fn new(words: &[u64], width: usize, split: bool) -> Plan {
        let n = words.len();
        let (least, most) = words.iter().fold((u64::MAX, 0), |(l, m), &w| (l.min(w), m.max(w)));
        let spread = bits(most - least);
        let mut best = Plan::For(least, spread);
        if Plan::Raw.size(n, width) <= best.size(n, width) { best = Plan::Raw; }
        if spread > 0 {
            let entries = distinct(words);
            if entries.len() < n {
                let dict = Plan::Dict(bits(entries[entries.len() - 1] - entries[0]), bits(entries.len() as u64 - 1), entries);
                if dict.size(n, width) < best.size(n, width) { best = dict; }
            }
            if split && width == 8 {
                let hi: Vec<u64> = words.iter().map(|w| w >> 32).collect();
                let lo: Vec<u64> = words.iter().map(|w| w & 0xFFFF_FFFF).collect();
                let halves = Plan::Split(Box::new(Plan::new(&hi, 4, false)), Box::new(Plan::new(&lo, 4, false)), hi, lo);
                if halves.size(n, width) < best.size(n, width) { best = halves; }
            }
        }
        best
    }

    /// The bytes of `n` values coded so.
    fn size(&self, n: usize, width: usize) -> usize {
        match self {
            Plan::Raw => 1 + n * width,
            Plan::For(_, b) => 2 + width + packed(n, *b),
            Plan::Dict(b, code, entries) => 3 + varint_len(entries.len()) + width + packed(entries.len(), *b) + packed(n, *code),
            Plan::Split(hi, lo, ..) => 1 + hi.size(n, 4) + lo.size(n, 4),
        }
    }

    fn write(&self, words: &[u64], width: usize, out: &mut Vec<u8>) {
        match self {
            Plan::Raw => {
                out.push(RAW);
                for &w in words { out.extend_from_slice(&w.to_le_bytes()[..width]); }
            }
            Plan::For(least, b) => {
                out.push(FOR);
                out.extend_from_slice(&least.to_le_bytes()[..width]);
                out.push(*b as u8);
                pack(words.iter().map(|&w| w - least), *b, out);
            }
            Plan::Dict(b, code, entries) => {
                out.push(DICT);
                varint(entries.len(), out);
                out.extend_from_slice(&entries[0].to_le_bytes()[..width]);
                out.push(*b as u8);
                pack(entries.iter().map(|&e| e - entries[0]), *b, out);
                out.push(*code as u8);
                pack(words.iter().map(|w| entries.binary_search(w).unwrap() as u64), *code, out);
            }
            Plan::Split(hi, lo, his, los) => {
                out.push(SPLIT);
                hi.write(his, 4, out);
                lo.write(los, 4, out);
            }
        }
    }
}

/// The distinct values of `words`, in order.
fn distinct(words: &[u64]) -> Vec<u64> {
    let mut entries = words.to_vec();
    entries.sort_unstable();
    entries.dedup();
    entries
}

/// Append the smallest coding of `words`, values `width` bytes wide (one, four or eight), to `out`; no values take no bytes.
fn encode_run(words: &[u64], width: usize, out: &mut Vec<u8>) {
    if words.is_empty() { return; }
    Plan::new(words, width, true).write(words, width, out);
}

/// Decode `n` values coded by [`encode_run`] at the start of `bytes`, appending each through `from` to `out`, and return the bytes read.
fn decode_run<W>(bytes: &[u8], n: usize, width: usize, out: &mut Vec<W>, mut from: impl FnMut(u64) -> W) -> usize {
    if n == 0 { return 0; }
    if bytes[0] != SPLIT { return decode_whole(bytes, n, width, out, from); }
    // The halves are coded whole, and each is decoded on its own and then joined in one pass, which measured two to three times faster than joining the low half to the high as it is decoded.
    let (mut hi, mut lo) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let at = 1 + decode_whole(&bytes[1..], n, 4, &mut hi, |h| h << 32);
    let end = at + decode_whole(&bytes[at..], n, 4, &mut lo, |l| l);
    out.extend(hi.iter().zip(&lo).map(|(h, l)| from(h | l)));
    end
}

/// [`decode_run`] for a run coded raw, by frame of reference, or by dictionary.
fn decode_whole<W>(bytes: &[u8], n: usize, width: usize, out: &mut Vec<W>, mut from: impl FnMut(u64) -> W) -> usize {
    match bytes[0] {
        RAW => {
            let end = 1 + n * width;
            let raw = &bytes[1..end];
            match width {
                1 => out.extend(raw.iter().map(|&b| from(b as u64))),
                8 => out.extend(raw.chunks_exact(8).map(|c| from(u64::from_le_bytes(c.try_into().unwrap())))),
                _ => out.extend(raw.chunks_exact(width).map(|c| from(word(c)))),
            }
            end
        }
        FOR => {
            let least = word(&bytes[1..1 + width]);
            let b = bytes[1 + width] as u32;
            let (start, end) = (2 + width, 2 + width + packed(n, b));
            unpack(&bytes[start..end], n, b, out, |r| from(least.wrapping_add(r)));
            end
        }
        DICT => {
            let (d, mut at) = read_varint(bytes, 1);
            let least = word(&bytes[at..at + width]);
            let b = bytes[at + width] as u32;
            at += width + 1;
            let mut entries = Vec::with_capacity(d);
            unpack(&bytes[at..at + packed(d, b)], d, b, &mut entries, |r| least.wrapping_add(r));
            at += packed(d, b);
            let code = bytes[at] as u32;
            at += 1;
            unpack(&bytes[at..at + packed(n, code)], n, code, out, |c| from(entries[c as usize]));
            at + packed(n, code)
        }
        tag => panic!("pack: unknown codec {tag}"),
    }
}

/// Append `values`, each less than `2^bits`, packed at `bits` bits from the lowest bit of the first byte up.
fn pack(values: impl Iterator<Item = u64>, bits: u32, out: &mut Vec<u8>) {
    if bits == 0 { return; }
    let (mut acc, mut held) = (0u128, 0u32);
    for v in values {
        acc |= (v as u128) << held;
        held += bits;
        if held >= 64 {
            out.extend_from_slice(&(acc as u64).to_le_bytes());
            acc >>= 64;
            held -= 64;
        }
    }
    out.extend_from_slice(&acc.to_le_bytes()[..held.div_ceil(8) as usize]);
}

/// Append `n` values packed by [`pack`] at `bits` bits, each through `f`.
fn unpack<W>(bytes: &[u8], n: usize, bits: u32, out: &mut Vec<W>, mut f: impl FnMut(u64) -> W) {
    if bits == 0 {
        out.extend((0..n).map(|_| f(0)));
        return;
    }
    let b = bits as usize;
    let mask = u64::MAX >> (64 - bits);
    // A value is in the sixteen bytes from its first; it reads from `bytes` in place while they are there.
    let at = |packed: &[u8], bit: usize| {
        let window = u128::from_le_bytes(packed[bit / 8..bit / 8 + 16].try_into().unwrap());
        (window >> (bit % 8)) as u64 & mask
    };
    let in_place = if bytes.len() < 16 { 0 } else { ((8 * (bytes.len() - 16) + 7) / b + 1).min(n) };
    out.extend((0..in_place).map(|i| f(at(bytes, i * b))));
    if in_place < n {
        let skip = in_place * b / 8;
        let mut tail = bytes[skip..].to_vec();
        tail.resize(tail.len() + 16, 0);
        out.extend((in_place..n).map(|i| f(at(&tail, i * b - 8 * skip))));
    }
}

fn varint(mut x: usize, out: &mut Vec<u8>) {
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

fn varint_len(x: usize) -> usize { (bits(x as u64).max(1) as usize).div_ceil(7) }

/// The varint at `bytes[at..]`, and where it ends.
fn read_varint(bytes: &[u8], mut at: usize) -> (usize, usize) {
    let (mut x, mut shift) = (0, 0);
    loop {
        let byte = bytes[at];
        at += 1;
        x |= ((byte & 0x7F) as usize) << shift;
        if byte < 0x80 { return (x, at); }
        shift += 7;
    }
}

#[cfg(test)]
mod test;
