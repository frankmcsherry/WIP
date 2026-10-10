//! The columnar value model: a `Value` is a whole column (a SEQ). Every operator
//! is a single `T0 -> T1` on one element, lifted 1:1 across the column; all
//! cardinality change lives *inside* a `List`.

use crate::shape::{shape_of_value, Shape};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Prim(Prim),                   // a leaf column at one byte width
    Prod(Vec<Value>),             // parallel columns, equal length
    Sum(Tags, Vec<Value>),        // per-row discriminant + within-variant offset (see `Tags`) + one
                                  // packed lane per variant (a variant no row carries is an empty
                                  // column of its shape — every lane is concrete).
    List(Bounds, Box<Value>),     // row partition (see `Bounds`) + flattened values
    Unit(usize),                  // a length-carrying unit column: `n` rows, no payload. The terminal
                                  // object as a COLUMN (a fieldless `Prod` has no length witness); the
                                  // `None` of `Option = Sum{Unit | T}`, and JSON `null`.
    Ref(Arc<Value>, Arc<Vec<usize>>),
                                  // a column of REFERENCED LIST ROWS (`&[T]`): row `j` is row
                                  // `rows[j]` of the shared list (always a `List`, the arena). `ref`
                                  // takes them (nothing copied), `clone` copies them out; `gather` on
                                  // a Ref moves only the row numbers. A lossy gather out of range
                                  // names an empty row of the arena (see [`with_empty_row`]). The explicit "by reference, not by value" — a
                                  // closure's `&ctx`, a `&str` into a shared text. Only a list row is
                                  // unbounded, so only a list row is ever referenced: `ref` passes
                                  // through products and sums and leaves bounded rows by value. (The
                                  // row numbers sit behind an `Arc` like a leaf: a clone is a
                                  // refcount bump.)
}

/// an arena with an empty row, and that row's number: the zero a reference names (the empty list,
/// what a lossy gather gives a position out of range). The arena's last row, when it is empty;
/// otherwise a new arena, the same values with one more row end. Only a lossy gather that misses
/// asks, the case that once panicked, so the copy of the row ends is paid only there.
pub(crate) fn with_empty_row(list: &Arc<Value>) -> (Arc<Value>, usize) {
    let (bounds, vals) = referenced(list);
    let n = bounds.len();
    if n > 0 && bounds.span(n - 1).0 == bounds.total() {
        return (list.clone(), n - 1);
    }
    let mut ends = bounds.to_vec();
    ends.push(bounds.total());
    (Arc::new(Value::List(ends.into(), Box::new(vals.clone()))), n)
}

/// a reference column's arena, as its row ends and values.
pub(crate) fn referenced(list: &Value) -> (&Bounds, &Value) {
    match list {
        Value::List(bounds, vals) => (bounds, vals),
        other => unreachable!("a Ref names rows of a List, not of {}", shape_of_value(other)),
    }
}

/// the rows of a haystack as a reader sees them: `span(i)` over one payload, whether the rows came
/// as a `List` (a partition of its payload) or as a `Ref` (rows of a shared list, by number). The
/// span-aware readers (`Gather`/`Find`/`Len`) take this via `rows_of`; every other
/// op takes `into_list`, which only accepts a `List` — a Ref is the shape error "clone first".
#[derive(Clone, Copy)]
pub(crate) enum Rows<'a> {
    Part(&'a Bounds),
    Named(&'a Bounds, &'a [usize]),
}

impl Rows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Rows::Part(b) => b.len(),
            Rows::Named(_, rows) => rows.len(),
        }
    }
    pub(crate) fn span(&self, i: usize) -> (usize, usize) {
        match self {
            Rows::Part(b) => b.span(i),
            Rows::Named(b, rows) => b.span(rows[i]),
        }
    }
}

/// how a `List`'s flattened `values` partition into rows. `Offsets` is the general end-offset-per-row
/// form (row `i` is `[ends[i-1]..ends[i])`, `ends[-1] = 0`). `Stride` is the UNIFORM case — `rows` rows
/// each exactly `stride` wide — a list carries it when its rows happen to be equal width. This is the
/// dynamic mirror of `columnar`'s `Strides`: detecting uniformity is O(1) (`strided`), so uniform data
/// recovers dense / array-language kernels for free, and the property PROPAGATES through a pipeline
/// instead of being re-derived per op. Equality/hash are by the partition, so a `Stride` and the
/// equivalent `Offsets` are interchangeable.
#[derive(Clone, Debug)]
pub enum Bounds {
    // end offset of each row. Behind an `Arc` for the same reason a leaf is: a partition is
    // write-once read-many and `eval` clones a value at every shared edge, so a clone must be a
    // refcount bump. A bare `Vec` here made cloning a `List<U64>` copy as many bytes as the data.
    Offsets(Arc<Vec<usize>>),
    Stride(usize, usize), // (stride, rows): row i spans [i*stride .. (i+1)*stride), total = stride*rows
}

impl Bounds {
    /// number of rows.
    pub(crate) fn len(&self) -> usize {
        match self {
            Bounds::Offsets(v) => v.len(),
            Bounds::Stride(_, rows) => *rows,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// end offset of row `i` (one past its last element).
    pub(crate) fn end(&self, i: usize) -> usize {
        match self {
            Bounds::Offsets(v) => v[i],
            Bounds::Stride(k, _) => (i + 1) * k,
        }
    }
    /// total flattened element count.
    pub(crate) fn total(&self) -> usize {
        match self {
            Bounds::Offsets(v) => v.last().copied().unwrap_or(0),
            Bounds::Stride(k, rows) => k * rows,
        }
    }
    /// row `i`'s `[start, end)` span.
    pub(crate) fn span(&self, i: usize) -> (usize, usize) {
        match self {
            Bounds::Offsets(v) => (if i == 0 { 0 } else { v[i - 1] }, v[i]),
            Bounds::Stride(k, _) => (i * k, (i + 1) * k),
        }
    }
    /// the uniform stride, if this partition is uniform — the O(1) detection that recovers array kernels.
    pub fn strided(&self) -> Option<usize> {
        match self {
            Bounds::Stride(k, _) => Some(*k),
            Bounds::Offsets(_) => None,
        }
    }
    /// iterate the per-row end offsets (materialized for `Stride`).
    pub(crate) fn ends(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).map(move |i| self.end(i))
    }
    /// materialize the general end-offset form — for ops not yet stride-aware, and for eq/show.
    pub fn to_vec(&self) -> Vec<usize> {
        match self {
            Bounds::Offsets(v) => (**v).clone(),
            Bounds::Stride(..) => self.ends().collect(),
        }
    }

    /// the general end-offset form, verbatim — NO uniformity check. For a caller that must preserve
    /// which representation it was handed (the codec records the form the sender held); everyone
    /// else wants `From<Vec<usize>>`, which compacts a uniform partition to a `Stride`.
    pub fn offsets(ends: Vec<usize>) -> Bounds {
        Bounds::Offsets(Arc::new(ends))
    }

    /// the uniform stride of `ends`, if the partition is uniform (`ends[i] == (i+1)*k`).
    fn uniform(ends: &[usize]) -> Option<usize> {
        let &last = ends.last()?;
        let n = ends.len();
        if last % n != 0 {
            return None;
        }
        let k = last / n;
        ends.iter().enumerate().all(|(i, &e)| e == (i + 1) * k).then_some(k)
    }

    /// recover the uniform `Stride` form if this partition happens to be uniform. `From<Vec<usize>>`
    /// is this check applied at construction; this is it applied to a partition already in hand, so
    /// a caller that must rebuild a `Bounds` does not have to unwrap and re-wrap the buffer.
    pub(crate) fn compact(self) -> Bounds {
        if let Bounds::Offsets(v) = &self {
            if let Some(k) = Bounds::uniform(v) {
                return Bounds::Stride(k, v.len());
            }
        }
        self
    }
}

impl From<Vec<usize>> for Bounds {
    fn from(v: Vec<usize>) -> Self {
        // One O(n) uniformity check at construction: a uniform partition becomes a `Stride`,
        // so `strided()` recovers the array kernels downstream at every `.into()` site for
        // free (and needs no allocation at all). Equality/hash are by the partition, so this
        // is representation-invisible.
        match Bounds::uniform(&v) {
            Some(k) => Bounds::Stride(k, v.len()),
            None => Bounds::Offsets(Arc::new(v)),
        }
    }
}

// equality/hash are by the PARTITION, so a `Stride` and the equivalent `Offsets` compare and hash equal.
impl PartialEq for Bounds {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // one buffer is one partition: the common case where two columns descend from the
            // same list, which is exactly what `Zip` asserts about its operands.
            (Bounds::Offsets(a), Bounds::Offsets(b)) => Arc::ptr_eq(a, b) || a == b,
            (Bounds::Stride(k0, n0), Bounds::Stride(k1, n1)) => k0 == k1 && n0 == n1,
            _ => self.len() == other.len() && self.ends().eq(other.ends()),
        }
    }
}
impl Eq for Bounds {}
impl std::hash::Hash for Bounds {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for e in self.ends() {
            e.hash(state);
        }
    }
}

/// how a `Sum`'s rows are assigned to its lanes: each row's discriminant, plus its offset WITHIN
/// that lane (carried, so comparison/search/hash read a row's rank instead of rescanning for it).
///
/// `Const` is the UNIFORM case — every row carries one tag, so row `i` sits at offset `i` in that
/// lane — and it is what `inject`, `lift`, and any `Fail` column that has not actually failed
/// produce. This is the `Sum`-side twin of [`Bounds::Stride`]: uniformity is O(1) to detect
/// (`const_tag`), it costs no columns at all to represent, and it PROPAGATES — a gather of one
/// stays one, a lane map leaves it alone. It mirrors `columnar`'s `Discriminant`, whose
/// "homogeneous" state stores `[tag, count]` and synthesises the identity offsets, so the dynamic
/// and static columnar layouts agree on the case. Equality and hash are by the ASSIGNMENT, so a
/// `Const` and the equivalent `Column` are interchangeable.
#[derive(Clone, Debug)]
pub enum Tags {
    Const(usize, usize), // (tag, rows): every row carries `tag`, row i at offset i in that lane
    Column(Prim, Arc<Vec<usize>>), // per-row discriminant (a u8 leaf) + per-row within-lane offset
}

impl Tags {
    /// the general assignment from a discriminant column and its within-lane offsets. Compacts to
    /// `Const` when every row carries one tag (the offsets are then forced to be the identity), the
    /// same construction-time check `Bounds::from` makes.
    pub(crate) fn column(tags: Prim, offsets: Vec<usize>) -> Tags {
        debug_assert_eq!(tags.len(), offsets.len(), "Tags: discriminant/offset length");
        match Tags::uniform(&tags) {
            Some(t) => Tags::Const(t, tags.len()),
            None => Tags::Column(tags, Arc::new(offsets)),
        }
    }

    /// the general assignment from tags alone: the within-lane offsets are each row's rank among
    /// the rows sharing its tag, computed in one cursor pass. `arity` is the lane count.
    pub(crate) fn from_tags(tags: Vec<usize>, arity: usize) -> Tags {
        assert!(arity <= 256, "Value::sum: {arity} variants exceeds the u8 tag width");
        let offsets = within_offsets(tags.iter().copied(), arity);
        // tags are stored as a u8 discriminant, so the variant count must fit a u8 — else `t as u8`
        // would silently truncate a tag onto the wrong lane.
        Tags::column(Prim::U8(Arc::new(tags.iter().map(|&t| t as u8).collect())), offsets)
    }

    /// the single tag every row carries, if there is one — the O(1) uniformity test.
    pub(crate) fn const_tag(&self) -> Option<usize> {
        match self {
            Tags::Const(t, _) => Some(*t),
            Tags::Column(..) => None,
        }
    }

    /// the one tag a discriminant column carries throughout, if any (an empty column carries none:
    /// `Const` names a lane, and no lane is named by no rows — `len` 0 compares equal either way).
    fn uniform(tags: &Prim) -> Option<usize> {
        let first = (tags.len() > 0).then(|| tags.usize_at(0))?;
        (0..tags.len()).all(|i| tags.usize_at(i) == first).then_some(first)
    }

    /// how many rows the assignment covers.
    pub fn len(&self) -> usize {
        match self {
            Tags::Const(_, rows) => *rows,
            Tags::Column(t, _) => t.len(),
        }
    }

    /// does the assignment cover no rows?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// row `i`'s discriminant.
    #[inline]
    pub fn tag_at(&self, i: usize) -> usize {
        match self {
            Tags::Const(t, _) => *t,
            Tags::Column(t, _) => t.usize_at(i),
        }
    }

    /// row `i`'s offset within its lane — read, never recomputed.
    #[inline]
    pub fn offset_at(&self, i: usize) -> usize {
        match self {
            Tags::Const(..) => i, // one lane in row order: the offset IS the row index
            Tags::Column(_, o) => o[i],
        }
    }

    /// every row's discriminant, in row order.
    pub(crate) fn tags_iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).map(move |i| self.tag_at(i))
    }
}

// equality/hash are by the ASSIGNMENT, so a `Const` and the equivalent `Column` agree (as a
// `Bounds::Stride` does with its offsets). An empty sum is empty under either representation.
impl PartialEq for Tags {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Tags::Const(t0, n0), Tags::Const(t1, n1)) => n0 == n1 && (n0 == &0 || t0 == t1),
            _ => {
                self.len() == other.len()
                    && (0..self.len()).all(|i| {
                        self.tag_at(i) == other.tag_at(i) && self.offset_at(i) == other.offset_at(i)
                    })
            }
        }
    }
}
impl Eq for Tags {}
impl std::hash::Hash for Tags {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for i in 0..self.len() {
            self.tag_at(i).hash(state);
            self.offset_at(i).hash(state);
        }
    }
}

/// a leaf's element type: how it orders, and what an integer it holds is. The integer storages are
/// `u8` (an integer from 0 to 255: bytes, masks, tags) and `i8`, `i16`, `i32`, `i64` (two's
/// complement); a float is held as its total-order key (`u64`).
pub(crate) trait Elem: Copy + Ord + Default + std::fmt::Debug {
    /// the storage width in bits.
    const BITS: u32;
    /// an unsigned key in the element's order, of the storage's width: the sort radixes these,
    /// and packs them `BITS` at a time.
    fn key(self) -> u64;
    /// the element whose key is `k`.
    fn from_key(k: u64) -> Self;
    /// the element's 64-bit word: an integer's two's complement (sign-extended), a float's key.
    /// Positions, tags and hashes read this, so a byte 5 and an `i64` 5 read alike.
    fn word(self) -> u64;
    /// the element holding the constant `s` (an integer that fits the storage, or a float).
    fn of_scalar(s: Scalar) -> Self;
}

const SIGN: u64 = 1 << 63;

/// the rows a kernel reads an operand in at a time when it must widen it: small enough that the
/// widened block stays in L1 beside the output, large enough that the per-tile dispatch vanishes.
pub(crate) const TILE: usize = 1024;

impl Elem for u8 {
    const BITS: u32 = 8;
    #[inline] fn key(self) -> u64 { self as u64 }
    #[inline] fn from_key(k: u64) -> Self { k as u8 }
    #[inline] fn word(self) -> u64 { self as u64 }
    #[inline] fn of_scalar(s: Scalar) -> Self { s.int() as u8 }
}
/// the signed storages: a key is the value with its sign bit flipped, at the storage's width.
macro_rules! signed_elem {
    ($($t:ty => $u:ty),+) => { $(
        impl Elem for $t {
            const BITS: u32 = <$t>::BITS;
            #[inline] fn key(self) -> u64 { ((self as $u) ^ (1 << (<$t>::BITS - 1))) as u64 }
            #[inline] fn from_key(k: u64) -> Self { ((k as $u) ^ (1 << (<$t>::BITS - 1))) as $t }
            #[inline] fn word(self) -> u64 { self as i64 as u64 }
            #[inline] fn of_scalar(s: Scalar) -> Self { s.int() as $t }
        }
    )+ };
}
signed_elem!(i8 => u8, i16 => u16, i32 => u32, i64 => u64);
impl Elem for u64 {
    const BITS: u32 = 64;
    #[inline] fn key(self) -> u64 { self }
    #[inline] fn from_key(k: u64) -> Self { k }
    #[inline] fn word(self) -> u64 { self }
    #[inline] fn of_scalar(s: Scalar) -> Self {
        match s {
            Scalar::Float(k) => k,
            Scalar::Int(x) => x as u64,
        }
    }
}

/// the storages an integer leaf can be held at, narrowest first. Unsigned only at a byte; each
/// signed width widens to the next, and a byte to `i16`. Two storages meet at the narrowest that
/// holds both ([`Storage::join`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Storage {
    U8,
    I8,
    I16,
    I32,
    I64,
}

impl Storage {
    /// the least and greatest values the storage holds.
    pub(crate) fn range(self) -> (i64, i64) {
        match self {
            Storage::U8 => (0, u8::MAX as i64),
            Storage::I8 => (i8::MIN as i64, i8::MAX as i64),
            Storage::I16 => (i16::MIN as i64, i16::MAX as i64),
            Storage::I32 => (i32::MIN as i64, i32::MAX as i64),
            Storage::I64 => (i64::MIN, i64::MAX),
        }
    }
    /// the narrowest storage holding every value from `lo` to `hi`.
    pub(crate) fn holding(lo: i64, hi: i64) -> Storage {
        [Storage::U8, Storage::I8, Storage::I16, Storage::I32]
            .into_iter()
            .find(|s| { let (a, b) = s.range(); a <= lo && hi <= b })
            .unwrap_or(Storage::I64)
    }
    /// the narrowest storage holding everything either does: `u8` with `i8` is `i16`.
    pub(crate) fn join(a: Storage, b: Storage) -> Storage {
        if a == b {
            return a;
        }
        let ((a0, a1), (b0, b1)) = (a.range(), b.range());
        Storage::holding(a0.min(b0), a1.max(b1))
    }
}

/// the random-storage test mode's storage for a leaf from `lo` to `hi`: a pseudo-random one of
/// those that hold it, from a sequence per thread, so a run repeats.
#[cfg(feature = "random-storage")]
pub(crate) fn random_storage(lo: i64, hi: i64) -> Storage {
    use std::cell::Cell;
    thread_local! { static STATE: Cell<u64> = const { Cell::new(0x9E37_79B9_7F4A_7C15) }; }
    let r = STATE.with(|s| {
        let x = s.get().wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        s.set(x);
        x >> 33
    });
    let holds: Vec<Storage> = [Storage::U8, Storage::I8, Storage::I16, Storage::I32, Storage::I64]
        .into_iter()
        .filter(|s| { let (a, b) = s.range(); a <= lo && hi <= b })
        .collect();
    holds[r as usize % holds.len()]
}

/// the order-preserving key of an `f64`: negatives flip every bit, the rest flip the sign bit, so
/// the unsigned order of keys is `f64::total_cmp`. A float leaf stores these.
pub(crate) fn f64_key(f: f64) -> u64 {
    let b = f.to_bits();
    if b >> 63 == 1 { !b } else { b ^ SIGN }
}
/// the `f64` whose key is `k`.
pub(crate) fn f64_of_key(k: u64) -> f64 {
    f64::from_bits(if k >> 63 == 1 { k ^ SIGN } else { !k })
}

/// the slice of `i64`s as their two's complement words: the same bytes, read as `u64`.
pub(crate) fn words_of(xs: &[i64]) -> &[u64] {
    // SAFETY: `i64` and `u64` have one size and alignment, and every bit pattern is a `u64`.
    unsafe { std::slice::from_raw_parts(xs.as_ptr() as *const u64, xs.len()) }
}

/// the `u64` words as the `i64`s whose two's complement they are, in the same buffer.
pub(crate) fn i64s_of_words(xs: Vec<u64>) -> Vec<i64> {
    let mut xs = std::mem::ManuallyDrop::new(xs);
    // SAFETY: `u64` and `i64` have one size and alignment, every bit pattern is an `i64`, and the
    // buffer's ownership moves to the new `Vec` (the old one is never dropped).
    unsafe { Vec::from_raw_parts(xs.as_mut_ptr() as *mut i64, xs.len(), xs.capacity()) }
}

/// the `i64`s as their two's complement words, in the same buffer.
pub(crate) fn words_of_i64s(xs: Vec<i64>) -> Vec<u64> {
    let mut xs = std::mem::ManuallyDrop::new(xs);
    // SAFETY: as `i64s_of_words`, the other way.
    unsafe { Vec::from_raw_parts(xs.as_mut_ptr() as *mut u64, xs.len(), xs.capacity()) }
}

/// a one-row constant an immediate op carries: an integer, or a float as its total-order key
/// (a key, not an `f64`, so that ops holding it are `Eq` and `Hash`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scalar {
    Int(i64),
    Float(u64),
}

impl Scalar {
    /// row `i` of a leaf, as a constant.
    pub(crate) fn of(p: &Prim, i: usize) -> Scalar {
        match p {
            Prim::F64(v) => Scalar::Float(v[i]),
            p => Scalar::Int(p.int_at(i)),
        }
    }
    /// the constant as an element of a leaf of its kind, at a storage that holds it.
    pub(crate) fn elem<T: Elem>(self) -> T {
        T::of_scalar(self)
    }
    /// the integer this constant is (a float's key, read as one, for a float).
    fn int(self) -> i64 {
        match self {
            Scalar::Int(x) => x,
            Scalar::Float(k) => k as i64,
        }
    }
    /// does this constant agree in kind with the leaf (an integer for an integer, a float for a float)?
    pub(crate) fn kind_of(&self, p: &Prim) -> bool {
        matches!((self, p), (Scalar::Float(_), Prim::F64(_))) || (matches!(self, Scalar::Int(_)) && p.is_int())
    }
}

/// the integers `xs` held at storage `s` (which must hold them): one loop per (source, target).
fn convert<S: Elem>(xs: &[S], s: Storage) -> Prim {
    fn collect<S: Elem, T>(xs: &[S], f: impl Fn(i64) -> T) -> Arc<Vec<T>> {
        Arc::new(xs.iter().map(|&x| f(x.word() as i64)).collect())
    }
    match s {
        Storage::U8 => Prim::U8(collect(xs, |x| x as u8)),
        Storage::I8 => Prim::I8(collect(xs, |x| x as i8)),
        Storage::I16 => Prim::I16(collect(xs, |x| x as i16)),
        Storage::I32 => Prim::I32(collect(xs, |x| x as i32)),
        Storage::I64 => Prim::I64(collect(xs, |x| x)),
    }
}

/// a leaf column: one storage per variant, each a naturally aligned `Vec` behind an `Arc` (leaves
/// are write-once read-many; `eval` clones freely for shared edges, so a leaf clone must be a
/// refcount bump, not a buffer copy). `U8`, `I8`, `I16`, `I32` and `I64` all hold integers, the same
/// values at several widths: a kernel reading two integer leaves at different storages brings them
/// to the narrowest that holds both first ([`Prim::meet`]). `F64` holds floats, as their
/// total-order keys. The `prim!` macro lists the
/// storages ONCE and generates the enum and every per-storage method.
macro_rules! prim {
    ($($V:ident => $t:ty),+ $(,)?) => {
        #[derive(Clone, Debug)]
        pub enum Prim {
            $( $V(Arc<Vec<$t>>), )+
        }

        impl Prim {
            pub(crate) fn len(&self) -> usize {
                match self { $( Prim::$V(v) => v.len(), )+ }
            }

            /// the storage width in bits.
            pub(crate) fn bits(&self) -> u32 {
                match self { $( Prim::$V(_) => <$t as Elem>::BITS, )+ }
            }

            /// row `i` as a `usize`, read in place — how a small-int column (a `Sum`'s
            /// discriminant) is read. There is deliberately no whole-column `Vec<usize>` decode:
            /// producing one to look at some of a column made a scalar `compare_at` O(column).
            #[inline]
            pub(crate) fn usize_at(&self, i: usize) -> usize {
                match self { $( Prim::$V(v) => v[i].word() as usize, )+ }
            }

            /// row `i`'s word: an integer's two's complement, a float's key. A position or a tag
            /// is read this way, so a negative one is past every row.
            #[inline]
            pub(crate) fn word_at(&self, i: usize) -> u64 {
                match self { $( Prim::$V(v) => v[i].word(), )+ }
            }

            /// every element as an `i64` (an integer leaf's values), one loop per storage.
            pub(crate) fn ints(&self) -> Vec<i64> {
                match self { $( Prim::$V(v) => v.iter().map(|&x| x.word() as i64).collect(), )+ }
            }

            /// every element's word, one loop per storage.
            pub(crate) fn words(&self) -> Vec<u64> {
                match self { $( Prim::$V(v) => v.iter().map(|&x| x.word()).collect(), )+ }
            }

            /// every element as a mask byte: 1 where it is nonzero.
            pub(crate) fn mask(&self) -> Vec<u8> {
                match self { $( Prim::$V(v) => v.iter().map(|&x| (x.word() != 0) as u8).collect(), )+ }
            }

            /// an integer leaf's least and greatest values (`None` for no rows, or a float leaf).
            pub(crate) fn int_range(&self) -> Option<(i64, i64)> {
                if !self.is_int() {
                    return None;
                }
                // at the storage's own width, where the loops vectorize widest
                match self {
                    $( Prim::$V(v) => {
                        let (lo, hi) = (v.iter().copied().min()?, v.iter().copied().max()?);
                        Some((lo.word() as i64, hi.word() as i64))
                    } )+
                }
            }

            /// the same integers held at storage `s`, which must hold them all: the leaf itself
            /// when it is held there already, otherwise one pass.
            pub(crate) fn to_storage(&self, s: Storage) -> Prim {
                if self.storage() == Some(s) {
                    return self.clone();
                }
                debug_assert!(self.int_range().is_none_or(|(lo, hi)| { let (a, b) = s.range(); a <= lo && hi <= b }), "to_storage: {s:?} can't hold the values");
                match self { $( Prim::$V(v) => convert(v, s), )+ }
            }

            /// row `i`'s order key (see [`Elem::key`]).
            #[inline]
            pub(crate) fn key_at(&self, i: usize) -> u64 {
                match self { $( Prim::$V(v) => v[i].key(), )+ }
            }

            /// `n` copies of row `i` — the leaf case of a constant column ([`crate::engine::fill`]).
            /// One fill, no index column: the broadcast a `gather` at a constant index amounts to.
            pub(crate) fn repeat(&self, i: usize, n: usize) -> Prim {
                match self {
                    $( Prim::$V(v) => Prim::$V(Arc::new(
                        if n == 0 { Vec::new() } else { vec![v[i]; n] }
                    )), )+
                }
            }

            /// row `j` of the result is row `idx[j]` of `self`.
            pub(crate) fn gather(&self, idx: &[usize]) -> Prim {
                match self {
                    $( Prim::$V(v) => Prim::$V(Arc::new(idx.iter().map(|&i| v[i]).collect())), )+
                }
            }

            /// the rows whose mask element is nonzero, in order, in one pass with no branch on the
            /// mask: every row is written to the next free slot, and the slot is kept only if the
            /// mask says so. The buffer has room for every row (plus the one slot written past the
            /// last kept row); a sparse mask touches only its front, so the rest costs no memory.
            pub(crate) fn compress(&self, mask: &[u8]) -> Prim {
                match self {
                    $( Prim::$V(v) => {
                        let mut out = vec![<$t>::default(); v.len() + 1];
                        let mut len = 0;
                        for (&x, &b) in v.iter().zip(mask) {
                            out[len] = x;
                            len += (b != 0) as usize;
                        }
                        out.truncate(len);
                        Prim::$V(Arc::new(out))
                    } )+
                }
            }

            /// `gather` with every position past the leaf reading zero (zero bits).
            pub(crate) fn gather_or_zero(&self, idx: &[usize]) -> Prim {
                match self {
                    $( Prim::$V(v) => Prim::$V(Arc::new(idx.iter().map(|&i| v.get(i).copied().unwrap_or_default()).collect())), )+
                }
            }

            /// multi-source gather: result row `k` is element `off[k]` of source `srcs[tags[k]]`. The
            /// leaf of [`crate::engine::gather_lanes`]; `gather` is the 1-source case. The tags may
            /// be a `Sum`'s own `u8` discriminants, read in place, or any `usize` column. Integer
            /// sources at different storages are widened first.
            pub(crate) fn gather_lanes<T: Copy>(srcs: &[&Prim], tags: &[T], off: &[usize]) -> Prim
            where
                usize: From<T>,
            {
                if srcs.iter().any(|s| s.storage() != srcs[0].storage()) {
                    let met = Prim::meet_all(srcs);
                    let refs: Vec<&Prim> = met.iter().collect();
                    return Prim::gather_lanes(&refs, tags, off);
                }
                match srcs[0] {
                    $( Prim::$V(_) => {
                        let cols: Vec<&[$t]> = srcs.iter().map(|s| match s {
                            Prim::$V(v) => v.as_slice(),
                            _ => panic!("gather_lanes: an integer meets a float"),
                        }).collect();
                        Prim::$V(Arc::new(tags.iter().zip(off).map(|(&t, &o)| cols[usize::from(t)][o]).collect()))
                    } )+
                }
            }

            /// the rows `index[..]` as order keys, appended to `out` — the one indirect read the
            /// indexed sort makes; every pass after it is sequential.
            pub(crate) fn pull_keys(&self, index: &[usize], out: &mut Vec<u64>) {
                match self { $( Prim::$V(v) => out.extend(index.iter().map(|&i| v[i].key())), )+ }
            }

            /// a leaf of this storage holding the elements whose keys are `keys`: the sorted keys
            /// are the sorted column.
            pub(crate) fn like_keys(&self, keys: &[u64]) -> Prim {
                self.like_keys_iter(keys.iter().copied())
            }

            /// `keys[q] = (keys[q] << width) | key(self[index[q]])`: this leaf's rows packed below
            /// the keys already there, at the leaf's storage width.
            pub(crate) fn pack_keys(&self, index: &[usize], keys: &mut [u64]) {
                match self {
                    $( Prim::$V(v) => {
                        let bits = <$t as Elem>::BITS;
                        for (k, &i) in keys.iter_mut().zip(index) { *k = k.checked_shl(bits).unwrap_or(0) | v[i].key(); }
                    } )+
                }
            }

            /// a leaf of this storage holding the elements whose keys `it` yields.
            pub(crate) fn like_keys_iter(&self, it: impl Iterator<Item = u64>) -> Prim {
                match self { $( Prim::$V(_) => Prim::$V(Arc::new(it.map(<$t>::from_key).collect())), )+ }
            }

            /// stable per-element hash: each element's word (an integer's two's complement, a
            /// float's key) mixed by the splitmix64 finalizer. The leaf of [`crate::hash::hash`]. An
            /// integer hashes by its value, so a byte 5 and an `i64` 5 hash alike.
            pub(crate) fn hashes(&self) -> Vec<u64> {
                match self {
                    $( Prim::$V(v) => v.iter().map(|&x| crate::hash::mix64(x.word())).collect(), )+
                }
            }

            /// Fold leaf hashes into an existing structural accumulator without materializing
            /// a temporary hash column. The leaf encoding and hash are identical to `hashes`.
            pub(crate) fn fold_hashes(&self, acc: &mut [u64], mut fold: impl FnMut(u64, u64) -> u64) {
                match self {
                    $( Prim::$V(v) => {
                        for (a, &x) in acc.iter_mut().zip(v.iter()) {
                            *a = fold(*a, crate::hash::mix64(x.word()));
                        }
                    } )+
                }
            }

            /// append same-storage leaves end to end. Test-only: the leaf of `engine::concat`, the
            /// `gather_lanes` reference oracle (no production path concatenates leaves).
            #[cfg(test)]
            pub(crate) fn concat(parts: &[&Prim]) -> Prim {
                if parts.iter().any(|s| s.storage() != parts[0].storage()) {
                    let met = Prim::meet_all(parts);
                    return Prim::concat(&met.iter().collect::<Vec<_>>());
                }
                match parts[0] {
                    $( Prim::$V(_) => {
                        let mut o = Vec::new();
                        for &p in parts {
                            match p {
                                Prim::$V(x) => o.extend_from_slice(x),
                                _ => panic!("concat: an integer meets a float"),
                            }
                        }
                        Prim::$V(Arc::new(o))
                    } )+
                }
            }
        }

        /// the pairwise kernels: each brings its two leaves to one storage ([`Prim::meet`]) and then
        /// runs one loop per storage.
        impl Prim {
            /// lane-wise min (`take_max=false`) or max (`true`) of two columns, by value — an order
            /// op, hence `cmp`'s, not arithmetic's. CONSUMES both operands and writes in place into
            /// whichever is uniquely owned (the opportunistic reuse of arithmetic's `bin_into`);
            /// only when both are shared do we allocate.
            pub(crate) fn lane_pick(self, other: Prim, take_max: bool) -> Prim {
                let (a, b) = Prim::meet(self, other);
                match (a, b) {
                    $( (Prim::$V(mut a), Prim::$V(mut b)) => {
                        let pick = |x: $t, y: $t| if take_max { x.max(y) } else { x.min(y) };
                        Prim::$V(if let Some(dst) = Arc::get_mut(&mut a) {
                            for (x, &y) in dst.iter_mut().zip(b.iter()) { *x = pick(*x, y); }
                            a
                        } else if let Some(dst) = Arc::get_mut(&mut b) {
                            for (&x, y) in a.iter().zip(dst.iter_mut()) { *y = pick(x, *y); }
                            b
                        } else {
                            Arc::new(a.iter().zip(b.iter()).map(|(&x, &y)| pick(x, y)).collect())
                        })
                    } )+
                    _ => unreachable!("meet brings both to one storage"),
                }
            }

            /// lane-wise blend of two columns by a selector: `out[i]` is `self[i]` where `pick[i]`
            /// is nonzero, else `other[i]`. It moves elements and never interprets them, and is
            /// BRANCHLESS: the lane body is an unconditional select, so it vectorizes. The leaf of
            /// [`crate::engine::blend`]. CONSUMES both and writes into whichever is uniquely owned.
            pub(crate) fn blend(self, other: Prim, pick: &[u8]) -> Prim {
                let (a, b) = Prim::meet(self, other);
                match (a, b) {
                    $( (Prim::$V(mut a), Prim::$V(mut b)) => {
                        Prim::$V(if let Some(dst) = Arc::get_mut(&mut a) {
                            for (x, (&y, &m)) in dst.iter_mut().zip(b.iter().zip(pick)) {
                                *x = if m != 0 { *x } else { y };
                            }
                            a
                        } else if let Some(dst) = Arc::get_mut(&mut b) {
                            for (y, (&x, &m)) in dst.iter_mut().zip(a.iter().zip(pick)) {
                                *y = if m != 0 { x } else { *y };
                            }
                            b
                        } else {
                            Arc::new(a.iter().zip(b.iter()).zip(pick)
                                .map(|((&x, &y), &m)| if m != 0 { x } else { y }).collect())
                        })
                    } )+
                    _ => unreachable!("meet brings both to one storage"),
                }
            }

            /// overwrite rows `active[p]` of `self` with `src`'s row `p`, IN PLACE — `make_mut` gives
            /// the buffer mutably when uniquely owned (the common case), or clones it once if shared.
            /// Touches only the `active` rows. A leaf receiving rows its storage can't hold widens
            /// first. The leaf of [`scatter`].
            pub(crate) fn scatter_into(&mut self, active: &[usize], src: &Prim) {
                let (mine, theirs) = (self.storage(), src.storage());
                if mine != theirs {
                    let j = Storage::join(mine.expect("an integer leaf"), theirs.expect("an integer leaf"));
                    if mine != Some(j) {
                        *self = self.to_storage(j);
                    }
                }
                let src = if src.storage() != self.storage() { std::borrow::Cow::Owned(src.to_storage(self.storage().expect("an integer leaf"))) } else { std::borrow::Cow::Borrowed(src) };
                match (self, &*src) {
                    $( (Prim::$V(dst), Prim::$V(s)) => {
                        let dst = Arc::make_mut(dst);
                        for (p, &r) in active.iter().enumerate() { dst[r] = s[p]; }
                    } )+
                    _ => panic!("scatter_into: an integer meets a float"),
                }
            }

            /// structural order of paired records: `out[k]` = sign of `self[ia[k]]` vs `other[ib[k]]`
            /// (`-1`/`0`/`+1`, as `Ordering as i8`). Reads through the indices, so gather-bound and
            /// scalar on NEON; the dense column-vs-column compare is [`Prim::rel`].
            pub(crate) fn cmp_idx(&self, ia: &[usize], ib: &[usize], other: &Prim) -> Vec<i8> {
                // integers at two storages compare pair by pair, by value: converting either
                // whole would cost the column for the pairs (a search reads a few per needle).
                if self.storage() != other.storage() && self.is_int() && other.is_int() {
                    return ia.iter().zip(ib).map(|(&i, &j)| self.int_at(i).cmp(&other.int_at(j)) as i8).collect();
                }
                let (a, b) = Prim::meet_ref(self, other);
                match (&*a, &*b) {
                    $( (Prim::$V(a), Prim::$V(b)) =>
                        ia.iter().zip(ib).map(|(&i, &j)| (a[i] > b[j]) as i8 - (a[i] < b[j]) as i8).collect(), )+
                    _ => unreachable!("meet brings both to one storage"),
                }
            }

            /// structural order of DENSE pairs: `out[k]` = sign of `self[k]` vs `other[k + skew]`,
            /// for `n` pairs. The implicit-index leaf compare — `skew` 0 is the diagonal (row i vs
            /// row i) and `skew` 1 the adjacent (row k vs row k+1). Both sides are read
            /// sequentially, so this vectorizes where [`Prim::cmp_idx`] is two gathers per lane.
            pub(crate) fn cmp_dense(&self, other: &Prim, n: usize, skew: usize) -> Vec<i8> {
                if Prim::meet_wide(self, other) {
                    return Prim::int_tiles(self, other, n, skew, |x, y| (x > y) as i8 - (x < y) as i8);
                }
                let (a, b) = Prim::meet_ref(self, other);
                match (&*a, &*b) {
                    $( (Prim::$V(a), Prim::$V(b)) => (0..n)
                        .map(|k| {
                            let (x, y) = (a[k], b[k + skew]);
                            (x > y) as i8 - (x < y) as i8
                        })
                        .collect(), )+
                    _ => unreachable!("meet brings both to one storage"),
                }
            }

            /// lane-wise relational compare of two columns → a 0/1 byte mask. The three order-flags
            /// arrive pre-resolved (`lt`/`eq`/`gt`), so the lane body is branchless and vectorizes.
            pub(crate) fn rel(&self, other: &Prim, lt: bool, eq: bool, gt: bool) -> Vec<u8> {
                if Prim::meet_wide(self, other) {
                    return Prim::int_tiles(self, other, self.len(), 0, |x, y| ((lt & (x < y)) | (eq & (x == y)) | (gt & (x > y))) as u8);
                }
                let (a, b) = Prim::meet_ref(self, other);
                match (&*a, &*b) {
                    $( (Prim::$V(a), Prim::$V(b)) => a.iter().zip(b.iter())
                        .map(|(x, y)| ((lt & (x < y)) | (eq & (x == y)) | (gt & (x > y))) as u8)
                        .collect(), )+
                    _ => unreachable!("meet brings both to one storage"),
                }
            }
        }

        /// the immediate kernels: a leaf against a constant of its kind. An integer leaf whose
        /// storage can't hold the constant works at the narrowest storage that holds both.
        impl Prim {
            /// `rel` against the constant `c`: the mask of rows whose comparison with `c` lands in
            /// the chosen order flags.
            pub(crate) fn rel_imm(&self, c: Scalar, lt: bool, eq: bool, gt: bool) -> Vec<u8> {
                if let Some(wider) = self.to_hold(c) {
                    return wider.rel_imm(c, lt, eq, gt);
                }
                match self {
                    // one loop per predicate: a single compare per lane, where folding the three
                    // order flags into the body measured ~20% slower on a `gt` filter.
                    $( Prim::$V(a) => {
                        let y: $t = c.elem();
                        match (lt, eq, gt) {
                            (true, false, false) => a.iter().map(|&x| (x < y) as u8).collect(),
                            (true, true, false) => a.iter().map(|&x| (x <= y) as u8).collect(),
                            (false, true, false) => a.iter().map(|&x| (x == y) as u8).collect(),
                            (true, false, true) => a.iter().map(|&x| (x != y) as u8).collect(),
                            (false, false, true) => a.iter().map(|&x| (x > y) as u8).collect(),
                            (false, true, true) => a.iter().map(|&x| (x >= y) as u8).collect(),
                            // no flag or every flag: the predicate is constant
                            (all, _, _) => vec![all as u8; a.len()],
                        }
                    } )+
                }
            }

            /// lane-wise min (or max, with `take_max`) against the constant `c`; in place when
            /// uniquely owned.
            pub(crate) fn pick_imm(self, c: Scalar, take_max: bool) -> Prim {
                if let Some(wider) = self.to_hold(c) {
                    return wider.pick_imm(c, take_max);
                }
                match self {
                    $( Prim::$V(mut a) => {
                        let c: $t = c.elem();
                        let pick = |x: $t| if take_max { x.max(c) } else { x.min(c) };
                        Prim::$V(if let Some(dst) = Arc::get_mut(&mut a) {
                            for x in dst.iter_mut() { *x = pick(*x); }
                            a
                        } else {
                            Arc::new(a.iter().map(|&x| pick(x)).collect())
                        })
                    } )+
                }
            }
        }
    };
}

prim! {
    U8 => u8,
    I8 => i8,
    I16 => i16,
    I32 => i32,
    I64 => i64,
    F64 => u64,
}

impl Prim {
    /// does this leaf hold integers (rather than floats)?
    pub(crate) fn is_int(&self) -> bool {
        !matches!(self, Prim::F64(_))
    }

    /// the storage an integer leaf is held at (`None` for a float leaf).
    pub(crate) fn storage(&self) -> Option<Storage> {
        match self {
            Prim::U8(_) => Some(Storage::U8),
            Prim::I8(_) => Some(Storage::I8),
            Prim::I16(_) => Some(Storage::I16),
            Prim::I32(_) => Some(Storage::I32),
            Prim::I64(_) => Some(Storage::I64),
            Prim::F64(_) => None,
        }
    }

    /// row `i` of an integer leaf, as an `i64`.
    #[inline]
    pub(crate) fn int_at(&self, i: usize) -> i64 {
        self.word_at(i) as i64
    }

    /// two integer leaves at different storages that would meet at `i64`: then a dense kernel reads
    /// them a tile at a time ([`Prim::int_tiles`]) rather than converting the narrower whole. (Two
    /// that meet narrower convert, since the narrow compare is the faster one.)
    pub(crate) fn meet_wide(a: &Prim, b: &Prim) -> bool {
        match (a.storage(), b.storage()) {
            (Some(x), Some(y)) => x != y && Storage::join(x, y) == Storage::I64,
            _ => false,
        }
    }

    /// `f(a[k], b[k + skew])` for `k` in `0..n`, two integer leaves at any storages read a tile at a
    /// time as `i64`s: how a compare of two storages runs without converting either column.
    pub(crate) fn int_tiles<T>(a: &Prim, b: &Prim, n: usize, skew: usize, f: impl Fn(i64, i64) -> T) -> Vec<T> {
        let (mut ba, mut bb) = (Vec::new(), Vec::new());
        let mut out = Vec::with_capacity(n);
        for at in (0..n).step_by(TILE) {
            let m = TILE.min(n - at);
            let (xs, ys) = (a.int_tile(at, m, &mut ba), b.int_tile(at + skew, m, &mut bb));
            out.extend(xs.iter().zip(ys).map(|(&x, &y)| f(x, y)));
        }
        out
    }

    /// rows `at..at + n` (at most [`TILE`]) of an integer leaf as `i64`s: the leaf's own slice when
    /// it is held as `i64`s, otherwise widened into `buf`. How a compare reads two storages that
    /// meet at `i64` a tile at a time, so neither is converted whole.
    #[inline]
    pub(crate) fn int_tile<'a>(&'a self, at: usize, n: usize, buf: &'a mut Vec<i64>) -> &'a [i64] {
        fn widen<T: Elem>(xs: &[T], buf: &mut Vec<i64>) {
            buf.clear();
            buf.extend(xs.iter().map(|&x| x.word() as i64));
        }
        match self {
            Prim::I64(v) => return &v[at..at + n],
            Prim::U8(v) => widen(&v[at..at + n], buf),
            Prim::I8(v) => widen(&v[at..at + n], buf),
            Prim::I16(v) => widen(&v[at..at + n], buf),
            Prim::I32(v) => widen(&v[at..at + n], buf),
            Prim::F64(_) => unreachable!("int_tile: an integer leaf"),
        }
        buf
    }

    /// the leaf at a storage that also holds the constant `c`, when its own can't (`None` when it
    /// can, or for a float).
    pub(crate) fn to_hold(&self, c: Scalar) -> Option<Prim> {
        let (Some(mine), Scalar::Int(x)) = (self.storage(), c) else { return None };
        let (lo, hi) = mine.range();
        (x < lo || x > hi).then(|| self.to_storage(Storage::join(mine, Storage::holding(x, x))))
    }

    /// two leaves at one storage, to be combined lane by lane: two integer leaves at different
    /// storages meet at the narrowest storage that holds both. A leaf already there is returned as
    /// it was. An integer and a float never meet; the typer keeps them apart, so meeting them is a
    /// bug.
    pub(crate) fn meet(a: Prim, b: Prim) -> (Prim, Prim) {
        assert_eq!(a.is_int(), b.is_int(), "an integer leaf meets a float leaf");
        match (a.storage(), b.storage()) {
            (Some(x), Some(y)) if x != y => {
                let j = Storage::join(x, y);
                (a.to_storage(j), b.to_storage(j))
            }
            _ => (a, b),
        }
    }

    /// [`Prim::meet`], borrowing where nothing needs to convert.
    pub(crate) fn meet_ref<'a>(a: &'a Prim, b: &'a Prim) -> (std::borrow::Cow<'a, Prim>, std::borrow::Cow<'a, Prim>) {
        use std::borrow::Cow;
        assert_eq!(a.is_int(), b.is_int(), "an integer leaf meets a float leaf");
        match (a.storage(), b.storage()) {
            (Some(x), Some(y)) if x != y => {
                let j = Storage::join(x, y);
                let conv = |p: &'a Prim| if p.storage() == Some(j) { Cow::Borrowed(p) } else { Cow::Owned(p.to_storage(j)) };
                (conv(a), conv(b))
            }
            _ => (Cow::Borrowed(a), Cow::Borrowed(b)),
        }
    }

    /// leaves at one storage: the narrowest that holds every one of them.
    fn meet_all(parts: &[&Prim]) -> Vec<Prim> {
        let j = parts.iter().filter_map(|p| p.storage()).reduce(Storage::join);
        parts.iter().map(|p| j.map_or_else(|| (*p).clone(), |j| p.to_storage(j))).collect()
    }

    /// an empty (zero-row) leaf: an integer one is `i64`, a float one is `F64`. Used to fill the
    /// unselected variants of an `Inject`.
    pub(crate) fn empty(int: bool) -> Prim {
        if int { Prim::I64(Arc::new(Vec::new())) } else { Prim::F64(Arc::new(Vec::new())) }
    }

    /// A leaf whose positions are the owned `idx` (each a word: a negative position is past every
    /// row). An `i64` haystack rewrites that buffer in place into the result; other storages
    /// allocate their own. The raw caller deliberately materializes even an identity gather
    /// rather than adding an identity-detection scan to its single indexing pass. An index past
    /// the leaf reads zero. (A clamped read and a select, with no branch, measured 10–18% slower
    /// on pointer chasing than this bounds test, whose branch is predicted when indices are in
    /// range.)
    pub(crate) fn gather_words_owned(&self, mut idx: Vec<u64>) -> Prim {
        if let Prim::I64(v) = self {
            for x in idx.iter_mut() {
                *x = v.get(*x as usize).copied().unwrap_or(0) as u64;
            }
            Prim::I64(Arc::new(i64s_of_words(idx)))
        } else {
            let idx: Vec<usize> = idx.into_iter().map(|i| usize::try_from(i).unwrap_or(usize::MAX)).collect();
            self.gather_or_zero(&idx)
        }
    }

    /// Validate one row of positions and gather it. Exact identity indices reuse the haystack
    /// leaf; an `i64` haystack otherwise validates and rewrites the owned index buffer in one
    /// pass, while other storages keep an all-or-nothing validation pass.
    pub(crate) fn gather_words_checked_owned(&self, mut idx: Vec<u64>, rowlen: usize) -> Option<Prim> {
        // A List invariant guarantees the flattened one-row leaf has exactly `rowlen`
        // elements. Identity reuse and checked indexing both rely on that correspondence.
        debug_assert_eq!(self.len(), rowlen, "gather: bounds/leaf length mismatch");
        let identity = idx.len() == rowlen
            && (rowlen == 0 || (idx[0] == 0 && idx.iter().enumerate().all(|(i, &x)| x == i as u64)));
        if identity {
            return Some(self.clone());
        }
        if let Prim::I64(v) = self {
            for x in idx.iter_mut() {
                if *x >= rowlen as u64 {
                    return None;
                }
                *x = v[*x as usize] as u64;
            }
            return Some(Prim::I64(Arc::new(i64s_of_words(idx))));
        }
        (!idx.iter().any(|&x| x >= rowlen as u64)).then(|| self.gather_words_owned(idx))
    }

    fn show(&self) -> String {
        match self {
            Prim::U8(xs) => format!("{xs:?}"),
            Prim::I8(xs) => format!("{xs:?}"),
            Prim::I16(xs) => format!("{xs:?}"),
            Prim::I32(xs) => format!("{xs:?}"),
            Prim::I64(xs) => format!("{xs:?}"),
            Prim::F64(xs) => format!("{:?}", xs.iter().map(|&k| f64_of_key(k)).collect::<Vec<_>>()),
        }
    }
}

// equality and hashing go by VALUE: an integer is the same whatever storage holds it, so a byte
// leaf and an `i64` leaf of the same integers are equal and hash alike.
impl PartialEq for Prim {
    fn eq(&self, other: &Prim) -> bool {
        match (self, other) {
            (Prim::U8(a), Prim::U8(b)) => a == b,
            (Prim::I64(a), Prim::I64(b)) => a == b,
            (Prim::F64(a), Prim::F64(b)) => a == b,
            (a, b) if a.is_int() && b.is_int() => {
                a.len() == b.len() && (0..a.len()).all(|i| a.int_at(i) == b.int_at(i))
            }
            _ => false,
        }
    }
}
impl Eq for Prim {}
impl std::hash::Hash for Prim {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.is_int().hash(state);
        self.len().hash(state);
        for i in 0..self.len() {
            self.word_at(i).hash(state);
        }
    }
}

/// within-variant offset of each row: `out[i]` = the index of row `i` inside `variants[tags[i]]`, in
/// one cursor pass. A `Sum` carries this (see [`Value::sum`]).
fn within_offsets(tags: impl Iterator<Item = usize>, k: usize) -> Vec<usize> {
    let mut cursor = vec![0usize; k];
    tags.map(|t| { let p = cursor[t]; cursor[t] += 1; p }).collect()
}

impl Value {
    /// leaf-column constructors — the funnel results pass through, so the representation lives in one place.
    /// Integers held as bytes (each from 0 to 255): text, masks, tags.
    pub fn u8(xs: Vec<u8>) -> Value { Value::Prim(Prim::U8(Arc::new(xs))) }
    /// Integers held as `i8`s, `i16`s, `i32`s or `i64`s.
    pub fn i8(xs: Vec<i8>) -> Value { Value::Prim(Prim::I8(Arc::new(xs))) }
    pub fn i16(xs: Vec<i16>) -> Value { Value::Prim(Prim::I16(Arc::new(xs))) }
    pub fn i32(xs: Vec<i32>) -> Value { Value::Prim(Prim::I32(Arc::new(xs))) }
    pub fn i64(xs: Vec<i64>) -> Value { Value::Prim(Prim::I64(Arc::new(xs))) }
    /// Integers from 0 to `hi`, collected at the narrowest storage that holds that range with no
    /// pass to find it: how an op that knows its bound (a length, a run's number) writes.
    pub(crate) fn upto(hi: usize, xs: impl Iterator<Item = usize>) -> Value {
        Value::Prim(match Storage::holding(0, hi as i64) {
            Storage::U8 => Prim::U8(Arc::new(xs.map(|x| x as u8).collect())),
            Storage::I16 => Prim::I16(Arc::new(xs.map(|x| x as i16).collect())),
            Storage::I32 => Prim::I32(Arc::new(xs.map(|x| x as i32).collect())),
            _ => Prim::I64(Arc::new(xs.map(|x| x as i64).collect())),
        })
    }
    /// Floats.
    pub fn f64(xs: Vec<f64>) -> Value { Value::Prim(Prim::F64(Arc::new(xs.into_iter().map(f64_key).collect()))) }

    /// the same value with each integer leaf held at the narrowest storage that holds its values.
    /// Storage is never part of a value, so no answer changes: a host tool, for handing corgi
    /// narrow columns.
    pub fn narrowed(self) -> Value {
        self.restored(&mut Storage::holding)
    }

    /// the same value with each integer leaf re-stored at `pick(lo, hi)`, a storage holding its
    /// least and greatest values (`(0, 0)` for a leaf with no rows). Floats stay as they are, and
    /// so does a referenced arena, whose sharing the references rely on.
    pub(crate) fn restored(self, pick: &mut impl FnMut(i64, i64) -> Storage) -> Value {
        match self {
            Value::Prim(p) if p.is_int() => {
                let (lo, hi) = p.int_range().unwrap_or((0, 0));
                Value::Prim(p.to_storage(pick(lo, hi)))
            }
            Value::Prod(vs) => Value::Prod(vs.into_iter().map(|v| v.restored(pick)).collect()),
            Value::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(|v| v.restored(pick)).collect()),
            Value::List(b, v) => Value::List(b, Box::new(v.restored(pick))),
            v @ (Value::Prim(_) | Value::Unit(_) | Value::Ref(..)) => v,
        }
    }

    /// a Sum from its discriminant `tags` (stored as a u8 leaf column — ≤256 variants) and the
    /// per-variant columns (every lane present; a variant no row carries is an empty column). The
    /// one place tags cross from `usize` into the `Prim` fold. The within-variant offset is computed
    /// here and carried, so comparison/search read it instead of re-deriving each row's rank.
    pub fn sum(tags: Vec<usize>, variants: Vec<Value>) -> Value {
        let arity = variants.len();
        assert!(arity > 0, "Value::sum: a sum needs at least one lane (a sum of none has no value, not even zero)");
        Value::Sum(Tags::from_tags(tags, arity), variants)
    }

    /// a Sum from a lane assignment and its lanes — the direct form for ops that computed the
    /// assignment themselves (`gather`, `Branch`, `inject`).
    pub(crate) fn sum_tagged(tags: Tags, lanes: Vec<Value>) -> Value {
        Value::Sum(tags, lanes)
    }

    /// a zero-row value of the given shape — the all-empty witness of each constructor. `Inject`
    /// fills the lanes it does not carry with this; the recursion mirrors `shape_of_value` inverted.
    pub fn empty(shape: &Shape) -> Value {
        match shape {
            Shape::Int => Value::Prim(Prim::empty(true)),
            Shape::Float => Value::Prim(Prim::empty(false)),
            Shape::Prod(ss) => Value::Prod(ss.iter().map(Value::empty).collect()),
            Shape::Sum(ss) => {
                Value::Sum(Tags::Const(0, 0), ss.iter().map(Value::empty).collect())
            }
            Shape::List(s) => Value::List(Bounds::offsets(Vec::new()), Box::new(Value::empty(s))),
            Shape::Unit => Value::Unit(0),
            Shape::Ref(s) => match &**s {
                Shape::List(_) => Value::Ref(Arc::new(Value::empty(s)), Arc::new(Vec::new())),
                other => panic!("Value::empty: Ref<{other}> — only list rows are referenced"),
            },
        }
    }

    /// SEQ length: how many rows this column holds.
    pub fn len(&self) -> usize {
        match self {
            Value::Prim(p) => p.len(),
            Value::Prod(c) => c.first().map_or(0, |c| c.len()),
            Value::Sum(t, _) => t.len(),
            Value::List(b, _) => b.len(),
            Value::Unit(n) => *n,
            Value::Ref(_, rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// a `Sum` taken apart: its lane assignment and its lanes.
pub type SumParts = (Tags, Vec<Value>);

// input accessors: destructure a `Value` to the shape an op expects; a mismatch is the shape ERROR
// the typer reports (an op's eval is `shape_of` when run on zero rows). `into_*` consume `self` and
// move the buffers out.
impl Value {
    pub(crate) fn into_pair(self, who: &str) -> Result<(Value, Value), String> {
        match self {
            Value::Prod(mut cols) if cols.len() == 2 => {
                let b = cols.pop().unwrap();
                let a = cols.pop().unwrap();
                Ok((a, b))
            }
            other => Err(format!("{who}: expected a pair, got {}", shape_of_value(&other))),
        }
    }

    pub fn into_prod(self, who: &str) -> Result<Vec<Value>, String> {
        match self {
            Value::Prod(cols) => Ok(cols),
            other => Err(format!("{who}: expected a product, got {}", shape_of_value(&other))),
        }
    }

    pub fn into_list(self, who: &str) -> Result<(Bounds, Value), String> {
        match self {
            Value::List(bounds, vals) => Ok((bounds, *vals)),
            other => Err(format!("{who}: expected a list, got {}", shape_of_value(&other))),
        }
    }

    /// a haystack's rows over its payload: a `List` (partition) or a `Ref` (rows of the shared
    /// list, by number), for the readers that address rows through `span(i)` and never need the
    /// payload to be exactly the rows. Borrowed: every reader only indexes the payload.
    pub(crate) fn rows_of(&self, who: &str) -> Result<(Rows<'_>, &Value), String> {
        match self {
            Value::List(bounds, vals) => Ok((Rows::Part(bounds), vals)),
            Value::Ref(list, rows) => {
                let (bounds, vals) = referenced(list);
                Ok((Rows::Named(bounds, rows), vals))
            }
            other => Err(format!("{who}: expected a list (or a referenced list), got {}", shape_of_value(other))),
        }
    }

    pub fn into_sum(self, who: &str) -> Result<SumParts, String> {
        match self {
            Value::Sum(tags, variants) => Ok((tags, variants)),
            other => Err(format!("{who}: expected a sum, got {}", shape_of_value(&other))),
        }
    }

    /// borrow an integer leaf as `i64`s — for an op that only READS its operand. A leaf held
    /// narrower widens (a copy); an `i64` leaf is borrowed.
    ///
    /// `into_i64` forces ownership, and ownership is a full column COPY whenever anyone else still
    /// holds the buffer: a graph node with fan-out 2, or a caller that keeps its input. Measured on
    /// a one-pass `fold_add` at 1M rows, that copy was 7.9x the whole operation. Reading needs none
    /// of it; only an op that rewrites its operand in place has to consume it.
    pub fn as_i64(&self, who: &str) -> Result<std::borrow::Cow<'_, [i64]>, String> {
        use std::borrow::Cow;
        match self {
            Value::Prim(Prim::I64(xs)) => Ok(Cow::Borrowed(&xs[..])),
            Value::Prim(p) if p.is_int() => Ok(Cow::Owned(p.ints())),
            other => Err(format!("{who}: expected Int, got {}", shape_of_value(other))),
        }
    }

    /// take an integer leaf as an owned `i64` buffer — for an op that REWRITES its operand in
    /// place. Moves the buffer out at refcount 1, and copies it when shared or held narrower.
    pub fn into_i64(self, who: &str) -> Result<Vec<i64>, String> {
        match self {
            Value::Prim(Prim::I64(xs)) => Ok(Arc::try_unwrap(xs).unwrap_or_else(|a| (*a).clone())),
            Value::Prim(p) if p.is_int() => Ok(p.ints()),
            other => Err(format!("{who}: expected Int, got {}", shape_of_value(&other))),
        }
    }

    /// an integer leaf as 64-bit words (two's complement) — how positions, counts and tags are
    /// read, so a negative one is past every row. Borrowed from an `i64` leaf, widened from others.
    pub(crate) fn as_words(&self, who: &str) -> Result<std::borrow::Cow<'_, [u64]>, String> {
        use std::borrow::Cow;
        match self {
            Value::Prim(Prim::I64(xs)) => Ok(Cow::Borrowed(words_of(xs))),
            Value::Prim(p) if p.is_int() => Ok(Cow::Owned(p.words())),
            other => Err(format!("{who}: expected Int, got {}", shape_of_value(other))),
        }
    }

    /// [`Value::as_words`], owned: the `i64` buffer itself when uniquely held.
    pub(crate) fn into_words(self, who: &str) -> Result<Vec<u64>, String> {
        Ok(words_of_i64s(self.into_i64(who)?))
    }

    /// an integer leaf as a mask: a byte per row, nonzero where the row is. A byte leaf (what
    /// every comparison writes) is borrowed as it is.
    pub(crate) fn as_mask(&self, who: &str) -> Result<std::borrow::Cow<'_, [u8]>, String> {
        use std::borrow::Cow;
        match self {
            Value::Prim(Prim::U8(xs)) => Ok(Cow::Borrowed(&xs[..])),
            Value::Prim(p) if p.is_int() => Ok(Cow::Owned(p.mask())),
            other => Err(format!("{who}: expected an Int mask, got {}", shape_of_value(other))),
        }
    }

    /// borrow a byte leaf — text, which is a list of integers held as bytes. Crate-private: a host
    /// reads integers by shape (`as_i64`), never by storage.
    pub(crate) fn as_u8(&self, who: &str) -> Result<&[u8], String> {
        match self {
            Value::Prim(Prim::U8(xs)) => Ok(&xs[..]),
            other => Err(format!("{who}: expected Int bytes, got {}", shape_of_value(other))),
        }
    }

    /// a float leaf's values.
    pub fn as_f64(&self, who: &str) -> Result<Vec<f64>, String> {
        match self {
            Value::Prim(Prim::F64(ks)) => Ok(ks.iter().map(|&k| f64_of_key(k)).collect()),
            other => Err(format!("{who}: expected Float, got {}", shape_of_value(other))),
        }
    }

    pub(crate) fn into_prim(self, who: &str) -> Result<Prim, String> {
        match self {
            Value::Prim(p) => Ok(p),
            other => Err(format!("{who}: expected a leaf, got {}", shape_of_value(&other))),
        }
    }
}

/// human-readable rendering used by tests and demos.
pub fn show(v: &Value) -> String {
    match v {
        Value::Prim(p) => p.show(),
        Value::Prod(c) => format!("({})", c.iter().map(show).collect::<Vec<_>>().join(", ")),
        Value::Sum(t, vs) => {
            let lanes: Vec<String> = vs.iter().map(show).collect();
            format!("Sum tags={:?} [{}]", t.tags_iter().collect::<Vec<_>>(), lanes.join(", "))
        }
        Value::List(b, vals) => format!("List ends={:?} <{}>", b.to_vec(), show(vals)),
        Value::Unit(n) => format!("()x{n}"),
        Value::Ref(..) => format!("Ref <{}>", show(&crate::engine::clone_ref(v.clone()))),
    }
}

#[cfg(test)]
mod tests;
