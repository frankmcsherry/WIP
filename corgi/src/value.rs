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
    pub(crate) fn uniform(ends: &[usize]) -> Option<usize> {
        let &last = ends.last()?;
        let n = ends.len();
        if last % n != 0 {
            return None;
        }
        let k = last / n;
        ends.iter().enumerate().all(|(i, &e)| e == (i + 1) * k).then_some(k)
    }

    /// rows `c..` as their own partition (rebased to start at 0), leaving rows `..c`. Also returns
    /// where the payload splits: the element count of the rows kept. See [`Value::split_off`].
    pub(crate) fn split_off(&mut self, c: usize) -> (Bounds, usize) {
        match self {
            Bounds::Stride(k, rows) => {
                let tail = Bounds::Stride(*k, *rows - c);
                *rows = c;
                (tail, c * *k)
            }
            Bounds::Offsets(ends) => {
                let at = if c == 0 { 0 } else { ends[c - 1] };
                let mut tail = split_arc(ends, c);
                if at > 0 {
                    for e in Arc::make_mut(&mut tail).iter_mut() {
                        *e -= at;
                    }
                }
                (Bounds::Offsets(tail), at)
            }
        }
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

    /// rows `c..` as their own assignment, leaving rows `..c`; also returns, per lane, how many of
    /// its rows stay (`lens` holds each lane's row count). A lane's rows are packed in row order (a
    /// row's offset is its rank among its lane's rows), so lane `l` keeps its first `keep[l]` rows
    /// and the tail's offsets drop by `keep`. Only the tail's rows are read. Either side that lies
    /// in one lane becomes `Const`. See [`Value::split_off`].
    pub(crate) fn split_off(&mut self, c: usize, lens: &[usize]) -> (Tags, Vec<usize>) {
        let n = self.len();
        match self {
            Tags::Const(t, rows) => {
                let mut keep = vec![0; lens.len()];
                keep[*t] = c;
                let tail = Tags::Const(*t, n - c);
                *rows = c;
                (tail, keep)
            }
            Tags::Column(tags, offsets) => {
                let tail_tags = tags.split_off(c);
                let mut tail_offsets = split_arc(offsets, c);
                let mut keep = lens.to_vec();
                for i in 0..n - c {
                    keep[tail_tags.usize_at(i)] -= 1;
                }
                let tail = match (0..lens.len()).find(|&l| lens[l] - keep[l] == n - c) {
                    Some(l) => Tags::Const(l, n - c),
                    None => {
                        if keep.iter().any(|&k| k > 0) {
                            for (i, o) in Arc::make_mut(&mut tail_offsets).iter_mut().enumerate() {
                                *o -= keep[tail_tags.usize_at(i)];
                            }
                        }
                        Tags::Column(tail_tags, tail_offsets)
                    }
                };
                if let Some(l) = (0..lens.len()).find(|&l| keep[l] == c) {
                    *self = Tags::Const(l, c);
                }
                (tail, keep)
            }
        }
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

/// a leaf column at one byte width, each width its own naturally-aligned `Vec<uN>` behind an `Arc`
/// (leaves are write-once read-many; `eval` clones freely for shared edges, so a leaf clone must be a
/// refcount bump, not a buffer copy). The `prim!` macro lists the widths ONCE and generates the enum +
/// every method, so adding a width is one line here.
macro_rules! prim {
    ($($V:ident => $t:ty),+ $(,)?) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub enum Prim {
            $( $V(Arc<Vec<$t>>), )+
        }

        impl Prim {
            pub(crate) fn len(&self) -> usize {
                match self { $( Prim::$V(v) => v.len(), )+ }
            }

            /// the leaf's bit width (the shape-level reflection of which variant this is).
            pub(crate) fn bits(&self) -> u32 {
                match self { $( Prim::$V(_) => (std::mem::size_of::<$t>() * 8) as u32, )+ }
            }

            /// row `i` as a `usize`, read in place — how a small-int column (a `Sum`'s
            /// discriminant) is read. There is deliberately no whole-column `Vec<usize>` decode:
            /// producing one to look at some of a column made a scalar `compare_at` O(column).
            #[inline]
            pub(crate) fn usize_at(&self, i: usize) -> usize {
                match self { $( Prim::$V(v) => v[i] as usize, )+ }
            }

            /// row `i`'s stored bits, zero-extended to a `u64`: how a literal's value is read. Not
            /// `usize_at`, which truncates to 32 bits where `usize` is 32 bits (WebAssembly).
            #[inline]
            pub(crate) fn u64_at(&self, i: usize) -> u64 {
                match self { $( Prim::$V(v) => v[i] as u64, )+ }
            }

            /// re-width every record to `bits`, kind-blind: read it zero-extended to u64,
            /// then keep the low bytes. (Signed/sign-extending widen is a numeric-layer job.)
            ///
            /// Same width is the IDENTITY: the leaf is already correct storage for the result, so
            /// this is an `Arc` bump, not a column copy. A genuine re-width is ONE pass — the
            /// (source, destination) pair is dispatched ABOVE the loop, so the lane body is a single
            /// `as` and there is no intermediate `u64` column between the two widths.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn cast(&self, bits: u32) -> Prim {
                /// the destination half of the grid: collect zero-extended values at `bits`.
                /// Each arm MOVES `src` — they are exclusive, so only one loop ever runs.
                fn to_width(src: impl Iterator<Item = u64>, bits: u32) -> Prim {
                    match bits {
                        $( b if b == (std::mem::size_of::<$t>() * 8) as u32 =>
                            Prim::$V(Arc::new(src.map(|x| x as $t).collect())), )+
                        _ => panic!("cast: unsupported width {bits}"),
                    }
                }
                if bits == self.bits() {
                    return self.clone();
                }
                match self {
                    $( Prim::$V(v) => to_width(v.iter().map(|&x| x as u64), bits), )+
                }
            }

            /// an empty (zero-row) leaf at `bits`, the leaf case of `Value::empty` (matches
            /// `cast`'s width dispatch). Used to fill the unselected variants of an `Inject`.
            pub(crate) fn empty(bits: u32) -> Prim {
                match bits {
                    $( b if b == (std::mem::size_of::<$t>() * 8) as u32 => Prim::$V(Arc::new(Vec::new())), )+
                    _ => panic!("empty: unsupported width {bits}"),
                }
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
            pub(crate) fn compress(&self, mask: &[u64]) -> Prim {
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

            /// A U64 index column is also correctly typed storage for a U64 gather result. Rewrite
            /// that owned buffer in place; other haystack widths allocate their native vector. The
            /// raw caller deliberately materializes even an identity gather rather than adding an
            /// identity-detection scan to its single indexing pass. An index past the leaf reads
            /// zero. (A clamped read and a select, with no branch, measured 10–18% slower on pointer
            /// chasing than this bounds test, whose branch is predicted when indices are in range.)
            pub(crate) fn gather_u64_owned(&self, mut idx: Vec<u64>) -> Prim {
                if let Prim::U64(v) = self {
                    for x in idx.iter_mut() {
                        *x = v.get(*x as usize).copied().unwrap_or(0);
                    }
                    Prim::U64(Arc::new(idx))
                } else {
                    let idx: Vec<usize> = idx.into_iter().map(|i| usize::try_from(i).unwrap_or(usize::MAX)).collect();
                    self.gather_or_zero(&idx)
                }
            }

            /// `gather` with every position past the leaf reading zero (zero bits).
            pub(crate) fn gather_or_zero(&self, idx: &[usize]) -> Prim {
                match self {
                    $( Prim::$V(v) => Prim::$V(Arc::new(idx.iter().map(|&i| v.get(i).copied().unwrap_or_default()).collect())), )+
                }
            }

            /// Validate one row of indices and gather it. Exact identity indices reuse the
            /// haystack leaf; U64 gathers otherwise validate and rewrite the owned index buffer in
            /// one pass, while other widths retain an all-or-nothing validation pass.
            pub(crate) fn gather_u64_checked_owned(
                &self,
                mut idx: Vec<u64>,
                rowlen: usize,
            ) -> Option<Prim> {
                // A List invariant guarantees the flattened one-row leaf has exactly `rowlen`
                // elements. Identity reuse and checked indexing both rely on that correspondence.
                debug_assert_eq!(self.len(), rowlen, "gather: bounds/leaf length mismatch");
                let identity = idx.len() == rowlen
                    && (rowlen == 0
                        || (idx[0] == 0
                            && idx.iter().enumerate().all(|(i, &x)| x == i as u64)));
                if identity {
                    return Some(self.clone());
                }
                if let Prim::U64(v) = self {
                    for x in idx.iter_mut() {
                        if *x >= rowlen as u64 {
                            return None;
                        }
                        *x = v[*x as usize];
                    }
                    return Some(Prim::U64(Arc::new(idx)));
                }
                (!idx.iter().any(|&x| x >= rowlen as u64))
                    .then(|| self.gather_u64_owned(idx))
            }

            /// lane-wise min (`take_max=false`) or max (`true`) of two same-width columns, KIND-BLIND:
            /// the leaf is stored order-preserving (unsigned native, signed/float swizzled), so byte
            /// min/max IS value min/max for every kind — no deswizzle. An order op, hence `cmp`'s, not
            /// arithmetic's. (The `cmp` analogue of `rel`: same kind-blindness, picks a value not a mask.)
            /// CONSUMES both operands and writes in place into whichever is uniquely owned (same
            /// opportunistic reuse as arithmetic's `bin_into`; min/max is a same-width elementwise binary
            /// like add/sub/mul, so it shares that path); only when both are shared do we allocate.
            pub(crate) fn lane_pick(self, other: Prim, take_max: bool) -> Prim {
                match (self, other) {
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
                    _ => panic!("min/max: prim width mismatch"),
                }
            }

            /// lane-wise min (or max, with `take_max`) against the constant `c`, given as stored
            /// bits that fit this width. Kind-blind, like `lane_pick`; in place when uniquely owned.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn pick_imm(self, c: u64, take_max: bool) -> Prim {
                match self {
                    $( Prim::$V(mut a) => {
                        let c = c as $t;
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

            /// lane-wise blend of two same-width columns by a 0/1 selector: `out[i]` is `self[i]`
            /// where `pick[i]` is nonzero, else `other[i]`. KIND-BLIND — it moves stored bytes and
            /// never interprets them — and BRANCHLESS: the lane body is an unconditional select, so
            /// it vectorizes, where reading the chosen side through an index would not. The leaf of
            /// [`crate::engine::blend`]. CONSUMES both and writes into whichever is uniquely owned
            /// (the `lane_pick` reuse policy: same width, elementwise, so the shape allows it).
            pub(crate) fn blend(self, other: Prim, pick: &[u64]) -> Prim {
                match (self, other) {
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
                    _ => panic!("select: prim width mismatch"),
                }
            }

            /// XOR the top (sign) bit of every element, at this width — the order-preserving signed
            /// swizzle (`enc_i64` generalized), an involution. Converts an unsigned column to the
            /// signed encoding of the same non-negative values and back; the numeric layer's `signed`.
            /// CONSUMES self and rewrites in place when uniquely owned (same elementwise/same-width
            /// shape as `bin_into`/`neg_into`/`lane_pick` — reuse where we can; see the policy note).
            pub(crate) fn xor_signbit(self) -> Prim {
                match self {
                    $( Prim::$V(mut v) => {
                        let m = !(<$t>::MAX >> 1);
                        Prim::$V(if let Some(dst) = Arc::get_mut(&mut v) {
                            for x in dst.iter_mut() { *x ^= m; }
                            v
                        } else {
                            Arc::new(v.iter().map(|&x| x ^ m).collect())
                        })
                    } )+
                }
            }

            /// overwrite rows `pos[i]` of `self` with `src`'s row `i`, in place: `make_mut` gives the
            /// buffer mutably when uniquely owned (the common case), or copies it once if shared.
            pub(crate) fn scatter_into(&mut self, pos: &[usize], src: &Prim) {
                match (self, src) {
                    $( (Prim::$V(dst), Prim::$V(s)) => {
                        let dst = Arc::make_mut(dst);
                        for (&p, &x) in pos.iter().zip(s.iter()) { dst[p] = x; }
                    } )+
                    _ => panic!("scatter_into: prim width mismatch"),
                }
            }

            /// rows `c..` moved out into a new leaf, leaving rows `..c` — the leaf of
            /// [`Value::split_off`] (see [`split_arc`] for when this copies).
            pub(crate) fn split_off(&mut self, c: usize) -> Prim {
                match self { $( Prim::$V(v) => Prim::$V(split_arc(v, c)), )+ }
            }

            /// multi-source gather: result row `k` is element `off[k]` of source `srcs[tags[k]]` (all
            /// same width). The leaf of [`crate::engine::gather_lanes`]; `gather` is the 1-source case.
            /// The tags may be a `Sum`'s own `u8` discriminants, read in place, or any `usize` column.
            pub(crate) fn gather_lanes<T: Copy>(srcs: &[&Prim], tags: &[T], off: &[usize]) -> Prim
            where
                usize: From<T>,
            {
                match srcs[0] {
                    $( Prim::$V(_) => {
                        let cols: Vec<&[$t]> = srcs.iter().map(|s| match s {
                            Prim::$V(v) => v.as_slice(),
                            _ => panic!("gather_lanes: prim width mismatch"),
                        }).collect();
                        Prim::$V(Arc::new(tags.iter().zip(off).map(|(&t, &o)| cols[usize::from(t)][o]).collect()))
                    } )+
                }
            }

            /// the rows `index[..]` widened to `u64`, appended to `out` — the one indirect read the
            /// indexed sort makes; every pass after it is sequential.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn pull_u64(&self, index: &[usize], out: &mut Vec<u64>) {
                match self { $( Prim::$V(v) => out.extend(index.iter().map(|&i| v[i] as u64)), )+ }
            }

            /// a leaf of this width holding `keys`, narrowed: the sorted keys are the sorted column.
            pub(crate) fn like(&self, keys: &[u64]) -> Prim {
                self.like_from(keys.iter().copied())
            }

            /// `keys[q] = (keys[q] << width) | self[index[q]]`: this leaf's rows packed below the
            /// keys already there, at the leaf's declared width.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn pack_u64(&self, index: &[usize], keys: &mut [u64]) {
                match self {
                    $( Prim::$V(v) => {
                        let bits = (std::mem::size_of::<$t>() * 8) as u32;
                        for (k, &i) in keys.iter_mut().zip(index) { *k = (*k << bits) | v[i] as u64; }
                    } )+
                }
            }

            /// a leaf of this width holding the keys `it` yields, narrowed.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn like_from(&self, it: impl Iterator<Item = u64>) -> Prim {
                match self { $( Prim::$V(_) => Prim::$V(Arc::new(it.map(|k| k as $t).collect())), )+ }
            }
            /// stable per-element hash: each element WIDENED to u64 (zero-extend) and mixed (splitmix64
            /// finalizer). The leaf of [`crate::hash::hash`]; reads the stored bytes only, so it is
            /// KIND-BLIND and — for the raw/unsigned reading — WIDTH-BLIND: `u8` 5 and `u64` 5 both
            /// hash `mix64(5)`, since the widen collapses them (so a narrowing/widening for storage is
            /// id-preserving). Signed/float store a WIDTH-DEPENDENT order-preserving encoding, so
            /// cross-width identity is NOT promised for those kinds; see [`crate::hash`].
            pub(crate) fn hashes(&self) -> Vec<u64> {
                match self {
                    $( Prim::$V(v) => v.iter().map(|&x| crate::hash::mix64(x as u64)).collect(), )+
                }
            }

            /// Fold leaf hashes into an existing structural accumulator without materializing
            /// a temporary hash column. The leaf encoding and hash are identical to `hashes`.
            pub(crate) fn fold_hashes(&self, acc: &mut [u64], mut fold: impl FnMut(u64, u64) -> u64) {
                match self {
                    $( Prim::$V(v) => {
                        for (a, &x) in acc.iter_mut().zip(v.iter()) {
                            *a = fold(*a, crate::hash::mix64(x as u64));
                        }
                    } )+
                }
            }

            /// structural order of paired records: `out[k]` = sign of `self[ia[k]]` vs `other[ib[k]]`
            /// (`-1`/`0`/`+1`, as `Ordering as i8`). Reads through the indices, so gather-bound and scalar
            /// on NEON; the dense column-vs-column compare is [`Prim::rel`].
            pub(crate) fn cmp_idx(&self, ia: &[usize], ib: &[usize], other: &Prim) -> Vec<i8> {
                match (self, other) {
                    $( (Prim::$V(a), Prim::$V(b)) =>
                        ia.iter().zip(ib).map(|(&i, &j)| (a[i] > b[j]) as i8 - (a[i] < b[j]) as i8).collect(), )+
                    _ => panic!("cmp_idx: prim width mismatch"),
                }
            }

            /// structural order of DENSE pairs: `out[k]` = sign of `self[k]` vs `other[k + skew]`,
            /// for `n` pairs. The implicit-index leaf compare — `skew` 0 is the diagonal (row i vs
            /// row i) and `skew` 1 the adjacent (row k vs row k+1). Both sides are read
            /// sequentially, so this vectorizes where [`Prim::cmp_idx`] is two gathers per lane.
            pub(crate) fn cmp_dense(&self, other: &Prim, n: usize, skew: usize) -> Vec<i8> {
                match (self, other) {
                    $( (Prim::$V(a), Prim::$V(b)) => (0..n)
                        .map(|k| {
                            let (x, y) = (a[k], b[k + skew]);
                            (x > y) as i8 - (x < y) as i8
                        })
                        .collect(), )+
                    _ => panic!("cmp_dense: prim width mismatch"),
                }
            }

            /// `rel` against the constant `c` (stored bits that fit this width): the mask of rows
            /// whose comparison with `c` lands in the chosen order flags.
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn rel_imm(&self, c: u64, lt: bool, eq: bool, gt: bool) -> Vec<u64> {
                match self {
                    // one loop per predicate: a single compare per lane, where folding the three
                    // order flags into the body measured ~20% slower on a `gt` filter.
                    $( Prim::$V(a) => {
                        let y = c as $t;
                        match (lt, eq, gt) {
                            (true, false, false) => a.iter().map(|&x| (x < y) as u64).collect(),
                            (true, true, false) => a.iter().map(|&x| (x <= y) as u64).collect(),
                            (false, true, false) => a.iter().map(|&x| (x == y) as u64).collect(),
                            (true, false, true) => a.iter().map(|&x| (x != y) as u64).collect(),
                            (false, false, true) => a.iter().map(|&x| (x > y) as u64).collect(),
                            (false, true, true) => a.iter().map(|&x| (x >= y) as u64).collect(),
                            // no flag or every flag: the predicate is constant
                            (all, _, _) => vec![all as u64; a.len()],
                        }
                    } )+
                }
            }

            /// lane-wise relational compare of two same-width columns → a 0/1 mask. Kind-blind: reads the
            /// stored bytes, correct for unsigned and order-preserving swizzled signed alike. The three
            /// order-flags arrive pre-resolved (`lt`/`eq`/`gt`), so the lane body is branchless and vectorizes.
            pub(crate) fn rel(&self, other: &Prim, lt: bool, eq: bool, gt: bool) -> Vec<u64> {
                match (self, other) {
                    $( (Prim::$V(a), Prim::$V(b)) => a.iter().zip(b.iter())
                        .map(|(x, y)| ((lt & (x < y)) | (eq & (x == y)) | (gt & (x > y))) as u64)
                        .collect(), )+
                    _ => panic!("rel: prim width mismatch"),
                }
            }

            /// append same-width leaves end to end. Test-only: the leaf of `engine::concat`, the
            /// `gather_lanes` reference oracle (no production path concatenates leaves).
            #[cfg(test)]
            pub(crate) fn concat(parts: &[&Prim]) -> Prim {
                match parts[0] {
                    $( Prim::$V(_) => {
                        let mut o = Vec::new();
                        for &p in parts {
                            match p {
                                Prim::$V(x) => o.extend_from_slice(x),
                                _ => panic!("concat: prim width mismatch"),
                            }
                        }
                        Prim::$V(Arc::new(o))
                    } )+
                }
            }

            fn show(&self) -> String {
                match self { $( Prim::$V(xs) => format!("{xs:?}"), )+ }
            }
        }
    };
}

prim! {
    U8 => u8,
    U16 => u16,
    U32 => u32,
    U64 => u64,
}

/// `Vec::split_off` for a shared buffer: entries `c..` move to a new buffer and `v` keeps `..c`.
/// A uniquely owned `v` is truncated in place and stays owned, so a later op can still write into
/// it; a shared one is copied (both halves). Splitting at either end moves nothing: at `len` the
/// tail is empty, at 0 the whole buffer is handed over as the tail.
fn split_arc<T: Clone>(v: &mut Arc<Vec<T>>, c: usize) -> Arc<Vec<T>> {
    if c == v.len() {
        return Arc::new(Vec::new());
    }
    if c == 0 {
        return std::mem::replace(v, Arc::new(Vec::new()));
    }
    match Arc::get_mut(v) {
        Some(owned) => Arc::new(owned.split_off(c)),
        None => {
            let tail = Arc::new(v[c..].to_vec());
            *v = Arc::new(v[..c].to_vec());
            tail
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
    pub fn  u8(xs: Vec<u8 >) -> Value { Value::Prim(Prim::U8(Arc::new(xs))) }
    pub fn u16(xs: Vec<u16>) -> Value { Value::Prim(Prim::U16(Arc::new(xs))) }
    pub fn u32(xs: Vec<u32>) -> Value { Value::Prim(Prim::U32(Arc::new(xs))) }
    pub fn u64(xs: Vec<u64>) -> Value { Value::Prim(Prim::U64(Arc::new(xs))) }

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
            Shape::Prim(w) => Value::Prim(Prim::empty(*w)),
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

    /// rows `c..` moved out into a new column, leaving rows `..c` — `Vec::split_off` for a column,
    /// through every shape. A `List` splits its payload where row `c` starts, a `Sum` each lane where
    /// its kept rows end, a `Ref` its spans (both halves keep the arena). A uniquely owned buffer is
    /// truncated in place and stays owned, so an op that writes in place still can; a shared one is
    /// copied. Otherwise the work is the tail's size, not the column's.
    pub(crate) fn split_off(&mut self, c: usize) -> Value {
        debug_assert!(c <= self.len(), "split_off: row {c} of {}", self.len());
        match self {
            Value::Prim(p) => Value::Prim(p.split_off(c)),
            Value::Prod(fields) => Value::Prod(fields.iter_mut().map(|f| f.split_off(c)).collect()),
            Value::Sum(tags, lanes) => {
                let lens: Vec<usize> = lanes.iter().map(Value::len).collect();
                let (tail, keep) = tags.split_off(c, &lens);
                Value::Sum(tail, lanes.iter_mut().zip(keep).map(|(lane, k)| lane.split_off(k)).collect())
            }
            Value::List(bounds, vals) => {
                let (tail, at) = bounds.split_off(c);
                Value::List(tail, Box::new(vals.split_off(at)))
            }
            Value::Unit(n) => {
                let tail = Value::Unit(*n - c);
                *n = c;
                tail
            }
            Value::Ref(payload, spans) => Value::Ref(payload.clone(), split_arc(spans, c)),
        }
    }
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

    /// borrow the leaf as a `u64` slice — for an op that only READS its operand.
    ///
    /// `into_u64` forces ownership, and ownership is a full column COPY whenever anyone else still
    /// holds the buffer: a graph node with fan-out 2, or a caller that keeps its input. Measured on
    /// a one-pass `fold_add` at 1M rows, that copy was 7.9x the whole operation. Reading needs none
    /// of it; only an op that rewrites its operand in place (`Shr`, `And`, `Scan`) has to
    /// consume it.
    pub fn as_u64(&self, who: &str) -> Result<&[u64], String> {
        match self {
            Value::Prim(Prim::U64(xs)) => Ok(&xs[..]),
            other => Err(format!("{who}: expected U64, got {}", shape_of_value(other))),
        }
    }

    /// borrow the leaf as a `u8` slice — the byte-column sibling of [`Value::as_u64`].
    pub fn as_u8(&self, who: &str) -> Result<&[u8], String> {
        match self {
            Value::Prim(Prim::U8(xs)) => Ok(&xs[..]),
            other => Err(format!("{who}: expected U8, got {}", shape_of_value(other))),
        }
    }

    /// take the leaf's `u64` buffer — for an op that REWRITES its operand in place. Moves the
    /// buffer out at refcount 1, and copies it when shared; see [`Value::as_u64`], which most
    /// callers want instead.
    pub fn into_u64(self, who: &str) -> Result<Vec<u64>, String> {
        match self {
            // move the buffer out if this is the last holder, else clone (shared leaf).
            Value::Prim(Prim::U64(xs)) => Ok(Arc::try_unwrap(xs).unwrap_or_else(|a| (*a).clone())),
            other => Err(format!("{who}: expected U64, got {}", shape_of_value(&other))),
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
