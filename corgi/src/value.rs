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
            // same list, which is exactly what `Zip` and `Filter` assert about their operands.
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
    /// every one of `rows` rows carries `tag` — the uniform assignment, stored in two words.
    pub fn constant(tag: usize, rows: usize) -> Tags {
        Tags::Const(tag, rows)
    }

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
    pub fn const_tag(&self) -> Option<usize> {
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
    pub fn tags_iter(&self) -> impl Iterator<Item = usize> + '_ {
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

            /// A U64 index column is also correctly typed storage for a U64 gather result. Rewrite
            /// that owned buffer in place; other haystack widths allocate their native vector. The
            /// raw caller deliberately materializes even an identity gather rather than adding an
            /// identity-detection scan to its single indexing pass.
            pub(crate) fn gather_u64_owned(&self, mut idx: Vec<u64>) -> Prim {
                if let Prim::U64(v) = self {
                    for x in idx.iter_mut() {
                        let i = *x;
                        *x = *v.get(i as usize).unwrap_or_else(|| {
                            panic!("Gather: index {i} out of row 0's bounds")
                        });
                    }
                    Prim::U64(Arc::new(idx))
                } else {
                    match self {
                        $( Prim::$V(v) => Prim::$V(Arc::new(
                            idx.iter().map(|&i| *v.get(i as usize).unwrap_or_else(|| {
                                panic!("Gather: index {i} out of row 0's bounds")
                            })).collect()
                        )), )+
                    }
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

            /// overwrite rows `active[p]` of `self` with `src`'s row `p`, IN PLACE — `make_mut` gives the
            /// buffer mutably when uniquely owned (the common case), or clones it once if shared. Touches
            /// only the `active` rows; no allocation in the unique case. The leaf of [`scatter`].
            pub(crate) fn scatter_into(&mut self, active: &[usize], src: &Prim) {
                match (self, src) {
                    $( (Prim::$V(dst), Prim::$V(s)) => {
                        let dst = Arc::make_mut(dst);
                        for (p, &r) in active.iter().enumerate() { dst[r] = s[p]; }
                    } )+
                    _ => panic!("scatter_into: prim width mismatch"),
                }
            }

            /// multi-source gather: result row `k` is element `off[k]` of source `srcs[tags[k]]` (all
            /// same width). The leaf of [`crate::engine::gather_lanes`]; `gather` is the 1-source case.
            pub(crate) fn gather_lanes(srcs: &[&Prim], tags: &[usize], off: &[usize]) -> Prim {
                match srcs[0] {
                    $( Prim::$V(_) => {
                        let cols: Vec<&[$t]> = srcs.iter().map(|s| match s {
                            Prim::$V(v) => v.as_slice(),
                            _ => panic!("gather_lanes: prim width mismatch"),
                        }).collect();
                        Prim::$V(Arc::new(tags.iter().zip(off).map(|(&t, &o)| cols[t][o]).collect()))
                    } )+
                }
            }

            /// stable LSD byte-radix over a mutable index slice, IN PLACE (`tmp` is caller
            /// scratch, resized as needed and reusable across calls — a refinement pass calls
            /// this once per block, and per-block allocations dominated a join-heavy profile).
            /// A counting sort per *significant* byte (high all-zero bytes skipped); blocks of
            /// <= 16 take a stable insertion sort instead — the radix set-up dwarfs tiny
            /// blocks, the common case once a prior pass has split the column into groups.
            pub(crate) fn sort_block_scratch(&self, idx: &mut [usize], tmp: &mut Vec<usize>) {
                match self {
                    $( Prim::$V(v) => {
                        let n = idx.len();
                        if n <= 1 {
                            return;
                        }
                        if n <= 16 {
                            for k in 1..n {
                                let mut j = k;
                                while j > 0 && v[idx[j - 1]] > v[idx[j]] {
                                    idx.swap(j - 1, j);
                                    j -= 1;
                                }
                            }
                            return;
                        }
                        let max = idx.iter().map(|&i| v[i]).max().unwrap_or(0);
                        let bits = std::mem::size_of::<$t>() * 8;
                        let nbytes = (bits - max.leading_zeros() as usize).div_ceil(8);
                        if tmp.len() < n {
                            tmp.resize(n, 0);
                        }
                        let mut src_is_idx = true;
                        for byte in 0..nbytes {
                            let shift = (byte * 8) as u32;
                            let mut counts = [0usize; 256];
                            {
                                let src: &[usize] = if src_is_idx { &idx[..] } else { &tmp[..n] };
                                for &i in src {
                                    counts[((v[i] >> shift) & 0xff) as usize] += 1;
                                }
                            }
                            let mut start = 0;
                            for c in counts.iter_mut() {
                                let cnt = *c;
                                *c = start;
                                start += cnt;
                            }
                            if src_is_idx {
                                for k in 0..n {
                                    let i = idx[k];
                                    let b = ((v[i] >> shift) & 0xff) as usize;
                                    tmp[counts[b]] = i;
                                    counts[b] += 1;
                                }
                            } else {
                                for k in 0..n {
                                    let i = tmp[k];
                                    let b = ((v[i] >> shift) & 0xff) as usize;
                                    idx[counts[b]] = i;
                                    counts[b] += 1;
                                }
                            }
                            src_is_idx = !src_is_idx;
                        }
                        if !src_is_idx {
                            idx.copy_from_slice(&tmp[..n]);
                        }
                    } )+
                }
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
                Value::Sum(Tags::constant(0, 0), ss.iter().map(Value::empty).collect())
            }
            Shape::List(s) => Value::List(Bounds::offsets(Vec::new()), Box::new(Value::empty(s))),
            Shape::Unit => Value::Unit(0),
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
    pub fn into_pair(self, who: &str) -> Result<(Value, Value), String> {
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
    /// of it; only an op that rewrites its operand in place (`AddU64`, `Shr`, `And`, `Scan`) has to
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

    pub fn into_u8(self, who: &str) -> Result<Vec<u8>, String> {
        match self {
            Value::Prim(Prim::U8(xs)) => Ok(Arc::try_unwrap(xs).unwrap_or_else(|a| (*a).clone())),
            other => Err(format!("{who}: expected U8, got {}", shape_of_value(&other))),
        }
    }

    pub fn into_prim(self, who: &str) -> Result<Prim, String> {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `cast` to the width a leaf already has is the identity, and must not copy the column: the
    /// stored bytes ARE the result's bytes, so the result shares the buffer (an `Arc` bump).
    #[test]
    fn identity_cast_reuses_the_buffer() {
        let xs = Arc::new(vec![10u32, 20, 30]);
        let p = Prim::U32(xs.clone());
        let Prim::U32(out) = p.cast(32) else { panic!("cast(32) must stay a U32 leaf") };
        assert!(Arc::ptr_eq(&out, &xs), "same-width cast copied the column");
    }

    /// A genuine re-width keeps the low bytes (narrowing) or zero-extends (widening), for every
    /// (source, destination) pair the `prim!` grid generates.
    #[test]
    fn rewidth_keeps_the_low_bytes() {
        let wide = Prim::U64(Arc::new(vec![0x0102_0304_0506_0708, 0xff, 0x1_0000]));
        assert_eq!(wide.cast(8), Prim::U8(Arc::new(vec![0x08, 0xff, 0x00])));
        assert_eq!(wide.cast(16), Prim::U16(Arc::new(vec![0x0708, 0x00ff, 0x0000])));
        assert_eq!(wide.cast(32), Prim::U32(Arc::new(vec![0x0506_0708, 0xff, 0x1_0000])));

        let narrow = Prim::U8(Arc::new(vec![0, 1, 255]));
        assert_eq!(narrow.cast(16), Prim::U16(Arc::new(vec![0, 1, 255])));
        assert_eq!(narrow.cast(64), Prim::U64(Arc::new(vec![0, 1, 255])));
    }

    /// A `Value` clone must be a refcount bump, not a column copy: `eval_graph` clones at every
    /// shared edge. A `List`'s partition is the same size as a `u64` payload column, so a bare
    /// `Vec` here made a shared edge cost as much again as the data it carried.
    #[test]
    fn cloning_a_list_shares_its_partition() {
        let list = Value::List(Bounds::offsets(vec![1, 3, 6]), Box::new(Value::u64(vec![0; 6])));
        let copy = list.clone();
        let (Value::List(Bounds::Offsets(a), _), Value::List(Bounds::Offsets(b), _)) = (&list, &copy)
        else {
            panic!("expected two offset-partitioned lists")
        };
        assert!(Arc::ptr_eq(a, b), "clone copied the partition");
    }

    /// Equality is still by the PARTITION — the shared-buffer check is only a fast path, so two
    /// distinct buffers describing one partition stay equal, as do a `Stride` and its offsets.
    #[test]
    fn equality_is_by_partition_not_buffer() {
        assert_eq!(Bounds::offsets(vec![1, 3, 6]), Bounds::offsets(vec![1, 3, 6]));
        assert_eq!(Bounds::offsets(vec![2, 4, 6]), Bounds::Stride(2, 3));
        assert_ne!(Bounds::offsets(vec![1, 3, 6]), Bounds::offsets(vec![1, 3, 5]));
    }

    /// `inject` assigns every row one tag, so the assignment is two words: no discriminant column
    /// and no offset column, at any row count. This is the `Sum`-side twin of a uniform `Bounds`
    /// becoming a `Stride`, and it is the state a `Fail` column that has not failed stays in.
    #[test]
    fn one_tag_throughout_costs_no_columns() {
        let t = Tags::from_tags(vec![2, 2, 2, 2], 3);
        assert_eq!(t.const_tag(), Some(2));
        assert!(matches!(t, Tags::Const(2, 4)));
        // ...and every row still reads back the same as the column form would answer.
        assert_eq!(t.tags_iter().collect::<Vec<_>>(), vec![2, 2, 2, 2]);
        assert_eq!((0..4).map(|i| t.offset_at(i)).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    /// Equality and hash are by the ASSIGNMENT, so the two representations are interchangeable —
    /// the property that lets `Const` appear anywhere a `Column` would without being observable.
    #[test]
    fn const_and_column_assignments_agree() {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let konst = Tags::Const(1, 3);
        let column = Tags::Column(Prim::U8(Arc::new(vec![1, 1, 1])), Arc::new(vec![0, 1, 2]));
        assert_eq!(konst, column);
        let h = |t: &Tags| {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        };
        assert_eq!(h(&konst), h(&column));
        // a mixed assignment is not equal to either.
        assert_ne!(konst, Tags::from_tags(vec![1, 0, 1], 2));
    }

    /// Narrowing then widening back is `mod 2^bits` — the documented truncating semantics, not a
    /// round trip. Pinned so a future "make cast lossless" change has to face the corpus.
    #[test]
    fn narrow_then_widen_truncates() {
        let wide = Prim::U64(Arc::new(vec![0x1_0000, 0x1_0001]));
        assert_eq!(wide.cast(16).cast(64), Prim::U64(Arc::new(vec![0, 1])));
    }
}
