//! The columnar value model: a `Value` is a whole column (a SEQ). Every operator
//! is a single `T0 -> T1` on one element, lifted 1:1 across the column; all
//! cardinality change lives *inside* a `List`.

use crate::shape::{shape_of_value, Shape};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Prim(Prim),                   // a leaf column at one byte width
    Prod(Vec<Value>),             // parallel columns, equal length
    Sum(Prim, Vec<usize>, Vec<Value>), // discriminant (u8 leaf) + within-variant offset per row + one
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
    Offsets(Vec<usize>),  // end offset of each row
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
    pub(crate) fn to_vec(&self) -> Vec<usize> {
        match self {
            Bounds::Offsets(v) => v.clone(),
            Bounds::Stride(..) => self.ends().collect(),
        }
    }
}

impl From<Vec<usize>> for Bounds {
    fn from(v: Vec<usize>) -> Self {
        // One O(n) uniformity check at construction: a uniform partition becomes a `Stride`,
        // so `strided()` recovers the array kernels downstream at every `.into()` site for
        // free. Equality/hash are by the partition, so this is representation-invisible.
        if let Some(&last) = v.last() {
            let n = v.len();
            if last % n == 0 {
                let k = last / n;
                if v.iter().enumerate().all(|(i, &e)| e == (i + 1) * k) {
                    return Bounds::Stride(k, n);
                }
            }
        }
        Bounds::Offsets(v)
    }
}

// equality/hash are by the PARTITION, so a `Stride` and the equivalent `Offsets` compare and hash equal.
impl PartialEq for Bounds {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Bounds::Offsets(a), Bounds::Offsets(b)) => a == b,
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

            /// the whole column as `usize`s (for small-int columns like Sum discriminants).
            pub(crate) fn usize_vec(&self) -> Vec<usize> {
                match self { $( Prim::$V(v) => v.iter().map(|&x| x as usize).collect(), )+ }
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
fn within_offsets(tags: &[usize], k: usize) -> Vec<usize> {
    let mut cursor = vec![0usize; k];
    tags.iter().map(|&t| { let p = cursor[t]; cursor[t] += 1; p }).collect()
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
        // tags are stored as a u8 discriminant, so the variant count must fit a u8 — else `t as u8`
        // would silently truncate a tag onto the wrong lane.
        assert!(variants.len() <= 256, "Value::sum: {} variants exceeds the u8 tag width", variants.len());
        let offset = within_offsets(&tags, variants.len());
        let tags = Prim::U8(Arc::new(tags.iter().map(|&t| t as u8).collect()));
        Value::Sum(tags, offset, variants)
    }

    /// a Sum from an existing tag column and its lanes; the within-variant offset is derived from
    /// the tags. For ops that already hold the tags as a `Prim` (`gather`/`concat`).
    pub(crate) fn sum_from_prim(tags: Prim, lanes: Vec<Value>) -> Value {
        let offset = within_offsets(&tags.usize_vec(), lanes.len());
        Value::Sum(tags, offset, lanes)
    }

    /// a zero-row value of the given shape — the all-empty witness of each constructor. `Inject`
    /// fills the lanes it does not carry with this; the recursion mirrors `shape_of_value` inverted.
    pub fn empty(shape: &Shape) -> Value {
        match shape {
            Shape::Prim(w) => Value::Prim(Prim::empty(*w)),
            Shape::Prod(ss) => Value::Prod(ss.iter().map(Value::empty).collect()),
            Shape::Sum(ss) => {
                Value::sum_from_prim(Prim::U8(Arc::new(Vec::new())), ss.iter().map(Value::empty).collect())
            }
            Shape::List(s) => Value::List(Bounds::Offsets(Vec::new()), Box::new(Value::empty(s))),
            Shape::Unit => Value::Unit(0),
        }
    }

    /// SEQ length: how many rows this column holds.
    pub fn len(&self) -> usize {
        match self {
            Value::Prim(p) => p.len(),
            Value::Prod(c) => c.first().map_or(0, |c| c.len()),
            Value::Sum(t, _, _) => t.len(),
            Value::List(b, _) => b.len(),
            Value::Unit(n) => *n,
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// a `Sum` taken apart: (tags as usize, within-variant offsets, lanes).
pub type SumParts = (Vec<usize>, Vec<usize>, Vec<Value>);

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
            Value::Sum(tags, offset, variants) => Ok((tags.usize_vec(), offset, variants)),
            other => Err(format!("{who}: expected a sum, got {}", shape_of_value(&other))),
        }
    }

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
        Value::Sum(t, _, vs) => {
            let lanes: Vec<String> = vs.iter().map(show).collect();
            format!("Sum tags={:?} [{}]", t.usize_vec(), lanes.join(", "))
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

    /// Narrowing then widening back is `mod 2^bits` — the documented truncating semantics, not a
    /// round trip. Pinned so a future "make cast lossless" change has to face the corpus.
    #[test]
    fn narrow_then_widen_truncates() {
        let wide = Prim::U64(Arc::new(vec![0x1_0000, 0x1_0001]));
        assert_eq!(wide.cast(16).cast(64), Prim::U64(Arc::new(vec![0, 1])));
    }
}
