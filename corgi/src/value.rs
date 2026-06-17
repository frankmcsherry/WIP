//! The columnar value model: a `Value` is a whole column (a SEQ). Every operator
//! is a single `T0 -> T1` on one element, lifted 1:1 across the column; all
//! cardinality change lives *inside* a `List`.

use crate::shape::Shape;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Prim(Prim),                   // a leaf column at one byte width
    Prod(Vec<Value>),             // parallel columns, equal length
    Sum(Prim, Vec<usize>, Vec<Option<Value>>), // discriminant (u8 leaf) + within-variant offset per row +
                                  // one lane per variant; a `None` lane is `⊥` — uncommitted, holds no rows
                                  // (no row carries its tag) and adopts a sibling's shape at a merge.
    List(Vec<usize>, Box<Value>), // end-offset per row; flattened values
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
            #[allow(clippy::unnecessary_cast)]
            pub(crate) fn cast(&self, bits: u32) -> Prim {
                let wide: Vec<u64> = match self {
                    $( Prim::$V(v) => v.iter().map(|&x| x as u64).collect(), )+
                };
                match bits {
                    $( b if b == (std::mem::size_of::<$t>() * 8) as u32 =>
                        Prim::$V(Arc::new(wide.iter().map(|&x| x as $t).collect())), )+
                    _ => panic!("cast: unsupported width {bits}"),
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

            /// XOR the top (sign) bit of every element, at this width — the order-preserving signed
            /// swizzle (`enc_i64` generalized), an involution. Converts an unsigned column to the
            /// signed encoding of the same non-negative values and back; the numeric layer's `signed`.
            pub(crate) fn xor_signbit(&self) -> Prim {
                match self {
                    $( Prim::$V(v) => {
                        let m = !(<$t>::MAX >> 1);
                        Prim::$V(Arc::new(v.iter().map(|&x| x ^ m).collect()))
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

            /// stable LSD byte-radix: `indices` reordered by leaf value. A counting sort
            /// per *significant* byte (high all-zero bytes skipped), so narrow values and
            /// narrow widths are cheap — a `u8` column is one counting pass.
            pub(crate) fn sort_block(&self, indices: &[usize]) -> Vec<usize> {
                match self {
                    $( Prim::$V(v) => {
                        let mut cur = indices.to_vec();
                        if cur.len() <= 1 {
                            return cur;
                        }
                        let max = cur.iter().map(|&i| v[i]).max().unwrap_or(0);
                        let bits = std::mem::size_of::<$t>() * 8;
                        let nbytes = (bits - max.leading_zeros() as usize).div_ceil(8);
                        let mut tmp = vec![0usize; cur.len()];
                        for byte in 0..nbytes {
                            let shift = (byte * 8) as u32;
                            let mut counts = [0usize; 256];
                            for &i in &cur {
                                counts[((v[i] >> shift) & 0xff) as usize] += 1;
                            }
                            let mut start = 0;
                            for c in counts.iter_mut() {
                                let n = *c;
                                *c = start;
                                start += n;
                            }
                            for &i in &cur {
                                let b = ((v[i] >> shift) & 0xff) as usize;
                                tmp[counts[b]] = i;
                                counts[b] += 1;
                            }
                            std::mem::swap(&mut cur, &mut tmp);
                        }
                        cur
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
    /// per-variant columns. The one place tags cross from `usize` into the `Prim` fold. The
    /// within-variant offset is computed here and carried, so comparison/search read it instead of
    /// re-deriving each row's rank.
    pub fn sum(tags: Vec<usize>, variants: Vec<Value>) -> Value {
        // every lane committed — the common case (branch, tests). `⊥` lanes go through `sum_opt`.
        Self::sum_opt(tags, variants.into_iter().map(Some).collect())
    }

    /// a Sum whose lanes may be `⊥` (`None`) — the `Inject` form, and the result shape of any merge.
    pub fn sum_opt(tags: Vec<usize>, lanes: Vec<Option<Value>>) -> Value {
        // tags are stored as a u8 discriminant, so the variant count must fit a u8 — else `t as u8`
        // would silently truncate a tag onto the wrong lane.
        assert!(lanes.len() <= 256, "Value::sum: {} variants exceeds the u8 tag width", lanes.len());
        let offset = within_offsets(&tags, lanes.len());
        let tags = Prim::U8(Arc::new(tags.iter().map(|&t| t as u8).collect()));
        Value::Sum(tags, offset, lanes)
    }

    /// a Sum from an existing tag column and its (possibly-`⊥`) lanes; the within-variant offset is
    /// derived from the tags. For ops that already hold the tags as a `Prim` (`gather`/`concat`).
    pub(crate) fn sum_from_prim(tags: Prim, lanes: Vec<Option<Value>>) -> Value {
        let offset = within_offsets(&tags.usize_vec(), lanes.len());
        Value::Sum(tags, offset, lanes)
    }

    /// a zero-row value of the given shape — the all-empty witness of each constructor. `gather_lanes`
    /// fills its `⊥` source slots with this; the recursion mirrors `shape_of_value` inverted.
    pub fn empty(shape: &Shape) -> Value {
        match shape {
            Shape::Prim(w) => Value::Prim(Prim::empty(*w)),
            Shape::Prod(ss) => Value::Prod(ss.iter().map(Value::empty).collect()),
            Shape::Sum(ss) => Value::sum_from_prim(
                Prim::U8(Arc::new(Vec::new())),
                ss.iter().map(|o| o.as_ref().map(Value::empty)).collect(),
            ),
            Shape::List(s) => Value::List(Vec::new(), Box::new(Value::empty(s))),
        }
    }

    /// SEQ length: how many rows this column holds.
    pub fn len(&self) -> usize {
        match self {
            Value::Prim(p) => p.len(),
            Value::Prod(c) => c.first().map_or(0, |c| c.len()),
            Value::Sum(t, _, _) => t.len(),
            Value::List(b, _) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

// input accessors: destructure a `Value` to the shape an op expects (`judge` already proved
// it, so a mismatch is a panic). `into_*` consume `self` and move the buffers out.
impl Value {
    pub fn into_pair(self, who: &str) -> (Value, Value) {
        match self {
            Value::Prod(mut cols) if cols.len() == 2 => {
                let b = cols.pop().unwrap();
                let a = cols.pop().unwrap();
                (a, b)
            }
            _ => panic!("{who}: expected a pair"),
        }
    }

    pub fn into_prod(self, who: &str) -> Vec<Value> {
        match self {
            Value::Prod(cols) => cols,
            _ => panic!("{who}: expected a product"),
        }
    }

    pub fn into_list(self, who: &str) -> (Vec<usize>, Value) {
        match self {
            Value::List(bounds, vals) => (bounds, *vals),
            _ => panic!("{who}: expected a list"),
        }
    }

    pub fn into_sum(self, who: &str) -> (Vec<usize>, Vec<usize>, Vec<Option<Value>>) {
        match self {
            Value::Sum(tags, offset, variants) => (tags.usize_vec(), offset, variants),
            _ => panic!("{who}: expected a sum"),
        }
    }

    pub fn into_u64(self, who: &str) -> Vec<u64> {
        match self {
            // move the buffer out if this is the last holder, else clone (shared leaf).
            Value::Prim(Prim::U64(xs)) => Arc::try_unwrap(xs).unwrap_or_else(|a| (*a).clone()),
            _ => panic!("{who}: expected U64"),
        }
    }

    pub fn into_u8(self, who: &str) -> Vec<u8> {
        match self {
            Value::Prim(Prim::U8(xs)) => Arc::try_unwrap(xs).unwrap_or_else(|a| (*a).clone()),
            _ => panic!("{who}: expected U8"),
        }
    }

    pub fn into_prim(self, who: &str) -> Prim {
        match self {
            Value::Prim(p) => p,
            _ => panic!("{who}: expected a leaf"),
        }
    }
}

/// human-readable rendering used by tests and demos.
pub fn show(v: &Value) -> String {
    match v {
        Value::Prim(p) => p.show(),
        Value::Prod(c) => format!("({})", c.iter().map(show).collect::<Vec<_>>().join(", ")),
        Value::Sum(t, _, vs) => {
            let lanes: Vec<String> = vs.iter().map(|o| o.as_ref().map_or("⊥".to_string(), show)).collect();
            format!("Sum tags={:?} [{}]", t.usize_vec(), lanes.join(", "))
        }
        Value::List(b, vals) => format!("List ends={:?} <{}>", b, show(vals)),
    }
}
