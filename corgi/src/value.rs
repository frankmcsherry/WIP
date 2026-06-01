//! The columnar value model: a `Value` is a whole column (a SEQ). Every operator
//! is a single `T0 -> T1` on one element, lifted 1:1 across the column; all
//! cardinality change lives *inside* a `List`.

use crate::shape::Shape;
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Prim(Prim),                        // a leaf column at one byte width
    Prod(Vec<Value>),                  // parallel columns, equal length
    Sum(Prim, Vec<usize>, Vec<Value>), // discriminant (u8 leaf) + within-variant offset per row + variants
    List(Vec<usize>, Box<Value>),      // end-offset per row; flattened values
}

/// a leaf column at one byte width, each width its own naturally-aligned `Vec<uN>` behind an
/// `Arc` (leaves are write-once read-many; the `eval` clones values freely for shared edges and
/// `tuple` formation, so a leaf clone must be a refcount bump, not a buffer copy). The `prim!`
/// macro lists the widths ONCE and generates the enum + every method, so adding a width is one
/// line here and the core's leaf arms stay one delegating line each (`Value::Prim(p) => p.method()`).
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

            /// structural order: record `i` of `self` vs record `j` of `other` (same width).
            pub(crate) fn cmp_at(&self, i: usize, other: &Prim, j: usize) -> Ordering {
                match (self, other) {
                    $( (Prim::$V(a), Prim::$V(b)) => a[i].cmp(&b[j]), )+
                    _ => panic!("cmp_at: prim width mismatch"),
                }
            }

            /// lane-wise relational compare of two same-width columns → a 0/1 mask (as `u64`s).
            /// Kind-blind: it reads the stored bytes (unsigned at the native width), which is correct
            /// for unsigned AND for order-preserving *swizzled* signed columns — the leaf analog of the
            /// discrimination order. The predicate arrives as `keep: Ordering -> bool` so this layer
            /// stays free of op-level concepts.
            pub(crate) fn rel(&self, other: &Prim, keep: impl Fn(Ordering) -> bool) -> Vec<u64> {
                match (self, other) {
                    $( (Prim::$V(a), Prim::$V(b)) =>
                        a.iter().zip(b.iter()).map(|(x, y)| keep(x.cmp(y)) as u64).collect(), )+
                    _ => panic!("rel: prim width mismatch"),
                }
            }

            /// append same-width leaves end to end.
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
/// one cursor pass. A `Sum` carries this (see [`Value::sum`]) so comparison/search read it instead of
/// re-deriving the rank; the ops that build sums (`gather`/`concat`/`Value::sum`) maintain it.
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
    /// re-deriving each row's rank (the cliff the bulk passes used to dodge by recomputing).
    pub fn sum(tags: Vec<usize>, variants: Vec<Value>) -> Value {
        // tags index the variants and are stored as a u8 discriminant, so the variant count must
        // fit a u8 — else `t as u8` would silently truncate a tag onto the wrong lane. A wider tag
        // leaf is the generalization (the width is derivable from the variant count), deferred until
        // a >256-variant shape actually appears.
        assert!(variants.len() <= 256, "Value::sum: {} variants exceeds the u8 tag width", variants.len());
        let offset = within_offsets(&tags, variants.len());
        let tags = Prim::U8(Arc::new(tags.iter().map(|&t| t as u8).collect()));
        Value::Sum(tags, offset, variants)
    }

    /// a Sum from an existing tag column; the within-variant offset is derived from it. For ops that
    /// already hold the tags as a `Prim` and the variant columns (`gather`/`concat`).
    pub(crate) fn sum_from_prim(tags: Prim, variants: Vec<Value>) -> Value {
        let offset = within_offsets(&tags.usize_vec(), variants.len());
        Value::Sum(tags, offset, variants)
    }

    /// a zero-row value of the given shape — the all-empty witness of each constructor. `Inject`
    /// builds its unselected variants with this; the recursion mirrors `shape_of_value` inverted.
    pub fn empty(shape: &Shape) -> Value {
        match shape {
            Shape::Prim(w) => Value::Prim(Prim::empty(*w)),
            Shape::Prod(ss) => Value::Prod(ss.iter().map(Value::empty).collect()),
            Shape::Sum(ss) => {
                Value::sum_from_prim(Prim::U8(Arc::new(Vec::new())), ss.iter().map(Value::empty).collect())
            }
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

    pub fn into_sum(self, who: &str) -> (Vec<usize>, Vec<usize>, Vec<Value>) {
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
            format!("Sum tags={:?} [{}]", t.usize_vec(), vs.iter().map(show).collect::<Vec<_>>().join(", "))
        }
        Value::List(b, vals) => format!("List ends={:?} <{}>", b, show(vals)),
    }
}
