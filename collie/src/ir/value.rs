//! Layer 1: the value representation.
//!
//! `Value` is the dynamic value model — the star of this file. The supporting
//! types (`Prim`, `BoundsRepr`, `Storage`) live in private submodules below
//! and are re-exported flat so external code uses `value::Prim` etc.
//!
//! Interpretation (i32 vs u32 vs f32) is *not* carried in the value — it
//! lives in operators. Width is.
//!
//! The value universe factors into two axes (per the agreed design):
//!
//!   Value = (PositionStorage, Content)
//!     Content := Prim | Prod | Sum | List          (closed, shape-bearing)
//!     PositionStorage := Identity | View {selector} (shape-transparent)
//!
//! Today both axes live in the same flat enum: the first four variants are
//! Content; `View` is the sole non-Identity PositionStorage. Per FOLLOWUPS
//! §11, Content stays closed — additions go on the storage axis. View
//! composes with itself via canonical-form collapsing in the smart
//! constructor (`view()`); operators that don't fast-path View call
//! `materialize()` and proceed against the unrestricted form.

use std::fmt;
use std::sync::Arc;

/// Colang's value universe. A closed inductive set of four content cases,
/// plus one storage-axis modifier (`View`):
///
/// - `Prim` — a flat column of width-tagged bytes (the leaf).
/// - `Prod` — a heterogeneous tuple; all fields share the same row count.
/// - `Sum`  — a tagged union; `disc[i]` selects which lane row `i` lives in.
/// - `List` — variable-length nesting; `bounds` carves the flat `values`
///            into rows.
/// - `View` — lazy-gather wrapper. Shape-transparent: `shape_of(View(s, _))
///            == shape_of(s)`. Outer access goes through `selector`.
///
/// All structural payloads (Prod's fields, Sum's lanes, List's inner, View's
/// source) are `Arc`-wrapped, so `Value::clone()` is O(1) regardless of
/// depth or width — only Arc-counter bumps, no data copies. Mutation in
/// place goes through `Arc::make_mut` for copy-on-write.
///
/// Adding a case to the *content* axis (Prim/Prod/Sum/List) is a deep change
/// touching every layer above. Doing the work *inside* a case (encoded
/// primitives, factorized joins, per-lane transforms) lives in operators.
/// The storage axis is the place to add laziness/borrowing/etc. (FOLLOWUPS §1).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Prim(Prim),
    Prod(Arc<Vec<Value>>),
    Sum  { disc: Prim, lanes: Arc<Vec<Value>> },
    List { bounds: BoundsRepr, values: Arc<Value> },
    View { source: Arc<Value>, selector: Selector },
}

/// Default for a `Value` is an empty `Prim::P64`. Used as the sentinel
/// left behind by `Ref { take: true }` when it moves a value out of the
/// env. Parser-level validation (use-after-take detection) should
/// prevent any op from ever reading a defaulted slot — but defaulting
/// to a benign empty Prim means we degrade to "wrong answer with valid
/// shape" rather than panic if a bug slips through.
impl Default for Value {
    fn default() -> Self { Value::Prim(Prim::P64(Arc::new(Vec::new()))) }
}

/// How a `View` maps logical positions to source positions.
///
/// Four variants. Three are rank-preserving (flat output, same shape as
/// source); `SequenceRange` is rank-lifting (output is a `List` of
/// sub-slices of source).
///
/// - `Indices`: random-access positions. Logical position i maps to
///   `source[indices[i]]`. Output shape: same as source.
/// - `Range`: one contiguous half-open interval `[start, end)`. Output
///   shape: same as source. **Equivalent to `Runs` with a single
///   element**; kept as a specialized variant because the O(1)
///   representation (two u64) is cheap and the case is very common.
/// - `Runs`: a series of half-open intervals `[(lo₀, hi₀), …, (lo_{N-1},
///   hi_{N-1}))`. The logical view is the *flat concatenation* of those
///   sub-slices: total length is `Σ (hi_i - lo_i)`. Output shape: same
///   as source (rank-preserving). Useful for filters that produce
///   contiguous-block selections, and as a building block for
///   "interval-based" filters.
/// - `SequenceRange`: per-row contiguous slices `[los[i], his[i])` into
///   the underlying source. Resulting view has shape `List<source's
///   inner>` — it looks like a list with `los.len()` outer rows, no
///   materialization. This is the form that lets per-row ops walk
///   per-anchor sub-lists of a shared flat column without allocating
///   per-row inner data.
///
///   *Conceptual note:* `SequenceRange` is structurally equivalent to
///   `(Runs + List reshape)` — it conflates "select N intervals" with
///   "wrap them as a List of N rows". It's kept as a separate variant
///   because the per-anchor adjacency workload (WCO triangle, etc.)
///   relies on its direct O(1)-per-row access path. Once the e-graph
///   optimizer (task #50) lands a rewrite rule that recognizes
///   `List<View<Runs>>` with matching bounds and emits the same direct
///   access pattern, this variant can be retired.
#[derive(Clone, Debug, PartialEq)]
pub enum Selector {
    Indices(Arc<Vec<u64>>),
    Runs(Arc<Vec<(u64, u64)>>),
    /// Bitmap over source positions. `mask[i] != 0` means position `i` in
    /// source is included; logical position `j` in the view corresponds
    /// to the `j`-th set bit. Length is `popcount(mask)`. The form
    /// produced by `filter` to defer materialization through chains.
    /// Composes with itself via bitwise AND (cheap); lowered to Indices
    /// for other compositions and for consumers that don't have a
    /// Mask-aware fast path.
    Mask(Arc<Vec<u8>>),
    SequenceRange { los: Arc<Vec<u64>>, his: Arc<Vec<u64>> },
}

impl Selector {
    /// Logical length of the view this selector produces.
    ///
    /// For flat selectors (`Indices`, `Range`) this is the element count.
    /// For `SequenceRange` this is the *outer row count* — the view looks
    /// like a List, so its length is the number of rows.
    pub fn len(&self) -> usize {
        match self {
            Selector::Indices(v) => v.len(),
            Selector::Runs(runs) => runs.iter().map(|(lo, hi)| (hi.saturating_sub(*lo)) as usize).sum(),
            Selector::Mask(m) => m.iter().filter(|&&b| b != 0).count(),
            Selector::SequenceRange { los, .. } => los.len(),
        }
    }

    /// Materialize the selector as a `Vec<usize>`. Only meaningful for flat
    /// selectors (`Indices`, `Runs`) — used by `gather` to walk source
    /// positions. Errors for `SequenceRange` because that's a row-shaped
    /// selector and a flat `Vec<usize>` would lose the row boundaries.
    pub fn to_usize_vec(&self) -> Vec<usize> {
        match self {
            Selector::Indices(v) => v.iter().map(|&i| i as usize).collect(),
            Selector::Runs(runs) => {
                let total: usize = runs.iter().map(|(lo, hi)| (hi - lo) as usize).sum();
                let mut out = Vec::with_capacity(total);
                for (lo, hi) in runs.iter() {
                    out.extend((*lo as usize)..(*hi as usize));
                }
                out
            }
            Selector::Mask(m) => {
                let mut out = Vec::with_capacity(self.len());
                for (i, &b) in m.iter().enumerate() {
                    if b != 0 { out.push(i); }
                }
                out
            }
            Selector::SequenceRange { .. } => panic!(
                "Selector::to_usize_vec on SequenceRange — use materialize_to_list instead"
            ),
        }
    }

    /// Constructor from a `Vec<u64>` of indices.
    pub fn from_indices(v: Vec<u64>) -> Self { Selector::Indices(Arc::new(v)) }

    /// Constructor for a single-interval `[start, end)` slice. Builds a
    /// `Runs` with one element. Range was previously a distinct variant
    /// for this case; it was retired in favor of Runs to keep the
    /// composition table small. The allocation cost (one Vec<(u64,u64)>
    /// of length 1) is negligible for the slicing use cases (`take`,
    /// `skip`, `view.range`) which aren't in inner loops.
    pub fn range(start: u64, end: u64) -> Self {
        Selector::Runs(Arc::new(vec![(start, end)]))
    }
}

/// Smart constructor for `View`. Collapses `View(View(s, inner), outer)` to a
/// single layer `View(s, compose(inner, outer))`, so the canonical form is
/// always "at most one View wrapper." A view of a non-View just becomes a
/// fresh View.
///
/// Composition (for `View(View(s, inner), outer)`): the outer selector
/// indexes *into* the inner view, so `composed[i] = inner[outer[i]]` at
/// source coordinates.
pub fn view(source: Value, selector: Selector) -> Value {
    match source {
        Value::View { source: inner_source, selector: inner_sel } => {
            let composed = compose_selectors(&inner_sel, &selector);
            Value::View { source: inner_source, selector: composed }
        }
        other => Value::View { source: Arc::new(other), selector },
    }
}

/// Compose two selectors: given `inner` (source -> outer view positions)
/// and `outer` (outer view -> consumer positions), produce a selector that
/// maps consumer positions directly to source positions.
///
/// `composed[i] = inner[outer[i]]`.
///
/// SequenceRange compositions error: composing per-row sub-list-views with
/// flat selectors doesn't have a single natural answer (does the flat
/// selector restrict rows? Or restrict positions within each row?). For
/// now, those compositions force eager materialization through `gather`.
pub fn compose_selectors(inner: &Selector, outer: &Selector) -> Selector {
    // Helper: extract (start, end) from a single-interval Runs. The
    // common case (formerly Range) gets specialized arms below.
    fn as_single_run(s: &Selector) -> Option<(u64, u64)> {
        if let Selector::Runs(runs) = s {
            if runs.len() == 1 { return Some(runs[0]); }
        }
        None
    }
    match (inner, outer) {
        (Selector::Indices(inn), Selector::Indices(out)) => {
            let composed: Vec<u64> = out.iter().map(|&j| inn[j as usize]).collect();
            Selector::Indices(Arc::new(composed))
        }
        // Specializations for single-interval Runs (formerly Range arms).
        (inner, Selector::Indices(out)) if as_single_run(inner).is_some() => {
            let (start, _) = as_single_run(inner).unwrap();
            let composed: Vec<u64> = out.iter().map(|&j| start + j).collect();
            Selector::Indices(Arc::new(composed))
        }
        (Selector::Indices(inn), outer) if as_single_run(outer).is_some() => {
            let (s, e) = as_single_run(outer).unwrap();
            let composed: Vec<u64> = inn[s as usize..e as usize].to_vec();
            Selector::Indices(Arc::new(composed))
        }
        (inner, outer) if as_single_run(inner).is_some() && as_single_run(outer).is_some() => {
            let (is, _) = as_single_run(inner).unwrap();
            let (os, oe) = as_single_run(outer).unwrap();
            Selector::range(is + os, is + oe)
        }
        // Mask ∩ Mask: the chained-filter hot path. Inner is a Mask over
        // source; outer is a Mask over the inner view (length =
        // popcount(inner)). The composed mask is: for each source
        // position i, composed[i] = inner[i] && outer[rank(inner, i)].
        // I.e., scan source positions, advance an outer-rank counter
        // each time inner is set, AND the corresponding outer bit.
        (Selector::Mask(m_inner), Selector::Mask(m_outer)) => {
            let n = m_inner.len();
            let mut composed = vec![0u8; n];
            let mut rank: usize = 0;
            for i in 0..n {
                if m_inner[i] != 0 {
                    if rank < m_outer.len() && m_outer[rank] != 0 {
                        composed[i] = 1;
                    }
                    rank += 1;
                }
            }
            Selector::Mask(Arc::new(composed))
        }
        // Other Mask compositions: lower to Indices and recompose.
        // (Specialize later as profiles show — e.g., Mask ∘ Indices
        // could be done in one pass via rank.)
        (Selector::Mask(_), _) | (_, Selector::Mask(_)) => {
            let inner_idx: Vec<u64> = inner.to_usize_vec().into_iter().map(|i| i as u64).collect();
            let outer_idx: Vec<u64> = outer.to_usize_vec().into_iter().map(|i| i as u64).collect();
            let composed: Vec<u64> = outer_idx.iter().map(|&j| inner_idx[j as usize]).collect();
            Selector::Indices(Arc::new(composed))
        }
        // General Runs compositions: lower to Indices and recompose.
        // (Multi-interval Runs cases haven't been profiled; adding
        // pair-specific implementations later is one of the natural
        // e-graph rewrite categories.)
        (Selector::Runs(_), _) | (_, Selector::Runs(_)) => {
            let inner_idx: Vec<u64> = inner.to_usize_vec().into_iter().map(|i| i as u64).collect();
            let outer_idx: Vec<u64> = outer.to_usize_vec().into_iter().map(|i| i as u64).collect();
            let composed: Vec<u64> = outer_idx.iter().map(|&j| inner_idx[j as usize]).collect();
            Selector::Indices(Arc::new(composed))
        }
        // SequenceRange compositions don't collapse — caller falls back to
        // materializing the inner view first.
        (Selector::SequenceRange { .. }, _) | (_, Selector::SequenceRange { .. }) => {
            // Returning the inner unchanged signals "no composition possible"
            // — callers that test for this case handle materialization.
            // The smart constructor for `view` won't ever feed us this case
            // because it checks for nested SequenceRange separately.
            inner.clone()
        }
    }
}

/// Construction helper for `Value::Prod`. Wraps fields in `Arc` so callers
/// don't have to.
pub fn prod(fs: Vec<Value>) -> Value { Value::Prod(Arc::new(fs)) }

/// Construction helper for `Value::Sum`.
pub fn sum(disc: Prim, lanes: Vec<Value>) -> Value {
    Value::Sum { disc, lanes: Arc::new(lanes) }
}

/// Construction helper for `Value::List`.
pub fn list(bounds: BoundsRepr, values: Value) -> Value {
    Value::List { bounds, values: Arc::new(values) }
}

impl Value {
    /// Row count. For `Prod`, the shared field length (0 if no fields).
    /// For `Sum`, the discriminant length. For `List`, the outer row count.
    /// For `View`, the selector's length (= row count of the materialized
    /// form, by construction).
    pub fn len(&self) -> usize {
        match self {
            Value::Prim(p) => p.len(),
            Value::Prod(fs) => fs.first().map(|f| f.len()).unwrap_or(0),
            Value::Sum { disc, .. } => disc.len(),
            Value::List { bounds, .. } => bounds.len(),
            Value::View { selector, .. } => selector.len(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Prim(Prim::P8(v))  => write!(f, "P8{:?}",  v),
            Value::Prim(Prim::P16(v)) => write!(f, "P16{:?}", v),
            Value::Prim(Prim::P32(v)) => write!(f, "P32{:?}", v),
            Value::Prim(Prim::P64(v)) => write!(f, "P64{:?}", v),
            Value::Prod(fs) => {
                write!(f, "(")?;
                for (i, x) in fs.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", x)?;
                }
                write!(f, ")")
            }
            Value::Sum { disc, lanes } => {
                write!(f, "Sum{{disc={:?}, lanes=[", disc)?;
                for (i, x) in lanes.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", x)?;
                }
                write!(f, "]}}")
            }
            Value::List { bounds, values } => {
                // Pretty-print List<P8> as Str[…] when the payload is valid UTF-8.
                if let Value::Prim(Prim::P8(bytes)) = values.as_ref() {
                    let bytes_slice: &[u8] = bytes;
                    if std::str::from_utf8(bytes_slice).is_ok() {
                        write!(f, "Str[")?;
                        for (i, (lo, hi)) in bounds.iter_pairs().enumerate() {
                            if i > 0 { write!(f, ", ")?; }
                            let slice = &bytes_slice[lo as usize..hi as usize];
                            write!(f, "{:?}", std::str::from_utf8(slice).unwrap())?;
                        }
                        return write!(f, "]");
                    }
                }
                write!(f, "List{{bounds={:?}, values={}}}", bounds, values)
            }
            Value::View { source, selector } => {
                // Display materializes — show what the view *means*, not how
                // it's stored. Three selector shapes, three rendering paths.
                match selector {
                    Selector::Indices(_) | Selector::Runs(_) | Selector::Mask(_) => {
                        let n = selector.len();
                        write!(f, "View<n={}>{{", n)?;
                        match source.as_ref() {
                            Value::Prim(p) => {
                                write!(f, "{}[", p.width())?;
                                let positions = selector.to_usize_vec();
                                for (i, pos) in positions.iter().enumerate() {
                                    if i > 0 { write!(f, ", ")?; }
                                    match p {
                                        Prim::P8(v)  => write!(f, "{}", v[*pos])?,
                                        Prim::P16(v) => write!(f, "{}", v[*pos])?,
                                        Prim::P32(v) => write!(f, "{}", v[*pos])?,
                                        Prim::P64(v) => write!(f, "{}", v[*pos])?,
                                    }
                                }
                                write!(f, "]")?;
                            }
                            other => write!(f, "{}", other)?,
                        }
                        write!(f, "}}")
                    }
                    Selector::SequenceRange { los, his } => {
                        // Row-shaped: render as a List-like sequence of sub-slices.
                        write!(f, "View<rows={}>{{", los.len())?;
                        match source.as_ref() {
                            Value::Prim(p) => {
                                for i in 0..los.len() {
                                    if i > 0 { write!(f, " ; ")?; }
                                    let lo = los[i] as usize;
                                    let hi = his[i] as usize;
                                    write!(f, "[")?;
                                    for j in lo..hi {
                                        if j > lo { write!(f, ", ")?; }
                                        match p {
                                            Prim::P8(v)  => write!(f, "{}", v[j])?,
                                            Prim::P16(v) => write!(f, "{}", v[j])?,
                                            Prim::P32(v) => write!(f, "{}", v[j])?,
                                            Prim::P64(v) => write!(f, "{}", v[j])?,
                                        }
                                    }
                                    write!(f, "]")?;
                                }
                            }
                            other => write!(f, "<source={}>", other)?,
                        }
                        write!(f, "}}")
                    }
                }
            }
        }
    }
}

/// Construction helper: build a `Value::Prim` from any `Vec<T>` where `T: Storage`.
/// The most common way to put data on the stack in tests and demos.
pub fn from_vec<T: Storage>(v: Vec<T>) -> Value { Value::Prim(T::wrap(v)) }

// Re-exports — supporting types live in submodules below; external code
// uses them as `value::Prim` / `value::BoundsRepr` / etc.
pub use prim::{Prim, PrimWidth, prim_p64, prim_p8};
pub use bounds::{BoundsRepr, bounds_var, bounds_var_from_ends, bounds_stride, bounds_runs};
pub use storage::Storage;

mod prim {
    //! The width-tagged primitive leaf of `Value`.
    //!
    //! `Prim` is a flat column of bytes tagged by width. The tag guarantees
    //! alignment, so `bytemuck::cast_slice` between same-width Pod types is
    //! total. Interpretation (i32 vs u32 vs f32) is *not* carried here —
    //! that lives in operators.
    use std::fmt;
    use std::sync::Arc;

    #[derive(Clone, Debug, PartialEq)]
    pub enum Prim {
        P8 (Arc<Vec<u8>>),
        P16(Arc<Vec<u16>>),
        P32(Arc<Vec<u32>>),
        P64(Arc<Vec<u64>>),
    }

    impl Prim {
        pub fn len(&self) -> usize {
            match self {
                Prim::P8(v) => v.len(),
                Prim::P16(v) => v.len(),
                Prim::P32(v) => v.len(),
                Prim::P64(v) => v.len(),
            }
        }
        pub fn width(&self) -> PrimWidth {
            match self {
                Prim::P8(_)  => PrimWidth::W8,
                Prim::P16(_) => PrimWidth::W16,
                Prim::P32(_) => PrimWidth::W32,
                Prim::P64(_) => PrimWidth::W64,
            }
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub enum PrimWidth { W8, W16, W32, W64 }

    impl fmt::Display for PrimWidth {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                PrimWidth::W8 => write!(f, "P8"),
                PrimWidth::W16 => write!(f, "P16"),
                PrimWidth::W32 => write!(f, "P32"),
                PrimWidth::W64 => write!(f, "P64"),
            }
        }
    }

    /// Common Prim construction helpers used by serialization, sum-disc, bounds.
    pub fn prim_p64(v: Vec<u64>) -> Prim { Prim::P64(Arc::new(v)) }
    pub fn prim_p8 (v: Vec<u8>)  -> Prim { Prim::P8 (Arc::new(v)) }
}

mod bounds {
    //! How `Value::List` stores its bounds.
    //!
    //! `Var` is the general case (a P64 of run-end offsets). `Stride` is the
    //! regularly-spaced fast path — all rows share an inner length, stored
    //! in 16 bytes regardless of row count. Operators that just iterate
    //! (`each`, `reduce.+`, `count`) can read either repr directly via
    //! `iter()`; operators that need a contiguous slice fall back to
    //! `to_vec()` / `to_prim()`, which materialize.
    use super::prim::{Prim, PrimWidth, prim_p64};

    use std::sync::Arc;

    #[derive(Clone, Debug)]
    pub enum BoundsRepr {
        Var(Prim),
        Stride { stride: u64, count: u64 },
        /// Bounds derived from a `Vec<(lo, hi)>`. The i-th bound is
        /// `Σ_{j≤i} (hi[j] - lo[j])`. Used by the row-shaped
        /// `List<bounds, View<Prim, Runs>>` form that replaces
        /// `Selector::SequenceRange` — sharing the Arc with the inner
        /// View avoids materializing a cumulative-bounds Vec at
        /// construction time (the WCO regression source for #53).
        Runs(Arc<Vec<(u64, u64)>>),
    }

    // Manual PartialEq: compare semantically via `.iter()` so two reprs
    // describing the same sequence (e.g., `Var([4, 8, 12])` and
    // `Stride{stride: 4, count: 3}`, or the new `Runs([(0,4),(4,8),(8,12)])`)
    // compare equal. The derived element-wise eq for the prior two-
    // variant enum had the same time complexity (O(N) inside the Arc),
    // so this is not a perf regression on existing paths.
    impl PartialEq for BoundsRepr {
        fn eq(&self, other: &Self) -> bool {
            if self.len() != other.len() { return false; }
            self.iter_starts().zip(other.iter_starts()).all(|(a, b)| a == b)
        }
    }

    impl BoundsRepr {
        /// Bounds always materialize to P64.
        pub fn width(&self) -> PrimWidth { PrimWidth::W64 }

        /// Outer row count. For `Var`, this is `entries - 1` since the
        /// canonical representation carries N+1 start-offsets (a leading
        /// 0 plus one end-offset per row).
        pub fn len(&self) -> usize {
            match self {
                BoundsRepr::Var(p) => p.len().saturating_sub(1),
                BoundsRepr::Stride { count, .. } => *count as usize,
                BoundsRepr::Runs(runs) => runs.len(),
            }
        }

        /// Iterate the N+1 start-offsets `[0, e₀, e₁, …, e_{N-1}]`.
        /// First entry is always 0; last entry is the total flat length.
        /// Zero-alloc for all reprs.
        pub fn iter_starts<'a>(&'a self) -> Box<dyn Iterator<Item = u64> + 'a> {
            match self {
                BoundsRepr::Var(Prim::P64(v)) => Box::new(v.iter().copied()),
                BoundsRepr::Stride { stride, count } => {
                    let s = *stride;
                    Box::new((0..=*count).map(move |i| i * s))
                }
                BoundsRepr::Runs(runs) => {
                    let mut acc = 0u64;
                    Box::new(std::iter::once(0u64).chain(runs.iter().map(move |(lo, hi)| {
                        acc += hi - lo;
                        acc
                    })))
                }
                _ => Box::new(std::iter::empty()),
            }
        }

        /// Iterate row `(lo, hi)` pairs. The preferred consumer API:
        /// every per-row loop wants both endpoints without tracking a
        /// `prev` carrier.
        pub fn iter_pairs<'a>(&'a self) -> Box<dyn Iterator<Item = (u64, u64)> + 'a> {
            match self {
                BoundsRepr::Var(Prim::P64(v)) => {
                    Box::new(v.windows(2).map(|w| (w[0], w[1])))
                }
                BoundsRepr::Stride { stride, count } => {
                    let s = *stride;
                    Box::new((0..*count).map(move |i| (i * s, (i + 1) * s)))
                }
                BoundsRepr::Runs(runs) => {
                    let mut acc = 0u64;
                    Box::new(runs.iter().map(move |(lo, hi)| {
                        let start = acc;
                        acc += hi - lo;
                        (start, acc)
                    }))
                }
                _ => Box::new(std::iter::empty()),
            }
        }

        /// Materialize as a `Vec<u64>` of N+1 start-offsets when a caller
        /// needs a contiguous slice. Cheap clone for `Var`; O(count)
        /// allocation for `Stride` / `Runs`.
        pub fn to_vec(&self) -> Vec<u64> {
            match self {
                BoundsRepr::Var(Prim::P64(v)) => (**v).clone(),
                _ => self.iter_starts().collect(),
            }
        }

        /// Materialize as a `Prim` (N+1 start-offsets) for use elsewhere as a Value.
        pub fn to_prim(&self) -> Prim {
            match self {
                BoundsRepr::Var(p) => p.clone(),
                BoundsRepr::Stride { .. } | BoundsRepr::Runs(_) => prim_p64(self.to_vec()),
            }
        }
    }

    /// Build a `BoundsRepr::Var` from a canonical `[0, e₀, …, e_{N-1}]` vector.
    /// Caller is responsible for the leading 0 + monotone shape; use
    /// `bounds_var_from_ends` for the legacy "ends-only" construction.
    pub fn bounds_var(v: Vec<u64>) -> BoundsRepr { BoundsRepr::Var(prim_p64(v)) }

    /// Build a `BoundsRepr::Var` from a `Vec<u64>` of N row-end offsets
    /// by prepending the canonical leading 0. Use when the caller has
    /// produced the old end-offsets-only form (cumsum during nesting,
    /// run-end offsets from grouping, etc.).
    pub fn bounds_var_from_ends(mut ends: Vec<u64>) -> BoundsRepr {
        let mut starts = Vec::with_capacity(ends.len() + 1);
        starts.push(0);
        starts.append(&mut ends);
        BoundsRepr::Var(prim_p64(starts))
    }

    /// Build a `BoundsRepr::Runs` from an `Arc<Vec<(lo, hi)>>`. Bounds
    /// are computed lazily as cumulative `hi - lo`. Used by the
    /// row-shaped view-of-list path to share the runs Arc with the
    /// inner `View<Prim, Selector::Runs>`.
    pub fn bounds_runs(runs: std::sync::Arc<Vec<(u64, u64)>>) -> BoundsRepr {
        BoundsRepr::Runs(runs)
    }

    /// Build a `BoundsRepr::Stride` with `count` rows of `stride` elements each.
    pub fn bounds_stride(stride: u64, count: u64) -> BoundsRepr {
        BoundsRepr::Stride { stride, count }
    }
}

mod storage {
    //! The bytemuck bridge: each Pod numeric type (u8/i8/.../f64) maps to a
    //! `Prim` variant for reads (`extract`) and writes (`wrap`). The width
    //! tag in `Prim` makes the cast total — no runtime alignment fallback.
    use std::sync::Arc;
    use super::prim::Prim;

    /// Per-interpretation-type bridge between `T` and its `Prim` carrier.
    ///
    /// `extract` is the borrow path (kept for callers that just need a
    /// slice). `extract_arc` is the owning path: takes the Prim by value
    /// and returns the underlying `Arc<Vec<Backing>>`. Callers that just
    /// want to read use `&*arc`; callers that want to mutate the buffer
    /// in place use `Arc::try_unwrap` — if refcount = 1, they own the
    /// `Vec<Backing>` and can cast it to `Vec<Self>` for free via
    /// bytemuck::cast_vec.
    ///
    /// The owning path is what enables Arc-1 buffer reuse: when we have
    /// the sole reference to a column, in-place mutation avoids the
    /// fresh allocation that's the dominant cost on per-row sum / add
    /// kernels.
    pub trait Storage: bytemuck::Pod + Copy + std::fmt::Debug + 'static {
        type Backing: bytemuck::Pod + Copy + 'static;
        fn extract(p: &Prim) -> Result<&[Self], String>;
        fn extract_arc(p: Prim) -> Result<Arc<Vec<Self::Backing>>, String>;
        fn wrap(v: Vec<Self>) -> Prim;
    }

    macro_rules! impl_storage_at_width {
        ($t:ty, $prim_v:ident, $backing:ty) => {
            impl Storage for $t {
                type Backing = $backing;
                #[inline(always)]
                fn extract(p: &Prim) -> Result<&[Self], String> {
                    match p {
                        Prim::$prim_v(v) => Ok(bytemuck::cast_slice::<$backing, $t>(v)),
                        other => Err(format!(
                            "{}: expected {}, got {:?}",
                            stringify!($t), stringify!($prim_v), other.width()
                        )),
                    }
                }
                #[inline(always)]
                fn extract_arc(p: Prim) -> Result<Arc<Vec<Self::Backing>>, String> {
                    match p {
                        Prim::$prim_v(v) => Ok(v),
                        other => Err(format!(
                            "{}: expected {}, got {:?}",
                            stringify!($t), stringify!($prim_v), other.width()
                        )),
                    }
                }
                #[inline(always)]
                fn wrap(v: Vec<Self>) -> Prim {
                    Prim::$prim_v(Arc::new(bytemuck::cast_vec::<$t, $backing>(v)))
                }
            }
        };
    }
    impl_storage_at_width!(u8,  P8,  u8);
    impl_storage_at_width!(i8,  P8,  u8);
    impl_storage_at_width!(u16, P16, u16);
    impl_storage_at_width!(i16, P16, u16);
    impl_storage_at_width!(u32, P32, u32);
    impl_storage_at_width!(i32, P32, u32);
    impl_storage_at_width!(f32, P32, u32);
    impl_storage_at_width!(u64, P64, u64);
    impl_storage_at_width!(i64, P64, u64);
    impl_storage_at_width!(f64, P64, u64);
}
