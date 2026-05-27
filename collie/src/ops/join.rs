//! Joins, surveys, and materialization — the largest ops file (~1000
//! LOC) because each op has a per-list variant in addition to the flat
//! one.
//!
//! - `intersect` — sort-merge intersect, gallop-based; the inner
//!   loop is asymmetric (smaller side drives), so the cost is
//!   `O(min(|a|, |b|))` not `O(|a| + |b|)`. Powers the WCO triangle
//!   examples.
//! - `search` — interp-free lower-bound lookup: sort the queries, then
//!   gallop a forward cursor through the sorted target (flat and
//!   per-list). Byte order; signed/float via order-form (swizzled) inputs.
//! - `gather` — apply a P64 position column to a value column.
//!   Materializing path for `View` consumers.
//! - `xprod` — per-list Cartesian product across two parallel lists.
//!
//! The per-list variants (e.g. `intersect` over `List<List<X>>`)
//! are the long part of each op. The flat-column variant is at the
//! top of each section and is the right place to start reading.

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop, pop_raw};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, Selector, Storage, from_vec, prod};
use crate::ir::shape::{Shape, bounds_as_u64};
use crate::ops::helpers::{gallop_to, gather, sort_merge_intersect, materialize};

/// `intersect` — sort-merge intersection, polymorphic on input shape,
/// **interp-free** (byte/width compare; signed/float order via order-form
/// inputs).
///
/// **Flat form (Prim, Prim) → (P64, P64):** sort-merge over two sorted
/// same-width Prim columns. Output is two same-length position columns:
/// matched positions in a, matched positions in b.
///
/// **List-shaped form (List<T>, List<T>) → (List<u64>, List<u64>):** each
/// input is a sequence of inner lists with matching outer length; for each
/// inner list, sort-merge the two — per-list matched positions, one Rust
/// kernel, no per-list interpreter dispatch.
///
/// The list-shaped form is the canonical per-list default; flat is the
/// single-list case. Input types pick the path automatically.
#[derive(Debug)] pub struct Intersect;
impl PrimOp for Intersect {
    fn name(&self) -> &str { "intersect" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 2)) }  // (a, b) → (positions_in_a, positions_in_b)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { intersect_run(st) }
}
/// `intersect` kernel (back-end `SystemOp::Intersect` calls this directly).
pub fn intersect_run(st: &mut Stack) -> Result<(), String> {
        // pop_raw so a View on either side flows through naturally; the
        // dispatcher materializes only when it needs to.
        let b = pop_raw(st)?;
        let a = pop_raw(st)?;
        intersect_run_dispatch(st, a, b)
}

/// Dispatch on input shape (rank-polymorphic). Returns matched
    /// **positions** in each side, in source coordinates:
    ///
    ///   - Flat: `SEQ<T> × SEQ<T> -> SEQ<u64> × SEQ<u64>` — positions in a,
    ///     positions in b.
    ///   - List-shaped: `SEQ<LIST<T>> × SEQ<LIST<T>> -> SEQ<LIST<u64>> ×
    ///     SEQ<LIST<u64>>` — per-list matched positions in each side's
    ///     underlying source Prim.
    ///
    /// The flat form is the rank-1 case of the list-shaped form. Matched
    /// values are derivable via `gather`; positions are strictly more
    /// informative (you can gather any column at those positions).
fn intersect_run_dispatch(st: &mut Stack, a: Value, b: Value) -> Result<(), String> {
        // Peel a flat View (Indices/Range); leave SequenceRange views.
        let a = peel_flat_view(a)?;
        let b = peel_flat_view(b)?;
        match (&a, &b) {
            // Flat form: rank-1 case. Positions are in the input columns
            // directly (source-coord = input-coord).
            (Value::Prim(_), Value::Prim(_)) => {
                let (ai, bi) = sort_merge_intersect(&a, &b)?;
                st.push(from_vec::<u64>(ai.into_iter().map(|i| i as u64).collect()));
                st.push(from_vec::<u64>(bi.into_iter().map(|i| i as u64).collect()));
                Ok(())
            }
            // List-shaped form: per-list sort-merge, source-coord positions.
            (a_row, b_row) if is_list_shaped(a_row) && is_list_shaped(b_row) => {
                let a_lists = ListAccess::new(a_row)?;
                let b_lists = ListAccess::new(b_row)?;
                if a_lists.n_lists() != b_lists.n_lists() {
                    return Err(format!(
                        "intersect: list-shaped outer length mismatch ({} vs {})",
                        a_lists.n_lists(), b_lists.n_lists()
                    ));
                }
                intersect_run_per_list(st, &a_lists, &b_lists)
            }
            (other_a, other_b) => Err(format!(
                "intersect: expected (Prim, Prim) or two list-shaped inputs, got ({:?}, {:?})",
                other_a, other_b
            )),
        }
}

fn intersect_run_per_list(
    st: &mut Stack,
    a: &ListAccess,
    b: &ListAccess,
) -> Result<(), String> {
        let n_lists = a.n_lists();
        // Per-row matched POSITIONS, in source coordinates: for each row,
        // emit the positions of matches in a's source AND in b's source.
        // Two output List<u64>s, both with the same per-list counts.
        macro_rules! per_row { ($t:ty) => {{
            let av_slice = <$t as Storage>::extract(a.source_prim())?;
            let bv_slice = <$t as Storage>::extract(b.source_prim())?;
            let mut flat_a: Vec<u64> = Vec::new();
            let mut flat_b: Vec<u64> = Vec::new();
            let mut bounds: Vec<u64> = Vec::with_capacity(n_lists);
            for i in 0..n_lists {
                let (a_lo, a_hi) = a.list_range(i);
                let (b_lo, b_hi) = b.list_range(i);
                let a_sub = &av_slice[a_lo..a_hi];
                let b_sub = &bv_slice[b_lo..b_hi];
                // Per-row sort-merge intersect (gallop-driven). Emit
                // source-coord positions: ia + a_lo for a, ib + b_lo for b.
                // For runs of equal values (multiset case), emit the full
                // cartesian product of (a-positions × b-positions) within
                // the run, matching the flat form's semantics.
                let mut ia = 0usize;
                let mut ib = 0usize;
                while ia < a_sub.len() && ib < b_sub.len() {
                    use std::cmp::Ordering::*;
                    match a_sub[ia].cmp(&b_sub[ib]) {
                        Less => {
                            let target = b_sub[ib];
                            ia = gallop_to(a_sub, ia + 1, |x| *x < target);
                        }
                        Equal => {
                            let v = a_sub[ia];
                            let mut ja = ia + 1;
                            while ja < a_sub.len() && a_sub[ja] == v { ja += 1; }
                            let mut jb = ib + 1;
                            while jb < b_sub.len() && b_sub[jb] == v { jb += 1; }
                            for pa in ia..ja {
                                for pb in ib..jb {
                                    flat_a.push((pa + a_lo) as u64);
                                    flat_b.push((pb + b_lo) as u64);
                                }
                            }
                            ia = ja; ib = jb;
                        }
                        Greater => {
                            let target = a_sub[ia];
                            ib = gallop_to(b_sub, ib + 1, |x| *x < target);
                        }
                    }
                }
                bounds.push(flat_a.len() as u64);
            }
            (bounds, flat_a, flat_b)
        }};}
        let (bounds, flat_a, flat_b) = match a.source_prim().width() {
            PrimWidth::W8  => per_row!(u8),
            PrimWidth::W16 => per_row!(u16),
            PrimWidth::W32 => per_row!(u32),
            PrimWidth::W64 => per_row!(u64),
        };
        // Two outputs: per-list positions in a's source, per-list positions in b's source.
        // Both share the same `bounds`.
        let pos_a = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(bounds.clone()),
            values: std::sync::Arc::new(from_vec::<u64>(flat_a)),
        };
        let pos_b = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(bounds),
            values: std::sync::Arc::new(from_vec::<u64>(flat_b)),
        };
        st.push(pos_a);
        st.push(pos_b);
        Ok(())
}

/// Peel a flat-selector View (Indices/Range) by materializing it. Leaves
/// SequenceRange Views alone (those are the list-shaped fast path). Anything
/// non-View passes through.
fn peel_flat_view(v: Value) -> Result<Value, String> {
    match &v {
        Value::View { selector, .. } => match selector {
            Selector::Indices(_) | Selector::Runs(_) | Selector::Mask(_) => materialize(v),
            Selector::SequenceRange { .. } => Ok(v),
        },
        _ => Ok(v),
    }
}

fn is_list_shaped(v: &Value) -> bool {
    match v {
        Value::List { .. } => true,
        Value::View { selector: Selector::SequenceRange { .. }, .. } => true,
        _ => false,
    }
}

/// Unified per-list sub-slice access for list-shaped values:
///   - `List<Prim>` — outer bounds index into the inner Prim.
///   - `View<Prim, SequenceRange>` — los/his per row directly.
///   - `List<bounds, View<Prim, Runs>>` where bounds[i]-bounds[i-1] equals
///     runs[i].1-runs[i].0 (each row is exactly one run) — scaffolding
///     for #53's eventual retirement of SequenceRange. Until that lands
///     this branch is dormant, but adding it now lets a new-form
///     constructor land without breaking the WCO kernel.
enum ListAccess<'a> {
    List {
        source: &'a Prim,
        bounds: std::borrow::Cow<'a, [u64]>,
    },
    View {
        source: &'a Prim,
        los: &'a [u64],
        his: &'a [u64],
    },
    Runs {
        source: &'a Prim,
        runs: &'a [(u64, u64)],
    },
}

impl<'a> ListAccess<'a> {
    fn new(v: &'a Value) -> Result<Self, String> {
        match v {
            Value::List { bounds, values } => {
                match values.as_ref() {
                    Value::Prim(source) => {
                        let bnds = bounds_as_u64(bounds)?;
                        Ok(ListAccess::List { source, bounds: bnds })
                    }
                    Value::View { source, selector: Selector::Runs(runs) } => {
                        // The `List<View<Runs>>` form is constructed by
                        // `view` op with list-shaped input; bounds are
                        // built as a `BoundsRepr::Runs` that shares the
                        // same Arc — so alignment is structural, no
                        // runtime check needed. (A future bounds-Var
                        // representation that pre-materializes the
                        // cumulative bounds would also be aligned by
                        // construction.)
                        let source = match source.as_ref() {
                            Value::Prim(p) => p,
                            other => return Err(format!(
                                "list-shaped intersect: View source must be Prim, got {:?}", other
                            )),
                        };
                        Ok(ListAccess::Runs { source, runs: runs.as_slice() })
                    }
                    other => Err(format!(
                        "list-shaped intersect: list inner must be Prim or View<Prim, Runs>, got {:?}", other
                    )),
                }
            }
            Value::View { source, selector: Selector::SequenceRange { los, his } } => {
                let source = match source.as_ref() {
                    Value::Prim(p) => p,
                    other => return Err(format!(
                        "list-shaped intersect: View source must be Prim, got {:?}", other
                    )),
                };
                Ok(ListAccess::View { source, los: los.as_slice(), his: his.as_slice() })
            }
            other => Err(format!("list-shaped intersect: not list-shaped: {:?}", other)),
        }
    }

    fn n_lists(&self) -> usize {
        match self {
            ListAccess::List { bounds, .. } => bounds.len().saturating_sub(1),
            ListAccess::View { los, .. } => los.len(),
            ListAccess::Runs { runs, .. } => runs.len(),
        }
    }

    fn source_prim(&self) -> &Prim {
        match self {
            ListAccess::List { source, .. } => source,
            ListAccess::View { source, .. } => source,
            ListAccess::Runs { source, .. } => source,
        }
    }

    fn list_range(&self, i: usize) -> (usize, usize) {
        match self {
            ListAccess::List { bounds, .. } => {
                (bounds[i] as usize, bounds[i + 1] as usize)
            }
            ListAccess::View { los, his, .. } => {
                (los[i] as usize, his[i] as usize)
            }
            ListAccess::Runs { runs, .. } => {
                (runs[i].0 as usize, runs[i].1 as usize)
            }
        }
    }
}

impl Typed for Intersect {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { intersect_tc(st) }
}
pub fn intersect_tc(st: &mut TypeStack) -> Result<(), String> {
        let b = tc_pop(st, "intersect")?;
        let a = tc_pop(st, "intersect")?;
        // Interp-free: the two sides need only agree in width.
        match (&a, &b) {
            // Flat form: SEQ<T> × SEQ<T> → SEQ<u64> × SEQ<u64> (positions in each).
            (Shape::Prim(wa), Shape::Prim(wb)) if wa == wb => {
                st.push(Shape::Prim(PrimWidth::W64));
                st.push(Shape::Prim(PrimWidth::W64));
                Ok(())
            }
            // List form: SEQ<LIST<T>> × SEQ<LIST<T>> → SEQ<LIST<u64>> × SEQ<LIST<u64>>
            //   (per-list positions in each side's underlying source).
            (Shape::List { inner: ia, .. }, Shape::List { inner: ib, .. }) => {
                match (ia.as_ref(), ib.as_ref()) {
                    (Shape::Prim(wa), Shape::Prim(wb)) if wa == wb => {
                        let list_p64 = Shape::List {
                            bounds: PrimWidth::W64,
                            inner: Box::new(Shape::Prim(PrimWidth::W64)),
                        };
                        st.push(list_p64.clone());
                        st.push(list_p64);
                        Ok(())
                    }
                    _ => Err(format!(
                        "intersect: list inners must be same-width Prim, got {} and {}",
                        a, b
                    )),
                }
            }
            _ => Err(format!(
                "intersect: expected (Prim, Prim) or (List<Prim>, List<Prim>), got {} and {}",
                a, b
            )),
        }
}

/// `search.<interp>` — for each value in `queries`, find its `lower_bound`
/// position in the sorted `target`. The result is a P64 of indices with one
/// entry per query, in the range `[0, target.len()]`. The sentinel position
/// `target.len()` means "would insert past the end" (i.e., the query is
/// greater than every element of target — *or* equal-to-nothing if a miss).
///
/// To distinguish a hit from a "would-insert-here" miss: gather `target` at
/// the returned positions and compare for equality with the queries. Misses
/// will have `target[pos] > query` (or be at the sentinel).
///
/// Stack effect: `(target, queries) -> indices`. Both inputs must be Prim of
/// the interp's width; target must be sorted ascending. Floats unsupported
/// (no total order — same as `intersect.<interp>`).
///
/// Cost: `O(|queries| * log(|target|))` via per-query gallop. Asymmetric —
/// good when `|queries| << |target|`. (For balanced sizes, `intersect` is
/// O(|target| + |queries|) and may be faster.)
#[derive(Debug)] pub struct Search;
impl PrimOp for Search {
    fn name(&self) -> &str { "search" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (target, queries) → positions
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { search_run(st) }
}
/// `search` kernel (back-end `SystemOp::Search` calls this directly).
pub fn search_run(st: &mut Stack) -> Result<(), String> {
        // Two input shapes:
        //   (Prim, Prim) — flat: gallop, output P64 of positions.
        //   (List/View<SequenceRange>, List/View<SequenceRange>) — list-shaped:
        //       per-list gallop, output List<u64> of per-list positions.
        let queries_raw = pop_raw(st)?;
        let target_raw  = pop_raw(st)?;

        // List-shaped form fires when both inputs are list-shaped.
        if is_list_shaped(&target_raw) && is_list_shaped(&queries_raw) {
            let target_lists = ListAccess::new(&target_raw)?;
            let queries_lists = ListAccess::new(&queries_raw)?;
            if target_lists.n_lists() != queries_lists.n_lists() {
                return Err(format!(
                    "search: list-shaped outer length mismatch ({} vs {})",
                    target_lists.n_lists(), queries_lists.n_lists()
                ));
            }
            return search_run_per_list(st, &target_lists, &queries_lists);
        }

        // Otherwise, flat form. Materialize each side to a contiguous Prim
        // (a flat View — Indices/Runs/Mask — gathers here), then sort the
        // queries and gallop a single forward cursor. Sequential; beats the
        // per-query random gallop at every size (bench 15). Byte/unsigned
        // order; signed/float order comes from order-form (swizzled) inputs.
        let target_p = flatten_to_prim(target_raw, "search")?;
        let queries_p = flatten_to_prim(queries_raw, "search")?;
        let positions = search_sort_gallop(&target_p, &queries_p)?;
        st.push(Value::Prim(Prim::P64(Arc::new(positions))));
        Ok(())
}

/// Per-list search. For each inner list i, sorts `queries[i]` and
/// galloples a single forward cursor over `target[i]` (sort-then-gallop,
/// not a per-query random restart). Output: List<u64> of per-list
/// positions (row-local within the corresponding target sub-list).
/// Position == |target[i]| is the past-end sentinel.
fn search_run_per_list(
    st: &mut Stack,
    target: &ListAccess,
    queries: &ListAccess,
) -> Result<(), String> {
        let n_lists = target.n_lists();
        macro_rules! per_row { ($t:ty) => {{
            let t_slice = <$t as Storage>::extract(target.source_prim())?;
            let q_slice = <$t as Storage>::extract(queries.source_prim())?;
            let mut flat: Vec<u64> = Vec::new();
            let mut bounds: Vec<u64> = Vec::with_capacity(n_lists);
            for i in 0..n_lists {
                let (t_lo, t_hi) = target.list_range(i);
                let (q_lo, q_hi) = queries.list_range(i);
                let t_sub = &t_slice[t_lo..t_hi];
                let q_sub = &q_slice[q_lo..q_hi];
                // Same kernel as flat search, applied to this list's slices.
                let base = flat.len();
                flat.resize(base + q_sub.len(), 0);
                sort_gallop_slice(t_sub, q_sub, &mut flat[base..]);
                bounds.push(flat.len() as u64);
            }
            Value::List {
                bounds: crate::ir::value::bounds_var_from_ends(bounds),
                values: Arc::new(from_vec::<u64>(flat)),
            }
        }};}
        let out = match target.source_prim().width() {
            PrimWidth::W8  => per_row!(u8),
            PrimWidth::W16 => per_row!(u16),
            PrimWidth::W32 => per_row!(u32),
            PrimWidth::W64 => per_row!(u64),
        };
        st.push(out);
        Ok(())
}

/// Materialize a flat Value (Prim or flat-selector View) to a contiguous
/// Prim. A flat View gathers here. Rejects SequenceRange (per-list) views:
/// search's flat path is not list-shaped, so surface the gap explicitly
/// rather than silently mis-shaping.
fn flatten_to_prim(v: Value, op: &str) -> Result<Prim, String> {
    if let Value::View { selector: Selector::SequenceRange { .. }, .. } = &v {
        return Err(format!(
            "{}: SequenceRange (per-list) inputs not supported on the flat path", op
        ));
    }
    match materialize(v)? {
        Value::Prim(p) => Ok(p),
        bad => Err(format!("{}: expected Prim, got {:?}", op, bad)),
    }
}
impl Typed for Search {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { search_tc(st) }
}
pub fn search_tc(st: &mut TypeStack) -> Result<(), String> {
        let queries = tc_pop(st, "search")?;
        let target = tc_pop(st, "search")?;
        // Interp-free: target and queries need only agree in width (search
        // compares unsigned bytes). Flat → P64; list-shaped → List<P64>.
        match (&target, &queries) {
            (Shape::Prim(wa), Shape::Prim(wb)) if wa == wb => {
                st.push(Shape::Prim(PrimWidth::W64));
                Ok(())
            }
            (Shape::List { inner: ti, .. }, Shape::List { inner: qi, .. }) => {
                match (ti.as_ref(), qi.as_ref()) {
                    (Shape::Prim(wa), Shape::Prim(wb)) if wa == wb => {
                        st.push(Shape::List {
                            bounds: PrimWidth::W64,
                            inner: Box::new(Shape::Prim(PrimWidth::W64)),
                        });
                        Ok(())
                    }
                    _ => Err(format!(
                        "search: list inners must be same-width Prim, got {} and {}",
                        target, queries
                    )),
                }
            }
            _ => Err(format!(
                "search: expected (Prim, Prim) or (List<Prim>, List<Prim>), got {} and {}",
                target, queries
            )),
        }
}

#[derive(Debug)] pub struct Gather;
impl PrimOp for Gather {
    fn name(&self) -> &str { "gather" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (col, idxs) → gathered
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { gather_run(st) }
}
impl Typed for Gather {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { gather_tc(st) }
}
/// `gather` kernel (back-end `SystemOp::Gather` calls this directly).
pub fn gather_run(st: &mut Stack) -> Result<(), String> {
    let idxs = match pop(st)? {
        Value::Prim(Prim::P64(i)) => i,
        other => return Err(format!("gather: idxs must be P64, got {:?}", other)),
    };
    let col = pop(st)?;
    let idxs_usize: Vec<usize> = idxs.iter().map(|&i| i as usize).collect();
    st.push(gather(&col, &idxs_usize)?);
    Ok(())
}
pub fn gather_tc(st: &mut TypeStack) -> Result<(), String> {
    let idxs = tc_pop(st, "gather")?;
    if idxs != Shape::Prim(PrimWidth::W64) { return Err(format!("gather: idxs must be Prim(P64), got {}", idxs)); }
    let col = tc_pop(st, "gather")?;
    st.push(col);
    Ok(())
}

#[derive(Debug)] pub struct XProd;
impl PrimOp for XProd {
    fn name(&self) -> &str { "xprod" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // Prod[List, List] → List<Prod>
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { xprod_run(st) }
}
/// `xprod` kernel (back-end `SystemOp::XProd` calls this directly).
pub fn xprod_run(st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        let (list_a, list_b) = match v {
            Value::Prod(fs) if fs.len() == 2 => (fs[0].clone(), fs[1].clone()),
            other => return Err(format!("xprod: expects Prod of arity 2, got {:?}", other)),
        };
        let (a_bounds_p, a_flat) = match list_a {
            Value::List { bounds, values } => (bounds, (*values).clone()),
            other => return Err(format!("xprod: field 0 must be a List, got {:?}", other)),
        };
        let (b_bounds_p, b_flat) = match list_b {
            Value::List { bounds, values } => (bounds, (*values).clone()),
            other => return Err(format!("xprod: field 1 must be a List, got {:?}", other)),
        };
        let a_bounds = bounds_as_u64(&a_bounds_p)?.to_vec();
        let b_bounds = bounds_as_u64(&b_bounds_p)?.to_vec();
        if a_bounds.len() != b_bounds.len() {
            return Err(format!("xprod: outer length mismatch {} vs {}", a_bounds.len(), b_bounds.len()));
        }
        // Canonical N+1 starts; row i is [a_bounds[i], a_bounds[i+1]) and same for b.
        let n_lists = a_bounds.len().saturating_sub(1);
        let mut out_bounds: Vec<u64> = Vec::with_capacity(n_lists);
        let mut a_idxs: Vec<usize> = Vec::new();
        let mut b_idxs: Vec<usize> = Vec::new();
        let mut acc: u64 = 0;
        for i in 0..n_lists {
            let a_lo = a_bounds[i] as usize;
            let a_hi = a_bounds[i + 1] as usize;
            let b_lo = b_bounds[i] as usize;
            let b_hi = b_bounds[i + 1] as usize;
            for ai in a_lo..a_hi {
                for bi in b_lo..b_hi {
                    a_idxs.push(ai);
                    b_idxs.push(bi);
                }
            }
            acc += ((a_hi - a_lo) * (b_hi - b_lo)) as u64;
            out_bounds.push(acc);
        }
        let a_g = gather(&a_flat, &a_idxs)?;
        let b_g = gather(&b_flat, &b_idxs)?;
        st.push(Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(out_bounds),
            values: Arc::new(prod(vec![a_g, b_g])),
        });
        Ok(())
}
/// Column-at-a-time `search`: instead of a per-query *random* gallop that
/// restarts on the whole target, **sort the queries**, then sweep a single
/// **monotonic forward cursor** through the target — for each sorted query
/// `gallop_to` jumps the cursor forward to that query's lower bound,
/// skipping the gap. The cursor never rewinds, so the target is touched in
/// strictly increasing order (≤ one forward sweep), and the N forward
/// gallops cost `N·log(M/N)` total. Then **unsort** the positions back to
/// query order. So `N·log N` (the sort) `+ N·log(M/N)` (the gallops),
/// **interp-free** (plain unsigned word compare, width-dispatched) and
/// **sequential-access** throughout — no random target probes.
///
/// Returns lower-bound positions (first target index `>=` the query), one
/// per query, in original query order. Unsigned order today (matches the
/// target's sortedness); signed/float come via swizzle (BACKLOG U).
/// Wired as `Search`'s unrestricted-unsigned-flat path; views / signed /
/// floats / list-shaped still use the gallop.
pub fn search_sort_gallop(target: &Prim, queries: &Prim) -> Result<Vec<u64>, String> {
    macro_rules! go { ($t:ty) => {{
        let tv = <$t as Storage>::extract(target)?;
        let qv = <$t as Storage>::extract(queries)?;
        let mut out = vec![0u64; qv.len()];
        sort_gallop_slice(tv, qv, &mut out);
        Ok(out)
    }};}
    match (target, queries) {
        (Prim::P8(_),  Prim::P8(_))  => go!(u8),
        (Prim::P16(_), Prim::P16(_)) => go!(u16),
        (Prim::P32(_), Prim::P32(_)) => go!(u32),
        (Prim::P64(_), Prim::P64(_)) => go!(u64),
        _ => Err("search_sort_gallop: target/queries width mismatch".into()),
    }
}

/// The gallop kernel shared by flat and per-list search. Sort the queries,
/// then walk a single monotonic forward cursor over `target` to each query's
/// lower bound — skipping gaps, never rewinding. Positions are written back
/// in original query order. `out` must have length `queries.len()`; on return
/// `out[k]` is the lower-bound position of `queries[k]` in `target`
/// (`target.len()` is the past-end sentinel). `target` is assumed sorted.
fn sort_gallop_slice<T: Ord + Copy>(target: &[T], queries: &[T], out: &mut [u64]) {
    let m = queries.len();
    let mut order: Vec<u32> = (0..m as u32).collect();
    order.sort_unstable_by_key(|&k| queries[k as usize]);
    let mut ti = 0usize;
    for &qi in &order {
        ti = gallop_to(target, ti, |x| *x < queries[qi as usize]);
        out[qi as usize] = ti as u64;
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "gather" => Some(Box::new(Gather)),
            "xprod" => Some(Box::new(XProd)),
            // `search` is interp-free (byte compare); signed/float order
            // via swizzled (order-form) inputs.
            "search" => Some(Box::new(Search)),
            // `intersect` is interp-free (byte compare); signed/float order
            // via swizzled (order-form) inputs.
            "intersect" => Some(Box::new(Intersect)),
            _ => None,
        }
    });
}

impl Typed for XProd {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { xprod_tc(st) }
}
pub fn xprod_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "xprod")?;
        match v {
            Shape::Prod(fs) if fs.len() == 2 => {
                let inner_a = match &fs[0] { Shape::List { inner, .. } => (**inner).clone(), other => return Err(format!("xprod: field 0 not a list: {}", other)) };
                let inner_b = match &fs[1] { Shape::List { inner, .. } => (**inner).clone(), other => return Err(format!("xprod: field 1 not a list: {}", other)) };
                st.push(Shape::List { bounds: PrimWidth::W64, inner: Box::new(Shape::Prod(vec![inner_a, inner_b])) });
                Ok(())
            }
            other => Err(format!("xprod: expected Prod[List, List], got {}", other)),
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::from_vec;

    fn search_u64(target: Vec<u64>, queries: Vec<u64>) -> Vec<u64> {
        let mut st: Vec<Value> = vec![from_vec::<u64>(target), from_vec::<u64>(queries)];
        let mut env = Vec::new();
        Search.run(&mut st, &mut env).unwrap();
        match st.pop().unwrap() {
            Value::Prim(Prim::P64(v)) => (*v).clone(),
            other => panic!("expected P64, got {:?}", other),
        }
    }

    #[test]
    fn search_sort_gallop_matches_lower_bound() {
        // The sort-then-gallop search must produce true lower-bound
        // positions (independent oracle: per-query partition_point),
        // across hits, misses, and the past-end sentinel.
        let target: Vec<u64> = (0..2000u64).map(|i| i * 3).collect(); // sorted, gaps
        let queries: Vec<u64> = (0..5000u64)
            .map(|i| i.wrapping_mul(2654435761) % 6500).collect(); // scrambled, some past end
        let got = search_sort_gallop(
            &Prim::P64(Arc::new(target.clone())),
            &Prim::P64(Arc::new(queries.clone())),
        ).unwrap();
        let oracle: Vec<u64> = queries.iter()
            .map(|&q| target.partition_point(|&x| x < q) as u64)
            .collect();
        assert_eq!(got, oracle);
    }

    #[test]
    fn search_all_hits() {
        // target = [10,20,30,40], queries hit each at its position
        assert_eq!(search_u64(vec![10, 20, 30, 40], vec![10, 20, 30, 40]), vec![0, 1, 2, 3]);
    }

    #[test]
    fn search_all_misses() {
        // queries all fall between target values; insertion positions
        assert_eq!(search_u64(vec![10, 20, 30, 40], vec![5, 15, 25, 35]), vec![0, 1, 2, 3]);
    }

    #[test]
    fn search_past_end() {
        // queries greater than all → sentinel = target.len()
        assert_eq!(search_u64(vec![10, 20, 30], vec![100, 200]), vec![3, 3]);
    }

    #[test]
    fn search_empty_target() {
        assert_eq!(search_u64(vec![], vec![1, 2, 3]), vec![0, 0, 0]);
    }

    #[test]
    fn search_empty_queries() {
        assert_eq!(search_u64(vec![1, 2, 3], vec![]), Vec::<u64>::new());
    }

    #[test]
    fn search_mixed() {
        // mix of hits, between-misses, and past-end
        // target = [10, 20, 30, 40, 50]
        // queries:  5   → 0   (before all)
        //           20  → 1   (hit at position 1)
        //           25  → 2   (would insert at 2)
        //           50  → 4   (hit at position 4)
        //           100 → 5   (sentinel)
        assert_eq!(
            search_u64(vec![10, 20, 30, 40, 50], vec![5, 20, 25, 50, 100]),
            vec![0, 1, 2, 4, 5],
        );
    }

    // The next tests exercise the View-aware fast path of `search` and prove
    // that it returns the same answers as the unviewed reference path. This
    // is the key correctness invariant: `view q sel search` ≡ `q sel gather
    // search`, on the same target.

    fn search_qview_u64(target: Vec<u64>, queries: Vec<u64>, qidxs: Vec<u64>) -> Vec<u64> {
        // Build (target, View(queries, Indices(qidxs))) on the stack.
        let q = from_vec::<u64>(queries);
        let qi = from_vec::<u64>(qidxs.clone());
        let viewed = crate::ir::value::view(q, Selector::from_indices(qidxs));
        let mut st: Vec<Value> = vec![from_vec::<u64>(target), viewed];
        let mut env = Vec::new();
        Search.run(&mut st, &mut env).unwrap();
        // Hush unused-qi warning.
        drop(qi);
        match st.pop().unwrap() {
            Value::Prim(Prim::P64(v)) => (*v).clone(),
            other => panic!("expected P64, got {:?}", other),
        }
    }

    #[test]
    fn search_with_view_equiv_to_gather() {
        // queries = [9, 25, 25, 50, 200], qidxs = [0, 2, 4]
        // Viewed queries (positions 0,2,4) = [9, 25, 200]
        // target = [10, 20, 30, 40, 50]
        // Expected: search-for-[9, 25, 200] = [0, 2, 5]
        assert_eq!(
            search_qview_u64(
                vec![10, 20, 30, 40, 50],
                vec![9, 25, 25, 50, 200],
                vec![0, 2, 4],
            ),
            vec![0, 2, 5],
        );
    }

    fn search_tview_u64(target: Vec<u64>, tidxs: Vec<u64>, queries: Vec<u64>) -> Vec<u64> {
        // Build (View(target, Indices(tidxs)), queries) on the stack.
        let t = from_vec::<u64>(target);
        let viewed = crate::ir::value::view(t, Selector::from_indices(tidxs));
        let mut st: Vec<Value> = vec![viewed, from_vec::<u64>(queries)];
        let mut env = Vec::new();
        Search.run(&mut st, &mut env).unwrap();
        match st.pop().unwrap() {
            Value::Prim(Prim::P64(v)) => (*v).clone(),
            other => panic!("expected P64, got {:?}", other),
        }
    }

    #[test]
    fn search_with_target_view_equiv() {
        // target = [10, 100, 20, 100, 30, 100, 40, 100, 50, 100]
        // tidxs = [0, 2, 4, 6, 8]  → viewed target = [10, 20, 30, 40, 50]
        // queries = [25, 50] → expected lower_bound positions: [2, 4]
        assert_eq!(
            search_tview_u64(
                vec![10, 100, 20, 100, 30, 100, 40, 100, 50, 100],
                vec![0, 2, 4, 6, 8],
                vec![25, 50],
            ),
            vec![2, 4],
        );
    }

    #[test]
    fn intersect_list_form_per_row_positions() {
        // Two parallel List<u64> with matching outer length. Each pair of
        // inner rows is sort-merge intersected. Output: TWO List<u64>s of
        // per-list matched positions in each side's source.
        //
        // A = [[1, 2, 3], [4, 5], [10, 20]]   bounds_a = [3, 5, 7]
        // B = [[2, 3, 4], [5],    [10, 30]]   bounds_b = [3, 4, 6]
        // Per-row intersection:
        //   row 0: {1,2,3} ∩ {2,3,4} = positions in a [1, 2], positions in b [0, 1]
        //   row 1: {4, 5}  ∩ {5}     = positions in a [4],     positions in b [3]
        //   row 2: {10,20} ∩ {10,30} = positions in a [5],     positions in b [4]
        // (Source-coord positions, lifted by row offsets.)
        let a_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![3, 5, 7]),
            values: Arc::new(from_vec::<u64>(vec![1, 2, 3, 4, 5, 10, 20])),
        };
        let b_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![3, 4, 6]),
            values: Arc::new(from_vec::<u64>(vec![2, 3, 4, 5, 10, 30])),
        };
        let mut st = vec![a_list, b_list];
        let mut env = Vec::new();
        Intersect.run(&mut st, &mut env).unwrap();
        // Two outputs: per-list positions in A, per-list positions in B.
        assert_eq!(st.len(), 2);
        match (&st[0], &st[1]) {
            (Value::List { bounds: ba, values: va },
             Value::List { bounds: bb, values: vb }) => {
                let bnds_a = crate::ir::shape::bounds_as_u64(ba).unwrap();
                let bnds_b = crate::ir::shape::bounds_as_u64(bb).unwrap();
                assert_eq!(&bnds_a[..], &[0u64, 2, 3, 4]);
                assert_eq!(&bnds_b[..], &[0u64, 2, 3, 4]);
                assert_eq!(**va, from_vec::<u64>(vec![1, 2, 4, 5]));
                assert_eq!(**vb, from_vec::<u64>(vec![0, 1, 3, 4]));
            }
            _ => panic!("expected two Lists"),
        }
    }

    #[test]
    fn intersect_list_form_empty_inner() {
        // Empty inner rows are valid; both outputs have empty rows for them.
        let a_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![0, 2, 2]),       // [], [1,2], []
            values: Arc::new(from_vec::<u64>(vec![1, 2])),
        };
        let b_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![1, 3, 3]),       // [9], [1,3], []
            values: Arc::new(from_vec::<u64>(vec![9, 1, 3])),
        };
        let mut st = vec![a_list, b_list];
        let mut env = Vec::new();
        Intersect.run(&mut st, &mut env).unwrap();
        match (&st[0], &st[1]) {
            (Value::List { bounds: ba, values: va },
             Value::List { bounds: bb, values: vb }) => {
                let bnds_a = crate::ir::shape::bounds_as_u64(ba).unwrap();
                let bnds_b = crate::ir::shape::bounds_as_u64(bb).unwrap();
                // Only row 1 has a match (value 1 at pos 0 in a, pos 1 in b).
                assert_eq!(&bnds_a[..], &[0u64, 0, 1, 1]);
                assert_eq!(&bnds_b[..], &[0u64, 0, 1, 1]);
                assert_eq!(**va, from_vec::<u64>(vec![0]));
                assert_eq!(**vb, from_vec::<u64>(vec![1]));
            }
            _ => panic!("expected two Lists"),
        }
    }

    #[test]
    fn search_list_form_per_row_positions() {
        // (List<u64> target, List<u64> queries) → List<u64> per-list
        // lower-bound positions.
        //
        // target  = [[10, 20, 30],  [5, 15],   [100, 200, 300, 400]]
        // queries = [[10, 25],      [3, 15],   [150, 350]]
        // Per row:
        //   row 0: 10 → 0, 25 → 2          → [0, 2]
        //   row 1: 3 → 0, 15 → 1            → [0, 1]
        //   row 2: 150 → 1, 350 → 3         → [1, 3]
        let t_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![3, 5, 9]),
            values: Arc::new(from_vec::<u64>(vec![10, 20, 30, 5, 15, 100, 200, 300, 400])),
        };
        let q_list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(vec![2, 4, 6]),
            values: Arc::new(from_vec::<u64>(vec![10, 25, 3, 15, 150, 350])),
        };
        let mut st = vec![t_list, q_list];
        let mut env = Vec::new();
        Search.run(&mut st, &mut env).unwrap();
        match &st[0] {
            Value::List { bounds, values } => {
                let bnds = crate::ir::shape::bounds_as_u64(bounds).unwrap();
                assert_eq!(&bnds[..], &[0u64, 2, 4, 6]);
                assert_eq!(**values, from_vec::<u64>(vec![0, 2, 0, 1, 1, 3]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn intersect_flat_positions() {
        // Flat form: sort-merge intersect of two Prim columns, returning
        // matched positions in each. This is the rank-1 case of the same
        // op as the row form (which returns per-list positions).
        let a = from_vec::<u64>(vec![1, 2, 3, 5, 7]);
        let b = from_vec::<u64>(vec![2, 3, 4, 5, 6]);
        let mut st = vec![a, b];
        let mut env = Vec::new();
        Intersect.run(&mut st, &mut env).unwrap();
        assert_eq!(st.len(), 2);
        // Common values: 2, 3, 5 at positions (1,0), (2,1), (3,3).
        assert_eq!(st[0], from_vec::<u64>(vec![1, 2, 3]));
        assert_eq!(st[1], from_vec::<u64>(vec![0, 1, 3]));
    }

    #[test]
    fn intersect_via_enlist_unlist_for_values() {
        // To recover matched VALUES from a flat intersection: gather one
        // side's values at the matched positions.
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        // intersect returns (pos_a, pos_b); gather a's values at pos_a.
        let prog = parse(
            "u64[1 2 3 5 7] u64[2 3 4 5 6] intersect drop \
             u64[1 2 3 5 7] swap gather",
            &reg,
        ).unwrap();
        let (g, _) = crate::pipeline::build(prog).unwrap();
        let st = crate::pipeline::eval_graph(&g).unwrap();
        assert_eq!(st.len(), 1);
        assert_eq!(st[0], from_vec::<u64>(vec![2, 3, 5]));
    }

    #[test]
    fn search_with_range_view_equiv() {
        // target = [99, 99, 10, 20, 30, 40, 50, 99, 99]
        // view.range [2, 7)  → viewed target = [10, 20, 30, 40, 50]
        // queries = [25, 50] → expected positions: [2, 4]
        let t = from_vec::<u64>(vec![99, 99, 10, 20, 30, 40, 50, 99, 99]);
        let viewed = crate::ir::value::view(t, Selector::range(2, 7));
        let queries = from_vec::<u64>(vec![25, 50]);
        let mut st: Vec<Value> = vec![viewed, queries];
        let mut env = Vec::new();
        Search.run(&mut st, &mut env).unwrap();
        let out = match st.pop().unwrap() {
            Value::Prim(Prim::P64(v)) => (*v).clone(),
            other => panic!("expected P64, got {:?}", other),
        };
        assert_eq!(out, vec![2, 4]);
    }
}
