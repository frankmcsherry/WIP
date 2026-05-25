//! `view` op: lazy-gather constructor.
//!
//! Stack effect: `(col, idxs) -> View(col, Indices(idxs))`. The smart
//! constructor in `value::view` collapses `View(View(c, inner), outer)` to a
//! single `View(c, compose(inner, outer))`, so the canonical form is at most
//! one layer of View.
//!
//! Semantics is "lazy gather": for any op that doesn't fast-path View, the
//! materializing `pop()` in `stack.rs` calls `gather(source, selector)` at
//! consumption time. Ops that *do* fast-path View use `pop_raw()` and
//! dispatch on the `Selector` variant.
//!
//! See `examples/10_view_range.col` for a small demo and
//! `examples/17_wco_list_intersect.col` for the motivating use case
//! (the WCO triangle, where view + intersect avoid materializing
//! per-anchor adjacency slices).

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop_raw};
use crate::ir::typecheck::{Op, Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, Selector, view};
use crate::ir::shape::Shape;

/// `view` — polymorphic lazy-gather constructor. Dispatches on input shape:
///
///   - `(Prim<T>, P64-positions) -> View<Prim<T>, Indices>`. Selects
///     individual elements; resulting view has the same shape as source.
///   - `(List<T>, P64-positions) -> View<Prim<T>, SequenceRange>`. Selects
///     rows of the list; resulting view has shape `List<T>` (row-shaped),
///     with `los`/`his` derived from the list's bounds. Zero allocation —
///     the inner Prim is shared by reference.
///
/// In both cases, positions are interpreted as logical units of the input
/// (elements for Prim, rows for List).
#[derive(Debug)] pub struct View;

impl PrimOp for View {
    fn name(&self) -> &str { "view" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (source, positions) → view
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let pos_v = crate::ir::stack::pop(st)?;
        let pos = match pos_v {
            Value::Prim(Prim::P64(v)) => v,
            other => return Err(format!(
                "view: positions must be Prim(P64), got {:?}", other
            )),
        };
        let source = pop_raw(st)?;
        // Dispatch on source shape: Prim → Indices view (collapses nested
        // views via the smart constructor), List → SequenceRange view (zero
        // alloc per-row sub-list-view).
        let source_shape = crate::ir::shape::shape_of(&source);
        match source_shape {
            Shape::Prim(_) => {
                // Build a flat Indices view; the smart constructor in
                // `value::view` collapses if source is itself a View.
                let sel = Selector::Indices(pos);
                st.push(view(source, sel));
                Ok(())
            }
            Shape::List { .. } => {
                // Row-shaped View: descend to the underlying Prim and
                // build a `SequenceRange` selector. Materialize first if
                // source is a View (we need owned bounds info).
                //
                // The `BoundsRepr::Runs` + `View<Runs>` retirement
                // target for #53 was attempted but reverted: even with
                // a shared runs Arc and skipped alignment check, the
                // (u64, u64) stride access pattern showed ~10% WCO
                // regression vs the parallel-arrays los/his form. The
                // scaffolding (`ListAccess::Runs`, decompose-view on
                // `List<View<Runs>>`, the `BoundsRepr::Runs` variant
                // itself) is left in place — they're dormant in this
                // construction path but available for a future tuned
                // implementation that closes the perf gap.
                let source = crate::ir::stack::materialize_top(source)?;
                let (bounds, values) = match source {
                    Value::List { bounds, values } => (bounds, values),
                    _ => unreachable!("shape_of said List but value wasn't"),
                };
                let inner = match (*values).clone() {
                    Value::Prim(p) => p,
                    other => return Err(format!(
                        "view: list inner must be Prim, got {:?}", other
                    )),
                };
                let bnds = crate::ir::shape::bounds_as_u64(&bounds)?;
                let row_count = bnds.len().saturating_sub(1);
                let mut los: Vec<u64> = Vec::with_capacity(pos.len());
                let mut his: Vec<u64> = Vec::with_capacity(pos.len());
                for &k in pos.iter() {
                    let k = k as usize;
                    if k >= row_count {
                        return Err(format!(
                            "view: list row index {} out of bounds (list has {} rows)",
                            k, row_count
                        ));
                    }
                    los.push(bnds[k]);
                    his.push(bnds[k + 1]);
                }
                st.push(Value::View {
                    source: Arc::new(Value::Prim(inner)),
                    selector: Selector::SequenceRange {
                        los: Arc::new(los),
                        his: Arc::new(his),
                    },
                });
                Ok(())
            }
            other => Err(format!(
                "view: source must be Prim or List, got {}", other
            )),
        }
    }
}

impl Typed for View {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let pos = tc_pop(st, "view")?;
        if pos != Shape::Prim(PrimWidth::W64) {
            return Err(format!("view: positions must be Prim(P64), got {}", pos));
        }
        let source = tc_pop(st, "view")?;
        match source {
            Shape::Prim(_) => {
                // Flat view: shape matches source.
                st.push(source);
                Ok(())
            }
            Shape::List { inner, .. } => {
                // Row-shaped view: shape is List<inner>, same as the source's
                // shape after row selection.
                st.push(Shape::List {
                    bounds: PrimWidth::W64,
                    inner,
                });
                Ok(())
            }
            other => Err(format!(
                "view: source must be Prim or List, got {}", other
            )),
        }
    }
}

/// `view.range` — view of a contiguous sub-range `[lo, hi)`. Reads two P64
/// scalars off the stack (length-1 each).
///
/// Stack: `col lo hi -- View(col, Range{lo,hi})`.
///
/// The `Range` selector lets reductive ops skip the indirection entirely —
/// they can read the source's underlying slice with an offset rather than
/// dereferencing per-element through an indices vector.
#[derive(Debug)] pub struct ViewRange;

impl PrimOp for ViewRange {
    fn name(&self) -> &str { "view.range" }
    fn arity(&self) -> Option<(usize, usize)> { Some((3, 1)) }  // (source, lo, hi) → view
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let hi_v = crate::ir::stack::pop(st)?;
        let lo_v = crate::ir::stack::pop(st)?;
        let hi = match hi_v {
            Value::Prim(Prim::P64(v)) if v.len() == 1 => v[0],
            other => return Err(format!("view.range: hi must be length-1 P64, got {:?}", other)),
        };
        let lo = match lo_v {
            Value::Prim(Prim::P64(v)) if v.len() == 1 => v[0],
            other => return Err(format!("view.range: lo must be length-1 P64, got {:?}", other)),
        };
        if hi < lo {
            return Err(format!("view.range: hi {} < lo {}", hi, lo));
        }
        let source = pop_raw(st)?;
        let sel = Selector::range(lo, hi);
        st.push(view(source, sel));
        Ok(())
    }
}

impl Typed for ViewRange {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let hi = tc_pop(st, "view.range")?;
        let lo = tc_pop(st, "view.range")?;
        if hi != Shape::Prim(PrimWidth::W64) {
            return Err(format!("view.range: hi must be Prim(P64), got {}", hi));
        }
        if lo != Shape::Prim(PrimWidth::W64) {
            return Err(format!("view.range: lo must be Prim(P64), got {}", lo));
        }
        let source = tc_pop(st, "view.range")?;
        st.push(source);
        Ok(())
    }
}

/// `decompose-view` — zero-cost View elim. Symmetric counterpart to `view` /
/// `view.range`. Pops a `Value::View { source, selector }` and pushes
/// `(source, selector_as_P64)`. This is the operator-algebra elim move that
/// completes the View shape's symmetry with `flatten` (List elim), `split`
/// (Sum elim), and `.i` (Prod elim).
///
/// Stack effect: `View<T> -> T, P64`. The selector is materialized as a P64
/// of source positions:
///
///   - `Indices(idxs)`         → push idxs as P64 directly (no copy via Arc).
///   - `Range { start, end }`  → push `[start, start+1, …, end-1]` as P64
///                               (materializes the iota; one alloc).
///
/// Use to back-project from per-row positional outputs (matched positions
/// in some op's result) into the original source-coordinate index space.
/// The view carries the source frame; decompose recovers `(source, indices)`
/// where the indices are already in source coordinates.
///
/// Errors at runtime if the popped value isn't a View. The typechecker
/// can't enforce this (View is shape-transparent), so the check is
/// runtime-only.
#[derive(Debug)] pub struct DecomposeView;

impl PrimOp for DecomposeView {
    fn name(&self) -> &str { "decompose-view" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 2)) }  // view → (source, positions)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop_raw(st)?;
        // The retired-SequenceRange form: `List<bounds, View<Prim, Runs>>`.
        // Decompose to (source, List<P64> of per-row position iotas) —
        // same shape and semantics as the prior SequenceRange path.
        if let Value::List { bounds, values } = &v {
            if let Value::View { source: inner_src, selector: Selector::Runs(runs) } = values.as_ref() {
                let bnds = crate::ir::shape::bounds_as_u64(bounds)?;
                let mut new_bounds: Vec<u64> = Vec::with_capacity(runs.len());
                let mut new_values: Vec<u64> = Vec::new();
                let mut acc: u64 = 0;
                for &(lo, hi) in runs.iter() {
                    for j in lo..hi { new_values.push(j); }
                    acc += hi.saturating_sub(lo);
                    new_bounds.push(acc);
                }
                let _ = bnds; // alignment validated downstream when needed
                st.push((**inner_src).clone());
                st.push(Value::List {
                    bounds: crate::ir::value::bounds_var_from_ends(new_bounds),
                    values: Arc::new(crate::ir::value::from_vec::<u64>(new_values)),
                });
                return Ok(());
            }
        }
        match v {
            Value::View { source, selector } => {
                match selector {
                    Selector::Indices(idxs) => {
                        // Flat: push source, push P64 of positions.
                        st.push((*source).clone());
                        st.push(Value::Prim(Prim::P64(idxs)));
                    }
                    Selector::Runs(runs) => {
                        // Flat union of intervals: materialize concatenated
                        // positions as P64.
                        let total: usize = runs.iter().map(|(lo, hi)| (hi - lo) as usize).sum();
                        let mut v: Vec<u64> = Vec::with_capacity(total);
                        for (lo, hi) in runs.iter() {
                            for j in *lo..*hi { v.push(j); }
                        }
                        st.push((*source).clone());
                        st.push(Value::Prim(Prim::P64(Arc::new(v))));
                    }
                    Selector::Mask(m) => {
                        // Scan mask, materialize positions where set.
                        let mut v: Vec<u64> = Vec::new();
                        for (i, &b) in m.iter().enumerate() {
                            if b != 0 { v.push(i as u64); }
                        }
                        st.push((*source).clone());
                        st.push(Value::Prim(Prim::P64(Arc::new(v))));
                    }
                    Selector::SequenceRange { los, his } => {
                        // Row-shaped: push source, push List<u64> where
                        // each row is iota[los[i]..his[i]) — the per-row
                        // position lists in source coordinates. This is
                        // back-projection: each row's positions are global
                        // positions in the source's flat space.
                        let mut bounds: Vec<u64> = Vec::with_capacity(los.len());
                        let mut values: Vec<u64> = Vec::new();
                        let mut acc: u64 = 0;
                        for i in 0..los.len() {
                            let lo = los[i];
                            let hi = his[i];
                            for j in lo..hi { values.push(j); }
                            acc += hi.saturating_sub(lo);
                            bounds.push(acc);
                        }
                        let positions_list = Value::List {
                            bounds: crate::ir::value::bounds_var_from_ends(bounds),
                            values: Arc::new(crate::ir::value::from_vec::<u64>(values)),
                        };
                        st.push((*source).clone());
                        st.push(positions_list);
                    }
                }
                Ok(())
            }
            other => Err(format!("decompose-view: expected View, got {:?}", other)),
        }
    }
}

impl Typed for DecomposeView {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "decompose-view")?;
        match v {
            // Flat selector view: push source's shape + P64.
            Shape::Prim(_) | Shape::Prod(_) | Shape::Sum { .. } => {
                st.push(v);
                st.push(Shape::Prim(PrimWidth::W64));
                Ok(())
            }
            // SequenceRange view: shape is List<inner>. Source is Prim of
            // inner's width. Position lists are List<P64>.
            Shape::List { inner, .. } => {
                match *inner {
                    Shape::Prim(w) => {
                        st.push(Shape::Prim(w));
                        st.push(Shape::List {
                            bounds: PrimWidth::W64,
                            inner: Box::new(Shape::Prim(PrimWidth::W64)),
                        });
                        Ok(())
                    }
                    other => Err(format!(
                        "decompose-view: List inner must be Prim (only SequenceRange views \
                         can be decomposed), got List<{}>", other
                    )),
                }
            }
        }
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "view"            => Some(Box::new(View)),
            "view.range"      => Some(Box::new(ViewRange)),
            "decompose-view"  => Some(Box::new(DecomposeView)),
            _ => None,
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::{from_vec, Prim};
    use std::sync::Arc;

    #[test]
    fn view_constructs_indexed() {
        // col=[10,20,30,40,50], idxs=[0,2,4]; expect View on top with len 3.
        let col = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let idxs = Value::Prim(Prim::P64(Arc::new(vec![0, 2, 4])));
        let mut st = vec![col, idxs];
        let mut env = Vec::new();
        View.run(&mut st, &mut env).unwrap();
        assert_eq!(st.len(), 1);
        match &st[0] {
            Value::View { selector, .. } => assert_eq!(selector.len(), 3),
            _ => panic!("expected View on top"),
        }
    }

    #[test]
    fn view_collapses_on_double_view() {
        // (((col[0,2,4]) [1])) should collapse to col[2] — single layer.
        let col = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let idxs1 = Value::Prim(Prim::P64(Arc::new(vec![0, 2, 4])));
        let idxs2 = Value::Prim(Prim::P64(Arc::new(vec![1])));
        let mut st = vec![col, idxs1];
        let mut env = Vec::new();
        View.run(&mut st, &mut env).unwrap();
        st.push(idxs2);
        View.run(&mut st, &mut env).unwrap();
        // Top of stack: a single View whose selector indexes into the
        // *original* col directly (composed).
        match &st[0] {
            Value::View { source, selector } => {
                // Source should be the original col, not a nested View.
                assert!(!matches!(source.as_ref(), Value::View { .. }));
                // Composed selector should pick col[2] = 30.
                let positions = selector.to_usize_vec();
                assert_eq!(positions, vec![2]);
            }
            _ => panic!("expected View on top"),
        }
    }

    #[test]
    fn pop_materializes_view() {
        // After constructing a View, calling pop should materialize it.
        let col = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let idxs = Value::Prim(Prim::P64(Arc::new(vec![0, 2, 4])));
        let mut st = vec![col, idxs];
        let mut env = Vec::new();
        View.run(&mut st, &mut env).unwrap();
        // The View is on top. pop() should materialize it.
        let v = crate::ir::stack::pop(&mut st).unwrap();
        assert_eq!(v, from_vec::<u64>(vec![10, 30, 50]));
    }

    #[test]
    fn view_range_constructs() {
        let col = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let lo = from_vec::<u64>(vec![1]);
        let hi = from_vec::<u64>(vec![4]);
        let mut st = vec![col, lo, hi];
        let mut env = Vec::new();
        ViewRange.run(&mut st, &mut env).unwrap();
        assert_eq!(st.len(), 1);
        match &st[0] {
            Value::View { selector: Selector::Runs(runs), .. } if runs.as_slice() == [(1, 4)] => {}
            other => panic!("expected View Runs([(1,4)]), got {:?}", other),
        }
        // Materialize to confirm: pop should yield [20, 30, 40].
        let v = crate::ir::stack::pop(&mut st).unwrap();
        assert_eq!(v, from_vec::<u64>(vec![20, 30, 40]));
    }

    // End-to-end programs exercising view + downstream op equivalence.
    fn run_program(src: &str) -> Vec<Value> {
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        use crate::ir::shape::Shape;
        use crate::ir::typecheck::typecheck;
        let reg = OpRegistry::standard();
        let prog = parse(src, &reg).unwrap();
        let mut ts: Vec<Shape> = Vec::new();
        let mut tenv: Vec<Shape> = Vec::new();
        typecheck(&prog, &mut ts, &mut tenv).unwrap();
        let mut st: Vec<Value> = Vec::new();
        let mut env: Vec<Value> = Vec::new();
        crate::ir::op::eval(&prog, &mut st, &mut env).unwrap();
        st
    }

    #[test]
    fn view_reduce_add_equiv_to_gather() {
        // Eager: gather then sum.
        let eager = run_program(
            "u64[10 20 30 40 50] u64[0 2 4] gather reduce.+.u64"
        );
        // Lazy: view then sum (uses the view-aware path).
        let lazy = run_program(
            "u64[10 20 30 40 50] u64[0 2 4] view reduce.+.u64"
        );
        // Both should sum 10+30+50 = 90.
        assert_eq!(eager, lazy);
        assert_eq!(eager[0], from_vec::<u64>(vec![90]));
    }

    #[test]
    fn view_where_equiv_to_gather() {
        // Both arms apply a mask via the new `where gather` pattern.
        // The parser fuses to `filter`, which produces a View<Mask>.
        // Materialize via pop on each.
        fn materialize_top(prog: &str) -> Value {
            let reg = crate::syntax::registry::OpRegistry::standard();
            let parsed = crate::syntax::parse::parse(prog, &reg).unwrap();
            let mut st = vec![];
            let mut env = Vec::new();
            crate::ir::op::eval(&parsed, &mut st, &mut env).unwrap();
            crate::ir::stack::pop(&mut st).unwrap()
        }
        let eager = materialize_top(
            "u64[10 20 30 40 50] u64[0 2 4] gather bool[t f t] where gather"
        );
        let lazy = materialize_top(
            "u64[10 20 30 40 50] u64[0 2 4] view bool[t f t] where gather"
        );
        assert_eq!(eager, lazy);
        assert_eq!(eager, from_vec::<u64>(vec![10, 50]));
    }

    #[test]
    fn view_range_reduce_equiv() {
        // Eager: gather [2..5) then sum.
        let eager = run_program(
            "u64[99 99 10 20 30 99 99] u64[2 3 4] gather reduce.+.u64"
        );
        // Lazy: view.range [2, 5) then sum.
        let lazy = run_program(
            "u64[99 99 10 20 30 99 99] 2u64 5u64 view.range reduce.+.u64"
        );
        assert_eq!(eager, lazy);
        assert_eq!(eager[0], from_vec::<u64>(vec![60]));
    }

    #[test]
    fn decompose_view_indices() {
        // view → decompose-view yields (source, selector-as-P64).
        let st = run_program(
            "u64[10 20 30 40 50] u64[0 2 4] view decompose-view"
        );
        assert_eq!(st.len(), 2);
        // Bottom: source (the original column). Top: selector as P64.
        assert_eq!(st[0], from_vec::<u64>(vec![10, 20, 30, 40, 50]));
        assert_eq!(st[1], from_vec::<u64>(vec![0, 2, 4]));
    }

    #[test]
    fn decompose_view_range() {
        // view.range → decompose-view materializes the range as a P64 iota.
        let st = run_program(
            "u64[10 20 30 40 50] 1u64 4u64 view.range decompose-view"
        );
        assert_eq!(st.len(), 2);
        assert_eq!(st[0], from_vec::<u64>(vec![10, 20, 30, 40, 50]));
        assert_eq!(st[1], from_vec::<u64>(vec![1, 2, 3]));
    }

    #[test]
    fn decompose_view_sequence_range() {
        // Build a list, view some rows, decompose. Expect (source, per-row
        // position lists).
        //
        // list = [[1,2,3], [4,5], [6,7,8,9]]
        //   bounds = [3, 5, 9], values = [1..9]
        // view rows [0, 2] → View<values, SequenceRange{los=[0,5], his=[3,9]}>
        // decompose-view → source=values, positions=List<[0,1,2], [5,6,7,8]>
        let st = run_program(
            "u64[1 2 3 4 5 6 7 8 9] u64[0 3 5 9] nest u64[0 2] view decompose-view"
        );
        assert_eq!(st.len(), 2);
        // [0]: the source values column.
        assert_eq!(st[0], from_vec::<u64>(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]));
        // [1]: List<u64> with per-row position iotas.
        match &st[1] {
            Value::List { bounds, values } => {
                let bnds = crate::ir::shape::bounds_as_u64(bounds).unwrap();
                assert_eq!(&bnds[..], &[0u64, 3, 7]);
                assert_eq!(**values, from_vec::<u64>(vec![0, 1, 2, 5, 6, 7, 8]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }
}
