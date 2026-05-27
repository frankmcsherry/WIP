//! Slicing and concatenation — `concat`, `take`, `skip`, `cat.N`,
//! `reverse`. The "ergonomic shortcut" family: each bundles a
//! position-producing survey with a `gather` (see the Idioms section in
//! `PRINCIPLES.md`).
//!
//! The polymorphic `sort` and `sort.perm` live in `sort.rs`. Typed sort
//! (`sort.<i>`) is retired — order-aware sorting is the user-space
//! `enswizzle.<i> sort deswizzle.<i>`.

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use std::sync::Arc;
use crate::ir::value::{Value, Prim, PrimWidth, bounds_var_from_ends};
use crate::ir::shape::Shape;
use crate::ops::helpers::{concat_values, gather};

// Typed sort (`sort.<interp>`) is retired: order-aware sorting is the
// user-space composition `enswizzle.<i> sort deswizzle.<i>` (the swizzle
// makes unsigned byte order match the interp's order). `sort_tests` below
// verifies that composition for floats.
#[cfg(test)]
mod sort_tests {
    use super::*;
    use crate::ir::value::from_vec;
    use crate::ir::shape::Interp;
    use crate::ops::swizzle::{Enswizzle, Deswizzle};
    use crate::ops::sort::SortPoly;

    fn swizzle_sort(v: Value, interp: Interp) -> Value {
        let mut st = vec![v];
        let mut env = Vec::new();
        Enswizzle { interp }.run(&mut st, &mut env).unwrap();
        SortPoly.run(&mut st, &mut env).unwrap();
        Deswizzle { interp }.run(&mut st, &mut env).unwrap();
        st.pop().unwrap()
    }

    #[test]
    fn swizzle_sort_f64_totalorder() {
        let v = from_vec::<f64>(vec![3.14, -1.5, 2.71, -0.5, 0.0]);
        assert_eq!(swizzle_sort(v, Interp::F64), from_vec::<f64>(vec![-1.5, -0.5, 0.0, 2.71, 3.14]));
    }

    #[test]
    fn swizzle_sort_f32_negatives_first() {
        let v = from_vec::<f32>(vec![1.0, -1.0, 0.0]);
        assert_eq!(swizzle_sort(v, Interp::F32), from_vec::<f32>(vec![-1.0, 0.0, 1.0]));
    }
}

#[cfg(test)]
mod segmented_tests {
    use super::*;
    use crate::ir::value::{from_vec, list, bounds_var_from_ends};
    use crate::ops::sort::SortSegmented;

    fn list_u64(ends: Vec<u64>, vals: Vec<u64>) -> Value {
        list(bounds_var_from_ends(ends), from_vec::<u64>(vals))
    }
    fn run1(op: &dyn PrimOp, stack: Vec<Value>) -> Value {
        let mut st = stack;
        let mut env = Vec::new();
        op.run(&mut st, &mut env).unwrap();
        st.pop().unwrap()
    }

    #[test]
    fn top_k_per_group_pipeline() {
        // Rows [3,1,2] | [5,4] — the example-15 pipeline at small scale:
        // sort.segmented → [1,2,3] | [4,5]; reverse.segmented → [3,2,1] | [5,4];
        // take.segmented 2 → [3,2] | [5,4].
        let v = list_u64(vec![3, 5], vec![3, 1, 2, 5, 4]);
        let v = run1(&SortSegmented, vec![v]);
        assert_eq!(v, list_u64(vec![3, 5], vec![1, 2, 3, 4, 5]));
        let v = run1(&ReverseSegmented, vec![v]);
        assert_eq!(v, list_u64(vec![3, 5], vec![3, 2, 1, 5, 4]));
        let v = run1(&TakeSegmented, vec![v, from_vec::<u64>(vec![2])]);
        // Row 0 had 3 elems → first 2; row 1 had 2 → kept whole.
        assert_eq!(v, list_u64(vec![2, 4], vec![3, 2, 5, 4]));
    }
}

/// Concat: List<T> -> T. Drops outer list structure, returns the flat values.
/// (Equivalent to `flatten drop`.)
#[derive(Debug)] pub struct Concat;
impl PrimOp for Concat {
    fn name(&self) -> &str { "concat" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { concat_run(st) }
}
impl Typed for Concat {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { concat_tc(st) }
}
pub fn concat_run(st: &mut Stack) -> Result<(), String> {
    let v = pop(st)?;
    match v {
        Value::List { values, .. } => { st.push((*values).clone()); Ok(()) }
        other => Err(format!("concat: not a list: {:?}", other)),
    }
}
pub fn concat_tc(st: &mut TypeStack) -> Result<(), String> {
    let v = tc_pop(st, "concat")?;
    match v {
        Shape::List { inner, .. } => { st.push(*inner); Ok(()) }
        other => Err(format!("concat: not a list: {}", other)),
    }
}

/// Take the first n rows of a list (or a flat column).
#[derive(Debug)] pub struct Take;
impl PrimOp for Take {
    fn name(&self) -> &str { "take" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (v, n) → first-n
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { take_run(st) }
}
impl Typed for Take {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { take_tc(st) }
}
pub fn take_run(st: &mut Stack) -> Result<(), String> {
    let n_v = pop(st)?;
    let n = match n_v {
        Value::Prim(crate::ir::value::Prim::P64(x)) if x.len() == 1 => x[0] as usize,
        other => return Err(format!("take: needs length-1 P64, got {:?}", other)),
    };
    let v = pop(st)?;
    let n = n.min(v.len());
    let idxs: Vec<usize> = (0..n).collect();
    st.push(gather(&v, &idxs)?);
    Ok(())
}
pub fn take_tc(st: &mut TypeStack) -> Result<(), String> {
    let n_t = tc_pop(st, "take")?;
    if n_t != Shape::Prim(crate::ir::value::PrimWidth::W64) { return Err(format!("take: count must be Prim(P64), got {}", n_t)); }
    let v = tc_pop(st, "take")?;
    st.push(v);
    Ok(())
}

/// Skip the first n rows.
#[derive(Debug)] pub struct Skip;
impl PrimOp for Skip {
    fn name(&self) -> &str { "skip" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (v, n) → skip-first-n
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { skip_run(st) }
}
impl Typed for Skip {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { skip_tc(st) }
}
pub fn skip_run(st: &mut Stack) -> Result<(), String> {
    let n_v = pop(st)?;
    let n = match n_v {
        Value::Prim(crate::ir::value::Prim::P64(x)) if x.len() == 1 => x[0] as usize,
        other => return Err(format!("skip: needs length-1 P64, got {:?}", other)),
    };
    let v = pop(st)?;
    let n = n.min(v.len());
    let total = v.len();
    let idxs: Vec<usize> = (n..total).collect();
    st.push(gather(&v, &idxs)?);
    Ok(())
}
pub fn skip_tc(st: &mut TypeStack) -> Result<(), String> {
    let n_t = tc_pop(st, "skip")?;
    if n_t != Shape::Prim(crate::ir::value::PrimWidth::W64) { return Err(format!("skip: count must be Prim(P64), got {}", n_t)); }
    let v = tc_pop(st, "skip")?;
    st.push(v);
    Ok(())
}

/// `cat.N` — vertical-stack N same-shape values. Pops N values; pushes one
/// with row-count equal to the sum of their lengths and the same shape.
///
/// Prim: concatenate the slices (allocates one fresh buffer).
/// Prod: recurses field-wise; result is a Prod of concatenated fields.
/// List: concatenates the inner values and re-bases the bounds.
///
/// Sum is currently unsupported (would need lane-relative bookkeeping —
/// follow up if needed).
///
/// Stack effect: `v_0 v_1 … v_{N-1} -- concat`. Order is preserved (v_0 is
/// the first segment of the result).
#[derive(Debug)] pub struct CatN { pub n: usize }
impl PrimOp for CatN {
    fn name(&self) -> &str { "cat" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { cat_run(self.n, st) }
}
impl Typed for CatN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { cat_tc(self.n, st) }
}
pub fn cat_run(n: usize, st: &mut Stack) -> Result<(), String> {
    if n == 0 {
        return Err("cat.0: no inputs to concatenate".into());
    }
    if st.len() < n {
        return Err(format!("cat.{}: stack underflow ({} on stack)", n, st.len()));
    }
    let mut parts: Vec<Value> = Vec::with_capacity(n);
    for _ in 0..n { parts.push(pop(st)?); }
    parts.reverse();
    st.push(concat_values(&parts)?);
    Ok(())
}
pub fn cat_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
    if n == 0 {
        return Err("cat.0: no inputs to concatenate".into());
    }
    if st.len() < n {
        return Err(format!("cat.{}: stack underflow ({} types on stack)", n, st.len()));
    }
    // All N inputs must share the same Shape. Pop them off, check, push
    // one copy back as the result shape.
    let mut shapes: Vec<Shape> = Vec::with_capacity(n);
    for _ in 0..n { shapes.push(st.pop().unwrap()); }
    shapes.reverse();
    let first = shapes[0].clone();
    for (i, s) in shapes.iter().enumerate().skip(1) {
        if *s != first {
            return Err(format!(
                "cat.{}: input {} has shape {}, expected {}",
                n, i, s, first
            ));
        }
    }
    st.push(first);
    Ok(())
}

/// Reverse a Prim column (or each field of a Prod). Outer row order is
/// reversed. For Lists, reverse the row order (bounds recomputed).
#[derive(Debug)] pub struct Reverse;
impl PrimOp for Reverse {
    fn name(&self) -> &str { "reverse" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { reverse_run(st) }
}
impl Typed for Reverse {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { reverse_tc(st) }
}
pub fn reverse_run(st: &mut Stack) -> Result<(), String> {
    let v = pop(st)?;
    let n = v.len();
    let idxs: Vec<usize> = (0..n).rev().collect();
    st.push(gather(&v, &idxs)?);
    Ok(())
}
pub fn reverse_tc(st: &mut TypeStack) -> Result<(), String> {
    let v = tc_pop(st, "reverse")?;
    st.push(v);
    Ok(())
}

/// `reverse.segmented` — reverse the elements *within* each row of a List
/// (outer row order and per-row counts unchanged). The per-row sibling of
/// flat `reverse`; with `sort.segmented` it makes per-group descending order.
#[derive(Debug)] pub struct ReverseSegmented;
impl PrimOp for ReverseSegmented {
    fn name(&self) -> &str { "reverse.segmented" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { reverse_seg_run(st) }
}
impl Typed for ReverseSegmented {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { reverse_seg_tc(st) }
}
pub fn reverse_seg_run(st: &mut Stack) -> Result<(), String> {
    let v = pop(st)?;
    match v {
        Value::List { bounds, values } => {
            let mut idxs: Vec<usize> = Vec::with_capacity(values.len());
            for (lo, hi) in bounds.iter_pairs() {
                for j in (lo..hi).rev() { idxs.push(j as usize); }
            }
            let new_vals = gather(values.as_ref(), &idxs)?;
            st.push(Value::List { bounds, values: Arc::new(new_vals) });
            Ok(())
        }
        other => Err(format!("reverse.segmented: expected List, got {:?}", other)),
    }
}
pub fn reverse_seg_tc(st: &mut TypeStack) -> Result<(), String> {
    let v = tc_pop(st, "reverse.segmented")?;
    match v {
        Shape::List { .. } => { st.push(v); Ok(()) }
        other => Err(format!("reverse.segmented: expected List, got {}", other)),
    }
}

/// `take.segmented` — keep the first `n` elements of *each* row of a List
/// (per-row head). Pops `n` (length-1 P64) and the List; rows shorter than
/// `n` are kept whole. The per-row sibling of flat `take`.
#[derive(Debug)] pub struct TakeSegmented;
impl PrimOp for TakeSegmented {
    fn name(&self) -> &str { "take.segmented" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { take_seg_run(st) }
}
impl Typed for TakeSegmented {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { take_seg_tc(st) }
}
pub fn take_seg_run(st: &mut Stack) -> Result<(), String> {
    let n = match pop(st)? {
        Value::Prim(Prim::P64(x)) if x.len() == 1 => x[0] as usize,
        other => return Err(format!("take.segmented: n must be length-1 P64, got {:?}", other)),
    };
    let v = pop(st)?;
    match v {
        Value::List { bounds, values } => {
            let mut idxs: Vec<usize> = Vec::new();
            let mut ends: Vec<u64> = Vec::with_capacity(bounds.len());
            for (lo, hi) in bounds.iter_pairs() {
                let keep = ((hi - lo) as usize).min(n);
                for j in 0..keep { idxs.push(lo as usize + j); }
                ends.push(idxs.len() as u64);
            }
            let new_vals = gather(values.as_ref(), &idxs)?;
            st.push(Value::List { bounds: bounds_var_from_ends(ends), values: Arc::new(new_vals) });
            Ok(())
        }
        other => Err(format!("take.segmented: expected List, got {:?}", other)),
    }
}
pub fn take_seg_tc(st: &mut TypeStack) -> Result<(), String> {
    let n_t = tc_pop(st, "take.segmented")?;
    if n_t != Shape::Prim(PrimWidth::W64) { return Err(format!("take.segmented: count must be Prim(P64), got {}", n_t)); }
    let v = tc_pop(st, "take.segmented")?;
    match v {
        Shape::List { .. } => { st.push(v); Ok(()) }
        other => Err(format!("take.segmented: expected List, got {}", other)),
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "concat" => Some(Box::new(Concat)),
            "take" => Some(Box::new(Take)),
            "skip" => Some(Box::new(Skip)),
            "reverse" => Some(Box::new(Reverse)),
            "take.segmented" => Some(Box::new(TakeSegmented)),
            "reverse.segmented" => Some(Box::new(ReverseSegmented)),
            _ => None,
        }
    });
    // `cat.N` — concatenate N same-shape values.
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let rest = t.strip_prefix("cat.")?;
        let n: usize = rest.parse().ok()?;
        if n == 0 { return None; }
        Some(Box::new(CatN { n }))
    });
}

