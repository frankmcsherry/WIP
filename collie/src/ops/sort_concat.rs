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
use crate::ir::value::Value;
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

/// Concat: List<T> -> T. Drops outer list structure, returns the flat values.
/// (Equivalent to `flatten drop`.)
#[derive(Debug)] pub struct Concat;
impl PrimOp for Concat {
    fn name(&self) -> &str { "concat" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { values, .. } => { st.push((*values).clone()); Ok(()) }
            other => Err(format!("concat: not a list: {:?}", other)),
        }
    }
}
impl Typed for Concat {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "concat")?;
        match v {
            Shape::List { inner, .. } => { st.push(*inner); Ok(()) }
            other => Err(format!("concat: not a list: {}", other)),
        }
    }
}

/// Take the first n rows of a list (or a flat column).
#[derive(Debug)] pub struct Take;
impl PrimOp for Take {
    fn name(&self) -> &str { "take" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (v, n) → first-n
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
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
}
impl Typed for Take {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let n_t = tc_pop(st, "take")?;
        if n_t != Shape::Prim(crate::ir::value::PrimWidth::W64) { return Err(format!("take: count must be Prim(P64), got {}", n_t)); }
        let v = tc_pop(st, "take")?;
        st.push(v);
        Ok(())
    }
}

/// Skip the first n rows.
#[derive(Debug)] pub struct Skip;
impl PrimOp for Skip {
    fn name(&self) -> &str { "skip" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (v, n) → skip-first-n
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
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
}
impl Typed for Skip {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let n_t = tc_pop(st, "skip")?;
        if n_t != Shape::Prim(crate::ir::value::PrimWidth::W64) { return Err(format!("skip: count must be Prim(P64), got {}", n_t)); }
        let v = tc_pop(st, "skip")?;
        st.push(v);
        Ok(())
    }
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        if self.n == 0 {
            return Err("cat.0: no inputs to concatenate".into());
        }
        if st.len() < self.n {
            return Err(format!("cat.{}: stack underflow ({} on stack)", self.n, st.len()));
        }
        let mut parts: Vec<Value> = Vec::with_capacity(self.n);
        for _ in 0..self.n { parts.push(pop(st)?); }
        parts.reverse();
        st.push(concat_values(&parts)?);
        Ok(())
    }
}
impl Typed for CatN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if self.n == 0 {
            return Err("cat.0: no inputs to concatenate".into());
        }
        if st.len() < self.n {
            return Err(format!("cat.{}: stack underflow ({} types on stack)", self.n, st.len()));
        }
        // All N inputs must share the same Shape. Pop them off, check, push
        // one copy back as the result shape.
        let mut shapes: Vec<Shape> = Vec::with_capacity(self.n);
        for _ in 0..self.n { shapes.push(st.pop().unwrap()); }
        shapes.reverse();
        let first = shapes[0].clone();
        for (i, s) in shapes.iter().enumerate().skip(1) {
            if *s != first {
                return Err(format!(
                    "cat.{}: input {} has shape {}, expected {}",
                    self.n, i, s, first
                ));
            }
        }
        st.push(first);
        Ok(())
    }
}

/// Reverse a Prim column (or each field of a Prod). Outer row order is
/// reversed. For Lists, reverse the row order (bounds recomputed).
#[derive(Debug)] pub struct Reverse;
impl PrimOp for Reverse {
    fn name(&self) -> &str { "reverse" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let n = v.len();
        let idxs: Vec<usize> = (0..n).rev().collect();
        st.push(gather(&v, &idxs)?);
        Ok(())
    }
}
impl Typed for Reverse {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "reverse")?;
        st.push(v);
        Ok(())
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

