//! Reductions over boolean columns — `any`, `all`, `not`, `and`,
//! `or`. The typed numeric reductions (`reduce.+`, `reduce.*`,
//! `reduce.min`, `reduce.max`) live in `list.rs` alongside the
//! shape-inspection ops they share inner kernels with.
//!
//! Same shape as those: consume a flat column, or a `List`
//! per-row, produce one value per row.

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Storage, from_vec, PrimWidth};
use crate::ir::shape::{Interp, Shape, bounds_as_u64};

/// Per-run reducer. Seeds the accumulator with `xs[prev]` to dodge
/// neutral-value pitfalls (e.g. min/max with `Default::default()` is wrong).
/// Empty rows are rejected — call sites should pre-filter or use a different
/// op if zero-row semantics are needed.
fn fold_runs<T: Storage, F: Fn(T, T) -> T>(
    values: &Value, bounds: &[u64], f: F,
) -> Result<Value, String> {
    let p = match values {
        Value::Prim(p) => p,
        _ => return Err("reducer: expected Prim".into()),
    };
    let xs = T::extract(p)?;
    let mut out: Vec<T> = Vec::with_capacity(bounds.len().saturating_sub(1));
    for w in bounds.windows(2) {
        let lo = w[0] as usize;
        let hi = w[1] as usize;
        if hi == lo { return Err("reducer: empty inner row".into()); }
        let mut acc = xs[lo];
        for &x in &xs[lo + 1..hi] { acc = f(acc, x); }
        out.push(acc);
    }
    Ok(from_vec::<T>(out))
}

fn fold_whole<T: Storage, F: Fn(T, T) -> T>(
    v: &Value, f: F,
) -> Result<Value, String> {
    // View-aware via `for_each_prim`: walks Prim, View(Indices), and
    // View(Range) without materializing an intermediate. Cost is one pass
    // through the logical elements.
    let mut acc: Option<T> = None;
    crate::ops::helpers::for_each_prim::<T, _>(v, |x| {
        acc = Some(match acc {
            None => x,
            Some(prev) => f(prev, x),
        });
    })?;
    match acc {
        Some(a) => Ok(from_vec::<T>(vec![a])),
        None => Err("reducer: empty input".into()),
    }
}

macro_rules! reducer_op {
    ($name:ident, $tag:literal, $accum:expr) => {
        #[derive(Debug)]
        pub struct $name { pub interp: Interp }
        impl PrimOp for $name {
            fn name(&self) -> &str { $tag }
            fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
            fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
                // pop_raw — `fold_whole` is View-aware and avoids the
                // intermediate gather that `pop()` would force.
                let v = crate::ir::stack::pop_raw(st)?;
                let out = match v {
                    Value::List { bounds, values } => {
                        let bnds = bounds_as_u64(&bounds)?.to_vec();
                        match self.interp {
                            Interp::U8  => fold_runs::<u8, _>(&values, &bnds, $accum)?,
                            Interp::I8  => fold_runs::<i8, _>(&values, &bnds, $accum)?,
                            Interp::U16 => fold_runs::<u16, _>(&values, &bnds, $accum)?,
                            Interp::I16 => fold_runs::<i16, _>(&values, &bnds, $accum)?,
                            Interp::U32 => fold_runs::<u32, _>(&values, &bnds, $accum)?,
                            Interp::I32 => fold_runs::<i32, _>(&values, &bnds, $accum)?,
                            Interp::U64 => fold_runs::<u64, _>(&values, &bnds, $accum)?,
                            Interp::I64 => fold_runs::<i64, _>(&values, &bnds, $accum)?,
                            Interp::F32 => fold_runs::<f32, _>(&values, &bnds, $accum)?,
                            Interp::F64 => fold_runs::<f64, _>(&values, &bnds, $accum)?,
                        }
                    }
                    other => match self.interp {
                        Interp::U8  => fold_whole::<u8, _>(&other, $accum)?,
                        Interp::I8  => fold_whole::<i8, _>(&other, $accum)?,
                        Interp::U16 => fold_whole::<u16, _>(&other, $accum)?,
                        Interp::I16 => fold_whole::<i16, _>(&other, $accum)?,
                        Interp::U32 => fold_whole::<u32, _>(&other, $accum)?,
                        Interp::I32 => fold_whole::<i32, _>(&other, $accum)?,
                        Interp::U64 => fold_whole::<u64, _>(&other, $accum)?,
                        Interp::I64 => fold_whole::<i64, _>(&other, $accum)?,
                        Interp::F32 => fold_whole::<f32, _>(&other, $accum)?,
                        Interp::F64 => fold_whole::<f64, _>(&other, $accum)?,
                    }
                };
                st.push(out);
                Ok(())
            }
        }
        impl Typed for $name {
            fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
                let v = tc_pop(st, $tag)?;
                let inner = match v { Shape::List { inner, .. } => *inner, other => other };
                match inner {
                    Shape::Prim(w) if w == self.interp.width() => st.push(Shape::Prim(w)),
                    other => return Err(format!("{}.{}: expected Prim({}), got {}", $tag, self.interp, self.interp.width(), other)),
                }
                Ok(())
            }
        }
    };
}

reducer_op!(ReduceMin, "reduce.min", |a, b| if a < b { a } else { b });
reducer_op!(ReduceMax, "reduce.max", |a, b| if a > b { a } else { b });
reducer_op!(ReduceMul, "reduce.*",   |a, b| a * b);

// any / all: input is P8 (boolean column or list of bools), output is P8 with one value per row.
#[derive(Debug)] pub struct Any;
impl PrimOp for Any {
    fn name(&self) -> &str { "any" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        // pop_raw so we can dispatch on View ourselves.
        let v = crate::ir::stack::pop_raw(st)?;
        let out = match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let xs = <u8 as Storage>::extract(&match (*values).clone() { Value::Prim(p) => p, _ => return Err("any: list inner must be Prim".into()) })?.to_vec();
                let mut out: Vec<u8> = Vec::with_capacity(bnds.len().saturating_sub(1));
                for w in bnds.windows(2) {
                    let lo = w[0] as usize;
                    let hi = w[1] as usize;
                    out.push(xs[lo..hi].iter().any(|&x| x != 0) as u8);
                }
                from_vec::<u8>(out)
            }
            other => {
                let mut found = 0u8;
                crate::ops::helpers::for_each_prim::<u8, _>(&other, |x| {
                    if x != 0 { found = 1; }
                })?;
                from_vec::<u8>(vec![found])
            }
        };
        st.push(out);
        Ok(())
    }
}
impl Typed for Any {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "any")?;
        let inner = match v { Shape::List { inner, .. } => *inner, other => other };
        if inner != Shape::Prim(PrimWidth::W8) { return Err(format!("any: needs Prim(P8), got {}", inner)); }
        st.push(Shape::Prim(PrimWidth::W8));
        Ok(())
    }
}

#[derive(Debug)] pub struct All;
impl PrimOp for All {
    fn name(&self) -> &str { "all" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = crate::ir::stack::pop_raw(st)?;
        let out = match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let xs = <u8 as Storage>::extract(&match (*values).clone() { Value::Prim(p) => p, _ => return Err("all: list inner must be Prim".into()) })?.to_vec();
                let mut out: Vec<u8> = Vec::with_capacity(bnds.len().saturating_sub(1));
                for w in bnds.windows(2) {
                    let lo = w[0] as usize;
                    let hi = w[1] as usize;
                    out.push(xs[lo..hi].iter().all(|&x| x != 0) as u8);
                }
                from_vec::<u8>(out)
            }
            other => {
                let mut all = 1u8;
                crate::ops::helpers::for_each_prim::<u8, _>(&other, |x| {
                    if x == 0 { all = 0; }
                })?;
                from_vec::<u8>(vec![all])
            }
        };
        st.push(out);
        Ok(())
    }
}
impl Typed for All {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "all")?;
        let inner = match v { Shape::List { inner, .. } => *inner, other => other };
        if inner != Shape::Prim(PrimWidth::W8) { return Err(format!("all: needs Prim(P8), got {}", inner)); }
        st.push(Shape::Prim(PrimWidth::W8));
        Ok(())
    }
}

#[derive(Debug)] pub struct Not;
impl PrimOp for Not {
    fn name(&self) -> &str { "not" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let p = match v { Value::Prim(p) => p, _ => return Err("not: expected Prim".into()) };
        let xs = <u8 as Storage>::extract(&p)?;
        let out: Vec<u8> = xs.iter().map(|&b| (b == 0) as u8).collect();
        st.push(from_vec::<u8>(out));
        Ok(())
    }
}
impl Typed for Not {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "not")?;
        if v != Shape::Prim(PrimWidth::W8) { return Err(format!("not: needs Prim(P8), got {}", v)); }
        st.push(Shape::Prim(PrimWidth::W8));
        Ok(())
    }
}

/// `and` — elementwise AND of two P8 masks. Treats non-zero as true, zero as
/// false; output is 0/1. Pops two same-length P8s, pushes P8 of same length.
#[derive(Debug)] pub struct And;
impl PrimOp for And {
    fn name(&self) -> &str { "and" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let b = pop(st)?;
        let a = pop(st)?;
        let pa = match a { Value::Prim(p) => p, _ => return Err("and: expected Prim".into()) };
        let pb = match b { Value::Prim(p) => p, _ => return Err("and: expected Prim".into()) };
        let xa = <u8 as Storage>::extract(&pa)?;
        let xb = <u8 as Storage>::extract(&pb)?;
        if xa.len() != xb.len() { return Err(format!("and: length mismatch {} vs {}", xa.len(), xb.len())); }
        let out: Vec<u8> = xa.iter().zip(xb.iter()).map(|(&x, &y)| ((x != 0) && (y != 0)) as u8).collect();
        st.push(from_vec::<u8>(out));
        Ok(())
    }
}
impl Typed for And {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let b = tc_pop(st, "and")?;
        let a = tc_pop(st, "and")?;
        if a != Shape::Prim(PrimWidth::W8) || b != Shape::Prim(PrimWidth::W8) {
            return Err(format!("and: needs Prim(P8) on both sides, got {} and {}", a, b));
        }
        st.push(Shape::Prim(PrimWidth::W8));
        Ok(())
    }
}

/// `or` — elementwise OR of two P8 masks.
#[derive(Debug)] pub struct Or;
impl PrimOp for Or {
    fn name(&self) -> &str { "or" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let b = pop(st)?;
        let a = pop(st)?;
        let pa = match a { Value::Prim(p) => p, _ => return Err("or: expected Prim".into()) };
        let pb = match b { Value::Prim(p) => p, _ => return Err("or: expected Prim".into()) };
        let xa = <u8 as Storage>::extract(&pa)?;
        let xb = <u8 as Storage>::extract(&pb)?;
        if xa.len() != xb.len() { return Err(format!("or: length mismatch {} vs {}", xa.len(), xb.len())); }
        let out: Vec<u8> = xa.iter().zip(xb.iter()).map(|(&x, &y)| ((x != 0) || (y != 0)) as u8).collect();
        st.push(from_vec::<u8>(out));
        Ok(())
    }
}
impl Typed for Or {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let b = tc_pop(st, "or")?;
        let a = tc_pop(st, "or")?;
        if a != Shape::Prim(PrimWidth::W8) || b != Shape::Prim(PrimWidth::W8) {
            return Err(format!("or: needs Prim(P8) on both sides, got {} and {}", a, b));
        }
        st.push(Shape::Prim(PrimWidth::W8));
        Ok(())
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "any" => Some(Box::new(Any)),
            "all" => Some(Box::new(All)),
            "not" => Some(Box::new(Not)),
            "and" => Some(Box::new(And)),
            "or"  => Some(Box::new(Or)),
            _ => None,
        }
    });
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        match head {
            "reduce.min" => Some(Box::new(ReduceMin { interp })),
            "reduce.max" => Some(Box::new(ReduceMax { interp })),
            "reduce.*"   => Some(Box::new(ReduceMul { interp })),
            _ => None,
        }
    });
}
