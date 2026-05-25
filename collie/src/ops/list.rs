//! List-domain ops — the biggest ops file because everything
//! list-shaped tends to land here. Roughly four clusters:
//!
//! - **Aggregations and scans**: `Group`, `Cumsum`, `Unique`,
//!   `Shift`, `ReduceAdd` (the typed `reduce.+`/`*`/`min`/`max`
//!   live elsewhere as `reduce_ops`).
//! - **Body-bearing escape hatches**: `Reduce { body }`, `Each`,
//!   `Repeat`. These are the principle-2 escape hatches —
//!   per-row interpreter dispatch. See `PRINCIPLES.md` for the
//!   trade.
//! - **Shape inspection**: `Bounds`, `BoundsToKeys`, `Count`,
//!   `Length`. Cheap, structural.
//! - **Construction and slicing**: `Singleton`, `Like`, `Head`,
//!   `Iota`, `Spread`, `Where_`, `Filter`. `Filter` is the
//!   parser-peephole fusion of `where` + `gather`.
//!
//! On a first read, skim the four section markers and the typed
//! variants. The body-bearing arms (`Reduce`, `Each`, `Repeat`)
//! each do an explicit `eval(&self.body, …)` loop and can be
//! understood independently of the rest.

use std::sync::Arc;
use crate::ir::op::{PrimOp, eval};
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Op, Typed, TypeStack, TypeEnv, tc_pop, typecheck};
use crate::ir::value::{Value, Prim, PrimWidth, from_vec, bounds_var_from_ends, prod};
use crate::ir::shape::{Interp, Shape, bounds_as_u64};
use crate::ops::helpers::{
    broadcast, concat_rows, concat_values, gather,
    slice_value, sum_runs, sum_whole,
};
use crate::ops::sort::{sort_blocks, run_layout};

#[derive(Debug)] pub struct Group;
impl PrimOp for Group {
    fn name(&self) -> &str { "group" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 2)) }  // (vals, keys) → (uniq_keys, list_of_grouped_vals)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let keys = pop(st)?;
        let vals = pop(st)?;
        if keys.len() != vals.len() {
            return Err(format!("group: vals len {} != keys len {}", vals.len(), keys.len()));
        }
        let n = keys.len();
        // group = sort by key + bundle. Sort the keys through the engine
        // (any shape; unsigned-word order — grouping is by equality, so
        // the partition is interp-independent), take the run-labels as the
        // group structure, and shuffle the vals by the same permutation.
        let order = vec![0u64; n];
        let (perm, labels) = sort_blocks(&order, &keys)?;
        let perm_usize: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
        let vals_sorted = gather(&vals, &perm_usize)?;
        let keys_sorted = gather(&keys, &perm_usize)?;
        let (bounds_ends, firsts) = run_layout(&labels);
        let unique_keys = gather(&keys_sorted, &firsts)?;
        let list = Value::List { bounds: bounds_var_from_ends(bounds_ends), values: Arc::new(vals_sorted) };
        st.push(unique_keys);
        st.push(list);
        Ok(())
    }
}
impl Typed for Group {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        // Keys may now be any shape the sort engine handles (Prim, Prod,
        // List, Sum) — not just Prim. The unique-keys output has the same
        // shape as the keys.
        let keys = tc_pop(st, "group")?;
        let vals = tc_pop(st, "group")?;
        st.push(keys.clone());
        st.push(Shape::List { bounds: PrimWidth::W64, inner: Box::new(vals) });
        Ok(())
    }
}

#[derive(Debug)] pub struct Reduce { pub body: Vec<Box<dyn Op>> }
impl PrimOp for Reduce {
    fn name(&self) -> &str { "reduce" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let (bounds, inner) = match v {
            Value::List { bounds, values } => (bounds_as_u64(&bounds)?.to_vec(), (*values).clone()),
            other => {
                let mut sub = vec![other];
                eval(&self.body, &mut sub, env)?;
                if sub.len() != 1 { return Err(format!("reduce body left {} values", sub.len())); }
                st.push(sub.pop().unwrap());
                return Ok(());
            }
        };
        let mut row_outs: Vec<Value> = Vec::with_capacity(bounds.len().saturating_sub(1));
        for w in bounds.windows(2) {
            let lo = w[0] as usize;
            let hi = w[1] as usize;
            let slice = slice_value(&inner, lo, hi)?;
            let mut sub = vec![slice];
            eval(&self.body, &mut sub, env)?;
            if sub.len() != 1 { return Err(format!("reduce body left {} values", sub.len())); }
            let out = sub.pop().unwrap();
            if out.len() != 1 {
                return Err(format!("reduce body produced len {}, expected 1", out.len()));
            }
            row_outs.push(out);
        }
        st.push(concat_rows(&row_outs)?);
        Ok(())
    }
}
impl Typed for Reduce {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "reduce")?;
        let inner = match v { Shape::List { inner, .. } => *inner, other => other };
        let mut sub = vec![inner];
        typecheck(&self.body, &mut sub, env).map_err(|e| format!("reduce body: {}", e))?;
        if sub.len() != 1 { return Err(format!("reduce body leaves {} types", sub.len())); }
        st.push(sub.pop().unwrap());
        Ok(())
    }
}

/// `cumsum.<interp>` — prefix sum / running total. Same shape as input
/// (unlike `reduce.+` which collapses each row to a scalar).
///
/// Flat: `[a0, a1, a2, …] → [a0, a0+a1, a0+a1+a2, …]`.
/// List<Prim>: per-row prefix sum; outer bounds unchanged.
///
/// Arithmetic wraps at the interp's width (same as `+`). For overflow-
/// safe accumulation widen first via `as.<wider>` then `cumsum.<wider>`.
#[derive(Debug)] pub struct Cumsum { pub interp: Interp }
impl PrimOp for Cumsum {
    fn name(&self) -> &str { "cumsum" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let new_values = cumsum_runs(&values, &bnds, self.interp)?;
                st.push(Value::List { bounds, values: Arc::new(new_values) });
            }
            Value::Prim(p) => {
                st.push(Value::Prim(cumsum_flat(&p, self.interp)?));
            }
            other => return Err(format!("cumsum.{}: expected Prim or List<Prim>, got {:?}", self.interp, other)),
        }
        Ok(())
    }
}
impl Typed for Cumsum {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "cumsum")?;
        let w = self.interp.width();
        match v {
            Shape::Prim(pw) if pw == w => { st.push(Shape::Prim(w)); Ok(()) }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prim(pw) if pw == w => {
                    st.push(Shape::List { bounds, inner: Box::new(Shape::Prim(w)) });
                    Ok(())
                }
                other => Err(format!("cumsum.{}: list inner must be Prim({}), got {}", self.interp, w, other)),
            }
            other => Err(format!("cumsum.{}: needs Prim({}) or List<Prim({})>, got {}", self.interp, w, w, other)),
        }
    }
}

fn cumsum_flat(p: &Prim, interp: Interp) -> Result<Prim, String> {
    use crate::ir::value::Storage;
    macro_rules! cs { ($t:ty) => {{
        let xs = <$t as Storage>::extract(p)?;
        let mut acc: $t = <$t as Default>::default();
        let out: Vec<$t> = xs.iter().map(|&x| { acc += x; acc }).collect();
        Ok(<$t as Storage>::wrap(out))
    }};}
    match interp {
        Interp::U8  => cs!(u8),
        Interp::I8  => cs!(i8),
        Interp::U16 => cs!(u16),
        Interp::I16 => cs!(i16),
        Interp::U32 => cs!(u32),
        Interp::I32 => cs!(i32),
        Interp::U64 => cs!(u64),
        Interp::I64 => cs!(i64),
        Interp::F32 => cs!(f32),
        Interp::F64 => cs!(f64),
    }
}

fn cumsum_runs(values: &Value, bounds: &[u64], interp: Interp) -> Result<Value, String> {
    use crate::ir::value::Storage;
    let p = match values {
        Value::Prim(p) => p,
        other => return Err(format!("cumsum: list inner must be Prim, got {:?}", other)),
    };
    macro_rules! cs { ($t:ty) => {{
        let xs = <$t as Storage>::extract(p)?;
        let mut out: Vec<$t> = Vec::with_capacity(xs.len());
        for w in bounds.windows(2) {
            let lo = w[0] as usize;
            let hi = w[1] as usize;
            let mut acc: $t = <$t as Default>::default();
            for &x in &xs[lo..hi] {
                acc += x;
                out.push(acc);
            }
        }
        Ok(Value::Prim(<$t as Storage>::wrap(out)))
    }};}
    match interp {
        Interp::U8  => cs!(u8),
        Interp::I8  => cs!(i8),
        Interp::U16 => cs!(u16),
        Interp::I16 => cs!(i16),
        Interp::U32 => cs!(u32),
        Interp::I32 => cs!(i32),
        Interp::U64 => cs!(u64),
        Interp::I64 => cs!(i64),
        Interp::F32 => cs!(f32),
        Interp::F64 => cs!(f64),
    }
}

/// `unique.<interp>` — distinct values from a flat Prim column. Sorts
/// (so caller doesn't have to) and dedupes. For per-row uniqueness use
/// `each { sort.<interp> ... }` patterns, or `group.<interp>` if the
/// satellite list is also needed.
#[derive(Debug)] pub struct Unique;
impl PrimOp for Unique {
    fn name(&self) -> &str { "unique" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let n = v.len();
        if n == 0 {
            st.push(v);
            return Ok(());
        }
        // unique = sort + first-of-each-run, on the engine. Any shape;
        // dedup is by equality so the unsigned-word order suffices.
        let order = vec![0u64; n];
        let (perm, labels) = sort_blocks(&order, &v)?;
        let perm_usize: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
        let sorted = gather(&v, &perm_usize)?;
        let (_, firsts) = run_layout(&labels);
        st.push(gather(&sorted, &firsts)?);
        Ok(())
    }
}
impl Typed for Unique {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        // Any shape the engine sorts (interp vestigial — dedup is by
        // equality). Output: the distinct elements, same shape as input.
        let v = tc_pop(st, "unique")?;
        st.push(v);
        Ok(())
    }
}

/// `shift.<interp>` — adjacent-row lookup ("what was the value N positions
/// earlier"). Pops `n` (length-1 P64) and the source. Positive `n` shifts
/// right (`out[i] = src[i-n]`, with the leftmost `n` filled by the
/// interp's default = 0). Useful for sessionization, deltas, and other
/// patterns that need adjacent-row comparisons.
///
/// Flat: shifts the whole column.
/// List<Prim>: per-row shift; row lengths unchanged.
///
/// Negative shifts (LEAD) and per-row variable shifts are follow-ups.
#[derive(Debug)] pub struct Shift { pub interp: Interp }
impl PrimOp for Shift {
    fn name(&self) -> &str { "shift" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let n_v = pop(st)?;
        let n = match n_v {
            Value::Prim(Prim::P64(v)) if v.len() == 1 => v[0] as usize,
            other => return Err(format!("shift: n must be length-1 P64, got {:?}", other)),
        };
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let new_values = shift_runs(&values, &bnds, n, self.interp)?;
                st.push(Value::List { bounds, values: Arc::new(new_values) });
            }
            Value::Prim(p) => {
                st.push(Value::Prim(shift_flat(&p, n, self.interp)?));
            }
            other => return Err(format!("shift.{}: expected Prim or List<Prim>, got {:?}", self.interp, other)),
        }
        Ok(())
    }
}
impl Typed for Shift {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let n_t = tc_pop(st, "shift")?;
        if n_t != Shape::Prim(PrimWidth::W64) {
            return Err(format!("shift: n must be Prim(P64), got {}", n_t));
        }
        let v = tc_pop(st, "shift")?;
        let w = self.interp.width();
        match v {
            Shape::Prim(pw) if pw == w => { st.push(Shape::Prim(w)); Ok(()) }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prim(pw) if pw == w => {
                    st.push(Shape::List { bounds, inner: Box::new(Shape::Prim(w)) });
                    Ok(())
                }
                other => Err(format!("shift.{}: list inner must be Prim({}), got {}", self.interp, w, other)),
            }
            other => Err(format!("shift.{}: needs Prim({}) or List<Prim({})>, got {}", self.interp, w, w, other)),
        }
    }
}

fn shift_flat(p: &Prim, n: usize, interp: Interp) -> Result<Prim, String> {
    use crate::ir::value::Storage;
    macro_rules! sh { ($t:ty) => {{
        let xs = <$t as Storage>::extract(p)?;
        let len = xs.len();
        let zero: $t = <$t as Default>::default();
        let n_capped = n.min(len);
        let mut out: Vec<$t> = vec![zero; n_capped];
        out.extend_from_slice(&xs[..len - n_capped]);
        Ok(<$t as Storage>::wrap(out))
    }};}
    match interp {
        Interp::U8  => sh!(u8),
        Interp::I8  => sh!(i8),
        Interp::U16 => sh!(u16),
        Interp::I16 => sh!(i16),
        Interp::U32 => sh!(u32),
        Interp::I32 => sh!(i32),
        Interp::U64 => sh!(u64),
        Interp::I64 => sh!(i64),
        Interp::F32 => sh!(f32),
        Interp::F64 => sh!(f64),
    }
}

fn shift_runs(values: &Value, bounds: &[u64], n: usize, interp: Interp) -> Result<Value, String> {
    use crate::ir::value::Storage;
    let p = match values {
        Value::Prim(p) => p,
        other => return Err(format!("shift: list inner must be Prim, got {:?}", other)),
    };
    macro_rules! sh { ($t:ty) => {{
        let xs = <$t as Storage>::extract(p)?;
        let zero: $t = <$t as Default>::default();
        let mut out: Vec<$t> = Vec::with_capacity(xs.len());
        for w in bounds.windows(2) {
            let lo = w[0] as usize;
            let hi = w[1] as usize;
            let row_len = hi - lo;
            let n_capped = n.min(row_len);
            for _ in 0..n_capped { out.push(zero); }
            out.extend_from_slice(&xs[lo..hi - n_capped]);
        }
        Ok(Value::Prim(<$t as Storage>::wrap(out)))
    }};}
    match interp {
        Interp::U8  => sh!(u8),
        Interp::I8  => sh!(i8),
        Interp::U16 => sh!(u16),
        Interp::I16 => sh!(i16),
        Interp::U32 => sh!(u32),
        Interp::I32 => sh!(i32),
        Interp::U64 => sh!(u64),
        Interp::I64 => sh!(i64),
        Interp::F32 => sh!(f32),
        Interp::F64 => sh!(f64),
    }
}

#[derive(Debug)] pub struct ReduceAdd { pub interp: Interp }
impl PrimOp for ReduceAdd {
    fn name(&self) -> &str { "reduce.+" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        // pop_raw so a `View` over a flat Prim can stream through without
        // an intermediate gather (`sum_whole` handles both via for_each_prim).
        let v = crate::ir::stack::pop_raw(st)?;
        match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let out = sum_runs(&values, &bnds, self.interp)?;
                st.push(out);
            }
            other => {
                st.push(sum_whole(&other, self.interp)?);
            }
        }
        Ok(())
    }
}
impl Typed for ReduceAdd {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "reduce.+")?;
        let inner = match v { Shape::List { inner, .. } => *inner, other => other };
        match inner {
            Shape::Prim(w) if w == self.interp.width() => st.push(Shape::Prim(w)),
            other => return Err(format!("reduce.+.{}: expected Prim({}), got {}", self.interp, self.interp.width(), other)),
        }
        Ok(())
    }
}

/// `repeat N { body }` — run `body` N times in sequence. `N` is a parse-time
/// integer literal.
///
/// Stack-effect contract: the body must be **stack-shape-preserving** —
/// after one run, the type-stack must be identical to before. This is what
/// makes "run N times" well-defined (each iteration's body sees the same
/// stack shape its predecessor produced). The typechecker enforces it by
/// running tc on the body once and comparing the type-stack before/after.
///
/// Uses include iterative algorithms with fixed step counts (Mandelbrot
/// escape test, Newton's method, fixed-point refinement, etc.).
#[derive(Debug)] pub struct Repeat { pub n: usize, pub body: Vec<Box<dyn Op>> }
impl PrimOp for Repeat {
    fn name(&self) -> &str { "repeat" }
    // Net effect is (N, N) where N = the body's actual stack consumption.
    // The body is stack-shape-preserving by contract, so n_in == n_out, but
    // the count itself depends on the body and isn't visible from arity()
    // alone. Graph builder dispatches to a dynamic-arity handler.
    fn arity(&self) -> Option<(usize, usize)> { None }
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        for _ in 0..self.n {
            eval(&self.body, st, env)?;
        }
        Ok(())
    }
}
impl Typed for Repeat {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        let before = st.clone();
        typecheck(&self.body, st, env).map_err(|e| format!("repeat body: {}", e))?;
        if *st != before {
            return Err(format!(
                "repeat {}: body is not stack-shape-preserving \
                 (before: {} item(s), after: {} item(s))",
                self.n, before.len(), st.len()
            ));
        }
        Ok(())
    }
}

/// `each { body }` — polymorphic over input shape.
///
/// Single List<A> input: body sees one A per row.
/// Prod[List<A>, ..., List<Z>] (any arity, co-bounded): body sees a
/// Prod[A, ..., Z] per row. Eliminates the need for each2/each3/each_k.
///
/// The body still produces one row of output per invocation; all per-row
/// outputs are concatenated and re-nested under fresh Var bounds.
#[derive(Debug)] pub struct Each { pub body: Vec<Box<dyn Op>> }
impl PrimOp for Each {
    fn name(&self) -> &str { "each" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // list → list (body internal)
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        // Returns (per-list bounds, per-list inners). For single-List input,
        // there's one entry; for Prod-of-Lists, one entry per field.
        let (bs, inners): (Vec<crate::ir::value::BoundsRepr>, Vec<Value>) = match v {
            Value::List { bounds, values } => (vec![bounds], vec![(*values).clone()]),
            Value::Prod(fs) if !fs.is_empty()
                && fs.iter().all(|f| matches!(f, Value::List { .. })) =>
            {
                let mut bs: Vec<crate::ir::value::BoundsRepr> = Vec::with_capacity(fs.len());
                let mut vs: Vec<Value> = Vec::with_capacity(fs.len());
                for f in fs.iter() {
                    if let Value::List { bounds, values } = f {
                        bs.push(bounds.clone());
                        vs.push((**values).clone());
                    }
                }
                // Only require matching *outer counts*; per-row inner lengths
                // can differ. This lets `each` zip parallel Lists for per-row
                // body work (e.g. row-wise intersect on differently-sized
                // c-lists) without forcing them into the same shape.
                let n0 = bs[0].len();
                for (i, b) in bs.iter().enumerate().skip(1) {
                    if b.len() != n0 {
                        return Err(format!(
                            "each: Prod field 0 has {} outer rows, field {} has {}", n0, i, b.len()
                        ));
                    }
                }
                (bs, vs)
            }
            other => return Err(format!("each: expected List or Prod-of-Lists, got {:?}", other)),
        };
        // Materialize each list's bounds to a Vec<u64> for indexed slicing.
        // Per-list cursors advance independently — row i of field j slices
        // inners[j] using bs[j]'s i-th range.
        let bs_vecs: Vec<Vec<u64>> = bs.iter()
            .map(|b| bounds_as_u64(b).map(|c| c.into_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        // bs_vecs[*].len() is row_count + 1 (canonical N+1 start-offsets).
        let n_lists = bs_vecs[0].len().saturating_sub(1);
        let mut new_bounds: Vec<u64> = Vec::with_capacity(n_lists);
        let mut parts: Vec<Value> = Vec::with_capacity(n_lists);
        let mut acc = 0u64;
        for row_idx in 0..n_lists {
            let row_val = if inners.len() == 1 {
                let lo = bs_vecs[0][row_idx] as usize;
                let hi = bs_vecs[0][row_idx + 1] as usize;
                slice_value(&inners[0], lo, hi)?
            } else {
                let mut slices = Vec::with_capacity(inners.len());
                for (j, inner) in inners.iter().enumerate() {
                    let lo = bs_vecs[j][row_idx] as usize;
                    let hi = bs_vecs[j][row_idx + 1] as usize;
                    slices.push(slice_value(inner, lo, hi)?);
                }
                prod(slices)
            };
            let mut sub = vec![row_val];
            eval(&self.body, &mut sub, env)?;
            if sub.len() != 1 { return Err(format!("each body left {} values", sub.len())); }
            let out = sub.pop().unwrap();
            acc += out.len() as u64;
            new_bounds.push(acc);
            parts.push(out);
        }
        let new_inner = concat_values(&parts)?;
        st.push(Value::List { bounds: bounds_var_from_ends(new_bounds), values: Arc::new(new_inner) });
        Ok(())
    }
}
impl Typed for Each {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "each")?;
        let (bw, body_in) = match v {
            Shape::List { bounds, inner } => (bounds, *inner),
            Shape::Prod(fs) if !fs.is_empty()
                && fs.iter().all(|f| matches!(f, Shape::List { .. })) =>
            {
                let mut bw: Option<PrimWidth> = None;
                let mut inner_shapes: Vec<Shape> = Vec::with_capacity(fs.len());
                for f in &fs {
                    if let Shape::List { bounds, inner } = f {
                        match bw {
                            None => bw = Some(*bounds),
                            Some(b) if b != *bounds =>
                                return Err("each: Prod fields have non-matching bounds widths".into()),
                            _ => {}
                        }
                        inner_shapes.push((**inner).clone());
                    }
                }
                (bw.unwrap(), Shape::Prod(inner_shapes))
            }
            other => return Err(format!("each: expected List or Prod-of-Lists, got {}", other)),
        };
        let mut sub = vec![body_in];
        typecheck(&self.body, &mut sub, env).map_err(|e| format!("each body: {}", e))?;
        if sub.len() != 1 { return Err(format!("each body leaves {} types", sub.len())); }
        st.push(Shape::List { bounds: bw, inner: Box::new(sub.pop().unwrap()) });
        Ok(())
    }
}

#[derive(Debug)] pub struct Bounds;
impl PrimOp for Bounds {
    fn name(&self) -> &str { "list>bounds" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, .. } => st.push(Value::Prim(bounds.to_prim())),
            other => return Err(format!("bounds: not a list, got {:?}", other)),
        }
        Ok(())
    }
}
impl Typed for Bounds {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "bounds")?;
        match v {
            Shape::List { bounds, .. } => st.push(Shape::Prim(bounds)),
            other => return Err(format!("bounds: not a list: {}", other)),
        }
        Ok(())
    }
}

#[derive(Debug)] pub struct BoundsToKeys;
impl PrimOp for BoundsToKeys {
    fn name(&self) -> &str { "bounds>keys" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, .. } => {
                let total = bounds.iter_pairs().last().map(|(_, h)| h).unwrap_or(0) as usize;
                let mut out: Vec<u64> = Vec::with_capacity(total);
                for (i, (lo, hi)) in bounds.iter_pairs().enumerate() {
                    for _ in lo..hi { out.push(i as u64); }
                }
                st.push(from_vec::<u64>(out));
                Ok(())
            }
            other => Err(format!("bounds>keys: not a list, got {:?}", other)),
        }
    }
}
impl Typed for BoundsToKeys {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "bounds>keys")?;
        match v {
            Shape::List { .. } => st.push(Shape::Prim(PrimWidth::W64)),
            other => return Err(format!("bounds>keys: not a list: {}", other)),
        }
        Ok(())
    }
}

#[derive(Debug)] pub struct Count;
impl PrimOp for Count {
    fn name(&self) -> &str { "count" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, .. } => {
                let mut out = Vec::with_capacity(bounds.len());
                for (lo, hi) in bounds.iter_pairs() { out.push(hi - lo); }
                st.push(from_vec::<u64>(out));
                Ok(())
            }
            other => Err(format!("count: not a list: {:?}", other)),
        }
    }
}
impl Typed for Count {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "count")?;
        match v {
            Shape::List { .. } => st.push(Shape::Prim(PrimWidth::W64)),
            other => return Err(format!("count: not a list: {}", other)),
        }
        Ok(())
    }
}

#[derive(Debug)] pub struct Like;
impl PrimOp for Like {
    fn name(&self) -> &str { "like" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (template, scalar) → broadcast
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let scalar = pop(st)?;
        let template = pop(st)?;
        if scalar.len() != 1 {
            return Err(format!("like: scalar must be length-1, got {}", scalar.len()));
        }
        st.push(broadcast(&scalar, template.len()));
        Ok(())
    }
}
impl Typed for Like {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let s = tc_pop(st, "like")?;
        let _t = tc_pop(st, "like")?;
        st.push(s);
        Ok(())
    }
}

#[derive(Debug)] pub struct Head;
impl PrimOp for Head {
    fn name(&self) -> &str { "head" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                let mut idxs = Vec::with_capacity(bounds.len());
                for (lo, hi) in bounds.iter_pairs() {
                    if hi == lo { return Err("head: empty inner row".into()); }
                    idxs.push(lo as usize);
                }
                st.push(gather(&values, &idxs)?);
                Ok(())
            }
            other => Err(format!("head: not a list: {:?}", other)),
        }
    }
}
impl Typed for Head {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "head")?;
        match v {
            Shape::List { inner, .. } => st.push(*inner),
            other => return Err(format!("head: not a list: {}", other)),
        }
        Ok(())
    }
}

#[derive(Debug)] pub struct Iota;
impl PrimOp for Iota {
    fn name(&self) -> &str { "iota" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let n = match v {
            Value::Prim(Prim::P64(x)) if x.len() == 1 => x[0],
            other => return Err(format!("iota: need length-1 P64, got {:?}", other)),
        };
        st.push(from_vec::<u64>((0..n).collect()));
        Ok(())
    }
}
impl Typed for Iota {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "iota")?;
        if v != Shape::Prim(PrimWidth::W64) { return Err(format!("iota: needs Prim(P64), got {}", v)); }
        st.push(Shape::Prim(PrimWidth::W64));
        Ok(())
    }
}

#[derive(Debug)] pub struct Spread;
impl PrimOp for Spread {
    fn name(&self) -> &str { "spread" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (col, counts) → spread col
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let counts = match pop(st)? {
            Value::Prim(Prim::P64(c)) => c,
            other => return Err(format!("spread: counts must be P64, got {:?}", other)),
        };
        let col = pop(st)?;
        if col.len() != counts.len() {
            return Err(format!("spread: col len {} != counts len {}", col.len(), counts.len()));
        }
        let mut idxs: Vec<usize> = Vec::new();
        for (i, &c) in counts.iter().enumerate() {
            for _ in 0..c { idxs.push(i); }
        }
        st.push(gather(&col, &idxs)?);
        Ok(())
    }
}
impl Typed for Spread {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let counts = tc_pop(st, "spread")?;
        if counts != Shape::Prim(PrimWidth::W64) { return Err(format!("spread: counts must be Prim(P64), got {}", counts)); }
        let col = tc_pop(st, "spread")?;
        st.push(col);
        Ok(())
    }
}

/// `where` — return *positions* where a boolean (P8) mask is true.
///
/// Rank-polymorphic:
///   - Flat:  `SEQ<u8> -> SEQ<u64>`  positions where mask != 0.
///   - Row:   `SEQ<LIST<u8>> -> SEQ<LIST<u64>>`  per-row positions where
///            each row's inner mask is true.
///
/// To recover the filtered VALUES of a parallel column: gather the column
/// at the `where` output. `col mask where gather` replaces the older
/// "where col by mask" semantics.
///
/// Positions are strictly more informative than filtered values — they can
/// be used to gather *any* column, not just the one originally filtered.
#[derive(Debug)] pub struct Where_;
impl PrimOp for Where_ {
    fn name(&self) -> &str { "where" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let mask = crate::ir::stack::pop_raw(st)?;
        match mask {
            Value::Prim(Prim::P8(m)) => {
                let ms: &[u8] = &m;
                let mut idxs: Vec<u64> = Vec::with_capacity(ms.len() / 2);
                for i in 0..ms.len() {
                    if ms[i] != 0 { idxs.push(i as u64); }
                }
                st.push(from_vec::<u64>(idxs));
                Ok(())
            }
            Value::List { bounds, values } => {
                // Row-shaped form: per-row positions where mask is true.
                let inner_mask = match (*values).clone() {
                    Value::Prim(Prim::P8(m)) => m,
                    other => return Err(format!(
                        "where: row-shaped input's inner must be Prim(P8), got {:?}", other
                    )),
                };
                let bnds = crate::ir::shape::bounds_as_u64(&bounds)?;
                let ms: &[u8] = &inner_mask;
                let mut flat: Vec<u64> = Vec::new();
                let mut out_bounds: Vec<u64> = Vec::with_capacity(bnds.len() - 1);
                for w in bnds.windows(2) {
                    let lo = w[0] as usize;
                    let hi = w[1] as usize;
                    for j in lo..hi {
                        if ms[j] != 0 {
                            // Row-relative position. (For source-coord
                            // positions, add lo — but the user typically
                            // wants per-row positions here, since the row
                            // is the natural reference frame.)
                            flat.push((j - lo) as u64);
                        }
                    }
                    out_bounds.push(flat.len() as u64);
                }
                st.push(Value::List {
                    bounds: bounds_var_from_ends(out_bounds),
                    values: Arc::new(from_vec::<u64>(flat)),
                });
                Ok(())
            }
            other => Err(format!("where: expected Prim(P8) or List<P8>, got {:?}", other)),
        }
    }
}
impl Typed for Where_ {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let mask = tc_pop(st, "where")?;
        match mask {
            Shape::Prim(PrimWidth::W8) => {
                st.push(Shape::Prim(PrimWidth::W64));
                Ok(())
            }
            Shape::List { inner, .. } => {
                match *inner {
                    Shape::Prim(PrimWidth::W8) => {
                        st.push(Shape::List {
                            bounds: PrimWidth::W64,
                            inner: Box::new(Shape::Prim(PrimWidth::W64)),
                        });
                        Ok(())
                    }
                    other => Err(format!(
                        "where: List inner must be Prim(P8), got {}", other
                    )),
                }
            }
            other => Err(format!(
                "where: expected Prim(P8) or List<P8>, got {}", other
            )),
        }
    }
}

/// `filter` — fused `where gather`. Pops `(src, mask)` (mask on top),
/// produces a `View<source, Mask(mask)>` rather than materializing
/// eagerly. This lets chained filters compose their masks without
/// allocating intermediate columns: each `filter` step composes the new
/// mask with the previous View's mask via `compose_selectors`'s
/// Mask ∩ Mask path. Materialization happens at the boundary when a
/// consumer that doesn't have a Mask-aware path uses `pop()` (which
/// auto-materializes Views via gather), or when downstream selectors
/// chain through `compose_selectors`.
///
/// For Prod sources (`filter` on a zipped Prod), the View wraps the
/// whole Prod; per-field selection happens at materialize/iterate time
/// via the inner-Value walk.
///
/// The parser emits this automatically when it sees `where gather` in
/// sequence; users can also write `filter` directly.
#[derive(Debug)] pub struct Filter;
impl PrimOp for Filter {
    fn name(&self) -> &str { "filter" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let mask = pop(st)?;
        let src = crate::ir::stack::pop_raw(st)?;
        let ms: Arc<Vec<u8>> = match mask {
            Value::Prim(Prim::P8(m)) => m,
            other => return Err(format!("filter: mask must be Prim(P8), got {:?}", other)),
        };
        if src.len() != ms.len() {
            return Err(format!("filter: src len {} != mask len {}", src.len(), ms.len()));
        }
        // Produce a View. If src is already a View, the smart constructor
        // `view()` composes selectors — for Mask ∘ Mask this fires the
        // bitwise-AND-style composition we wrote in compose_selectors.
        st.push(crate::ir::value::view(src, crate::ir::value::Selector::Mask(ms)));
        Ok(())
    }
}
impl Typed for Filter {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let mask = tc_pop(st, "filter")?;
        if !matches!(mask, Shape::Prim(PrimWidth::W8)) {
            return Err(format!("filter: mask must be Prim(P8), got {}", mask));
        }
        let src = tc_pop(st, "filter")?;
        st.push(src);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::{from_vec, bounds_var_from_ends, prod, list};

    #[test]
    fn group_by_structured_prod_key() {
        // group by a composite (u64, u64) key — only possible now that
        // group sorts through the engine rather than requiring a Prim key.
        // keys: (1,1),(2,1),(1,1),(2,2)  vals: 10,20,30,40
        let keys = prod(vec![
            from_vec::<u64>(vec![1, 2, 1, 2]),
            from_vec::<u64>(vec![1, 1, 1, 2]),
        ]);
        let vals = from_vec::<u64>(vec![10, 20, 30, 40]);
        // group pops keys (top) then vals.
        let out = run1(&Group, vec![vals, keys]);
        let list_out = out[1].clone();
        let uniq = out[0].clone();
        // Distinct keys in sorted order: (1,1),(2,1),(2,2).
        assert_eq!(uniq, prod(vec![
            from_vec::<u64>(vec![1, 2, 2]),
            from_vec::<u64>(vec![1, 1, 2]),
        ]));
        // Grouped vals: [10,30] | [20] | [40].
        assert_eq!(list_out, list(bounds_var_from_ends(vec![2, 3, 4]), from_vec::<u64>(vec![10, 30, 20, 40])));
    }

    #[test]
    fn unique_structured_prod() {
        // unique over a composite (u64, u64) value — distinct rows, sorted.
        let keys = prod(vec![
            from_vec::<u64>(vec![1, 2, 1, 2]),
            from_vec::<u64>(vec![1, 1, 1, 2]),
        ]);
        let out = run1(&Unique, vec![keys]);
        assert_eq!(out[0], prod(vec![
            from_vec::<u64>(vec![1, 2, 2]),
            from_vec::<u64>(vec![1, 1, 2]),
        ]));
    }

    fn run1(op: &dyn PrimOp, stack: Vec<Value>) -> Vec<Value> {
        let mut st = stack;
        let mut env = Vec::new();
        op.run(&mut st, &mut env).unwrap();
        st
    }

    fn list_u64(bounds: Vec<u64>, values: Vec<u64>) -> Value {
        Value::List { bounds: bounds_var_from_ends(bounds), values: Arc::new(from_vec::<u64>(values)) }
    }

    #[test]
    fn cumsum_flat_u64() {
        let st = run1(&Cumsum { interp: Interp::U64 }, vec![from_vec::<u64>(vec![1, 2, 3, 4, 5])]);
        assert_eq!(st[0], from_vec::<u64>(vec![1, 3, 6, 10, 15]));
    }

    #[test]
    fn cumsum_per_row() {
        // Rows: [1,2], [3], [4,5,6] — cumsum per row.
        let v = list_u64(vec![2, 3, 6], vec![1, 2, 3, 4, 5, 6]);
        let st = run1(&Cumsum { interp: Interp::U64 }, vec![v]);
        let expected = list_u64(vec![2, 3, 6], vec![1, 3, 3, 4, 9, 15]);
        assert_eq!(st[0], expected);
    }

    #[test]
    fn shift_flat_by_one() {
        let st = run1(&Shift { interp: Interp::U64 }, vec![
            from_vec::<u64>(vec![1, 2, 3, 4, 5]),
            from_vec::<u64>(vec![1]),
        ]);
        assert_eq!(st[0], from_vec::<u64>(vec![0, 1, 2, 3, 4]));
    }

    #[test]
    fn shift_per_row() {
        let v = list_u64(vec![3, 6], vec![10, 20, 30, 40, 50, 60]);
        let st = run1(&Shift { interp: Interp::U64 }, vec![v, from_vec::<u64>(vec![1])]);
        let expected = list_u64(vec![3, 6], vec![0, 10, 20, 0, 40, 50]);
        assert_eq!(st[0], expected);
    }

    #[test]
    fn unique_dedupes_and_sorts() {
        let st = run1(&Unique, vec![
            from_vec::<u64>(vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]),
        ]);
        assert_eq!(st[0], from_vec::<u64>(vec![1, 2, 3, 4, 5, 6, 9]));
    }

    #[test]
    fn unique_empty() {
        let st = run1(&Unique, vec![from_vec::<u64>(vec![])]);
        assert_eq!(st[0], from_vec::<u64>(vec![]));
    }

    #[test]
    fn unique_already_sorted() {
        let st = run1(&Unique, vec![
            from_vec::<u64>(vec![1, 1, 2, 3, 3, 5]),
        ]);
        assert_eq!(st[0], from_vec::<u64>(vec![1, 2, 3, 5]));
    }

    #[test]
    fn shift_beyond_length_zeros() {
        let st = run1(&Shift { interp: Interp::U64 }, vec![
            from_vec::<u64>(vec![1, 2, 3]),
            from_vec::<u64>(vec![10]),
        ]);
        assert_eq!(st[0], from_vec::<u64>(vec![0, 0, 0]));
    }

    #[test]
    fn cumsum_empty() {
        let st = run1(&Cumsum { interp: Interp::U64 }, vec![from_vec::<u64>(vec![])]);
        assert_eq!(st[0], from_vec::<u64>(vec![]));
    }

    #[test]
    fn count_per_row() {
        let l = list_u64(vec![3, 5, 8], vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let st = run1(&Count, vec![l]);
        assert_eq!(st[0], from_vec::<u64>(vec![3, 2, 3]));
    }

    #[test]
    fn length_desugars_to_enlist_count() {
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let prog = parse("u64[1 2 3 4 5 6 7 8] length", &reg).unwrap();
        let mut st = vec![];
        let mut env = Vec::new();
        crate::ir::op::eval(&prog, &mut st, &mut env).unwrap();
        assert_eq!(st.len(), 1);
        assert_eq!(st[0], from_vec::<u64>(vec![8]));
    }

    #[test]
    fn group_sales_by_region() {
        let vals = from_vec::<u64>(vec![100, 200, 50, 300, 150, 75]);
        let keys = from_vec::<u8>(vec![0, 1, 0, 2, 1, 0]);
        let mut st = vec![vals, keys];
        let mut env = Vec::new();
        Group.run(&mut st, &mut env).unwrap();
        // Stack: K_unique, List<V>
        assert_eq!(st.len(), 2);
        assert_eq!(st[0], from_vec::<u8>(vec![0, 1, 2]));
        match &st[1] {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(bounds).unwrap();
                assert_eq!(&bnds[..], &[0u64, 3, 5, 6]);
                assert_eq!(**values, from_vec::<u64>(vec![100, 50, 75, 200, 150, 300]));
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn reduce_add_per_row() {
        let l = list_u64(vec![3, 5, 8], vec![1, 2, 3, 10, 20, 100, 200, 300]);
        let st = run1(&ReduceAdd { interp: Interp::U64 }, vec![l]);
        // Rows: [1,2,3]->6, [10,20]->30, [100,200,300]->600
        assert_eq!(st[0], from_vec::<u64>(vec![6, 30, 600]));
    }

    #[test]
    fn iota_makes_range() {
        let st = run1(&Iota, vec![from_vec::<u64>(vec![5])]);
        assert_eq!(st[0], from_vec::<u64>(vec![0, 1, 2, 3, 4]));
    }

    #[test]
    fn where_returns_positions() {
        // where : mask → positions where mask != 0.
        let mask = Value::Prim(crate::ir::value::prim_p8(vec![1, 0, 1, 0, 1]));
        let st = run1(&Where_, vec![mask]);
        assert_eq!(st[0], from_vec::<u64>(vec![0, 2, 4]));
    }

    #[test]
    fn where_then_gather_filters() {
        // Recovery of filtered values: col mask where gather.
        // The parser fuses `where gather` to `filter`, which now produces
        // a `View<Mask>`. Materialize via pop() to get the values.
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let prog = parse(
            "u64[10 20 30 40 50] bool[t f t f t] where gather",
            &reg,
        ).unwrap();
        let mut st = vec![];
        let mut env = Vec::new();
        crate::ir::op::eval(&prog, &mut st, &mut env).unwrap();
        let materialized = crate::ir::stack::pop(&mut st).unwrap();
        assert_eq!(materialized, from_vec::<u64>(vec![10, 30, 50]));
    }

    #[test]
    fn spread_repeats_by_counts() {
        let col = from_vec::<u64>(vec![100, 200, 300]);
        let counts = from_vec::<u64>(vec![3, 1, 0]);
        let st = run1(&Spread, vec![col, counts]);
        assert_eq!(st[0], from_vec::<u64>(vec![100, 100, 100, 200]));
    }

    #[test]
    fn repeat_runs_body_n_times() {
        // body: pop, add 1, push. Stack-shape preserving.
        // 5 iterations on starting value [10]: [10] → [11] → [12] → [13] → [14] → [15]
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let prog = parse("u64[10] repeat 5 { 1u64 +.u64 }", &reg).unwrap();
        let mut st: Vec<Value> = Vec::new();
        let mut env: Vec<Value> = Vec::new();
        crate::ir::op::eval(&prog, &mut st, &mut env).unwrap();
        assert_eq!(st.len(), 1);
        assert_eq!(st[0], from_vec::<u64>(vec![15]));
    }

    #[test]
    fn repeat_zero_is_noop() {
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let prog = parse("u64[42] repeat 0 { 1u64 +.u64 }", &reg).unwrap();
        let mut st: Vec<Value> = Vec::new();
        let mut env: Vec<Value> = Vec::new();
        crate::ir::op::eval(&prog, &mut st, &mut env).unwrap();
        assert_eq!(st[0], from_vec::<u64>(vec![42]));
    }

    #[test]
    fn repeat_rejects_non_shape_preserving_body() {
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        use crate::ir::shape::Shape;
        use crate::ir::typecheck::typecheck;
        let reg = OpRegistry::standard();
        // body produces one extra item per iteration (push 1u64). Not preserving.
        let prog = parse("u64[10] repeat 3 { 1u64 }", &reg).unwrap();
        let mut ts: Vec<Shape> = Vec::new();
        let mut tenv: Vec<Shape> = Vec::new();
        let result = typecheck(&prog, &mut ts, &mut tenv);
        assert!(result.is_err(), "expected typecheck to reject non-shape-preserving body");
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "count" => Some(Box::new(Count)),
            "like" => Some(Box::new(Like)),
            "head" => Some(Box::new(Head)),
            "iota" => Some(Box::new(Iota)),
            "spread" => Some(Box::new(Spread)),
            "where" => Some(Box::new(Where_)),
            "filter" => Some(Box::new(Filter)),
            "bounds>keys" => Some(Box::new(BoundsToKeys)),
            "list>bounds" => Some(Box::new(Bounds)),
            // group / unique sort by equality — interp-free (the engine
            // sorts unsigned; the partition is interp-independent).
            "group" => Some(Box::new(Group)),
            "unique" => Some(Box::new(Unique)),
            _ => None,
        }
    });
    // reduce.+.<interp>, cumsum.<interp>, shift.<interp>
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        match head {
            "reduce.+" => Some(Box::new(ReduceAdd { interp })),
            "cumsum" => Some(Box::new(Cumsum { interp })),
            "shift" => Some(Box::new(Shift { interp })),
            _ => None,
        }
    });
}
