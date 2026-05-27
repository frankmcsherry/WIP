//! List-domain ops — the biggest ops file because everything
//! list-shaped tends to land here. Roughly four clusters:
//!
//! - **Aggregations and scans**: `Group`, `Cumsum`, `Unique`,
//!   `Shift`, `ReduceAdd` (the typed `reduce.+`/`*`/`min`/`max`
//!   live elsewhere as `reduce_ops`).
//!   (No body-bearing escape hatches remain in this file — or anywhere:
//!   `each` became segmented ops, `match`/`cleave` desugar in the parser,
//!   and `repeat` was removed. The engine no longer runs any sub-program
//!   via legacy eval. A loop can return later as a dataflow construct —
//!   see `dev/LAYERING.md`.)
//! - **Shape inspection**: `Bounds`, `BoundsToKeys`, `Count`.
//!   Cheap, structural.
//! - **Construction and slicing**: `Like`, `Head`, `Iota`,
//!   `Spread`, `Where_`, `Filter`. `Filter` is the parser-peephole
//!   fusion of `where` + `gather`.
//!
//! On a first read, skim the section markers and the typed variants.

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, from_vec, bounds_var_from_ends, prod, view, Selector};
use crate::ir::shape::{Interp, Shape, bounds_as_u64};
use crate::ops::helpers::{broadcast, gather, sum_runs, sum_whole};
use crate::ops::sort::{sort_blocks, run_layout};

#[derive(Debug)] pub struct Group;
impl PrimOp for Group {
    fn name(&self) -> &str { "group" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 2)) }  // (vals, keys) → (uniq_keys, list_of_grouped_vals)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { group_run(st) }
}
/// `group` kernel (back-end `SystemOp::Group` calls this directly).
pub fn group_run(st: &mut Stack) -> Result<(), String> {
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
impl Typed for Group {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { group_tc(st) }
}
pub fn group_tc(st: &mut TypeStack) -> Result<(), String> {
        // Keys may now be any shape the sort engine handles (Prim, Prod,
        // List, Sum) — not just Prim. The unique-keys output has the same
        // shape as the keys.
        let keys = tc_pop(st, "group")?;
        let vals = tc_pop(st, "group")?;
        st.push(keys.clone());
        st.push(Shape::List { bounds: PrimWidth::W64, inner: Box::new(vals) });
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { cumsum_run(self.interp, st) }
}
impl Typed for Cumsum {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { cumsum_tc(self.interp, st) }
}
/// `cumsum.<interp>` kernel (back-end `SystemOp::Cumsum` calls this directly).
pub fn cumsum_run(interp: Interp, st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let new_values = cumsum_runs(&values, &bnds, interp)?;
                st.push(Value::List { bounds, values: Arc::new(new_values) });
            }
            Value::Prim(p) => {
                st.push(Value::Prim(cumsum_flat(&p, interp)?));
            }
            other => return Err(format!("cumsum.{}: expected Prim or List<Prim>, got {:?}", interp, other)),
        }
        Ok(())
}
pub fn cumsum_tc(interp: Interp, st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "cumsum")?;
        let w = interp.width();
        match v {
            Shape::Prim(pw) if pw == w => { st.push(Shape::Prim(w)); Ok(()) }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prim(pw) if pw == w => {
                    st.push(Shape::List { bounds, inner: Box::new(Shape::Prim(w)) });
                    Ok(())
                }
                other => Err(format!("cumsum.{}: list inner must be Prim({}), got {}", interp, w, other)),
            }
            other => Err(format!("cumsum.{}: needs Prim({}) or List<Prim({})>, got {}", interp, w, w, other)),
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { unique_run(st) }
}
/// `unique` kernel (back-end `SystemOp::Unique` calls this directly).
pub fn unique_run(st: &mut Stack) -> Result<(), String> {
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
impl Typed for Unique {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { unique_tc(st) }
}
pub fn unique_tc(st: &mut TypeStack) -> Result<(), String> {
        // Any shape the engine sorts (interp vestigial — dedup is by
        // equality). Output: the distinct elements, same shape as input.
        let v = tc_pop(st, "unique")?;
        st.push(v);
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { shift_run(self.interp, st) }
}
impl Typed for Shift {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { shift_tc(self.interp, st) }
}
/// `shift.<interp>` kernel (back-end `SystemOp::Shift` calls this directly).
pub fn shift_run(interp: Interp, st: &mut Stack) -> Result<(), String> {
        let n_v = pop(st)?;
        let n = match n_v {
            Value::Prim(Prim::P64(v)) if v.len() == 1 => v[0] as usize,
            other => return Err(format!("shift: n must be length-1 P64, got {:?}", other)),
        };
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                let bnds = bounds_as_u64(&bounds)?.to_vec();
                let new_values = shift_runs(&values, &bnds, n, interp)?;
                st.push(Value::List { bounds, values: Arc::new(new_values) });
            }
            Value::Prim(p) => {
                st.push(Value::Prim(shift_flat(&p, n, interp)?));
            }
            other => return Err(format!("shift.{}: expected Prim or List<Prim>, got {:?}", interp, other)),
        }
        Ok(())
}
pub fn shift_tc(interp: Interp, st: &mut TypeStack) -> Result<(), String> {
        let n_t = tc_pop(st, "shift")?;
        if n_t != Shape::Prim(PrimWidth::W64) {
            return Err(format!("shift: n must be Prim(P64), got {}", n_t));
        }
        let v = tc_pop(st, "shift")?;
        let w = interp.width();
        match v {
            Shape::Prim(pw) if pw == w => { st.push(Shape::Prim(w)); Ok(()) }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prim(pw) if pw == w => {
                    st.push(Shape::List { bounds, inner: Box::new(Shape::Prim(w)) });
                    Ok(())
                }
                other => Err(format!("shift.{}: list inner must be Prim({}), got {}", interp, w, other)),
            }
            other => Err(format!("shift.{}: needs Prim({}) or List<Prim({})>, got {}", interp, w, w, other)),
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { reduce_add_run(self.interp, st) }
}
impl Typed for ReduceAdd {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { reduce_add_tc(self.interp, st) }
}

/// `reduce.+` kernel (back-end `SystemOp::Reduce{Add}` calls this directly).
pub fn reduce_add_run(interp: Interp, st: &mut Stack) -> Result<(), String> {
    // pop_raw so a `View` over a flat Prim can stream through without
    // an intermediate gather (`sum_whole` handles both via for_each_prim).
    let v = crate::ir::stack::pop_raw(st)?;
    match v {
        Value::List { bounds, values } => {
            let bnds = bounds_as_u64(&bounds)?.to_vec();
            let out = sum_runs(&values, &bnds, interp)?;
            st.push(out);
        }
        other => {
            st.push(sum_whole(&other, interp)?);
        }
    }
    Ok(())
}
pub fn reduce_add_tc(interp: Interp, st: &mut TypeStack) -> Result<(), String> {
    let v = tc_pop(st, "reduce.+")?;
    let inner = match v { Shape::List { inner, .. } => *inner, other => other };
    match inner {
        Shape::Prim(w) if w == interp.width() => st.push(Shape::Prim(w)),
        other => return Err(format!("reduce.+.{}: expected Prim({}), got {}", interp, interp.width(), other)),
    }
    Ok(())
}

#[derive(Debug)] pub struct Bounds;
impl PrimOp for Bounds {
    fn name(&self) -> &str { "list>bounds" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { bounds_run(st) }
}
impl Typed for Bounds {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { bounds_tc(st) }
}
/// `list>bounds` kernel (back-end `SystemOp::Bounds` calls this directly).
pub fn bounds_run(st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, .. } => st.push(Value::Prim(bounds.to_prim())),
            other => return Err(format!("bounds: not a list, got {:?}", other)),
        }
        Ok(())
}
pub fn bounds_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "bounds")?;
        match v {
            Shape::List { bounds, .. } => st.push(Shape::Prim(bounds)),
            other => return Err(format!("bounds: not a list: {}", other)),
        }
        Ok(())
}

/// `list>ranges` — the per-row `(lower, upper)` offset ranges of a List,
/// as a `Prod[View<Range>, View<Range>]` of length N (one pair per row).
/// The two fields are zero-copy windows over the single N+1 bounds buffer,
/// offset by one (`lower = bounds[0..N]`, `upper = bounds[1..N+1]`) — the
/// logical `(lo, hi)` relation that the N+1 layout stores by sharing
/// interior endpoints. Degree is `upper - lower`; no separate `count`.
#[derive(Debug)] pub struct ListRanges;
impl PrimOp for ListRanges {
    fn name(&self) -> &str { "list>ranges" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { list_ranges_run(st) }
}
impl Typed for ListRanges {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { list_ranges_tc(st) }
}
/// `list>ranges` kernel (back-end `SystemOp::ListRanges` calls this directly).
pub fn list_ranges_run(st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, .. } => {
                let n = bounds.len() as u64;   // row count (len() is N, not N+1)
                let bp = Value::Prim(bounds.to_prim());   // the N+1 offset buffer
                let lower = view(bp.clone(), Selector::range(0, n));
                let upper = view(bp, Selector::range(1, n + 1));
                st.push(prod(vec![lower, upper]));
            }
            other => return Err(format!("list>ranges: not a list, got {:?}", other)),
        }
        Ok(())
}
pub fn list_ranges_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "list>ranges")?;
        match v {
            Shape::List { bounds, .. } =>
                st.push(Shape::Prod(vec![Shape::Prim(bounds), Shape::Prim(bounds)])),
            other => return Err(format!("list>ranges: not a list: {}", other)),
        }
        Ok(())
}

#[derive(Debug)] pub struct BoundsToKeys;
impl PrimOp for BoundsToKeys {
    fn name(&self) -> &str { "bounds>keys" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { bounds_to_keys_run(st) }
}
impl Typed for BoundsToKeys {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { bounds_to_keys_tc(st) }
}
/// `bounds>keys` kernel (back-end `SystemOp::BoundsKeys` calls this directly).
pub fn bounds_to_keys_run(st: &mut Stack) -> Result<(), String> {
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
pub fn bounds_to_keys_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "bounds>keys")?;
        match v {
            Shape::List { .. } => st.push(Shape::Prim(PrimWidth::W64)),
            other => return Err(format!("bounds>keys: not a list: {}", other)),
        }
        Ok(())
}

#[derive(Debug)] pub struct Count;
impl PrimOp for Count {
    fn name(&self) -> &str { "count" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { count_run(st) }
}
impl Typed for Count {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { count_tc(st) }
}
/// `count` kernel (back-end `SystemOp::Count` calls this directly).
pub fn count_run(st: &mut Stack) -> Result<(), String> {
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
pub fn count_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "count")?;
        match v {
            Shape::List { .. } => st.push(Shape::Prim(PrimWidth::W64)),
            other => return Err(format!("count: not a list: {}", other)),
        }
        Ok(())
}

#[derive(Debug)] pub struct Like;
impl PrimOp for Like {
    fn name(&self) -> &str { "like" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (template, scalar) → broadcast
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { like_run(st) }
}
impl Typed for Like {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { like_tc(st) }
}
/// `like` kernel (back-end `SystemOp::Like` calls this directly).
pub fn like_run(st: &mut Stack) -> Result<(), String> {
        let scalar = pop(st)?;
        let template = pop(st)?;
        if scalar.len() != 1 {
            return Err(format!("like: scalar must be length-1, got {}", scalar.len()));
        }
        st.push(broadcast(&scalar, template.len()));
        Ok(())
}
pub fn like_tc(st: &mut TypeStack) -> Result<(), String> {
        let s = tc_pop(st, "like")?;
        let _t = tc_pop(st, "like")?;
        st.push(s);
        Ok(())
}

#[derive(Debug)] pub struct Head;
impl PrimOp for Head {
    fn name(&self) -> &str { "head" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { head_run(st) }
}
impl Typed for Head {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { head_tc(st) }
}
/// `head` kernel (back-end `SystemOp::Head` calls this directly).
pub fn head_run(st: &mut Stack) -> Result<(), String> {
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
pub fn head_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "head")?;
        match v {
            Shape::List { inner, .. } => st.push(*inner),
            other => return Err(format!("head: not a list: {}", other)),
        }
        Ok(())
}

#[derive(Debug)] pub struct Iota;
impl PrimOp for Iota {
    fn name(&self) -> &str { "iota" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { iota_run(st) }
}
impl Typed for Iota {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { iota_tc(st) }
}
/// `iota` kernel (back-end `SystemOp::Iota` calls this directly).
pub fn iota_run(st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        let n = match v {
            Value::Prim(Prim::P64(x)) if x.len() == 1 => x[0],
            other => return Err(format!("iota: need length-1 P64, got {:?}", other)),
        };
        st.push(from_vec::<u64>((0..n).collect()));
        Ok(())
}
pub fn iota_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "iota")?;
        if v != Shape::Prim(PrimWidth::W64) { return Err(format!("iota: needs Prim(P64), got {}", v)); }
        st.push(Shape::Prim(PrimWidth::W64));
        Ok(())
}

#[derive(Debug)] pub struct Spread;
impl PrimOp for Spread {
    fn name(&self) -> &str { "spread" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (col, counts) → spread col
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { spread_run(st) }
}
impl Typed for Spread {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { spread_tc(st) }
}
/// `spread` kernel (back-end `SystemOp::Spread` calls this directly).
pub fn spread_run(st: &mut Stack) -> Result<(), String> {
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
pub fn spread_tc(st: &mut TypeStack) -> Result<(), String> {
        let counts = tc_pop(st, "spread")?;
        if counts != Shape::Prim(PrimWidth::W64) { return Err(format!("spread: counts must be Prim(P64), got {}", counts)); }
        let col = tc_pop(st, "spread")?;
        st.push(col);
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { where_run(st) }
}
/// `where` kernel (back-end `SystemOp::Where` calls this directly).
pub fn where_run(st: &mut Stack) -> Result<(), String> {
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
impl Typed for Where_ {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { where_tc(st) }
}
pub fn where_tc(st: &mut TypeStack) -> Result<(), String> {
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { filter_run(st) }
}
/// `filter` kernel (back-end `SystemOp::Filter` calls this directly).
pub fn filter_run(st: &mut Stack) -> Result<(), String> {
        let mask = crate::ir::stack::pop_raw(st)?;
        let src = crate::ir::stack::pop_raw(st)?;

        // Segmented (count-changing) path, keyed on the mask being a
        // `List<P8>`: a `List<T>` filtered by an aligned `List<P8>` (equal
        // bounds) → `List<T>`, keeping per-row the elements where the mask is
        // true. Unlike the flat path this is eager: per-row output length
        // differs, so we compute new bounds from the per-row survivor counts
        // (the "nest by counts" idiom, folded into one op). The src may be a
        // row-shaped View (`p_adj p_pos view`) — its shape is List, so we
        // materialize it into a real List first.
        if let Value::List { bounds: mb, values: mv } = &mask {
            let m = match mv.as_ref() {
                Value::Prim(Prim::P8(m)) => m,
                other => return Err(format!("segmented filter: mask inner must be Prim(P8), got {:?}", other)),
            };
            let src_list = crate::ir::stack::materialize_top(src)?;
            let (sb, sv) = match &src_list {
                Value::List { bounds, values } => (bounds, values),
                other => return Err(format!("segmented filter: src must be List-shaped, got {:?}", other)),
            };
            if sb != mb {
                return Err("segmented filter: bounds differ".into());
            }
            let bnds = bounds_as_u64(sb)?;
            let mut keep: Vec<usize> = Vec::new();
            let mut ends: Vec<u64> = Vec::with_capacity(sb.len());
            for w in bnds.windows(2) {
                let (lo, hi) = (w[0] as usize, w[1] as usize);
                for j in lo..hi { if m[j] != 0 { keep.push(j); } }
                ends.push(keep.len() as u64);
            }
            let new_vals = gather(sv, &keep)?;
            st.push(Value::List { bounds: bounds_var_from_ends(ends), values: Arc::new(new_vals) });
            return Ok(());
        }

        // Flat path: produce a View. If src is already a View, the smart
        // constructor `view()` composes selectors — for Mask ∘ Mask this
        // fires the bitwise-AND-style composition in compose_selectors.
        let ms: Arc<Vec<u8>> = match crate::ops::helpers::materialize(mask)? {
            Value::Prim(Prim::P8(m)) => m,
            other => return Err(format!("filter: mask must be Prim(P8), got {:?}", other)),
        };
        if src.len() != ms.len() {
            return Err(format!("filter: src len {} != mask len {}", src.len(), ms.len()));
        }
        st.push(crate::ir::value::view(src, crate::ir::value::Selector::Mask(ms)));
        Ok(())
}
impl Typed for Filter {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { filter_tc(st) }
}
pub fn filter_tc(st: &mut TypeStack) -> Result<(), String> {
        let mask = tc_pop(st, "filter")?;
        let src = tc_pop(st, "filter")?;
        match (&src, &mask) {
            // Flat: mask is P8 (src may be any shape — e.g. a zipped Prod).
            (_, Shape::Prim(PrimWidth::W8)) => { st.push(src); Ok(()) }
            // Segmented: List<T> filtered by an equal-bounds List<P8> → List<T>.
            (Shape::List { bounds: sb, .. }, Shape::List { bounds: mb, inner })
                if sb == mb && crate::ir::shape::prim_width(inner) == Some(PrimWidth::W8) => {
                st.push(src); Ok(())
            }
            _ => Err(format!("filter: mask must be Prim(P8) or equal-bounds List<P8>, got src {} mask {}", src, mask)),
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
    fn enlist_count_is_flat_total() {
        // `length` was retired as atom-form sugar; the flat total is now
        // spelled `enlist count` (the SEQ-first `count` rejects a bare Prim).
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let prog = parse("u64[1 2 3 4 5 6 7 8] enlist count", &reg).unwrap();
        let (g, _) = crate::pipeline::build(prog).unwrap();
        let st = crate::pipeline::eval_graph(&g).unwrap();
        assert_eq!(st.len(), 1);
        assert_eq!(st[0], from_vec::<u64>(vec![8]));
    }

    #[test]
    fn list_ranges_are_offset_views() {
        // `list>ranges` exposes a List's per-row (lower, upper) offsets as two
        // windows over the N+1 bounds buffer: lower = bounds[0..N], upper =
        // bounds[1..N+1]. Degree = upper - lower must equal `count`.
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let run = |src: &str| {
            let prog = parse(src, &reg).unwrap();
            let (g, _) = crate::pipeline::build(prog).unwrap();
            crate::pipeline::eval_graph(&g).unwrap().pop().unwrap()
        };
        // [10 20] ; [30 40 50] — bounds [0, 2, 5].
        let base = "u64[10 20 30 40 50] u64[0 2 5] nest list>ranges";
        // Materialize each view via an identity gather to compare to a Prim.
        assert_eq!(run(&format!("{base} .0 u64[0 1] gather")), from_vec::<u64>(vec![0, 2]));
        assert_eq!(run(&format!("{base} .1 u64[0 1] gather")), from_vec::<u64>(vec![2, 5]));
        assert_eq!(run(&format!("{base} dup .1 swap .0 -.u64")), from_vec::<u64>(vec![2, 3]));
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
        let (g, _) = crate::pipeline::build(prog).unwrap();
        let mut st = crate::pipeline::eval_graph(&g).unwrap();
        let materialized = crate::ir::stack::pop(&mut st).unwrap();
        assert_eq!(materialized, from_vec::<u64>(vec![10, 30, 50]));
    }

    #[test]
    fn segmented_filter_keeps_per_row_survivors() {
        // src rows [10,20,30] | [40,50] | [60], mask [1,0,1] | [0,1] | [0].
        // Per-row survivors: [10,30] | [50] | [], new bounds [0,2,3,3].
        let src = list_u64(vec![3, 5, 6], vec![10, 20, 30, 40, 50, 60]);
        let mask = Value::List {
            bounds: bounds_var_from_ends(vec![3, 5, 6]),
            values: Arc::new(Value::Prim(crate::ir::value::prim_p8(vec![1, 0, 1, 0, 1, 0]))),
        };
        let out = run1(&Filter, vec![src, mask]);
        assert_eq!(out[0], list_u64(vec![2, 3, 3], vec![10, 30, 50]));
    }

    #[test]
    fn segmented_filter_mismatched_bounds_errors() {
        let src = list_u64(vec![3, 5], vec![10, 20, 30, 40, 50]);
        let mask = Value::List {
            bounds: bounds_var_from_ends(vec![2, 5]),
            values: Arc::new(Value::Prim(crate::ir::value::prim_p8(vec![1, 0, 1, 0, 1]))),
        };
        let mut st = vec![src, mask];
        let mut env = Vec::new();
        assert!(Filter.run(&mut st, &mut env).is_err());
    }

    #[test]
    fn spread_repeats_by_counts() {
        let col = from_vec::<u64>(vec![100, 200, 300]);
        let counts = from_vec::<u64>(vec![3, 1, 0]);
        let st = run1(&Spread, vec![col, counts]);
        assert_eq!(st[0], from_vec::<u64>(vec![100, 100, 100, 200]));
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
            "list>ranges" => Some(Box::new(ListRanges)),
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
