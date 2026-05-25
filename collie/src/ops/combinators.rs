//! Structural ops over the value universe — constructors and
//! eliminators for `Prod`, `Sum`, and `List`. Three families:
//!
//! - **Prod**: `zipN`, `detupleN`, `proj` (`.i`), `entupleN`.
//! - **Sum**: `injectN`, `split`, `partitionN`, `branch`, `match`,
//!   `cleave` (`.{ path0 ; path1 }`).
//! - **List shape**: `nest`, `nest.stride`, `flatten`, `enlist`,
//!   `unlist`.
//!
//! `match` and `cleave` are body-bearing but dispatch per-lane /
//! per-path (bounded by the type structure, not by N rows), so
//! they're principle-2-aligned in spirit — see `PRINCIPLES.md`.

use crate::ir::op::{PrimOp, eval};
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Op, Typed, TypeStack, TypeEnv, tc_pop, typecheck};
use crate::ir::value::{Value, PrimWidth, prod, sum, list};
use crate::ir::shape::{Shape, disc_as_u8};
use crate::ops::helpers::{broadcast, gather, merge_by_disc};

#[derive(Debug)] pub struct ZipN { pub n: usize }
impl PrimOp for ZipN {
    fn name(&self) -> &str { "zip" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let n = self.n;
        let mut fs = Vec::with_capacity(n);
        for _ in 0..n { fs.push(pop(st)?); }
        fs.reverse();
        let l0 = fs[0].len();
        if !fs.iter().all(|f| f.len() == l0) {
            return Err(format!("zip{}: mismatched lengths {:?}", n,
                               fs.iter().map(|f| f.len()).collect::<Vec<_>>()));
        }
        st.push(prod(fs));
        Ok(())
    }
}
impl Typed for ZipN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < self.n { return Err(format!("zip{}: stack {}", self.n, st.len())); }
        let mut fs = Vec::with_capacity(self.n);
        for _ in 0..self.n { fs.push(st.pop().unwrap()); }
        fs.reverse();
        st.push(Shape::Prod(fs));
        Ok(())
    }
}

/// `detuple.K` (or `detuple` for K=2) — opposite of `zipK`/`entupleK`. Pops a
/// `Prod[A, B, …, K-th]` and pushes the fields onto the stack, last field on
/// top. K is parse-time; the prod must have exactly K fields.
#[derive(Debug)] pub struct DetupleN { pub n: usize }
impl PrimOp for DetupleN {
    fn name(&self) -> &str { "detuple" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, self.n)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::Prod(fs) => {
                if fs.len() != self.n {
                    return Err(format!("detuple{}: prod has {} fields", self.n, fs.len()));
                }
                for f in fs.iter() { st.push(f.clone()); }
                Ok(())
            }
            other => Err(format!("detuple{}: not a prod: {:?}", self.n, other)),
        }
    }
}
impl Typed for DetupleN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "detuple")?;
        match v {
            Shape::Prod(fs) => {
                if fs.len() != self.n {
                    return Err(format!("detuple{}: prod has {} fields", self.n, fs.len()));
                }
                for f in fs { st.push(f); }
                Ok(())
            }
            other => Err(format!("detuple{}: not a prod: {}", self.n, other)),
        }
    }
}

#[derive(Debug)] pub struct Proj { pub i: usize }
impl PrimOp for Proj {
    fn name(&self) -> &str { "." }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // Prod (or List<Prod>) → field
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::Prod(fs) => {
                if self.i >= fs.len() {
                    return Err(format!(".{}: only {} fields", self.i, fs.len()));
                }
                st.push(fs[self.i].clone());
                Ok(())
            }
            // List<Prod[A, B, C]> projects to List<field_i>, preserving
            // the outer bounds. Lets cleave-on-grouped patterns work:
            // `grouped .{ count ; .0 reduce.+ ; .1 reduce.+ }` without
            // needing an `each` to descend through the list.
            Value::List { bounds, values } => match &*values {
                Value::Prod(fs) => {
                    if self.i >= fs.len() {
                        return Err(format!(".{}: List<Prod> has {} fields", self.i, fs.len()));
                    }
                    let inner = fs[self.i].clone();
                    st.push(Value::List { bounds, values: std::sync::Arc::new(inner) });
                    Ok(())
                }
                other => Err(format!(".{}: List inner is not a product: {:?}", self.i, other)),
            },
            other => Err(format!(".{}: not a product: {:?}", self.i, other)),
        }
    }
}
impl Typed for Proj {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, ".i")?;
        match v {
            Shape::Prod(fs) => {
                if self.i >= fs.len() { return Err(format!(".{}: prod has {} fields", self.i, fs.len())); }
                st.push(fs.into_iter().nth(self.i).unwrap());
                Ok(())
            }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prod(fs) => {
                    if self.i >= fs.len() {
                        return Err(format!(".{}: List<Prod> has {} fields", self.i, fs.len()));
                    }
                    let field = fs.into_iter().nth(self.i).unwrap();
                    st.push(Shape::List { bounds, inner: Box::new(field) });
                    Ok(())
                }
                other => Err(format!(".{}: List inner is not a product: {}", self.i, other)),
            },
            other => Err(format!(".{}: not a product: {}", self.i, other)),
        }
    }
}

#[derive(Debug)] pub struct InjectN { pub n: usize }
impl PrimOp for InjectN {
    fn name(&self) -> &str { "inject" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n + 1, 1)) }  // disc + N lanes → Sum
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let mut lanes = Vec::with_capacity(self.n);
        for _ in 0..self.n { lanes.push(pop(st)?); }
        lanes.reverse();
        let disc_v = pop(st)?;
        let disc = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("inject: disc must be Prim, got {:?}", other)),
        };
        let disc_u8 = disc_as_u8(&disc)?;
        for (k, lane) in lanes.iter().enumerate() {
            let want = disc_u8.iter().filter(|&&d| d as usize == k).count();
            if lane.len() != want {
                return Err(format!("inject{}: lane {} has len {}, expected {}",
                                   self.n, k, lane.len(), want));
            }
        }
        st.push(sum(disc, lanes));
        Ok(())
    }
}
impl Typed for InjectN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        if st.len() < self.n + 1 { return Err(format!("inject{}: stack {}", self.n, st.len())); }
        let mut lanes = Vec::with_capacity(self.n);
        for _ in 0..self.n { lanes.push(st.pop().unwrap()); }
        lanes.reverse();
        let disc = st.pop().unwrap();
        let disc_w = match disc {
            Shape::Prim(w) => w,
            other => return Err(format!("inject{}: disc must be Prim, got {}", self.n, other)),
        };
        st.push(Shape::Sum { disc: disc_w, lanes });
        Ok(())
    }
}

#[derive(Debug)] pub struct Split;
impl PrimOp for Split {
    fn name(&self) -> &str { "split" }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::Sum { disc, lanes } => {
                st.push(Value::Prim(disc));
                for lane in lanes.iter() { st.push(lane.clone()); }
                Ok(())
            }
            other => Err(format!("split: not a sum: {:?}", other)),
        }
    }
}
impl Typed for Split {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "split")?;
        match v {
            Shape::Sum { disc, lanes } => {
                st.push(Shape::Prim(disc));
                for l in lanes { st.push(l); }
                Ok(())
            }
            other => Err(format!("split: not a sum: {}", other)),
        }
    }
}

#[derive(Debug)] pub struct PartitionN { pub n: usize }
impl PrimOp for PartitionN {
    fn name(&self) -> &str { "partition" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, self.n)) }  // (col, disc) → N lanes
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let disc_v = pop(st)?;
        let disc = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("partition: disc must be Prim, got {:?}", other)),
        };
        let disc_u8 = disc_as_u8(&disc)?.to_vec();
        let col = pop(st)?;
        if col.len() != disc_u8.len() {
            return Err(format!("partition{}: col len {} != disc len {}", self.n, col.len(), disc_u8.len()));
        }
        let mut lanes: Vec<Vec<usize>> = vec![Vec::new(); self.n];
        for (i, &d) in disc_u8.iter().enumerate() {
            let k = d as usize;
            if k >= self.n { return Err(format!("partition{}: disc {} out of range", self.n, k)); }
            lanes[k].push(i);
        }
        for idxs in lanes {
            st.push(gather(&col, &idxs)?);
        }
        Ok(())
    }
}
impl Typed for PartitionN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let disc = tc_pop(st, "partition")?;
        match disc {
            Shape::Prim(_) => {}
            other => return Err(format!("partition{}: disc must be Prim, got {}", self.n, other)),
        }
        let col = tc_pop(st, "partition")?;
        for _ in 0..self.n { st.push(col.clone()); }
        Ok(())
    }
}

/// `branch.K` — the *constructor* pair for `match`. Pops `(col, disc)` and
/// pushes `Sum<disc | K × inner>`. Errors if any disc value is `>= K`.
///
/// `K` is parse-time (default 2). `branch.4` (K=4), bare `branch` (K=2), and
/// `branch.K` all parse.
#[derive(Debug)] pub struct Branch { pub k: usize }
impl PrimOp for Branch {
    fn name(&self) -> &str { "branch" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (col, disc) → sum
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let disc_v = pop(st)?;
        let disc_prim = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("branch.{}: disc must be Prim, got {:?}", self.k, other)),
        };
        let disc_u8: Vec<u8> = disc_as_u8(&disc_prim)?.to_vec();
        let col = pop(st)?;
        if col.len() != disc_u8.len() {
            return Err(format!(
                "branch.{}: col len {} != disc len {}", self.k, col.len(), disc_u8.len()
            ));
        }
        for &d in &disc_u8 {
            if (d as usize) >= self.k {
                return Err(format!("branch.{}: disc value {} >= K", self.k, d));
            }
        }

        // Partition by disc, build lanes.
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); self.k];
        for (i, &d) in disc_u8.iter().enumerate() {
            buckets[d as usize].push(i);
        }
        let mut lanes: Vec<Value> = Vec::with_capacity(self.k);
        for idxs in &buckets {
            lanes.push(gather(&col, idxs)?);
        }

        st.push(sum(crate::ir::value::prim_p8(disc_u8), lanes));
        Ok(())
    }
}
impl Typed for Branch {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let disc = tc_pop(st, "branch")?;
        match disc {
            Shape::Prim(_) => {}
            other => return Err(format!("branch.{}: disc must be Prim, got {}", self.k, other)),
        }
        let col = tc_pop(st, "branch")?;
        let mut lanes = Vec::with_capacity(self.k);
        for _ in 0..self.k { lanes.push(col.clone()); }
        st.push(Shape::Sum { disc: PrimWidth::W8, lanes });
        Ok(())
    }
}

#[derive(Debug)] pub struct Match { pub arms: Vec<Vec<Box<dyn Op>>> }
impl PrimOp for Match {
    fn name(&self) -> &str { "match" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // sum → merged value
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let (disc, lanes) = match v {
            Value::Sum { disc, lanes } => (disc, lanes),
            other => return Err(format!("match: not a sum: {:?}", other)),
        };
        if lanes.len() != self.arms.len() {
            return Err(format!("match: {} arms, {} variants", self.arms.len(), lanes.len()));
        }
        let mut outputs = Vec::with_capacity(self.arms.len());
        for (arm, lane) in self.arms.iter().zip(lanes.iter()) {
            let lane = lane.clone();
            let mut sub = vec![lane.clone()];
            let target_len = lane.len();
            eval(arm, &mut sub, env)?;
            if sub.len() != 1 {
                return Err(format!("match arm left {} values on stack", sub.len()));
            }
            let mut out = sub.pop().unwrap();
            if out.len() == 1 && target_len != 1 {
                out = broadcast(&out, target_len);
            }
            if out.len() != target_len {
                return Err(format!("match arm produced len {}, expected {}", out.len(), target_len));
            }
            outputs.push(out);
        }
        let disc_u8 = disc_as_u8(&disc)?.to_vec();
        let merged = merge_by_disc(&disc_u8, &outputs)?;
        st.push(merged);
        Ok(())
    }
}
impl Typed for Match {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "match")?;
        let lanes = match v {
            Shape::Sum { lanes, .. } => lanes,
            other => return Err(format!("match: expected Sum, got {}", other)),
        };
        if self.arms.len() != lanes.len() {
            return Err(format!("match: {} arms but {} variants", self.arms.len(), lanes.len()));
        }
        let mut common: Option<Shape> = None;
        for (k, (arm, lane)) in self.arms.iter().zip(lanes.iter()).enumerate() {
            let mut sub = vec![lane.clone()];
            typecheck(arm, &mut sub, env).map_err(|e| format!("match arm {}: {}", k, e))?;
            if sub.len() != 1 {
                return Err(format!("match arm {}: leaves {} values on type-stack", k, sub.len()));
            }
            let out = sub.pop().unwrap();
            match &common {
                None => common = Some(out),
                Some(o) if o == &out => {}
                Some(o) => return Err(format!("match arms disagree: arm 0 -> {}, arm {} -> {}", o, k, out)),
            }
        }
        st.push(common.unwrap());
        Ok(())
    }
}

/// `.{ p0 ; p1 ; ... ; pN }` — tree navigation / cleave combinator.
///
/// Pops one value `v`, runs each path `pi` against a fresh copy of `v`, and
/// pushes a `Prod[p0(v), p1(v), ..., pN(v)]`. Each `pi` must consume exactly
/// one value and leave exactly one. Generalizes chained projections —
/// `.{ .0 ; .2 }` extracts fields 0 and 2; `.{ .0 ; .1 +.u64 }` lets each path
/// be an arbitrary subprogram.
#[derive(Debug)] pub struct Cleave { pub paths: Vec<Vec<Box<dyn Op>>> }
impl PrimOp for Cleave {
    fn name(&self) -> &str { ".{}" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // value → Prod[path_i(value)]
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let mut results = Vec::with_capacity(self.paths.len());
        for (i, path) in self.paths.iter().enumerate() {
            let mut sub = vec![v.clone()];
            crate::ir::op::eval(path, &mut sub, env)?;
            if sub.len() != 1 {
                return Err(format!(".{{}}: path {} left {} values, expected 1", i, sub.len()));
            }
            results.push(sub.pop().unwrap());
        }
        st.push(prod(results));
        Ok(())
    }
}
impl Typed for Cleave {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, ".{}")?;
        let mut shapes = Vec::with_capacity(self.paths.len());
        for (i, path) in self.paths.iter().enumerate() {
            let mut sub = vec![v.clone()];
            crate::ir::typecheck::typecheck(path, &mut sub, env)
                .map_err(|e| format!(".{{}}: path {}: {}", i, e))?;
            if sub.len() != 1 {
                return Err(format!(".{{}}: path {} leaves {} types, expected 1", i, sub.len()));
            }
            shapes.push(sub.pop().unwrap());
        }
        st.push(Shape::Prod(shapes));
        Ok(())
    }
}

#[derive(Debug)] pub struct Nest;
impl PrimOp for Nest {
    fn name(&self) -> &str { "nest" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (values, bounds) → list
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let bounds_v = pop(st)?;
        let bounds = match bounds_v {
            Value::Prim(p) => crate::ir::value::BoundsRepr::Var(p),
            other => return Err(format!("nest: bounds must be Prim, got {:?}", other)),
        };
        let values = pop(st)?;
        // Validate: bounds are the canonical N+1 start-offset form —
        // `[0, e₀, …, e_{N-1}]`, monotone non-decreasing, leading 0, last
        // entry equal to values.len(). Catches user errors at the boundary
        // rather than panicking deep inside a downstream op.
        let n = values.len() as u64;
        let bnds: Vec<u64> = bounds.iter_starts().collect();
        if bnds.is_empty() {
            return Err("nest: bounds must contain at least the leading 0".into());
        }
        if bnds[0] != 0 {
            return Err(format!("nest: first bound must be 0, got {}", bnds[0]));
        }
        for w in bnds.windows(2) {
            if w[1] < w[0] {
                return Err(format!("nest: bounds not monotone ({} < {})", w[1], w[0]));
            }
        }
        let last = *bnds.last().unwrap();
        if last != n {
            return Err(format!(
                "nest: last bound {} != values len {}", last, n
            ));
        }
        st.push(list(bounds, values));
        Ok(())
    }
}
impl Typed for Nest {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let bounds = tc_pop(st, "nest")?;
        let bw = match bounds {
            Shape::Prim(w) => w,
            other => return Err(format!("nest: bounds must be Prim, got {}", other)),
        };
        let v = tc_pop(st, "nest")?;
        st.push(Shape::List { bounds: bw, inner: Box::new(v) });
        Ok(())
    }
}

/// `nest.stride` — pop a `values` Value and wrap it as a List whose bounds are
/// a Stride: `stride = inner_length / count`. Reads a literal `count` from the
/// top of the stack first (P64 length-1).
///
/// Stack: `values count -- list`
///
/// Errors if `values.len() % count != 0`. The List materializes only when
/// bounds are queried; consumers that just iterate (each/reduce/count/length)
/// use the Stride directly.
#[derive(Debug)] pub struct NestStride;
impl PrimOp for NestStride {
    fn name(&self) -> &str { "nest.stride" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (values, count) → list
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let count = match pop(st)? {
            Value::Prim(crate::ir::value::Prim::P64(v)) if v.len() == 1 => v[0],
            other => return Err(format!("nest.stride: need length-1 P64 count, got {:?}", other)),
        };
        let values = pop(st)?;
        let n = values.len() as u64;
        if count == 0 {
            return Err("nest.stride: count must be > 0".into());
        }
        if n % count != 0 {
            return Err(format!("nest.stride: values len {} not divisible by count {}", n, count));
        }
        let stride = n / count;
        let bounds = crate::ir::value::bounds_stride(stride, count);
        st.push(list(bounds, values));
        Ok(())
    }
}
impl Typed for NestStride {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let count = tc_pop(st, "nest.stride")?;
        if count != Shape::Prim(crate::ir::value::PrimWidth::W64) {
            return Err(format!("nest.stride: count must be Prim(P64), got {}", count));
        }
        let v = tc_pop(st, "nest.stride")?;
        st.push(Shape::List { bounds: crate::ir::value::PrimWidth::W64, inner: Box::new(v) });
        Ok(())
    }
}

/// `enlist` — wrap `SEQ<T>` as `SEQ<LIST<T>>` of length 1, where the single
/// row of the output list contains the entire input as its inner content.
///
/// Stack effect: `T -> List<T>` (a one-row list whose row is the input).
///
/// Conceptual model: every collie value is implicitly a SEQUENCE; `enlist`
/// inserts a `LIST` element-type wrapping around `T`, producing a length-1
/// sequence-of-lists. The dual of `unlist`.
///
/// Zero-cost: builds bounds `[len(input)]` and points values at the input.
/// Recovers the "atom semantics" for sequence-shaped ops — e.g.
/// `a enlist b enlist intersect unlist` runs intersect on two flat
/// columns as a single-row intersection problem.
#[derive(Debug)] pub struct Enlist;
impl PrimOp for Enlist {
    fn name(&self) -> &str { "enlist" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let n = v.len() as u64;
        let bounds = crate::ir::value::bounds_var_from_ends(vec![n]);
        st.push(crate::ir::value::list(bounds, v));
        Ok(())
    }
}
impl Typed for Enlist {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "enlist")?;
        st.push(Shape::List {
            bounds: PrimWidth::W64,
            inner: Box::new(v),
        });
        Ok(())
    }
}

/// `unlist` — extract the sole row of a length-1 `List<T>` as a plain `T`.
///
/// Stack effect: `List<T> -> T` (extracts the row; errors if list length ≠ 1).
///
/// Dual of `enlist`. Together they let ops that take sequence-of-lists
/// (`intersect`, `search`, etc.) be invoked on a single pair of flat
/// sequences with no syntactic ceremony.
#[derive(Debug)] pub struct Unlist;
impl PrimOp for Unlist {
    fn name(&self) -> &str { "unlist" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                if bounds.len() != 1 {
                    return Err(format!(
                        "unlist: list must have exactly 1 row, got {}", bounds.len()
                    ));
                }
                st.push((*values).clone());
                Ok(())
            }
            other => Err(format!("unlist: expected List, got {:?}", other)),
        }
    }
}
impl Typed for Unlist {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "unlist")?;
        match v {
            Shape::List { inner, .. } => {
                st.push(*inner);
                Ok(())
            }
            other => Err(format!("unlist: expected List, got {}", other)),
        }
    }
}

#[derive(Debug)] pub struct Flatten;
impl PrimOp for Flatten {
    fn name(&self) -> &str { "flatten" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 2)) }  // list → (values, bounds)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::List { bounds, values } => {
                st.push((*values).clone());
                st.push(Value::Prim(bounds.to_prim()));
                Ok(())
            }
            other => Err(format!("flatten: not a list: {:?}", other)),
        }
    }
}
impl Typed for Flatten {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "flatten")?;
        match v {
            Shape::List { bounds, inner } => {
                st.push(*inner);
                st.push(Shape::Prim(bounds));
                Ok(())
            }
            other => Err(format!("flatten: not a list: {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::{from_vec, prim_p8, prim_p64};

    fn run1(op: &dyn PrimOp, stack: Vec<Value>) -> Vec<Value> {
        let mut st = stack;
        let mut env = Vec::new();
        op.run(&mut st, &mut env).unwrap();
        st
    }

    #[test]
    fn zip2_bundles() {
        let st = run1(&ZipN { n: 2 }, vec![
            from_vec::<u64>(vec![1, 2, 3]),
            from_vec::<u64>(vec![10, 20, 30]),
        ]);
        match &st[0] {
            Value::Prod(fs) => {
                assert_eq!(fs.len(), 2);
                assert_eq!(fs[0], from_vec::<u64>(vec![1, 2, 3]));
                assert_eq!(fs[1], from_vec::<u64>(vec![10, 20, 30]));
            }
            _ => panic!("expected Prod"),
        }
    }

    #[test]
    fn proj_field_one() {
        let p = prod(vec![
            from_vec::<u64>(vec![1, 2]),
            from_vec::<u64>(vec![100, 200]),
        ]);
        let st = run1(&Proj { i: 1 }, vec![p]);
        assert_eq!(st[0], from_vec::<u64>(vec![100, 200]));
    }

    #[test]
    fn nest_then_flatten_roundtrip() {
        let values = from_vec::<u64>(vec![1, 2, 3, 4, 5]);
        // Canonical bounds: N+1 start-offsets with leading 0 and trailing total.
        let bounds = Value::Prim(prim_p64(vec![0, 2, 5]));
        let st = run1(&Nest, vec![values.clone(), bounds.clone()]);
        match &st[0] {
            Value::List { bounds: b, values: v } => {
                assert_eq!(Value::Prim(b.to_prim()), bounds);
                assert_eq!(**v, values);
            }
            _ => panic!("expected List"),
        }
        let st2 = run1(&Flatten, st);
        // Flatten pushes (values, bounds)
        assert_eq!(st2[0], values);
        assert_eq!(st2[1], bounds);
    }

    #[test]
    fn inject_split_roundtrip() {
        // disc:[0,1,0], lane0 (u64)=[a,b], lane1 (u64)=[c]
        let disc = Value::Prim(prim_p8(vec![0, 1, 0]));
        let lane0 = from_vec::<u64>(vec![10, 30]);
        let lane1 = from_vec::<u64>(vec![20]);
        let mut st = vec![disc.clone(), lane0.clone(), lane1.clone()];
        let mut env = Vec::new();
        InjectN { n: 2 }.run(&mut st, &mut env).unwrap();
        // Now stack has one Sum value
        Split.run(&mut st, &mut env).unwrap();
        // Stack should be [disc, lane0, lane1]
        assert_eq!(st.len(), 3);
        assert_eq!(st[0], disc);
        assert_eq!(st[1], lane0);
        assert_eq!(st[2], lane1);
    }

    #[test]
    fn partition_by_disc() {
        // col = [10, 20, 30, 40, 50], disc = [0, 1, 0, 1, 0]
        let col = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let disc = Value::Prim(prim_p8(vec![0, 1, 0, 1, 0]));
        let mut st = vec![col, disc];
        let mut env = Vec::new();
        PartitionN { n: 2 }.run(&mut st, &mut env).unwrap();
        // Stack: lane0 (where disc==0), lane1 (where disc==1)
        assert_eq!(st.len(), 2);
        assert_eq!(st[0], from_vec::<u64>(vec![10, 30, 50]));
        assert_eq!(st[1], from_vec::<u64>(vec![20, 40]));
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "split" => Some(Box::new(Split)),
            "nest" => Some(Box::new(Nest)),
            "nest.stride" => Some(Box::new(NestStride)),
            "flatten" => Some(Box::new(Flatten)),
            "enlist" => Some(Box::new(Enlist)),
            "unlist" => Some(Box::new(Unlist)),
            _ => None,
        }
    });
    // entuple.K / detuple.K / injectN / partitionN
    // - `entuple` / `detuple` are the structural verbs: bundle / unbundle K
    //   stack items. `entuple.K` accepts any K; bare `entuple`/`detuple`
    //   default to K=2. (The `zipK` alias for `entuple.K` was retired —
    //   one spelling for the bundle op.)
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        if let Some(rest) = t.strip_prefix("entuple") {
            if rest.is_empty() { return Some(Box::new(ZipN { n: 2 })); }
            if let Some(rest) = rest.strip_prefix('.') {
                if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(ZipN { n })); }
            }
        }
        if let Some(rest) = t.strip_prefix("detuple") {
            if rest.is_empty() { return Some(Box::new(DetupleN { n: 2 })); }
            if let Some(rest) = rest.strip_prefix('.') {
                if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(DetupleN { n })); }
            }
        }
        if let Some(rest) = t.strip_prefix("inject") {
            if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(InjectN { n })); }
        }
        if let Some(rest) = t.strip_prefix("partition") {
            if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(PartitionN { n })); }
        }
        None
    });
    // branch / branch.K
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let rest = match t.strip_prefix("branch") {
            Some(r) => r,
            None => return None,
        };
        let k = if rest.is_empty() {
            2usize
        } else {
            let n = rest.strip_prefix('.')?.parse::<usize>().ok()?;
            if n < 2 { return None; }
            n
        };
        Some(Box::new(Branch { k }))
    });
    // .i — projection
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        if let Some(rest) = t.strip_prefix('.') {
            if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(Proj { i: n })); }
        }
        None
    });
}
