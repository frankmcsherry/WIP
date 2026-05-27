//! Structural ops over the value universe — constructors and
//! eliminators for `Prod`, `Sum`, and `List`. Three families:
//!
//! - **Prod**: `zipN`, `detupleN`, `proj` (`.i`), `entupleN`.
//! - **Sum**: `injectN`, `split`, `mergeN`, `partitionN`, `branch`,
//!   `cleave` (`.{ path0 ; path1 }`).
//! - **List shape**: `nest`, `nest.stride`, `flatten`, `enlist`,
//!   `unlist`.
//!
//! `cleave` is body-bearing but dispatches per-path (bounded by the type
//! structure, not by N rows), so it's principle-2-aligned in spirit — see
//! `PRINCIPLES.md`. (`match` was retired: it desugars in the parser to
//! `split` / per-lane arms / `mergeN`, so no body-bearing op is built.)

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, PrimWidth, prod, sum, list};
use crate::ir::shape::{Shape, disc_as_u8};
use crate::ops::helpers::{gather, merge_by_disc};

#[derive(Debug)] pub struct ZipN { pub n: usize }
impl PrimOp for ZipN {
    fn name(&self) -> &str { "zip" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { zip_run(self.n, st) }
}
impl Typed for ZipN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { zip_tc(self.n, st) }
}
/// `zipN` kernel (back-end `SystemOp::Zip` calls this directly).
pub fn zip_run(n: usize, st: &mut Stack) -> Result<(), String> {
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
pub fn zip_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
        if st.len() < n { return Err(format!("zip{}: stack {}", n, st.len())); }
        let mut fs = Vec::with_capacity(n);
        for _ in 0..n { fs.push(st.pop().unwrap()); }
        fs.reverse();
        st.push(Shape::Prod(fs));
        Ok(())
}

/// `detuple.K` (or `detuple` for K=2) — opposite of `zipK`/`entupleK`. Pops a
/// `Prod[A, B, …, K-th]` and pushes the fields onto the stack, last field on
/// top. K is parse-time; the prod must have exactly K fields.
#[derive(Debug)] pub struct DetupleN { pub n: usize }
impl PrimOp for DetupleN {
    fn name(&self) -> &str { "detuple" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, self.n)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { detuple_run(self.n, st) }
}
impl Typed for DetupleN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { detuple_tc(self.n, st) }
}
/// `detupleN` kernel (back-end `SystemOp::Detuple` calls this directly).
pub fn detuple_run(n: usize, st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::Prod(fs) => {
                if fs.len() != n {
                    return Err(format!("detuple{}: prod has {} fields", n, fs.len()));
                }
                for f in fs.iter() { st.push(f.clone()); }
                Ok(())
            }
            other => Err(format!("detuple{}: not a prod: {:?}", n, other)),
        }
}
pub fn detuple_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "detuple")?;
        match v {
            Shape::Prod(fs) => {
                if fs.len() != n {
                    return Err(format!("detuple{}: prod has {} fields", n, fs.len()));
                }
                for f in fs { st.push(f); }
                Ok(())
            }
            other => Err(format!("detuple{}: not a prod: {}", n, other)),
        }
}

#[derive(Debug)] pub struct Proj { pub i: usize }
impl PrimOp for Proj {
    fn name(&self) -> &str { "." }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }  // Prod (or List<Prod>) → field
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { proj_run(self.i, st) }
}
impl Typed for Proj {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { proj_tc(self.i, st) }
}
/// `.i` projection kernel (back-end `SystemOp::Proj` calls this directly).
pub fn proj_run(i: usize, st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        match v {
            Value::Prod(fs) => {
                if i >= fs.len() {
                    return Err(format!(".{}: only {} fields", i, fs.len()));
                }
                st.push(fs[i].clone());
                Ok(())
            }
            // List<Prod[A, B, C]> projects to List<field_i>, preserving
            // the outer bounds. Lets cleave-on-grouped patterns work:
            // `grouped .{ count ; .0 reduce.+ ; .1 reduce.+ }` without
            // needing an `each` to descend through the list.
            Value::List { bounds, values } => match &*values {
                Value::Prod(fs) => {
                    if i >= fs.len() {
                        return Err(format!(".{}: List<Prod> has {} fields", i, fs.len()));
                    }
                    let inner = fs[i].clone();
                    st.push(Value::List { bounds, values: std::sync::Arc::new(inner) });
                    Ok(())
                }
                other => Err(format!(".{}: List inner is not a product: {:?}", i, other)),
            },
            other => Err(format!(".{}: not a product: {:?}", i, other)),
        }
}
pub fn proj_tc(i: usize, st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, ".i")?;
        match v {
            Shape::Prod(fs) => {
                if i >= fs.len() { return Err(format!(".{}: prod has {} fields", i, fs.len())); }
                st.push(fs.into_iter().nth(i).unwrap());
                Ok(())
            }
            Shape::List { bounds, inner } => match *inner {
                Shape::Prod(fs) => {
                    if i >= fs.len() {
                        return Err(format!(".{}: List<Prod> has {} fields", i, fs.len()));
                    }
                    let field = fs.into_iter().nth(i).unwrap();
                    st.push(Shape::List { bounds, inner: Box::new(field) });
                    Ok(())
                }
                other => Err(format!(".{}: List inner is not a product: {}", i, other)),
            },
            other => Err(format!(".{}: not a product: {}", i, other)),
        }
}

#[derive(Debug)] pub struct InjectN { pub n: usize }
impl PrimOp for InjectN {
    fn name(&self) -> &str { "inject" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n + 1, 1)) }  // disc + N lanes → Sum
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { inject_run(self.n, st) }
}
impl Typed for InjectN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { inject_tc(self.n, st) }
}
/// `injectN` kernel (back-end `SystemOp::Inject` calls this directly).
pub fn inject_run(n: usize, st: &mut Stack) -> Result<(), String> {
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n { lanes.push(pop(st)?); }
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
                                   n, k, lane.len(), want));
            }
        }
        st.push(sum(disc, lanes));
        Ok(())
}
pub fn inject_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
        if st.len() < n + 1 { return Err(format!("inject{}: stack {}", n, st.len())); }
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n { lanes.push(st.pop().unwrap()); }
        lanes.reverse();
        let disc = st.pop().unwrap();
        let disc_w = match disc {
            Shape::Prim(w) => w,
            other => return Err(format!("inject{}: disc must be Prim, got {}", n, other)),
        };
        st.push(Shape::Sum { disc: disc_w, lanes });
        Ok(())
}

#[derive(Debug)] pub struct Split;
impl PrimOp for Split {
    fn name(&self) -> &str { "split" }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { split_run(st) }
}
impl Typed for Split {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { split_tc(st) }
}
/// `split` kernel (back-end `SystemOp::Split` calls this directly).
pub fn split_run(st: &mut Stack) -> Result<(), String> {
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
pub fn split_tc(st: &mut TypeStack) -> Result<(), String> {
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

/// `mergeN` — reassemble N per-lane columns into one column, ordered by a
/// discriminant. The inverse of `split`/`partition`: given
/// `disc lane0 … lane_{N-1}` (disc deepest, as `split` produces), walk `disc`
/// and emit one element per position from the matching lane's cursor. Each
/// lane's length must equal the count of its disc value. Output shape is the
/// (shared) lane shape.
///
/// This is the merge half of `match` — `match { -> a0 -> a1 }` desugars to
/// `split :[disc l0 l1]  disc  l0 a0  l1 a1  merge2`. (Dotless + arity-suffixed
/// like `injectN`/`partitionN`; bare `merge` is reserved for sorted-set union.)
#[derive(Debug)] pub struct MergeN { pub n: usize }
impl PrimOp for MergeN {
    fn name(&self) -> &str { "merge" }
    fn arity(&self) -> Option<(usize, usize)> { Some((self.n + 1, 1)) }  // disc + N lanes → merged
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { merge_run(self.n, st) }
}
impl Typed for MergeN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { merge_tc(self.n, st) }
}
/// `mergeN` kernel (back-end `SystemOp::Merge` calls this directly).
pub fn merge_run(n: usize, st: &mut Stack) -> Result<(), String> {
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n { lanes.push(pop(st)?); }
        lanes.reverse();
        let disc_v = pop(st)?;
        let disc = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("merge{}: disc must be Prim, got {:?}", n, other)),
        };
        let disc_u8 = disc_as_u8(&disc)?;
        let merged = merge_by_disc(disc_u8, &lanes)?;
        st.push(merged);
        Ok(())
}
pub fn merge_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
        if st.len() < n + 1 { return Err(format!("merge{}: stack {}", n, st.len())); }
        let mut lanes = Vec::with_capacity(n);
        for _ in 0..n { lanes.push(st.pop().unwrap()); }
        lanes.reverse();
        let _disc = st.pop().unwrap();  // P8 discriminant (width-checked at run)
        // All lanes must agree in shape; the merged column has that shape.
        let first = lanes[0].clone();
        for (k, l) in lanes.iter().enumerate().skip(1) {
            if *l != first {
                return Err(format!("merge{}: lane {} shape {} != lane 0 shape {}", n, k, l, first));
            }
        }
        st.push(first);
        Ok(())
}

#[derive(Debug)] pub struct PartitionN { pub n: usize }
impl PrimOp for PartitionN {
    fn name(&self) -> &str { "partition" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, self.n)) }  // (col, disc) → N lanes
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { partition_run(self.n, st) }
}
impl Typed for PartitionN {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { partition_tc(self.n, st) }
}
/// `partitionN` kernel (back-end `SystemOp::Partition` calls this directly).
pub fn partition_run(n: usize, st: &mut Stack) -> Result<(), String> {
        let disc_v = pop(st)?;
        let disc = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("partition: disc must be Prim, got {:?}", other)),
        };
        let disc_u8 = disc_as_u8(&disc)?.to_vec();
        let col = pop(st)?;
        if col.len() != disc_u8.len() {
            return Err(format!("partition{}: col len {} != disc len {}", n, col.len(), disc_u8.len()));
        }
        let mut lanes: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, &d) in disc_u8.iter().enumerate() {
            let k = d as usize;
            if k >= n { return Err(format!("partition{}: disc {} out of range", n, k)); }
            lanes[k].push(i);
        }
        for idxs in lanes {
            st.push(gather(&col, &idxs)?);
        }
        Ok(())
}
pub fn partition_tc(n: usize, st: &mut TypeStack) -> Result<(), String> {
        let disc = tc_pop(st, "partition")?;
        match disc {
            Shape::Prim(_) => {}
            other => return Err(format!("partition{}: disc must be Prim, got {}", n, other)),
        }
        let col = tc_pop(st, "partition")?;
        for _ in 0..n { st.push(col.clone()); }
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { branch_run(self.k, st) }
}
impl Typed for Branch {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { branch_tc(self.k, st) }
}
/// `branch.K` kernel (back-end `SystemOp::Branch` calls this directly).
pub fn branch_run(k_arity: usize, st: &mut Stack) -> Result<(), String> {
        let disc_v = pop(st)?;
        let disc_prim = match disc_v {
            Value::Prim(p) => p,
            other => return Err(format!("branch.{}: disc must be Prim, got {:?}", k_arity, other)),
        };
        let disc_u8: Vec<u8> = disc_as_u8(&disc_prim)?.to_vec();
        let col = pop(st)?;
        if col.len() != disc_u8.len() {
            return Err(format!(
                "branch.{}: col len {} != disc len {}", k_arity, col.len(), disc_u8.len()
            ));
        }
        for &d in &disc_u8 {
            if (d as usize) >= k_arity {
                return Err(format!("branch.{}: disc value {} >= K", k_arity, d));
            }
        }

        // Partition by disc, build lanes.
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); k_arity];
        for (i, &d) in disc_u8.iter().enumerate() {
            buckets[d as usize].push(i);
        }
        let mut lanes: Vec<Value> = Vec::with_capacity(k_arity);
        for idxs in &buckets {
            lanes.push(gather(&col, idxs)?);
        }

        st.push(sum(crate::ir::value::prim_p8(disc_u8), lanes));
        Ok(())
}
pub fn branch_tc(k_arity: usize, st: &mut TypeStack) -> Result<(), String> {
        let disc = tc_pop(st, "branch")?;
        match disc {
            Shape::Prim(_) => {}
            other => return Err(format!("branch.{}: disc must be Prim, got {}", k_arity, other)),
        }
        let col = tc_pop(st, "branch")?;
        let mut lanes = Vec::with_capacity(k_arity);
        for _ in 0..k_arity { lanes.push(col.clone()); }
        st.push(Shape::Sum { disc: PrimWidth::W8, lanes });
        Ok(())
}

#[derive(Debug)] pub struct Nest;
impl PrimOp for Nest {
    fn name(&self) -> &str { "nest" }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }  // (values, bounds) → list
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { nest_run(st) }
}
/// `nest` kernel (back-end `SystemOp::Nest` calls this directly).
pub fn nest_run(st: &mut Stack) -> Result<(), String> {
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
impl Typed for Nest {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { nest_tc(st) }
}
pub fn nest_tc(st: &mut TypeStack) -> Result<(), String> {
        let bounds = tc_pop(st, "nest")?;
        let bw = match bounds {
            Shape::Prim(w) => w,
            other => return Err(format!("nest: bounds must be Prim, got {}", other)),
        };
        let v = tc_pop(st, "nest")?;
        st.push(Shape::List { bounds: bw, inner: Box::new(v) });
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { nest_stride_run(st) }
}
/// `nest.stride` kernel (back-end `SystemOp::NestStride` calls this directly).
pub fn nest_stride_run(st: &mut Stack) -> Result<(), String> {
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
impl Typed for NestStride {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { nest_stride_tc(st) }
}
pub fn nest_stride_tc(st: &mut TypeStack) -> Result<(), String> {
        let count = tc_pop(st, "nest.stride")?;
        if count != Shape::Prim(crate::ir::value::PrimWidth::W64) {
            return Err(format!("nest.stride: count must be Prim(P64), got {}", count));
        }
        let v = tc_pop(st, "nest.stride")?;
        st.push(Shape::List { bounds: crate::ir::value::PrimWidth::W64, inner: Box::new(v) });
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { enlist_run(st) }
}
impl Typed for Enlist {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { enlist_tc(st) }
}
/// `enlist` kernel (back-end `SystemOp::Enlist` calls this directly).
pub fn enlist_run(st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        let n = v.len() as u64;
        let bounds = crate::ir::value::bounds_var_from_ends(vec![n]);
        st.push(crate::ir::value::list(bounds, v));
        Ok(())
}
pub fn enlist_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "enlist")?;
        st.push(Shape::List {
            bounds: PrimWidth::W64,
            inner: Box::new(v),
        });
        Ok(())
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
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { unlist_run(st) }
}
impl Typed for Unlist {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { unlist_tc(st) }
}
/// `unlist` kernel (back-end `SystemOp::Unlist` calls this directly).
pub fn unlist_run(st: &mut Stack) -> Result<(), String> {
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
pub fn unlist_tc(st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "unlist")?;
        match v {
            Shape::List { inner, .. } => {
                st.push(*inner);
                Ok(())
            }
            other => Err(format!("unlist: expected List, got {}", other)),
        }
}

#[derive(Debug)] pub struct Flatten;
impl PrimOp for Flatten {
    fn name(&self) -> &str { "flatten" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 2)) }  // list → (values, bounds)
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { flatten_run(st) }
}
impl Typed for Flatten {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { flatten_tc(st) }
}
/// `flatten` kernel (back-end `SystemOp::Flatten` calls this directly).
pub fn flatten_run(st: &mut Stack) -> Result<(), String> {
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
pub fn flatten_tc(st: &mut TypeStack) -> Result<(), String> {
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
    fn merge_reproduces_match() {
        // The match desugaring `split / arms / mergeN` must equal `match`.
        // Sum disc:[0,1,0,1], lane0=[10,30], lane1=[20,40]; arms double / triple.
        use crate::syntax::registry::OpRegistry;
        use crate::syntax::parse::parse;
        let reg = OpRegistry::standard();
        let run = |src: &str| {
            let prog = parse(src, &reg).unwrap();
            let (g, _) = crate::pipeline::build(prog).unwrap();
            crate::pipeline::eval_graph(&g).unwrap().pop().unwrap()
        };
        let setup = "u8[0 1 0 1] u64[10 30] u64[20 40] inject2";
        let via_match  = run(&format!("{setup} match {{ -> 2u64 *.u64 -> 3u64 *.u64 }}"));
        let via_merge  = run(&format!("{setup} split :[disc l0 l1]  disc  l0 2u64 *.u64  l1 3u64 *.u64  merge2"));
        assert_eq!(via_match, via_merge);
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
        // `mergeN` (dotless, like inject/partition); bare `merge` left free
        // for the future sorted-set union.
        if let Some(rest) = t.strip_prefix("merge") {
            if let Ok(n) = rest.parse::<usize>() { return Some(Box::new(MergeN { n })); }
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
