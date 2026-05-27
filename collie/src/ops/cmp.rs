//! Binary comparison — `<`, `<=`, `=`, `!=`, `>=`, `>`. Result is
//! `Prim::P8` with 0/1 values (a mask column `where`/`filter` consume
//! directly). **Interp-free:** ordering compares unsigned words (so
//! signed/float order needs order-form/swizzled inputs), and `=`/`!=`
//! are bit-equality (float `=` is bitwise, not IEEE).
//!
//! Width-monomorphic kernels. The
//! mask-aware fast paths (View<Mask> vs scalar, View<Mask> vs full
//! Prim, View<Mask> × View<Mask>) take ~75% of the file and can be
//! skipped on a first read — the default `materialize-and-walk`
//! path is at the top.

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop_raw, materialize_top};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, Selector, Storage};
use crate::ir::shape::Shape;
use crate::ir::shape::prim_width;
use crate::ops::helpers::{extract_prim, list_elementwise2};

#[derive(Copy, Clone, Debug)]
pub enum CmpOp { Lt, Le, Eq, Ne, Ge, Gt }

#[derive(Debug)]
pub struct Cmp { pub op: CmpOp }

impl Cmp {
    pub fn op_name(&self) -> &'static str { op_name(self.op) }
}

/// Operator symbol — a free fn so the back-end `SystemOp::Cmp` can name the
/// op without reconstructing a `Cmp` struct.
pub fn op_name(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Lt => "<", CmpOp::Le => "<=",
        CmpOp::Eq => "=", CmpOp::Ne => "!=",
        CmpOp::Ge => ">=", CmpOp::Gt => ">",
    }
}

impl PrimOp for Cmp {
    fn name(&self) -> &str { op_name(self.op) }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { run(self.op, st) }
}

impl Typed for Cmp {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { tc(st) }
}

/// The comparison kernel. The back-end `SystemOp::Cmp` calls this directly;
/// the `Cmp` struct's `run` is a thin shim. Pops two operands; pushes a `P8`
/// mask (or a `List<P8>` for the segmented equal-bounds-List path).
pub fn run(op: CmpOp, st: &mut Stack) -> Result<(), String> {
        let b = pop_raw(st)?;
        let a = pop_raw(st)?;

        // Segmented (List-preserving) path: equal-bounds Lists, or a List
        // against a length-1 scalar. Comparing never crosses a row boundary,
        // so we compute on the flat inner values and reattach the same bounds.
        if let Some(res) = list_elementwise2(&a, &b, |va, vb| {
            let pa = extract_prim(va, "cmp")?;
            let pb = extract_prim(vb, "cmp")?;
            Ok(Value::Prim(do_cmp(&pa, &pb, op)?))
        }) {
            st.push(res?);
            return Ok(());
        }

        // Mask-aware fast paths — chained-filter inner loop. `a` is
        // `View<Prim, Mask>`; depending on `b` we have three streaming
        // shapes that avoid materializing the masked view:
        //
        //   1. b is a length-1 broadcast scalar (Prim). Stream a's
        //      source through mask vs the scalar.
        //   2. b is a full-length flat Prim whose length equals
        //      popcount(mask) — element-wise compare, advance through
        //      `a`'s mask while reading `b` sequentially.
        //   3. b is another `View<Prim, Mask>` with the same logical
        //      length — two-cursor walk, advancing both masks in
        //      parallel to the k-th set bit.
        //
        // The chained-filter benchmark exercises shapes (2) and (3)
        // when cmp/arith pipelines feed each other.
        if let Value::View { source: src_a, selector: Selector::Mask(mask_a) } = &a {
            if let Value::Prim(_) = src_a.as_ref() {
                if let Value::Prim(pb) = &b {
                    if pb.len() == 1 {
                        let out = do_cmp_masked_scalar(src_a.as_ref(), mask_a, pb, op)?;
                        st.push(Value::Prim(out));
                        return Ok(());
                    }
                    if pb.len() == a.len() {
                        let out = do_cmp_masked_full(src_a.as_ref(), mask_a, pb, op)?;
                        st.push(Value::Prim(out));
                        return Ok(());
                    }
                }
                if let Value::View { source: src_b, selector: Selector::Mask(mask_b) } = &b {
                    if let Value::Prim(_) = src_b.as_ref() {
                        if a.len() == b.len() {
                            let out = do_cmp_both_masked(
                                src_a.as_ref(), mask_a, src_b.as_ref(), mask_b,
                                op)?;
                            st.push(Value::Prim(out));
                            return Ok(());
                        }
                    }
                }
            }
        }

        // Indices read-through: both sides are gather-views
        // (`View<Prim, Indices>`) of equal length. Compare
        // `source_a[idx_a[k]]` vs `source_b[idx_b[k]]` directly, skipping
        // the two materialized gather columns. (The random reads happen
        // either way; this just fuses the gathers into the compare.)
        if let Value::View { source: src_a, selector: Selector::Indices(idx_a) } = &a {
            if let Value::View { source: src_b, selector: Selector::Indices(idx_b) } = &b {
                if matches!(src_a.as_ref(), Value::Prim(_))
                    && matches!(src_b.as_ref(), Value::Prim(_))
                    && idx_a.len() == idx_b.len()
                {
                    let out = do_cmp_both_indices(src_a.as_ref(), idx_a, src_b.as_ref(), idx_b, op)?;
                    st.push(Value::Prim(out));
                    return Ok(());
                }
            }
        }

        // Fall through: materialize the view(s) and use the regular
        // typed cmp path.
        let a_mat = materialize_top(a)?;
        let b_mat = materialize_top(b)?;
        let pa = extract_prim(&a_mat, "cmp")?;
        let pb = extract_prim(&b_mat, "cmp")?;
        let out = do_cmp(&pa, &pb, op)?;
        st.push(Value::Prim(out));
        Ok(())
}

/// The comparison typecheck (op-independent: width-only). Back-end
/// `SystemOp::Cmp` calls this directly.
pub fn tc(st: &mut TypeStack) -> Result<(), String> {
        // Interp-free: ordering is by unsigned word, equality bitwise; the
        // two sides need only agree in width. (Signed/float ordering via
        // order-form inputs; `=`/`!=` are bit-equality.) Result is a P8 mask.
        let b = tc_pop(st, "cmp")?;
        let a = tc_pop(st, "cmp")?;
        match (&a, &b) {
            (Shape::Prim(wa), Shape::Prim(wb)) if wa == wb => {
                st.push(Shape::Prim(PrimWidth::W8));
                Ok(())
            }
            // Segmented: List<Prim> on both sides (equal bounds width, equal
            // inner width) → List<P8>; bounds propagate.
            (Shape::List { bounds: lba, inner: ia }, Shape::List { bounds: lbb, inner: ib })
                if lba == lbb && prim_width(ia).is_some() && prim_width(ia) == prim_width(ib) => {
                st.push(Shape::List { bounds: *lba, inner: Box::new(Shape::Prim(PrimWidth::W8)) });
                Ok(())
            }
            // List<Prim> against a length-1 scalar (broadcast) → List<P8>.
            (Shape::List { bounds, inner }, Shape::Prim(wb)) if prim_width(inner) == Some(*wb) => {
                st.push(Shape::List { bounds: *bounds, inner: Box::new(Shape::Prim(PrimWidth::W8)) });
                Ok(())
            }
            (Shape::Prim(wa), Shape::List { bounds, inner }) if prim_width(inner) == Some(*wa) => {
                st.push(Shape::List { bounds: *bounds, inner: Box::new(Shape::Prim(PrimWidth::W8)) });
                Ok(())
            }
            _ => Err(format!("cmp: needs same-width Prim or equal-bounds List<Prim> on both sides, got {} and {}", a, b)),
        }
}

fn cmp_apply<T, F>(a: &[T], b: &[T], f: F) -> Result<Vec<u8>, String>
where T: Copy, F: Fn(T, T) -> bool
{
    let na = a.len(); let nb = b.len();
    Ok(if na == nb {
        a.iter().zip(b.iter()).map(|(&x, &y)| f(x, y) as u8).collect()
    } else if na == 1 {
        let s = a[0]; b.iter().map(|&y| f(s, y) as u8).collect()
    } else if nb == 1 {
        let s = b[0]; a.iter().map(|&x| f(x, s) as u8).collect()
    } else {
        return Err(format!("cmp: length mismatch {} vs {}", na, nb));
    })
}

fn cmp_from_uninterp<T, F>(a: &Prim, b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> bool
{
    let aa = T::extract(a)?;
    let bb = T::extract(b)?;
    let out = cmp_apply(aa, bb, f)?;
    Ok(Prim::P8(Arc::new(out)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::from_vec;

    fn run_cmp(op: CmpOp, a: Value, b: Value) -> Value {
        let mut st = vec![a, b];
        let mut env = Vec::new();
        Cmp { op }.run(&mut st, &mut env).unwrap();
        st.pop().unwrap()
    }

    #[test]
    fn lt_u64_produces_p8_zero_one() {
        let r = run_cmp(CmpOp::Lt,
            from_vec::<u64>(vec![1, 5, 3]),
            from_vec::<u64>(vec![2, 5, 1]));
        assert_eq!(r, from_vec::<u8>(vec![1, 0, 0]));
    }

    #[test]
    fn ge_with_broadcast_scalar() {
        let r = run_cmp(CmpOp::Ge,
            from_vec::<u64>(vec![1, 5, 3, 9, 2, 7, 4]),
            from_vec::<u64>(vec![4]));
        assert_eq!(r, from_vec::<u8>(vec![0, 1, 0, 1, 0, 1, 1]));
    }

    #[test]
    fn masked_view_vs_full_length_prim() {
        // a = View<Prim<u64>[1,9,3,7,5], Mask([1,0,1,0,1])> → logical view [1, 3, 5].
        // b = Prim<u64>[2, 3, 4]. Compare a < b → [1, 0, 0] (1<2, 3<3=false, 5<4=false).
        use crate::ir::value::{view, Selector};
        use std::sync::Arc;
        let src = from_vec::<u64>(vec![1, 9, 3, 7, 5]);
        let mask = Selector::Mask(Arc::new(vec![1, 0, 1, 0, 1]));
        let a = view(src, mask);
        let b = from_vec::<u64>(vec![2, 3, 4]);
        let r = run_cmp(CmpOp::Lt, a, b);
        assert_eq!(r, from_vec::<u8>(vec![1, 0, 0]));
    }

    #[test]
    fn both_masked_views_same_length() {
        // a = View<[1,9,3,7,5], Mask([1,0,1,0,1])> → [1, 3, 5].
        // b = View<[2,8,4,6,0], Mask([0,1,0,1,0])> → [8, 6]. Wait — different popcounts!
        // Use matching popcounts:
        // a = View<[1,9,3,7,5], Mask([1,0,1,0,1])> → [1, 3, 5] (popcount 3).
        // b = View<[10,20,30,40,50], Mask([1,1,1,0,0])> → [10, 20, 30] (popcount 3).
        // Compare a < b → [1, 1, 1].
        use crate::ir::value::{view, Selector};
        use std::sync::Arc;
        let src_a = from_vec::<u64>(vec![1, 9, 3, 7, 5]);
        let mask_a = Selector::Mask(Arc::new(vec![1, 0, 1, 0, 1]));
        let a = view(src_a, mask_a);
        let src_b = from_vec::<u64>(vec![10, 20, 30, 40, 50]);
        let mask_b = Selector::Mask(Arc::new(vec![1, 1, 1, 0, 0]));
        let b = view(src_b, mask_b);
        let r = run_cmp(CmpOp::Lt, a, b);
        assert_eq!(r, from_vec::<u8>(vec![1, 1, 1]));
    }

    #[test]
    fn both_indices_views_read_through() {
        // a = View<[10,20,30,40], Indices([3,1,0])> → [40, 20, 10].
        // b = View<[ 5,25,15,35], Indices([0,2,1])> → [ 5, 15, 25].
        // a < b → [0, 0, 1]. Must match gather+cmp (the materialize path).
        use crate::ir::value::{view, Selector};
        use std::sync::Arc;
        let src_a = from_vec::<u64>(vec![10, 20, 30, 40]);
        let a = view(src_a, Selector::Indices(Arc::new(vec![3, 1, 0])));
        let src_b = from_vec::<u64>(vec![5, 25, 15, 35]);
        let b = view(src_b, Selector::Indices(Arc::new(vec![0, 2, 1])));
        let r = run_cmp(CmpOp::Lt, a, b);
        assert_eq!(r, from_vec::<u8>(vec![0, 0, 1]));
    }

    #[test]
    fn segmented_lt_preserves_list_bounds() {
        // Two Lists with equal bounds [0,2,5]: rows [1,2] | [3,4,5] vs
        // [9,1] | [9,9,9]. Per-row `<` → [1,0] | [1,1,1], same bounds.
        use crate::ir::value::{bounds_var_from_ends, list};
        let a = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![1, 2, 3, 4, 5]));
        let b = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![9, 1, 9, 9, 9]));
        let r = run_cmp(CmpOp::Lt, a, b);
        let expected = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u8>(vec![1, 0, 1, 1, 1]));
        assert_eq!(r, expected);
    }

    #[test]
    fn segmented_cmp_against_scalar_broadcasts() {
        // List<u64> rows [1,5] | [3] compared `>=` 3u64 → [0,1] | [1].
        use crate::ir::value::{bounds_var_from_ends, list};
        let a = list(bounds_var_from_ends(vec![2, 3]), from_vec::<u64>(vec![1, 5, 3]));
        let r = run_cmp(CmpOp::Ge, a, from_vec::<u64>(vec![3]));
        let expected = list(bounds_var_from_ends(vec![2, 3]), from_vec::<u8>(vec![0, 1, 1]));
        assert_eq!(r, expected);
    }

    #[test]
    fn segmented_cmp_mismatched_bounds_errors() {
        use crate::ir::value::{bounds_var_from_ends, list};
        let a = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![1, 2, 3, 4, 5]));
        let b = list(bounds_var_from_ends(vec![3, 5]), from_vec::<u64>(vec![1, 2, 3, 4, 5]));
        let mut st = vec![a, b];
        let mut env = Vec::new();
        assert!(Cmp { op: CmpOp::Lt }.run(&mut st, &mut env).is_err());
    }

    #[test]
    fn eq_is_bitwise() {
        // `=` is now bit-equality (interp-free), not IEEE: identical bit
        // patterns compare equal, so NaN bits == NaN bits → 1. (IEEE
        // NaN!=NaN would need an interp-aware op or a NaN filter.)
        let nan = f64::NAN;
        let r = run_cmp(CmpOp::Eq,
            from_vec::<f64>(vec![nan, 1.0]),
            from_vec::<f64>(vec![nan, 1.0]));
        assert_eq!(r, from_vec::<u8>(vec![1, 1]));
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    // Interp-free: bare comparison tokens (ordering by unsigned word,
    // equality bitwise; signed/float order via swizzled inputs).
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let op = match t {
            "<"  => CmpOp::Lt,
            "<=" => CmpOp::Le,
            "="  => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            ">=" => CmpOp::Ge,
            ">"  => CmpOp::Gt,
            _ => return None,
        };
        Some(Box::new(Cmp { op }))
    });
}

/// Mask-aware cmp: source is a Prim wrapped in a View<Mask>; b is a
/// length-1 broadcast scalar. Walks source via mask, applies cmp,
/// produces a P8 mask of length popcount.
fn do_cmp_masked_scalar(source: &Value, mask: &[u8], b: &Prim, op: CmpOp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        CmpOp::Lt => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x <  y),
        CmpOp::Le => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x <= y),
        CmpOp::Eq => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x == y),
        CmpOp::Ne => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x != y),
        CmpOp::Ge => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x >= y),
        CmpOp::Gt => cmp_masked_scalar::<$t, _>(source, mask, b, |x, y| x >  y),
    } }; }
    match b.width() {
        PrimWidth::W8  => by_op!(u8),
        PrimWidth::W16 => by_op!(u16),
        PrimWidth::W32 => by_op!(u32),
        PrimWidth::W64 => by_op!(u64),
    }
}

fn cmp_masked_scalar<T, F>(source: &Value, mask: &[u8], b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> bool
{
    let src_prim = match source {
        Value::Prim(p) => p,
        other => return Err(format!("cmp masked: expected Prim source, got {:?}", other)),
    };
    let xs = T::extract(src_prim)?;
    let bs = T::extract(b)?;
    let scalar = bs[0];
    let popcnt = mask.iter().filter(|&&b| b != 0).count();
    let mut out: Vec<u8> = Vec::with_capacity(popcnt);
    for i in 0..xs.len() {
        if mask[i] != 0 {
            out.push(if f(xs[i], scalar) { 1 } else { 0 });
        }
    }
    Ok(Prim::P8(Arc::new(out)))
}

/// `View<Mask>` vs full-length flat Prim. Output length = popcount(mask) =
/// length of b. Walks source positions, emitting one output per set bit,
/// pairing with the running cursor into b.
fn do_cmp_masked_full(source: &Value, mask: &[u8], b: &Prim, op: CmpOp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        CmpOp::Lt => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x <  y),
        CmpOp::Le => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x <= y),
        CmpOp::Eq => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x == y),
        CmpOp::Ne => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x != y),
        CmpOp::Ge => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x >= y),
        CmpOp::Gt => cmp_masked_full::<$t, _>(source, mask, b, |x, y| x >  y),
    } }; }
    match b.width() {
        PrimWidth::W8  => by_op!(u8),
        PrimWidth::W16 => by_op!(u16),
        PrimWidth::W32 => by_op!(u32),
        PrimWidth::W64 => by_op!(u64),
    }
}

fn cmp_masked_full<T, F>(source: &Value, mask: &[u8], b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> bool
{
    let src_prim = match source {
        Value::Prim(p) => p,
        other => return Err(format!("cmp masked-full: expected Prim source, got {:?}", other)),
    };
    let xs = T::extract(src_prim)?;
    let bs = T::extract(b)?;
    let popcnt = mask.iter().filter(|&&b| b != 0).count();
    if bs.len() != popcnt {
        return Err(format!("cmp masked-full: b length {} != popcount {}", bs.len(), popcnt));
    }
    let mut out: Vec<u8> = Vec::with_capacity(popcnt);
    let mut k = 0usize;
    for i in 0..xs.len() {
        if mask[i] != 0 {
            out.push(if f(xs[i], bs[k]) { 1 } else { 0 });
            k += 1;
        }
    }
    Ok(Prim::P8(Arc::new(out)))
}

/// Both inputs are `View<Prim, Mask>` with equal logical length. Walk both
/// masks in parallel, advancing to the next set bit in each. Reads source
/// positions a-side and b-side directly — no materialization.
fn do_cmp_both_masked(
    src_a: &Value, mask_a: &[u8], src_b: &Value, mask_b: &[u8],
    op: CmpOp,
) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        CmpOp::Lt => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x <  y),
        CmpOp::Le => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x <= y),
        CmpOp::Eq => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x == y),
        CmpOp::Ne => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x != y),
        CmpOp::Ge => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x >= y),
        CmpOp::Gt => cmp_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x >  y),
    } }; }
    let w = match src_a { Value::Prim(p) => p.width(), o => return Err(format!("cmp: a source must be Prim, got {:?}", o)) };
    match w {
        PrimWidth::W8  => by_op!(u8),
        PrimWidth::W16 => by_op!(u16),
        PrimWidth::W32 => by_op!(u32),
        PrimWidth::W64 => by_op!(u64),
    }
}

fn cmp_both_masked<T, F>(
    src_a: &Value, mask_a: &[u8], src_b: &Value, mask_b: &[u8], f: F,
) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> bool
{
    let pa = match src_a {
        Value::Prim(p) => p,
        other => return Err(format!("cmp both-masked: a source must be Prim, got {:?}", other)),
    };
    let pb = match src_b {
        Value::Prim(p) => p,
        other => return Err(format!("cmp both-masked: b source must be Prim, got {:?}", other)),
    };
    let xs = T::extract(pa)?;
    let ys = T::extract(pb)?;
    let popcount = mask_a.iter().filter(|&&b| b != 0).count();
    let mut out = Vec::with_capacity(popcount);
    let mut ia = 0usize;
    let mut ib = 0usize;
    loop {
        while ia < mask_a.len() && mask_a[ia] == 0 { ia += 1; }
        while ib < mask_b.len() && mask_b[ib] == 0 { ib += 1; }
        if ia >= mask_a.len() || ib >= mask_b.len() { break; }
        out.push(if f(xs[ia], ys[ib]) { 1 } else { 0 });
        ia += 1; ib += 1;
    }
    Ok(Prim::P8(Arc::new(out)))
}

/// Interp-free comparison: ordering by unsigned word, equality bitwise
/// (width-dispatched). Signed/float ordering comes from order-form
/// (swizzled) inputs; `=`/`!=` are bit-equality (so float `=` is bitwise,
/// not IEEE).
pub fn do_cmp(a: &Prim, b: &Prim, op: CmpOp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        CmpOp::Lt => cmp_from_uninterp::<$t, _>(a, b, |x, y| x <  y),
        CmpOp::Le => cmp_from_uninterp::<$t, _>(a, b, |x, y| x <= y),
        CmpOp::Eq => cmp_from_uninterp::<$t, _>(a, b, |x, y| x == y),
        CmpOp::Ne => cmp_from_uninterp::<$t, _>(a, b, |x, y| x != y),
        CmpOp::Ge => cmp_from_uninterp::<$t, _>(a, b, |x, y| x >= y),
        CmpOp::Gt => cmp_from_uninterp::<$t, _>(a, b, |x, y| x >  y),
    } }; }
    match a.width() {
        PrimWidth::W8  => by_op!(u8),
        PrimWidth::W16 => by_op!(u16),
        PrimWidth::W32 => by_op!(u32),
        PrimWidth::W64 => by_op!(u64),
    }
}

/// Both sides are `View<Prim, Indices>` of equal length — compare
/// `src_a[idx_a[k]]` vs `src_b[idx_b[k]]` without materializing the gathers.
fn do_cmp_both_indices(src_a: &Value, idx_a: &[u64], src_b: &Value, idx_b: &[u64], op: CmpOp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        CmpOp::Lt => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x <  y),
        CmpOp::Le => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x <= y),
        CmpOp::Eq => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x == y),
        CmpOp::Ne => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x != y),
        CmpOp::Ge => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x >= y),
        CmpOp::Gt => cmp_both_indices::<$t, _>(src_a, idx_a, src_b, idx_b, |x, y| x >  y),
    } }; }
    let w = match src_a { Value::Prim(p) => p.width(), o => return Err(format!("cmp: a source must be Prim, got {:?}", o)) };
    match w {
        PrimWidth::W8  => by_op!(u8),
        PrimWidth::W16 => by_op!(u16),
        PrimWidth::W32 => by_op!(u32),
        PrimWidth::W64 => by_op!(u64),
    }
}

fn cmp_both_indices<T, F>(src_a: &Value, idx_a: &[u64], src_b: &Value, idx_b: &[u64], f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> bool
{
    let pa = match src_a { Value::Prim(p) => p, o => return Err(format!("cmp indices: a source must be Prim, got {:?}", o)) };
    let pb = match src_b { Value::Prim(p) => p, o => return Err(format!("cmp indices: b source must be Prim, got {:?}", o)) };
    let xs = T::extract(pa)?;
    let ys = T::extract(pb)?;
    let mut out = Vec::with_capacity(idx_a.len());
    for k in 0..idx_a.len() {
        out.push(if f(xs[idx_a[k] as usize], ys[idx_b[k] as usize]) { 1 } else { 0 });
    }
    Ok(Prim::P8(Arc::new(out)))
}
