//! Binary and unary arithmetic — `+.i32`, `-.f64`, `neg.i32`, `abs.f64`,
//! etc. One op token per `(operation, interpretation)`; the kernel is
//! width-monomorphic Rust the compiler autovectorizes.
//!
//! Scalar broadcast (`seq 5 +.u64`) is handled inline. View-with-Mask
//! and View-with-Mask × scalar have fast paths that stream the source
//! through the mask without materializing — these are the longest
//! match arms in the file and can be skipped on a first read.

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop, pop_raw, materialize_top};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, Selector, Storage};
use crate::ir::shape::{Interp, Shape, prim_width};
use crate::ops::helpers::{extract_prim, list_elementwise1, list_elementwise2};

#[derive(Copy, Clone, Debug)]
pub enum ArithOp { Add, Sub, Mul, Div, Mod }

#[derive(Debug)]
pub struct Arith { pub op: ArithOp, pub interp: Interp }

impl Arith {
    pub fn op_name(&self) -> &'static str { op_name(self.op) }
}

/// Operator symbol — free fn so `SystemOp::Arith` names the op without a struct.
pub fn op_name(op: ArithOp) -> &'static str {
    match op {
        ArithOp::Add => "+", ArithOp::Sub => "-",
        ArithOp::Mul => "*", ArithOp::Div => "/",
        ArithOp::Mod => "%",
    }
}

impl PrimOp for Arith {
    fn name(&self) -> &str { op_name(self.op) }
    fn arity(&self) -> Option<(usize, usize)> { Some((2, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { run(self.op, self.interp, st) }
}

impl Typed for Arith {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { tc(self.interp, st) }
}

/// The binary-arith kernel (back-end `SystemOp::Arith` calls this directly;
/// the struct's `run` is a thin shim).
pub fn run(op: ArithOp, interp: Interp, st: &mut Stack) -> Result<(), String> {
        let b = pop_raw(st)?;
        let a = pop_raw(st)?;

        // Segmented (List-preserving) path: equal-bounds Lists, or a List
        // against a length-1 scalar. Arithmetic never crosses a row boundary,
        // so compute on the flat inner values and reattach the same bounds.
        if let Some(res) = list_elementwise2(&a, &b, |va, vb| {
            let pa = extract_prim(va, "arith")?;
            let pb = extract_prim(vb, "arith")?;
            Ok(Value::Prim(do_arith(&pa, &pb, op, interp)?))
        }) {
            st.push(res?);
            return Ok(());
        }

        // Mask-aware fast paths, parallel to cmp.rs:
        //   1. a=View<Mask>, b=scalar Prim         (broadcast)
        //   2. a=View<Mask>, b=full-length Prim   (paired walk through mask)
        //   3. a=View<Mask>, b=View<Mask>         (two-cursor walk)
        // All emit a fresh Prim of length popcount(mask_a); no intermediate
        // materialization.
        if let Value::View { source: src_a, selector: Selector::Mask(mask_a) } = &a {
            if let Value::Prim(_) = src_a.as_ref() {
                if let Value::Prim(pb) = &b {
                    if pb.len() == 1 {
                        let out = do_arith_masked_scalar(src_a.as_ref(), mask_a, pb, op, interp)?;
                        st.push(Value::Prim(out));
                        return Ok(());
                    }
                    if pb.len() == a.len() {
                        let out = do_arith_masked_full(src_a.as_ref(), mask_a, pb, op, interp)?;
                        st.push(Value::Prim(out));
                        return Ok(());
                    }
                }
                if let Value::View { source: src_b, selector: Selector::Mask(mask_b) } = &b {
                    if let Value::Prim(_) = src_b.as_ref() {
                        if a.len() == b.len() {
                            let out = do_arith_both_masked(
                                src_a.as_ref(), mask_a, src_b.as_ref(), mask_b,
                                op, interp)?;
                            st.push(Value::Prim(out));
                            return Ok(());
                        }
                    }
                }
            }
        }

        // Fall through: materialize, owned-extract, regular path (with
        // Arc-1 reuse from #49).
        let a_mat = materialize_top(a)?;
        let b_mat = materialize_top(b)?;
        let pa = match a_mat { Value::Prim(p) => p, other => return Err(format!("arith: expected Prim, got {:?}", other)) };
        let pb = match b_mat { Value::Prim(p) => p, other => return Err(format!("arith: expected Prim, got {:?}", other)) };
        let out = do_arith_owned(pa, pb, op, interp)?;
        st.push(Value::Prim(out));
        Ok(())
}

/// The binary-arith typecheck (back-end `SystemOp::Arith` calls this directly).
pub fn tc(interp: Interp, st: &mut TypeStack) -> Result<(), String> {
        let b = tc_pop(st, "arith")?;
        let a = tc_pop(st, "arith")?;
        let w = interp.width();
        match (&a, &b) {
            (Shape::Prim(wa), Shape::Prim(wb)) if *wa == w && *wb == w => {
                st.push(Shape::Prim(w));
                Ok(())
            }
            // Segmented: List<Prim(w)> on both sides (equal bounds) → List<Prim(w)>.
            (Shape::List { bounds: lba, inner: ia }, Shape::List { bounds: lbb, inner: ib })
                if lba == lbb && prim_width(ia) == Some(w) && prim_width(ib) == Some(w) => {
                st.push(Shape::List { bounds: *lba, inner: Box::new(Shape::Prim(w)) });
                Ok(())
            }
            // List<Prim(w)> against a length-1 scalar (broadcast) → List<Prim(w)>.
            (Shape::List { bounds, inner }, Shape::Prim(wb)) if prim_width(inner) == Some(w) && *wb == w => {
                st.push(Shape::List { bounds: *bounds, inner: Box::new(Shape::Prim(w)) });
                Ok(())
            }
            (Shape::Prim(wa), Shape::List { bounds, inner }) if *wa == w && prim_width(inner) == Some(w) => {
                st.push(Shape::List { bounds: *bounds, inner: Box::new(Shape::Prim(w)) });
                Ok(())
            }
            _ => Err(format!("arith.{}: needs Prim({}) or equal-bounds List<Prim({})> on both sides, got {} and {}", interp, w, w, a, b)),
        }
}

/// Generic kernel: vec/vec, scalar/vec, vec/scalar (no broadcast materialization).
fn arith_apply<T, F>(a: &[T], b: &[T], f: F) -> Result<Vec<T>, String>
where T: Copy, F: Fn(T, T) -> T
{
    let na = a.len(); let nb = b.len();
    Ok(if na == nb {
        a.iter().zip(b.iter()).map(|(&x, &y)| f(x, y)).collect()
    } else if na == 1 {
        let s = a[0]; b.iter().map(|&y| f(s, y)).collect()
    } else if nb == 1 {
        let s = b[0]; a.iter().map(|&x| f(x, s)).collect()
    } else {
        return Err(format!("arith: length mismatch {} vs {}", na, nb));
    })
}

fn arith_from_uninterp<T, F>(a: &Prim, b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> T
{
    let aa = T::extract(a)?;
    let bb = T::extract(b)?;
    let out = arith_apply(aa, bb, f)?;
    Ok(T::wrap(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::from_vec;

    fn run_arith(op: ArithOp, interp: Interp, a: Value, b: Value) -> Value {
        let mut st = vec![a, b];
        let mut env = Vec::new();
        Arith { op, interp }.run(&mut st, &mut env).unwrap();
        st.pop().unwrap()
    }

    #[test]
    fn add_u64_vec_vec() {
        let r = run_arith(ArithOp::Add, Interp::U64,
            from_vec::<u64>(vec![1, 2, 3]),
            from_vec::<u64>(vec![10, 20, 30]));
        assert_eq!(r, from_vec::<u64>(vec![11, 22, 33]));
    }

    #[test]
    fn add_i32_scalar_broadcast() {
        let r = run_arith(ArithOp::Add, Interp::I32,
            from_vec::<i32>(vec![1, 2, 3]),
            from_vec::<i32>(vec![10]));
        assert_eq!(r, from_vec::<i32>(vec![11, 12, 13]));
    }

    #[test]
    fn mul_f64() {
        let r = run_arith(ArithOp::Mul, Interp::F64,
            from_vec::<f64>(vec![1.5, 2.0]),
            from_vec::<f64>(vec![2.0, 3.0]));
        assert_eq!(r, from_vec::<f64>(vec![3.0, 6.0]));
    }

    #[test]
    fn div_i64_rounds_toward_zero() {
        let r = run_arith(ArithOp::Div, Interp::I64,
            from_vec::<i64>(vec![10, -10, 7]),
            from_vec::<i64>(vec![3, 3, 2]));
        assert_eq!(r, from_vec::<i64>(vec![3, -3, 3]));
    }

    #[test]
    fn segmented_add_preserves_list_bounds() {
        // Rows [1,2] | [3,4,5] + [10,20] | [30,40,50] → [11,22] | [33,44,55].
        use crate::ir::value::{bounds_var_from_ends, list};
        let a = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![1, 2, 3, 4, 5]));
        let b = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![10, 20, 30, 40, 50]));
        let r = run_arith(ArithOp::Add, Interp::U64, a, b);
        let expected = list(bounds_var_from_ends(vec![2, 5]), from_vec::<u64>(vec![11, 22, 33, 44, 55]));
        assert_eq!(r, expected);
    }

    #[test]
    fn segmented_mul_by_scalar_broadcasts() {
        // List rows [1,2] | [3] * 10u64 → [10,20] | [30], bounds preserved.
        use crate::ir::value::{bounds_var_from_ends, list};
        let a = list(bounds_var_from_ends(vec![2, 3]), from_vec::<u64>(vec![1, 2, 3]));
        let r = run_arith(ArithOp::Mul, Interp::U64, a, from_vec::<u64>(vec![10]));
        let expected = list(bounds_var_from_ends(vec![2, 3]), from_vec::<u64>(vec![10, 20, 30]));
        assert_eq!(r, expected);
    }

    #[test]
    fn width_mismatch_errors() {
        let mut st = vec![from_vec::<u32>(vec![1]), from_vec::<u64>(vec![1])];
        let mut env = Vec::new();
        let err = Arith { op: ArithOp::Add, interp: Interp::U64 }.run(&mut st, &mut env);
        assert!(err.is_err(), "should error on width mismatch");
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        let op = match head {
            "+"   => ArithOp::Add,
            "-"   => ArithOp::Sub,
            "*"   => ArithOp::Mul,
            "/"   => ArithOp::Div,
            "%" | "mod" => ArithOp::Mod,
            _ => return None,
        };
        Some(Box::new(Arith { op, interp }))
    });
    // Unary arith: neg.<i>, abs.<i>
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        let op = match head {
            "neg" => UnaryArithOp::Neg,
            "abs" => UnaryArithOp::Abs,
            _ => return None,
        };
        Some(Box::new(UnaryArith { op, interp }))
    });
}

/// Owning entry point: tries to reuse `a`'s buffer when its Arc is unique
/// (refcount = 1). Falls back to `do_arith` (fresh allocation) when shared.
///
/// This is the hot path for inline arith like `qty 100u64 *.u64 pr +.u64`
/// where intermediate columns have no other holders. Saves the 4MB
/// allocation of the result column on a 1M-element u64 add.
pub fn do_arith_owned(a: Prim, b: Prim, op: ArithOp, interp: Interp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        ArithOp::Add => arith_owned_at::<$t, _>(a, b, |x, y| x + y),
        ArithOp::Sub => arith_owned_at::<$t, _>(a, b, |x, y| x - y),
        ArithOp::Mul => arith_owned_at::<$t, _>(a, b, |x, y| x * y),
        ArithOp::Div => arith_owned_at::<$t, _>(a, b, |x, y| x / y),
        ArithOp::Mod => arith_owned_at::<$t, _>(a, b, |x, y| x % y),
    } }; }
    match interp {
        Interp::I8  => by_op!(i8),
        Interp::U8  => by_op!(u8),
        Interp::I16 => by_op!(i16),
        Interp::U16 => by_op!(u16),
        Interp::I32 => by_op!(i32),
        Interp::U32 => by_op!(u32),
        Interp::F32 => by_op!(f32),
        Interp::I64 => by_op!(i64),
        Interp::U64 => by_op!(u64),
        Interp::F64 => by_op!(f64),
    }
}

/// Per-type kernel: take owned (a, b) Prims, try Arc-1 reuse of a's buffer.
fn arith_owned_at<T, F>(a: Prim, b: Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> T
{
    let arc_a = T::extract_arc(a)?;
    let arc_b = T::extract_arc(b)?;
    let na = arc_a.len();
    let nb = arc_b.len();
    // Same-length case: try to reuse either a's or b's buffer.
    // Common pattern in chains like `x intermediate +.u64`: a is shared
    // (e.g., env-bound x), b is unique (fresh intermediate). Trying both
    // matters — only checking a misses half the wins.
    if na == nb {
        match std::sync::Arc::try_unwrap(arc_a) {
            Ok(vec_back_a) => {
                // a unique — mutate a's buffer.
                let bs: &[T] = bytemuck::cast_slice(&*arc_b);
                let mut v_t: Vec<T> = bytemuck::cast_vec(vec_back_a);
                for i in 0..v_t.len() { v_t[i] = f(v_t[i], bs[i]); }
                Ok(T::wrap(v_t))
            }
            Err(arc_a) => match std::sync::Arc::try_unwrap(arc_b) {
                Ok(vec_back_b) => {
                    // b unique — mutate b's buffer (compute a op b into b).
                    let as_: &[T] = bytemuck::cast_slice(&*arc_a);
                    let mut v_t: Vec<T> = bytemuck::cast_vec(vec_back_b);
                    for i in 0..v_t.len() { v_t[i] = f(as_[i], v_t[i]); }
                    Ok(T::wrap(v_t))
                }
                Err(arc_b) => {
                    // Both shared — fall back to fresh allocation.
                    let as_: &[T] = bytemuck::cast_slice(&*arc_a);
                    let bs: &[T] = bytemuck::cast_slice(&*arc_b);
                    let out: Vec<T> = as_.iter().zip(bs.iter()).map(|(&x, &y)| f(x, y)).collect();
                    Ok(T::wrap(out))
                }
            }
        }
    } else if na == 1 {
        // Scalar a × vec b — output length is b's; can't reuse a.
        let as_: &[T] = bytemuck::cast_slice(&*arc_a);
        let bs: &[T] = bytemuck::cast_slice(&*arc_b);
        let s = as_[0];
        let out: Vec<T> = bs.iter().map(|&y| f(s, y)).collect();
        Ok(T::wrap(out))
    } else if nb == 1 {
        // Vec a × scalar b — output is a's length; can reuse a.
        let bs: &[T] = bytemuck::cast_slice(&*arc_b);
        let s = bs[0];
        match std::sync::Arc::try_unwrap(arc_a) {
            Ok(vec_back) => {
                let mut v_t: Vec<T> = bytemuck::cast_vec(vec_back);
                for i in 0..v_t.len() { v_t[i] = f(v_t[i], s); }
                Ok(T::wrap(v_t))
            }
            Err(arc_a) => {
                let as_: &[T] = bytemuck::cast_slice(&*arc_a);
                let out: Vec<T> = as_.iter().map(|&x| f(x, s)).collect();
                Ok(T::wrap(out))
            }
        }
    } else {
        Err(format!("arith: length mismatch {} vs {}", na, nb))
    }
}

/// Mask-aware arith: source is a Prim wrapped in a View<Mask>; b is a
/// length-1 broadcast scalar. Walks source via mask, applies op, emits
/// a fresh Prim of length popcount. The output is a flat Prim (no
/// surviving mask context) — downstream filtering composes from there.
fn do_arith_masked_scalar(source: &Value, mask: &[u8], b: &Prim, op: ArithOp, interp: Interp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        ArithOp::Add => arith_masked_scalar::<$t, _>(source, mask, b, |x, y| x + y),
        ArithOp::Sub => arith_masked_scalar::<$t, _>(source, mask, b, |x, y| x - y),
        ArithOp::Mul => arith_masked_scalar::<$t, _>(source, mask, b, |x, y| x * y),
        ArithOp::Div => arith_masked_scalar::<$t, _>(source, mask, b, |x, y| x / y),
        ArithOp::Mod => arith_masked_scalar::<$t, _>(source, mask, b, |x, y| x % y),
    } }; }
    match interp {
        Interp::I8  => by_op!(i8),
        Interp::U8  => by_op!(u8),
        Interp::I16 => by_op!(i16),
        Interp::U16 => by_op!(u16),
        Interp::I32 => by_op!(i32),
        Interp::U32 => by_op!(u32),
        Interp::F32 => by_op!(f32),
        Interp::I64 => by_op!(i64),
        Interp::U64 => by_op!(u64),
        Interp::F64 => by_op!(f64),
    }
}

fn arith_masked_scalar<T, F>(source: &Value, mask: &[u8], b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> T
{
    let src_prim = match source {
        Value::Prim(p) => p,
        other => return Err(format!("arith masked: expected Prim source, got {:?}", other)),
    };
    let xs = T::extract(src_prim)?;
    let bs = T::extract(b)?;
    let scalar = bs[0];
    let popcnt = mask.iter().filter(|&&b| b != 0).count();
    let mut out: Vec<T> = Vec::with_capacity(popcnt);
    for i in 0..xs.len() {
        if mask[i] != 0 {
            out.push(f(xs[i], scalar));
        }
    }
    Ok(T::wrap(out))
}

/// `View<Mask>` arith vs full-length Prim. Length of b == popcount(mask).
fn do_arith_masked_full(source: &Value, mask: &[u8], b: &Prim, op: ArithOp, interp: Interp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        ArithOp::Add => arith_masked_full::<$t, _>(source, mask, b, |x, y| x + y),
        ArithOp::Sub => arith_masked_full::<$t, _>(source, mask, b, |x, y| x - y),
        ArithOp::Mul => arith_masked_full::<$t, _>(source, mask, b, |x, y| x * y),
        ArithOp::Div => arith_masked_full::<$t, _>(source, mask, b, |x, y| x / y),
        ArithOp::Mod => arith_masked_full::<$t, _>(source, mask, b, |x, y| x % y),
    } }; }
    match interp {
        Interp::I8  => by_op!(i8),  Interp::U8  => by_op!(u8),
        Interp::I16 => by_op!(i16), Interp::U16 => by_op!(u16),
        Interp::I32 => by_op!(i32), Interp::U32 => by_op!(u32), Interp::F32 => by_op!(f32),
        Interp::I64 => by_op!(i64), Interp::U64 => by_op!(u64), Interp::F64 => by_op!(f64),
    }
}

fn arith_masked_full<T, F>(source: &Value, mask: &[u8], b: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> T
{
    let src_prim = match source {
        Value::Prim(p) => p,
        other => return Err(format!("arith masked-full: expected Prim source, got {:?}", other)),
    };
    let xs = T::extract(src_prim)?;
    let bs = T::extract(b)?;
    let popcnt = mask.iter().filter(|&&b| b != 0).count();
    if bs.len() != popcnt {
        return Err(format!("arith masked-full: b length {} != popcount {}", bs.len(), popcnt));
    }
    let mut out: Vec<T> = Vec::with_capacity(popcnt);
    let mut k = 0usize;
    for i in 0..xs.len() {
        if mask[i] != 0 {
            out.push(f(xs[i], bs[k]));
            k += 1;
        }
    }
    Ok(T::wrap(out))
}

/// Both inputs are `View<Prim, Mask>` with equal logical length. Two-cursor
/// walk through both masks, emitting one output per matched k-th set bit.
fn do_arith_both_masked(
    src_a: &Value, mask_a: &[u8], src_b: &Value, mask_b: &[u8],
    op: ArithOp, interp: Interp,
) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        ArithOp::Add => arith_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x + y),
        ArithOp::Sub => arith_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x - y),
        ArithOp::Mul => arith_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x * y),
        ArithOp::Div => arith_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x / y),
        ArithOp::Mod => arith_both_masked::<$t, _>(src_a, mask_a, src_b, mask_b, |x, y| x % y),
    } }; }
    match interp {
        Interp::I8  => by_op!(i8),  Interp::U8  => by_op!(u8),
        Interp::I16 => by_op!(i16), Interp::U16 => by_op!(u16),
        Interp::I32 => by_op!(i32), Interp::U32 => by_op!(u32), Interp::F32 => by_op!(f32),
        Interp::I64 => by_op!(i64), Interp::U64 => by_op!(u64), Interp::F64 => by_op!(f64),
    }
}

fn arith_both_masked<T, F>(
    src_a: &Value, mask_a: &[u8], src_b: &Value, mask_b: &[u8], f: F,
) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T, T) -> T
{
    let pa = match src_a {
        Value::Prim(p) => p,
        other => return Err(format!("arith both-masked: a source must be Prim, got {:?}", other)),
    };
    let pb = match src_b {
        Value::Prim(p) => p,
        other => return Err(format!("arith both-masked: b source must be Prim, got {:?}", other)),
    };
    let xs = T::extract(pa)?;
    let ys = T::extract(pb)?;
    let popcount = mask_a.iter().filter(|&&b| b != 0).count();
    let mut out: Vec<T> = Vec::with_capacity(popcount);
    let mut ia = 0usize;
    let mut ib = 0usize;
    loop {
        while ia < mask_a.len() && mask_a[ia] == 0 { ia += 1; }
        while ib < mask_b.len() && mask_b[ib] == 0 { ib += 1; }
        if ia >= mask_a.len() || ib >= mask_b.len() { break; }
        out.push(f(xs[ia], ys[ib]));
        ia += 1; ib += 1;
    }
    Ok(T::wrap(out))
}

pub fn do_arith(a: &Prim, b: &Prim, op: ArithOp, interp: Interp) -> Result<Prim, String> {
    macro_rules! by_op { ($t:ty) => { match op {
        ArithOp::Add => arith_from_uninterp::<$t, _>(a, b, |x, y| x + y),
        ArithOp::Sub => arith_from_uninterp::<$t, _>(a, b, |x, y| x - y),
        ArithOp::Mul => arith_from_uninterp::<$t, _>(a, b, |x, y| x * y),
        ArithOp::Div => arith_from_uninterp::<$t, _>(a, b, |x, y| x / y),
        ArithOp::Mod => arith_from_uninterp::<$t, _>(a, b, |x, y| x % y),
    } }; }
    match interp {
        Interp::I8  => by_op!(i8),
        Interp::U8  => by_op!(u8),
        Interp::I16 => by_op!(i16),
        Interp::U16 => by_op!(u16),
        Interp::I32 => by_op!(i32),
        Interp::U32 => by_op!(u32),
        Interp::F32 => by_op!(f32),
        Interp::I64 => by_op!(i64),
        Interp::U64 => by_op!(u64),
        Interp::F64 => by_op!(f64),
    }
}

// ============================================================
// Unary arith: neg.<i>, abs.<i>
// ============================================================

#[derive(Copy, Clone, Debug)]
pub enum UnaryArithOp { Neg, Abs }

#[derive(Debug)]
pub struct UnaryArith { pub op: UnaryArithOp, pub interp: Interp }

/// Operator name — free fn so `SystemOp::UnaryArith` names it without a struct.
pub fn unary_name(op: UnaryArithOp) -> &'static str {
    match op { UnaryArithOp::Neg => "neg", UnaryArithOp::Abs => "abs" }
}

impl PrimOp for UnaryArith {
    fn name(&self) -> &str { unary_name(self.op) }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> { unary_run(self.op, self.interp, st) }
}
impl Typed for UnaryArith {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> { unary_tc(self.op, self.interp, st) }
}

/// The unary-arith kernel (back-end `SystemOp::UnaryArith` calls this directly).
pub fn unary_run(op: UnaryArithOp, interp: Interp, st: &mut Stack) -> Result<(), String> {
        let v = pop(st)?;
        // Segmented: a List maps per-element; bounds unchanged.
        if let Some(res) = list_elementwise1(&v, |va| {
            let p = extract_prim(va, "unary arith")?;
            Ok(Value::Prim(do_unary(&p, op, interp)?))
        }) {
            st.push(res?);
            return Ok(());
        }
        let p = extract_prim(&v, "unary arith")?;
        let out = do_unary(&p, op, interp)?;
        st.push(Value::Prim(out));
        Ok(())
}

/// The unary-arith typecheck.
pub fn unary_tc(op: UnaryArithOp, interp: Interp, st: &mut TypeStack) -> Result<(), String> {
        let v = tc_pop(st, "unary arith")?;
        let w = interp.width();
        match &v {
            Shape::Prim(vw) if *vw == w => { st.push(Shape::Prim(w)); Ok(()) }
            Shape::List { bounds, inner } if prim_width(inner) == Some(w) => {
                st.push(Shape::List { bounds: *bounds, inner: Box::new(Shape::Prim(w)) });
                Ok(())
            }
            _ => Err(format!("{}.{}: needs Prim({}) or List<Prim({})>, got {}",
                unary_name(op), interp, w, w, v)),
        }
}

fn unary_apply<T, F>(xs: &[T], f: F) -> Vec<T>
where T: Copy, F: Fn(T) -> T
{
    xs.iter().map(|&x| f(x)).collect()
}

fn unary_from_uninterp<T, F>(p: &Prim, f: F) -> Result<Prim, String>
where T: Storage + Copy, F: Fn(T) -> T
{
    let xs = T::extract(p)?;
    Ok(T::wrap(unary_apply(xs, f)))
}

pub fn do_unary(p: &Prim, op: UnaryArithOp, interp: Interp) -> Result<Prim, String> {
    // neg / abs: signed and float only. Unsigned errors loudly.
    match (op, interp) {
        (UnaryArithOp::Neg, Interp::I8)  => unary_from_uninterp::<i8,  _>(p, |x| -x),
        (UnaryArithOp::Neg, Interp::I16) => unary_from_uninterp::<i16, _>(p, |x| -x),
        (UnaryArithOp::Neg, Interp::I32) => unary_from_uninterp::<i32, _>(p, |x| -x),
        (UnaryArithOp::Neg, Interp::I64) => unary_from_uninterp::<i64, _>(p, |x| -x),
        (UnaryArithOp::Neg, Interp::F32) => unary_from_uninterp::<f32, _>(p, |x| -x),
        (UnaryArithOp::Neg, Interp::F64) => unary_from_uninterp::<f64, _>(p, |x| -x),
        (UnaryArithOp::Abs, Interp::I8)  => unary_from_uninterp::<i8,  _>(p, |x| x.wrapping_abs()),
        (UnaryArithOp::Abs, Interp::I16) => unary_from_uninterp::<i16, _>(p, |x| x.wrapping_abs()),
        (UnaryArithOp::Abs, Interp::I32) => unary_from_uninterp::<i32, _>(p, |x| x.wrapping_abs()),
        (UnaryArithOp::Abs, Interp::I64) => unary_from_uninterp::<i64, _>(p, |x| x.wrapping_abs()),
        (UnaryArithOp::Abs, Interp::F32) => unary_from_uninterp::<f32, _>(p, |x| x.abs()),
        (UnaryArithOp::Abs, Interp::F64) => unary_from_uninterp::<f64, _>(p, |x| x.abs()),
        (op, interp) => Err(format!(
            "{:?}.{}: unsupported for unsigned (use signed or float)", op, interp
        )),
    }
}
