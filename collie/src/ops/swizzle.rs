//! Order-preserving byte (de)swizzling for flat primitive columns.
//!
//! These ops let the unsigned byte sort (`sort` on a Prim) order signed and
//! float data correctly: transform a column into an *order-form* byte
//! image whose unsigned ordering equals the column's semantic ordering,
//! sort the bytes, then transform back. The transforms are bijections,
//! so they leave equality (and thus `group`/`unique`) untouched.
//!
//! The interp tag selects the per-element bit flip; it is *not* recorded
//! in the value. The result is byte-identical in shape to the input —
//! the "this column is in order-form" invariant lives with whoever
//! emits these ops (a type-aware front end), not in the core type, which
//! is width-only by design.
//!
//!   - unsigned: identity (the front end need not emit it at all).
//!   - signed:   flip the sign bit — maps two's complement to offset
//!               binary, so unsigned order matches signed order.
//!               Self-inverse, so en/de are the same flip.
//!   - float:    flip the sign bit if positive, all bits if negative —
//!               the standard IEEE-754 monotone encoding. En and de are
//!               distinct (see `float_dec`).

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim};
use crate::ir::shape::{Shape, Interp};

#[inline] fn f32_enc(x: u32) -> u32 { x ^ (if x >> 31 == 1 { 0xFFFF_FFFF } else { 0x8000_0000 }) }
#[inline] fn f32_dec(x: u32) -> u32 { x ^ (if x >> 31 == 1 { 0x8000_0000 } else { 0xFFFF_FFFF }) }
#[inline] fn f64_enc(x: u64) -> u64 { x ^ (if x >> 63 == 1 { u64::MAX } else { 1 << 63 }) }
#[inline] fn f64_dec(x: u64) -> u64 { x ^ (if x >> 63 == 1 { 1 << 63 } else { u64::MAX }) }

/// Map a Prim's words in place (reusing the buffer when Arc-1) with `f`.
fn map_words<T, F>(arc: Arc<Vec<T>>, f: F) -> Arc<Vec<T>>
where T: Copy, F: Fn(T) -> T {
    let mut v = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
    for x in v.iter_mut() { *x = f(*x); }
    Arc::new(v)
}

/// Apply the order-preserving swizzle (or its inverse) for `interp`.
/// Errors if the Prim width doesn't match the interp.
pub fn swizzle(p: Prim, interp: Interp, encode: bool) -> Result<Prim, String> {
    use Interp::*;
    let bad = |p: &Prim| format!("swizzle.{}: expected {} column, got {:?}", interp, interp.width(), p.width());
    Ok(match interp {
        U8 | U16 | U32 | U64 => p,
        I8  => match p { Prim::P8(a)  => Prim::P8(map_words(a, |x| x ^ 0x80)),                       o => return Err(bad(&o)) },
        I16 => match p { Prim::P16(a) => Prim::P16(map_words(a, |x| x ^ 0x8000)),                    o => return Err(bad(&o)) },
        I32 => match p { Prim::P32(a) => Prim::P32(map_words(a, |x| x ^ 0x8000_0000)),               o => return Err(bad(&o)) },
        I64 => match p { Prim::P64(a) => Prim::P64(map_words(a, |x| x ^ (1 << 63))),                 o => return Err(bad(&o)) },
        F32 => match p { Prim::P32(a) => Prim::P32(map_words(a, if encode { f32_enc } else { f32_dec })), o => return Err(bad(&o)) },
        F64 => match p { Prim::P64(a) => Prim::P64(map_words(a, if encode { f64_enc } else { f64_dec })), o => return Err(bad(&o)) },
    })
}

fn run_swizzle(st: &mut Stack, interp: Interp, encode: bool, who: &str) -> Result<(), String> {
    match pop(st)? {
        Value::Prim(p) => { st.push(Value::Prim(swizzle(p, interp, encode)?)); Ok(()) }
        other => Err(format!("{}: expected Prim, got {}", who, other)),
    }
}

fn tc_swizzle(st: &mut TypeStack, interp: Interp, who: &str) -> Result<(), String> {
    let s = tc_pop(st, who)?;
    let w = interp.width();
    match s {
        Shape::Prim(pw) if pw == w => { st.push(Shape::Prim(w)); Ok(()) }
        other => Err(format!("{}.{}: needs Prim({}), got {}", who, interp, w, other)),
    }
}

#[derive(Debug)] pub struct Enswizzle { pub interp: Interp }
#[derive(Debug)] pub struct Deswizzle { pub interp: Interp }

impl PrimOp for Enswizzle {
    fn name(&self) -> &str { "enswizzle" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        run_swizzle(st, self.interp, true, "enswizzle")
    }
}
impl Typed for Enswizzle {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        tc_swizzle(st, self.interp, "enswizzle")
    }
}

impl PrimOp for Deswizzle {
    fn name(&self) -> &str { "deswizzle" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        run_swizzle(st, self.interp, false, "deswizzle")
    }
}
impl Typed for Deswizzle {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        tc_swizzle(st, self.interp, "deswizzle")
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        match head {
            "enswizzle" => Some(Box::new(Enswizzle { interp })),
            "deswizzle" => Some(Box::new(Deswizzle { interp })),
            _ => None,
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::from_vec;

    fn roundtrip(interp: Interp, p: Prim) {
        let enc = swizzle(p.clone(), interp, true).unwrap();
        let dec = swizzle(enc, interp, false).unwrap();
        assert_eq!(dec, p, "roundtrip failed for {:?}", interp);
    }

    #[test]
    fn signed_order_preserving() {
        // i32 values: -2, -1, 0, 1, 2 → encoded must be ascending unsigned.
        let raw: Vec<i32> = vec![-2, -1, 0, 1, 2];
        let p = if let Value::Prim(p) = from_vec(raw) { p } else { unreachable!() };
        let enc = swizzle(p, Interp::I32, true).unwrap();
        let words: &[u32] = if let Prim::P32(a) = &enc { a } else { unreachable!() };
        assert!(words.windows(2).all(|w| w[0] < w[1]), "not ascending: {:?}", words);
    }

    #[test]
    fn float_order_preserving() {
        let raw: Vec<f64> = vec![-2.5, -0.0, 0.0, 1.0, 3.5, f64::INFINITY];
        let p = if let Value::Prim(p) = from_vec(raw) { p } else { unreachable!() };
        let enc = swizzle(p, Interp::F64, true).unwrap();
        let words: &[u64] = if let Prim::P64(a) = &enc { a } else { unreachable!() };
        assert!(words.windows(2).all(|w| w[0] <= w[1]), "not ascending: {:?}", words);
    }

    #[test]
    fn roundtrips() {
        roundtrip(Interp::I8, Prim::P8(Arc::new(vec![0, 1, 200, 255])));
        roundtrip(Interp::I64, if let Value::Prim(p) = from_vec::<i64>(vec![-5, 0, 7]) { p } else { unreachable!() });
        roundtrip(Interp::F32, if let Value::Prim(p) = from_vec::<f32>(vec![-1.5, 0.0, 2.0]) { p } else { unreachable!() });
        roundtrip(Interp::F64, if let Value::Prim(p) = from_vec::<f64>(vec![-1.5, 0.0, 2.0]) { p } else { unreachable!() });
    }
}
