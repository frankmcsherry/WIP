//! Layer 3: the type system. Adds `Typed` trait, `TypeStack`/`TypeEnv`, and
//! the typecheck runner. Defines the combined `Op` trait that the parser
//! and most consumers actually use (PrimOp + Typed bundled).

use crate::ir::op::PrimOp;
use crate::ir::shape::Shape;

pub type TypeStack = Vec<Shape>;
pub type TypeEnv = Vec<Shape>;

/// Optional layer-3 trait: operators that want static type-checking
/// implement this. The default impl is a no-op (best-effort checker).
pub trait Typed {
    fn tc(&self, _st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        Ok(())
    }
}

/// The combined trait that parser+eval+typecheck all share.
/// An operator must implement both PrimOp (run) and Typed (tc).
pub trait Op: PrimOp + Typed {}
impl<T: PrimOp + Typed + ?Sized> Op for T {}

/// Walk a program type-checking each op.
pub fn typecheck<O: Typed + ?Sized>(
    prog: &[Box<O>],
    st: &mut TypeStack,
    env: &mut TypeEnv,
) -> Result<(), String> {
    for op in prog { op.tc(st, env)?; }
    Ok(())
}

pub fn tc_pop(st: &mut TypeStack, ctx: &str) -> Result<Shape, String> {
    st.pop().ok_or_else(|| format!("{}: type-stack underflow", ctx))
}
