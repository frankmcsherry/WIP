//! Demonstration of plug-in operators: this file is OUTSIDE `ops/` and is
//! *only* referenced from `main.rs`. The core, type checker, parser, and
//! standard operators have no knowledge of these ops. Yet because the parser
//! takes an `OpRegistry`, simply registering these factories makes them
//! available to programs — same syntax, same eval path, same type checker.
//!
//! Two ops here:
//!   - `square.<interp>` — per-element x*x
//!   - `clamp.<interp>` — given (col, lo, hi), clamp col into [lo, hi]

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Storage, from_vec};
use crate::ir::shape::{Interp, Shape};
use crate::syntax::registry::OpRegistry;

#[derive(Debug)]
pub struct Square { pub interp: Interp }

impl PrimOp for Square {
    fn name(&self) -> &str { "square" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let p = match v { Value::Prim(p) => p, other => return Err(format!("square: not a prim: {:?}", other)) };
        macro_rules! sq { ($t:ty) => {{
            let xs = <$t as Storage>::extract(&p)?;
            from_vec::<$t>(xs.iter().map(|&x| x * x).collect())
        }};}
        let out = match self.interp {
            Interp::U8  => sq!(u8),
            Interp::I8  => sq!(i8),
            Interp::U16 => sq!(u16),
            Interp::I16 => sq!(i16),
            Interp::U32 => sq!(u32),
            Interp::I32 => sq!(i32),
            Interp::U64 => sq!(u64),
            Interp::I64 => sq!(i64),
            Interp::F32 => sq!(f32),
            Interp::F64 => sq!(f64),
        };
        st.push(out);
        Ok(())
    }
}
impl Typed for Square {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "square")?;
        match &v {
            Shape::Prim(w) if *w == self.interp.width() => { st.push(v); Ok(()) }
            other => Err(format!("square.{}: needs Prim({}), got {}", self.interp, self.interp.width(), other)),
        }
    }
}

#[derive(Debug)]
pub struct Clamp { pub interp: Interp }

impl PrimOp for Clamp {
    fn name(&self) -> &str { "clamp" }
    fn arity(&self) -> Option<(usize, usize)> { Some((3, 1)) }  // (col, lo, hi) → clamped
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        // stack: col lo hi  →  clamped col
        let hi = pop(st)?;
        let lo = pop(st)?;
        let col = pop(st)?;
        let pc = match col { Value::Prim(p) => p, other => return Err(format!("clamp: col not prim: {:?}", other)) };
        let plo = match lo  { Value::Prim(p) => p, other => return Err(format!("clamp: lo not prim: {:?}", other)) };
        let phi = match hi  { Value::Prim(p) => p, other => return Err(format!("clamp: hi not prim: {:?}", other)) };
        macro_rules! cl { ($t:ty) => {{
            let xs = <$t as Storage>::extract(&pc)?;
            let los = <$t as Storage>::extract(&plo)?;
            let his = <$t as Storage>::extract(&phi)?;
            if los.len() != 1 || his.len() != 1 {
                return Err("clamp: lo and hi must be length-1".into());
            }
            let (l, h) = (los[0], his[0]);
            from_vec::<$t>(xs.iter().map(|&x| if x < l { l } else if x > h { h } else { x }).collect())
        }};}
        let out = match self.interp {
            Interp::U8  => cl!(u8),
            Interp::I8  => cl!(i8),
            Interp::U16 => cl!(u16),
            Interp::I16 => cl!(i16),
            Interp::U32 => cl!(u32),
            Interp::I32 => cl!(i32),
            Interp::U64 => cl!(u64),
            Interp::I64 => cl!(i64),
            Interp::F32 => return Err("clamp.f32: floats not Ord; use min/max".into()),
            Interp::F64 => return Err("clamp.f64: floats not Ord; use min/max".into()),
        };
        st.push(out);
        Ok(())
    }
}
impl Typed for Clamp {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let hi = tc_pop(st, "clamp")?;
        let lo = tc_pop(st, "clamp")?;
        let col = tc_pop(st, "clamp")?;
        let w = self.interp.width();
        for (name, s) in [("hi", &hi), ("lo", &lo), ("col", &col)] {
            match s {
                Shape::Prim(ww) if *ww == w => {}
                other => return Err(format!("clamp.{}: {} must be Prim({}), got {}", self.interp, name, w, other)),
            }
        }
        st.push(col);
        Ok(())
    }
}

/// Register the extra ops. Called from main.rs once; the core never sees this file.
pub fn register(reg: &mut OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    reg.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        match head {
            "square" => Some(Box::new(Square { interp })),
            "clamp" => Some(Box::new(Clamp { interp })),
            _ => None,
        }
    });
}
