//! Conversions and literals — `as.<i>` (width-cast),
//! `show.<i>` (debug-print under a given interpretation),
//! `LitNum` (scalar literal — `5u64`, `3.14f32`), and `LitArr`
//! (column literal — `u64[1 2 3]`).
//!
//! `LitNum` carries both an `i128` and an `f64` field so float
//! literals don't truncate. `LitArr` is a single struct
//! parameterized by `tag + Prim`; the parser builds the `Prim`
//! once at parse time, `run` is a cheap `Arc` clone.

use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, Storage, from_vec};
use crate::ir::shape::{Interp, Shape, is_primitive};

#[derive(Debug)]
pub struct As { pub interp: Interp }

impl PrimOp for As {
    fn name(&self) -> &str { "as" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let p = match v {
            Value::Prim(p) => p,
            other => return Err(format!("as: not a prim: {:?}", other)),
        };
        macro_rules! cast_from { ($src_t:ty, $($dst_interp:pat => $dst_t:ty),+) => {{
            let xs = <$src_t as Storage>::extract(&p)?;
            match self.interp {
                $( $dst_interp => from_vec::<$dst_t>(xs.iter().map(|&x| x as $dst_t).collect()), )+
            }
        }};}
        let out = match p.width() {
            PrimWidth::W8 => cast_from!(u8,
                Interp::U8 => u8, Interp::I8 => i8,
                Interp::U16 => u16, Interp::I16 => i16,
                Interp::U32 => u32, Interp::I32 => i32, Interp::F32 => f32,
                Interp::U64 => u64, Interp::I64 => i64, Interp::F64 => f64),
            PrimWidth::W16 => cast_from!(u16,
                Interp::U8 => u8, Interp::I8 => i8,
                Interp::U16 => u16, Interp::I16 => i16,
                Interp::U32 => u32, Interp::I32 => i32, Interp::F32 => f32,
                Interp::U64 => u64, Interp::I64 => i64, Interp::F64 => f64),
            PrimWidth::W32 => cast_from!(u32,
                Interp::U8 => u8, Interp::I8 => i8,
                Interp::U16 => u16, Interp::I16 => i16,
                Interp::U32 => u32, Interp::I32 => i32, Interp::F32 => f32,
                Interp::U64 => u64, Interp::I64 => i64, Interp::F64 => f64),
            PrimWidth::W64 => cast_from!(u64,
                Interp::U8 => u8, Interp::I8 => i8,
                Interp::U16 => u16, Interp::I16 => i16,
                Interp::U32 => u32, Interp::I32 => i32, Interp::F32 => f32,
                Interp::U64 => u64, Interp::I64 => i64, Interp::F64 => f64),
        };
        st.push(out);
        Ok(())
    }
}

impl Typed for As {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "as")?;
        if !is_primitive(&v) { return Err(format!("as.{}: not primitive: {}", self.interp, v)); }
        st.push(Shape::Prim(self.interp.width()));
        Ok(())
    }
}

/// Scalar literal. Carries both an integer and a float field so float
/// literals (`3.14f64`) don't get truncated to their integer part.
/// `n` is used for integer interpretations; `f` is used for `F32`/`F64`.
#[derive(Debug)]
pub struct LitNum { pub n: i128, pub f: f64, pub interp: Interp }

impl PrimOp for LitNum {
    fn name(&self) -> &str { "lit" }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let n = self.n;
        let f = self.f;
        let v = match self.interp {
            Interp::U8  => from_vec::<u8>(vec![n as u8]),
            Interp::I8  => from_vec::<i8>(vec![n as i8]),
            Interp::U16 => from_vec::<u16>(vec![n as u16]),
            Interp::I16 => from_vec::<i16>(vec![n as i16]),
            Interp::U32 => from_vec::<u32>(vec![n as u32]),
            Interp::I32 => from_vec::<i32>(vec![n as i32]),
            Interp::F32 => from_vec::<f32>(vec![f as f32]),
            Interp::U64 => from_vec::<u64>(vec![n as u64]),
            Interp::I64 => from_vec::<i64>(vec![n as i64]),
            Interp::F64 => from_vec::<f64>(vec![f]),
        };
        st.push(v); Ok(())
    }
}

impl Typed for LitNum {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        st.push(Shape::Prim(self.interp.width())); Ok(())
    }
}

/// `<type>[ elem … ]` literal. The parser builds the `Prim` once at
/// parse time; `run` clones the underlying `Arc<Vec<_>>` (cheap).
/// `tag` is the surface form (`"u64[]"`, `"bool[]"`, …) for diagnostics.
#[derive(Debug)]
pub struct LitArr { pub tag: &'static str, pub prim: Prim }

impl PrimOp for LitArr {
    fn name(&self) -> &str { self.tag }
    fn arity(&self) -> Option<(usize, usize)> { Some((0, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        st.push(Value::Prim(self.prim.clone())); Ok(())
    }
}
impl Typed for LitArr {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        st.push(Shape::Prim(self.prim.width())); Ok(())
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    use crate::syntax::registry::{parse_interp, split_suffix};
    // as.<interp> and show.<interp>
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        let interp = parse_interp(sfx)?;
        match head {
            "as" => Some(Box::new(As { interp })),
            "show" => Some(Box::new(Show { interp })),
            _ => None,
        }
    });
    // numeric literal with type-tag suffix: 5u64, 3.14f32, etc.
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        for (sfx, interp) in [
            ("u8", Interp::U8), ("i8", Interp::I8),
            ("u16", Interp::U16), ("i16", Interp::I16),
            ("u32", Interp::U32), ("i32", Interp::I32), ("f32", Interp::F32),
            ("u64", Interp::U64), ("i64", Interp::I64), ("f64", Interp::F64),
        ] {
            if let Some(rest) = t.strip_suffix(sfx) {
                if let Ok(n) = rest.parse::<i128>() {
                    return Some(Box::new(LitNum { n, f: n as f64, interp }));
                }
                if let Ok(f) = rest.parse::<f64>() {
                    return Some(Box::new(LitNum { n: f as i128, f, interp }));
                }
            }
        }
        None
    });
}

/// `show.<interp>` — print top-of-stack to stderr with the given interpretation,
/// then push it back. Useful for debugging when the default Display (raw bytes)
/// loses interpretation (e.g., F64 printed as u64 bits).
#[derive(Debug)] pub struct Show { pub interp: Interp }
impl PrimOp for Show {
    fn name(&self) -> &str { "show" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = crate::ir::stack::pop(st)?;
        eprintln!("[show.{}] {}", self.interp, show_value(&v, self.interp));
        st.push(v);
        Ok(())
    }
}
impl Typed for Show {
    fn tc(&self, _st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        // Stack-shape-preserving; layer-3 inspection optional.
        Ok(())
    }
}

/// Format a Value, reinterpreting Prim leaves under the given interpretation.
fn show_value(v: &Value, interp: Interp) -> String {
    match v {
        Value::Prim(p) => show_prim(p, interp),
        Value::Prod(fs) => {
            let mut s = String::from("(");
            for (i, x) in fs.iter().enumerate() {
                if i > 0 { s.push_str(", "); }
                s.push_str(&show_value(x, interp));
            }
            s.push(')'); s
        }
        Value::Sum { disc, lanes } => {
            let mut s = format!("Sum{{disc={:?}, lanes=[", disc);
            for (i, x) in lanes.iter().enumerate() {
                if i > 0 { s.push_str(", "); }
                s.push_str(&show_value(x, interp));
            }
            s.push_str("]}"); s
        }
        Value::List { bounds, values } => {
            format!("List{{bounds={:?}, values={}}}", bounds, show_value(values, interp))
        }
        Value::View { .. } => {
            // Materialize before reinterpreting; show.<interp> is a debugging
            // path, the gather is fine here.
            match crate::ops::helpers::materialize_ref(v) {
                Ok(mat) => show_value(mat.as_ref(), interp),
                Err(e) => format!("View<{}>", e),
            }
        }
    }
}

fn show_prim(p: &crate::ir::value::Prim, interp: Interp) -> String {
    macro_rules! fmt { ($t:ty) => {
        match <$t as Storage>::extract(p) {
            Ok(xs) => format!("{}{:?}", stringify!($t), xs),
            Err(e) => format!("<{}>", e),
        }
    };}
    match interp {
        Interp::U8  => fmt!(u8),
        Interp::I8  => fmt!(i8),
        Interp::U16 => fmt!(u16),
        Interp::I16 => fmt!(i16),
        Interp::U32 => fmt!(u32),
        Interp::I32 => fmt!(i32),
        Interp::F32 => fmt!(f32),
        Interp::U64 => fmt!(u64),
        Interp::I64 => fmt!(i64),
        Interp::F64 => fmt!(f64),
    }
}

