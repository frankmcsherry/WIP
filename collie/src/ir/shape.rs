//! Structural shape — the typechecker's parallel to `Value`. Mirrors
//! the closed universe (Prim with width, Prod, Sum, List) but
//! contains no values. Carries widths only; interpretations
//! (i32 vs u32 vs f32) live on the op tokens, not on shapes
//! (principle 3 in `PRINCIPLES.md`).
//!
//! `Interp` is here too because both the typechecker and the
//! op-token side need to talk about it.

use std::fmt;
use crate::ir::value::{PrimWidth, Prim, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    Prim(PrimWidth),
    Prod(Vec<Shape>),
    Sum  { disc: PrimWidth, lanes: Vec<Shape> },
    List { bounds: PrimWidth, inner: Box<Shape> },
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Shape::Prim(w) => write!(f, "{}", w),
            Shape::Prod(fs) => {
                write!(f, "(")?;
                for (i, s) in fs.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", s)?;
                }
                write!(f, ")")
            }
            Shape::Sum { disc, lanes } => {
                write!(f, "Sum[disc={}]<", disc)?;
                for (i, s) in lanes.iter().enumerate() {
                    if i > 0 { write!(f, " | ")?; }
                    write!(f, "{}", s)?;
                }
                write!(f, ">")
            }
            Shape::List { bounds, inner } => write!(f, "List[bounds={}]<{}>", bounds, inner),
        }
    }
}

pub fn shape_of(v: &Value) -> Shape {
    match v {
        Value::Prim(p) => Shape::Prim(p.width()),
        Value::Prod(fs) => Shape::Prod(fs.iter().map(shape_of).collect()),
        Value::Sum { disc, lanes } => Shape::Sum {
            disc: disc.width(),
            lanes: lanes.iter().map(shape_of).collect(),
        },
        Value::List { bounds, values } => Shape::List {
            bounds: bounds.width(),
            inner: Box::new(shape_of(values)),
        },
        // View shape depends on the selector variant:
        //   Indices / Range → flat: view inherits source's shape.
        //   SequenceRange   → row-shaped: view looks like List<source's shape>,
        //                     since each "row" is a sub-slice of source.
        Value::View { source, selector } => match selector {
            crate::ir::value::Selector::SequenceRange { .. } => Shape::List {
                bounds: PrimWidth::W64,
                inner: Box::new(shape_of(source)),
            },
            _ => shape_of(source),
        },
    }
}

pub fn is_primitive(s: &Shape) -> bool { matches!(s, Shape::Prim(_)) }

/// Per-op interpretation tag: how to read the bytes of a Prim.
/// Lives in layer 1 because both operators (layer 2) and the parser (layer 4)
/// need to reference it, and it's a tiny enum that doesn't ascribe meaning to
/// any particular Value — only to a specific op invocation.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Interp { I8, U8, I16, U16, I32, U32, F32, I64, U64, F64 }

impl Interp {
    pub fn width(&self) -> PrimWidth {
        match self {
            Interp::I8  | Interp::U8                                  => PrimWidth::W8,
            Interp::I16 | Interp::U16                                 => PrimWidth::W16,
            Interp::I32 | Interp::U32 | Interp::F32                   => PrimWidth::W32,
            Interp::I64 | Interp::U64 | Interp::F64                   => PrimWidth::W64,
        }
    }
}

impl fmt::Display for Interp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Interp::I8  => write!(f, "i8"),
            Interp::U8  => write!(f, "u8"),
            Interp::I16 => write!(f, "i16"),
            Interp::U16 => write!(f, "u16"),
            Interp::I32 => write!(f, "i32"),
            Interp::U32 => write!(f, "u32"),
            Interp::F32 => write!(f, "f32"),
            Interp::I64 => write!(f, "i64"),
            Interp::U64 => write!(f, "u64"),
            Interp::F64 => write!(f, "f64"),
        }
    }
}

/// Disc Prim helper used by Sum constructions.
pub fn disc_as_u8(p: &Prim) -> Result<&[u8], String> {
    match p {
        Prim::P8(v) => Ok(v),
        other => Err(format!("expected P8 disc, got {:?}", other.width())),
    }
}

/// Bounds-as-start-offsets helper. Returns a borrowed slice of N+1 entries
/// when stored as P64; allocates a fresh Vec when synthesized from Stride
/// or Runs. The leading entry is always 0; the trailing entry is the total
/// flat length. Most consumers should prefer `BoundsRepr::iter_pairs()` if
/// they only iterate, to avoid the alloc for Stride/Runs.
pub fn bounds_as_u64(b: &crate::ir::value::BoundsRepr) -> Result<std::borrow::Cow<'_, [u64]>, String> {
    use std::borrow::Cow;
    use crate::ir::value::BoundsRepr;
    match b {
        BoundsRepr::Var(Prim::P64(v)) => Ok(Cow::Borrowed(v.as_slice())),
        BoundsRepr::Var(other) => Err(format!("expected P64 bounds, got {:?}", other.width())),
        BoundsRepr::Stride { .. } => Ok(Cow::Owned(b.to_vec())),
        BoundsRepr::Runs(_) => Ok(Cow::Owned(b.to_vec())),
    }
}
