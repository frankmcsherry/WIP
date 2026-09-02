//! Structural shapes. The core's "type" is a *shape* — structure (Prod/Sum/List)
//! plus the leaf's bit width (`Prim(w)` for w in 8/16/32/64). Its whole job is
//! to turn the engine's shape panics
//! into static errors. Numeric kinds (i32/f32/u32) are NOT shapes — they're an
//! interpretation a higher layer (front-end) supplies; the core never learns them.
//!
//! The shape-checker is `eval` lifted to shape terms: each op's rule (`Op::judge`)
//! pattern-matches the input shape, reads arity off it, and propagates forward. Every
//! shape is concrete — a `Sum` names all its lanes' shapes, so `Inject` carries the
//! whole sum it builds and the merge ops (`Unwrap`, `Select`, `Find`'s two lists) simply
//! require equality. Lengths/strata are a separate pass.

use crate::value::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Shape {
    Prim(u32), // a leaf column, by bit width (8/16/32/64, matching the `Prim` widths)
    Prod(Vec<Shape>),
    Sum(Vec<Shape>), // one shape per variant lane (a lane no row carries is an empty column of it)
    List(Box<Shape>),
    Unit, // the length-carrying unit (payload-free); `None` of `Option = Sum{Unit | T}`.
}

/// the one shape two merging operands must share: their common shape, or the type error.
pub fn same(a: &Shape, b: &Shape) -> Result<Shape, String> {
    if a == b { Ok(a.clone()) } else { Err(format!("shapes differ: {a} vs {b}")) }
}

/// the structural shape of a concrete value.
pub fn shape_of_value(v: &Value) -> Shape {
    match v {
        Value::Prim(p) => Shape::Prim(p.bits()),
        Value::Prod(cols) => Shape::Prod(cols.iter().map(shape_of_value).collect()),
        Value::Sum(_, variants) => Shape::Sum(variants.iter().map(shape_of_value).collect()),
        Value::List(_, vals) => Shape::List(Box::new(shape_of_value(vals))),
        Value::Unit(_) => Shape::Unit,
    }
}

impl std::fmt::Display for Shape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Shape::Prim(w) => write!(f, "U{w}"),
            Shape::Prod(ts) => {
                let inner: Vec<String> = ts.iter().map(|t| t.to_string()).collect();
                write!(f, "({})", inner.join(", "))
            }
            Shape::Sum(ts) => {
                let inner: Vec<String> = ts.iter().map(|t| t.to_string()).collect();
                write!(f, "{{{}}}", inner.join(" | "))
            }
            Shape::List(t) => write!(f, "List<{t}>"),
            Shape::Unit => write!(f, "()"),
        }
    }
}
