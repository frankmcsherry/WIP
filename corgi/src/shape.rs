//! Structural shapes. The core's "type" is a *shape* — structure (Prod/Sum/List)
//! plus the leaf's bit width (`Prim(w)` for w in 8/16/32/64). Its whole job is
//! to turn the engine's shape panics
//! into static errors. Numeric kinds (i32/f32/u32) are NOT shapes — they're an
//! interpretation a higher layer (front-end) supplies; the core never learns them.
//!
//! The shape-checker is `eval` lifted to shape terms: each op's rule (`Op::judge`)
//! pattern-matches the input shape, reads arity off it, and propagates forward. For a
//! concrete `Input` shape this needs no unification — constraints (`Unwrap`
//! homogeneity, `Find`'s two lists) are equality checks. Lengths/strata are separate.

use crate::value::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Prim(u32), // a leaf column, by bit width (8/16/32/64, matching the `Prim` widths)
    Prod(Vec<Shape>),
    Sum(Vec<Shape>),
    List(Box<Shape>),
}

/// the structural shape of a concrete value.
pub fn shape_of_value(v: &Value) -> Shape {
    match v {
        Value::Prim(p) => Shape::Prim(p.bits()),
        Value::Prod(cols) => Shape::Prod(cols.iter().map(shape_of_value).collect()),
        Value::Sum(_, variants) => Shape::Sum(variants.iter().map(shape_of_value).collect()),
        Value::List(_, vals) => Shape::List(Box::new(shape_of_value(vals))),
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
        }
    }
}
