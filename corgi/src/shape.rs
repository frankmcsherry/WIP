//! Structural shapes. The core's "type" is a *shape* — structure (Prod/Sum/List) over two leaves,
//! `Int` (an integer) and `Float` (an `f64`). Its whole job is to turn the engine's shape panics
//! into static errors. An integer's storage (a byte, an `i64`) is not part of its shape: the same
//! integers are the same value however they are held.
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
    Int,   // a leaf of integers, at whatever storage holds them
    Float, // a leaf of floats (`f64`)
    Prod(Vec<Shape>),
    Sum(Vec<Shape>), // one shape per variant lane (a lane no row carries is an empty column of it)
    List(Box<Shape>),
    Unit, // the length-carrying unit (payload-free); `None` of `Option = Sum{Unit | T}`.
    Ref(Box<Shape>), // references to rows of the named shape; today only `Ref<List<T>>` (`&[T]`), made by `ref`
}

/// the one shape two merging operands must share: their common shape, or the type error.
pub(crate) fn same(a: &Shape, b: &Shape) -> Result<Shape, String> {
    if a == b { Ok(a.clone()) } else { Err(format!("shapes differ: {a} vs {b}")) }
}

/// the structural shape of a concrete value.
pub fn shape_of_value(v: &Value) -> Shape {
    match v {
        Value::Prim(p) if p.is_int() => Shape::Int,
        Value::Prim(_) => Shape::Float,
        Value::Prod(cols) => Shape::Prod(cols.iter().map(shape_of_value).collect()),
        Value::Sum(_, variants) => Shape::Sum(variants.iter().map(shape_of_value).collect()),
        Value::List(_, vals) => Shape::List(Box::new(shape_of_value(vals))),
        Value::Unit(_) => Shape::Unit,
        Value::Ref(list, _) => Shape::Ref(Box::new(shape_of_value(list))),
    }
}

impl std::fmt::Display for Shape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Shape::Int => write!(f, "Int"),
            Shape::Float => write!(f, "Float"),
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
            Shape::Ref(t) => write!(f, "Ref<{t}>"),
        }
    }
}
