//! Structural shapes. The core's "type" is a *shape* — structure (Prod/Sum/List)
//! plus the leaf's bit width (`Prim(w)` for w in 8/16/32/64). Its whole job is
//! to turn the engine's shape panics
//! into static errors. Numeric kinds (i32/f32/u32) are NOT shapes — they're an
//! interpretation a higher layer (front-end) supplies; the core never learns them.
//!
//! The shape-checker is `eval` lifted to shape terms: each op's rule (`Op::judge`)
//! pattern-matches the input shape, reads arity off it, and propagates forward. The one
//! non-equality constraint is `⊥`, which lives only as a `None` lane of a `Sum`: `Inject`
//! commits a single lane and leaves the rest `None`, and the merge ops (`Unwrap`, `Find`'s
//! two lists) resolve them by `join`, so an empty lane adopts its sibling's shape. Lengths/
//! strata are a separate pass.

use crate::value::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Prim(u32), // a leaf column, by bit width (8/16/32/64, matching the `Prim` widths)
    Prod(Vec<Shape>),
    Sum(Vec<Option<Shape>>), // one slot per variant; `None` is `⊥` — an uncommitted (e.g. `Inject`'d-past)
                             // lane, the bottom of the lattice that `join` lets adopt a sibling's shape.
    List(Box<Shape>),
    Unit, // the length-carrying unit (payload-free); `None` of `Option = Sum{Unit | T}`.
}

/// the join of two shapes: equal concretes join to themselves, the recursion is structural, and a
/// `Sum` joins lane-wise where `None ⊔ s = s` (an uncommitted lane adopts its sibling). A mismatch of
/// two *concrete* shapes — including a differing arity — is the genuine type error (the `Some(int)`
/// vs `Some(str)` clash).
pub fn join(a: &Shape, b: &Shape) -> Result<Shape, String> {
    use Shape::*;
    Ok(match (a, b) {
        (Prim(x), Prim(y)) if x == y => Prim(*x),
        (Prod(xs), Prod(ys)) if xs.len() == ys.len() => {
            Prod(xs.iter().zip(ys).map(|(x, y)| join(x, y)).collect::<Result<_, _>>()?)
        }
        (Sum(xs), Sum(ys)) if xs.len() == ys.len() => {
            Sum(xs.iter().zip(ys).map(|(x, y)| join_lane(x, y)).collect::<Result<_, _>>()?)
        }
        (List(x), List(y)) => List(Box::new(join(x, y)?)),
        (Unit, Unit) => Unit,
        _ => return Err(format!("shapes do not unify: {a} vs {b}")),
    })
}

/// join of two `Sum` lanes: `None` is `⊥` and adopts the other side; two committed lanes join structurally.
fn join_lane(a: &Option<Shape>, b: &Option<Shape>) -> Result<Option<Shape>, String> {
    match (a, b) {
        (None, x) | (x, None) => Ok(x.clone()),
        (Some(x), Some(y)) => Ok(Some(join(x, y)?)),
    }
}

/// the structural shape of a concrete value.
pub fn shape_of_value(v: &Value) -> Shape {
    match v {
        Value::Prim(p) => Shape::Prim(p.bits()),
        Value::Prod(cols) => Shape::Prod(cols.iter().map(shape_of_value).collect()),
        Value::Sum(_, _, variants) => {
            Shape::Sum(variants.iter().map(|o| o.as_ref().map(shape_of_value)).collect())
        }
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
                let inner: Vec<String> =
                    ts.iter().map(|t| t.as_ref().map_or("⊥".to_string(), |s| s.to_string())).collect();
                write!(f, "{{{}}}", inner.join(" | "))
            }
            Shape::List(t) => write!(f, "List<{t}>"),
            Shape::Unit => write!(f, "()"),
        }
    }
}
