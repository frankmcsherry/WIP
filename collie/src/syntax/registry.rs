//! Layer 4 helper: a registry of operator factories indexed by token text.
//!
//! The parser uses this to look up "+", "+.i32", "dup", etc. without
//! hardcoding the full operator vocabulary. Each operator family registers
//! one or more factory closures; the parser walks the registry per token
//! and returns the first factory that produces an op.
//!
//! This is one way to implement the parser's name→op mapping. A different
//! front-end (JSON, programmatic) wouldn't need a registry; nothing in the
//! core or operator layers depends on this file.

use crate::ir::typecheck::Op;

/// A factory is anything that, given a token, decides whether it can
/// construct an op from it. Returns None to defer to the next factory.
pub type Factory = Box<dyn Fn(&str) -> Option<Box<dyn Op>> + Send + Sync>;

pub struct OpRegistry {
    factories: Vec<Factory>,
}

impl OpRegistry {
    pub fn new() -> Self { Self { factories: Vec::new() } }

    pub fn add<F>(&mut self, f: F)
    where F: Fn(&str) -> Option<Box<dyn Op>> + Send + Sync + 'static
    {
        self.factories.push(Box::new(f));
    }

    /// Walk factories in registration order; first hit wins.
    pub fn make(&self, token: &str) -> Option<Box<dyn Op>> {
        for f in &self.factories {
            if let Some(op) = f(token) { return Some(op); }
        }
        None
    }

    /// Build the standard registry from all the op modules.
    pub fn standard() -> Self {
        let mut r = Self::new();
        crate::ops::stack::register(&mut r);
        crate::ops::combinators::register(&mut r);
        crate::ops::list::register(&mut r);
        crate::ops::join::register(&mut r);
        crate::ops::arith::register(&mut r);
        crate::ops::cmp::register(&mut r);
        crate::ops::convert::register(&mut r);
        crate::ops::reduce_ops::register(&mut r);
        crate::ops::sort_concat::register(&mut r);
        crate::ops::sort::register(&mut r);
        crate::ops::swizzle::register(&mut r);
        crate::ops::view::register(&mut r);
        r
    }
}

/// Helper for ops with `<name>.<interp>` form. Parses out the interpretation
/// suffix and hands `(head, interp)` to the factory closure.
pub fn parse_interp(s: &str) -> Option<crate::ir::shape::Interp> {
    use crate::ir::shape::Interp;
    Some(match s {
        "i8"  => Interp::I8,  "u8"  => Interp::U8,
        "i16" => Interp::I16, "u16" => Interp::U16,
        "i32" => Interp::I32, "u32" => Interp::U32, "f32" => Interp::F32,
        "i64" => Interp::I64, "u64" => Interp::U64, "f64" => Interp::F64,
        _ => return None,
    })
}

/// Split a token on its last `.` into `(head, suffix)`. Returns `None` if
/// there's no `.`.
pub fn split_suffix(t: &str) -> Option<(&str, &str)> {
    t.rfind('.').map(|idx| (&t[..idx], &t[idx + 1..]))
}
