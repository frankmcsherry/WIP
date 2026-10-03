//! `Program` — the convenience layer over the core engine. It bundles `parse_ml` + `eval_graph` +
//! `shape_of` so the tour, the tests, and a future CLI share one compile-and-run path.
//!
//! It lives strictly ABOVE the core: `Builder`/`Graph`/`eval_graph`/`shape_of` are the integration
//! API and never depend on this. An integrator (e.g. DDIR) lowers its own IR to a `Graph` and calls
//! `eval_graph` directly, ignoring `Program` entirely — so it's opt-in by simply not being used.
//! `Program` is not ML-specific: `compile_ml` is one constructor; `from_graph` wraps any `Graph`.

use super::parse_ml;
use crate::effect::lower_effects;
use crate::graph::{eval_graph, shape_of, Graph};
use crate::ops::NumOp;
use crate::optimize::{dce, immediates};
use crate::shape::Shape;
use crate::value::Value;

pub struct Program {
    /// the graph with its effects lowered into the pure vocabulary — what actually runs and types.
    lowered: Graph<NumOp>,
}

impl Program {
    /// compile an `ml` source string into a runnable program (a parse — use [`Program::shape`] to
    /// type-check it against an input shape).
    pub fn compile_ml(src: &str) -> Result<Program, String> {
        Ok(Program::from_graph(parse_ml(src)?))
    }

    /// wrap an already-built graph — from the `Builder`, the optimizer, or a host's own lowering.
    /// What runs is the graph with constant operands made immediates and unreachable nodes dropped
    /// (both exact), then effect-lowered.
    pub fn from_graph(graph: Graph<NumOp>) -> Program {
        Program { lowered: lower_effects(&dce(&immediates(&graph))) }
    }

    /// the output shape for a given input shape — the typer, over the lowered program: a fallible
    /// stage's downstream types as running on its Ok lane, and an un-`try`'d output as `Sum{T | Unit}`.
    pub fn shape(&self, input: &Shape) -> Result<Shape, String> {
        shape_of(&self.lowered, input)
    }

    /// run the program to its value. A failure no `try` takes up reaches the output, which is then
    /// `Fail<T> = Sum{ T | Unit }` (Ok rows at lane 0, errored rows counted at lane 1): the same value
    /// a trailing `try` would reveal, and the shape [`Program::shape`] gives.
    pub fn run(&self, input: Value) -> Value {
        eval_graph(&self.lowered, input)
    }
}
