//! `Program` — the convenience layer over the core engine. It bundles `parse_ml` + `eval_graph` +
//! `shape_of` so the tour, the tests, and a future CLI share one compile-and-run path.
//!
//! It lives strictly ABOVE the core: `Builder`/`Graph`/`eval_graph`/`shape_of` are the integration
//! API and never depend on this. An integrator (e.g. DDIR) lowers its own IR to a `Graph` and calls
//! `eval_graph` directly, ignoring `Program` entirely — so it's opt-in by simply not being used.
//! `Program` is not ML-specific: `compile_ml` is one constructor; `from_graph` wraps any `Graph`.

use super::parse_ml;
use crate::graph::{eval_graph, shape_of, Graph};
use crate::ops::NumOp;
use crate::optimize::optimize;
use crate::shape::Shape;
use crate::value::Value;

pub struct Program {
    /// The graph that runs and types, exactly as supplied to `from_graph`.
    graph: Graph<NumOp>,
}

impl Program {
    /// Parse and explicitly normalize ML into a runnable program. Use [`Program::shape`] to
    /// type-check it against an input shape.
    pub fn compile_ml(src: &str) -> Result<Program, String> {
        Ok(Program::from_graph(optimize(&parse_ml(src)?)))
    }

    /// wrap an already-built graph — from the `Builder`, the optimizer, or a host's own lowering.
    /// No rewriting occurs here. Call `optimize` explicitly when normalization is wanted.
    pub fn from_graph(graph: Graph<NumOp>) -> Program {
        Program { graph }
    }

    /// the output shape for a given input shape.
    pub fn shape(&self, input: &Shape) -> Result<Shape, String> {
        shape_of(&self.graph, input)
    }

    /// run the program to its value.
    pub fn run(&self, input: Value) -> Value {
        eval_graph(&self.graph, input)
    }
}
