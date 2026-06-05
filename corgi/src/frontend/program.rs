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
use crate::shape::{shape_of_value, Shape};
use crate::value::Value;

pub struct Program {
    graph: Graph<NumOp>,
}

impl Program {
    /// compile an `ml` source string into a runnable program (a parse — use [`Program::check`] for
    /// structural validation and [`Program::shape`] to type-check it against an input shape).
    pub fn compile_ml(src: &str) -> Result<Program, String> {
        Ok(Program { graph: parse_ml(src)? })
    }

    /// wrap an already-built graph — from the `Builder`, the optimizer, or a host's own lowering.
    pub fn from_graph(graph: Graph<NumOp>) -> Program {
        Program { graph }
    }

    /// the underlying graph, for inspection or optimization.
    pub fn graph(&self) -> &Graph<NumOp> {
        &self.graph
    }

    /// structural well-formedness (panics on a malformed graph — a parser/builder bug, not a user error).
    pub fn check(&self) {
        self.graph.check();
    }

    /// the output shape for a given input shape — the typer.
    pub fn shape(&self, input: &Shape) -> Result<Shape, String> {
        shape_of(&self.graph, input)
    }

    /// run the program on an input, consuming it — after shape-checking the graph against the input's
    /// shape, so an ill-typed program is a located `Err` here rather than a panic deep inside an op.
    /// This is the enforced surface boundary: nothing reaches `eval_graph` without passing the typer.
    pub fn run(&self, input: Value) -> Result<Value, String> {
        shape_of(&self.graph, &shape_of_value(&input))?;
        Ok(eval_graph(&self.graph, input))
    }
}
