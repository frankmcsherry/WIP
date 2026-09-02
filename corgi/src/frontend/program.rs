//! `Program` — the convenience layer over the core engine. It bundles `parse_ml` + `eval_graph` +
//! `shape_of` so the tour, the tests, and a future CLI share one compile-and-run path.
//!
//! It lives strictly ABOVE the core: `Builder`/`Graph`/`eval_graph`/`shape_of` are the integration
//! API and never depend on this. An integrator (e.g. DDIR) lowers its own IR to a `Graph` and calls
//! `eval_graph` directly, ignoring `Program` entirely — so it's opt-in by simply not being used.
//! `Program` is not ML-specific: `compile_ml` is one constructor; `from_graph` wraps any `Graph`.

use super::parse_ml;
use crate::effect::{is_total, lower_effects};
use crate::graph::{eval_graph, shape_of, Graph};
use crate::ops::NumOp;
use crate::shape::Shape;
use crate::value::Value;

pub struct Program {
    graph: Graph<NumOp>,
    /// the graph with its effects lowered into the pure vocabulary — what actually runs and types.
    lowered: Graph<NumOp>,
}

impl Program {
    /// compile an `ml` source string into a runnable program (a parse — use [`Program::check`] for
    /// structural validation and [`Program::shape`] to type-check it against an input shape).
    pub fn compile_ml(src: &str) -> Result<Program, String> {
        Ok(Program::from_graph(parse_ml(src)?))
    }

    /// wrap an already-built graph — from the `Builder`, the optimizer, or a host's own lowering.
    pub fn from_graph(graph: Graph<NumOp>) -> Program {
        let lowered = lower_effects(&graph);
        Program { graph, lowered }
    }

    /// the underlying graph, for inspection or optimization.
    pub fn graph(&self) -> &Graph<NumOp> {
        &self.graph
    }

    /// structural well-formedness (panics on a malformed graph — a parser/builder bug, not a user error).
    pub fn check(&self) {
        self.graph.check();
    }

    /// the output shape for a given input shape — the typer, over the lowered program: a fallible
    /// stage's downstream types as running on its Ok lane, and an un-`try`'d output as `Sum{T | Unit}`.
    pub fn shape(&self, input: &Shape) -> Result<Shape, String> {
        shape_of(&self.lowered, input)
    }

    /// the lowered graph — the pure-vocabulary program that [`Program::run_partial`] evaluates.
    pub fn lowered(&self) -> &Graph<NumOp> {
        &self.lowered
    }

    /// run a TOTAL program to its value. A partial program (an un-`try`'d fallible stage) is an `Err`
    /// here: its output is a `Fail` column, which [`Program::run_partial`] returns as a `Sum{T | Unit}`.
    pub fn run(&self, input: Value) -> Result<Value, String> {
        if !self.is_total() {
            return Err("partial program (an un-try'd fallible stage); use run_partial or add a try".into());
        }
        Ok(self.run_partial(input))
    }

    /// run any program: a total program yields its value; a partial one yields its output wrapped as
    /// `Fail<T> = Sum{ T | Unit }` (Ok rows at lane 0, errored rows counted at lane 1) — the same value
    /// a trailing `try` would reveal. Totality is the separate, syntactic [`Program::is_total`].
    pub fn run_partial(&self, input: Value) -> Value {
        eval_graph(&self.lowered, input)
    }

    /// is this program total — does every fallible stage get taken up by a `try` before the output?
    /// Syntactic, read off the op tags.
    pub fn is_total(&self) -> bool {
        is_total(&self.graph)
    }
}
