//! `Program` — the convenience layer over the core engine. It bundles `parse_ml` + `eval_graph` +
//! `shape_of` so the tour, the tests, and a future CLI share one compile-and-run path.
//!
//! It lives strictly ABOVE the core: `Builder`/`Graph`/`eval_graph`/`shape_of` are the integration
//! API and never depend on this. An integrator (e.g. DDIR) lowers its own IR to a `Graph` and calls
//! `eval_graph` directly, ignoring `Program` entirely — so it's opt-in by simply not being used.
//! `Program` is not ML-specific: `compile_ml` is one constructor; `from_graph` wraps any `Graph`.

use super::parse_ml;
use crate::effect::{effect_eval_graph, is_total, EffectValues};
use crate::graph::{shape_of, Graph};
use crate::ops::NumOp;
use crate::shape::Shape;
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

    /// run a TOTAL program to its bare value — a convenience over [`Program::run_effect`] for programs
    /// with no un-`TRY`'d `FailOp` (the output is `Pure`). A partial program is an `Err` here: there is
    /// no single bare `Value` for a `Fail` column, so use [`Program::run_effect`] (or add a `TRY`).
    /// (The old static length/range gates are gone — partiality is carried by the effect layer, not
    /// rejected up front; their proofs would return only as opt-in optimizer demotion, not run-path.)
    pub fn run(&self, input: Value) -> Result<Value, String> {
        match self.run_effect(input) {
            EffectValues::Pure(v) => Ok(v),
            EffectValues::Fail(_) => {
                Err("partial program (an un-TRY'd FailOp); use run_effect or add a TRY".into())
            }
        }
    }

    /// run over the EFFECT layer: the input is pure, the output is `Pure` or `Fail` (a per-row
    /// Ok/Error column). A `Fail` output is the honest result of a partial program, not an error —
    /// totality is the separate, syntactic [`Program::is_total`] query, and `TRY` (`eval_try`) is how
    /// a program turns a `Fail` back into matchable `Pure` data. This is the total successor to `run`:
    /// it needs no static length/range gate, because partiality is carried, not rejected.
    pub fn run_effect(&self, input: Value) -> EffectValues {
        effect_eval_graph(&self.graph, EffectValues::Pure(input))
    }

    /// is this program total — does its output column type `Pure` (every `FailOp` discharged by a
    /// `TRY`)? The syntactic totality query, read off the op tags (the effect-typer).
    pub fn is_total(&self) -> bool {
        is_total(&self.graph)
    }
}
