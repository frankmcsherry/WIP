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

    /// run the program on an input, consuming it — after shape-checking the graph and proving its two
    /// static gates: Class-A bounds (`check_lengths`: `Zip`/`Filter` agreement) and Class-B ranges
    /// (`check_ranges`: `branch` tag-in-range). An ill-typed or gate-failing program is a located `Err`
    /// here rather than a panic deep inside an op. NOTE the scope: these gates cover `Zip`/`Filter`/
    /// `branch` only. An UNCHECKED `*_uns` op (`get_uns`/`gather_uns`/`slices_uns`, incl. `head_uns`
    /// which lowers to `get_uns 0`) has a data-dependent precondition no gate proves and can still panic
    /// in `eval` — for the full no-panic guarantee a caller must additionally require `check_total`
    /// (`run` deliberately does not, so the corpus can exercise the `_uns` tier).
    pub fn run(&self, input: Value) -> Result<Value, String> {
        let shape = shape_of_value(&input);
        shape_of(&self.graph, &shape)?;
        crate::lengths::check_lengths(&self.graph, &shape)?;
        crate::ranges::check_ranges(&self.graph, &shape)?;
        Ok(eval_graph(&self.graph, input))
    }
}
