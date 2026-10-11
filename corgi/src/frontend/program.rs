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
use crate::optimize::{dce, immediates};
use crate::pool::{self, Pool};
use crate::shape::Shape;
use crate::value::Value;
use std::sync::{Mutex, TryLockError};

pub struct Program {
    /// the graph that runs and types: the one given, with constants made immediates.
    graph: Graph<NumOp>,
    /// leaf buffers freed by this program's earlier runs, for its later runs to write into. Each run
    /// installs it on its thread (see `crate::pool`), so it carries from run to run and so from block
    /// to block; it holds about as many buffers as one run takes.
    pool: Mutex<Pool>,
}

impl Program {
    /// compile an `ml` source string into a runnable program (a parse — use [`Program::shape`] to
    /// type-check it against an input shape).
    pub fn compile_ml(src: &str) -> Result<Program, String> {
        Ok(Program::from_graph(parse_ml(src)?))
    }

    /// wrap an already-built graph — from the `Builder`, the optimizer, or a host's own lowering.
    /// What runs is the graph with constant operands made immediates and unreachable nodes dropped
    /// (both exact).
    pub fn from_graph(graph: Graph<NumOp>) -> Program {
        Program { graph: dce(&immediates(&graph)), pool: Mutex::new(Pool::default()) }
    }

    /// the output shape for a given input shape.
    pub fn shape(&self, input: &Shape) -> Result<Shape, String> {
        shape_of(&self.graph, input)
    }

    /// run the program to its value.
    ///
    /// Leaf buffers freed during the run go to the program's pool, and ops take their outputs from
    /// it, so a program run repeatedly (a block at a time, say) reuses its buffers rather than
    /// allocating fresh ones. Two threads running one program at once: the second runs without it.
    pub fn run(&self, input: Value) -> Value {
        let mut pool = match self.pool.try_lock() {
            Ok(pool) => pool,
            Err(TryLockError::Poisoned(pool)) => pool.into_inner(), // a run panicked; the pool is fine
            Err(TryLockError::WouldBlock) => return eval_graph(&self.graph, input),
        };
        pool::run_with(&mut pool, || eval_graph(&self.graph, input))
    }
}
