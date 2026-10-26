//! Optimizer schedules. Individual rewrites live in `passes`.

use crate::graph::Graph;
use crate::NumOp;
pub(crate) mod passes;
pub use passes::{cancel_isos, cse, dce, fuse_maps, immediates, peephole};

/// Normalize structural wiring without an input shape. Preserve fusion before CSE.
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    let graph = passes::cleanup(g);
    let graph = passes::fuse_maps(&graph);
    let graph = passes::cleanup(&graph);
    let graph = passes::cse(&graph);
    passes::cleanup(&graph)
}
