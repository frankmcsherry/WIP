//! Optimizer schedules. Individual rewrites live in `passes`.

use crate::graph::Graph;
use crate::NumOp;
pub(crate) mod passes;
pub use passes::{cancel_isos, cse, dce, fuse_maps, immediates, peephole};

/// Normalize structural wiring without an input shape. Each pass runs once.
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    // Fusion exposes tuple wiring. Fold scalar producers before choosing
    // immediate kernels, then share surviving work and remove dead producers.
    let graph = passes::fuse_maps(g.clone());
    let graph = passes::peephole(graph);
    let graph = passes::cancel_isos(graph);
    let graph = passes::fold_scalar(graph);
    let graph = passes::immediates(graph);
    let graph = passes::cse(graph);
    passes::dce(graph)
}
