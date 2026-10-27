//! Optimizer schedules. Individual rewrites live in `passes`.

use crate::graph::Graph;
use crate::NumOp;
pub(crate) mod passes;
pub use passes::{cancel_isos, cse, dce, fuse_maps, immediates, peephole};

/// Normalize structural wiring without an input shape. Each pass runs once.
pub fn optimize(g: &Graph<NumOp>) -> Graph<NumOp> {
    normalize(g.clone())
}

fn normalize(g: Graph<NumOp>) -> Graph<NumOp> {
    // Fusion exposes tuple wiring. Fold scalar producers before choosing
    // immediate kernels, then share surviving work and remove dead producers.
    let graph = passes::fuse_maps(g);
    let graph = passes::peephole(graph);
    let graph = passes::cancel_isos(graph);
    let graph = passes::fold_scalar(graph);
    let graph = passes::immediates(graph);
    let graph = passes::cse(graph);
    passes::dce(graph)
}

/// A normalized graph and the input type it was checked at.
/// Construct this with [`compile`]; the graph and type cannot be changed separately.
pub struct Compiled {
    graph: Graph<NumOp>,
    input: crate::Shape,
}

impl Compiled {
    pub fn graph(&self) -> &Graph<NumOp> { &self.graph }
    pub fn input(&self) -> &crate::Shape { &self.input }
}

/// Compile at `input`: check the whole graph, strip the patterns typing proved,
/// including in closed bodies, then run the usual normalization once.
pub fn compile(graph: Graph<NumOp>, input: &crate::Shape) -> Result<Compiled, String> {
    crate::graph::node_shapes(&graph, input)?;
    Ok(Compiled { graph: normalize(passes::strip_patterns(graph)), input: input.clone() })
}
