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

/// Optimize with known input types and report the columns the caller must supply.
/// No boundary adapter is built. The graph accepts both the original input and the
/// validated `demand.to_shape(input)` layout, with the original output shape.
pub fn optimize_with_demand(g: &Graph<NumOp>, input: &crate::Shape) -> Result<(Graph<NumOp>, crate::Demand), String> {
    let output = crate::shape_of(g, input)?;
    let graph = optimize(g);
    let graph = passes::demand::prune_producers(&graph, input, &output)?;
    let graph = optimize(&graph);
    let (mut demand, _) = crate::demand::input_demand(&graph, input, crate::Demand::All)?;
    if crate::shape_of(&graph, &demand.to_shape(input)?).as_ref() != Ok(&output) {
        demand = crate::Demand::All;
    }
    Ok((graph, demand))
}
