//! The total-subset boundary. A program for which `check_total` returns `Ok` uses no UNCHECKED kernel
//! op — so the length + range gates (run by `Program::run`) make it run to a value for every input.
//! The unchecked tier is `index_uns` / `head_uns` / `slices_uns`: random access whose data-dependent
//! precondition (in-bounds / non-empty) no static pass proves. Reaching for one is a deliberate,
//! located opt-out — the `unsafe` of corgi — so `check_total` reports each site rather than rejecting:
//! an agent or codegen requires `Ok`; a human writing pointer-jumping opts out knowingly. The total
//! forms are `index` (gather's), `index 0` (head's), and the future `slices_try` (slices').

use crate::graph::{Graph, NodeKind, OpLike};
use crate::ops::{NumOp, Op};

/// `Ok(())` iff the graph (and every body sub-graph) is in the guaranteed-total subset; otherwise the
/// names of the unchecked-tier ops it uses, in node order.
pub fn check_total(g: &Graph<NumOp>) -> Result<(), Vec<&'static str>> {
    let mut sites = Vec::new();
    scan(g, &mut sites);
    if sites.is_empty() {
        Ok(())
    } else {
        Err(sites)
    }
}

fn scan(g: &Graph<NumOp>, sites: &mut Vec<&'static str>) {
    for node in &g.nodes {
        let NodeKind::Op(op) = &node.kind else { continue };
        match op {
            NumOp::Core(Op::Gather) => sites.push("index_uns"),
            NumOp::Core(Op::Head) => sites.push("head_uns"),
            NumOp::Core(Op::Slices) => sites.push("slices_uns"),
            _ => {}
        }
        for child in op.children() {
            scan(child, sites); // a kernel op inside a map/fold/scan body counts too
        }
    }
}
