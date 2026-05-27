//! Stage D — execute a term graph.
//!
//! Walks terms in topological order, runs each op against a sub-stack
//! built from its children's outputs, and gathers the roots. Uses
//! take-on-last-use (the final reader of an output moves it; earlier
//! readers clone), preserving the Arc-1 reuse the legacy stack eval gets.

use crate::pipeline::graph::Graph;
use crate::ir::stack::Stack;
use crate::ir::value::Value;

/// Per-output consumer counts: `counts[term][out]` is how many edges
/// reference that output (children across all terms, plus roots).
pub fn use_counts(g: &Graph) -> Vec<Vec<usize>> {
    let mut counts: Vec<Vec<usize>> = g.terms.iter()
        .map(|t| vec![0usize; t.n_outputs])
        .collect();
    for term in &g.terms {
        for ch in &term.children {
            counts[ch.term][ch.idx] += 1;
        }
    }
    for r in &g.roots {
        counts[r.term][r.idx] += 1;
    }
    counts
}

/// Evaluate a graph; return the materialized roots (the final stack,
/// bottom-to-top). The engine is self-contained over the IR: no op reads
/// the bound-value `env` during graph eval (binding is boiled to edges in
/// lowering, and no body-bearing op survives), so a throwaway env is passed
/// to satisfy the `PrimOp::run` signature and never populated.
pub fn eval_graph(g: &Graph) -> Result<Vec<Value>, String> {
    let mut env: Vec<Value> = Vec::new();
    let mut counts = use_counts(g);
    let mut outs: Vec<Vec<Value>> = Vec::with_capacity(g.terms.len());
    for term in &g.terms {
        let mut sub: Stack = Vec::with_capacity(term.children.len());
        for ch in &term.children {
            let remaining = &mut counts[ch.term][ch.idx];
            *remaining = remaining.saturating_sub(1);
            let slot = &mut outs[ch.term][ch.idx];
            if *remaining == 0 {
                sub.push(std::mem::take(slot));
            } else {
                sub.push(slot.clone());
            }
        }
        term.op.run(&mut sub, &mut env)?;
        if sub.len() != term.n_outputs {
            return Err(format!(
                "graph eval: op {} produced {} outputs, declared {}",
                term.op.name(), sub.len(), term.n_outputs
            ));
        }
        outs.push(sub);
    }
    let mut result: Vec<Value> = Vec::with_capacity(g.roots.len());
    for r in &g.roots {
        let remaining = &mut counts[r.term][r.idx];
        *remaining = remaining.saturating_sub(1);
        let slot = &mut outs[r.term][r.idx];
        if *remaining == 0 {
            result.push(std::mem::take(slot));
        } else {
            result.push(slot.clone());
        }
    }
    Ok(result)
}
