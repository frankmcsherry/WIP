//! Stage A — lower a parsed op stream into the term-graph IR.
//!
//! Pure 1:1 lowering: every op becomes exactly one term. No CSE, no
//! elision, no rewriting — those are `optimize`-stage passes. `Let`/`Ref`
//! and the body-bearing ops stay as terms (their bodies run via legacy
//! eval inside `op.run`); routing/binding resolution into edges is a
//! planned later refinement.

use crate::pipeline::graph::{Graph, Term, OutRef};
use crate::pipeline::sysop::promote;
use crate::ir::typecheck::{Op, TypeStack, TypeEnv};
use crate::ir::shape::Shape;

/// Lower a parsed op stream into a term graph. Returns the graph and
/// per-term output shapes (side-table; valid for the freshly-built graph,
/// stale after any reindexing pass — recompute if a later stage needs them).
pub fn build(prog: Vec<Box<dyn Op>>) -> Result<(Graph, Vec<Vec<Shape>>), String> {
    let mut g = Graph::default();
    let mut bstack: Vec<OutRef> = Vec::new();
    let mut tstack: TypeStack = Vec::new();
    let mut tenv: TypeEnv = Vec::new();
    let mut shapes: Vec<Vec<Shape>> = Vec::new();
    build_in(prog, &mut g, &mut bstack, &mut tstack, &mut tenv, &mut shapes)?;
    g.roots = bstack;
    Ok((g, shapes))
}

fn build_in(
    prog: Vec<Box<dyn Op>>,
    g: &mut Graph,
    bstack: &mut Vec<OutRef>,
    tstack: &mut TypeStack,
    tenv: &mut TypeEnv,
    shapes: &mut Vec<Vec<Shape>>,
) -> Result<(), String> {
    for op in prog {
        // Stack-routing ops (dup/drop/swap/over/rot/pick/roll) carry no
        // semantic content — they only rearrange the stack. Resolve them
        // here into direct edges so the system graph is routing-free by
        // construction. (Routing ops *inside* opaque bodies stay there;
        // legacy eval handles them via op.run.)
        if let Some(map) = op.routing_map() {
            resolve_routing(&*op, &map, bstack, tstack, tenv)?;
            continue;
        }
        emit_term(op, g, bstack, tstack, tenv, shapes)?;
    }
    Ok(())
}

/// Apply a routing op's `map` to the build stack: pop its inputs, push the
/// aliased OutRefs. The op produces no term. `tc` is run to keep the
/// type-stack in lockstep (routing ops still shift shapes around).
fn resolve_routing(
    op: &dyn Op,
    map: &[usize],
    bstack: &mut Vec<OutRef>,
    tstack: &mut TypeStack,
    tenv: &mut TypeEnv,
) -> Result<(), String> {
    let n_in = op.arity()
        .ok_or_else(|| format!("graph build: routing op {} lacks static arity", op.name()))?
        .0;
    if bstack.len() < n_in {
        return Err(format!(
            "graph build: routing op {} needs {} inputs, has {}",
            op.name(), n_in, bstack.len()
        ));
    }
    op.tc(tstack, tenv).map_err(|e| format!("graph build: {}: {}", op.name(), e))?;
    let inputs: Vec<OutRef> = bstack.split_off(bstack.len() - n_in);
    for &in_i in map {
        bstack.push(inputs[in_i]);
    }
    Ok(())
}

/// Emit one term for one op. Always allocates — lowering is 1:1.
fn emit_term(
    op: Box<dyn Op>,
    g: &mut Graph,
    bstack: &mut Vec<OutRef>,
    tstack: &mut TypeStack,
    tenv: &mut TypeEnv,
    shapes: &mut Vec<Vec<Shape>>,
) -> Result<(), String> {
    // Run tc to discover the output shapes and (for dynamic-arity ops) the
    // input/output counts. Snapshot type-stack height before tc.
    let pre = tstack.len();
    op.tc(tstack, tenv).map_err(|e| format!("graph build: {}: {}", op.name(), e))?;
    let (n_in, n_out): (usize, usize) = match op.arity() {
        Some((i, o)) => (i, o),
        None => derive_dynamic_arity(op.as_ref(), pre, tstack)?,
    };
    if bstack.len() < n_in {
        return Err(format!(
            "graph build: op {} needs {} stack inputs, has {}",
            op.name(), n_in, bstack.len()
        ));
    }

    let children: Vec<OutRef> = bstack.split_off(bstack.len() - n_in);
    let out_shapes: Vec<Shape> = tstack[tstack.len() - n_out..].to_vec();

    let id = g.terms.len();
    // Promote into the system operator vocabulary: a first-class variant
    // where the system models the op, else `Foreign`.
    g.terms.push(Term { op: promote(op), children, n_outputs: n_out });
    shapes.push(out_shapes);
    for i in 0..n_out {
        bstack.push(OutRef { term: id, idx: i });
    }
    Ok(())
}

/// Derive (n_in, n_out) for ops whose `arity()` returned None. Today:
/// `split` (output count = 1 + Sum lane count), and `let`/`repeat` (bodies
/// run on the outer stack at depths not visible from tc; treated
/// conservatively as consuming and re-emitting the whole current stack).
fn derive_dynamic_arity(
    op: &dyn Op,
    pre: usize,
    tstack: &TypeStack,
) -> Result<(usize, usize), String> {
    let any_ref: &dyn std::any::Any = op as &dyn std::any::Any;
    if any_ref.downcast_ref::<crate::ops::combinators::Split>().is_some() {
        let post = tstack.len();
        let n_out = (post + 1).saturating_sub(pre);  // popped 1, pushed n_out
        return Ok((1, n_out));
    }
    if any_ref.downcast_ref::<crate::ops::list::Repeat>().is_some() {
        return Ok((pre, pre));
    }
    if any_ref.downcast_ref::<crate::ops::letbind::Let>().is_some() {
        let post = tstack.len();
        return Ok((pre, post));
    }
    Err(format!(
        "graph build: op {} has unknown arity() and no dynamic-arity handler",
        op.name()
    ))
}
