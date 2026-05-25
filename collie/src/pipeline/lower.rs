//! Stage A — lower a parsed op stream into the term-graph IR.
//!
//! Mostly 1:1 lowering: each op becomes one term. Two classes resolve
//! into edges instead, so the graph the optimizer sees is as flat as
//! possible:
//!
//! - **Routing** (`dup`/`swap`/`pick`/…) — alias OutRefs, no term.
//! - **Binding** (`Let`/`Ref`) — *boiled away* when the binding stays in
//!   the graph world: a build-time `env: Vec<OutRef>` mirrors the runtime
//!   env, `Let` pushes its inputs' OutRefs and recurses into its body,
//!   and `Ref` resolves to the bound OutRef. No `Let`/`Ref` term, and the
//!   graph's own take-on-last-use (see `execute.rs`) subsumes `Ref`'s
//!   `take` flag.
//!
//! A `Let` can only be boiled if its body holds no opaque body-bearing op
//! (`each`/`repeat`/`match`/`cleave`): those run via legacy eval and read
//! the *runtime* env at parse-time indices, so any binding they might
//! capture must stay materialized there. Such a `Let` is kept Foreign
//! (today's behavior) and its whole body runs via legacy eval — we never
//! recurse into it, so boiled and kept regions never share an env and no
//! re-indexing is needed.

use crate::pipeline::graph::{Graph, Term, OutRef};
use crate::pipeline::sysop::promote;
use crate::ir::typecheck::{Op, TypeStack, TypeEnv};
use crate::ir::shape::Shape;
use crate::ops::letbind::{Let, Ref};

/// Lower a parsed op stream into a term graph. Returns the graph and
/// per-term output shapes (side-table; valid for the freshly-built graph,
/// stale after any reindexing pass — recompute if a later stage needs them).
pub fn build(prog: Vec<Box<dyn Op>>) -> Result<(Graph, Vec<Vec<Shape>>), String> {
    let mut g = Graph::default();
    let mut bstack: Vec<OutRef> = Vec::new();
    let mut tstack: TypeStack = Vec::new();
    let mut tenv: TypeEnv = Vec::new();
    let mut shapes: Vec<Vec<Shape>> = Vec::new();
    // Build-time env of bound producers, mirroring the runtime `Let` env.
    // Empty at top level — `Ref`s only ever appear inside a `Let` body.
    let mut env: Vec<OutRef> = Vec::new();
    build_in(prog, &mut g, &mut bstack, &mut tstack, &mut tenv, &mut shapes, &mut env)?;
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
    env: &mut Vec<OutRef>,
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
        // Binding: boil into edges where the body is graph-visible.
        // Classify on a borrow, then consume `op` accordingly.
        enum Kind { BoilLet, RefIdx(usize), Other }
        let kind = {
            let any: &dyn std::any::Any = op.as_ref();
            if let Some(l) = any.downcast_ref::<Let>() {
                if body_has_opaque(&l.body) { Kind::Other } else { Kind::BoilLet }
            } else if let Some(r) = any.downcast_ref::<Ref>() {
                Kind::RefIdx(r.idx)
            } else {
                Kind::Other
            }
        };
        match kind {
            Kind::BoilLet => {
                let any_box: Box<dyn std::any::Any> = op;
                let l = *any_box.downcast::<Let>().expect("classified as Let");
                boil_let(l, g, bstack, tstack, tenv, shapes, env)?;
            }
            Kind::RefIdx(idx) => {
                // Resolve to the bound producer's OutRef. The graph's
                // take-on-last-use makes `Ref`'s `take` flag irrelevant.
                let slot = *env.get(idx).ok_or_else(|| {
                    format!("graph build: ref {} out of env (len {})", idx, env.len())
                })?;
                op.tc(tstack, tenv).map_err(|e| format!("graph build: ref: {}", e))?;
                bstack.push(slot);
            }
            Kind::Other => emit_term(op, g, bstack, tstack, tenv, shapes)?,
        }
    }
    Ok(())
}

/// Boil a `Let` into the graph: bind its inputs' OutRefs into `env`
/// (mirroring `Let::run`/`tc`'s env push), lower its body, then unwind.
/// Produces no term.
fn boil_let(
    l: Let,
    g: &mut Graph,
    bstack: &mut Vec<OutRef>,
    tstack: &mut TypeStack,
    tenv: &mut TypeEnv,
    shapes: &mut Vec<Vec<Shape>>,
    env: &mut Vec<OutRef>,
) -> Result<(), String> {
    let n = l.names.len();
    if bstack.len() < n || tstack.len() < n {
        return Err(format!("graph build: let needs {} inputs, has {}", n, bstack.len()));
    }
    // Move the bound values off both stacks into the envs, in stack order
    // (deepest..top) — exactly what `Let::run`/`tc` do.
    let bound_refs = bstack.split_off(bstack.len() - n);
    let bound_shapes = tstack.split_off(tstack.len() - n);
    let env0 = env.len();
    let tenv0 = tenv.len();
    env.extend(bound_refs);
    tenv.extend(bound_shapes);
    build_in(l.body, g, bstack, tstack, tenv, shapes, env)?;
    env.truncate(env0);
    tenv.truncate(tenv0);
    Ok(())
}

/// Does this op stream contain an opaque body-bearing op
/// (`each`/`repeat`/`match`/`cleave`) anywhere, including nested `Let`
/// bodies? Such ops run via legacy eval and read the runtime env, so a
/// `Let` whose body holds one cannot be boiled.
fn body_has_opaque(ops: &[Box<dyn Op>]) -> bool {
    use crate::ops::list::{Each, Repeat};
    use crate::ops::combinators::{Match, Cleave};
    for op in ops {
        let any: &dyn std::any::Any = op.as_ref();
        if any.is::<Each>() || any.is::<Repeat>() || any.is::<Match>() || any.is::<Cleave>() {
            return true;
        }
        if let Some(l) = any.downcast_ref::<Let>() {
            if body_has_opaque(&l.body) { return true; }
        }
    }
    false
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
