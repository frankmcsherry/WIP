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
//! Every `Let` boils: there are no longer any opaque body-bearing ops that
//! would force one to stay Foreign (`each`/`match`/`cleave` desugar, `repeat`
//! was removed). So no `Let`/`Ref` term ever reaches the graph, and the
//! engine never runs a sub-program via legacy eval.
//!
//! Note: binding-boiling lives here in lowering *by necessity*, not as an
//! optimization — `:name` binds the rest of its block, so an un-boiled
//! top-level `Let` would swallow the whole remaining program into one
//! legacy-eval blob. Boiling is what turns the concatenative front end's
//! scoped binding into a dataflow graph at all; it is the concatenative
//! lowering, not a removable `Graph → Graph` pass. See dev/LAYERING.md.

use crate::pipeline::graph::{Graph, Term, OutRef};
use crate::pipeline::sysop::{promote, SystemOp};
use crate::ir::typecheck::{Op, TypeStack, TypeEnv};
use crate::ir::shape::{Shape, shape_of};
use crate::ir::value::Value;
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

/// Lower an *open* program — one that expects `seeds` already on the stack —
/// by prepending a `Const` source term per seed. The result is a closed
/// graph the engine runs directly (each `Const` re-produces its value per
/// eval). Used where the op-stream interpreter's "push inputs, then run"
/// shape is wanted on the graph engine (e.g. the bench harness).
pub fn build_seeded(prog: Vec<Box<dyn Op>>, seeds: Vec<Value>) -> Result<(Graph, Vec<Vec<Shape>>), String> {
    let mut g = Graph::default();
    let mut bstack: Vec<OutRef> = Vec::new();
    let mut tstack: TypeStack = Vec::new();
    let mut tenv: TypeEnv = Vec::new();
    let mut shapes: Vec<Vec<Shape>> = Vec::new();
    let mut env: Vec<OutRef> = Vec::new();
    for v in seeds {
        let sh = shape_of(&v);
        let id = g.terms.len();
        g.terms.push(Term { op: SystemOp::Const(v), children: vec![], n_outputs: 1 });
        shapes.push(vec![sh.clone()]);
        bstack.push(OutRef { term: id, idx: 0 });
        tstack.push(sh);
    }
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
        // Binding: always boiled into edges. (There are no longer any
        // opaque body-bearing ops that would force a `Let` to stay Foreign
        // — `each`/`match`/`cleave`/`repeat` are all gone — so every `Let`
        // boils and no `Let`/`Ref` term ever reaches the graph.)
        enum Kind { BoilLet, RefIdx(usize), Other }
        let kind = {
            let any: &dyn std::any::Any = op.as_ref();
            if any.is::<Let>() {
                Kind::BoilLet
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

/// Derive (n_in, n_out) for ops whose `arity()` returned None. Today only
/// `split` (output count = 1 + Sum lane count); `let`/`ref` are boiled, not
/// emitted as terms, and the body-bearing ops that needed this are gone.
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
    Err(format!(
        "graph build: op {} has unknown arity() and no dynamic-arity handler",
        op.name()
    ))
}
