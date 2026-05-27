//! Parser-time inference passes that mutate the IR after construction.
//!
//! Currently: last-use Ref inference. For each binding introduced by a
//! `Let`, find the last `Ref` to it (recursively through nested bodies)
//! and mark that Ref as `take: true`. This makes bare `name` references
//! at their last position equivalent to explicit `name>`, getting
//! Arc-1 buffer reuse for free.
//!
//! ## Safety: when can we mark a Ref as take?
//!
//! A take is correct only if the binding is referenced *exactly once
//! more* — at this Ref. If a later op also uses the binding, taking
//! here would leave the slot drained for the later use.
//!
//! Every body now runs exactly once: `Let` bodies are sequential, and the
//! only other body-bearing ops (`each`/`match`/`cleave`/`repeat`) have all
//! been retired. So last-use marking is always "find the rightmost
//! reference" — no multi-execution loop body to worry about. (If a future
//! loop construct returns, references inside its body run multiple times,
//! and this pass must again avoid marking Refs to outer bindings as take
//! inside such a body.)

use std::any::Any;

use crate::ir::typecheck::Op;
use crate::ops::letbind::{Let, Ref};

/// Mark the last reference to each name bound by the just-closed `Let`
/// (whose bound positions are `start..start + n_names`) as `take: true`.
///
/// `body` is the Let's body — the inference walks it (recursively) to
/// find the rightmost Ref targeting each bound position.
///
/// Refs to outer bindings (idx < start) are not touched.
pub fn mark_last_use_in_body(body: &mut Vec<Box<dyn Op>>, start: usize, n_names: usize) {
    for i in 0..n_names {
        let target_idx = start + i;
        mark_last_ref_for_idx(body, target_idx);
    }
}

/// Find the rightmost `Ref { idx: target_idx }` in `body` (top-level
/// scan; recurses into single-execution bodies for last-position
/// computation but only marks if the rightmost is a direct Ref).
///
/// Returns true if it found a marking site and marked it.
fn mark_last_ref_for_idx(body: &mut Vec<Box<dyn Op>>, target_idx: usize) -> bool {
    // Scan body right-to-left. The first op (rightmost) that touches
    // `target_idx` is the "last use." If it's a direct Ref, mark it.
    // If it's any other op (body-bearing op containing a Ref to this
    // idx in a nested body), we can't safely mark — bail.
    for pos in (0..body.len()).rev() {
        // Try as direct Ref first.
        let op: &mut dyn Op = body[pos].as_mut();
        let any_mut: &mut dyn Any = op;
        if let Some(r) = any_mut.downcast_mut::<Ref>() {
            if r.idx == target_idx {
                if !r.take {
                    r.take = true;
                }
                return true;
            }
            // A Ref to a different idx — keep scanning.
            continue;
        }
        // Not a Ref. Could be a body-bearing op that uses the target
        // transitively. If so, we can't safely mark (it might be inside
        // a loop body, or we'd be inside-mutating which is a hidden
        // hazard). Bail.
        if op_uses_idx(body[pos].as_ref(), target_idx) {
            return false;
        }
    }
    false
}

/// Recursively check whether `op` (or any of its sub-bodies) contains a
/// Ref to `target_idx`.
fn op_uses_idx(op: &dyn Op, target_idx: usize) -> bool {
    let any_ref: &dyn Any = op;
    if let Some(r) = any_ref.downcast_ref::<Ref>() {
        return r.idx == target_idx;
    }
    // Walk sub-bodies.
    for body in op_sub_bodies(op) {
        for child in body {
            if op_uses_idx(child.as_ref(), target_idx) {
                return true;
            }
        }
    }
    false
}

/// For body-bearing ops, return a list of sub-program slices to walk.
/// After `each`/`match`/`cleave`/`repeat` were retired, `Let` is the only
/// body-bearing op left (and it's boiled to edges during lowering).
fn op_sub_bodies<'a>(op: &'a dyn Op) -> Vec<&'a [Box<dyn Op>]> {
    let any_ref: &dyn Any = op;
    if let Some(l) = any_ref.downcast_ref::<Let>() {
        return vec![&l.body];
    }
    Vec::new()
}
