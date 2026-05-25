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
//! For sequential ops (Let body, top-level program) this is just "find
//! the rightmost reference."
//!
//! For loop-body ops (`Each`, `Repeat` with N>1), references
//! inside the body are executed *multiple times* — once per iteration.
//! Marking a Ref inside a loop body as take would consume the binding
//! on the first iteration, leaving the second iteration with a drained
//! slot. **The inference must not mark Refs to outer bindings as take
//! when those Refs sit inside a loop body.**
//!
//! For body-bearing ops that run their body exactly once (`Let`, `Match`
//! arm, `Cleave` path, `Repeat` with N=1), references inside the body
//! are safe to mark. The current implementation conservatively treats
//! all body-bearing ops as "single-execution" — which is wrong for
//! `Each`/`Repeat`. The mitigation: the inference only marks
//! Refs whose idx falls within the *current* Let's bound range, and the
//! inference is run after each Let close. So Refs inside an outer
//! loop's body that target the loop's own local bindings are marked
//! (safe — local bindings are fresh per iteration), but Refs targeting
//! outer bindings are not touched by inner-Let inference.

use std::any::Any;

use crate::ir::typecheck::Op;
use crate::ops::letbind::{Let, Ref};
use crate::ops::list::{Each, Repeat};
use crate::ops::combinators::{Match, Cleave};

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
/// Single-body ops return a Vec with one slice; multi-body ops (Match
/// arms, Cleave paths) return one per arm/path.
fn op_sub_bodies<'a>(op: &'a dyn Op) -> Vec<&'a [Box<dyn Op>]> {
    let any_ref: &dyn Any = op;
    if let Some(l) = any_ref.downcast_ref::<Let>() {
        return vec![&l.body];
    }
    if let Some(e) = any_ref.downcast_ref::<Each>() {
        return vec![&e.body];
    }
    if let Some(r) = any_ref.downcast_ref::<Repeat>() {
        return vec![&r.body];
    }
    if let Some(m) = any_ref.downcast_ref::<Match>() {
        return m.arms.iter().map(|a| a.as_slice()).collect();
    }
    if let Some(c) = any_ref.downcast_ref::<Cleave>() {
        return c.paths.iter().map(|p| p.as_slice()).collect();
    }
    Vec::new()
}
