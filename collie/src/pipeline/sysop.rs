//! The system operator vocabulary — what a graph [`Term`](super::graph::Term)
//! holds.
//!
//! This is the *operator* layer: abstract, introspectable, what the
//! optimizer pattern-matches on. Each variant carries the op's parameters
//! (interp, arity, op-kind). The *kernel* layer is the `Op`/`PrimOp` trait:
//! `as_kernel` reconstructs the kernel struct for a variant, and
//! `run`/`tc`/`name`/`arity` delegate through it. (Stage C will later lift
//! kernel choice out of this reconstruct-and-delegate.)
//!
//! `Foreign` is the escape hatch — ops the system doesn't model as a
//! first-class variant: the body-bearing ops (`each`/`reduce {body}`/
//! `repeat`/`match`/`cleave`), binding (`let`/`ref`), diagnostics
//! (`time`/`profile.*`/`show`), literals, and any front-end/FFI ops.
//! Routing ops (`dup`/`pick`/…) never reach here — they're resolved into
//! edges during lowering.

use crate::ir::stack::Stack;
use crate::ir::typecheck::{Op, TypeStack, TypeEnv};
use crate::ir::value::Value;
use crate::ir::shape::Interp;
use crate::ops::arith::{ArithOp, UnaryArithOp};
use crate::ops::cmp::CmpOp;

/// Which associative reduction (`reduce.+/min/max/*`).
#[derive(Debug, Clone, Copy)]
pub enum ReduceKind { Add, Min, Max, Mul }

#[derive(Debug)]
pub enum SystemOp {
    // Per-element compute
    Arith { op: ArithOp, interp: Interp },
    UnaryArith { op: UnaryArithOp, interp: Interp },
    Cmp { op: CmpOp },
    As { interp: Interp },
    // Boolean
    Not, And, Or, Any, All,
    // Aggregations / scans
    Reduce { kind: ReduceKind, interp: Interp },
    Cumsum { interp: Interp },
    Shift { interp: Interp },
    Count,
    // Surveys / joins
    Where, Filter, Gather, Spread,
    Intersect,
    Search,
    XProd,
    // Sort family
    SortPerm, Sort, Group, Unique,
    Enswizzle { interp: Interp }, Deswizzle { interp: Interp },
    // Structural — Prod
    Zip { n: usize }, Detuple { n: usize }, Proj { i: usize },
    // Structural — Sum
    Inject { n: usize }, Split, Partition { n: usize }, Branch { k: usize },
    // Structural — List / View
    Nest, NestStride, Flatten, Bounds, BoundsKeys, Head, Like, Enlist, Unlist, Iota,
    View, ViewRange, DecomposeView,
    // Slicing / concat
    Concat, Cat { n: usize }, Take, Skip, Reverse,

    /// An operator the system doesn't model as a first-class variant
    /// (body-bearing, binding, diagnostics, literals, FFI). Opaque to
    /// optimization; runnable/typecheckable via the wrapped kernel.
    Foreign(Box<dyn Op>),
}

impl SystemOp {
    /// Reconstruct the kernel for a modeled variant. `None` for `Foreign`
    /// (which carries its own kernel) — callers delegate to the box there.
    fn as_kernel(&self) -> Option<Box<dyn Op>> {
        use crate::ops::{arith, cmp, list, combinators as cmb, join, sort, sort_concat as sc, convert, reduce_ops as red};
        Some(match self {
            SystemOp::Arith { op, interp } => Box::new(arith::Arith { op: *op, interp: *interp }),
            SystemOp::UnaryArith { op, interp } => Box::new(arith::UnaryArith { op: *op, interp: *interp }),
            SystemOp::Cmp { op } => Box::new(cmp::Cmp { op: *op }),
            SystemOp::As { interp } => Box::new(convert::As { interp: *interp }),
            SystemOp::Not => Box::new(red::Not),
            SystemOp::And => Box::new(red::And),
            SystemOp::Or => Box::new(red::Or),
            SystemOp::Any => Box::new(red::Any),
            SystemOp::All => Box::new(red::All),
            SystemOp::Reduce { kind, interp } => match kind {
                ReduceKind::Add => Box::new(list::ReduceAdd { interp: *interp }),
                ReduceKind::Min => Box::new(red::ReduceMin { interp: *interp }),
                ReduceKind::Max => Box::new(red::ReduceMax { interp: *interp }),
                ReduceKind::Mul => Box::new(red::ReduceMul { interp: *interp }),
            },
            SystemOp::Cumsum { interp } => Box::new(list::Cumsum { interp: *interp }),
            SystemOp::Shift { interp } => Box::new(list::Shift { interp: *interp }),
            SystemOp::Count => Box::new(list::Count),
            SystemOp::Where => Box::new(list::Where_),
            SystemOp::Filter => Box::new(list::Filter),
            SystemOp::Gather => Box::new(join::Gather),
            SystemOp::Spread => Box::new(list::Spread),
            SystemOp::Intersect => Box::new(join::Intersect),
            SystemOp::Search => Box::new(join::Search),
            SystemOp::XProd => Box::new(join::XProd),
            SystemOp::SortPerm => Box::new(sort::SortPerm),
            SystemOp::Sort => Box::new(sort::SortPoly),
            SystemOp::Enswizzle { interp } => Box::new(crate::ops::swizzle::Enswizzle { interp: *interp }),
            SystemOp::Deswizzle { interp } => Box::new(crate::ops::swizzle::Deswizzle { interp: *interp }),
            SystemOp::Group => Box::new(list::Group),
            SystemOp::Unique => Box::new(list::Unique),
            SystemOp::Zip { n } => Box::new(cmb::ZipN { n: *n }),
            SystemOp::Detuple { n } => Box::new(cmb::DetupleN { n: *n }),
            SystemOp::Proj { i } => Box::new(cmb::Proj { i: *i }),
            SystemOp::Inject { n } => Box::new(cmb::InjectN { n: *n }),
            SystemOp::Split => Box::new(cmb::Split),
            SystemOp::Partition { n } => Box::new(cmb::PartitionN { n: *n }),
            SystemOp::Branch { k } => Box::new(cmb::Branch { k: *k }),
            SystemOp::Nest => Box::new(cmb::Nest),
            SystemOp::NestStride => Box::new(cmb::NestStride),
            SystemOp::Flatten => Box::new(cmb::Flatten),
            SystemOp::Bounds => Box::new(list::Bounds),
            SystemOp::BoundsKeys => Box::new(list::BoundsToKeys),
            SystemOp::Head => Box::new(list::Head),
            SystemOp::Like => Box::new(list::Like),
            SystemOp::Enlist => Box::new(cmb::Enlist),
            SystemOp::Unlist => Box::new(cmb::Unlist),
            SystemOp::Iota => Box::new(list::Iota),
            SystemOp::View => Box::new(crate::ops::view::View),
            SystemOp::ViewRange => Box::new(crate::ops::view::ViewRange),
            SystemOp::DecomposeView => Box::new(crate::ops::view::DecomposeView),
            SystemOp::Concat => Box::new(sc::Concat),
            SystemOp::Cat { n } => Box::new(sc::CatN { n: *n }),
            SystemOp::Take => Box::new(sc::Take),
            SystemOp::Skip => Box::new(sc::Skip),
            SystemOp::Reverse => Box::new(sc::Reverse),
            SystemOp::Foreign(_) => return None,
        })
    }

    pub fn name(&self) -> String {
        match self {
            SystemOp::Foreign(o) => o.name().to_string(),
            other => other.as_kernel().unwrap().name().to_string(),
        }
    }

    pub fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        match self {
            SystemOp::Foreign(o) => o.run(st, env),
            other => other.as_kernel().unwrap().run(st, env),
        }
    }

    pub fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        match self {
            SystemOp::Foreign(o) => o.tc(st, env),
            other => other.as_kernel().unwrap().tc(st, env),
        }
    }

    pub fn arity(&self) -> Option<(usize, usize)> {
        match self {
            SystemOp::Foreign(o) => o.arity(),
            other => other.as_kernel().unwrap().arity(),
        }
    }

    pub fn routing_map(&self) -> Option<Vec<usize>> {
        match self {
            SystemOp::Foreign(o) => o.routing_map(),
            _ => None,
        }
    }

    /// Observable beyond its data outputs (prints, timing, profiling).
    /// Such an op must not be CSE-merged (re-running changes behavior)
    /// nor dropped by DCE. All modeled variants are pure; only `Foreign`
    /// can wrap an effecting op.
    pub fn is_side_effecting(&self) -> bool {
        use crate::ops::{stack::{TimeOp, ProfileStart, ProfilePrint}, convert::Show};
        match self {
            SystemOp::Foreign(o) => {
                let any: &dyn std::any::Any = o.as_ref();
                any.is::<TimeOp>() || any.is::<ProfileStart>()
                    || any.is::<ProfilePrint>() || any.is::<Show>()
            }
            _ => false,
        }
    }

    /// Carries an opaque sub-program (`each`/`reduce {body}`/`repeat`/
    /// `match`/`cleave`). DCE keeps these live conservatively — the body
    /// may itself be side-effecting, which isn't visible from here.
    pub fn has_body(&self) -> bool {
        use crate::ops::{list::{Each, Reduce, Repeat}, combinators::{Match, Cleave}};
        match self {
            SystemOp::Foreign(o) => {
                let any: &dyn std::any::Any = o.as_ref();
                any.is::<Each>() || any.is::<Reduce>() || any.is::<Repeat>()
                    || any.is::<Match>() || any.is::<Cleave>()
            }
            _ => false,
        }
    }
}

/// Recognize a parsed (boxed) op and promote it to a first-class
/// `SystemOp` variant where the system models it; otherwise `Foreign`.
/// The stack front-end's "token's op → system operator" mapping, done by
/// downcast during lowering. Shrinks as the parser learns to construct
/// variants directly.
pub fn promote(op: Box<dyn Op>) -> SystemOp {
    use crate::ops::{arith, cmp, list, combinators as cmb, join, sort, sort_concat as sc, convert, reduce_ops as red};
    let any: &dyn std::any::Any = op.as_ref();

    macro_rules! zst { ($t:ty, $v:expr) => { if any.is::<$t>() { return $v; } }; }
    macro_rules! one { ($t:ty, $f:ident, $v:expr) => {
        if let Some(o) = any.downcast_ref::<$t>() { let $f = o.$f; return $v; }
    }; }

    // Per-element compute
    if let Some(o) = any.downcast_ref::<arith::Arith>() { return SystemOp::Arith { op: o.op, interp: o.interp }; }
    if let Some(o) = any.downcast_ref::<arith::UnaryArith>() { return SystemOp::UnaryArith { op: o.op, interp: o.interp }; }
    if let Some(o) = any.downcast_ref::<cmp::Cmp>() { return SystemOp::Cmp { op: o.op }; }
    one!(convert::As, interp, SystemOp::As { interp });
    // Boolean
    zst!(red::Not, SystemOp::Not); zst!(red::And, SystemOp::And); zst!(red::Or, SystemOp::Or);
    zst!(red::Any, SystemOp::Any); zst!(red::All, SystemOp::All);
    // Reductions / scans
    one!(list::ReduceAdd, interp, SystemOp::Reduce { kind: ReduceKind::Add, interp });
    one!(red::ReduceMin, interp, SystemOp::Reduce { kind: ReduceKind::Min, interp });
    one!(red::ReduceMax, interp, SystemOp::Reduce { kind: ReduceKind::Max, interp });
    one!(red::ReduceMul, interp, SystemOp::Reduce { kind: ReduceKind::Mul, interp });
    one!(list::Cumsum, interp, SystemOp::Cumsum { interp });
    one!(list::Shift, interp, SystemOp::Shift { interp });
    zst!(list::Count, SystemOp::Count);
    // Surveys / joins
    zst!(list::Where_, SystemOp::Where); zst!(list::Filter, SystemOp::Filter);
    zst!(join::Gather, SystemOp::Gather); zst!(list::Spread, SystemOp::Spread);
    zst!(join::Intersect, SystemOp::Intersect);
    zst!(join::Search, SystemOp::Search);
    zst!(join::XProd, SystemOp::XProd);
    // Sort family
    zst!(sort::SortPerm, SystemOp::SortPerm); zst!(sort::SortPoly, SystemOp::Sort);
    one!(crate::ops::swizzle::Enswizzle, interp, SystemOp::Enswizzle { interp });
    one!(crate::ops::swizzle::Deswizzle, interp, SystemOp::Deswizzle { interp });
    zst!(list::Group, SystemOp::Group);
    zst!(list::Unique, SystemOp::Unique);
    // Structural
    one!(cmb::ZipN, n, SystemOp::Zip { n });
    one!(cmb::DetupleN, n, SystemOp::Detuple { n });
    one!(cmb::Proj, i, SystemOp::Proj { i });
    one!(cmb::InjectN, n, SystemOp::Inject { n });
    zst!(cmb::Split, SystemOp::Split);
    one!(cmb::PartitionN, n, SystemOp::Partition { n });
    one!(cmb::Branch, k, SystemOp::Branch { k });
    zst!(cmb::Nest, SystemOp::Nest); zst!(cmb::NestStride, SystemOp::NestStride);
    zst!(cmb::Flatten, SystemOp::Flatten);
    zst!(list::Bounds, SystemOp::Bounds); zst!(list::BoundsToKeys, SystemOp::BoundsKeys);
    zst!(list::Head, SystemOp::Head); zst!(list::Like, SystemOp::Like);
    zst!(cmb::Enlist, SystemOp::Enlist); zst!(cmb::Unlist, SystemOp::Unlist);
    zst!(list::Iota, SystemOp::Iota);
    zst!(crate::ops::view::View, SystemOp::View);
    zst!(crate::ops::view::ViewRange, SystemOp::ViewRange);
    zst!(crate::ops::view::DecomposeView, SystemOp::DecomposeView);
    // Slicing / concat
    zst!(sc::Concat, SystemOp::Concat);
    one!(sc::CatN, n, SystemOp::Cat { n });
    zst!(sc::Take, SystemOp::Take); zst!(sc::Skip, SystemOp::Skip); zst!(sc::Reverse, SystemOp::Reverse);

    SystemOp::Foreign(op)
}
