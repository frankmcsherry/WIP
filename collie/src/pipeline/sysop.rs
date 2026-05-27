//! The system operator vocabulary — what a graph [`Term`](super::graph::Term)
//! holds.
//!
//! This is the *operator* layer: abstract, introspectable, what the
//! optimizer pattern-matches on. Each variant carries the op's parameters
//! (interp, arity, op-kind). `run`/`tc`/`name`/`arity` dispatch directly to
//! free-function kernels in `crate::ops::*` (e.g. `cmp::run`, `list::group_run`)
//! — the back-end IR owns the kernel mapping; it never reconstructs a
//! front-end struct. `Foreign` is the sole exception: it carries its own
//! boxed kernel and delegates to it.
//!
//! `Foreign` is the escape hatch — ops the system doesn't model as a
//! first-class variant: binding (`let`/`ref`), diagnostics
//! (`time`/`profile.*`/`show`), literals, and any front-end/FFI ops. No
//! body-bearing op remains — `each` became segmented ops, `match`/`cleave`
//! desugar in the parser, and `repeat` was removed — so **no `Foreign` op
//! runs a sub-program via legacy eval**; the engine is self-contained over
//! the IR. (`let`/`ref` are boiled to edges in lowering and never reach a
//! surviving term.) Routing ops (`dup`/`pick`/…) never reach here either —
//! resolved into edges during lowering. Promoting literals/diagnostics to
//! variants is deferred (cosmetic; revisit with const-folding).

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
    SortPerm, Sort, SortSegmented, Group, Unique,
    Enswizzle { interp: Interp }, Deswizzle { interp: Interp },
    // Structural — Prod
    Zip { n: usize }, Detuple { n: usize }, Proj { i: usize },
    // Structural — Sum
    Inject { n: usize }, Split, Merge { n: usize }, Partition { n: usize }, Branch { k: usize },
    // Structural — List / View
    Nest, NestStride, Flatten, Bounds, ListRanges, BoundsKeys, Head, Like, Enlist, Unlist, Iota,
    View, ViewRange, DecomposeView,
    // Slicing / concat
    Concat, Cat { n: usize }, Take, Skip, Reverse,
    TakeSegmented, ReverseSegmented,

    /// A constant source term: produces a fixed `Value` (no inputs). Used to
    /// seed an *open* program's stack inputs into a closed graph (see
    /// `lower::build_seeded`), so the engine can run programs that would
    /// otherwise expect pre-pushed stack values (e.g. the bench harness).
    /// Also the natural home for promoted literals.
    Const(Value),

    /// An operator the system doesn't model as a first-class variant
    /// (body-bearing, binding, diagnostics, literals, FFI). Opaque to
    /// optimization; runnable/typecheckable via the wrapped kernel.
    Foreign(Box<dyn Op>),
}

impl SystemOp {
    pub fn name(&self) -> String {
        match self {
            SystemOp::Foreign(o) => o.name().to_string(),
            SystemOp::Const(_) => "const".to_string(),
            SystemOp::Reduce { kind, .. } => match kind {
                ReduceKind::Add => "reduce.+", ReduceKind::Min => "reduce.min",
                ReduceKind::Max => "reduce.max", ReduceKind::Mul => "reduce.*",
            }.to_string(),
            SystemOp::Cmp { op } => crate::ops::cmp::op_name(*op).to_string(),
            SystemOp::Arith { op, .. } => crate::ops::arith::op_name(*op).to_string(),
            SystemOp::UnaryArith { op, .. } => crate::ops::arith::unary_name(*op).to_string(),
            SystemOp::As { .. } => "as".to_string(),
            SystemOp::Not => "not".to_string(),
            SystemOp::And => "and".to_string(),
            SystemOp::Or => "or".to_string(),
            SystemOp::Any => "any".to_string(),
            SystemOp::All => "all".to_string(),
            SystemOp::Concat => "concat".to_string(),
            SystemOp::Cat { .. } => "cat".to_string(),
            SystemOp::Take => "take".to_string(),
            SystemOp::Skip => "skip".to_string(),
            SystemOp::Reverse => "reverse".to_string(),
            SystemOp::TakeSegmented => "take.segmented".to_string(),
            SystemOp::ReverseSegmented => "reverse.segmented".to_string(),
            SystemOp::SortPerm => "sort.perm".to_string(),
            SystemOp::Sort => "sort".to_string(),
            SystemOp::SortSegmented => "sort.segmented".to_string(),
            SystemOp::Enswizzle { .. } => "enswizzle".to_string(),
            SystemOp::Deswizzle { .. } => "deswizzle".to_string(),
            SystemOp::View => "view".to_string(),
            SystemOp::ViewRange => "view.range".to_string(),
            SystemOp::DecomposeView => "decompose-view".to_string(),
            SystemOp::Gather => "gather".to_string(),
            SystemOp::Intersect => "intersect".to_string(),
            SystemOp::Search => "search".to_string(),
            SystemOp::XProd => "xprod".to_string(),
            SystemOp::Cumsum { .. } => "cumsum".to_string(),
            SystemOp::Shift { .. } => "shift".to_string(),
            SystemOp::Count => "count".to_string(),
            SystemOp::Where => "where".to_string(),
            SystemOp::Filter => "filter".to_string(),
            SystemOp::Spread => "spread".to_string(),
            SystemOp::Group => "group".to_string(),
            SystemOp::Unique => "unique".to_string(),
            SystemOp::Bounds => "list>bounds".to_string(),
            SystemOp::ListRanges => "list>ranges".to_string(),
            SystemOp::BoundsKeys => "bounds>keys".to_string(),
            SystemOp::Head => "head".to_string(),
            SystemOp::Like => "like".to_string(),
            SystemOp::Iota => "iota".to_string(),
            SystemOp::Zip { .. } => "zip".to_string(),
            SystemOp::Detuple { .. } => "detuple".to_string(),
            SystemOp::Proj { .. } => ".".to_string(),
            SystemOp::Inject { .. } => "inject".to_string(),
            SystemOp::Split => "split".to_string(),
            SystemOp::Merge { .. } => "merge".to_string(),
            SystemOp::Partition { .. } => "partition".to_string(),
            SystemOp::Branch { .. } => "branch".to_string(),
            SystemOp::Nest => "nest".to_string(),
            SystemOp::NestStride => "nest.stride".to_string(),
            SystemOp::Flatten => "flatten".to_string(),
            SystemOp::Enlist => "enlist".to_string(),
            SystemOp::Unlist => "unlist".to_string(),
        }
    }

    pub fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
        match self {
            SystemOp::Foreign(o) => o.run(st, env),
            SystemOp::Const(v) => { st.push(v.clone()); Ok(()) }
            SystemOp::Reduce { kind, interp } => match kind {
                ReduceKind::Add => crate::ops::list::reduce_add_run(*interp, st),
                ReduceKind::Min => crate::ops::reduce_ops::reduce_min_run(*interp, st),
                ReduceKind::Max => crate::ops::reduce_ops::reduce_max_run(*interp, st),
                ReduceKind::Mul => crate::ops::reduce_ops::reduce_mul_run(*interp, st),
            },
            SystemOp::Cmp { op } => crate::ops::cmp::run(*op, st),
            SystemOp::Arith { op, interp } => crate::ops::arith::run(*op, *interp, st),
            SystemOp::UnaryArith { op, interp } => crate::ops::arith::unary_run(*op, *interp, st),
            SystemOp::As { interp } => crate::ops::convert::run(*interp, st),
            SystemOp::Not => crate::ops::reduce_ops::not_run(st),
            SystemOp::And => crate::ops::reduce_ops::and_run(st),
            SystemOp::Or => crate::ops::reduce_ops::or_run(st),
            SystemOp::Any => crate::ops::reduce_ops::any_run(st),
            SystemOp::All => crate::ops::reduce_ops::all_run(st),
            SystemOp::Concat => crate::ops::sort_concat::concat_run(st),
            SystemOp::Cat { n } => crate::ops::sort_concat::cat_run(*n, st),
            SystemOp::Take => crate::ops::sort_concat::take_run(st),
            SystemOp::Skip => crate::ops::sort_concat::skip_run(st),
            SystemOp::Reverse => crate::ops::sort_concat::reverse_run(st),
            SystemOp::TakeSegmented => crate::ops::sort_concat::take_seg_run(st),
            SystemOp::ReverseSegmented => crate::ops::sort_concat::reverse_seg_run(st),
            SystemOp::SortPerm => crate::ops::sort::sort_perm_run(st),
            SystemOp::Sort => crate::ops::sort::sort_poly_run(st),
            SystemOp::SortSegmented => crate::ops::sort::sort_seg_run(st),
            SystemOp::Enswizzle { interp } => crate::ops::swizzle::run_swizzle(st, *interp, true, "enswizzle"),
            SystemOp::Deswizzle { interp } => crate::ops::swizzle::run_swizzle(st, *interp, false, "deswizzle"),
            SystemOp::View => crate::ops::view::view_run(st),
            SystemOp::ViewRange => crate::ops::view::view_range_run(st),
            SystemOp::DecomposeView => crate::ops::view::decompose_view_run(st),
            SystemOp::Gather => crate::ops::join::gather_run(st),
            SystemOp::Intersect => crate::ops::join::intersect_run(st),
            SystemOp::Search => crate::ops::join::search_run(st),
            SystemOp::XProd => crate::ops::join::xprod_run(st),
            SystemOp::Cumsum { interp } => crate::ops::list::cumsum_run(*interp, st),
            SystemOp::Shift { interp } => crate::ops::list::shift_run(*interp, st),
            SystemOp::Count => crate::ops::list::count_run(st),
            SystemOp::Where => crate::ops::list::where_run(st),
            SystemOp::Filter => crate::ops::list::filter_run(st),
            SystemOp::Spread => crate::ops::list::spread_run(st),
            SystemOp::Group => crate::ops::list::group_run(st),
            SystemOp::Unique => crate::ops::list::unique_run(st),
            SystemOp::Bounds => crate::ops::list::bounds_run(st),
            SystemOp::ListRanges => crate::ops::list::list_ranges_run(st),
            SystemOp::BoundsKeys => crate::ops::list::bounds_to_keys_run(st),
            SystemOp::Head => crate::ops::list::head_run(st),
            SystemOp::Like => crate::ops::list::like_run(st),
            SystemOp::Iota => crate::ops::list::iota_run(st),
            SystemOp::Zip { n } => crate::ops::combinators::zip_run(*n, st),
            SystemOp::Detuple { n } => crate::ops::combinators::detuple_run(*n, st),
            SystemOp::Proj { i } => crate::ops::combinators::proj_run(*i, st),
            SystemOp::Inject { n } => crate::ops::combinators::inject_run(*n, st),
            SystemOp::Split => crate::ops::combinators::split_run(st),
            SystemOp::Merge { n } => crate::ops::combinators::merge_run(*n, st),
            SystemOp::Partition { n } => crate::ops::combinators::partition_run(*n, st),
            SystemOp::Branch { k } => crate::ops::combinators::branch_run(*k, st),
            SystemOp::Nest => crate::ops::combinators::nest_run(st),
            SystemOp::NestStride => crate::ops::combinators::nest_stride_run(st),
            SystemOp::Flatten => crate::ops::combinators::flatten_run(st),
            SystemOp::Enlist => crate::ops::combinators::enlist_run(st),
            SystemOp::Unlist => crate::ops::combinators::unlist_run(st),
        }
    }

    pub fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String> {
        match self {
            SystemOp::Foreign(o) => o.tc(st, env),
            SystemOp::Const(v) => { st.push(crate::ir::shape::shape_of(v)); Ok(()) }
            SystemOp::Reduce { kind, interp } => match kind {
                ReduceKind::Add => crate::ops::list::reduce_add_tc(*interp, st),
                ReduceKind::Min => crate::ops::reduce_ops::reduce_min_tc(*interp, st),
                ReduceKind::Max => crate::ops::reduce_ops::reduce_max_tc(*interp, st),
                ReduceKind::Mul => crate::ops::reduce_ops::reduce_mul_tc(*interp, st),
            },
            SystemOp::Cmp { .. } => crate::ops::cmp::tc(st),
            SystemOp::Arith { interp, .. } => crate::ops::arith::tc(*interp, st),
            SystemOp::UnaryArith { op, interp } => crate::ops::arith::unary_tc(*op, *interp, st),
            SystemOp::As { interp } => crate::ops::convert::tc(*interp, st),
            SystemOp::Not => crate::ops::reduce_ops::not_tc(st),
            SystemOp::And => crate::ops::reduce_ops::and_tc(st),
            SystemOp::Or => crate::ops::reduce_ops::or_tc(st),
            SystemOp::Any => crate::ops::reduce_ops::any_tc(st),
            SystemOp::All => crate::ops::reduce_ops::all_tc(st),
            SystemOp::Concat => crate::ops::sort_concat::concat_tc(st),
            SystemOp::Cat { n } => crate::ops::sort_concat::cat_tc(*n, st),
            SystemOp::Take => crate::ops::sort_concat::take_tc(st),
            SystemOp::Skip => crate::ops::sort_concat::skip_tc(st),
            SystemOp::Reverse => crate::ops::sort_concat::reverse_tc(st),
            SystemOp::TakeSegmented => crate::ops::sort_concat::take_seg_tc(st),
            SystemOp::ReverseSegmented => crate::ops::sort_concat::reverse_seg_tc(st),
            SystemOp::SortPerm => crate::ops::sort::sort_perm_tc(st),
            SystemOp::Sort => crate::ops::sort::sort_poly_tc(st),
            SystemOp::SortSegmented => crate::ops::sort::sort_seg_tc(st),
            SystemOp::Enswizzle { interp } => crate::ops::swizzle::tc_swizzle(st, *interp, "enswizzle"),
            SystemOp::Deswizzle { interp } => crate::ops::swizzle::tc_swizzle(st, *interp, "deswizzle"),
            SystemOp::View => crate::ops::view::view_tc(st),
            SystemOp::ViewRange => crate::ops::view::view_range_tc(st),
            SystemOp::DecomposeView => crate::ops::view::decompose_view_tc(st),
            SystemOp::Gather => crate::ops::join::gather_tc(st),
            SystemOp::Intersect => crate::ops::join::intersect_tc(st),
            SystemOp::Search => crate::ops::join::search_tc(st),
            SystemOp::XProd => crate::ops::join::xprod_tc(st),
            SystemOp::Cumsum { interp } => crate::ops::list::cumsum_tc(*interp, st),
            SystemOp::Shift { interp } => crate::ops::list::shift_tc(*interp, st),
            SystemOp::Count => crate::ops::list::count_tc(st),
            SystemOp::Where => crate::ops::list::where_tc(st),
            SystemOp::Filter => crate::ops::list::filter_tc(st),
            SystemOp::Spread => crate::ops::list::spread_tc(st),
            SystemOp::Group => crate::ops::list::group_tc(st),
            SystemOp::Unique => crate::ops::list::unique_tc(st),
            SystemOp::Bounds => crate::ops::list::bounds_tc(st),
            SystemOp::ListRanges => crate::ops::list::list_ranges_tc(st),
            SystemOp::BoundsKeys => crate::ops::list::bounds_to_keys_tc(st),
            SystemOp::Head => crate::ops::list::head_tc(st),
            SystemOp::Like => crate::ops::list::like_tc(st),
            SystemOp::Iota => crate::ops::list::iota_tc(st),
            SystemOp::Zip { n } => crate::ops::combinators::zip_tc(*n, st),
            SystemOp::Detuple { n } => crate::ops::combinators::detuple_tc(*n, st),
            SystemOp::Proj { i } => crate::ops::combinators::proj_tc(*i, st),
            SystemOp::Inject { n } => crate::ops::combinators::inject_tc(*n, st),
            SystemOp::Split => crate::ops::combinators::split_tc(st),
            SystemOp::Merge { n } => crate::ops::combinators::merge_tc(*n, st),
            SystemOp::Partition { n } => crate::ops::combinators::partition_tc(*n, st),
            SystemOp::Branch { k } => crate::ops::combinators::branch_tc(*k, st),
            SystemOp::Nest => crate::ops::combinators::nest_tc(st),
            SystemOp::NestStride => crate::ops::combinators::nest_stride_tc(st),
            SystemOp::Flatten => crate::ops::combinators::flatten_tc(st),
            SystemOp::Enlist => crate::ops::combinators::enlist_tc(st),
            SystemOp::Unlist => crate::ops::combinators::unlist_tc(st),
        }
    }

    pub fn arity(&self) -> Option<(usize, usize)> {
        match self {
            SystemOp::Foreign(o) => o.arity(),
            SystemOp::Const(_) => Some((0, 1)),
            SystemOp::Cmp { .. } => Some((2, 1)),
            SystemOp::Arith { .. } => Some((2, 1)),
            SystemOp::UnaryArith { .. } => Some((1, 1)),
            SystemOp::As { .. } => Some((1, 1)),
            SystemOp::Not | SystemOp::Any | SystemOp::All => Some((1, 1)),
            SystemOp::And | SystemOp::Or => Some((2, 1)),
            SystemOp::Reduce { .. } => Some((1, 1)),
            SystemOp::Concat | SystemOp::Reverse | SystemOp::ReverseSegmented => Some((1, 1)),
            SystemOp::Take | SystemOp::Skip | SystemOp::TakeSegmented => Some((2, 1)),
            SystemOp::Cat { n } => Some((*n, 1)),
            SystemOp::SortPerm | SystemOp::Sort | SystemOp::SortSegmented
            | SystemOp::Enswizzle { .. } | SystemOp::Deswizzle { .. } => Some((1, 1)),
            SystemOp::View => Some((2, 1)),
            SystemOp::ViewRange => Some((3, 1)),
            SystemOp::DecomposeView => Some((1, 2)),
            SystemOp::Gather | SystemOp::Search => Some((2, 1)),
            SystemOp::Intersect => Some((2, 2)),
            SystemOp::XProd => Some((1, 1)),
            SystemOp::Cumsum { .. } | SystemOp::Count | SystemOp::Where | SystemOp::Unique
            | SystemOp::Bounds | SystemOp::ListRanges | SystemOp::BoundsKeys
            | SystemOp::Head | SystemOp::Iota => Some((1, 1)),
            SystemOp::Shift { .. } | SystemOp::Filter | SystemOp::Spread | SystemOp::Like => Some((2, 1)),
            SystemOp::Group => Some((2, 2)),
            SystemOp::Proj { .. } | SystemOp::Enlist | SystemOp::Unlist => Some((1, 1)),
            SystemOp::Zip { n } => Some((*n, 1)),
            SystemOp::Detuple { n } => Some((1, *n)),
            SystemOp::Inject { n } | SystemOp::Merge { n } => Some((*n + 1, 1)),
            SystemOp::Partition { n } => Some((2, *n)),
            SystemOp::Branch { .. } | SystemOp::Nest | SystemOp::NestStride => Some((2, 1)),
            SystemOp::Flatten => Some((1, 2)),
            SystemOp::Split => None,
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

    // Literals: fold into the constant source term rather than carrying
    // them as opaque `Foreign` ops (introspectable, CSE-able, const-foldable).
    if let Some(l) = any.downcast_ref::<convert::LitNum>() { return SystemOp::Const(l.to_value()); }
    if let Some(l) = any.downcast_ref::<convert::LitArr>() { return SystemOp::Const(l.to_value()); }
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
    zst!(sort::SortSegmented, SystemOp::SortSegmented);
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
    one!(cmb::MergeN, n, SystemOp::Merge { n });
    one!(cmb::PartitionN, n, SystemOp::Partition { n });
    one!(cmb::Branch, k, SystemOp::Branch { k });
    zst!(cmb::Nest, SystemOp::Nest); zst!(cmb::NestStride, SystemOp::NestStride);
    zst!(cmb::Flatten, SystemOp::Flatten);
    zst!(list::Bounds, SystemOp::Bounds); zst!(list::BoundsToKeys, SystemOp::BoundsKeys);
    zst!(list::ListRanges, SystemOp::ListRanges);
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
    zst!(sc::TakeSegmented, SystemOp::TakeSegmented); zst!(sc::ReverseSegmented, SystemOp::ReverseSegmented);

    SystemOp::Foreign(op)
}
