//! Layer 2: operator implementations. Each submodule is one operator family.
//! Operators are structs that impl `PrimOp` (for `run`) and `Typed` (for `tc`).

pub mod helpers;
pub mod stack;
pub mod arith;
pub mod cmp;
pub mod convert;
pub mod combinators;
pub mod list;
pub mod join;
pub mod letbind;
pub mod reduce_ops;
pub mod sort_concat;
pub mod sort;
pub mod swizzle;
pub mod view;
