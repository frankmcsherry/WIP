//! Layer 1 + Layer 3: the language definition.
//!
//! - `value` — the dynamic value model (Prim, Prod, Sum, List, View).
//! - `stack` — value stack with View-materializing pop.
//! - `op` — the PrimOp trait and the eval loop.
//! - `shape` — structural shape (no interpretation tags).
//! - `typecheck` — Typed trait and the typecheck runner.
//! - `profile` — per-op self-time profiling instrumentation for eval.

pub mod value;
pub mod stack;
pub mod op;
pub mod shape;
pub mod typecheck;
pub mod profile;
