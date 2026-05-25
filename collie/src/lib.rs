//! collie: a tiny interpreted array-language++ over a columnar value model.
//!
//! ## Architecture
//!
//! Four layers, each independent of those above it. The library exports
//! the bottom three; the `tools/` subdir (REPL, bench, pretty, etc.) is
//! binary-only and not re-exported.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │ Layer 4: syntax/   parse + registry                     │
//! ├─────────────────────────────────────────────────────────┤
//! │ Layer 3: ir/typecheck    Typed trait, typecheck runner  │
//! ├─────────────────────────────────────────────────────────┤
//! │ Layer 2: ops/      operators (each = PrimOp + Typed)    │
//! ├─────────────────────────────────────────────────────────┤
//! │ Layer 1: ir/{value, stack, op, shape, profile}          │
//! │          value model + eval loop. Knows nothing of      │
//! │          specific operators, types, or syntax.          │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Library use
//!
//! ```ignore
//! use collie::ir::{op::eval, shape::Shape, typecheck::typecheck, value::Value};
//! use collie::syntax::{parse::parse, registry::OpRegistry};
//!
//! let reg = OpRegistry::standard();
//! let prog = parse("u64[1 2 3] reduce.+.u64", &reg)?;
//! let mut ts: Vec<Shape> = Vec::new();
//! let mut tenv: Vec<Shape> = Vec::new();
//! typecheck(&prog, &mut ts, &mut tenv)?;
//! let mut stack: Vec<Value> = Vec::new();
//! let mut env: Vec<Value> = Vec::new();
//! eval(&prog, &mut stack, &mut env)?;
//! // stack now holds the result.
//! ```

pub mod ir;
pub mod ops;
pub mod syntax;
pub mod pipeline;

// Binary-only helpers. Lives in the library crate so it can use `crate::ir`,
// but not part of the public language API — library consumers should ignore
// it. The `main.rs` binary uses these to provide REPL, bench, examples, etc.
pub mod tools;
