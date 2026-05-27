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
//! │          value model + PrimOp trait. Knows nothing of   │
//! │          specific operators, types, or syntax.          │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Library use
//!
//! ```ignore
//! use collie::pipeline::{build, optimize, eval_graph};
//! use collie::syntax::{parse::parse, registry::OpRegistry};
//!
//! let reg = OpRegistry::standard();
//! let prog = parse("u64[1 2 3] reduce.+.u64", &reg)?;
//! let (graph, _shapes) = build(prog)?;   // lower to the term graph (typechecks)
//! let graph = optimize(graph);            // Graph → Graph (optional; --no-opt skips)
//! let stack = eval_graph(&graph)?;        // the only evaluator
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
