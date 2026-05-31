//! corgi — a minimal columnar, **single-input** term-graph IR.
//!
//! Every semantic op is a unary `T0 -> T1`, evaluated as `eval(Value) -> Value` —
//! the "1:1 map" taken literally. There is no `arity()`: a node's shape requirement
//! lives in its input's type, which the typer needs anyway.
//!
//! The leaf is a width-tagged `Prim` column (`u8`/`u16`/`u32`/`u64`); signed and float are
//! KINDS the numeric layer encodes onto it, so the core stays kind-blind. Booleans use the
//! idiom `0 = false, nonzero = true` (a mask is just a leaf) — no `Bool` leaf. `Unit` is
//! deferred until JSON `null` needs it.
//!
//! Structural nodes (the only arity != 1 nodes):
//!   * `Input` — arity 0, the stratum root (reads the parameter)
//!   * `Tuple` — arity N, the sole fan-in (collect edges into a product)
//!
//! Everything else is a unary op via [`op::Op::eval`], including `Lit` (a constant
//! element filled to its input's length — anchored to a stratum) and the two
//! closed-body ops `MapList` / `MapSum` (they recurse into [`graph::eval_graph`]).
//!
//! Layers: [`value`] (the data) → [`engine`] (`gather`/`concat` + index gen) /
//! [`cmp`] (`compare2`/structural order) → [`op`] (the vocabulary) → [`graph`]
//! (the IR + evaluator).

pub mod cmp;
pub mod engine;
pub mod find_search; // PROTOTYPE: structural-search find, to the side for examination (not wired into eval)
pub mod frontend;
pub mod graph;
pub mod ops;
pub mod optimize;
pub mod shape;
pub mod value;

pub use frontend::{parse_ml, Program};
pub use graph::{eval_graph, shape_of, Builder, Graph, OpLike};
pub use ops::{dec_i64, enc_i64, ArithOp, BinOp, CmpOp, Kind, NumOp, Op, Pred};
pub use optimize::{cse, dce, optimize, peephole};
pub use shape::{shape_of_value, Shape};
pub use value::{show, Value};
