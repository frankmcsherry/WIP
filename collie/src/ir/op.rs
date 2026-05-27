//! Layer 1: the PrimOp trait.
//!
//! The core knows nothing about specific operators. The op-stream
//! interpreter that used to live here (`eval`) was removed — programs are
//! lowered to a term graph (`pipeline::lower`) and run by the graph engine
//! (`pipeline::execute::eval_graph`), the only evaluator.

use crate::ir::stack::Stack;
use crate::ir::value::Value;

/// An operator that can be executed against a stack.
///
/// The minimal contract for a collie operator. Layer 3 (`typecheck.rs`) adds
/// a separate `Typed` trait that operators may also implement.
///
/// `Any` is a supertrait so the parser's last-use inference pass can
/// downcast `Box<dyn PrimOp>` to concrete types (specifically `Ref` and
/// the body-bearing ops like `Let`, `Repeat`, etc.) for AST traversal.
/// `Any` is auto-implemented for any `'static` type — implementations
/// don't need to do anything.
pub trait PrimOp: std::fmt::Debug + std::any::Any {
    /// Op's surface name. Used by the parser peephole and diagnostic
    /// output; kept on the trait so every op carries it.
    fn name(&self) -> &str;
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String>;

    /// Stack effect: `(pops, pushes)`. The number of values this op
    /// consumes from the top of the stack and the number it pushes
    /// back. Constant per op instance — parameterized ops (e.g.
    /// `cat.N`, `entuple.N`, `cleave.N`) read from `&self` fields.
    /// Returns `None` for ops whose stack effect depends on runtime
    /// values (e.g., `split` produces 1 + (number of Sum lanes)
    /// outputs, only knowable from the value).
    fn arity(&self) -> Option<(usize, usize)> { None }

    /// Pure stack-routing ops (dup/drop/swap/over/rot/pick/roll) declare,
    /// for each output, which input index it aliases. `Some(map)` means
    /// the op computes nothing — output `i` is exactly input `map[i]` —
    /// so a graph build can resolve it into direct edges. `None` (default)
    /// means the op is computational and stays a node.
    ///
    /// Inputs and outputs are both stack order (index 0 = deepest /
    /// pushed first), matching `arity`.
    fn routing_map(&self) -> Option<Vec<usize>> { None }
}
