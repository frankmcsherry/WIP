//! Host kernels: an op whose `eval` is supplied from outside corgi, over whole columns.
//!
//! The one open variant in an otherwise closed vocabulary (`NumOp::Host`). A host kernel declares
//! its input and output shapes and maps `n` rows to `n` rows; a one-to-many kernel returns a
//! `List`. Corgi types it the way it types everything, by `eval` on zero rows: the adapter here
//! answers that from the declared output without calling the kernel, and checks the declared
//! shapes at every call, so a kernel never sees a column it did not declare.

use std::sync::Arc;

use crate::shape::{shape_of_value, Shape};
use crate::value::Value;

/// A kernel over columns. `eval` receives a column of shape `input()` with at least one row, and
/// must return a column of shape `output()` with the same number of rows.
///
/// The contract, which corgi relies on but cannot check:
/// - **Deterministic, no side effects.** The optimizer rewrites host nodes like any other op:
///   common-subexpression elimination merges calls of one kernel on one input, and dead-code
///   elimination drops unused calls.
/// - **Row-local.** Output row `i` depends only on input row `i`. Batches are split and joined
///   differently with worker count and inside `MapList`, so no row may see another.
/// - **`Err` is fatal.** It is not an in-language failure: `eval_graph` panics on it. A kernel that
///   can fail on data returns a sum. Return `Err` only for a broken contract.
/// - **Shapes must carry their row count.** `Value::len` of a `Prod` is its first field's, so a
///   shape whose chain of first fields ends in an empty `Prod` has no rows. The adapter rejects
///   such an input or output shape as a type error; a kernel with no arguments declares `Unit`.
pub trait HostKernel: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn input(&self) -> &Shape;
    fn output(&self) -> &Shape;
    fn eval(&self, input: Value) -> Result<Value, String>;
}

/// A host kernel as an op. Equal only to itself (the same `Arc`), so common-subexpression
/// elimination merges two calls of one kernel and never two different kernels.
#[derive(Clone)]
pub struct HostOp(pub Arc<dyn HostKernel>);

impl PartialEq for HostOp {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}
impl Eq for HostOp {}
impl std::fmt::Debug for HostOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Host({})", self.0.name()) }
}
impl std::hash::Hash for HostOp {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.0.name().hash(h) }
}

impl HostOp {
    pub fn eval(&self, input: Value) -> Result<Value, String> {
        let k = &self.0;
        // Checked before the zero-row return, so typing (`shape_of`) reports it.
        for (what, s) in [("input", k.input()), ("output", k.output())] {
            if !counts_rows(s) {
                return Err(format!("{}: {what} shape {s} has no column that counts its rows (use Unit)", k.name()));
            }
        }
        let got = shape_of_value(&input);
        if &got != k.input() {
            return Err(format!("{}: expected input {}, got {}", k.name(), k.input(), got));
        }
        let n = input.len();
        if n == 0 {
            return Ok(Value::empty(k.output()));
        }
        let out = k.eval(input)?;
        let shape = shape_of_value(&out);
        if &shape != k.output() {
            return Err(format!("{}: declared output {}, returned {}", k.name(), k.output(), shape));
        }
        if out.len() != n {
            return Err(format!("{}: {} rows in, {} rows out", k.name(), n, out.len()));
        }
        Ok(out)
    }
}

/// Whether a value of shape `s` knows its row count: follow first fields (as `Value::len` does)
/// down to a column that carries its own length.
fn counts_rows(s: &Shape) -> bool {
    match s {
        Shape::Prod(fs) => fs.first().is_some_and(counts_rows),
        Shape::Int | Shape::Float | Shape::List(_) | Shape::Sum(_) | Shape::Unit | Shape::Ref(_) => true,
    }
}

#[cfg(test)]
mod tests;
