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
/// - **`Err` is fatal.** It is not an in-language failure: `eval_graph` panics on it, and
///   `is_total` counts a host call as total. Return `Err` only for a broken contract.
/// - **No arguments: declare `Unit` input.** A `Prod` of no columns has no rows (`Value::len`
///   reads its first column), so a kernel over it would never run; `Unit` carries its row count.
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
impl std::hash::Hash for HostOp {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.0.name().hash(h) }
}

impl HostOp {
    pub fn eval(&self, input: Value) -> Result<Value, String> {
        let k = &self.0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::NumOp;
    use crate::{eval_graph, shape_of, Builder};

    /// `repeat(x, k) -> [(x, i) for i in 0..k]`: a one-to-many kernel.
    struct Repeat { input: Shape, output: Shape }
    impl HostKernel for Repeat {
        fn name(&self) -> &str { "repeat" }
        fn input(&self) -> &Shape { &self.input }
        fn output(&self) -> &Shape { &self.output }
        fn eval(&self, input: Value) -> Result<Value, String> {
            let args = input.into_prod("repeat")?;
            let (xs, ks) = (args[0].as_u64("repeat")?, args[1].as_u64("repeat")?);
            let (mut x, mut i, mut ends) = (Vec::new(), Vec::new(), Vec::new());
            for r in 0..xs.len() {
                for k in 0..ks[r] {
                    x.push(xs[r]);
                    i.push(k);
                }
                ends.push(x.len());
            }
            Ok(Value::List(crate::value::Bounds::offsets(ends), Box::new(Value::Prod(vec![Value::u64(x), Value::u64(i)]))))
        }
    }
    fn repeat() -> HostOp {
        let p = Shape::Prim(64);
        HostOp(Arc::new(Repeat {
            input: Shape::Prod(vec![p.clone(), p.clone()]),
            output: Shape::List(Box::new(Shape::Prod(vec![p.clone(), p]))),
        }))
    }

    #[test]
    fn a_host_kernel_types_runs_and_checks() {
        let p = Shape::Prim(64);
        let mut b = Builder::<NumOp>::default();
        let x = b.input();
        let out = b.add(NumOp::Host(repeat()), vec![x]);
        let g = b.finish(out);
        let input_shape = Shape::Prod(vec![p.clone(), p.clone()]);
        // Typed on zero rows, without running the kernel.
        assert_eq!(shape_of(&g, &input_shape).unwrap(), Shape::List(Box::new(Shape::Prod(vec![p.clone(), p.clone()]))));
        // A mismatched input is a type error.
        assert!(shape_of(&g, &p).is_err());
        let v = eval_graph(&g, Value::Prod(vec![Value::u64(vec![7, 9]), Value::u64(vec![2, 0])]));
        let (bounds, vals) = v.into_list("test").unwrap();
        assert_eq!(bounds.to_vec(), vec![2, 2]);
        let cols = vals.into_prod("test").unwrap();
        assert_eq!(cols[0].as_u64("t").unwrap(), &[7, 7]);
        assert_eq!(cols[1].as_u64("t").unwrap(), &[0, 1]);
    }

    /// Two calls of one kernel on one input merge under CSE; calls of two kernels do not.
    #[test]
    fn cse_merges_calls_of_one_kernel_only() {
        let (k, other) = (repeat(), repeat());
        let mut b = Builder::<NumOp>::default();
        let x = b.input();
        let calls = vec![
            b.add(NumOp::Host(k.clone()), vec![x]),
            b.add(NumOp::Host(k), vec![x]),
            b.add(NumOp::Host(other), vec![x]),
        ];
        let out = b.tuple(calls);
        let g = b.finish(out);
        let hosts = |g: &crate::Graph<NumOp>| g.nodes.iter().filter(|n| matches!(n.kind, crate::graph::NodeKind::Op(NumOp::Host(_)))).count();
        assert_eq!(hosts(&g), 3);
        assert_eq!(hosts(&crate::cse(&g)), 2);
    }

    #[test]
    fn two_kernels_are_distinct_one_kernel_is_itself() {
        let (a, b) = (repeat(), repeat());
        assert!(a == a.clone());
        assert!(a != b);
    }
}
