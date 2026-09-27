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

    #[test]
    fn two_kernels_are_distinct_one_kernel_is_itself() {
        let (a, b) = (repeat(), repeat());
        assert!(a == a.clone());
        assert!(a != b);
    }
}
