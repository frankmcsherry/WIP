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
        let (xs, ks) = (args[0].as_i64("repeat")?, args[1].as_i64("repeat")?);
        let (mut x, mut i, mut ends) = (Vec::new(), Vec::new(), Vec::new());
        for r in 0..xs.len() {
            for k in 0..ks[r] {
                x.push(xs[r]);
                i.push(k);
            }
            ends.push(x.len());
        }
        Ok(Value::List(crate::value::Bounds::offsets(ends), Box::new(Value::Prod(vec![Value::i64(x), Value::i64(i)]))))
    }
}
fn repeat() -> HostOp {
    let p = Shape::Int;
    HostOp(Arc::new(Repeat {
        input: Shape::Prod(vec![p.clone(), p.clone()]),
        output: Shape::List(Box::new(Shape::Prod(vec![p.clone(), p]))),
    }))
}

#[test]
fn a_host_kernel_types_runs_and_checks() {
    let p = Shape::Int;
    let mut b = Builder::<NumOp>::default();
    let x = b.input();
    let out = b.add(NumOp::Host(repeat()), vec![x]);
    let g = b.finish(out);
    let input_shape = Shape::Prod(vec![p.clone(), p.clone()]);
    // Typed on zero rows, without running the kernel.
    assert_eq!(shape_of(&g, &input_shape).unwrap(), Shape::List(Box::new(Shape::Prod(vec![p.clone(), p.clone()]))));
    // A mismatched input is a type error.
    assert!(shape_of(&g, &p).is_err());
    let v = eval_graph(&g, Value::Prod(vec![Value::i64(vec![7, 9]), Value::i64(vec![2, 0])]));
    let (bounds, vals) = v.into_list("test").unwrap();
    assert_eq!(bounds.to_vec(), vec![2, 2]);
    let cols = vals.into_prod("test").unwrap();
    assert_eq!(&*cols[0].as_i64("t").unwrap(), &[7, 7]);
    assert_eq!(&*cols[1].as_i64("t").unwrap(), &[0, 1]);
}

/// Input or output shapes that lose their row count are type errors, at typing time.
#[test]
fn shapes_without_a_row_count_are_type_errors() {
    struct K(Shape, Shape);
    impl HostKernel for K {
        fn name(&self) -> &str { "k" }
        fn input(&self) -> &Shape { &self.0 }
        fn output(&self) -> &Shape { &self.1 }
        fn eval(&self, input: Value) -> Result<Value, String> { Ok(input) }
    }
    let p = Shape::Int;
    let empty = Shape::Prod(vec![]);
    let nested = Shape::Prod(vec![empty.clone(), p.clone()]);
    let typed = |input: &Shape, output: &Shape| {
        let mut b = Builder::<NumOp>::default();
        let x = b.input();
        let out = b.add(NumOp::Host(HostOp(Arc::new(K(input.clone(), output.clone())))), vec![x]);
        shape_of(&b.finish(out), input)
    };
    assert!(typed(&empty, &p).is_err());
    assert!(typed(&nested, &p).is_err());
    assert!(typed(&p, &empty).is_err());
    assert!(typed(&p, &nested).is_err());
    assert_eq!(typed(&Shape::Unit, &p).unwrap(), p);
    assert_eq!(typed(&Shape::Prod(vec![p.clone(), empty.clone()]), &p).unwrap(), p);
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
