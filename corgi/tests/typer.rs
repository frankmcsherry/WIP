//! Shaper unit tests: a few positive shape results and the structural errors the
//! typer must reject at "compile" time (what `eval` would otherwise panic on).

use corgi::ArithOp::*;
use corgi::CmpOp::*;
use corgi::Op::*;
use corgi::BinOp::*;
use corgi::{parse_ml, shape_of, Builder, NumOp, Shape};

/// a one-op graph: Input -> op.
fn one(op: impl Into<NumOp>) -> corgi::Graph<NumOp> {
    let mut b = Builder::default();
    let inp = b.input();
    let out = b.add(op, vec![inp]);
    b.finish(out)
}

fn list(t: Shape) -> Shape {
    Shape::List(Box::new(t))
}

#[test]
fn enlist_is_polymorphic() {
    // X -> List<X> for any X (here a sum)
    let g = one(Enlist);
    let sum = Shape::Sum(vec![Shape::Int, list(Shape::Int)]);
    assert_eq!(shape_of(&g, &sum).unwrap(), list(sum));
}

#[test]
fn transpose_reads_arity_off_input() {
    // works for any product width; here a 3-tuple
    let g = one(Transpose);
    let three = list(Shape::Prod(vec![Shape::Int, Shape::Int, Shape::Int]));
    assert_eq!(
        shape_of(&g, &three).unwrap(),
        Shape::Prod(vec![list(Shape::Int), list(Shape::Int), list(Shape::Int)])
    );
}

#[test]
fn unwrap_homogeneous_ok_heterogeneous_errs() {
    let g = one(Unwrap);
    assert_eq!(shape_of(&g, &Shape::Sum(vec![Shape::Int, Shape::Int])).unwrap(), Shape::Int);
    assert!(shape_of(&g, &Shape::Sum(vec![Shape::Int, list(Shape::Int)])).is_err());
}

#[test]
fn field_out_of_range_errs() {
    assert!(shape_of(&one(Field(0)), &Shape::Int).is_err());
    assert!(shape_of(&one(Field(5)), &Shape::Prod(vec![Shape::Int, Shape::Int])).is_err());
}

#[test]
fn add_wrong_shape_errs() {
    let bad = Shape::Prod(vec![Shape::Int, list(Shape::Int)]);
    assert!(shape_of(&one(Bin(Add)), &bad).is_err());
}

#[test]
fn find_mismatched_elements_errs() {
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(Find, vec![inp]);
    let g = b.finish(out);
    // needle List<Int> vs haystack List<(Int,Int)> — element types differ
    let bad = Shape::Prod(vec![list(Shape::Int), list(Shape::Prod(vec![Shape::Int, Shape::Int]))]);
    assert!(shape_of(&g, &bad).is_err());
}

#[test]
fn judge_rejects_unrepresentable_parameters() {
    // parameters eval cannot represent must die in the typer, not at runtime: sum arities beyond
    // the u8 tag column.
    assert!(shape_of(&one(Inject(0, vec![Shape::Int; 300])), &Shape::Int).is_err());
    assert!(shape_of(&one(Branch(300)), &Shape::Prod(vec![Shape::Int, Shape::Int])).is_err());
}

#[test]
fn literals_type_as_int_or_float() {
    // a bare integer, negative or not, is an Int; a fraction or an exponent makes a Float. The
    // literal's value never picks a width.
    let g = parse_ml("(5, -3, 0, 255, 9223372036854775807, 0.5, -2.0, 1e3)").unwrap();
    let (i, f) = (Shape::Int, Shape::Float);
    let want = Shape::Prod(vec![i.clone(), i.clone(), i.clone(), i.clone(), i, f.clone(), f.clone(), f]);
    assert_eq!(shape_of(&g, &Shape::Int).unwrap(), want);
    // the declared shapes are spelled `int` and `float`
    let g = parse_ml("enum N = I int | F float in input inject I").unwrap();
    assert_eq!(shape_of(&g, &Shape::Int).unwrap(), Shape::Sum(vec![Shape::Int, Shape::Float]));
}

#[test]
fn mapsum_rejects_duplicate_variant() {
    // two arms on the SAME variant breaks the disjoint-arms invariant — the typer rejects it.
    let id = || {
        let mut b = Builder::<NumOp>::default();
        let i = b.input();
        b.finish(i)
    };
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(MapSum(vec![(0, id()), (0, id())]), vec![inp]);
    let g = b.finish(out);
    assert!(shape_of(&g, &Shape::Sum(vec![Shape::Int, Shape::Int])).is_err());
}
