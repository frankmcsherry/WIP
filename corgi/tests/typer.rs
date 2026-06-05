//! Shaper unit tests: a few positive shape results and the structural errors the
//! typer must reject at "compile" time (what `eval` would otherwise panic on).

use corgi::ArithOp::*;
use corgi::CmpOp::*;
use corgi::Op::*;
use corgi::{BinOp::*, Kind::*};
use corgi::{shape_of, Builder, NumOp, Shape};

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
    let sum = Shape::Sum(vec![Some(Shape::Prim(64)), Some(list(Shape::Prim(64)))]);
    assert_eq!(shape_of(&g, &sum).unwrap(), list(sum));
}

#[test]
fn transpose_reads_arity_off_input() {
    // works for any product width; here a 3-tuple
    let g = one(Transpose);
    let three = list(Shape::Prod(vec![Shape::Prim(64), Shape::Prim(64), Shape::Prim(64)]));
    assert_eq!(
        shape_of(&g, &three).unwrap(),
        Shape::Prod(vec![list(Shape::Prim(64)), list(Shape::Prim(64)), list(Shape::Prim(64))])
    );
}

#[test]
fn unwrap_homogeneous_ok_heterogeneous_errs() {
    let g = one(Unwrap);
    assert_eq!(shape_of(&g, &Shape::Sum(vec![Some(Shape::Prim(64)), Some(Shape::Prim(64))])).unwrap(), Shape::Prim(64));
    assert!(shape_of(&g, &Shape::Sum(vec![Some(Shape::Prim(64)), Some(list(Shape::Prim(64)))])).is_err());
}

#[test]
fn field_out_of_range_errs() {
    assert!(shape_of(&one(Field(0)), &Shape::Prim(64)).is_err());
    assert!(shape_of(&one(Field(5)), &Shape::Prod(vec![Shape::Prim(64), Shape::Prim(64)])).is_err());
}

#[test]
fn add_wrong_shape_errs() {
    let bad = Shape::Prod(vec![Shape::Prim(64), list(Shape::Prim(64))]);
    assert!(shape_of(&one(Bin(Add, U, 64)), &bad).is_err());
}

#[test]
fn find_mismatched_elements_errs() {
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(Find, vec![inp]);
    let g = b.finish(out);
    // needle List<U64> vs haystack List<(U64,U64)> — element types differ
    let bad = Shape::Prod(vec![list(Shape::Prim(64)), list(Shape::Prod(vec![Shape::Prim(64), Shape::Prim(64)]))]);
    assert!(shape_of(&g, &bad).is_err());
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
    assert!(shape_of(&g, &Shape::Sum(vec![Some(Shape::Prim(64)), Some(Shape::Prim(64))])).is_err());
}
