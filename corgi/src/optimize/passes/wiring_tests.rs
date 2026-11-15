use super::*;
use crate::{compile, eval_graph, parse_ml, shape_of, Shape};

fn compiled(src: &str, shape: &Shape) -> Graph<NumOp> {
    let g = parse_ml(src).unwrap();
    compile(g, shape).unwrap().graph().clone()
}

#[test]
fn tuple_eta_uses_checked_arity_and_unit_fields() {
    let shape = Shape::Prod(vec![Shape::Int, Shape::Unit]);
    for src in ["(input.0, input.1)", "let (a,b)=input in (a,b)", "(input.0, ())"] {
        let g = compiled(src, &shape);
        assert_eq!(g.nodes.len(), 1, "{src}");
        assert_eq!(shape_of(&g, &shape).unwrap(), shape);
    }
}

#[test]
fn unit_field_becomes_unit() {
    let g = compiled("input.1", &Shape::Prod(vec![Shape::Int, Shape::Unit]));
    assert!(matches!(g.nodes[g.output].kind, NodeKind::Op(NumOp::Core(Op::Unit))));
    assert_eq!(eval_graph(&g, Value::Prod(vec![Value::i64(vec![7,8]), Value::Unit(2)])), Value::Unit(2));
}

#[test]
fn identity_list_map_disappears_in_closed_bodies_too() {
    let shape = Shape::List(Box::new(Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])))));
    let g = compiled("input map (xs -> xs map ((a,b) -> (a,b)))", &shape);
    assert_eq!(g.nodes.len(), 1);
}

#[test]
fn products_keep_their_visible_arity_and_unit_slots() {
    let shape = Shape::Prod(vec![Shape::Int, Shape::Unit]);
    for (src, expected) in [
        ("(input.0)", Shape::Prod(vec![Shape::Int])),
        ("(input.1)", Shape::Prod(vec![Shape::Unit])),
        ("(input.1, input.0)", Shape::Prod(vec![Shape::Unit, Shape::Int])),
        ("(input.0, input.0)", Shape::Prod(vec![Shape::Int, Shape::Int])),
    ] { assert_eq!(shape_of(&compiled(src, &shape), &shape).unwrap(), expected); }
}

#[test]
fn typed_wiring_is_idempotent_and_checks_dead_code() {
    let shape = Shape::List(Box::new(Shape::Prod(vec![Shape::Int, Shape::Unit])));
    for src in ["input map ((a,b) -> (a,()))", "input map ((a,b) -> (a,19) mul) len", "input map (x -> x.0) map (x -> (x,19) mul) len"] {
        let wiring = typed_wiring(parse_ml(src).unwrap(), &shape).unwrap();
        assert_eq!(typed_wiring(wiring.clone(), &shape).unwrap(), wiring);
        let g = compiled(src, &shape);
        assert_eq!(compiled_graph_again(&g, &shape), g);
    }
    assert!(compile(parse_ml("let bad=input.2 in ()").unwrap(), &Shape::Prod(vec![Shape::Int])).is_err());
}

fn compiled_graph_again(g: &Graph<NumOp>, shape: &Shape) -> Graph<NumOp> {
    compile(g.clone(), shape).unwrap().graph().clone()
}
