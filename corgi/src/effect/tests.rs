use super::*;
use crate::frontend::parse_ml;
use crate::graph::{eval_graph, shape_of};
use crate::shape::Shape;
use crate::value::{show, Value};

fn run(src: &str, n: u64) -> String {
    let g = parse_ml(src).unwrap();
    let lowered = lower_effects(&g);
    // the lowered program is well-typed in the pure vocabulary.
    shape_of(&lowered, &Shape::Prim(64)).unwrap_or_else(|e| panic!("{src}: {e}"));
    show(&eval_graph(&lowered, Value::u64(vec![n])))
}

#[test]
fn get_then_lifted_add() {
    // `head` fails on the empty row; the `add` downstream runs on the Ok lane only.
    assert_eq!(run("(input iota head, 10u64) add", 3), "Sum tags=[0] [[10], ()x0]");
    assert_eq!(run("(input iota head, 10u64) add", 0), "Sum tags=[1] [[], ()x1]");
}

#[test]
fn tuple_hoists_a_fallible_field() {
    // (pure, fallible) -> a row errs iff the fallible field does.
    let src = "let xs = input iota in (xs len, xs head) add";
    assert_eq!(run(src, 3), "Sum tags=[0] [[3], ()x0]");
    assert_eq!(run(src, 0), "Sum tags=[1] [[], ()x1]");
}

#[test]
fn maplist_hoists_a_fallible_body() {
    // each element x becomes [0..x) head: x=0 errs its element, so the whole row errs.
    let src = "input iota map (x -> x iota head)";
    assert_eq!(run(src, 0), "Sum tags=[0] [List ends=[0] <[]>, ()x0]"); // no elements: Ok
    assert_eq!(run(src, 3), "Sum tags=[1] [List ends=[] <[]>, ()x1]"); // element 0 errs
    let src = "(input, 1u64) add iota map (x -> (x, 1u64) add iota head)";
    assert_eq!(run(src, 2), "Sum tags=[0] [List ends=[3] <[0, 0, 0]>, ()x0]");
}

#[test]
fn try_erases_and_discharges() {
    let g = parse_ml("input iota head try").unwrap();
    assert!(is_total(&g));
    assert!(!is_total(&parse_ml("input iota head").unwrap()));
    assert_eq!(run("input iota head try", 0), "Sum tags=[1] [[], ()x1]");
    // matching on the revealed sum is ordinary pure code again.
    let src = "input iota head try match (0 (x -> (x, 100u64) add), 1 (u -> 7u64))";
    assert_eq!(run(src, 0), "[7]");
    assert_eq!(run(src, 5), "[100]");
}

#[test]
fn fold_with_a_fallible_body_errs_the_row() {
    // fold over [0..n): the body reads element `x` of a length-3 list, so x >= 3 errs the row.
    let src = "(0u64, input iota) fold ((acc, x) -> ((x, 3u64 iota) get, acc) add)";
    assert_eq!(run(src, 3), "Sum tags=[0] [[3], ()x0]"); // 0+1+2
    assert_eq!(run(src, 4), "Sum tags=[1] [[], ()x1]"); // x=3 out of range
}

#[test]
fn nested_fallible_ops_squash_flat() {
    // two fallible stages in a row stay one Fail layer deep.
    let src = "input iota head iota head";
    let g = parse_ml(src).unwrap();
    let lowered = lower_effects(&g);
    let s = shape_of(&lowered, &Shape::Prim(64)).unwrap();
    assert_eq!(s.to_string(), "{U64 | ()}");
    assert_eq!(run(src, 0), "Sum tags=[1] [[], ()x1]");
    assert_eq!(run(src, 1), "Sum tags=[1] [[], ()x1]"); // head of [0] is 0; [0..0) is empty
    assert_eq!(run(src, 2), "Sum tags=[1] [[], ()x1]"); // head of [0,1] is 0 again
}
