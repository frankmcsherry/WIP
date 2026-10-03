use super::*;
use crate::value::Value;

fn u(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

/// reference for `gather_lanes`: index into `concat(variants)` by lane-start + offset.
fn oracle(variants: &[Value], tags: &[usize], off: &[usize]) -> Value {
    let mut start = vec![0usize; variants.len()];
    let mut acc = 0;
    for (t, v) in variants.iter().enumerate() {
        start[t] = acc;
        acc += v.len();
    }
    let idx: Vec<usize> = tags.iter().zip(off).map(|(&t, &o)| start[t] + o).collect();
    gather(&concat(variants), &idx)
}

/// `gather_lanes` must match the concat+gather oracle; `off` is the within-variant rank.
fn check(tags: &[usize], variants: Vec<Value>) {
    let mut cur = vec![0usize; variants.len()];
    let off: Vec<usize> = tags.iter().map(|&t| { let p = cur[t]; cur[t] += 1; p }).collect();
    let refs: Vec<Option<&Value>> = variants.iter().map(Some).collect();
    assert_eq!(gather_lanes(&refs, tags, &off), oracle(&variants, tags, &off));
}

/// `blend` takes a lane-wise path at fixed-width levels and the `gather_lanes` path elsewhere.
/// The two must agree exactly, including inside a MIXED product where one field takes each.
#[test]
fn blend_matches_the_gather_lanes_path() {
    // the general path, written out: row i from lane `mask[i] != 0`, at its own position.
    fn oracle(mask: &[u64], then: &Value, els: &Value) -> Value {
        let tags: Vec<usize> = mask.iter().map(|&m| (m != 0) as usize).collect();
        let off: Vec<usize> = (0..tags.len()).collect();
        gather_lanes(&[Some(els), Some(then)], &tags, &off)
    }
    let mask = [1u64, 0, 0, 1];
    let list = |ends: Vec<usize>, xs: &[u64]| Value::List(ends.into(), Box::new(u(xs)));

    // leaf, product of leaves, unit — the lane-wise path.
    for (t, e) in [
        (u(&[1, 2, 3, 4]), u(&[10, 20, 30, 40])),
        (
            Value::Prod(vec![u(&[1, 2, 3, 4]), Value::u8(vec![5, 6, 7, 8])]),
            Value::Prod(vec![u(&[9, 8, 7, 6]), Value::u8(vec![1, 2, 3, 4])]),
        ),
        (Value::Unit(4), Value::Unit(4)),
    ] {
        assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));
    }

    // a list (ragged rows: no constant slot) — the fallback path.
    let (t, e) = (list(vec![1, 3, 3, 6], &[1, 2, 3, 4, 5, 6]), list(vec![2, 2, 5, 5], &[7, 8, 9, 1, 2]));
    assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));

    // a MIXED product: field 0 blends lane-wise, field 1 falls back, and the result is the same.
    let t = Value::Prod(vec![u(&[1, 2, 3, 4]), list(vec![1, 3, 3, 6], &[1, 2, 3, 4, 5, 6])]);
    let e = Value::Prod(vec![u(&[9, 8, 7, 6]), list(vec![2, 2, 5, 5], &[7, 8, 9, 1, 2])]);
    assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));
}

/// Two sources of one sum shape, each using only one of its lanes (the other is an empty
/// column of the declared shape): the gather reads each row from its lane and the result
/// carries both lanes, whichever source comes first.
#[test]
fn gather_lanes_sums_using_different_lanes() {
    let a = Value::sum(vec![0, 0], vec![u(&[10, 11]), u(&[])]);
    let b = Value::sum(vec![1, 1], vec![u(&[]), u(&[20, 21])]);
    let (tags, off) = (vec![0usize, 1, 0, 1], vec![0usize, 0, 1, 1]);
    let out = gather_lanes(&[Some(&a), Some(&b)], &tags, &off);
    match &out {
        Value::Sum(t, lanes) => {
            assert_eq!(lanes.len(), 2);
            assert_eq!(t.tags_iter().collect::<Vec<_>>(), vec![0, 1, 0, 1]);
            assert_eq!(lanes[0], u(&[10, 11]));
            assert_eq!(lanes[1], u(&[20, 21]));
        }
        other => panic!("expected a Sum, got {other:?}"),
    }
    let flipped = gather_lanes(&[Some(&b), Some(&a)], &tags, &off);
    assert_eq!(flipped.len(), 4);
}

#[test]
fn gather_lanes_matches_concat_gather() {
    let tags = [0usize, 1, 0, 1, 0]; // t0 ×3, t1 ×2
    // leaf
    check(&tags, vec![u(&[10, 20, 30]), u(&[40, 50])]);
    // product
    check(
        &tags,
        vec![
            Value::Prod(vec![u(&[1, 2, 3]), u(&[4, 5, 6])]),
            Value::Prod(vec![u(&[7, 8]), u(&[9, 10])]),
        ],
    );
    // list payload (ragged spans, the recursive value gather)
    check(
        &tags,
        vec![
            Value::List(vec![2, 3, 6].into(), Box::new(u(&[1, 2, 3, 4, 5, 6]))),
            Value::List(vec![1, 3].into(), Box::new(u(&[7, 8, 9]))),
        ],
    );
    // sum payload (nested tags + within-offset remap)
    check(
        &tags,
        vec![
            Value::sum(vec![0, 1, 0], vec![u(&[1, 2]), u(&[3])]),
            Value::sum(vec![1, 0], vec![u(&[4]), u(&[5])]),
        ],
    );
    // empty
    check(&[], vec![u(&[]), u(&[])]);
}

/// out of range, `gather_or_zero` reads the zero of the element's shape: zero bits, the empty list,
/// a unit, lane 0 holding its own zero; in range it reads as `gather`. A sum of no lanes has no zero.
#[test]
fn gather_or_zero_reads_the_shapes_zero() {
    let leaf = Value::u64(vec![7, 8]);
    assert_eq!(gather_or_zero(&leaf, &[1, 5]).unwrap(), Value::u64(vec![8, 0]));
    let pair = Value::Prod(vec![Value::u64(vec![1, 2]), Value::u8(vec![3, 4])]);
    assert_eq!(gather_or_zero(&pair, &[9, 0]).unwrap(), Value::Prod(vec![Value::u64(vec![0, 1]), Value::u8(vec![0, 3])]));
    let lists = Value::List(vec![2, 3].into(), Box::new(Value::u64(vec![5, 6, 7])));
    assert_eq!(gather_or_zero(&lists, &[1, 4, 0]).unwrap(), Value::List(vec![1, 1, 3].into(), Box::new(Value::u64(vec![7, 5, 6]))));
    let sum = Value::sum(vec![1, 0], vec![Value::u64(vec![40]), Value::u64(vec![30])]);
    let got = gather_or_zero(&sum, &[0, 2, 1]).unwrap();
    assert_eq!(got, Value::sum(vec![1, 0, 0], vec![Value::u64(vec![0, 40]), Value::u64(vec![30])]));
    let none = Value::Sum(crate::value::Tags::Const(0, 0), Vec::new());
    assert!(gather_or_zero(&none, &[0]).is_err());
}
