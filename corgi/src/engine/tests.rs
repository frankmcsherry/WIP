use super::*;
use crate::value::Value;

/// integers from their 64-bit words (so the full range, negatives included, is easy to write).
fn u(xs: &[u64]) -> Value {
    Value::i64(xs.iter().map(|&x| x as i64).collect())
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
    fn oracle(mask: &[u8], then: &Value, els: &Value) -> Value {
        let tags: Vec<usize> = mask.iter().map(|&m| (m != 0) as usize).collect();
        let off: Vec<usize> = (0..tags.len()).collect();
        gather_lanes(&[Some(els), Some(then)], &tags, &off)
    }
    let mask = [1u8, 0, 0, 1];
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
    let leaf = Value::i64(vec![7, 8]);
    assert_eq!(gather_or_zero(&leaf, &[1, 5]).unwrap(), Value::i64(vec![8, 0]));
    let pair = Value::Prod(vec![Value::i64(vec![1, 2]), Value::u8(vec![3, 4])]);
    assert_eq!(gather_or_zero(&pair, &[9, 0]).unwrap(), Value::Prod(vec![Value::i64(vec![0, 1]), Value::u8(vec![0, 3])]));
    let lists = Value::List(vec![2, 3].into(), Box::new(Value::i64(vec![5, 6, 7])));
    assert_eq!(gather_or_zero(&lists, &[1, 4, 0]).unwrap(), Value::List(vec![1, 1, 3].into(), Box::new(Value::i64(vec![7, 5, 6]))));
    let sum = Value::sum(vec![1, 0], vec![Value::i64(vec![40]), Value::i64(vec![30])]);
    let got = gather_or_zero(&sum, &[0, 2, 1]).unwrap();
    assert_eq!(got, Value::sum(vec![1, 0, 0], vec![Value::i64(vec![0, 40]), Value::i64(vec![30])]));
    let none = Value::Sum(crate::value::Tags::Const(0, 0), Vec::new());
    assert!(gather_or_zero(&none, &[0]).is_err());
    // a reference's zero is an empty row of its arena: in range, the arena is the one shared; out of
    // range, a new arena with one more, empty row (or the last row, when that is empty already)
    let arena = std::sync::Arc::new(Value::List(vec![2, 3].into(), Box::new(Value::i64(vec![5, 6, 7]))));
    let refs = Value::Ref(arena.clone(), std::sync::Arc::new(vec![1, 0]));
    let Value::Ref(same, _) = gather_or_zero(&refs, &[1, 0]).unwrap() else { panic!() };
    assert!(std::sync::Arc::ptr_eq(&same, &arena));
    let missed = gather_or_zero(&refs, &[0, 9]).unwrap();
    let Value::Ref(grown, rows) = &missed else { panic!() };
    assert_eq!((grown.len(), rows.to_vec()), (3, vec![1, 2]));
    assert_eq!(clone_ref(missed.clone()), Value::List(vec![1, 1].into(), Box::new(Value::i64(vec![7]))));
    let Value::Ref(again, rows) = gather_or_zero(&missed, &[5]).unwrap() else { panic!() };
    assert!(std::sync::Arc::ptr_eq(&again, grown) && rows.to_vec() == vec![2], "an empty last row is reused");
}

/// `compress` takes the kept rows by whichever path fits how many there are, and either way they are
/// the rows `gather` takes at the kept positions: few kept and most kept, over a product of a leaf,
/// a list, a sum and a reference.
#[test]
fn compress_keeps_what_gather_at_the_kept_positions_does() {
    let n = 40;
    // rows of lengths 0, 1, 2, ..: row i ends at i(i+1)/2
    let ends: Vec<usize> = (1..=n).map(|i| i * (i - 1) / 2).collect();
    let lists = Value::List(ends.clone().into(), Box::new(u(&(0..ends[n - 1] as u64).collect::<Vec<_>>())));
    let sum = Value::sum((0..n).map(|i| i % 2).collect(), vec![u(&(0..20).collect::<Vec<_>>()), Value::u8((0..20).collect())]);
    let refs = take_ref(lists.clone());
    let v = Value::Prod(vec![u(&(0..n as u64).collect::<Vec<_>>()), lists, sum, refs]);
    for mask in [(0..n).map(|i| (i == 3 || i == 30) as u8).collect::<Vec<_>>(), (0..n).map(|i| (i % 5 != 0) as u8).collect()] {
        let kept: Vec<usize> = (0..n).filter(|&i| mask[i] != 0).collect();
        assert_eq!(compress(&v, &mask, kept.len()), gather(&v, &kept));
    }
}
/// `gather` element by element, as it was before list rows moved a range at a time: the reference
/// the range copies must match. (A sum's arm is unchanged, so it is `gather`'s own.)
fn by_positions(v: &Value, idx: &[usize]) -> Value {
    match v {
        Value::Prim(p) => Value::Prim(p.gather(idx)),
        Value::Prod(cols) => Value::Prod(cols.iter().map(|c| by_positions(c, idx)).collect()),
        Value::Unit(_) => Value::Unit(idx.len()),
        Value::List(bounds, vals) => {
            let (mut elem, mut ends) = (Vec::new(), Vec::new());
            for &i in idx {
                let (s, e) = bounds.span(i);
                elem.extend(s..e);
                ends.push(elem.len());
            }
            Value::List(ends.into(), Box::new(by_positions(vals, &elem)))
        }
        Value::Ref(list, rows) => Value::Ref(list.clone(), Arc::new(idx.iter().map(|&i| rows[i]).collect())),
        Value::Sum(..) => gather(v, idx),
    }
}

/// Lists of every kind of element, moved by `gather`, `gather_lanes` and `blend` a range
/// of rows at a time, agree with moving them element by element: rows taken in order, repeated,
/// reversed and empty, at several depths.
#[test]
fn range_copies_match_element_positions() {
    // equal as values: references compare by the rows they name
    let same = |a: Value, b: Value| assert_eq!(clone_ref(a), clone_ref(b));
    let bytes = |ends: Vec<usize>, xs: &[u8]| Value::List(ends.into(), Box::new(Value::u8(xs.to_vec())));
    let strings = bytes(vec![2, 2, 5, 6, 6, 9], &[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let arena = Arc::new(bytes(vec![1, 3, 6], &[1, 2, 3, 4, 5, 6]));
    // rows long enough to be copied a range at a time, rather than gathered by position
    let long = |ends: &[usize]| Value::List(ends.to_vec().into(), Box::new(Value::u8((0..*ends.last().unwrap()).map(|i| i as u8).collect())));
    let long_strings = long(&[20, 20, 37, 67, 67, 92]);
    let values = [
        strings.clone(),
        long_strings.clone(),
        // a list of (number, long string) pairs, and lists of long strings
        Value::List(vec![1, 3, 3, 4, 6, 6].into(), Box::new(Value::Prod(vec![u(&[1, 2, 3, 4, 5, 6]), long_strings.clone()]))),
        Value::List(vec![2, 3, 3, 4, 6, 6].into(), Box::new(long_strings.clone())),
        // a list of (number, string) pairs
        Value::List(vec![1, 3, 3, 4, 6, 6].into(), Box::new(Value::Prod(vec![u(&[1, 2, 3, 4, 5, 6]), strings.clone()]))),
        // a stride list of pairs
        Value::List(Bounds::Stride(2, 6), Box::new(Value::Prod(vec![u(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]), Value::u8((0..12).collect())]))),
        // a list of lists of strings
        Value::List(vec![2, 3, 3, 4, 6, 6].into(), Box::new(strings.clone())),
        // a list of sums with a string lane
        Value::List(vec![1, 2, 2, 4, 5, 6].into(), Box::new(Value::sum(vec![0, 1, 1, 0, 1, 0], vec![u(&[7, 8, 9]), bytes(vec![1, 1, 3], &[4, 5, 6])]))),
        // a list of references
        Value::List(vec![2, 2, 3, 5, 6, 6].into(), Box::new(Value::Ref(arena, Arc::new(vec![2, 0, 1, 1, 2, 0])))),
    ];
    let idx = [3, 4, 5, 0, 1, 1, 2, 5, 4, 0];
    let mask = [1u8, 1, 0, 1, 1, 1];
    for v in &values {
        same(gather(v, &idx), by_positions(v, &idx));
        // two sources: row k of the second is row k of `v` reversed, so a row can come from either
        let other = gather(v, &[5, 4, 3, 2, 1, 0]);
        let (tags, off) = ([0usize, 0, 1, 1, 0, 1, 1], [0usize, 1, 2, 3, 4, 4, 0]);
        let both = concat(&[v.clone(), other.clone()]);
        let at: Vec<usize> = tags.iter().zip(&off).map(|(&t, &o)| t * 6 + o).collect();
        same(gather_lanes(&[Some(v), Some(&other)], &tags, &off), by_positions(&both, &at));
        let picks: Vec<usize> = mask.iter().enumerate().map(|(i, &m)| if m != 0 { 6 + i } else { i }).collect();
        same(blend(&mask, other.clone(), v.clone()), by_positions(&concat(&[v.clone(), other]), &picks));
    }
    // lists held at different storages meet at `i64`
    let words = Value::List(vec![2, 2, 5, 6, 6, 9].into(), Box::new(u(&[1, 2, 300, 4, 5, 6, 7, 8, 9])));
    let picks: Vec<usize> = mask.iter().enumerate().map(|(i, &m)| if m != 0 { 6 + i } else { i }).collect();
    same(blend(&mask, words.clone(), strings.clone()), by_positions(&concat(&[strings, words]), &picks));
}
