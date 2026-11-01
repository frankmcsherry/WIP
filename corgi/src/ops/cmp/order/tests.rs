use super::*;
use crate::engine::gather;
use crate::arrange::sort_blocks;

/// integers from their 64-bit words (so the full range, negatives included, is easy to write).
fn u(xs: &[u64]) -> Value {
    Value::i64(xs.iter().map(|&x| x as i64).collect())
}

/// the obviously-correct scalar reference: structural order of row `i` of `a` vs row `j` of `b`,
/// recursing through the type (leaf, Prod field-by-field, List lexicographic, Sum tag-then-payload).
/// The Sum arm recovers each row's within-variant offset by a prefix scan — O(i), so this is the
/// O(n²) standard the bulk `compare_idx` is checked against, and the order `sort` must materialise.
fn compare2(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
    match (a, b) {
        // i8 sign back to the oracle's `Ordering` (the one i8→Ordering boundary, test-only).
        (Value::Prim(pa), Value::Prim(pb)) => pa.cmp_idx(&[i], &[j], pb)[0].cmp(&0),
        (Value::Prod(ca), Value::Prod(cb)) => {
            for (x, y) in ca.iter().zip(cb) {
                match compare2(x, i, y, j) {
                    Ordering::Equal => continue,
                    o => return o,
                }
            }
            Ordering::Equal
        }
        (Value::List(ab, av), Value::List(bb, bv)) => {
            let (si, ei) = crate::engine::row_span(ab, i);
            let (sj, ej) = crate::engine::row_span(bb, j);
            let (li, lj) = (ei - si, ej - sj);
            // lexicographic: the first differing element decides; a proper prefix sorts first.
            for k in 0..li.min(lj) {
                match compare2(av, si + k, bv, sj + k) {
                    Ordering::Equal => continue,
                    o => return o,
                }
            }
            li.cmp(&lj)
        }
        (Value::Sum(ta, va), Value::Sum(tb, vb)) => {
            let (tav, tbv): (Vec<usize>, Vec<usize>) =
                (ta.tags_iter().collect(), tb.tags_iter().collect());
            let (ti, tj) = (tav[i], tbv[j]);
            match ti.cmp(&tj) {
                Ordering::Equal => {
                    let wi = tav[..i].iter().filter(|&&t| t == ti).count();
                    let wj = tbv[..j].iter().filter(|&&t| t == ti).count();
                    compare2(&va[ti], wi, &vb[ti], wj)
                }
                o => o,
            }
        }
        _ => panic!("compare2: shape mismatch"),
    }
}

/// `compare_cols` must match the scalar `compare2` lane for lane — same contract, bulk path.
fn agree_cmp(a: &Value, b: &Value) {
    let got = compare_cols(a, b);
    let want: Vec<i8> = (0..a.len()).map(|i| compare2(a, i, b, i) as i8).collect();
    assert_eq!(got, want);
    // asked only for equality, the same pairs are equal
    let eq: Vec<bool> = equal_cols(a, b).iter().map(|&o| o == 0).collect();
    assert_eq!(eq, want.iter().map(|&o| o == 0).collect::<Vec<_>>(), "equal_cols");
}

/// The implicit pair forms must answer exactly what the same pairs written out do — they are a
/// cheaper way to SAY the pairs, not a different comparison. Checked over every shape the
/// comparator recurses through, since only the entry is implicit and each arm has to carry it.
#[test]
fn implicit_pairs_match_explicit_ones() {
    let shapes = [
        u(&[5, 3, 3, 8, 1]),
        Value::Prod(vec![u(&[1, 1, 1, 2, 2]), u(&[7, 7, 9, 0, 0])]),
        Value::sum(vec![0, 0, 1, 1, 0], vec![u(&[4, 4, 6]), u(&[2, 2])]),
        Value::List(vec![1, 3, 3, 6, 6].into(), Box::new(u(&[9, 1, 1, 5, 5, 5]))),
        Value::Unit(5),
    ];
    for v in shapes {
        let n = v.len();
        let id: Vec<usize> = (0..n).collect();
        assert_eq!(compare_cols(&v, &v), compare_idx(&v, &v, &id, &id), "diagonal");
        // adjacent pairs are asked only whether they are equal
        let zero = |o: Vec<i8>| o.into_iter().map(|s| s == 0).collect::<Vec<_>>();
        assert_eq!(
            zero(compare_adjacent(&v)),
            zero(compare_idx(&v, &v, &id[..n - 1], &id[1..])),
            "adjacent"
        );
    }
    // an empty column has no pairs either way (the adjacent count must not underflow).
    assert!(compare_adjacent(&u(&[])).is_empty());
    assert!(compare_adjacent(&u(&[7])).is_empty());
}

#[test]
fn payload_free_product_fields_do_not_change_comparison() {
    let first = u(&[2, 1, 2, 1, 2]);
    let second = u(&[4, 8, 3, 8, 4]);
    let plain = Value::Prod(vec![first.clone(), second.clone()]);
    let padded = Value::Prod(vec![
        Value::Unit(5),
        first,
        Value::Prod(vec![Value::Unit(5), Value::Unit(5)]),
        second,
        Value::Unit(5),
    ]);
    let left = [0usize, 1, 2, 3, 4, 0, 4];
    let right = [4usize, 3, 0, 1, 2, 1, 3];
    assert_eq!(compare_idx(&padded, &padded, &left, &right), compare_idx(&plain, &plain, &left, &right));
    assert_eq!(equal_idx(&padded, &padded, &left, &right), equal_idx(&plain, &plain, &left, &right));
    assert_eq!(compare_adjacent(&padded), compare_adjacent(&plain));

    let lists = Value::List(
        vec![0, 1, 3, 3, 6].into(),
        Box::new(Value::Prod(vec![Value::Unit(6), Value::Unit(6)])),
    );
    let lengths = Value::i64(vec![0, 1, 2, 0, 3]);
    assert_eq!(compare_idx(&lists, &lists, &left, &right), compare_idx(&lengths, &lengths, &left, &right));
}

#[test]
fn compare_cols_matches_scalar() {
    // prim
    agree_cmp(&u(&[5, 3, 8, 1, 9]), &u(&[5, 4, 2, 1, 0]));
    // product: lexicographic fold over fields
    agree_cmp(
        &Value::Prod(vec![u(&[2, 1, 2, 1]), u(&[10, 20, 5, 30])]),
        &Value::Prod(vec![u(&[2, 1, 1, 1]), u(&[10, 25, 5, 30])]),
    );
    // sum: equal-tag lanes hit the payload compare, unequal-tag lanes the tag order.
    agree_cmp(
        &Value::sum(vec![0, 1, 0, 1, 0], vec![u(&[5, 7, 9]), u(&[2, 4])]),
        &Value::sum(vec![0, 1, 1, 1, 0], vec![u(&[5, 8]), u(&[2, 3, 1])]),
    );
    // list: position-wise first difference over ragged rows, then a proper prefix first
    agree_cmp(
        &Value::List(vec![2, 2, 5, 6].into(), Box::new(u(&[3, 1, 4, 5, 9, 0]))),
        &Value::List(vec![2, 3, 6, 7].into(), Box::new(u(&[3, 2, 7, 4, 5, 1, 0]))),
    );
    // nested: a sum in secondary product position (the within-offset remap under a fold)
    agree_cmp(
        &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 1, 0], vec![u(&[5, 8]), u(&[3])])]),
        &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 0, 1], vec![u(&[5, 9]), u(&[3])])]),
    );
    // nested: a sum AS the list element — the position loop gathers sum rows and remaps offsets.
    agree_cmp(
        &Value::List(vec![2, 4].into(), Box::new(Value::sum(vec![0, 1, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]))),
        &Value::List(vec![2, 4].into(), Box::new(Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 7]), u(&[2, 9])]))),
    );
}

#[test]
fn compare_idx_cross_pairs() {
    // arbitrary (i,j) pairs — the find/probe path, with a sum (cross within-offsets) and a list.
    let a = Value::sum(vec![0, 1, 0, 1, 0], vec![u(&[5, 7, 9]), u(&[2, 4])]);
    let b = Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 8]), u(&[2, 9])]);
    let (ia, ib) = (&[0usize, 2, 4, 1, 3], &[3usize, 1, 0, 2, 0]);
    let got = compare_idx(&a, &b, ia, ib);
    let want: Vec<i8> = ia.iter().zip(ib).map(|(&i, &j)| compare2(&a, i, &b, j) as i8).collect();
    assert_eq!(got, want);

    let la = Value::List(vec![2, 2, 5, 6].into(), Box::new(u(&[3, 1, 4, 5, 9, 0])));
    let lb = Value::List(vec![2, 3, 6, 7].into(), Box::new(u(&[3, 2, 7, 4, 5, 1, 0])));
    let (ja, jb) = (&[3usize, 0, 2, 1], &[3usize, 0, 2, 1]);
    let got = compare_idx(&la, &lb, ja, jb);
    let want: Vec<i8> = ja.iter().zip(jb).map(|(&i, &j)| compare2(&la, i, &lb, j) as i8).collect();
    assert_eq!(got, want);
}

#[test]
fn public_indexed_comparison_across_storage() {
    use std::sync::Arc;
    use crate::arrange::{compare_idx, equal_idx};
    let lists = Value::List(vec![0, 1, 3, 4].into(), Box::new(Value::i64(vec![1, 1, 2, -3])));
    let refs = Value::Ref(Arc::new(lists.clone()), Arc::new(vec![3, 0, 2, 1]));
    let other = Value::List(vec![0, 1, 4, 5].into(), Box::new(Value::u8(vec![1, 1, 2, 3, 3])));
    let columns = [&lists, &refs, &other];
    let rows = [vec![vec![], vec![1i64], vec![1, 2], vec![-3]],
                vec![vec![-3], vec![], vec![1, 2], vec![1]],
                vec![vec![], vec![1], vec![1, 2, 3], vec![3]]];
    let ia: Vec<_> = (0..4).flat_map(|i| [i; 4]).collect();
    let ib: Vec<_> = (0..4).cycle().take(16).collect();
    for (x, a) in columns.iter().enumerate() { for (y, b) in columns.iter().enumerate() {
        let expected: Vec<_> = ia.iter().zip(&ib).map(|(&i, &j)| rows[x][i].cmp(&rows[y][j]) as i8).collect();
        assert_eq!(compare_idx(a, b, &ia, &ib), expected);
        assert_eq!(equal_idx(a, b, &ia, &ib).iter().map(|&s| s == 0).collect::<Vec<_>>(),
                   expected.iter().map(|&s| s == 0).collect::<Vec<_>>());
        assert!(equal_idx(a, b, &[], &[]).is_empty());
        assert!(compare_idx(a, b, &[], &[]).is_empty());
    }}

    let bytes = Value::u8(vec![0, 1, 127, 255]);
    let ints = Value::i64(vec![-1, 1, 127, i64::MAX]);
    assert_eq!(compare_idx(&bytes, &ints, &[0, 1, 2, 3], &[0, 1, 2, 3]), [1, 0, 0, -1]);
    assert_eq!(compare_cols(&bytes, &ints), [1, 0, 0, -1]);
    assert_eq!(compare_cols(&ints, &bytes), [-1, 0, 0, 1]);
    assert_eq!(equal_idx(&ints, &bytes, &[1, 2, 3], &[1, 2, 3]).iter()
        .map(|&s| s == 0).collect::<Vec<_>>(), [true, true, false]);
}

#[test]
#[should_panic(expected = "compare_idx: pair count")]
fn public_indexed_comparison_requires_matching_index_counts() {
    crate::arrange::compare_idx(&Value::Unit(1), &Value::Unit(1), &[0], &[]);
}

#[test]
#[should_panic(expected = "equal_idx: pair count")]
fn indexed_equality_requires_matching_index_counts() {
    crate::arrange::equal_idx(&Value::Unit(1), &Value::Unit(1), &[0], &[]);
}

#[test]
fn compare_cols_sum_at_scale() {
    // many tagged rows; the bulk path must match the (here O(n²)) scalar reference.
    let n = 300usize;
    let mk = |tags: Vec<usize>| -> Value {
        let vars: Vec<Value> = (0..3)
            .map(|t| {
                let c = tags.iter().filter(|&&x| x == t).count() as u64;
                u(&(0..c).map(|k| (k.wrapping_mul(2654435761) >> 5) % 50).collect::<Vec<_>>())
            })
            .collect();
        Value::sum(tags, vars)
    };
    let ta: Vec<usize> = (0..n).map(|i| i % 3).collect();
    let tb: Vec<usize> = (0..n).map(|i| (i % 2) * 2).collect(); // tags 0 or 2
    agree_cmp(&mk(ta), &mk(tb));
}

/// reference order: comparison sort by `compare2`, then materialize.
fn reference(v: &Value) -> Value {
    let mut idx: Vec<usize> = (0..v.len()).collect();
    idx.sort_by(|&a, &b| compare2(v, a, v, b));
    gather(v, &idx)
}
/// discrimination must agree with the reference on the sorted VALUES (equal rows may permute differently,
/// but materialise identically).
fn agree(v: &Value) {
    assert_eq!(gather(v, &sort_blocks(&[], v).0), reference(v));
}

#[test]
fn leaf() {
    agree(&u(&[5, 3, 8, 1, 3, 9, 2, 3]));
}

#[test]
fn every_storage() {
    // byte, i64 (negatives included) and float leaves sort/gather/compare through the same
    // storage-generic kernel; each must agree with the compare2 reference, alone and inside a product.
    agree(&Value::u8(vec![5, 3, 8, 1, 3, 9, 2]));
    agree(&Value::i64(vec![500, -30, 800, i64::MIN, -30, 30, i64::MAX]));
    agree(&Value::f64(vec![0.5, -0.0, 0.0, f64::NAN, -1.5, 0.5]));
    agree(&Value::Prod(vec![Value::u8(vec![2, 1, 2, 1]), Value::i64(vec![10, -20, 5, 30])]));
}

#[test]
fn product_lex() {
    agree(&Value::Prod(vec![u(&[2, 1, 2, 1, 3, 1]), u(&[10, 20, 5, 30, 7, 20])]));
}

#[test]
fn sum_by_tag_then_payload() {
    // the quadratic case: rows t0=5, t1=1, t0=3, t1=4, t0=9, t1=1
    agree(&Value::sum(vec![0, 1, 0, 1, 0, 1], vec![u(&[5, 3, 9]), u(&[1, 4, 1])]));
}

#[test]
fn list_lexicographic() {
    // rows [3,1,2], [], [5], [9,0] — sorted element-wise, a proper prefix first
    agree(&Value::List(vec![3, 3, 4, 6].into(), Box::new(u(&[3, 1, 2, 5, 9, 0]))));
}

#[test]
fn prod_of_sum() {
    let sums = Value::sum(vec![0, 1, 0, 1], vec![u(&[7, 9]), u(&[3, 4])]);
    agree(&Value::Prod(vec![u(&[2, 1, 2, 1]), sums]));
}

#[test]
fn list_of_sum_fully_discriminated() {
    // List<Sum> — structural all the way down.
    let inner = Value::sum(vec![1, 0, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]);
    agree(&Value::List(vec![2, 4].into(), Box::new(inner)));
}

#[test]
fn variable_length_lists_at_scale() {
    // many u64-list rows of differing length — exercises the list arm and its position recursion, rows
    // ending at every position; must agree with the compare2 reference.
    let m = 200u64;
    let mut bounds = Vec::new();
    let mut vals = Vec::new();
    let mut acc = 0usize;
    for i in 0..m {
        let len = (i.wrapping_mul(2654435761) >> 5) % 5; // 0..4
        for j in 0..len {
            vals.push((i.wrapping_mul(40503) ^ j) % 7);
        }
        acc += len as usize;
        bounds.push(acc);
    }
    agree(&Value::List(bounds.into(), Box::new(u(&vals))));
}

#[test]
fn radix_full_range_at_scale() {
    // full 64-bit values force all 8 byte-passes; large n; must match the reference.
    let xs: Vec<u64> = (0..500u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (i << 31)).collect();
    agree(&u(&xs));
}

#[test]
fn scrambled_at_scale() {
    let xs: Vec<u64> = (0..500u64).map(|i| (i.wrapping_mul(2654435761) ^ (i << 13)) % 50).collect();
    agree(&u(&xs));
    let ys: Vec<u64> = (0..500u64).map(|i| i.wrapping_mul(40503) % 7).collect();
    agree(&Value::Prod(vec![u(&xs), u(&ys)]));
}

#[test]
fn labels_mark_runs() {
    // sorted [1,1,3,4,5] → run labels [0,0,1,2,3]
    let seed = vec![0u64; 5];
    let (_perm, labels) = sort_blocks(&seed, &u(&[3, 1, 4, 1, 5]));
    assert_eq!(labels, vec![0, 0, 1, 2, 3]);
}

