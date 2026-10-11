use super::*;
use crate::value::Bounds;

fn u(xs: &[i64]) -> Value {
    Value::i64(xs.to_vec())
}
fn h(v: &Value) -> Vec<u64> {
    hash(v)
}

#[test]
fn deterministic() {
    let v = Value::Prod(vec![u(&[1, 2, 3]), u(&[9, 8, 7])]);
    assert_eq!(h(&v), h(&v));
}

#[test]
fn equal_rows_hash_equal() {
    // duplicate rows in a column must produce identical ids (the equal-for-equal contract).
    let v = Value::Prod(vec![u(&[5, 5, 6]), u(&[7, 7, 7])]);
    let hs = h(&v);
    assert_eq!(hs[0], hs[1]); // rows (5,7) and (5,7)
    assert_ne!(hs[0], hs[2]); // vs (6,7)
}

#[test]
fn storage_invariant() {
    // the id addresses the value, not its storage: the same integers held as bytes or as i64s
    // hash identically (the fold reads every integer's word). Lets a two-input join match keys
    // carried at different storages.
    assert_eq!(h(&Value::u8(vec![5])), h(&Value::i64(vec![5])));
    assert_eq!(h(&Value::u8(vec![0, 200, 255])), h(&Value::i64(vec![0, 200, 255])));
}

#[test]
fn lists_of_leaves_are_storage_invariant() {
    // a list of integers hashes by its values: held as bytes or as i64s, the same rows agree
    let ends = vec![2, 2, 5];
    let bytes = Value::List(Bounds::offsets(ends.clone()), Box::new(Value::u8(vec![1, 2, 0, 200, 255])));
    let words = Value::List(Bounds::offsets(ends.clone()), Box::new(u(&[1, 2, 0, 200, 255])));
    assert_eq!(h(&bytes), h(&words));
    // eight small integers and one large integer with the same little-endian bytes are different
    // lists, and hash apart
    let eight = Value::List(Bounds::offsets(vec![8]), Box::new(Value::u8(vec![1, 1, 0, 0, 0, 0, 0, 0])));
    let one = Value::List(Bounds::offsets(vec![1]), Box::new(u(&[257])));
    assert_ne!(h(&eight), h(&one));
    // a list of one element is not the element, nor the empty list
    assert_ne!(h(&Value::List(Bounds::offsets(vec![1]), Box::new(u(&[5])))), h(&u(&[5])));
    assert_ne!(
        h(&Value::List(Bounds::offsets(vec![0]), Box::new(Value::u8(vec![])))),
        h(&Value::List(Bounds::offsets(vec![0]), Box::new(Value::f64(vec![])))),
    );
}

#[test]
fn stride_and_offsets_agree() {
    // representation-independence: the DEEP invariant for stable ids. A uniform list carried as a
    // `Stride` must hash the same as the equivalent end-offset form (they are `Bounds`-equal).
    let vals = u(&[0, 1, 2, 3, 4, 5]);
    let strided = Value::List(Bounds::Stride(2, 3), Box::new(vals.clone()));
    let offsets = Value::List(Bounds::offsets(vec![2, 4, 6]), Box::new(vals));
    assert_eq!(h(&strided), h(&offsets));
}

#[test]
fn structure_disambiguates() {
    // length-first folding separates lists of different length/content and different nestings.
    let lists = Value::List(
        Bounds::offsets(vec![0, 1, 3]),
        Box::new(u(&[9, 9, 9])),
    ); // rows [], [9], [9,9]
    let hs = h(&lists);
    assert_ne!(hs[0], hs[1]);
    assert_ne!(hs[1], hs[2]);
    // an empty list row and a Unit row (both "empty") must not collide.
    assert_ne!(hs[0], h(&Value::Unit(1))[0]);
}

#[test]
fn field_order_matters() {
    // (a,b) and (b,a) are distinct products, so their hashes differ (combine is order-sensitive).
    let ab = Value::Prod(vec![u(&[1]), u(&[2])]);
    let ba = Value::Prod(vec![u(&[2]), u(&[1])]);
    assert_ne!(h(&ab), h(&ba));
}

#[test]
fn sum_tag_and_payload() {
    // same payload value under different tags must differ; same tag+payload must agree.
    let s = Value::sum(vec![0, 1, 0], vec![u(&[5, 5]), u(&[5])]);
    let hs = h(&s);
    assert_ne!(hs[0], hs[1]); // tag 0 val 5  vs  tag 1 val 5
    assert_eq!(hs[0], hs[2]); // both tag 0 val 5
}

/// The id addresses the CONTENT of a row, not the buffer holding it: distinct `Arc`s and an
/// over-capacity backing vector must not change it.
#[test]
fn ignores_arc_identity_and_capacity() {
    let a = u(&[7, 8, 9]);
    let mut backing = Vec::with_capacity(64);
    backing.extend_from_slice(&[7i64, 8, 9]);
    let b = Value::Prim(crate::value::Prim::I64(std::sync::Arc::new(backing)));
    assert_eq!(h(&a), h(&b));
}

/// ...nor the row's POSITION: reordering a column carries each row's id with it. This is the
/// property the dataflow boundary rests on — a row keeps its id across a gather, a merge, or a
/// round trip through another operator.
#[test]
fn permutation_is_position_independent() {
    let v = Value::Prod(vec![
        Value::i64(vec![3, 1, 4, 1, 5, 9]),
        Value::u8(vec![30, 10, 40, 10, 50, 90]),
    ]);
    let perm = vec![5usize, 0, 3, 2, 1, 4];
    let base = h(&v);
    let permuted = h(&crate::engine::gather(&v, &perm));
    for (k, &p) in perm.iter().enumerate() {
        assert_eq!(permuted[k], base[p]);
    }
}

/// One pass over every constructor: a nested product, a sum with an unused lane, a ragged list,
/// and a unit column whose rows are all identical.
#[test]
fn covers_prim_prod_sum_list_unit() {
    let prod = Value::Prod(vec![
        Value::u8(vec![1, 2, 3]),
        Value::Prod(vec![Value::i64(vec![10, 20, 30]), Value::i64(vec![-100, 200, 300])]),
    ]);
    let hp = h(&prod);
    assert_eq!(hp.len(), 3);
    assert!(hp[0] != hp[1] && hp[1] != hp[2]);

    // rows 0 and 2 both land in lane 1, with different payloads.
    let s = Value::sum(
        vec![1, 2, 1, 2],
        vec![Value::u8(vec![]), Value::i64(vec![5, 7]), Value::i64(vec![9, 11])],
    );
    let hs = h(&s);
    assert_eq!(hs.len(), 4);
    assert_ne!(hs[0], hs[2]);

    let list = Value::List(Bounds::offsets(vec![2, 2, 5]), Box::new(Value::u8(vec![1, 2, 3, 4, 5])));
    let hl = h(&list);
    assert_eq!(hl.len(), 3);
    assert_ne!(hl[0], hl[1]); // a width-2 row and an empty row differ
    assert_ne!(hl[1], hl[2]);

    let hu = h(&Value::Unit(4));
    assert_eq!(hu.len(), 4);
    assert!(hu.iter().all(|&x| x == hu[0]));
}

#[test]
fn stable_golden() {
    // GOLDEN LOCK: pin exact outputs so the hash can never silently change (that would re-id the
    // whole system). If this fails after an intentional change, update the constants deliberately.
    assert_eq!(
        h(&u(&[0, 1, 2])),
        vec![0, 6238072747940578789, 15839785061582574730],
    );
}

#[test]
fn fused_product_hashes_match_scalar_rows() {
    use crate::value::Prim;
    // Independent row traversal locks the structural fold across every constructor;
    // unlike the columnar implementation it has no intermediate hash columns.
    fn row(v: &Value, r: usize) -> u64 {
        match v {
            Value::Prim(p) => mix64(match p {
                Prim::U8(v) => v[r] as u64,
                Prim::I64(v) => v[r] as u64,
                Prim::F64(v) => v[r],
            }),
            Value::Prod(cols) => cols.iter().fold(PROD, |a, c| combine(a, row(c, r))),
            Value::Unit(_) => UNIT,
            Value::Sum(tags, lanes) => {
                let tag = tags.tag_at(r);
                combine(
                    combine(SUM, tag as u64),
                    row(&lanes[tag], tags.offset_at(r)),
                )
            }
            Value::List(bounds, vals) => {
                let (s, e) = bounds.span(r);
                (s..e).fold(combine(LIST, (e - s) as u64), |a, i| {
                    combine(a, row(vals, i))
                })
            }
            Value::Ref(list, rows) => row(list, rows[r]),
        }
    }
    for n in [0, 1, 2, 31, 257] {
        let mut tags = Vec::new();
        let (mut a, mut b) = (Vec::new(), Vec::new());
        let (mut ends, mut vals) = (Vec::new(), Vec::new());
        for i in 0..n {
            tags.push(i % 2);
            if i % 2 == 0 {
                a.push(i as u8);
            } else {
                b.push(-(i as i64));
            }
            vals.extend((0..i % 4).map(|j| (i + j) as i64));
            ends.push(vals.len());
        }
        let nested = Value::Prod(vec![
            Value::Unit(n),
            Value::List(
                Bounds::offsets(ends),
                Box::new(Value::Prod(vec![Value::Unit(vals.len()), u(&vals)])),
            ),
            Value::sum(tags, vec![Value::u8(a), u(&b), Value::Unit(0)]),
        ]);
        let v = Value::Prod(vec![
            Value::u8((0..n).map(|i| i as u8).collect()),
            Value::f64((0..n).map(|i| i as f64 * 0.5).collect()),
            u(&(0..n).map(|i| i64::MIN + i as i64).collect::<Vec<_>>()),
            nested,
        ]);
        assert_eq!(hash(&v), (0..n).map(|r| row(&v, r)).collect::<Vec<_>>());
    }
    assert!(hash(&Value::Prod(vec![])).is_empty());
}
