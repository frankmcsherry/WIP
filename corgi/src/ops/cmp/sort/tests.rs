use super::*;
use crate::ops::cmp::order::compare_at;
use std::cmp::Ordering;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn leaf(rng: &mut Rng, rows: usize) -> Value {
    match rng.below(4) {
        0 => Value::u8((0..rows).map(|_| rng.below(6) as u8).collect()),
        1 => Value::i64((0..rows).map(|_| rng.below(1000) as i64 - 500).collect()),
        2 => Value::f64((0..rows).map(|_| [0.0, -0.0, 1.5, -2.5, f64::NAN][rng.below(5)]).collect()),
        _ => Value::i64((0..rows).map(|_| if rng.below(2) == 0 { rng.below(5) as i64 } else { rng.next() as i64 }).collect()),
    }
}

/// a random column of `rows` rows, nesting up to `depth` levels below the top.
fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
    if depth == 0 {
        return leaf(rng, rows);
    }
    match rng.below(6) {
        0 => leaf(rng, rows),
        1 => Value::Prod((0..1 + rng.below(3)).map(|_| random_value(rng, rows, depth - 1)).collect()),
        2 => {
            let arity = 1 + rng.below(3);
            let tags: Vec<usize> = (0..rows).map(|_| rng.below(arity)).collect();
            let lanes = (0..arity)
                .map(|t| random_value(rng, tags.iter().filter(|&&x| x == t).count(), depth - 1))
                .collect();
            Value::sum(tags, lanes)
        }
        3 => {
            let mut ends = Vec::with_capacity(rows);
            let mut total = 0;
            for _ in 0..rows {
                total += rng.below(4);
                ends.push(total);
            }
            Value::List(ends.into(), Box::new(random_value(rng, total, depth - 1)))
        }
        4 => {
            let k = 1 + rng.below(3);
            Value::List(Bounds::Stride(k, rows), Box::new(random_value(rng, rows * k, depth - 1)))
        }
        _ => Value::Unit(rows),
    }
}

/// the scalar reference: positions sorted stably by (label, structural order of the row),
/// then the dense run index of (label, row equality).
fn reference(v: &Value, labels: &[u64], index: &[usize]) -> (Vec<usize>, Vec<usize>, Vec<u64>) {
    let lab = |q: usize| labels.get(q).copied().unwrap_or(0); // absent labels: one block
    let mut perm: Vec<usize> = (0..index.len()).collect();
    perm.sort_by(|&a, &b| lab(a).cmp(&lab(b)).then_with(|| compare_at(v, index[a], v, index[b])));
    let rows: Vec<usize> = perm.iter().map(|&q| index[q]).collect();
    let mut out = Vec::with_capacity(rows.len());
    let mut next = 0u64;
    for k in 0..rows.len() {
        if k > 0
            && (lab(perm[k]) != lab(perm[k - 1])
                || compare_at(v, rows[k], v, rows[k - 1]) != Ordering::Equal)
        {
            next += 1;
        }
        out.push(next);
    }
    (perm, rows, out)
}

/// the kernel, with and without emitting, against the reference: same rows (stability makes
/// them unique), same labels, and the emitted column is the rows gathered; and the public
/// form's permutation.
fn check(v: &Value, labels: &[u64], index: &[usize]) {
    let (_, rows_ref, labels_ref) = reference(v, labels, index);
    for emit in [Emit::Index, Emit::Both] {
        let (mut l, mut i) = (labels.to_vec(), index.to_vec());
        let mut scratch = SortScratch::default();
        let out = sort_indexed(v, &mut l, &mut i, emit, &mut scratch);
        assert_eq!(i, rows_ref, "rows\n{}", crate::value::show(v));
        assert_eq!(l, labels_ref, "labels\n{}", crate::value::show(v));
        match out {
            Some(o) => assert_eq!(o, gather(v, &i), "values\n{}", crate::value::show(v)),
            None => assert!(emit == Emit::Index),
        }
    }
    // values only: the column and the labels, nothing promised of the index
    let (mut l, mut i) = (labels.to_vec(), index.to_vec());
    let mut scratch = SortScratch::default();
    let out = sort_indexed(v, &mut l, &mut i, Emit::Values, &mut scratch);
    assert_eq!(out.unwrap(), gather(v, &rows_ref), "values only\n{}", crate::value::show(v));
    assert_eq!(l, labels_ref, "values-only labels\n{}", crate::value::show(v));
}

/// dense non-decreasing labels over `n` positions: one block, blocks of random size, or a
/// block per position.
fn label_patterns(rng: &mut Rng, n: usize) -> Vec<Vec<u64>> {
    let mut blocks = Vec::with_capacity(n);
    let mut b = 0u64;
    for _ in 0..n {
        if rng.below(3) == 0 {
            b += 1;
        }
        blocks.push(b);
    }
    vec![Vec::new(), vec![0; n], blocks, (0..n as u64).collect()]
}

#[test]
fn ordered_refinements_preserve_labels_and_stable_positions() {
    // Each label class is ordered, but keys decrease across class boundaries.
    // Non-identity source coordinates must stay in their original tie order.
    for n in [0, 1, 2, 33, 257, 32769] {
        let keys: Vec<i64> = (0..n).map(|i| i64::MIN + ((i % 129) / 3) as i64).collect();
        let labels: Vec<u64> = (0..n).map(|i| 7 + 9 * (i / 129) as u64).collect();
        let mut stored = keys.clone();
        stored.reverse();
        let index: Vec<usize> = (0..n).rev().collect();
        check(&Value::i64(stored), &labels, &index);
        let mut sorted = keys;
        sorted.sort();
        check(&Value::i64(sorted.clone()), &[], &(0..n).collect::<Vec<_>>());
        // A late inversion must still take the normal sort, without partially
        // refining the labels before the fast path rejects the input.
        if n > 3 {
            sorted[n - 1] = 0;
            check(&Value::i64(sorted), &labels, &(0..n).collect::<Vec<_>>());
        }
    }
}

#[test]
fn random_shapes_agree_with_the_scalar_order() {
    for seed in 1..80u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let rows = 1 + rng.below(40);
        let v = random_value(&mut rng, rows, 3);
        let index: Vec<usize> = (0..rows).collect();
        for labels in label_patterns(&mut rng, rows) {
            check(&v, &labels, &index);
        }
    }
}

#[test]
fn subsets_in_any_order_sort_without_a_gather() {
    for seed in 1..80u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1);
        let rows = 2 + rng.below(40);
        let v = random_value(&mut rng, rows, 3);
        // a subset of the rows, in scrambled order, each row at most once
        let mut index: Vec<usize> = (0..rows).filter(|_| rng.below(3) != 0).collect();
        for k in (1..index.len()).rev() {
            index.swap(k, rng.below(k + 1));
        }
        for labels in label_patterns(&mut rng, index.len()) {
            check(&v, &labels, &index);
        }
    }
}

#[test]
fn emitted_columns_are_the_sorted_data_at_scale() {
    let n = 5000usize;
    let mut rng = Rng(7);
    let full: Vec<i64> = (0..n).map(|_| rng.next() as i64).collect();
    let narrow: Vec<i64> = (0..n).map(|_| rng.below(50) as i64).collect();
    let index: Vec<usize> = (0..n).collect();
    let zeros = vec![0u64; n];
    check(&Value::i64(full.clone()), &zeros, &index);
    check(&Value::i64(narrow.clone()), &zeros, &index);
    check(&Value::Prod(vec![Value::i64(narrow.clone()), Value::i64(full.clone())]), &zeros, &index);
    let tags: Vec<usize> = (0..n).map(|_| rng.below(3)).collect();
    let lanes: Vec<Value> = (0..3)
        .map(|t| Value::i64(narrow.iter().zip(&tags).filter(|(_, &x)| x == t).map(|(&v, _)| v).collect()))
        .collect();
    check(&Value::sum(tags, lanes), &zeros, &index);
    let mut ends = Vec::with_capacity(n);
    let mut total = 0;
    for _ in 0..n {
        total += rng.below(4);
        ends.push(total);
    }
    let elems: Vec<i64> = (0..total).map(|_| rng.below(4) as i64).collect();
    check(&Value::List(ends.into(), Box::new(Value::i64(elems))), &zeros, &index);
}

#[test]
fn byte_records_pack_and_unpack() {
    let mut rng = Rng(11);
    for k in 1..=9usize {
        let rows = 300;
        let bytes: Vec<u8> = (0..rows * k).map(|_| rng.below(3) as u8).collect();
        let v = Value::List(Bounds::Stride(k, rows), Box::new(Value::u8(bytes)));
        let index: Vec<usize> = (0..rows).collect();
        for labels in label_patterns(&mut rng, rows) {
            check(&v, &labels, &index);
        }
    }
}

#[test]
fn the_labels_form_is_the_identity_index() {
    let v = Value::Prod(vec![Value::i64(vec![2, 1, 2, 1, 3, 1]), Value::i64(vec![10, 20, 5, 30, 7, 20])]);
    let labels = [0, 0, 0, 1, 1, 1];
    let (perm, refined) = sort_blocks(&labels, &v);
    let (_, rows, labels_ref) = reference(&v, &labels, &[0, 1, 2, 3, 4, 5]);
    assert_eq!(perm, rows);
    assert_eq!(refined, labels_ref);
    let (perm2, refined2, sorted) = sort_values(&labels, &v);
    assert_eq!((perm2, refined2), (perm, refined));
    assert_eq!(sorted, gather(&v, &rows));
}

/// Signed values either side of zero: +1 and -1 differ in every bit of their keys, so the radix
/// reads such keys less the least. Checked at the block sizes where the kernel changes (insertion
/// up to 32, then 8-, 11- and 16-bit digits), alone, among the extremes, and as product fields and
/// list elements.
#[test]
fn signed_values_around_zero() {
    let mut rng = Rng(23);
    let sets: [&[i64]; 4] = [&[-1, 1], &[-1, 0, 1], &[-3, -2, -1, 0, 1, 2, 3], &[i64::MIN, -1, 0, 1, i64::MAX]];
    for n in [5usize, 33, 1000, 1 << 15, 1 << 20] {
        for vals in sets {
            let xs: Vec<i64> = (0..n).map(|_| vals[rng.below(vals.len())]).collect();
            // a leaf's whole answer: the stable order of its values, and their dense ranks
            let mut want: Vec<usize> = (0..n).collect();
            want.sort_by_key(|&i| xs[i]);
            let mut ranks = Vec::with_capacity(n);
            for k in 0..n {
                let next = ranks.last().copied().unwrap_or(0) + (k > 0 && xs[want[k]] != xs[want[k - 1]]) as u64;
                ranks.push(next);
            }
            let (perm, labels) = sort_blocks(&[], &Value::i64(xs.clone()));
            assert_eq!(perm, want, "order of {vals:?} at {n} rows");
            assert_eq!(labels, ranks, "runs of {vals:?} at {n} rows");
            if n <= 1000 {
                let index: Vec<usize> = (0..n).collect();
                let ys: Vec<i64> = (0..n).map(|_| vals[rng.below(vals.len())]).collect();
                check(&Value::Prod(vec![Value::i64(xs.clone()), Value::i64(ys)]), &[], &index);
                let mut ends = Vec::with_capacity(n);
                let mut total = 0;
                for _ in 0..n {
                    total += rng.below(4);
                    ends.push(total);
                }
                let elems: Vec<i64> = (0..total).map(|_| vals[rng.below(vals.len())]).collect();
                check(&Value::List(ends.into(), Box::new(Value::i64(elems))), &[], &index);
            }
        }
    }
}
