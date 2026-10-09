use super::*;
use crate::engine::gather;
use crate::ops::cmp::order::compare_at;
use crate::ops::cmp::sort::sort_values;
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

/// a random column of `rows` rows over a small value space, so that two draws share rows.
fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
    if depth == 0 {
        return Value::i64((0..rows).map(|_| rng.below(4) as i64 - 2).collect());
    }
    match rng.below(6) {
        0 => Value::u8((0..rows).map(|_| rng.below(3) as u8).collect()),
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
                total += rng.below(3);
                ends.push(total);
            }
            Value::List(ends.into(), Box::new(random_value(rng, total, depth - 1)))
        }
        4 => Value::List(Bounds::Stride(2, rows), Box::new(random_value(rng, rows * 2, depth - 1))),
        _ => Value::Unit(rows),
    }
}

/// the same shape, two independent draws, each sorted.
fn two_sorted(rng: &mut Rng, na: usize, nb: usize, depth: usize) -> (Value, Value) {
    let both = random_value(rng, na + nb, depth);
    let a = gather(&both, &(0..na).collect::<Vec<_>>());
    let b = gather(&both, &(na..na + nb).collect::<Vec<_>>());
    let (_, _, a) = sort_values(&[], &a);
    let (_, _, b) = sort_values(&[], &b);
    (a, b)
}

/// the group oracle: a two-pointer walk with `compare_at`, classes galloped by scanning.
fn naive_groups(a: &Value, b: &Value) -> Vec<GroupRun> {
    let (na, nb) = (a.len(), b.len());
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    while i < na && j < nb {
        match compare_at(a, i, b, j) {
            Ordering::Less => {
                let s = i;
                while i < na && compare_at(a, i, b, j) == Ordering::Less {
                    i += 1;
                }
                out.push(GroupRun::A(s, i));
            }
            Ordering::Greater => {
                let s = j;
                while j < nb && compare_at(b, j, a, i) == Ordering::Less {
                    j += 1;
                }
                out.push(GroupRun::B(s, j));
            }
            Ordering::Equal => {
                let (si, sj) = (i, j);
                while i < na && compare_at(a, i, b, sj) == Ordering::Equal {
                    i += 1;
                }
                while j < nb && compare_at(b, j, a, si) == Ordering::Equal {
                    j += 1;
                }
                out.push(GroupRun::Both(si, i, sj, j));
            }
        }
    }
    if i < na {
        out.push(GroupRun::A(i, na));
    }
    if j < nb {
        out.push(GroupRun::B(j, nb));
    }
    out
}

#[test]
fn groups_agree_with_the_scalar_walk_on_random_shapes() {
    for seed in 1..120u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let (na, nb) = (rng.below(30), rng.below(30));
        let (a, b) = two_sorted(&mut rng, na, nb, 3);
        assert_eq!(survey_groups(&a, &b), naive_groups(&a, &b), "\n{}\n{}", crate::value::show(&a), crate::value::show(&b));
    }
}

#[test]
fn groups_are_maximal_and_equal() {
    let mut rng = Rng(5);
    let (a, b) = two_sorted(&mut rng, 400, 300, 3);
    let (mut ca, mut cb) = (0, 0);
    for r in survey_groups(&a, &b) {
        match r {
            GroupRun::A(lo, hi) => {
                assert_eq!(lo, ca);
                assert!(hi > lo);
                ca = hi;
            }
            GroupRun::B(lo, hi) => {
                assert_eq!(lo, cb);
                assert!(hi > lo);
                cb = hi;
            }
            GroupRun::Both(alo, ahi, blo, bhi) => {
                assert_eq!((alo, blo), (ca, cb));
                assert!((alo..ahi).all(|k| compare_at(&a, k, &b, blo) == Ordering::Equal));
                assert!((blo..bhi).all(|k| compare_at(&b, k, &a, alo) == Ordering::Equal));
                assert!(ahi == a.len() || compare_at(&a, ahi, &a, alo) != Ordering::Equal, "not maximal in a");
                assert!(bhi == b.len() || compare_at(&b, bhi, &b, blo) != Ordering::Equal, "not maximal in b");
                ca = ahi;
                cb = bhi;
            }
        }
    }
    assert_eq!((ca, cb), (a.len(), b.len()));
}

#[test]
fn nested_product_reports_can_be_refined_by_an_enclosing_field() {
    // A terminal product's reports may be consumed by an outer product,
    // list, or sum. Their next refinement must reconstruct the right rows,
    // including duplicates, rather than depend on the terminal's row lists.
    for seed in 1..40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let n = 24;
        let nested = Value::Prod(vec![random_value(&mut rng, n, 3), Value::u8((0..n).map(|_| rng.below(3) as u8).collect())]);
        for both in [
            nested.clone(),
            Value::Prod(vec![nested.clone(), Value::u8((0..n).map(|_| rng.below(3) as u8).collect())]),
            Value::List(Bounds::Stride(2, n / 2), Box::new(nested.clone())),
            Value::sum(vec![0; n], vec![nested]),
        ] {
            let (_, _, sorted) = sort_values(&[], &both);
            let a = gather(&sorted, &(0..sorted.len() - 2).collect::<Vec<_>>());
            let b = gather(&sorted, &(2..sorted.len()).collect::<Vec<_>>());
            assert_eq!(survey_groups(&a, &b), naive_groups(&a, &b), "seed={seed}");
        }
    }
}

/// two rows of long lists decide on their spans at once, not a level per element.
#[test]
fn long_lists_one_row_a_side() {
    let n = 200_000usize;
    let mk = |last: i64| Value::List(Bounds::offsets(vec![n]), Box::new(Value::i64((0..n as i64).map(|i| if i + 1 == n as i64 { last } else { 7 }).collect())));
    let (a, b) = (mk(1), mk(2));
    assert_eq!(survey_groups(&a, &b), vec![GroupRun::A(0, 1), GroupRun::B(0, 1)]);
    assert_eq!(survey_groups(&b, &a), vec![GroupRun::B(0, 1), GroupRun::A(0, 1)]);
    assert_eq!(survey_groups(&a, &a), vec![GroupRun::Both(0, 1, 0, 1)]);
}

#[test]
fn one_side_empty_is_one_run() {
    let (a, e) = (Value::i64(vec![1, 2, 2]), Value::i64(vec![]));
    assert_eq!(survey_groups(&a, &e), vec![GroupRun::A(0, 3)]);
    assert_eq!(survey_groups(&e, &a), vec![GroupRun::B(0, 3)]);
    assert!(survey_groups(&e, &e).is_empty());
}
