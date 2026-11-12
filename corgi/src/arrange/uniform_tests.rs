//! A list of one length, or a sum of one tag, takes its own path through each kernel that moves,
//! compares, sorts or merges rows. Each such path must give what the same value gives held loose:
//! its lists' bounds written out as offsets and its sums' tags written out row by row, the form no
//! uniform path takes.

use crate::engine::{blend, gather, gather_lanes, gather_or_zero};
use crate::ops::cmp::order::{compare_adjacent, compare_cols, compare_idx, equal_idx};
use crate::ops::cmp::sort::sort_values;
use crate::ops::cmp::survey::survey_groups;
use crate::value::{Bounds, Prim, Tags, Value};
use std::sync::Arc;

fn loose(v: &Value) -> Value {
    match v {
        Value::Prod(fields) => Value::Prod(fields.iter().map(loose).collect()),
        Value::Sum(tags, lanes) => {
            let rows = 0..tags.len();
            let written = Tags::Column(
                Prim::U8(Arc::new(rows.clone().map(|i| tags.tag_at(i) as u8).collect())),
                Arc::new(rows.map(|i| tags.offset_at(i)).collect()),
            );
            Value::Sum(written, lanes.iter().map(loose).collect())
        }
        Value::List(bounds, vals) => Value::List(Bounds::offsets(bounds.ends().collect()), Box::new(loose(vals))),
        other => other.clone(),
    }
}

/// `rows` lists of `k` elements each, one column for each shape an element takes: a leaf, a byte, a
/// product, a ragged list, a list of one, a list of two, a sum, a sum whose rows all hold the
/// same tags in the same order, and a sum of one tag. Then a sum of one tag outside any list. The
/// integers are small, so rows tie, and `salt` varies them.
fn values(k: usize, rows: usize, salt: u64) -> Vec<Value> {
    let n = k * rows;
    let ints = |m: usize, mul: u64| Value::i64((0..m as u64).map(|i| ((i * mul + salt) % 5) as i64 - 2).collect());
    let ragged: Vec<usize> = (0..n).scan(0, |end, i| { *end += (i + salt as usize) % 3; Some(*end) }).collect();
    let total = ragged.last().copied().unwrap_or(0);
    let sum = |tags: Vec<usize>| {
        let ones = tags.iter().filter(|&&t| t == 1).count();
        Value::sum(tags.clone(), vec![ints(tags.len() - ones, 7), ints(ones, 3)])
    };
    let one_tag = |m: usize| Value::Sum(Tags::Const(1, m), vec![ints(0, 7), ints(m, 3)]);
    let elems = vec![
        ints(n, 7),
        Value::u8((0..n as u64).map(|i| ((i * 7 + salt) % 5) as u8).collect()),
        Value::Prod(vec![ints(n, 3), ints(n, 11)]),
        Value::List(Bounds::offsets(ragged), Box::new(ints(total, 7))),
        Value::List(Bounds::Stride(1, n), Box::new(ints(n, 7))),
        Value::List(Bounds::Stride(2, n), Box::new(ints(2 * n, 3))),
        sum((0..n).map(|i| (i + salt as usize) % 3 % 2).collect()),
        sum((0..n).map(|i| i % k.max(1) % 2).collect()),
        one_tag(n),
    ];
    let mut out: Vec<Value> = elems.into_iter().map(|e| Value::List(Bounds::Stride(k, rows), Box::new(e))).collect();
    out.push(one_tag(rows));
    out
}

#[test]
fn uniform_values_act_as_their_loose_forms_do() {
    // no rows is one partition, whatever the stride
    assert_eq!(Bounds::Stride(4, 0), Bounds::Stride(5, 0));
    let rows = 6;
    let signs = |o: Vec<i8>| o.into_iter().map(i8::signum).collect::<Vec<_>>();
    // equality forms promise only zero or not
    let equal = |o: Vec<i8>| o.into_iter().map(|o| o == 0).collect::<Vec<_>>();
    let (tags, off) = ([0usize, 2, 2, 0, 0, 2, 0], [3usize, 0, 2, 0, 1, 1, 2]);
    let (ia, ib) = ([5usize, 0, 3, 3, 1], [0usize, 0, 2, 3, 4]);
    let mask = [1u8, 0, 0, 1, 1, 0];
    let unweave = |v: &Value| crate::ops::Op::<crate::NumOp>::Unweave.eval(v.clone()).ok();
    for k in 0..=3 {
        let sets = values(k, rows, 0).into_iter().zip(values(k, rows, 1)).zip(values(k + 1, rows, 2));
        for ((a, b), c) in sets {
            let (la, lb, lc) = (loose(&a), loose(&b), loose(&c));
            let what = format!("k {k}\n{}", crate::value::show(&a));
            // moved: positions repeated, left out and past the rows; from sources of one length,
            // and with a source of another length, which takes the general path
            for idx in [vec![], vec![5, 0, 0, 3, 2, 5, 1], vec![2, 9, 6]] {
                assert_eq!(gather_or_zero(&a, &idx), gather_or_zero(&la, &idx), "{what}");
            }
            let none = gather(&a, &[]);
            for (x, lx) in [(&b, &lb), (&c, &lc)] {
                let got = gather_lanes(&[Some(&a), Some(&none), Some(x)], &tags, &off);
                assert_eq!(got, gather_lanes(&[Some(&la), Some(&loose(&none)), Some(lx)], &tags, &off), "{what}");
            }
            assert_eq!(blend(&mask, a.clone(), b.clone()), blend(&mask, la.clone(), lb.clone()), "{what}");
            assert_eq!(unweave(&a), unweave(&la), "{what}");
            // compared: row against row, at named pairs, and each row against the next
            assert_eq!(signs(compare_cols(&a, &b)), signs(compare_cols(&la, &lb)), "{what}");
            assert_eq!(signs(compare_idx(&a, &b, &ia, &ib)), signs(compare_idx(&la, &lb, &ia, &ib)), "{what}");
            assert_eq!(equal(equal_idx(&a, &b, &ia, &ib)), equal(equal_idx(&la, &lb, &ia, &ib)), "{what}");
            assert_eq!(equal(compare_adjacent(&a)), equal(compare_adjacent(&la)), "{what}");
            // sorted, whole and within blocks, then merged
            for labels in [vec![], vec![0, 0, 0, 1, 1, 1]] {
                let (pa, ka, sa) = sort_values(&labels, &a);
                let (pl, kl, sl) = sort_values(&labels, &la);
                assert_eq!((pa, ka, &sa), (pl, kl, &sl), "{what}");
            }
            let (_, _, sa) = sort_values(&[], &a);
            let (_, _, sb) = sort_values(&[], &b);
            assert_eq!(survey_groups(&sa, &sb), survey_groups(&loose(&sa), &loose(&sb)), "{what}");
            assert_eq!(survey_groups(&sb, &sa), survey_groups(&loose(&sb), &loose(&sa)), "{what}");
        }
    }
}
