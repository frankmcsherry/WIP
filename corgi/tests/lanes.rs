//! `unwrap` and `unweave` over sums of every lane count and payload shape, against a reference
//! built row by row from the tags and the values. These pin the paths that read a sum's `u8`
//! discriminants in place (lanes of leaves and of products of leaves, one list row or many).

use corgi::{Bounds, Program, Value};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 17
    }
}

fn run(src: &str, input: Value) -> Value {
    Program::compile_ml(src).unwrap().run(input)
}

/// a sum of `lanes` lanes over `rows` rows; lane `l` holds (row index, row index * 10 + l), as a
/// product when `prod`, else just the row index. Returns the sum and each row's (tag, payload).
fn sum(rows: usize, lanes: usize, prod: bool, rng: &mut Rng) -> (Value, Vec<(usize, i64)>) {
    let tags: Vec<usize> = (0..rows).map(|_| (rng.next() % lanes as u64) as usize).collect();
    let mut cols: Vec<(Vec<i64>, Vec<i64>)> = vec![(Vec::new(), Vec::new()); lanes];
    for (r, &t) in tags.iter().enumerate() {
        cols[t].0.push(r as i64);
        cols[t].1.push(r as i64 * 10 + t as i64);
    }
    let variants = cols
        .into_iter()
        .map(|(a, b)| if prod { Value::Prod(vec![Value::i64(a), Value::i64(b)]) } else { Value::i64(a) })
        .collect();
    let rows_out = tags.iter().enumerate().map(|(r, &t)| (t, r as i64)).collect();
    (Value::sum(tags, variants), rows_out)
}

#[test]
fn unwrap_reads_each_row_from_its_lane() {
    let mut rng = Rng(11);
    for lanes in 1..=4 {
        for rows in [0, 1, 2, 7, 100, 1000] {
            for prod in [false, true] {
                let (v, want) = sum(rows, lanes, prod, &mut rng);
                let got = run("input unwrap", v);
                let firsts: Vec<i64> = want.iter().map(|&(_, r)| r).collect();
                let expect = if prod {
                    let seconds: Vec<i64> = want.iter().map(|&(t, r)| r * 10 + t as i64).collect();
                    Value::Prod(vec![Value::i64(firsts), Value::i64(seconds)])
                } else {
                    Value::i64(firsts)
                };
                assert_eq!(got, expect, "lanes {lanes}, rows {rows}, prod {prod}");
            }
        }
    }
}

#[test]
fn unweave_counts_each_rows_elements_per_lane() {
    let mut rng = Rng(12);
    for lanes in 1..=4 {
        for rows in [0usize, 1, 2, 9, 200] {
            let total = rows * 3;
            let (inner, want) = sum(total, lanes, false, &mut rng);
            // rows of random length summing to `total` (the first row takes what the others leave)
            let mut ends: Vec<usize> = (0..rows.saturating_sub(1)).map(|_| (rng.next() as usize) % (total + 1)).collect();
            ends.sort();
            if rows > 0 {
                ends.push(total);
            }
            let input = Value::List(Bounds::offsets(ends.clone()), Box::new(inner));
            let got = run("input unweave", input);
            let mut expect = Vec::new();
            let tag_list: Vec<u8> = want.iter().map(|&(t, _)| t as u8).collect();
            expect.push(Value::List(Bounds::offsets(ends.clone()), Box::new(Value::u8(tag_list))));
            for l in 0..lanes {
                let (mut lb, mut vals, mut start) = (Vec::new(), Vec::new(), 0);
                for &end in &ends {
                    vals.extend(want[start..end].iter().filter(|&&(t, _)| t == l).map(|&(_, r)| r));
                    lb.push(vals.len());
                    start = end;
                }
                expect.push(Value::List(Bounds::offsets(lb), Box::new(Value::i64(vals))));
            }
            assert_eq!(got, Value::Prod(expect), "lanes {lanes}, rows {rows}");
        }
    }
}

/// Rows of one length that all hold the same tags in the same order unweave as the same rows held
/// as offsets do, and so do rows of one length whose tags do not repeat.
#[test]
fn unweave_of_uniform_rows_matches_offsets() {
    for lanes in 1..=3usize {
        for (k, rows) in [(1usize, 5usize), (3, 4), (4, 1), (2, 0), (3, 7)] {
            let repeating: Vec<usize> = (0..k * rows).map(|i| (i % k) % lanes).collect();
            let shifting: Vec<usize> = (0..k * rows).map(|i| (i % k + i / k) % lanes).collect();
            for tags in [repeating, shifting] {
                let variants = (0..lanes)
                    .map(|l| Value::i64(tags.iter().enumerate().filter(|&(_, &t)| t == l).map(|(i, _)| i as i64).collect()))
                    .collect::<Vec<_>>();
                let inner = Value::sum(tags.clone(), variants);
                let uniform = Value::List(Bounds::Stride(k, rows), Box::new(inner.clone()));
                let offsets = Value::List(Bounds::offsets((1..=rows).map(|r| r * k).collect()), Box::new(inner));
                assert_eq!(run("input unweave", uniform), run("input unweave", offsets), "lanes {lanes}, k {k}, rows {rows}, tags {tags:?}");
            }
        }
    }
}
