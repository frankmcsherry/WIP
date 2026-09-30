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
    Program::compile_ml(src).unwrap().run(input).unwrap()
}

/// a sum of `lanes` lanes over `rows` rows; lane `l` holds (row index, row index * 10 + l), as a
/// product when `prod`, else just the row index. Returns the sum and each row's (tag, payload).
fn sum(rows: usize, lanes: usize, prod: bool, rng: &mut Rng) -> (Value, Vec<(usize, u64)>) {
    let tags: Vec<usize> = (0..rows).map(|_| (rng.next() % lanes as u64) as usize).collect();
    let mut cols: Vec<(Vec<u64>, Vec<u64>)> = vec![(Vec::new(), Vec::new()); lanes];
    for (r, &t) in tags.iter().enumerate() {
        cols[t].0.push(r as u64);
        cols[t].1.push(r as u64 * 10 + t as u64);
    }
    let variants = cols
        .into_iter()
        .map(|(a, b)| if prod { Value::Prod(vec![Value::u64(a), Value::u64(b)]) } else { Value::u64(a) })
        .collect();
    let rows_out = tags.iter().enumerate().map(|(r, &t)| (t, r as u64)).collect();
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
                let firsts: Vec<u64> = want.iter().map(|&(_, r)| r).collect();
                let expect = if prod {
                    let seconds: Vec<u64> = want.iter().map(|&(t, r)| r * 10 + t as u64).collect();
                    Value::Prod(vec![Value::u64(firsts), Value::u64(seconds)])
                } else {
                    Value::u64(firsts)
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
            let tag_list: Vec<u64> = want.iter().map(|&(t, _)| t as u64).collect();
            expect.push(Value::List(Bounds::offsets(ends.clone()), Box::new(Value::u64(tag_list))));
            for l in 0..lanes {
                let (mut lb, mut vals, mut start) = (Vec::new(), Vec::new(), 0);
                for &end in &ends {
                    vals.extend(want[start..end].iter().filter(|&&(t, _)| t == l).map(|&(_, r)| r));
                    lb.push(vals.len());
                    start = end;
                }
                expect.push(Value::List(Bounds::offsets(lb), Box::new(Value::u64(vals))));
            }
            assert_eq!(got, Value::Prod(expect), "lanes {lanes}, rows {rows}");
        }
    }
}
