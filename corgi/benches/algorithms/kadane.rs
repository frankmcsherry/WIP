//! Kadane's maximum subarray sum over signed values.

use crate::common::{run_case, u64_lists, Cfg, Rng};
use corgi::{enc_i64, Value};

fn kadane(xs: &[i64]) -> i64 {
    let Some(&first) = xs.first() else { return 0 };
    let (mut cur, mut best) = (first, first);
    for &x in &xs[1..] {
        cur = x.max(cur + x);
        best = best.max(cur);
    }
    best
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(14);
    let rows: Vec<Vec<i64>> = (0..cfg.rows).map(|_| { let n = rng.below(33) as usize; (0..n).map(|_| rng.below(201) as i64 - 100).collect() }).collect();
    let input = u64_lists(&rows.iter().map(|r| r.iter().map(|&x| enc_i64(x)).collect::<Vec<_>>()).collect::<Vec<_>>());
    let expected = Value::u64(rows.iter().map(|r| enc_i64(kadane(r))).collect());
    let rust = || rows.iter().map(|r| kadane(r)).collect::<Vec<_>>();
    let what = "0-32 values in -100..=100";
    run_case(cfg, "kadane", what, include_str!("../../algorithms/kadane.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "kadane_prefix", what, include_str!("../../algorithms/kadane_prefix.col"), input, expected, rust);
}
