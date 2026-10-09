//! Kadane's maximum subarray sum over signed values.

use crate::common::{int_lists, run_case, Cfg, Rng};
use corgi::Value;

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
    let rows: Vec<Vec<i64>> = (0..cfg.list_rows()).map(|_| { let n = cfg.list_len(&mut rng, 33); (0..n).map(|_| rng.below(201) - 100).collect() }).collect();
    let input = int_lists(&rows);
    let expected = Value::i64(rows.iter().map(|r| kadane(r)).collect());
    let rust = || rows.iter().map(|r| kadane(r)).collect::<Vec<_>>();
    let what = "0-32 values in -100..=100";
    run_case(cfg, "kadane", what, include_str!("../../algorithms/kadane.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "kadane_prefix", what, include_str!("../../algorithms/kadane_prefix.col"), input, expected, rust);
}
