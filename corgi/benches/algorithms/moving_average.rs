//! Sums of every window of four consecutive values.

use crate::common::{run_case, u64_lists, Cfg, Rng};

fn window_sums(xs: &[u64]) -> Vec<u64> {
    xs.windows(4).map(|w| w.iter().sum()).collect()
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(12);
    let rows: Vec<Vec<u64>> = (0..cfg.rows).map(|_| { let n = rng.below(33) as usize; (0..n).map(|_| rng.below(1000)).collect() }).collect();
    let input = u64_lists(&rows);
    let expected = u64_lists(&rows.iter().map(|r| window_sums(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| window_sums(r)).collect::<Vec<_>>();
    let what = "lists of 0-32 values";
    run_case(cfg, "moving_average", what, include_str!("../../algorithms/moving_average.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "moving_average_prefix", what, include_str!("../../algorithms/moving_average_prefix.col"), input, expected, rust);
}
