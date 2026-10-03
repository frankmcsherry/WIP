//! A fixed-bucket histogram per row.

use crate::common::{run_case, u64_lists, Cfg, Rng};

fn reference(xs: &[u64]) -> Vec<u64> {
    let mut counts = vec![0u64; 8];
    for &x in xs {
        counts[(x / 125) as usize] += 1;
    }
    counts
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(17);
    let rows: Vec<Vec<u64>> = (0..cfg.rows).map(|_| (0..rng.below(33)).map(|_| rng.below(1000)).collect()).collect();
    let input = u64_lists(&rows);
    let expected = u64_lists(&rows.iter().map(|r| reference(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    let what = "0-32 values below 1000 into 8 buckets";
    run_case(cfg, "histogram", what, include_str!("../../algorithms/histogram.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "histogram_sorted", what, include_str!("../../algorithms/histogram_sorted.col"), input, expected, rust);
}
