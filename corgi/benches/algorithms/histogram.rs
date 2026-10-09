//! A fixed-bucket histogram per row.

use crate::common::{int_lists, run_case, Cfg, Rng};

fn reference(xs: &[i64]) -> Vec<i64> {
    let mut counts = vec![0i64; 8];
    for &x in xs {
        counts[(x / 125) as usize] += 1;
    }
    counts
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(17);
    let rows: Vec<Vec<i64>> = (0..cfg.list_rows()).map(|_| (0..cfg.list_len(&mut rng, 33)).map(|_| rng.below(1000)).collect()).collect();
    let input = int_lists(&rows);
    let expected = int_lists(&rows.iter().map(|r| reference(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    let what = "0-32 values below 1000 into 8 buckets";
    run_case(cfg, "histogram", what, include_str!("../../algorithms/histogram.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "histogram_sorted", what, include_str!("../../algorithms/histogram_sorted.col"), input, expected, rust);
}
