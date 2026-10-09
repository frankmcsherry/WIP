//! The three largest values of each row, largest first.

use crate::common::{int_lists, run_case, Cfg, Rng};

fn reference(xs: &[i64]) -> Vec<i64> {
    let mut xs = xs.to_vec();
    xs.sort_unstable_by(|a, b| b.cmp(a));
    xs.truncate(3);
    xs
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(13);
    let rows: Vec<Vec<i64>> = (0..cfg.list_rows()).map(|_| (0..cfg.list_len(&mut rng, 33)).map(|_| rng.below(1000)).collect()).collect();
    let input = int_lists(&rows);
    let expected = int_lists(&rows.iter().map(|r| reference(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    run_case(cfg, "top_k", "top 3 of 0-32 values below 1000", include_str!("../../algorithms/top_k.col"), input, expected, rust);
}
