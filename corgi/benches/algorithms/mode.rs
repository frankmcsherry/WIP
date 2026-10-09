//! The most frequent value of each row, smallest on ties.

use crate::common::{int_lists, run_case, Cfg, Rng};
use corgi::Value;

fn mode(xs: &[i64]) -> i64 {
    let mut s = xs.to_vec();
    s.sort_unstable();
    let (mut best, mut best_n) = (0, 0);
    for run in s.chunk_by(|a, b| a == b) {
        if run.len() > best_n {
            (best, best_n) = (run[0], run.len());
        }
    }
    best
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(16);
    let rows: Vec<Vec<i64>> = (0..cfg.list_rows()).map(|_| { let n = cfg.list_len(&mut rng, 33); (0..n).map(|_| rng.below(16)).collect() }).collect();
    let input = int_lists(&rows);
    let expected = Value::i64(rows.iter().map(|r| mode(r)).collect());
    let rust = || rows.iter().map(|r| mode(r)).collect::<Vec<_>>();
    run_case(cfg, "mode", "0-32 values in 0..16", include_str!("../../algorithms/mode.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "mode_cut", "0-32 values in 0..16", include_str!("../../algorithms/mode_cut.col"), input, expected, rust);
}
