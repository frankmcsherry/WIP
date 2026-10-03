//! The most frequent value of each row, smallest on ties.

use crate::common::{run_case, u64_lists, Cfg, Rng};
use corgi::Value;

fn mode(xs: &[u64]) -> u64 {
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
    let rows: Vec<Vec<u64>> = (0..cfg.rows).map(|_| { let n = rng.below(33) as usize; (0..n).map(|_| rng.below(16)).collect() }).collect();
    let input = u64_lists(&rows);
    let expected = Value::u64(rows.iter().map(|r| mode(r)).collect());
    let rust = || rows.iter().map(|r| mode(r)).collect::<Vec<_>>();
    run_case(cfg, "mode", "0-32 values in 0..16", include_str!("../../algorithms/mode.col"), input, expected, rust);
}
