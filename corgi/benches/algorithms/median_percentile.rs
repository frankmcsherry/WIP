//! Median and 90th percentile (nearest rank) of each row's values.

use crate::common::{int_lists, run_case, Cfg, Rng};
use corgi::Value;

fn median_p90(xs: &[i64]) -> (i64, i64) {
    if xs.is_empty() {
        return (0, 0);
    }
    let mut s = xs.to_vec();
    s.sort_unstable();
    let n = s.len();
    (s[(n - 1) / 2], s[(9 * n).div_ceil(10) - 1])
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(11);
    let rows: Vec<Vec<i64>> = (0..cfg.list_rows()).map(|_| { let n = cfg.list_len(&mut rng, 33); (0..n).map(|_| rng.below(1000)).collect() }).collect();
    let input = int_lists(&rows);
    let out: Vec<(i64, i64)> = rows.iter().map(|r| median_p90(r)).collect();
    let expected = Value::Prod(vec![Value::i64(out.iter().map(|o| o.0).collect()), Value::i64(out.iter().map(|o| o.1).collect())]);
    let rust = || rows.iter().map(|r| median_p90(r)).collect::<Vec<_>>();
    run_case(cfg, "median_percentile", "lists of 0-32 values", include_str!("../../algorithms/median_percentile.col"), input, expected, rust);
}
