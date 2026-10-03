//! Two-sum: does a pair of distinct positions sum to the target?

use crate::common::{run_case, u64_lists, Cfg, Rng};
use corgi::Value;
use std::collections::HashSet;

fn reference(xs: &[u64], t: u64) -> u64 {
    let mut seen = HashSet::new();
    for &x in xs {
        if x <= t && seen.contains(&(t - x)) {
            return 1;
        }
        seen.insert(x);
    }
    0
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(16);
    let rows: Vec<(Vec<u64>, u64)> = (0..cfg.list_rows())
        .map(|_| ((0..cfg.list_len(&mut rng, 33)).map(|_| rng.below(500)).collect(), rng.below(1000)))
        .collect();
    let input = Value::Prod(vec![
        u64_lists(&rows.iter().map(|r| &r.0[..]).collect::<Vec<_>>()),
        Value::u64(rows.iter().map(|r| r.1).collect()),
    ]);
    let expected = Value::u64(rows.iter().map(|(xs, t)| reference(xs, *t)).collect());
    let rust = || rows.iter().map(|(xs, t)| reference(xs, *t)).collect::<Vec<_>>();
    run_case(cfg, "two_sum", "0-32 values below 500, target below 1000", include_str!("../../algorithms/two_sum.col"), input, expected, rust);
}
