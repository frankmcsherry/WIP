//! A polynomial evaluated at x by Horner's rule, wrapping.

use crate::common::{run_case, u64_lists, Cfg, Rng};
use corgi::Value;

fn horner(coeffs: &[u64], x: u64) -> u64 {
    coeffs.iter().fold(0u64, |acc, &c| acc.wrapping_mul(x).wrapping_add(c))
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(17);
    let rows: Vec<(Vec<u64>, u64)> = (0..cfg.rows)
        .map(|_| { let n = rng.below(17) as usize; ((0..n).map(|_| rng.below(100)).collect(), rng.below(10)) })
        .collect();
    let input = Value::Prod(vec![u64_lists(&rows.iter().map(|r| &r.0).collect::<Vec<_>>()), Value::u64(rows.iter().map(|r| r.1).collect())]);
    let expected = Value::u64(rows.iter().map(|(c, x)| horner(c, *x)).collect());
    let rust = || rows.iter().map(|(c, x)| horner(c, *x)).collect::<Vec<_>>();
    run_case(cfg, "horner", "degree 0-16, x in 0..10", include_str!("../../algorithms/horner.col"), input, expected, rust);
}
