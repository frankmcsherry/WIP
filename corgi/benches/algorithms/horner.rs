//! A polynomial evaluated at x by Horner's rule.

use crate::common::{int_lists, run_case, Cfg, Rng};
use corgi::Value;

fn horner(coeffs: &[i64], x: i64) -> i64 {
    coeffs.iter().fold(0, |acc, &c| acc * x + c)
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(17);
    let rows: Vec<(Vec<i64>, i64)> = (0..cfg.rows)
        .map(|_| { let n = rng.below(17) as usize; ((0..n).map(|_| rng.below(100)).collect(), rng.below(10)) })
        .collect();
    let input = Value::Prod(vec![int_lists(&rows.iter().map(|r| &r.0).collect::<Vec<_>>()), Value::i64(rows.iter().map(|r| r.1).collect())]);
    let expected = Value::i64(rows.iter().map(|(c, x)| horner(c, *x)).collect());
    let rust = || rows.iter().map(|(c, x)| horner(c, *x)).collect::<Vec<_>>();
    run_case(cfg, "horner", "degree 0-16, x in 0..10", include_str!("../../algorithms/horner.col"), input, expected, rust);
}
