//! Greatest common divisor by Euclid's algorithm.

use crate::common::{run_case, Cfg, Rng};
use corgi::Value;

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(15);
    // Values below 2^32, a common factor in half the pairs, and some zeros.
    let pairs: Vec<(u64, u64)> = (0..cfg.rows)
        .map(|_| {
            let f = if rng.below(2) == 0 { 1 + rng.below(1000) } else { 1 };
            let mut v = || if rng.below(32) == 0 { 0 } else { f * (rng.next() >> 42) };
            (v(), v())
        })
        .collect();
    let input = Value::Prod(vec![Value::u64(pairs.iter().map(|p| p.0).collect()), Value::u64(pairs.iter().map(|p| p.1).collect())]);
    let expected = Value::u64(pairs.iter().map(|&(a, b)| gcd(a, b)).collect());
    let rust = || pairs.iter().map(|&(a, b)| gcd(a, b)).collect::<Vec<_>>();
    run_case(cfg, "gcd", "pairs below 2^32", include_str!("../../algorithms/gcd.col"), input, expected, rust);
}
