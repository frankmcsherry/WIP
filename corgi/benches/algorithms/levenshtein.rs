//! Levenshtein edit distance between two byte strings.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn reference(a: &[u8], b: &[u8]) -> u64 {
    let mut prev: Vec<u64> = (0..=b.len() as u64).collect();
    let mut cur = vec![0u64; b.len() + 1];
    for (i, &c) in a.iter().enumerate() {
        cur[0] = i as u64 + 1;
        for j in 0..b.len() {
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + (c != b[j]) as u64);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(2);
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.rows)
        .map(|_| {
            let (n1, n2) = (rng.below(16) as usize, rng.below(16) as usize);
            (rng.string(n1, b"abcd"), rng.string(n2, b"abcd"))
        })
        .collect();
    let input = Value::Prod(vec![
        bytes_col(&pairs.iter().map(|p| &p.0).collect::<Vec<_>>()),
        bytes_col(&pairs.iter().map(|p| &p.1).collect::<Vec<_>>()),
    ]);
    let expected = Value::u64(pairs.iter().map(|(a, b)| reference(a, b)).collect());
    let rust = || pairs.iter().map(|(a, b)| reference(a, b)).collect::<Vec<u64>>();
    run_case(cfg, "levenshtein", "two strings of 0-15 bytes over 4 letters", include_str!("../../algorithms/levenshtein.col"), input, expected, rust);
}
