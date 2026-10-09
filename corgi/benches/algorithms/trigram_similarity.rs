//! pg_trgm-style trigram similarity of two strings.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn grams(s: &[u8]) -> Vec<[u8; 3]> {
    let mut p = b"  ".to_vec();
    p.extend(s.iter().map(|c| c.to_ascii_lowercase()));
    p.push(b' ');
    let mut g: Vec<[u8; 3]> = p.windows(3).map(|w| [w[0], w[1], w[2]]).collect();
    g.sort_unstable();
    g.dedup();
    g
}

fn reference(a: &[u8], b: &[u8]) -> f64 {
    let (ga, gb) = (grams(a), grams(b));
    let shared = ga.iter().filter(|g| gb.binary_search(g).is_ok()).count();
    shared as f64 / (ga.len() + gb.len() - shared) as f64
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(4);
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.rows)
        .map(|_| {
            let (n1, n2) = (rng.below(17) as usize, rng.below(17) as usize);
            (rng.string(n1, b"abcdeABCDE"), rng.string(n2, b"abcdeABCDE"))
        })
        .collect();
    let input = Value::Prod(vec![
        bytes_col(&pairs.iter().map(|p| &p.0).collect::<Vec<_>>()),
        bytes_col(&pairs.iter().map(|p| &p.1).collect::<Vec<_>>()),
    ]);
    let expected = Value::f64(pairs.iter().map(|(a, b)| reference(a, b)).collect());
    let rust = || pairs.iter().map(|(a, b)| reference(a, b)).collect::<Vec<f64>>();
    run_case(cfg, "trigram_similarity", "two strings of 0-16 bytes over 5 letters, mixed case", include_str!("../../algorithms/trigram_similarity.col"), input, expected, rust);
}
