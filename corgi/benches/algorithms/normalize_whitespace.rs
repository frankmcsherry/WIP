//! Whitespace and case normalization of a text field.

use crate::common::{bytes_col, run_case, Cfg, Rng};

fn reference(s: &[u8]) -> Vec<u8> {
    let lower = s.to_ascii_lowercase();
    let words: Vec<&[u8]> = lower.split(|&c| c == b' ' || c == b'\t').filter(|w| !w.is_empty()).collect();
    words.join(&b' ')
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(9);
    let mut inputs: Vec<Vec<u8>> = ["  Hello \t World ", "", "   ", "a", " a ", "A\tB"].iter().map(|s| s.as_bytes().to_vec()).collect();
    while inputs.len() < cfg.rows {
        let n = rng.below(25) as usize;
        inputs.push(rng.string(n, b"abcABC   \t"));
    }
    let expected = bytes_col(&inputs.iter().map(|s| reference(s)).collect::<Vec<_>>());
    let rust = || inputs.iter().map(|s| reference(s)).collect::<Vec<_>>();
    run_case(cfg, "normalize_whitespace", "text of 0-24 bytes, about 40% spaces and tabs", include_str!("../../algorithms/normalize_whitespace.col"), bytes_col(&inputs), expected, rust);
}
