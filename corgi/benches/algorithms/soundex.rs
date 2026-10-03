//! American Soundex code of an ASCII word.

use crate::common::{bytes_col, run_case, Cfg, Rng};

fn code(c: u8) -> u8 {
    b"01230120022455012623010202"[(c.to_ascii_lowercase() - b'a') as usize]
}

fn reference(w: &[u8]) -> Vec<u8> {
    let Some(&first) = w.first() else { return b"0000".to_vec() };
    let mut out = vec![first.to_ascii_uppercase()];
    let mut last = code(first);
    for &c in &w[1..] {
        let d = code(c);
        if d != b'0' && d != last {
            out.push(d);
        }
        if !matches!(c.to_ascii_lowercase(), b'h' | b'w') {
            last = d;
        }
    }
    out.resize(4, b'0');
    out.truncate(4);
    out
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(3);
    let letters = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZhhwwaeiou";
    let mut words: Vec<Vec<u8>> = ["Robert", "Rupert", "Rubin", "Ashcraft", "Ashcroft", "Tymczak", "Pfister", "Honeyman", "", "a", "hhw"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    while words.len() < cfg.rows {
        let n = rng.below(13) as usize;
        words.push(rng.string(n, letters));
    }
    let input = bytes_col(&words);
    let expected = bytes_col(&words.iter().map(|w| reference(w)).collect::<Vec<_>>());
    let rust = || words.iter().map(|w| reference(w)).collect::<Vec<_>>();
    run_case(cfg, "soundex", "words of 0-12 letters", include_str!("../../algorithms/soundex.col"), input, expected, rust);
}
