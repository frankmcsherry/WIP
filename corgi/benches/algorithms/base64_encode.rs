//! Base64 encoding (standard alphabet, '=' padding).

use crate::common::{bytes_col, run_case, Cfg, Rng};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn reference(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len().div_ceil(3) * 4);
    for g in s.chunks(3) {
        let b = [g[0], *g.get(1).unwrap_or(&0), *g.get(2).unwrap_or(&0)];
        let v = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(ALPHABET[(v >> 18) as usize & 63]);
        out.push(ALPHABET[(v >> 12) as usize & 63]);
        out.push(if g.len() > 1 { ALPHABET[(v >> 6) as usize & 63] } else { b'=' });
        out.push(if g.len() > 2 { ALPHABET[v as usize & 63] } else { b'=' });
    }
    out
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(6);
    let all: Vec<u8> = (0..=255).collect();
    let mut inputs: Vec<Vec<u8>> = vec![b"".to_vec(), b"f".to_vec(), b"fo".to_vec(), b"foo".to_vec(), b"foob".to_vec(), b"fooba".to_vec(), b"foobar".to_vec()];
    while inputs.len() < cfg.rows {
        let n = rng.below(25) as usize;
        inputs.push(rng.string(n, &all));
    }
    let input = bytes_col(&inputs);
    let expected = bytes_col(&inputs.iter().map(|s| reference(s)).collect::<Vec<_>>());
    let rust = || inputs.iter().map(|s| reference(s)).collect::<Vec<_>>();
    let what = "byte strings of 0-24 bytes";
    run_case(cfg, "base64_encode", what, include_str!("../../algorithms/base64_encode.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "base64_encode_arith", what, include_str!("../../algorithms/base64_encode_arith.col"), input, expected, rust);
}
