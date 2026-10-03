//! Run-length encoding of a byte string.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::{Bounds, Value};

fn reference(s: &[u8]) -> Vec<(u8, u64)> {
    let mut runs: Vec<(u8, u64)> = Vec::new();
    for &c in s {
        match runs.last_mut() {
            Some((b, k)) if *b == c => *k += 1,
            _ => runs.push((c, 1)),
        }
    }
    runs
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(8);
    let inputs: Vec<Vec<u8>> = (0..cfg.rows)
        .map(|_| {
            // runs of 1-4 of a few letters, so runs are common.
            let mut s = Vec::new();
            let target = rng.below(25) as usize;
            while s.len() < target {
                let c = b"aab"[rng.below(3) as usize];
                let k = 1 + rng.below(4) as usize;
                s.extend(std::iter::repeat_n(c, k));
            }
            s.truncate(target);
            s
        })
        .collect();
    let encoded: Vec<Vec<(u8, u64)>> = inputs.iter().map(|s| reference(s)).collect();
    let mut ends = Vec::new();
    let (mut bytes, mut counts) = (Vec::new(), Vec::new());
    for r in &encoded {
        bytes.extend(r.iter().map(|x| x.0));
        counts.extend(r.iter().map(|x| x.1));
        ends.push(bytes.len());
    }
    let expected = Value::List(Bounds::offsets(ends), Box::new(Value::Prod(vec![Value::u8(bytes), Value::u64(counts)])));
    let rust = || inputs.iter().map(|s| reference(s)).collect::<Vec<_>>();
    let what = "strings of 0-24 bytes in runs of 1-4";
    run_case(cfg, "run_length_encode", what, include_str!("../../algorithms/run_length_encode.col"), bytes_col(&inputs), expected.clone(), rust);
    run_case(cfg, "run_length_encode_scan", what, include_str!("../../algorithms/run_length_encode_scan.col"), bytes_col(&inputs), expected, rust);
}
