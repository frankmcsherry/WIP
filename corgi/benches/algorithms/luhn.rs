//! The Luhn checksum of a digit string.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn reference(s: &[u8]) -> u64 {
    if s.is_empty() {
        return 0;
    }
    let sum: u64 = s
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &c)| {
            let d = (c - b'0') as u64;
            if i % 2 == 1 {
                let t = 2 * d;
                if t > 9 { t - 9 } else { t }
            } else {
                d
            }
        })
        .sum();
    sum.is_multiple_of(10) as u64
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(5);
    let mut numbers: Vec<Vec<u8>> = vec![b"79927398713".to_vec(), b"79927398710".to_vec(), b"".to_vec(), b"0".to_vec(), b"59".to_vec()];
    while numbers.len() < cfg.rows {
        let n = 12 + rng.below(8) as usize;
        let mut s = rng.string(n, b"0123456789");
        // half the numbers get the check digit that makes them valid.
        if rng.below(2) == 0 {
            let last = s.len() - 1;
            for d in b'0'..=b'9' {
                s[last] = d;
                if reference(&s) == 1 {
                    break;
                }
            }
        }
        numbers.push(s);
    }
    let input = bytes_col(&numbers);
    let expected = Value::u64(numbers.iter().map(|s| reference(s)).collect());
    let rust = || numbers.iter().map(|s| reference(s)).collect::<Vec<u64>>();
    run_case(cfg, "luhn", "digit strings of 12-19 digits, half valid", include_str!("../../algorithms/luhn.col"), input, expected, rust);
}
