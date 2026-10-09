//! Parse a dotted-quad IPv4 address.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn reference(s: &[u8]) -> Option<i64> {
    let mut parts = 0;
    let mut addr = 0i64;
    for p in s.split(|&c| c == b'.') {
        parts += 1;
        if p.is_empty() || p.len() > 3 || !p.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let v = p.iter().fold(0i64, |v, &c| v * 10 + (c - b'0') as i64);
        if v > 255 {
            return None;
        }
        addr = addr * 256 + v;
    }
    (parts == 4).then_some(addr)
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(7);
    let mut inputs: Vec<Vec<u8>> = ["192.168.0.1", "10.0.0.255", "010.1.2.3", "1.2.3", "1.2.3.4.5", "256.1.1.1", "1..2.3", "", "1.2.3.4x", "1000.1.1.1"]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    while inputs.len() < cfg.rows {
        let mut s: Vec<u8> = Vec::new();
        let parts = if rng.below(10) == 0 { 3 + rng.below(3) } else { 4 };
        for k in 0..parts {
            if k > 0 {
                s.push(b'.');
            }
            let v = if rng.below(20) == 0 { 256 + rng.below(800) } else { rng.below(256) };
            let mut t = v.to_string();
            if rng.below(20) == 0 {
                t.insert(0, '0');
            }
            s.extend_from_slice(t.as_bytes());
        }
        if rng.below(30) == 0 {
            let at = rng.below(s.len() as i64) as usize;
            s[at] = b'x';
        }
        inputs.push(s);
    }
    let parsed: Vec<Option<i64>> = inputs.iter().map(|s| reference(s)).collect();
    let tags = parsed.iter().map(|p| p.is_none() as usize).collect();
    let addrs = parsed.iter().flatten().copied().collect();
    let bad = parsed.iter().filter(|p| p.is_none()).count();
    let expected = Value::sum(tags, vec![Value::i64(addrs), Value::Unit(bad)]);
    let rust = || inputs.iter().map(|s| reference(s)).collect::<Vec<_>>();
    run_case(cfg, "ipv4_parse", "dotted quads, about 1 in 6 invalid", include_str!("../../algorithms/ipv4_parse.col"), bytes_col(&inputs), expected, rust);
}
