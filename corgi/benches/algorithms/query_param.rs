//! Look up a parameter in a URL query string.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::Value;

fn reference(q: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    for p in q.split(|&c| c == b'&') {
        let mut it = p.split(|&c| c == b'=');
        let k = it.next().unwrap();
        let v = it.next().unwrap_or(&[]);
        if k == key {
            return Some(v.to_vec());
        }
    }
    None
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(10);
    let keys: [&[u8]; 6] = [b"a", b"b", b"id", b"q", b"page", b"x"];
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.rows)
        .map(|_| {
            let mut q = Vec::new();
            for k in 0..rng.below(6) {
                if k > 0 {
                    q.push(b'&');
                }
                q.extend_from_slice(keys[rng.below(5) as usize]);
                if rng.below(8) != 0 {
                    q.push(b'=');
                    let n = rng.below(8) as usize;
                    q.extend(rng.string(n, b"0123456789abcdef"));
                }
            }
            (q, keys[rng.below(6) as usize].to_vec())
        })
        .collect();
    let found: Vec<Option<Vec<u8>>> = rows.iter().map(|(q, k)| reference(q, k)).collect();
    let tags = found.iter().map(|f| f.is_none() as usize).collect();
    let values: Vec<&Vec<u8>> = found.iter().flatten().collect();
    let missing = found.iter().filter(|f| f.is_none()).count();
    let expected = Value::sum(tags, vec![bytes_col(&values), Value::Unit(missing)]);
    let input = Value::Prod(vec![
        bytes_col(&rows.iter().map(|r| &r.0).collect::<Vec<_>>()),
        bytes_col(&rows.iter().map(|r| &r.1).collect::<Vec<_>>()),
    ]);
    let rust = || rows.iter().map(|(q, k)| reference(q, k)).collect::<Vec<_>>();
    run_case(cfg, "query_param", "queries of 0-5 pairs over 5 keys", include_str!("../../algorithms/query_param.col"), input, expected, rust);
}
