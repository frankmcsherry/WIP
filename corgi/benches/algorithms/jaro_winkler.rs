//! Jaro-Winkler similarity of two byte strings, as `strsim::jaro_winkler` computes it on ASCII.
//! Two programs: `direct` follows the textbook loop, `by_byte` reformulates the match per byte value.

use crate::common::{bytes_col, enc_f64, run_case, Cfg, Rng};
use corgi::Value;

fn reference(a: &[u8], b: &[u8]) -> f64 {
    let (la, lb) = (a.len(), b.len());
    if la == 0 && lb == 0 {
        return 1.0;
    }
    if la == 0 || lb == 0 {
        return 0.0;
    }
    let d = (la.max(lb) / 2).saturating_sub(1);
    let (mut fa, mut fb) = (vec![false; la], vec![false; lb]);
    let mut m = 0usize;
    for i in 0..la {
        for j in i.saturating_sub(d)..lb.min(i + d + 1) {
            if a[i] == b[j] && !fb[j] {
                fa[i] = true;
                fb[j] = true;
                m += 1;
                break;
            }
        }
    }
    if m == 0 {
        return 0.0;
    }
    let mut bs = (0..lb).filter(|&j| fb[j]);
    let t = (0..la).filter(|&i| fa[i]).filter(|&i| a[i] != b[bs.next().unwrap()]).count() / 2;
    let sim = ((m as f64 / la as f64) + (m as f64 / lb as f64) + ((m - t) as f64 / m as f64)) / 3.0;
    if sim > 0.7 {
        let p = a.iter().take(4).zip(b).take_while(|(x, y)| x == y).count();
        sim + 0.1 * p as f64 * (1.0 - sim)
    } else {
        sim
    }
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(1);
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.rows)
        .map(|_| {
            let (n1, n2) = (rng.below(16) as usize, rng.below(16) as usize);
            (rng.string(n1, b"abcde"), rng.string(n2, b"abcde"))
        })
        .collect();
    let input = Value::Prod(vec![
        bytes_col(&pairs.iter().map(|p| &p.0).collect::<Vec<_>>()),
        bytes_col(&pairs.iter().map(|p| &p.1).collect::<Vec<_>>()),
    ]);
    let expected = Value::u64(pairs.iter().map(|(a, b)| enc_f64(reference(a, b))).collect());
    let rust = || pairs.iter().map(|(a, b)| reference(a, b)).collect::<Vec<f64>>();
    let what = "two strings of 0-15 bytes over 5 letters";
    run_case(cfg, "jaro_winkler_direct", what, include_str!("../../algorithms/jaro_winkler_direct.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "jaro_winkler_by_byte", what, include_str!("../../algorithms/jaro_winkler_by_byte.col"), input, expected, rust);
}
