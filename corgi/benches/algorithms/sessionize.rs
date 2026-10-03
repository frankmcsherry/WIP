//! Sessions in a sorted stream of event times: a gap over 30 starts a new session.

use crate::common::{run_case, u64_lists, Cfg, Rng};
use corgi::Value;

fn reference(ts: &[u64]) -> (u64, u64) {
    let (mut sessions, mut longest, mut run) = (0u64, 0u64, 0u64);
    for (i, &t) in ts.iter().enumerate() {
        if i == 0 || t - ts[i - 1] > 30 {
            sessions += 1;
            run = 0;
        }
        run += 1;
        longest = longest.max(run);
    }
    (sessions, longest)
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(12);
    let rows: Vec<Vec<u64>> = (0..cfg.rows)
        .map(|_| {
            let n = rng.below(33) as usize;
            let mut t = rng.below(100);
            (0..n).map(|_| { t += rng.below(50); t }).collect()
        })
        .collect();
    let input = u64_lists(&rows);
    let out: Vec<(u64, u64)> = rows.iter().map(|r| reference(r)).collect();
    let expected = Value::Prod(vec![Value::u64(out.iter().map(|o| o.0).collect()), Value::u64(out.iter().map(|o| o.1).collect())]);
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    run_case(cfg, "sessionize", "0-32 sorted events, gaps 0-49, break over 30", include_str!("../../algorithms/sessionize.col"), input, expected, rust);
}
