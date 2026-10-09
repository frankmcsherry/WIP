//! Merging overlapping or touching half-open intervals.

use crate::common::{run_case, Cfg, Rng};
use crate::common_data::tuple_lists;

fn reference(ivs: &[[i64; 2]]) -> Vec<[i64; 2]> {
    let mut ivs = ivs.to_vec();
    ivs.sort_unstable();
    let mut out: Vec<[i64; 2]> = Vec::new();
    for [lo, hi] in ivs {
        match out.last_mut() {
            Some(last) if lo <= last[1] => last[1] = last[1].max(hi),
            _ => out.push([lo, hi]),
        }
    }
    out
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(11);
    let rows: Vec<Vec<[i64; 2]>> = (0..cfg.rows)
        .map(|_| {
            let n = rng.below(17) as usize;
            (0..n).map(|_| { let lo = rng.below(200); [lo, lo + 1 + rng.below(20)] }).collect()
        })
        .collect();
    let input = tuple_lists(&rows);
    let expected = tuple_lists(&rows.iter().map(|r| reference(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    run_case(cfg, "interval_merge", "0-16 intervals of length 1-20 in 0..220", include_str!("../../algorithms/interval_merge.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "interval_merge_cut", "0-16 intervals of length 1-20 in 0..220", include_str!("../../algorithms/interval_merge_cut.col"), input, expected, rust);
}
