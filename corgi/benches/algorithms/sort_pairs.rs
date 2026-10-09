//! Sorting (a, b) pairs: by one sort, a column at a time, as positions, and only the first ten.

use crate::common::{int_lists, run_case, Cfg, Rng};
use crate::common_data::tuple_lists;

fn sorted(r: &[[i64; 2]]) -> Vec<[i64; 2]> {
    let mut r = r.to_vec();
    r.sort_unstable();
    r
}

/// positions in sorted order, ties in input order.
fn argsort(r: &[[i64; 2]]) -> Vec<i64> {
    let mut at: Vec<i64> = (0..r.len() as i64).collect();
    at.sort_by_key(|&i| r[i as usize]);
    at
}

fn top10(r: &[[i64; 2]]) -> Vec<[i64; 2]> {
    let mut r = r.to_vec();
    if r.len() > 10 {
        r.select_nth_unstable(10);
        r.truncate(10);
    }
    r.sort_unstable();
    r
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(19);
    // about four pairs share each value of a, at any `--scale`
    let a_below = 8 * cfg.scale as i64;
    let rows: Vec<Vec<[i64; 2]>> = (0..cfg.list_rows())
        .map(|_| (0..cfg.list_len(&mut rng, 33)).map(|_| [rng.below(a_below), rng.below(1_000_000)]).collect())
        .collect();
    let input = tuple_lists(&rows);
    let what = "0-32 (a, b) pairs, a below 8";
    let expected = tuple_lists(&rows.iter().map(|r| sorted(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| sorted(r)).collect::<Vec<_>>();
    run_case(cfg, "sort_pairs", what, include_str!("../../algorithms/sort_pairs.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "sort_pairs_steps", what, include_str!("../../algorithms/sort_pairs_steps.col"), input.clone(), expected, rust);
    let expected = int_lists(&rows.iter().map(|r| argsort(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| argsort(r)).collect::<Vec<_>>();
    run_case(cfg, "argsort_pairs", what, include_str!("../../algorithms/argsort_pairs.col"), input.clone(), expected, rust);
    let expected = tuple_lists(&rows.iter().map(|r| top10(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| top10(r)).collect::<Vec<_>>();
    run_case(cfg, "top_pairs", what, include_str!("../../algorithms/top_pairs.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "top_pairs_limit", what, include_str!("../../algorithms/top_pairs_limit.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "top_pairs_steps", what, include_str!("../../algorithms/top_pairs_steps.col"), input, expected, rust);
}
