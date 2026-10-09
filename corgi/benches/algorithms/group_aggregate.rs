//! GROUP BY with count, sum and max.

use crate::common::{run_case, Cfg, Rng};
use crate::common_data::tuple_lists;
use std::collections::BTreeMap;

fn reference(kvs: &[[i64; 2]]) -> Vec<[i64; 4]> {
    let mut groups: BTreeMap<i64, [i64; 3]> = BTreeMap::new();
    for &[k, v] in kvs {
        let g = groups.entry(k).or_insert([0, 0, 0]);
        g[0] += 1;
        g[1] += v;
        g[2] = g[2].max(v);
    }
    groups.into_iter().map(|(k, [n, s, m])| [k, n, s, m]).collect()
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(18);
    let rows: Vec<Vec<[i64; 2]>> = (0..cfg.list_rows())
        .map(|_| (0..cfg.list_len(&mut rng, 33)).map(|_| [rng.below(8), rng.below(1000)]).collect())
        .collect();
    let input = tuple_lists(&rows);
    let expected = tuple_lists(&rows.iter().map(|r| reference(r)).collect::<Vec<_>>());
    let rust = || rows.iter().map(|r| reference(r)).collect::<Vec<_>>();
    run_case(cfg, "group_aggregate", "0-32 (key below 8, value below 1000) pairs", include_str!("../../algorithms/group_aggregate.col"), input, expected, rust);
}
