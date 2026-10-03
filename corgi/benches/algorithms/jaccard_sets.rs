//! Jaccard similarity of two sets of integers.

use crate::common::{enc_f64, run_case, u64_lists, Cfg, Rng};
use corgi::Value;
use std::collections::HashSet;

fn reference(a: &[u64], b: &[u64]) -> f64 {
    let sa: HashSet<u64> = a.iter().copied().collect();
    let sb: HashSet<u64> = b.iter().copied().collect();
    let union = sa.union(&sb).count();
    if union == 0 {
        return 1.0;
    }
    sa.intersection(&sb).count() as f64 / union as f64
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(15);
    let list = |rng: &mut Rng| (0..cfg.list_len(rng, 33)).map(|_| rng.below(40 * cfg.scale as u64)).collect::<Vec<u64>>();
    let rows: Vec<(Vec<u64>, Vec<u64>)> = (0..cfg.list_rows()).map(|_| (list(&mut rng), list(&mut rng))).collect();
    let input = Value::Prod(vec![
        u64_lists(&rows.iter().map(|r| &r.0[..]).collect::<Vec<_>>()),
        u64_lists(&rows.iter().map(|r| &r.1[..]).collect::<Vec<_>>()),
    ]);
    let expected = Value::u64(rows.iter().map(|(a, b)| enc_f64(reference(a, b))).collect());
    let rust = || rows.iter().map(|(a, b)| reference(a, b)).collect::<Vec<_>>();
    run_case(cfg, "jaccard_sets", "two lists of 0-32 values below 40", include_str!("../../algorithms/jaccard_sets.col"), input, expected, rust);
}
