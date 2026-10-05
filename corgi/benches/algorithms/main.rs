//! Algorithms written the way a user would write them, each checked against a plain-Rust
//! reference and timed against it: a corpus for finding what the optimizer should rewrite.
//!
//!   cargo bench --bench algorithms [-- NAME ...] [--check] [--explain] [--rows N]
//!   cargo bench --bench algorithms --features profile -- NAME --profile
//!
//! `--check` compares on small inputs without timing; `--explain` prints each program's lowered
//! graph; `--profile` prints time per op; `--optimize` runs `corgi::optimize` on each program first;
//! `--scale K` makes the lists of numbers K times longer in K times fewer rows.

mod common;
mod common_data;

mod balanced_brackets;
mod base64_encode;
mod days_from_civil;
mod gcd;
mod group_aggregate;
mod histogram;
mod horner;
mod interval_merge;
mod ipv4_parse;
mod jaccard_sets;
mod jaro_winkler;
mod kadane;
mod levenshtein;
mod linear_regression;
mod luhn;
mod median_percentile;
mod mode;
mod moving_average;
mod normalize_whitespace;
mod query_param;
mod run_length_encode;
mod sessionize;
mod sort_pairs;
mod soundex;
mod text_misc;
mod top_k;
mod trigram_similarity;
mod two_sum;

use common::Cfg;

/// a family of cases: its name (or its cases' names) and its runner.
type Family = (&'static str, fn(&Cfg));

/// every family.
const CASES: &[Family] = &[
    ("balanced_brackets", balanced_brackets::run),
    ("base64_encode", base64_encode::run),
    ("days_from_civil", days_from_civil::run),
    ("gcd", gcd::run),
    ("group_aggregate", group_aggregate::run),
    ("histogram", histogram::run),
    ("horner", horner::run),
    ("interval_merge", interval_merge::run),
    ("ipv4_parse", ipv4_parse::run),
    ("jaccard_sets", jaccard_sets::run),
    ("jaro_winkler", jaro_winkler::run),
    ("kadane", kadane::run),
    ("levenshtein", levenshtein::run),
    ("linear_regression", linear_regression::run),
    ("luhn", luhn::run),
    ("median_percentile", median_percentile::run),
    ("mode", mode::run),
    ("moving_average", moving_average::run),
    ("normalize_whitespace", normalize_whitespace::run),
    ("query_param", query_param::run),
    ("run_length_encode", run_length_encode::run),
    ("sessionize", sessionize::run),
    ("sort_pairs argsort_pairs top_pairs", sort_pairs::run),
    ("soundex", soundex::run),
    ("substring_count itoa word_topk", text_misc::run),
    ("top_k", top_k::run),
    ("trigram_similarity", trigram_similarity::run),
    ("two_sum", two_sum::run),
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let flag = |f: &str| args.iter().any(|a| a == f);
    let check = flag("--check");
    let arg = |f: &str| args.iter().position(|a| a == f).map(|i| args[i + 1].parse::<usize>().expect("a number"));
    let rows = arg("--rows").unwrap_or(if check { 2_000 } else { 1 << 16 });
    let names: Vec<String> = args.iter().filter(|a| !a.starts_with("--") && a.parse::<usize>().is_err()).cloned().collect();
    let cfg = Cfg { rows, check, explain: flag("--explain"), profile: flag("--profile"), optimize: flag("--optimize"), scale: arg("--scale").unwrap_or(1), names: names.clone() };
    // a family runs when a name is part of its key or its key part of a name (`kadane_prefix`);
    // `run_case` then runs only the cases a name matches.
    for (family, run) in CASES {
        if names.is_empty() || names.iter().any(|n| family.contains(n.as_str()) || n.contains(family)) {
            run(&cfg);
        }
    }
}
