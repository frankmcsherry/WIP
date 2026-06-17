//! Total-subset tests: `check_total` reports the unchecked kernel ops (`*_uns`) and only those.

use corgi::{check_total, parse_ml};

#[test]
fn total_subset_program_is_ok() {
    // index / branch_try / scan / select — all total or gated; no kernel-tier op.
    let g = parse_ml(
        "let table = input iota in \
         (input add_u64 2 iota, table) index map (e -> e match (0 (o -> o lit 0), 1 (v -> v)))",
    )
    .unwrap();
    assert!(check_total(&g).is_ok());
}

#[test]
fn flags_index_uns() {
    let g = parse_ml("input iota map (x -> x shr 1) map (p -> (p, p) index_uns)");
    // the above is one graph; the index_uns must be reported.
    let g = g.unwrap();
    assert_eq!(check_total(&g), Err(vec!["index_uns"]));
}

#[test]
fn flags_head_uns_inside_a_body() {
    // a kernel op buried in a map body still counts.
    let g = parse_ml("input iota map (x -> x add_u64 1 iota) map (r -> r sort head_uns)").unwrap();
    assert_eq!(check_total(&g), Err(vec!["head_uns"]));
}
