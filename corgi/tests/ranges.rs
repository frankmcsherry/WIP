//! Range-checker tests: the Class-B pass must REJECT a `branch k` whose tag isn't proven `< k` (it
//! would panic at runtime) and ACCEPT the masked/compared/structured tags that are. Soundness = no
//! accepted branch can route out of range; precision = the `and`/comparison idioms (and ranges
//! threaded through zip/field/map) pass.

use corgi::{check_ranges, parse_ml, Shape};

fn ok(src: &str) -> bool {
    check_ranges(&parse_ml(src).unwrap(), &Shape::Prim(64)).is_ok()
}

#[test]
fn rejects_branch_on_fold_accumulator() {
    // REGRESSION: the accumulator is the fed-back output, so the seed's range is valid only at round 0.
    // Branching on a climbing accumulator must be rejected even though the seed is in range — earlier
    // this passed all gates and then panicked at eval (Branch: tag out of range). The accumulator slot
    // must be ⊤ at the fold back-edge.
    assert!(!ok("let xs = input iota in let seed = input and 1 in \
                 (seed, xs) foldscan ((acc, x) -> (acc add_u64 1, (x, acc) branch 2))"));
    // the element keeps its range, so a branch on a freshly-masked ELEMENT inside a fold body is fine.
    assert!(ok("let xs = input iota in let seed = input and 1 in \
                (seed, xs) foldscan ((acc, x) -> (acc add_u64 1, (x, x and 1) branch 2))"));
}

#[test]
fn rejects_unbounded_tag() {
    // the tag is the iota element itself (range [0,n), unknown) — branch 2 could route past lane 1.
    assert!(!ok("input iota map (x -> (x, x) branch 2)"));
}

#[test]
fn rejects_mask_too_wide() {
    // `and 7` gives [0,7], not < 4 — rejected (the bound and the arity disagree).
    assert!(!ok("input iota map (x -> (x, x and 7) branch 4)"));
}

#[test]
fn accepts_and_mask() {
    // `and 1` -> [0,1] < 2; `and 3` -> [0,3] < 4.
    assert!(ok("input iota map (x -> (x, x and 1) branch 2)"));
    assert!(ok("input iota map (x -> (x, x and 3) branch 4)"));
}

#[test]
fn accepts_comparison_mask() {
    // a comparison yields a 0/1 mask -> [0,1] < 2.
    assert!(ok("input iota map (x -> (x, x gt 3) branch 2)"));
}

#[test]
fn accepts_range_threaded_through_zip() {
    // the corpus-34 shape: the mask is built by `and 1`, zipped with data, then a field-1 branch —
    // the [0,1] range must survive map -> zip -> field for this to prove.
    assert!(ok(
        "let data = input iota in \
         let mask = data map (x -> x and 1) in \
         (data, mask) zip map (p -> (p.0, p.1) branch 2)"
    ));
}
