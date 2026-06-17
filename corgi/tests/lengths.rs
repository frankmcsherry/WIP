//! Length-checker tests: the Class-A size pass must REJECT the `Zip`/`Filter` programs that would
//! panic at runtime (bounds disagree) and ACCEPT the ones whose bounds provably share a source.
//! Soundness = no accepted program can hit a bounds panic; precision = the share-a-source idioms pass.

use corgi::{check_lengths, parse_ml, Shape};

fn ok(src: &str) -> bool {
    check_lengths(&parse_ml(src).unwrap(), &Shape::Prim(64)).is_ok()
}

#[test]
fn rejects_independent_zip() {
    // two iotas of different per-row lengths (x vs x+1): zip would panic. Must be rejected.
    assert!(!ok("input iota map (x -> (x iota, x add_u64 1 iota) zip)"));
}

#[test]
fn rejects_unshared_same_length_zip() {
    // even though both iotas have the SAME length, they are separate nodes (distinct tokens), so the
    // checker conservatively rejects — the escape is to share the source (next test).
    assert!(!ok("input iota map (x -> (x iota, x iota) zip)"));
}

#[test]
fn accepts_shared_zip() {
    // one `let`-shared list zipped with itself: a single node, one token — provable.
    assert!(ok("input iota map (x -> let xi = x iota in (xi, xi) zip)"));
}

#[test]
fn accepts_transpose_zip() {
    // transpose splits one list into columns that share its bounds, so zip re-pairs them provably.
    assert!(ok("input iota map (x -> (x, x)) transpose zip"));
}

#[test]
fn rejects_independent_filter() {
    // mask built independently of the data, with a different per-row length: filter would panic.
    assert!(!ok("input iota map (x -> (x iota, x add_u64 1 iota) filter)"));
}

#[test]
fn accepts_derived_mask_filter() {
    // the idiomatic filter: the mask is the data mapped pointwise (MapList preserves bounds), so the
    // data and mask provably share bounds. (Corpus program 04's pattern.)
    assert!(ok("let xs = input iota in (xs, xs map (x -> x gt 2)) filter"));
}
