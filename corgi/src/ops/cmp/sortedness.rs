//! Is this column ALREADY in order, and if so what are its runs?
//!
//! The question `sort`, `dedup` and `group` ask before they sort. A sort costs 20 to 40 times
//! the question, and an unsorted column pays a few loads for it, since the scan exits at the
//! first inversion. It is answered from the LEADING component of the structural order, a leaf's
//! value, a product's leading leaf, a list's row LENGTH, a sum's TAG, which settles the question
//! in either direction in one pass: an inversion rules the column out whatever follows, and a
//! strictly increasing leading component rules it in, because a lexicographic order whose first
//! component separates every adjacent pair never consults the rest.
//!
//! Where it applies: a leaf, or a product of leaves, is settled here. A list or sum whose
//! leading component ties DECLINES, so a `dedup` or `group` after a `sort` of strings or of a
//! sum column, which is the corpus's `uniq -c` idiom, still sorts. Declining is always safe,
//! since the caller then sorts. [`sorted_signs`] returns the adjacent-order signs rather than a
//! bool because they ARE the run structure the sorted path needs next: a zero is a duplicate, a
//! nonzero a boundary, which is what `dedup` and `group` would otherwise sort to discover.

use super::order::compare_adjacent;
use crate::value::{Bounds, Prim, Value};

/// Does this value compare EQUAL on every pair of rows, with no payload anywhere? Only a `Unit`
/// and a product built from those: a `Prim` has a value, and a `List` or `Sum` has a length or a
/// tag, which the order reads even when the payload below is itself payload-free. This is the one
/// thing that licenses [`Leading::AllEqual`], and it is decided on the shape, not the data.
fn payload_free(v: &Value) -> bool {
    match v {
        Value::Unit(_) => true,
        Value::Prod(cols) => cols.iter().all(payload_free),
        _ => false,
    }
}

/// The first leaf a structural order reads, when the order starts at one: a `Prim`, or a `Prod`'s
/// first field that carries a payload at all, recursively. `None` when the order starts somewhere
/// this cannot express, a leading `List`'s length or a leading `Sum`'s tag, and the caller then
/// has to decide the question the expensive way.
///
/// Payload-free leading fields are SKIPPED, not fatal: a `Unit` field compares equal on every pair,
/// so the order falls straight through it to the next field. `(Unit, key)` is the padded tuple a
/// host builds when a field is absent, and its leading component is the key.
fn leading_leaf(v: &Value) -> Option<&Prim> {
    match v {
        Value::Prim(p) => Some(p),
        Value::Prod(cols) => cols.iter().find(|c| !payload_free(c)).and_then(leading_leaf),
        _ => None,
    }
}

/// What one pass over a column's LEADING component settles about its order.
enum Leading {
    /// It decreases somewhere: the column is NOT in order, whatever the rest says.
    Inversion,
    /// It strictly increases, so it already separates every adjacent pair: the column IS in order,
    /// and nothing below it is consulted. A compound key whose leading field is an identifier or
    /// a hash lands here.
    Strict,
    /// Every row compares equal at every level (no payload at all): in order, trivially.
    AllEqual,
    /// Non-decreasing with equal neighbours: what lies below decides.
    Ties,
}

/// Scan a monotone per-element key within each row of `bounds`, exiting at the first inversion.
fn scan_key(bounds: &Bounds, key: impl Fn(usize) -> u64) -> Leading {
    let mut strict = true;
    let mut start = 0;
    for end in bounds.ends() {
        for i in start + 1..end {
            let (a, b) = (key(i - 1), key(i));
            if a > b {
                return Leading::Inversion;
            }
            strict &= a != b;
        }
        start = end;
    }
    if strict { Leading::Strict } else { Leading::Ties }
}

fn leading_order(bounds: &Bounds, vals: &Value) -> Leading {
    match vals {
        // no payload to compare: every row is equal to every other. This is the ONLY thing that
        // may claim `AllEqual`: a product whose leading field is a `List` also has no leading
        // leaf, and it very much does not compare equal on every pair.
        v if payload_free(v) => Leading::AllEqual,
        Value::Prim(_) | Value::Prod(_) => {
            // A leading `List` or `Sum` inside the product: the order starts at a length or a tag
            // that this scan cannot reach, so nothing is established yet and the `Ties` path
            // settles it structurally.
            let Some(p) = leading_leaf(vals) else { return Leading::Ties };
            let mut strict = true;
            let mut start = 0;
            for end in bounds.ends() {
                match p.order_of_range(start, end) {
                    None => return Leading::Inversion,
                    Some(s) => strict &= s,
                }
                start = end;
            }
            if strict { Leading::Strict } else { Leading::Ties }
        }
        // length-first: a row's length is what the order reads before any element.
        Value::List(inner, _) => scan_key(bounds, |i| {
            let (s, e) = inner.span(i);
            (e - s) as u64
        }),
        // tag-first.
        Value::Sum(tags, _) => scan_key(bounds, |i| tags.tag_at(i) as u64),
        // the payload-free guard above takes every `Unit`; this arm is what the exhaustiveness
        // check needs, since it does not read guards.
        Value::Unit(_) => Leading::AllEqual,
    }
}

/// Can we CHEAPLY establish that every row of `bounds` is already in non-decreasing structural
/// order? `false` means "not established", which is not the same as "not sorted"; declining is
/// always safe, since the caller then sorts.
///
/// A leaf settles the whole question in its own scan, and a product narrows to the surviving
/// ties field by field, so its structural pass costs in proportion to what the leading field left
/// undecided. A list or sum whose leading component ties would need a full structural pass, which
/// is not cheaper than the sort it would save, so we decline rather than spend it on a question
/// the sort answers anyway.
pub(crate) fn known_sorted(bounds: &Bounds, vals: &Value) -> bool {
    match leading_order(bounds, vals) {
        Leading::Inversion => false,
        Leading::Strict | Leading::AllEqual => true,
        Leading::Ties => match vals {
            Value::Prim(_) => true,
            Value::Prod(_) => signs_sorted(bounds, &compare_adjacent(vals)),
            _ => false,
        },
    }
}

/// The adjacent-order signs of `vals` when [`known_sorted`] holds, `None` otherwise. The signs
/// come back rather than a bool because they ARE what the sorted path needs next: `out[k]`
/// compares flattened row `k` with row `k+1`, so a zero marks a duplicate and a nonzero a run
/// boundary, the run structure `dedup` and `group` would otherwise sort to discover.
pub(crate) fn sorted_signs(bounds: &Bounds, vals: &Value) -> Option<Vec<i8>> {
    let established = match leading_order(bounds, vals) {
        Leading::Inversion => false,
        Leading::Strict | Leading::AllEqual => true,
        Leading::Ties => matches!(vals, Value::Prim(_) | Value::Prod(_)),
    };
    if !established {
        return None;
    }
    let signs = compare_adjacent(vals);
    signs_sorted(bounds, &signs).then_some(signs)
}

/// Do the adjacent signs describe rows that are each non-decreasing? Row boundaries are skipped: a
/// row's last element may exceed the next row's first without the column being out of order.
fn signs_sorted(bounds: &Bounds, signs: &[i8]) -> bool {
    let mut start = 0;
    for end in bounds.ends() {
        if end > start && signs[start..end - 1].iter().any(|&o| o > 0) {
            return false;
        }
        start = end;
    }
    true
}

/// First index of each maximal equal-value run, given per-row `bounds` and the adjacent signs of
/// an already-sorted column ([`sorted_signs`]). A row boundary always starts a run, since runs
/// never cross rows: the same partition `run_layout` reads off a sorted column's refined labels.
pub(crate) fn run_firsts(bounds: &Bounds, signs: &[i8]) -> Vec<usize> {
    let mut firsts = Vec::new();
    let mut start = 0;
    for end in bounds.ends() {
        for k in start..end {
            if k == start || signs[k - 1] != 0 {
                firsts.push(k);
            }
        }
        start = end;
    }
    firsts
}
