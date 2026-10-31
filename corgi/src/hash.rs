//! Structural, content-addressed hashing: `hash(v) -> U64 column`, one stable u64 per row.
//!
//! The hash-analogue of the structural comparator ([`crate::ops::cmp`]'s `compare_cols`): it folds
//! the SAME value structure — leaf words, `Prod` field-by-field, `List` length and ordered contents, `Sum`
//! tag-then-payload — so it is CONSISTENT WITH corgi's structural equality. Rows that compare equal
//! (hence collapse under `sort`/`dedup`/`group`) hash identically; the converse holds only up to the
//! birthday bound (collisions are accepted, see below).
//!
//! Three properties the boundary relies on:
//!   * KIND-BLIND / structural — any shape (`Prim`/`Prod`/`Sum`/`List`/`Unit`), reading stored leaf
//!     bytes, so signed/float encodings hash by their stored form just as equality compares them.
//!   * COLUMNAR — one bottom-up pass, no per-row `Value`; a `Sum` reads its carried within-variant
//!     offset O(1) per row (no rank rescan), exactly like `compare_idx`.
//!   * STABLE — pure integer math with fixed seeds, no address or batch dependence — so
//!     the same value yields the same u64 on every machine and every run.
//!
//! REPRESENTATION-BLIND — the id addresses the VALUE, not its layout. A `List`'s `Stride` and the
//! equivalent `Offsets` hash the same (by partition), and a leaf's WIDTH drops out for the raw/unsigned
//! reading: `u8` 5 and `u64` 5 collapse to one id (the fold widens every leaf to u64), so a
//! narrowing/widening for storage is id-preserving and a join can match equal keys carried at
//! different widths. A list of leaves hashes as bytes when every word is in 0..=255, otherwise
//! as little-endian u64 words under a separate seed. This decision is per row, including references;
//! neither the containing batch nor the storage variant affects it. Float leaves use their stored
//! order keys, as does structural equality.
//!
//! This is the boundary id function: present a record as `((hash(key), hash(value)), time, diff)` and
//! the ids stay equal-for-equal-values across operators and runs (a reduce OUTPUT hashes to the same
//! id when it reappears downstream as an INPUT). 64-bit, collisions ~ n²/2⁶⁵ accepted; a 128-bit
//! widening is a later change to the accumulator type here, never a registry.
//!
//! NB: the fold reads only lanes some row's tag NAMES, so two `Sum`s that differ only in an
//! unreferenced (empty) lane's shape hash equal — every row's OBSERVABLE value is identical, so sharing an
//! id is correct (and more stable than derived `PartialEq`, which would call them distinct).

use crate::value::{Prim, Value};

mod foldhash;

/// splitmix64 finalizer — a full-avalanche 64-bit mix for primitive leaves ([`Prim::hashes`])
/// and the structural [`combine`]. Lists of leaves use foldhash quality instead.
pub(crate) fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// per-constructor salts: seed each structural node from its own constant, so a bare leaf, a 1-tuple,
// a length-1 list, and an injected value don't collapse together. Arbitrary fixed words (hex digits
// of π) — they PIN the hash: changing one re-ids the whole system.
const PROD: u64 = 0x243f_6a88_85a3_08d3;
const SUM: u64 = 0x1319_8a2e_0370_7344;
const LIST: u64 = 0xa409_3822_299f_31d0;
const UNIT: u64 = 0x082e_fa98_ec4e_6c89;

/// fold one child hash into an accumulator — ORDER-SENSITIVE, so field/element order and tag position
/// all matter (mix the child to avalanche it, xor in, then multiply by an odd word).
fn combine(acc: u64, x: u64) -> u64 {
    (acc ^ mix64(x)).wrapping_mul(K)
}

/// [`combine`]'s multiplier.
const K: u64 = 0x9e37_79b9_7f4a_7c15;

/// the columnar structural hash: `out[r]` is the stable id of row `r` of `v`. One bottom-up pass;
/// each level folds its children exactly as the comparator orders them.
///
/// THE id function — there is one, and every caller reaches it here. `Op::Hash` wraps the result as
/// a `U64` column because an op must return a `Value`; nothing else wants the wrapper, so nothing
/// else pays for it.
pub fn hash(v: &Value) -> Vec<u64> {
    match v {
        Value::Prim(p) => p.hashes(),

        // product = fold the fields in order, each seeded from the PROD salt. A fieldless product has
        // no length witness (`len` is 0), so this is empty — consistent with `Value::len`.
        Value::Prod(cols) => {
            let mut acc = vec![PROD; v.len()];
            for c in cols {
                // Leaves can fold directly into the parent. Materializing their hashes
                // only to consume them here adds a full-width temporary per field.
                match c {
                    Value::Prim(p) => p.fold_hashes(&mut acc, combine),
                    Value::Unit(n) => {
                        for a in acc.iter_mut().take(*n) {
                            *a = combine(*a, UNIT);
                        }
                    }
                    _ => {
                        for (a, x) in acc.iter_mut().zip(hash(c)) {
                            *a = combine(*a, x);
                        }
                    }
                }
            }
            acc
        }

        // sum = tag first, then the payload read from the row's lane at its carried within-variant
        // offset.
        Value::Sum(tags, variants) => {
            let lanes: Vec<Vec<u64>> = variants.iter().map(hash).collect();
            // the assignment is read in place: this fold looks at each row's tag and offset exactly
            // once, so decoding either into a wider column first is pure overhead.
            (0..tags.len())
                .map(|r| {
                    let t = tags.tag_at(r);
                    combine(combine(SUM, t as u64), lanes[t][tags.offset_at(r)])
                })
                .collect()
        }

        // Lists hash by SPAN — so a `Stride` and the
        // equivalent `Offsets` hash identically (matching `Bounds` equality, which is by the
        // partition, not its representation). A list of leaves uses a single row hash.
        Value::List(bounds, vals) if matches!(**vals, Value::Prim(_)) => {
            let ends: Vec<usize> = bounds.ends().collect();
            leaf_list_rows(vals, ends.len(), |r| (if r == 0 { 0 } else { ends[r - 1] }, ends[r]))
        }
        Value::List(bounds, vals) => {
            let ch = hash(vals);
            (0..bounds.len()).map(|r| hash_span(&ch, bounds.span(r))).collect()
        }

        // unit = no payload; every row hashes to the same constant.
        Value::Unit(n) => vec![UNIT; *n],

        // a referenced row hashes as the list row it names (a reference has the identity of what it
        // names). A list of leaves hashes just the rows named; any other arena is hashed whole.
        Value::Ref(list, rows) => match &**list {
            Value::List(bounds, vals) if matches!(**vals, Value::Prim(_)) => {
                leaf_list_rows(vals, rows.len(), |i| bounds.span(rows[i]))
            }
            _ => {
                let rh = hash(list);
                rows.iter().map(|&r| rh[r]).collect()
            }
        },
    }
}

/// `n` rows of a list of leaves, row `i` the elements `span(i)` of `vals`: the hash of each row,
/// computed in the row's own loop.
fn leaf_list_rows(vals: &Value, n: usize, span: impl Fn(usize) -> (usize, usize)) -> Vec<u64> {
    match vals {
        Value::Prim(Prim::U8(bytes)) => byte_rows(n, span, bytes),
        Value::Prim(Prim::I64(xs)) => leaf_rows(n, span, xs, |x| x as u64),
        Value::Prim(Prim::F64(keys)) => leaf_rows(n, span, keys, |k| k),
        _ => unreachable!("a list of leaves"),
    }
}

/// Lists of leaves use foldhash quality, with distinct domains for bytes and words.
fn byte_rows(n: usize, span: impl Fn(usize) -> (usize, usize), bytes: &[u8]) -> Vec<u64> {
    (0..n).map(|r| {
        let (s, e) = span(r);
        foldhash::hash(&bytes[s..e], LIST)
    }).collect()
}

/// Reuse one encoding buffer; byte-valued rows must agree with U8 storage, even in a mixed batch.
fn leaf_rows<T: Copy>(n: usize, span: impl Fn(usize) -> (usize, usize), xs: &[T], word: impl Fn(T) -> u64) -> Vec<u64> {
    const WORDS: u64 = 0x4528_21e6_38d0_1377;
    let mut bytes = Vec::new();
    (0..n)
        .map(|r| {
            let (s, e) = span(r);
            let row = &xs[s..e];
            let is_bytes = row.iter().all(|&x| word(x) <= 255);
            bytes.clear();
            if is_bytes {
                bytes.extend(row.iter().map(|&x| word(x) as u8));
            } else {
                bytes.extend(row.iter().flat_map(|&x| word(x).to_le_bytes()));
            }
            foldhash::hash(&bytes, if is_bytes { LIST } else { WORDS })
        })
        .collect()
}

/// one list row's hash: length first, then each element in order, over the span `(s, e)` of the
/// element hashes `ch`.
fn hash_span(ch: &[u64], (s, e): (usize, usize)) -> u64 {
    ch[s..e].iter().fold(combine(LIST, (e - s) as u64), |a, &x| combine(a, x))
}

#[cfg(test)]
mod tests;
