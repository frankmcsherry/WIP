//! Structural, content-addressed hashing: `hash(v) -> U64 column`, one stable u64 per row.
//!
//! The hash-analogue of the structural comparator ([`crate::ops::cmp`]'s `compare_cols`): it folds
//! the SAME type structure — leaf bytes, `Prod` field-by-field, `List` length-then-elements, `Sum`
//! tag-then-payload — so it is CONSISTENT WITH corgi's structural equality. Rows that compare equal
//! (hence collapse under `sort`/`dedup`/`group`) hash identically; the converse holds only up to the
//! birthday bound (collisions are accepted, see below).
//!
//! Three properties the boundary relies on:
//!   * KIND-BLIND / structural — any shape (`Prim`/`Prod`/`Sum`/`List`/`Unit`), reading stored leaf
//!     bytes, so signed/float encodings hash by their stored form just as equality compares them.
//!   * COLUMNAR — one bottom-up pass, no per-row `Value`; a `Sum` reads its carried within-variant
//!     offset O(1) per row (no rank rescan), exactly like `compare_idx`.
//!   * STABLE — pure integer math with fixed constants, no seed, no address or batch dependence — so
//!     the same value yields the same u64 on every machine and every run.
//!
//! REPRESENTATION-BLIND — the id addresses the VALUE, not its layout. A `List`'s `Stride` and the
//! equivalent `Offsets` hash the same (by partition), and a leaf's WIDTH drops out for the raw/unsigned
//! reading: `u8` 5 and `u64` 5 collapse to one id (the fold widens every leaf to u64), so a
//! narrowing/widening for storage is id-preserving and a join can match equal keys carried at
//! different widths. CAVEAT for signed/float: those store a WIDTH-DEPENDENT order-preserving encoding
//! (`i8` -1 = `0x7F`, `i64` -1 = `0x7FFF…FF` — not zero-extensions), so cross-width identity is NOT
//! promised for them (kind-blindness — ignoring the U/I/F label — still holds; it is width-invariance
//! that does not). Downstream constraint on the DD side: keep signed/float leaf widths consistent
//! across a join's two inputs and across the output→input boundary, or equal keys can hash unequal.
//!
//! This is the boundary id function: present a record as `((hash(key), hash(value)), time, diff)` and
//! the ids stay equal-for-equal-values across operators and runs (a reduce OUTPUT hashes to the same
//! id when it reappears downstream as an INPUT). 64-bit, collisions ~ n²/2⁶⁵ accepted; a 128-bit
//! widening is a later change to the accumulator type here, never a registry.
//!
//! NB: the fold reads only lanes some row's tag NAMES, so two `Sum`s that differ only in an
//! unreferenced (empty) lane's shape hash equal — every row's OBSERVABLE value is identical, so sharing an
//! id is correct (and more stable than derived `PartialEq`, which would call them distinct).

use crate::value::{Bounds, Prim, Value};
use rapidhash::v3::{rapidhash_v3_seeded, RapidSecrets};

/// splitmix64 finalizer — a full-avalanche 64-bit mix. The one bit-mixing primitive; both the leaf
/// hashing ([`Prim::hashes`]) and the structural [`combine`] build on it.
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
    (acc ^ mix64(x)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

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

        // a list of leaves (text, say): each row's elements as one string of bytes (`leaf_rows`)
        Value::List(bounds, vals) if matches!(**vals, Value::Prim(_)) => {
            let Value::Prim(p) = &**vals else { unreachable!() };
            leaf_rows(bounds, p)
        }

        // any other list = length first, then each element in order, folded by SPAN — so a `Stride`
        // and the equivalent `Offsets` hash identically (matching `Bounds` equality, which is by
        // the partition, not its representation).
        Value::List(bounds, vals) => {
            let ch = hash(vals);
            (0..bounds.len()).map(|r| hash_span(&ch, bounds.span(r))).collect()
        }

        // unit = no payload; every row hashes to the same constant.
        Value::Unit(n) => vec![UNIT; *n],

        // a referenced row hashes as the list row it names (a reference has the identity of what it
        // names). The whole arena is hashed, even where the references name little of it.
        Value::Ref(list, rows) => {
            let rh = hash(list);
            rows.iter().map(|&r| rh[r]).collect()
        }
    }
}

// the seeds of a list of leaves' three byte strings, so that rows of different kinds of string
// can't share one: integers that each fit in a byte, other integers as 64-bit words, floats' keys.
const BYTES: RapidSecrets = RapidSecrets::seed(LIST);
const WORDS: RapidSecrets = RapidSecrets::seed(LIST ^ 1);
const FLOATS: RapidSecrets = RapidSecrets::seed(LIST ^ 2);

/// a list of leaves, a row at a time: the row's elements as one string of bytes, through rapidhash
/// (eight and more bytes per step, where folding an element at a time is one multiply per byte).
/// A row of integers that each fit in a byte hashes as those bytes however it is stored, so storage
/// stays invisible; any other integer row hashes its 64-bit words (little-endian), and a float row
/// its keys, each under its own seed. Rows by span, so a `Stride` and its `Offsets` agree.
fn leaf_rows(bounds: &Bounds, p: &Prim) -> Vec<u64> {
    let mut bytes: Vec<u8> = Vec::new();
    match p {
        Prim::U8(b) => by_row(bounds, |s, e| rapidhash_v3_seeded(&b[s..e], &BYTES)),
        Prim::I64(w) => by_row(bounds, |s, e| {
            let row = &w[s..e];
            bytes.clear();
            if row.iter().all(|&x| (0..256).contains(&x)) {
                bytes.extend(row.iter().map(|&x| x as u8));
                rapidhash_v3_seeded(&bytes, &BYTES)
            } else {
                bytes.extend(row.iter().flat_map(|x| x.to_le_bytes()));
                rapidhash_v3_seeded(&bytes, &WORDS)
            }
        }),
        Prim::F64(k) => by_row(bounds, |s, e| {
            bytes.clear();
            bytes.extend(k[s..e].iter().flat_map(|x| x.to_le_bytes()));
            rapidhash_v3_seeded(&bytes, &FLOATS)
        }),
    }
}

/// `f` of each row's span.
fn by_row(bounds: &Bounds, mut f: impl FnMut(usize, usize) -> u64) -> Vec<u64> {
    let mut start = 0;
    bounds.ends().map(|end| { let h = f(start, end); start = end; h }).collect()
}

/// one list row's hash: length first, then each element in order, over the span `(s, e)` of the
/// element hashes `ch`.
fn hash_span(ch: &[u64], (s, e): (usize, usize)) -> u64 {
    ch[s..e].iter().fold(combine(LIST, (e - s) as u64), |a, &x| combine(a, x))
}

#[cfg(test)]
mod tests;
