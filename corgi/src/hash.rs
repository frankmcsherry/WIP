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

use crate::value::Value;

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

/// the columnar structural hash: a `U64` column whose row `r` is the stable hash of row `r` of `v`.
/// One bottom-up pass; each level folds its children exactly as the comparator orders them.
pub fn hash(v: &Value) -> Value {
    Value::u64(hash_cols(v))
}

/// the raw per-row hashes (the kernel [`hash`] wraps as a `Value`). See the module doc for the fold.
pub(crate) fn hash_cols(v: &Value) -> Vec<u64> {
    match v {
        Value::Prim(p) => p.hashes(),

        // product = fold the fields in order, each seeded from the PROD salt. A fieldless product has
        // no length witness (`len` is 0), so this is empty — consistent with `Value::len`.
        Value::Prod(cols) => {
            let mut acc = vec![PROD; v.len()];
            for c in cols {
                for (a, x) in acc.iter_mut().zip(hash_cols(c)) {
                    *a = combine(*a, x);
                }
            }
            acc
        }

        // sum = tag first, then the payload read from the row's lane at its carried within-variant
        // offset.
        Value::Sum(tags, offset, variants) => {
            let lanes: Vec<Vec<u64>> = variants.iter().map(hash_cols).collect();
            // tags are read in place: this fold looks at each row's tag exactly once, so decoding
            // the column into a wider one first is pure overhead.
            tags.usize_iter().zip(offset).map(|(t, &o)| combine(combine(SUM, t as u64), lanes[t][o])).collect()
        }

        // list = length first, then each element in order, folded by SPAN — so a `Stride` and the
        // equivalent `Offsets` hash identically (matching `Bounds` equality, which is by the
        // partition, not its representation).
        Value::List(bounds, vals) => {
            let ch = hash_cols(vals);
            (0..bounds.len())
                .map(|r| {
                    let (s, e) = bounds.span(r);
                    let mut a = combine(LIST, (e - s) as u64);
                    for &x in &ch[s..e] {
                        a = combine(a, x);
                    }
                    a
                })
                .collect()
        }

        // unit = no payload; every row hashes to the same constant.
        Value::Unit(n) => vec![UNIT; *n],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Bounds;

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }
    fn h(v: &Value) -> Vec<u64> {
        hash_cols(v)
    }

    #[test]
    fn deterministic() {
        let v = Value::Prod(vec![u(&[1, 2, 3]), u(&[9, 8, 7])]);
        assert_eq!(h(&v), h(&v));
    }

    #[test]
    fn equal_rows_hash_equal() {
        // duplicate rows in a column must produce identical ids (the equal-for-equal contract).
        let v = Value::Prod(vec![u(&[5, 5, 6]), u(&[7, 7, 7])]);
        let hs = h(&v);
        assert_eq!(hs[0], hs[1]); // rows (5,7) and (5,7)
        assert_ne!(hs[0], hs[2]); // vs (6,7)
    }

    #[test]
    fn width_invariant_for_unsigned() {
        // the id addresses the value, not its storage width: the same unsigned value at any leaf width
        // hashes identically (the fold widens every leaf to u64). Lets a narrowing/widening be
        // id-preserving and a two-input join match keys carried at different widths. (Signed/float
        // store a width-dependent encoding and are NOT promised this — see the module doc.)
        assert_eq!(h(&Value::u8(vec![5])), h(&Value::u64(vec![5]))); // u8 5 == u64 5
        assert_eq!(h(&Value::u16(vec![5, 300])), h(&Value::u64(vec![5, 300])));
        assert_eq!(h(&Value::u32(vec![5, 300, 70000])), h(&Value::u64(vec![5, 300, 70000])));
    }

    #[test]
    fn stride_and_offsets_agree() {
        // representation-independence: the DEEP invariant for stable ids. A uniform list carried as a
        // `Stride` must hash the same as the equivalent end-offset form (they are `Bounds`-equal).
        let vals = u(&[0, 1, 2, 3, 4, 5]);
        let strided = Value::List(Bounds::Stride(2, 3), Box::new(vals.clone()));
        let offsets = Value::List(Bounds::Offsets(vec![2, 4, 6]), Box::new(vals));
        assert_eq!(h(&strided), h(&offsets));
    }

    #[test]
    fn structure_disambiguates() {
        // length-first folding separates lists of different length/content and different nestings.
        let lists = Value::List(
            Bounds::Offsets(vec![0, 1, 3]),
            Box::new(u(&[9, 9, 9])),
        ); // rows [], [9], [9,9]
        let hs = h(&lists);
        assert_ne!(hs[0], hs[1]);
        assert_ne!(hs[1], hs[2]);
        // an empty list row and a Unit row (both "empty") must not collide.
        assert_ne!(hs[0], h(&Value::Unit(1))[0]);
    }

    #[test]
    fn field_order_matters() {
        // (a,b) and (b,a) are distinct products, so their hashes differ (combine is order-sensitive).
        let ab = Value::Prod(vec![u(&[1]), u(&[2])]);
        let ba = Value::Prod(vec![u(&[2]), u(&[1])]);
        assert_ne!(h(&ab), h(&ba));
    }

    #[test]
    fn sum_tag_and_payload() {
        // same payload value under different tags must differ; same tag+payload must agree.
        let s = Value::sum(vec![0, 1, 0], vec![u(&[5, 5]), u(&[5])]);
        let hs = h(&s);
        assert_ne!(hs[0], hs[1]); // tag 0 val 5  vs  tag 1 val 5
        assert_eq!(hs[0], hs[2]); // both tag 0 val 5
    }

    #[test]
    fn stable_golden() {
        // GOLDEN LOCK: pin exact outputs so the hash can never silently change (that would re-id the
        // whole system). If this fails after an intentional change, update the constants deliberately.
        assert_eq!(
            h(&u(&[0, 1, 2])),
            vec![0, 6238072747940578789, 15839785061582574730],
        );
    }
}
