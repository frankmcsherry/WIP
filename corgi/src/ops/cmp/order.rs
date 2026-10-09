//! Structural comparison — the order machinery `sort`/`dedup`/`group`/`find` reduce to, minus the sort
//! itself, which is `super::sort`. Here: `mod compare` (the bulk structural comparator `compare_idx`,
//! which `Rel` and `find` reduce to), `mod labels` (the block-label vocabulary the sort speaks and
//! `dedup`/`group` read), and `group_bounds`; the merge kernel is `super::survey`.

use crate::value::{Bounds, Value};
use std::cmp::Ordering;

pub(crate) use compare::*;
pub(crate) use labels::*;

/// Scalar structural compare: row `i` of `a` vs row `j` of `b` (same shape). The merge/search
/// scalar form of [`compare_idx`]; exposed (via `crate::arrange`) for using corgi columns as a
/// differential-dataflow arrangement substrate.
pub(crate) fn compare_at(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
    match compare_idx(a, b, &[i], &[j])[0] {
        s if s < 0 => Ordering::Less,
        0 => Ordering::Equal,
        _ => Ordering::Greater,
    }
}

/// Segment ends of the maximal equal-value runs in a structurally-sorted column `keys`: `out[g]` is
/// the exclusive end of group `g`, so group `g` occupies `out[g-1]..out[g]` (with an implicit
/// `out[-1] = 0`) and `out.last() == keys.len()`. One columnar adjacent-compare pass — the
/// single-column analogue of the equal-key boundaries a survey reveals across two runs.
pub fn group_bounds(keys: &Value) -> Vec<usize> {
    let n = keys.len();
    if n == 0 {
        return Vec::new();
    }
    // signs[k] = order of keys[k] vs keys[k+1]; a nonzero sign is a group boundary after k.
    let signs = compare_adjacent(keys);
    let mut ends = Vec::new();
    for (k, &s) in signs.iter().enumerate() {
        if s != 0 {
            ends.push(k + 1);
        }
    }
    ends.push(n);
    ends
}

mod compare {
    //! The bulk structural comparator: a total structural order on rows, recursing through the type —
    //! leaf value, then Prod field-by-field, List LEXICOGRAPHIC (first differing element; a proper prefix first),
    //! Sum tag-then-payload. The discrimination sort matches this order, so `find` stays consistent with `sort`.
    //!
    //! `compare_idx` is the kernel: it compares an explicit list of `(i, j)` index pairs in one descent per
    //! type level, PUSHING the indices down rather than gathering. The Sum arm is the subtle one — comparing
    //! two equal-tag rows needs each row's offset WITHIN its variant, and a `Value::Sum` carries that
    //! offset (built once at construction), so the arm reads it O(1) and recurses; the Sum comparison
    //! stays LINEAR rather than the O(n²) of a per-pair rank scan. `compare2` (below) is the scalar oracle.
    //!
    //! `compare_cols` is the diagonal case (`Rel`'s lane compare); arbitrary pairs give the probe comparator
    //! `find`'s batched binary search wants — and because the carried offset is read, not recomputed, sparse
    //! `find` over a sum-shaped haystack is `O(|needle|·log|haystack|)` with no per-round offset rebuild.

    use super::*;

    /// Bulk structural order over a list of index pairs: `out[k]` = the order of row `ia[k]` of `a` vs
    /// row `ib[k]` of `b`, one descent per type level (see the module doc). Each level folds its
    /// contribution lexicographically (first nonzero sign wins), only the leaf reads — nothing is
    /// materialised. Linear: within-offset cursor passes O(column), leaf reads O(pairs·depth). Diagonal
    /// pairs are `Rel`'s lane compare ([`compare_cols`]); arbitrary pairs are what `find`'s search wants.
    pub fn compare_idx(a: &Value, b: &Value, ia: &[usize], ib: &[usize]) -> Vec<i8> {
        debug_assert_eq!(ia.len(), ib.len());
        compare_pairs(a, b, Pairs::Explicit(ia, ib))
    }

    /// Which row pairs a comparison covers. `Diagonal` and `Adjacent` are the IMPLICIT forms — row
    /// `i` against row `i` (the lane compare) and row `k` against row `k+1` (the run boundaries in a
    /// sorted column) — which the caller would otherwise materialise as index columns describing
    /// `i` and `i+1`. Only how a comparison ENTERS is ever implicit: below the first level the
    /// comparator always holds real indices (a tie set, a lane's offsets, a row's elements), and
    /// descends as `Explicit`.
    #[derive(Clone, Copy)]
    pub(crate) enum Pairs<'a> {
        Explicit(&'a [usize], &'a [usize]),
        Diagonal(usize), // (i, i) for i in 0..n
        Adjacent(usize), // (k, k+1) for k in 0..n — `n` is the PAIR count, one less than the rows
    }

    impl Pairs<'_> {
        pub(crate) fn len(&self) -> usize {
            match self {
                Pairs::Explicit(ia, _) => ia.len(),
                Pairs::Diagonal(n) | Pairs::Adjacent(n) => *n,
            }
        }
        #[inline]
        fn left(&self, k: usize) -> usize {
            match self {
                Pairs::Explicit(ia, _) => ia[k],
                Pairs::Diagonal(_) | Pairs::Adjacent(_) => k,
            }
        }
        #[inline]
        fn right(&self, k: usize) -> usize {
            match self {
                Pairs::Explicit(_, ib) => ib[k],
                Pairs::Diagonal(_) => k,
                Pairs::Adjacent(_) => k + 1,
            }
        }
    }

    /// [`compare_idx`] over any [`Pairs`] — the kernel proper.
    pub(crate) fn compare_pairs(a: &Value, b: &Value, pairs: Pairs) -> Vec<i8> {
        let m = pairs.len();
        match (a, b) {
            // leaf: read all pairs in one width-dispatched pass. An implicit form reads BOTH sides
            // densely (`i` and `i`, or `k` and `k+1`), which vectorizes; the indexed form is two
            // gathers per lane and does not.
            (Value::Prim(pa), Value::Prim(pb)) => match pairs {
                Pairs::Explicit(ia, ib) => pa.cmp_idx(ia, ib, pb),
                Pairs::Diagonal(n) => pa.cmp_dense(pb, n, 0),
                Pairs::Adjacent(n) => pa.cmp_dense(pb, n, 1),
            },

            // single-field product: the field's order IS the order — skip the fold + tie vec.
            (Value::Prod(ca), Value::Prod(cb)) if ca.len() == 1 && cb.len() == 1 => {
                compare_pairs(&ca[0], &cb[0], pairs)
            }

            // product = lexicographic: field 0 over all pairs, then each later field over the
            // SURVIVING TIES only — when an early field discriminates most pairs (the common
            // case), later fields cost proportionally to the ties, not to m.
            (Value::Prod(ca), Value::Prod(cb)) => {
                assert_eq!(ca.len(), cb.len(), "compare_idx: product arity");
                let mut ord = compare_pairs(&ca[0], &cb[0], pairs);
                if ca.len() > 1 {
                    let mut tie_k: Vec<usize> = (0..m).filter(|&k| ord[k] == 0).collect();
                    let mut tia: Vec<usize> = tie_k.iter().map(|&k| pairs.left(k)).collect();
                    let mut tib: Vec<usize> = tie_k.iter().map(|&k| pairs.right(k)).collect();
                    for (x, y) in ca[1..].iter().zip(&cb[1..]) {
                        if tie_k.is_empty() {
                            break;
                        }
                        let sub = compare_pairs(x, y, Pairs::Explicit(&tia, &tib));
                        let mut w = 0usize;
                        for t in 0..tie_k.len() {
                            let k = tie_k[t];
                            if sub[t] != 0 {
                                ord[k] = sub[t];
                            } else {
                                tie_k[w] = k;
                                tia[w] = tia[t];
                                tib[w] = tib[t];
                                w += 1;
                            }
                        }
                        tie_k.truncate(w);
                        tia.truncate(w);
                        tib.truncate(w);
                    }
                }
                ord
            }

            // sum = tag order first; equal-tag pairs recurse into the lane at their within-variant
            // offsets (`oa`/`ob`, carried by the value). No gather: the remapped indices descend as
            // the next level's pairs.
            (Value::Sum(ta, va), Value::Sum(tb, vb)) => {
                assert_eq!(va.len(), vb.len(), "compare_idx: sum arity");
                // Both sides one lane, the same one: the tag decides nothing and the offsets are
                // the identity, so the comparison IS the lane's, at the pairs we were handed.
                if let (Some(t), Some(u)) = (ta.const_tag(), tb.const_tag()) {
                    if t == u {
                        return compare_pairs(&va[t], &vb[t], pairs);
                    }
                }
                // Read the discriminants in place. Decoding a whole tag column per call made a
                // scalar `compare_at` O(column): a chunk merge over sum-shaped keys spent 40% of
                // its time re-decoding tags it looked at one row of.
                let mut ord: Vec<i8> = (0..m)
                    .map(|k| ta.tag_at(pairs.left(k)).cmp(&tb.tag_at(pairs.right(k))) as i8)
                    .collect();
                let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); va.len()];
                for k in 0..m {
                    let t = ta.tag_at(pairs.left(k));
                    if t == tb.tag_at(pairs.right(k)) { by_tag[t].push(k); }
                }
                for (t, ks) in by_tag.iter().enumerate() {
                    if ks.is_empty() { continue; }
                    // the carried within-lane offsets — read, not recomputed.
                    let sia: Vec<usize> = ks.iter().map(|&k| ta.offset_at(pairs.left(k))).collect();
                    let sib: Vec<usize> = ks.iter().map(|&k| tb.offset_at(pairs.right(k))).collect();
                    let sub = compare_pairs(&va[t], &vb[t], Pairs::Explicit(&sia, &sib));
                    // tag was Equal on these pairs, so the payload order IS the order.
                    for (&k, o) in ks.iter().zip(sub) { ord[k] = o; }
                }
                ord
            }

            // list = lexicographic: each pair expands to its element index pairs up to the shorter
            // length, recurse ONCE (no per-position loop — `sort` needs that refinement, `cmp`
            // doesn't), then read each pair's first difference off its segment; a pair with none
            // is decided by length, a proper prefix first.
            // A referenced list compares as the rows it names, read through its arena.
            (Value::List(..) | Value::Ref(..), Value::List(..) | Value::Ref(..)) => {
                let (ba, va) = a.rows_of("compare_idx").expect("a list");
                let (bb, vb) = b.rows_of("compare_idx").expect("a list");
                let mut ord = vec![0i8; m];
                let (mut sia, mut sib) = (Vec::new(), Vec::new());
                let mut seg: Vec<(usize, usize, usize)> = Vec::new(); // (pair k, start in batch, len)
                for (k, o) in ord.iter_mut().enumerate() {
                    let (i, j) = (pairs.left(k), pairs.right(k));
                    let ((s_a, e_a), (s_b, e_b)) = (ba.span(i), bb.span(j));
                    let (la, lb) = (e_a - s_a, e_b - s_b);
                    *o = la.cmp(&lb) as i8;
                    let common = la.min(lb);
                    if common > 0 {
                        seg.push((k, sia.len(), common));
                        for p in 0..common { sia.push(s_a + p); sib.push(s_b + p); }
                    }
                }
                let cmp = compare_pairs(va, vb, Pairs::Explicit(&sia, &sib));
                for (k, start, len) in seg {
                    if let Some(o) = cmp[start..start + len].iter().copied().find(|&o| o != 0) {
                        ord[k] = o;
                    }
                }
                ord
            }

            // Unit rows carry no payload — always equal. (Added for `crate::arrange`: a unit-valued
            // column, e.g. `distinct`'s output, must be a sortable arrangement payload.)
            (Value::Unit(_), Value::Unit(_)) => vec![0i8; m],

            _ => panic!("compare_idx: shape mismatch"),
        }
    }

    /// the diagonal case: `out[i]` = the order of row `i` of `a` vs row `i` of `b` — `Rel`'s lane compare.
    pub fn compare_cols(a: &Value, b: &Value) -> Vec<i8> {
        compare_pairs(a, b, Pairs::Diagonal(a.len()))
    }

    /// the adjacent case: `out[k]` = the order of row `k` of `v` vs row `k+1` — the run boundaries
    /// of a sorted column ([`super::group_bounds`]), and the shape a `windows(2)` scan has.
    pub fn compare_adjacent(v: &Value) -> Vec<i8> {
        compare_pairs(v, v, Pairs::Adjacent(v.len().saturating_sub(1)))
    }
}

mod labels {
    //! The label vocabulary the discrimination sort (`super::super::sort`) speaks: a non-decreasing
    //! `labels` vector partitions positions into blocks — runs of one label — and a sort returns a
    //! refinement of it, two positions sharing a label iff they did before AND their rows are equal.
    //! Nothing here sorts; these seed the labels for a per-row sort and read the runs back out.

    use super::*;

    /// per-element labels seeding a SEGMENTED sort: each element of outer row `r` gets label `r`, so
    /// the sort orders within each row and rows stay contiguous and in order.
    pub fn segment_labels(bounds: &Bounds) -> Vec<u64> {
        let mut labels = Vec::with_capacity(bounds.total());
        let mut start = 0;
        for (r, end) in bounds.ends().enumerate() {
            for _ in start..end {
                labels.push(r as u64);
            }
            start = end;
        }
        labels
    }
}

#[cfg(test)]
mod tests;
