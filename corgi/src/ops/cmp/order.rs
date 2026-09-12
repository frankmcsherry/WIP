//! Structural comparison — the order machinery `sort`/`dedup`/`group`/`find` reduce to, minus the sort
//! itself, which is `super::sort`. Here: `mod compare` (the bulk structural comparator `compare_idx`,
//! which `Rel` and `find` reduce to), `mod equal` (the bulk structural equality `equal_idx`, the
//! sign-free reading the sort's uniform-class check and `group_bounds` want), `mod labels` (the
//! block-label vocabulary the sort speaks and `dedup`/`group` read), and `group_bounds`; the merge
//! kernel is `super::survey`.

use crate::engine::row_span;
use crate::value::{Bounds, Value};
use std::cmp::Ordering;

pub(crate) use compare::*;
pub(crate) use equal::*;
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
/// `out[-1] = 0`) and `out.last() == keys.len()`. One columnar adjacent-equality pass — the
/// single-column analogue of the equal-key boundaries a survey reveals across two runs, and the
/// `Value`-column counterpart of [`run_layout`]'s `ends` (which reads a precomputed labels vector).
/// A boundary needs no sign, so this is the equality kernel, which compares a list row as a span.
pub fn group_bounds(keys: &Value) -> Vec<usize> {
    let n = keys.len();
    if n == 0 {
        return Vec::new();
    }
    // eq[k] iff keys[k] == keys[k+1]; an inequality is a group boundary after k.
    let eq = equal_adjacent(keys);
    let mut ends = Vec::new();
    for (k, &e) in eq.iter().enumerate() {
        if !e {
            ends.push(k + 1);
        }
    }
    ends.push(n);
    ends
}

mod compare {
    //! The bulk structural comparator: a total structural order on rows, recursing through the type —
    //! leaf value, then Prod field-by-field, List LENGTH-FIRST (shorter first; equal lengths element-wise),
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
        pub(super) fn left(&self, k: usize) -> usize {
            match self {
                Pairs::Explicit(ia, _) => ia[k],
                Pairs::Diagonal(_) | Pairs::Adjacent(_) => k,
            }
        }
        #[inline]
        pub(super) fn right(&self, k: usize) -> usize {
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

            // list = length-first: unequal-length pairs decided by length. Equal-length pairs expand
            // to their element index pairs, recurse ONCE (no per-position loop — `sort` needs that
            // refinement, `cmp` doesn't), then read each pair's first difference off its segment.
            (Value::List(ba, va), Value::List(bb, vb)) => {
                let mut ord = vec![0i8; m];
                let (mut sia, mut sib) = (Vec::new(), Vec::new());
                let mut seg: Vec<(usize, usize, usize)> = Vec::new(); // (pair k, start in batch, len)
                for (k, o) in ord.iter_mut().enumerate() {
                    let (i, j) = (pairs.left(k), pairs.right(k));
                    let ((s_a, e_a), (s_b, e_b)) = (row_span(ba, i), row_span(bb, j));
                    let (la, lb) = (e_a - s_a, e_b - s_b);
                    match la.cmp(&lb) {
                        Ordering::Equal if la > 0 => {
                            seg.push((k, sia.len(), la));
                            for p in 0..la { sia.push(s_a + p); sib.push(s_b + p); }
                        }
                        Ordering::Equal => {}    // equal length 0 — stays Equal (0)
                        ow => *o = ow as i8,     // length decides
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

mod equal {
    //! The bulk structural equality: the sign-free reading of [`compare::compare_pairs`], for the
    //! two places that ask only whether rows are equal — the sort's check that a tied class is
    //! uniform, and the run boundaries of a sorted column. Same descent, cheaper levels: a leaf is
    //! one lane compare, a product narrows to the pairs still equal, a sum matches tags then lanes
    //! at the carried offsets, and a list of leaf elements is decided by ONE span comparison per
    //! pair rather than an element position at a time, so equal long strings cost their bytes and
    //! unequal ones their common prefix.

    use super::*;

    /// Bulk structural equality over index pairs: `out[k]` iff row `ia[k]` of `a` equals row
    /// `ib[k]` of `b`, one descent per type level.
    pub fn equal_idx(a: &Value, b: &Value, ia: &[usize], ib: &[usize]) -> Vec<bool> {
        debug_assert_eq!(ia.len(), ib.len());
        equal_pairs(a, b, Pairs::Explicit(ia, ib))
    }

    /// The adjacent case: `out[k]` iff row `k` of `v` equals row `k+1` — the run structure of a
    /// sorted column, as [`super::group_bounds`] reads it.
    pub fn equal_adjacent(v: &Value) -> Vec<bool> {
        equal_pairs(v, v, Pairs::Adjacent(v.len().saturating_sub(1)))
    }

    /// [`equal_idx`] over any [`Pairs`] — the kernel proper.
    pub(crate) fn equal_pairs(a: &Value, b: &Value, pairs: Pairs) -> Vec<bool> {
        let m = pairs.len();
        match (a, b) {
            (Value::Prim(pa), Value::Prim(pb)) => match pairs {
                Pairs::Explicit(ia, ib) => pa.eq_idx(ia, ib, pb),
                Pairs::Diagonal(n) => pa.eq_dense(pb, n, 0),
                Pairs::Adjacent(n) => pa.eq_dense(pb, n, 1),
            },

            // product: field 0 over all pairs, each later field over the pairs still equal.
            (Value::Prod(ca), Value::Prod(cb)) => {
                assert_eq!(ca.len(), cb.len(), "equal_idx: product arity");
                if ca.is_empty() {
                    return vec![true; m];
                }
                let mut eq = equal_pairs(&ca[0], &cb[0], pairs);
                if ca.len() > 1 {
                    let mut live: Vec<usize> = (0..m).filter(|&k| eq[k]).collect();
                    let mut lia: Vec<usize> = live.iter().map(|&k| pairs.left(k)).collect();
                    let mut lib: Vec<usize> = live.iter().map(|&k| pairs.right(k)).collect();
                    for (x, y) in ca[1..].iter().zip(&cb[1..]) {
                        if live.is_empty() {
                            break;
                        }
                        let sub = equal_pairs(x, y, Pairs::Explicit(&lia, &lib));
                        let mut w = 0usize;
                        for t in 0..live.len() {
                            if sub[t] {
                                live[w] = live[t];
                                lia[w] = lia[t];
                                lib[w] = lib[t];
                                w += 1;
                            } else {
                                eq[live[t]] = false;
                            }
                        }
                        live.truncate(w);
                        lia.truncate(w);
                        lib.truncate(w);
                    }
                }
                eq
            }

            // sum: equal tags, then the lane at the carried within-variant offsets.
            (Value::Sum(ta, va), Value::Sum(tb, vb)) => {
                assert_eq!(va.len(), vb.len(), "equal_idx: sum arity");
                if let (Some(t), Some(u)) = (ta.const_tag(), tb.const_tag()) {
                    return if t == u { equal_pairs(&va[t], &vb[t], pairs) } else { vec![false; m] };
                }
                let mut eq = vec![false; m];
                let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); va.len()];
                for k in 0..m {
                    let t = ta.tag_at(pairs.left(k));
                    if t == tb.tag_at(pairs.right(k)) {
                        by_tag[t].push(k);
                    }
                }
                for (t, ks) in by_tag.iter().enumerate() {
                    if ks.is_empty() {
                        continue;
                    }
                    let sia: Vec<usize> = ks.iter().map(|&k| ta.offset_at(pairs.left(k))).collect();
                    let sib: Vec<usize> = ks.iter().map(|&k| tb.offset_at(pairs.right(k))).collect();
                    let sub = equal_pairs(&va[t], &vb[t], Pairs::Explicit(&sia, &sib));
                    for (&k, e) in ks.iter().zip(sub) {
                        eq[k] = e;
                    }
                }
                eq
            }

            // list: equal lengths, then the elements — leaf elements as one span comparison per
            // pair, anything else as element pairs recursed once and folded per row.
            (Value::List(ba, va), Value::List(bb, vb)) => {
                let mut eq = vec![false; m];
                let leaves = match (&**va, &**vb) {
                    (Value::Prim(pa), Value::Prim(pb)) => Some((pa, pb)),
                    _ => None,
                };
                let (mut sia, mut sib) = (Vec::new(), Vec::new());
                let mut seg: Vec<(usize, usize, usize)> = Vec::new(); // (pair k, start in batch, len)
                for (k, e) in eq.iter_mut().enumerate() {
                    let (i, j) = (pairs.left(k), pairs.right(k));
                    let ((s_a, e_a), (s_b, e_b)) = (row_span(ba, i), row_span(bb, j));
                    let len = e_a - s_a;
                    if len != e_b - s_b {
                        continue;
                    }
                    if len == 0 {
                        *e = true;
                    } else if let Some((pa, pb)) = leaves {
                        *e = pa.eq_spans((s_a, e_a), pb, (s_b, e_b));
                    } else {
                        seg.push((k, sia.len(), len));
                        for p in 0..len {
                            sia.push(s_a + p);
                            sib.push(s_b + p);
                        }
                    }
                }
                if !seg.is_empty() {
                    let sub = equal_pairs(va, vb, Pairs::Explicit(&sia, &sib));
                    for (k, start, len) in seg {
                        eq[k] = sub[start..start + len].iter().all(|&x| x);
                    }
                }
                eq
            }

            (Value::Unit(_), Value::Unit(_)) => vec![true; m],

            _ => panic!("equal_idx: shape mismatch"),
        }
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

    /// the run structure of non-decreasing `labels` (e.g. a sort's refined labels): `ends[i]` is the
    /// exclusive end of run `i`, `firsts[i]` its first index. Runs are maximal equal-label spans — equal
    /// value within a block. `group` reads `ends` as inner bounds and the representatives at `firsts`;
    /// `dedup` keeps `firsts`; `uniq -c` reads the run lengths.
    pub fn run_layout(labels: &[u64]) -> (Vec<usize>, Vec<usize>) {
        let n = labels.len();
        let mut ends = Vec::new();
        let mut firsts = Vec::new();
        if n == 0 {
            return (ends, firsts);
        }
        firsts.push(0);
        for k in 1..n {
            if labels[k] != labels[k - 1] {
                ends.push(k);
                firsts.push(k);
            }
        }
        ends.push(n);
        (ends, firsts)
    }

    /// project run starts onto outer rows: `out[r]` is the count of run firsts strictly before
    /// `bounds[r]`, cumulative (both are ascending). This is the new outer-bounds `dedup`/`group`
    /// emit — a run never crosses a row, so each falls under exactly one outer row.
    pub fn runs_per_row(bounds: &Bounds, firsts: &[usize]) -> Vec<usize> {
        let mut out = Vec::with_capacity(bounds.len());
        let mut g = 0;
        for end in bounds.ends() {
            while g < firsts.len() && firsts[g] < end { g += 1; }
            out.push(g);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::gather;
    use crate::ops::cmp::sort::sort_blocks;

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }

    /// single-block sort of `v`'s rows → the permutation.
    fn sort_perm(v: &Value) -> Vec<usize> {
        sort_blocks(&vec![0u64; v.len()], v).0
    }

    /// the obviously-correct scalar reference: structural order of row `i` of `a` vs row `j` of `b`,
    /// recursing through the type (leaf, Prod field-by-field, List length-first, Sum tag-then-payload).
    /// The Sum arm recovers each row's within-variant offset by a prefix scan — O(i), so this is the
    /// O(n²) standard the bulk `compare_idx` is checked against, and the order `sort` must materialise.
    fn compare2(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
        match (a, b) {
            // i8 sign back to the oracle's `Ordering` (the one i8→Ordering boundary, test-only).
            (Value::Prim(pa), Value::Prim(pb)) => pa.cmp_idx(&[i], &[j], pb)[0].cmp(&0),
            (Value::Prod(ca), Value::Prod(cb)) => {
                for (x, y) in ca.iter().zip(cb) {
                    match compare2(x, i, y, j) {
                        Ordering::Equal => continue,
                        o => return o,
                    }
                }
                Ordering::Equal
            }
            (Value::List(ab, av), Value::List(bb, bv)) => {
                let (si, ei) = row_span(ab, i);
                let (sj, ej) = row_span(bb, j);
                let (li, lj) = (ei - si, ej - sj);
                // length-first: shorter list sorts first; equal lengths compare element-wise.
                match li.cmp(&lj) {
                    Ordering::Equal => {
                        for k in 0..li {
                            match compare2(av, si + k, bv, sj + k) {
                                Ordering::Equal => continue,
                                o => return o,
                            }
                        }
                        Ordering::Equal
                    }
                    o => o,
                }
            }
            (Value::Sum(ta, va), Value::Sum(tb, vb)) => {
                let (tav, tbv): (Vec<usize>, Vec<usize>) =
                    (ta.tags_iter().collect(), tb.tags_iter().collect());
                let (ti, tj) = (tav[i], tbv[j]);
                match ti.cmp(&tj) {
                    Ordering::Equal => {
                        let wi = tav[..i].iter().filter(|&&t| t == ti).count();
                        let wj = tbv[..j].iter().filter(|&&t| t == ti).count();
                        compare2(&va[ti], wi, &vb[ti], wj)
                    }
                    o => o,
                }
            }
            _ => panic!("compare2: shape mismatch"),
        }
    }

    /// `compare_cols` must match the scalar `compare2` lane for lane — same contract, bulk path.
    fn agree_cmp(a: &Value, b: &Value) {
        let got = compare_cols(a, b);
        let want: Vec<i8> = (0..a.len()).map(|i| compare2(a, i, b, i) as i8).collect();
        assert_eq!(got, want);
    }

    /// The implicit pair forms must answer exactly what the same pairs written out do — they are a
    /// cheaper way to SAY the pairs, not a different comparison. Checked over every shape the
    /// comparator recurses through, since only the entry is implicit and each arm has to carry it.
    #[test]
    fn implicit_pairs_match_explicit_ones() {
        let shapes = [
            u(&[5, 3, 3, 8, 1]),
            Value::Prod(vec![u(&[1, 1, 1, 2, 2]), u(&[7, 7, 9, 0, 0])]),
            Value::sum(vec![0, 0, 1, 1, 0], vec![u(&[4, 4, 6]), u(&[2, 2])]),
            Value::List(vec![1, 3, 3, 6, 6].into(), Box::new(u(&[9, 1, 1, 5, 5, 5]))),
            Value::Unit(5),
        ];
        for v in shapes {
            let n = v.len();
            let id: Vec<usize> = (0..n).collect();
            assert_eq!(compare_cols(&v, &v), compare_idx(&v, &v, &id, &id), "diagonal");
            assert_eq!(
                compare_adjacent(&v),
                compare_idx(&v, &v, &id[..n - 1], &id[1..]),
                "adjacent"
            );
        }
        // an empty column has no pairs either way (the adjacent count must not underflow).
        assert!(compare_adjacent(&u(&[])).is_empty());
        assert!(compare_adjacent(&u(&[7])).is_empty());
    }

    /// Equality is the sign-free reading of the comparison: agree with `compare_idx == 0` on every
    /// pair form and every constructor, including the span path of a leaf-element list and the
    /// element path of a structured-element list.
    #[test]
    fn equality_is_the_zero_of_compare() {
        let shapes = [
            u(&[5, 3, 3, 8, 1, 3]),
            Value::Prod(vec![u(&[1, 1, 1, 2, 2, 1]), u(&[7, 7, 9, 0, 0, 7])]),
            Value::sum(vec![0, 0, 1, 1, 0, 1], vec![u(&[4, 4, 6]), u(&[2, 2, 2])]),
            Value::List(vec![1, 3, 3, 6, 6, 8].into(), Box::new(u(&[9, 1, 1, 5, 5, 5, 1, 1]))),
            Value::List(vec![2, 4, 4, 6].into(), Box::new(Value::sum(vec![0, 1, 0, 1, 0, 1], vec![u(&[5, 5, 5]), u(&[2, 2, 2])]))),
            Value::List(Bounds::Stride(2, 5), Box::new(Value::u8(vec![1, 2, 1, 2, 3, 4, 1, 2, 3, 3]))),
            Value::Unit(5),
        ];
        for v in shapes {
            let n = v.len();
            let id: Vec<usize> = (0..n).collect();
            let all: Vec<bool> = compare_cols(&v, &v).iter().map(|&o| o == 0).collect();
            assert_eq!(equal_pairs(&v, &v, Pairs::Diagonal(n)), all, "diagonal");
            if n > 0 {
                let adj: Vec<bool> = compare_adjacent(&v).iter().map(|&o| o == 0).collect();
                assert_eq!(equal_adjacent(&v), adj, "adjacent");
                // every ordered pair, both ways round.
                let (mut ia, mut ib) = (Vec::new(), Vec::new());
                for i in 0..n {
                    for j in 0..n {
                        ia.push(i);
                        ib.push(j);
                    }
                }
                let want: Vec<bool> = compare_idx(&v, &v, &ia, &ib).iter().map(|&o| o == 0).collect();
                assert_eq!(equal_idx(&v, &v, &ia, &ib), want, "all pairs\n{}", crate::value::show(&v));
                let _ = &id;
            }
        }
        assert!(equal_adjacent(&u(&[])).is_empty());
        // two columns with different tag assignments and a constant-tag side.
        let a = Value::sum(vec![0, 1, 0, 1], vec![u(&[5, 5]), u(&[2, 9])]);
        let b = Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 7]), u(&[2, 9])]);
        let c = Value::sum(vec![0, 0, 0, 0], vec![u(&[5, 7, 5, 5]), u(&[])]);
        for (x, y) in [(&a, &b), (&a, &c), (&c, &a), (&c, &c)] {
            let want: Vec<bool> = compare_cols(x, y).iter().map(|&o| o == 0).collect();
            assert_eq!(equal_pairs(x, y, Pairs::Diagonal(4)), want);
        }
    }

    #[test]
    fn group_bounds_reads_lists_by_span() {
        // sorted list rows with equal neighbours of every length, ragged and strided:
        // [], [], [7], [7], [9], [1,2], [1,2], [1,3].
        let v = Value::List(vec![0, 0, 1, 2, 3, 5, 7, 9].into(), Box::new(u(&[7, 7, 9, 1, 2, 1, 2, 1, 3])));
        assert_eq!(group_bounds(&v), vec![2, 4, 5, 7, 8]);
        let s = Value::List(Bounds::Stride(3, 4), Box::new(Value::u8(vec![1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 2])));
        assert_eq!(group_bounds(&s), vec![2, 4]);
    }

    #[test]
    fn compare_cols_matches_scalar() {
        // prim
        agree_cmp(&u(&[5, 3, 8, 1, 9]), &u(&[5, 4, 2, 1, 0]));
        // product: lexicographic fold over fields
        agree_cmp(
            &Value::Prod(vec![u(&[2, 1, 2, 1]), u(&[10, 20, 5, 30])]),
            &Value::Prod(vec![u(&[2, 1, 1, 1]), u(&[10, 25, 5, 30])]),
        );
        // sum: equal-tag lanes hit the payload compare, unequal-tag lanes the tag order.
        agree_cmp(
            &Value::sum(vec![0, 1, 0, 1, 0], vec![u(&[5, 7, 9]), u(&[2, 4])]),
            &Value::sum(vec![0, 1, 1, 1, 0], vec![u(&[5, 8]), u(&[2, 3, 1])]),
        );
        // list: length-first, then position-wise first difference over ragged rows
        agree_cmp(
            &Value::List(vec![2, 2, 5, 6].into(), Box::new(u(&[3, 1, 4, 5, 9, 0]))),
            &Value::List(vec![2, 3, 6, 7].into(), Box::new(u(&[3, 2, 7, 4, 5, 1, 0]))),
        );
        // nested: a sum in secondary product position (the within-offset remap under a fold)
        agree_cmp(
            &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 1, 0], vec![u(&[5, 8]), u(&[3])])]),
            &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 0, 1], vec![u(&[5, 9]), u(&[3])])]),
        );
        // nested: a sum AS the list element — the position loop gathers sum rows and remaps offsets.
        agree_cmp(
            &Value::List(vec![2, 4].into(), Box::new(Value::sum(vec![0, 1, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]))),
            &Value::List(vec![2, 4].into(), Box::new(Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 7]), u(&[2, 9])]))),
        );
    }

    #[test]
    fn compare_idx_cross_pairs() {
        // arbitrary (i,j) pairs — the find/probe path, with a sum (cross within-offsets) and a list.
        let a = Value::sum(vec![0, 1, 0, 1, 0], vec![u(&[5, 7, 9]), u(&[2, 4])]);
        let b = Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 8]), u(&[2, 9])]);
        let (ia, ib) = (&[0usize, 2, 4, 1, 3], &[3usize, 1, 0, 2, 0]);
        let got = compare_idx(&a, &b, ia, ib);
        let want: Vec<i8> = ia.iter().zip(ib).map(|(&i, &j)| compare2(&a, i, &b, j) as i8).collect();
        assert_eq!(got, want);

        let la = Value::List(vec![2, 2, 5, 6].into(), Box::new(u(&[3, 1, 4, 5, 9, 0])));
        let lb = Value::List(vec![2, 3, 6, 7].into(), Box::new(u(&[3, 2, 7, 4, 5, 1, 0])));
        let (ja, jb) = (&[3usize, 0, 2, 1], &[3usize, 0, 2, 1]);
        let got = compare_idx(&la, &lb, ja, jb);
        let want: Vec<i8> = ja.iter().zip(jb).map(|(&i, &j)| compare2(&la, i, &lb, j) as i8).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn compare_cols_sum_at_scale() {
        // many tagged rows; the bulk path must match the (here O(n²)) scalar reference.
        let n = 300usize;
        let mk = |tags: Vec<usize>| -> Value {
            let vars: Vec<Value> = (0..3)
                .map(|t| {
                    let c = tags.iter().filter(|&&x| x == t).count() as u64;
                    u(&(0..c).map(|k| (k.wrapping_mul(2654435761) >> 5) % 50).collect::<Vec<_>>())
                })
                .collect();
            Value::sum(tags, vars)
        };
        let ta: Vec<usize> = (0..n).map(|i| i % 3).collect();
        let tb: Vec<usize> = (0..n).map(|i| (i % 2) * 2).collect(); // tags 0 or 2
        agree_cmp(&mk(ta), &mk(tb));
    }

    /// reference order: comparison sort by `compare2`, then materialize.
    fn reference(v: &Value) -> Value {
        let mut idx: Vec<usize> = (0..v.len()).collect();
        idx.sort_by(|&a, &b| compare2(v, a, v, b));
        gather(v, &idx)
    }
    /// discrimination must agree with the reference on the sorted VALUES (equal rows may permute differently,
    /// but materialise identically).
    fn agree(v: &Value) {
        assert_eq!(gather(v, &sort_perm(v)), reference(v));
    }

    #[test]
    fn leaf() {
        agree(&u(&[5, 3, 8, 1, 3, 9, 2, 3]));
    }

    #[test]
    fn narrow_widths() {
        // the new u8/u16/u32 leaves sort/gather/compare through the same width-generic kernel; each must agree
        // with the compare2 reference, alone and inside a product.
        agree(&Value::u8(vec![5, 3, 8, 1, 3, 9, 2]));
        agree(&Value::u16(vec![500, 30, 800, 1, 30, 30]));
        agree(&Value::u32(vec![70000, 3, 70000, 3, 2]));
        agree(&Value::Prod(vec![Value::u8(vec![2, 1, 2, 1]), Value::u32(vec![10, 20, 5, 30])]));
    }

    #[test]
    fn product_lex() {
        agree(&Value::Prod(vec![u(&[2, 1, 2, 1, 3, 1]), u(&[10, 20, 5, 30, 7, 20])]));
    }

    #[test]
    fn sum_by_tag_then_payload() {
        // the quadratic case: rows t0=5, t1=1, t0=3, t1=4, t0=9, t1=1
        agree(&Value::sum(vec![0, 1, 0, 1, 0, 1], vec![u(&[5, 3, 9]), u(&[1, 4, 1])]));
    }

    #[test]
    fn list_length_first() {
        // rows [3,1,2], [], [5], [9,0] — sorted length-first, then element-wise
        agree(&Value::List(vec![3, 3, 4, 6].into(), Box::new(u(&[3, 1, 2, 5, 9, 0]))));
    }

    #[test]
    fn prod_of_sum() {
        let sums = Value::sum(vec![0, 1, 0, 1], vec![u(&[7, 9]), u(&[3, 4])]);
        agree(&Value::Prod(vec![u(&[2, 1, 2, 1]), sums]));
    }

    #[test]
    fn list_of_sum_fully_discriminated() {
        // List<Sum> — structural all the way down.
        let inner = Value::sum(vec![1, 0, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]);
        agree(&Value::List(vec![2, 4].into(), Box::new(inner)));
    }

    #[test]
    fn variable_length_lists_at_scale() {
        // many u64-list rows of differing length — exercises the length-first arm and its position recursion;
        // must agree with the (length-first) compare2 reference.
        let m = 200u64;
        let mut bounds = Vec::new();
        let mut vals = Vec::new();
        let mut acc = 0usize;
        for i in 0..m {
            let len = (i.wrapping_mul(2654435761) >> 5) % 5; // 0..4
            for j in 0..len {
                vals.push((i.wrapping_mul(40503) ^ j) % 7);
            }
            acc += len as usize;
            bounds.push(acc);
        }
        agree(&Value::List(bounds.into(), Box::new(u(&vals))));
    }

    #[test]
    fn radix_full_range_at_scale() {
        // full 64-bit values force all 8 byte-passes; large n; must match the reference.
        let xs: Vec<u64> = (0..500u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (i << 31)).collect();
        agree(&u(&xs));
    }

    #[test]
    fn scrambled_at_scale() {
        let xs: Vec<u64> = (0..500u64).map(|i| (i.wrapping_mul(2654435761) ^ (i << 13)) % 50).collect();
        agree(&u(&xs));
        let ys: Vec<u64> = (0..500u64).map(|i| i.wrapping_mul(40503) % 7).collect();
        agree(&Value::Prod(vec![u(&xs), u(&ys)]));
    }

    #[test]
    fn labels_mark_runs() {
        // sorted [1,1,3,4,5] → run labels [0,0,1,2,3]
        let seed = vec![0u64; 5];
        let (_perm, labels) = sort_blocks(&seed, &u(&[3, 1, 4, 1, 5]));
        assert_eq!(labels, vec![0, 0, 1, 2, 3]);
    }

    #[test]
    fn run_layout_reads_runs() {
        // labels [0,0,1,2,2] → 3 runs: [0,2), [2,3), [3,5)
        let (ends, firsts) = run_layout(&[0, 0, 1, 2, 2]);
        assert_eq!(ends, vec![2, 3, 5]);
        assert_eq!(firsts, vec![0, 2, 3]);
    }
}
