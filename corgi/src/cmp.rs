//! Structural comparison and discrimination — the order machinery `sort`/`dedup`/`group`/`find` reduce to.
//! Two coherent pieces: `mod compare` (the bulk structural comparator `compare_idx`, which `Rel` and `find`
//! reduce to) and `mod discriminate` (the discrimination sort `sort` uses); both re-exported at this level.
//! Next: the comparison op-bucket (`Gt` and friends) joins once the `Cmp` layer splits out of `core`, at
//! which point these tighten from `pub` to private.

use crate::engine::{gather, row_span};
use crate::value::{Prim, Value};
use std::cmp::Ordering;

pub use compare::*;
pub use discriminate::*;

mod compare {
    //! The bulk structural comparator: a total structural order on rows, recursing through the type —
    //! leaf value, then Prod field-by-field, List LENGTH-FIRST (shorter first; equal lengths element-wise),
    //! Sum tag-then-payload. The discrimination sort matches this order, so `find` stays consistent with `sort`.
    //!
    //! `compare_idx` is the kernel: it compares an explicit list of `(i, j)` index pairs in one descent per
    //! type level, PUSHING the indices down rather than gathering. The Sum arm is the subtle one — comparing
    //! two equal-tag rows needs each row's offset WITHIN its variant — but a `Value::Sum` now CARRIES that
    //! offset (built once at construction), so the arm reads it O(1) and recurses. That keeps the Sum
    //! comparison LINEAR; a naive scalar comparator re-deriving the rank by a prefix scan per pair is O(n²),
    //! which is why the scalar `compare2` survives only as the test oracle.
    //!
    //! `compare_cols` is the diagonal case (`Rel`'s lane compare); arbitrary pairs give the probe comparator
    //! `find`'s batched binary search wants — and because the carried offset is read, not recomputed, sparse
    //! `find` over a sum-shaped haystack is `O(|needle|·log|haystack|)` with no per-round offset rebuild.

    use super::*;

    /// Bulk structural order over an explicit list of index pairs: `out[k]` = the order of row `ia[k]`
    /// of `a` vs row `ib[k]` of `b`, one descent per type level. The pairs are PUSHED DOWN rather than gathered: Sum remaps
    /// `(i,j)` to the within-variant `(wa[i], wb[j])`, List expands each equal-length pair to its
    /// element pairs, and only the leaf actually reads (via `cmp_idx`) — nothing is materialised. Each
    /// level computes its contribution for all pairs and folds lexicographically (keep the first nonzero
    /// sign); no early exit. Linear:
    /// the within-offset cursor passes are O(column), the leaf reads O(pairs·depth).
    ///
    /// The diagonal pairs give `Rel`'s lane compare ([`compare_cols`]); arbitrary pairs give the probe
    /// comparator `find`'s batched binary search wants — one kernel, no gather, for both.
    pub fn compare_idx(a: &Value, b: &Value, ia: &[usize], ib: &[usize]) -> Vec<i8> {
        debug_assert_eq!(ia.len(), ib.len());
        let m = ia.len();
        match (a, b) {
            // leaf: read all pairs at their two indices in one width-dispatched pass.
            (Value::Prim(pa), Value::Prim(pb)) => pa.cmp_idx(ia, ib, pb),

            // product = lexicographic: fold the fields (same pairs), each refining prior ties — keep
            // the first field that broke the tie (the first nonzero sign).
            (Value::Prod(ca), Value::Prod(cb)) => {
                assert_eq!(ca.len(), cb.len(), "compare_idx: product arity");
                let mut ord = vec![0i8; m];
                for (x, y) in ca.iter().zip(cb) {
                    for (o, c) in ord.iter_mut().zip(compare_idx(x, y, ia, ib)) {
                        if *o == 0 { *o = c; }
                    }
                }
                ord
            }

            // sum = tag order first; equal-tag pairs recurse into the lane at their within-variant
            // indices (`wa`/`wb`, one cursor pass per side — the un-quadratic-ing step). No gather:
            // the remapped indices descend as the next level's pairs.
            (Value::Sum(ta, oa, va), Value::Sum(tb, ob, vb)) => {
                assert_eq!(va.len(), vb.len(), "compare_idx: sum arity");
                let (ta_v, tb_v) = (ta.usize_vec(), tb.usize_vec());
                let mut ord: Vec<i8> =
                    ia.iter().zip(ib).map(|(&i, &j)| ta_v[i].cmp(&tb_v[j]) as i8).collect();
                let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); va.len()];
                for (k, (&i, &j)) in ia.iter().zip(ib).enumerate() {
                    if ta_v[i] == tb_v[j] { by_tag[ta_v[i]].push(k); }
                }
                for (t, ks) in by_tag.iter().enumerate() {
                    if ks.is_empty() { continue; }
                    // `oa`/`ob` are the carried within-variant offsets — read, not recomputed.
                    let sia: Vec<usize> = ks.iter().map(|&k| oa[ia[k]]).collect();
                    let sib: Vec<usize> = ks.iter().map(|&k| ob[ib[k]]).collect();
                    let sub = compare_idx(&va[t], &vb[t], &sia, &sib);
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
                for (k, (&i, &j)) in ia.iter().zip(ib).enumerate() {
                    let ((s_a, e_a), (s_b, e_b)) = (row_span(ba, i), row_span(bb, j));
                    let (la, lb) = (e_a - s_a, e_b - s_b);
                    match la.cmp(&lb) {
                        Ordering::Equal if la > 0 => {
                            seg.push((k, sia.len(), la));
                            for p in 0..la { sia.push(s_a + p); sib.push(s_b + p); }
                        }
                        Ordering::Equal => {}       // equal length 0 — stays Equal (0)
                        ow => ord[k] = ow as i8,    // length decides
                    }
                }
                let cmp = compare_idx(va, vb, &sia, &sib);
                for (k, start, len) in seg {
                    if let Some(o) = cmp[start..start + len].iter().copied().find(|&o| o != 0) {
                        ord[k] = o;
                    }
                }
                ord
            }

            _ => panic!("compare_idx: shape mismatch"),
        }
    }

    /// the diagonal case: `out[i]` = the order of row `i` of `a` vs row `i` of `b` — `Rel`'s lane compare.
    pub fn compare_cols(a: &Value, b: &Value) -> Vec<i8> {
        let id: Vec<usize> = (0..a.len()).collect();
        compare_idx(a, b, &id, &id)
    }
}

mod discriminate {
    //! The discrimination sort, `sort_blocks(labels, v) -> (perm, new_labels)`: reorder `v`'s rows WITHIN each
    //! equal-`labels` block, returning the permutation and a REFINED partition — two rows share a `new_labels`
    //! value iff they shared a `labels` value AND are equal.
    //!
    //! `labels` (a non-decreasing block partition) is the whole trick. Discrimination is top-down partition-
    //! refinement, and the "most-significant digit first" order lives in the TYPE STRUCTURE, not in leaf bytes.
    //! Prod refines by field 0, then field 1 within field-0's buckets, then field 2, … (`sort_prod_blocks`
    //! threads `labels` through the field loop); Sum refines by tag first, then by the chosen lane's payload
    //! (`sort_sum_blocks`, computing each row's within-lane offset once); List refines by length first, then by
    //! element at position 0, 1, … (`sort_list_blocks`) — a variable length can't be a radix key, so it MUST
    //! decompose this way. Each level only reorders within the buckets the levels above already separated.
    //! Because it never compares whole rows, the Sum arm never hits a scalar comparator's per-call prefix scan
    //! (the old O(n²)); every stage is linear, so the kernel is O(total input size).
    //!
    //! The leaf (`sort_leaf_blocks`) is the terminal: sort a fixed-width column within each block via
    //! `Prim::sort_block` (a stable LSD byte-radix) plus an O(n) `cmp_idx` scan for the label boundaries. That
    //! scan is the one spot that isn't top-down — pure (Henglein) discrimination would MSB-byte-partition the
    //! leaf too and read labels straight off the partition, with early-out; both are linear, so an MSD leaf is a
    //! constant-factor win that drops `cmp_idx` from the sort path, not a linearity change.

    use super::*;

    /// half-open `[lo,hi)` intervals of equal-label runs in a non-decreasing `labels`.
    fn find_blocks(labels: &[u64]) -> Vec<(usize, usize)> {
        let n = labels.len();
        if n == 0 {
            return Vec::new();
        }
        let mut blocks = Vec::new();
        let mut lo = 0;
        for i in 1..n {
            if labels[i] != labels[i - 1] {
                blocks.push((lo, i));
                lo = i;
            }
        }
        blocks.push((lo, n));
        blocks
    }

    /// sort `v`'s rows within each `labels` block, returning `(perm, refined labels)`. See the module doc for
    /// the algorithm; this is just the dispatch.
    pub fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
        debug_assert_eq!(labels.len(), v.len());
        match v {
            Value::Prim(p) => sort_leaf_blocks(labels, p),
            Value::Prod(cols) => sort_prod_blocks(labels, cols),
            Value::Sum(tags, within, variants) => sort_sum_blocks(labels, tags, within, variants),
            Value::List(bounds, vals) => sort_list_blocks(labels, bounds, vals),
        }
    }

    /// single-block sort of `v`'s rows → the permutation. Test-only: only the
    /// reference-check test consumes it; the ops reach the sort through `sort_blocks`.
    #[cfg(test)]
    pub(crate) fn sort_perm(v: &Value) -> Vec<usize> {
        let labels = vec![0u64; v.len()];
        sort_blocks(&labels, v).0
    }

    /// per-element labels seeding a SEGMENTED sort: each element of outer row `r` gets label `r`, so
    /// `sort_blocks` sorts within each row and rows stay contiguous and in order.
    pub fn segment_labels(bounds: &[usize]) -> Vec<u64> {
        let mut labels = Vec::with_capacity(bounds.last().copied().unwrap_or(0));
        let mut start = 0;
        for (r, &end) in bounds.iter().enumerate() {
            for _ in start..end {
                labels.push(r as u64);
            }
            start = end;
        }
        labels
    }

    /// the run structure of non-decreasing `labels` (e.g. `sort_blocks`' output): `ends[i]` is the exclusive
    /// end of run `i`, `firsts[i]` its first index. Runs are maximal equal-label spans — equal value within a
    /// block. `group` reads `ends` as inner bounds and the representatives at `firsts`; `dedup` keeps `firsts`;
    /// `uniq -c` reads the run lengths.
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
    pub fn runs_per_row(bounds: &[usize], firsts: &[usize]) -> Vec<usize> {
        let mut out = Vec::with_capacity(bounds.len());
        let mut g = 0;
        for &end in bounds {
            while g < firsts.len() && firsts[g] < end { g += 1; }
            out.push(g);
        }
        out
    }

    fn sort_leaf_blocks(labels: &[u64], p: &Prim) -> (Vec<usize>, Vec<u64>) {
        let mut perm = Vec::with_capacity(p.len());
        let mut new_labels = Vec::with_capacity(p.len());
        let mut next = 0u64;
        for (lo, hi) in find_blocks(labels) {
            // stable byte-radix within the block (one comparator gone); a single `cmp_idx` over the
            // adjacent pairs feeds the O(n) run-label scan — one width-dispatch, not one per element.
            // Radix is stable, so the recursion stays stable — group keeps V-within-key order.
            let sorted = p.sort_block(&(lo..hi).collect::<Vec<usize>>());
            let adj = p.cmp_idx(&sorted[1..], &sorted[..sorted.len() - 1], p);
            for (k, &i) in sorted.iter().enumerate() {
                if k > 0 && adj[k - 1] != 0 {
                    next += 1;
                }
                new_labels.push(next);
                perm.push(i);
            }
            next += 1;
        }
        (perm, new_labels)
    }

    fn sort_prod_blocks(labels: &[u64], cols: &[Value]) -> (Vec<usize>, Vec<u64>) {
        let n = labels.len();
        if cols.is_empty() {
            return ((0..n).collect(), labels.to_vec());
        }
        // lexicographic = sort by field 0, then refine within ties by field 1, ...
        let mut perm: Vec<usize> = (0..n).collect();
        let mut cur = labels.to_vec();
        for c in cols {
            let reordered = gather(c, &perm);
            let (sub_perm, sub_labels) = sort_blocks(&cur, &reordered);
            perm = sub_perm.iter().map(|&k| perm[k]).collect();
            cur = sub_labels;
        }
        (perm, cur)
    }

    fn sort_sum_blocks(labels: &[u64], tags: &Prim, within: &[usize], variants: &[Value]) -> (Vec<usize>, Vec<u64>) {
        let n = labels.len();
        // 1. discriminate by the tag column directly — a u8 leaf, so a single-pass radix.
        let (perm_disc, labels_disc) = sort_leaf_blocks(labels, tags);
        // 2. each row's position within its lane — read from the carried offset (no recompute).
        let tag_vec = tags.usize_vec();
        // 3. within each tag-block, recurse into that lane's gathered rows.
        let mut perm = perm_disc.clone();
        let mut new_labels = vec![0u64; n];
        let mut next = 0u64;
        for (lo, hi) in find_blocks(&labels_disc) {
            if hi - lo == 1 {
                new_labels[lo] = next;
                next += 1;
                continue;
            }
            let t = tag_vec[perm_disc[lo]]; // the whole block shares a tag
            let lane_pos: Vec<usize> = (lo..hi).map(|i| within[perm_disc[i]]).collect();
            let lane = gather(&variants[t], &lane_pos);
            let seed = vec![0u64; hi - lo];
            let (sub_perm, sub_labels) = sort_blocks(&seed, &lane);
            let span = sub_labels.iter().copied().max().unwrap_or(0);
            for (i, (&sp, &sl)) in sub_perm.iter().zip(&sub_labels).enumerate() {
                perm[lo + i] = perm_disc[lo + sp];
                new_labels[lo + i] = next + sl;
            }
            next += span + 1;
        }
        (perm, new_labels)
    }

    fn sort_list_blocks(labels: &[u64], bounds: &[usize], vals: &Value) -> (Vec<usize>, Vec<u64>) {
        let n = labels.len();
        // start-offsets (n+1): row i is vals[starts[i]..starts[i+1]).
        let starts: Vec<usize> = std::iter::once(0).chain(bounds.iter().copied()).collect();
        let lengths: Vec<u64> = starts.windows(2).map(|w| (w[1] - w[0]) as u64).collect();

        // length-first: refine rows by length, then (below) position by position.
        let (perm, cur_labels) = sort_blocks(labels, &Value::u64(lengths));

        let mut new_perm = perm.clone();
        let mut new_labels = vec![0u64; n];
        let mut next_offset = 0u64;
        for (lo, hi) in find_blocks(&cur_labels) {
            let block_size = hi - lo;
            if block_size == 1 {
                new_labels[lo] = next_offset;
                next_offset += 1;
                continue;
            }
            let sample = perm[lo]; // the whole block shares a length
            let len = starts[sample + 1] - starts[sample];
            // sort the block by element 0, then 1, … len-1 — each a structural recursion.
            let mut local_perm: Vec<usize> = (0..block_size).collect();
            let mut local_labels = vec![0u64; block_size];
            for pos in 0..len {
                let positions: Vec<usize> =
                    local_perm.iter().map(|&k| starts[perm[lo + k]] + pos).collect();
                let elem = gather(vals, &positions);
                let (sub_perm, sub_labels) = sort_blocks(&local_labels, &elem);
                local_perm = sub_perm.iter().map(|&k| local_perm[k]).collect();
                local_labels = sub_labels;
            }
            let block_max = local_labels.iter().copied().max().unwrap_or(0);
            for (i, (&lp, &ll)) in local_perm.iter().zip(&local_labels).enumerate() {
                new_perm[lo + i] = perm[lo + lp];
                new_labels[lo + i] = next_offset + ll;
            }
            next_offset += block_max + 1;
        }
        (new_perm, new_labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
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
            (Value::Sum(ta, _, va), Value::Sum(tb, _, vb)) => {
                let (tav, tbv) = (ta.usize_vec(), tb.usize_vec());
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

    #[test]
    fn compare_cols_matches_scalar() {
        // prim
        agree_cmp(&u(&[5, 3, 8, 1, 9]), &u(&[5, 4, 2, 1, 0]));
        // product: lexicographic fold over fields
        agree_cmp(
            &Value::Prod(vec![u(&[2, 1, 2, 1]), u(&[10, 20, 5, 30])]),
            &Value::Prod(vec![u(&[2, 1, 1, 1]), u(&[10, 25, 5, 30])]),
        );
        // sum: equal-tag lanes hit the payload compare, unequal-tag lanes the tag order — the arm
        // that was quadratic through `compare2`.
        agree_cmp(
            &Value::sum(vec![0, 1, 0, 1, 0], vec![u(&[5, 7, 9]), u(&[2, 4])]),
            &Value::sum(vec![0, 1, 1, 1, 0], vec![u(&[5, 8]), u(&[2, 3, 1])]),
        );
        // list: length-first, then position-wise first difference over ragged rows
        agree_cmp(
            &Value::List(vec![2, 2, 5, 6], Box::new(u(&[3, 1, 4, 5, 9, 0]))),
            &Value::List(vec![2, 3, 6, 7], Box::new(u(&[3, 2, 7, 4, 5, 1, 0]))),
        );
        // nested: a sum in secondary product position (the within-offset remap under a fold)
        agree_cmp(
            &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 1, 0], vec![u(&[5, 8]), u(&[3])])]),
            &Value::Prod(vec![u(&[1, 2, 1]), Value::sum(vec![0, 0, 1], vec![u(&[5, 9]), u(&[3])])]),
        );
        // nested: a sum AS the list element — the position loop gathers sum rows and remaps offsets.
        agree_cmp(
            &Value::List(vec![2, 4], Box::new(Value::sum(vec![0, 1, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]))),
            &Value::List(vec![2, 4], Box::new(Value::sum(vec![0, 0, 1, 1], vec![u(&[5, 7]), u(&[2, 9])]))),
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

        let la = Value::List(vec![2, 2, 5, 6], Box::new(u(&[3, 1, 4, 5, 9, 0])));
        let lb = Value::List(vec![2, 3, 6, 7], Box::new(u(&[3, 2, 7, 4, 5, 1, 0])));
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
        agree(&Value::List(vec![3, 3, 4, 6], Box::new(u(&[3, 1, 2, 5, 9, 0]))));
    }

    #[test]
    fn prod_of_sum() {
        let sums = Value::sum(vec![0, 1, 0, 1], vec![u(&[7, 9]), u(&[3, 4])]);
        agree(&Value::Prod(vec![u(&[2, 1, 2, 1]), sums]));
    }

    #[test]
    fn list_of_sum_fully_discriminated() {
        // List<Sum> — now structural all the way down (was the residual quadratic).
        let inner = Value::sum(vec![1, 0, 0, 1], vec![u(&[5, 8]), u(&[2, 9])]);
        agree(&Value::List(vec![2, 4], Box::new(inner)));
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
        agree(&Value::List(bounds, Box::new(u(&vals))));
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
