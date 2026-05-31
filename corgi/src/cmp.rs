//! Structural comparison and discrimination — the order machinery `sort`/`dedup`/`group`/`find` reduce to.
//! Two coherent pieces: `mod compare` (the comparator `find` searches with) and `mod discriminate` (the
//! discrimination sort `sort` uses); both re-exported at this level. Next: the comparison op-bucket (`Gt` and
//! friends) joins once the `Cmp` layer splits out of `core`, at which point these tighten from `pub` to private.

use crate::engine::{gather, row_span};
use crate::value::{Prim, Value};
use std::cmp::Ordering;

pub use compare::*;
pub use discriminate::*;

mod compare {
    //! The comparator `find` searches with: a total structural order on rows, recursing through the type —
    //! leaf value, then Prod field-by-field, List LENGTH-FIRST (shorter first; equal lengths element-wise),
    //! Sum tag-then-payload. The discrimination sort matches this order, so `find` stays consistent with `sort`.
    //!
    //! COST NOTE — the Sum arm. To compare two equal-tag sum rows we need each row's offset WITHIN its variant's
    //! payload array, and a bare tag column doesn't store it: `compare2` recovers it by a prefix scan (rank of the
    //! row among prior rows of the same tag), so a single sum comparison is O(i), and `find` binary-searching an
    //! m-row haystack of sums degrades to O(n·m) — the dominant probe at ~m/2 costs O(m), swamping the log. The
    //! columnar crate avoids this by storing a per-sum within-variant offset `Vec<usize>`; we deliberately don't,
    //! to keep `Value` sequential-first (sort never needs it — `sort_sum_blocks` computes lane offsets in bulk).
    //!
    //! The cliff is LATENT (no current workload searches a sum-shaped haystack) and the remedy is find-LOCAL, not a
    //! representation change: `find`'s haystack is always SORTED, and tag is the most-significant key, so each
    //! variant occupies a CONTIGUOUS run and the within-variant offset collapses to `m − block_start[tag[m]]` —
    //! O(1) per probe from a K-sized boundary table (K = variant count), found once per `find` in O(K·log m). The
    //! needle side stays cheap by pre-extracting each probe row as a singleton (O(depth), and the needle set is
    //! small). So when a sum-haystack workload appears, add a find-local comparator that carries haystack block
    //! starts down the recursion; leave `compare2` as the side-condition-free reference the sort tests use.

    use super::*;

    /// Structural order ACROSS two same-shape columns: row `i` of `a` vs row `j` of `b` — the cross-column
    /// form `find` needs (needle vs haystack); within one column it's `compare2(v, i, v, j)`.
    pub fn compare2(a: &Value, i: usize, b: &Value, j: usize) -> Ordering {
        match (a, b) {
            (Value::Prim(pa), Value::Prim(pb)) => pa.cmp_at(i, pb, j),
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
                let (ti, tj) = (ta.usize_at(i), tb.usize_at(j));
                match ti.cmp(&tj) {
                    Ordering::Equal => {
                        let wi = (0..i).filter(|&k| ta.usize_at(k) == ti).count();
                        let wj = (0..j).filter(|&k| tb.usize_at(k) == ti).count();
                        compare2(&va[ti], wi, &vb[ti], wj)
                    }
                    o => o,
                }
            }
            _ => panic!("compare2: shape mismatch"),
        }
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
    //! Because it never compares whole rows, the Sum arm never hits `compare2`'s per-call prefix scan (the old
    //! O(n²)); every stage is linear, so the kernel is O(total input size).
    //!
    //! The leaf (`sort_leaf_blocks`) is the terminal: sort a fixed-width column within each block via
    //! `Prim::sort_block` (a stable LSD byte-radix) plus an O(n) `cmp_at` scan for the label boundaries. That
    //! scan is the one spot that isn't top-down — pure (Henglein) discrimination would MSB-byte-partition the
    //! leaf too and read labels straight off the partition, with early-out; both are linear, so an MSD leaf is a
    //! constant-factor win that drops `cmp_at` from the sort path, not a linearity change.

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
            Value::Sum(tags, variants) => sort_sum_blocks(labels, tags, variants),
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

    fn sort_leaf_blocks(labels: &[u64], p: &Prim) -> (Vec<usize>, Vec<u64>) {
        let mut perm = Vec::with_capacity(p.len());
        let mut new_labels = Vec::with_capacity(p.len());
        let mut next = 0u64;
        for (lo, hi) in find_blocks(labels) {
            // stable byte-radix within the block (one comparator gone); `cmp_at` stays only for the O(n)
            // run-label scan. Radix is stable, so the recursion stays stable — group keeps V-within-key order.
            let sorted = p.sort_block(&(lo..hi).collect::<Vec<usize>>());
            for (k, &i) in sorted.iter().enumerate() {
                if k > 0 && p.cmp_at(i, p, sorted[k - 1]) != Ordering::Equal {
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

    fn sort_sum_blocks(labels: &[u64], tags: &Prim, variants: &[Value]) -> (Vec<usize>, Vec<u64>) {
        let n = labels.len();
        // 1. discriminate by the tag column directly — a u8 leaf, so a single-pass radix.
        let (perm_disc, labels_disc) = sort_leaf_blocks(labels, tags);
        // 2. each row's position within its lane — computed ONCE (the un-quadratic-ing step).
        let tag_vec = tags.usize_vec();
        let mut cursor = vec![0usize; variants.len()];
        let mut within = vec![0usize; n];
        for (i, &t) in tag_vec.iter().enumerate() {
            within[i] = cursor[t];
            cursor[t] += 1;
        }
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
