//! Structural comparison and discrimination — the order machinery `sort`/`dedup`/`group`/`find` reduce to.
//! Two coherent pieces: `mod compare` (the bulk structural comparator `compare_idx`, which `Rel` and `find`
//! reduce to) and `mod discriminate` (the discrimination sort `sort` uses); both re-exported at this level.

use crate::engine::{gather, row_span};
use crate::value::{Bounds, Prim, Value};
use std::cmp::Ordering;

pub(crate) use compare::*;
pub(crate) use discriminate::*;

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

/// One report from [`survey`]: a maximal range drawn from one side of the interleaving, or a
/// single matched pair present in both. The bidirectional generalization of `find_ranges`' report.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Run {
    /// Rows `a[lo..hi)` come next in merged order, all strictly less than the current head of `b`
    /// (exclusive to `a` over this range).
    A(usize, usize),
    /// Rows `b[lo..hi)` come next in merged order, all strictly less than the current head of `a`
    /// (exclusive to `b` over this range).
    B(usize, usize),
    /// A single matched pair: row `a[ia]` and row `b[ib]` are structurally equal.
    Both(usize, usize),
}

/// Advance `idx` while the supplied sorted-position predicate remains true. Exponential probe plus
/// binary refinement costs `O(log gap)` comparisons rather than one per row.
fn gallop_lt_by(idx: &mut usize, hi: usize, lt: impl Fn(usize) -> bool) {
    // nothing to do unless the row at the cursor is itself still below the pivot.
    if *idx < hi && lt(*idx) {
        let mut step = 1;
        while *idx + step < hi && lt(*idx + step) {
            *idx += step;
            step <<= 1;
        }
        // binary refine over the last (overshot) doubling.
        step >>= 1;
        while step > 0 {
            if *idx + step < hi && lt(*idx + step) {
                *idx += step;
            }
            step >>= 1;
        }
        // `*idx` sits on the last row `< piv`; step past it to the first row `>= piv`.
        *idx += 1;
    }
}

/// Shared zig-zag gallop over two sorted domains with a caller-supplied comparison.
///
/// Surveying mutual interleaving returns a sequence of [`Run`]s — maximal ranges exclusive to one
/// side and the single matched pairs present in both — instead of a per-pair two-pointer. The
/// bidirectional generalization of `find_ranges` (the one-directional needle-into-haystack gallop).
///
/// The caller bulk-`gather`s each `A`/`B` range and consolidates only at the `Both` pairs, so the
/// merge crosses the corgi/Rust boundary once per *range* rather than once per *row*. This owns no
/// times/diffs: it reports only positions, and the caller drives its own lattice logic off the runs.
///
/// Guarantees: the `A` ranges plus every `Both`'s `ia` cover `0..a.len()` in order with no gap or
/// overlap (`b` likewise via `hi`/`ib`); expanding the runs to their rows yields a non-decreasing
/// structural sequence; every `Both(ia, ib)` has `compare_at(a, ia, b, ib) == Equal`. Equal
/// duplicates within one side after the match fall through as follow-on `A`/`B` runs (as in DD's
/// `trie_merger::survey`, the reference this ports).
fn survey_by(
    na: usize,
    nb: usize,
    compare: impl Fn(usize, usize) -> Ordering,
) -> Vec<Run> {
    let (mut i, mut j) = (0usize, 0usize);
    let mut out = Vec::new();
    while i < na && j < nb {
        match compare(i, j) {
            Ordering::Less => {
                let start = i;
                i += 1;
                gallop_lt_by(&mut i, na, |k| compare(k, j) == Ordering::Less);
                out.push(Run::A(start, i));
            }
            Ordering::Equal => {
                out.push(Run::Both(i, j));
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                let start = j;
                j += 1;
                gallop_lt_by(&mut j, nb, |k| compare(i, k) == Ordering::Greater);
                out.push(Run::B(start, j));
            }
        }
    }
    // one side exhausted: the remainder of the other is a single trailing run.
    if i < na {
        out.push(Run::A(i, na));
    }
    if j < nb {
        out.push(Run::B(j, nb));
    }
    out
}

/// Survey two structurally-sorted columns, hoisting primitive-width dispatch out of the gallop.
pub fn survey(a: &Value, b: &Value) -> Vec<Run> {
    match (a, b) {
        (Value::Prim(Prim::U8(a)), Value::Prim(Prim::U8(b))) => {
            survey_by(a.len(), b.len(), |i, j| a[i].cmp(&b[j]))
        }
        (Value::Prim(Prim::U16(a)), Value::Prim(Prim::U16(b))) => {
            survey_by(a.len(), b.len(), |i, j| a[i].cmp(&b[j]))
        }
        (Value::Prim(Prim::U32(a)), Value::Prim(Prim::U32(b))) => {
            survey_by(a.len(), b.len(), |i, j| a[i].cmp(&b[j]))
        }
        (Value::Prim(Prim::U64(a)), Value::Prim(Prim::U64(b))) => {
            survey_by(a.len(), b.len(), |i, j| a[i].cmp(&b[j]))
        }
        _ => survey_by(a.len(), b.len(), |i, j| compare_at(a, i, b, j)),
    }
}

/// Segment ends of the maximal equal-value runs in a structurally-sorted column `keys`: `out[g]` is
/// the exclusive end of group `g`, so group `g` occupies `out[g-1]..out[g]` (with an implicit
/// `out[-1] = 0`) and `out.last() == keys.len()`. One columnar adjacent-compare pass — the
/// single-column analogue of the equal-key boundaries a [`survey`] reveals across two runs, and the
/// `Value`-column counterpart of [`run_layout`]'s `ends` (which reads a precomputed labels vector).
pub fn group_bounds(keys: &Value) -> Vec<usize> {
    let n = keys.len();
    if n == 0 {
        return Vec::new();
    }
    // signs[k] = order of keys[k] vs keys[k+1]; a nonzero sign is a group boundary after k.
    let ia: Vec<usize> = (0..n - 1).collect();
    let ib: Vec<usize> = (1..n).collect();
    let signs = compare_idx(keys, keys, &ia, &ib);
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
        let m = ia.len();
        match (a, b) {
            // leaf: read all pairs at their two indices in one width-dispatched pass.
            (Value::Prim(pa), Value::Prim(pb)) => pa.cmp_idx(ia, ib, pb),

            // single-field product: the field's order IS the order — skip the fold + tie vec.
            (Value::Prod(ca), Value::Prod(cb)) if ca.len() == 1 && cb.len() == 1 => {
                compare_idx(&ca[0], &cb[0], ia, ib)
            }

            // product = lexicographic: field 0 over all pairs, then each later field over the
            // SURVIVING TIES only — when an early field discriminates most pairs (the common
            // case), later fields cost proportionally to the ties, not to m.
            (Value::Prod(ca), Value::Prod(cb)) => {
                assert_eq!(ca.len(), cb.len(), "compare_idx: product arity");
                let mut ord = compare_idx(&ca[0], &cb[0], ia, ib);
                if ca.len() > 1 {
                    let mut tie_k: Vec<usize> = (0..m).filter(|&k| ord[k] == 0).collect();
                    let mut tia: Vec<usize> = tie_k.iter().map(|&k| ia[k]).collect();
                    let mut tib: Vec<usize> = tie_k.iter().map(|&k| ib[k]).collect();
                    for (x, y) in ca[1..].iter().zip(&cb[1..]) {
                        if tie_k.is_empty() {
                            break;
                        }
                        let sub = compare_idx(x, y, &tia, &tib);
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
            (Value::Sum(ta, oa, va), Value::Sum(tb, ob, vb)) => {
                assert_eq!(va.len(), vb.len(), "compare_idx: sum arity");
                // Read the discriminants in place. Decoding a whole tag column per call made a
                // scalar `compare_at` O(column): a chunk merge over sum-shaped keys spent 40% of
                // its time re-decoding tags it looked at one row of.
                let mut ord: Vec<i8> = ia
                    .iter()
                    .zip(ib)
                    .map(|(&i, &j)| ta.usize_at(i).cmp(&tb.usize_at(j)) as i8)
                    .collect();
                let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); va.len()];
                for (k, (&i, &j)) in ia.iter().zip(ib).enumerate() {
                    let t = ta.usize_at(i);
                    if t == tb.usize_at(j) { by_tag[t].push(k); }
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

            // Unit rows carry no payload — always equal. (Added for `crate::arrange`: a unit-valued
            // column, e.g. `distinct`'s output, must be a sortable arrangement payload.)
            (Value::Unit(_), Value::Unit(_)) => vec![0i8; ia.len()],

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
            // unit rows are all equal: stable identity perm, no label refinement.
            Value::Unit(n) => ((0..*n).collect(), labels.to_vec()),
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
    pub fn segment_labels(bounds: &Bounds) -> Vec<u64> {
        let bounds = bounds.to_vec(); // step 1: materialize; step 2 makes this stride-aware
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
    pub fn runs_per_row(bounds: &Bounds, firsts: &[usize]) -> Vec<usize> {
        let bounds = bounds.to_vec();
        let mut out = Vec::with_capacity(bounds.len());
        let mut g = 0;
        for &end in &bounds {
            while g < firsts.len() && firsts[g] < end { g += 1; }
            out.push(g);
        }
        out
    }

    fn sort_leaf_blocks(labels: &[u64], p: &Prim) -> (Vec<usize>, Vec<u64>) {
        // Per-block: stable byte-radix (or tiny-block insertion sort) IN PLACE over the perm
        // slice, with one shared scratch — a refinement pass produces millions of tiny blocks,
        // and per-block allocations (index collect + sort output + adjacent-compare vec) were
        // ~17% self of a join-heavy profile. The adjacent compare runs ONCE over the whole
        // column afterwards (one width dispatch), with block boundaries forcing label breaks.
        let n = p.len();
        let mut perm: Vec<usize> = Vec::with_capacity(n);
        let mut ends: Vec<usize> = Vec::new();
        let mut tmp: Vec<usize> = Vec::new();
        for (lo, hi) in find_blocks(labels) {
            let start = perm.len();
            perm.extend(lo..hi);
            if hi - lo > 1 {
                p.sort_block_scratch(&mut perm[start..], &mut tmp);
            }
            ends.push(perm.len());
        }
        let mut new_labels = Vec::with_capacity(n);
        if n > 0 {
            let adj = if n > 1 { p.cmp_idx(&perm[1..], &perm[..n - 1], p) } else { Vec::new() };
            let mut next = 0u64;
            let mut b = 0usize;
            for k in 0..n {
                if k > 0 {
                    let boundary = ends[b] == k;
                    if boundary {
                        b += 1;
                    }
                    if boundary || adj[k - 1] != 0 {
                        next += 1;
                    }
                }
                new_labels.push(next);
            }
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
        // 2. within each tag-block, recurse into that lane's gathered rows, at each row's position
        //    within its lane — read from the carried offset (no recompute).
        let mut perm = perm_disc.clone();
        let mut new_labels = vec![0u64; n];
        let mut next = 0u64;
        for (lo, hi) in find_blocks(&labels_disc) {
            if hi - lo == 1 {
                new_labels[lo] = next;
                next += 1;
                continue;
            }
            // the whole block shares a tag, so this reads ONE tag per block — decoding the whole
            // column for it (as this did) is O(column) work for O(blocks) reads.
            let t = tags.usize_at(perm_disc[lo]);
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

    fn sort_list_blocks(labels: &[u64], bounds: &Bounds, vals: &Value) -> (Vec<usize>, Vec<u64>) {
        let n = labels.len();
        // STRIDE FAST PATH (the array-language special case): equal-width byte records — a uniform
        // inner list — packable into a u64 sort as ONE wide leaf, because for equal lengths the
        // lexicographic byte order IS big-endian numeric order. O(1) stride detection diverts to the
        // dense leaf radix instead of the length-first + position-by-position structural sort below
        // (the `stride_sort_matches_offsets` test pins the two to the same result). Records wider than
        // 8 bytes, or non-byte leaves, fall through to the general path.
        if let (Some(k), Value::Prim(Prim::U8(bytes))) = (bounds.strided(), vals) {
            if (1..=8).contains(&k) {
                let keys: Vec<u64> = (0..n)
                    .map(|r| (0..k).fold(0u64, |key, p| (key << 8) | bytes[r * k + p] as u64))
                    .collect();
                return sort_leaf_blocks(labels, &Prim::U64(std::sync::Arc::new(keys)));
            }
        }
        let bounds = bounds.to_vec();
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
    fn primitive_survey_dispatch_matches_the_structural_comparator() {
        fn agree(a: Value, b: Value) {
            let fallback = survey_by(a.len(), b.len(), |i, j| compare_at(&a, i, &b, j));
            assert_eq!(survey(&a, &b), fallback);
        }

        agree(Value::u8(vec![1, 3, 5]), Value::u8(vec![2, 3, 6]));
        agree(Value::u16(vec![1, 4, 9]), Value::u16(vec![0, 4, 10]));
        agree(Value::u32(vec![2, 7, 11]), Value::u32(vec![1, 7, 12]));
        agree(Value::u64(vec![1, 2, 8]), Value::u64(vec![2, 3, 9]));
        agree(
            Value::Prod(vec![Value::u64(vec![1, 2]), Value::u8(vec![4, 0])]),
            Value::Prod(vec![Value::u64(vec![1, 3]), Value::u8(vec![5, 0])]),
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
