//! The discrimination sort, in datatoad's shape: `(labels, index)` in, sorted data out.
//!
//! [`sort_indexed`] sorts the rows `index[..]` of a column within the blocks `labels` describes,
//! by structural order: a leaf by its stored unsigned bytes, `Prod` lexicographically by field,
//! `Sum` by tag then payload, `List` length first and then element by element, `Unit` all equal.
//! Position `k` of the problem is row `index[k]`, in block `labels[k]`.
//!
//! Contract. `labels` is one `u64` per position and non-decreasing (checked in debug builds;
//! blocks are runs of equal adjacent labels, so a non-monotone vector would be mis-sorted, not
//! rejected). Label values mean nothing beyond equality. On return `index` is permuted into
//! sorted order — block-stable, and stable within a block — and `labels` is rewritten in place
//! as the refined partition in that order: two positions share a label iff they shared one before
//! and their rows are structurally equal; the values are the dense run index. The returned
//! permutation `perm` says where each position came from (`new_index[k] == old_index[perm[k]]`),
//! so a caller can move any parallel array the same way. With `emit` the sorted rows come back as
//! a column.
//!
//! Nothing is gathered before a level sorts. A leaf pulls its keys through `index` once, into a
//! packed `(key, position)` buffer, and every radix pass after that is sequential; the sorted keys
//! ARE the output column, so no gather reforms the data afterwards. A `Prod` sorts each field at
//! the same positions with the labels the previous field refined; a field's output is final the
//! moment it is emitted, because later fields only permute within its classes, on which it is
//! constant. A `Sum` sorts the tag as a virtual leaf and then each lane at the carried offsets.
//! A `List` sorts the length, refines element by element without emitting, and gathers the
//! elements once at the end: row-major output of variable-length rows is a scatter, so this is
//! the one place a permutation is applied to data. [`sort_blocks`] is the labels form, this
//! kernel at the identity index.

use crate::engine::gather;
use crate::value::{Bounds, Prim, Tags, Value};

/// Buffers the sort reuses across every block and level of one call: the pulled keys of the
/// level in progress, the alternate key and permutation buffers the radix ping-pongs against,
/// a spare index buffer for applying a permutation, and the digit counters. Hoisted because a
/// refinement pass produces millions of tiny blocks and must not allocate per block.
#[derive(Default)]
pub(crate) struct SortScratch {
    keys: Vec<u64>,
    keys_alt: Vec<u64>,
    perm_alt: Vec<usize>,
    index_alt: Vec<usize>,
    counts: Vec<u32>,
}

/// Sort the rows `index[..]` of `v` within the blocks of `labels`. See the module doc for the
/// contract. Returns the permutation applied to the positions and, with `emit`, the sorted rows.
pub(crate) fn sort_indexed(
    v: &Value,
    labels: &mut [u64],
    index: &mut [usize],
    emit: bool,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    debug_assert_eq!(labels.len(), index.len(), "sort_indexed: one label per position");
    debug_assert!(labels.windows(2).all(|w| w[0] <= w[1]), "sort_indexed: labels must be non-decreasing");
    let m = index.len();
    if m <= 1 {
        if m == 1 {
            labels[0] = 0;
        }
        return ((0..m).collect(), emit.then(|| gather(v, index)));
    }
    match v {
        Value::Prim(p) => sort_leaf(p, labels, index, emit, scratch),
        Value::Prod(cols) => sort_prod(cols, labels, index, emit, scratch),
        Value::Sum(tags, lanes) => sort_sum(tags, lanes, labels, index, emit, scratch),
        Value::List(bounds, vals) => sort_list(bounds, vals, labels, index, emit, scratch),
        // every unit row is equal: the order is already the answer and no block splits, but the
        // labels are renumbered densely like every other arm's.
        Value::Unit(_) => {
            refine(labels, |_| false);
            ((0..m).collect(), emit.then_some(Value::Unit(m)))
        }
    }
}

/// The labels form: sort all of `v`'s rows, in stored order, within the blocks of `labels`.
/// `perm[k]` is the input row at output position `k`; the refined labels are aligned with `perm`.
pub(crate) fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let (perm, _) = sort_indexed(v, &mut labels, &mut index, false, &mut scratch);
    debug_assert_eq!(perm, index, "from the identity index the permutation is the index");
    (index, labels)
}

/// The labels form with the data: `(perm, refined labels, the sorted rows)`.
pub(crate) fn sort_values(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>, Value) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let (_, out) = sort_indexed(v, &mut labels, &mut index, true, &mut scratch);
    (index, labels, out.expect("emit was requested"))
}

// ---- the arms -------------------------------------------------------------------------------

fn sort_leaf(
    p: &Prim,
    labels: &mut [u64],
    index: &mut [usize],
    emit: bool,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    // the one indirect read: each position's key, widened to u64, in position order.
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    p.pull_u64(index, &mut keys);
    let perm = sort_keys(&mut keys, labels, scratch);
    permute(index, &perm, &mut scratch.index_alt);
    // the keys are now in sorted order: narrowed back to the leaf's width they are the column.
    let out = emit.then(|| Value::Prim(p.like(&keys)));
    scratch.keys = keys;
    (perm, out)
}

fn sort_prod(
    cols: &[Value],
    labels: &mut [u64],
    index: &mut [usize],
    emit: bool,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let m = index.len();
    let mut perm: Vec<usize> = (0..m).collect();
    if cols.is_empty() {
        refine(labels, |_| false);
        return (perm, emit.then_some(Value::Prod(Vec::new())));
    }
    let mut outs = Vec::with_capacity(cols.len());
    let mut settled = false;
    for c in cols {
        if settled {
            // no two positions are tied, so this field cannot move a row or split a class; its
            // sorted form is its rows in the order the index already has.
            if emit {
                outs.push(gather(c, index));
            }
            continue;
        }
        let (step, out) = sort_indexed(c, labels, index, emit, scratch);
        permute(&mut perm, &step, &mut scratch.index_alt);
        if let Some(o) = out {
            outs.push(o);
        }
        settled = fully_discriminated(labels);
    }
    (perm, emit.then_some(Value::Prod(outs)))
}

fn sort_sum(
    tags: &Tags,
    lanes: &[Value],
    labels: &mut [u64],
    index: &mut [usize],
    emit: bool,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let m = index.len();
    // one lane throughout: the tag decides nothing and row i is that lane's row i.
    if let Some(t) = tags.const_tag() {
        let (perm, out) = sort_indexed(&lanes[t], labels, index, emit, scratch);
        let out = out.map(|sorted| {
            let mut ls: Vec<Value> = lanes.iter().map(|l| gather(l, &[])).collect();
            ls[t] = sorted;
            Value::sum_tagged(Tags::constant(t, m), ls)
        });
        return (perm, out);
    }
    let Tags::Column(tag_col, within) = tags else { unreachable!("const handled above") };
    // 1. the tag, as a virtual leaf read through the index.
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    keys.extend(index.iter().map(|&r| tag_col.usize_at(r) as u64));
    let mut perm = sort_keys(&mut keys, labels, scratch);
    permute(index, &perm, &mut scratch.index_alt);
    let tags_out: Vec<usize> = keys.iter().map(|&k| k as usize).collect();
    scratch.keys = keys;
    let after_tags: Vec<u64> = labels.to_vec();
    // 2. each lane, at the positions that carry its tag, reading the lane at the carried offsets.
    //    Within a block the positions of one tag are now contiguous, so a lane's sub-problem is a
    //    subset in increasing position order with monotone labels.
    let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); lanes.len()];
    for (q, &t) in tags_out.iter().enumerate() {
        by_tag[t].push(q);
    }
    let mut lanes_out = Vec::with_capacity(lanes.len());
    let (mut rows, mut old_perm) = (Vec::new(), Vec::new());
    for (t, lane) in lanes.iter().enumerate() {
        let qs = &by_tag[t];
        if qs.is_empty() {
            if emit {
                lanes_out.push(gather(lane, &[]));
            }
            continue;
        }
        let mut index_t: Vec<usize> = qs.iter().map(|&q| within[index[q]]).collect();
        let mut labels_t: Vec<u64> = qs.iter().map(|&q| labels[q]).collect();
        let (step, out) = sort_indexed(lane, &mut labels_t, &mut index_t, emit, scratch);
        rows.clear();
        rows.extend(qs.iter().map(|&q| index[q]));
        old_perm.clear();
        old_perm.extend(qs.iter().map(|&q| perm[q]));
        for (i, &q) in qs.iter().enumerate() {
            index[q] = rows[step[i]];
            perm[q] = old_perm[step[i]];
            labels[q] = labels_t[i];
        }
        if let Some(o) = out {
            lanes_out.push(o);
        }
    }
    // 3. the lanes numbered their classes locally; (block-and-tag class, lane class) is monotone
    //    over the output, so one pass makes it dense.
    densify(&after_tags, labels);
    // the emitted lanes hold each tag's rows in output order, which is lane storage order.
    let out = emit.then(|| Value::sum_tagged(Tags::from_tags(tags_out, lanes.len()), lanes_out));
    (perm, out)
}

fn sort_list(
    bounds: &Bounds,
    vals: &Value,
    labels: &mut [u64],
    index: &mut [usize],
    emit: bool,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let m = index.len();
    // Equal-width byte records up to 8 wide pack into one u64 key: for equal lengths the
    // lexicographic byte order IS big-endian numeric order, so this is one leaf sort, and the
    // sorted keys unpack straight back into the sorted records.
    if let (Some(k), Value::Prim(Prim::U8(bytes))) = (bounds.strided(), vals) {
        if (1..=8).contains(&k) {
            let mut keys = std::mem::take(&mut scratch.keys);
            keys.clear();
            keys.extend(index.iter().map(|&r| (0..k).fold(0u64, |key, p| (key << 8) | bytes[r * k + p] as u64)));
            let perm = sort_keys(&mut keys, labels, scratch);
            permute(index, &perm, &mut scratch.index_alt);
            let out = emit.then(|| {
                let mut o = Vec::with_capacity(m * k);
                for &key in keys.iter() {
                    for p in (0..k).rev() {
                        o.push((key >> (8 * p)) as u8);
                    }
                }
                Value::List(Bounds::Stride(k, m), Box::new(Value::u8(o)))
            });
            scratch.keys = keys;
            return (perm, out);
        }
    }
    let len_of = |r: usize| {
        let (s, e) = bounds.span(r);
        e - s
    };
    // 1. length first — unless every row has the same length, when it cannot split anything.
    let mut perm: Vec<usize> = (0..m).collect();
    if bounds.strided().is_none() {
        let mut keys = std::mem::take(&mut scratch.keys);
        keys.clear();
        keys.extend(index.iter().map(|&r| len_of(r) as u64));
        perm = sort_keys(&mut keys, labels, scratch);
        permute(index, &perm, &mut scratch.index_alt);
        scratch.keys = keys;
    }
    let after_len: Vec<u64> = labels.to_vec();
    // 2. element by element, over the rows still long enough, refining only. A block holds rows
    //    of one length, so it is live or not as a whole and the live positions keep their order.
    let mut live: Vec<usize> = (0..m).filter(|&q| len_of(index[q]) > 0).collect();
    let mut pos = 0;
    let (mut elem, mut labels_j, mut rows, mut old_perm) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    while !live.is_empty() {
        // no two live positions tied: the remaining elements cannot move a row or split a class.
        if !live.windows(2).any(|w| labels[w[0]] == labels[w[1]]) {
            break;
        }
        elem.clear();
        elem.extend(live.iter().map(|&q| bounds.span(index[q]).0 + pos));
        labels_j.clear();
        labels_j.extend(live.iter().map(|&q| labels[q]));
        let (step, _) = sort_indexed(vals, &mut labels_j, &mut elem, false, scratch);
        rows.clear();
        rows.extend(live.iter().map(|&q| index[q]));
        old_perm.clear();
        old_perm.extend(live.iter().map(|&q| perm[q]));
        for (i, &q) in live.iter().enumerate() {
            index[q] = rows[step[i]];
            perm[q] = old_perm[step[i]];
            labels[q] = labels_j[i];
        }
        pos += 1;
        live.retain(|&q| len_of(index[q]) > pos);
    }
    // 3. each length class numbered its classes over its own last refinement; (length class,
    //    that numbering) is monotone over the output.
    densify(&after_len, labels);
    // 4. the data: the rows' elements in their final order, the one gather this sort makes.
    let out = emit.then(|| {
        let mut elems = Vec::new();
        let mut ends = Vec::with_capacity(m);
        for &r in index.iter() {
            let (s, e) = bounds.span(r);
            elems.extend(s..e);
            ends.push(elems.len());
        }
        Value::List(ends.into(), Box::new(gather(vals, &elems)))
    });
    (perm, out)
}

// ---- the kernel -----------------------------------------------------------------------------

/// Sort positions by `keys` within the runs of equal `labels`, stably. `keys` comes back in
/// sorted order, `labels` refined in place, and the permutation is returned.
fn sort_keys(keys: &mut [u64], labels: &mut [u64], scratch: &mut SortScratch) -> Vec<usize> {
    let m = keys.len();
    let mut perm: Vec<usize> = (0..m).collect();
    let mut lo = 0;
    while lo < m {
        let mut hi = lo + 1;
        while hi < m && labels[hi] == labels[lo] {
            hi += 1;
        }
        if hi - lo > 1 {
            sort_block(&mut keys[lo..hi], &mut perm[lo..hi], scratch);
        }
        lo = hi;
    }
    refine(labels, |q| keys[q] != keys[q - 1]);
    perm
}

/// Stable LSD radix of one block, the keys and the positions travelling together, every pass
/// sequential in and out. A block of 32 or fewer takes an insertion sort, which is what a
/// refinement pass hands this most often. The digit widens with the block, so a wide digit's
/// counter array is only paid where it halves the passes; high all-zero digits are skipped.
fn sort_block(keys: &mut [u64], perm: &mut [usize], scratch: &mut SortScratch) {
    let n = keys.len();
    if n <= 32 {
        for k in 1..n {
            let mut j = k;
            while j > 0 && keys[j - 1] > keys[j] {
                keys.swap(j - 1, j);
                perm.swap(j - 1, j);
                j -= 1;
            }
        }
        return;
    }
    // more rows than a `u32` counter can hold: a stable comparison sort is the same answer, and a
    // block this big can afford it.
    if n > COUNTED_MAX {
        let mut pairs: Vec<(u64, usize)> = keys.iter().copied().zip(perm.iter().copied()).collect();
        pairs.sort_by_key(|p| p.0);
        for (i, (k, q)) in pairs.into_iter().enumerate() {
            keys[i] = k;
            perm[i] = q;
        }
        return;
    }
    let max = keys.iter().copied().max().unwrap_or(0);
    let sig = 64 - max.leading_zeros();
    if sig == 0 {
        return; // every key equal: the identity is already the stable answer
    }
    let d = digit_width(n);
    let buckets = 1usize << d;
    let mask = (buckets - 1) as u64;
    let passes = sig.div_ceil(d);
    let SortScratch { keys_alt, perm_alt, counts, .. } = scratch;
    if keys_alt.len() < n {
        keys_alt.resize(n, 0);
    }
    if perm_alt.len() < n {
        perm_alt.resize(n, 0);
    }
    if counts.len() < buckets {
        counts.resize(buckets, 0);
    }
    let counts = &mut counts[..buckets];
    let (keys_alt, perm_alt) = (&mut keys_alt[..n], &mut perm_alt[..n]);
    let mut primary = true;
    for p in 0..passes {
        let shift = p * d;
        counts.iter_mut().for_each(|c| *c = 0);
        {
            let src: &[u64] = if primary { keys } else { keys_alt };
            for &k in src {
                counts[((k >> shift) & mask) as usize] += 1;
            }
        }
        let mut start = 0u32;
        for c in counts.iter_mut() {
            let cnt = *c;
            *c = start;
            start += cnt;
        }
        if primary {
            for j in 0..n {
                let (k, q) = (keys[j], perm[j]);
                let b = ((k >> shift) & mask) as usize;
                let slot = counts[b] as usize;
                counts[b] += 1;
                keys_alt[slot] = k;
                perm_alt[slot] = q;
            }
        } else {
            for j in 0..n {
                let (k, q) = (keys_alt[j], perm_alt[j]);
                let b = ((k >> shift) & mask) as usize;
                let slot = counts[b] as usize;
                counts[b] += 1;
                keys[slot] = k;
                perm[slot] = q;
            }
        }
        primary = !primary;
    }
    if !primary {
        keys.copy_from_slice(keys_alt);
        perm.copy_from_slice(perm_alt);
    }
}

/// Above this many rows in one block the `u32` bucket counters would overflow. They are `u32`
/// and not `usize` because at small blocks the counter array shares L1 with the data.
const COUNTED_MAX: usize = u32::MAX as usize;

/// Digit width for a radix pass over `n` rows: buckets stay at or under `n / 16`, so the counter
/// clear and prefix scan stay under a sixteenth of the element work.
fn digit_width(n: usize) -> u32 {
    if n >= (1 << 20) {
        16
    } else if n >= (1 << 15) {
        11
    } else {
        8
    }
}

/// Rewrite `labels` in place as the dense run index of the refined partition, in the order the
/// positions now stand: a run starts where the old label changes, or where `split(q)` says the
/// rows at `q - 1` and `q` differ.
fn refine(labels: &mut [u64], split: impl Fn(usize) -> bool) {
    let mut next = 0u64;
    let mut prev_old = labels.first().copied().unwrap_or(0);
    for (q, label) in labels.iter_mut().enumerate() {
        let old = *label;
        if q > 0 && (old != prev_old || split(q)) {
            next += 1;
        }
        prev_old = old;
        *label = next;
    }
}

/// Rewrite `labels` in place as the dense run index of the pairs `(coarse[q], labels[q])`, which
/// the arms build so that a coarser partition's classes are each numbered locally.
fn densify(coarse: &[u64], labels: &mut [u64]) {
    let mut next = 0u64;
    let mut prev = (coarse[0], labels[0]);
    for (q, label) in labels.iter_mut().enumerate() {
        let cur = (coarse[q], *label);
        if q > 0 && cur != prev {
            next += 1;
        }
        prev = cur;
        *label = next;
    }
}

/// `xs[k] = old xs[perm[k]]`, through a spare buffer.
fn permute(xs: &mut [usize], perm: &[usize], tmp: &mut Vec<usize>) {
    tmp.clear();
    tmp.extend(perm.iter().map(|&q| xs[q]));
    xs.copy_from_slice(tmp);
}

/// Is every position in a class of its own? Labels are non-decreasing, so this is one scan that
/// stops at the first tie.
fn fully_discriminated(labels: &[u64]) -> bool {
    labels.windows(2).all(|w| w[0] != w[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::cmp::order::compare_at;
    use std::cmp::Ordering;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn leaf(rng: &mut Rng, rows: usize) -> Value {
        match rng.below(4) {
            0 => Value::u8((0..rows).map(|_| rng.below(6) as u8).collect()),
            1 => Value::u16((0..rows).map(|_| rng.below(1000) as u16).collect()),
            2 => Value::u32((0..rows).map(|_| rng.below(3) as u32 * 70000).collect()),
            _ => Value::u64((0..rows).map(|_| if rng.below(2) == 0 { rng.below(5) as u64 } else { rng.next() }).collect()),
        }
    }

    /// a random column of `rows` rows, nesting up to `depth` levels below the top.
    fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
        if depth == 0 {
            return leaf(rng, rows);
        }
        match rng.below(6) {
            0 => leaf(rng, rows),
            1 => Value::Prod((0..1 + rng.below(3)).map(|_| random_value(rng, rows, depth - 1)).collect()),
            2 => {
                let arity = 1 + rng.below(3);
                let tags: Vec<usize> = (0..rows).map(|_| rng.below(arity)).collect();
                let lanes = (0..arity)
                    .map(|t| random_value(rng, tags.iter().filter(|&&x| x == t).count(), depth - 1))
                    .collect();
                Value::sum(tags, lanes)
            }
            3 => {
                let mut ends = Vec::with_capacity(rows);
                let mut total = 0;
                for _ in 0..rows {
                    total += rng.below(4);
                    ends.push(total);
                }
                Value::List(ends.into(), Box::new(random_value(rng, total, depth - 1)))
            }
            4 => {
                let k = 1 + rng.below(3);
                Value::List(Bounds::Stride(k, rows), Box::new(random_value(rng, rows * k, depth - 1)))
            }
            _ => Value::Unit(rows),
        }
    }

    /// the scalar reference: positions sorted stably by (label, structural order of the row),
    /// then the dense run index of (label, row equality).
    fn reference(v: &Value, labels: &[u64], index: &[usize]) -> (Vec<usize>, Vec<usize>, Vec<u64>) {
        let mut perm: Vec<usize> = (0..index.len()).collect();
        perm.sort_by(|&a, &b| labels[a].cmp(&labels[b]).then_with(|| compare_at(v, index[a], v, index[b])));
        let rows: Vec<usize> = perm.iter().map(|&q| index[q]).collect();
        let mut out = Vec::with_capacity(rows.len());
        let mut next = 0u64;
        for k in 0..rows.len() {
            if k > 0
                && (labels[perm[k]] != labels[perm[k - 1]]
                    || compare_at(v, rows[k], v, rows[k - 1]) != Ordering::Equal)
            {
                next += 1;
            }
            out.push(next);
        }
        (perm, rows, out)
    }

    /// the kernel, with and without emitting, against the reference: same rows, same permutation
    /// (stability makes it unique), same labels, and the emitted column is the rows gathered.
    fn check(v: &Value, labels: &[u64], index: &[usize]) {
        let (perm_ref, rows_ref, labels_ref) = reference(v, labels, index);
        for emit in [false, true] {
            let (mut l, mut i) = (labels.to_vec(), index.to_vec());
            let mut scratch = SortScratch::default();
            let (perm, out) = sort_indexed(v, &mut l, &mut i, emit, &mut scratch);
            assert_eq!(i, rows_ref, "rows\n{}", crate::value::show(v));
            assert_eq!(perm, perm_ref, "perm\n{}", crate::value::show(v));
            assert_eq!(l, labels_ref, "labels\n{}", crate::value::show(v));
            match out {
                Some(o) => assert_eq!(o, gather(v, &i), "values\n{}", crate::value::show(v)),
                None => assert!(!emit),
            }
        }
    }

    /// dense non-decreasing labels over `n` positions: one block, blocks of random size, or a
    /// block per position.
    fn label_patterns(rng: &mut Rng, n: usize) -> Vec<Vec<u64>> {
        let mut blocks = Vec::with_capacity(n);
        let mut b = 0u64;
        for _ in 0..n {
            if rng.below(3) == 0 {
                b += 1;
            }
            blocks.push(b);
        }
        vec![vec![0; n], blocks, (0..n as u64).collect()]
    }

    #[test]
    fn random_shapes_agree_with_the_scalar_order() {
        for seed in 1..80u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let rows = 1 + rng.below(40);
            let v = random_value(&mut rng, rows, 3);
            let index: Vec<usize> = (0..rows).collect();
            for labels in label_patterns(&mut rng, rows) {
                check(&v, &labels, &index);
            }
        }
    }

    #[test]
    fn subsets_in_any_order_sort_without_a_gather() {
        for seed in 1..80u64 {
            let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1);
            let rows = 2 + rng.below(40);
            let v = random_value(&mut rng, rows, 3);
            // a subset of the rows, in scrambled order, each row at most once
            let mut index: Vec<usize> = (0..rows).filter(|_| rng.below(3) != 0).collect();
            for k in (1..index.len()).rev() {
                index.swap(k, rng.below(k + 1));
            }
            for labels in label_patterns(&mut rng, index.len()) {
                check(&v, &labels, &index);
            }
        }
    }

    #[test]
    fn emitted_columns_are_the_sorted_data_at_scale() {
        let n = 5000usize;
        let mut rng = Rng(7);
        let full: Vec<u64> = (0..n).map(|_| rng.next()).collect();
        let narrow: Vec<u64> = (0..n).map(|_| rng.below(50) as u64).collect();
        let index: Vec<usize> = (0..n).collect();
        let zeros = vec![0u64; n];
        check(&Value::u64(full.clone()), &zeros, &index);
        check(&Value::u64(narrow.clone()), &zeros, &index);
        check(&Value::Prod(vec![Value::u64(narrow.clone()), Value::u64(full.clone())]), &zeros, &index);
        let tags: Vec<usize> = (0..n).map(|_| rng.below(3)).collect();
        let lanes: Vec<Value> = (0..3)
            .map(|t| Value::u64(narrow.iter().zip(&tags).filter(|(_, &x)| x == t).map(|(&v, _)| v).collect()))
            .collect();
        check(&Value::sum(tags, lanes), &zeros, &index);
        let mut ends = Vec::with_capacity(n);
        let mut total = 0;
        for _ in 0..n {
            total += rng.below(4);
            ends.push(total);
        }
        let elems: Vec<u64> = (0..total).map(|_| rng.below(4) as u64).collect();
        check(&Value::List(ends.into(), Box::new(Value::u64(elems))), &zeros, &index);
    }

    #[test]
    fn byte_records_pack_and_unpack() {
        let mut rng = Rng(11);
        for k in 1..=9usize {
            let rows = 300;
            let bytes: Vec<u8> = (0..rows * k).map(|_| rng.below(3) as u8).collect();
            let v = Value::List(Bounds::Stride(k, rows), Box::new(Value::u8(bytes)));
            let index: Vec<usize> = (0..rows).collect();
            for labels in label_patterns(&mut rng, rows) {
                check(&v, &labels, &index);
            }
        }
    }

    #[test]
    fn the_labels_form_is_the_identity_index() {
        let v = Value::Prod(vec![Value::u64(vec![2, 1, 2, 1, 3, 1]), Value::u64(vec![10, 20, 5, 30, 7, 20])]);
        let labels = [0, 0, 0, 1, 1, 1];
        let (perm, refined) = sort_blocks(&labels, &v);
        let (_, rows, labels_ref) = reference(&v, &labels, &[0, 1, 2, 3, 4, 5]);
        assert_eq!(perm, rows);
        assert_eq!(refined, labels_ref);
        let (perm2, refined2, sorted) = sort_values(&labels, &v);
        assert_eq!((perm2, refined2), (perm, refined));
        assert_eq!(sorted, gather(&v, &rows));
    }
}
