//! The discrimination sort: `(labels, index)` in, sorted data out.
//!
//! [`sort_indexed`] orders the rows `index[..]` of a column within the blocks `labels` describes,
//! by structural order: a leaf by its stored unsigned bytes, `Prod` lexicographically by field,
//! `Sum` by tag then payload, `List` length first and then element by element, `Unit` all equal.
//! Nothing is gathered before a level sorts. A leaf pulls its keys through the index once and
//! radixes them with the positions alongside, every pass sequential, and the sorted keys are the
//! output column. Layout, top down: the entry points, the four arms, the leaf kernel, the label
//! and position helpers.

use crate::engine::gather;
use crate::value::{Bounds, Prim, Tags, Value};

/// Buffers reused across every block and level of one call, so that a refinement pass producing
/// millions of tiny blocks allocates nothing per block.
#[derive(Default)]
pub(crate) struct SortScratch {
    keys: Vec<u64>,      // the level's pulled keys, taken while in use
    keys_alt: Vec<u64>,  // the radix's alternate key buffer
    perm_alt: Vec<usize>, // the radix's alternate position buffer
    index_alt: Vec<usize>, // a spare index for applying a permutation
    counts: Vec<u32>,    // digit counters
    rows: Vec<usize>,    // a sub-call's rows, while its permutation is applied
    old: Vec<usize>,     // and the positions they came from
}

/// What a sort hands back besides the refined labels.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Emit {
    /// the permuted index and the permutation
    Index,
    /// those, and the sorted rows as a column
    Values,
    /// the sorted rows only: `index` and the returned permutation are unspecified afterwards,
    /// which lets a leaf sort its keys without carrying positions
    ValuesOnly,
}

impl Emit {
    fn values(self) -> bool {
        self != Emit::Index
    }
    /// the mode for a level whose index a later level still reads
    fn keeping_index(self) -> Emit {
        if self == Emit::ValuesOnly { Emit::Values } else { self }
    }
}

// ---- entry points ---------------------------------------------------------------------------

/// Sort the rows `index[..]` of `v` within the blocks of `labels`.
///
/// Requires: `labels.len() == index.len()`; `labels` non-decreasing, position `k` being row
/// `index[k]` in block `labels[k]` (checked in debug builds); every `index[k]` a row of `v`.
///
/// Ensures: `index` is permuted so that each block holds its rows in structural order, blocks
/// keep their places and equal rows keep their order; `labels` is rewritten as the dense run
/// index of the refined partition in that order, two positions sharing a label iff they did
/// before and their rows are structurally equal; the returned `perm` has
/// `new_index[k] == old_index[perm[k]]`; with `Emit::Values` the returned column is
/// `gather(v, &new_index)`, produced by the sort rather than by a gather. Under
/// `Emit::ValuesOnly` the column and the labels are as above and `index` and `perm` are
/// unspecified.
pub(crate) fn sort_indexed(
    v: &Value,
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    debug_assert_eq!(labels.len(), index.len(), "sort_indexed: one label per position");
    debug_assert!(labels.windows(2).all(|w| w[0] <= w[1]), "sort_indexed: labels must be non-decreasing");
    let m = index.len();
    if m <= 1 {
        if m == 1 {
            labels[0] = 0;
        }
        return ((0..m).collect(), emit.values().then(|| gather(v, index)));
    }
    // nothing tied: no row can move and no class can split, at any depth.
    if fully_discriminated(labels) {
        refine(labels, |_| false);
        return ((0..m).collect(), emit.values().then(|| gather(v, index)));
    }
    match v {
        Value::Prim(p) => sort_leaf(p, labels, index, emit, scratch),
        Value::Prod(cols) => sort_prod(cols, labels, index, emit, scratch),
        // a sum's lanes and a list's elements are read through the index after their sorts.
        Value::Sum(tags, lanes) => sort_sum(tags, lanes, labels, index, emit.keeping_index(), scratch),
        Value::List(bounds, vals) => sort_list(bounds, vals, labels, index, emit.keeping_index(), scratch),
        Value::Unit(_) => {
            refine(labels, |_| false);
            ((0..m).collect(), emit.values().then_some(Value::Unit(m)))
        }
    }
}

/// The labels form: every row of `v`, in stored order. Requires `labels` non-decreasing over
/// `v`'s rows. Returns `(perm, labels)`: `perm[k]` is the input row at output position `k`, and
/// the refined labels are aligned with `perm`.
pub(crate) fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let (perm, _) = sort_indexed(v, &mut labels, &mut index, Emit::Index, &mut scratch);
    debug_assert_eq!(perm, index, "from the identity index the permutation is the index");
    (index, labels)
}

/// [`sort_blocks`] and the sorted rows: `(perm, labels, sorted)` with `sorted == gather(v, &perm)`.
pub(crate) fn sort_values(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>, Value) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let (_, out) = sort_indexed(v, &mut labels, &mut index, Emit::Values, &mut scratch);
    (index, labels, out.expect("emit was requested"))
}

/// The sorted rows and the refined labels, and nothing else: a leaf sorts its keys without
/// carrying positions.
pub(crate) fn sort_values_only(labels: &[u64], v: &Value) -> (Vec<u64>, Value) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let (_, out) = sort_indexed(v, &mut labels, &mut index, Emit::ValuesOnly, &mut scratch);
    (labels, out.expect("emit was requested"))
}

// ---- the arms -------------------------------------------------------------------------------

/// A leaf: one indirect read per position, then sequential passes; the sorted keys, narrowed
/// back to the leaf's width, are the column.
fn sort_leaf(
    p: &Prim,
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    p.pull_u64(index, &mut keys);
    let perm = if emit == Emit::ValuesOnly {
        sort_keys_only(&mut keys, labels, scratch);
        Vec::new()
    } else {
        sort_keys(&mut keys, labels, index, scratch)
    };
    let out = emit.values().then(|| Value::Prim(p.like(&keys)));
    scratch.keys = keys;
    (perm, out)
}

/// A product: its fields in turn, at the same positions, under the labels the fields before
/// refined. Consecutive leaf fields whose declared widths fit one `u64` sort as one packed key
/// ([`sort_packed`]). A field's output is final when emitted, since later fields permute only
/// within its classes, on which it is constant; once nothing is tied the remaining fields are
/// read out by the index.
fn sort_prod(
    cols: &[Value],
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let m = index.len();
    if cols.is_empty() {
        refine(labels, |_| false);
        return ((0..m).collect(), emit.values().then_some(Value::Prod(Vec::new())));
    }
    let mut perm: Option<Vec<usize>> = None; // the first segment's step is the running permutation
    let mut outs: Vec<Value> = Vec::with_capacity(cols.len());
    let mut settled = false;
    let mut f = 0;
    while f < cols.len() {
        if settled {
            if emit.values() {
                outs.push(gather(&cols[f], index));
            }
            f += 1;
            continue;
        }
        let (step, out, next) = match &cols[f] {
            Value::Prim(_) => sort_packed(cols, f, labels, index, emit, scratch),
            c => {
                // only the last segment may leave the index behind
                let mode = if f + 1 == cols.len() { emit } else { emit.keeping_index() };
                let (step, out) = sort_indexed(c, labels, index, mode, scratch);
                (step, out.into_iter().collect(), f + 1)
            }
        };
        if emit != Emit::ValuesOnly {
            perm = Some(match perm {
                None => step,
                Some(mut running) => {
                    permute(&mut running, &step, &mut scratch.index_alt);
                    running
                }
            });
        }
        outs.extend(out);
        settled = fully_discriminated(labels);
        f = next;
    }
    (perm.unwrap_or_default(), emit.values().then_some(Value::Prod(outs)))
}

/// The leaf fields of `cols` from `f` on, as many as fit one `u64` by their declared widths,
/// sorted as one key, most significant field first: one set of passes and one refinement for
/// the run. Returns the permutation, one column per field with `emit`, and the index of the
/// first field not taken.
fn sort_packed(
    cols: &[Value],
    f: usize,
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Vec<Value>, usize) {
    let leaf = |c: &Value| match c {
        Value::Prim(p) => p.clone(),
        _ => unreachable!("sort_packed: a leaf field"),
    };
    let mut g = f;
    let mut used = 0u32;
    while g < cols.len() {
        let Value::Prim(p) = &cols[g] else { break };
        if used + p.bits() > 64 {
            break;
        }
        used += p.bits();
        g += 1;
    }
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    leaf(&cols[f]).pull_u64(index, &mut keys);
    for c in &cols[f + 1..g] {
        leaf(c).pack_u64(index, &mut keys);
    }
    let perm = if emit == Emit::ValuesOnly && g == cols.len() {
        sort_keys_only(&mut keys, labels, scratch);
        Vec::new()
    } else {
        sort_keys(&mut keys, labels, index, scratch)
    };
    let mut outs = Vec::new();
    if emit.values() {
        let mut shift = used;
        for c in &cols[f..g] {
            let p = leaf(c);
            shift -= p.bits();
            let mask = if p.bits() == 64 { u64::MAX } else { (1u64 << p.bits()) - 1 };
            outs.push(Value::Prim(p.like_from(keys.iter().map(|&k| (k >> shift) & mask))));
        }
    }
    scratch.keys = keys;
    (perm, outs, g)
}

/// A sum: the tag as a virtual leaf, then each lane at the positions that carry its tag and are
/// still tied, through the carried within-lane offsets. While the lanes refine subsets, a label
/// is its run's starting position (see [`run_starts`]); dense afterwards. A lane whose rows were
/// all sorted is emitted by the sort; otherwise it is read once at its final offsets.
fn sort_sum(
    tags: &Tags,
    lanes: &[Value],
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
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
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    keys.extend(index.iter().map(|&r| tag_col.usize_at(r) as u64));
    let mut perm = sort_keys(&mut keys, labels, index, scratch);
    let tags_out: Vec<usize> = keys.iter().map(|&k| k as usize).collect();
    scratch.keys = keys;
    run_starts(labels);
    let mut all: Vec<Vec<usize>> = vec![Vec::new(); lanes.len()];
    let mut tied: Vec<Vec<usize>> = vec![Vec::new(); lanes.len()];
    for (q, &t) in tags_out.iter().enumerate() {
        all[t].push(q);
        if in_tie(labels, q) {
            tied[t].push(q);
        }
    }
    let mut lanes_out = Vec::with_capacity(lanes.len());
    for (t, lane) in lanes.iter().enumerate() {
        let qs = &tied[t];
        let mut sorted = None;
        if !qs.is_empty() {
            let mut index_t: Vec<usize> = qs.iter().map(|&q| within[index[q]]).collect();
            let mut labels_t: Vec<u64> = qs.iter().map(|&q| labels[q]).collect();
            let (step, out) = sort_indexed(lane, &mut labels_t, &mut index_t, emit, scratch);
            apply(index, &mut perm, qs, &step, scratch);
            write_starts(labels, qs, &labels_t);
            sorted = out;
        }
        if emit.values() {
            lanes_out.push(match sorted {
                Some(o) if qs.len() == all[t].len() => o,
                _ => gather(lane, &all[t].iter().map(|&q| within[index[q]]).collect::<Vec<_>>()),
            });
        }
    }
    refine(labels, |_| false);
    (perm, emit.values().then(|| Value::sum_tagged(Tags::from_tags(tags_out, lanes.len()), lanes_out)))
}

/// A list: the length as a virtual leaf, then one refining pass per element position over the
/// rows still tied and still that long, then one gather of the elements in final order, the
/// only gather this sort makes. Equal-width byte records up to 8 wide pack into one `u64` key
/// and unpack from the sorted keys.
fn sort_list(
    bounds: &Bounds,
    vals: &Value,
    labels: &mut [u64],
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<usize>, Option<Value>) {
    let m = index.len();
    if let (Some(k), Value::Prim(Prim::U8(bytes))) = (bounds.strided(), vals) {
        if (1..=8).contains(&k) {
            let mut keys = std::mem::take(&mut scratch.keys);
            keys.clear();
            keys.extend(index.iter().map(|&r| (0..k).fold(0u64, |key, p| (key << 8) | bytes[r * k + p] as u64)));
            let perm = sort_keys(&mut keys, labels, index, scratch);
            let out = emit.values().then(|| {
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
    let mut perm: Vec<usize> = (0..m).collect();
    if bounds.strided().is_none() {
        let mut keys = std::mem::take(&mut scratch.keys);
        keys.clear();
        keys.extend(index.iter().map(|&r| len_of(r) as u64));
        perm = sort_keys(&mut keys, labels, index, scratch);
        scratch.keys = keys;
    }
    run_starts(labels);
    // a block holds rows of one length, so it is live or not as a whole.
    let mut live: Vec<usize> = (0..m).filter(|&q| len_of(index[q]) > 0 && in_tie(labels, q)).collect();
    let mut pos = 0;
    let (mut elem, mut labels_j) = (Vec::new(), Vec::new());
    while !live.is_empty() {
        elem.clear();
        elem.extend(live.iter().map(|&q| bounds.span(index[q]).0 + pos));
        labels_j.clear();
        labels_j.extend(live.iter().map(|&q| labels[q]));
        let (step, _) = sort_indexed(vals, &mut labels_j, &mut elem, Emit::Index, scratch);
        apply(index, &mut perm, &live, &step, scratch);
        write_starts(labels, &live, &labels_j);
        pos += 1;
        live.retain(|&q| len_of(index[q]) > pos && in_tie(labels, q));
    }
    refine(labels, |_| false);
    let out = emit.values().then(|| {
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

// ---- the leaf kernel ------------------------------------------------------------------------

/// Sort positions by `keys` within the runs of equal `labels`, stably, and apply the result.
///
/// Requires `keys`, `labels` and `index` of one length, `labels` non-decreasing. Ensures `keys`
/// sorted within each run, `labels` the dense run index of the refined partition, `index`
/// permuted alike; returns the permutation.
fn sort_keys(keys: &mut [u64], labels: &mut [u64], index: &mut [usize], scratch: &mut SortScratch) -> Vec<usize> {
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
    permute(index, &perm, &mut scratch.index_alt);
    perm
}

/// [`sort_keys`] without positions: `keys` sorted within each run and `labels` refined, for a
/// caller that will not read the index again.
fn sort_keys_only(keys: &mut [u64], labels: &mut [u64], scratch: &mut SortScratch) {
    let m = keys.len();
    let mut lo = 0;
    while lo < m {
        let mut hi = lo + 1;
        while hi < m && labels[hi] == labels[lo] {
            hi += 1;
        }
        if hi - lo > 1 {
            sort_block_impl::<false>(&mut keys[lo..hi], &mut [], scratch);
        }
        lo = hi;
    }
    refine(labels, |q| keys[q] != keys[q - 1]);
}

/// Stable sort of one block by `keys`, `perm` moving with them. Blocks of 32 or fewer take an
/// insertion sort; the rest an LSD radix with every pass sequential, a digit widening with the
/// block ([`digit_width`]). One sweep counts every digit at once; a digit on which every key
/// agrees is skipped, as are the leading all-zero digits.
fn sort_block(keys: &mut [u64], perm: &mut [usize], scratch: &mut SortScratch) {
    sort_block_impl::<true>(keys, perm, scratch)
}

/// [`sort_block`], with (`PERM`) or without the positions travelling alongside.
fn sort_block_impl<const PERM: bool>(keys: &mut [u64], perm: &mut [usize], scratch: &mut SortScratch) {
    let n = keys.len();
    if n <= 32 {
        for k in 1..n {
            let mut j = k;
            while j > 0 && keys[j - 1] > keys[j] {
                keys.swap(j - 1, j);
                if PERM {
                    perm.swap(j - 1, j);
                }
                j -= 1;
            }
        }
        return;
    }
    if n > COUNTED_MAX {
        if PERM {
            let mut pairs: Vec<(u64, usize)> = keys.iter().copied().zip(perm.iter().copied()).collect();
            pairs.sort_by_key(|p| p.0);
            for (i, (k, q)) in pairs.into_iter().enumerate() {
                keys[i] = k;
                perm[i] = q;
            }
        } else {
            keys.sort_unstable(); // equal keys are indistinguishable
        }
        return;
    }
    let max = keys.iter().copied().max().unwrap_or(0);
    let sig = 64 - max.leading_zeros();
    if sig == 0 {
        return;
    }
    let d = digit_width(n);
    let buckets = 1usize << d;
    let mask = (buckets - 1) as u64;
    let passes = sig.div_ceil(d) as usize;
    let SortScratch { keys_alt, perm_alt, counts, .. } = scratch;
    keys_alt.resize(keys_alt.len().max(n), 0);
    if PERM {
        perm_alt.resize(perm_alt.len().max(n), 0);
    }
    counts.resize(counts.len().max(passes * buckets), 0);
    let counts = &mut counts[..passes * buckets];
    counts.iter_mut().for_each(|c| *c = 0);
    for &k in keys.iter() {
        for p in 0..passes {
            counts[p * buckets + ((k >> (p as u32 * d)) & mask) as usize] += 1;
        }
    }
    let keys_alt = &mut keys_alt[..n];
    let perm_alt = if PERM { &mut perm_alt[..n] } else { &mut perm_alt[..0] };
    let mut primary = true;
    for p in 0..passes {
        let counts = &mut counts[p * buckets..(p + 1) * buckets];
        if counts.iter().any(|&c| c as usize == n) {
            continue; // every key agrees on this digit
        }
        let shift = p as u32 * d;
        let mut start = 0u32;
        for c in counts.iter_mut() {
            let cnt = *c;
            *c = start;
            start += cnt;
        }
        if primary {
            for j in 0..n {
                let k = keys[j];
                let b = ((k >> shift) & mask) as usize;
                let slot = counts[b] as usize;
                counts[b] += 1;
                keys_alt[slot] = k;
                if PERM {
                    perm_alt[slot] = perm[j];
                }
            }
        } else {
            for j in 0..n {
                let k = keys_alt[j];
                let b = ((k >> shift) & mask) as usize;
                let slot = counts[b] as usize;
                counts[b] += 1;
                keys[slot] = k;
                if PERM {
                    perm[slot] = perm_alt[j];
                }
            }
        }
        primary = !primary;
    }
    if !primary {
        keys.copy_from_slice(keys_alt);
        if PERM {
            perm.copy_from_slice(perm_alt);
        }
    }
}

/// Above this many rows in one block the `u32` digit counters would overflow; such a block takes
/// a stable comparison sort instead. `u32`, not `usize`, so the counters share L1 with a small block.
const COUNTED_MAX: usize = u32::MAX as usize;

/// Digit width for a radix pass over `n` rows: at most `n / 16` buckets, so the counter clear and
/// prefix scan stay under a sixteenth of the element work.
fn digit_width(n: usize) -> u32 {
    if n >= (1 << 20) {
        16
    } else if n >= (1 << 15) {
        11
    } else {
        8
    }
}

// ---- labels and positions -------------------------------------------------------------------

/// Renumber `labels` in place as the dense run index, a run ending where the label changes or
/// where `split(q)` says positions `q - 1` and `q` differ.
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

/// Relabel each run by the position it starts at. Unique per class over the whole problem, so
/// a class one sub-call splits can never be confused with one another sub-call, or none, left
/// alone; the `Sum` and `List` arms keep this form while sub-calls refine subsets.
fn run_starts(labels: &mut [u64]) {
    let mut start = 0u64;
    let mut prev = labels.first().copied().unwrap_or(0);
    for (q, label) in labels.iter_mut().enumerate() {
        if *label != prev {
            start = q as u64;
        }
        prev = *label;
        *label = start;
    }
}

/// Write a sub-call's `refined` labels for the positions `at` back as run starts. Requires the
/// members of each refined class to be consecutive positions.
fn write_starts(labels: &mut [u64], at: &[usize], refined: &[u64]) {
    let mut start = 0u64;
    for (i, &q) in at.iter().enumerate() {
        if i == 0 || refined[i] != refined[i - 1] {
            start = q as u64;
        }
        labels[q] = start;
    }
}

/// `xs[k] = old xs[perm[k]]`, through a spare buffer.
fn permute(xs: &mut [usize], perm: &[usize], tmp: &mut Vec<usize>) {
    tmp.clear();
    tmp.extend(perm.iter().map(|&q| xs[q]));
    xs.copy_from_slice(tmp);
}

/// Apply a sub-call's permutation `step` over the positions `at` to `index` and `perm`:
/// position `at[i]` takes what position `at[step[i]]` held.
fn apply(index: &mut [usize], perm: &mut [usize], at: &[usize], step: &[usize], scratch: &mut SortScratch) {
    let SortScratch { rows, old, .. } = scratch;
    rows.clear();
    rows.extend(at.iter().map(|&q| index[q]));
    old.clear();
    old.extend(at.iter().map(|&q| perm[q]));
    for (i, &q) in at.iter().enumerate() {
        index[q] = rows[step[i]];
        perm[q] = old[step[i]];
    }
}

/// No two positions share a label. One scan, stopping at the first tie.
fn fully_discriminated(labels: &[u64]) -> bool {
    labels.windows(2).all(|w| w[0] != w[1])
}

/// Position `q` shares its label with a neighbour, i.e. sits in a block of more than one row.
fn in_tie(labels: &[u64], q: usize) -> bool {
    (q > 0 && labels[q - 1] == labels[q]) || (q + 1 < labels.len() && labels[q + 1] == labels[q])
}

/// Whether the shape holds a `List` anywhere; a `List`'s sorted form is a gather of every
/// element, which a caller keeping only some rows would rather do itself.
pub(crate) fn contains_list(v: &Value) -> bool {
    match v {
        Value::List(..) => true,
        Value::Prod(cols) => cols.iter().any(contains_list),
        Value::Sum(_, lanes) => lanes.iter().any(contains_list),
        Value::Prim(_) | Value::Unit(_) => false,
    }
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
        for emit in [Emit::Index, Emit::Values] {
            let (mut l, mut i) = (labels.to_vec(), index.to_vec());
            let mut scratch = SortScratch::default();
            let (perm, out) = sort_indexed(v, &mut l, &mut i, emit, &mut scratch);
            assert_eq!(i, rows_ref, "rows\n{}", crate::value::show(v));
            assert_eq!(perm, perm_ref, "perm\n{}", crate::value::show(v));
            assert_eq!(l, labels_ref, "labels\n{}", crate::value::show(v));
            match out {
                Some(o) => assert_eq!(o, gather(v, &i), "values\n{}", crate::value::show(v)),
                None => assert!(emit == Emit::Index),
            }
        }
        // values only: the column and the labels, nothing promised of the index
        let (mut l, mut i) = (labels.to_vec(), index.to_vec());
        let mut scratch = SortScratch::default();
        let (_, out) = sort_indexed(v, &mut l, &mut i, Emit::ValuesOnly, &mut scratch);
        assert_eq!(out.unwrap(), gather(v, &rows_ref), "values only\n{}", crate::value::show(v));
        assert_eq!(l, labels_ref, "values-only labels\n{}", crate::value::show(v));
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
