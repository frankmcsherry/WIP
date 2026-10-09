//! The discrimination sort: `(labels, index)` in, sorted data out.
//!
//! [`sort_indexed`] orders the rows `index[..]` of a column within the blocks `labels` describes,
//! by structural order: a leaf by its stored unsigned bytes, `Prod` lexicographically by field,
//! `Sum` by tag then payload, `List` lexicographically (a proper prefix first), `Unit` all equal.
//! Nothing is gathered before a level sorts. A leaf pulls its keys through the index once and
//! radixes them with the index's rows alongside, every pass sequential, and the sorted keys are
//! the output column. The index is the only positional state: it is the rows in their current
//! order, a later level reads its data through it, and from an identity start it is the
//! permutation a caller wants. Layout, top down: the entry points, the four arms, the leaf
//! kernel, the label and position helpers.

use crate::engine::gather;
use crate::value::{Bounds, Prim, Tags, Value};

/// Buffers reused across every block and level of one call, so that a refinement pass producing
/// millions of tiny blocks allocates nothing per block.
#[derive(Default)]
pub(crate) struct SortScratch {
    keys: Vec<u64>,       // the level's pulled keys, taken while in use
    keys_alt: Vec<u64>,   // the radix's alternate key buffer
    rows_alt: Vec<usize>, // the radix's alternate row buffer
    counts: Vec<u32>,     // digit counters
    slot: Vec<usize>,     // a sum lane's or list's offset -> row map, taken while in use
}

/// What a sort hands back. The refined labels always come back; beyond them:
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Emit {
    /// the rows: `index` permuted into sorted order
    Index,
    /// the values: the sorted rows as a column, `index` left unspecified, which lets a leaf sort
    /// its keys without carrying rows
    Values,
    /// both
    Both,
}

impl Emit {
    fn values(self) -> bool {
        self != Emit::Index
    }
    /// the mode for a level whose index a later level still reads
    fn keeping_index(self) -> Emit {
        if self == Emit::Values { Emit::Both } else { self }
    }
}

// ---- entry points ---------------------------------------------------------------------------

/// Sort the rows `index[..]` of `v` within the blocks of `labels`.
///
/// Requires: `labels` empty, meaning every position is one block, or `labels.len() == index.len()`
/// and non-decreasing, position `k` being row `index[k]` in block `labels[k]` (checked in debug
/// builds); every `index[k]` a row of `v`.
///
/// Ensures: `labels` is rewritten as the dense run index of the refined partition, two positions
/// sharing a label iff they did before and their rows are structurally equal. With
/// `Emit::Index` or `Emit::Both`, `index` is permuted so that each block holds its rows in
/// structural order, blocks keep their places and equal rows keep their order; with
/// `Emit::Values` or `Emit::Both` the returned column is the rows in that order, produced by
/// the sort rather than by a gather. Under `Emit::Values` the index is unspecified.
pub(crate) fn sort_indexed(
    v: &Value,
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> Option<Value> {
    debug_assert!(labels.is_empty() || labels.len() == index.len(), "sort_indexed: one label per position");
    debug_assert!(labels.windows(2).all(|w| w[0] <= w[1]), "sort_indexed: labels must be non-decreasing");
    let m = index.len();
    if m <= 1 {
        labels.clear();
        labels.resize(m, 0);
        return emit.values().then(|| gather(v, index));
    }
    // nothing tied: no row can move and no class can split, at any depth.
    if !labels.is_empty() && fully_discriminated(labels) {
        refine(labels, |_| false);
        return emit.values().then(|| gather(v, index));
    }
    match v {
        Value::Prim(p) => sort_leaf(p, labels, index, emit, scratch),
        Value::Prod(cols) => sort_prod(cols, labels, index, emit, scratch),
        // a sum's lanes and a list's elements are read through the index after their sorts.
        Value::Sum(tags, lanes) => sort_sum(tags, lanes, labels, index, emit.keeping_index(), scratch),
        Value::List(bounds, vals) => sort_list(bounds, vals, labels, index, emit.keeping_index(), scratch),
        // a reference sorts as what it names, read through its arena: the arena's rows are sorted
        // in place of the references, and mapped back to positions; the sorted column is still
        // references. (An arena row named twice falls back to a scratch clone.)
        Value::Ref(list, rows) => {
            // references to every arena row in order (as `ref` makes them) name the arena itself
            if rows.len() == list.len() && rows.iter().enumerate().all(|(i, &r)| i == r) {
                sort_indexed(list, labels, index, Emit::Index, scratch);
                return emit.values().then(|| gather(v, index));
            }
            let mut at: Vec<usize> = index.iter().map(|&i| rows[i]).collect();
            let mut back = vec![usize::MAX; list.len()];
            let once = index.iter().zip(&at).all(|(&i, &r)| std::mem::replace(&mut back[r], i) == usize::MAX);
            if once {
                sort_indexed(list, labels, &mut at, Emit::Index, scratch);
                for (slot, &r) in index.iter_mut().zip(&at) {
                    *slot = back[r];
                }
            } else {
                let owned = crate::engine::clone_ref(v.clone());
                sort_indexed(&owned, labels, index, emit.keeping_index(), scratch);
            }
            emit.values().then(|| gather(v, index))
        }
        Value::Unit(_) => {
            densify(labels, m);
            emit.values().then_some(Value::Unit(m))
        }
    }
}

/// The labels form: every row of `v`, in stored order. Requires `labels` non-decreasing over
/// `v`'s rows, or empty for one block. Returns `(perm, labels)`: `perm[k]` is the input row at
/// output position `k` (the sorted identity index), and the refined labels are aligned with it.
pub(crate) fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    sort_indexed(v, &mut labels, &mut index, Emit::Index, &mut scratch);
    (index, labels)
}

/// [`sort_blocks`] and the sorted rows: `(perm, labels, sorted)` with `sorted == gather(v, &perm)`.
pub(crate) fn sort_values(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>, Value) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let out = sort_indexed(v, &mut labels, &mut index, Emit::Both, &mut scratch);
    (index, labels, out.expect("emit was requested"))
}

/// The sorted rows and the refined labels, and nothing else: a leaf sorts its keys without
/// carrying rows.
pub(crate) fn sort_values_only(labels: &[u64], v: &Value) -> (Vec<u64>, Value) {
    let mut labels = labels.to_vec();
    let mut index: Vec<usize> = (0..v.len()).collect();
    let mut scratch = SortScratch::default();
    let out = sort_indexed(v, &mut labels, &mut index, Emit::Values, &mut scratch);
    (labels, out.expect("emit was requested"))
}

// ---- the arms -------------------------------------------------------------------------------

/// A leaf: one indirect read per position, then sequential passes; the sorted keys, narrowed
/// back to the leaf's width, are the column.
fn sort_leaf(
    p: &Prim,
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> Option<Value> {
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    p.pull_u64(index, &mut keys);
    if emit == Emit::Values {
        sort_keys_only(&mut keys, labels, scratch);
    } else {
        sort_keys(&mut keys, labels, index, scratch);
    }
    let out = emit.values().then(|| Value::Prim(p.like(&keys)));
    scratch.keys = keys;
    out
}

/// A product: its fields in turn, at the same positions, under the labels the fields before
/// refined. Consecutive leaf fields whose declared widths fit one `u64` sort as one packed key
/// ([`sort_packed`]). A field's output is final when emitted, since later fields permute only
/// within its classes, on which it is constant; once nothing is tied the remaining fields are
/// read out by the index.
fn sort_prod(
    cols: &[Value],
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> Option<Value> {
    let m = index.len();
    if cols.is_empty() {
        densify(labels, m);
        return emit.values().then_some(Value::Prod(Vec::new()));
    }
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
        let (out, next) = match &cols[f] {
            Value::Prim(_) => sort_packed(cols, f, labels, index, emit, scratch),
            c => {
                // only the last segment may leave the index behind
                let mode = if f + 1 == cols.len() { emit } else { emit.keeping_index() };
                (sort_indexed(c, labels, index, mode, scratch).into_iter().collect(), f + 1)
            }
        };
        outs.extend(out);
        settled = fully_discriminated(labels);
        f = next;
    }
    emit.values().then_some(Value::Prod(outs))
}

/// The leaf fields of `cols` from `f` on, as many as fit one `u64` by their declared widths,
/// sorted as one key, most significant field first: one set of passes and one refinement for
/// the run. Returns one column per field with `emit`, and the index of the first field not taken.
fn sort_packed(
    cols: &[Value],
    f: usize,
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> (Vec<Value>, usize) {
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
    if emit == Emit::Values && g == cols.len() {
        sort_keys_only(&mut keys, labels, scratch);
    } else {
        sort_keys(&mut keys, labels, index, scratch);
    }
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
    (outs, g)
}

/// A sum: the tag as a virtual leaf, then each lane at the positions that carry its tag and are
/// still tied, through the carried within-lane offsets. While the lanes refine subsets, a label
/// is its run's starting position (see [`run_starts`]); dense afterwards. A lane whose rows were
/// all sorted is emitted by the sort; otherwise it is read once at its final offsets.
fn sort_sum(
    tags: &Tags,
    lanes: &[Value],
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> Option<Value> {
    let m = index.len();
    // one lane throughout: the tag decides nothing and row i is that lane's row i.
    if let Some(t) = tags.const_tag() {
        let out = sort_indexed(&lanes[t], labels, index, emit, scratch);
        return out.map(|sorted| {
            let mut ls: Vec<Value> = lanes.iter().map(|l| gather(l, &[])).collect();
            ls[t] = sorted;
            Value::sum_tagged(Tags::Const(t, m), ls)
        });
    }
    let Tags::Column(tag_col, within) = tags else { unreachable!("const handled above") };
    let mut keys = std::mem::take(&mut scratch.keys);
    keys.clear();
    keys.extend(index.iter().map(|&r| tag_col.usize_at(r) as u64));
    sort_keys(&mut keys, labels, index, scratch);
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
    let mut slot = std::mem::take(&mut scratch.slot);
    let mut lanes_out = Vec::with_capacity(lanes.len());
    for (t, lane) in lanes.iter().enumerate() {
        let qs = &tied[t];
        let mut sorted = None;
        if !qs.is_empty() {
            // The lane sorts its own offsets; `slot` takes each offset back to its row.
            let mut index_t: Vec<usize> = qs.iter().map(|&q| within[index[q]]).collect();
            reach(&mut slot, lane.len());
            for (&o, &q) in index_t.iter().zip(qs) {
                slot[o] = index[q];
            }
            let mut labels_t: Vec<u64> = qs.iter().map(|&q| labels[q]).collect();
            sorted = sort_indexed(lane, &mut labels_t, &mut index_t, emit, scratch);
            for (&o, &q) in index_t.iter().zip(qs) {
                index[q] = slot[o];
            }
            write_starts(labels, qs, &labels_t);
        }
        if emit.values() {
            lanes_out.push(match sorted {
                Some(o) if qs.len() == all[t].len() => o,
                _ => gather(lane, &all[t].iter().map(|&q| within[index[q]]).collect::<Vec<_>>()),
            });
        }
    }
    scratch.slot = slot;
    refine(labels, |_| false);
    emit.values().then(|| Value::sum_tagged(Tags::from_tags(tags_out, lanes.len()), lanes_out))
}

/// A list, lexicographically: one refining pass per element position over the rows still tied,
/// a row that ends there first in its block and leaf elements read straight into keys, then one
/// gather of the elements in final order, the only gather this sort makes. Equal-width byte records up to 8 wide pack into one `u64` key
/// and unpack from the sorted keys.
fn sort_list(
    bounds: &Bounds,
    vals: &Value,
    labels: &mut Vec<u64>,
    index: &mut [usize],
    emit: Emit,
    scratch: &mut SortScratch,
) -> Option<Value> {
    let m = index.len();
    if let (Some(k), Value::Prim(Prim::U8(bytes))) = (bounds.strided(), vals) {
        if (1..=8).contains(&k) {
            let mut keys = std::mem::take(&mut scratch.keys);
            keys.clear();
            keys.extend(index.iter().map(|&r| (0..k).fold(0u64, |key, p| (key << 8) | bytes[r * k + p] as u64)));
            sort_keys(&mut keys, labels, index, scratch);
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
            return out;
        }
    }
    if labels.is_empty() {
        labels.resize(m, 0);
    }
    run_starts(labels);
    // Lexicographic: position by position over the rows still tied. In each tied block the rows
    // that end at `pos` go first, as one class (each is a proper prefix of the rest), and the
    // rows that go on sort by their element at `pos`: a leaf's read straight into keys, anything
    // else's through its positions.
    let leaf = match vals {
        Value::Prim(p) => Some(p),
        _ => None,
    };
    let mut live: Vec<usize> = (0..m).filter(|&q| in_tie(labels, q)).collect();
    let mut pos = 0;
    let (mut going, mut rows, mut keys, mut labels_j, mut elem) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut slot = std::mem::take(&mut scratch.slot);
    while !live.is_empty() {
        // `live` is whole blocks of consecutive positions. One read of each row's span splits its
        // block: the rows that end here move up to its front, in order, and the rest are gathered
        // to sort, under a label of their own.
        going.clear();
        rows.clear();
        keys.clear();
        labels_j.clear();
        let mut b = 0;
        while b < live.len() {
            let q0 = live[b];
            let mut e = b + 1;
            while e < live.len() && labels[live[e]] == labels[q0] {
                e += 1;
            }
            let q1 = q0 + (e - b);
            let mut ends = 0;
            for q in q0..q1 {
                let r = index[q];
                let (s, t) = bounds.span(r);
                if s + pos == t {
                    index[q0 + ends] = r;
                    ends += 1;
                } else {
                    if let Some(p) = leaf {
                        keys.push(p.u64_at(s + pos));
                    }
                    rows.push(r);
                }
            }
            labels_j.resize(rows.len(), (q0 + ends) as u64);
            going.extend(q0 + ends..q1);
            b = e;
        }
        if going.is_empty() {
            break;
        }
        if leaf.is_some() {
            sort_keys(&mut keys, &mut labels_j, &mut rows, scratch);
        } else {
            // distinct rows have distinct elements, and `slot` takes each element back to its
            // row once the elements are sorted.
            elem.clear();
            elem.extend(rows.iter().map(|&r| bounds.span(r).0 + pos));
            reach(&mut slot, vals.len());
            for (&e, &r) in elem.iter().zip(&rows) {
                slot[e] = r;
            }
            sort_indexed(vals, &mut labels_j, &mut elem, Emit::Index, scratch);
            for (r, &e) in rows.iter_mut().zip(&elem) {
                *r = slot[e];
            }
        }
        for (&q, &r) in going.iter().zip(&rows) {
            index[q] = r;
        }
        write_starts(labels, &going, &labels_j);
        pos += 1;
        live.clear();
        live.extend(going.iter().copied().filter(|&q| in_tie(labels, q)));
    }
    scratch.slot = slot;
    refine(labels, |_| false);
    emit.values().then(|| {
        let mut elems = Vec::new();
        let mut ends = Vec::with_capacity(m);
        for &r in index.iter() {
            let (s, e) = bounds.span(r);
            elems.extend(s..e);
            ends.push(elems.len());
        }
        Value::List(ends.into(), Box::new(gather(vals, &elems)))
    })
}

// ---- the leaf kernel ------------------------------------------------------------------------

/// Sort positions by `keys` within the runs of equal `labels`, stably, the rows moving with them.
///
/// Requires `keys`, `labels` and `rows` of one length, `labels` non-decreasing (or empty for one
/// run). Ensures `keys` sorted within each run, `labels` the dense run index of the refined
/// partition, `rows` permuted alike. Keys already in order within every run move nothing.
fn sort_keys(keys: &mut [u64], labels: &mut Vec<u64>, rows: &mut [usize], scratch: &mut SortScratch) {
    let m = keys.len();
    if refine_if_ordered(keys, labels) {
        return;
    }
    if labels.is_empty() {
        sort_block(keys, rows, scratch);
        label_runs(labels, m, |q| keys[q] != keys[q - 1]);
    } else {
        let mut lo = 0;
        while lo < m {
            let mut hi = lo + 1;
            while hi < m && labels[hi] == labels[lo] {
                hi += 1;
            }
            if hi - lo > 1 {
                sort_block(&mut keys[lo..hi], &mut rows[lo..hi], scratch);
            }
            lo = hi;
        }
        refine(labels, |q| keys[q] != keys[q - 1]);
    }
}

/// [`sort_keys`] without rows: `keys` sorted within each run and `labels` refined, for a caller
/// that will not read the index again.
fn sort_keys_only(keys: &mut [u64], labels: &mut Vec<u64>, scratch: &mut SortScratch) {
    let m = keys.len();
    if refine_if_ordered(keys, labels) {
        return;
    }
    if labels.is_empty() {
        sort_block_impl::<false>(keys, &mut [], scratch);
        label_runs(labels, m, |q| keys[q] != keys[q - 1]);
        return;
    }
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

/// Keys already in order within each run of `labels`: refine the labels by the keys' equal runs
/// and report it, touching nothing else. Most later product fields are constant, or ascending,
/// within the classes the fields before them formed. Leaves the labels as they were otherwise.
fn refine_if_ordered(keys: &[u64], labels: &mut Vec<u64>) -> bool {
    let ordered = if labels.is_empty() {
        keys.windows(2).all(|w| w[0] <= w[1])
    } else {
        keys.windows(2).zip(labels.windows(2)).all(|(k, l)| k[0] <= k[1] || l[0] != l[1])
    };
    if ordered {
        if labels.is_empty() {
            label_runs(labels, keys.len(), |q| keys[q] != keys[q - 1]);
        } else {
            refine(labels, |q| keys[q] != keys[q - 1]);
        }
    }
    ordered
}

/// Stable sort of one block by `keys`, `rows` moving with them. Blocks of 32 or fewer take an
/// insertion sort; the rest an LSD radix with every pass sequential, a digit widening with the
/// block ([`digit_width`]). One sweep counts every digit at once; a digit on which every key
/// agrees is skipped, as are the leading all-zero digits.
fn sort_block(keys: &mut [u64], rows: &mut [usize], scratch: &mut SortScratch) {
    sort_block_impl::<true>(keys, rows, scratch)
}

/// [`sort_block`], with (`ROWS`) or without the positions travelling alongside.
fn sort_block_impl<const ROWS: bool>(keys: &mut [u64], rows: &mut [usize], scratch: &mut SortScratch) {
    let n = keys.len();
    if n <= 32 {
        for k in 1..n {
            let mut j = k;
            while j > 0 && keys[j - 1] > keys[j] {
                keys.swap(j - 1, j);
                if ROWS {
                    rows.swap(j - 1, j);
                }
                j -= 1;
            }
        }
        return;
    }
    if n > COUNTED_MAX {
        if ROWS {
            let mut pairs: Vec<(u64, usize)> = keys.iter().copied().zip(rows.iter().copied()).collect();
            pairs.sort_by_key(|p| p.0);
            for (i, (k, q)) in pairs.into_iter().enumerate() {
                keys[i] = k;
                rows[i] = q;
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
    let SortScratch { keys_alt, rows_alt, counts, .. } = scratch;
    keys_alt.resize(keys_alt.len().max(n), 0);
    if ROWS {
        rows_alt.resize(rows_alt.len().max(n), 0);
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
    let rows_alt = if ROWS { &mut rows_alt[..n] } else { &mut rows_alt[..0] };
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
                if ROWS {
                    rows_alt[slot] = rows[j];
                }
            }
        } else {
            for j in 0..n {
                let k = keys_alt[j];
                let b = ((k >> shift) & mask) as usize;
                let slot = counts[b] as usize;
                counts[b] += 1;
                keys[slot] = k;
                if ROWS {
                    rows[slot] = rows_alt[j];
                }
            }
        }
        primary = !primary;
    }
    if !primary {
        keys.copy_from_slice(keys_alt);
        if ROWS {
            rows.copy_from_slice(rows_alt);
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

/// Fill absent `labels` as the dense run index over `m` positions, a run ending where `split(q)`
/// says positions `q - 1` and `q` differ.
fn label_runs(labels: &mut Vec<u64>, m: usize, split: impl Fn(usize) -> bool) {
    labels.clear();
    labels.reserve(m);
    let mut next = 0u64;
    for q in 0..m {
        if q > 0 && split(q) {
            next += 1;
        }
        labels.push(next);
    }
}

/// Renumber `labels` densely without splitting anything; absent labels become one class.
fn densify(labels: &mut Vec<u64>, m: usize) {
    if labels.is_empty() {
        labels.resize(m, 0);
    } else {
        refine(labels, |_| false);
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

/// Grow `slot` to hold at least `n` entries; the entries a caller reads it wrote first.
fn reach(slot: &mut Vec<usize>, n: usize) {
    if slot.len() < n {
        slot.resize(n, 0);
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

#[cfg(test)]
mod tests;
