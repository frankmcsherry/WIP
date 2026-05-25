//! Discrimination-based recursive sort over Prim/Prod/List/Sum.
//!
//! Engine (principle 7): `sort_seq(order, things) → (sorted things,
//! order/group labels)`. Data-returning — it hands back the reordered
//! value and the refined order/group ids, never a permutation. `sort`
//! routes through it. The single-block flat-Prim case moves the bytes
//! directly via `sort_bytes_radix` (no `(key, pos)` pairs, no gather);
//! everything else delegates to the perm core below.
//!
//! Internal mechanism: `sort_blocks(labels, value) → (perm, new_labels)`
//! — the perm-producing discrimination core. Still used by the compound
//! cases (which need a per-step reindexing to carry sibling columns) and
//! by `sort.perm` (the random-access escape hatch). Replacing its
//! `perm` + `gather` with in-place distribution — so the compound sorts
//! stop gathering too — is BACKLOG item S step 3.
//!
//! `labels` is a non-decreasing P64-style block partition. Positions
//! sharing the same label are in the same block. Sort refines blocks
//! by value:
//!
//!   - **Block-stable.** If `labels[a] < labels[b]` then `a` precedes
//!     `b` in the output. Sort only reorders *within* each input block.
//!   - **Refined output.** Within each input block, output positions
//!     are sorted by value.
//!   - **Refined labels.** Returned `new_labels` (non-decreasing) is a
//!     refinement of input labels: two positions share a new label iff
//!     they shared an input label AND have equal value.
//!
//! `perm[k]` is the input position that becomes output position `k`.
//!
//! Per-shape:
//!   Prim — comparison sort per block (recursive case). Top-level
//!          single-block path uses counting (u8) or LSD byte-radix
//!          (u16) when the keys are unsigned-narrow; u32/u64 fall back
//!          to `sort_unstable_by_key` (pdqsort).
//!   Prod — sequential field sorts; each reorders the whole Prod.
//!   List — LENGTH-FIRST: refine by row length, then by element at each
//!          position 0..max_len-1. Inner shape is general (any shape
//!          `gather` accepts).
//!   Sum  — refine by disc; per-lane recursion into the gathered lane
//!          content with globally-renumbered labels.
//!
//! Default leaf interpretation is unsigned at storage width. For
//! signed/float order on a flat Prim, swizzle first:
//! `enswizzle.<i> sort deswizzle.<i>`.
//!
//! ## Two approaches (the file's organizing axis)
//!
//! Every function below belongs to one of two families, marked by `----`
//! banners:
//!
//!   - **Perm-based discrimination core** — returns `(perm, labels)` or a
//!     bare `Vec<u64>`. The `Vec<u64>` *is* the permutation (`perm[k]` =
//!     the input position that lands at output `k`); no data moves here,
//!     only indices. Compound shapes compose by reindexing siblings, so
//!     nesting and `sort.perm` live here.
//!   - **Value-carried leaf kernels** — returns `Prim`/`Value`. They move
//!     the bytes directly (LSD value-radix / packed key): no perm, no
//!     gather (principle 7). They fire only when a subtree is simple
//!     enough; otherwise the dispatcher falls back to the perm core.
//!
//! Tell at a glance: a `Vec<u64>` / `labels` return ⇒ perm-based; a
//! `Prim` / `Value` return ⇒ value-carried. `sort_seq` is the dispatcher
//! that picks value-carried where it can and delegates the rest.

use std::sync::Arc;
use crate::ir::op::PrimOp;
use crate::ir::stack::{Stack, pop};
use crate::ir::typecheck::{Typed, TypeStack, TypeEnv, tc_pop};
use crate::ir::value::{Value, Prim, PrimWidth, BoundsRepr, Storage};
use crate::ir::shape::Shape;
use crate::ops::helpers::{gather, materialize_ref};

// The two approaches live in sibling modules; re-export their public
// surface so `crate::ops::sort::X` paths — and `use super::*` inside each
// module — keep resolving as before.
pub use perm::*;
pub use value::*;
pub use runs::*;

// ---- User-facing ops ----

/// `sort.perm` — polymorphic. Returns a P64 permutation that sorts
/// the top of stack. This is the random-access escape hatch
/// (principle 7): a permutation is the fully-resolved special case of
/// the engine's order/group labels, and consuming one via `gather`
/// is random access. Prefer `sort` (data-returning) where you can.
#[derive(Debug)]
pub struct SortPerm;

impl PrimOp for SortPerm {
    fn name(&self) -> &str { "sort.perm" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        // Fast path: top-level Prim.
        if let Value::Prim(p) = &v {
            let perm = sort_prim_perm_top(p);
            st.push(Value::Prim(Prim::P64(Arc::new(perm))));
            return Ok(());
        }
        let n = v.len();
        let labels = vec![0u64; n];
        let (perm, _) = sort_blocks(&labels, &v)?;
        st.push(Value::Prim(Prim::P64(Arc::new(perm))));
        Ok(())
    }
}

impl Typed for SortPerm {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let _ = tc_pop(st, "sort.perm")?;
        st.push(Shape::Prim(PrimWidth::W64));
        Ok(())
    }
}

/// `sort` — polymorphic. Returns the sorted value. The data-returning
/// face of the engine (`sort_seq`); the order/group labels it also
/// produces are discarded here.
#[derive(Debug)]
pub struct SortPoly;

impl PrimOp for SortPoly {
    fn name(&self) -> &str { "sort" }
    fn arity(&self) -> Option<(usize, usize)> { Some((1, 1)) }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let order = vec![0u64; v.len()];
        let (sorted, _labels) = sort_seq(&order, &v, false)?;
        st.push(sorted);
        Ok(())
    }
}

impl Typed for SortPoly {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "sort")?;
        st.push(v);
        Ok(())
    }
}

pub fn register(r: &mut crate::syntax::registry::OpRegistry) {
    use crate::ir::typecheck::Op;
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        match t {
            "sort.perm"  => Some(Box::new(SortPerm)),
            "sort"       => Some(Box::new(SortPoly)),
            _ => None,
        }
    });
}

// ---- Entry point: value-returning dispatcher ----

/// The sort engine, data-returning form (principle 7).
///
/// `order`: non-decreasing order/group ids over the N elements — the
///   ordering established so far. Distinct ⇒ fully determined (no work);
///   all-equal ⇒ unconstrained; repeats ⇒ order by id, then refine
///   within equal-id groups by `things`' own value.
/// `things`: the data to sort; reordered by structural recursion.
///
/// Returns `(things reordered, refined order/group ids)`. The refined
/// ids are the carrier between e.g. successive tuple fields, and — when
/// fully resolved to distinct values — encode the permutation. A
/// permutation as a *primary* output is the random-access escape hatch
/// (`sort.perm`); this engine keeps you in sequential-access land.
///
/// First cut: the reordering is realized by an internal perm + `gather`.
/// Replacing that with in-place distribution (no gather, no random
/// access) is BACKLOG item S — the contract here is unchanged by it.
///
/// `want_labels`: when `false`, the labels output is *dead* and its
/// computation is skipped (the returned Vec is empty). This is the
/// stage-C seed — kernel work elided by output liveness. `sort` (which
/// discards the labels) passes `false`; callers that consume the run
/// structure pass `true`.
pub fn sort_seq(order: &[u64], things: &Value, want_labels: bool) -> Result<(Value, Vec<u64>), String> {
    let n = things.len();
    if order.len() != n {
        return Err(format!("sort_seq: order.len() {} != things.len() {}", order.len(), n));
    }
    // Fast path: a flat Prim under no external ordering constraint
    // (single block) sorts via the top-level radix; labels fall out of
    // the sorted runs (skipped when not wanted).
    if let Value::Prim(p) = things {
        if order.first().map_or(true, |&f| order.iter().all(|&l| l == f)) {
            // Value-only single-block sort: move the bytes directly
            // (step 1 radix) — no (key, pos) pairs, no permutation, no
            // gather.
            let sorted = Value::Prim(sort_bytes_radix(p.clone()));
            let labels = if want_labels { prim_run_labels(&sorted) } else { Vec::new() };
            return Ok((sorted, labels));
        }
    }
    // Prod of flat Prims: sort the row "as one thing" — pack each row into
    // a single key (sized to actual ranges), radix, and decode the sorted
    // keys back into columns. No index, no gather. Falls back to the
    // perm+gather core when a field isn't a flat Prim or the key would
    // exceed 128 bits.
    if let Value::Prod(fs) = things {
        if let Some(res) = sort_prod_packed(order, fs, want_labels) {
            return Ok(res);
        }
    }
    let (perm, labels) = sort_blocks(order, things)?;
    let perm_usize: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
    Ok((gather(things, &perm_usize)?, labels))
}

// ---- Perm-based discrimination core (returns indices + labels) ----
mod perm {
    use super::*;

    /// Find contiguous label-blocks in a non-decreasing labels array.
    /// Returns half-open intervals `[lo, hi)` covering each block.
    fn find_blocks(labels: &[u64]) -> Vec<(usize, usize)> {
        let n = labels.len();
        if n == 0 { return Vec::new(); }
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

    /// Perm-producing discrimination core (internal). `sort_seq` is the
    /// data-returning entry point; this is the mechanism it (and `sort.perm`)
    /// delegate to where a permutation is still needed. See module docs.
    pub fn sort_blocks(labels: &[u64], value: &Value) -> Result<(Vec<u64>, Vec<u64>), String> {
        let n = value.len();
        if labels.len() != n {
            return Err(format!("sort_blocks: labels.len() {} != value.len() {}", labels.len(), n));
        }
        if n == 0 { return Ok((Vec::new(), Vec::new())); }

        let v_cow = materialize_ref(value)?;
        let v: &Value = v_cow.as_ref();

        // Fast path: top-level single block over a flat Prim → the radix /
        // counting perm sort (`sort_prim_perm_top`), with labels derived from
        // adjacent sorted keys. Recursive multi-block calls fall through to
        // the per-block `sort_prim_blocks`. This is what gives `group` (which
        // sorts a single key column) its counting/radix key-sort.
        if let Value::Prim(p) = v {
            if labels.first().map_or(true, |&f| labels.iter().all(|&l| l == f)) {
                let perm = sort_prim_perm_top(p);
                let labels_out = prim_perm_run_labels(p, &perm);
                return Ok((perm, labels_out));
            }
        }

        match v {
            Value::Prim(p) => sort_prim_blocks(labels, p),
            Value::Prod(fs) => sort_prod_blocks(labels, fs.as_ref()),
            Value::List { bounds, values } => sort_list_blocks(labels, bounds, values.as_ref()),
            Value::Sum { disc, lanes } => sort_sum_blocks(labels, disc, lanes.as_ref()),
            Value::View { .. } => unreachable!("materialize_ref dropped View"),
        }
    }
    /// Average block size at or above which the packed-label radix is
    /// preferred over the per-block comparison sort. Below it (data spread
    /// across many tiny blocks) the per-block sorts are small and cache-hot
    /// and win; at/above it (few/large blocks, incl. the enlist'd single-list
    /// global sort) the radix amortizes. Heuristic — tune with measurement.
    const PACKED_AVG_BLOCK_MIN: usize = 256;

    /// Packed-label radix for the multi-block Prim sort: pack `(label, value)`
    /// into one u64 key (label high → block-stable, value low → within-block
    /// order), radix the whole column once carrying the original index —
    /// "prepend the index and sort once." `None` when the packed key would
    /// exceed 64 bits (caller falls back to per-block sort).
    fn sort_prim_blocks_packed(labels: &[u64], p: &Prim) -> Option<(Vec<u64>, Vec<u64>)> {
        let n = p.len();
        let bits = |max: u64| -> u32 { if max == 0 { 0 } else { 64 - max.leading_zeros() } };
        // Cheap budget check first (max scans, no allocation) — only copy the
        // column into packed keys if it actually packs.
        let value_bits = bits(prim_max(p));
        let label_bits = bits(labels.iter().copied().max().unwrap_or(0));
        if label_bits + value_bits > 64 { return None; }

        let vals = prim_words_u64(p);
        let mut pairs: Vec<(u64, u64)> = (0..n)
            .map(|i| ((labels[i] << value_bits) | vals[i], i as u64))
            .collect();
        if n > 1 {
            let max_key = pairs.iter().map(|pr| pr.0).max().unwrap_or(0);
            let effective = if max_key == 0 { 0 } else {
                ((64 - max_key.leading_zeros() as usize) + 7) / 8
            };
            if effective > 0 {
                let mut tmp = vec![(0u64, 0u64); n];
                msd_radix_u64(&mut pairs, effective, &mut tmp);
            }
        }
        let mut perm = vec![0u64; n];
        let mut new_labels = vec![0u64; n];
        let mut lbl = 0u64;
        let mut prev_key = 0u64;
        for k in 0..n {
            let key = pairs[k].0;
            if k > 0 && key != prev_key { lbl += 1; }
            prev_key = key;
            new_labels[k] = lbl;
            perm[k] = pairs[k].1;
        }
        Some((perm, new_labels))
    }

    /// Perm-based: sorts a Prim *within* each label-block, returning `(perm,
    /// new_labels)` — never sorted values. The perm is what lets compound
    /// callers (`sort_prod_blocks` etc.) reorder sibling columns by this
    /// field's order. The value-carried analogue is `sort_bytes_radix`.
    fn sort_prim_blocks(labels: &[u64], p: &Prim) -> Result<(Vec<u64>, Vec<u64>), String> {
        // Dispatch by block structure: few/large blocks → packed-label radix
        // (amortizes); many tiny blocks → per-block comparison sort (small,
        // cache-hot). Both kernels are kept; the radix lost ~2× on all-tiny
        // blocks (bench 11) but wins at scale / for the enlist'd single-list
        // global sort, so we gate rather than drop it.
        // One block scan, reused for both the gate and the per-block loop.
        let n = labels.len();
        let blocks = find_blocks(labels);
        if !blocks.is_empty() && n / blocks.len() >= PACKED_AVG_BLOCK_MIN {
            if let Some(res) = sort_prim_blocks_packed(labels, p) {
                return Ok(res);
            }
        }
        // Recursive case: blocks within an outer sort (e.g., subsequent
        // fields of a Prod). Block sizes shrink across recursion, so
        // comparison sort is the right kernel here. The top-level
        // single-block hot path uses `sort_prim_perm_top` instead.
        let mut perm: Vec<u64> = Vec::with_capacity(p.len());
        let mut new_labels: Vec<u64> = Vec::with_capacity(p.len());
        let mut next_label: u64 = 0;

        macro_rules! per_block { ($t:ty) => {{
            let xs: &[$t] = <$t as Storage>::extract(p)?;
            for &(lo, hi) in &blocks {
                if hi - lo == 1 {
                    perm.push(lo as u64);
                    new_labels.push(next_label);
                    next_label += 1;
                    continue;
                }
                let mut idxs: Vec<u64> = (lo as u64..hi as u64).collect();
                // Bundled-API stability comes from label refinement (the
                // outer Prod / List driver only re-sorts subsequent fields
                // *within* same-value blocks). Unstable within a block is
                // fine.
                idxs.sort_unstable_by_key(|&i| xs[i as usize]);
                let mut prev = xs[idxs[0] as usize];
                new_labels.push(next_label);
                perm.push(idxs[0]);
                for k in 1..idxs.len() {
                    let cur = xs[idxs[k] as usize];
                    if cur != prev {
                        next_label += 1;
                        prev = cur;
                    }
                    new_labels.push(next_label);
                    perm.push(idxs[k]);
                }
                next_label += 1;
            }
        }};}

        match p {
            Prim::P8(_)  => per_block!(u8),
            Prim::P16(_) => per_block!(u16),
            Prim::P32(_) => per_block!(u32),
            Prim::P64(_) => per_block!(u64),
        }

        Ok((perm, new_labels))
    }

    fn sort_prod_blocks(labels: &[u64], fields: &[Value]) -> Result<(Vec<u64>, Vec<u64>), String> {
        let n = labels.len();
        if fields.is_empty() {
            return Ok(((0..n as u64).collect(), labels.to_vec()));
        }
        let mut perm: Vec<u64> = (0..n as u64).collect();
        let mut cur_labels: Vec<u64> = labels.to_vec();

        for f in fields.iter() {
            let perm_usize: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
            let f_perm = gather(f, &perm_usize)?;
            let (sub_perm, sub_labels) = sort_blocks(&cur_labels, &f_perm)?;
            let new_perm: Vec<u64> = sub_perm.iter().map(|&k| perm[k as usize]).collect();
            perm = new_perm;
            cur_labels = sub_labels;
        }

        Ok((perm, cur_labels))
    }

    fn sort_list_blocks(
        labels: &[u64],
        bounds: &BoundsRepr,
        values: &Value,
    ) -> Result<(Vec<u64>, Vec<u64>), String> {
        let n = labels.len();
        // Materialize bounds as N+1 start-offsets for the random-access lookups
        // below (sample_row / row → row_start).
        let bnds: Vec<u64> = bounds.iter_starts().collect();

        // Row lengths.
        let mut lengths: Vec<u64> = Vec::with_capacity(n);
        for w in bnds.windows(2) { lengths.push(w[1] - w[0]); }

        // Length-first refinement. After this, `cur_labels` partitions rows
        // into length-uniform blocks (in `perm`-order).
        let len_value = Value::Prim(Prim::P64(Arc::new(lengths.clone())));
        let (perm, cur_labels) = sort_blocks(labels, &len_value)?;

        // Length-stratified position refinement. For each length-L block,
        // touch only the rows in *that* block, for positions 0..L-1. This
        // gives O(Σ_i row_length_i) total work per List level instead of
        // O(max_len × N) — matches Henglein-style discrimination.
        let mut new_perm: Vec<u64> = perm.clone();
        let mut new_labels: Vec<u64> = vec![0; n];
        let mut next_offset: u64 = 0;

        for (lo, hi) in find_blocks(&cur_labels) {
            let block_size = hi - lo;
            if block_size == 1 {
                new_labels[lo] = next_offset;
                next_offset += 1;
                continue;
            }

            // All rows in this block share the same length; read it off any
            // representative.
            let sample_row = perm[lo] as usize;
            let s_lo = bnds[sample_row] as usize;
            let s_hi = bnds[sample_row + 1] as usize;
            let len = s_hi - s_lo;

            // Within-block sort. `local_perm[i]` is the block-relative index
            // (0..block_size) of the row that lands at output position
            // `lo + i`. `local_labels` is a refinement of the all-zeros
            // partition.
            let mut local_perm: Vec<u64> = (0..block_size as u64).collect();
            let mut local_labels: Vec<u64> = vec![0u64; block_size];

            for pos in 0..len {
                // Gather only this block's rows at position `pos`.
                let positions: Vec<usize> = local_perm.iter().map(|&k| {
                    let row = perm[lo + k as usize] as usize;
                    bnds[row] as usize + pos
                }).collect();
                let elem_val = gather(values, &positions)?;
                let (sub_perm, sub_labels) = sort_blocks(&local_labels, &elem_val)?;
                local_perm = sub_perm.iter().map(|&k| local_perm[k as usize]).collect();
                local_labels = sub_labels;
            }

            let block_max = local_labels.iter().copied().max().unwrap_or(0);
            for i in 0..block_size {
                new_perm[lo + i] = perm[lo + local_perm[i] as usize];
                new_labels[lo + i] = next_offset + local_labels[i];
            }
            next_offset += block_max + 1;
        }

        Ok((new_perm, new_labels))
    }

    fn sort_sum_blocks(
        labels: &[u64],
        disc: &Prim,
        lanes: &[Value],
    ) -> Result<(Vec<u64>, Vec<u64>), String> {
        // Algorithm:
        //   1. Sort by disc → groups positions by discriminant.
        //   2. For each disc-block, gather the corresponding entries from
        //      that lane (using per-position lane cursors), recurse to sort
        //      the lane data within the block.
        //   3. Apply the per-block reordering to the running permutation.
        //   4. Renumber labels to remain globally non-decreasing.
        let n = labels.len();
        let disc_bytes = match disc {
            Prim::P8(v) => v.clone(),
            other => return Err(format!("sum disc must be P8, got {:?}", other.width())),
        };

        // Step 1: disc sort.
        let disc_value = Value::Prim(disc.clone());
        let (perm_disc, labels_disc) = sort_blocks(labels, &disc_value)?;

        // Per-original-position cursor into its lane.
        let mut lane_cursors = vec![0u64; n];
        let mut lane_counts = vec![0u64; lanes.len()];
        for i in 0..n {
            let k = disc_bytes[i] as usize;
            if k >= lanes.len() {
                return Err(format!("sum disc[{}]={} but only {} lanes", i, k, lanes.len()));
            }
            lane_cursors[i] = lane_counts[k];
            lane_counts[k] += 1;
        }

        // Step 2 + 3 + 4: walk blocks, refine each.
        let mut final_perm = perm_disc.clone();
        let mut final_labels: Vec<u64> = vec![0; n];
        let mut next_offset: u64 = 0;

        for (lo, hi) in find_blocks(&labels_disc) {
            if hi - lo == 1 {
                final_labels[lo] = next_offset;
                next_offset += 1;
                continue;
            }
            // All positions in this block have the same disc value `k`.
            let sample = perm_disc[lo] as usize;
            let k = disc_bytes[sample] as usize;

            // Lane positions for each block element.
            let lane_positions: Vec<usize> = (lo..hi)
                .map(|i| lane_cursors[perm_disc[i] as usize] as usize)
                .collect();
            let lane_data = gather(&lanes[k], &lane_positions)?;

            // Block labels (constant within the block) — use all-zeros so the
            // recursive sort starts fresh.
            let block_labels = vec![0u64; hi - lo];
            let (sub_perm, sub_labels) = sort_blocks(&block_labels, &lane_data)?;
            let max_sub = sub_labels.iter().copied().max().unwrap_or(0);

            for i in 0..(hi - lo) {
                final_labels[lo + i] = next_offset + sub_labels[i];
                final_perm[lo + i] = perm_disc[lo + sub_perm[i] as usize];
            }
            next_offset += max_sub + 1;
        }

        Ok((final_perm, final_labels))
    }

    /// Top-level "single block" fast path: sort a Prim directly to a
    /// permutation without allocating the labels/idxs scaffolding.
    ///
    /// u8 → counting sort (one pass, 256 buckets).
    /// u16/u32/u64 → MSD byte-radix on (key, position) pairs. Subsequent
    /// recursion operates on contiguous buckets that fit in cache. Falls
    /// back to pdqsort for buckets under 32 elements (insertion-sort
    /// territory) and for top-level N < 4096 where radix setup dominates.
    pub fn sort_prim_perm_top(p: &Prim) -> Vec<u64> {
        let n = p.len();
        match p {
            Prim::P8(v) => {
                let xs: &[u8] = &v;
                let mut counts = [0usize; 256];
                for &x in xs { counts[x as usize] += 1; }
                let mut cursors = [0usize; 256];
                let mut s = 0;
                for j in 0..256 { cursors[j] = s; s += counts[j]; }
                let mut perm = vec![0u64; n];
                for (i, &x) in xs.iter().enumerate() {
                    perm[cursors[x as usize]] = i as u64;
                    cursors[x as usize] += 1;
                }
                perm
            }
            Prim::P16(v) => {
                let xs: &[u16] = &v;
                if n < 4096 {
                    let mut perm: Vec<u64> = (0..n as u64).collect();
                    perm.sort_unstable_by_key(|&i| xs[i as usize]);
                    perm
                } else {
                    let mut pairs: Vec<(u16, u64)> = xs.iter().enumerate()
                        .map(|(i, &x)| (x, i as u64)).collect();
                    let mut tmp: Vec<(u16, u64)> = vec![(0, 0); n];
                    msd_radix_u16(&mut pairs, 2, &mut tmp);
                    pairs.into_iter().map(|(_, v)| v).collect()
                }
            }
            Prim::P32(v) => {
                let xs: &[u32] = &v;
                if n < 4096 {
                    let mut perm: Vec<u64> = (0..n as u64).collect();
                    perm.sort_unstable_by_key(|&i| xs[i as usize]);
                    return perm;
                }
                // Effective-bytes scan: start MSD at the highest non-zero
                // byte. Saves wasted passes on small-range data (e.g., Q1
                // GROUP BY where keys are 0..6 stored as u32).
                let max = xs.iter().copied().max().unwrap_or(0);
                let effective = if max == 0 { 0 } else {
                    ((32 - max.leading_zeros() as usize) + 7) / 8
                };
                if effective == 0 {
                    return (0..n as u64).collect();
                }
                let mut pairs: Vec<(u32, u64)> = xs.iter().enumerate()
                    .map(|(i, &x)| (x, i as u64)).collect();
                let mut tmp: Vec<(u32, u64)> = vec![(0, 0); n];
                msd_radix_u32(&mut pairs, effective, &mut tmp);
                pairs.into_iter().map(|(_, v)| v).collect()
            }
            Prim::P64(v) => {
                let xs: &[u64] = &v;
                if n < 4096 {
                    let mut perm: Vec<u64> = (0..n as u64).collect();
                    perm.sort_unstable_by_key(|&i| xs[i as usize]);
                    return perm;
                }
                let max = xs.iter().copied().max().unwrap_or(0);
                let effective = if max == 0 { 0 } else {
                    ((64 - max.leading_zeros() as usize) + 7) / 8
                };
                if effective == 0 {
                    return (0..n as u64).collect();
                }
                let mut pairs: Vec<(u64, u64)> = xs.iter().enumerate()
                    .map(|(i, &x)| (x, i as u64)).collect();
                let mut tmp: Vec<(u64, u64)> = vec![(0, 0); n];
                msd_radix_u64(&mut pairs, effective, &mut tmp);
                pairs.into_iter().map(|(_, v)| v).collect()
            }
        }
    }

    macro_rules! msd_radix_impl {
        ($name:ident, $t:ty) => {
            /// MSD byte-radix on (key, position) pairs. Sorts in place using a tmp
            /// buffer of the same length; tmp content is undefined on return.
            /// `byte_idx` indicates which byte to process next (1-indexed).
            /// Recursion stops at small buckets, falling back to pdqsort.
            fn $name(pairs: &mut [($t, u64)], byte_idx: usize, tmp: &mut [($t, u64)]) {
                if pairs.len() <= 32 || byte_idx == 0 {
                    if pairs.len() > 1 { pairs.sort_unstable_by_key(|p| p.0); }
                    return;
                }
                let shift = ((byte_idx - 1) * 8) as u32;
                let mut counts = [0usize; 257];
                for p in pairs.iter() {
                    counts[((p.0 >> shift) & 0xff) as usize + 1] += 1;
                }
                for j in 1..257 { counts[j] += counts[j - 1]; }
                let starts = counts;
                let mut cursors = counts;
                for p in pairs.iter() {
                    let b = ((p.0 >> shift) & 0xff) as usize;
                    tmp[cursors[b]] = *p;
                    cursors[b] += 1;
                }
                pairs.copy_from_slice(&tmp[..pairs.len()]);
                for j in 0..256 {
                    let s = starts[j];
                    let e = if j < 255 { starts[j + 1] } else { pairs.len() };
                    if e > s + 1 {
                        $name(&mut pairs[s..e], byte_idx - 1, &mut tmp[s..e]);
                    }
                }
            }
        };
    }
    msd_radix_impl!(msd_radix_u16, u16);
    msd_radix_impl!(msd_radix_u32, u32);
    msd_radix_impl!(msd_radix_u64, u64);
}

// ---- Value-carried leaf kernels (move bytes; no perm, no gather) ----
mod value {
    use super::*;

    /// LSD byte-radix that reorders the *values* in place via a ping-pong
    /// buffer — no `(key, position)` payload, no permutation, no gather.
    /// This is the sequential leaf (principle 7): each pass is a counting
    /// sort over one byte, streaming src→dst. `effective` bytes (skipping
    /// high all-zero bytes) keeps small-range columns cheap.
    macro_rules! radix_lsd_impl {
        ($name:ident, $t:ty, $bits:expr) => {
            fn $name(mut src: Vec<$t>) -> Vec<$t> {
                let n = src.len();
                if n <= 1 { return src; }
                let max = src.iter().copied().max().unwrap_or(0);
                let nbytes = if max == 0 { 0 } else {
                    (($bits - max.leading_zeros() as usize) + 7) / 8
                };
                if nbytes == 0 { return src; }
                let mut dst: Vec<$t> = vec![0; n];
                for byte in 0..nbytes {
                    let shift = (byte * 8) as u32;
                    let mut counts = [0usize; 256];
                    for &x in src.iter() { counts[((x >> shift) & 0xff) as usize] += 1; }
                    let mut c = 0usize;
                    for j in 0..256 { let t = counts[j]; counts[j] = c; c += t; }
                    for &x in src.iter() {
                        let b = ((x >> shift) & 0xff) as usize;
                        dst[counts[b]] = x;
                        counts[b] += 1;
                    }
                    std::mem::swap(&mut src, &mut dst);
                }
                // Each pass swaps after writing dst, so the sorted data lands
                // back in `src` after `nbytes` swaps.
                src
            }
        };
    }
    radix_lsd_impl!(radix_lsd_u8,  u8,  8);
    radix_lsd_impl!(radix_lsd_u16, u16, 16);
    radix_lsd_impl!(radix_lsd_u32, u32, 32);
    radix_lsd_impl!(radix_lsd_u64, u64, 64);

    #[inline]
    fn unwrap_or_clone<T: Clone>(a: Arc<Vec<T>>) -> Vec<T> {
        Arc::try_unwrap(a).unwrap_or_else(|a| (*a).clone())
    }

    /// In-place unsigned value radix over a flat Prim (the principle-7 leaf).
    /// Internal kernel: `sort` uses it for the single-block Prim case, and the
    /// typed sort brackets it with `enswizzle`/`deswizzle`.
    pub fn sort_bytes_radix(p: Prim) -> Prim {
        match p {
            Prim::P8(a)  => Prim::P8(Arc::new(radix_lsd_u8(unwrap_or_clone(a)))),
            Prim::P16(a) => Prim::P16(Arc::new(radix_lsd_u16(unwrap_or_clone(a)))),
            Prim::P32(a) => Prim::P32(Arc::new(radix_lsd_u32(unwrap_or_clone(a)))),
            Prim::P64(a) => Prim::P64(Arc::new(radix_lsd_u64(unwrap_or_clone(a)))),
        }
    }

    /// Comparison baseline for the value radix, using `sort_unstable` (pdqsort).
    /// Kept alongside the radix so the two can be A/B'd (bench 13); the op
    /// uses the radix.
    pub fn sort_bytes_std(p: Prim) -> Prim {
        macro_rules! s { ($variant:ident, $a:expr) => {{
            let mut w = unwrap_or_clone($a);
            w.sort_unstable();
            Prim::$variant(Arc::new(w))
        }};}
        match p {
            Prim::P8(a)  => s!(P8, a),
            Prim::P16(a) => s!(P16, a),
            Prim::P32(a) => s!(P32, a),
            Prim::P64(a) => s!(P64, a),
        }
    }

    /// Max word of a Prim (zero-extended), 0 if empty. A read-only scan —
    /// no allocation, so the pack/no-pack decision is cheap on the fallback.
    pub fn prim_max(p: &Prim) -> u64 {
        match p {
            Prim::P8(a)  => a.iter().map(|&x| x as u64).max().unwrap_or(0),
            Prim::P16(a) => a.iter().map(|&x| x as u64).max().unwrap_or(0),
            Prim::P32(a) => a.iter().map(|&x| x as u64).max().unwrap_or(0),
            Prim::P64(a) => a.iter().copied().max().unwrap_or(0),
        }
    }

    /// Words of a Prim, zero-extended to u64 (sequential read).
    pub fn prim_words_u64(p: &Prim) -> Vec<u64> {
        match p {
            Prim::P8(a)  => a.iter().map(|&x| x as u64).collect(),
            Prim::P16(a) => a.iter().map(|&x| x as u64).collect(),
            Prim::P32(a) => a.iter().map(|&x| x as u64).collect(),
            Prim::P64(a) => (**a).clone(),
        }
    }

    /// Rebuild a width-`w` Prim from u64 words (truncating to the width).
    fn words_to_prim(w: PrimWidth, words: &[u64]) -> Prim {
        match w {
            PrimWidth::W8  => Prim::P8(Arc::new(words.iter().map(|&x| x as u8).collect())),
            PrimWidth::W16 => Prim::P16(Arc::new(words.iter().map(|&x| x as u16).collect())),
            PrimWidth::W32 => Prim::P32(Arc::new(words.iter().map(|&x| x as u32).collect())),
            PrimWidth::W64 => Prim::P64(Arc::new(words.to_vec())),
        }
    }

    /// Sort a Prod *as one thing*: when every field is a flat Prim and the
    /// whole row's significant bits (`order` label, most significant, then the
    /// fields in order, each sized to its actual value range) fit in 128, pack
    /// each row into a single key, radix the keys, and DECODE the sorted keys
    /// straight back into the sorted columns. No index, no gather, no per-field
    /// labels — one radix and a decode. `None` (fall back) when a field is not
    /// a flat Prim or the packed key would exceed 128 bits.
    pub fn sort_prod_packed(order: &[u64], fields: &[Value], want_labels: bool) -> Option<(Value, Vec<u64>)> {
        let n = order.len();
        let prims: Vec<&Prim> = fields.iter()
            .map(|f| if let Value::Prim(p) = f { Some(p) } else { None })
            .collect::<Option<_>>()?;
        if prims.is_empty() { return None; }

        // Decide pack/no-pack from cheap max-scans (no allocation), so the
        // fallback doesn't pay for column materialization.
        let bits = |max: u64| -> u32 { if max == 0 { 0 } else { 64 - max.leading_zeros() } };
        let field_bits: Vec<u32> = prims.iter().map(|p| bits(prim_max(p))).collect();
        let label_bits = bits(order.iter().copied().max().unwrap_or(0));
        let total: u32 = label_bits + field_bits.iter().sum::<u32>();
        // Pack only into a u64 key. Measured: packing wins for narrow rows
        // (few radix passes) but a wide (≤128-bit) u128 key *loses* to
        // perm+gather (~2× on full-range u64 pairs). Capping at 64 bits keeps
        // this path a strict improvement; wider rows fall back.
        if total > 64 { return None; }

        let cols: Vec<Vec<u64>> = prims.iter().map(|p| prim_words_u64(p)).collect();

        // Shifts: least-significant field last, label most significant.
        let mut shift = 0u32;
        let mut field_shift = vec![0u32; cols.len()];
        for i in (0..cols.len()).rev() { field_shift[i] = shift; shift += field_bits[i]; }
        let label_shift = shift;

        // Build keys in u128 (shifts up to 64 are safe there) then narrow to
        // u64 — `total <= 64` guarantees the fit.
        let mut keys: Vec<u64> = Vec::with_capacity(n);
        for i in 0..n {
            let mut key = (order[i] as u128) << label_shift;
            for f in 0..cols.len() { key |= (cols[f][i] as u128) << field_shift[f]; }
            keys.push(key as u64);
        }
        let keys = radix_lsd_u64(keys);

        // Decode each field from the sorted keys; labels are run-ids over keys.
        let mask = |b: u32| -> u64 { if b >= 64 { u64::MAX } else { (1u64 << b) - 1 } };
        let mut out_fields: Vec<Value> = Vec::with_capacity(cols.len());
        for f in 0..cols.len() {
            let (sh, m) = (field_shift[f], mask(field_bits[f]));
            let words: Vec<u64> = if field_bits[f] == 0 {
                vec![0u64; n]
            } else {
                keys.iter().map(|&k| (k >> sh) & m).collect()
            };
            out_fields.push(Value::Prim(words_to_prim(prims[f].width(), &words)));
        }
        // Labels are run-ids over the sorted keys — skipped when dead.
        let labels = if want_labels {
            let mut labels = vec![0u64; n];
            let mut lbl = 0u64;
            for k in 1..n {
                if keys[k] != keys[k - 1] { lbl += 1; }
                labels[k] = lbl;
            }
            labels
        } else {
            Vec::new()
        };
        Some((crate::ir::value::prod(out_fields), labels))
    }
}

// ---- Labels → run structure (shared: group, unique, sort_seq) ----
mod runs {
    use super::*;

    /// The run structure of a sorted column, from its non-decreasing
    /// `labels`: `bounds_ends` (the run-end offsets — `bounds_var_from_ends`
    /// turns them into List bounds) and `firsts` (the first position of each
    /// run, in sorted order). This is the shared substrate the run-shaped ops
    /// project from — `group` (bundle by `bounds_ends`, distinct keys via
    /// `firsts`), `unique` (gather `firsts`), and — via the run lengths
    /// implicit in `bounds_ends` — `uniq -c` / `reduce-by-key`.
    pub fn run_layout(labels: &[u64]) -> (Vec<u64>, Vec<usize>) {
        let n = labels.len();
        let mut bounds_ends: Vec<u64> = Vec::new();
        let mut firsts: Vec<usize> = Vec::new();
        if n == 0 { return (bounds_ends, firsts); }
        firsts.push(0);
        for k in 1..n {
            if labels[k] != labels[k - 1] { bounds_ends.push(k as u64); firsts.push(k); }
        }
        bounds_ends.push(n as u64);
        (bounds_ends, firsts)
    }

    /// Run-id labels for a permutation of a Prim: compares adjacent sorted
    /// keys (`p[perm[k]]` vs `p[perm[k-1]]`) without materializing the sorted
    /// column. Used by the `sort_blocks` single-block fast path.
    pub fn prim_perm_run_labels(p: &Prim, perm: &[u64]) -> Vec<u64> {
        let n = perm.len();
        let mut out = vec![0u64; n];
        macro_rules! go { ($t:ty) => {{
            let xs = <$t as Storage>::extract(p).expect("prim_perm_run_labels width");
            let mut lbl = 0u64;
            for k in 1..n {
                if xs[perm[k] as usize] != xs[perm[k - 1] as usize] { lbl += 1; }
                out[k] = lbl;
            }
        }};}
        match p {
            Prim::P8(_)  => go!(u8),
            Prim::P16(_) => go!(u16),
            Prim::P32(_) => go!(u32),
            Prim::P64(_) => go!(u64),
        }
        out
    }

    /// Run-id labels for an already-sorted Prim: a non-decreasing partition
    /// that increments on each value change. The refinement of an all-equal
    /// input order by a sorted column.
    pub fn prim_run_labels(v: &Value) -> Vec<u64> {
        let p = match v { Value::Prim(p) => p, _ => return vec![0; v.len()] };
        macro_rules! runs { ($t:ty) => {{
            let xs = <$t as Storage>::extract(p).expect("prim_run_labels width");
            let mut out = Vec::with_capacity(xs.len());
            let mut lbl = 0u64;
            for i in 0..xs.len() {
                if i > 0 && xs[i] != xs[i - 1] { lbl += 1; }
                out.push(lbl);
            }
            out
        }};}
        match p {
            Prim::P8(_)  => runs!(u8),
            Prim::P16(_) => runs!(u16),
            Prim::P32(_) => runs!(u32),
            Prim::P64(_) => runs!(u64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::value::{from_vec, prod, list, bounds_var_from_ends};

    fn perm_to_vec(v: Value) -> Vec<u64> {
        if let Value::Prim(Prim::P64(p)) = v { (*p).to_vec() } else { panic!("not P64") }
    }

    #[test]
    fn sort_prim_u64() {
        let v = from_vec::<u64>(vec![3, 1, 4, 1, 5]);
        let labels = vec![0u64; 5];
        let (perm, new_labels) = sort_blocks(&labels, &v).unwrap();
        // Sorted = [1, 1, 3, 4, 5], positions in input: [1,3,0,2,4].
        assert_eq!(perm, vec![1, 3, 0, 2, 4]);
        // Refined labels: two 1s get the same label, then three singletons.
        assert_eq!(new_labels, vec![0, 0, 1, 2, 3]);
    }

    #[test]
    fn sort_prim_singletons_skip() {
        let v = from_vec::<u64>(vec![1, 2, 3]);
        let labels = vec![0, 1, 2];
        let (perm, _) = sort_blocks(&labels, &v).unwrap();
        assert_eq!(perm, vec![0, 1, 2]);
    }

    #[test]
    fn sort_prod_two_fields() {
        // (a, b) where a = [2, 1, 2, 1], b = [10, 20, 5, 30]
        // Sorting lex: (1, 20), (1, 30), (2, 5), (2, 10)
        // -> perm = [1, 3, 2, 0]
        let v = prod(vec![
            from_vec::<u64>(vec![2, 1, 2, 1]),
            from_vec::<u64>(vec![10, 20, 5, 30]),
        ]);
        let labels = vec![0u64; 4];
        let (perm, _) = sort_blocks(&labels, &v).unwrap();
        assert_eq!(perm, vec![1, 3, 2, 0]);
    }

    #[test]
    fn prod_packed_matches_perm_gather() {
        // The packed-row Prod sort must agree with the perm+gather core
        // (sort_blocks) on both data and labels.
        let cases = vec![
            prod(vec![
                from_vec::<u64>(vec![2, 1, 2, 1, 3, 1]),
                from_vec::<u64>(vec![10, 20, 5, 30, 7, 20]),
            ]),
            prod(vec![
                from_vec::<u8>(vec![1, 1, 0, 0, 1]),
                from_vec::<u64>(vec![9, 3, 9, 3, 3]),
                from_vec::<u32>(vec![100, 200, 50, 50, 10]),
            ]),
        ];
        for v in cases {
            let n = v.len();
            let order = vec![0u64; n];
            let fields = if let Value::Prod(fs) = &v { fs.as_ref().clone() } else { unreachable!() };
            let (got_data, got_labels) = sort_prod_packed(&order, &fields, true).expect("packable");
            let (perm, ref_labels) = sort_blocks(&order, &v).unwrap();
            let perm_usize: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
            let ref_data = gather(&v, &perm_usize).unwrap();
            assert_eq!(got_data, ref_data, "prod data mismatch");
            assert_eq!(got_labels, ref_labels, "prod labels mismatch");
        }
        // A full-range u64 pair needs a >64-bit key and must fall back.
        let wide = prod(vec![
            from_vec::<u64>(vec![u64::MAX, 0, 7]),
            from_vec::<u64>(vec![3, 9, 1]),
        ]);
        let wf = if let Value::Prod(fs) = &wide { fs.as_ref().clone() } else { unreachable!() };
        assert!(sort_prod_packed(&vec![0u64; 3], &wf, true).is_none(), "wide key should fall back");
    }

    #[test]
    fn radix_matches_std_across_widths() {
        // Scrambled inputs at each width; radix must agree with the
        // sort_unstable baseline (and thus with true sorted order).
        let n = 5000usize;
        let p64 = Prim::P64(Arc::new((0..n as u64).map(|i| i.wrapping_mul(2654435761) ^ (i << 17)).collect()));
        let p32 = Prim::P32(Arc::new((0..n as u32).map(|i| i.wrapping_mul(2654435761)).collect()));
        let p16 = Prim::P16(Arc::new((0..n as u16).map(|i| i.wrapping_mul(40503)).collect()));
        let p8  = Prim::P8(Arc::new((0..n).map(|i| (i.wrapping_mul(37)) as u8).collect()));
        for p in [p64, p32, p16, p8] {
            assert_eq!(sort_bytes_radix(p.clone()), sort_bytes_std(p), "radix != std");
        }
    }

    #[test]
    fn radix_handles_empty_and_singleton() {
        assert_eq!(sort_bytes_radix(Prim::P64(Arc::new(vec![]))), Prim::P64(Arc::new(vec![])));
        assert_eq!(sort_bytes_radix(Prim::P64(Arc::new(vec![42]))), Prim::P64(Arc::new(vec![42])));
        // All-zero (nbytes == 0 fast exit).
        assert_eq!(sort_bytes_radix(Prim::P32(Arc::new(vec![0, 0, 0]))), Prim::P32(Arc::new(vec![0, 0, 0])));
    }

    #[test]
    fn sort_seq_returns_sorted_data_and_labels() {
        let v = from_vec::<u64>(vec![3, 1, 4, 1, 5]);
        let order = vec![0u64; 5];
        let (sorted, labels) = sort_seq(&order, &v, true).unwrap();
        assert_eq!(sorted, from_vec::<u64>(vec![1, 1, 3, 4, 5]));
        // Refined labels: the two 1s share a group, then three singletons.
        assert_eq!(labels, vec![0, 0, 1, 2, 3]);
    }

    #[test]
    fn sort_seq_prod_lex_returns_data() {
        let v = prod(vec![
            from_vec::<u64>(vec![2, 1, 2, 1]),
            from_vec::<u64>(vec![10, 20, 5, 30]),
        ]);
        let order = vec![0u64; 4];
        let (sorted, _labels) = sort_seq(&order, &v, true).unwrap();
        // Lex order (1,20),(1,30),(2,5),(2,10) ⇒ rows 1,3,2,0.
        let expected = prod(vec![
            from_vec::<u64>(vec![1, 1, 2, 2]),
            from_vec::<u64>(vec![20, 30, 5, 10]),
        ]);
        assert_eq!(sorted, expected);
    }

    #[test]
    fn sort_poly_op_returns_sorted_prim() {
        let v = from_vec::<u64>(vec![3, 1, 4, 1, 5]);
        let mut st = vec![v];
        let mut env = Vec::new();
        SortPoly.run(&mut st, &mut env).unwrap();
        let out = st.pop().unwrap();
        assert_eq!(out, from_vec::<u64>(vec![1, 1, 3, 4, 5]));
    }

    #[test]
    fn sort_perm_op_returns_p64() {
        let v = from_vec::<u64>(vec![3, 1, 4, 1, 5]);
        let mut st = vec![v];
        let mut env = Vec::new();
        SortPerm.run(&mut st, &mut env).unwrap();
        let p = st.pop().unwrap();
        assert_eq!(perm_to_vec(p), vec![1, 3, 0, 2, 4]);
    }

    #[test]
    fn sort_list_of_prod_lex() {
        // List<Prod[u64, u64]> rows: [(2,5), (1,3)] vs [(1,4)] vs [(2,5), (1,2)].
        // Length-first: row 1 (length 1) < rows 0, 2 (length 2).
        // Within length-2 block: position 0 → row 0 has (first .0 = 2), row 2 has (first .0 = 2). Tie.
        //                       position 1 → row 0 has (.0 = 1, .1 = 3), row 2 has (.0 = 1, .1 = 2).
        //                                    Inner Prod compare: .0 same, .1 differs. Row 2 (.1=2) < row 0 (.1=3).
        // Expected order: row 1 (length 1) first, then row 2 (length 2, smaller at pos 1), then row 0.
        // Perm = [1, 2, 0].
        let f0 = from_vec::<u64>(vec![2, 1,  1,  2, 1]);
        let f1 = from_vec::<u64>(vec![5, 3,  4,  5, 2]);
        let inner = prod(vec![f0, f1]);
        let bnds = bounds_var_from_ends(vec![2, 3, 5]);
        let v = list(bnds, inner);
        let labels = vec![0u64; 3];
        let (perm, _) = sort_blocks(&labels, &v).unwrap();
        assert_eq!(perm, vec![1, 2, 0]);
    }

    #[test]
    fn sort_sum_recurses_into_lanes() {
        use crate::ir::value::{sum, prim_p8};
        // Sum: disc=[0,1,0,1], lanes[0]=Prim<u64>[5,3], lanes[1]=Prim<u64>[1,4].
        // Position 0 (disc 0, lane[0][0]=5), 1 (disc 1, lane[1][0]=1),
        //          2 (disc 0, lane[0][1]=3), 3 (disc 1, lane[1][1]=4).
        // After disc sort (block-stable for ties): positions become [0,2,1,3]
        // (disc 0 first, disc 1 second). After lane refinement:
        //   disc-0 block: lane values [5,3] → sorted [3,5] → perm reorders to [2,0].
        //   disc-1 block: lane values [1,4] → sorted [1,4] → perm stays [1,3].
        // Final perm: [2, 0, 1, 3].
        let disc = prim_p8(vec![0, 1, 0, 1]);
        let lane0 = from_vec::<u64>(vec![5, 3]);
        let lane1 = from_vec::<u64>(vec![1, 4]);
        let v = sum(disc, vec![lane0, lane1]);
        let labels = vec![0u64; 4];
        let (perm, _) = sort_blocks(&labels, &v).unwrap();
        assert_eq!(perm, vec![2, 0, 1, 3]);
    }

    #[test]
    fn sort_list_length_first() {
        // Rows: [3], [1,2], [], [2]. Length-first ordering:
        //   length 0: row 2 → []
        //   length 1: rows 0, 3 → [3], [2] (sorted by value: [2], [3])
        //   length 2: row 1 → [1,2]
        // Expected perm: [2, 3, 0, 1].
        let inner = from_vec::<u64>(vec![3, 1, 2, 2]);
        let bnds = bounds_var_from_ends(vec![1, 3, 3, 4]);
        let v = list(bnds, inner);
        let labels = vec![0u64; 4];
        let (perm, _) = sort_blocks(&labels, &v).unwrap();
        assert_eq!(perm, vec![2, 3, 0, 1]);
    }
}
