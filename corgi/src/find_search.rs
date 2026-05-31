//! PROTOTYPE — the structural-search `find`, the search-dual of the discrimination sort, kept to the
//! side for complexity examination. NOT wired into `CmpOp::Find` (which stays the `compare2` binary
//! search). The contract matches: given a needle `List<X>` and a SORTED haystack `List<X>` (same `X`),
//! return, per needle element, its `equal_range` `(lo,hi)` in the matching haystack row, row-relative.
//!
//! The idea: recurse on the key's type structure exactly as `sort_blocks` does, but at each layer
//! *binary-search* the haystack column over the current range instead of *scanning* to partition.
//! The haystack range `[lo,hi)` is the state threaded down; each layer narrows it; the final range is
//! the answer. Cost is O(probes · depth · log m) — proportional to the needle, logarithmic per layer
//! in the haystack — and it never compares a whole row, so the `Sum` cliff in `compare2` never arises.
//!
//! Where the complexity concentrates (the thing to examine): the `Sum` arm. To recurse from a flat
//! tag-block `[a,b)` into the variant column it must map flat positions to within-variant indices, so
//! it computes a within-offset array per sum node (`within_offsets`, O(column len)). Consecutive
//! same-tag flat positions have consecutive within-offsets, so a contiguous flat block maps to a
//! contiguous variant slice — which is what keeps the recursion clean even for a sum in a SECONDARY
//! key position (a sum under a product whose earlier field is the sort key). A sum on the PRIMARY
//! spine could skip `within_offsets` entirely (tag-block bases come from cumulative variant lengths);
//! the prototype uses the uniform within-offset path for clarity, and that uniform O(m)-per-sum-node
//! pass is the honest cost to weigh against adoption.
//!
//! NOT YET HANDLED: `List`-shaped keys (find over `List<List<X>>`), i.e. a list as the element type —
//! the length-first, variable-span narrowing. The segmentation list (the per-row wrapper) IS handled.

use crate::value::{Prim, Value};
use std::cmp::Ordering;

/// one needle element in flight: `out` is its original index (where the answer is written), `n_pos`
/// its position in the CURRENT needle column, `[lo,hi)` its range in the CURRENT haystack column.
struct Probe {
    out: usize,
    n_pos: usize,
    lo: usize,
    hi: usize,
}

/// within-variant offset of each row: `out[i]` is the index of row `i` inside `variants[tags[i]]`.
/// The same per-lane cursor `gather`/`sort_sum_blocks` compute; here it maps a flat sum position to
/// its variant-column index so the recursion can descend into the lane.
fn within_offsets(tags: &[usize], k: usize) -> Vec<usize> {
    let mut cursor = vec![0usize; k];
    tags.iter().map(|&t| { let p = cursor[t]; cursor[t] += 1; p }).collect()
}

/// leaf `equal_range`: the half-open `[lower,upper)` of positions in `ph[lo..hi)` equal to `pn[npos]`,
/// over a column sorted ascending. `lower` = first `>= needle`, `upper` = first `> needle`.
fn leaf_range(ph: &Prim, lo: usize, hi: usize, pn: &Prim, npos: usize) -> (usize, usize) {
    let lower = {
        let (mut a, mut b) = (lo, hi);
        while a < b {
            let m = (a + b) / 2;
            if ph.cmp_at(m, pn, npos) == Ordering::Less { a = m + 1; } else { b = m; }
        }
        a
    };
    let upper = {
        let (mut a, mut b) = (lo, hi);
        while a < b {
            let m = (a + b) / 2;
            if ph.cmp_at(m, pn, npos) != Ordering::Greater { a = m + 1; } else { b = m; }
        }
        a
    };
    (lower, upper)
}

/// the half-open block of positions in `tags[lo..hi)` equal to `t`, over a tag column sorted ascending
/// within `[lo,hi)` (which holds because the haystack is sorted: at this layer the range is an
/// equal-prefix block, so this field's tags are sorted within it).
fn tag_block(tags: &[usize], lo: usize, hi: usize, t: usize) -> (usize, usize) {
    let lower = {
        let (mut a, mut b) = (lo, hi);
        while a < b { let m = (a + b) / 2; if tags[m] < t { a = m + 1; } else { b = m; } }
        a
    };
    let upper = {
        let (mut a, mut b) = (lo, hi);
        while a < b { let m = (a + b) / 2; if tags[m] <= t { a = m + 1; } else { b = m; } }
        a
    };
    (lower, upper)
}

/// narrow each probe's haystack `[lo,hi)` by the key at this layer. `nv`/`hv` are the needle/haystack
/// columns at the current depth (same shape). On return each probe's range is refined by this layer.
fn narrow(nv: &Value, hv: &Value, probes: &mut [Probe]) {
    match (nv, hv) {
        (Value::Prim(pn), Value::Prim(ph)) => {
            for p in probes.iter_mut() {
                let (lo, hi) = leaf_range(ph, p.lo, p.hi, pn, p.n_pos);
                p.lo = lo;
                p.hi = hi;
            }
        }

        // product = lexicographic: each field narrows the range the previous field left. Positions
        // are unchanged (parallel columns share the row index), so only `[lo,hi)` threads through.
        (Value::Prod(nc), Value::Prod(hc)) => {
            assert_eq!(nc.len(), hc.len(), "find_search: product arity");
            for f in 0..nc.len() {
                narrow(&nc[f], &hc[f], probes);
            }
        }

        // sum = locate the tag block by searching the tag column (never compare a whole row), then
        // recurse into the variant lane. Flat block `[a,b)` maps to the contiguous variant slice
        // `[hw[a], hw[a]+(b-a))`; the recursion's answer maps back by `a + (sub - va_lo)`.
        (Value::Sum(nt, nvars), Value::Sum(ht, hvars)) => {
            assert_eq!(nvars.len(), hvars.len(), "find_search: sum arity");
            let nt_v = nt.usize_vec();
            let ht_v = ht.usize_vec();
            let hw = within_offsets(&ht_v, hvars.len());
            let nw = within_offsets(&nt_v, nvars.len());

            // ── HOW THE OFFSETS WOULD BE USED ───────────────────────────────────────────────
            // `hw` is the O(m)-per-sum-node recompute the design discussion flagged: it maps a flat
            // haystack position to its index within `variants[t]`. Two ways to obtain that index
            // WITHOUT the per-find scan, neither touching the comparator:
            //
            // (1) PRIMARY-SPINE sum — this sum is the leading sort key, so its tag-`t` rows are
            //     GLOBALLY contiguous and `variants[t]` *is* that block. The lane index is then
            //     derivable in O(K) from the cumulative variant lengths, with no offset array:
            //
            //         let block_start: Vec<usize> = hvars.iter()
            //             .scan(0usize, |a, v| { let s = *a; *a += v.len(); Some(s) }).collect();
            //         // ...then at the use site below, in place of `hw[a]`:
            //         let va_lo = a - block_start[t];   // every position in [block_start[t], a) is tag t
            //
            //     Valid ONLY while `[lo,hi)` is aligned to this sum's tag blocks (the primary spine).
            //
            // (2) SECONDARY sum — reached after an earlier key, so tag-`t` rows are NOT globally
            //     contiguous; `a - block_start[t]` is wrong and the rank of `a` is not binary-
            //     searchable. Here the haystack `Sum` must CARRY the offset, built once by the sort
            //     that precedes every `find` and read in O(1). With an optional field on the value
            //     — `Value::Sum(tags, off: Option<Vec<usize>>, variants)` — the arm becomes:
            //
            //         match off {
            //             Some(off) => { let va_lo = off[a]; }   // O(1); off[q] = index of row q in variants[tags[q]]
            //             None      => { /* no index attached → fall back to co-discrimination here */ }
            //         }
            //
            //     `off` is exactly what `within_offsets(&ht_v, hvars.len())` computes, but produced at
            //     sort time and stored, not rebuilt per find. The sort already visits every row, so it
            //     is incremental there; unsearched (or transformed-since-sort) sums carry `None`.
            // ────────────────────────────────────────────────────────────────────────────────

            // group probe indices by the needle's tag at this layer (free grouping — the discrimination).
            let mut by_tag: Vec<Vec<usize>> = vec![Vec::new(); nvars.len()];
            for (idx, p) in probes.iter().enumerate() {
                by_tag[nt_v[p.n_pos]].push(idx);
            }

            for (t, group) in by_tag.iter().enumerate() {
                if group.is_empty() { continue; }
                let mut sub: Vec<Probe> = Vec::with_capacity(group.len());
                let mut link: Vec<(usize, usize, usize)> = Vec::with_capacity(group.len()); // (probe idx, flat a, va_lo)
                for &pi in group {
                    let (lo, hi) = (probes[pi].lo, probes[pi].hi);
                    let (a, b) = tag_block(&ht_v, lo, hi, t);
                    if a == b {
                        // no haystack rows of this tag in range: empty match at the insertion point.
                        probes[pi].lo = a;
                        probes[pi].hi = a;
                        continue;
                    }
                    let va_lo = hw[a]; // ← with offsets: `off[a]` (carried) or `a - block_start[t]` (primary spine); see note above
                    let va_hi = va_lo + (b - a);
                    sub.push(Probe { out: probes[pi].out, n_pos: nw[probes[pi].n_pos], lo: va_lo, hi: va_hi });
                    link.push((pi, a, va_lo));
                }
                if !sub.is_empty() {
                    narrow(&nvars[t], &hvars[t], &mut sub);
                    for (s, &(pi, a, va_lo)) in sub.iter().zip(&link) {
                        probes[pi].lo = a + (s.lo - va_lo);
                        probes[pi].hi = a + (s.hi - va_lo);
                    }
                }
            }
        }

        (Value::List(..), Value::List(..)) => {
            panic!("find_search: List-shaped keys not yet supported (length-first narrowing TODO)")
        }

        _ => panic!("find_search: needle/haystack shape mismatch"),
    }
}

/// the prototype entry. `nb`/`hb` are the per-row bounds (the segmentation list); `nvals`/`hvals` the
/// element columns of shape `X`. Returns `(lo, hi)` per needle element, row-relative — same as `Find`.
pub fn find_search_ranges(nb: &[usize], nvals: &Value, hb: &[usize], hvals: &Value) -> (Vec<u64>, Vec<u64>) {
    assert_eq!(nb.len(), hb.len(), "find_search: row count");
    let n = nvals.len();
    let mut probes: Vec<Probe> = Vec::with_capacity(n);
    let mut base = vec![0usize; n]; // each element's row start, for the row-relative answer
    let (mut ns, mut hs) = (0, 0);
    for r in 0..nb.len() {
        let (ne, he) = (nb[r], hb[r]);
        base[ns..ne].fill(hs); // each element of this row shares its row start
        for i in ns..ne {
            probes.push(Probe { out: i, n_pos: i, lo: hs, hi: he });
        }
        ns = ne;
        hs = he;
    }
    narrow(nvals, hvals, &mut probes);
    let mut lo = vec![0u64; n];
    let mut hi = vec![0u64; n];
    for p in &probes {
        lo[p.out] = (p.lo - base[p.out]) as u64;
        hi[p.out] = (p.hi - base[p.out]) as u64;
    }
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmp::{compare2, segment_labels, sort_blocks};
    use crate::engine::gather;

    /// obviously-correct reference: count `< needle` (lower) and `<= needle` (upper) per row by linear
    /// scan with the structural comparator. For a sorted haystack these ARE the equal_range bounds.
    fn brute(nb: &[usize], nvals: &Value, hb: &[usize], hvals: &Value) -> (Vec<u64>, Vec<u64>) {
        let mut lo = Vec::new();
        let mut hi = Vec::new();
        let (mut ns, mut hs) = (0, 0);
        for r in 0..nb.len() {
            let (ne, he) = (nb[r], hb[r]);
            for i in ns..ne {
                let (mut l, mut u) = (0u64, 0u64);
                for j in hs..he {
                    match compare2(hvals, j, nvals, i) {
                        Ordering::Less => { l += 1; u += 1; }
                        Ordering::Equal => { u += 1; }
                        Ordering::Greater => {}
                    }
                }
                lo.push(l);
                hi.push(u);
            }
            ns = ne;
            hs = he;
        }
        (lo, hi)
    }

    /// sort a haystack's rows in place (the precondition `find` assumes), via the discrimination sort.
    fn sort_rows(bounds: &[usize], vals: &Value) -> Value {
        let (perm, _) = sort_blocks(&segment_labels(bounds), vals);
        gather(vals, &perm)
    }

    fn check(nb: &[usize], nvals: &Value, hb: &[usize], hvals: &Value) {
        let hsorted = sort_rows(hb, hvals);
        let got = find_search_ranges(nb, nvals, hb, &hsorted);
        let want = brute(nb, nvals, hb, &hsorted);
        assert_eq!(got, want);
    }

    fn u(xs: &[u64]) -> Value { Value::u64(xs.to_vec()) }

    #[test]
    fn leaf_key() {
        // one row: needle values found in a sorted u64 haystack.
        check(&[5], &u(&[7, 2, 5, 9, 0]), &[6], &u(&[3, 7, 2, 5, 9, 7]));
    }

    #[test]
    fn leaf_two_rows() {
        check(&[2, 4], &u(&[5, 1, 9, 3]), &[3, 6], &u(&[1, 4, 5, 0, 3, 9]));
    }

    #[test]
    fn product_key() {
        let n = Value::Prod(vec![u(&[2, 1, 2]), u(&[10, 20, 5])]);
        let h = Value::Prod(vec![u(&[2, 1, 2, 1, 3]), u(&[10, 20, 5, 30, 7])]);
        check(&[3], &n, &[5], &h);
    }

    #[test]
    fn sum_key() {
        // needle (t0,7),(t1,2),(t0,5),(t1,9) vs haystack of tagged values.
        let n = Value::sum(vec![0, 1, 0, 1], vec![u(&[7, 5]), u(&[2, 9])]);
        let h = Value::sum(vec![0, 0, 1, 1], vec![u(&[3, 7]), u(&[2, 5])]);
        check(&[4], &n, &[4], &h);
    }

    #[test]
    fn product_of_sum_secondary_key() {
        // field 0 (U64) is the primary key; field 1 is a SUM in secondary position — the case the
        // offset-free shortcut can't take, exercising the within-offset remap.
        let n = Value::Prod(vec![
            u(&[1, 2, 1, 2]),
            Value::sum(vec![0, 1, 1, 0], vec![u(&[5, 8]), u(&[3, 9])]),
        ]);
        let h = Value::Prod(vec![
            u(&[1, 1, 2, 2, 2]),
            Value::sum(vec![0, 1, 0, 1, 1], vec![u(&[5, 8]), u(&[3, 7, 9])]),
        ]);
        check(&[4], &n, &[5], &h);
    }

    #[test]
    fn nested_sum_key() {
        // a sum whose payload is itself a sum — the recursion must descend twice.
        let inner_n = Value::sum(vec![0, 1], vec![u(&[4]), u(&[6])]);
        let n = Value::sum(vec![0, 1], vec![inner_n, u(&[2])]);
        let inner_h = Value::sum(vec![0, 0, 1], vec![u(&[4, 5]), u(&[6])]);
        let h = Value::sum(vec![0, 0, 1, 1], vec![inner_h, u(&[2, 8])]);
        check(&[2], &n, &[4], &h);
    }

    #[test]
    fn scale_sum_key() {
        // many tagged rows, one haystack; agree with the linear reference.
        let m = 300u64;
        let ntag: Vec<usize> = (0..120).map(|i| (i % 3) as usize).collect();
        let htag: Vec<usize> = (0..m).map(|i| (i % 3) as usize).collect();
        let nvar: Vec<Value> = (0..3)
            .map(|t| u(&(0..ntag.iter().filter(|&&x| x == t).count() as u64)
                .map(|k| (k.wrapping_mul(2654435761) >> 5) % 50).collect::<Vec<_>>()))
            .collect();
        let hvar: Vec<Value> = (0..3)
            .map(|t| u(&(0..htag.iter().filter(|&&x| x == t).count() as u64)
                .map(|k| (k.wrapping_mul(40503) ^ (t as u64)) % 50).collect::<Vec<_>>()))
            .collect();
        let n = Value::sum(ntag.clone(), nvar);
        let h = Value::sum(htag.clone(), hvar);
        check(&[ntag.len()], &n, &[htag.len()], &h);
    }
}
