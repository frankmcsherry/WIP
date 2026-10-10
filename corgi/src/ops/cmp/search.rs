//! `find` on leaf columns: each needle's equal range `[lo, hi)` in its sorted haystack row, by a
//! search per needle rather than the lockstep bisection the structured `find` runs (whose cost is
//! its own per-round arrays: a midpoint, a sign and a window per needle per round, written and
//! read back, with a branch on every sign).
//!
//! Two kernels, chosen per row from that row's data:
//!
//! - **Walk.** A row whose needles are in order and dense (at least one needle per
//!   [`WALK_DENSITY`] haystack rows) is walked: each needle gallops forward from the previous
//!   needle's lower bound ([`walk_ranges`]), `O(log gap)` probes in lines near the last ones.
//! - **Search.** Every other needle gets a branch-free binary search for its lower bound
//!   (Khuong's form: a fixed number of halvings set by the length alone, each a conditional move,
//!   no early exit; never `(lo + hi) / 2`), [`LANES`] needles at a time in lockstep, so that
//!   that many independent chains of loads are in flight. One search at a time waits on each load
//!   in turn: from memory, that is several times slower. The upper bound then gallops forward from
//!   the lower: one probe, in the line just read, when keys are distinct.
//!
//! Needles of a row with at least [`LANES`] of them are searched in groups that share the row;
//! the rest (short rows, and the tail of each long one) are gathered into groups across rows,
//! where each lane carries its own haystack span and the group runs as many halvings as its
//! longest span needs.

use super::survey::{gallop, walk_ranges};
use crate::value::{Bounds, Prim, Rows, Value};
use std::hint::select_unpredictable;

/// Needles searched together. Measured on 64K–16M haystacks (`corgi-find/README.md`): of 4, 8, 16
/// and 32, 16 is the fastest or within 12% of the fastest at every size and density tried.
const LANES: usize = 16;

/// A sorted row is walked when it has at least one needle per this many haystack rows. Measured
/// (`corgi-find/README.md`): the walk takes 0.5–0.8x as long as the search at one needle per
/// haystack row, 0.7–1.2x at one per 4, and up to 7.5x at one per 4,096; on haystacks far larger
/// than the caches it is ahead again around one needle per 64–256 rows (by up to 1.4x) before
/// falling behind.
const WALK_DENSITY: usize = 4;

/// Every needle's `[lo, hi)` relative to its haystack row, for leaf needles and haystacks; `None`
/// for any other shape. Integer needles and haystacks at different storages meet first.
pub(crate) fn find_leaf(nb: &Bounds, needles: &Value, hb: Rows, hay: &Value) -> Option<(Vec<i64>, Vec<i64>)> {
    let (Value::Prim(np), Value::Prim(hp)) = (needles, hay) else { return None };
    let (np, hp) = Prim::meet_ref(np, hp);
    match (&*np, &*hp) {
        (Prim::U8(nv), Prim::U8(hv)) => Some(find_rows(nb, hb, nv, hv)),
        (Prim::I8(nv), Prim::I8(hv)) => Some(find_rows(nb, hb, nv, hv)),
        (Prim::I16(nv), Prim::I16(hv)) => Some(find_rows(nb, hb, nv, hv)),
        (Prim::I32(nv), Prim::I32(hv)) => Some(find_rows(nb, hb, nv, hv)),
        (Prim::I64(nv), Prim::I64(hv)) => Some(find_rows(nb, hb, nv, hv)),
        (Prim::F64(nv), Prim::F64(hv)) => Some(find_rows(nb, hb, nv, hv)),
        _ => unreachable!("meet brings both to one storage"),
    }
}

/// [`find_leaf`] over typed columns: needle row `r` is `needles[nb(r)]`, its haystack `hay[hb(r)]`.
pub(crate) fn find_rows<T: Ord + Copy>(nb: &Bounds, hb: Rows, needles: &[T], hay: &[T]) -> (Vec<i64>, Vec<i64>) {
    let n = needles.len();
    let (mut lo, mut hi) = (vec![0i64; n], vec![0i64; n]);
    if n == 0 {
        return (lo, hi);
    }
    let mut pending = Lanes::new(needles[0]);
    let mut ns = 0usize;
    for r in 0..nb.len() {
        let ne = nb.end(r);
        let (hs, he) = hb.span(r);
        let (xs, row) = (&needles[ns..ne], &hay[hs..he]);
        // an empty haystack row leaves its needles at (0, 0)
        if !xs.is_empty() && !row.is_empty() {
            if xs.len() * WALK_DENSITY >= row.len() && xs.windows(2).all(|w| w[0] <= w[1]) {
                let mut k = ns;
                walk_ranges(xs, row, |l, h| {
                    lo[k] = l as i64;
                    hi[k] = h as i64;
                    k += 1;
                });
            } else {
                let mut k = ns;
                let mut groups = xs.chunks_exact(LANES);
                for group in &mut groups {
                    let group: &[T; LANES] = group.try_into().unwrap();
                    let at = lower_bounds(row, group);
                    for g in 0..LANES {
                        lo[k + g] = at[g] as i64;
                        hi[k + g] = run_end(row, at[g], group[g]) as i64;
                    }
                    k += LANES;
                }
                for &x in groups.remainder() {
                    pending.push(k, x, hs, he);
                    if pending.len == LANES {
                        pending.flush(hay, &mut lo, &mut hi);
                    }
                    k += 1;
                }
            }
        }
        ns = ne;
    }
    pending.flush(hay, &mut lo, &mut hi);
    (lo, hi)
}

/// The lower bounds of `LANES` needles in one sorted row: the first position whose value is not
/// less than the needle. Every lane runs the same halvings (the row's length sets them), each a
/// conditional move; `base + half` is below the row's length throughout.
#[inline(always)]
fn lower_bounds<T: Ord + Copy>(row: &[T], xs: &[T; LANES]) -> [usize; LANES] {
    let mut base = [0usize; LANES];
    let mut n = row.len();
    while n > 1 {
        let half = n / 2;
        for g in 0..LANES {
            base[g] = select_unpredictable(row[base[g] + half] < xs[g], base[g] + half, base[g]);
        }
        n -= half;
    }
    for g in 0..LANES {
        base[g] += (row[base[g]] < xs[g]) as usize;
    }
    base
}

/// One needle's lower bound in one sorted, non-empty row (the same search, one lane).
#[inline(always)]
fn lower_bound<T: Ord + Copy>(row: &[T], x: T) -> usize {
    let (mut base, mut n) = (0usize, row.len());
    while n > 1 {
        let half = n / 2;
        base = select_unpredictable(row[base + half] < x, base + half, base);
        n -= half;
    }
    base + (row[base] < x) as usize
}

/// The end of the run of `x` that starts at `lo` in `row`: galloping, so one probe when `x` is
/// absent or appears once, `O(log run)` when it repeats.
#[inline(always)]
fn run_end<T: Ord + Copy>(row: &[T], lo: usize, x: T) -> usize {
    let mut hi = lo;
    gallop(&mut hi, row.len(), |j| row[j] <= x);
    hi
}

/// Needles from rows too short to fill a group of their own, each with its haystack row's span in
/// the shared payload (`start`, `len`, never empty), searched together once there are `LANES`.
struct Lanes<T> {
    len: usize,
    k: [usize; LANES],
    x: [T; LANES],
    start: [usize; LANES],
    span: [usize; LANES],
}

impl<T: Ord + Copy> Lanes<T> {
    fn new(fill: T) -> Self {
        Lanes { len: 0, k: [0; LANES], x: [fill; LANES], start: [0; LANES], span: [0; LANES] }
    }

    fn push(&mut self, k: usize, x: T, hs: usize, he: usize) {
        let i = self.len;
        (self.k[i], self.x[i], self.start[i], self.span[i]) = (k, x, hs, he - hs);
        self.len += 1;
    }

    /// Search the pending needles and write their answers. A full group runs in lockstep with a
    /// span per lane: as many halvings as the longest span needs, a lane whose span is down to one
    /// position probing it again and staying put (its `half` is 0). A partial group (the last one)
    /// runs one needle at a time.
    fn flush(&mut self, hay: &[T], lo: &mut [i64], hi: &mut [i64]) {
        if self.len == LANES {
            let mut base = self.start;
            let mut n = self.span;
            let longest = n.iter().copied().max().unwrap_or(1);
            let rounds = usize::BITS - (longest - 1).leading_zeros();
            for _ in 0..rounds {
                for g in 0..LANES {
                    let half = n[g] / 2;
                    base[g] = select_unpredictable(hay[base[g] + half] < self.x[g], base[g] + half, base[g]);
                    n[g] -= half;
                }
            }
            for g in 0..LANES {
                let at = base[g] + (hay[base[g]] < self.x[g]) as usize - self.start[g];
                let row = &hay[self.start[g]..self.start[g] + self.span[g]];
                lo[self.k[g]] = at as i64;
                hi[self.k[g]] = run_end(row, at, self.x[g]) as i64;
            }
        } else {
            for g in 0..self.len {
                let row = &hay[self.start[g]..self.start[g] + self.span[g]];
                let at = lower_bound(row, self.x[g]);
                lo[self.k[g]] = at as i64;
                hi[self.k[g]] = run_end(row, at, self.x[g]) as i64;
            }
        }
        self.len = 0;
    }
}
