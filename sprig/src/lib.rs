//! sprig kernel: the columnar substrate (`Data`) and the primitive operations
//! that move data. These functions are the single source of truth -- the Joy
//! language in main.rs drives them, and the benchmarks in bin/bench.rs measure
//! them directly, so the numbers describe the code that actually runs.
//!
//! Five shapes of information motion, plus generators/measures:
//!
//!     map_into                  elementwise scalar op, in place
//!     prefix_sum / reduce_sum   the + monoid (scan / reduce)
//!     gather                    read at a column of indices
//!     scatter_add               write at a column of indices (dual of gather;
//!                               `vals = [1]` makes it a histogram / bincount)
//!     argsort                   the permutation that stably orders a column

use std::fmt;
use std::sync::Arc;

/// The whole value universe: a node of `i64` values and child layers.
///
/// A leaf (`layers` empty) is a primitive column; a scalar is a leaf of length
/// one. Products, sums, lists, and tries are conventions over this, maintained
/// by words in the language, not by the kernel.
///
/// `values` is `Arc`-shared, so `dup` and the `$x` value-load are O(1) -- they
/// bump a refcount, not copy the column. The few verbs that mutate a column in
/// place (the map family, `cat`) call `Arc::make_mut`, which clones the buffer
/// only when it is actually shared. So aliasing stays cheap and value semantics
/// hold: a write through one alias never disturbs another (copy-on-write).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Data {
    pub values: Arc<Vec<i64>>,
    pub layers: Vec<Data>,
}

impl Data {
    pub fn scalar(x: i64) -> Self {
        Data { values: Arc::new(vec![x]), layers: Vec::new() }
    }
    pub fn leaf(values: Vec<i64>) -> Self {
        Data { values: Arc::new(values), layers: Vec::new() }
    }
}

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        for v in self.values.iter() {
            write!(f, " {}", v)?;
        }
        for l in &self.layers {
            write!(f, " {}", l)?;
        }
        write!(f, " }}")
    }
}

// --- primitive kernels (slice level) ---

/// map: `out[i] = f(out[i], a[i])`, in place. Equal lengths; the broadcast cases
/// live in the language wrapper. One branchless loop the compiler can vectorize.
#[inline]
pub fn map_into(out: &mut [i64], a: &[i64], f: impl Fn(i64, i64) -> i64) {
    for (x, &y) in out.iter_mut().zip(a) {
        *x = f(*x, y);
    }
}

/// scan: inclusive prefix sum.
pub fn prefix_sum(values: &[i64]) -> Vec<i64> {
    let mut acc = 0i64;
    values.iter().map(|&v| { acc = acc.wrapping_add(v); acc }).collect()
}

/// reduce: sum.
pub fn reduce_sum(values: &[i64]) -> i64 {
    values.iter().copied().fold(0, i64::wrapping_add)
}

/// gather: `out[i] = src[idx[i]]`. Panics on out-of-range index, by design.
pub fn gather(src: &[i64], idx: &[i64]) -> Vec<i64> {
    idx.iter().map(|&i| src[i as usize]).collect()
}

/// scatter-add: `out[idx[i]] += vals[i]` into a fresh column of length `len`.
/// A length-1 `vals` broadcasts, so `vals = [1]` is a histogram / bincount.
/// Collisions are cheap (a hot slot stays in cache); cost grows with `len`.
pub fn scatter_add(idx: &[i64], len: usize, vals: &[i64]) -> Vec<i64> {
    let mut out = vec![0i64; len];
    if vals.len() == 1 {
        let v = vals[0];
        for &j in idx { out[j as usize] += v; }
    } else {
        for (i, &j) in idx.iter().enumerate() { out[j as usize] += vals[i]; }
    }
    out
}

/// generator: `[ 0 1 ... n-1 ]`.
pub fn iota(n: i64) -> Vec<i64> {
    (0..n).collect()
}

/// merge: linear 2-way merge of two SORTED columns into their sorted union,
/// dropping duplicates. The LSM kept runs as `cat sorted distinct`, which re-sorts
/// the whole merged run every time; but the runs are already sorted, so a single
/// O(|a|+|b|) pass suffices. This is what lets `consolidate` collapse the run pile
/// cheaply -- the seeks then fold over one run, not a per-round stack of them.
pub fn merge2(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    let push = |out: &mut Vec<i64>, v: i64| {
        if out.last() != Some(&v) { out.push(v); }
    };
    while i < a.len() && j < b.len() {
        let (x, y) = (a[i], b[j]);
        if x < y { push(&mut out, x); i += 1; }
        else if y < x { push(&mut out, y); j += 1; }
        else { push(&mut out, x); i += 1; j += 1; }
    }
    while i < a.len() { push(&mut out, a[i]); i += 1; }
    while j < b.len() { push(&mut out, b[j]); j += 1; }
    out
}

/// seek: batched segmented membership by binary search -- the WCO join's core.
///
/// For each `i`, returns 1 if `needle[i]` occurs in the sorted run
/// `hay[beg[i] .. end[i]]`, else 0. This is the galloping/leapfrog intersection
/// reduced to its essence: many independent seeks into a shared sorted backbone,
/// resolved in one pass so the per-op interpreter cost amortizes over all of
/// them. Binary search, not exponential galloping, because the seeks are
/// independent (no shared advancing pointer to exploit); the cost is the same
/// `log` factor and the result is identical.
///
/// This is the fifth shape of motion the README foretold: the one intersection
/// step that is not a word, because a word-level binary search would have to
/// thread the (large) haystack column through an `iter` loop, and `dup` is not
/// yet O(1). Compose it from words for everything around it; spend a verb here.
pub fn searchsorted(hay: &[i64], beg: &[i64], end: &[i64], needle: &[i64]) -> Vec<i64> {
    let m = needle.len();
    // A length-1 `beg` or `end` broadcasts, so intersecting two whole columns
    // (one shared segment) needs no index columns: `beg = {0}`, `end = {len}`.
    let at = |c: &[i64], i: usize| if c.len() == 1 { c[0] } else { c[i] };
    let mut out = vec![0i64; m];
    for i in 0..m {
        let e = at(end, i) as usize;
        let mut lo = at(beg, i) as usize;
        let mut hi = e;
        let v = needle[i];
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if hay[mid] < v { lo = mid + 1; } else { hi = mid; }
        }
        out[i] = (lo < e && hay[lo] == v) as i64;
    }
    out
}

/// seek (position): like `searchsorted`, but returns the lower-bound INDEX -- the
/// number of entries in `hay[beg..end]` strictly less than `needle[i]` (the place
/// it would insert). A sorted index needs this: not "is the key here?" but "where
/// is it?", so its segment can be read from an offsets column. Membership is then
/// `hay[locate] == needle`. This is what makes a sparse pair-keyed index possible
/// where dense offsets cannot reach.
pub fn locate(hay: &[i64], beg: &[i64], end: &[i64], needle: &[i64]) -> Vec<i64> {
    let m = needle.len();
    let at = |c: &[i64], i: usize| if c.len() == 1 { c[0] } else { c[i] };
    let mut out = vec![0i64; m];
    for i in 0..m {
        let mut lo = at(beg, i) as usize;
        let mut hi = at(end, i) as usize;
        let v = needle[i];
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if hay[mid] < v { lo = mid + 1; } else { hi = mid; }
        }
        out[i] = lo as i64;
    }
    out
}

/// seek (galloping): like `locate`, the lower-bound INDEX of `needle[i]` in
/// `hay[beg[i]..end[i]]`, but found by exponential search OUTWARD from `beg[i]`
/// instead of bisecting the whole `[beg,end)`. Identical result to `locate`; the
/// difference is cost. `probe`/`degr` first `locate` the bucket start (`beg`), then
/// want the bucket end -- which is only a bucket-width away (the key's degree, ~19
/// on GALEN), not half the closure. Galloping finds it in O(log degree), not
/// O(log closure), so the second of the two seeks per key stops scaling with the
/// closure size. The first seek (an arbitrary `beg`) still bisects.
pub fn locate_gallop(hay: &[i64], beg: &[i64], end: &[i64], needle: &[i64]) -> Vec<i64> {
    let m = needle.len();
    let at = |c: &[i64], i: usize| if c.len() == 1 { c[0] } else { c[i] };
    let mut out = vec![0i64; m];
    for i in 0..m {
        let lo0 = at(beg, i) as usize;
        let hi = at(end, i) as usize;
        let v = needle[i];
        // Exponential search: double the reach from lo0 until hay overshoots v
        // (or we hit hi). This brackets the answer in (lo0+bound/2, lo0+bound].
        let mut bound = 1usize;
        while lo0 + bound < hi && hay[lo0 + bound] < v { bound <<= 1; }
        // Binary-search the lower bound within the bracket [a, b).
        let mut a = lo0 + (bound >> 1);
        let mut b = (lo0 + bound).min(hi);
        while a < b {
            let mid = a + (b - a) / 2;
            if hay[mid] < v { a = mid + 1; } else { b = mid; }
        }
        out[i] = a as i64;
    }
    out
}

/// Direct LSD radix sort of the values themselves into sorted order, then adjacent
/// dedup -- i.e. `sorted distinct` of one column in a single kernel. The language's
/// `sorted distinct` goes through `argsort` (which reads `values[idx[k]]` -- a random
/// gather every pass) then a `gather` to apply the permutation then a boundary scan;
/// all three are cache-hostile. Here each pass reads the (partly-sorted) values
/// sequentially and writes them into bucket order, so reads stay sequential and no
/// permutation is materialized. Used for the candidate dedup, which sorts ~10x the
/// output volume each fixpoint and dominates the runtime.
pub fn sort_unique(values: &[i64]) -> Vec<i64> {
    let n = values.len();
    if n <= 1 { return values.to_vec(); }
    let signed = values.iter().any(|&v| v < 0);
    let key = |v: i64| if signed { (v as u64) ^ (1u64 << 63) } else { v as u64 };
    let mut orkey = 0u64;
    for &v in values { orkey |= key(v); }

    let mut a: Vec<i64> = values.to_vec();
    let mut b = vec![0i64; n];
    let mut shift = 0u32;
    while shift < 64 && (orkey >> shift) != 0 {
        let mut count = [0u32; 257];
        for &v in &a { count[(((key(v) >> shift) & 0xff) as usize) + 1] += 1; }
        for i in 1..257 { count[i] += count[i - 1]; }
        for &v in &a {
            let bkt = ((key(v) >> shift) & 0xff) as usize;
            b[count[bkt] as usize] = v;
            count[bkt] += 1;
        }
        std::mem::swap(&mut a, &mut b);
        shift += 8;
    }
    a.dedup();
    a
}

/// antijoin: elements of sorted `a` that are NOT in sorted `b`, in order. The `b`
/// pointer only advances, so it is one linear pass -- the merge-based set difference
/// that pairs with `merge2`. `diffL` uses it for the candidate antijoin: after the
/// dedup-sort `a` is sorted, so testing membership against the (sorted) closure run
/// is a merge, not a binary search per candidate. `a` is assumed already deduped.
pub fn diff2(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::new();
    let mut j = 0usize;
    for &x in a {
        while j < b.len() && b[j] < x { j += 1; }
        if !(j < b.len() && b[j] == x) { out.push(x); }
    }
    out
}

/// Stable argsort by LSD radix on the i64 keys, returned as an index column.
///
/// Non-negative keys (the common case: indices, counts, ids) radix on their raw
/// bytes and stop at the highest nonzero byte, so small ranges cost few passes.
/// If any key is negative we flip the sign bit first for correct signed order.
/// Distribution-insensitive: cost is set by key bit-width, not input order.
pub fn argsort(values: &[i64]) -> Vec<i64> {
    let n = values.len();
    if n <= 1 {
        return (0..n as i64).collect();
    }
    let signed = values.iter().any(|&v| v < 0);
    let key = |v: i64| if signed { (v as u64) ^ (1u64 << 63) } else { v as u64 };
    let mut orkey = 0u64;
    for &v in values { orkey |= key(v); }

    let mut idx: Vec<u32> = (0..n as u32).collect();
    let mut tmp = vec![0u32; n];
    let mut shift = 0u32;
    while shift < 64 && (orkey >> shift) != 0 {
        let mut count = [0u32; 257];
        for &v in values { count[(((key(v) >> shift) & 0xff) as usize) + 1] += 1; }
        for i in 1..257 { count[i] += count[i - 1]; }
        for &i in &idx {
            let bkt = ((key(values[i as usize]) >> shift) & 0xff) as usize;
            tmp[count[bkt] as usize] = i;
            count[bkt] += 1;
        }
        std::mem::swap(&mut idx, &mut tmp);
        shift += 8;
    }
    idx.into_iter().map(|i| i as i64).collect()
}
