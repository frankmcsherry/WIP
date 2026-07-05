//! Reference + measurement for the triangle WCO join on `arc`.
//!
//! Triangle count = number of ordered (a,b,c) with a->b, b->c, c->a. Grouped by
//! the first edge, this is  sum over edges (a->b) of  |out(b) intersect in(a)|,
//! where out(v) and in(v) are v's out- and in-neighbour lists, each sorted.
//!
//! Two computations of the same number, so each checks the other:
//!  * `ground_truth` -- per edge, intersect smaller-into-larger by binary search.
//!  * `pipeline`     -- the batched, columnar form the language port mirrors:
//!                      one `searchsorted` over all seeks at once, no per-edge
//!                      loop, no dense E x D bincount.
//!
//! Run with:  cargo run --release --bin triangle [N]

use std::time::Instant;

use sprig::{argsort, gather, prefix_sum, reduce_sum, scatter_add, searchsorted};

/// The `arc` relation, mirroring the language words: edges 0->(1..N),
/// (1..N)->0, (1..N)->(2..N+1). Returns (src, dst), each of length 3N.
fn gen_arc(n: i64) -> (Vec<i64>, Vec<i64>) {
    let mut src = Vec::with_capacity(3 * n as usize);
    let mut dst = Vec::with_capacity(3 * n as usize);
    for k in 1..=n { src.push(0); dst.push(k); }      // 0 -> k
    for k in 1..=n { src.push(k); dst.push(0); }      // k -> 0
    for k in 1..=n { src.push(k); dst.push(k + 1); }  // k -> k+1
    (src, dst)
}

/// Exclusive prefix sum (offsets from per-node degrees).
fn exclusive(deg: &[i64]) -> Vec<i64> {
    let inc = prefix_sum(deg);
    inc.iter().zip(deg).map(|(&s, &d)| s - d).collect()
}

/// CSR for out- and in-neighbours. Neighbours are sorted within each node's
/// block (we argsort edges by (key, neighbour)), which the seek requires.
struct Csr {
    outdst: Vec<i64>, out_off: Vec<i64>, out_deg: Vec<i64>,
    innbr: Vec<i64>,  in_off: Vec<i64>,  in_deg: Vec<i64>,
}

fn build_csr(src: &[i64], dst: &[i64], d: usize) -> Csr {
    let m = d as i64;
    let out_deg = scatter_add(src, d, &[1]);
    let in_deg = scatter_add(dst, d, &[1]);
    // out: order edges by (src, dst) so each src block is dst-sorted.
    let key_o: Vec<i64> = src.iter().zip(dst).map(|(&s, &t)| s * m + t).collect();
    let perm_o = argsort(&key_o);
    let outdst = gather(dst, &perm_o);
    // in: order edges by (dst, src) so each dst block is src-sorted.
    let key_i: Vec<i64> = src.iter().zip(dst).map(|(&s, &t)| t * m + s).collect();
    let perm_i = argsort(&key_i);
    let innbr = gather(src, &perm_i);
    Csr {
        outdst, out_off: exclusive(&out_deg), out_deg,
        innbr, in_off: exclusive(&in_deg), in_deg,
    }
}

/// Per-edge intersection, smaller-into-larger, by direct binary search.
fn ground_truth(src: &[i64], dst: &[i64], c: &Csr) -> i64 {
    let mut total = 0i64;
    for e in 0..src.len() {
        let (a, b) = (src[e] as usize, dst[e] as usize);
        let ob = c.out_off[b] as usize; let od = c.out_deg[b] as usize;
        let ia = c.in_off[a] as usize;  let id = c.in_deg[a] as usize;
        let (sml, s0, s1, lrg) = if od <= id {
            (&c.outdst[ob..ob + od], ia, ia + id, &c.innbr)
        } else {
            (&c.innbr[ia..ia + id], ob, ob + od, &c.outdst)
        };
        for &v in sml {
            let (mut lo, mut hi) = (s0, s1);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if lrg[mid] < v { lo = mid + 1; } else { hi = mid; }
            }
            if lo < s1 && lrg[lo] == v { total += 1; }
        }
    }
    total
}

/// "Expand by counts": given per-segment counts, return (grp, loc) of length
/// sum(counts), where needle j belongs to segment grp[j] at local index loc[j].
/// This is exactly the word pipeline: exclusive offsets, mark segment starts by
/// scatter, prefix-sum to a running segment id, then j - off[grp] for the local.
fn expand(counts: &[i64]) -> (Vec<i64>, Vec<i64>) {
    let noff = exclusive(counts);
    let m = counts.iter().sum::<i64>() as usize;
    let marks = scatter_add(&noff, m, &[1]);     // a +1 at each segment start
    let grp: Vec<i64> = prefix_sum(&marks).iter().map(|&x| x - 1).collect();
    let off_at: Vec<i64> = gather(&noff, &grp);
    let loc: Vec<i64> = (0..m as i64).map(|j| j - off_at[j as usize]).collect();
    (grp, loc)
}

/// One direction of the batched seek: for the edges selected by `take_out`
/// (true = out(b) is the smaller side), generate every needle and resolve all
/// the seeks in a single `searchsorted`. Returns the match total.
fn seek_pass(src: &[i64], dst: &[i64], c: &Csr, take_out: bool) -> i64 {
    // Select edges by which side is smaller (ties -> the `out` pass). An empty
    // small list contributes nothing, so we drop count==0 edges here; this also
    // keeps every segment offset below the needle count (no scatter past the end).
    let sel: Vec<usize> = (0..src.len()).filter(|&e| {
        let od = c.out_deg[dst[e] as usize];
        let id = c.in_deg[src[e] as usize];
        if take_out { od <= id && od > 0 } else { id < od && id > 0 }
    }).collect();

    // Per selected edge: where its small list starts, the large segment, count.
    let mut s_beg = Vec::with_capacity(sel.len());   // small-list start
    let mut l_beg = Vec::with_capacity(sel.len());   // large-segment start
    let mut l_end = Vec::with_capacity(sel.len());   // large-segment end
    let mut count = Vec::with_capacity(sel.len());   // small-list length
    for &e in &sel {
        let (a, b) = (src[e] as usize, dst[e] as usize);
        if take_out {
            s_beg.push(c.out_off[b]);
            l_beg.push(c.in_off[a]); l_end.push(c.in_off[a] + c.in_deg[a]);
            count.push(c.out_deg[b]);
        } else {
            s_beg.push(c.in_off[a]);
            l_beg.push(c.out_off[b]); l_end.push(c.out_off[b] + c.out_deg[b]);
            count.push(c.in_deg[a]);
        }
    }

    // Expand to one needle per element of every small list.
    let (grp, loc) = expand(&count);
    let small_arr = if take_out { &c.outdst } else { &c.innbr };
    let large_arr = if take_out { &c.innbr } else { &c.outdst };

    let nidx: Vec<i64> = grp.iter().zip(&loc)
        .map(|(&g, &t)| s_beg[g as usize] + t).collect();
    let needle = gather(small_arr, &nidx);
    let beg = gather(&l_beg, &grp);
    let end = gather(&l_end, &grp);

    reduce_sum(&searchsorted(large_arr, &beg, &end, &needle))
}

fn pipeline(src: &[i64], dst: &[i64], c: &Csr) -> i64 {
    seek_pass(src, dst, c, true) + seek_pass(src, dst, c, false)
}

/// Like `seek_pass`, but emits the surviving triangles instead of counting them.
/// A matched needle is a `c` that is in both out(b) and in(a), so (a,b,c) is a
/// directed triangle a->b->c->a; we read `a` and `b` back from the needle's edge.
fn produce_pass(src: &[i64], dst: &[i64], c: &Csr, take_out: bool,
                tri: &mut Vec<(i64, i64, i64)>) {
    let sel: Vec<usize> = (0..src.len()).filter(|&e| {
        let od = c.out_deg[dst[e] as usize];
        let id = c.in_deg[src[e] as usize];
        if take_out { od <= id && od > 0 } else { id < od && id > 0 }
    }).collect();

    let mut s_beg = Vec::with_capacity(sel.len());
    let mut l_beg = Vec::with_capacity(sel.len());
    let mut l_end = Vec::with_capacity(sel.len());
    let mut count = Vec::with_capacity(sel.len());
    for &e in &sel {
        let (a, b) = (src[e] as usize, dst[e] as usize);
        if take_out {
            s_beg.push(c.out_off[b]);
            l_beg.push(c.in_off[a]); l_end.push(c.in_off[a] + c.in_deg[a]);
            count.push(c.out_deg[b]);
        } else {
            s_beg.push(c.in_off[a]);
            l_beg.push(c.out_off[b]); l_end.push(c.out_off[b] + c.out_deg[b]);
            count.push(c.in_deg[a]);
        }
    }

    let (grp, loc) = expand(&count);
    let small_arr = if take_out { &c.outdst } else { &c.innbr };
    let large_arr = if take_out { &c.innbr } else { &c.outdst };
    let nidx: Vec<i64> = grp.iter().zip(&loc)
        .map(|(&g, &t)| s_beg[g as usize] + t).collect();
    let needle = gather(small_arr, &nidx);
    let beg = gather(&l_beg, &grp);
    let end = gather(&l_end, &grp);
    let matched = searchsorted(large_arr, &beg, &end, &needle);

    for i in 0..needle.len() {
        if matched[i] == 1 {
            let e = sel[grp[i] as usize];
            tri.push((src[e], dst[e], needle[i]));
        }
    }
}

fn produce(src: &[i64], dst: &[i64], c: &Csr) -> Vec<(i64, i64, i64)> {
    let mut tri = Vec::new();
    produce_pass(src, dst, c, true, &mut tri);
    produce_pass(src, dst, c, false, &mut tri);
    tri
}

fn main() {
    let n: i64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(3);
    let d = (n + 2) as usize;        // node ids run 0 ..= N+1

    let t = Instant::now();
    let (src, dst) = gen_arc(n);
    let c = build_csr(&src, &dst, d);
    let build_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let gt = ground_truth(&src, &dst, &c);
    let gt_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let pl = pipeline(&src, &dst, &c);
    let pl_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let mut tri = produce(&src, &dst, &c);
    let pr_ms = t.elapsed().as_secs_f64() * 1e3;

    let expect = 3 * (n - 1);
    println!("N = {}   edges = {}   nodes = {}", n, src.len(), d);
    println!("  build    {:>10.3} ms", build_ms);
    println!("  ground   {:>10.3} ms   -> {} (count)", gt_ms, gt);
    println!("  pipeline {:>10.3} ms   -> {} (count)", pl_ms, pl);
    println!("  produce  {:>10.3} ms   -> {} tuples", pr_ms, tri.len());
    println!("  expected 3(N-1)          -> {}", expect);
    assert_eq!(gt, pl, "ground truth and pipeline disagree");
    assert_eq!(pl, expect, "pipeline disagrees with closed form");
    assert_eq!(tri.len() as i64, expect, "produced tuple count is wrong");
    if tri.len() <= 24 {
        tri.sort();
        println!("  triangles (a,b,c):");
        for (a, b, cc) in &tri { println!("    ({}, {}, {})", a, b, cc); }
    }
    println!("  OK");
}
