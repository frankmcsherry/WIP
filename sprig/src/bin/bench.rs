//! Broad-spectrum microbenchmarks for sprig's primitive verbs.
//!
//! These call the real kernel functions from the `sprig` library (no copies),
//! so the numbers describe the code the interpreter runs. We sweep each
//! primitive across the cache hierarchy and across data distributions, to see
//! the bandwidth knees, the gather/scatter latency cliffs, the skew behaviour,
//! and where composition costs passes. "hand" rows are tight local baselines.
//!
//! Run with:  cargo run --release --bin bench

use std::hint::black_box;
use std::time::Instant;

use sprig::{argsort, gather, map_into, prefix_sum, reduce_sum, scatter_add};

fn run(name: &str, elems: usize, bytes: usize, iters: u32, mut f: impl FnMut()) {
    for _ in 0..2 { f(); }
    let t = Instant::now();
    for _ in 0..iters { f(); }
    let s = t.elapsed().as_secs_f64() / iters as f64;
    println!("    {:<28} {:>8.3} ms   {:>6.1} GB/s   {:>8.0} M/s",
             name, s * 1e3, bytes as f64 / s / 1e9, elems as f64 / s / 1e6);
}

fn iters_for(elems: usize) -> u32 {
    ((1u64 << 28) / elems.max(1) as u64).clamp(5, 4000) as u32
}

// comparison-sort baseline, for contrast with the kernel's radix argsort.
fn cmp_argsort(values: &[i64]) -> Vec<u32> {
    let mut p: Vec<u32> = (0..values.len() as u32).collect();
    p.sort_by_key(|&i| values[i as usize]);
    p
}

fn rand_idx(n: usize, range: usize) -> Vec<i64> {
    let mut s = 0x9e3779b97f4a7c15u64;
    (0..n).map(|_| {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        (s % range as u64) as i64
    }).collect()
}

fn main() {
    let n = 1usize << 23;            // 64 MB per i64 column
    let a: Vec<i64> = (0..n as i64).collect();
    let b: Vec<i64> = a.iter().map(|x| x.wrapping_mul(2654435761)).collect();
    let mut out = vec![0i64; n];

    println!("\n=== streaming: kernel vs hand-rolled (n = {} M) ===\n", n >> 20);
    let mut bw = b.clone();
    run("map_into (kernel)", n, 3 * n * 8, 20, || {
        map_into(&mut bw, &a, |x, y| x.wrapping_add(y));
        black_box(&bw);
    });
    run("map add (hand)", n, 3 * n * 8, 20, || {
        for i in 0..n { out[i] = b[i].wrapping_add(a[i]); }
        black_box(&out);
    });
    run("reduce_sum (kernel)", n, n * 8, 40, || { black_box(reduce_sum(&a)); });
    run("prefix_sum (kernel)", n, 2 * n * 8, 20, || { black_box(prefix_sum(&a)); });

    println!("\n=== map_into: bandwidth across the cache hierarchy ===\n");
    for &lg in &[10usize, 13, 16, 19, 21, 23, 25] {
        let sz = 1 << lg;
        let aa: Vec<i64> = (0..sz as i64).collect();
        let mut bb: Vec<i64> = aa.clone();
        run(&format!("2^{:<2}  ({:>6} KB/col)", lg, sz * 8 / 1024), sz, 3 * sz * 8, iters_for(sz), || {
            map_into(&mut bb, &aa, |x, y| x.wrapping_add(y));
            black_box(&bb);
        });
    }

    println!("\n=== gather: 4M probes, source size swept (latency cliff) ===\n");
    let probes = 1 << 22;
    for &lg in &[10usize, 13, 16, 19, 21, 23, 25] {
        let sz = 1 << lg;
        let src: Vec<i64> = (0..sz as i64).collect();
        let idx = rand_idx(probes, sz);
        run(&format!("src 2^{:<2} ({:>7} KB)", lg, sz * 8 / 1024), probes, probes * 8, iters_for(probes.max(sz)), || {
            black_box(gather(&src, &idx));
        });
    }

    println!("\n=== scatter_add: 4M updates, target size swept (+ skew) ===\n");
    let updates = 1 << 22;
    let one = [1i64];
    for &lg in &[8usize, 13, 16, 19, 21, 23] {
        let r = 1 << lg;
        let idx = rand_idx(updates, r);
        run(&format!("R=2^{:<2} ({:>7} KB)", lg, r * 8 / 1024), updates, updates * 8, iters_for(updates.max(r)), || {
            black_box(scatter_add(&idx, r, &one));
        });
    }
    let one_slot = vec![0i64; updates];        // every update collides on slot 0
    run("R=4M but all -> slot 0", updates, updates * 8, 10, || {
        black_box(scatter_add(&one_slot, 1 << 22, &one));
    });

    println!("\n=== sort: radix (kernel) vs comparison, across distributions ===\n");
    let m = 1 << 22;
    let random = rand_idx(m, 1 << 30);
    let sorted: Vec<i64> = (0..m as i64).collect();
    let reversed: Vec<i64> = (0..m as i64).rev().collect();
    let few = rand_idx(m, 16);
    for (name, ks) in [("random 30-bit", &random), ("already sorted", &sorted),
                       ("reversed", &reversed), ("few distinct (0..16)", &few)] {
        run(&format!("radix  {:<20}", name), m, m * 8, 8, || { black_box(argsort(ks)); });
    }
    run("cmp    random 30-bit     ", m, m * 8, 4, || { black_box(cmp_argsort(&random)); });
    run("cmp    already sorted    ", m, m * 8, 8, || { black_box(cmp_argsort(&sorted)); });

    println!("\n=== composition: dot product (passes) ===\n");
    let mut scratch = b.clone();
    run("2-pass (map_into; reduce)", n, 3 * n * 8, 20, || {
        map_into(&mut scratch, &a, |x, y| x.wrapping_mul(y));
        black_box(reduce_sum(&scratch));
    });
    run("1-pass (fused, hand)", n, 2 * n * 8, 20, || {
        let mut acc = 0i64;
        for i in 0..n { acc = acc.wrapping_add(b[i].wrapping_mul(a[i])); }
        black_box(acc);
    });

    println!();
}
