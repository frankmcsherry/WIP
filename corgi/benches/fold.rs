//! Fold and FoldScan throughput: what a lockstep loop costs per element, across the row-length
//! regimes a fold meets. Each case is timed as ns per list element (JW: ns per pair), best of
//! several batches, against a plain per-row Rust loop where one is meaningful.
//!
//!   * one-op body (`(a, x) add`) and identity body, uniform rows stored as offsets and as a stride,
//!     and ragged rows (lengths 0..=20);
//!   * FoldScan with the same one-op body;
//!   * list-valued state (a growing collect, and a fixed four-element list), a product state, and a
//!     body that reads a table (`get`);
//!   * the two Jaro-Winkler examples over 1,000 ASCII pairs shorter than 16 and than 64.
//!
//! Run: `cargo bench --bench fold`. `--smoke` runs every case once on tiny inputs.

use corgi::{Bounds, Program, Value};
use std::hint::black_box;
use std::time::Instant;

/// best-of-`batches` ns per unit for `f`, which does `units` units of work per call. Each batch
/// repeats `f` for about 20 ms (one warm-up call sizes it); with one batch (smoke), `f` runs once.
fn best(units: usize, batches: usize, mut f: impl FnMut()) -> f64 {
    let t = Instant::now();
    f();
    let once = t.elapsed().as_nanos().max(1) as usize;
    let reps = if batches == 1 { 1 } else { (20_000_000 / once).max(1) };
    let mut b = f64::MAX;
    for _ in 0..batches {
        let t = Instant::now();
        for _ in 0..reps {
            f();
        }
        b = b.min(t.elapsed().as_nanos() as f64 / (reps * units) as f64);
    }
    b
}

fn run(p: &Program, input: &Value, units: usize, batches: usize) -> f64 {
    best(units, batches, || {
        black_box(p.run(black_box(input.clone())));
    })
}

/// xorshift, so inputs are deterministic without a dependency.
fn rng(seed: u64) -> impl FnMut() -> u64 {
    let mut s = seed;
    move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    }
}

/// a `List<Int>` with the given row lengths, values small (mod 1000).
fn list(lens: &[usize]) -> (Value, Vec<i64>) {
    let total: usize = lens.iter().sum();
    let vals: Vec<i64> = (0..total as u64).map(|x| (x.wrapping_mul(2654435761) % 1000) as i64).collect();
    let ends: Vec<usize> = lens.iter().scan(0, |e, &l| { *e += l; Some(*e) }).collect();
    (Value::List(Bounds::offsets(ends), Box::new(Value::i64(vals.clone()))), vals)
}

fn compile(src: &str) -> Program {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"))
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let batches = if smoke { 1 } else { 7 };

    let add = compile("(0, input) fold ((a, x) -> (a, x) add)");
    let ident = compile("(0, input) fold ((a, x) -> a)");
    let scan = compile("(0, input) foldscan ((a, x) -> let b = (a, x) add in (b, b))");
    let pair = compile("((0, 0), input) fold ((acc, x) -> ((acc.0, x) add, (acc.1, 1) add))");
    let collect = compile("(0 iota, input) fold ((acc, x) -> (acc, x enlist) append)");
    let four = compile("(4 iota, input) fold ((acc, x) -> (x, acc) cap_list map ((x, a) -> (a, x) add))");
    let table = compile("(0, input) fold ((a, x) -> ((x and 7, 8 iota) get, a) add)");

    println!("-- one-op fold, foldscan: ns per element --");
    let shapes: &[(usize, usize)] = if smoke { &[(8, 3)] } else { &[(1_000, 10), (1_000, 64), (100_000, 10)] };
    for &(n, k) in shapes {
        let lens = vec![k; n];
        let (offsets, vals) = list(&lens);
        let stride = Value::List(Bounds::Stride(k, n), Box::new(Value::i64(vals.clone())));
        let e = n * k;
        let rust = best(e, batches, || {
            let v = black_box(&vals);
            let mut out = Vec::with_capacity(n);
            for r in 0..n {
                out.push(v[r * k..(r + 1) * k].iter().fold(0i64, |a, &x| a.wrapping_add(x)));
            }
            black_box(out);
        });
        println!(
            "{n:>7} x {k:>3}  add offsets {:6.2}  add stride {:6.2}  identity offsets {:6.2}  identity stride {:6.2}  foldscan offsets {:6.2}  foldscan stride {:6.2}  | rust per-row {:5.2}",
            run(&add, &offsets, e, batches),
            run(&add, &stride, e, batches),
            run(&ident, &offsets, e, batches),
            run(&ident, &stride, e, batches),
            run(&scan, &offsets, e, batches),
            run(&scan, &stride, e, batches),
            rust,
        );
    }

    println!("-- ragged rows (lengths uniform in 0..=20): ns per element --");
    let ragged_rows: &[usize] = if smoke { &[8] } else { &[1_000, 100_000] };
    for &n in ragged_rows {
        let mut next = rng(0x9E37_79B9_7F4A_7C15 ^ n as u64);
        let lens: Vec<usize> = (0..n).map(|_| (next() % 21) as usize).collect();
        let (input, _) = list(&lens);
        let e: usize = lens.iter().sum();
        println!(
            "{n:>7} rows  add {:6.2}  identity {:6.2}  foldscan {:6.2}  pair state {:6.2}  table read {:6.2}",
            run(&add, &input, e, batches),
            run(&ident, &input, e, batches),
            run(&scan, &input, e, batches),
            run(&pair, &input, e, batches),
            run(&table, &input, e, batches),
        );
    }

    println!("-- list-valued state, ragged rows: ns per element --");
    let list_cases: &[(usize, u64)] = if smoke { &[(8, 5)] } else { &[(1_000, 21), (1_000, 65), (100_000, 21)] };
    for &(n, m) in list_cases {
        let mut next = rng(0xD1B5_4A32_D192_ED03 ^ n as u64 ^ m);
        let lens: Vec<usize> = (0..n).map(|_| (next() % m) as usize).collect();
        let (input, _) = list(&lens);
        let e: usize = lens.iter().sum();
        println!(
            "{n:>7} rows, lengths 0..{m:<3}  collect (growing list) {:7.2}  four-element list {:6.2}",
            run(&collect, &input, e, batches),
            run(&four, &input, e, batches),
        );
    }

    println!("-- Jaro-Winkler, 1,000 ASCII pairs over five letters: ns per pair --");
    let direct = compile(include_str!("../algorithms/jaro_winkler_direct.col"));
    let by_byte = compile(include_str!("../algorithms/jaro_winkler_by_byte.col"));
    for max in [16u64, 64] {
        let mut next = rng(0x2545_F491_4F6C_DD1D ^ max);
        let pairs = if smoke { 8 } else { 1_000 };
        let mut string = || (0..next() % max).map(|_| b'a' + (next() % 5) as u8).collect::<Vec<u8>>();
        let ps: Vec<(Vec<u8>, Vec<u8>)> = (0..pairs).map(|_| (string(), string())).collect();
        let column = |xs: Vec<&Vec<u8>>| {
            let ends: Vec<usize> = xs.iter().scan(0, |e, x| { *e += x.len(); Some(*e) }).collect();
            Value::List(ends.into(), Box::new(Value::u8(xs.into_iter().flatten().copied().collect())))
        };
        let input = Value::Prod(vec![
            column(ps.iter().map(|p| &p.0).collect()),
            column(ps.iter().map(|p| &p.1).collect()),
        ]);
        println!(
            "lengths < {max:<3}  direct {:8.0}  by_byte {:8.0}",
            run(&direct, &input, pairs, batches),
            run(&by_byte, &input, pairs, batches),
        );
    }
}
