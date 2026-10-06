//! Adaptive preparation versus fixed-width kernels. Inputs are constructed
//! outside timing and borrowed by every run; output allocation/drop is timed.
//! Median of seven warmed samples. CSV is deliberately easy to compare with a
//! different spike. --smoke checks the cases; --full adds 8M rows.
use corgi::{arrange, eval_graph, hash, ArithOp, BinOp, Builder, Integer,
    IntegerBinary as B, IntegerEncoding as E, IntegerFrame as F, Kind, NumOp, Value};
use std::{hint::black_box, time::Instant};

fn time<T>(mut f: impl FnMut() -> T) -> f64 {
    black_box(f());
    let mut times = Vec::new();
    for _ in 0..7 {
        let t = Instant::now(); black_box(f()); times.push(t.elapsed().as_secs_f64());
    }
    times.sort_by(f64::total_cmp); times[3]
}
fn row<T>(label: &str, n: usize, input: usize, output: usize, plan: &str, f: impl FnMut() -> T) {
    let seconds = time(f);
    println!("{label},{n},{input},{output},{plan},{:.3}", seconds * 1e9 / n as f64);
}
fn native(width: u32, frame: F) -> E { E::Native { width, frame } }
fn add_case(label: &str, ax: &[i128], bx: &[i128], ae: E, be: E) {
    let a = Integer::with_encoding(ax.to_vec(), ae).unwrap();
    let b = Integer::with_encoding(bx.to_vec(), be).unwrap();
    let p = a.binary_plan(&b, B::Add).unwrap();
    let out = a.binary(&b, B::Add).unwrap();
    for i in 0..ax.len() { assert_eq!(out.at(i), ax[i] + bx[i]); }
    let plan = format!("w{}-direct{}-prepare{}", p.encoding.width(), p.direct_inputs, p.prepared_inputs);
    row(label, a.len(), a.payload_bytes() + b.payload_bytes(), out.payload_bytes(), &plan,
        || a.binary(black_box(&b), B::Add).unwrap());
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let sizes = if args.iter().any(|a| a == "--smoke") { vec![4096] }
        else if args.iter().any(|a| a == "--full") { vec![8192, 1 << 20, 1 << 23] }
        else { vec![8192, 1 << 20] };
    println!("case,rows,input_payload_bytes,output_payload_bytes,execution,median_ns_per_row");
    for n in sizes {
        let ax: Vec<_> = (0..n).map(|i| ((i * 73) % 64) as i128).collect();
        let bx: Vec<_> = (0..n).map(|i| ((i * 31) % 64) as i128).collect();
        add_case("add8-compatible", &ax, &bx, native(8, F::Zero), native(8, F::Zero));
        add_case("add8-mixed-width", &ax, &bx, native(8, F::Zero), native(64, F::Zero));
        add_case("add8-both-stored-wide", &ax, &bx, native(64, F::Zero), native(64, F::Zero));
        let a8: Vec<_> = ax.iter().map(|&x| x as u8).collect();
        let b8: Vec<_> = bx.iter().map(|&x| x as u8).collect();
        row("rust-u8-add", n, 2 * n, n, "native8", || a8.iter().zip(black_box(&b8)).map(|(&a, &b)| a + b).collect::<Vec<_>>());
        let a64: Vec<_> = ax.iter().map(|&x| x as u64).collect();
        let b64: Vec<_> = bx.iter().map(|&x| x as u64).collect();
        row("rust-u64-add", n, 16 * n, 8 * n, "native64", || a64.iter().zip(black_box(&b64)).map(|(&a, &b)| a + b).collect::<Vec<_>>());
        let mut g = Builder::<NumOp>::default(); let input = g.input();
        let output = g.add(ArithOp::Bin(BinOp::Add, Kind::U, 64), vec![input]); let g = g.finish(output);
        let v = Value::Prod(vec![Value::u64(a64.clone()), Value::u64(b64.clone())]);
        row("corgi-fixed-u64-add", n, 16 * n, 8 * n, "native64", || eval_graph(&g, black_box(v.clone())));

        let neg: Vec<_> = ax.iter().map(|&x| x - 32).collect();
        add_case("add8-biased-compatible", &neg, &neg, native(8, F::Biased), native(8, F::Biased));
        add_case("add-mixed-frames", &ax, &neg, native(8, F::Zero), native(8, F::Biased));
        let large: Vec<_> = (0..n).map(|i| ((i * 73) % 65536) as i128).collect();
        add_case("add16-promotes32", &large, &large, native(16, F::Zero), native(16, F::Zero));
        let bit: Vec<_> = (0..n).map(|i| (i % 2) as i128).collect();
        add_case("add-packed-bits", &bit, &bit, E::Bits, E::Bits);
        let high: Vec<_> = ax.iter().map(|&x| u64::MAX as i128 - x).collect();
        let minus: Vec<_> = bx.iter().map(|&x| -x - 1).collect();
        add_case("add-full-u64-and-negative", &high, &minus, native(64, F::Zero), native(8, F::Biased));

        let compact = Value::Int(Integer::new(large.clone()));
        let fixed = Value::u64(large.iter().map(|&x| x as u64).collect());
        row("sort-adaptive16", n, 2 * n, 8 * n, "u64-scratch", || arrange::sort_perm(black_box(&compact)));
        row("sort-fixed64", n, 8 * n, 8 * n, "u64-scratch", || arrange::sort_perm(black_box(&fixed)));
        row("hash-adaptive16", n, 2 * n, 8 * n, "canonical", || hash(black_box(&compact)));
        row("hash-fixed64", n, 8 * n, 8 * n, "canonical", || hash(black_box(&fixed)));
        let three = Integer::new(ax.clone());
        let others = Integer::with_encoding(bx.clone(), native(64, F::Biased)).unwrap();
        row("sum3-mixed", n, 10 * n, n, "common8", || Integer::sum(&[&three, &others, &three]).unwrap());
    }
}
