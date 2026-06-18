//! Stride spike: how much would O(1) stride-detection (a dynamic mirror of columnar's `Strides`) buy?
//! Sort N fixed-width 8-byte keys two ways in corgi, on the SAME keys:
//!   dense  — `List<U64>` (one row of N u64 keys): the leaf byte-radix path — the ARRAY special case a
//!            stride-detecting list would divert to by reinterpreting a uniform inner list as a leaf.
//!   ragged — `List<List<U8>>` (N inner lists of 8 bytes): the general structural byte/string sort.
//! Both are the SAME bytes; the ragged form just doesn't know its rows are uniform width 8. The
//! ragged/dense ratio is the prize a `Bounds {Stride(k)|Offsets}` recovers for free. Rust's u64 sort
//! is the floor. Run: `cargo bench --bench stride`.

use corgi::{eval_graph, Bounds, Builder, CmpOp, Graph, NumOp, Value};
use std::hint::black_box;
use std::time::{Duration, Instant};

fn scrambled(n: usize) -> Vec<u64> {
    (0..n as u64).map(|i| i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)).collect()
}

fn corgi_t(g: &Graph<NumOp>, arg: &Value, reps: u32) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let a = arg.clone();
        let t = Instant::now();
        black_box(eval_graph(g, black_box(a)));
        best = best.min(t.elapsed());
    }
    best
}

fn sort_graph() -> Graph<NumOp> {
    let mut b = Builder::default();
    let i = b.input();
    let o = b.add(CmpOp::SortList, vec![i]);
    b.finish(o)
}

fn main() {
    for (n, reps) in [(1usize << 20, 10u32), (1 << 23, 5)] {
        let keys = scrambled(n);
        // dense: one row of n u64 keys (each key IS an 8-byte record, stored as a width-8 leaf).
        let dense = Value::List(vec![n].into(), Box::new(Value::u64(keys.clone())));
        // ragged: one row of n inner lists, each the 8 little-endian bytes of a key — same bytes, but
        // the representation carries a full offset per key instead of knowing the stride is 8.
        let mut bytes = Vec::with_capacity(n * 8);
        let mut inner = Vec::with_capacity(n);
        for (k, &key) in keys.iter().enumerate() {
            bytes.extend_from_slice(&key.to_le_bytes());
            inner.push((k + 1) * 8);
        }
        let ragged = Value::List(vec![n].into(), Box::new(Value::List(inner.into(), Box::new(Value::u8(bytes.clone())))));
        // ragged-but-strided: SAME bytes, but the inner bounds declare "uniform width 8" — so O(1)
        // stride detection fires and the sort diverts to the packed-u64 leaf radix (step 2).
        let strided = Value::List(vec![n].into(), Box::new(Value::List(Bounds::Stride(8, n), Box::new(Value::u8(bytes)))));

        let g = sort_graph();
        let d = corgi_t(&g, &dense, reps);
        let r = corgi_t(&g, &ragged, reps);
        let rs = corgi_t(&g, &strided, reps);
        // rust floor: sort the u64 keys directly.
        let mut rt = Duration::MAX;
        for _ in 0..reps {
            let mut v = keys.clone();
            let t = Instant::now();
            v.sort_unstable();
            black_box(&v);
            rt = rt.min(t.elapsed());
        }
        let np = |x: Duration| x.as_nanos() as f64 / n as f64;
        println!(
            "n={n:>9}  dense {:>6.2}  ragged {:>7.2}  strided {:>6.2}  rust {:>6.2} ns  |  ragged/dense {:>5.2}x  strided/dense {:>4.2}x  dense/rust {:>4.2}x",
            np(d), np(r), np(rs), np(rt),
            r.as_secs_f64() / d.as_secs_f64(),
            rs.as_secs_f64() / d.as_secs_f64(),
            d.as_secs_f64() / rt.as_secs_f64(),
        );
    }
}
