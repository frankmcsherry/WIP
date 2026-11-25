//! SPIKE: string columns from ClickBench under one list form (`CORGI_LISTS`), sizes and kernel times.
//!
//!   CORGI_LISTS=lens CORGI_CLICKBENCH=DIR cargo run --release --example lists_spike

use corgi::{Program, Value};
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

fn read_u64s(path: &Path) -> Vec<u64> {
    std::fs::read(path).unwrap().chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect()
}

fn best(mut f: impl FnMut()) -> Duration {
    let (mut best, start, mut runs) = (Duration::MAX, Instant::now(), 0);
    while runs < 5 || (start.elapsed() < Duration::from_millis(1500) && runs < 100) {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed());
        runs += 1;
    }
    best
}

const KERNELS: [(&str, &str); 8] = [
    ("cut_mask", "input map (s -> s map (c -> ((c, 47) eq, c)))"),
    ("cut", "input map (s -> s map (c -> ((c, 47) eq, c)) cut)"),
    ("len", "input map (s -> s len)"),
    ("filter", "input map (s -> ((s len, 0) gt, s)) filter"),
    ("hash_group", "input map (s -> (s, ())) hash_group len"),
    ("sort", "input sort"),
    ("dedup", "input dedup len"),
    ("reverse", "input map (s -> s) map (xs -> (xs len iota, xs) zip map ((i, x) -> x))"),
];

fn main() {
    let dir = std::path::PathBuf::from(std::env::var("CORGI_CLICKBENCH").expect("CORGI_CLICKBENCH"));
    let form = format!("{}{}{}", std::env::var("CORGI_LISTS").unwrap_or("offsets".into()), std::env::var("CORGI_LENS_LAYOUT").map(|l| format!("/{l}")).unwrap_or_default(), if std::env::var("CORGI_DECODE").as_deref() == Ok("never") { "/nd" } else { "" });
    let rows: usize = std::env::var("ROWS").ok().and_then(|r| r.parse().ok()).unwrap_or(usize::MAX);
    // synthetic bounds only, to time random spans past the caches: SYNTH=rows,maxlen
    if let Ok(spec) = std::env::var("SYNTH") {
        let (rows, max) = spec.split_once(',').map(|(a, b)| (a.parse::<usize>().unwrap(), b.parse::<u64>().unwrap())).unwrap();
        let mut x = 0x1234u64;
        let mut acc = 0;
        let ends: Vec<usize> = (0..rows).map(|_| { x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); acc += ((x >> 33) % (max + 1)) as usize; acc }).collect();
        let bounds: corgi::Bounds = ends.into();
        let idx: Vec<usize> = (0..10_000_000).map(|_| { x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((x >> 33) as usize) % rows }).collect();
        let mut out = Vec::new();
        assert_eq!(bounds.bench_spans_batch(&idx, &mut out), bounds.bench_spans(&idx));
        let d1 = best(|| { black_box(bounds.bench_spans(black_box(&idx))); });
        let d2 = best(|| { black_box(bounds.bench_spans_batch(black_box(&idx), &mut out)); });
        println!("{form:18} synth {rows} rows max {max}: {:.2} bits/row  span_random {:.2} ns  span_batch {:.2} ns",
            8.0 * bounds.heap_bytes() as f64 / rows as f64, d1.as_nanos() as f64 / idx.len() as f64, d2.as_nanos() as f64 / idx.len() as f64);
        return;
    }
    let only = |var: &str, x: &str| std::env::var(var).map_or(true, |v| v.split(',').any(|y| y == x));
    for name in ["SearchPhrase", "MobilePhoneModel", "URL", "Title", "Referer"] {
        if !only("COLS", name) { continue; }
        let mut ends: Vec<usize> = read_u64s(&dir.join(format!("{name}.ends"))).into_iter().map(|e| e as usize).collect();
        ends.truncate(rows);
        let mut bytes = std::fs::read(dir.join(format!("{name}.bytes"))).unwrap();
        bytes.truncate(ends.last().copied().unwrap_or(0));
        let (n, t) = (ends.len(), bytes.len());
        let tb = Instant::now();
        let bounds: corgi::Bounds = black_box(ends).into();
        let build = tb.elapsed();
        let bb = bounds.heap_bytes();
        println!(
            "{form:8} {name:16} rows {n} bytes {t} ({:.1}/row) bounds {bb} B = {:.2} bits/row, {:.1}% of payload; build {:.2} ns/row",
            t as f64 / n as f64,
            8.0 * bb as f64 / n as f64,
            100.0 * bb as f64 / t as f64,
            build.as_nanos() as f64 / n as f64
        );
        if only("KERNELS", "spans") {
            let mut x = 0x5eedu64;
            let idx: Vec<usize> = (0..1_000_000).map(|_| { x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); ((x >> 33) as usize) % n }).collect();
            let seq: Vec<usize> = (0..n).collect();
            let d = best(|| { black_box(bounds.bench_spans(black_box(&idx))); });
            println!("{form:18} {name:16} {:12} {:8.2} ns/row", "span_random", d.as_nanos() as f64 / idx.len() as f64);
            let mut out = Vec::new();
            assert_eq!(bounds.bench_spans_batch(&idx, &mut out), bounds.bench_spans(&idx), "batch and single spans agree");
            let all: Vec<usize> = (0..n).collect();
            assert_eq!(bounds.bench_spans_batch(&all, &mut out), bounds.bench_spans(&all), "batch and single spans agree");
            let d = best(|| { black_box(bounds.bench_spans_batch(black_box(&idx), &mut out)); });
            println!("{form:18} {name:16} {:12} {:8.2} ns/row", "span_batch", d.as_nanos() as f64 / idx.len() as f64);
            let d = best(|| { black_box(bounds.bench_spans(black_box(&seq))); });
            println!("{form:18} {name:16} {:12} {:8.2} ns/row", "span_seq", d.as_nanos() as f64 / n as f64);
        }
        let col = Value::List(bounds, Box::new(Value::u8(bytes)));
        let input = Value::List(vec![n].into(), Box::new(col));
        for (k, src) in KERNELS {
            if !only("KERNELS", k) { continue; }
            let p = Program::compile_ml(src).unwrap_or_else(|e| panic!("{k}: {e}"));
            let d = best(|| { black_box(p.run(black_box(input.clone()))); });
            println!("{form:18} {name:16} {k:12} {:8.2} ns/row", d.as_nanos() as f64 / n as f64);
        }
    }
}
