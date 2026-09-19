//! `input sort dedup` on GALEN-shaped tuples: N rows of (u32, u32, u32) with values below 240K and
//! ~10% duplicates. `cargo run --release --example sortbench -- 20000000`
use corgi::{Program, Value};
use std::time::Instant;
fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(20_000_000);
    let mut x = 0x9E3779B97F4A7C15u64;
    let mut next = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
    let distinct = n * 9 / 10;
    let mut cols: Vec<Vec<u32>> = vec![Vec::with_capacity(n); 3];
    for i in 0..n {
        let seed = if i < distinct { i as u64 } else { next() % distinct as u64 }; // duplicates of earlier rows
        let mut y = seed.wrapping_mul(0x9E3779B97F4A7C15) ^ (seed >> 3);
        for c in cols.iter_mut() {
            y ^= y << 13; y ^= y >> 7; y ^= y << 17;
            c.push((y % 240_000) as u32);
        }
    }
    let v = Value::List(vec![n].into(), Box::new(Value::Prod(cols.into_iter().map(Value::u32).collect())));
    let run = |text: &str, v: &Value, label: &str| {
        let p = Program::compile_ml(text).unwrap();
        for rep in 0..2 {
            let t = Instant::now();
            let out = p.run_partial(v.clone());
            let e = t.elapsed();
            let m = match &out { Value::List(_, vals) => vals.len(), Value::Sum(_, l) => l[0].len(), _ => 0 };
            println!("{label} rep {rep}: {n} rows -> {m} out in {:.3}s  ({:.1} ns/row)", e.as_secs_f64(), e.as_nanos() as f64 / n as f64);
        }
    };
    run("input sort dedup", &v, "flat sort dedup            ");
    // the same rows already ordered by column 0 (as an extension's output is): group is linear,
    // then the rest is sorted within groups
    let sorted0 = Program::compile_ml("input map (t -> (t.0, (t.1, t.2))) sort map (t -> (t.0, t.1.0, t.1.1))").unwrap().run_partial(v.clone());
    run("input sort dedup", &sorted0, "flat sort dedup, presorted0");
    run("input map (t -> (t.0, (t.1, t.2))) group", &sorted0, "group by col0 (sorted)     ");
    run("input map (t -> (t.0, (t.1, t.2))) group map (p -> (p.0, p.1 sort dedup))", &sorted0, "group + segmented sort dedup");
    run("(input map (t -> (t.0, (t.1, t.2))) group map (p -> (p.0, p.1 sort dedup) cap_list) flatten).1 map (q -> (q.0, q.1.0, q.1.1))", &sorted0, "grouped path to flat rows   ");
}
