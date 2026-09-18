//! Run the WCO triangle program (`programs/62-wco-triangle.col`) at scale and time it:
//! `cargo run --release --example wco -- 1000000`. Prints the triangle count (3(N-1)) and wall time.
use corgi::{show, Program, Value};
use std::time::Instant;

fn main() {
    let n: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1 << 20);
    let text = std::fs::read_to_string("programs/62-wco-triangle.col").expect("run from the corgi directory");
    let prog: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#') && !l.trim().is_empty())
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" ");
    let p = Program::compile_ml(&prog).unwrap_or_else(|e| panic!("parse: {e}"));
    for rep in 0..3 {
        let t = Instant::now();
        let out = p.run_partial(Value::u64(vec![n]));
        let el = t.elapsed();
        println!("N={n} rep={rep}: {} in {:.3}s  ({:.1} ns per arc)", show(&out), el.as_secs_f64(), el.as_nanos() as f64 / (3 * n) as f64);
    }
}
