//! Stage breakdown of the WCO triangle program: time cumulative prefixes of `programs/62` so the
//! differences attribute wall time to stages. `cargo run --release --example wco_stages -- 1000000`.
use corgi::{Program, Value};
use std::time::Instant;

fn main() {
    let n: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1 << 20);
    let text = std::fs::read_to_string("programs/62-wco-triangle.col").expect("run from the corgi directory");
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim_start().starts_with('#') && !l.trim().is_empty()).map(|l| l.trim()).collect();
    // prefixes end at a `let NAME = ... in` line; the prefix program returns NAME's row count via `len`.
    let stages = [
        ("arcs (build the 3N arcs)", "arcs"),
        ("fwd = arcs sort group transpose", "fwd.0"),
        ("bwd = (swap) sort group transpose", "bwd.0"),
        ("fadj/badj = inner lists by ref", "badj"),
        ("pb/pa = find keys", "pa"),
        ("trip = zip zip", "trip"),
        ("ok/good = mask + filter", "good"),
        ("cs = capture + per-anchor body", "cs"),
    ];
    let mut prev = 0.0;
    for (label, name) in stages {
        let var = name.split('.').next().unwrap();
        let upto = lines.iter().position(|l| l.starts_with(&format!("let {var} ="))).expect(name);
        // include continuation lines of a multi-line let (the `cs` body) up to the next `let`.
        let mut end = upto + 1;
        if var == "cs" {
            end = lines.iter().position(|l| l.starts_with("let flat =")).unwrap();
        }
        let prog = format!("{} {} len", lines[..end].join(" "), name);
        let p = Program::compile_ml(&prog).unwrap_or_else(|e| panic!("{name}: {e}"));
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            let _ = p.run_partial(Value::u64(vec![n]));
            best = best.min(t.elapsed().as_secs_f64());
        }
        println!("{:<38} cumulative {:>7.3}s   stage {:>7.3}s", label, best, best - prev);
        prev = best;
    }
    let full: String = lines.join(" ");
    let p = Program::compile_ml(&full).unwrap();
    let t = Instant::now();
    let _ = p.run_partial(Value::u64(vec![n]));
    println!("{:<38} cumulative {:>7.3}s   stage {:>7.3}s", "flatten + len (full program)", t.elapsed().as_secs_f64(), t.elapsed().as_secs_f64() - prev);
}
