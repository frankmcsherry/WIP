//! What every algorithm case shares: random inputs, column builders, timing, and the report line.

use corgi::{dce, immediates, lower_effects, parse_ml, Bounds, Graph, NumOp, Program, Value};
use std::hint::black_box;
use std::time::{Duration, Instant};

/// how a case is run: `rows` rows; `check` only compares against the reference (no timing);
/// `explain` prints the lowered graph; `profile` prints time per op (needs `--features profile`).
pub struct Cfg {
    pub rows: usize,
    pub check: bool,
    pub explain: bool,
    pub profile: bool,
    /// run the program through `corgi::optimize` (peephole, iso cancellation, map fusion, cse, dce)
    /// before `Program` takes it.
    pub optimize: bool,
    /// lists `scale` times longer and rows `scale` times fewer, for the cases over lists of numbers.
    pub scale: usize,
    /// the case names asked for (a case runs when one is part of its name); empty, all of them.
    pub names: Vec<String>,
}

impl Cfg {
    /// how many rows a list case generates.
    pub fn list_rows(&self) -> usize {
        (self.rows / self.scale).max(1)
    }
    /// a list length below `n` scaled: `0..n * scale`.
    pub fn list_len(&self, rng: &mut Rng, n: i64) -> usize {
        rng.below(n * self.scale as i64) as usize
    }
}

/// a small xorshift generator, so inputs are the same on every run.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: i64) -> i64 {
        (self.next() % n as u64) as i64
    }
    /// `len` bytes drawn from `alphabet`.
    pub fn string(&mut self, len: usize, alphabet: &[u8]) -> Vec<u8> {
        (0..len).map(|_| alphabet[self.below(alphabet.len() as i64) as usize]).collect()
    }
}

/// a text column, one row per string: a `List<Int>` of its bytes, held as bytes.
pub fn bytes_col<B: AsRef<[u8]>>(rows: &[B]) -> Value {
    list_col(rows, |xs| Value::u8(xs.iter().flat_map(|r| r.as_ref().iter().copied()).collect()))
}

/// a `List<Int>` column.
pub fn int_lists<B: AsRef<[i64]>>(rows: &[B]) -> Value {
    list_col(rows, |xs| Value::i64(xs.iter().flat_map(|r| r.as_ref().iter().copied()).collect()))
}

fn list_col<B, T: AsRef<[B]>>(rows: &[T], values: impl FnOnce(&[T]) -> Value) -> Value {
    let mut ends = Vec::with_capacity(rows.len());
    let mut end = 0;
    for r in rows {
        end += r.as_ref().len();
        ends.push(end);
    }
    Value::List(Bounds::offsets(ends), Box::new(values(rows)))
}

/// the graph a `Program` runs for `src`: constants as immediates, dead nodes dropped, effects lowered.
pub fn lowered(src: &str) -> Graph<NumOp> {
    lower_effects(&dce(&immediates(&parse_ml(src).unwrap_or_else(|e| panic!("parse: {e}")))))
}

/// best wall time of `f` over enough runs to take about 0.3 s (at least 3).
pub fn best(mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    let start = Instant::now();
    let mut runs = 0;
    while runs < 3 || (start.elapsed() < Duration::from_millis(300) && runs < 1000) {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed());
        runs += 1;
    }
    best
}

/// run one case: compile, check corgi's output against the reference's, then (unless `check`)
/// time both and print one line. `expected` is the reference's output as a corgi value; `rust`
/// recomputes it in plain Rust for timing.
pub fn run_case<R>(cfg: &Cfg, name: &str, what: &str, src: &str, input: Value, expected: Value, mut rust: impl FnMut() -> R) {
    if !cfg.names.is_empty() && !cfg.names.iter().any(|n| name.contains(n.as_str())) {
        return;
    }
    let graph = parse_ml(src).unwrap_or_else(|e| panic!("{name}: {e}"));
    let p = Program::from_graph(if cfg.optimize { corgi::optimize(&graph) } else { graph });
    let rows = input.len();
    let got = p.run(input.clone());
    if got != expected {
        let (g, e) = (corgi::show(&got), corgi::show(&expected));
        let at = g.bytes().zip(e.bytes()).take_while(|(a, b)| a == b).count().saturating_sub(60);
        panic!("{name}: corgi and the reference disagree\n  corgi: …{}\n  rust:  …{}", &g[at..(at + 400).min(g.len())], &e[at..(at + 400).min(e.len())]);
    }
    if cfg.explain {
        println!("== {name}: lowered graph\n{}", corgi::explain::explain(&lowered(src)));
    }
    #[cfg(feature = "profile")]
    if cfg.profile {
        corgi::explain::profile::reset();
        black_box(p.run(input.clone()));
        let report = corgi::explain::profile::report();
        let total: Duration = report.iter().map(|r| r.2).sum();
        println!("== {name}: time per op, {rows} rows, {:.0} ns/row in all", total.as_nanos() as f64 / rows as f64);
        for (op, n, t) in report.iter().take(16) {
            println!("  {:>6.1}%  {:>8.1} ns/row  {:>6} runs  {op}", 100.0 * t.as_secs_f64() / total.as_secs_f64(), t.as_nanos() as f64 / rows as f64, n);
        }
    }
    #[cfg(not(feature = "profile"))]
    if cfg.profile {
        println!("== {name}: --profile needs --features profile");
    }
    if cfg.check {
        println!("{name:<20} ok ({rows} rows)");
        return;
    }
    let c = best(|| { black_box(p.run(black_box(input.clone()))); });
    let r = best(|| { black_box(rust()); });
    let per = |d: Duration| d.as_nanos() as f64 / rows as f64;
    println!("{name:<20} {rows:>8}  corgi {:>8.1}  rust {:>7.1} ns/row  {:>6.2}x  {what}", per(c), per(r), per(c) / per(r));
}
