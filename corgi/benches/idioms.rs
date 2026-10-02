//! Idiomatic corgi against idiomatic Rust: each case is a corgi program and the Rust a programmer
//! would write for the same task (`Vec<enum>`, `Vec<Vec<_>>`, `Vec<struct>`, `HashSet`,
//! `partition_point`), on the same data, built outside the timer. The rows of `performance.md`
//! that `gaps.rs` does not measure come from here.
//!
//! Each case reports ns per row for both sides (best of 7 samples) and their ratio. Run all cases
//! with `cargo bench --bench idioms`, or name some: `cargo bench --bench idioms -- sum_map find`.
//! `dispatch` reports ns per run of a tiny program instead: the guard on small batches.

use corgi::{Bounds, Program, Value};
use std::hint::black_box;
use std::time::{Duration, Instant};

const ROWS: usize = 1_000_000;

/// best of 7 samples of ns per row; each sample repeats `f` enough to cover about 4M rows.
fn best(rows: usize, mut f: impl FnMut()) -> f64 {
    let reps = ((1usize << 22) / rows).max(1);
    let mut b = f64::MAX;
    for _ in 0..7 {
        let t = Instant::now();
        for _ in 0..reps {
            f();
        }
        b = b.min(t.elapsed().as_nanos() as f64 / (reps * rows) as f64);
    }
    b
}

/// as `best`, timing only `f`: `setup` builds what `f` consumes (a copy for an in-place sort), and
/// what `f` returns is dropped after the timer stops.
fn best_setup<T, U>(rows: usize, mut setup: impl FnMut() -> T, mut f: impl FnMut(T) -> U) -> f64 {
    let reps = ((1usize << 22) / rows).max(1);
    let mut b = f64::MAX;
    for _ in 0..7 {
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            let x = setup();
            let t = Instant::now();
            let out = black_box(f(x));
            total += t.elapsed();
            drop(out);
        }
        b = b.min(total.as_nanos() as f64 / (reps * rows) as f64);
    }
    b
}

fn corgi(src: &str) -> Program {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"))
}

/// ns per row for one run of `p` on `input` (cloned outside the timer, an `Arc` bump).
fn corgi_t(rows: usize, p: &Program, input: &Value) -> f64 {
    best(rows, || {
        black_box(p.run_partial(black_box(input.clone())));
    })
}

/// a deterministic LCG, so both sides see the same data.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 17
    }
}

/// a tag per row: `pct` percent are tag 1.
fn tags(rows: usize, pct: u64, rng: &mut Rng) -> Vec<usize> {
    (0..rows).map(|_| (rng.next() % 100 < pct) as usize).collect()
}

/// `Result<u64, u64>` rows and the same rows as a corgi sum (Ok in lane 0, Err in lane 1). Values
/// are at least 10, so subtracting one never underflows (corgi's `sub` checks; Rust's wraps).
fn results(rows: usize, pct: u64, modulus: u64, rng: &mut Rng) -> (Vec<Result<u64, u64>>, Value) {
    let t = tags(rows, pct, rng);
    let vals: Vec<u64> = (0..rows).map(|_| rng.next() % modulus + 10).collect();
    let rs: Vec<Result<u64, u64>> = t.iter().zip(&vals).map(|(&t, &v)| if t == 0 { Ok(v) } else { Err(v) }).collect();
    let lane = |want: usize| Value::u64(t.iter().zip(&vals).filter(|(t, _)| **t == want).map(|(_, v)| *v).collect());
    (rs, Value::sum(t.clone(), vec![lane(0), lane(1)]))
}

/// a column of one row holding `inner` as a list.
fn one_row(rows: usize, inner: Value) -> Value {
    Value::List(Bounds::offsets(vec![rows]), Box::new(inner))
}

/// `Vec<Vec<u64>>` rows of random length `0..=max`, and the same rows as a corgi list column.
fn lists(rows: usize, max: u64, rng: &mut Rng) -> (Vec<Vec<u64>>, Value) {
    let ls: Vec<Vec<u64>> = (0..rows)
        .map(|_| {
            let l = rng.next() % (max + 1);
            (0..l).map(|_| rng.next() % 1000).collect()
        })
        .collect();
    let (mut ends, mut flat) = (Vec::with_capacity(rows), Vec::new());
    for l in &ls {
        flat.extend_from_slice(l);
        ends.push(flat.len());
    }
    (ls, Value::List(Bounds::offsets(ends), Box::new(Value::u64(flat))))
}

fn report(case: &str, param: &str, rust: f64, corgi: f64, what: &str) {
    println!("{case:<14} {param:<7} corgi {corgi:8.2}  rust {rust:8.2} ns/row  {:6.2}x  {what}", corgi / rust);
}

/// `Result<u64, u64>`: subtract one from Ok, add one to Err.
fn sum_map(pct: u64, param: &str) {
    let mut rng = Rng(0x5eed);
    let (rs, input) = results(ROWS, pct, 1_000_000, &mut rng);
    let p = corgi("input map_variant 0 (x -> (x, 1u64) sub) map_variant 1 (e -> (e, 1u64) add)");
    let r = best(ROWS, || {
        black_box(black_box(&rs).iter().map(|r| match r { Ok(x) => Ok(x - 1), Err(e) => Err(e + 1) }).collect::<Vec<_>>());
    });
    report("sum_map", param, r, corgi_t(ROWS, &p, &input), "a match whose arms are one op");
}

/// `Result<u64, u64>` with a few ops in each arm.
fn sum_heavy(pct: u64, param: &str) {
    let mut rng = Rng(0x5eed);
    let (rs, input) = results(ROWS, pct, 1_000_000, &mut rng);
    let p = corgi("input map_variant 0 (x -> ((((x, 3u64) mul, 7u64) add) shr 1, 11u64) mul) map_variant 1 (e -> ((e shr 2, 5u64) mul, e) add)");
    let r = best(ROWS, || {
        black_box(black_box(&rs).iter().map(|r| match r { Ok(x) => Ok(((x * 3 + 7) >> 1) * 11), Err(e) => Err((e >> 2) * 5 + e) }).collect::<Vec<_>>());
    });
    report("sum_heavy", param, r, corgi_t(ROWS, &p, &input), "a match with a few ops per arm");
}

/// `Result<u64, u64>` to `u64`: the arms' results merged back into one column by tag.
fn sum_merge() {
    let mut rng = Rng(0x5eed);
    let (rs, input) = results(ROWS, 50, 1_000_000, &mut rng);
    let p = corgi("input match (0 (x -> (x, 1u64) sub), 1 (e -> (e, 1u64) add))");
    let r = best(ROWS, || {
        black_box(black_box(&rs).iter().map(|r| match r { Ok(x) => x - 1, Err(e) => e + 1 }).collect::<Vec<u64>>());
    });
    report("sum_merge", "50%", r, corgi_t(ROWS, &p, &input), "a match whose arms merge into one column");
}

/// the area of a circle or a rectangle, in `f64` (corgi's floats are order-preserving encodings).
#[allow(clippy::approx_constant)] // the same literal as the corgi program's
fn shapes() {
    #[derive(Clone, Copy)]
    enum Shape {
        Circle(f64),
        Rect(f64, f64),
    }
    fn enc(f: f64) -> u64 {
        let b = f.to_bits();
        if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
    }
    let mut rng = Rng(0x5eed);
    let t = tags(ROWS, 50, &mut rng);
    let f = |rng: &mut Rng| (rng.next() % 10_000) as f64 / 100.0 + 0.5;
    let shapes: Vec<Shape> = t.iter().map(|&t| if t == 0 { Shape::Circle(f(&mut rng)) } else { Shape::Rect(f(&mut rng), f(&mut rng)) }).collect();
    let (mut r, mut w, mut h) = (Vec::new(), Vec::new(), Vec::new());
    for s in &shapes {
        match *s {
            Shape::Circle(x) => r.push(enc(x)),
            Shape::Rect(a, b) => {
                w.push(enc(a));
                h.push(enc(b));
            }
        }
    }
    let input = Value::sum(t, vec![Value::u64(r), Value::Prod(vec![Value::u64(w), Value::u64(h)])]);
    let p = corgi("input match (0 (r -> ((r, r) mul_f64, 3.14159f64) mul_f64), 1 ((w, h) -> (w, h) mul_f64))");
    let rt = best(ROWS, || {
        black_box(black_box(&shapes).iter().map(|s| match *s { Shape::Circle(r) => r * r * 3.14159, Shape::Rect(w, h) => w * h }).collect::<Vec<f64>>());
    });
    report("shapes", "50%", rt, corgi_t(ROWS, &p, &input), "a two-variant match in f64, merged into one column");
}

/// the sum of the Err payloads: Rust reads every row, corgi reads the Err lane.
fn sum_err_total(pct: u64, param: &str) {
    let mut rng = Rng(0x5eed);
    let (rs, inner) = results(ROWS, pct, 1000, &mut rng);
    let input = one_row(ROWS, inner);
    let p = corgi("input unweave .2 fold_add");
    let r = best(ROWS, || {
        black_box(black_box(&rs).iter().filter_map(|r| r.err()).sum::<u64>());
    });
    report("sum_err_total", param, r, corgi_t(ROWS, &p, &input), "the sum of a Result column's Err payloads");
}

/// one field of a struct of eight `u64`, summed over all rows.
fn field_sum() {
    #[derive(Clone, Copy)]
    struct Row {
        _a: u64, _b: u64, c: u64, _d: u64, _e: u64, _f: u64, _g: u64, _h: u64,
    }
    let mut rng = Rng(0x5eed);
    let data: Vec<Row> = (0..ROWS)
        .map(|_| Row { _a: rng.next(), _b: rng.next(), c: rng.next() % 1000, _d: rng.next(), _e: rng.next(), _f: rng.next(), _g: rng.next(), _h: rng.next() })
        .collect();
    let cols: Vec<Value> = (0..8).map(|k| Value::u64(data.iter().map(|r| if k == 2 { r.c } else { r._a }).collect())).collect();
    let input = one_row(ROWS, Value::Prod(cols));
    let p = corgi("input map (r -> r.2) fold_add");
    let r = best(ROWS, || {
        black_box(black_box(&data).iter().map(|r| r.c).sum::<u64>());
    });
    report("field_sum", "", r, corgi_t(ROWS, &p, &input), "one field of an eight-field struct, summed");
}

/// an enum with one small and one wide variant: add one to the small variant's payload. Rust's
/// rows are as wide as the widest variant, and Rust updates them in place.
fn sum_wide(pct: u64, param: &str) {
    #[derive(Clone, Copy)]
    enum Msg {
        Small(u64),
        Big([u64; 7]),
    }
    let mut rng = Rng(0x5eed);
    let t = tags(ROWS, pct, &mut rng);
    let msgs: Vec<Msg> = t.iter().map(|&t| if t == 0 { Msg::Small(rng.next() % 1000) } else { Msg::Big([rng.next(); 7]) }).collect();
    let small: Vec<u64> = msgs.iter().filter_map(|m| if let Msg::Small(x) = m { Some(*x) } else { None }).collect();
    let big: Vec<Value> = (0..7).map(|k| Value::u64(msgs.iter().filter_map(|m| if let Msg::Big(a) = m { Some(a[k]) } else { None }).collect())).collect();
    let input = Value::sum(t, vec![Value::u64(small), Value::Prod(big)]);
    let p = corgi("input map_variant 0 (x -> x add_u64 1)");
    let mut v = msgs.clone();
    let r = best(ROWS, || {
        for m in black_box(&mut v).iter_mut() {
            if let Msg::Small(x) = m {
                *x += 1
            }
        }
    });
    report("sum_wide", param, r, corgi_t(ROWS, &p, &input), "an enum with one small and one 56-byte variant, the small one updated");
}

/// a per-row sum, and a per-row count of elements above a threshold, over `Vec<Vec<u64>>`.
fn list_reduce(max: u64) {
    let mut rng = Rng(0x5eed);
    let (ls, input) = lists(ROWS, max, &mut rng);
    let param = format!("0..={max}");
    let p = corgi("input fold_add");
    let r = best(ROWS, || {
        black_box(black_box(&ls).iter().map(|l| l.iter().sum::<u64>()).collect::<Vec<u64>>());
    });
    report("list_sum", &param, r, corgi_t(ROWS, &p, &input), "each row's list summed");
    let p = corgi("input map (x -> x gt 500) fold_add");
    let r = best(ROWS, || {
        black_box(black_box(&ls).iter().map(|l| l.iter().filter(|&&x| x > 500).count() as u64).collect::<Vec<u64>>());
    });
    report("list_count", &param, r, corgi_t(ROWS, &p, &input), "each row's elements above a threshold, counted");
}

/// sort a column of lists (structural order: length, then elements). Rust sorts its own copy in
/// place; the copy is made outside the timer.
fn sort_lists(max: u64) {
    let mut rng = Rng(0x5eed);
    let (ls, inner) = lists(ROWS, max, &mut rng);
    let input = one_row(ROWS, inner);
    let p = corgi("input sort");
    let r = best_setup(ROWS, || ls.clone(), |mut v| {
        v.sort_unstable_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        v
    });
    report("sort_lists", &format!("0..={max}"), r, corgi_t(ROWS, &p, &input), "a column of lists, sorted");
}

/// sort a column of `Result<u64, u64>`. Rust sorts its own copy in place.
fn sort_results() {
    let mut rng = Rng(0x5eed);
    let (rs, inner) = results(ROWS, 50, u64::MAX >> 17, &mut rng);
    let input = one_row(ROWS, inner);
    let p = corgi("input sort");
    let r = best_setup(ROWS, || rs.clone(), |mut v| {
        v.sort_unstable();
        v
    });
    report("sort_results", "50%", r, corgi_t(ROWS, &p, &input), "a column of Results, sorted");
}

/// count the distinct `(u64, u64)` pairs.
fn distinct_pairs() {
    let mut rng = Rng(0x5eed);
    let pairs: Vec<(u64, u64)> = (0..ROWS).map(|_| (rng.next() % 1000, rng.next() % 1000)).collect();
    let input = one_row(ROWS, Value::Prod(vec![Value::u64(pairs.iter().map(|p| p.0).collect()), Value::u64(pairs.iter().map(|p| p.1).collect())]));
    let p = corgi("input dedup len");
    let r = best(ROWS, || {
        black_box(black_box(&pairs).iter().collect::<std::collections::HashSet<_>>().len());
    });
    report("distinct_pairs", "", r, corgi_t(ROWS, &p, &input), "distinct pairs counted (Rust: HashSet)");
}

/// the equal range of each needle in a sorted haystack of `ROWS` keys (each key four times).
fn find(sorted: bool, param: &str) {
    let mut rng = Rng(0x5eed);
    let hay: Vec<u64> = (0..ROWS as u64).map(|x| x >> 2).collect();
    let mut needles: Vec<u64> = (0..ROWS / 16).map(|_| rng.next() % (ROWS as u64 / 4)).collect();
    if sorted {
        needles.sort_unstable();
    }
    let input = Value::Prod(vec![one_row(needles.len(), Value::u64(needles.clone())), one_row(ROWS, Value::u64(hay.clone()))]);
    let p = corgi("let (n, h) = input in (n, h) find");
    let n = needles.len();
    let r = best(n, || {
        let (ns, hs) = (black_box(&needles), black_box(&hay));
        black_box(ns.iter().map(|x| (hs.partition_point(|y| y < x), hs.partition_point(|y| y <= x))).collect::<Vec<_>>());
    });
    report("find", param, r, corgi_t(n, &p, &input), "equal ranges of 62,500 needles in 1M sorted keys (Rust: partition_point), ns per needle");
}

/// ns per run of a chain of `k` in-place adds on a one-element column: the per-run and per-op
/// cost a small batch pays. No Rust side; this is a guard, not a comparison.
fn dispatch() {
    let input = Value::u64(vec![1]);
    let mut line = String::from("dispatch       k ops   ns per run:");
    for k in [1usize, 2, 4, 8, 16, 32] {
        let mut src = String::from("let x = input in ");
        for _ in 0..k {
            src.push_str("let x = x add_u64 1 in ");
        }
        src.push('x');
        let p = corgi(&src);
        let mut b = f64::MAX;
        for _ in 0..7 {
            let t = Instant::now();
            for _ in 0..100_000 {
                black_box(p.run_partial(black_box(input.clone())));
            }
            b = b.min(t.elapsed().as_nanos() as f64 / 100_000.0);
        }
        line.push_str(&format!("  k={k}: {b:.0}"));
    }
    println!("{line}");
}

fn main() {
    let wanted: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with('-')).collect();
    let runs = |c: &str| wanted.is_empty() || wanted.iter().any(|w| w == c);
    if runs("sum_map") { sum_map(50, "50%"); sum_map(5, "5%"); }
    if runs("sum_heavy") { sum_heavy(50, "50%"); sum_heavy(5, "5%"); }
    if runs("sum_merge") { sum_merge(); }
    if runs("shapes") { shapes(); }
    if runs("sum_err_total") { sum_err_total(50, "50%"); sum_err_total(5, "5%"); }
    if runs("field_sum") { field_sum(); }
    if runs("sum_wide") { sum_wide(50, "50%"); sum_wide(5, "5%"); }
    if runs("list") { for m in [4, 16, 64] { list_reduce(m); } }
    if runs("sort_lists") { for m in [2, 4, 16] { sort_lists(m); } }
    if runs("sort_results") { sort_results(); }
    if runs("distinct_pairs") { distinct_pairs(); }
    if runs("find") { find(true, "sorted"); find(false, "random"); }
    if runs("dispatch") { dispatch(); }
}
