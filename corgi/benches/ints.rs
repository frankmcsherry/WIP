//! Integer columns whose width is an encoding (`dev/integers.md`): what narrow storage buys sort
//! and `find`, what the overflow policy costs arithmetic, `unweave` without widening its tags, and
//! a decode that views its message instead of copying it.
//!
//! Each line is the best of `reps` runs of one whole program (`eval_graph` of the lowered graph),
//! the input built outside the timer and handed over by `Arc` clone, in ns per row. Rust lines are
//! the hand-written loop over the same data. Inputs come from a fixed xorshift, so runs repeat.
//! `cargo bench --bench ints` (all), or `-- sort find arith unweave codec narrow` to pick sections,
//! `-- --rows N` to change the row count (default 1M), `-- --reps N` the repetitions (default 11).

use corgi::bytes::{length_in_bytes, read_from, read_from_words, write_to};
use corgi::{eval_graph, Bounds, Graph, Int, NumOp, Program, Value};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn graph(src: &str) -> Graph<NumOp> {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}")).lowered().clone()
}

/// best-of-`reps` wall time of one evaluation, the argument cloned (an `Arc` bump) outside the timer
/// and the output dropped inside it.
fn time(g: &Graph<NumOp>, arg: &Value, reps: u32) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let a = arg.clone();
        let t = Instant::now();
        black_box(eval_graph(g, black_box(a)));
        best = best.min(t.elapsed());
    }
    best
}

fn time_fn(reps: u32, mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed());
    }
    best
}

fn ns(d: Duration, n: usize) -> f64 {
    d.as_nanos() as f64 / n as f64
}

fn one_list(v: Value) -> Value {
    Value::List(Bounds::offsets(vec![v.len()]), Box::new(v))
}

/// the same values stored narrow by hand: the narrowest `Prim` width that holds `bits`.
fn by_hand(xs: &[u64], bits: u32) -> Value {
    match bits {
        0..=8 => Value::u8(xs.iter().map(|&x| x as u8).collect()),
        9..=16 => Value::u16(xs.iter().map(|&x| x as u16).collect()),
        17..=32 => Value::u32(xs.iter().map(|&x| x as u32).collect()),
        _ => Value::u64(xs.to_vec()),
    }
}

fn values(n: usize, bits: u32, seed: u64) -> Vec<u64> {
    let mut rng = Rng(seed);
    let mask = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
    (0..n).map(|_| rng.next() & mask).collect()
}

fn sort_section(n: usize, reps: u32) {
    println!("\n## sort: `input sort` over one list of {n} values uniform in [0, 2^b)");
    println!("{:<6} {:>10} {:>14} {:>12} {:>14}", "b", "U64", "narrow Prim", "Int", "(Int width)");
    let g = graph("input sort");
    for bits in [8u32, 16, 20, 32] {
        let xs = values(n, bits, 7 + bits as u64);
        let wide = one_list(Value::u64(xs.clone()));
        let hand = one_list(by_hand(&xs, bits));
        let int = Int::from_u64s(xs.clone());
        let w = int.width();
        let int = one_list(Value::Int(int));
        let (a, b, c) = (time(&g, &wide, reps), time(&g, &hand, reps), time(&g, &int, reps));
        println!("{:<6} {:>10.2} {:>14.2} {:>12.2} {:>14?}", bits, ns(a, n), ns(b, n), ns(c, n), w);
    }

    println!("\n## sort: `input sort` over one list of {n} three-field rows (a, b, c), each uniform in [0, 2^b)");
    println!("   Int fields pack into sort keys by the bits of their spans; Prim fields by their widths.");
    println!("   'adopted' is Int held as 64-bit words with no frame (packs like U64); '+narrow' narrows");
    println!("   those three columns first, inside the timer: what measuring the range on demand costs.");
    println!("{:<6} {:>10} {:>10} {:>10} {:>12} {:>16}", "b", "U64 x3", "U32 x3", "Int x3", "adopted x3", "adopted+narrow");
    for bits in [10u32, 16, 20] {
        let cols: Vec<Vec<u64>> = (0..3).map(|k| values(n, bits, 100 + k)).collect();
        let wide = one_list(Value::Prod(cols.iter().map(|c| Value::u64(c.clone())).collect()));
        let hand = one_list(Value::Prod(cols.iter().map(|c| by_hand(c, 32)).collect()));
        let int = one_list(Value::Prod(cols.iter().map(|c| Value::int_u64(c.clone())).collect()));
        let adopted: Vec<Int> = cols.iter().map(|c| Int::adopt_u64s(c.clone())).collect();
        let adopt = one_list(Value::Prod(adopted.iter().map(|c| Value::Int(c.clone())).collect()));
        let (a, b, c, d) = (time(&g, &wide, reps), time(&g, &hand, reps), time(&g, &int, reps), time(&g, &adopt, reps));
        let e = time_fn(reps, || {
            let v = one_list(Value::Prod(adopted.iter().map(|c| Value::Int(c.narrow())).collect()));
            black_box(eval_graph(&g, v));
        });
        println!("{:<6} {:>10.2} {:>10.2} {:>10.2} {:>12.2} {:>16.2}", bits, ns(a, n), ns(b, n), ns(c, n), ns(d, n), ns(e, n));
    }

    println!("\n## sort + dedup: `input dedup` over {n} three-field rows, each field uniform in [0, 2^b) (many duplicates at b=8)");
    let g = graph("input dedup");
    println!("{:<6} {:>10} {:>14} {:>12}", "b", "U64 x3", "U32 x3", "Int x3");
    for bits in [8u32, 20] {
        let cols: Vec<Vec<u64>> = (0..3).map(|k| values(n, bits, 200 + k)).collect();
        let wide = one_list(Value::Prod(cols.iter().map(|c| Value::u64(c.clone())).collect()));
        let hand = one_list(Value::Prod(cols.iter().map(|c| by_hand(c, 32)).collect()));
        let int = one_list(Value::Prod(cols.iter().map(|c| Value::int_u64(c.clone())).collect()));
        let (a, b, c) = (time(&g, &wide, reps), time(&g, &hand, reps), time(&g, &int, reps));
        println!("{:<6} {:>10.2} {:>14.2} {:>12.2}", bits, ns(a, n), ns(b, n), ns(c, n));
    }
}

fn find_section(n: usize, reps: u32) {
    println!("\n## find: `input find`, {n} needles against a sorted haystack of {n}, all uniform in [0, 2^b)");
    println!("{:<6} {:>10} {:>14} {:>12} {:>22}", "b", "U64", "narrow Prim", "Int", "Int, needles 64-bit");
    let g = graph("input find");
    for bits in [16u32, 20, 32] {
        let mut hay = values(n, bits, 11 + bits as u64);
        hay.sort();
        let needles = values(n, bits, 13 + bits as u64);
        let arg = |h: Value, nd: Value| Value::Prod(vec![one_list(nd), one_list(h)]);
        let wide = arg(Value::u64(hay.clone()), Value::u64(needles.clone()));
        let hand = arg(by_hand(&hay, bits), by_hand(&needles, bits));
        let int = arg(Value::int_u64(hay.clone()), Value::int_u64(needles.clone()));
        // needles held as adopted 64-bit words: re-encoded into the haystack's encoding per call
        let int2 = arg(Value::int_u64(hay.clone()), Value::int_adopt(needles.clone()));
        let (a, b, c, d) = (time(&g, &wide, reps), time(&g, &hand, reps), time(&g, &int, reps), time(&g, &int2, reps));
        println!("{:<6} {:>10.2} {:>14.2} {:>12.2} {:>22.2}", bits, ns(a, n), ns(b, n), ns(c, n), ns(d, n));
    }
}

/// one timed run of a program on its argument (the argument cloned outside the timer).
fn once(g: &Graph<NumOp>, arg: &Value) -> Duration {
    let a = arg.clone();
    let t = Instant::now();
    black_box(eval_graph(g, black_box(a)));
    t.elapsed()
}

fn arith_section(n: usize, reps: u32) {
    println!("\n## arithmetic: {n} rows, `(a, b) add` on U64 (wrapping) vs `add_int` on Int (result frame chosen up front)");
    println!("   every case runs once per round, {reps} rounds interleaved, best kept: the system allocator's");
    println!("   state (whether an 8 MB output faults in fresh pages) otherwise depends on what ran before.");
    println!("{:<44} {:>10} {:>14}", "case", "ns/row", "bytes out/row");
    let add = graph("input add");
    let add_int = graph("input add_int");
    let mul = graph("input mul");
    let mul_int = graph("input mul_int");
    // (name, program, argument, bytes written per row)
    let mut cases: Vec<(String, &Graph<NumOp>, Value, usize)> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    for bits in [7u32, 15, 20, 31, 40] {
        let (a, b) = (values(n, bits, 1), values(n, bits, 2));
        let wide = Value::Prod(vec![Value::u64(a.clone()), Value::u64(b.clone())]);
        let ia = Int::from_u64s(a.clone());
        let ib = Int::from_u64s(b.clone());
        let sum = corgi::int_bin(corgi::IntBin::Add, ia.clone(), ib.clone()).unwrap();
        let int = Value::Prod(vec![Value::Int(ia.clone()), Value::Int(ib.clone())]);
        cases.push((format!("b={bits}: U64 add"), &add, wide.clone(), 8));
        cases.push((format!("b={bits}: Int add_int ({:?} -> {:?})", ia.width(), sum.width()), &add_int, int.clone(), sum.width().bits() as usize / 8));
        cases.push((format!("b={bits}: U64 mul"), &mul, wide, 8));
        match corgi::int_bin(corgi::IntBin::Mul, ia.clone(), ib.clone()) {
            Ok(p) => cases.push((format!("b={bits}: Int mul_int ({:?} -> {:?})", ia.width(), p.width()), &mul_int, int, p.width().bits() as usize / 8)),
            Err(_) => notes.push(format!("b={bits}: Int mul_int is an error: products need {} bits (U64 mul wraps silently)", 2 * bits)),
        }
    }
    // a loose frame: 64-bit words adopted as they came, so the bound says nothing until narrowed
    let (a, b) = (values(n, 20, 1), values(n, 20, 2));
    cases.push(("b=20: Int add_int, both adopted (narrows first)".into(), &add_int, Value::Prod(vec![Value::int_adopt(a.clone()), Value::int_adopt(b.clone())]), 4));
    // a constant: the base moves
    cases.push(("b=20: Int add_int of a constant".into(), &add_int, Value::Prod(vec![Value::int_u64(a.clone()), Value::Int(Int::constant(-7, n))]), 0));
    let chain = graph("let (a, b, c, d) = input in (((a, b) add, c) add, d) add");
    let chain_int = graph("let (a, b, c, d) = input in (((a, b) add_int, c) add_int, d) add_int");
    let cols: Vec<Vec<u64>> = (0..4).map(|k| values(n, 15, 30 + k)).collect();
    let chain_w = Value::Prod(cols.iter().map(|c| Value::u64(c.clone())).collect());
    let chain_i = Value::Prod(cols.iter().map(|c| Value::int_u64(c.clone())).collect());
    cases.push(("b=15: three adds, U64".into(), &chain, chain_w, 24));
    cases.push(("b=15: three adds, Int (results 16, 32, 32 bits)".into(), &chain_int, chain_i, 10));
    let mut best = vec![Duration::MAX; cases.len()];
    for _ in 0..reps {
        for (k, (_, g, arg, _)) in cases.iter().enumerate() {
            best[k] = best[k].min(once(g, arg));
        }
    }
    for ((name, _, _, bytes), d) in cases.iter().zip(&best) {
        println!("{:<44} {:>10.3} {:>14}", name, ns(*d, n), bytes);
    }
    for note in notes {
        println!("{note}");
    }

    println!("\n   Rust loops over the same b=20 data (fresh output):");
    let row = |name: &str, d: Duration| println!("{:<44} {:>10.3}", name, ns(d, n));
    let (a, b) = (values(n, 20, 1), values(n, 20, 2));
    row("Rust u64 wrapping add", time_fn(reps, || {
        let v: Vec<u64> = black_box(&a).iter().zip(black_box(&b)).map(|(&x, &y)| x.wrapping_add(y)).collect();
        black_box(v);
    }));
    row("Rust u64 add, overflow flag per row", time_fn(reps, || {
        let mut any = false;
        let v: Vec<u64> = black_box(&a).iter().zip(black_box(&b)).map(|(&x, &y)| {
            let (s, o) = x.overflowing_add(y);
            any |= o;
            s
        }).collect();
        black_box((v, any));
    }));
    let (a32, b32): (Vec<u32>, Vec<u32>) = (a.iter().map(|&x| x as u32).collect(), b.iter().map(|&x| x as u32).collect());
    row("Rust u32 add, overflow flag per row", time_fn(reps, || {
        let mut any = false;
        let v: Vec<u32> = black_box(&a32).iter().zip(black_box(&b32)).map(|(&x, &y)| {
            let (s, o) = x.overflowing_add(y);
            any |= o;
            s
        }).collect();
        black_box((v, any));
    }));
    row("Rust u32 + u32 -> u32 (no check)", time_fn(reps, || {
        let v: Vec<u32> = black_box(&a32).iter().zip(black_box(&b32)).map(|(&x, &y)| x + y).collect();
        black_box(v);
    }));
    row("Rust max pass over u64 (one operand)", time_fn(reps, || {
        black_box(black_box(&a).iter().copied().max());
    }));
    row("Rust zeroed 8 MB buffer (vec![0u64; n])", time_fn(reps, || {
        black_box(vec![0u64; black_box(n)]);
    }));
}

/// corgi-wins' LCG and tag pattern, so the case is the same data as `sum_err_total`.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 17
    }
}

fn unweave_section(n: usize, reps: u32) {
    println!("\n## unweave then sum one lane: `input unweave .2 fold_add` (the corgi-wins case sum_err_total), {n} rows");
    println!("{:<8} {:>16} {:>18} {:>18} {:>12}", "pattern", "unweave (U64)", "unweave_int (Int)", "Rust filter_map", "u8->u64 widen");
    let old = graph("input unweave .2 fold_add");
    let new = graph("input unweave_int .2 fold_add");
    for (pattern, pct) in [("mixed", 50u64), ("rare", 5)] {
        let mut rng = Lcg(0x5eed);
        let t: Vec<usize> = (0..n).map(|_| (rng.next() % 100 < pct) as usize).collect();
        let vals: Vec<u64> = (0..n).map(|_| rng.next() % 1000).collect();
        let rs: Vec<Result<u64, u64>> = t.iter().zip(&vals).map(|(&t, &v)| if t == 0 { Ok(v) } else { Err(v) }).collect();
        let ok: Vec<u64> = t.iter().zip(&vals).filter(|(t, _)| **t == 0).map(|(_, v)| *v).collect();
        let err: Vec<u64> = t.iter().zip(&vals).filter(|(t, _)| **t == 1).map(|(_, v)| *v).collect();
        let tags8: Vec<u8> = t.iter().map(|&x| x as u8).collect();
        let input = one_list(Value::sum(t, vec![Value::u64(ok), Value::u64(err)]));
        let (a, b) = (time(&old, &input, reps), time(&new, &input, reps));
        let r = time_fn(reps, || {
            black_box(black_box(&rs).iter().filter_map(|r| r.err()).sum::<u64>());
        });
        let w = time_fn(reps, || {
            black_box(black_box(&tags8).iter().map(|&x| x as u64).collect::<Vec<u64>>());
        });
        println!("{:<8} {:>16.3} {:>18.3} {:>18.3} {:>12.3}", pattern, ns(a, n), ns(b, n), ns(r, n), ns(w, n));
    }
}

fn codec_section(n: usize, reps: u32) {
    println!("\n## decode one column of {n} values: copied (`read_from`) vs viewed in the message (`read_from_words`)");
    println!("{:<44} {:>10} {:>12}", "column", "ns/row", "GB/s");
    let row = |name: &str, d: Duration, bytes: usize| {
        println!("{:<44} {:>10.3} {:>12.2}", name, ns(d, n), bytes as f64 / d.as_secs_f64() / 1e9)
    };
    let xs = values(n, 32, 5);
    let cases = [
        ("U64 leaf, copied", Value::u64(xs.clone())),
        ("Int, 32-bit offsets, copied", Value::int_u64(xs.clone())),
        ("Int, 64-bit adopted, copied", Value::int_adopt(xs.clone())),
    ];
    for (name, v) in &cases {
        let mut bytes = Vec::with_capacity(length_in_bytes(v));
        write_to(v, &mut bytes).unwrap();
        row(name, time_fn(reps, || {
            black_box(read_from(black_box(&bytes)).unwrap());
        }), bytes.len());
    }
    for (name, v) in [("Int, 32-bit offsets, viewed (checks span)", Value::int_u64(xs.clone())), ("Int, 64-bit adopted, viewed (nothing to check)", Value::int_adopt(xs.clone()))] {
        let mut bytes = Vec::with_capacity(length_in_bytes(&v));
        write_to(&v, &mut bytes).unwrap();
        let words: Arc<Vec<u64>> = Arc::new(bytes.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect());
        row(name, time_fn(reps, || {
            black_box(read_from_words(black_box(&words)).unwrap());
        }), bytes.len());
    }
}

fn narrow_section(n: usize, reps: u32) {
    println!("\n## narrowing host data: {n} u64 values uniform in [0, 2^20)");
    println!("{:<52} {:>10}", "how", "ns/row");
    let xs = values(n, 20, 9);
    let row = |name: &str, d: Duration| println!("{:<52} {:>10.3}", name, ns(d, n));
    // each run needs its own vector to consume; clone outside the timer
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let v = xs.clone();
        let t = Instant::now();
        black_box(Int::from_u64s(black_box(v)));
        best = best.min(t.elapsed());
    }
    row("Int::from_u64s (range pass + pack in place)", best);
    row("adopt as 64-bit words (no pass)", time_fn(reps, || {
        black_box(Int::adopt_u64s(Vec::new()));
    }));
    row("Rust: to a fresh Vec<u32>", time_fn(reps, || {
        black_box(black_box(&xs).iter().map(|&x| x as u32).collect::<Vec<u32>>());
    }));
    let adopted = Int::adopt_u64s(xs.clone());
    row("narrow an adopted column (range pass + copy)", time_fn(reps, || {
        black_box(black_box(&adopted).narrow());
    }));
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let mut n = 1 << 20;
    let mut reps = 11;
    let mut picks = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--rows" {
            n = args[i + 1].parse().expect("--rows N");
            i += 2;
        } else if args[i] == "--reps" {
            reps = args[i + 1].parse().expect("--reps N");
            i += 2;
        } else {
            picks.push(args[i].clone());
            i += 1;
        }
    }
    let on = |s: &str| picks.is_empty() || picks.iter().any(|p| p == s);
    println!("# integer columns, {n} rows, best of {reps}");
    if on("sort") { sort_section(n, reps); }
    if on("find") { find_section(n, reps); }
    if on("arith") { arith_section(n, reps); }
    if on("unweave") { unweave_section(n, reps); }
    if on("codec") { codec_section(n, reps); }
    if on("narrow") { narrow_section(n, reps); }
}
