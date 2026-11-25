//! Lists of leaves through the kernels that order and move rows: the merge survey, the sort, the
//! comparisons, hashing, and the gathers and select. Rows are ragged or all one length, held as
//! bytes or as words, tied or not: a kernel should cost about the same whichever a column is.
//! Each case reports ns per row, best of REPS (default 5), over 2^LOG2_ROWS rows (default 18).
//!
//!   cargo bench --bench list_spans -- [LOG2_ROWS REPS] [CASE...]
//!
//! The cases are `survey`, `sort`, `bare`, `pairs`, `hash`, `compare`, `positions`, `select` and
//! `lanes`. `LANES_RUN=1` makes `lanes` take single rows from alternating sources.

use corgi::arrange::{compare_idx, gather, gather_lanes, sort_perm, survey_groups};
use corgi::{Bounds, Value};
use std::hint::black_box;
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
}

fn best(reps: usize, rows: usize, mut f: impl FnMut()) -> f64 {
    let mut b = f64::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        f();
        b = b.min(t.elapsed().as_nanos() as f64 / rows as f64);
    }
    b
}

/// what the elements of a list are held as.
#[derive(Clone, Copy)]
enum Leaf { Bytes, Words }

/// `rows` lists: row `r` is word `word(r)` of a dictionary of `distinct` words, each of `len`
/// elements (or `1..=len` when `ragged`), sharing a prefix of `shared` elements.
fn lists(rng: &mut Rng, rows: usize, distinct: usize, len: usize, shared: usize, ragged: bool, leaf: Leaf, word: impl Fn(usize, &mut Rng) -> usize) -> Value {
    let dict: Vec<Vec<u8>> = (0..distinct).map(|_| {
        let n = if ragged { shared.max(1) + rng.below(len - shared.max(1) + 1) } else { len };
        (0..n).map(|p| if p < shared { b'a' } else { b'a' + rng.below(26) as u8 }).collect()
    }).collect();
    let (mut ends, mut flat) = (Vec::with_capacity(rows), Vec::new());
    for r in 0..rows {
        flat.extend_from_slice(&dict[word(r, rng)]);
        ends.push(flat.len());
    }
    let vals = match leaf {
        Leaf::Bytes => Value::u8(flat),
        Leaf::Words => Value::i64(flat.into_iter().map(|b| b as i64 * 1_000_003).collect()),
    };
    Value::List(ends.into(), Box::new(vals))
}

fn sorted(v: &Value) -> Value { gather(v, &sort_perm(v)) }

fn main() {
    let args: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with("--")).collect();
    let sized = args.len() >= 2 && args[..2].iter().all(|a| a.parse::<usize>().is_ok());
    let (n, reps) = if sized { (1usize << args[0].parse::<u32>().unwrap(), args[1].parse::<usize>().unwrap()) } else { (1 << 18, 5) };
    let cases = &args[if sized { 2 } else { 0 }..];
    let want = |name: &str| cases.is_empty() || cases.iter().any(|a| name.starts_with(a.as_str()));
    let mut rng = Rng(0x9E3779B97F4A7C15);

    // (key, text): `group` rows share a key, and a key's rows share its text (`same`) or not.
    let keyed = |rng: &mut Rng, rows: usize, group: usize, same: bool, len: usize, shared: usize, ragged: bool, leaf: Leaf| {
        let keys: Vec<i64> = (0..rows).map(|r| (r / group) as i64).collect();
        let distinct = if same { rows / group } else { rows };
        let text = lists(rng, rows, distinct, len, shared, ragged, leaf, |r, _| if same { r / group } else { r });
        Value::Prod(vec![Value::i64(keys), text])
    };
    for (leaf, lname) in [(Leaf::Bytes, "bytes"), (Leaf::Words, "words")] {
        for (same, sname) in [(true, "equal text in a key"), (false, "distinct text")] {
            for (len, shared, ragged, shape) in [(24, 0, true, "ragged 1..=24"), (24, 0, false, "fixed 24"), (64, 48, true, "ragged, 48 shared")] {
                let name = format!("survey {lname}, {sname}, {shape}");
                if want("survey") {
                    // two sides over the same keys, 16 rows a key a side.
                    let a = sorted(&keyed(&mut rng, n / 2, 16, same, len, shared, ragged, leaf));
                    let b = if same { a.clone() } else { sorted(&keyed(&mut rng, n / 2, 16, same, len, shared, ragged, leaf)) };
                    let runs = survey_groups(&a, &b).len();
                    let t = best(reps, n, || { black_box(survey_groups(black_box(&a), black_box(&b))); });
                    println!("{name:<52} {t:8.2} ns/row  ({runs} runs)");
                }
                let name = format!("sort   {lname}, {sname}, {shape}");
                if want("sort") {
                    let v = keyed(&mut rng, n, 16, same, len, shared, ragged, leaf);
                    let mut perm: Vec<usize> = (0..n).collect();
                    for i in (1..n).rev() { perm.swap(i, rng.below(i + 1)); }
                    let v = gather(&v, &perm);
                    let t = best(reps, n, || { black_box(sort_perm(black_box(&v))); });
                    println!("{name:<52} {t:8.2} ns/row");
                }
            }
        }
    }
    if want("bare") {
        // a list column alone, every row distinct: one block, sharing nothing or two elements.
        for (leaf, lname) in [(Leaf::Bytes, "bytes"), (Leaf::Words, "words")] {
            for shared in [0, 2] {
                let v = lists(&mut rng, n, n, 24, shared, true, leaf, |r, _| r);
                let t = best(reps, n, || { black_box(sort_perm(black_box(&v))); });
                println!("{:<52} {t:8.2} ns/row", format!("bare sort {lname}, distinct, {shared} shared"));
            }
        }
    }
    if want("pairs") {
        // rows of 24 against rows of 24, by explicit pairs and by neighbours, under a stride and
        // under ragged bounds (one more row, empty, that no pair names).
        let a = lists(&mut rng, n, 1024, 24, 0, false, Leaf::Bytes, |_, rng| rng.below(1024));
        let Value::List(_, vals) = &a else { unreachable!() };
        let mut ends: Vec<usize> = (1..=n).map(|i| i * 24).collect();
        ends.push(n * 24);
        let loose = Value::List(Bounds::Offsets(ends.into()), vals.clone());
        let (ia, ib): (Vec<usize>, Vec<usize>) = (0..n).map(|_| (rng.below(n), rng.below(n))).unzip();
        for (name, v) in [("fixed 24", &a), ("ragged bounds, rows of 24", &loose)] {
            let t = best(reps, n, || { black_box(compare_idx(black_box(v), black_box(v), &ia, &ib)); });
            println!("{:<52} {t:8.2} ns/row", format!("compare pairs, {name}"));
            let t = best(reps, n, || { black_box(corgi::arrange::group_bounds(black_box(v))); });
            println!("{:<52} {t:8.2} ns/row", format!("group bounds, {name}"));
        }
    }
    if want("hash") {
        for (leaf, lname) in [(Leaf::Bytes, "bytes"), (Leaf::Words, "words")] {
            for len in [8, 64] {
                let v = lists(&mut rng, n, 1024, len, 0, false, leaf, |_, rng| rng.below(1024));
                let t = best(reps, n, || { black_box(corgi::hash(black_box(&v))); });
                println!("{:<52} {t:8.2} ns/row", format!("hash   {lname}, rows of {len}"));
            }
        }
    }
    if want("compare") {
        // row against row of two lists of the same text, one held as bytes and one as words.
        let a = lists(&mut rng, n, n, 24, 0, true, Leaf::Bytes, |r, _| r);
        let Value::List(bounds, vals) = &a else { unreachable!() };
        let wide = Value::List(bounds.clone(), Box::new(Value::i64(vals.as_i64("x").unwrap().to_vec())));
        let idx: Vec<usize> = (0..n).collect();
        for (name, b) in [("compare bytes with bytes", &a), ("compare bytes with words", &wide)] {
            let t = best(reps, n, || { black_box(compare_idx(black_box(&a), black_box(b), &idx, &idx)); });
            println!("{name:<52} {t:8.2} ns/row");
        }
    }
    if want("positions") {
        // each row gathers 8 of its own 16 elements, the positions held as bytes or as words.
        let p = corgi::Program::compile_ml("let (at, hay) = input in (at, hay) gather").expect("gather");
        let hay = lists(&mut rng, n, 1024, 16, 0, false, Leaf::Words, |_, rng| rng.below(1024));
        let at: Vec<u8> = (0..n * 8).map(|_| rng.below(16) as u8).collect();
        for (name, vals) in [("words", Value::i64(at.iter().map(|&x| x as i64).collect())), ("bytes", Value::u8(at.clone()))] {
            let input = Value::Prod(vec![Value::List(Bounds::Stride(8, n), Box::new(vals)), hay.clone()]);
            let t = best(reps, n, || { black_box(p.run(black_box(input.clone()))); });
            println!("{:<52} {t:8.2} ns/row", format!("gather 8 positions a row, held as {name}"));
        }
    }
    if want("select") {
        // row by row, one of two lists by a mask: half the rows from each side, in runs or alone.
        let p = corgi::Program::compile_ml("input map ((m, a, b) -> (m, a, b) select)").expect("select");
        for (single, sname) in [(false, "runs"), (true, "single rows")] {
            let mask: Vec<u8> = if single { (0..n).map(|i| (i % 2) as u8).collect() } else {
                let mut m = Vec::with_capacity(n);
                while m.len() < n { let t = rng.below(2) as u8; for _ in 0..(1 + rng.below(7)).min(n - m.len()) { m.push(t); } }
                m
            };
            for k in [2usize, 4, 16, 64] {
                let mut side = || lists(&mut rng, n, 1024, k, 0, false, Leaf::Bytes, |_, rng| rng.below(1024));
                let rows = Value::Prod(vec![Value::u8(mask.clone()), side(), side()]);
                let input = Value::List(vec![n].into(), Box::new(rows));
                let t = best(reps, n, || { black_box(p.run(black_box(input.clone()))); });
                println!("{:<52} {t:8.2} ns/row", format!("select bytes, fixed {k}, {sname}"));
            }
        }
    }
    if want("lanes") {
        // a merge's output: rows taken from two sources in runs of about four.
        let (mut tags, mut off, mut at) = (Vec::with_capacity(n), Vec::with_capacity(n), [0usize; 2]);
        // LANES_RUN=1 takes single rows instead: the two sources strictly alternate.
        let single = std::env::var("LANES_RUN").is_ok_and(|v| v == "1");
        while tags.len() < n {
            let t = if single { tags.len() % 2 } else { rng.below(2) };
            for _ in 0..(if single { 1 } else { 1 + rng.below(7) }).min(n - tags.len()) {
                tags.push(t);
                off.push(at[t]);
                at[t] += 1;
            }
        }
        for k in [2usize, 4, 16, 64] {
            for (leaf, lname) in [(Leaf::Bytes, "bytes"), (Leaf::Words, "words")] {
                let srcs: Vec<Value> = at.iter().map(|&rows| lists(&mut rng, rows, 1024, k, 0, false, leaf, |_, rng| rng.below(1024))).collect();
                assert!(matches!(&srcs[0], Value::List(Bounds::Stride(..), _)));
                let refs: Vec<Option<&Value>> = srcs.iter().map(Some).collect();
                let t = best(reps, n, || { black_box(gather_lanes(black_box(&refs), &tags, &off)); });
                println!("{:<52} {t:8.2} ns/row", format!("lanes  {lname}, fixed {k}"));
                // the same rows under ragged bounds: one more row of another length in each source.
                let ragged: Vec<Value> = srcs.iter().zip(at).map(|(s, rows)| {
                    let Value::List(_, v) = s else { unreachable!() };
                    let mut ends: Vec<usize> = (1..=rows).map(|i| i * k).collect();
                    ends.push(rows * k);
                    Value::List(Bounds::Offsets(ends.into()), v.clone())
                }).collect();
                let refs: Vec<Option<&Value>> = ragged.iter().map(Some).collect();
                let t = best(reps, n, || { black_box(gather_lanes(black_box(&refs), &tags, &off)); });
                println!("{:<52} {t:8.2} ns/row", format!("lanes  {lname}, ragged bounds, rows of {k}"));
            }
        }
    }
}
