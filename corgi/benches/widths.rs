//! The same programs over integer columns held at each storage, ns per element, best of 7.
//! Values are 0..120 unless a case says otherwise, so that every storage holds them; each result is
//! checked against the program's result on `i64`s.
//!
//!   cargo bench --bench widths

use corgi::{Bounds, IntWidth, Program, Value};
use std::hint::black_box;
use std::time::Instant;

fn best(n: usize, mut f: impl FnMut()) -> f64 {
    let mut b = f64::MAX;
    for _ in 0..7 {
        let t = Instant::now();
        f();
        b = b.min(t.elapsed().as_nanos() as f64 / n as f64);
    }
    b
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> i64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) as i64
    }
}

fn main() {
    let n = 1 << 22;
    let mut rng = Rng(7);
    let a: Vec<i64> = (0..n).map(|_| rng.next() % 120).collect();
    let b: Vec<i64> = (0..n).map(|_| rng.next() % 120).collect();
    let c: Vec<i64> = (0..n).map(|_| rng.next() % 120).collect();
    // diffs, as DDIR carries them: +1 and -1
    let d: Vec<i64> = (0..n).map(|_| if rng.next() % 2 == 0 { 1 } else { -1 }).collect();
    // short rows of 0..8 elements, for per-row folds
    let mut ends = Vec::new();
    let mut at = 0;
    while at < n {
        at = (at + (rng.next() % 9) as usize).min(n);
        ends.push(at);
    }
    let one = |w: IntWidth, xs: &[i64]| Value::List(Bounds::from(vec![xs.len()]), Box::new(Value::ints_at(w, xs.iter().copied())));
    let rows = |w: IntWidth, xs: &[i64]| Value::List(Bounds::from(ends.clone()), Box::new(Value::ints_at(w, xs.iter().copied())));
    let col = |w: IntWidth, xs: &[i64]| Value::ints_at(w, xs.iter().copied());
    let two = |w: IntWidth, x: &[i64], y: &[i64]| Value::Prod(vec![col(w, x), col(w, y)]);
    let cases: Vec<(&str, &str, Box<dyn Fn(IntWidth) -> Value>)> = vec![
        ("x + y", "(input.0, input.1) add", Box::new(|w| two(w, &a, &b))),
        ("x * y", "(input.0, input.1) mul", Box::new(|w| two(w, &a, &b))),
        ("x + 5", "(input, 5) add", Box::new(|w| col(w, &a))),
        ("x + y + z", "((input.0, input.1) add, input.2) add", Box::new(|w| Value::Prod(vec![col(w, &a), col(w, &b), col(w, &c)]))),
        ("x > 100 (mask)", "(input, 100) gt", Box::new(|w| col(w, &a))),
        ("x == y (mask)", "(input.0, input.1) eq", Box::new(|w| two(w, &a, &b))),
        ("x min y", "(input.0, input.1) min", Box::new(|w| two(w, &a, &b))),
        ("x xor y", "(input.0, input.1) xor", Box::new(|w| two(w, &a, &b))),
        ("neg x", "input neg", Box::new(|w| col(w, &a))),
        ("sum of one list", "input fold_add", Box::new(|w| one(w, &a))),
        ("sum per short row", "input fold_add", Box::new(|w| rows(w, &a))),
        ("diffs: sum per short row", "input fold_add", Box::new(|w| rows(w, &d))),
        ("diffs: x * y", "(input.0, input.1) mul", Box::new(|w| two(w, &d, &d))),
        ("sort a list", "input sort", Box::new(|w| one(w, &a))),
        ("sort (x, y) pairs", "input sort", Box::new(|w| Value::List(Bounds::from(vec![n]), Box::new(two(w, &a, &b))))),
        ("hash", "input hash", Box::new(|w| col(w, &a))),
    ];
    let widths = [IntWidth::I64, IntWidth::I32, IntWidth::I16, IntWidth::I8, IntWidth::U8];
    print!("| program |");
    for w in widths { print!(" {w:?} |"); }
    println!();
    print!("|---|");
    for _ in widths { print!("---:|"); }
    println!();
    for (name, src, input) in &cases {
        let p = Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"));
        let base = p.run(input(IntWidth::I64));
        print!("| {name} |");
        for w in widths {
            if name.starts_with("diffs") && w == IntWidth::U8 {
                print!(" |");
                continue;
            }
            let v = input(w);
            assert_eq!(p.run(v.clone()), base, "{name} at {w:?}");
            let t = best(n, || { black_box(p.run(black_box(v.clone()))); });
            print!(" {t:.2} |");
        }
        println!();
    }
}
