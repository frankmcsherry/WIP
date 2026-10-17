//! Arrangement-boundary scalar comparisons; separate timing and allocation passes.
//! cargo run --release --example scalar_compare_bench -- [rows=100000] [rounds=11]
use corgi::{Bounds, Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

struct Counting;
static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(l)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc_zeroed(l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(p, l, n)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn pass(a: &Value, b: &Value, n: usize) {
    let mut equal = 0;
    for i in 0..n {
        equal += (corgi::arrange::compare_at(black_box(a), i, black_box(b), i).is_eq()) as usize;
    }
    assert_eq!(equal, n);
}
fn main() {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(100_000);
    let rounds: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(11);
    assert!(n > 0 && rounds > 0);
    let ints = Value::i64((0..n).map(|i| i as i64 - n as i64 / 2).collect());
    let bytes = Value::u8((0..n).map(|i| (i % 256) as u8).collect());
    let wide = Value::i64((0..n).map(|i| (i % 256) as i64).collect());
    let pair = Value::Prod(vec![ints.clone(), ints.clone()]);
    let list = Value::List(
        Bounds::offsets((1..=n).map(|i| 8 * i).collect()),
        Box::new(Value::Prod(vec![
            Value::i64(vec![0; 8 * n]),
            Value::i64(vec![-1; 8 * n]),
        ])),
    );
    let sum = Value::sum(
        (0..n).map(|i| i % 2).collect(),
        vec![
            Value::i64(vec![-3; n.div_ceil(2)]),
            Value::i64(vec![7; n / 2]),
        ],
    );
    println!("shape,rows,median_ns_per_comparison,allocation_calls,samples_ns");
    for (name, a, b, limit) in [
        ("int", &ints, &ints, n),
        ("pair", &pair, &pair, n),
        ("list_pair8", &list, &list, n),
        ("sum", &sum, &sum, n),
        ("byte_vs_i64", &bytes, &wide, n.min(1000)),
    ] {
        // Mixed-width baseline converts the whole byte column per comparison: keep its probe bounded.
        pass(a, b, limit);
        CALLS.store(0, Ordering::Relaxed);
        ENABLED.store(true, Ordering::Relaxed);
        pass(a, b, limit);
        ENABLED.store(false, Ordering::Relaxed);
        let allocations = CALLS.load(Ordering::Relaxed);
        let mut samples = Vec::new();
        for _ in 0..rounds {
            let start = Instant::now();
            pass(a, b, limit);
            samples.push(start.elapsed().as_nanos());
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        println!(
            "{name},{limit},{:.2},{allocations},{samples:?}",
            sorted[rounds / 2] as f64 / limit as f64
        );
    }
}
