//! Free-standing eval benchmarks. Run with `cargo bench`. `harness = false` (see Cargo.toml) makes
//! cargo run THIS `main`, which times whole-graph `eval` over scalable columnar inputs with
//! `std::time::Instant` — no framework, no dependency. The workloads double as profiling targets
//! (run the bench binary under perf → pollard). Kept deliberately small and factored: it touches no
//! `src`, and criterion is the upgrade path if statistical rigor is ever wanted.

use corgi::{eval_graph, ArithOp, Builder, CmpOp, Graph, NumOp, Value};
use std::hint::black_box;
use std::time::{Duration, Instant};

/// best-of-`reps` wall time for one `eval` (min rejects scheduler/turbo noise). `black_box` keeps
/// the optimizer from eliding the work or hoisting it out of the loop.
fn bench(g: &Graph<NumOp>, arg: &Value, reps: u32) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let a = arg.clone(); // hand off an owned copy (clone = Arc bump, outside the timer)
        let t = Instant::now();
        black_box(eval_graph(g, black_box(a)));
        best = best.min(t.elapsed());
    }
    best
}

fn report(name: &str, n: usize, d: Duration) {
    let ns_per = d.as_nanos() as f64 / n as f64;
    println!("{name:<12} n={n:>9}  {:>7.2} ms  {ns_per:>6.2} ns/row", d.as_secs_f64() * 1e3);
}

/// a deterministic, non-sorted leaf column (no rng dependency); masked to 32 bits so the byte-radix
/// sort does a few passes rather than the full eight.
fn scrambled(n: usize) -> Vec<u64> {
    (0..n as u64).map(|i| i.wrapping_mul(2654435761) & 0xffff_ffff).collect()
}

/// one big list of `n` scrambled values: `List<U64>` with a single row.
fn one_list(n: usize) -> Value {
    Value::List(vec![n].into(), Box::new(Value::u64(scrambled(n))))
}

/// a `List<U64>` of `m` rows each `l` wide — `m` independent sub-lists in one column. `ReduceSum`
/// folds each row separately, so this is `m` list-sums delivered in a single bulk pass.
fn lists(m: usize, l: usize) -> Value {
    let bounds: Vec<usize> = (1..=m).map(|r| r * l).collect();
    Value::List(bounds.into(), Box::new(Value::u64(scrambled(m * l))))
}

fn graph(op: impl Into<NumOp>) -> Graph<NumOp> {
    let mut b = Builder::default();
    let inp = b.input();
    let out = b.add(op, vec![inp]);
    b.finish(out)
}

/// a chain of `k` AddU64 ops — each link is its own pass over memory; interior links operate in
/// place on the moved buffer (move-on-last-use), so the per-link cost is the headroom an
/// arithmetic-fusion pass would collapse into a single pass.
fn add_chain(k: usize) -> Graph<NumOp> {
    let mut b = Builder::default();
    let mut cur = b.input();
    for _ in 0..k {
        cur = b.add(ArithOp::AddU64(7), vec![cur]);
    }
    b.finish(cur)
}

fn main() {
    let reps = 20;
    // sweep two sizes: 8 MB (fits the M2 P-cluster's ~16 MB L2) vs 64 MB (streams from DRAM). The
    // arithmetic workloads' per-pass cost should jump across that cliff; sort shouldn't care.
    for n in [1usize << 20, 1 << 23] {
        report("add_const", n, bench(&graph(ArithOp::AddU64(7)), &Value::u64(scrambled(n)), reps));
        report("add_chain8", n, bench(&add_chain(8), &Value::u64(scrambled(n)), reps));
    }
    let n = 1 << 20;
    // ReduceSum over one big list — into_list + into_u64.
    report("reduce_add", n, bench(&graph(ArithOp::Reduce(corgi::Red::Add)), &one_list(n), reps));
    // SortList over one big list — the discrimination / byte-radix leaf sort.
    report("sort_list", n, bench(&graph(CmpOp::SortList), &one_list(n), reps));

    // sum_list, after roto's HN benchmark: m lists of l u64 each. Two regimes for the SAME work —
    // one bulk ReduceSum (corgi's columnar regime) vs m separate evals (row-at-a-time, where roto's
    // per-item host calls live). The ratio is what the bulk pass amortizes away.
    let (m, l) = (50_000usize, 1024usize);
    let sums = graph(ArithOp::Reduce(corgi::Red::Add));
    report("sum_list/bulk", m * l, bench(&sums, &lists(m, l), reps));
    {
        let one = Value::List(vec![l].into(), Box::new(Value::u64(scrambled(l))));
        let t = Instant::now();
        for _ in 0..m {
            black_box(eval_graph(&sums, black_box(one.clone())));
        }
        report("sum_list/percall", m * l, t.elapsed());
    }
}
