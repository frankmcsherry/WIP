//! Performance-gap suite: corgi's achieved throughput vs an honest hand-written-Rust ceiling, per
//! task family, at L2/SLC-to-DRAM working sets. The companion to `expressivity-gaps.md` — that audit
//! asks "what can't the surface say?", this one asks "where does the engine leave throughput on the
//! floor?". The point is to size the **destination-passing-style / fusion** payoff BEFORE building it:
//! the gap is decomposed into a mechanism (extra passes · intermediate materialization · allocation ·
//! scalar index loop · compute kernel), and only the pass/intermediate mechanisms are what DPS fixes.
//!
//! Method (after collie's bake-off harness): inline Rust ceilings in THIS binary, same input data,
//! best-of-`reps` timing (min rejects scheduler/turbo noise), `black_box` on input and output so LLVM
//! can't elide the work, deterministic LCG-scrambled inputs (no rng dep). Inputs are built once and
//! handed to `eval` by Arc-clone inside the loop (outside the timer) — a real pipeline's column flow.
//!
//! Sizing targets the M4 mini (128 KB L1D, ~16 MB L2/cluster, ~8 MB SLC; no conventional L3): the
//! design center is the 1 M-row (8 MB/col) L2/SLC point; 8 M rows (64 MB/col) is the DRAM-streaming
//! eye; 8 K rows (64 KB) is a control that proves dispatch is amortized — NOT a target. Run:
//! `cargo bench --bench gaps`.

use corgi::{eval_graph, parse_ml, ArithOp, Builder, Graph, NumOp, Value};
use std::hint::black_box;
use std::time::{Duration, Instant};

// ----- harness -----------------------------------------------------------

/// best-of-`reps` wall time for one whole-graph `eval`. The arg is cloned (Arc bump) outside the timer,
/// matching a pipeline that hands an owned column to each op.
fn corgi_t(g: &Graph<NumOp>, arg: &Value, reps: u32) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let a = arg.clone();
        let t = Instant::now();
        black_box(eval_graph(g, black_box(a)));
        best = best.min(t.elapsed());
    }
    best
}

/// best-of-`reps` wall time for a Rust-ceiling closure (it owns its allocation, timed; the input slice
/// it reads is `black_box`ed at the call site so the read isn't hoisted).
fn rust_t(reps: u32, mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed());
    }
    best
}

fn ns_per(d: Duration, n: usize) -> f64 {
    d.as_nanos() as f64 / n as f64
}

/// GB/s over the *input* footprint (`n` u64s) — a single comparable bandwidth number across engine and
/// ceiling; the 8 MB→64 MB drop in this figure is the bandwidth-bound vs compute-bound tell.
fn gbs(d: Duration, n: usize) -> f64 {
    (n as f64 * 8.0) / d.as_secs_f64() / 1e9
}

/// one corgi-vs-ceiling row: ns/row for each, the input-GB/s for corgi, the slowdown, and the named
/// mechanism the slowdown is attributed to (the decompose-before-reporting discipline).
fn row(task: &str, n: usize, c: Duration, r: Duration, mech: &str) {
    println!(
        "{task:<22} n={n:>8}  corgi {:>6.2} ns  rust {:>6.2} ns  {:>5.1} GB/s  {:>5.2}x   {mech}",
        ns_per(c, n),
        ns_per(r, n),
        gbs(c, n),
        c.as_secs_f64() / r.as_secs_f64(),
    );
}

/// the chain decomposition: corgi (k passes) vs Rust-k-pass (un-fused) vs Rust-1-pass (fused). Two
/// ratios fall out — corgi_k/rust_k is the engine's per-pass tax (dispatch + alloc), rust_k/rust_1 is
/// the pure fusion prize DPS would capture. Reported separately so neither hides in a single number.
fn row_chain(task: &str, n: usize, ck: Duration, rk: Duration, r1: Duration) {
    println!(
        "{task:<22} n={n:>8}  corgi_k {:>6.2}  rust_k {:>6.2}  rust_1 {:>6.2} ns  | tax {:>4.2}x  fusion-prize {:>4.2}x",
        ns_per(ck, n),
        ns_per(rk, n),
        ns_per(r1, n),
        ck.as_secs_f64() / rk.as_secs_f64(),
        rk.as_secs_f64() / r1.as_secs_f64(),
    );
}

// ----- inputs ------------------------------------------------------------

/// deterministic non-sorted column via an LCG step (no rng dep). Masked to 32 bits so the byte-radix
/// sort runs four passes, not eight, and so group/dedup keys have a realistic distinct count.
fn scrambled(n: usize) -> Vec<u64> {
    (0..n as u64).map(|i| i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407) >> 32).collect()
}

fn leaf(n: usize) -> Value {
    Value::u64(scrambled(n))
}

/// one big `List<U64>` of a single `n`-wide row — per-row ops (reduce/sort/filter/group/scan) fold the
/// whole column in one bulk pass.
fn one_list(n: usize) -> Value {
    Value::List(vec![n].into(), Box::new(Value::u64(scrambled(n))))
}

/// a SORTED big `List<U64>` (one `n`-wide row of `0..n`) — the equi-join feeds find/slices a haystack
/// already in key order, so the measurement isolates the join primitives from a sort cost.
fn sorted_list(n: usize) -> Value {
    Value::List(vec![n].into(), Box::new(Value::u64((0..n as u64).collect())))
}

fn compile(src: &str) -> Graph<NumOp> {
    parse_ml(src).unwrap_or_else(|e| panic!("compile {src:?}: {e}"))
}

/// a fixed 5-letter lowercase word, base-26 of `v` — the word-count vocabulary generator (mod a vocab
/// size at the call site, so a small distinct count makes dedup/find do real grouping work).
fn word5(mut v: u64) -> [u8; 5] {
    let mut w = [0u8; 5];
    for c in w.iter_mut() { *c = b'a' + (v % 26) as u8; v /= 26; }
    w
}

/// a space-separated text of `m` words drawn from `vocab` distinct words, returned BOTH as a one-row
/// `List<U8>` (corgi's input) and as the raw bytes (so the Rust ceiling splits and sorts the same
/// ragged words — no fixed-width-array advantage over corgi's structural string sort).
fn words_text(m: usize, vocab: u64) -> (Value, Vec<u8>) {
    let src = scrambled(m);
    let mut bytes = Vec::with_capacity(m * 6);
    for (i, &v) in src.iter().enumerate() {
        if i > 0 { bytes.push(b' '); }
        bytes.extend_from_slice(&word5(v % vocab));
    }
    (Value::List(vec![bytes.len()].into(), Box::new(Value::u8(bytes.clone()))), bytes)
}

/// a comma-separated CSV of `m` small integers, returned BOTH as a one-row `List<U8>` and as the raw
/// bytes (so the Rust ceiling does the SAME split + atoi work corgi does, not a pre-parsed sum). All
/// fields parse cleanly (the Err lane stays empty).
fn csv_text(m: usize) -> (Value, Vec<u8>) {
    let nums = scrambled(m);
    let mut bytes = Vec::with_capacity(m * 6);
    for (i, &v) in nums.iter().enumerate() {
        if i > 0 { bytes.push(b','); }
        bytes.extend_from_slice((v % 100_000).to_string().as_bytes());
    }
    (Value::List(vec![bytes.len()].into(), Box::new(Value::u8(bytes.clone()))), bytes)
}

/// a chain of `k` in-place `AddU64` passes — each link its own pass over memory (interior links mutate
/// the moved buffer). The pure fusion headroom: `k` passes a single fused loop collapses to one.
fn add_chain(k: usize) -> Graph<NumOp> {
    let mut b = Builder::default();
    let mut cur = b.input();
    for _ in 0..k {
        cur = b.add(ArithOp::AddU64(7), vec![cur]);
    }
    b.finish(cur)
}

// ----- families ----------------------------------------------------------

/// A — pointwise / streaming. The DPS target: each op is its own memory pass; a fused Rust loop is one.
fn family_a(n: usize, reps: u32) {
    let lf = leaf(n);
    let src = scrambled(n);

    // A1 add_const — the 1-pass ceiling control. Expect ~1x: a single SIMD pass is already at bandwidth.
    let g = compile("input add_u64 7");
    let c = corgi_t(&g, &lf, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().map(|&x| x.wrapping_add(7)).collect::<Vec<u64>>());
    });
    row("A1 add_const", n, c, r, "1 pass — at-ceiling control");

    // A2 add_chain8 — 8 identical passes. corgi_k vs rust_k = tax; rust_k vs rust_1 = the fusion prize.
    let g = add_chain(8);
    let ck = corgi_t(&g, &lf, reps);
    let rk = rust_t(reps, || {
        let s = black_box(&src);
        let mut v: Vec<u64> = s.to_vec();
        for _ in 0..8 {
            for x in v.iter_mut() { *x = x.wrapping_add(7); }
        }
        black_box(v);
    });
    let r1 = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().map(|&x| x.wrapping_add(56)).collect::<Vec<u64>>());
    });
    row_chain("A2 add_chain8", n, ck, rk, r1);

    // A3 mixed_chain — 4 heterogeneous kernels: (((x+5)*3)-2)>>1.
    let g = compile("input add_u64 5 mul 3 sub 2 shr 1");
    let ck = corgi_t(&g, &lf, reps);
    let rk = rust_t(reps, || {
        let s = black_box(&src);
        let mut v: Vec<u64> = s.iter().map(|&x| x.wrapping_add(5)).collect();
        for x in v.iter_mut() { *x = x.wrapping_mul(3); }
        for x in v.iter_mut() { *x = x.wrapping_sub(2); }
        for x in v.iter_mut() { *x >>= 1; }
        black_box(v);
    });
    let r1 = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().map(|&x| (x.wrapping_add(5).wrapping_mul(3).wrapping_sub(2)) >> 1).collect::<Vec<u64>>());
    });
    row_chain("A3 mixed_chain", n, ck, rk, r1);

    // A4 map_then_reduce — sum of 2x. Fusion folds the map into the reduce (no intermediate column).
    let g = compile("let xs = input in xs map (e -> e mul 2) fold_add");
    let li = one_list(n);
    let ck = corgi_t(&g, &li, reps);
    let r1 = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().fold(0u64, |a, &x| a.wrapping_add(x.wrapping_mul(2))));
    });
    let rk = rust_t(reps, || {
        let s = black_box(&src);
        let m: Vec<u64> = s.iter().map(|&x| x.wrapping_mul(2)).collect();
        black_box(m.iter().fold(0u64, |a, &x| a.wrapping_add(x)));
    });
    row_chain("A4 map_reduce", n, ck, rk, r1);
}

/// B — predicate / selection. Mask materialization + (for filter) a scalar survivor loop.
fn family_b(n: usize, reps: u32) {
    let li = one_list(n);
    let src = scrambled(n);
    let t = 0x8000_0000u64; // ~half pass the threshold (32-bit-masked inputs)

    // B1 filter — keep values > T. corgi: mask pass + `filter_mask` scalar gather. rust: predicated push.
    let g = compile("let xs = input in (xs, xs map (e -> e gt 2147483648)) filter");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let mut out = Vec::with_capacity(s.len());
        for &x in s { if x > t { out.push(x); } }
        black_box(out);
    });
    row("B1 filter_values", n, c, r, "mask pass + scalar filter_mask vs predicated push");

    // B2 select/blend — min(x+7, 3x) via cmp + branchless select. corgi: add,mul,cmp,select passes.
    let g = compile("input map (x -> let a = x add_u64 7 in let b = x mul 3 in ((a, b) lt, a, b) select)");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().map(|&x| {
            let a = x.wrapping_add(7);
            let b = x.wrapping_mul(3);
            if a < b { a } else { b }
        }).collect::<Vec<u64>>());
    });
    row("B2 cmp_select", n, c, r, "4 passes (add,mul,cmp,select) vs 1 fused");
}

/// C — aggregation. Monoid reductions are one SIMD pass (at ceiling); group/scan/fold are the interesting cases.
fn family_c(n: usize, reps: u32) {
    let li = one_list(n);
    let src = scrambled(n);

    // C1 fold_add — the 1-pass aggregate ceiling control.
    let g = compile("input fold_add");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().fold(0u64, |a, &x| a.wrapping_add(x)));
    });
    row("C1 fold_add", n, c, r, "1 SIMD pass — at-ceiling control");

    // C2 fold_max — kind-blind lane max horizontal fold.
    let g = compile("input fold_max");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().copied().fold(0u64, u64::max));
    });
    row("C2 fold_max", n, c, r, "1 SIMD pass");

    // C3 group_by + sum — 256 buckets (key = x & 255). corgi: sort-based group; rust: sort-by-key + segment sum.
    let g = compile("let g = input map (x -> (x and 255, x)) group in g map ((k, vs) -> (k, vs fold_add))");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let mut acc = [0u64; 256];
        for &x in s { acc[(x & 255) as usize] = acc[(x & 255) as usize].wrapping_add(x); }
        black_box(acc);
    });
    row("C3 group_by_sum", n, c, r, "sort-based group vs direct bucket accum");

    // C4 scan — inclusive prefix sum. Sequential within the row; rust is a tight cumsum loop.
    let g = compile("let xs = input in (xs lit 0, xs) scan ((a, x) -> (a, x) add)");
    let c = corgi_t(&g, &li, reps.min(3)); // ~hundreds of ns/row; few reps suffice and save minutes
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let mut acc = 0u64;
        black_box(s.iter().map(|&x| { acc = acc.wrapping_add(x); acc }).collect::<Vec<u64>>());
    });
    row("C4 scan_prefix", n, c, r, "lockstep foldscan vs cumsum loop");

    // C4k scan_add — the monoid PREFIX kernel: one in-place pass, the fast path the general scan above
    // should lower to. Same cumsum, vs the same Rust loop — the measured close of the C4 gap.
    let g = compile("input scan_add");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let mut acc = 0u64;
        black_box(s.iter().map(|&x| { acc = acc.wrapping_add(x); acc }).collect::<Vec<u64>>());
    });
    row("C4k scan_add(kernel)", n, c, r, "monoid prefix kernel, one in-place pass");

    // C5 fold (sum, count) — heterogeneous accumulator, non-monoid shape.
    let g = compile("let seed = (input lit 0, input lit 0) in (seed, input) fold ((acc, x) -> ((acc.0, x) add, acc.1 add_u64 1))");
    let c = corgi_t(&g, &li, reps.min(3));
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let mut sum = 0u64;
        let mut cnt = 0u64;
        for &x in s { sum = sum.wrapping_add(x); cnt += 1; }
        black_box((sum, cnt));
    });
    row("C5 fold_sum_count", n, c, r, "lockstep fold vs single accum loop");
}

/// D — sort / order. The lone compute-bound corner; the structural comparator is the distinctive piece.
fn family_d(n: usize, reps: u32) {
    let li = one_list(n);
    let src = scrambled(n);

    // D1 sort — byte-radix leaf sort vs pdqsort.
    let g = compile("input sort");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps.min(10), || {
        let mut v = black_box(&src).to_vec();
        v.sort_unstable();
        black_box(v);
    });
    row("D1 sort_u64", n, c, r, "byte-radix vs pdqsort");

    // D2 dedup — sort then unique.
    let g = compile("input dedup");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps.min(10), || {
        let mut v = black_box(&src).to_vec();
        v.sort_unstable();
        v.dedup();
        black_box(v);
    });
    row("D2 dedup", n, c, r, "sort+unique vs sort+dedup");
}

/// E — relational / index generators. `gather` is fresh-allocating by necessity; the open question is
/// the SCALAR index generators (`resolve_indices`/`filter_mask`/`expand_ranges`) layered above it.
fn family_e(n: usize, reps: u32) {
    let mask = n as u64 - 1; // n is a power of two, so `& mask` is an in-bounds row-relative index
    let src = scrambled(n);

    // E1 single-key equi-join (find + slices) over a SORTED haystack — the join primitives, no sort cost.
    let g = compile(
        "let bn = input in let build = bn map (x -> (x shr 8, x)) in \
         let probes = bn map (x -> x shr 8) dedup in let t = build transpose in \
         let r = (probes, t.0) find in (r, t.1) slices_uns",
    );
    let sl = sorted_list(n);
    let c = corgi_t(&g, &sl, reps);
    let r = rust_t(reps, || {
        let keys: Vec<u64> = (0..n as u64).map(|x| x >> 8).collect(); // sorted by construction
        let vals: Vec<u64> = (0..n as u64).collect();
        let mut probes = keys.clone();
        probes.dedup();
        // materialize the matched value ranges into a flat (values, bounds) list — corgi's `slices`
        // produces exactly this, so the ceiling must pay the same output copy, not reference ranges.
        let mut flat: Vec<u64> = Vec::with_capacity(n);
        let mut bounds: Vec<usize> = Vec::with_capacity(probes.len());
        let mut j = 0usize;
        for &p in &probes {
            let start = j;
            while j < keys.len() && keys[j] == p { j += 1; }
            flat.extend_from_slice(&vals[start..j]);
            bounds.push(flat.len());
        }
        black_box((flat, bounds));
    });
    row("E1 join_find_slices", n, c, r, "find (per-probe search)+slices vs two-pointer merge, both materializing");

    // E2 gather — random permutation. corgi: `resolve_indices` (scalar, +bounds assert) then `Prim::gather`.
    let g = compile(&format!("let h = input in (h map (e -> e and {mask}), h) gather_uns"));
    let li = one_list(n);
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let idx: Vec<usize> = s.iter().map(|&e| (e & mask) as usize).collect();
        black_box(idx.iter().map(|&i| s[i]).collect::<Vec<u64>>());
    });
    row("E2 gather", n, c, r, "resolve_indices (scalar) + gather vs map+gather");

    // E3 gather-chain (pointer jump) — two composed gathers; the index-composition rewrite target.
    let g = compile(&format!(
        "let p = input map (e -> e and {mask}) in let pp = (p, p) gather_uns in \
         let p4 = (pp, pp) gather_uns in (pp, p4)"
    ));
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let p: Vec<usize> = s.iter().map(|&e| (e & mask) as usize).collect();
        let pp: Vec<usize> = p.iter().map(|&i| p[i]).collect();
        let p4: Vec<usize> = pp.iter().map(|&i| pp[i]).collect();
        black_box((pp, p4));
    });
    row("E3 gather_chain", n, c, r, "2 gathers vs 2 gathers (index-rewrite headroom on top)");
}

/// F — sum-type / variant. The differentiator: data-parallel `branch`/`match` keep each lane dense
/// (SIMD per-lane) where a scalar loop branches per element; on an unpredictable tag that is the trade.
fn family_f(n: usize, reps: u32) {
    let li = one_list(n);
    let src = scrambled(n);

    // F1 branch+match — parity dispatch (50/50, unpredictable). corgi: mask+partition+per-lane+recombine.
    let g = compile("input map (x -> (x, x and 1) branch 2 match (0 (e -> e add_u64 3), 1 (o -> o add_u64 7)))");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        black_box(s.iter().map(|&x| if x & 1 == 0 { x.wrapping_add(3) } else { x.wrapping_add(7) }).collect::<Vec<u64>>());
    });
    row("F1 branch_match", n, c, r, "columnar partition+recombine vs scalar branchy map");

    // F2 shred — build a Sum column then `unweave` it to SoA, vs a hand partition-by-tag.
    let g = compile("let xs = input in xs map (x -> (x, x and 1) branch 2) unweave");
    let c = corgi_t(&g, &li, reps);
    let r = rust_t(reps, || {
        let s = black_box(&src);
        let (mut e, mut o, mut tags) = (Vec::new(), Vec::new(), Vec::with_capacity(s.len()));
        for &x in s {
            tags.push((x & 1) as u8);
            if x & 1 == 0 { e.push(x); } else { o.push(x); }
        }
        black_box((e, o, tags));
    });
    row("F2 unweave_shred", n, c, r, "branch(build sum)+unweave vs one-pass partition");
}

/// G — text. split + the relational uniq-c idiom, and a parse+aggregate. Sized by element COUNT (`m`).
fn family_g(m: usize, reps: u32) {
    // G1 word-count (uniq -c): split, sort, dedup, find, hi-lo. Ceiling = the SAME sort-based algorithm
    // on byte-slice words split from the same text (no fixed-width-array shortcut).
    let (txt, txt_bytes) = words_text(m, 4096);
    let g = compile(
        "let words = input split \" \" in let sorted = words sort in let keys = sorted dedup in \
         let spans = (keys, sorted) find in (keys, spans map ((lo, hi) -> (hi, lo) sub))",
    );
    let c = corgi_t(&g, &txt, reps);
    let r = rust_t(reps.min(10), || {
        let bytes = black_box(&txt_bytes);
        let mut v: Vec<&[u8]> = bytes.split(|&b| b == b' ').collect();
        v.sort_unstable();
        let mut counts: Vec<u64> = Vec::new();
        let (mut i, mut run) = (0usize, 0usize);
        while i < v.len() {
            run = 1;
            while i + run < v.len() && v[i + run] == v[i] { run += 1; }
            counts.push(run as u64);
            i += run;
        }
        black_box((v, counts, run));
    });
    row("G1 word_count", m, c, r, "split+sort+dedup+find vs split+sort+run-count (same algo)");

    // G2 csv-sum: split, parse_u64 (total), default Err->0, unwrap, reduce. Ceiling does the SAME work:
    // one pass over the bytes, atoi at each comma, accumulate (no pre-parsed shortcut).
    let (csv, csv_bytes) = csv_text(m);
    let g = compile(
        "input split \",\" map (w -> w parse_u64 map_variant 0 (e -> e lit 0) unwrap) fold_add",
    );
    let c = corgi_t(&g, &csv, reps);
    let r = rust_t(reps, || {
        let bytes = black_box(&csv_bytes);
        let (mut sum, mut cur) = (0u64, 0u64);
        for &b in bytes {
            if b == b',' { sum = sum.wrapping_add(cur); cur = 0; }
            else { cur = cur.wrapping_mul(10).wrapping_add((b - b'0') as u64); }
        }
        black_box(sum.wrapping_add(cur));
    });
    row("G2 csv_sum", m, c, r, "split+parse_u64(Sum)+reduce vs split+atoi+sum");
}

// ----- driver ------------------------------------------------------------

fn main() {
    // (n, reps). 8 K = L1 control; 1 M = L2/SLC design center; 8 M = DRAM eye.
    let core: [(usize, u32); 3] = [(1 << 13, 2000), (1 << 20, 50), (1 << 23, 10)];
    for (n, reps) in core {
        println!("\n==== n = {n}  ({} MB/col) ============================================", n * 8 / (1 << 20));
        println!("-- A pointwise --");
        family_a(n, reps);
        println!("-- B selection --");
        family_b(n, reps);
        println!("-- C aggregation --");
        family_c(n, reps);
        println!("-- D order --");
        family_d(n, reps);
    }
    // E/F at the L2/SLC and DRAM points only (skip the L1 control — these are not single-pass ops).
    for (n, reps) in [(1usize << 20, 30u32), (1 << 23, 8)] {
        println!("\n==== n = {n}  ({} MB/col) — relational / sum-type ====================", n * 8 / (1 << 20));
        println!("-- E relational / index generators --");
        family_e(n, reps);
        println!("-- F sum-type --");
        family_f(n, reps);
    }
    // G sized by element (word/field) count: ~1 M and ~4 M elements.
    for (m, reps) in [(1usize << 20, 15u32), (1 << 22, 6)] {
        println!("\n==== m = {m} elements — text ========================================", );
        println!("-- G text --");
        family_g(m, reps);
    }
}
