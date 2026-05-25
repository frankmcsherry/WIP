//! Benchmark harness.

use std::sync::Arc;
use std::time::Instant;
use crate::ir::op::eval;
use crate::syntax::parse::parse;
use crate::ir::value::{Value, Prim, from_vec, Storage, prod};
use crate::ir::shape::Interp;
use crate::ops::arith::{ArithOp, do_arith};

fn bench_run<F: FnMut()>(label: &str, n: usize, runs: usize, mut f: F) -> std::time::Duration {
    f();  // warmup
    let t0 = Instant::now();
    for _ in 0..runs { f(); }
    let total = t0.elapsed();
    let per = total / runs as u32;
    let ns_per = per.as_nanos() as f64 / n as f64;
    println!("  {:20}  {:>9.3} ms / run  ({:>6.2} ns/elem)",
             label, per.as_secs_f64() * 1000.0, ns_per);
    per
}

pub fn run_bench() -> Result<(), String> {
    println!("\n========== benchmarks ==========\n");
    let reg = crate::syntax::registry::OpRegistry::standard();

    // Methodology: each collie bench builds inputs ONCE outside the timed loop,
    // then clones (Arc::clone — O(1) for primitives) inside. We time only eval().
    // Rust baselines correspondingly skip any input cloning that LLVM would elide.
    // This matches a real pipeline where columns flow through many ops; it's no
    // longer comparing "build input + eval" against "do the work."

    {
        let n = 1_000_000;
        let a_src: Vec<u64> = (0..n as u64).collect();
        let b_src: Vec<u64> = (0..n as u64).map(|x| x * 2).collect();
        let prog = parse("dup .0 swap .1 +.u64", &reg)?;
        println!("[bench 1] per-row sum, N = {}", n);
        let runs = 20;
        let mut env: Vec<Value> = Vec::new();
        let pairs = prod(vec![from_vec::<u64>(a_src.clone()), from_vec::<u64>(b_src.clone())]);
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![pairs.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let out: Vec<u64> = a_src.iter().zip(b_src.iter()).map(|(&x, &y)| x + y).collect();
            std::hint::black_box(out);
        });
        println!("  slowdown: {:.1}x\n", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }

    {
        let n = 1_000_000;
        let src: Vec<u64> = (0..n as u64).map(|x| x % 100).collect();
        let prog = parse("dup 50u64 >= where", &reg)?;
        println!("[bench 2] filter (keep >= 50), N = {}", n);
        let runs = 20;
        let mut env: Vec<Value> = Vec::new();
        let v_in = from_vec::<u64>(src.clone());
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![v_in.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let out: Vec<u64> = src.iter().copied().filter(|&x| x >= 50).collect();
            std::hint::black_box(out);
        });
        println!("  slowdown: {:.1}x\n", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }

    {
        let n = 1_000_000;
        let regions: Vec<u8> = (0..n).map(|i| (i % 100) as u8).collect();
        let sales: Vec<u64> = (0..n as u64).collect();
        let prog = parse("dup .1 swap .0 group reduce.+.u64 entuple.2", &reg)?;
        println!("[bench 3] GROUP BY (100 regions, sum), N = {}", n);
        let runs = 10;
        let mut env: Vec<Value> = Vec::new();
        let pair = prod(vec![from_vec::<u8>(regions.clone()), from_vec::<u64>(sales.clone())]);
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![pair.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let mut perm: Vec<usize> = (0..n).collect();
            perm.sort_by_key(|&i| regions[i]);
            let mut out_k: Vec<u8> = Vec::new();
            let mut out_v: Vec<u64> = Vec::new();
            let mut i = 0;
            while i < n {
                let k = regions[perm[i]];
                let mut sum = 0u64;
                while i < n && regions[perm[i]] == k { sum += sales[perm[i]]; i += 1; }
                out_k.push(k);
                out_v.push(sum);
            }
            std::hint::black_box((out_k, out_v));
        });
        println!("  slowdown: {:.1}x\n", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }

    {
        let n_left = 100_000usize;
        let n_right = 100_000usize;
        let lk: Vec<u64> = (0..n_left as u64).map(|x| x * 2).collect();
        let la: Vec<u64> = (0..n_left as u64).collect();
        let mut rk: Vec<u64> = (0..n_right as u64).map(|x| if x % 2 == 0 { x } else { x + 100_000 }).collect();
        rk.sort();
        let rb: Vec<f64> = (0..n_right as u64).map(|x| x as f64).collect();
        let prog = parse("over .0 over .0 intersect \
                          pick 3 .1 pick 2 gather \
                          pick 3 .1 pick 2 gather \
                          pick 4 .0 pick 3 gather \
                          rot rot entuple.3 \
                          roll 4 drop roll 3 drop roll 2 drop swap drop", &reg)?;
        println!("[bench 4] sort-merge equijoin, L = {}, R = {}", n_left, n_right);
        let runs = 10;
        let n_total = n_left + n_right;
        let mut env: Vec<Value> = Vec::new();
        let l = prod(vec![from_vec::<u64>(lk.clone()), from_vec::<u64>(la.clone())]);
        let r = prod(vec![from_vec::<u64>(rk.clone()), from_vec::<f64>(rb.clone())]);
        let colang_time = bench_run("collie", n_total, runs, || {
            let mut st = vec![l.clone(), r.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n_total, runs, || {
            let mut out_k: Vec<u64> = Vec::new();
            let mut out_a: Vec<u64> = Vec::new();
            let mut out_b: Vec<f64> = Vec::new();
            let mut i = 0; let mut j = 0;
            while i < lk.len() && j < rk.len() {
                use std::cmp::Ordering::*;
                match lk[i].cmp(&rk[j]) {
                    Less => i += 1,
                    Equal => {
                        out_k.push(lk[i]); out_a.push(la[i]); out_b.push(rb[j]);
                        i += 1; j += 1;
                    }
                    Greater => j += 1,
                }
            }
            std::hint::black_box((out_k, out_a, out_b));
        });
        println!("  slowdown: {:.1}x\n", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }

    // bench 5: u64 add at N=1024
    {
        let n = 1024usize;
        let a_src: Vec<u64> = (0..n as u64).collect();
        let b_src: Vec<u64> = (0..n as u64).map(|x| x + 1).collect();
        let prog = parse("+.u64", &reg)?;
        println!("[bench 5] add at N = {} (L1-resident)", n);
        let runs = 100_000;
        let mut env: Vec<Value> = Vec::new();
        let a = from_vec::<u64>(a_src.clone());
        let b = from_vec::<u64>(b_src.clone());
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![a.clone(), b.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let out: Vec<u64> = a_src.iter().zip(b_src.iter()).map(|(&x, &y)| x + y).collect();
            std::hint::black_box(out);
        });
        println!("  slowdown: {:.1}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
        let pa = Prim::P64(Arc::new(a_src.clone()));
        let pb = Prim::P64(Arc::new(b_src.clone()));
        let kernel_time = bench_run("kernel ", n, runs, || {
            let out = do_arith(&pa, &pb, ArithOp::Add, Interp::U64).unwrap();
            std::hint::black_box(out);
        });
        println!("  kernel-only slowdown: {:.1}x\n", kernel_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }

    // bench 6: i32 add at N=1024
    {
        let n = 1024usize;
        let a_src: Vec<i32> = (0..n as i32).collect();
        let b_src: Vec<i32> = (0..n as i32).map(|x| x + 1).collect();
        let prog = parse("+.i32", &reg)?;
        println!("[bench 6] i32 add at N = {} (Frank's reference setup)", n);
        let runs = 100_000;
        let mut env: Vec<Value> = Vec::new();
        let a = from_vec::<i32>(a_src.clone());
        let b = from_vec::<i32>(b_src.clone());
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![a.clone(), b.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let out: Vec<i32> = a_src.iter().zip(b_src.iter()).map(|(&x, &y)| x + y).collect();
            std::hint::black_box(out);
        });
        println!("  slowdown: {:.1}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
        let pa = <i32 as Storage>::wrap(a_src.clone());
        let pb = <i32 as Storage>::wrap(b_src.clone());
        let kernel_time = bench_run("kernel ", n, runs, || {
            let out = do_arith(&pa, &pb, ArithOp::Add, Interp::I32).unwrap();
            std::hint::black_box(out);
        });
        println!("  kernel-only vs rust: {:.1}x", kernel_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
        println!("  reference (Frank, bencher crate, i32 N=1024): ~0.13 ns/elem\n");
    }

    // [bench 7] dup of a wide/nested Value should be flat (O(1)) — the
    // payload sharing means cloning a value with 100 Prim fields, or a 4-deep
    // List, costs only Arc bumps, not data copies.
    {
        println!("[bench 7] Value::clone (dup) cost — wide and deep");
        let leaf_n = 1_000_000;
        let leaf = from_vec::<u64>((0..leaf_n as u64).collect());
        // wide: a 100-field Prod where every field is the same 1M-element column
        let wide_fields: Vec<Value> = (0..100).map(|_| leaf.clone()).collect();
        let wide = prod(wide_fields);
        // deep: 8 nested singleton-Lists wrapping the leaf
        let mut deep = leaf.clone();
        for _ in 0..8 {
            deep = crate::ir::value::list(crate::ir::value::bounds_stride(deep.len() as u64, 1), deep);
        }
        let runs = 1_000_000;
        // Each "run" of bench_run divides by leaf_n by convention; for clone
        // cost the per-element view doesn't really mean anything — read the
        // ms/run column directly.
        bench_run("clone leaf (1M)", 1, runs, || {
            let v = leaf.clone();
            std::hint::black_box(v);
        });
        bench_run("clone wide (100×1M)", 1, runs, || {
            let v = wide.clone();
            std::hint::black_box(v);
        });
        bench_run("clone deep (8 List<…<1M>…>)", 1, runs, || {
            let v = deep.clone();
            std::hint::black_box(v);
        });
        println!("  expected: all three should be ≈ same constant time —");
        println!("  clone is now O(1) regardless of width / depth.\n");
    }

    // Sort benches across shapes. Compare collie's polymorphic sort
    // against the closest Rust equivalent.
    {
        let n = 1_000_000usize;
        // Scrambled u64 keys.
        let xs: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407))
            .collect();
        println!("[bench 8] sort 1M u64 (scrambled)");
        let runs = 5;
        let prog = parse("sort", &reg)?;
        let v = from_vec::<u64>(xs.clone());
        let mut env: Vec<Value> = Vec::new();
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![v.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let mut perm: Vec<usize> = (0..n).collect();
            perm.sort_unstable_by_key(|&i| xs[i]);
            let sorted: Vec<u64> = perm.iter().map(|&i| xs[i]).collect();
            std::hint::black_box(sorted);
        });
        println!("  slowdown: {:.2}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }
    println!();

    {
        let n = 1_000_000usize;
        let k0: Vec<u64> = (0..n as u64).map(|i| i % 7).collect();
        let k1: Vec<u64> = (0..n as u64).map(|i| i % 100).collect();
        println!("[bench 9] sort 1M Prod[u64, u64] (lex)");
        let runs = 5;
        let prog = parse("sort", &reg)?;
        let pair = prod(vec![from_vec::<u64>(k0.clone()), from_vec::<u64>(k1.clone())]);
        let mut env: Vec<Value> = Vec::new();
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![pair.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n, runs, || {
            let mut perm: Vec<usize> = (0..n).collect();
            perm.sort_unstable_by_key(|&i| (k0[i], k1[i]));
            let sk0: Vec<u64> = perm.iter().map(|&i| k0[i]).collect();
            let sk1: Vec<u64> = perm.iter().map(|&i| k1[i]).collect();
            std::hint::black_box((sk0, sk1));
        });
        println!("  slowdown: {:.2}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }
    println!();

    {
        // Ragged List<u64>: 250K rows, lengths cycle through {1,2,3,4}.
        // Total ≈ 625K elements. Rust baseline sorts rows length-first
        // then lex — implemented inline as a fair-ish comparison.
        let n_lists = 250_000usize;
        let lens: Vec<u64> = (0..n_lists as u64).map(|i| (i % 4) + 1).collect();
        let mut bounds: Vec<u64> = Vec::with_capacity(n_lists);
        let mut acc = 0u64;
        for &l in &lens { acc += l; bounds.push(acc); }
        let total: usize = acc as usize;
        let flat: Vec<u64> = (0..total as u64).collect();
        println!("[bench 10] sort 250K-row ragged List<u64> (length-first lex)");
        let runs = 5;
        let prog = parse("sort", &reg)?;
        let list = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(bounds.clone()),
            values: Arc::new(from_vec::<u64>(flat.clone())),
        };
        let mut env: Vec<Value> = Vec::new();
        let colang_time = bench_run("collie", n_lists, runs, || {
            let mut st = vec![list.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        let rust_time = bench_run("rust  ", n_lists, runs, || {
            // Sort row indices by (length, lex of contents).
            let mut perm: Vec<usize> = (0..n_lists).collect();
            perm.sort_unstable_by(|&a, &b| {
                let la = lens[a];
                let lb = lens[b];
                if la != lb { return la.cmp(&lb); }
                let alo = if a == 0 { 0 } else { bounds[a - 1] as usize };
                let blo = if b == 0 { 0 } else { bounds[b - 1] as usize };
                let l = la as usize;
                for k in 0..l {
                    let r = flat[alo + k].cmp(&flat[blo + k]);
                    if r != std::cmp::Ordering::Equal { return r; }
                }
                std::cmp::Ordering::Equal
            });
            std::hint::black_box(perm);
        });
        println!("  slowdown: {:.2}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }
    println!();

    {
        // List<List<u64>>: 50K outer × 2 inner × 5 elements.
        // No "natural" Rust idiom — we sort Vec<Vec<Vec<u64>>> lex.
        let outer = 50_000usize;
        let inner_per_outer = 2usize;
        let elems_per_inner = 5usize;
        let total = outer * inner_per_outer * elems_per_inner;
        let flat: Vec<u64> = (0..total as u64).map(|i| i.wrapping_mul(12345)).collect();
        let mut ib: Vec<u64> = Vec::new();
        let mut ob: Vec<u64> = Vec::new();
        let mut ia = 0u64;
        let mut oa = 0u64;
        for _ in 0..outer {
            for _ in 0..inner_per_outer {
                ia += elems_per_inner as u64;
                ib.push(ia);
            }
            oa += inner_per_outer as u64;
            ob.push(oa);
        }
        println!("[bench 11] sort 50K-row List<List<u64>> (length-first × 2)");
        let runs = 5;
        let prog = parse("sort", &reg)?;
        let inner = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(ib.clone()),
            values: Arc::new(from_vec::<u64>(flat.clone())),
        };
        let deep = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(ob.clone()),
            values: Arc::new(inner.clone()),
        };
        let mut env: Vec<Value> = Vec::new();
        let colang_time = bench_run("collie", outer, runs, || {
            let mut st = vec![deep.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });
        // Rust baseline: materialize as Vec<Vec<Vec<u64>>>, sort lex.
        let materialized: Vec<Vec<Vec<u64>>> = {
            let mut outers = Vec::with_capacity(outer);
            let mut o_prev = 0u64;
            for &o_end in &ob {
                let mut inners = Vec::new();
                for inner_idx in o_prev..o_end {
                    let i_prev = if inner_idx == 0 { 0 } else { ib[inner_idx as usize - 1] };
                    let i_end = ib[inner_idx as usize];
                    inners.push(flat[i_prev as usize..i_end as usize].to_vec());
                }
                outers.push(inners);
                o_prev = o_end;
            }
            outers
        };
        let rust_time = bench_run("rust  ", outer, runs, || {
            let mut v = materialized.clone();
            v.sort_unstable();
            std::hint::black_box(v);
        });
        println!("  slowdown: {:.2}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }
    println!();

    {
        // Sum<Prim<u64>, Prod[u64, u64], List<u64>> with 1M total rows.
        // The lane shapes are deliberately different to exercise the
        // disc-then-lane recursion. Rust baseline: derive Ord on a
        // 3-variant enum and sort a Vec<V>.
        let n = 1_000_000usize;
        let disc: Vec<u8> = (0..n as u64).map(|i| (i % 3) as u8).collect();
        // Per-lane element counts: count of disc == k.
        let n0 = disc.iter().filter(|&&d| d == 0).count();
        let n1 = disc.iter().filter(|&&d| d == 1).count();
        let n2 = disc.iter().filter(|&&d| d == 2).count();
        let lane0: Vec<u64> = (0..n0 as u64).map(|i| i.wrapping_mul(2654435761)).collect();
        // Lane 1 is Prod[u64, u64]: two parallel columns.
        let lane1_a: Vec<u64> = (0..n1 as u64).map(|i| i % 50).collect();
        let lane1_b: Vec<u64> = (0..n1 as u64).map(|i| i % 17).collect();
        // Lane 2 is List<u64>: ragged rows of length (i % 3) + 1.
        let lens: Vec<u64> = (0..n2 as u64).map(|i| (i % 3) + 1).collect();
        let mut bounds: Vec<u64> = Vec::with_capacity(n2);
        let mut acc = 0u64;
        for &l in &lens { acc += l; bounds.push(acc); }
        let total2 = acc as usize;
        let flat2: Vec<u64> = (0..total2 as u64).map(|i| i.wrapping_mul(7919)).collect();

        println!("[bench 12] sort 1M Sum<u64, Prod[u64,u64], List<u64>>");
        let runs = 5;
        let prog = parse("sort", &reg)?;
        let lane0_v = from_vec::<u64>(lane0.clone());
        let lane1_v = prod(vec![from_vec::<u64>(lane1_a.clone()), from_vec::<u64>(lane1_b.clone())]);
        let lane2_v = Value::List {
            bounds: crate::ir::value::bounds_var_from_ends(bounds.clone()),
            values: Arc::new(from_vec::<u64>(flat2.clone())),
        };
        let disc_prim = crate::ir::value::prim_p8(disc.clone());
        let sum_v = Value::Sum {
            disc: disc_prim,
            lanes: Arc::new(vec![lane0_v, lane1_v, lane2_v]),
        };
        let mut env: Vec<Value> = Vec::new();
        let colang_time = bench_run("collie", n, runs, || {
            let mut st = vec![sum_v.clone()];
            env.clear();
            eval(&prog, &mut st, &mut env).unwrap();
        });

        // Rust baseline: derive Ord on a 3-variant enum, materialize a
        // Vec<V>, sort.
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
        enum V {
            Int(u64),
            Pair(u64, u64),
            List(Vec<u64>),
        }
        // Pre-materialize Vec<V> outside the timed loop so we measure
        // the sort itself (matches the collie setup, which clones the
        // Sum value but the inner Arcs are cheap).
        let materialized: Vec<V> = {
            let mut out = Vec::with_capacity(n);
            let mut c0 = 0usize;
            let mut c1 = 0usize;
            let mut c2 = 0usize;
            for &d in &disc {
                match d {
                    0 => { out.push(V::Int(lane0[c0])); c0 += 1; }
                    1 => { out.push(V::Pair(lane1_a[c1], lane1_b[c1])); c1 += 1; }
                    2 => {
                        let lo = if c2 == 0 { 0 } else { bounds[c2 - 1] as usize };
                        let hi = bounds[c2] as usize;
                        out.push(V::List(flat2[lo..hi].to_vec()));
                        c2 += 1;
                    }
                    _ => unreachable!(),
                }
            }
            out
        };
        let rust_time = bench_run("rust  ", n, runs, || {
            let mut v = materialized.clone();
            v.sort_unstable();
            std::hint::black_box(v);
        });
        println!("  slowdown: {:.2}x", colang_time.as_nanos() as f64 / rust_time.as_nanos() as f64);
    }
    println!();

    // [bench 13] value-radix kernel: the in-place value radix vs the
    // sort_unstable baseline vs Rust's sort_unstable. Kernel-level
    // (no eval). Both collie kernels include a 1M unwrap/clone (the
    // input Arc is shared by the held `p`), matching the rust baseline's
    // explicit clone — so the comparison is apples-to-apples on movement.
    {
        use crate::ops::sort::{sort_bytes_radix, sort_bytes_std};
        let n = 1_000_000usize;
        let scrambled: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407))
            .collect();
        let small_range: Vec<u64> = (0..n as u64).map(|i| i % 1000).collect();
        let runs = 10;
        for (label, xs) in [("scrambled (full 8 bytes)", &scrambled), ("small range (% 1000)", &small_range)] {
            println!("[bench 13] value radix 1M u64 — {}", label);
            let p = Prim::P64(Arc::new(xs.clone()));
            let t_radix = bench_run("radix    ", n, runs, || {
                std::hint::black_box(sort_bytes_radix(p.clone()));
            });
            let t_std = bench_run("std (pdq)", n, runs, || {
                std::hint::black_box(sort_bytes_std(p.clone()));
            });
            let t_rust = bench_run("rust     ", n, runs, || {
                let mut v = xs.clone();
                v.sort_unstable();
                std::hint::black_box(v);
            });
            println!("  radix vs std: {:.2}x   radix vs rust: {:.2}x\n",
                     t_radix.as_nanos() as f64 / t_std.as_nanos() as f64,
                     t_radix.as_nanos() as f64 / t_rust.as_nanos() as f64);
        }
    }

    // [bench 14] Prod[u64,u64] sort variants head-to-head: the wired
    // packed-row sort vs the perm+gather core vs Rust. Small-range keys
    // pack to ~2 radix passes; full-range exercises the wide u128 key.
    {
        use crate::ops::sort::{sort_seq, sort_blocks};
        use crate::ops::helpers::gather;
        let n = 1_000_000usize;
        let sk0: Vec<u64> = (0..n as u64).map(|i| i % 7).collect();
        let sk1: Vec<u64> = (0..n as u64).map(|i| i % 100).collect();
        let fk0: Vec<u64> = (0..n as u64).map(|i| i.wrapping_mul(2654435761)).collect();
        let fk1: Vec<u64> = (0..n as u64).map(|i| i.wrapping_mul(40503).wrapping_add(7)).collect();
        let runs = 5;
        for (label, k0, k1) in [
            ("small range (%7, %100) — packs", &sk0, &sk1),
            ("full-range u64 (scrambled) — falls back", &fk0, &fk1),
        ] {
            println!("[bench 14] sort Prod[u64,u64] — {}", label);
            let prodv = prod(vec![from_vec::<u64>(k0.clone()), from_vec::<u64>(k1.clone())]);
            let order = vec![0u64; n];
            // Wired dispatch: packs when the key fits u64, else perm+gather.
            let t_wired = bench_run("collie   ", n, runs, || {
                std::hint::black_box(sort_seq(&order, &prodv, false).unwrap());
            });
            // The perm+gather core alone, for reference.
            let t_pg = bench_run("perm+gath", n, runs, || {
                let (perm, _l) = sort_blocks(&order, &prodv).unwrap();
                let pu: Vec<usize> = perm.iter().map(|&i| i as usize).collect();
                std::hint::black_box(gather(&prodv, &pu).unwrap());
            });
            // NOTE: a fair Rust baseline is omitted here — the obvious
            // loop-invariant closure gets LICM-hoisted out of the timed
            // loop and reports impossibly fast times. Compare against
            // perm+gather (real work) instead.
            println!("  collie vs perm+gather: {:.2}x\n",
                     t_wired.as_nanos() as f64 / t_pg.as_nanos() as f64);
        }
    }

    // [bench 15] search crossover: galloping sort-merge (sequential,
    // interp-free) vs the per-query random gallop, across query/target
    // ratios. M fixed; N swept from sparse to balanced.
    {
        use crate::ops::join::{search_sort_gallop, Search};
        use crate::ir::op::PrimOp;
        let m = 1_000_000usize;
        let target: Vec<u64> = (0..m as u64).map(|i| i * 3).collect(); // sorted
        let runs = 10;
        for n in [1_000usize, 10_000, 100_000, 1_000_000] {
            let queries: Vec<u64> = (0..n as u64)
                .map(|i| i.wrapping_mul(2654435761) % (m as u64 * 3)).collect();
            println!("[bench 15] search N={} into M={}", n, m);
            let tp = Prim::P64(Arc::new(target.clone()));
            let qp = Prim::P64(Arc::new(queries.clone()));
            let target_v = Value::Prim(tp.clone());
            let queries_v = Value::Prim(qp.clone());
            let t_merge = bench_run("merge    ", n, runs, || {
                std::hint::black_box(search_sort_gallop(&tp, &qp).unwrap());
            });
            let mut env: Vec<Value> = Vec::new();
            let t_gallop = bench_run("gallop   ", n, runs, || {
                let mut st = vec![target_v.clone(), queries_v.clone()];
                env.clear();
                Search.run(&mut st, &mut env).unwrap();
                std::hint::black_box(st.pop());
            });
            println!("  merge vs gallop: {:.2}x\n",
                     t_merge.as_nanos() as f64 / t_gallop.as_nanos() as f64);
        }
    }

    Ok(())
}
