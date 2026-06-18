# Corgi performance gaps — pre-DPS baseline

The throughput companion to `expressivity-gaps.md`.
That audit asks what the surface cannot say; this one asks where the engine leaves throughput on the floor, measured against an honest hand-written-Rust ceiling, before destination-passing style is built.

The suite is `benches/gaps.rs` (`cargo bench --bench gaps`).
Every number below is best-of-reps wall time on this M4 mini, corgi `eval` vs an inline Rust ceiling on identical data, `black_box`ed both ends.
The gap is always decomposed to a mechanism, because only some mechanisms are what DPS fixes.

## Method

Sizing targets the cache hierarchy, not L1 microbenchmarks.
The M4 mini has 128 KB L1D, ~16 MB L2 per P-cluster, and an ~8 MB SLC; there is no conventional L3.
Three anchors: 8 K rows (64 KB, an L1 control that proves dispatch is amortized — not a target), 1 M rows (8 MB/column, the L2/SLC design center), and 8 M rows (64 MB/column, the DRAM-streaming eye).

The Rust ceiling is the honest efficient version, not a strawman: a single fused loop for a chain, a predicated push for filter, a two-pointer sort-merge for the join, a bucket-accumulate for a low-cardinality group-by.

For chains the gap is split two ways.
`tax = corgi_k / rust_k` compares corgi's k passes to Rust's k *un-fused* passes — the engine's per-pass overhead (dispatch, allocation).
`fusion-prize = rust_k / rust_1` compares Rust un-fused to Rust fused — the pure win DPS would capture.
Reporting them separately keeps the fusion headroom from hiding inside an execution-overhead number, and vice versa.

The single bandwidth figure is input-GB/s.
On this machine a single-thread streaming pass tops out near 30–40 GB/s in L2/SLC and ~21–24 GB/s from DRAM; the drop across the 8 MB→64 MB boundary is the bandwidth-bound vs compute-bound tell.

## The gap map

Ratios are corgi/Rust slowdown (higher = corgi slower) at the L2/SLC design center (1 M) and the DRAM eye (8 M); for chains, the fusion-prize is shown instead of a single ratio.
"DPS?" marks whether destination-passing / fusion is the lever, or the gap is something else entirely.

| task | family | 1 M | 8 M | mechanism | DPS? |
|---|---|---|---|---|---|
| A1 add_const | pointwise | 1.6× | 1.8× | single pass, near bandwidth ceiling | — at ceiling |
| **A2 add_chain8** | pointwise | tax 1.0×, **prize 8.4×** | tax 1.0×, **prize 8.0×** | per-pass at Rust ceiling; gap is 8 un-fused passes | **YES — the clean case** |
| A3 mixed_chain | pointwise | tax 3.6×, prize 1.7× | tax 3.3×, prize 1.4× | `mul`/`sub` by constant materialize a full constant column + a `Prod` | immediate kernels first, then DPS |
| A4 map_reduce | pointwise | tax 4.1×, prize 2.8× | tax 3.3×, prize 2.7× | intermediate map column, then a fold over it | **YES — fuse map into reduce** |
| B1 filter_values | selection | 2.8× | 3.5× | mask column + scalar `filter_mask` + gather (3 passes) vs one predicated push | partly — fusion + SIMD select |
| B2 cmp_select | selection | 7.7× | 7.8× | 4–5 column materializations vs one fused loop | **YES — fusion** |
| C1 fold_add | aggregation | 2.3× | 2.6× | read-only fold **clones the shared Arc buffer** before summing | reuse: borrow on read |
| C2 fold_max | aggregation | 1.7× | 2.2× | same shared-buffer clone | reuse |
| **C3 group_by_sum** | aggregation | **48×** | **94×** | sort-based group where a 256-bucket accumulate is one O(n) pass | no — missing narrow-key op |
| **C4 scan_prefix** (general) | aggregation | **~1400×** | **~1300×** | lockstep foldscan degenerates on one long row: #rounds = row length, body re-eval per round | monoid body → C4k; general → interpreter |
| C4k scan_add (kernel) | aggregation | **1.6×** | **1.8×** | the monoid prefix kernel — one in-place pass; **closes C4 by ~800×**, now at the single-pass floor | **DONE** (`ArithOp::Scan`) |
| **C5 fold** | aggregation | **~6900×** | **~5000×** | same lockstep degeneration; here a product-of-monoids `(sum,count)` accumulator | monoid → kernel; else interpreter |
| D1 sort_u64 | order | 2.6× | 4.7× | radix-permute + gather; the gather scatter is DRAM-bound at scale | no — sort kernel |
| D2 dedup | order | 2.9× | 4.8× | sort + adjacent unique | no — sort kernel |
| **E1 join_find_slices** | relational | **13×** | **13×** | `find` is a per-probe search, not a merge over sorted probes; + `slices` gather | no — wants a merge-join path |
| E2 gather | relational | 1.8× | 1.3× | `resolve_indices` scalar pass above the gather; latency-bound at DRAM | minor |
| E3 gather_chain | relational | 1.9× | 1.4× | two gathers, each resolve+gather | index-composition rewrite |
| **F1 branch_match** | sum-type | **45×** | **45×** | partition+recombine where the scalar form vectorizes to a blend | no — use `select`; `match` pays off only on heterogeneous lanes |
| F2 unweave_shred | sum-type | 4.5× | 4.1× | build sum (branch) + unweave vs one-pass partition | partly |
| G1 word_count | text | 3.5× | 3.7× | structural ragged-string sort + find vs slice-sort + run-count | no — sort kernel |
| G2 csv_sum | text | 2.2× | 2.2× | total `parse_u64` (Sum) + reduce — competitive with hand atoi | — near ceiling |

## What this reorders

The headline finding is that DPS is **not** the largest gap.
The suite was built to size the DPS payoff, and it does: on a pure pointwise chain corgi already runs at the Rust per-pass ceiling (tax ≈ 1×), so the entire ~8× gap is un-fused passes that fusion collapses — the textbook DPS win.
On `map_reduce` and `cmp_select`, fusion is worth ~2.7× and ~7.8×.

But three larger gaps sit beside it, none of which DPS touches:

1. **fold / scan on a long column is ~1000–7000× off a trivial loop.**
   The lockstep fold is built to vectorize across *many short rows*; with one long row it runs one round per element, re-evaluating the body sub-graph with per-round allocation — a tree-walking interpreter at element granularity.
   Prefix-sum of a column is the common case and it is the worst case.
   This needs a single-row (tight scalar accumulator) fast path, not fusion.
   The cases stratify by fixability:
   - **scalar-leaf monoid** (cumsum, running min/max/product/all/any) — **done.** `ArithOp::Scan(Red)` (`scan_add`/`scan_min`/…) is a one-pass in-place kernel, the prefix sibling of `fold_*`; measured 1.6–1.8× (the C4 ~1400× → C4k floor), corpus `55`/`56`.
   - **fixed-width non-monoid body** (affine recurrences, small state machines) — inherently sequential, so the lever is the per-step constant: a single-row interpreter stepping the body over register/stack scratch, no per-element heap. ~10–50×, usable, not yet built.
   - **product-of-monoids accumulator** (`(sum, count)`) — a kernel that updates each field, reached either by a richer named form or a `FoldScan[monoid body] → kernel` rewrite. Not a bare verb.
   - **`List`/`Sum` (variable-size) accumulator** — the genuine residual: the accumulator is heap and reshapes per step, so it allocates by necessity (the eval already `gather_lanes`-rebuilds here). No tight-loop fix; keep it correct, steer to structural ops, accept it. Rare.

2. **group-by on a low-cardinality key is 48–94× off.**
   corgi only has the general structural `group`, which sorts; a 256-bucket sum is one O(n) accumulate pass.
   The missing piece is a narrow-key fast path (histogram / counting accumulate), the same lever collie added to its `group`.

3. **`mul`/`sub` by a constant cost a full extra column.**
   The `pair_imm` desugaring (`x sub 1` → `(x, x lit 1) sub`) makes `Lit` broadcast an n-element constant column and build a product, where `AddU64`/`Shr`/`And`/`Gt` have immediate kernels that touch neither.
   This is the bulk of the mixed-chain tax and is a cheap, local fix.

And one reuse gap that is DPS-adjacent: read-only reductions clone the shared input buffer (`into_u64` forces ownership) before folding, doubling traffic on an operation that only reads.

The relational and sum-type families resolved two stated unknowns:

4. **The scalar index generators are cheap, not the bottleneck.**
   `gather` runs at 1.3–1.9× of hand-written Rust; `resolve_indices` adds one near-identity pass and the random-access latency dominates, so the two converge at DRAM scale.
   The worry that `filter_mask`/`resolve_indices`/`expand_ranges` would dominate join/gather work is not borne out for the point gather.

5. **The single-key join is 13× — because `find` searches per probe instead of merging.**
   With both sides sorted, the Rust ceiling is a two-pointer merge; corgi's `find` does an independent search per probe key and then `slices` materializes.
   This is a relational-op gap (a merge-join path for the sorted-probe case), not fusion.

6. **`branch`/`match` is the wrong tool for a uniform conditional (45×).**
   The scalar `if` vectorizes to a blend; corgi's data-parallel `branch`/`match` physically partitions and recombines (several gathers).
   The right corgi spelling for a same-typed conditional is `select`; `branch`/`match` earns its partition cost only when the lanes carry *different* types or work — which is exactly the expressivity no array language has, and not what this microbench exercises.
   This is a documentation/usage finding, not an engine defect.

Text came out competitive once the ceiling was honest: `csv_sum` at 2.2× (the total `parse_u64` Sum path is close to hand atoi), `word_count` at 3.5× (corgi's structural ragged-string sort vs a fixed slice sort).

## Recommended order (preliminary)

1. **fold/scan single-row fast path** — by far the largest gap (~1000–7000×), a common workload (prefix-sum / running aggregate of a column), self-contained.
   *Scalar-leaf monoid case done* (`ArithOp::Scan`, ~1400× → 1.6×); the fixed-width-body interpreter and the product-of-monoids kernel are the remaining tiers (the variable-size-accumulator case is left expensive by design).
2. **group-by narrow-key fast path** — second-largest (48–94×), a known pattern (counting/bucket accumulate for low-cardinality keys).
3. **immediate kernels for `mul`/`sub`/`min`/`max` by a constant** — cheap, removes the constant-column tax (3–5×) across every arithmetic pipeline.
4. **merge-join path for `find` over sorted probes** — closes most of the 13× single-key join gap.
5. **read-only reuse for reductions** — stop cloning a buffer you only read (~2.3×).
6. **DPS / tile fusion** — the clean ~8× on chains and ~2–8× on map-reduce / filter / cmp-select, exactly as the backlog frames it; now with a measured baseline to verify against, and confirmed to be the right lever for the pointwise and selection families specifically.

The ordering is the point.
The suite was meant to size DPS, and it does: corgi already runs at the Rust per-pass ceiling, so fusion is the whole prize on chains.
But fold/scan, group-by, the constant-column tax, and the per-probe join each dwarf it, and the index generators it was feared might dominate turned out cheap.
DPS stays on the roadmap; it is no longer obviously first.
