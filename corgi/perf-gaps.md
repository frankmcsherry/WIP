# Corgi performance gaps

> **Status:** current. Measured 2026-09-02 at `b9bb413` on an Apple M4 mini (16 GB), by
> `cargo bench --bench gaps`. This replaces the pre-DPS baseline, which predated the A–I/R family
> definition and was wrong about where the gaps are — see *What this reorders*.

To exercise the definition without producing reportable measurements, run
`cargo bench --bench gaps -- --smoke`. Reportable runs can select one or more families with
`cargo bench --bench gaps -- --family A` or `--family C,R`; omitting the selector runs all families.

The throughput companion to `expressivity-gaps.md`.
That audit asks what the surface cannot say; this one asks where the engine leaves throughput on the floor, measured against an honest hand-written-Rust ceiling.
The gap is always decomposed to a mechanism, because only some mechanisms are what fusion fixes.

## Method

Sizing targets the cache hierarchy, not L1 microbenchmarks.
The M4 mini has 128 KB L1D, ~16 MB L2 per P-cluster, and an ~8 MB SLC; there is no conventional L3.
Three anchors: 8 K rows (64 KB, an L1 control that proves dispatch is amortized — not a target), 1 M rows (8 MB/column, the L2/SLC design center), and 8 M rows (64 MB/column, the DRAM-streaming eye).

The Rust ceiling is the honest efficient version, not a strawman: a single fused loop for a chain, a predicated push for filter, a two-pointer sort-merge for the join, a bucket-accumulate for a low-cardinality group-by.

For chains the gap is split two ways.
`tax = corgi_k / rust_k` compares corgi's k passes to Rust's k *un-fused* passes — the engine's per-pass overhead (dispatch, allocation).
`fusion-prize = rust_k / rust_1` compares Rust un-fused to Rust fused — the pure win fusion would capture.
Reporting them separately keeps the fusion headroom from hiding inside an execution-overhead number, and vice versa.

The single bandwidth figure is input-GB/s.
On this machine a single-thread streaming pass tops out near 30–40 GB/s in L2/SLC and ~21–24 GB/s from DRAM; the drop across the 8 MB→64 MB boundary is the bandwidth-bound vs compute-bound tell.

### Two things to know before reading a row

**The input column is SHARED.** `corgi_t` hands each rep `arg.clone()` and keeps `arg` alive, so
inside the timer the leaf's `Arc` is at refcount 2 and an op that CONSUMES its operand cannot take
the buffer — it copies the column first.
That is real (a node with fan-out 2 pays exactly this) but it is not what the harness comment claims
to measure, and on a single-pass workload it *is* the measurement.
It used to apply to almost every row; it now applies only to the four ops that rewrite their operand
in place (`AddU64`, `Shr`, `And`, `Scan`), because every op that merely READS now borrows.
Probed directly at 1 M rows, sole-owned input against the same call with a reference retained:

| workload | sole-owner | shared | |
|---|---|---|---|
| `fold_add` (reader) | 0.102 ns/row | 0.071 | 1.0× — was 7.9× |
| `add_u64 1` (in-place) | 0.119 | 0.870 | 7.3× |
| `scan_add` (in-place) | 0.247 | 0.699 | 2.8× |

So A1 and C4k are still upper bounds; the rest of the matrix is no longer distorted by it.
Fixing the harness properly is not a one-liner: handing each rep a freshly-built column removes the
copy but leaves the buffer cache-warm, and corgi's in-place ops then measure *above* the machine's
memory bandwidth (A1 reads back at 65 GB/s against a ~21–24 GB/s DRAM ceiling), which is just as
wrong in the other direction. It wants pre-built owned columns plus cache eviction between build and
timer.

**Ratios below ~2×, and any ratio whose Rust side is sub-nanosecond, carry run-to-run noise of up to
2×.** The Rust ceilings move: `F1`'s has measured anywhere in 0.46–0.93 ns/row across runs, which
alone swings that row between 8.5× and 16×. Track corgi's absolute ns/row for corgi's own progress,
and read the ratios as an order of magnitude, not a measurement.

## The gap map

Ratios are corgi/Rust slowdown (higher = corgi slower); for chains, tax and fusion-prize are shown instead.
"Lever" names what would close it.

| task | family | 8 K | 1 M | 8 M | mechanism | lever |
|---|---|---|---|---|---|---|
| A1 add_const | pointwise | 1.8× | 0.92× | 1.09× | single pass at the bandwidth ceiling (upper bound — in-place op, shared input) | — at ceiling |
| A2 add_chain8 | pointwise | tax 1.06 / prize 6.65 | tax 0.93 / prize 2.11 | tax 1.26 / prize 3.69 | per-pass at the Rust ceiling; the gap is un-fused passes | **fusion** |
| A3 mixed_chain | pointwise | tax 2.76 | tax 2.79 | tax 1.61 | `mul`/`sub` by a constant lack immediate kernels, so `Lit` broadcasts a column and builds a `Prod` | immediate kernels, then fusion |
| A4 map_reduce | pointwise | tax 2.54 / prize 2.18 | tax 1.10 / prize 14.4 | tax 1.25 / prize 9.30 | intermediate map column, then a fold over it | **fusion — the biggest prize on the board** |
| B1 filter_values | selection | 3.3× | 2.7× | 3.9× | mask column + scalar `filter_mask` + gather vs one predicated push | partly fusion; SIMD compaction |
| B2 cmp_select | selection | 4.5× | 3.3× | 3.7× | 4 passes vs 1 fused | fusion |
| C1 fold_add | aggregation | 1.2× | **1.00×** | **1.00×** | one SIMD pass, at the Rust ceiling | — at ceiling |
| C2 fold_max | aggregation | 1.05× | **1.00×** | **1.01×** | one SIMD pass, at the Rust ceiling | — at ceiling |
| **C3 group_by_sum** | aggregation | 22× | **49–63×** | **79–82×** | sort-based group where a 256-bucket accumulate is one O(n) pass | no — **missing narrow-key op** |
| **C4 scan_prefix** (general) | aggregation | **1150×** | **252×** | **205×** | lockstep foldscan on ONE long row: #rounds = row length, body re-evaluated per round | monoid body → C4k; general → single-row interpreter |
| C4k scan_add (kernel) | aggregation | 1.4× | 0.98× | 1.09× | the monoid prefix kernel — one in-place pass | **DONE** |
| **C5 fold_sum_count** | aggregation | **6394×** | **4716×** | **2949×** | same lockstep degeneration, product-of-monoids accumulator | monoid kernel, or the interpreter |
| D1 sort_u64 | order | 1.3× | 1.2× | 2.6× | the indexed sort: keys pulled once, radixed with the permutation alongside, emitted as the column; the carried permutation is the residue | values-only leaf mode |
| D2 dedup | order | 1.7× | 1.5× | 2.8× | the same sort, run starts read off the sorted column | values-only leaf mode |
| **E1 join_find_slices** | relational | — | 5.3× | 6.3× | `find` searches per probe instead of merging two sorted runs | merge-join path |
| E2 gather | relational | — | **0.71×** | 1.03× | corgi at or below the Rust ceiling | — |
| E3 gather_chain | relational | — | 1.00× | 1.25× | two gathers, each resolve+gather | index-composition rewrite |
| **F1 branch_match** | sum-type | — | **16×** | **15×** | columnar partition + recombine where the scalar form vectorizes to a blend | use `select`; `match` pays off on heterogeneous lanes |
| F2 unweave_shred | sum-type | — | 3.0× | 3.4× | build sum (branch) + unweave vs one-pass partition | partly |
| G1 word_count | text | — | 1.8× | 1.7× | structural ragged-string sort + find vs slice-sort + run-count | no — sort kernel |
| G2 csv_sum | text | — | 1.9× | 1.9× | total `parse_u64` (Sum) + reduce vs hand atoi | — near ceiling |
| H gather (safe) | safety | 0.33 vs 0.67 | 1.04 vs 0.89 | 0.57 vs 1.40 ns/row | corgi's TOTAL (bounds-checked) sequential gather vs Rust `unsafe` | — **no safety tax** |
| I pointer-chase | latency | — | 0.64 vs 5.40 | 0.78 vs 9.58 ns/step | lockstep gather extracts MLP a serial chase cannot | — **8–15× FASTER** |
| R1 arrange_sort_perm | arrangement | — | **0.50×** | **0.83×** | the indexed sort's permutation vs Rust stable sort with cached keys | — |
| R7 arrange_sort_sum | arrangement | — | 0.74× | 0.68× | a Sum of four u64 lanes, one block, vs a stable cached-key Rust sort on (tag, payload) | — |
| R8 arrange_sort_list | arrangement | — | 0.48× | 0.27× | a List of u64 of lengths 0..4, one block, vs a stable Rust sort on (len, elements) | — |
| R9 arrange_sort_sum_seg | arrangement | — | 5.1× | 5.5× | the Sum under a block per four rows, vs a stable Rust sort per row (was 15×) | per-call fixed cost |
| R10 arrange_sort_list_seg | arrangement | — | 3.5× | 3.3× | the List under a block per four rows, vs a stable Rust sort per row (was 18×) | per-call fixed cost |
| R2 arrange_compare | arrangement | — | 3.3× | 2.6× | batched adjacent compare vs a direct leaf compare | — |
| R3 arrange_find | arrangement | — | 0.98× | 1.01× | the u64 fast path, at the Rust partition-point ceiling | — |
| R4 arrange_survey | arrangement | — | **0.25×** | **0.78×** | galloping runs beat a two-pointer survey | — |
| R5 arrange_gather2 | arrangement | — | 1.36× | 1.12× | two-source column gather vs a direct two-slice gather | — |

## What this reorders

**Fusion is still not the largest gap, and the largest gap is still the general fold.**
`scan_prefix` and `fold_sum_count` are 205–6394× off a trivial loop, and nothing else on the board is within two orders of magnitude of them.
The lockstep fold is built to vectorize across *many short rows*; handed one long row it runs one round per element, re-evaluating the body sub-graph each round.
Prefix-sum of a column is the common case and it is the worst case.
The cases stratify by fixability, unchanged from the previous audit:

- **scalar-leaf monoid** (cumsum, running min/max/product/all/any) — **done.** `ArithOp::Scan(Red)` is a one-pass in-place kernel, and C4k now measures 0.98–1.4×: at or below the Rust cumsum loop.
- **fixed-width non-monoid body** (affine recurrences, small state machines) — inherently sequential, so the lever is the per-step constant: a single-row interpreter stepping the body over register/stack scratch, no per-element heap. Not built.
- **product-of-monoids accumulator** (`(sum, count)`) — a kernel that updates each field, reached by a richer named form or a `FoldScan[monoid body] → kernel` rewrite. This is C5, the worst row on the board.
- **`List`/`Sum` (variable-size) accumulator** — the genuine residual: the accumulator is heap and reshapes per step. No tight-loop fix; keep it correct, steer to structural ops, accept it. Rare.

**But the same lockstep mechanism is a WIN, which the old audit never recorded.**
Family I did not exist then. Corgi's lockstep gather chases 1024 pointer chains at 0.64–0.78 ns/step against a naive serial chase at 5.40–9.58 — **8–15× faster**, and within 1.1–1.6× of a hand-written lockstep loop.
The fold is not broken; it is specialized for many rows and pathological for one. Read C4/C5 and I together or you will draw the wrong conclusion from either.

**Corgi's total gather has no safety tax.**
Family H, also new: the bounds-checked sequential `gather` runs at 0.33/1.04/0.57 ns/row against Rust `unsafe` at 0.67/0.89/1.40, winning at the L1 and DRAM points and within 17% at 1 M.
The random-access variant trails `unsafe` at 1 M (1.55 vs 1.00) and beats it at 64 MB (4.60 vs 5.32), where latency dominates.
The vectorized all-or-nothing check is cheaper than Rust's per-element bounds check, and competitive with eliding the check entirely.

**Two rows the old audit called gaps are now wins.**
`E2 gather` is 0.71×/1.03× — at or below the Rust ceiling, where the old baseline reported 1.8×/1.3×.
`R4 arrange_survey` is 0.25×/0.78×, and `E3 gather_chain` has closed from 1.9×/1.4× to 1.00×/1.25×.
The old baseline's `E1 join` at 13× is now 5.3–6.3×.
Those rows are why navigating by the old map was unsafe: it was directionally wrong about the relational family.

**Three gaps stand, unchanged in character:**

1. **group-by on a low-cardinality key, 49–82×.** corgi has only the general structural `group`, which sorts; a 256-bucket sum is one O(n) accumulate pass. The missing piece is a narrow-key fast path, the same lever collie added to its `group`.
2. **the single-key join, 5.3–6.3×.** With both sides sorted the Rust ceiling is a two-pointer merge; corgi's `find` does an independent search per probe and then `slices` materializes. A relational-op gap, not fusion. (`arrange::survey` already *is* the merge kernel — it is the surface `join` that does not reach it.)
3. **`mul`/`sub` by a constant cost a full extra column.** The `pair_imm` desugaring makes `Lit` broadcast an n-element constant and build a product, where `AddU64`/`Shr`/`And`/`Gt` have immediate kernels that touch neither. Cheap and local; A3's tax is 1.6–2.8× and this is most of it.

**The aggregation controls were 8–12× off the ceiling they are named for, and the mechanism was a copy.**
`C1 fold_add` and `C2 fold_max` measured 12.1× and 8.0× at 1 M under a harness that shares the input
column, because `Reduce` took its operand by value and so copied 8 MB to read it. 15 of the 20
`into_u64`/`into_u8` call sites only READ their operand; they now borrow (`as_u64`/`as_u8`), and both
rows measure at 1.00×. `B1`/`B2` (the masks) and `G2` (the byte column) picked up 10–20% from the
same change. Ownership survives exactly where an op rewrites its operand — `AddU64`, `Shr`, `And`,
`Scan`, and the one-row gather kernels that turn the index buffer into the result — which is why A1
and C4k did not move, and that is the tell that the mechanism was the copy and not something else.

**What is left on the measurement side:** fix `corgi_t` to hand over pre-built owned columns with the
cache cleared between build and timer, so A1 and C4k stop being upper bounds; and consider reporting
corgi's absolute ns/row alongside the ratio, since the Rust ceilings move by up to 2× between runs.

## K — closure capture, by copy and by reference (measured 2026-09-30, master + `Ref`)

> `cargo bench --bench gaps -- --family K`. Same machine and method as above; ns/element and × Rust.

`cap_list` (the List strength, `(X, List<Y>) -> List<(X,Y)>`) is `gather(x, owner_ids(bounds))`, and `gather` recurses through every shape.
For a scalar context that is the SIMD operand you want.
For a **List-shaped context** it copies the whole list once per element, so capture costs `elements × context length` where a Rust closure holds a reference and pays `elements`; a tuple context copies every field, read or not.
`Ref` (#34) makes the other choice writable: `(ctx ref, ys) cap_list` captures one span per element, and `gather` on a reference moves spans, never the payload.

The suite fixes the element count at 64 K (64 owners × 1 K elements; the `n=1` column is one dictionary broadcast to every key) and sweeps the captured list's length `L`.
Each task has three spellings: by reference, by value (`x`), and a capture-free **control** (`c`) — what Field pushdown or the capture→gather rewrite would produce.

| task | L=16 | L=256 | L=2048 | n=1, L=2048 | ns/element, L=16 → 2048 |
|---|---|---|---|---|---|
| **K1** `(ctx ref, ys) cap_list … get` — capture a list, `get` one element per element | 4.6× | 4.7× | **1.05×** | 11.7× | 2.8 → 3.0 |
| K1x the same without `ref` (copies) | 31× | 472× | 2360× | 8144× | 19 → 6703 |
| K1c `(ys, ctx) gather` (no capture) | 2.1× | 2.2× | 0.49× | 0.87× | 1.3 → 1.4 |
| **K2** `(ctx ref, ys) cap_list … (c.0, y) add` — capture `(scalar, list)`, read the scalar | 3.3× | 3.4× | **1.27×** | 0.78× | 1.24 → 1.26 |
| K2x the same without `ref` (copies) | 49× | 1214× | 4956× | 3548× | 18 → 4919 |
| K2c capture `ctx.0` only | 2.0× | 2.0× | 2.0× | 0.47× | 0.75 → 1.97 |

Reading the sweep: the referenced spellings are flat in L (2.8 → 3.0 ns and 1.24 → 1.26 ns); the by-value spellings are ≈ 1.4–3 ns × L, a copy of the context per element.
The ratios fall at L=2048 because the Rust ceiling rises there (its random `get` into a 1 MB context misses L1), not because corgi got faster.
K2 is referenced through the tuple — `ref` leaves the scalar by value and makes the unread list one span per element — and costs within 2× of capturing the scalar alone; the rest is Field pushdown's job.
K1 at `n=1` (7.2 ns) is the one referenced row above 3 ns; not investigated.

The by-value spellings stay in the suite on purpose: the copy is a visible choice in the program, and its cost is the row below the reference.
A first spike put the spans inside `Bounds` and decided reference-vs-copy by which gather a producer called (branch `corgi-spans`); it closed the same gap and was rejected because nothing in the program or the shape showed the choice.

## P — permuted references read at random (measured 2026-09-30, master + `Ref`)

> `cargo bench --bench gaps -- --family P`. ns/element and × Rust, two runs agreeing to ~20% except where noted.

The other side of K: a reference reads its row where the row lives.
A column of references in another order than its payload — after a `sort`, a `gather`, a merge — reads the payload at random, one miss per row where a by-value column streams.
The tasks fix the element count (1 M = 8 MB, the L2/SLC point; 8 M = 64 MB, DRAM) and sweep the row length k, all rows in one list row (`List<Ref<List<U64>>>`, the shape `slices` hands out).

| task | 1 M, k=1 | 1 M, k=8 | 1 M, k=64 | 8 M, k=1 | 8 M, k=8 | 8 M, k=64 |
|---|---|---|---|---|---|---|
| P1 `clone`, references in payload order | 1.9 | 0.96 | 0.73 | 2.1 | 0.96 | 0.74 |
| **P1p** `clone`, references permuted | 2.7 | 1.4 | 1.0–2.1 | **5.3** | **3.1** | **2.3** |
| P1p / P1 (corgi) | 1.4× | 1.4× | 1.4–2.9× | **2.5×** | **3.2×** | **3.1×** |
| P1p / P1 (Rust copy-out) | 1.1× | 1.6× | 1.6× | 5.3× | 6.2× | 5.6× |
| **P2** `sort clone` — sort permuted references, then read | 27–32 | 8.0–10.6 | 4.7–6.1 | 59–61 | 14.3–14.6 | 9.5–10.0 |
| P2c `clone sort` — clone first, sort by value | 21–24 | 5.8–7.8 | 4.3–4.5 | 44–46 | 10.8 | 6.7–7.4 |

Reading it: at the L2/SLC point permuted references cost ~1.4×; at DRAM 2.5–3.2×, and a longer row does not amortize the miss away (k=64 is 512 B a row and still 3×).
The Rust ratio is worse (5–6× at DRAM) because Rust's sequential copy is ~3× faster than corgi's; the gap will widen as corgi's constant factors improve.
(At k=1 corgi's permuted read beats a per-row Rust loop, 5.3 vs 10.8 ns: `clone` resolves every position first and then gathers in one pass, which keeps more misses in flight.)

For a sort the order of operations matters: `sort` on references orders a scratch copy and hands back permuted references, so every later full read pays P1p; `clone sort` pays one copy and hands on sequential data.
P2c is never slower than P2 and is 1.05–1.4× faster, before counting any second read.
Whether ordering ops on a reference should require `clone` (the shape error "clone first") instead of returning references is the open design question this family exists to inform.

## W — the WCO join step through references (measured 2026-09-30, master + `Ref`)

> `cargo bench --bench gaps -- --family W`. 1024 anchors × 16 searches fixed; the large side D swept 16 → 16384. ns/search and × Rust.

The worst-case-optimal step: per anchor, a SMALL list and a range of a shared sorted adjacency (the LARGE side); every element of the small side binary-searches its anchor's large side, for Σ |small| · log |large|.
`find` already pairs needle row i with haystack row i and searches per element, so the question is how the per-anchor haystack rows are produced.
By value, `slices` copies each anchor's range out (Σ |large|, the cost WCO exists to avoid). By reference, each anchor holds a reference to the adjacency, `slices` on it hands out one reference per anchor's range, and `find` searches through it:

```
let (small, ranges, adj) = input in
let hay = (ranges len sub 1, (ranges, adj) slices) get in
(small, hay) find
```

| spelling | D=16 | D=256 | D=4096 | D=16384 | ns/search | shape in D |
|---|---|---|---|---|---|---|
| **W1** `adj` referenced per anchor | 9.2× | 7.9× | 6.2× | **5.3×** | 27 → 47 → 66 → 86 | log D — WCO cost |
| W1x `adj clone` per anchor, `slices` copies the range | 23× | 26× | 119× | **410×** | 68 → 156 → 1283 → 6722 | linear in D — the copy |

Rust's ceiling is 2.9 → 16.4 ns per search; W1 tracks it with a 5–9× constant (slices, get and find each a pass), W1x tracks D. Both spellings are checked equal once per run.
The per-anchor reference to the adjacency is built by the host — the surface has no value broadcast (`lit` lifts u64 constants only) — so "the same adjacency for every anchor" is not yet writable in a program; mechanical closure capture is what would supply it.
`programs/62-wco-triangle.col` spells a whole triangle query this way, choosing the smaller side per anchor with a `branch`.
