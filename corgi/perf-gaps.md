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

## K — closure capture copies the context (measured 2026-09-17, current master)

> Added after the pre-DPS baseline above; measured on the same M4 mini with `cargo bench --bench gaps -- --family K`.

The worst case in the engine today is not a slow kernel but a **representation choice**: `cap_list` (the List strength, `(X, List<Y>) -> List<(X,Y)>`) is implemented as `gather(x, owner_ids(bounds))`, and `gather` recurses through every shape.
For a scalar context that is the SIMD operand you want.
For a **List-shaped context** it copies the whole list once per element, so capture costs `elements × context length` where a Rust closure holds a reference and pays `elements`.
A tuple context is no better: every field is gathered, including the ones the body never reads.

The suite fixes the element count at 64 K (64 owners × 1 K elements; the `n=1` row is one dictionary broadcast to every key) and sweeps the captured list's length `L`.
Each task has a corgi **control** that reaches the same result without the capture — the spelling a by-reference capture (or the capture→gather / Field-pushdown rewrite) would recover — so the gap splits into capture-copy vs engine.

| task | L=16 | L=256 | L=2048 | n=1, L=2048 | ns/element at L=2048 | control (no capture) |
|---|---|---|---|---|---|---|
| **K1 cap_list_get** — capture a list, `get` one element per element | 41× | 746× | **2088×** | **2226×** | 2829 ns (corgi) vs 1.36 (Rust) | `(ys, ctx) gather`: 0.9 ns, **0.7–1.6×** Rust |
| **K2 cap_tuple_add** — capture `(scalar, list)`, body reads only the scalar | 54× | 345× | **8196×** | **8396×** | 2704 ns (corgi) vs 0.33 (Rust) | capture `ctx.0` only: 0.65 ns, **2.0×** Rust |

Reading the sweep: corgi's cost per element is ≈ 1.4 ns × L (21 → 396 → 2829 ns as L goes 16 → 256 → 2048) — a memcpy of the context per element, exactly linear in what should be a free reference.
The controls are flat in L at 0.5–0.9 ns/element, at or under the Rust ceiling.
(The Rust K1 ceiling rises 0.5 → 1.4 ns with L because the random `get` into a 1 MB context misses L1; the 1.3 ns Rust K2 reading at L=256 is timer noise at 30 reps.)
Peak memory tracks the same product: at L=2048 the capture materializes 128 M words (1 GB) plus a 1 GB index vector to produce 64 K lookups.

**Mechanism.** Two things compound: (1) capture is spelled as a copy of the context per element rather than a reference to it, and (2) capture of a Prod is eager on every field, so an unread list field costs as much as a read one.
Neither is a kernel gap; `gather` itself runs at ~1× Rust (E2). This is a value-model gap.

**Fix — two spikes, same machine, same suite.**

*Spike A, branch `corgi-spans`:* a `Bounds::Spans` form so a List row can be a reference, produced implicitly by the capture family (`gather_shared`) and read through a second accessor (`into_list_shared`), with `into_list` compacting for everyone else. It closes the gap (K1 2088× → 3.6×, K2 8196× → 7.5×) but it folds the reference-or-copy decision into which gather a producer calls, which accessor a reader calls, and a bounds variant nobody can see in the program or the shape. Rejected for that reason.

*Spike B, branch `corgi-ref` (this branch):* **the reference is a shape.** `Ref<T>` is a column of references; `ref` takes them (O(rows), nothing copied), `clone` copies the rows out and is the ONLY place referenced data is copied. The representation is fixed by the referenced shape — a referenced list row is a `(lo, hi)` span of the list's payload (`&[T]`), any other referenced row is a row index into the arena (`&T`) — so there is no runtime choice. The two forms are Rust's thin and fat pointers (`Refs::Thin` / `Refs::Fat`); the fat one is what lets `slices` hand out sub-range references, which is why the split is kept rather than collapsed to (VALUE, INDEXES). `gather` has one meaning everywhere (by value) plus one arm: on a Ref it moves refs and never touches the arena, which makes `cap_list`/`cap_sum`/`lit` free on a referenced context with no code of their own. `Field` projects through a referenced product by reference. The readers `get`/`gather`/`find`/`slices`/`len` accept a referenced list haystack (`into_rows`: rows via `span(i)` over one payload); every other op given a Ref is the shape error "expected a list, got Ref<..>" — nothing is silently materialized. `slices` on a referenced haystack returns `List<Ref<List<T>>>`, references, O(ranges); on a list it copies as before. `Bounds` is untouched and always a partition.

| task | L=16 | L=256 | L=2048 | n=1, L=2048 | ns/element at L=2048 |
|---|---|---|---|---|---|
| K1 `(ctx ref, ys) cap_list … get` | 7.1× | 3.1× | **7.3×** | **3.7×** | 4.1 ns |
| K1x the same without `ref` (copies) | 38× | 259× | 5025× | 2212× | 2843 ns |
| K2 `(ctx ref, ys) cap_list … (c.0 clone, y) add` | 3.7× | 3.7× | **4.0×** | **0.9×** | 1.3 ns |
| K2x the same without `ref` (copies) | 54× | 1261× | 8381× | 2032× | 2707 ns |

Both referenced spellings are flat in L. K2 lands better than spike A's 7.5× because a referenced tuple is one index per element and only the scalar is ever copied (at the `clone`), where spike A still built one span per element for the unread list field. The by-value spellings stay in the suite on purpose: the copy is now a visible choice in the program, and its cost is the row above it.

What the spike did NOT need: a second gather, a second list accessor, compaction-on-read, content-based `Value` equality, or a new bounds form on the wire. What it did need: a `Value`/`Shape` constructor (nine exhaustive matches: hash, codec, sort, `fixed_width`, `gather_lanes`, …), two ops, the `Rows` view for the five readers. Known copies left in: `gather_lanes`/`Unwrap` over refs and sorting a Ref column clone first (documented at the site); the wire sends a Ref as the rows it names.

Next: Field pushdown through `cap_list`/`cap_sum` in the optimizer and the mechanical closure-capture pass, which now has an explicit target — insert `ref` around a captured list or tuple, `clone` at the leaves the body reaches. The same `Ref` is the μ-type recursion knot.

## L — growing state in a fold is quadratic (measured 2026-09-17, branch `corgi-ref`)

> `cargo bench --bench gaps -- --family L`. Same machine and method as K.

The one genuinely **quadratic** pattern. A Rust loop that pushes onto a `Vec` is amortized O(1) per step, and it can read back what it pushed (`v.push(v[x])`) at O(1). corgi's `fold` carries the accumulator as a value and rebuilds a List-shaped one every round (`gather_lanes`-rebuild), so a list that grows to k costs O(k²) per row.
The suite fixes the element count at 64 K and sweeps the row length k, so a flat line is amortized push and a line rising with k is the rebuild.

| task | k=16 | k=256 | k=4096 | ns/element | Rust pattern |
|---|---|---|---|---|---|
| **L1 fold_collect** — `(acc, x) -> (acc, x enlist) append` | 3.5× | 187× | **5927×** | 15 → 217 → 3327 | `v.push(x)` |
| L1s foldscan control — fixed state, element streamed to the output | 0.8× | 5.4× | 34× | 3.2 → 6.3 → 19 | the same, written as mapAccumL |
| **L2 fold_selfref** — `(acc, x) -> (acc, [acc[x]]) append` | 2.8× | 166× | **4567×** | 19 → 225 → 3397 | `v.push(v[x])` |

Reading the sweep: L1 and L2 cost ≈ 0.8 ns × k per element — one copy of the accumulator per step, quadratic per row exactly as predicted. (The Rust L1 ceiling *falls* with k, 4.2 → 0.56 ns, because a per-row `Vec` allocation is amortized over more pushes.)

Two different gaps hide in the two rows:

- **L1 is a usage finding, like F1.** The unconditional collect has a linear corgi spelling — `foldscan` with a fixed (here unit) state and the element pushed to the output — and it should be written that way. What L1s then shows is source 2 of the K/L analysis, the **rounds term**: `foldscan` runs one round per element of the longest row, and at k=4096 with only 16 rows the per-round dispatch is amortized over 16 elements, so the streamed form drifts from 0.8× to 34×. That is the C4/C5 single-row degeneration, not the quadratic.
- **L2 is the real gap.** A step that must *read back* the growing accumulator has no non-quadratic spelling today: the state has to be the whole list because the body needs random access into it, and the state is rebuilt per round. Rust's `Vec` is both the state and the output. The fix `Ref` makes possible: a `FoldScan` whose fixed-size state may hold a **fat ref into the output emitted so far** — the emitted stream is append-only and immutable, so the reference is stable, `get` through it is O(1), and the accumulator stops being carried as a value. That is a stack machine / parser / self-referential recurrence at O(k) per row. Not built; the design item is recorded in NOTES.md.

L2's body is built with the `Builder` rather than the surface: it needs the partial-tier `Get` (a fold state cannot carry a `Fail`), and that tier is deliberately not a surface word.

## W — the WCO join step is reachable with Ref (measured 2026-09-17, branch `corgi-ref`)

> `cargo bench --bench gaps -- --family W`. 1024 anchors × 16 searches fixed; the large side D swept 16 → 16384.

The worst-case-optimal step: per anchor, a SMALL list and a range of a shared sorted adjacency (the LARGE side); every element of the small side binary-searches its anchor's large side, for Σ |small| · log |large|. corgi's `find` already pairs needle row i with haystack row i and searches per element, so the whole question is how the per-anchor haystack rows are produced. Before Ref, `slices` copied each anchor's range out — Σ |large|, the cost WCO exists to avoid — and the capture of the adjacency into every anchor copied the adjacency itself. Now each anchor holds a fat ref to the adjacency, `slices` on it hands out one fat ref per anchor's range, `get 0` unnests, and `find` searches through the refs:

```
let (small, ranges, adj) = input in
let hay = (ranges len sub 1, (ranges, adj) slices) get in
(small, hay) find
```

| spelling | D=16 | D=256 | D=4096 | D=16384 | ns/search | shape in D |
|---|---|---|---|---|---|---|
| **W1** `adj` a per-anchor ref, slices a fat ref, find through it | 8.7× | 8.1× | 6.7× | **5.4×** | 23 → 43 → 64 → 84 | log D — WCO cost |
| W1x `adj clone` per anchor, slices copies the range | 29× | 42× | 237× | **531×** | 76 → 224 → 2237 → 8289 | linear in D — the copy |

Rust's ceiling is 2.7 → 15.6 ns per search (log D plus cache); W1 tracks it with a ~5–9× constant (slices + get + find each a pass, and the `Fail` lanes of `get`/`slices` threaded through), W1x tracks D. Both spellings are checked equal once per run.

Two notes. The per-anchor ref to the adjacency is built by the host — the surface has no value broadcast (`lit` lifts u64 constants only), so "the same adjacency for every anchor" cannot yet be written in a program; mechanical capture (below) is what would supply it. And the choose-the-smaller-side-per-anchor half of WCO is a `branch` on `len lt` into two lanes with the roles swapped, then `weave`; it is orthogonal to the reference question and not measured here.

