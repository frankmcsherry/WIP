# Corgi performance gaps

> **Status:** current. Measured 2026-09-03 on an Apple M4 mini (16 GB) by `cargo bench --bench gaps`.
> This replaces the `b9bb413` baseline, which predates the sort, sortedness, immediate-axis,
> merged-find, byte-mask and monoid-fold work and is wrong about the A3, B, C5, D, E1 and F rows —
> see *What this reorders*.

To exercise the definition without producing reportable measurements, run
`cargo bench --bench gaps -- --smoke`. Reportable runs can select one or more families with
`cargo bench --bench gaps -- --family A` or `--family C,R`; omitting the selector runs all families.
`--optimized` runs every program through `optimize` before lowering, so the suite can be run twice
and diffed — see *The optimizer*.

The throughput companion to the expressivity audit: that one asks what the surface cannot say, this
one asks where the engine leaves throughput on the floor, measured against an honest
hand-written-Rust ceiling. The gap is always decomposed to a mechanism, because only some mechanisms
are what fusion fixes.

## Method

Sizing targets the cache hierarchy, not L1 microbenchmarks.
The M4 mini has 128 KB L1D, ~16 MB L2 per P-cluster, and an ~8 MB SLC; there is no conventional L3.
Three **anchors**, where every family runs: 8 K rows (64 KB, an L1 control that proves dispatch is
amortized — not a target), 1 M rows (8 MB/column, the L2/SLC design center), and 8 M rows
(64 MB/column, the DRAM-streaming eye).

Families A and B also run a **tile sweep** at 1 K / 4 K / 16 K. A whole multi-pass chain stays
L1-resident there, so those rows measure what a tile executor's steady state would be — the number
that sizes destination-passing style *before* it is built. `anchor / sweep minimum` is the tiling
prize; the rise below the minimum is per-op fixed cost.

The Rust ceiling is the honest efficient version, not a strawman: a single fused loop for a chain, a
predicated push for filter, a two-pointer sort-merge for the join, a bucket-accumulate for a
low-cardinality group-by, and — for the already-ordered rows — the work the answer actually is.

For chains the gap is split two ways.
`tax = corgi_k / rust_k` compares corgi's k passes to Rust's k *un-fused* passes — the engine's
per-pass overhead (dispatch, allocation).
`fusion-prize = rust_k / rust_1` compares Rust un-fused to Rust fused — the pure win fusion would
capture.
Reporting them separately keeps the fusion headroom from hiding inside an execution-overhead number,
and vice versa.

The single bandwidth figure is input-GB/s.
On this machine a single-thread streaming pass tops out near 30–40 GB/s in L2/SLC and ~21–24 GB/s
from DRAM; the drop across the 8 MB→64 MB boundary is the bandwidth-bound vs compute-bound tell.

### Two things to know before reading a row

**The input column is SHARED.** `corgi_t` hands each rep `arg.clone()` and keeps `arg` alive, so
inside the timer the leaf's `Arc` is at refcount 2 and an op that CONSUMES its operand cannot take
the buffer — it copies the column first.
That is real (a node with fan-out 2 pays exactly this) but it is not what the harness comment claims
to measure, and on a single-pass workload it *is* the measurement.
It applies only to the ops that rewrite their operand in place (`BinImm`, `Scan`), because every op
that merely READS now borrows. So A1 and C4k are still upper bounds; the rest of the matrix is not
distorted by it. Fixing the harness properly is not a one-liner: handing each rep a freshly-built
column removes the copy but leaves the buffer cache-warm, and corgi's in-place ops then measure
*above* the machine's memory bandwidth, which is just as wrong in the other direction. It wants
pre-built owned columns plus cache eviction between build and timer.

**Ratios below ~2×, and any ratio whose Rust side is sub-nanosecond, carry run-to-run noise of up to
2×.** The Rust ceilings move: E1's has measured anywhere in 0.37–1.19 ns/row across runs, which alone
swings that row between 3.1× and 7.8×. Track corgi's absolute ns/row for corgi's own progress, and
read the ratios as an order of magnitude, not a measurement. **F1 is worse than noisy, it is
BIMODAL** — 6.5–7.5 or 9.8–10.0 ns/row at 1 M, on the same build, reproducibly split across runs
including at the pre-work baseline. Take its band, not a number.

## The gap map

Ratios are corgi/Rust slowdown (higher = corgi slower); for chains, tax and fusion-prize are shown
instead. "Lever" names what would close it.

| task | family | 8 K | 1 M | 8 M | mechanism | lever |
|---|---|---|---|---|---|---|
| A1 add_const | pointwise | 1.23× | 0.78× | 1.00× | single pass at the bandwidth ceiling (upper bound — in-place op, shared input) | — at ceiling |
| A2 add_chain8 | pointwise | tax 0.93 / prize 6.6 | tax 1.05 / prize 3.7 | tax 0.95 / prize 2.1 | per-pass at the Rust ceiling; the gap is un-fused passes | **tiling, then fusion** |
| A3 mixed_chain | pointwise | tax 1.07 | tax 0.99 | tax 1.00 | at the Rust ceiling — the immediate cells landed | — at ceiling |
| A4 map_reduce | pointwise | tax 1.67 / prize 2.4 | tax 0.81 / prize 11.5 | tax 1.20 / prize 7.5 | intermediate map column, then a fold over it | **fusion — the biggest prize on the board** |
| B1 filter_values | selection | 3.0× | 2.4× | 2.4× | BYTE mask column + scalar `filter_mask` + gather vs one predicated push | a writer; SIMD compaction |
| B2 cmp_select | selection | 2.3× | 1.9× | 1.9× | 4 passes vs 1 fused | tiling, then fusion |
| C1 fold_add | aggregation | 1.10× | **1.03×** | **1.01×** | one SIMD pass, at the Rust ceiling | — at ceiling |
| C2 fold_max | aggregation | 1.05× | **1.00×** | **1.01×** | one SIMD pass, at the Rust ceiling | — at ceiling |
| **C3 group_by_sum** | aggregation | 22× | **48×** | **74×** | sort-based group where a 256-bucket accumulate is one O(n) pass | see *the group-by decomposition* |
| **C4 scan_prefix** (general) | aggregation | **757×** | **192×** | **153×** | lockstep foldscan on ONE long row: #rounds = row length, and every round allocates | monoid body → C4k; general → a single-row interpreter |
| C4k scan_add (kernel) | aggregation | 1.41× | 1.17× | 0.98× | the monoid prefix kernel — one in-place pass | **DONE** |
| C5 fold_sum_count | aggregation | 6.9× | 6.8× | 9.9× | recognized as a product of monoids: one reduce per field. What is left is the ordinary un-fused-passes gap | tiling, then fusion |
| D1 sort_u64 | order | **0.55×** | **0.43×** | **0.85×** | the value radix: no permutation, no gather. NB the input carries **32 significant bits**, so this is two radix passes | — past the ceiling |
| D1b sort_u64_full | order | 1.05× | **0.81×** | 1.00× | the same sort at 64 significant bits: FOUR passes. The radix's win over pdqsort is key entropy, and this is its floor | — at ceiling |
| D2 dedup | order | **0.64×** | **0.52×** | **0.89×** | value radix + one compacting pass (same 32-bit input as D1) | — past the ceiling |
| D3 sort_compound | order | 1.21× | **1.05×** | 2.22× | key-carrying permutation radix + 2 payload gathers | pack narrow fields into one key |
| D4 sort_sorted | order | **1.19×** | **1.13×** | **1.14×** | already ordered: detect and return | — at ceiling |
| D5 dedup_sorted | order | 5.6× | 4.5× | 4.1× | run boundaries off the order check, then a gather | a writer (as B1) |
| E1 join_find_slices | relational | — | 3.0× | 4.3× | `find` MERGES when the needle is sorted; what is left is the `slices` materialization | a writer (as B1) |
| E2 gather | relational | — | **0.76×** | **0.86×** | corgi at or below the Rust ceiling | — |
| E3 gather_chain | relational | — | **0.74×** | 1.17× | two gathers, each resolve+gather | index-composition rewrite |
| **F1 branch_match** | sum-type | — | **12.9×** | **8.3×** | columnar partition + recombine where the scalar form vectorizes to a blend. NB this row is BIMODAL — 6.5–7.5 or 9.8–10.0 at 1 M, on the same build, including at `de0f2ac` | if-convert to `select`; `match` pays off on heterogeneous lanes |
| F2 unweave_shred | sum-type | — | 2.8× | 4.6× | build sum (branch) + unweave vs one-pass partition | partly |
| G1 word_count | text | — | 1.7× | 1.5× | structural ragged-string sort + find vs slice-sort + run-count | sort kernel; `memchr` for the split |
| G2 csv_sum | text | — | 1.7× | 1.7× | total `parse_u64` (Sum) + reduce vs hand atoi | — near ceiling |
| H gather (safe) | safety | 0.33 vs 0.67 | 1.04 vs 1.32 | 1.05 vs 1.40 ns/row | corgi's TOTAL (bounds-checked) sequential gather vs Rust `unsafe` | — **no safety tax** |
| I pointer-chase | latency | — | 0.63 vs 4.76 | 0.80 vs 9.43 ns/step | lockstep gather extracts MLP a serial chase cannot | — **8–12× FASTER** |
| R1 arrange_sort_perm | arrangement | — | **0.69×** | **0.96×** | stable key-carrying radix argsort | — past the ceiling |
| R2 arrange_compare | arrangement | — | 3.5× | 2.6× | batched adjacent compare vs a direct leaf compare | — |
| R3 arrange_find | arrangement | — | 1.17× | 0.98× | the u64 fast path, at the Rust partition-point ceiling | — |
| R4 arrange_survey | arrangement | — | **0.20×** | **0.75×** | galloping runs beat a two-pointer survey | — |
| R5 arrange_gather2 | arrangement | — | 1.34× | 1.09× | two-source column gather vs a direct two-slice gather | — |

### The tile sweep (A and B, corgi ns/row)

| row | 1 K | 4 K | 8 K | 16 K | 1 M | 8 M | prize over the floor |
|---|---|---|---|---|---|---|---|
| A2 add_chain8 | 0.69 | 0.59 | **0.53** | 0.63 | 1.07 | 2.09 | 2.0× / 3.9× |
| A3 mixed_chain | 0.69 | **0.27** | 0.31 | 0.36 | 0.91 | 1.00 | 3.4× / 3.7× |
| A4 map_reduce | 0.49 | **0.20** | 0.20 | 0.24 | 0.72 | 0.87 | 3.6× / 4.4× |
| B1 filter_values | 1.22 | 1.27 | **0.86** | 0.89 | 1.48 | 1.74 | 1.7× / 2.0× |
| B2 cmp_select | 0.77 | 0.61 | **0.63** | 0.63 | 1.80 | 2.06 | 2.9× / 3.3× |

The two large anchors in the B rows are 3-run medians; everything else is one run. B is worth
measuring that way and A is not — see the mask note below.

**The byte mask, measured.** `Rel` producing a `u8` mask rather than a `u64` one is worth, against
the same suite on the same machine, 3 runs each: B1 1.66 → 1.48 ns/row at 1 M and 1.87 → 1.68 at
8 M; B2 1.94 → 1.80 and 2.22 → 2.06. So **1.08–1.14×**, on the two rows that read a mask at all —
not the 1.3–1.6× a single run of each side suggested when the mask first landed, and not the
regression a single run suggested when it came back. Single runs of B are worth little: the u64 side
is tight (B1 at 1 M was 1.656 / 1.664 / 1.664) and the byte side is not (B2 at 1 M was 1.18 / 1.80 /
1.84), so any one pairing can say almost anything. The DDIR workload set does not move at all,
because it barely filters — see the integration note in `dd-corgi-dist`.

One thing that looked obvious and is not: `Select`'s `blend` reads a byte selector against 64-bit
data, which costs lane-unpacking that a same-width selector would not. Widening the mask once
inside `Select` to get a uniform blend measured WORSE than paying the unpack (B2 at 1 M: 1.80 with
the byte selector, 2.22 with the widen), so the widen is not there. `filter_mask` has no such
problem — it reads the mask alone, generic over its width.

The floor is 4 K–8 K rows and the rise below it is per-op fixed cost, which puts the useful tile
floor near 2 K rows. **Tiling is worth 1.6–4.0× at the design center and 2.2–4.9× at the DRAM
point** — and that is all of it: A2 at its floor is still 0.53 ns/row for eight ops against a fused
Rust loop's 0.08, so the rest of the fusion-prize column is a compiler, not a calling convention.

## What this reorders

**The largest gap is the general SCAN, and it is now alone there.**
`scan_prefix` is 153–757× off a trivial loop; `fold_sum_count`, which used to sit beside it at
2000–5000×, is 6.9–9.9× since the product-of-monoids recognition landed. The lockstep fold is built
to vectorize across *many short rows*; handed one long row it runs one round per element.

Three contributors have been removed and a fourth named. The body's graph is PREPARED once outside
the round loop (consumer counts and evaluation buffers computed per graph, not per call — see
`graph::Prepared`); `FoldScan` SCATTERS a fixed-width output straight into one column instead of
keeping a `Value` and two `usize` per element and stitching at the end; and a `Fold` whose body is a
product of monoids does not run the loop at all. Together: C4 244 → 175 ns/row at 1 M (1.4×) and
C5 308 → 0.4–1.0 (300–700×).

**What is left is allocation, and it is most of what remains.** With one long row, a round processes
ONE element, and it still allocates a one-element gather result, a two-element `Prod` for the body's
argument, and — in `Fold` — the accumulator gather and scatter. Those are per-element allocations
around a per-element scalar op, and preparing the graph removed only one of about five. Removing the
rest is the single-row interpreter (register/stack scratch, no heap per step), or the windowed leaf
that would let a round BORROW its element instead of gathering it.

The cases still stratify by fixability:

- **scalar-leaf monoid** (cumsum, running min/max/product/all/any) — **done.** `ArithOp::Scan(Red)`
  is a one-pass in-place kernel and C4k measures 1.1–1.8×.
- **product-of-monoids accumulator** (`(sum, count)`) — **done.** A `Fold` whose body is a `Tuple`
  of per-field monoid updates, each from a contribution that never reads the accumulator, is
  `seed_i ⊕ reduce_i(list)` — one pass per field. Recognized at eval time in the NUMERIC layer
  (`ops::numeric::monoid_fold`), since "is this op a monoid" is a numeric question and the core is
  blind to it. C5 went 240 → 0.4–1.0 ns/row at 1 M, from the worst row on the board by two orders of
  magnitude to a single-digit multiple of a fused loop.
- **fixed-width non-monoid body** — inherently sequential, so the lever is the per-step constant, and
  what is left of that constant is per-round allocation. **One subcase is not inherently sequential:**
  an affine recurrence (`x[t+1] = a·x[t] + b`) is a monoid in disguise, since composition of affine
  maps is associative, so it reduces to a scan over the affine semigroup — the same kernel C4k
  already is, with a pair accumulator.
- **`List`/`Sum` (variable-size) accumulator** — the genuine residual: the accumulator is heap and
  reshapes per step. No tight-loop fix; keep it correct, steer to structural ops, accept it. Rare.

**But the same lockstep mechanism is a WIN.**
Corgi's lockstep gather chases 1024 pointer chains at 0.63–0.80 ns/step against a naive serial chase
at 4.76–9.43 — **8–12× faster**, and within 1.3–1.6× of a hand-written lockstep loop. The fold is not
broken; it is specialized for many rows and pathological for one. Read C4/C5 and I together or you
will draw the wrong conclusion from either. The general statement: **corgi converts depth into width,
so it is optimal when the problem is wide and pathological when it is deep and narrow.**

**Corgi's total gather has no safety tax.**
The bounds-checked sequential `gather` runs at 0.33/1.04/1.05 ns/row against Rust `unsafe` at
0.67/1.32/1.40, winning at every size. The vectorized all-or-nothing check is cheaper than Rust's
per-element bounds check, and competitive with eliding the check entirely.

**Six rows the old baseline called gaps are closed or much smaller.**

1. **A3 mixed_chain was tax 1.6–2.8×, and is now 1.0–1.3×.** Its mechanism line used to read
   "`mul`/`sub` by a constant lack immediate kernels, so `Lit` broadcasts a column and builds a
   `Prod`". The grid now has an IMMEDIATE axis (`ArithOp::BinImm` at every kind and width, plus
   `CmpOp::RelImm`), and the four U64-only one-off ops that used to sit outside it are gone.
2. **D4 sort_sorted, a row that did not exist, is at the ceiling.** `sort`, `dedup` and `group` now
   ask whether the column is already ordered before they sort it, and answer cheaply or decline.
   An already-ordered 1 M column sorts in 0.26 ns/row against 24.0 for the old path.
3. **The sort is now past its ceiling.** Three changes: the discrimination stops once no two rows
   are tied (as `compare_pairs` next door always has) and reads field 0 directly instead of
   gathering it through an identity permutation; a bare leaf sorts its VALUES, with no permutation
   to build and no gather after; and the permutation path materializes each key once and carries it,
   so every radix pass reads sequentially instead of twice-per-element through the index. D1 went
   24.0 → 5.2 ns/row at 1 M and 50.0 → 9.6 at 8 M, D2 26.8 → 6.1 and 54.4 → 9.1, D3 25.6 → 15.1 and
   54.5 → 41.2, R1 24.5 → 15.0 and 44.9 → 32.1. corgi now sorts a u64 column faster than
   `sort_unstable`. The one cost is at the 8 K L1 control, where D3 is ~10% slower: carrying the key
   is extra traffic that an L1-resident indirect read does not repay. That point is a control, not a
   target.

   **How much of that is key entropy.** A radix costs `ceil(significant_bits / digit_width)` passes
   and skips all-zero high digits, so its win over a comparison sort is a function of the key, not
   just of the kernel. The suite's input carries 32 significant bits — a hash lane or a dense
   identifier, which is the realistic case — and that is two passes at the 16-bit digit. D1b is the
   same sort at 64 significant bits, which is four: 0.81× at 1 M and 1.00× at 8 M, against D1's 0.43×
   and 0.85×. So "corgi sorts faster than `sort_unstable`" holds for keys with headroom and becomes
   a wash for keys that fill their u64. The comparison sort barely moves between the two (its cost is
   comparisons, not key bits), which is what makes the row a clean read of the pass count.
4. **C5 fold_sum_count was the worst row on the board and is now single-digit.** A `Fold` whose
   body is a product of monoids becomes one reduce per field: 308 → 0.4–1.0 ns/row at 1 M.
5. **B1 and B2 came down with the lane body.** `rel` resolves its predicate to ONE comparison
   above the loop rather than evaluating all three order-flags per element. B1 1.72 → 1.65 ns/row at
   1 M, B2 2.81 → 2.13. A BYTE mask on top of that measured 1.26 / 1.78 — but a comparison's result
   is a VALUE at the DDIR seam, not only a control mask, and narrowing it made three of DDIR's 33
   AoC programs return nothing. The mask stays u64; the consumers still read any width, which is the
   half of that idea that was free.
6. **F1 branch_match was 16×/15×** at `b9bb413`; it now measures 8.3–20×, and the spread is the row
   itself (see the bimodality note above), not a change.

**E1's number was wrong, not just stale.** Its probe side was built by a `dedup` — a full sort —
*inside* the timed program, so the row measured a sort plus a join and moved with every sort change.
Both sides are now built outside the timer.

**Three gaps stand, unchanged in character:**

1. **group-by on a low-cardinality key, 22–74×.** It decomposes: about 2.8–4.6× of it is reachable
   inside the discrimination sort (a narrow leaf's histogram IS the partition, so labels come off it
   with no comparison at all — the Henglein MSD path the module doc names as unbuilt). The residual
   ~17× is structural: `group` produces the grouped VALUES, where a bucket accumulate materializes
   nothing. Closing that means one fused op in the "fused forms & producers" tier
   (`ReduceKey`), which is a decision, not a bug fix.
2. **prefix search inside a ragged `List` column.** The single-key join is fixed: `find` now merges
   when the needle is itself in key order, which is what a join has, and E1 went 2.90 → 1.58 ns/row
   at 1 M and 3.69 → 1.62 at 8 M, flat in n where the per-probe search was `n log n`.
   NOTE the scope, which is the whole of the remaining problem: a merge replaces a search only for a
   scalar or hashed key. Seeking a PREFIX in a ragged `List` column is different, because corgi's
   list order is LENGTH-FIRST and length-first is exactly the order that lacks prefix contiguity —
   the extensions of a prefix scatter across one range per length class, where plain lexicographic
   order (end-of-list sorting below every element) would make them one contiguous range. Fixed-arity
   tuples do not have the problem (equal lengths order the same either way), so encoding a k-ary key
   as a `Prod` sidesteps it entirely; ragged data is where it bites, and changing `List`'s order is a
   seam decision, since the order is wire-visible.
3. **narrow compound keys still radix once per field.** The sort now stops as soon as no two rows
   are tied and carries each key with its permutation, so a compound key costs one pass per field
   that actually discriminates. When several fields are narrow, one packed key would do instead of
   several: four discriminating u16 fields measure 27.7 ns/row as a `Prod`, against 9.5 for the same
   key packed into a single u64. That is the trick `sort_list_blocks` already has for strided byte
   lists, generalized: pack a fixed-width prefix of the structural key into one wide integer, radix
   once, refine only if ties remain. Not built — and it does NOT help DDIR's key, whose leading
   field is a full 64-bit hash that the tie check already stops on.

## The optimizer

`optimize` is on no evaluated path — not `Program`, not this suite, not the corpus. `--optimized`
puts it on this one, and reports total graph nodes before and after.

**On this suite it does nothing: one node removed out of 89, and no timed row moves in a consistent
direction.** That is not evidence the passes are weak. It is evidence that the gaps programs are
hand-minimal by construction — no redundant subexpressions for `cse`, no dead bindings for `dce`, no
adjacent `MapList`s for `fuse_maps`. Run by hand on programs that *do* carry redundancy, `fuse_maps`
pays 2.0× and `dce` 2.2×, and **`cse` cost 25%**: sharing a column gives it fan-out 2, so the
consumer that would have rewritten it in place has to copy instead. The optimizer's problem is not
sophistication, it is that it has no cost model and no knowledge of the move-to-last-use discipline
the evaluator runs on.

Answering "does the optimizer matter" wants a corpus written for clarity rather than for
measurement — the DDIR AoC programs are the obvious one. Report node count before/after, time
before/after, and time against a hand-tuned version of the same program; the third is the one that
names the missing pass.

**What is left on the measurement side:** fix `corgi_t` to hand over pre-built owned columns with the
cache cleared between build and timer, so A1 and C4k stop being upper bounds; add a radix-sort
ceiling (D1's only ceiling is `sort_unstable`, which corgi should beat); and add explicit NEON
ceilings for the three compute-bound kernels — compare-to-a-u8-mask, mask compaction (no
`vpcompressd` on NEON, so the reference is a `tbl` shuffle table), and a radix histogram — so a row
at 1.0× stops being ambiguous between "both vectorized" and "neither did".
