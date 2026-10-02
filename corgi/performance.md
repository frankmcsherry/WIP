# corgi's performance surface

What corgi does well and must keep doing well, what it does badly and could do better, and what it
does badly and may keep doing badly. This replaces `perf-gaps.md`. It is meant to be backed by one
benchmark cohort that measures every row below in one run; until that exists, the rows come from
the harnesses listed under "Where the numbers come from".

Each row is a kind of work, written as a corgi program, timed against:
- **columnar Rust**: the honest hand-written ceiling (a fused loop, a predicated push, a two-pointer
  merge, a bucket loop);
- **row-at-a-time Rust**: what someone would naturally write (`Vec<enum>`, `Vec<Vec<_>>`,
  `HashSet`); this is where corgi should win;
- other targets where they were measured: single-threaded ParlayLib, the Mac GPU through Metal,
  corgi built for WebAssembly.

Ratios are "takes N× as long as" the named target; below 1 means corgi is faster. Ranges run over
1M and 8M rows unless a size is given. "With #50/#51" is master with those two open PRs merged in.

Verdicts:
- **Keep:** at or better than the target. A change that makes it worse needs a reason.
- **Improve:** behind, with a known or suspected cause that fits the goals.
- **Accept:** behind, and closing it would cost a goal (per-row scalar loops, code generation),
  so it may stay behind.

## 1. Streaming chains

| work | verdict | corgi against targets |
|---|---|---|
| one element-wise op over a column | Keep | 1.1–1.24× columnar Rust; 1.73× at 8K rows (fixed cost) |
| sum and max of a column | Keep | 1.00–1.04× |
| eight adds in a chain | Accept | 3.4–3.7× one fused loop; each pass is at the Rust ceiling (1.0× eight un-fused Rust passes), so the whole gap is fusion |
| map then reduce | Improve, then Accept | 5.8–7.2× one fused loop on master, 2.8–3.5× with #50/#51 (the pool); already 0.51–0.55× un-fused Rust, the rest is fusion |
| a chain with constant operands | Keep | 1.5–1.8× one fused loop; 1.04–1.15× the same passes in Rust |
| the block chain `x*3+1`, compare, compress, sum (16M) | Keep | in 64K blocks 0.71 ns per element against 0.67 for plain Rust doing the same passes (*older*, #50's measurement) |

## 2. Selection and partition

| work | verdict | corgi against targets |
|---|---|---|
| compress by a mask | Keep | 1.3× a plain loop at 50% kept; ParlayLib's `pack` takes 7.7× as long |
| filter by a computed comparison | Keep | 1.3–2.3× one predicated-push loop on master, 1.2–1.6× with #50/#51; the rest is the separate compare pass |
| compare-then-select | Accept | 3.4–4.0× one fused loop (2.6–3.6× with #50/#51): four passes against one |
| partition into lanes (`branch`) | Improve | 4.8–5.1× ParlayLib, 5.6–6.1× plain Rust at 16M (count, size exactly, write once) |

## 3. Reductions, scans, grouping, folds

| work | verdict | corgi against targets |
|---|---|---|
| prefix sum (the monoid scan kernel) | Keep | 0.96–1.43× columnar Rust; ParlayLib's scan takes 1.5–2× as long |
| group by a small integer key and sum | Improve | 54–59× a Rust bucket loop (16× at 8K rows); needs a counting partition or a scatter with a combine |
| scatter | Improve | none in corgi; spelled through `group`, 29× ParlayLib at 256 buckets |
| a general scan over one long row | Improve | 445–1,130× a cumsum loop (370–920× with #50/#51): one round per element, the body re-run each round |
| fold with a (sum, count) accumulator over one long row | Improve | 3,150–6,550× a plain loop (2,460–4,950× with #50/#51): same cause |
| fold over rows of very different lengths | Improve | 47–66× when 1% of rows are long (fold unrolled per position); 1.65–3.75× with a per-row loop and the list by reference (*older*) |
| a general sequential recurrence (affine scan `y = 31y + x`, 1M) | Accept | 246× ParlayLib, 494× a plain loop: a per-element scalar loop, which is a non-goal |

## 4. Ordering

| work | verdict | corgi against targets |
|---|---|---|
| sort of `u64` values | Keep | 0.64–0.85× Rust's `sort_unstable` |
| dedup | Keep | 0.90–1.16× sort then dedup in Rust |
| argsort of `u64` | Keep | 0.39–0.69× a stable Rust sort with cached keys |
| sort of sums and of short lists | Keep | 0.26–0.59× the matching Rust sort; 0.61–1.05× row-at-a-time Rust on lists |
| sort and dedup as words over the sort's own state | Keep | 0.96–1.06× and 0.80–1.14× today's built-in ops (*older*) |
| distinct pairs (sort-based) | Keep | 0.64× a Rust `HashSet` |
| sort of `Result` rows | Improve | 1.7× row-at-a-time Rust |
| integer sort of pairs, 16M | Improve | 3.6× ParlayLib (16.3 against 4.6 ns per element: top digit first, passes in cache, payload carried) |
| sort under many small segments (a block per four rows) | Improve | 3.0–4.8× a Rust sort per row: fixed cost per segment |
| adjacent compare (`arrange::compare_adjacent`) | Improve | 3.3–3.5× a direct leaf compare |

## 5. Search and merge

| work | verdict | corgi against targets |
|---|---|---|
| `find` with many needles (#51, held) | Keep once landed | 7.2 ns per needle at 16M sorted needles against 103 for Rust's `partition_point`; 0.97–1.46× the best plain Rust loop at 4,096 needles or more (*older*: the `find` comparison earlier on 2026-10-02) |
| single-key join of sorted inputs (find then slices) | Improve | 2.5–2.6× a two-pointer merge on master, 2.0–2.2× with #51 |
| merge of two sorted columns, equal sizes | Improve | 5.6× ParlayLib, 6.9× plain Rust at 16M (block copies, branch-free merge) |
| merge kernel `survey_groups` alone | unmeasured | needs a row |

## 6. Sums

| work | verdict | corgi against targets |
|---|---|---|
| a sum whose variants differ in size | Keep | 0.04–0.36× row-at-a-time Rust |
| one field of a wide struct | Keep | 0.08× row-at-a-time Rust |
| a match whose arms are one op | Keep | 0.41–0.45× row-at-a-time Rust; 1.14× when one variant is rare |
| a match with heavier arms | Keep | 0.98–1.46× |
| reading the errors out of a `Result` column (`unweave`) | Improve (minor) | 1.6–1.8× row-at-a-time Rust; was 12–13× on 2026-09-30, before #45 read tags in place |
| merging lanes back by tag | Improve | 2.5–3.0× row-at-a-time Rust |
| a match over several shapes | Improve | 3.4–3.7× row-at-a-time Rust |
| build a sum then unweave it | Improve | 3.0–3.3× a one-pass partition |
| a two-arm arithmetic match that Rust turns into a blend | Accept | 12–16.5×; `select` is the spelling for this, `match` pays off when lanes differ |

## 7. Nested lists

| work | verdict | corgi against targets |
|---|---|---|
| sum of each row's list | Keep | 0.95–0.99× row-at-a-time Rust |
| count of each row's list, length 4 | Keep | 1.0× |
| count of each row's list, length 64 | Improve | 2.6× row-at-a-time Rust |
| gather, bounds-checked | Keep | 0.49–1.49× unchecked Rust; 0.82–1.03× columnar Rust; two gathers 1.0–1.19× |
| gather from two sources | Keep | 1.2–1.36× |
| ranges into one column, rows of 16 | Improve | 3.3× ParlayLib at 16M (9.7× at 1M); 2.6–2.9× plain Rust |
| per-row work on short rows | Improve | rows of 16 cost 0.5 ns per element more than one long list: kernels loop per row instead of treating bounds as data (*older*) |
| filter and slices rebuilt as positions then gather | Improve | 1.1–2.3× the built-in ops: row-relative positions, ranges expanded per element, an extra checking pass (*older*) |

## 8. Indirection and capture

| work | verdict | corgi against targets |
|---|---|---|
| pointer chasing, many chains at once | Keep | 0.09–0.13× a serial chase; 0.28–0.33× row-at-a-time Rust |
| a body reading a value from its enclosing row | unmeasured | the copying trap: without sharing, the value is copied once per element |
| fold with the list passed by value | Improve | 3.9–42×: every round copies every active row's list (*older*) |

## 9. Text and bytes

| work | verdict | corgi against targets |
|---|---|---|
| word count | Keep | 0.72× slice-sort plus run count |
| parse and sum a CSV column | Keep | 1.5–1.7× hand-written atoi |
| decode from serialized bytes, touched columns only | spike only | 0.03–0.18 ns per row with borrowed columns (prototype, not on master) |
| per-pair string distance on short strings | Accept | 13–90× Rust at 100K pairs: per-row scalar work (*older*) |

## 10. Small batches

| work | verdict | corgi against targets |
|---|---|---|
| fixed cost per run | Improve | 375 ns with #50 (514 before); sets the smallest useful block at about 16K elements (*older*) |
| one element-wise op at 8K rows | Improve | 1.73× columnar Rust, against 1.1–1.24× at 1M |
| one needle per `find` call | Accept | 350–520 ns against 65–130 ns in Rust: per-call cost dominates single-row use (*older*) |

## 11. Whole workloads

| work | verdict | corgi against targets |
|---|---|---|
| datatoad's GALEN, all corgi | Improve | 77.8 s against datatoad's 11.0 s; 10.8 s with six Rust kernels doing the hot loops (*older*) |
| DDIR operators | Keep (corgi's part) | DDIR's 3.3–7.7× gap to compiled code (August) is in DD's time machinery; corgi's value work is near zero (*older*) |

## Where the numbers come from

- **Columnar Rust rows:** `cargo bench --bench gaps`, on 2026-10-02 at `16ec7f8` and with #50/#51
  merged in, two alternated rounds on an idle Apple M4 mini, best corgi time per version against the
  best Rust time over all runs. Each program runs through `Program`, the path a user's program
  takes. The harness keeps its input alive, so ops that rewrite their operand in place (one
  element-wise op, the chain of adds, the prefix sum) copy it first and are upper bounds. Ratios
  under about 1.1× are within run-to-run noise.
- **Row-at-a-time Rust rows:** a small comparison crate outside the repo (`corgi-wins`), same
  machine, versions and rounds; it moves into the cohort.
- **ParlayLib rows:** ParlayLib built single-threaded against corgi with #49/#50/#51 and plain Rust,
  2026-10-02; the harness is outside the repo.
- **Rows marked *older*:** earlier spikes, not re-run: local branches `corgi-alloc-spike` (the
  pool), `corgi-blocks-spike` (blocks and per-row cost), `scratch-kernel-words` (words over the
  sort's state, folds), `corgi-find-spike` (`find`), `corgi-serialized` (decoding),
  `corgi-toad-hosted` (GALEN), `corgi-gpu-spike`, `corgi-wasm-spike`; and DDIR's August
  performance matrix.

## Other targets, where measured

- **GPU (Metal):** streaming kernels at 100–105 GB/s against 60–95 GB/s for one core, so 1.1–1.5×
  faster than a good single thread; sort, search and scatter win outright. Each kernel step in a
  submission costs about 4.5 µs and each submission 90 µs or more, so only large batches pay.
- **WebAssembly:** element-wise ops, gather, scan, sort and `find` at 0.94–1.18× native. Outliers:
  unsigned 64-bit compares (split into scalar lanes), the 64-bit multiply, unrolled reductions.
