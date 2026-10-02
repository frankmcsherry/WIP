# corgi's performance surface

What corgi does well and must keep doing well, what it does badly and could do better, and what it
does badly and may keep doing badly. Every row is idiomatic corgi against the idiomatic Rust for the
same task, and every row is reproduced by a benchmark in this crate: `benches/gaps.rs` or
`benches/idioms.rs`. A claim without a reproduction here does not belong in this file.

The Rust side is what a programmer would write for the task: a loop, an iterator chain, a
`Vec<enum>` or `Vec<Vec<_>>`, a `HashSet`, `sort_unstable`, `partition_point`. It is not Rust
written to follow corgi's own algorithm, which would only measure interpretation.

Ratios are "takes N× as long as" the Rust; below 1 means corgi is faster. Sizes stay clear of the
cache boundary, where results swing (see "How it's measured"): `gaps.rs` rows are at 8M rows (64 MB
a column), and `idioms.rs` rows give two sizes, 64K rows (inside the caches) and a size whose data
is 100 MB or more. Figures are for master; "with #50/#51" is master with those two open PRs merged
in, where it differs.

Verdicts:
- **Keep:** at or better than the Rust. A change that makes it worse needs a reason.
- **Improve:** behind, with a known or suspected cause that fits the goals.
- **Accept:** behind, and closing it would cost a goal (code generation, or per-row scalar
  loops), so it may stay behind.

## 1. Chains of element-wise ops

| work | verdict | corgi against Rust |
|---|---|---|
| one element-wise op over a column | Keep | 1.11× a loop (an upper bound: the harness keeps its input alive, so this in-place op copies it first) |
| a chain against one fused loop | Accept | eight adds 3.6×; four ops with constants 1.8×; map then sum 5.8× (2.8× with #50/#51): one pass per op against one loop |

## 2. Selection

| work | verdict | corgi against Rust |
|---|---|---|
| filter by a comparison | Accept | 2.3× one predicated-push loop (1.6× with #50/#51): compare, then compress, against one loop |
| compare-then-select | Accept | 3.4× one fused loop (2.6× with #50/#51): four passes against one |

## 3. Reductions, scans, grouping, folds

| work | verdict | corgi against Rust |
|---|---|---|
| sum and max of a column | Keep | 1.01–1.02× |
| prefix sum | Keep | 0.99× a cumsum loop (an upper bound: an in-place op on a kept input) |
| group by a small integer key and sum | Improve | 59× a 256-bucket loop (16× at 8K rows): corgi sorts where Rust indexes buckets |
| fold with a (sum, count) accumulator over one long row | Improve | 3,150× a loop (2,460× with #50/#51): the fold interprets its body once per element; a tuple of monoids could instead become two reductions |
| a general scan over one long row | Accept | 445× a cumsum loop (370× with #50/#51): the body is interpreted once per element, which is a per-element scalar loop |

## 4. Ordering

| work | verdict | corgi against Rust |
|---|---|---|
| sort of `u64` values | Keep | 0.85× `sort_unstable` (values below 2^32, so the radix sort makes four passes) |
| dedup | Keep | 1.16× `sort_unstable` then `dedup` |
| distinct pairs, counted | Keep | 1.18× a `HashSet` at 64K rows; 0.18× at 8M, where the `HashSet` outgrows the caches |
| sort of a column of short lists | Keep | 0.91–0.98× sorting a `Vec<Vec<u64>>` of up to 2 elements, 0.94–1.11× up to 4 (64K and 2M rows) |
| sort of a column of longer lists | Improve | 1.31× sorting a `Vec<Vec<u64>>` of up to 16 elements at 64K rows, 1.63× at 2M |
| sort of a column of `Result<u64, u64>` | Improve | 2.16× sorting a `Vec<Result<u64, u64>>` at 64K rows, 1.77× at 8M |

## 5. Search and join

| work | verdict | corgi against Rust |
|---|---|---|
| equal ranges of many needles (one per 16 keys) in a sorted column | Improve (Keep once #51 lands) | on master 3.8–5.7× `partition_point` at 64K keys and 0.92–3.1× at 16M; with #51 0.38–0.42× and 0.19–0.20× |
| single-key join of a sorted column (dedup the probes, find, slices) | Improve | 2.6× a two-pointer merge (2.2× with #51); corgi's dedup sorts probes that Rust dedups in one pass |

## 6. Sums

| work | verdict | corgi against Rust |
|---|---|---|
| an enum with one small and one 56-byte variant, the small one updated | Keep | 0.04–0.09× at 50% each, 0.24–0.34× when the wide variant is 5% (64K and 8M rows): corgi touches only the small lane |
| one field of an eight-field struct, summed | Keep | 0.11–0.16×: corgi reads one column |
| a match whose arms are one op | Keep | 0.44× a `Vec<Result>` map at 64K rows, 0.51× at 8M |
| a match with a few ops per arm | Keep | 1.04× at 64K rows, 0.87–0.89× at 8M |
| the sum of a `Result` column's `Err` payloads | Improve | 0.59–0.82× at 64K rows, but 2.3–2.6× at 8M, though corgi reads only the `Err` lane |
| a match whose arms merge into one column | Improve | 3.4× at 64K rows, 2.5× at 8M (2.0× with #50/#51) |
| a two-variant match in `f64`, merged into one column | Improve | 4.7× at 64K rows, 3.2× at 8M (2.8× with #50/#51): floats go through the order-preserving encoding on every op |
| build a sum, then unweave it | Improve | 3.3× a one-pass partition into two vectors |
| a two-arm arithmetic match that Rust turns into a blend | Accept | 13×; `select` is the spelling for this, `match` pays off when lanes differ |

## 7. Nested lists and gathers

| work | verdict | corgi against Rust |
|---|---|---|
| each row's list summed | Keep | 0.91–1.05× a `Vec<Vec<u64>>` (64K and 2M rows) |
| each row's elements above a threshold, counted | Accept | 1.1–1.3× for rows up to 4 elements, 1.5–1.6× up to 16, 1.6× (64K rows) to 2.6× (2M) up to 64: the comparison is a column of its own before the per-row sum |
| gather, bounds-checked | Keep | 0.44–1.10× Rust's `.get()` collected into an `Option` (the same total semantics); 0.59–1.46× indexing, which panics |
| two gathers in a row | Keep | 1.16× |

## 8. Indirection

| work | verdict | corgi against Rust |
|---|---|---|
| pointer chasing, many chains at once | Keep | 0.10–0.12× a serial chase per chain (32–64 MB of pointers): corgi steps every chain at once |
| a body reading a value from its enclosing row | unmeasured | the copying trap: without sharing, the value is copied once per element; needs a case |

## 9. Text

| work | verdict | corgi against Rust |
|---|---|---|
| word count | Keep | 0.72× splitting, sorting byte slices and counting runs |
| parse and sum a CSV column | Keep | 1.7× a hand-written atoi loop |

## 10. Small batches

Small batches are otherwise a non-goal; this row holds the line.

| work | verdict | corgi |
|---|---|---|
| one run of a tiny program | Keep | about 90 ns for one op, plus about 27 ns per further op (with #50/#51: about 95 ns, plus about 14 ns per op) |

## 11. Kernels a host calls directly

DDIR calls these from Rust rather than through a program.

| work | verdict | corgi against Rust |
|---|---|---|
| argsort of `u64` | Keep | 0.69× a stable sort with cached keys |
| sort of sums and of short lists | Keep | 0.26–0.48× the matching stable Rust sort |
| sort under many small segments (a block per four rows) | Improve | 3.1–4.8× a Rust sort per row: a fixed cost per segment |
| gather from two sources by tag | Keep | 1.19× |

## Reports

Measurements of other systems, kept as a record the way a paper's numbers are. Nothing in this
crate reproduces them, and they are not updated.

**ParlayLib on one thread (2026-10-02).** ParlayLib, CMU's library of parallel sequence primitives
(commit `5101769`), built single-threaded, against corgi (master with #49, #50 and #51) and the best
plain Rust loop (with a caching allocator where the loop allocates its output), on the same data
with answers checked equal. Apple M4 mini, 16M elements unless noted, ns per element.

| work | ParlayLib | corgi | plain Rust |
|---|---|---|---|
| sum of `u64` | 0.118 | 0.139 | 0.112 |
| prefix sum of `u64` | 0.367 | 0.241 | 0.247 |
| compress, 50% kept | 2.44 | 0.315 | 0.249 |
| partition by tag, 2 buckets | 0.873 | 4.41 | 0.72 |
| partition by tag, 16 buckets | 1.28 | 6.15 | 1.09 |
| scatter-add into 256 buckets | 0.333 | 9.80 (no scatter: group, then sum) | 0.290 |
| integer sort, `u32` key with `u32` payload | 4.59 | 16.3 | 5.23 (datatoad's radix sort) |
| integer sort, `u64` keys | 8.38 | 10.8 | 8.70 (datatoad's radix sort) |
| dedup, 10% distinct | 4.66 (hashing, any order); 36.5 (ordered) | 6.99 | 11.5 |
| group, 10 values per key | 6.78 | 12.2 | 16.6 |
| merge two sorted columns, equal sizes | 2.12 | 11.9 | 1.72 |
| merge two sorted columns, 1:100 | 2.10 | 2.48 | 0.355 |
| search, random needles, 64M haystack | 600 (binary search) | 86.4 | 281 |
| ranges into one column, rows of 16 | 1.00 | 3.25 | 1.25 |
| `x*3+1`, keep below a threshold, sum | 2.89 | 0.896 | 0.236 (one fused loop) |
| affine scan `y = 31y + x`, 1M | 1.04 | 256 | 0.518 |

ParlayLib was ahead on integer sort (it splits on the top digit first, then sorts cache-sized
buckets, moving key and payload together), partition (count, size exactly, write once), scatter,
group, merge (a branch-free loop) and ranges (block copies). corgi was ahead on compress, prefix sum,
search, ordered dedup and the chain.

## How it's measured

`cargo bench --bench gaps` and `cargo bench --bench idioms` on an idle Apple M4 mini, 2026-10-02:
master at `16ec7f8`, and master with #50/#51 merged in, alternated over two rounds. `gaps.rs` runs
each program through `Program`. `idioms.rs` ran each case in two fresh processes per round, and each
figure is the best over all four processes per version.

Sizes stay clear of the cache boundary. At 1M rows of `u64` a program's columns total about 16 MB,
the size of the M4's L2 per cluster of performance cores, and there the same program on the same
data ran up to 3× faster or slower from one process to the next, staying that way for the life of
the process; the Rust side swung too. Away from that boundary the best and worst processes mostly
agree to within 10%, the list rows to within 1.2× (1.4× at 64K rows). Treat a ratio under about
1.1× as noise.
