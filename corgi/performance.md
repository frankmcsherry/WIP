# corgi's performance surface

What corgi does well and must keep doing well, what it does badly and could do better, and what it
does badly and may keep doing badly. Every row is idiomatic corgi against the idiomatic Rust for the
same task, and every row is reproduced by a benchmark in this crate: `benches/gaps.rs` or
`benches/idioms.rs`. A claim without a reproduction here does not belong in this file.

The Rust side is what a programmer would write for the task: a loop, an iterator chain, a
`Vec<enum>` or `Vec<Vec<_>>`, a `HashSet`, `sort_unstable`, `partition_point`. It is not Rust
written to follow corgi's own algorithm, which would only measure interpretation.

Ratios are "takes N× as long as" the Rust; below 1 means corgi is faster. Ranges run over 1M and
8M rows for `gaps.rs` and are at 1M rows for `idioms.rs`, unless a size is given. Figures are for
master; "with #50/#51" is master with those two open PRs merged in, where it differs.

Verdicts:
- **Keep:** at or better than the Rust. A change that makes it worse needs a reason.
- **Improve:** behind, with a known or suspected cause that fits the goals.
- **Accept:** behind, and closing it would cost a goal (code generation, or per-row scalar
  loops), so it may stay behind.

## 1. Chains of element-wise ops

| work | verdict | corgi against Rust |
|---|---|---|
| one element-wise op over a column | Keep | 1.1–1.24× a loop (an upper bound: the harness keeps its input alive, so this in-place op copies it first) |
| a chain against one fused loop | Accept | eight adds 3.4–3.7×; four ops with constants 1.5–1.8×; map then sum 5.8–7.2× (2.8–3.5× with #50/#51): one pass per op against one loop |

## 2. Selection

| work | verdict | corgi against Rust |
|---|---|---|
| filter by a comparison | Accept | 1.3–2.3× one predicated-push loop (1.2–1.6× with #50/#51): compare, then compress, against one loop |
| compare-then-select | Accept | 3.4–3.5× one fused loop (2.6× with #50/#51): four passes against one |

## 3. Reductions, scans, grouping, folds

| work | verdict | corgi against Rust |
|---|---|---|
| sum and max of a column | Keep | 1.00–1.02× |
| prefix sum | Keep | 0.99–1.23× a cumsum loop (an upper bound: an in-place op on a kept input) |
| group by a small integer key and sum | Improve | 57–59× a 256-bucket loop (16× at 8K rows): corgi sorts where Rust indexes buckets |
| fold with a (sum, count) accumulator over one long row | Improve | 3,150–5,030× a loop (2,460–3,940× with #50/#51): the fold interprets its body once per element; a tuple of monoids could instead become two reductions |
| a general scan over one long row | Accept | 445–640× a cumsum loop (370–530× with #50/#51): the body is interpreted once per element, which is a per-element scalar loop |

## 4. Ordering

| work | verdict | corgi against Rust |
|---|---|---|
| sort of `u64` values | Keep | 0.64–0.85× `sort_unstable` (values below 2^32, so the radix sort makes four passes) |
| dedup | Keep | 0.91–1.16× `sort_unstable` then `dedup` |
| distinct pairs, counted | Keep | 0.64× a `HashSet` |
| sort of a column of short lists | Keep | 0.89× sorting a `Vec<Vec<u64>>` of up to 2 elements, 1.04× up to 4 |
| sort of a column of longer lists | Improve | 1.71× sorting a `Vec<Vec<u64>>` of up to 16 elements |
| sort of a column of `Result<u64, u64>` | Improve | 1.75× sorting a `Vec<Result<u64, u64>>` |

## 5. Search and join

| work | verdict | corgi against Rust |
|---|---|---|
| equal ranges of many needles in a sorted column | Improve (Keep once #51 lands) | 5.7–6.5× `partition_point` on master; 0.27–0.35× with #51 |
| single-key join of a sorted column (dedup the probes, find, slices) | Improve | 2.5–2.6× a two-pointer merge (2.0–2.2× with #51); corgi's dedup sorts probes that Rust dedups in one pass |

## 6. Sums

| work | verdict | corgi against Rust |
|---|---|---|
| an enum with one small and one 56-byte variant, the small one updated | Keep | 0.04× at 50% each, 0.34× when the wide variant is 5%: corgi touches only the small lane |
| one field of an eight-field struct, summed | Keep | 0.08×: corgi reads one column |
| a match whose arms are one op | Keep | 1.10–1.17× a `Vec<Result>` map |
| a match with a few ops per arm | Accept | 1.38–1.46×: a pass per op against one loop |
| the sum of a `Result` column's `Err` payloads | Improve (minor) | 1.56–1.82×, though corgi reads only the `Err` lane |
| a match whose arms merge into one column | Improve | 2.9× (2.4× with #50/#51) |
| a two-variant match in `f64`, merged into one column | Improve | 3.85× (3.47× with #50/#51): floats go through the order-preserving encoding on every op |
| build a sum, then unweave it | Improve | 3.1–3.3× a one-pass partition into two vectors |
| a two-arm arithmetic match that Rust turns into a blend | Accept | 13–16.5×; `select` is the spelling for this, `match` pays off when lanes differ |

## 7. Nested lists and gathers

| work | verdict | corgi against Rust |
|---|---|---|
| each row's list summed | Keep | 0.92–0.98× a `Vec<Vec<u64>>` |
| each row's elements above a threshold, counted | Accept | 0.98× for rows up to 4 elements, 1.40× up to 16, 2.63× up to 64: the comparison is a column of its own before the per-row sum |
| gather, bounds-checked | Keep | 0.44–1.17× Rust's `.get()` collected into an `Option` (the same total semantics); 0.59–1.46× indexing, which panics |
| two gathers in a row | Keep | 1.07–1.16× |

## 8. Indirection

| work | verdict | corgi against Rust |
|---|---|---|
| pointer chasing, many chains at once | Keep | 0.09–0.13× a serial chase per chain: corgi steps every chain at once |
| a body reading a value from its enclosing row | unmeasured | the copying trap: without sharing, the value is copied once per element; needs a case |

## 9. Text

| work | verdict | corgi against Rust |
|---|---|---|
| word count | Keep | 0.72–0.74× splitting, sorting byte slices and counting runs |
| parse and sum a CSV column | Keep | 1.5–1.7× a hand-written atoi loop |

## 10. Small batches

Small batches are otherwise a non-goal; this row holds the line.

| work | verdict | corgi |
|---|---|---|
| one run of a tiny program | Keep | about 80–90 ns for one op, plus about 27 ns per further op (with #50/#51: 92–100 ns, plus about 15 ns per op) |

## 11. Kernels a host calls directly

DDIR calls these from Rust rather than through a program.

| work | verdict | corgi against Rust |
|---|---|---|
| argsort of `u64` | Keep | 0.39–0.69× a stable sort with cached keys |
| sort of sums and of short lists | Keep | 0.26–0.59× the matching stable Rust sort |
| sort under many small segments (a block per four rows) | Improve | 3.0–4.8× a Rust sort per row: a fixed cost per segment |
| gather from two sources by tag | Keep | 1.2–1.36× |

## How it's measured

`cargo bench --bench gaps` and `cargo bench --bench idioms` on an idle Apple M4 mini, 2026-10-02:
master at `16ec7f8`, and master with #50/#51 merged in, alternated over two rounds. `gaps.rs` runs
each program through `Program`; `idioms.rs` ran each case in three fresh processes per round, and
each figure is the best over all of them.

Fresh processes matter: the same program on the same data can run up to 2× slower in one process
than in another, and stay that way for the life of the process (the enum update took 0.10 ns per
row at best and 0.19–0.20 at worst; the `Err` sum 0.28 and 0.40; the Rust sides vary too). Report
the best and the worst over several processes, and treat a ratio under about 1.1× as noise.
