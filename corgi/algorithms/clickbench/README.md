# ClickBench in corgi

ClickBench's 43 queries over its `hits` table, written in corgi and checked against DuckDB. Each
`qNN.col` names the SQL it answers (`# sql:`, made deterministic), the columns it reads, and its
output's kinds. The table is one row: each column is a one-row list.

    curl -O https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
    python3 benches/clickbench/prepare.py DIR hits_0.parquet     # needs the duckdb and numpy packages
    CORGI_CLICKBENCH=DIR cargo bench --bench clickbench [-- NAME ...] [--check] [--profile]

`prepare.py` writes the columns as raw files, DuckDB's answers, and DuckDB's times. The bench
checks corgi's answers against DuckDB's, then times both.

## What changed from the SQL

- **Written as the SQL is:** 35 queries.
- **A different method, because corgi lacks the operation:**
  - q03 (AVG of a signed column): sums of 32-bit halves, since there is no signed-to-float.
  - q20–q22 (LIKE): split and compare.
  - q28 (REGEXP_REPLACE): the equivalent byte logic.
- **Cut down:** q23 returns three columns for `SELECT *`.
- **Changes to the SQL itself:**
  - every ORDER BY breaks ties;
  - string order is shorter-first, as corgi orders lists;
  - a string MIN is corgi's first in that order;
  - HAVING thresholds are divided by 100 (this is 1M rows of the full 100M);
  - EventDate is compared as day numbers.

## Measured

Measured 2026-10-03 on an M4 mini, one partition (`hits_0`, 1M rows). Times are the best run, in ms.
The queries whose ORDER BY … LIMIT became `sort_limit` and moved (q12–q14, q18, q24–q27, q33, q34,
q36, q38–q40) were measured again after that change; the rest are as first written. DuckDB holds the
table in memory and runs on one thread; its per-query overhead is about 0.1 ms. The last column is
from one `--profile` run, taken while `sort`, `dedup` and `group` were the kernels `SortList`,
`DedupList` and `GroupKey`. They are words over `sort_by` now: across the queries that use them,
1.01× those times, the slowest q27 at 1.19×.

| query | corgi | DuckDB 1 thread | × | what dominates |
|---|---|---|---|---|
| q00 | 0.00 | 0.09 | 0 | Nothing to do: the row count is in the bounds. |
| q01 | 0.45 | 0.40 | 1.12 | At parity: one compare pass and a sum. |
| q02 | 0.16 | 0.40 | 0.40 | Ahead: two SIMD sums; DuckDB pays its per-query overhead (~0.1 ms) at this size. |
| q03 | 0.90 | 0.74 | 1.22 | Approximated in method: corgi has no signed-to-float and no float sum. Two extra passes (shr, and). |
| q04 | 15.57 | 3.65 | 4.27 | Sort-based distinct (DedupList 21 ms) against DuckDB's hash set. |
| q05 | 45.60 | 5.70 | 8.00 | Structural sort of 1M byte strings (47 ms) against a hash set. |
| q06 | 0.22 | 0.63 | 0.35 | Ahead: two SIMD passes. |
| q07 | 2.00 | 0.54 | 3.70 | The COUNT(*) idiom builds a column of ones (lit) and value lists only to count them; Filter of 1M pairs dominates. Key domain is tiny (bincount). |
| q08 | 28.37 | 5.06 | 5.61 | Two sorts: GroupKey on RegionID then a DedupList per group, where DuckDB hashes (RegionID, UserID) once. |
| q09 | 31.64 | 13.21 | 2.40 | As q08: per-group distinct is the cost. |
| q10 | 3.41 | 1.62 | 2.10 | Small after the filter (few phones); `len` over 1M strings shows up three times (`s len` per element runs three times). |
| q11 | 4.04 | 1.84 | 2.20 | As q10. |
| q12 | 44.08 | 3.07 | 14.36 | GroupKey on the 69K non-empty phrases (38 ms) against a hash aggregate. |
| q13 | 45.41 | 4.07 | 11.16 | As q12. |
| q14 | 45.69 | 3.49 | 13.09 | As q12. |
| q15 | 19.47 | 5.25 | 3.71 | Integer key, 80K distinct of 1M: GroupKey 19 ms vs hash 5.5 ms. |
| q16 | 75.01 | 9.99 | 7.51 | Mixed integer/string key: GroupKey 72 ms. |
| q17 | 70.43 | 9.99 | 7.05 | As q16; the SQL's unordered LIMIT made deterministic by key order, which corgi's group gives for free. |
| q18 | 84.00 | 22.67 | 3.71 | GroupKey 79 ms; the top ten is `sort_limit` (4 ms). |
| q19 | 0.98 | 0.12 | 8.17 | DuckDB skips row groups by min/max (zone maps); corgi scans 1M. |
| q20 | 333.98 | 37.22 | 8.97 | Approximated in method (no substring search). gather_try 109 ms + split 85 ms + the "googl" literal filled per piece 43 ms. |
| q21 | 333.48 | 5.59 | 59.66 | DuckDB runs the cheap predicate first and LIKE only on survivors (5.8 ms); corgi searches all 1M URLs. |
| q22 | 436.49 | 17.59 | 24.81 | Approximated in method. Two splits, two gather_trys; see q20. |
| q23 | 340.07 | 37.23 | 9.13 | Approximated: three columns for SELECT *. Cost is q20's search. |
| q24 | 8.10 | 2.05 | 3.95 | `sort_limit` takes 0.7 ms; building and filtering the 1M (time, phrase) pairs takes 7. q24r carries the phrases by reference: 4.7 ms. |
| q25 | 7.01 | 1.79 | 3.92 | As q24; `sort_limit` reads the phrases eight bytes at a time and stops once ten are settled. |
| q26 | 8.21 | 2.03 | 4.04 | As q24. |
| q27 | 6.43 | 6.46 | 1.00 | At parity: integer key, few groups. |
| q28 | 2302.03 | 526.75 | 4.37 | Approximated in method (no regex). SortList 1.4 s: MIN(Referer) sorts every group's referers to take one; GroupKey on domains 0.3 s. The regexp logic itself is ~0.4 s (filters, iotas, selects, cap_lists). |
| q29 | 35.14 | 3.29 | 10.68 | Ninety passes against DuckDB's fused one; an optimizer would write sum + k * count. |
| q30 | 6.35 | 3.17 | 2.00 | Near: small after the filter. |
| q31 | 6.35 | 3.66 | 1.73 | As q30. |
| q32 | 32.40 | 38.17 | 0.85 | Ahead: radix sort on two integer columns (GroupKey 22 ms) beats DuckDB's hash table at one group per row. |
| q33 | 1646.77 | 28.88 | 57.02 | GroupKey on 1M URLs (88 MB of bytes, 275K distinct) 1.6 s; the top ten of the 275K groups is `sort_limit` (4 ms). |
| q34 | 1648.88 | 30.60 | 53.88 | As q33. |
| q35 | 30.40 | 7.79 | 3.90 | Grouping four columns where one decides the rest (28 ms vs 8). |
| q36 | 540.00 | 13.42 | 40.24 | GroupKey on the 377K URLs that pass 0.49 s; the top ten by `sort_limit` 2 ms. |
| q37 | 429.10 | 8.41 | 51.02 | GroupKey on Titles 0.38 s. |
| q38 | 22.78 | 2.71 | 8.41 | Fewer rows survive; GroupKey 16 ms. |
| q39 | 1118.17 | 32.35 | 34.56 | GroupKey on 5-tuples with two strings 0.8 s; `select` blending byte lists 0.2 s. |
| q40 | 6.23 | 1.55 | 4.02 | Predicate chain: one pass per predicate and per `mul`; nothing survives. |
| q41 | 5.37 | 2.51 | 2.14 | As q40. |
| q42 | 7.86 | 2.97 | 2.65 | Predicate chain, then a small group. |

Geometric mean, q00 excluded: 5.3× DuckDB on one thread (6.3× before `sort_limit`). In total, corgi
takes 9.8 s and DuckDB 0.9 s.

| query shape | queries | corgi / DuckDB |
|---|---|---|
| scans, whole-table aggregates | q01–q03, q06 | 0.35–1.2× |
| integer GROUP BY, about one group per row | q32 | 0.81× |
| small groups after a selective filter | q10, q11, q27, q30, q31 | 1.0–2.2× |
| integer GROUP BY, COUNT DISTINCT | q04, q07–q09, q15, q35 | 2.4–5.6× |
| predicate chains, then a small group | q40–q42 | 2.1–4.0× |
| ORDER BY … LIMIT, by `sort_limit` | q24–q26 | 3.9–4.0× (18–26× by full sort) |
| point lookup | q19 | 8.2× |
| ninety SUMs | q29 | 10.7× |
| string GROUP BY, low and mid cardinality | q05, q12–q14, q16–q18 | 3.7–14× |
| substring search (LIKE) | q20–q23 | 9–60× |
| string GROUP BY, high cardinality (URL, Title) | q33, q34, q36, q37, q39 | 35–57× |

## What the queries argue for

1. **Grouping, dedup and count-distinct on byte strings.** Today these sort the strings
   structurally. That is about 7 of corgi's 10 s: q33's `group` of 1M URLs takes 1.6 s, while
   DuckDB answers the whole query in 29 ms. Either of these would avoid it:
   - group on a u64 hash, and compare bytes only within equal hashes;
   - dictionary codes: a reference column over a distinct list of strings is already a dictionary.

   Order is only needed for the final top ten.
2. **Top-k** is `sort_limit` now. It sorts the order's levels one at a time and keeps, after
   each, only the first k and the ties at the k-th. q24–q26 went from 18–26× to 4×, and the final
   sorts of q33, q36 and q39 from 0.15–0.3 s to a few ms. Most of what remains in q24 is building
   and filtering the (time, phrase) pairs; q24r carries the phrases by reference and takes 4.7 ms.
3. **Missing words: substring search and string min/max.**
   - q20–q23 spend 9–60× on split-and-compare.
   - MIN(Referer) by sorting each group is 1.4 s of q28.
4. **COUNT(\*) without value lists.** `(k, 1u64) group … ones len` builds lists only to count
   them. Run lengths (`adjacent`, `cut`) or a bincount for a small key domain would not.
5. **Predicate chains.** Each comparison, and each `mul` used as AND, is its own pass. DuckDB
   fuses them, evaluates the cheap ones first (q21's LIKE runs only on survivors), and skips blocks
   by min/max (q19).

The corgi side is ahead in one shape: one group per row on integer keys (q32), where radix sort
beats DuckDB's hash table.
