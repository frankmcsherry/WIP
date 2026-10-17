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
  - q03 (AVG of a 64-bit id): sums of 32-bit halves, since the ids sum to about 2^80 and an Int
    sum wraps at the i64 edge.
  - q20–q22 (LIKE): split and compare.
  - q28 (REGEXP_REPLACE): the equivalent byte logic.
- **Cut down:** q23 returns three columns for `SELECT *`.
- **Changes to the SQL itself:**
  - every ORDER BY breaks ties;
  - HAVING thresholds are divided by 100 (this is 1M rows of the full 100M);
  - EventDate is compared as day numbers.

## Measured

Measured 2026-10-16 on an M4 mini, on the branch that writes the GROUP BY queries with `scatter`
(after #103), one partition (`hits_0`, 1M rows). Times are the best of three processes, in ms. DuckDB holds the table in memory and runs on one thread; its
per-query overhead is about 0.1 ms. The last column is from one `--profile` run: each op kind's
share of the query's time.

| query | corgi | DuckDB 1 thread | × | what dominates |
|---|---|---|---|---|
| q00 | 0.00 | 0.09 | 0 | Nothing to do: the row count is in the bounds. |
| q01 | 0.08 | 0.40 | 0.20 | Ahead: one compare pass and a sum. |
| q02 | 0.14 | 0.40 | 0.35 | Ahead: two sums. |
| q03 | 0.80 | 0.74 | 1.08 | Approximated in method (no integer wider than i64): the div and rem by 2^32 are 79%. |
| q04 | 5.55 | 3.44 | 1.61 | COUNT(DISTINCT UserID) as `scatter_add`, then `len`: numbering the 80K distinct ids about 40%, summing the ones about 30%. |
| q05 | 14.18 | 6.10 | 2.32 | `scatter_add` on search phrases, then `len`: the exactness check about 45%, hashing 27%. |
| q06 | 0.22 | 0.63 | 0.35 | Ahead: a min and a max. |
| q07 | 0.48 | 0.54 | 0.89 | Ahead: the filter keeps 1.4% of rows (71%). |
| q08 | 9.02 | 5.09 | 1.77 | Distinct users per region, as two `scatter_add`s: on the 1M (region, user) pairs, hashing, numbering and the exactness check about a quarter each. |
| q09 | 11.22 | 13.77 | 0.81 | Ahead: as q08, with the sums by a third `scatter_add`. |
| q10 | 1.29 | 1.69 | 0.76 | Ahead: the filter 36%, `len` 29%, the two scatters 22%. |
| q11 | 1.39 | 1.94 | 0.72 | As q10. |
| q12 | 3.91 | 3.22 | 1.21 | `scatter_add` on the 69K non-empty phrases, by reference (66%): hashing them is 35%. |
| q13 | 5.75 | 4.52 | 1.27 | As q12, as two `scatter_add`s. |
| q14 | 4.30 | 3.52 | 1.22 | As q12. |
| q15 | 5.90 | 5.40 | 1.09 | Integer key, 80K distinct: numbering about 40%, summing about 30%. |
| q16 | 19.54 | 10.19 | 1.92 | `scatter_add` on (user, phrase), 98K distinct: hashing and the exactness check a third each, numbering and copying the keys out 13% each. |
| q17 | 20.82 | 9.96 | 2.09 | As q16. |
| q18 | 32.51 | 22.44 | 1.45 | As q16, with the minute in the key: 387K distinct, the exactness check a third, numbering 27%. |
| q19 | 0.42 | 0.12 | 3.50 | DuckDB skips row groups by min/max; corgi's filter scans all 1M rows (79%). |
| q20 | 196.78 | 36.91 | 5.33 | Approximated in method (no substring search): `split` 40%, `gather` 25%. |
| q21 | 199.07 | 5.70 | 34.92 | As q20; DuckDB runs LIKE only on the rows that pass the cheap predicate. |
| q22 | 291.99 | 17.39 | 16.79 | As q20, with two splits. |
| q23 | 200.55 | 37.07 | 5.41 | As q20; three columns for SELECT *. |
| q24 | 5.43 | 2.08 | 2.61 | Filtering 1M (time, phrase) pairs is 84%; `sort_limit` 8%. q24r carries the phrases by reference: 1.87 ms, 1.02×. |
| q25 | 6.29 | 1.57 | 4.01 | As q24. |
| q26 | 5.82 | 1.86 | 3.13 | As q24. |
| q27 | 3.28 | 6.84 | 0.48 | Ahead: integer key, few groups; `scatter_add` 44%, the filters 33%. |
| q28 | 651.34 | 461.84 | 1.41 | Approximated in method (no regex): the byte logic's filters 29%, `hash_group`'s sort 17%, MIN by `sort_limit 1` 13%. |
| q29 | 17.85 | 3.44 | 5.19 | Ninety passes, each a map then a sum, against DuckDB's one. |
| q30 | 2.01 | 3.27 | 0.61 | Ahead: small after the filter. |
| q31 | 3.23 | 3.87 | 0.83 | As q30, about one group per row: `sort_limit` 29%. |
| q32 | 36.49 | 40.19 | 0.91 | Ahead: 1M groups of one row each. `scatter_add` about half, `sort_limit` about half (see 3 below). |
| q33 | 105.80 | 28.26 | 3.74 | `scatter_add` on 1M URLs (88 MB), 275K distinct: copying the keys out 41%, hashing 38%, the exactness check 12%. |
| q34 | 106.26 | 28.51 | 3.73 | As q33. |
| q35 | 13.64 | 7.76 | 1.76 | Grouping four columns where one decides the rest, 68K distinct: the exactness check 37%, hashing 30%, numbering 18%. |
| q36 | 31.22 | 13.50 | 2.31 | `scatter_add` on the 377K URLs that pass, by reference: hashing 60%, the exactness check 20%. |
| q37 | 19.12 | 8.45 | 2.26 | `scatter_add` on Titles: hashing 63%, the exactness check 25%. |
| q38 | 3.99 | 2.83 | 1.41 | Few rows survive; hashing 42%, `sort_limit` 19%. |
| q39 | 78.92 | 29.66 | 2.66 | `scatter_add` on 5-tuples with two strings: hashing 52%, the exactness check 16%, `select` 12%. |
| q40 | 1.98 | 1.64 | 1.21 | Predicate chain, then a small group. |
| q41 | 1.79 | 2.52 | 0.71 | As q40. |
| q42 | 2.67 | 2.96 | 0.90 | Predicate chain, then a small group. |

Geometric mean over q01–q42: 1.6× DuckDB on one thread (2.1× on 2026-10-12, 5.3× on 2026-10-03). In
total, corgi takes 2.1 s and DuckDB 0.84 s; q28 is 0.65 s of corgi's.

| query shape | queries | corgi / DuckDB |
|---|---|---|
| scans, whole-table aggregates | q01–q03, q06 | 0.20–1.08× |
| integer GROUP BY, about one group per row | q32 | 0.91× |
| small groups after a selective filter | q07, q10, q11, q27, q30, q31 | 0.48–0.89× |
| predicate chains, then a small group | q40–q42 | 0.71–1.21× |
| string GROUP BY | q05, q12–q14, q16–q18, q33, q34, q36–q39 | 1.21–3.7× |
| integer GROUP BY, COUNT DISTINCT | q04, q08, q09, q15, q35 | 0.81–1.8× |
| ORDER BY … LIMIT after filtering strings | q24–q26 | 2.6–4.0× (1.0× with references, q24r) |
| point lookup | q19 | 3.5× |
| ninety SUMs | q29 | 5.2× |
| substring search (LIKE), by split and compare | q20–q23 | 5.3–35× |

## What the queries argue for

1. **Grouping is `scatter`.** Every GROUP BY whose aggregates are counts, sums, mins and maxes is a
   `scatter_<op>`: totals by key, no rows moved. COUNT(DISTINCT) is two of them, one making the
   (group, value) pairs distinct and one counting those by group. Written with `group` and
   `hash_group` before, the 28 queries that changed now take 0.69× the time (geometric mean; q08
   30 → 9.0 ms, q15 17 → 5.9). What remains in them:
   - **The hash.** corgi's structural hash, the stable id of a value, takes a multiply per byte. A
     hash of byte lists that reads eight bytes at a time, tried in `scatter` alone: q33 took 78 ms
     against 106, q36 20 against 31, q37 12 against 19, q39 54 against 79.
   - **Copying string keys out.** `scatter` copies each distinct key, and `gather` copies a list of
     bytes an element at a time: in q33 and q34, 41%. Open PR #96 copies rows as ranges.
   - **The exactness check**, a third of q05, q16 and q18: each key is compared with its number's
     first occurrence, which lies anywhere in the column.
2. **One group per row** (q31, q32). `scatter_add` numbers q32's 1M distinct (WatchID, ClientIP)
   keys in about the time `group` took (20 against about 21 ms). What grew is `sort_limit`.
3. **Top-k is `sort_limit`.** It sorts the order's levels one at a time and keeps, after each, only
   the first k and the ties at the k-th. When the first level is all ties, as q32's counts are
   (every one 1), every row survives into the next: 18 ms for ten of 1M rows, against 6.7 when
   `group` handed them over sorted by key, and DuckDB's 2.3. In q24–q26, what remains is filtering
   the strings before it; q24r carries them by reference. Open PR #96 (list rows move as ranges of
   elements) takes q24–q26 to 1.2–2.4 ms.
4. **Substring search is left to the host.** corgi has no substring-search word, and adding one is
   not planned: a host that needs LIKE can provide it. q20–q23 keep split-and-compare as a record
   of the cost.
5. **Predicate chains.** Each comparison, and each AND (`min` of two masks), is its own pass.
   DuckDB fuses them, evaluates the cheap ones first (q21's LIKE runs only on survivors), and skips
   blocks by min/max (q19).
6. **q29's ninety sums** are ninety passes, each writing a column before summing it. Fusing a map
   into the sum that consumes it, or rewriting `sum(x + k)` as `sum + k · count`, would remove them.

The corgi side is ahead on scans and whole-table aggregates, on small groups after a selective
filter, and on one group per row with integer keys (q32).
