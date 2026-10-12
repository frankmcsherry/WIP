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

Measured 2026-10-12 at master `6275a5d` on an M4 mini, one partition (`hits_0`, 1M rows). Times are
the best of three processes, in ms. DuckDB holds the table in memory and runs on one thread; its
per-query overhead is about 0.1 ms. The last column is from one `--profile` run: each op kind's
share of the query's time.

| query | corgi | DuckDB 1 thread | × | what dominates |
|---|---|---|---|---|
| q00 | 0.00 | 0.09 | 0 | Nothing to do: the row count is in the bounds. |
| q01 | 0.08 | 0.39 | 0.21 | Ahead: one compare pass and a sum. |
| q02 | 0.13 | 0.40 | 0.33 | Ahead: two sums. |
| q03 | 0.75 | 0.73 | 1.03 | Approximated in method (no integer wider than i64): the div and rem by 2^32 are 79%. |
| q04 | 16.41 | 3.52 | 4.66 | COUNT(DISTINCT UserID): sorting 1M 64-bit ids is 90%, against DuckDB's hash set. |
| q05 | 28.48 | 5.77 | 4.94 | `hash_group` on search phrases: sorting the hashes 52%, the exactness check 28%, hashing 15%. |
| q06 | 0.22 | 0.63 | 0.35 | Ahead: a min and a max. |
| q07 | 0.52 | 0.54 | 0.96 | At parity: the filter keeps 1.4% of rows. |
| q08 | 29.68 | 4.93 | 6.02 | Distinct users per region: sorting (region, user) is 89%. |
| q09 | 32.18 | 13.75 | 2.34 | As q08. |
| q10 | 2.01 | 1.73 | 1.16 | Near: `group` on the 2% of rows with a phone model (44%), and the filter (30%). |
| q11 | 2.06 | 1.80 | 1.14 | As q10. |
| q12 | 4.75 | 3.05 | 1.56 | `hash_group` on the 69K non-empty phrases: hashing 32%, sorting 20%, the exactness check 17%. |
| q13 | 5.79 | 3.99 | 1.45 | As q12. |
| q14 | 5.26 | 3.48 | 1.51 | As q12. |
| q15 | 17.30 | 5.37 | 3.22 | Integer key, 80K distinct: sorting 1M ids is 84%. |
| q16 | 41.70 | 9.95 | 4.19 | `hash_group` on (user, phrase): sorting the hashes 44%, the exactness check 36%, hashing 14%. |
| q17 | 42.64 | 9.89 | 4.31 | As q16. |
| q18 | 45.68 | 23.51 | 1.94 | As q16, with the minute in the key. |
| q19 | 0.42 | 0.12 | 3.50 | DuckDB skips row groups by min/max; corgi's filter scans all 1M rows (79%). |
| q20 | 192.41 | 36.96 | 5.21 | Approximated in method (no substring search): `split` 40%, `gather` 25%. |
| q21 | 193.79 | 5.52 | 35.11 | As q20; DuckDB runs LIKE only on the rows that pass the cheap predicate. |
| q22 | 299.22 | 17.61 | 16.99 | As q20, with two splits. |
| q23 | 198.37 | 37.18 | 5.34 | As q20; three columns for SELECT *. |
| q24 | 5.71 | 1.87 | 3.05 | Filtering 1M (time, phrase) pairs is 84%; `sort_limit` 8%. q24r carries the phrases by reference: 1.77 ms, 0.95×. |
| q25 | 6.41 | 1.61 | 3.98 | As q24. |
| q26 | 5.73 | 1.88 | 3.05 | As q24. |
| q27 | 6.09 | 6.41 | 0.95 | At parity: integer key, few groups. |
| q28 | 645.50 | 459.91 | 1.40 | Approximated in method (no regex): the byte logic's filters 29%, `hash_group`'s sort 17%, MIN by `sort_limit 1` 13%. |
| q29 | 17.26 | 3.19 | 5.41 | Ninety passes, each a map then a sum, against DuckDB's one. |
| q30 | 2.56 | 3.16 | 0.81 | Ahead: small after the filter. |
| q31 | 2.37 | 3.70 | 0.64 | As q30. |
| q32 | 26.52 | 39.15 | 0.68 | Ahead: a radix sort on two integer columns beats a hash table at one group per row. |
| q33 | 88.13 | 27.55 | 3.20 | `hash_group` on 1M URLs (88 MB): hashing 43%, the exactness check 35%, sorting 16%. |
| q34 | 88.41 | 31.19 | 2.83 | As q33. |
| q35 | 22.90 | 7.84 | 2.92 | Grouping four columns where one decides the rest: the sort is 79%. |
| q36 | 34.04 | 14.18 | 2.40 | `hash_group` on the 377K URLs that pass: hashing 52%. |
| q37 | 26.38 | 8.32 | 3.17 | `hash_group` on Titles: hashing 42%, the exactness check 28%. |
| q38 | 3.99 | 2.75 | 1.45 | Few rows survive; hashing 28%, `sort_limit` 17%. |
| q39 | 81.78 | 29.34 | 2.79 | `hash_group` on 5-tuples with two strings: hashing 47%, the exactness check 17%, `select` 13%. |
| q40 | 2.40 | 1.55 | 1.55 | Predicate chain, then a small group. |
| q41 | 2.23 | 2.49 | 0.90 | As q40. |
| q42 | 4.26 | 2.99 | 1.42 | Predicate chain, then a small group. |

Geometric mean over q01–q42: 2.1× DuckDB on one thread (5.3× on 2026-10-03). In total, corgi takes
2.2 s and DuckDB 0.84 s; q28 is 0.65 s of corgi's.

| query shape | queries | corgi / DuckDB |
|---|---|---|
| scans, whole-table aggregates | q01–q03, q06 | 0.21–1.03× |
| integer GROUP BY, about one group per row | q32 | 0.68× |
| small groups after a selective filter | q07, q10, q11, q27, q30, q31 | 0.64–1.16× |
| predicate chains, then a small group | q40–q42 | 0.90–1.55× |
| string GROUP BY, by `hash_group` | q05, q12–q14, q16–q18, q33, q34, q36–q39 | 1.45–4.9× |
| integer GROUP BY, COUNT DISTINCT | q04, q08, q09, q15, q35 | 2.3–6.0× |
| ORDER BY … LIMIT after filtering strings | q24–q26 | 3.1–4.0× (0.95× with references, q24r) |
| point lookup | q19 | 3.5× |
| ninety SUMs | q29 | 5.4× |
| substring search (LIKE), by split and compare | q20–q23 | 5.2–35× |

## What the queries argue for

1. **Grouping on strings: done, by hash.** `hash_group` sorts 64-bit hashes rather than the
   strings, hands keys back by reference, and checks the groups are exact (rows whose keys share a
   hash are grouped by sorting the keys). Where a filter comes first, the key goes through it by
   reference (`k ref`). q33 went from 1.6 s to 88 ms. It pays where keys are many or long; for a few
   distinct short keys, sorting them costs less than hashing every row, so q10, q11 and TPC-H q01
   and q12 keep `group` (q10 and q01 measured 1.24× and 1.7× slower with `hash_group`). What
   remains in these queries:
   - **The hash.** On URL keys it is about half the time. `foldhash`'s `quality` variant, pinned,
     measured 0.76 of master's time over the fourteen `hash_group` queries (q33 92 → 59 ms) with the
     same quality as rapidhash; whether to take the dependency is undecided. The `fast` variant
     fails avalanche on short keys and is not a candidate; ahash's output depends on the CPU.
   - **The exactness check** (`adjacent` on the keys in hash order), 17–36%: it reads each key from
     a random place.
   - **Sorting the hashes**, 16–52%: a radix sort over all 64 bits, where grouping needs only equal
     hashes together.
2. **Integer GROUP BY and COUNT DISTINCT on many keys** (q04, q08, q09, q15, q35): sorting 1M
   64-bit keys is 79–90% of each. DuckDB uses a hash table. The same sort is the largest part of the
   short-key `hash_group` queries (q05, q16–q18), so a faster grouping of 64-bit keys would serve
   about nine queries.
3. **Top-k is `sort_limit`.** It sorts the order's levels one at a time and keeps, after each, only
   the first k and the ties at the k-th. What remains in q24–q26 is filtering the strings before it;
   q24r carries them by reference and is at parity. Open PR #96 (list rows move as ranges of
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
filter, and on one group per row with integer keys (q32), where radix sort beats DuckDB's hash table.
