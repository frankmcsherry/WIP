# TPC-H joins in corgi

TPC-H's join queries Q3, Q5, Q10, Q12, Q14 and Q19, and Q1 and Q6 without joins, written in corgi
and checked against DuckDB. Some programs have a second or third version: the same query written
another way, to compare formulations. Each `qNN.col` names the SQL it answers, the columns it
reads (from several tables), and its output's kinds. Every table is one row: each column is a
one-row list.

    python3 benches/tpch/prepare.py DIR 0.2      # DuckDB's tpch extension; needs duckdb and numpy
    CORGI_TPCH=DIR cargo bench --bench tpch [-- NAME ...] [--check] [--profile]

The columns are integers throughout: prices in cents, discounts and taxes in percent, quantities in
units, dates in days since 1970. Each query's SQL computes the same integer expressions, so the
answers match exactly.

## How the joins are written

Every join is the same four steps:
1. Sort the build side by key.
2. `find` the probe keys in it.
3. Keep the matches:
   `(found, probe) zip map (((lo, hi), x) -> ((hi, lo) gt, (lo, x))) filter`.
4. Gather the build side's columns at `lo`.

The filter has to come first. A table is one row, so a checked `gather` with one unmatched key
fails the whole table.

Q3 is written three ways:
- **q03:** probe from the large side into the sorted small side, then `group`.
- **q03b:** each order finds its range in the sorted lineitems, and `slices` + `fold_add` sum the
  range. No `group` is needed, but sorting the large side is 7 of its 13.6 ms.
- **q03c:** q03b without that sort, since lineitem is stored in order-key order. It is the
  fastest, at DuckDB's single-thread time.

Which side to sort is a plan decision. Corgi can't express it today: it doesn't know a column is
sorted, and `sort` is no cheaper on sorted input.

## Measured

Measured 2026-10-03 on an M4 mini, at scale factor 0.2 (1.2M lineitem rows), at this commit. Times
are the best run, in ms. DuckDB holds the tables in memory and runs on one thread.

| query | what | corgi | DuckDB 1 thread | × |
|---|---|---|---|---|
| q01 | group by two flags, 6 aggregates | 30.48 | 18.71 | 1.63 |
| q01b | q01 with the flags as bytes, not strings | 25.63 | 18.60 | 1.38 |
| q03 | 3-way: lineitems probe sorted orders, then group | 8.69 | 5.95 | 1.46 |
| q03b | q03 the other way: orders probe sorted lineitems (find + slices) | 13.57 | 5.95 | 2.28 |
| q03c | q03b without the lineitem sort (stored in key order) | 6.29 | 6.14 | 1.02 |
| q05 | 6-way join chain, group by nation | 7.13 | 6.42 | 1.11 |
| q06 | filter and sum, no join | 5.04 | 1.85 | 2.72 |
| q10 | 4-way, top 20 customers | 14.57 | 9.03 | 1.61 |
| q12 | 2-way, string predicates, conditional counts | 82.19 | 15.25 | 5.39 |
| q12b | q12 with literal hashes folded to constants, CSE | 22.39 | 15.50 | 1.44 |
| q14 | 2-way, `LIKE 'PROMO%'` | 5.40 | 2.98 | 1.81 |
| q19 | 2-way, OR of three string-heavy predicates | 143.62 | 23.81 | 6.03 |
| q19b | q19 with literal hashes folded to constants | 39.63 | 23.32 | 1.70 |

The natural versions are 1.1–1.6× DuckDB for the joins, except where string predicates dominate:
q12 at 5.4× and q19 at 6.0×.

## What the joins argue for

1. **String equality.** `eq` compares leaves only, so `s = 'BUILDING'` is spelled
   `(s hash, "BUILDING" hash) eq`, which is 80–95% of q12 and q19. Folding the literal's hash to a
   constant, plus CSE, gives q12b and q19b: 3.6× faster. What remains is hashing the column itself.
   An equality kernel against a constant, or dictionary codes, would remove that too.
2. **Known sortedness.** Tables arrive in key order, and every query sorts its build side again.
   Skipping the sort (q03c) makes the range join the fastest form.
3. **A lookup for unique keys.** Every join here is on a primary key, so `hi − lo` is 0 or 1.
   `find` searches both bounds and then goes through a mask, a zip and a filter:
   - `find` is 15–35% of the join queries, and `Filter` 25–37%.
   - The lower bound plus one equality probe would do.
4. **Late materialization.** Each step of q05's chain filters and gathers again the columns it
   carries. Passing positions forward and gathering once at the end would avoid that. q10 does
   this by hand: it aggregates first, then looks up names for only the top 20.
5. **Reduce by key for small key domains.** q01 spends 12–16 ms in `group` to put 1.2M rows into
   4 groups, moving 4 value columns. After that comes fusion of predicate chains (q06's ten passes).

Joining on several columns, such as (key, nation) in q05, is done as two lookups and an equality
filter.
