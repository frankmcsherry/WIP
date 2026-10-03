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

Measured 2026-10-03 on an M4 mini, at scale factor 0.2 (1.2M lineitem rows). Times are the best
run, in ms. DuckDB holds the tables in memory and runs on one thread.

| query | what | corgi | DuckDB 1 thread | × |
|---|---|---|---|---|
| q01 | group by two flags, 6 aggregates | 37.95 | 18.71 | 2.03 |
| q01b | q01 with the flags as bytes, not strings | 29.08 | 18.60 | 1.56 |
| q03 | 3-way: lineitems probe sorted orders, then group | 8.12 | 5.95 | 1.36 |
| q03b | q03 the other way: orders probe sorted lineitems (find + slices) | 12.92 | 5.95 | 2.17 |
| q03c | q03b without the lineitem sort (stored in key order) | 5.82 | 6.14 | 0.95 |
| q05 | 6-way join chain, group by nation | 6.81 | 6.42 | 1.06 |
| q06 | filter and sum, no join | 4.95 | 1.85 | 2.68 |
| q10 | 4-way, top 20 customers | 20.48 | 9.03 | 2.27 |
| q12 | 2-way, string predicates (`eq` on strings), conditional counts | 70.22 | 15.25 | 4.60 |
| q14 | 2-way, `LIKE 'PROMO%'` | 5.20 | 2.98 | 1.74 |
| q19 | 2-way, OR of three string-heavy predicates (`eq` on strings) | 110.04 | 23.81 | 4.62 |

q01 and q01b were measured again after `group` became a word over `sort_by` (1.25× and 1.10× the
kernel's time: four groups over 1.2M rows, where the sort is cheap and the word's separate passes
for run starts, keys and pieces show); the other queries moved less than 5%.

The joins run 1.1–1.4× DuckDB (q03, q05), and 0.95× when the large table is already in key order
(q03c). Where string predicates dominate, they run 2.3–4.6× (q10, q12, q19).

## What the joins argue for

1. **String equality against a constant.** `eq` compares lists structurally, so
   `(s, "BUILDING") eq` works. It costs about 20 ns per string, for two reasons:
   - the literal is filled once per row (`lit`, 25 of q12's 70 ms);
   - the list comparator builds index pairs for every element (`Rel(Eq)`, 41 ms).

   Comparing against the constant in place (an immediate, as leaf constants already are) would
   make it one pass over the bytes. These programs first compared `hash`es, on the belief that
   `eq` took only leaves (its comments said so). For q12 and q19 that was 1.2–1.3× slower still.
   For q10's one-byte flag it was faster (14.6 against 20.5 ms): there the per-row literal fill is
   most of the cost.
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
