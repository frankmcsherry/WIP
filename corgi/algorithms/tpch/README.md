# TPC-H joins in corgi

TPC-H's join queries Q3, Q5, Q10, Q12, Q14, Q18 and Q19, and Q1 and Q6 without joins, written in
corgi and checked against DuckDB. Some programs have a second or third version: the same query
written another way, to compare formulations. Each `qNN.col` names the SQL it answers, the columns
it reads (from several tables), and its output's kinds. Every table is one row: each column is a
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
- **q03:** probe from the large side into the sorted small side, then `scatter_add` by order.
- **q03b:** each order finds its range in the sorted lineitems, and `slices` + `fold_add` sum the
  range. No sum by key is needed, but sorting the large side is 57% of its time.
- **q03c:** q03b without that sort, since lineitem is stored in order-key order. It is the
  fastest, 0.82× DuckDB's single-thread time.

Which side to sort is a plan decision. Corgi can't express it today: it doesn't know a column is
sorted, and `sort` is cheaper only on input whose leading field strictly ascends, which the line
items, several to an order, do not.

## Measured

Measured 2026-10-12 at master `6275a5d` on an M4 mini, at scale factor 0.2 (1.2M lineitem rows).
Times are the best of three processes, in ms. DuckDB holds the tables in memory and runs on one
thread. The last column is from one `--profile` run.

| query | what | corgi | DuckDB 1 thread | × | what dominates |
|---|---|---|---|---|---|
| q01 | group by two flags, 6 aggregates | 34.05 | 18.76 | 1.82 | `group`'s sort 50%, the filter 35% |
| q01b | q01 with the flags as bytes, not strings | 25.45 | 18.98 | 1.34 | the sort 46%, reading the flags as bytes 22% |
| q03 | 3-way: lineitems probe sorted orders, then sum by order | 6.36 | 6.27 | 1.01 | filters 40%, `find` 32% |
| q03b | q03 the other way: orders probe sorted lineitems (find + slices) | 11.96 | 5.67 | 2.11 | sorting lineitem 57% |
| q03c | q03b without the lineitem sort (stored in key order) | 4.86 | 5.91 | 0.82 | filters 49%, `find` 20% |
| q05 | 6-way join chain, group by nation | 5.16 | 6.31 | 0.82 | `find` 47%, filters 24% |
| q06 | filter and sum, no join | 2.09 | 1.83 | 1.14 | ten passes of compares, ANDs and products |
| q10 | 4-way, top 20 customers | 5.95 | 8.66 | 0.69 | `eq` with the flag 35%, filters 30% |
| q12 | 2-way, string predicates, conditional counts | 7.25 | 14.99 | 0.48 | `eq` with the two ship modes 52% |
| q14 | 2-way, `LIKE 'PROMO%'` | 2.71 | 2.94 | 0.92 | arithmetic and filters |
| q19 | 2-way, OR of three string-heavy predicates | 7.82 | 22.88 | 0.34 | `eq` with string constants 57% |

Geometric mean over the eleven programs: 0.92× DuckDB on one thread (2.0× on 2026-10-03). Taking one
program per query (q01, q03c and the rest), 0.79×.

Since 2026-10-03:
- `eq` and `ne` against a string literal compare each row with the constant in place, lengths first
  and then bytes (#88, #89). q12 went from 70 to 7 ms and q19 from 110 to 8: string predicates went
  from 4.6× to 0.3–0.7×.
- Filter chooses between compressing and gathering positions by how many rows it keeps (#97):
  q05 and q14 about 1.3× faster.
- Open PR #95 stores the two one-character flags (CHAR(1), always one ASCII letter) as their byte.
  q01 then reads them like q01b, which goes; on regenerated data q01 takes 18.9 ms against DuckDB's
  13.6 (1.39×). Existing data directories need `prepare.py` run again.

The remaining gap was q01. It puts 1.2M rows into four groups: `group` sorts them, then moves the
four summed columns into group order. DuckDB adds each row into its group's sums as it scans. q01s
does the same with `scatter_add`: each row's group is its two flag bytes as one number (or -1 when
the date leaves it out), and one scatter totals every column by group, a 1 per row counting them. At
scale factor 1, on one thread: q01 173 ms, q01s 67 ms, DuckDB 94 ms. Of q01s's time, 30 ms reads the
first byte of each one-character flag string; with the flags stored as bytes (#95) q01s takes 37 ms
against DuckDB's 68.

q18 is TPC-H's large aggregation: every order's line-item quantities summed, 6M rows into 1.5M
orders at scale factor 1, before the orders over 300 units are kept. On one thread at scale factor
1, q18 sums them with `group` in 52 ms, q18s with `scatter_add` in 27, and DuckDB takes 94. The
order keys are Ints in a narrow range, so `scatter_add` numbers them by a table over the range, and
as the line items are stored in order-key order, each one adds into the slot after the last one's.
Numbered by their hashes instead, as keys that are not Ints are, the same sums take 80 ms. Data
directories made before q18 need `prepare.py` run again, for `o_totalprice` and q18's answers.

q03, q05, q10 and q12 sum by key with `scatter_add` too. Their sums come after joins and filters
have cut the rows to tens of thousands, so the change is small: at scale factor 1, q03 33.8 → 33.4
ms, q05 30.6 → 30.3, q10 32.0 → 30.9, q12 35.6 → 35.1.

## Which queries are here

Nine of TPC-H's 22: those that are joins on keys, a group, and arithmetic. The others need a
query planner's work rather than an engine's: correlated subqueries (q02, q17, q20), EXISTS and
NOT EXISTS (q04, q21, q22), an outer join (q13), or `LIKE '%…%'` (q09, q13, q16), which corgi leaves
to the host. Translating them by hand would measure the translation more than corgi. q07, q08, q11
and q15 are direct enough to add if wanted; q11 needs the `partsupp` table in `prepare.py`.

## What the joins argue for

1. **Known sortedness.** Tables arrive in key order, and every query sorts its build side again.
   Skipping the sort (q03c) makes the range join the fastest form.
2. **A lookup for unique keys.** Every join here is on a primary key, so `hi − lo` is 0 or 1.
   `find` searches both bounds and then goes through a mask, a zip and a filter: `find` is 20–47%
   of the join queries. The lower bound plus one equality probe would do.
3. **Late materialization.** Each step of q05's chain filters and gathers again the columns it
   carries. Passing positions forward and gathering once at the end would avoid that. q10 does
   this by hand: it aggregates first, then looks up names for only the top 20.
4. **Reduce by key for small key domains** (q01, above). After that comes fusion of predicate
   chains (q06's ten passes).

Joining on several columns, such as (key, nation) in q05, is done as two lookups and an equality
filter.
