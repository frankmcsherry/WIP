# Algorithms: a corpus for the optimizer

Programs written the way a user would first write them: the direct translation of the textbook
algorithm, not arranged for speed. Each is checked against a plain-Rust reference and timed
against it by `benches/algorithms`:

    cargo bench --bench algorithms [-- NAME ...] [--check] [--explain] [--optimize] [--rows N] [--scale K]
    cargo bench --bench algorithms --features profile -- NAME --check --profile

`--check` compares outputs on small inputs; `--explain` prints the graph `Program` runs;
`--profile` prints each op kind's own time; `--optimize` runs `corgi::optimize` first.

A program named `<name>_<variant>` is the same algorithm rewritten by hand. The difference between
the two is a rewrite the optimizer could aim at.

## The corpus

All times are in ns per row, at 65,536 rows on an M4 mini, measured 2026-10-12 at master `6275a5d`:
the best of three processes per program, since one process can run 20% slower than the next. The
last column is corgi's time divided by Rust's.

| program | computes | corgi | Rust | × |
|---|---|---|---|---|
| argsort_pairs | positions of (a, b) pairs in sorted order, by `sort_by` | 382 | 234 | 1.6 |
| balanced_brackets | `()[]{}` balanced, by a fold with a stack | 463 | 57.7 | 8.0 |
| balanced_brackets_levels | the same from scans and a sort, no stack | 329 | 57.9 | 5.7 |
| base64_encode | base64 with `=` padding | 248 | 27.6 | 9.0 |
| base64_encode_arith | the same without the alphabet table or appends | 293 | 27.4 | 10.7 |
| days_from_civil | days since 1970 and weekday (Hinnant) | 5.4 | 2.6 | 2.1 |
| gcd | Euclid, as a fold over a fixed round count | 339 | 32.0 | 10.6 |
| group_aggregate | GROUP BY key: count, sum, max | 360 | 255 | 1.4 |
| group_aggregate_scatter | the same by `scatter_add` and `scatter_max` | 162 | 247 | 0.65 |
| histogram | 8-bucket counts, as an outer product | 488 | 26.2 | 18.6 |
| histogram_sorted | the same by sorting the bucket ids | 270 | 26.6 | 10.2 |
| histogram_scatter | the same by `scatter_add` | 122 | 26.7 | 4.6 |
| horner | polynomial at x | 39.9 | 5.4 | 7.4 |
| interval_merge | merge overlapping intervals | 343 | 133 | 2.6 |
| interval_merge_cut | the same, merged intervals cut at their marks | 248 | 134 | 1.8 |
| interval_merge_scatter | the same, each one's ends by `scatter_min` and `scatter_max` | 293 | 130 | 2.3 |
| ipv4_parse | dotted quad to `{Int \| ()}` | 75.3 | 15.3 | 4.9 |
| itoa | a non-negative Int's decimal digits | 194 | 23.9 | 8.1 |
| jaccard_sets | \|A∩B\| / \|A∪B\| of two lists as sets | 542 | 569 | 1.0 |
| jaro_winkler_direct | Jaro-Winkler, the textbook loop | 1424 | 97.8 | 14.6 |
| jaro_winkler_by_byte | the same, matched per byte value | 1063 | 97.3 | 10.9 |
| kadane | maximum subarray sum, as a fold | 34.9 | 11.6 | 3.0 |
| kadane_prefix | the same from prefix sums | 105 | 11.7 | 9.0 |
| levenshtein | edit distance, the textbook table | 1276 | 78.5 | 16.3 |
| linear_regression | least-squares slope and intercept | 117 | 8.9 | 13.2 |
| linear_regression_sums | the same from four Float sums | 64.6 | 8.9 | 7.3 |
| luhn | Luhn check of a digit string | 93.4 | 7.2 | 13.0 |
| median_percentile | median and 90th percentile | 170 | 78.2 | 2.2 |
| mode | most frequent value | 335 | 116 | 2.9 |
| mode_cut | the same as runs cut from the sorted values | 342 | 116 | 2.9 |
| mode_scatter | the same, counted by `scatter_add` | 146 | 114 | 1.3 |
| moving_average | sums of every 4-wide window | 238 | 27.4 | 8.7 |
| moving_average_prefix | the same from prefix sums | 107 | 27.9 | 3.8 |
| normalize_whitespace | lowercase, collapse and trim spaces | 122 | 112 | 1.1 |
| query_param | a key's value in `a=1&b=2` | 154 | 31.8 | 4.8 |
| run_length_encode | runs as (byte, count), gaps and islands | 188 | 45.1 | 4.2 |
| run_length_encode_scan | the same as the loop | 130 | 45.0 | 2.9 |
| run_length_encode_cut | the same, runs marked by `adjacent` and `cut` | 42.0 | 44.6 | 0.9 |
| sessionize | sessions split at gaps over 30 | 156 | 10.2 | 15.3 |
| sort_pairs | (a, b) pairs sorted | 341 | 212 | 1.6 |
| sort_pairs_steps | the same a column at a time, as Datatoad sorts | 534 | 217 | 2.5 |
| soundex | American Soundex code | 192 | 50.2 | 3.8 |
| soundex_ref | the same, the digit table captured by reference | 168 | 49.5 | 3.4 |
| substring_count | overlapping occurrences of a pattern | 1023 | 61.9 | 16.5 |
| substring_count_ref | the same, the text captured by reference | 369 | 64.7 | 5.7 |
| top_k | the three largest values | 186 | 88.3 | 2.1 |
| top_pairs | the first ten (a, b) pairs, sort then take | 385 | 197 | 2.0 |
| top_pairs_limit | the same by `sort_limit` | 387 | 193 | 2.0 |
| top_pairs_steps | the same a column at a time, pruned between the columns | 508 | 198 | 2.6 |
| trigram_similarity | pg_trgm similarity of two strings | 1247 | 270 | 4.6 |
| two_sum | does a pair sum to the target | 301 | 334 | 0.9 |
| word_topk | the three most frequent words | 663 | 295 | 2.2 |
| word_topk_scatter | the same, counted by `scatter_add` | 615 | 291 | 2.1 |

The `_scatter` rows were measured 2026-10-16, on the branch that adds them, the same way.

The rows near 1× (jaccard_sets, two_sum, group_aggregate, normalize_whitespace) are measured
against Rust that hashes or allocates. Rust written for the small key domain would be several times
faster, so those ratios flatter corgi.

The optimizer passes that exist (`--optimize`: peephole, iso cancellation, map fusion, CSE) changed
almost nothing when last measured (2026-10-03): gcd was 11% faster, and every other program within
the run-to-run spread (±4%).

### Longer rows

`--scale K` makes the lists of numbers K times longer in K times fewer rows, so a run handles the
same number of values. Ratios to Rust, best of three processes, 2026-10-12:

| program | today's lists | 32× longer | 1024× longer |
|---|---|---|---|
| group_aggregate | 1.40 | 0.90 | 0.99 |
| group_aggregate_scatter | 0.65 | 0.38 | 0.34 |
| histogram | 17 | 46 | 44 |
| histogram_sorted | 10 | 12 | 11 |
| jaccard_sets | 0.96 | 0.79 | 0.70 |
| kadane | 2.91 | 2.43 | 22 |
| kadane_prefix | 8.75 | 6.86 | 7.50 |
| linear_regression | 13 | 9.17 | 43 |
| median_percentile | 2.14 | 1.55 | 0.98 |
| mode | 2.83 | 2.19 | 2.50 |
| mode_scatter | 1.29 | 0.43 | 0.47 |
| moving_average | 8.36 | 51 | 53 |
| moving_average_prefix | 3.79 | 14 | 18 |
| sessionize | 15 | 15 | 17 |
| top_k | 2.07 | 1.46 | 0.98 |
| two_sum | 0.89 | 7.01 | 181 |

At long rows, sort, group and set programs match or beat Rust: radix sort and the dense search pay
off. Loops fall behind. The lockstep fold is parallel only across rows, so a few long rows give it
a few elements per round: kadane is 22× here, while the same algorithm as prefix scans
(kadane_prefix) holds at 7.5×. two_sum's 181× is early exit: Rust stops at the first pair it finds,
which in a long list comes almost at once, while corgi finishes every row.

### Sorting as words over `sort_by`

`sort_by` is the sort's own output: stable by key, a payload carried, each run of equal keys
numbered (dev/indexed-sort.md). `sort`, `dedup` and `group` are words over it; the kernels they
replaced (`SortList`, `DedupList`, `GroupKey`) are gone. Before they went, the words took 1.01× the
kernels' time across the corpus programs that use them (TPC-H 1.05×, ClickBench 1.01×), best of
three processes from one binary. The slowest were run_length_encode at 1.12×, whose keys arrive
already sorted, and word_topk at 1.05×.

The sort_pairs programs, on today's lists (ns per row) and on one row of about a million pairs
(`--scale 65536`, ms). "Kernels" is `sort` as the `SortList` kernel, before #70:

| program | kernels | words | one row, kernels | one row, words | one row, Rust |
|---|---|---|---|---|---|
| sort_pairs | 346 | 339 | 341 | 212 | 1.6 |
| top_pairs | 392 | 380 | 385 | 197 | 2.0 |
| argsort_pairs | | 380 | | 22.7 | 56.3 |
| sort_pairs_steps | | 515 | | 27.5 | 25.7 |
| top_pairs_limit | | 395 | | 11.0 | 2.3 |
| top_pairs_steps | | 755 | | 11.5 | 2.2 |

- `sort` as a word costs what the kernel did.
- Positions cost about what values cost: `argsort_pairs` carries `iota` as the payload, which
  follows the sort's permutation.
- A column at a time, as Datatoad sorts, costs 1.5× one sort over both columns (515 against 339; on
  one long row 27.5 against 20.2 ms). Each step carries positions and reads the next column
  through them.
- For the first ten, `sort_limit` pays on one long row (11.0 ms against 20.4 for sorting
  everything); on short rows there is little to prune. The same pruning written as words between
  the columns matches it on the long row and is about 2× slower on short ones.
- Packing the key and the payload into one word, at the widths their values use, made several of
  these about 2× faster in an experiment. It is not in the sort (see dev/indexed-sort.md).

## What the optimizer could do

Below, "time" is a program's per-op profile (own time, ns per row, from one `--profile` run at
65,536 rows on 2026-10-12, master `6275a5d`; one process, so up to 20% above the table's best of
three), and a share is that op's fraction of the program's time. Items are grouped by what they
need, and ranked within groups by how much time they would recover across the corpus. `SortBy` is
the sort under `sort`, `dedup` and `group`.

### Exact rewrites, no analysis

1. **A lookup table written inside a body is copied once per element.** In
   `w map (c -> (i, "0123…") get)`, the literal fills to the body's length, so a 26-byte table is
   built for every letter.
   - Rewrite: a gather whose haystack is a literal reads the literal's one row.
   - At stake: soundex `lit` 52 (27%), base64_encode `lit` 23 (9%). A literal filled to every row is
     now one block copy per row (#87), which took most of this away; soundex_ref, which reads the
     table by reference, runs 168 against soundex's 194. base64_encode_arith, which removes the
     table by hand, is now the slower of the two (293 against 248).
2. **`get` with a zero default is the raw gather.** Done: the plain `get`/`gather`/`zip`/`chunk` are
   the lossy kernels, so `(i, xs) get` reads zero out of range, and the checked forms are the words
   `try_get` and so on, written only where a program wants the miss.
3. **Run CSE and the peephole in `Program`.**
   - Repeated `len`, compares, `iota`s and constant seeds today run once per occurrence.
   - Measured with `--optimize`: gcd 11% faster, the rest unchanged. Cheap, but small on its own.
4. **Division and remainder by a constant become a multiply-high and a shift.** A power of two
   already does: `div` and `rem` by one run as shifts (toward zero, so a negative dividend is
   biased first). `select` with a constant operand should become an immediate too.
   - Any other constant divisor runs the scalar divider, and NEON has no integer divide.
   - At stake: days_from_civil's divisions and remainders by 100, 5, 400 and 7 are 26% of it;
     histogram (`/125`); itoa (19 rounds of `/10`, `%10`); gcd's `select` with 0 (Select 55, 17%).

### Rewrites that need a structural fact

5. **A gather through a `range` is a slice.** Each pattern below builds an index column, then
   gathers element by element:
   - drop-first and drop-last (sessionize);
   - windows (trigram_similarity; moving_average through `slices`);
   - lag via `max(i, 1) - 1` (run_length_encode, normalize_whitespace);
   - pop (balanced_brackets);
   - the first four (soundex);
   - Levenshtein's two shifted views of the previous row.

   Rewrites:
   - When the positions are a per-row affine range, run a block copy per row.
   - An affine map over a range folds into the range.
   - Copy list rows (`get k` of split pieces in query_param) as blocks too.

   At stake:
   - levenshtein: Gather 246 + Range 155;
   - moving_average: Gather 132 + Range 72 (82%);
   - trigram_similarity: Gather 120 + Range 97;
   - query_param: Gather 56 (35%);
   - sessionize: Gather 60 + Range 27.
6. **Appending one element is a bounds shift and a block copy.** `(0 enlist, xs) append`,
   `(xs, x enlist) append` and constant padding all go through `Append`. For lists of numbers it
   copies each row's two runs (#87); a bounds shift would copy nothing.
   `(0 enlist, xs scan_add) append` is an exclusive scan.
   - At stake: levenshtein Append 102, base64_encode 68, trigram_similarity 54, sessionize 20,
     moving_average_prefix 14, kadane_prefix 13.
   - Cheap shifts are also what make the fold-to-scan rewrites in 8 pay: kadane_prefix's shift
     costs 68 of its 108 (Append 13, Iota 20, Gather 35).
7. **Checks that cannot fail, and the plumbing they bring.** Done the same way as 2: the corpus's
   checks that could not fail (every `zip` here pairs maps of one list, or a list with `len x iota`;
   `get k` reads rows of `chunk n` with k < n; positions come from `range(a, len xs - k)`) were the
   checked ops plus a closing `try match` to remove a `Fail` that could not happen. They are now the
   plain ops, with no `try match`. Where a miss can happen and its default is not zero (soundex's
   empty word, query_param's missing key), the program says `try_get`.

### Loops

The lockstep `fold` and `foldscan` are the largest single cost in the corpus. Their own time
(machinery, not body):
- levenshtein 618 (48%), jaro_winkler_direct 444 (32%);
- gcd 153 (47%), run_length_encode_scan 77 (57%), interval_merge 74 (22%), linear_regression 58
  (50%), balanced_brackets 33 (7%; its `branch` and the merge after it are 58%), kadane 29 (76%),
  horner 26 (60%).

8. **Folds whose state is a monoid or an affine map become reductions and scans.**
   - A fold that updates each field independently, `acc.k ⊕ f_k(x)`, is a `map` plus one reduction
     per field: linear_regression's four sums. Float sums run in row order, so this is exact:
     linear_regression_sums, by hand, is 7.3x Rust against the fold's 13x.
   - A running max carried in a foldscan is an exclusive `scan_max` (interval_merge).
   - `acc * c + x` composes as affine maps and so is associative (ipv4_parse's `acc*256 + v`,
     horner).
9. **Loop-invariant state and per-row constants.**
   - A field the body hands back unchanged is still part of every round's state: Levenshtein
     carries `s2`, JW carries the window `d`. It passes through each round as it is now, and is
     copied once, when its row finishes.
   - A per-row constant that a body needs is copied to every element by `cap_list`: horner's x,
     luhn's n, two_sum's t, top_k's n, mode's top, normalize_whitespace's total.
   - Rewrites: keep invariant fields out of the round state and read them by row; give binary ops a
     per-row scalar operand, so `(s, xs) cap_list map ((s, x) -> f(s, x))` needs no copy of s.
10. **A fold that ignores its element is a counted loop, and a row whose state stops changing can
    retire.**
    - gcd folds over `93 iota`, which is 48 MB built only to count rounds (Iota 52, 16%).
    - Every row runs all 93 rounds, though they finish in about 20.
11. **A list captured into every element is a lookup, not data to copy.**
    - substring_count captures the text into every start position: CapList 753 (71%).
    - jaro_winkler_direct captures its state list per byte: CapList 602 (44%).
    - When the body only reads from the captured list (`get`, `gather`, `len`), capture a
      reference instead. `ref` already does this when written by hand, so this is the automatic
      closure-capture pass. Or decorrelate: move the body's reads to one gather at the outer level.
    - Measured by hand: substring_count_ref captures the text with `ref` and runs 369 ns/row
      against substring_count's 1023; soundex_ref captures its table with `ref` and runs 168
      against 192. What remains of the capture is a reference per element (substring_count_ref's
      CapList is still 103 ns/row), and a literal is still filled once per row before it can be
      referenced.

The machinery itself is a runtime matter rather than a rewrite. Done: rows run in order of length,
longest first, so the running rows are a prefix of the state and no round gathers or scatters it
(kadane 0.37×, levenshtein 0.45×, balanced_brackets 0.50× of before, #83).

### Sorting, grouping and searching

12. **Grouping by a key that is already in order is a split.** `run = marks scan_add; (run, x) zip
    group` sorts a non-decreasing key. The groups are the stretches between marks: new bounds, no
    sort, no copy.
    - At stake: run_length_encode SortBy 76 (37%), interval_merge 169 (49%).
    - `adjacent` (where a run of equal elements starts) and `cut` (split a row at marks, moving no
      values) say this directly. Written with them: run_length_encode_cut 42 ns/row against
      run_length_encode's 188 (0.9× Rust), interval_merge_cut 248 against 343. mode_cut no longer
      gains over mode (342 against 335).
    - `group` as a word over `sort_by` costs what the kernel does or less (above). Spelled by hand
      as a structural sort, `adjacent` and `cut`, it was slower (group_aggregate 352 → 500): that
      sorts whole pairs, not keys, and compares the keys again where the sort already numbered
      its runs.
13. **Read only part of the output, compute only that part (projection pushdown into kernels).**
    - `find` read only as `hi - lo` or `hi > lo` is a count or a membership test. Against sorted
      needles it is one merge. Applies to jaccard_sets, two_sum, histogram_sorted, and
      trigram_similarity (Find 452, 36%).
    - `group` whose value lists are only `len`'d, or summed when they are ones, is a count per key:
      mode SortBy 213 (61%), word_topk SortBy 465 (71%). Counted by `scatter_add`, mode_scatter
      takes 146 against mode's 325; word_topk_scatter 615 against 641, its sort of the counted words
      remaining. Any `group` whose values only meet monoid folds is a keyed reduction: interval_merge
      (`fold_min`, `fold_max`) 293 against 337 written with them.
    - A foldscan output field that copies the input element is projected back out (interval_merge).
    - query_param extracts every pair's value though only the first match is used.
14. **Sort, then take k, is top-k.** top_k spends 168 of 188 in SortBy to read three values;
    word_topk sorts all words for the top three. `sort_limit k` now says it directly: on one long
    row of integer pairs it takes 11 ms against 20 for sorting everything (top_pairs), and it took
    ClickBench q24–q26 from 18–26× DuckDB to 4×.
15. **Tuples of narrow leaves sort as one packed leaf.** trigram_similarity's `dedup` and `find` on
    three-byte grams `(Int, Int, Int)` take 863 (68%) as structured comparisons. Packed into one
    integer (order preserved by big-endian concatenation) they are a radix sort and a leaf search.
16. **Counting over a small domain is a bincount.**
    - histogram, `(xs, 8 iota) cap_list map ((xs, k) -> xs map (x == k) fold_add)`, copies the list
      8 times: CapList 386 (80%).
    - group_aggregate's keys are below 8, so a counting sort would serve. `scatter`'s table over a
      narrow range is that count: group_aggregate_scatter takes 162 against 351, 0.65× Rust.

### Not a rewrite

Long pointwise chains still make one pass per op: luhn 13, days_from_civil 16, sessionize 7
materialized columns, against one fused Rust loop each. This is the tiling / destination-passing
item in NOTES.md, an execution strategy rather than a graph rewrite.

## Language friction met while writing

- **Defaults and empty values:**
  - A default other than zero costs a check and a branch: `try_get match (0 (v -> v), 1 (_ -> d))`.
  - A typed empty list for a fallback arm is spelled `0 iota map (z -> …)`.
- **Missing list words:** lag or shift, exclusive scan, take, drop, slice, split-where-mask,
  pairwise, descending sort or top-k, and tuple to list.
- **`eq` on lists was thought missing.** The comments described `eq` as comparing leaves, though it
  compares any shape structurally. query_param first compared `hash`es for that reason.
- **Arithmetic:**
  - No shift by a variable amount. (Float reductions and scans now exist: linear_regression_sums.)
  - When integers were unsigned, `n - 1` wrapped, so the programs spell it `((n, 1) max, 1) sub`.
    Ints are signed now: `n - 1` is -1, and `iota` or `range` to it is empty.
- **Closed bodies:**
  - Match arms are closed too.
  - A per-row constant inside a body must be carried in the accumulator or copied per element with
    `cap_list`.
  - Without functions, trigram_similarity's per-string pipeline is written twice.
- **No loop until done**, only a fold over a fixed count.
