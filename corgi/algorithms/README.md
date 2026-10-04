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

All times are in ns per row, at 65,536 rows on an M4 mini, measured 2026-10-03: the best of three
processes per program, since one process can run 20% slower than the next. The last column is
corgi's time divided by Rust's. `sort`, `dedup` and `group` became words over `sort_by` after most
of these rows were measured; the rows that moved by more than 5% with that change are measured
again (group_aggregate, interval_merge_cut, mode, mode_cut, word_topk, and the sort_pairs and
top_pairs programs).

| program | computes | corgi | Rust | × |
|---|---|---|---|---|
| argsort_pairs | positions of (a, b) pairs in sorted order, by `sort_by` | 380 | 228 | 1.7 |
| balanced_brackets | `()[]{}` balanced, by a fold with a stack | 1109 | 58.5 | 19.0 |
| balanced_brackets_levels | the same from scans and a sort, no stack | 397 | 57.9 | 6.9 |
| base64_encode | base64 with `=` padding | 618 | 26.7 | 23.1 |
| base64_encode_arith | the same without the alphabet table or appends | 370 | 27.0 | 13.7 |
| days_from_civil | days since 1970 and weekday (Hinnant) | 5.7 | 2.1 | 2.7 |
| gcd | Euclid, as a fold over a fixed round count | 316 | 32.6 | 9.7 |
| group_aggregate | GROUP BY key: count, sum, max | 370 | 244 | 1.5 |
| histogram | 8-bucket counts, as an outer product | 482 | 27.0 | 17.9 |
| histogram_sorted | the same by sorting the bucket ids | 265 | 27.1 | 9.8 |
| horner | polynomial at x | 57.7 | 5.4 | 10.7 |
| interval_merge | merge overlapping intervals | 349 | 128 | 2.7 |
| interval_merge_cut | the same, merged intervals cut at their marks | 283 | 130 | 2.2 |
| ipv4_parse | dotted quad to `{U64 \| ()}` | 83.2 | 14.0 | 5.9 |
| itoa | a u64's decimal digits | 355 | 22.4 | 15.8 |
| jaccard_sets | \|A∩B\| / \|A∪B\| of two lists as sets | 569 | 548 | 1.0 |
| jaro_winkler_direct | Jaro-Winkler, the textbook loop | 2417 | 96.2 | 25.1 |
| jaro_winkler_by_byte | the same, matched per byte value | 1088 | 96.5 | 11.3 |
| kadane | maximum subarray sum, as a fold | 90.7 | 11.6 | 7.8 |
| kadane_prefix | the same from prefix sums | 133 | 11.7 | 11.3 |
| levenshtein | edit distance, the textbook table | 2800 | 79.6 | 35.2 |
| linear_regression | least-squares slope and intercept | 238 | 8.9 | 26.8 |
| linear_regression_sums | the same from four `fold_add_f64` sums | 73.4 | 9.2 | 8.0 |
| luhn | Luhn check of a digit string | 105 | 6.9 | 15.2 |
| median_percentile | median and 90th percentile | 242 | 76.6 | 3.2 |
| mode | most frequent value | 477 | 114 | 4.2 |
| mode_cut | the same as runs cut from the sorted values | 358 | 114 | 3.1 |
| moving_average | sums of every 4-wide window | 256 | 27.2 | 9.4 |
| moving_average_prefix | the same from prefix sums | 144 | 27.4 | 5.3 |
| normalize_whitespace | lowercase, collapse and trim spaces | 147 | 108 | 1.4 |
| query_param | a key's value in `a=1&b=2` | 253 | 32.3 | 7.8 |
| run_length_encode | runs as (byte, count), gaps and islands | 178 | 43.7 | 4.1 |
| run_length_encode_scan | the same as the loop | 179 | 44.5 | 4.0 |
| run_length_encode_cut | the same, runs marked by `adjacent` and `cut` | 50.7 | 44.3 | 1.1 |
| sessionize | sessions split at gaps over 30 | 184 | 10.9 | 16.9 |
| sort_pairs | (a, b) pairs sorted | 339 | 207 | 1.6 |
| sort_pairs_steps | the same a column at a time, as Datatoad sorts | 515 | 208 | 2.5 |
| soundex | American Soundex code | 381 | 48.6 | 7.8 |
| substring_count | overlapping occurrences of a pattern | 1027 | 64.6 | 15.9 |
| top_k | the three largest values | 179 | 88.5 | 2.0 |
| top_pairs | the first ten (a, b) pairs, sort then take | 380 | 191 | 2.0 |
| top_pairs_limit | the same by `sort_limit` | 395 | 192 | 2.1 |
| top_pairs_steps | the same a column at a time, pruned between the columns | 755 | 191 | 4.0 |
| trigram_similarity | pg_trgm similarity of two strings | 1146 | 266 | 4.3 |
| two_sum | does a pair sum to the target | 353 | 330 | 1.1 |
| word_topk | the three most frequent words | 641 | 298 | 2.2 |

The rows near 1× (jaccard_sets, two_sum, group_aggregate, normalize_whitespace) are measured
against Rust that hashes or allocates. Rust written for the small key domain would be several times
faster, so those ratios flatter corgi.

The optimizer passes that exist (`--optimize`: peephole, iso cancellation, map fusion, CSE) change
almost nothing: gcd is 11% faster, and every other program is within the run-to-run spread (±4%).

### Longer rows

`--scale K` makes the lists of numbers K times longer in K times fewer rows, so a run handles the
same number of values. Ratios to Rust, best of three processes:

| program | today's lists | 32× longer | 1024× longer |
|---|---|---|---|
| group_aggregate | 1.36 | 0.84 | 0.90 |
| histogram | 18 | 44 | 45 |
| histogram_sorted | 9.76 | 9.46 | 8.83 |
| jaccard_sets | 1.04 | 0.85 | 0.77 |
| kadane | 7.82 | 6.24 | 30 |
| kadane_prefix | 11 | 9.67 | 11 |
| linear_regression | 27 | 19 | 66 |
| median_percentile | 3.17 | 1.45 | 1.01 |
| mode | 4.36 | 2.30 | 3.06 |
| moving_average | 9.43 | 43 | 57 |
| moving_average_prefix | 5.27 | 19 | 27 |
| sessionize | 17 | 20 | 20 |
| top_k | 2.02 | 1.36 | 0.95 |
| two_sum | 1.07 | 8.58 | 217 |

At long rows, sort, group and set programs match or beat Rust: radix sort and the dense search pay
off. Loops fall behind. The lockstep fold is parallel only across rows, so a few long rows give it
a few elements per round: kadane is 30× here, while the same algorithm as prefix scans
(kadane_prefix) holds at 11×. two_sum's 217× is early exit: Rust stops at the first pair it finds,
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
| sort_pairs | 346 | 339 | 20.6 | 20.2 | 25.6 |
| top_pairs | 392 | 380 | 21.1 | 20.4 | 2.3 |
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
65,536 rows; one process, so up to 20% above the table's best of three), and a share is that op's
fraction of the program's time. Items are grouped by what they need, and ranked within groups by
how much time they would recover across the corpus. The profiles were taken while `sort`, `dedup`
and `group` were the kernels `SortList`, `DedupList` and `GroupKey`; they are words over `sort_by`
now, at about the same cost (above).

### Exact rewrites, no analysis

1. **A lookup table written inside a body is copied once per element.** In
   `w map (c -> (i, "0123…") get)`, the literal fills to the body's length, so a 26-byte table is
   built for every letter.
   - Rewrite: a gather whose haystack is a literal reads the literal's one row.
   - At stake: soundex `lit` 196 (45%), base64_encode `lit` 370 (53%). base64_encode_arith removes
     the table by hand and is 45% faster.
2. **`get` with a zero default is the raw gather.** `(i, xs) get try match (0 (v -> v), 1 (_ -> 0u64))`
   reads zero out of range, which is exactly what the lossy `Gather` does.
   - The rewrite removes the per-element check and the lane merge at the end.
   - It also removes the failure path: whenever any row misses, the checked gather first copies the
     Ok rows of the whole haystack.
   - At stake: median_percentile's two scalar lookups cost 87 (33%). Also mode, the stack peek in
     balanced_brackets, and itoa. A `get_or d` word is the general form (see friction below).
3. **Run CSE and the peephole in `Program`.**
   - Repeated `len`, compares, `iota`s and constant seeds today run once per occurrence.
   - Measured with `--optimize`: gcd 11% faster, the rest unchanged. Cheap, but small on its own.
4. **Division and remainder by a constant become a multiply-high and a shift.** A power of two
   becomes `shr` or `and`, and `select` with a constant operand becomes an immediate.
   - Today the divide runs the scalar divider, and NEON has no integer divide.
   - At stake: days_from_civil's five constant divisions are 37% of it; histogram (`/125`); itoa
     (20 rounds of `/10`, `%10`); gcd's `select` with 0 (lit 20 plus part of Select 82).

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
   - levenshtein: TryGather 301 + Range 189;
   - moving_average: TryGather 148 + Range 68 (79%);
   - trigram_similarity: TryGather 126 + Range 72;
   - query_param: TryGather 132 (50%);
   - sessionize: TryGather 74 + Range 26.
6. **Appending one element is a bounds shift and a block copy.** `(0 enlist, xs) append`,
   `(xs, x enlist) append` and constant padding all go through `Append`, which builds a tag and an
   offset per element and gathers from two lanes. `(0 enlist, xs scan_add) append` is an exclusive
   scan.
   - At stake: Append is 663 summed over the corpus: levenshtein 196, base64_encode 133,
     trigram_similarity 100, moving_average_prefix 40, sessionize 38, kadane_prefix 37.
   - Cheap shifts are also what make the fold-to-scan rewrites in 8 pay: kadane_prefix's scans and
     reduction cost 30 together, but its shift costs 94 (Append 37, Iota 20, TryGather 37).
7. **Checks that cannot fail, and the plumbing they bring.** Length facts that flow from `iota`,
   `len`, `map`, `chunk`, `split`, `group` and `range` show the check cannot fail in all of these:
   - every `zip` here pairs maps of one list, or a list with `len x iota`;
   - `get k` reads rows of `chunk n` with k < n;
   - `head` of a group's values, or `get 0` of a `split`;
   - positions come from `range(a, len xs - k)`.

   The checked op then runs as its raw kernel. With nothing left to fail, the effect lowering adds
   no `Lift`/`HoistProd`/`Squash`, and the user's closing `try match` is a match on a sum with one
   live lane: the identity.
   - 24 of the 36 programs end in a `try match` written only to remove a `Fail` that cannot happen.
     The lowered corpus has 90 checked ops and 176 plumbing nodes.
   - When nothing fails the plumbing costs almost nothing. The gain is TryGather's check, the closing
     `Unwrap` (query_param 34, ipv4_parse 8), and graphs the other rewrites can see through.
   - This is what optimizing out the Try overhead amounts to.

### Loops

The lockstep `fold` and `foldscan` are the largest single cost in the corpus. Their own time
(machinery, not body) is:
- levenshtein 1564 (53%), jaro_winkler_direct 1164 (47%), balanced_brackets 468 (40%);
- linear_regression 161 (64%), gcd 159 (43%), kadane 140 (90%), run_length_encode_scan 111 (59%);
- interval_merge 94 (26%), horner 46 (73%).

8. **Folds whose state is a monoid or an affine map become reductions and scans.**
   - A fold that updates each field independently, `acc.k ⊕ f_k(x)`, is a `map` plus one reduction
     per field: linear_regression's four sums. With `fold_add_f64` (float sums in row order, so
     bit-identical to the fold) linear_regression_sums runs 241 → 73 ns/row, and on rows 1024×
     longer 692 → 66 µs per row (65× → 6.2× Rust).
   - A running max carried in a foldscan is an exclusive `scan_max` (interval_merge).
   - `acc * c + x` composes as affine maps and so is associative (ipv4_parse's `acc*256 + v`,
     horner).
9. **Loop-invariant state and per-row constants.**
   - A field the body hands back unchanged is still gathered and scattered every round:
     Levenshtein carries `s2`, JW carries the window `d`.
   - A per-row constant that a body needs is copied to every element by `cap_list`: horner's x,
     luhn's n, two_sum's t (twice), top_k's n, mode's top, normalize_whitespace's total.
   - Rewrites: keep invariant fields out of the round state and read them by row; give binary ops a
     per-row scalar operand, so `(s, xs) cap_list map ((s, x) -> f(s, x))` needs no copy of s.
10. **A fold that ignores its element is a counted loop, and a row whose state stops changing can
    retire.**
    - gcd folds over `93 iota`, which is 48 MB built only to count rounds (Iota 48).
    - Every row runs all 93 rounds, though they finish in about 20.
11. **A list captured into every element is a lookup, not data to copy.**
    - substring_count captures the text into every start position: CapList 769 (68%).
    - jaro_winkler_direct captures its state list per byte: CapList 648 (26%).
    - When the body only reads from the captured list (`get`, `gather`, `len`), capture a
      reference instead. `ref` already does this when written by hand, so this is the automatic
      closure-capture pass. Or decorrelate: move the body's reads to one gather at the outer level.
    - Measured by hand: substring_count_ref captures the text with `ref` and runs 1006 → 398
      ns/row; soundex_ref captures its table with `ref` and runs 363 → 252. What remains of the
      capture is a 16-byte span per element (substring_count_ref's CapList is still 104 ns/row),
      and a literal is still filled once per row before it can be referenced.

The machinery itself is a runtime matter rather than a rewrite. Every round gathers the active
rows' state and scatters it back. Running rows in order of length would make the active rows a
prefix, with no gather or scatter.

### Sorting, grouping and searching

12. **Grouping by a key that is already in order is a split.** `run = marks scan_add; (run, x) zip
    group` sorts a non-decreasing key. The groups are the stretches between marks: new bounds, no
    sort, no copy.
    - At stake: run_length_encode GroupKey 96 (53%), interval_merge 96 (26%).
    - `adjacent` (where a run of equal elements starts) and `cut` (split a row at marks, moving no
      values) say this directly. Written with them: run_length_encode 183 → 51 ns/row (1.1× Rust),
      mode 499 → 350, interval_merge 358 → 289.
    - `group` as a word over `sort_by` costs what the kernel does or less (above). Spelled by hand
      as a structural sort, `adjacent` and `cut`, it was slower (group_aggregate 352 → 500): that
      sorts whole pairs, not keys, and compares the keys again where the sort already numbered
      its runs.
13. **Read only part of the output, compute only that part (projection pushdown into kernels).**
    - `find` read only as `hi - lo` or `hi > lo` is a count or a membership test. Against sorted
      needles it is one merge. Applies to jaccard_sets, two_sum, histogram_sorted, and
      trigram_similarity (Find 415, 36%).
    - `group` whose value lists are only `len`'d, or summed when they are ones, is a count per key:
      mode GroupKey 512 (81%), word_topk.
    - A foldscan output field that copies the input element is projected back out (interval_merge).
    - query_param extracts every pair's value though only the first match is used.
14. **Sort, then take k, is top-k.** top_k spends 162 of 183 in SortList to read three values;
    word_topk sorts all words for the top three. `sort_limit k` now says it directly: on one long
    row of integer pairs it takes 11 ms against 20 for sorting everything (top_pairs), and it took
    ClickBench q24–q26 from 18–26× DuckDB to 4×.
15. **Tuples of narrow leaves sort as one packed leaf.** trigram_similarity's `dedup` and `find` on
    `(U8, U8, U8)` grams take 801 (69%) as structured comparisons. Packed into one integer (order
    preserved by big-endian concatenation) they are a radix sort and a leaf search.
16. **Counting over a small domain is a bincount.**
    - histogram, `(xs, 8 iota) cap_list map ((xs, k) -> xs map (x == k) fold_add)`, copies the list
      8 times: CapList 373 (73%).
    - group_aggregate's keys are below 8, so a counting sort would serve.

### Not a rewrite

Long pointwise chains still make one pass per op: luhn 13, days_from_civil 16, sessionize 7
materialized columns, against one fused Rust loop each. This is the tiling / destination-passing
item in NOTES.md, an execution strategy rather than a graph rewrite.

## Language friction met while writing

- **Defaults and empty values:**
  - A default costs four ops: `get try match (0 (v -> v), 1 (_ -> d))`. `get_or d` would be one.
  - A typed empty list for a fallback arm is spelled `0u64 iota map (z -> …)`.
- **Missing list words:** lag or shift, exclusive scan, take, drop, slice, split-where-mask,
  pairwise, descending sort or top-k, and tuple to list.
- **`eq` on lists was thought missing.** The comments described `eq` as comparing leaves, though it
  compares any shape structurally. query_param first compared `hash`es for that reason.
- **Arithmetic:**
  - No unsuffixed `div` (though `rem` exists), `shl`, `or`, or shift by a variable amount. (Float and
    signed reductions and scans were missing too; `fold_add_f64`, `scan_add_i64` and the rest exist
    now.)
  - `n - 1` wraps, so a safe spelling is `((n, 1u64) max, 1u64) sub`. Without it, `range` to 2^64
    allocates that much.
- **Closed bodies:**
  - Match arms are closed too.
  - A per-row constant inside a body must be carried in the accumulator or copied per element with
    `cap_list`.
  - Without functions, trigram_similarity's per-string pipeline is written twice.
- **No loop until done**, only a fold over a fixed count.
- **Lists order shorter-first, then by content**, unlike Rust's slices. word_topk's ties follow
  corgi's order.
