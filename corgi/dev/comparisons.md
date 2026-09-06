# Comparisons in corgi: rank at a time, or not

A survey of every place corgi (and DDIR's corgi backend) compares rows — sorting,
finding, equating, surveying, and the scalar escape hatch — asking of each: is it
rank at a time, where does it first depart, what stands between it and being
wholly rank at a time, and what it should be judged against.

**Rank at a time** here means: one pass per level of the type structure, over every
row or pair alive at that level, with the control information (which rows are still
tied, where they sit in the lane) carried as index lists and labels between levels.
Its opposite, **row at a time**, is a recursion through the shape for each row or
pair separately. The difference is not asymptotic; it is who pays for the shape walk
and the allocations — once per level, or once per row.

## The kernel: `compare_pairs`

Every batched comparison bottoms out in `ops::cmp::order::compare_pairs`, which
takes a set of `(i, j)` row pairs — explicit lists, or the implicit `Diagonal`
(`i` vs `i`) and `Adjacent` (`k` vs `k+1`) forms — and returns a sign per pair.

| shape | what it does | rank at a time? |
|---|---|---|
| leaf | one width-dispatched pass over all pairs (`cmp_idx`); the implicit forms read both sides densely and vectorize | yes |
| Prod | field 0 over all pairs, then each later field over the surviving ties only, compacting the tie set in place | yes |
| Sum | tags over all pairs, then per tag one payload pass at the rows' carried within-variant offsets | yes |
| List | lengths over all pairs, then one element-level recursion over the concatenated element pairs of every equal-length pair, then the first nonzero sign per segment | yes, with a caveat |
| Unit | all equal | — |

It is the one piece that is rank at a time by construction, and the model for the
rest. Its two caveats:

- **Allocation per level per call.** The tie vectors (`tie_k`, `tia`, `tib`), the
  per-variant buckets (`by_tag`, one `Vec` per variant), and the element lists
  (`sia`, `sib`) are fresh on every call. For a large batch that is noise; for a
  batch of one it is the whole cost, which is what made `compare_at` a heap
  allocation per comparison (PR #20 replaces that one caller with a walk). Anything
  that calls the kernel in a loop of small batches — a binary search round, a
  per-block sort — pays it per call.
- **Lists expand before they compare.** Equal-length pairs contribute every element
  pair to one level-wise batch, so the work is the sum of the lengths, not the sum
  of the compared prefixes. A position-by-position refinement over the surviving
  ties (as `sort_list_blocks` does) would early-out; the level-wise form cannot.

## Sorting: `sort_blocks`

Discrimination sort: partition refinement, most-significant level first, with the
"digits" being the type structure rather than bytes.

| arm | mechanism | departs from rank at a time at |
|---|---|---|
| leaf | stable byte radix per block in place, one shared scratch, then ONE adjacent-compare pass for labels | nowhere; this is the arm that was fixed after per-block allocations showed as 17% of a join profile |
| Prod | field loop: `gather` the field by the running permutation, `sort_blocks` it under the running labels | it materializes a reordered copy of each field (gather) instead of pushing the permutation down; linear, but data movement per field |
| Sum | tag radix (a u8 leaf), then **per tag block** a gather of that block's lane rows and a recursive sort | the per-block recursion: one `lane_pos`, one gather, one seed vector, one sub-sort per block, so a column of many small tag blocks is allocation-bound |
| List | length radix (skipped for strided lists; byte lists up to 8 wide pack into a u64 leaf), then **per length block, per position** a gather and a recursive sort | the same per-block loop, now nested in positions: a column of many short lists is the worst case |

The line of work for sort is the one already applied to the leaf: hoist the
per-block loops into per-level passes. Sum blocks of one tag are one segmented
refinement; List blocks of one length are one segmented refinement per position,
with `labels` already being the segmentation the kernel understands. No new
algorithm, only the loop order.

## Finding: `find_ranges` and `Find`

Two paths.

- **`u64` needles into a `u64` haystack**: a `partition_point` per needle. Row at a
  time by needle, but each step is one leaf compare, and the `R3` gap row puts it at
  the Rust ceiling. Galloping from the previous hit was measured as a loss (sparse
  needles prefer a predictable bisection). This is what DDIR's reduce and join probe
  with, always with sorted needles.
- **Everything else**: `Find` runs `batched_bound`, a lockstep binary search —
  every live needle's window halves per round, one `compare_idx` per round over the
  (mid, needle) pairs, the live set compacted as windows collapse. Rank at a time
  across needles, `log(span)` rounds. This is the "parallel binary search" that the
  LIST descent fought: each round is a fresh call into the kernel, so each round
  pays the kernel's per-level allocations, and for lists each round expands every
  live pair's elements — over haystack rows that are not contiguous, so the element
  lanes are gathers, never strides.

The line of work for find is to stop searching when the needles are sorted, which
in every DDIR call site they are: a merge-based find is `survey(needles, haystack)`
read as ranges. Branch `corgi-implicit-narrowing` already carries a commit in that
direction ("find merges when the needle is sorted, instead of searching per
probe"). That folds find into survey, and survey's completion (below) then covers
both.

## Equating: `compare_cols`, `compare_adjacent`, `group_bounds`

The diagonal and adjacent forms of the kernel: `Rel`'s lane compare, the run
boundaries of a sorted column, and `group_bounds` as the cumulative ends of those
runs. Rank at a time, and the leaf forms vectorize. They inherit the kernel's
caveats and nothing else. `Rel`, `Min`, `Max` themselves are leaf-only lane ops;
structural equality reaches `compare_cols`.

## Surveying: `survey`

The merge kernel: the interleaving of two sorted columns as maximal exclusive
ranges and matched pairs, by a bidirectional gallop. It is a **sequential** walk
whose per-step cost is one comparison, so the question is only what that
comparison costs:

- bare primitive columns: a direct read (the `R4` row: a quarter of a Rust
  two-pointer's time at 1M, because the gallop skips runs);
- products of `u64` lanes, nested and unit-padded (PR #22): lane by lane, direct;
- anything else: `compare_at` per step — on master a batch of one, so this is the
  arm where a survey costs *more* than a row loop.

Where it departs: `Both` names a single matched pair, not the two equal groups, so
a caller that must consolidate groups (the chunk merge) rediscovers them; and the
structured tail of a `(hash, key)` column is compared one pair at a time on every
tie, which in a merge means on every updated key.

The line of work is the general ADT survey: report matched **group ranges**
(`survey_groups`), and refine ties breadth-first — all tied groups of one level in
one segmented pass, reusing the kernel's tie compaction — rather than recursing per
group. Prod refines by field, Sum by tag then variant lane at the carried offsets,
List by length then position, the same three shapes the sort already descends.
Lists are not the obstacle under length-first order; the obstacle is per-group
depth-first recursion, which is the LIST descent's failure mode in `Find` seen from
the other side.

## The scalar hatch: `compare_at`

One pair, structurally. On master it is `compare_idx` with one pair, and every
loop that calls it pays an allocation per comparison. PR #20 makes it a direct
walk. Its remaining callers after this session's work: the join's hash-collision
checks and `RunRef` value compares on structured values, and the survey fallback
above. Its proper role is the floor under the fallbacks, not a hot path.

## DDIR's corgi backend: where the rows are

| site | today | escape hatch |
|---|---|---|
| chunk `from_columns` | `sort_perm` + one `compare_adjacent` | none |
| chunk `advance` | `group_bounds` once, then a per-row memo lookup on the *time* column (`cmp_cross`) | the memo lookup; times are columnar's, not corgi's, and a per-batch distinct-times pass would replace it |
| chunk `merge` | (branch `survey-merge`) `survey` + group scans on the lanes, row loops only inside matched groups on times | `compare_at` in the group scan for non-lane shapes; the integer fast path on `bench-spike` is the row-at-a-time alternative |
| reduce `collect_present` | `find_ranges` on the identifier lane, needles sliced per chunk | none (u64 lanes) |
| reduce `merge_present` | k-way heap on `(id, value-id)` `u64` pairs | none |
| reduce `ordered_keys` | `compare_adjacent` | none (was a `compare_at` per row) |
| reduce `min` | `sort_blocks` over the candidate column | none |
| join `advance_leaf` | two-pointer over identifier lanes with `partition_point` per run | per-run, `u64` |
| join `RunRef::val_less/eq` | lane compares when values are leaves | `compare_at` per pair otherwise |
| join collision checks | two `compare_at` per matched key, only under a colliding token | rare by design |

## What to judge against

Three row-at-a-time reference points already exist:

1. **corgi's own gap suite**, family `R`: `sort_perm` vs Rust's stable sort with
   cached keys, `compare_adjacent` vs a direct leaf compare, `find` vs
   `partition_point`, `survey` vs a two-pointer, `gather` vs a two-slice gather,
   and (PR #20) `compare_at` vs a `u64` compare. Numbers for this revision are in
   the table below. These are leaf-shaped ceilings; there is no structured-shape
   row, and there should be one per shape (a `Prod`, a `Sum`, a `List` of leaves)
   for sort, find and survey, against a typed-Rust `Ord` on the same data.
2. **DDIR's integer-lane merge fast path** (`bench-spike`) against the survey
   merge (`survey-merge`): at parity at 100k and 1M nodes once the time column
   copies by range, i.e. the batched form costs nothing and buys nothing at those
   scales; the win over both was the copy, not the compare.
3. **DDIR's vec backend**: whole programs on typed rows. corgi is 1.0–2.5x ahead per
   workload at 100k nodes, with the reduce and the arrangement machinery, which
   both backends share, setting the pace.

### The `R` rows at this revision (master + the lane-wise survey), ns per element unless the row says otherwise

| row | n | corgi | Rust | corgi / Rust | what the Rust side is |
|---|---|---|---|---|---|
| R1 arrange_sort_perm | 1,048,576 | 23.097 | 20.852 | 1.108x | stable radix argsort vs stable cached-key Rust sort |
| R2 arrange_compare | 1,048,575 | 0.896 | 0.223 | 4.015x | batched adjacent compare vs direct leaf compare |
| R3 arrange_find | 65,536 | 104.739 | 102.740 | 1.019x | u64 fast path vs the same two partition points |
| R4 arrange_survey | 2,097,152 | 0.064 | 0.279 | 0.228x | 256-row galloping runs vs two-pointer run survey |
| R5 arrange_gather2 | 1,048,576 | 0.636 | 0.485 | 1.312x | two-source column gather vs direct two-slice gather |
| R1 arrange_sort_perm | 8,388,608 | 42.902 | 32.155 | 1.334x | stable radix argsort vs stable cached-key Rust sort |
| R2 arrange_compare | 8,388,607 | 0.876 | 0.259 | 3.380x | batched adjacent compare vs direct leaf compare |
| R3 arrange_find | 524,288 | 742.303 | 767.710 | 0.967x | u64 fast path vs the same two partition points |
| R4 arrange_survey | 16,777,216 | 0.222 | 0.284 | 0.783x | 256-row galloping runs vs two-pointer run survey |
| R5 arrange_gather2 | 8,388,608 | 0.961 | 1.213 | 0.792x | two-source column gather vs direct two-slice gather |

Below 1x corgi is faster. `R3`'s numbers are per probe over the whole search, not per element.

Datatoad's experience is the caution: the rank-at-a-time forms were slower first
and became faster once control storage was reused across calls and levels. The
kernel's per-call allocations are exactly that debt here.

## A line of work

In order, each measured on the `R` rows and the DDIR workloads:

1. **Kernel hygiene.** Scratch reuse for the tie, bucket and element vectors in
   `compare_pairs`; `compare_at` as a walk (#20) so the fallbacks have a floor.
2. **Sort's per-block loops become per-level passes** in the Sum and List arms, as
   the leaf arm already did. The list-heavy DDIR shapes (`collect`, `unnest`,
   `ast`'s lists) are the workloads.
3. **Find by survey when needles are sorted**, retiring `batched_bound` from the
   sorted paths.
4. **`survey_groups` and breadth-first tie refinement**: the general ADT survey.
   Flat prefixes stay on the lane path (#22); structured tails refine per level
   over all ties at once. The chunk merge and the join's run alignment both
   consume it.
5. **Structured-shape rows in the gap suite**, so 1–4 are judged against typed
   Rust and not against each other.
6. **A wholly rank-at-a-time merge as the control**: merge as a discrimination
   sort of the union with a side label. Shape-general, no gallop; it says what
   generality costs when the interleaving is fine, which is the case the gallop
   cannot win.

