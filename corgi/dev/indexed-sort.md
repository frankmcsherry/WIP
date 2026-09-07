# The indexed sort: what `sort_blocks` is today, and what replaces it

Branch `corgi-indexed-sort`, off `master` 8b7d878. Line 2 of the corgi work: a discrimination
sort that takes `(labels, index)` and returns sorted data, in the shape of datatoad's
`FactColumn::sort(lists, groups, indexs, last)`. This file is the map of the ground it starts
from, written because the module's own documentation does not state its contract.

## 1. The entry point, and what the other `pub fn`s are

`corgi/src/ops/cmp/order.rs`, `mod discriminate` (`:355-623`). The ONE entry point is

    pub fn sort_blocks(labels: &[u64], v: &Value) -> (Vec<usize>, Vec<u64>)      // :398

Everything else in the module is one of three things:

| function | line | role |
|---|---|---|
| `find_blocks` | :379 | private: splits `labels` into runs of equal adjacent values |
| `sort_leaf_blocks` / `sort_prod_blocks` / `sort_sum_blocks` / `sort_list_blocks` | :467 / :506 / :523 / :561 | private: the four arms `sort_blocks` dispatches to |
| `sort_perm` | :413 | `#[cfg(test)]`: `sort_blocks` with all-zero labels |
| `segment_labels` | :420 | NOT the sort: builds the seed labels for a per-row sort from a `List`'s bounds |
| `run_layout` | :436 | NOT the sort: reads the runs (`ends`, `firsts`) back off a labels vector |
| `runs_per_row` | :457 | NOT the sort: counts run starts per outer row, for `dedup`/`group` bounds |

The three label utilities are `pub` because `cmp.rs:11` imports them and `lib.rs::arrange` re-exports
two of them; `pub(crate) use discriminate::*` at `:10` makes all of it crate-visible.

Callers of `sort_blocks`: `cmp.rs:89` (SortList), `:96` (DedupList), `:109` (GroupKey);
`lib.rs:125` (`arrange::sort_perm`) and `:157` (`arrange::sort_blocks`, used by DDIR's reduce at
`interactive/src/corgi/reduce.rs:498` and `:534`); and the recursion at `order.rs:516, :528, :550, :586, :610`.

## 2. The contract `sort_blocks` actually has

Inputs: a column `v` of any shape, and `labels`, one `u64` per row of `v` **in `v`'s stored order**.
There is no index argument; the rows in play are always all of `v`, in the order `v` stores them.

Preconditions:

1. `labels.len() == v.len()` — `debug_assert` only (`:399`).
2. `labels` is non-decreasing — **unchecked**. Blocks are recovered as runs of equal *adjacent*
   labels (`find_blocks :386-391`), so a non-monotone vector is silently split into extra blocks
   and rows that should share a block are sorted apart. `[0, 1, 0]` is three blocks.
3. Label values carry no meaning beyond equality of neighbours. They need not be dense or start at 0.

Because of (2), the rows of a block must already be physically adjacent, in block order, before the
call. The seed callers satisfy this by construction, not by copying: `segment_labels` gives each
element its row id, and elements of one row are contiguous in storage; a whole-column sort seeds
`vec![0; n]`. **The copies happen below the top level**: every arm gathers the sub-column it is about
to sort into block order before recursing (section 3). So corgi does not pre-permute the values it is
handed; it pre-permutes at every level under that.

Outputs, for `n = v.len()`:

- `perm`: a permutation of `0..n`; `perm[k]` is the INPUT row at OUTPUT position `k`. Block-stable:
  for each input block `[lo, hi)`, `perm[lo..hi]` is a permutation of `lo..hi`. Within a block the
  rows are in corgi structural order, and ties keep input order (the leaf radix is LSD and the tiny-block
  insertion sort swaps only on strict `>`, `value.rs:523-531`).
- `new_labels`: length `n`, aligned with OUTPUT positions (`new_labels[k]` belongs to row `perm[k]`),
  non-decreasing, and refining: `new_labels[j] == new_labels[k]` iff `labels[perm[j]] == labels[perm[k]]`
  and rows `perm[j]`, `perm[k]` are structurally equal. Dense from 0 in every arm except the two
  degenerate ones, `Unit` (`:406`) and a field-less `Prod` (`:509`), which return `labels` unchanged.
  The dense id is the run index, which is why `run_layout(new_labels)` reads `ends`/`firsts` off it.

Structural order: leaf by stored unsigned bytes; `Prod` lexicographic by field; `Sum` by tag then
payload; `List` length-first, then element by element; `Unit` all equal. `compare_at`
(`order.rs:15`) is the scalar oracle the tests check against.

## 3. Where the copies are

Per call of `sort_blocks` on `n` rows, master allocates and moves:

| where | what | lines |
|---|---|---|
| leaf | `perm`, `ends`, radix scratch `tmp`, `new_labels`, and the adjacent-compare vector `adj` | `:474-476, :485, :487` |
| Prod, per field | `gather(c, &perm)` — the whole field copied into block order — then a composed `perm` | `:515, :517` |
| Sum, per tag block | `lane_pos` (the carried offsets), `gather(&variants[t], &lane_pos)`, a `seed` label vector, and the recursive call's own allocations | `:547-550` |
| List | the `lengths` column materialized as a `u64` leaf; then per length block, per position, a `positions` vector and `gather(vals, &positions)` | `:584-586, :607-609` |
| consumers | the output is reformed by a gather through `perm`: `cmp.rs:90` (SortList), `interactive/src/corgi/chunk.rs:432` (the arrangement's `kv_s`); the reduce gathers its candidate subset out before calling (`reduce.rs:497`) | |

The leaf radix on master reads `v[idx[k]]` twice per element per byte pass (`value.rs:516-577`).

## 4. Where the unmerged sort and comparison work lives

All branches in `frankmcsherry/WIP`, all pushed. Three of them are overlapping copies of one line of
work, re-committed under different hashes, which is why it is hard to see what exists.

| branch | base | ahead | what it holds | state |
|---|---|---|---|---|
| `master` | — | — | the sort described above | 8b7d878 = de0f2ac + #21 + #23 |
| `corgi-opportunities` | de0f2ac | 23 | **PR #19**: the value radix that emits sorted data (d3e58bc: `sort` 24.3 → 4.9 ns/row @1M), the key-carrying permutation radix (62e8d62: `sort_perm` 23.1 → 13.9), the tie early-out `fully_discriminated` (5c79108), sortedness detection for sort/dedup/group (601b2bc), plus immediates, folds of monoids, byte masks | OPEN |
| `corgi-scalar-compare` | de0f2ac | 2 | **PR #20**: `compare_at` as a direct walk of the type instead of a batch of one, and a gaps row for it | OPEN |
| `corgi-implicit-narrowing` | de0f2ac | 26 | #19 and #20 re-applied (same messages, new hashes), then `find` merging a sorted needle (30654f0), `order.rs` split into `compare`/`discriminate`/`merge`/`sortedness` modules, byte masks, and the implicit-compare narrowing it is named for | no PR; the main worktree sat on it until 2026-09-06 |
| `corgi-sort-bfs` | 8b7d878 | 2 | the Sum and List arms refined per level instead of per block, one-row blocks skipped, gap rows R7–R10 | no PR; worktree `WIP-sort` |
| `corgi-survey-lanes` | 8b7d878 | 1 | **PR #22**: `survey` compares products of `u64` leaves lane by lane inside the gallop | OPEN, held back on purpose |
| `corgi-comparisons-survey` | 8b7d878 | 1 | `corgi/dev/comparisons.md`, the survey of every comparison path | no PR; worktree `WIP-docs` |
| `corgi-survey-groups` | 2221578 | 2 | `survey_groups`, matched classes as ranges on both sides | stale base |

None of the three de0f2ac-based branches applies onto master cleanly with the others, because
`corgi-implicit-narrowing` restructured `order.rs`. This branch takes master as its base and re-derives
what it needs from the leaf work in #19 (the value radix and the key-carrying radix are the two pieces
worth porting by body); it does not merge any of the above.

## 5. The design this branch collects

Datatoad's shape (`datatoad/src/facts/trie.rs:898`, `col_sort :1184`, `u32_sort :1212`): positions
`p` with `groups[p]` and `indexs[p]`, sorted by `(group, value(indexs[p]))` with `p` as payload; the
sorted values come out as a new layer; `groups` is rewritten in place with each position's class;
`last` skips the rewrite. The subset is an index list, so nothing is gathered before the sort.

Proposed corgi kernel, keeping the monotone invariant but on the INDEX, not on the data:

    sort_indexed(v: &Value, labels: &mut Vec<u64>, index: &mut Vec<usize>,
                 emit: Emit, scratch: &mut SortScratch) -> Option<Value>

`index[k]` is the row of `v` at position `k`; `labels[k]` its block, non-decreasing in `k`. On return
`index` is in sorted order, `labels` is the refined partition in that order, rewritten in place, and
with `Emit::Values` the sorted rows come back as a column built by the radix itself. The permuted index
is the radix's payload column, so keeping labels monotone costs nothing and lets the leaf run per block
over value digits only, which is the regime under the hash-prefixed key (blocks of a few rows).

Arms: leaf packs `(key(v[index[k]]), index[k])` once, radixes per block (or packed with the label when
it fits 64 bits and blocks are large), emits the key column as the output. Prod sorts each field with the
same index and the evolving labels; each field's output is final when emitted, since later fields only
permute within its classes. Sum sorts the tag as a virtual leaf, then one call per lane at the carried
offsets. List sorts the length, then one refine-only call per element position, and forms the output by
one gather of the elements, the one place a permutation legitimately survives (collie's note,
`collie/dev/BACKLOG.md:92-110`). Multiplicity is kept; distinctness is a mode.

## 6. Status (2026-09-06, second commit)

Built, on this branch, as `corgi/src/ops/cmp/sort.rs`:

    sort_indexed(v, labels: &mut [u64], index: &mut [usize], emit: bool, scratch) -> (perm, Option<Value>)
    sort_blocks(labels, v) -> (perm, labels)                     // the old form, at the identity index
    sort_values(labels, v) -> (perm, labels, sorted Value)       // the old form, with the data

`order.rs`'s `mod discriminate` is gone; what remains of it is `mod labels` (`segment_labels`,
`run_layout`, `runs_per_row`), which sorts nothing. `SortList`, `DedupList` and `GroupKey` take the
data from the sort (`cmp.rs`); `arrange` exposes `sort_values` and `sort_indexed` for DDIR. The
leaf reads each key once through the index into a packed `(key, position)` buffer and every radix
pass after that is sequential (`sort_block`); the sorted keys are the output column. The arms
gather nothing: `Prod` re-sorts each field at the same positions, `Sum` sorts the tag as a virtual
leaf then each lane at its carried offsets, `List` sorts the length then refines position by
position over the rows still long enough, gathering the elements once at the end. Contract
checked by `sort.rs`'s tests against the scalar comparator on random nested shapes, with the
identity index and with scrambled subsets.

Gap rows, this branch against master, ns per row (the R7–R10 rows are the ones from
`corgi-sort-bfs`, added here):

| row | n | master | this branch | Rust |
|---|---|---|---|---|
| D1 sort_u64 | 1M | 24.1 | 12.4 | 10.0 |
| D1 sort_u64 | 8M | 48.3 | 31.0 | 11.7 |
| D2 dedup | 1M | 28.7 | 16.4 | 10.4 |
| R1 arrange_sort_perm | 1M | 24.3 | 11.4 | 24.2 |
| R1 arrange_sort_perm | 8M | 44.5 | 28.7 | 32.8 |
| R7 sort_sum, one block | 1M / 8M | 27.3 / 34.5 | 27.5 / 36.0 | 37.9 / 47.1 |
| R8 sort_list, one block | 1M / 8M | 43.1 / 58.5 | 43.4 / 62.5 | 94.7 / 244.8 |
| R9 sort_sum, a block per row | 1M / 8M | 37.0 / 38.6 | 16.4 / 21.6 | 2.4 / 3.1 |
| R10 sort_list, a block per row | 1M / 8M | 72.0 / 74.5 | 18.0 / 20.4 | 4.0 / 4.5 |

So the per-row-label regime (`corgi-sort-bfs`'s 2–2.6x) is 2.3x on Sum and 4x on List here, and
the one-big-block regime that branch lost 15–25% on is flat. The leaf carries its permutation
through every pass, which is what a value-only sort (D1) does not need: `corgi-opportunities`'
value radix reaches 4.9 ns at 1M by not carrying it, so a values-only mode of `sort_block` is the
next leaf step. After that: DDIR's `from_columns` taking `sort_values` instead of `sort_perm` plus
a gather, the reduce taking `sort_indexed` on its candidate subset instead of gathering it out,
and collie's packed `(label, key)` single radix for the few-big-blocks case.
