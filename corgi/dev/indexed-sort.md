# The indexed discrimination sort

`corgi/src/ops/cmp/sort.rs`. The sort takes the rows to order as an index list and hands back
the sorted data; nothing is gathered on the way down. The earlier form, its contract and the
copies it made per level are in this file's history (commit 9c32786).

## Contract

    sort_indexed(v, labels: &mut [u64], index: &mut [usize], emit: bool, scratch) -> (perm, Option<Value>)

Requires `labels.len() == index.len()`, `labels` non-decreasing (position `k` is row `index[k]`
in block `labels[k]`; checked in debug builds), and every `index[k]` a row of `v`. Ensures:

- `index` is permuted so that each block holds its rows in structural order; blocks keep their
  places, equal rows keep their order.
- `labels` is the dense run index of the refined partition, in that order: two positions share a
  label iff they did before and their rows are structurally equal.
- `perm` satisfies `new_index[k] == old_index[perm[k]]`, so a caller can move a parallel array
  the same way.
- With `emit`, the returned column is `gather(v, &new_index)`, produced by the sort.

Structural order: leaf by stored unsigned bytes; `Prod` lexicographic by field; `Sum` by tag
then payload; `List` length first, then element by element; `Unit` all equal.

`sort_blocks(labels, v) -> (perm, labels)` is the same at the identity index (`perm[k]` the input
row at output position `k`); `sort_values(labels, v)` adds the sorted column. `arrange` exposes
`sort_perm`, `sort_blocks`, `sort_values` and `sort_indexed`.

## Design

- **Leaf.** Each key is read once through the index into a `(key, position)` buffer; the radix
  runs per block with every pass sequential; the sorted keys, narrowed to the leaf's width, are
  the output column.
- **Prod.** Each field in turn at the same positions, under the labels the previous field
  refined. A field's emitted column is final: later fields permute only within its classes, on
  which it is constant. Once no two positions are tied, the remaining fields are read out by
  the index.
- **Sum.** The tag as a virtual leaf, then each lane at the positions that carry its tag and are
  still tied, through the carried within-lane offsets. A lane whose rows were all sorted is
  emitted by the sort; otherwise it is read once at its final offsets.
- **List.** The length as a virtual leaf, then one refining pass per element position over the
  rows still tied and still that long, then one gather of the elements in final order, the only
  gather in the file. Byte records up to 8 wide pack into one `u64` key.
- **Subsets.** The `Sum` and `List` arms sort only tied positions; a row drops out as soon as its
  block has split down to itself. While subsets refine, a label is the position its run starts
  at, unique per class over the whole problem, so a class one sub-call splits cannot collide
  with a class another sub-call left alone. One pass makes labels dense at the end.
- **Consumers.** `SortList`, `DedupList` and `GroupKey` take their output from the sort. A shape
  holding a `List` has a sorted form that is itself a gather of every element, so `dedup` and
  `group` gather their kept rows from the source there (`cmp.rs::representatives`).

## Measurements (2026-09-06, M4, ns per row)

Gap rows at 1M, master → here, Rust in parentheses: D1 sort_u64 24.1 → 11.9 (9.8); R1
arrange_sort_perm 24.3 → 10.8 (21.8); R7 Sum in one block 27.3 → 27.3 (36.7); R8 List in one
block 43.1 → 42.8 (89.6); R9 Sum under a block per four rows 37.0 → 12.6 (2.5); R10 List
likewise 72.0 → 13.4 (3.9). At 8M: D1 48.3 → 28.2 (11.0), R1 44.5 → 26.4 (31.9).

A probe over `Prod([key, value])` at 1M rows, the key unique or with some rows in pairs, the
value a `Sum` of four `u64` lanes, a ragged `List<u64>`, or `List<Sum<Prod<u64,u64> | List<u64>>>`:

| key | master, Sum / List / deep | here |
|---|---|---|
| unique | 65.6 / 74.9 / 94.0 | 20.2 / 20.0 / 20.4 |
| 1% in pairs | 63.0 / 76.6 / 92.4 | 26.0 / 29.4 / 29.2 |
| 10% in pairs | 66.7 / 80.4 / 101.0 | 25.7 / 29.6 / 31.3 |
| all in pairs | 75.5 / 97.4 / 165.9 | 30.9 / 33.3 / 37.2 |

Also: dedup over 1M lists with 50 distinct 40.5 → 40.7, over all-distinct lists 58.8 → 44.0; a
deep nested sort of 200k rows 129.7 → 86.5; a 64-row `Sum` sorted twenty thousand times 32.1 →
34.2, the fixed cost of a call. DDIR at medium scale, one worker, initial epoch: list_val 84.7 →
74.5 ms, sum_key 103.1 → 97.0, sum_val 71.0 → 67.2, unnest 254.8 → 238.4, scc 1.71 → 1.69 s;
churn unchanged; its test suite passes against this crate unchanged.

## Follow-ups

1. A values-only leaf mode: `sort` and `dedup` on a bare leaf carry a permutation they never
   use; the value radix on `corgi-opportunities` is 2.5x faster there (4.9 ns at 1M).
2. Range copies for the `List` arm's final gather, corgi's `extend_from_self`; `gather` on a
   `List` is element-wise today (`engine.rs:119-130`).
3. Dedup as you go: one position per class per level, expanding from the runs at the end.
4. A caller-held scratch, for tiny calls in a loop.
5. DDIR taking `sort_values` in `from_columns` and `sort_indexed` for the reduce's candidates.
