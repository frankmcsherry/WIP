# The indexed discrimination sort

`corgi/src/ops/cmp/sort.rs`. The sort takes the rows to order as an index list and hands back
the sorted data; nothing is gathered on the way down. The earlier form, its contract and the
copies it made per level are in this file's history (commit 9c32786).

## Contract

    sort_indexed(v, labels: &mut [u64], index: &mut [usize], emit: bool, scratch) -> (perm, Option<Value>)

Requires `labels` empty or of `index`'s length and non-decreasing (position `k` is row `index[k]`
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

`labels` may arrive empty, meaning every position is one block, which is the first rank of any
sort and the whole of a primitive column's; the refined labels always come back. `emit` is
`Groups` (the permuted index and `perm`), `Values` (the column; `index` and `perm` unspecified,
so a leaf sorts its keys without carrying positions), or `Both`. `sort_blocks(labels, v) ->
(perm, labels)` is `Groups` at the identity index (`perm[k]` the input row at output position
`k`); `sort_values(labels, v)` is `Both`; `sort_values_only(labels, v)` is `Values`. `arrange` exposes
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
- **Packed leaf runs.** Consecutive leaf fields of a `Prod` whose declared widths fit one `u64`
  sort as one key (`sort_packed`), most significant field first: one set of passes and one
  refinement for the run, and the digit skip below drops the zero digits a narrow value leaves.
  The rule is a function of the shape alone. An adaptive form that pulled each field to learn
  its significant bits and packed by those was measured at 1.7x better on `u64` columns holding
  small values (four fields under 64: 28 → 16 ns per row) and set aside as past the complexity
  budget; narrowing such columns to their width gets the same result under the simple rule.
  Multiword keys, the fields as separate words, lose whenever a wide field joins the run (four
  unique `u64` fields, 21 → 114): an LSD radix sorts by the last word first, so every pass over
  the later fields is spent before the first field, which alone decides the order, is reached,
  where field by field the sort stops after that first field. Sorting only the tied positions
  field by field, as the `Sum` and `List` arms do, loses 15–40% on every product shape: the leaf
  already passes a singleton block in O(1), and the subset bookkeeping costs more than the
  sequential passes it saves.
- **Values only.** `sort` and `dedup` never read the permutation, so their last leaf sorts its
  keys alone: half the bytes through every pass, no permute of the index. Only the last segment
  of a product, and a leaf, take the mode; a sum's lanes and a list's elements are read through
  the index afterwards and keep it. With the first rank's labels left absent rather than
  materialized as zeros: D1 sort_u64 7.1 ns per row at 1M (pdqsort 10.1) and 11.1 at 8M (11.6);
  D2 dedup 9.4 and 15.5 (10.6, 11.6).
- **Digits.** One sweep counts every digit of a block at once, and a digit on which every key
  agrees is skipped, as are the leading all-zero ones (datatoad's `lsb_range`). Neutral on the
  leaf rows, and what makes a packed `(u32, u32)` cost two passes rather than three.
- **Subsets.** The `Sum` and `List` arms sort only tied positions; a row drops out as soon as its
  block has split down to itself. While subsets refine, a label is the position its run starts
  at, unique per class over the whole problem, so a class one sub-call splits cannot collide
  with a class another sub-call left alone. One pass makes labels dense at the end.
- **Consumers.** `SortList`, `DedupList` and `GroupKey` take their output from the sort. A shape
  holding a `List` has a sorted form that is itself a gather of every element, so `dedup` and
  `group` gather their kept rows from the source there (`cmp.rs::representatives`).

## Measurements (2026-09-06, M4, ns per row)

Gap rows at 1M, master → here, Rust in parentheses: D1 sort_u64 24.1 → 7.1 (10.1); D2 dedup 28.7 → 9.4 (10.6); R1
arrange_sort_perm 24.3 → 10.8 (21.8); R7 Sum in one block 27.3 → 27.3 (36.7); R8 List in one
block 43.1 → 42.8 (89.6); R9 Sum under a block per four rows 37.0 → 12.6 (2.5); R10 List
likewise 72.0 → 13.4 (3.9). At 8M: D1 48.3 → 11.1 (11.6), D2 53.6 → 15.5 (11.6), R1 44.5 → 26.4 (31.9).

A probe over `Prod([key, value])` at 1M rows, the key unique or with some rows in pairs, the
value a `Sum` of four `u64` lanes, a ragged `List<u64>`, or `List<Sum<Prod<u64,u64> | List<u64>>>`:

| key | master, Sum / List / deep | here |
|---|---|---|
| unique | 65.6 / 74.9 / 94.0 | 20.2 / 20.0 / 20.4 |
| 1% in pairs | 63.0 / 76.6 / 92.4 | 26.0 / 29.4 / 29.2 |
| 10% in pairs | 66.7 / 80.4 / 101.0 | 25.7 / 29.6 / 31.3 |
| all in pairs | 75.5 / 97.4 / 165.9 | 30.9 / 33.3 / 37.2 |

Products of leaves at 1M rows, `sort_perm` / `sort_values`, master → packed by declared width:

| fields | master | here |
|---|---|---|
| (u32 % 65536, u32 % 65536) | 18.4 / 23.0 | 13.0 / 16.5 |
| (u8 % 4) × 3, u64 unique | 33.8 / 32.2 | 24.2 / 25.3 |
| (u64 unique, u64 unique) | 17.0 / 18.5 | 17.6 / 19.1 |
| (u64 % 1000, u64 % 1000) | 15.5 / 20.6 | 17.7 / 21.2 |
| (u64 % 64) × 4 | 28.3 / 34.2 | 28.9 / 35.7 |
| (u64 unique) × 4 | 20.6 / 23.6 | 17.3 / 26.7 |

Also: dedup over 1M lists with 50 distinct 40.5 → 40.7, over all-distinct lists 58.8 → 44.0; a
deep nested sort of 200k rows 129.7 → 86.5; a 64-row `Sum` sorted twenty thousand times 32.1 →
34.2, the fixed cost of a call. DDIR at medium scale, one worker, initial epoch: list_val 84.7 →
74.5 ms, sum_key 103.1 → 97.0, sum_val 71.0 → 67.2, unnest 254.8 → 238.4, scc 1.71 → 1.69 s;
churn unchanged; its test suite passes against this crate unchanged.

## Follow-ups

1. The 8M `sort_perm` row still carries its permutation through 16-bit-digit passes and sits
   at 26 against the Rust stable sort's 32; a two-level radix for blocks past a few million rows
   is the next thing to try there.
2. Range copies for the `List` arm's final gather, corgi's `extend_from_self`; `gather` on a
   `List` is element-wise today (`engine.rs:119-130`).
3. Dedup as you go: one position per class per level, expanding from the runs at the end.
4. A caller-held scratch, for tiny calls in a loop.
5. DDIR taking `sort_values` in `from_columns` and `sort_indexed` for the reduce's candidates.
