# Hash lanes: sorting structured keys and values by identifier

## The question

DDIR stores an arrangement key under its hash, `Prod([hash, key])`, so the chunk sort is a radix
on the lane with the structural order breaking ties. The ties are the rows of one key — two to a
few hundred of them — and the sort descended into them level by level: a `List` key sorted its
lengths, then each element position, over rows that were one value all along. A string key of
8 to 16 bytes cost 219 ns/row at 1M rows against 31 for the lane radix alone. Values had no lane
at all: within a key they sorted structurally, and the reduce re-hashed them at every
presentation to name them.

Frank's framing (2026-09-11): order keys and values by hash, let corgi sort the lanes mostly
ignorant of the structured columns, and take a chance — confirm distinctness with an equality
pass rather than a sort, and do the work of a structural sort only where two distinct values
share a hash.

## Design

- **The equality kernel** (`order.rs`, `mod equal`): `equal_idx(a, b, ia, ib)` is the sign-free
  reading of `compare_idx`, one descent per level. A leaf is one lane compare; a product narrows
  to the pairs still equal; a sum matches tags then lanes at the carried offsets; a list of leaf
  elements is decided by ONE span comparison per pair, so an equal string costs its bytes and an
  unequal one its common prefix. `group_bounds` reads its run ends from `equal_adjacent`.
- **The uniform check** (`sort.rs`, `sort_uniform`): a `Sum` or `List` sorted under labels first
  compares every tied position with its block's first row. Blocks that are one value are done —
  their labels stand, their rows do not move — and only the blocks where some row differs go to
  the arm, as a sub-call over those positions. A product reaches this field by field, after its
  leaves have refined, which is how `Prod([hash, key])` arrives: the hash radix makes the blocks,
  the check confirms them.
- **Taking the chance cheaply**: the check samples 256 tied positions first and hands the column
  to the arm whole when most of those differ — the labels were not a hash of these rows — so a
  structured column sorted under unrelated labels (a reduce's candidates by bracket, `dedup` over
  distinct elements) pays nothing measurable. When most blocks are uniform but more than half the
  positions are not, the arm also takes the whole column rather than a copied subset.
- **Nothing changes in the order.** The result equals `sort_blocks` on every input: colliding
  values sort structurally within their hash, so the stored order `(key id, key, value id, value)`
  is total and the same in every batch. Tested against the scalar reference with hashes folded to
  a few bits so that collisions are common.

## Measurements (2026-09-11, M4, 1M rows, ns/row, best of 3; `$SCRATCH/probe`)

Shapes: `pair` = `Prod([u64, u64])`, `str` = `List<u8>` of 8..16 bytes, `list3` = `List<u64>` of
three, `quad` = four `u64` fields, `sum` = two `u64` lanes. Keys below `1M/2` distinct, values
random. `today` = `sort_perm(Prod([present_key(K), V]))`, the arrangement sort with a key lane only;
`vlane` = the same with `Prod([hash(V), V])` for the value; `lanes` = the two `u64` lanes alone.

| key | val | master today | branch today | branch vlane | lanes | hash(V) |
|---|---|---:|---:|---:|---:|---:|
| u64 | list3 | 43.6 | 46.7 | 26.5 | 24.0 | 6.9 |
| u64 | str | 36.0 | 35.7 | 27.3 | 23.9 | 26.5 |
| u64 | sum | 39.9 | 39.8 | 26.6 | 22.9 | 2.8 |
| u64 | quad | 32.5 | 30.0 | 36.6 | 19.2 | 4.0 |
| pair | list3 | 73.0 | 72.3 | 50.3 | 30.9 | 6.9 |
| str | u64 | 218.8 | 53.1 | 51.7 | 26.2 | — |
| str | str | 233.2 | 66.5 | 57.9 | 30.6 | 25.1 |
| str | list3 | 241.0 | 79.8 | 57.8 | 31.0 | 6.9 |

Read: a structured key under its lane now costs what a pair key does (4x on strings); a value
lane takes a structured value to within a few ns of the lane radix (1.5x over master on lists,
sums and strings); a product of leaves is better left alone (the lane is a pass for nothing, and
the check does not run on it). The lane's own price is the hash: 2 ns/byte on strings, since the
list hash mixes each element — a word-at-a-time byte hash would cut that, but it re-identifies
every value (DDIR's `ir::structural_hash` mirrors this fold and would move with it).

Gap rows (`cargo bench --bench gaps -- --family R`): R13 `(hash, str key)` sort 39.7 ns/row
(Rust cached-key sort on (hash, bytes) 90.0); R14 the same with a `(hash, list3)` value 50.9
(Rust 112.2); R15 `group_bounds` over a sorted `List<u64>` 3.7 (Rust `windows(2)` over spans 2.0).

## Follow-ups

1. `equal_idx` on a list of products of leaves (`List<(u64, u64)>`, a `collect` of pairs) expands
   element pairs; spans per lane would make it one comparison per lane per pair.
2. The uniform check gathers its emitted column with `gather`; the arms' own emission (a sum's
   lanes) is not used on the uniform path.
3. DDIR (differential-dataflow branch `corgi-hash-lanes`): `present_val` stores a structured value
   under its hash, the consolidation reads runs off `sort_values`' labels, the reduce reads value
   ids off the lane and merges presentations in identifier order after one batched equality over
   adjacent equal-identifier rows.
