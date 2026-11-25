# List bounds spike: lengths and unary bits instead of offsets (2026-11-24)

Lengths are worth adopting for byte lists; unary bits are not.

`Bounds` gained two variants (`src/value/lists.rs`), and `CORGI_LISTS=offsets|lens|unary` picks the form every non-uniform partition takes, so the whole crate runs under each.
All tests pass under all three forms.

- `Lens`: each row's length at the narrowest width that holds the longest row (u8/u16/u32/u64), plus the start of every 64th row as a `usize`.
- `Unary`: per row, its length in one bits then a zero (rows + elements bits), plus zeros-before per 512-bit block and the block of every 256th zero.

Data: ClickBench `hits_0.parquet` (1M rows), prepared into `~/Projects/datasets/clickbench/data`.
Machine: mini-03 (M4), one thread.

## Size

Bits per row for the partition alone.

| column | payload bytes/row | offsets | lens | unary |
|---|---:|---:|---:|---:|
| SearchPhrase | 3.5 | 64 | 17 | 5.2 |
| MobilePhoneModel | 0.1 | 64 | 9 | 1.3 |
| URL | 88.6 | 64 | 17 | 101 |
| Title | 138.4 | 64 | 17 | 157 |
| Referer | 79.6 | 64 | 17 | 91 |

Unary is one bit per element, which is 12.5% of a byte payload; for URL that is more than the offsets (9%).
Lens is u16 on every column but MobilePhoneModel, because one long row sets the width for all rows.

## Time

`examples/lists_spike.rs`, ns/row, best of runs (`micro3.summary` has every column).
Lower is better; the ratio is form / offsets.

| column | kernel | offsets | lens | unary |
|---|---|---:|---:|---:|
| SearchPhrase | len | 0.33 | 0.29 (0.88x) | 1.09 (3.3x) |
| SearchPhrase | filter | 1.27 | 1.47 (1.16x) | 3.01 (2.4x) |
| SearchPhrase | hash_group | 29.7 | 34.0 (1.15x) | 36.8 (1.24x) |
| SearchPhrase | sort | 18.7 | 20.4 (1.09x) | 21.6 (1.16x) |
| URL | len | 0.36 | 0.28 (0.78x) | 3.17 (8.8x) |
| URL | filter | 7.90 | 8.09 (1.02x) | 18.8 (2.4x) |
| URL | hash_group | 58.6 | 65.3 (1.12x) | 74.7 (1.27x) |
| URL | sort | 324 | 328 (1.01x) | 339 (1.05x) |

ClickBench end to end (`r4.summary`, best of two runs): with lens, string queries run 1.05x-1.35x of offsets, and a few short ones 1.5x-1.7x (q12-q14, q24); unary runs 1.2x-3.5x.
q29 reads no strings and flips between 18 and 36 ms from run to run; ignore it.

## What it took, and where it falls apart

1. In-order reads are free once the loop is written as one pass.
   `Bounds::spans()` / `ends_from()` and an `Ends::fold` that picks the form once made `len` faster with lens than with offsets.
   The trap: `collect` on a non-`TrustedLen` iterator calls `next`, not `fold`, so the per-element form match stays; `for_each` avoids it.
2. Random reads need a decode to offsets (`Bounds::random_access`, taken when queries >= rows / 8).
   Sort, gather by a permutation, survey and `compare_idx` all take it.
   The transient is the 64 bits/row we were trying to avoid, and it is paid per call: `hash_group`'s +12-15% is mostly repeated decodes inside `compare_in` (`from_iter<Ends>` is 8.8% of the lens profile).
3. Ascending reads (a filter's kept rows) need no decode: a cursor adds the skipped lengths (`sorted_rows` in `engine.rs`).
4. Gather, filter and the list sort write lengths directly (`lists::Builder`); I did not isolate whether that beats writing ends.
   The cost I did measure is elsewhere: most kernels still build a `Vec<usize>` of ends and convert it, at 0.6-0.8 ns/row for lens and 1.3-2.3 for unary.
5. Equality fell back to comparing ends one by one, and `Zip` checks it on every call: 20% of the first lens profile.
   Comparing the length vectors directly fixed it.
6. Unary's in-order scan is 1-4 ns/row (a dependent walk over the words), and its random read is a select.
   With size also worse than offsets on long rows, it loses everywhere except tiny rows.

## Open, not done

- A decode cache inside `Lens` would remove repeated decodes but brings back the 64 bits/row while it lives.
- Kernels that build `Vec<usize>` ends and then convert (about 140 `.into()` sites) would need to build lengths directly to stop paying the conversion.
- The width is set by the longest row; a per-block width (u8 blocks with u16 escape) would make SearchPhrase about 9 bits/row.
- Wire format: compact forms are written as offsets (`bytes.rs`).

The earlier standalone study (`~/Projects/widths-spike`, 2026-10-31) measured Elias-Fano at 6-14 bits/row; it is the fix for unary's long-row size, not tried here.
