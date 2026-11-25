# List bounds spike: lengths and unary bits instead of offsets (2026-11-24)

Lengths are worth adopting for byte lists; unary bits are not.

`Bounds` gained two variants (`src/value/lists.rs`), and `CORGI_LISTS=offsets|lens|unary` picks the form every non-uniform partition takes, so the whole crate runs under each.
All tests pass under all three forms.

- `Lens`: each row's length at the narrowest width that holds the longest row (u8/u16/u32/u64), plus the start of every 64th row as a `usize`.
- `Unary`: per row, its length in one bits then a zero (rows + elements bits), plus zeros-before per 512-bit block and the block of every 256th zero.

Data: ClickBench `hits_0.parquet` (1M rows), prepared with `python3 benches/clickbench/prepare.py DIR hits_0.parquet`; set `CORGI_CLICKBENCH=DIR`.
Machine: an M4 Mac mini, one thread.

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

## Follow-up: `cut` writing lengths

`cut_lens` in `src/ops/core.rs` (on top of PR #161's branch-free `cut`) writes both levels as lengths, at the width of the longest input row, which bounds every piece and every piece count.
Each position writes itself relative to its row, a mark keeps it, and the row's ends become lengths with one subtraction per piece.

Cutting ClickBench strings at `/` (`KERNELS=cut`, ns/row, lower is better):

| column | pieces/row | offsets | lens | output bounds, offsets → lens |
|---|---:|---:|---:|---:|
| SearchPhrase | 0.07 | 3.40 | 2.92 (0.86x) | 8.6 → 2.3 MB |
| MobilePhoneModel | 0.02 | 2.30 | 1.59 (0.69x) | 8.2 → 1.1 MB |
| URL | 5.64 | 45.9 | 51.5 (1.12x) | 53.1 → 14.1 MB |
| Title | 1.06 | 36.6 | 37.7 (1.03x) | 16.5 → 4.4 MB |
| Referer | 5.76 | 42.0 | 47.4 (1.13x) | 54.1 → 14.4 MB |

The output is 3.8x smaller, but `cut` alone gets no faster on long strings.
82% of the URL time is the per-byte mask loop (about 0.4 ns/byte); every position writes the same slot until a mark moves it on, so the output stores stay in cache whatever their width.
Forcing u32 or u64 lengths does not move URL (52.1, 53.7), so the 12% loss is not the store width; I have not found its cause.
`benches/idioms -- cut cut_rows` (i64 lists) shows lens at 0.95x-1.21x of offsets.

## Follow-up: block size by width, starts inline, and `len` for free

Switches (all read once): `CORGI_LENS_LAYOUT=fixed|width|inline`, `CORGI_LENS_SUM=plain|masked`, `CORGI_LENS_TOUCH=1`, `CORGI_DECODE=never`.

- `width`: one start per 64 bytes of lengths (64 rows at u8, 32 at u16, 16 at u32, 8 at u64).
- `inline`: 64-byte lines at the lengths' width, each the block's start in its first 8 bytes and then 56 bytes of lengths, aligned to cache lines.
- `masked`: sum the whole block under a mask, with no branch on the row's place in it.
- `touch`: a batch read first does a pass of plain loads over 256 queries, then the sums.

Random span reads, ns per read, lower is better (`SYNTH=rows,longest` in the example; "batch" is `Lens::spans_at`, with the width and layout decided once per batch):

| rows, longest | offsets | fixed | width, masked | inline, masked | width, touch | inline, touch |
|---|---:|---:|---:|---:|---:|---:|
| 1M, 300 (u16; bounds in cache) | 1.3 | 14.9 | 6.0 / 6.5 batch | 8.0 / 6.7 batch | 7.5 batch | 7.2 batch |
| 100M, 300 (u16) | 7.1 | 24.1 | 25.3 / 29.8 batch | 42.5 / 29.2 batch | 12.4 batch | 10.3 batch |
| 100M, 200 (u8) | 7.2 | 22.1 | 22.6 / 35.2 batch | 35.8 / 35.2 batch | 14.3 batch | 12.7 batch |

What the numbers say:

1. The variable-length sum was the cost in cache: its trip count depends on the row, so it is mispredicted.
   A masked sum over the whole block halves a read (14.9 to 6.0 ns), and only together with the width-sized block (64 bytes, not 128).
2. Past the caches, the long body of each read limits how many reads the core keeps in flight: offsets do about 5 instructions per read and overlap their misses (7 ns per read at 800 MB); lengths do several times more and do not.
   A first pass of plain loads per 256 queries restores the overlap: 29.8 to 12.4 ns (width), 29.2 to 10.3 ns (inline).
3. Inline starts help only once reads are batched with that pass, by about 17% (one line per read instead of two); one read at a time they are slower, because assembling the start from the line is more work.
4. Lengths stay 1.5x-1.8x slower than offsets per random read at best.
   The kernels still decode before random reads, so none of this moves them: with `CORGI_DECODE=never` sort runs 1.2x-1.8x slower, because the kernels read one span at a time, not in batches.

`len` on a list of byte lengths is now the lengths themselves, shared (`Narrow::U8` is behind an `Arc`): MobilePhoneModel `len` 0.33 to 0.00 ns/row.
The u16 columns still widen to i64 (0.28 ns/row), since corgi has no u16 leaf.

## Decisions (2026-11-24)

This spike is for learning, not for adoption.
Variable-width integers in corgi come first; lengths would then be an ordinary leaf, and `len` free at every width.

- Keep: blocks sized by width (64 bytes of lengths) and the branch-free masked block sum.
- Think about: two-pass batched reads; they pay only past the caches and only for kernels that read spans in batches.
- Drop: starts inline with their lengths.
