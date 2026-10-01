# Integers whose width is an encoding, not a type (spike, 2026-09-30)

Branch `corgi-int-spike` (from `corgi-ml-lowlevel`). The question: should corgi have an integer
column that does not commit to a width, and perhaps not to a sign, with each column packed as its
values need and a tag telling kernels the encoding? This note answers the five questions in the
brief, says what the prototype covers, and reports what it measured. Measurements are at the end,
with their conditions; numbers quoted in the answers come from those tables.

## In one paragraph

Yes, for integers used as values (keys, terms, tags, counts), with fixed-width bit columns kept for
what wraps (hashes, bytes, floats). The prototype is a second leaf kind, `Int`, beside `Prim`: a
column of integers stored as a per-column base plus unsigned offsets at 0, 8, 16, 32 or 64 bits,
with a bound on the offsets, in word-backed storage. Its clear wins are where corgi pays for
widths it does not need: `unweave` then sum a lane goes from 2.30 to 0.056 ns/row (Rust's
row-at-a-time loop is 0.21), a three-field sort of 20-bit terms from 21.1 to 11.3 ns/row (hand-
narrowed 32-bit terms: 17.7), an add of 7- to 31-bit data 1.5 to 7.5 times faster, and a decode
of a large column from 0.96 ns/row to nothing (0.1 if its declared bound is checked). Single-
column sort and `find` gain only 5 to 10 percent: they were never width-bound. Most of the sort
win comes from knowing each field's range, not from storing it narrow, and a sort could measure
ranges for today's `u64` columns too.

## What was built

All of it is contained: a new leaf kind with its own shape, a new op bucket, and a new arm in each
existing kernel that takes a leaf. `Prim`, every existing op's behaviour, and every existing test
are unchanged.

- `src/words.rs` (113 lines): word-backed storage. A buffer of `u64` words read and written as a
  slice of `u8`/`u16`/`u32`/`u64`, through `bytemuck::cast_slice`. corgi has no `unsafe`
  (`#![deny(unsafe_code)]`); `bytemuck` is its one dependency besides the optional `serde`.
- `src/int.rs` (about 1,130 lines plus 145 of tests): the `Int` column. Construction with
  narrowing (from host `u64`s, packed in place in the host vector's own allocation; from `i64`s;
  from `i128`s), adoption of host `u64`s untouched, constants that store nothing, re-encoding and
  bringing two columns to one encoding, gather, multi-source gather (`unwrap`, `select`, fold
  accumulators), sort keys, comparison, `find`'s needle re-encoding, hashing and equality by value,
  add/sub/mul with the overflow policy below, and the codec leaf (copied, or viewed in place).
- Arms in existing kernels (about 120 lines over 13 files): `gather`, `fill`, `gather_lanes`,
  `blend`, fold scatter, sort (a leaf arm, and product fields packing by span), the comparator,
  the merge kernel (`survey`), `Rel`/`gt`/`min`/`max`/`find`, `hash`, the codec (`read_from_words`
  is new), `weave` accepting integer tags, `cast` refusing an `Int`.
- `src/ops/int.rs` (109 lines): `add_int`, `sub_int`, `mul_int`, `narrow`, `to_int`,
  `to_int_signed`, `to_u64`, `fold_add_int`, `unweave_int`.
- The ML surface: the shape `int`, the literal `5int` / `-3int`, the op names above.
  `programs/62-int.col` uses them.
- `tests/int.rs`: every structural op on `Int` columns built four ways (narrowed, adopted at 64
  bits, decoded as a window of a shared message, constant), against `i128` arithmetic and Rust's
  own sort and binary search; codec round trips; `unweave_int` against `unweave`.
- `benches/ints.rs`: the measurements below.

Not covered: positions and counts as integers (`len`, `iota`, `find`'s ranges and `gather`'s
indices stay `U64`), the sum's tag column stored as an `Int` (so `unweave_int` copies one byte per
row rather than sharing the column), reductions other than `fold_add_int`, floats.

## 1. Representation: words with a width tag

**Answer: word-backed.** An `Int` column's offsets live in `Arc<Vec<u64>>`, read as a slice at the
column's width through `words::view`, and a column is a window `(at, len)` of its buffer. Kernels
see the same thing they see today, a typed slice per width (`View::U8(&[u8])` and so on, expanded
by a macro as `prim!` does), so the code reads as it does for `Prim`.

Why:
- The argument that chose per-width vectors (`dev/bytes-kickoff.md`) was that the `Prim` enum makes
  the type system hold a leaf's width and its data consistent. Once the width is no longer part of
  the type, it is a run-time value either way, and the one accessor that hands out the typed slice
  is where consistency is checked. That argument no longer favours the enum.
- Decoding without copying needs it. A received message is one allocation; only a cast can read a
  `u32` column out of a `u64` buffer. Measured on a 16M-row column: today's `U64` decode 0.98
  ns/row (it converts element by element), the same bytes copied as words 0.43, viewed in place 0
  (an adopted 64-bit column) or 0.095 (a 32-bit column, whose declared bound is checked by one read
  pass).
- Narrowing reuses the host's allocation: `Int::from_u64s` packs offsets into the front of the
  vector it was handed (0.33 ns/row at 1M and 16M rows, against 0.37 to 0.43 for measuring the
  range and copying into a fresh buffer).
- A column can be a window of another's buffer, which the decode uses and which would also give
  zero-copy slicing of a leaf.

The cast is `bytemuck`'s. Reading lanes out of words without a cast is not a substitute: shifting and masking, or `to_le_bytes`, measured 1.5 to 5 times slower on dense loops
(summing a column of bytes: 0.015 ns/row through a view, 0.078 without; adding two 16-bit columns
into 32 bits: 0.19 against 0.29 to 0.36), and 1.1 to 1.5 times slower on random gathers. The view
itself is as fast as a native `Vec<uN>`.

Costs found:
- Fresh outputs are zeroed (`vec![0u64; n]`), since writing narrow lanes into uninitialized words
  needs another `unsafe` function. About 0.06 ns/row on a 1M-row output, which is most of why `Int`
  arithmetic at 64 bits is 0.285 ns/row against `U64`'s 0.232.
- A host `Vec<u8>` or `Vec<u32>` cannot be adopted (its allocation is not 8-aligned or not word-
  sized); only `Vec<u64>` can. Text should stay in `Prim` for this reason, at least for now.
- The sum's discriminant column is a `Prim::U8`, so it cannot be shared into an `Int` without a
  copy; `unweave_int` copies one byte per row. Storing tags as an `Int` would make it an `Arc`
  clone.
- Views assume little-endian for the wire format; a big-endian target decodes by copying.

Rejected:
- **Per-width vectors plus a frame** (`Int { base, span, data: Prim }`). No `unsafe`, adopts any
  host vector, shares the sum's tags directly. But no decoding in place, no narrowing in place, no
  windows. It is the fallback if `unsafe` and a dependency are both unwelcome: every kernel in the
  prototype takes a slice and would work unchanged.
- **A flat byte buffer.** Rejected before for alignment, and still.
- **Two hand-written `unsafe` casts.** What the spike first did, to keep corgi free of dependencies.
  `bytemuck::cast_slice` makes the same argument; Frank chose it (2026-09-30), so corgi stays free
  of `unsafe`.

## 2. Sign: frame of reference, so there is no sign

**Answer: a per-column base (an `i128`) plus unsigned offsets.** A column of values in `[-3, 7]` is
base -3 with offsets up to 10; a column of `u64`s near `2^63` is a base near `2^63` with small
offsets. Signed and unsigned are the same type. The encoding preserves order within a column, so
sort, `find`, comparison and the merge kernel read offsets as unsigned numbers, exactly as they
read a `Prim` today. It also narrows data that sits in a band away from zero (timestamps, ids), it
makes a constant column free (width 0, nothing stored), and it makes adding a constant free (the
base moves; measured 0.000 ns/row).

"Signed by default" gives up nothing for integers used as values: a column holds any `i128`
values whose spread fits 64 bits, so every `u64` and every `i64` fits, including `u64`s above
`2^63`, which an `i64` default would have lost. What it gives up is wrapping: hashes, random-number
mixing and bit tricks want arithmetic modulo `2^64`, and corgi's existing comment on `fold_add`
relies on reading a negative difference as a large `u64`. Those stay on `Prim`, whose ops wrap.
That is the case for keeping both leaves: `Int` for integers, `Prim` for bits.

Rejected:
- **The sign-bit swizzle corgi uses now.** The encoding depends on the width (`-1` is `0x7f` at 8
  bits and `0x7fff...ff` at 64), so widening re-encodes and comparing two widths decodes; and a
  signed reading of 64 bits cannot hold `u64`s above `2^63`. Converting a swizzled column costs
  nothing, though: its stored bits are the offsets of the frame based at `-2^(w-1)`
  (`to_int_signed` is one base change).
- **Zigzag.** Small magnitudes get small codes, but the order is lost (0, -1, 1, -2, ...), and
  corgi's sort, `find` and comparison all rely on stored order.

## 3. Overflow: plan the result's frame before reading a row

**Answer: interval arithmetic on the operands' frames picks the result frame, so the loop has no
check.** For `a + b`: base `ba + bb`, bound `sa + sb`, offset `x + y`. For `a - b`: base
`ba - bb - sb`, bound `sa + sb`, offset `x + (sb - y)`. For `a * b` with non-negative bases: base
`ba * bb`, bound `(ba + sa)(bb + sb) - ba * bb`, offset `x * y` when both bases are 0 and
`ba * y + bb * x + x * y` otherwise (a negative base takes the exact path below). A constant
operand of `+` or `-` moves the base and touches nothing. When the planned bound spreads past 64
bits (say, the operands were adopted as 64-bit words and promise nothing), both operands are
narrowed to their actual ranges and the plan is made again; if it still does not fit, the result
is computed exactly in `i128` and is an error if its values really spread past 64 bits.

What was measured:
- **Per-row overflow checks are not what costs.** A Rust loop adding `u64`s takes 0.204 ns/row,
  and 0.212 with an overflow flag per row; `u32`: 0.093 and 0.095. In a streaming loop the check is
  free. So the policy question is not "check or not" but which width the result gets and how many
  passes it takes.
- **Narrow results are where the time goes.** At 1M rows, `add_int` on 7-bit data 0.035 ns/row
  (8-bit result) against `U64` `add` 0.265; 15-bit 0.071 (16-bit) against 0.244; 20-bit 0.157
  (32-bit) against 0.252. Three chained adds of 15-bit data: 0.378 against 0.661 (the results 16,
  32 and 32 bits wide). `mul_int` of 20-bit data produces 64-bit results and is slower than `U64`
  `mul`, 0.380 against 0.281: the same output bytes, plus the zeroed buffer, plus a widening
  multiply loop that is not as tight.
- **Loose frames cost a pass.** Adding two adopted 64-bit columns of 20-bit data, which must
  narrow first: 0.735 ns/row against 0.157 for the narrowed ones.
- **Past 64 bits is reachable.** Multiplying two 40-bit columns gives 80-bit products: `mul_int`
  reports an error where today's `mul` wraps without a word.

Costs of the policy: a result can be a width wider than its values need (`255 + 255` plans 9
bits; the column is 16 bits wide even if every sum happened to be small), and bounds drift along a
chain (`x - x` has twice `x`'s bound though every value is 0). `narrow` resets both; nothing calls
it implicitly mid-program.

Rejected:
- **Compute at the operands' width, check, redo wider on overflow** (what CBQN does). The check is
  free, but the result's width then depends on the data, an overflow costs a second pass, and a
  64-bit multiply has no vector overflow check on this machine. Planning from frames gives one pass
  and a width that is a function of the inputs' encodings.
- **Widen the whole column after the fact.** The same second pass.
- **Keep the few wide rows in a separate lane** (a sum over encodings). With a tag per row it costs
  as much as the data it saves: a one-byte tag is the whole of an 8-bit column, and every kernel
  would split and merge lanes, which corgi-wins measures at 5 times Rust. As a sparse list of
  exceptions (positions and values) it is a compression scheme; every kernel would need a patch
  path. Neither pays for compute.
- **A big-integer lane past 64 bits.** No workload in view needs one: GALEN's terms, tags, counts,
  positions and differential dataflow's diffs fit 64 bits, and hashing wants wrapping, not
  growth. An error for now; a 128-bit lane would be the next step if one is needed (one more lane
  type; a 128-bit sort key would take two radix levels).

## 4. Narrowing: at the boundary, and by kernels for their own use

**Answer:** narrow when data enters (host vectors, `to_int`, literals, decoded columns keep the
sender's encoding), and when an op makes a column whose bound it already knows (arithmetic plans
it; `unweave_int`'s tags are 8 bits by construction). Do not re-narrow columns implicitly in the
middle of a program, except arithmetic narrowing its operands when their bounds are too loose to
plan with. `narrow` is the explicit op. A kernel that benefits from knowing a range rather than
from narrow storage should read the frame, and measure the range itself when a column has none.

The sort is that kernel, and the measurements say the range is what matters, not the storage:
- One column, 1M values: `U64` against the narrowest `Prim` by hand against `Int`: 4.12 / 3.66 /
  3.50 ns/row for 8-bit values, 6.36 / 6.20 / 6.11 for 32-bit values. The radix already skips
  passes the keys' maximum does not need, and it sorts `u64` keys whatever the storage; narrow
  storage saves only the reads and writes.
- Three fields of 20 bits each: `U64` 21.1, `U32` by hand 17.7, `Int` 11.3 ns/row. `Int` fields
  pack into one 64-bit key by the bits of their bounds (60 bits: one radix sort), where `U64`
  fields take three levels and `U32` fields two. Sort and dedup: 24.9 / 20.1 / 14.7; with heavy
  duplication (8-bit fields), 24.8 / 21.4 / 10.6.
- The same three columns adopted at 64 bits with no frame sort like `U64` (21.0); narrowing them
  inside the timer first gives 12.5, nearly all of the win. So a sort that measured each `Prim`
  field's range (a max pass, 0.13 ns/row per field) would get it for today's `u64` columns too.
  That change is independent of `Int` and worth making regardless.

`find` is not width-bound either: 1M needles in a 1M haystack, 20-bit values, 216.8 / 211.7 /
196.5 ns/row (`U64` / `U32` / `Int`), and at 8M, 306 / 284 / 266. Needles in another encoding
than the haystack's are re-encoded into it (193 to 198 ns/row with the needles held as 64-bit
words); the haystack is never copied, and needles outside its frame are answered without a search.
The `u64`-only fast paths on the branch that ran GALEN (`corgi-galen`) are not on this one; here
every width goes through the same search, and `Int` needs no special case because both sides share
one encoding.

The codec writes each column in its own encoding. An encoder that narrowed on the way out would
halve the bytes of 32-bit data, but the receiver's in-place view wants the encoding it is sent; let
the sender choose.

## 5. Types: `Int`, with no width

**The shape is `Shape::Int`.** The typer is `eval` on zero rows, and an empty `Int` column has some
encoding (width 0) and every integer op returns an `Int`, so the typer needed no change and never
sees a width. The cost is that a shape cannot promise a kernel a width; kernels choose one at run
time, as they already do for `Prim`'s four widths.

**Equality and hashing are by value.** Two columns with the same values are equal whatever their
encodings, and an integer in `0..2^64` hashes as the `u64` leaf of the same value, so an `Int` key
and a `U64` key join by hash. (A swizzled signed `Prim` still hashes its stored bits, as today.)

**Comparison across encodings** is a pairwise step before the kernel: the two columns are brought
to the frame covering both, and a side already in that encoding is not copied (`Rel`, `min`/`max`,
`select`, merges, `gather_lanes`). `find` re-encodes only the needles. The comparator, reached from
nested shapes, compares values directly when two sub-columns disagree, without copying. If two
columns' values spread past 64 bits together, they cannot share an encoding; then comparisons fall
back to values, and merges into one column are an error.

**Floats** are out of scope and not cheap. CBQN's numbers are doubles stored as integers when they
fit; the corgi version would be a float encoding beside the integer ones, every arithmetic kernel
gaining a float path, and one total order across integer and float encodings. Floats stay on
`Prim` with the float ops.

**The surface** keeps its explicit-width spellings for the mechanical level, and adds the shape
`int`, the literal suffix `int` (`5int`, `-3int`, a constant column that stores nothing), and
`add_int` and its siblings. The brief suggested an unsuffixed `5` for the abstract integer. The
prototype does not take it: `tests/ml.rs` pins `(input, -3) add` as a parse error, and that is a
decision for you. With bare literals as `Int`, that line would become a type error at checking
time instead (`add` takes `U64`), which keeps its point.

## Measurements

Conditions: Apple M4 (10 cores), macOS, rustc 1.95, release, the system allocator, 2^20 rows
unless noted. Each number is the best of 15 runs of one whole program (`eval_graph` of the lowered
graph, the input built outside the timer and handed over by `Arc` clone, the output dropped inside
it), best of three processes. Other agents were compiling (load average 2.3 to 3.5). Values are
uniform random in `[0, 2^b)`. `cargo bench --bench ints` reproduces every table but the first and
the corgi-wins row.

Reading narrow lanes out of `u64` words (a scratch benchmark, ns/row at 1M rows, 8M in brackets):

| loop | native `Vec<uN>` | view of words (cast) | words without a cast |
|---|---|---|---|
| add 16-bit + 16-bit into 32-bit | 0.201 | 0.189 | 0.286 to 0.359 |
| sum of bytes | 0.015 | 0.015 | 0.078 |
| random gather of 32-bit values | 0.760 (2.15) | 0.726 (2.20) | 0.821 (3.40) |

Sort, one column (`input sort`), ns/row:

| b | U64 | narrowest Prim | Int (width) |
|---|---|---|---|
| 8 | 4.12 | 3.66 | 3.50 (8) |
| 16 | 4.82 | 4.51 | 4.41 (16) |
| 20 | 5.88 | 5.64 | 5.59 (32) |
| 32 | 6.36 | 6.20 | 6.11 (32) |

Sort, three fields `(a, b, c)` each `b` bits, ns/row. "Adopted" is `Int` held as 64-bit words with no
frame; "+ narrow" narrows those three columns inside the timer first.

| b | U64 ×3 | U32 ×3 | Int ×3 | adopted ×3 | adopted + narrow |
|---|---|---|---|---|---|
| 10 | 20.87 | 15.57 | 7.44 | 20.61 | 8.15 |
| 16 | 19.15 | 12.97 | 10.38 | 19.08 | 11.08 |
| 20 | 21.13 | 17.66 | 11.27 | 21.00 | 12.46 |

Sort and dedup, three fields (`input dedup`), ns/row: b=8 (many duplicates) 24.80 / 21.44 / 10.57;
b=20, 24.87 / 20.14 / 14.74 (`U64` / `U32` / `Int`).

`find`, 1M needles into a sorted haystack of 1M, ns/row:

| b | U64 | narrowest Prim | Int | Int, needles as 64-bit words |
|---|---|---|---|---|
| 16 | 216.3 | 206.5 | 190.6 | 193.3 |
| 20 | 216.8 | 211.7 | 196.5 | 197.8 |
| 32 | 216.5 | 210.4 | 195.9 | 196.6 |

At 8M rows (best of 3 runs, one process): b=20, 306.5 / 283.8 / 266.1 / 269.2. `Int` is a few
percent faster than the hand-narrowed `Prim` on the same search; I did not find why (the comparison
loops have the same shape).

Arithmetic, ns/row, cases interleaved round by round (otherwise whether an 8 MB output faults in
fresh pages depends on what ran before, and the `U64` rows varied from 0.20 to 0.45):

| b | U64 add | add_int (widths) | U64 mul | mul_int (widths) |
|---|---|---|---|---|
| 7 | 0.265 | 0.035 (8 → 8) | 0.273 | 0.048 (8 → 16) |
| 15 | 0.244 | 0.071 (16 → 16) | 0.272 | 0.098 (16 → 32) |
| 20 | 0.252 | 0.157 (32 → 32) | 0.281 | 0.380 (32 → 64) |
| 31 | 0.232 | 0.155 (32 → 32) | 0.279 | 0.382 (32 → 64) |
| 40 | 0.232 | 0.285 (64 → 64) | 0.306 | error: 80-bit products |

Also: `add_int` of two adopted 64-bit columns (narrowed first) 0.735; of a constant 0.000; three
chained adds of 15-bit data, `U64` 0.661, `Int` 0.378 (results 16, 32, 32 bits). Rust over the
same 20-bit data: `u64` add 0.204, with an overflow flag 0.212; `u32` add 0.093, with a flag 0.095;
one max pass over a `u64` column 0.126; a zeroed 8 MB buffer 0.059.

`unweave` then sum one lane (`input unweave .2 fold_add`, on the data of the `sum_err_total` case in
`~/Projects/corgi-wins`: 1M rows of a two-variant sum, half or 5 percent of them in the lane
summed), ns/row:

| pattern | unweave (this branch) | unweave_int | Rust `filter_map` | widening 1M tags to u64 alone |
|---|---|---|---|---|
| mixed | 2.300 | 0.056 | 0.206 | 0.311 |
| rare | 2.586 | 0.025 | 0.205 | 0.316 |

For reference, `unweave` on the `corgi-lane-kernels` branch (it reads the tags in place and widens
them in one pass), run through the corgi-wins harness on the same machine the same evening: 0.35
(mixed), 0.32 (rare), against Rust's 0.20 there. What is left of that is the widening.

Decoding one column (`read_from` copies; `read_from_words` views the message), ns/row:

| column | 1M rows | 16M rows |
|---|---|---|
| U64 leaf, copied (today) | 0.959 | 0.981 |
| Int, 32-bit offsets, copied | 0.164 | 0.309 |
| Int, 64-bit, copied | 0.368 | 0.432 |
| Int, 32-bit offsets, viewed (bound checked) | 0.095 | 0.095 |
| Int, 64-bit, viewed (no bound to check) | 0.000 | 0.000 |

Narrowing host data (20-bit values in `u64`s), ns/row at 1M (16M): `Int::from_u64s`, a range pass
and packing in place, 0.324 (0.331); Rust converting to a fresh `Vec<u32>` with no range pass 0.207
(0.302); measuring the range of an adopted column and copying it narrow 0.372 (0.430).

## For you to decide

1. Whether integers as values become a first-class leaf beside the bits leaf, with the semantic
   split that brings: `Int` arithmetic widens and errors, `Prim` arithmetic wraps. (Frank,
   2026-09-30: seems a good idea.)
2. Word-backed storage, against per-width vectors with a frame (no decoding in place). (Frank:
   word-backed, through `bytemuck`; done.)
3. Whether a bare `5` should be the integer literal. (Frank: `5int` for now; eventually the
   default integer should be arbitrary precision.)
4. Independently of the rest: whether the sort should pack `Prim` fields by measured range (about
   half the three-field sort time on narrow-valued `u64` data, for a max pass per field). (Frank:
   possibly moot once values are `Int`; not now.)

## If you pursue it

In order of payoff for effort:
1. Store the sum's discriminant column as an `Int`, so `unweave_int`'s tags are an `Arc` clone, and
   make `unweave` itself return them (`programs/33` compares the tag list with a `U64` list and
   would change).
2. Pack `Prim` sort fields by measured range (item 4 above).
3. Positions and counts as `Int`: `len`, `iota`, `find`'s ranges, `gather`'s indices. These are
   where narrow integers occur by construction (bounded by row lengths), so this is the largest
   systematic payoff, and the largest change (every index consumer).
4. Port the GALEN driver's terms to `Int` and measure the 2x sort gap to datatoad again.
5. Write fresh outputs into uninitialized words instead of zeroing them (one more `unsafe`
   function; about 0.06 ns/row at 1M rows).
6. Decide 128-bit lanes against the error past 64 bits, when a workload asks.

The leaf is about 1,100 lines against `Prim`'s 350: the frames (bringing columns to one encoding,
re-encoding), the arithmetic that now lives with the leaf, and the codec. Folding `Prim` into the
same word-backed storage (one leaf with a "bits of width w" encoding beside the integer one) would
share the storage, the views and the codec, and is the shape I would aim for if this lands.
