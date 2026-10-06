# Adaptive integers through independent input preparation

Spike branch: `codex/corgi-adaptive-preparation`, based on `corgi-ref` at
`93af76306948f91b014c1fc1378ddfed77b29aa3`. Comparison target: the locally available
`origin/corgi-int-spike` at `309884e`, inspected in a separate detached worktree.
Its base differs from this branch; this is a comparison of prototypes, not a
controlled replacement of one implementation detail.

The two spikes converge on the main design: an integer's value and logical shape
should not depend on its column's storage width or signedness. They differ on
how much information the storage descriptor carries and how inputs reach a
kernel. This prototype demonstrates that each input can be prepared separately,
in bounded tiles, before a kernel selected by one execution width. Arbitrary
frames are compatible with that architecture; they need not introduce another
logical type or a binary dispatch axis.

My recommendation is to keep the shared logical design, use independent input
preparation to contain kernel combinations, and decide frame selection from
workload evidence. The restricted frames here are straightforward and preserve
byte buffers, but they give up substantial compression for constants and narrow
ranges far from zero. The arbitrary-frame prototype convincingly demonstrates
those benefits. Neither prototype yet removes all user-visible width decisions.

## Representation and semantics

`Value::Int(Integer)` has `Shape::Int`. Its physical encodings are:

| Encoding | Decoded value | Intended use |
| --- | --- | --- |
| Packed bits | 0 or 1 | Compact boolean-valued integer columns |
| Unsigned native 8/16/32/64 | lane | Nonnegative values, including the full `u64` range |
| Biased native 8/16/32/64 | lane − 2^(width−1) | Negative and mixed-sign values |
| Native `i128` | lane | Checked escape for ranges the native frames cannot hold |

The biased native encoding is CORGI's existing signed order encoding, viewed as
a base plus unsigned delta. For two's-complement input, XOR with the sign bit
produces these ordered lanes. It is not a new sort order. Kernels that sort one
native column reuse the unsigned radix machinery directly.

Construction chooses the narrowest permitted representation from measured
bounds. Arithmetic can retain conservative bounds and a wider representation;
`compact()` explicitly rescans and repacks. Equality, integer hashes, structural
comparison, and shape are independent of this choice. Tests include the same
value isolated, mixed with negative values, and mixed with `i128::MIN`.

The numeric domain in this prototype is bounded `i128`, not arbitrary precision.
Binary arithmetic uses checked `i128` when necessary. At the graph surface an
overflow becomes the existing `Fail<Integer>` sum, per row, and participates in
effect lowering through maps and fold back-edges. The direct Rust column API
returns `Result`. N-ary addition checks the left-to-right intermediate sums in
the wide escape; cancellation beyond `i128` is not supported.

`Prim` remains useful for bytes, encoded floats, and existing word operations.
Text retains the existing byte-list interface, including fixed-stride lists for
fixed-size byte records. `Integer::from_bytes(Arc<Vec<u8>>)` and `to_bytes()` share
the allocation for a zero-frame byte column. Other integer layouts convert with
a checked 0..255 bound. Bytes 128..255 retain their usual nonnegative values.
`integer_signed` imports CORGI's already biased signed order keys.

Ordinary integer arithmetic has no width parameter. Explicit wrapping arithmetic
does: `wrapping(other, op, width)` truncates operands and results modulo 2^width,
regardless of their storage layouts. That distinction is necessary for hashes
and other word algorithms whose width affects the answer. A storage width must
not silently select an arithmetic modulus. Text encoding and byte interpretation
similarly belong to the interface or operation.

## Execution without an operand-layout cross-product

For add/subtract/multiply, a plan uses cached input ranges and conservative output
bounds to select one execution encoding that can hold both inputs and the result.
Each operand independently gets a prepared reader. A matching native layout
borrows its slice; another layout decodes and shifts into a 1024-element tile.
The selected kernel runs on the prepared slices. Frames are runtime shifts and
a runtime bias, not additional kernel instantiations.

There are five execution lane types: `u8/u16/u32/u64/i128`. Decode functions are
specialized for a source layout and execution lane type; this still has a finite
conversion matrix. What disappears is a separate arithmetic kernel for each
combination of every operand's width and frame. `Integer::sum(&[&Integer])` uses
the same readers with runtime arity. Its scratch space stays bounded independently
of row count and arity, apart from the list of reader descriptors.

The tile arrays use at most 32 KiB for a binary wide operation. Outputs are fresh
allocations. This spike deliberately does not implement unique-buffer reuse.
Native arithmetic dispatches the operation above the element loop and extends
an exact-size iterator into the output; dispatching and pushing within the loop
initially prevented the compiler from producing a fast dense kernel.

This execution policy has a cost. Every source must fit the execution frame, even
if the result has a much narrower range. Full-range unsigned values combined with
negative operands therefore select the `i128` escape here. Arbitrary operand
frames can instead calculate directly on small deltas. Widening two inputs also
requires tile conversion in this implementation; the other spike can fuse the
widening into its input-width/output-width kernel.

## Where the prototypes differ

| Question | This spike | `corgi-int-spike` |
| --- | --- | --- |
| Logical integer shape | Width-independent | Width-independent |
| Native frame | 0 or −2^(w−1) | Arbitrary `i128` base plus bounded unsigned deltas |
| Column spread | Any spread within `i128`, using the escape | At most `u64::MAX` |
| Constants | Stored as bits or native lanes | Width 0, no payload; add a constant by shifting base |
| Boolean-valued columns | Packed 1-bit payload | Byte deltas, except constants |
| Storage | Per-width native vectors | Shared `u64` words, typed views through `bytemuck` |
| Byte import | Can share `Arc<Vec<u8>>` | Copies a byte allocation into word storage |
| Message decode | Copies payload and measures range | Supports shared word-backed windows |
| Differing input widths | Per-source tiles into one execution encoding | Whole-column re-encoding to a common input width |
| Arithmetic specialization | Execution lane type | Common input width × output width |
| N-ary addition | Same readers, runtime arity | Separate binary and fold implementations |
| Output packing | May narrow stored-wide operands during execution | Output width is at least the common stored input width |
| Product sorting | Existing machinery; integer fields not packed together | Packs integer fields by significant span bits |
| Wide failure | Per-row effect at graph surface | No corresponding integer effect lifting; representation limits return errors |

Useful examples are `[-1, 254]` and `[2^50, 2^50 + 255]`: each has a byte-sized
spread in an arbitrary frame, but needs 16-bit and 64-bit lanes respectively in
the restricted frames. Conversely, `[-1, u64::MAX]` has spread 2^64 and is rejected
by the arbitrary-frame prototype; this implementation uses `i128` lanes.

Both wire formats use leaf tag 6 with different headers. They are incompatible.
No cross-spike wire compatibility is implied by the shared logical shape.

## Measurements

Raw results are in `integer-preparation-baseline.csv` and
`integer-spikes-comparison.csv` beside this note. The latter comes from one shared
Rust harness and two small adapters, not the other spike's earlier benchmark
tables. Apple M4, arm64, rustc 1.96.0, release profile; median of seven calls after
one warmup. Inputs are retained, bounds established before timing, and output
allocation/drop is included. The frame adapter clones retained Arcs, so its
unique-owned input reuse path is excluded. Forced-64 inputs carry the same tight
bounds in both implementations, using the other spike's codec to construct them.

These are warm repeated microbenchmarks with regular input patterns. There is no
cache eviction, hardware-counter attribution, or production workload. Differences
in output width are intentional and recorded. Sorting comparisons also inherit
other differences between the branches.

The baseline on 1,048,576 rows gives 0.057 ns/row for matching byte add versus
0.262 for the existing fixed-`u64` CORGI add, with 1 MiB versus 8 MiB output.
Mixed byte/64-bit preparation takes 0.311; preparing two stored-wide inputs takes
0.468. Thus compact storage can deliver the expected benefit, while repeated
conversion can consume it. Native biased lanes remain efficient. Packed bits
save input bytes but add takes 0.674 because it unpacks both inputs to bytes.

Single-column sort is 5.651 ns/row for the adaptive 16-bit column versus 5.008 for
the raw 64-bit column. Both use 64-bit sort scratch and a 64-bit permutation.
Narrowing the source alone did not accelerate that workload. The other spike's
range-based product-key packing is a separate, promising optimization; this
spike does not reproduce or independently validate its reported product-sort gain.

The common-harness comparison confirms that arbitrary frames store the far-away
narrow-band add in byte lanes and handle constant add without traversing rows.
This prototype needs 64-bit lanes for the former and scans the latter. The other
prototype also wins clearly when two 16-bit inputs produce 32-bit results: its
widening kernel avoids our preparation tax. On mixed byte/64-bit inputs, this
prototype instead produces a byte result while the other retains 64-bit output.
Selected common-harness results on 1,048,576 rows (ns/row):

| Add workload | This spike | Arbitrary frames | Output payload: this / other |
| --- | ---: | ---: | --- |
| Matching bytes | 0.043 | 0.050 | 1 / 1 MiB |
| Byte + stored 64-bit | 0.257 | 0.378 | 1 / 8 MiB |
| Two stored 64-bit, small values | 0.442 | 0.296 | 1 / 8 MiB |
| 16-bit inputs, 32-bit result | 0.430 | 0.080 | 4 / 4 MiB |
| Narrow band near 2^50 + small values | 0.269 | 0.030 | 8 / 1 MiB |
| Near `u64::MAX` + negative values | 2.204 | 0.030 | 16 / 1 MiB |

None establishes a universal winner. The last two cases especially combine
packing and execution differences; they are not equivalent-width kernel tests.

The common harness also probes `i128::MAX + 1` and shifting
`[i128::MAX−1, i128::MAX]` by 1. This spike rejects overflow. At the compared
revision, the other spike returns success in release: its constant fast path
uses unchecked `shifted`. That path needs an explicit policy and checked bounds
before treating it as exact bounded-integer arithmetic. This is an implementation
edge case, not an inherent problem with arbitrary frames.

## Coverage and unfinished work

The implementation covers integer add/subtract/multiply, n-ary addition, explicit
word wrapping, min/max, relational and structural comparison, radix/native sort,
dedup, find, merge survey, gather/append/blend/fold rebuilding, hashes, codec,
shape inference, and the ML surface. For example:

```text
(input lit_int 250, input lit_int 10) int_add       -- 260, through Fail<Integer>
(input lit_int 250, input lit_int 10) wrap_add 8    -- 4, through Integer
```

The existing test suite passes with all features. Eleven new integration tests
pass in release with all features, covering layout pairs against independent
checked arithmetic, multiple full tiles and a tail, n-ary layout permutations,
hash identity, mixed-sign boundaries, byte buffer sharing, structural operations,
codec corruption, per-row overflow, and width-changing fold state. Clippy passes
for all targets and all features with warnings denied. Both common-harness
adapters check all successful add values and sort permutations outside timing.

Remaining limitations matter before adopting this as the default integer layer:

- Indices, counts, masks, and several existing reductions still require raw `U64`.
  The other spike also leaves indices/counts at `U64`. Making user programs fully
  width-free requires the bridges or migrations for these interfaces.
- Multi-source gather, blend, and fold rebuilding currently materialize `i128`
  values and repack. They need encoding-aware movement before judging performance
  of iterative workloads. Native single-source gather preserves the encoding.
- Bit and wide sorts use comparison sort. Packed booleans need direct bit
  kernels if frequent arithmetic or predicate work is expected.
- Imports, compaction, and codec reads scan bounds; kernel timings exclude those
  costs. Conservative bounds may also retain unnecessary width.
- Division, shifts/bitwise operations on abstract integers, general reductions,
  arbitrary precision, and negative integer literal syntax are outside the spike.

I would next combine the common logical shape with explicit byte/word interfaces,
add zero-width constants, and benchmark arbitrary frames through the independent
reader interface. Keep conversion selection independent of operation kernels,
but permit a small set of fused widening kernels where measured conversion costs
justify them. Then measure mixed-width fold/gather and multi-field sorts. Those
workloads will decide whether arbitrary frames should be the normal packing
policy or an optional encoding, more usefully than a signed/unsigned type choice.

## Reproduction

From this worktree, with Cargo on PATH:

```sh
cargo test --manifest-path corgi/Cargo.toml --all-features
cargo test --manifest-path corgi/Cargo.toml --release --test integer --all-features
cargo clippy --manifest-path corgi/Cargo.toml --all-targets --all-features -- -D warnings
cargo bench --manifest-path corgi/Cargo.toml --bench integer
python3 corgi/benches/compare_spikes.py /path/to/other-worktree --rows 1048576
```

The comparison script builds temporary, separate Cargo projects and does not edit
either checkout. `cargo bench ... -- --full` adds 8M-row cases; `-- --smoke`
checks a small dataset. The comparison script accepts `--cargo /path/to/cargo`.
