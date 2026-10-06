# Integer spike review and collaboration brief

Please independently review the two CORGI integer spikes and propose a design
synthesis. Begin with a written assessment of agreement, disagreement, and the
experiments that would resolve the disagreements. Defer porting or merging code
until that assessment establishes which choices are worth adopting.

The user's goal is a consistent integer design that reduces memory traffic and
kernel complexity while keeping integer programs convenient and byte interfaces
natural. These prototypes are evidence toward that decision. Neither is a proposed
replacement to adopt wholesale.

## The user's constraints

The discussion started with adaptive integer widths, as in BQN: users should
normally ask for an integer and let the engine choose its packing. The user
identified three constraints:

1. Avoid spelling widths and making users manage representation changes.
2. Support text as character sequences or byte slices, and fixed-size byte records
   such as `[u8; K]` for hashes and other byte work.
3. Avoid a large cross-product of variants for operations with multiple inputs.

Their preferred direction allows naming a width such as 1/8/16/32/64/+ when
needed, while avoiding a signed/unsigned distinction in the ordinary integer
type. Interpretation might belong to an operation or interface. Sorting should
retain unsigned order keys; byte-oriented APIs should retain ordinary byte values.

They also observed that a base of 0 or −2^(w−1) describes CORGI's existing
unsigned and biased-signed order encodings. Restricted frames could therefore be
a useful reinterpretation of the existing machinery. Arbitrary bases had been
discussed, with concern about complexity. The requested spike was intended to
provide an independent approach for comparison with another spike.

## Revisions and reading order

The implementation under review is commit
`016ef1adc848d638c9952d983056faefa5a05eee`, on branch
`codex/corgi-adaptive-preparation` in `frankmcsherry/WIP`.
It starts from `corgi-ref` at `93af76306948f91b014c1fc1378ddfed77b29aa3`.
This brief is a subsequent documentation commit on the same branch.

The comparison target is `corgi-int-spike` at
`309884ed649c2c66e8c4df1162e8fe160bc8a817`. The remote still pointed there when
this brief was prepared. If you have a newer implementation, identify its revision
and distinguish conclusions that apply to the compared version from current behavior.
The branches have different bases; unrelated engine changes can affect comparisons.

Read these in order:

1. [Design report](adaptive-integer-preparation.md): rationale, coverage,
   comparison, results, and limitations.
2. [Integer implementation](../src/integer.rs): `binary_plan`, `Prepared`,
   `binary_kernel`, `sum_kernel`, and `IntegerOp` are the architectural core.
3. [Integration tests](../tests/integer.rs): the representation-independent
   semantic contracts, including effect lifting and width-changing fold state.
4. [Shared comparison runner](../benches/compare_spikes.py) and its
   [common harness](../benches/comparison/main.rs.in): inspect both adapters before
   interpreting [comparison results](integer-spikes-comparison.csv).
5. The other spike's `corgi/dev/integers.md`, `corgi/src/int.rs`, and
   `corgi/src/words.rs` at the pinned revision.

## What the independent approach actually tests

This implementation adds one logical integer leaf and shape. Native storage has
width 8/16/32/64 and frame 0 or −2^(w−1), alongside packed 0/1 bits and a checked
`i128` escape. Ordinary arithmetic ignores stored width; explicitly wrapping
arithmetic names its modulus. Compatible byte buffers can be shared. Value
equality, hashing, and comparison ignore frame and width.

The main experiment is independent input preparation. One execution encoding is
chosen from input and possible output ranges. Each input supplies a direct slice
or a decoder into a bounded tile. The operation kernel is selected by that one
execution lane type. N-ary addition uses the same readers with runtime arity.
Frames are runtime metadata and shifts, rather than another type specialization.

This contains the arithmetic operand-layout cross-product, but it still creates
source-layout × destination-lane decoder specializations. There is conversion
work, function-pointer dispatch per tile, and output allocation. Please assess
both the code-complexity argument and the cost; neither disappears by naming the
abstraction.

Several other choices are independent axes:

| Axis | Choices to distinguish |
| --- | --- |
| Logical domain | Integers as values; explicit modular words; encoded text/bytes |
| Frame selection | Restricted bases; arbitrary bases; zero-width constants |
| Allocation ownership | Per-width vectors; word-backed views; compatible imported buffers |
| Execution | Per-input tiles; whole-column normalization; selected fused kernels |
| Range knowledge | Exact scans; conservative cached bounds; unknown imported bounds |
| Failure policy | Numeric overflow; unsupported column spread; resource failure |

The two implemented bundles do not establish that all their respective choices
must travel together. In particular, arbitrary frames can use independent readers,
and word-backed storage does not require whole-column normalization.

## Evidence and its intended interpretation

The common harness uses the same datasets, seven-sample medians after warmup,
retained input ownership, and timed output allocation/drop. The arbitrary-frame
adapter clones retained Arcs, so it excludes that implementation's unique-buffer
reuse advantage. Forced-wide inputs have equal tight bounds, constructed through
the other spike's codec. Timings exclude import and bound-measurement costs.

These are warm microbenchmarks on Apple M4 with regular data patterns. The
reported numbers support particular workload observations, not a general ranking.
Successful addition values and sort permutations are checked outside timing.

| Observation | What it supports | What remains open |
| --- | --- | --- |
| Compact matching byte addition beats the existing `u64` baseline | Narrow storage can yield the expected benefit | Gains after import, conversion, allocation, and composition |
| Independent preparation produces byte output from retained 64-bit inputs | Input storage need not dictate output storage | Whether narrowing on each operation is a good total-work policy |
| Our 16-bit to 32-bit addition is much slower than the other spike's | This implementation pays a material conversion tax | Specialized decoding, tile size, or fused widening alternatives |
| Arbitrary frames strongly benefit narrow ranges near large values | Restricted frames lose useful compression | The workload frequency and full cost of richer frame metadata |
| Zero-width constants avoid scans and payload | Constants deserve separate treatment | Checked frame shifts and production integration |
| Packed-bit addition saves input space but loses time to unpacking | Storage compression and arithmetic speed can diverge | Direct bit/predicate kernels and workload-specific selection |
| Our single-column sort does not improve with narrower input | Existing sort scratch limits this benchmark's gain | Significant-bit product packing and other sort strategies |

One important limitation of this implementation is its common *value* frame:
execution must hold both decoded operands and the output. It can promote to
`i128` for near-`u64::MAX` plus negative inputs even when the actual output has
a small spread. The other spike's small deltas avoid that. This limitation is a
property of our chosen planner; independent readers need not impose it. Consider
whether readers can preserve separate operand bases while kernels operate on
common-width offsets and receive runtime correction terms.

The other spike's product-key packing and shared-message decoding are substantive
capabilities this implementation does not match. Our comparison does not
independently reproduce its reported product-sort gains. Preserve those questions
when proposing a synthesis.

## Semantics and boundary cases needing a decision

The `i128` escape here is an experimental bounded domain. It provides a concrete
fallback and failure path, but does not settle the user's proposed width `+`.
Its graph operations produce per-row `Fail<Integer>`; its direct column API
returns `Result`. Wide n-ary addition checks left-to-right intermediate sums.

The other spike supports arbitrary `i128` bases but limits each column's spread
to `u64::MAX`. A column containing `[-1, u64::MAX]` therefore fails construction,
although its individual values fit `i128`. Assess whether representation limits
may affect program success when grouping or batch neighbors change. A fallback
representation, a documented domain, or a distinct resource error may be needed.

The shared harness also finds that the compared revision's constant-shift fast
path accepts `i128::MAX + 1` and shifting `[i128::MAX−1, i128::MAX]` by 1 in
release. Inspect `shifted` and validate both base and upper bound. This is a
specific unchecked-path issue; arbitrary frames themselves do not require it.

Both prototypes leave many indices and counts as raw `U64`. Our integer leaf
also has slow rebuilding for mixed-source gather/blend/fold. Those remaining
interfaces and movement paths are central to achieving a width-free user model
and assessing iterative workloads.

The two codecs reuse tag 6 with different layouts. Treat their wire encodings as
incompatible experimental formats. Hashing is value-based within each integer
implementation; the spikes do not promise identical hash algorithms across revisions.

## Questions for your assessment

1. Which conclusions do you independently agree with, and which do you dispute?
   For each disagreement, identify a counterexample, source location, or measurement.
2. Does independent preparation materially simplify n-ary kernels, or merely move
   comparable complexity into conversion and planning? What would you actually keep?
3. Can arbitrary frames retain their arithmetic and constant advantages while using
   independent readers? Which operations need additional runtime metadata or a
   small family of fused kernels?
4. What storage ownership best serves both byte imports and shared message views?
   Can the interface accommodate both without multiplying logical integer kinds?
5. What should ordinary integers promise about overflow, column spread, batch
   neighbors, and hashing? Which width decisions belong to explicit word interfaces?
6. Which interfaces must change to remove widths from normal user programs,
   including counts, indices, fold state, text, and fixed-size byte records?
7. What are the smallest experiments that would decide the remaining choices?
   Include mixed-width folds/gathers, constants, large-base narrow ranges, and
   multi-field sorts; specify correctness checks and benchmark ownership conditions.

The desired deliverable is a written comparison, a proposed architecture with
its invariants, and a short ordered experiment plan. Mark each conclusion as
established by current code/tests, supported by these measurements, or still a
hypothesis. Explain any changes to your existing spike's view. If the proposals
converge, show which mechanisms lead to that agreement and which costs remain;
if they differ, preserve the disagreement and name the deciding evidence.

My current preference is a width-independent integer shape, explicit byte/word
interfaces, independent preparation as the default way to contain combinations,
zero-width constants, and evaluation of arbitrary frames within that architecture.
Selected fused widening kernels may be worthwhile. This is a provisional view
for you to challenge, especially where the measurements expose our conversion tax.
