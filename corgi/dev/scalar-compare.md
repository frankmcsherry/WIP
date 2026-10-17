# Allocation-free scalar structural comparison

`arrange::compare_at(a, i, b, j)` called the bulk indexed comparator with a
single pair. That allocated sign and index scratch at each structural level;
mixed byte/i64 comparison could widen an entire byte column for one pair.
The scalar path now borrows and compares just the requested elements, walking
products, lexicographic lists/references and tagged sums directly. Float
columns compare their existing total-order keys. Bulk APIs are unchanged.

This refreshes the idea from commit
[2e4acfa](https://github.com/frankmcsherry/WIP/commit/2e4acfa2ed4134be5baf4dd583163c9bc0a5b8d0)
and [PR #20](https://github.com/frankmcsherry/WIP/pull/20), which was closed
without merging. It updates that approach for current signed/mixed-width
integers, float storage, list ordering and references. It is based on current
master, rather than reopening the older implementation unchanged.

From `corgi/`:

```sh
cargo test --features serde
cargo run --release --example scalar_compare_bench -- 100000 11
```

Existing bulk comparison tests also check the scalar result against their
structural reference. Additional cross-pair coverage includes mixed storage,
NaN/negative zero/infinity keys, and list/reference views.

Historical Apple M4 control: System allocator, equal values, 100,000-row
columns, three alternating runs with eleven timings each. Mixed storage uses
1,000 comparisons into those columns to bound the slow baseline.

| Shape | Before ns/pair | After ns/pair | Before allocations/pair | After |
| --- | ---: | ---: | ---: | ---: |
| Integer | 15.79 | 2.47 | 1 | 0 |
| Pair | 68.61 | 6.54 | 5 | 0 |
| List of eight pairs | 238.69 | 61.19 | 12 | 0 |
| Sum | 88.24 | 4.40 | 6 | 0 |
| Mixed byte/i64 | 10,287.67 | 2.46 | 3 | 0 |

[Raw samples](scalar-compare.json) were collected on DDIR's Corgi pin 41e8731.
Comparison and primitive-storage source files are identical at that pin and
this branch's master base 0c7f869. The mixed-storage result specifically exposes
whole-column widening in the old scalar path; it is not a general query ratio.

Copy the example into the parent checkout to make a baseline binary, build
both sides before alternating runs, and keep the timing and allocation passes
separate. This is a scalar-boundary control, not a reason to replace bulk sort
or merge kernels with scalar loops.
