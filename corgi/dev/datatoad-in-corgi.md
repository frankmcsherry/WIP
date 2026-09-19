# How much of datatoad can corgi reproduce? (2026-09-18, branch `corgi-ref`)

Single-threaded datatoad only; the timely exchange (`comms.rs`) is out of scope.

## The one worked example: the WCO triangle

`programs/62-wco-triangle.col` is datatoad's README triangle — `arc = (0,k) ∪ (k,0) ∪ (k,k+1)`,
`tri(a,b,c) :- arc(a,b), arc(b,c), arc(c,a)` — written the way collie's `examples/17`/`18` write it:
group both adjacency directions, hold the inner lists by reference, capture them into every anchor
arc at one thin ref per anchor, pick the smaller side per anchor (`branch` + `match`), clone it (the
WCO budget), and binary-search each of its elements in the larger side through the reference
(`find`). Count = 3(N−1). `cargo run --release --example wco -- 1000000`:

| engine | N = 1 M, triangle rule | notes |
|---|---|---|
| collie `17` (view + row intersect) | 0.25 s | 0.11 s build + 0.14 s join |
| collie `18` (LFTJ lanes, `def`) | 0.39 s | |
| datatoad (`.time` around the `tri` rule) | 0.94 s | includes the `Permute-1-0` index copy and the LSM dedup |
| **corgi `62`** (per-anchor searches) | 1.7 s → 0.96 s → **0.75 s** | 570 → 250 ns per arc |
| **corgi `63`** (every search a merge) | 0.80 s → **0.58 s** | 193 ns per arc |

(The last column of each is after rebasing onto master's `Tags::Const`, `per_row_try` Fail plumbing and the indexed sort — about 0.2 s of the remaining time had already been fixed upstream while this branch was measuring it.)

The first number was 4–7× collie and 1.8× datatoad. Profiling (`examples/wco_stages`, samply →
pollard) put 1.1 of the 1.7 s in `find`: the lockstep binary search collected a comparison vector
per round (a third of all samples) and ran lower and upper bound as separate forty-round passes,
and the program probed 3N random positions of the key list twice. Two engine fixes (compare and
update in one loop over the live needles; resolve the upper bound by scanning the equal run past
the lower bound) took 62 to 0.96 s. A program restructuring (63) then made every search a merge —
anchors visited in a-group order, so the a-positions are one sorted `find` over the group keys and
each group's b-positions are a `find` whose needles are the group's sorted b list against the
broadcast key list (`fwd.0 ref` captured into every group) — which the new galloping-merge path in
`find` (sorted needles: exponential + binary search from the previous bound, datatoad's `gallop`)
runs in streaming order: 0.80 s, under datatoad. What remains is the two `group`s (0.25 s, radix
discrimination + gathers; collie builds both indexes in 0.11 s), the per-anchor `Fail` plumbing of
`get`/`filter`/`branch` and the `Prod`/`Sum` re-gathers, and `find` itself at ~2× a Rust
`partition_point` loop (family W).

Two surface gaps showed up writing it, both already on the list: no value broadcast (the per-anchor
adjacency refs had to be built by capture rather than `lit`), and no nested tuple patterns (`let
(a, (b, c)) = …`), so bodies destructure with `let u = t.0 in …`.

**Where 63's 0.58 s goes** (samply → pollard, rebased branch, per rep):

| share | what | note |
|---|---|---|
| ~23% | the searches (`merge_lower` + `run_ends`) | 6 M two-element needle lists galloping into 1 M-element rows, plus 3 M b-positions per group; ~180 M steps, memory-bound. Inherent to this graph's degree skew. |
| ~28% | the two `group`s (`sort_block`, `sort_keys`, `pull_u64`) | two 3 M-row (u64,u64) sorts; collie builds both indexes in 0.11 s vs ~0.17 s here |
| ~16% | `Vec` collects: gathers | `branch`/`match` partition + recombine (`gather_lanes`), `cap_list` of the anchors, `clone` of the small side (the WCO budget), `per_row_try`'s Ok-subset gather |
| ~11% | op-body scalar loops (`Op::eval`/`CmpOp::eval` self) | Find's window setup and rewalk, `len`, mask loops — three or four passes per op over 3–6 M rows |
| ~12% | allocator + memcpy (unsymbolicated libsystem) | one fresh column per intermediate; ~10 ops per anchor |
| ~5% | Fail mask passes, `filter_mask`, `rel` | what is left of the plumbing after `Tags::Const` and `per_row_try` |

Search + sort ≈ 0.31 s already exceeds collie's whole 0.25 s, so collie is not winning on the kernels alone: its `intersect` is one fused op doing what 63's body spells as `get`, `len`, `branch`, `match` (two arms), `clone`, `find`, `map`, `filter` — the ~0.3 s of collects, loops and allocation is the op count. Cheapest next step: drop the `branch`/`match` (a partition and a recombine per anchor) for a refs-level `select` — picking the needle/haystack roles per anchor is a blend of two `Ref` columns, 8–16 bytes per row, which `blend` can do without `gather_lanes`; then the body is one `find` over 3 M anchors. After that it is fusion.

## The map from datatoad's primitives to corgi's

datatoad's data plane is `facts/trie.rs` + `facts/mod.rs` + `rules/exec.rs` + `rules/atoms/data.rs`,
about 2,800 of its 5,700 lines. It is already columnar: a relation is a trie stored as one ragged
`List` per column (layer *i*'s lists are layer *i−1*'s items), relations live in an LSM of such tries,
and joins are done a variable at a time in bulk. Nearly every kernel takes and returns index
vectors. That is corgi's model, so the map is close to one-to-one:

| datatoad kernel | what it does | corgi | cost match |
|---|---|---|---|
| `col_sort` / `sort_terms` (`trie.rs:1148`) | sort + dedup + group, writing back group ids | `sort group` (radix discrimination; `GroupKey` is exactly sort-dedup-group) | ✓ O(n·w) both |
| `union` (`trie.rs:941`) | two-cursor merge of sorted layers | `append sort dedup` | ✗ O(n·w) vs O(n+m): **a merge kernel is missing** |
| `intersection` + `gallop` (`trie.rs:1023`, `facts/mod.rs:619`) | aligned merge-intersect, exponential search on skew | `find` (batched binary search) | ✓ O(m log n); loses the adaptive merge-vs-search choice |
| `advance_bounds`, `expand` (`trie.rs:1116`, `:688`) | list-space → item-space, repeat-by-count | the bounds themselves; `flatten`, `slices`, `cap_list` | ✓ |
| `retain_lists` / `retain_items` / `filter_items` | masks propagated across layers | `filter` + `len`/`fold_any` per row | ✓ |
| `permute_subset` (`trie.rs:345`) | reorder columns = re-sort | `map (p -> (p.1, p.0)) sort group` | ✓ (datatoad also re-sorts) |
| `join_cols` (`trie.rs:433`) | n-way prefix-aligned join | `find` + `slices` (family E1) | ✓ asymptotically; 6× constant |
| `retain_inner` (semi/antijoin) | intersect to a mask, propagate | `find` then `lo lt hi` (or `eq`) mask, `filter` | ✓ |
| count phase (`data.rs:58`) | degree of each prefix in each atom | `find` gives `(lo, hi)`; degree = `hi − lo` | ✓ — no materialization, as in datatoad |
| count-notes argmin (`exec.rs:236`, `data.rs:107`) | per row, the atom with the fewest extensions | elementwise `lt`/`select` over the k count columns | ✓ |
| shard by winner (`exec.rs:258`) | partition rows by winning atom | `branch k` into lanes | ✓ |
| propose + validate (`exec.rs:291`) | extend by the winner's list, semijoin the rest | `slices` **by reference**, then `find`-masks | ✓ — this is program 62's body |
| `Rc<Layer>` sharing, `make_mut` | layers shared across base/permuted/salad | `Ref` (the arena is `Arc`'d; `clone` is the copy) | ✓ now; was the copy before Ref |
| logic atoms `:range`/`:plus` (`logic.rs`) | code-backed relations, bidirectional | per-row `map` with arithmetic; `:range` delve = `iota` per row + `flatten` | ✓ |
| variable-width `Terms` + width upgrade | byte strings, specialized at runtime | `Prim` widths 8–64; `List<U8>` for the ragged case | ✓ |
| LSM `push`/`tidy` (`facts/mod.rs:257`) | merge-until-geometric | host loop over `append sort dedup` | ✓ modulo the merge kernel |
| `advance`: `recent = to_add antijoin stable` | the semi-naive delta | one corgi program per round | ✓ |
| `State::update` fixpoint loop, rule skipping | the outer `while` | **host** | — |
| `plan_body` variable ordering (`plan.rs:251`) | BTreeMaps over variable names, per rule | **host** (compile time) | — |
| memory-budget chunking (`trie.rs:536`) | data-dependent `while` over running sums | host loop, or drop | — |
| early exits (`retain_core`, all-keep/all-drop) | skip work when a mask is trivial | drop (bulk evaluates the mask) | constant only |
| non-stratified negation | antijoin reads a moving relation mid-round | host loop semantics; the corgi program is per round | — |

## Verdict

**Data plane: yes, essentially all of it, at matching asymptotics with one exception.** The trie
layers are `List`s, every kernel is one of sort-dedup-group, gather, bounds propagation, or search,
and the WCO step is exactly what `Ref` + `slices` + `find` now express (program 62, family W). The
exception is `union`: datatoad merges two sorted layers in O(n+m) and corgi would re-sort, O(n·w)
with the byte-radix. For the LSM that is a factor of the key width per merge, not a blow-up, but a
sorted-merge kernel (two-cursor, the `find` sibling) is the one primitive worth adding for this.

**Control plane: stays in the host.** The semi-naive round structure, the delta bookkeeping across
rules, the LSM tidy policy, the variable-order planner, and the memory-budget chunking are all
per-rule or per-round control with maps over names; corgi has no fixpoint and no maps and should not
grow them. Each rule becomes a corgi program generated by the host per (rule, delta atom); the host
runs rounds until no relation has a delta. That is ~1,000 lines of datatoad that would be
re-written as a driver, not ported.

**What corgi would still lack after that**, in order of how much it would show up:
1. A sorted-merge kernel (`union`), so LSM merges are linear.
2. Mechanical capture with `ref`, so the per-rule index lists reach the anchors without the
   hand-written `cap_list` and the host-built broadcast.
3. Constant factors — the 4–7× to collie on the triangle: `Fail` threading through bodies,
   per-anchor materialization, `group` passes. Profile program 62 before choosing.
4. The galloping merge's adaptivity (a merge that switches to search on skew); `find` commits to
   search, which is the right side of the trade for WCO but not for near-equal-size intersections.

Nothing in datatoad's inner join path is row-at-a-time, hashed, pointer-chased, or control-flow
heavy, which is why the map is this clean; the README says it expects to "migrate in the direction
of an array language," and this is what that migration looks like.

## GALEN (2026-09-18): the real test

datatoad's GALEN (`~/Projects/galen.dt`: six rules, `p` and `q` mutually recursive, 1 M input rows,
p = 7,560,179 and q = 16,595,494 at the fixpoint) runs in **10.4 s** single-threaded here. Three
corgi drivers, all reaching the same counts in 33 rounds (`examples/galen*.rs`):

| driver | how partial tuples are held | GALEN |
|---|---|---|
| `galen` | flat rows; binary-join chain, static order | 190 s — rules 4 and 5 built 30–44 M-row intermediates |
| `galen` + WCO step | per row: count the fan-out in both candidates, extend by the smaller, validate | 101 s |
| `galen` + tuple-key `find` fast paths | (engine) keys of two columns had fallen onto the structural batched search | 54 s |
| `galen_trie` | a trie per candidate per stage (re-grouped from rows), layered `find` | 58 s → 44.5 s once the redundant `sort` before every `group` went |
| `galen_salad` | the SEED's trie walked, never re-sorted; candidates attached as refs at their key layer; WCO step at the deepest layer | 60 s → 56 s (output rows carry the seed prefix only) → 42 s (`Refs` behind an `Arc`) |

**Where `galen_salad`'s 42 s goes** (samply → pollard): sort family ~36 % — almost all of it the
output side: `sort dedup` of 211 M raw derived rows (11.6 s, 55 ns/row, 9× the final volume) and
the LSM merges (2.9 s) — plus the seed `group`s and the index builds; `Vec` collects 16 %
(filters, the `cap_list` descents, gathers); the per-item `find`s 12 % (5 s — one merge per item,
a two-element needle list galloping into a million-element row; datatoad's intersection pays the
same); op bodies, Fail masks, memcpy under 4 % each. Rule evaluation is 22 s and contains no sort.

**What each step taught.**
1. A static join order cannot evaluate GALEN: the per-row choice is necessary (rules 4, 5). The
   count-and-choose step is ~60 lines of driver over existing ops.
2. `find` fell off its leaf fast paths on two-column keys; the lanes view fixed it, and the right
   fix is `survey_groups`' layered merge. In the salad driver every key is ONE seed column, so the
   question does not arise: column-independent, as datatoad.
3. The trie driver's 60 % sort was `sort group` ordering every column; `group` alone is the
   key-only discrimination.
4. The salad driver's first profile was 24 % memcpy: `Refs` were plain `Vec`s, so every clone of a
   reference column (and `eval` clones for every shared edge) copied them. `Arc` them like leaves
   and tags: 55 → 42 s. Also: a per-key list of refs carried by value multiplies under the descent
   — everything per key must be ONE fat ref.
5. Left-fold `append` of k outputs is O(nk) memcpy; a balanced tree is O(n log k).

**What remains, at 4× datatoad.** The output side (14.5 s) is now the largest single item: corgi's
head projection scrambles the order the salad produced, so the dedup is a full multi-column sort of
9× the final volume; datatoad emits grouped by the salad's prefix and dedups layer-wise. Inside the
rules (22 s) the finds are 5 s and the rest is pass count: each attach is ~8 column passes over the
items, each descent replicates every carried column, each lane kernel is ~6 passes — one fresh
column per pass. That is the fusion tax with no algorithmic difference left.

## Kernel by kernel: where corgi and datatoad differ, and why (2026-09-18)

Both engines are layer-wise. On GALEN they run the same algorithm — semi-naive rounds, the same
delta convention, per-row count-and-choose for the three-atom rules, tries intersected a column at
a time — and the two profiles say precisely where the 4× (42 s vs 10.5 s) is.

**datatoad, samply → pollard, 10.6 s:**

| share | function | what it is |
|---|---|---|
| 57 % | `lsb_paged` (paged LSB radix sort, u32) | the sort behind everything below |
| 54 % total | `join_cols` | the extension: intersection, `expand`, then `sort_terms` of every emitted layer |
| 29 % total | `permute_subset` | `align_to` / `prune_to`: the salad re-sorted into an atom's key order or the target order — layer-wise, over the trie |
| 9 % | `union` | LSM merges (a two-cursor merge, linear) |
| 8 % | `retain_inner` | the semijoins (validation) |
| 6.6 % | `intersection` | the count phase's merges, galloping |
| 5 % | `expand` | repeat index vectors by list ranges |
| 2 % | `retain_items` | the shards |
| ~1.5 % | memcpy | |

So datatoad is ~75 % sort and ~3 s of everything else.

**corgi `galen_salad`, per program text, 42 s** (28,555 program runs, 4.07 G input rows summed —
**10 ns per row per program**):

| secs | runs | rows | ns/row | program |
|---|---|---|---|---|
| 11.6 | 72 | 212 M | 55 | `input sort dedup` — the round's derived rows |
| 4.8 | 2098 | 180 M | 26 | `find` — the broadcast attaches and the antijoin-free lookups |
| 4.0 | 1151 | 738 M | 5.4 | `append` — pieces of output, LSM |
| 2.8 | 35 | 30 M | 94 | `append sort dedup` — LSM merges |
| 2.3 | 418 | 666 M | 3.4 | antijoin: `find` + `eq` mask + `filter` |
| 1.8 | 171 | 9.5 M | 185 | index build, inner `group transpose` |
| 3.7 | ~500 | ~290 M | 12 | the seed and index `group`s |
| 1.4 | 4196 | 360 M | 3.9 | `gather` — refs down the layers |
| 1.2 | 25 | 3.6 M | 344 | the WCO lane kernel (clone, find, filters, nested cap_list) |
| 0.6 | 3016 | 94 M | 6.8 | `filter` |

corgi: ~15 s sort family (36 %), ~27 s everything else. Against datatoad's ~7.5 s sort and ~3 s
everything else, the sort gap is 2× and the non-sort gap is **9×**. That is the finding.

### The differences, one by one

1. **Non-sort work is nine times more expensive, and it is pass count, not kernels.** 4.07 G
   row-passes at 10 ns each is the whole 42 s. Each corgi program is several ops, each op one pass
   allocating a fresh column. A count phase is ~8 corgi passes per (candidate, batch) where datatoad
   does `intersection` + `advance_bounds` + one write into the notes; a descent replicates every
   carried column where datatoad `expand`s one index vector; the lane kernel is ~6 passes where
   `join_cols` is one. The kernels themselves are at parity: corgi's `find` merge and datatoad's
   `intersection` both gallop; corgi's `gather` at 3.9 ns/row and `filter` at 6.8 ns/row are
   memory-bound. Nothing here is a wrong algorithm. It is the cost of expressing one datatoad
   kernel as five to eight corgi ops with no fusion between them.

2. **Index vectors versus values.** datatoad's data plane passes *index vectors* into immutable
   layers and reads data through them lazily; nothing is materialized before the output sort. The
   salad driver already had the tool for this — a `Ref` column IS an index vector into a layer —
   and used it for the candidates' sub-tries, but still carried the seed *values* and one `(mask,
   ext, extras)` triple per (candidate, batch) as columns replicated down each descent. The lean
   form is one thin ref per item into the seed trie and one thin ref per (candidate, batch) into the
   index's key layer, resolved at the deepest layer by a gather. That is a driver change, not an
   engine one.

3. **The output is sorted flat where datatoad sorts layer-wise, and on 8-byte terms where datatoad
   uses 4.** 212 M raw rows at 55 ns is a full three-column sort and dedup per round. datatoad's
   `join_cols` emits its output layers already grouped by the salad's prefix (it inherits the
   trie's order) and `sort_terms` refines within groups; the LSM `union` is then a linear merge.
   corgi's head projection reorders the columns and hands the dedup a flat list. The fix has two
   parts: emit nested (`List<(x, List<z>)>` grouped by the salad's leading variable) and dedup per
   group with `map (l -> l sort dedup)` — a segmented sort, which the surface `sort` per row IS —
   and choose, per rule, an output order that shares the salad's prefix (datatoad's planner sets
   "stage output order by demand from later stages plus the head"; when it cannot, datatoad pays
   `prune_to`, a permute, which is 29 % of its profile too). 4-byte terms halve the sort's bytes;
   corgi's kernels are width-generic but the `find` fast paths added this week are u64-only, which
   is why `GALEN_U32=1` runs *slower* (57 s) — a gap between the width-generic claim and the
   practice, to close by generating those paths through the `prim!` macro like the rest.

4. **Mutable notes versus ids.** datatoad's argmin across atoms is an in-place overwrite of a
   4-byte notes column aligned with the salad's items. corgi has no scatter, so alignment costs
   either an id sort (the trie driver) or carrying the candidates' columns down the trie (the salad
   driver, item 2). The salad walk is the right answer: it needs no ids at all, because the
   attach happens at the layer where the key lives and rides down with the structure. datatoad's
   `align_to` per atom is the price it pays for not doing this: 29 % of its time is permutes.
   Here corgi's structure is *better* — it never re-sorts the salad — and it still loses on the
   constant.

5. **In-place buffer reuse.** `retain_items` refills the same `VecDeque`; `col_sort` writes group
   ids back into its caller's slice; `expand` rewrites the index vector in place. Every corgi pass
   allocates its output. This is the same fact as item 1 seen from the allocator: 738 M rows
   appended, 360 M gathered, 94 M filtered, each into a fresh `Vec`.

6. **LSM batches multiply columns.** datatoad's count phase folds every batch of an atom into the
   same notes column (`counts[delta_idx] += ...` over `other_facts`); the extension then joins each
   batch and concatenates. The salad driver attaches one `(mask, ext, extras)` triple per (candidate,
   batch) and runs the lane kernel per batch pair. Summing counts into one column per candidate and
   keeping one thin ref per batch would cut the carried columns by 3×.

### What it says about corgi

The surface expressed every datatoad kernel, and the only additions the week needed were `Ref` (so
a list row can be named rather than copied), the tuple-key and merge paths in `find` (which
`survey_groups` subsumes), and two host helpers corgi should have as ops: `unnest` (drop a one-row
wrapper) and a value broadcast. Closure capture was worked around by hand everywhere (`cap_list`
threading), which is the mechanical-capture pass again.

The engine difference is not in the kernels. On this workload corgi's sort is 2× datatoad's (bytes
and structure) and its non-sort work is 9× — and that 9× is one number, 10 ns per row per program,
paid once per op for materializing the column. datatoad's kernels are what corgi's programs would
be after fusion. Until then, the lever inside the driver is item 2 (carry indices, resolve late)
and item 3 (emit grouped, dedup segmented), each removing passes rather than speeding one up.

## The iteration log (2026-09-18, `galen_salad`, datatoad 10.5 s)

| iteration | change | GALEN | what it was |
|---|---|---|---|
| 0 | persistent salad, `Refs` behind `Arc` | 42.0 s | |
| 1 | carry (mask, position), resolve refs at the deepest layer | 39.1 s | index vectors, not values, down the trie |
| 2 | layered product-key `find`: field 0 merged, later fields merged per class, upper bounds once per run of equal needles; terms at the narrowest width that fits | 36.8 s | lane-at-a-time replaces the u64 tuple compare; 4-byte terms cut the output sort 11.4 → 7.8 s |
| 3 | pieces concatenated by one multi-source gather | 34.3 s | 738 M rows of `append` copying |
| 4 | linear union (`survey_groups` + gather) for LSM merges and piece merging | 33.9 s | LSM merges 2.0 → 0.8 s; the per-round dedup a wash |
| 5 | `group` linear on sorted keys; the segmented sort folds short blocks' labels into the radix key | 32.6 s | index builds 1.7 → 1.1 s, seed groups 1.4 → 0.5 s |
| 6 | per-item dedup before flatten | 39.7 s, reverted | the 9× duplication is across items and rules, not within an item |
| — | mimalloc, as datatoad | **27.5 s** | one sixth of the run was the system allocator under one fresh column per pass |

Kernel checks along the way, same data (20 M rows of three 4-byte terms, 10 % duplicates):
sort + dedup, datatoad layer-wise `from_columns` 37 ns/row, corgi `sort dedup` 46; datatoad's
whole-row radix 18.5 (not lane-at-a-time, used only on load). The segmented sort within 240 K
groups costs as much as the flat three-column sort (per-field radix either way), so a grouped
emission only pays if the group is free (it now is, on sorted keys) and the extra passes are not
spent — which means keeping the output nested through the LSM: the trie-shaped Forest.

What is left at 27.5 s: the per-round dedup of 212 M raw rows (7.7 s, volume-bound, kernel at
parity; datatoad sorts only the new layers because its outputs stay tries), the antijoin against
each stable batch (5.1 s), the broadcast `find`s at the key layers (4.6 s), unions (2.5 s), and the
walk's own passes. Nothing left is a wrong algorithm or a kernel more than 1.3× datatoad's; the
remainder is (a) the output side not being a trie, and (b) one fresh column per op.

### Iterations 7 and 8 (generalization; the grouped output path)

7. **Generalized driver, self-tested.** A candidate's key may span several seed layers (walked
   as successive index layers, an absolute position carried down and the next layer's children
   gathered through it), any number of atoms compete per item (k-way argmin, each lane validating
   against every other extending atom and crossing all extras), atoms bound by the seed alone
   validate, indexes are Ref-layer tries to any depth. `GALEN_SELFTEST=1` checks seven rule
   shapes — binary with extras, two-layer key, two and three candidates (one with a two-layer
   key), validation-only with leaves, leaves in the head, recursion — against a naive evaluator on
   random relations. GALEN unchanged: 28.6 s. Not general yet: one stage per rule (an atom whose
   variables are not all bound after the stage would need the stage's output as the next salad —
   the natural continuation of the persistent trie), and the extension variable is chosen by
   most-constrained-first without datatoad's per-row `log2` count buckets.
8. **Grouped emission with a segmented sort, measured, not a win.** When the head's leading
   variables are the seed's leading layers (195 of 297 rule-seed evaluations on GALEN), the rows
   leave the kernel grouped on them; `group` is then linear and only the remaining columns sort
   within groups — datatoad's "sort only the new layers." On GALEN it costs what the flat sort
   costs: 41 ns/row (3 columns, grouped on 1) and 33 (2 on 1) against 35 flat. corgi's sort already
   refines field by field, so the leading field's pass is a third of the work at most, and the
   `map`, `group`, `cap_list`, `flatten`, `map` passes around the segmented sort spend it. Kept
   behind `GALEN_GROUPED=1` as the first half of a trie-shaped output side; the second half (a trie
   LSM with layer-wise union and antijoin) would inherit the same per-pass cost, so under corgi's
   cost model the flat sorted-tuple LSM is not the thing to replace.

## `examples/toad.rs`: the second stage, and the size question (2026-09-18)

429 lines of Rust, against `galen_salad.rs`'s 971. A stage binds one variable: the salad is a flat
sorted tuple list over its bound variables; every atom containing the variable with a bound key is
a candidate; its key is looked up by one `find` (a multi-column key is the layered find inside
corgi) into an index that is a linear `group` off the batch's copy permuted to (key, ext) —
datatoad's `Permute-*` forms; per tuple a ref to the distinct values under the key, the fewest
cloned, each proposal validated by a `find` into the other candidates' refs; the output is the next
salad. The salad is aligned to the first candidate's key per stage (datatoad's `align_to`, here a
key-only `group`). A miss resolves to an appended empty list, so no masks exist anywhere: a tuple
that misses proposes nothing and validates nothing. Eight rule shapes, including a rule that needs
two stages before an atom can validate, agree with a naive evaluator.

What the Rust absorbs, beyond the plan (variable choice, the delta convention, the LSM): union
(`survey_groups` + gather — the merge the surface lacks), one-pass concat (gather_lanes),
nest/unnest of the one-row wrapper, and an empty row of a shape. Broadcasting a per-row scalar to a
row's elements needed no Rust: `(keys len, t) cap_list`.

GALEN: **45.8 s** (galen_salad 28.6 s, datatoad 10.5 s), 346 distinct programs. The difference to
the layered driver is where datatoad spends `align_to`: a permute of the salad per stage and one
index per (key, ext) pair rebuilt from a permuted copy, where the layered walk attached candidates
at the layer their key lives and reused one nested trie per key order across stages. That is a
1.6× price for the general, one-stage-at-a-time form in a third of the code; datatoad pays the same
structural price and wins on constants.
