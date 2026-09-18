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
| **corgi `62`** (per-anchor searches) | 1.7 s → **0.96 s** | 570 → 320 ns per arc |
| **corgi `63`** (every search a merge) | **0.80 s** | 265 ns per arc |

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
