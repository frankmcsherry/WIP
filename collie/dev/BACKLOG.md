# Backlog

**The truth about what's left.** Read this first each session; update
before ending. No item should live only here — each links to either a
Task #NN (concrete deliverable) or a FOLLOWUPS §X (architectural
sketch).

Status conventions: `pending` (not yet started), `in-progress` (active
this session), `done` (close out and move to bottom each session),
`parked` (deliberately not now).

---

## Active

### C. Stage C — output-liveness-directed kernel selection

The vision (Frank): ops have several outputs; pick the kernel by *which
outputs are live*. `uniq` / `counts` / `uniq -c` are one op projected by
liveness; `sort` / `sort.perm` are two projections of one `(value, perm)`
sort; `group` has `(keys, list)`. One rule — "elide dead outputs,
specialize the kernel to the live set" — covers all of them, and it's the
thing the `SystemOp` factorization was built to enable.

- **Seed landed:** `sort_seq(order, things, want_labels)`. When the labels
  output is dead, its computation is skipped (Prim fast-path
  `prim_run_labels` scan and Prod-packed labels loop). `sort` (SortPoly)
  passes `want_labels = false`; callers consuming the run structure pass
  `true`. Kernel-level proof of the mechanism — bench 8 `sort` ~9.5ms
  (was ~10–13ms with the dead scan).
- **Generalization (the real stage C):** the graph already tracks
  per-output consumer counts (`use_counts` in execute.rs). Thread that
  live-output mask into a *select* stage that chooses the kernel per term
  — subsuming `want_labels`, `sort`-vs-`sort.perm`, the `uniq`/`counts`
  family, and `group`'s outputs. Needs: ops exposed as genuinely
  multi-output, and a selector reading the mask. (`eliminate_dead` prunes
  whole terms today, not individual dead outputs.)

### T. Sort-like op consolidation onto the engine

- **`group` landed:** rerouted off the private `sort_perm_by_key` +
  `run_bounds` onto the engine (`sort_blocks` → perm + run-labels, then
  bundle vals by the labels). Now (a) supports **structurally rich keys**
  — Prod/List/Sum, not just Prim (`group_by_structured_prod_key`) — and
  (b) faster: added a top-level single-block Prim fast path to
  `sort_blocks` (`sort_prim_perm_top`: counting u8 / radix u32-64), so
  bench 3 (GROUP BY) ~5.3ms = **0.6× rust** (was ~11–12ms on the pdq
  path). Grouping is by *equality* so it's interp-independent; `group`'s
  `interp` is now vestigial (order-only). All example `group` use is
  `group.u64` — no behavior change.
- **`unique` landed:** rerouted onto the engine (sort → first-of-each-run
  from the labels); now polymorphic over any shape (`unique_structured_prod`),
  interp vestigial.
- **`Sort` (typed) landed:** rerouted to the **swizzle idiom** —
  `swizzle(enc)` → `sort_bytes_radix` → `swizzle(dec)`. Validates the
  swizzle ops end-to-end (`sort_f64_totalorder` still green) and gives the
  typed sort the fast value-radix instead of pdqsort-by-key.
- **`sort_perm_by_key` + `run_bounds` deleted** — no callers left after
  group/unique/Sort moved onto the engine. Two interp-aware kernels gone,
  subsumed by engine + swizzle.
- **`group` / `unique` are now interp-free bare tokens.** Dropped the
  vestigial `interp` from the structs and `SystemOp` variants (grouping is
  by equality); registry registers bare `group`/`unique`; all example
  sites + bench rewritten (`group.u64`/`group.u8` → `group`).
- **`sort.bytes` op removed.** It was `sort` restricted to a flat Prim,
  and `sort` already delegates to the same kernel there — strictly
  redundant (no example used the token). The op (struct, registry,
  `SystemOp` variant) is gone; the **kernel `sort_bytes_radix` stays**
  internal (used by `sort`'s Prim path and the typed sort's swizzle).
  Surface sort family is now just `sort` (data) + `sort.perm` (indices),
  plus `enswizzle`/`deswizzle`.
- **Typed `Sort` (`sort.<interp>`) retired.** Removed the op, registry
  token, and `SystemOp::SortTyped`; order-aware sorting is now the
  user-space `enswizzle.<i> sort deswizzle.<i>` (the `swizzle_sort_*`
  tests verify float order). Example 15's `sort.u64` → bare `sort`.
- **`run_layout` extracted** (`sort.rs`): the labels→run-structure
  projection (`bounds_ends` + `firsts`), now shared by `group` and
  `unique` instead of each walking the labels inline. The shared
  substrate the run-shaped ops project from.
- **Next (op-level unification):** a `runs` op (`SEQ<T> → SEQ<LIST<T>>`,
  sort + self-group into equal-runs) would back `unique` (first-of-run),
  `uniq -c` (run lengths = `count`), with `group` the keyed/payload
  variant. Design call — weigh whether the op-level primitive is worth it
  over the current `run_layout`-sharing.
- **Sort-adjacent (consume sorted data; not cleanup):** `intersect`
  (sort-merge/gallop), `search` (gallop into a sorted target) — the
  merge/search half of the family. Future unification: if inputs were
  swizzle-encoded, their merges could compare byte-wise (interp-free).
  `gather` is the shuffle — sort-expressible (the deferred "gather is a
  sort" optimization).

### Why the perm-based sort stays (durable — don't re-litigate)

The recurring temptation is "make `sort_seq` value-carried everywhere and
retire the perm core." It doesn't fully collapse, for two distinct reasons:

- **Composition currency.** The perm (`sort_blocks → (perm, labels)`) is how
  the recursive core reorders *sibling* columns: sort a Prod's field 0, then
  reorder fields 1.. by the same order = gather-by-perm. The value-carried
  fast paths (`sort_bytes_radix`, `sort_prod_packed`) are leaf optimizations
  that fire only when a whole subtree packs/byte-sorts; anything beyond that
  (nested, or rows > 64 bits) composes via perm. The perm is contagious along
  the path: needed at any node ⇒ needed on the path between it and where its
  order lands.
- **Lists specifically.** We sort *by* a list (an immutable, opaque key —
  like sorting words without reordering letters), not its contents. Reordering
  variable-length rows is a scatter; the cheap realization carries a fixed-size
  row proxy through the refinements and materializes once — that proxy *is* a
  perm. So List is exactly where a (localized) perm legitimately survives.
  ENUM/PROD can go perm-free; List cannot.

**Investigation (this session): the current List sort is already linear.**
`sort_list_blocks` is length-stratified + per-position, touching each
`(row, pos)` once → `O(Σ len)`. Probe (held total ~2M elems, swept list len L):
ns/elem flat-to-decreasing in L for `List<u64>` (random *and* shared-prefix
worst case ~within 1.3×), and `List<Prod[u64,u64]>` ~2.5× constant (∝ field
count), still flat in L. No quadratic, no whole-list recopy. So the
transpose / value-carried-chunk rewrite is a **constant-factor cache win, not
an asymptotic fix** — optional, workload-gated, perm path stays regardless.

**`Stride` fast-path (optional, when a workload points at it).**
`BoundsRepr::Stride{stride}` already asserts fixed arity = `List<T> ≡ Prod[T;
stride]`, but `sort_list_blocks` flattens it to generic starts and ignores it.
Recognizing `Stride` (and runtime length-uniform blocks) → dispatch into the
Prod kernel (value-carried, perm-free; ordering is unambiguous since all
lengths equal). collie has no Prim-level analogue of datatoad's `[u8]`-as-u32
problem (widths are native), so this is the only place the idea applies. Not a
rewrite; a localized dispatch.

### `sort.rs` is now structured by approach (orientation for readers)

`sort.rs` reads top-down: module doc (with a "Two approaches" key) →
**user-facing ops** (`sort`, `sort.perm`, `register`) → **`sort_seq`** (the
value-returning dispatcher) → three submodules, then tests. The submodules
make the value-carried/perm split explicit: `mod perm` (discrimination core —
returns `(perm, labels)`; `sort_blocks` + the `sort_*_blocks` + MSD pair-radix),
`mod value` (byte/packed kernels — return `Prim`/`Value`; `sort_bytes_radix`,
`sort_prod_packed`), `mod runs` (labels→run-structure helpers). Re-exported via
`pub use perm/value/runs::*` so external `crate::ops::sort::X` paths are
unchanged. Tell at a glance: `Vec<u64>`/`labels` return ⇒ perm-based;
`Prim`/`Value` return ⇒ value-carried.

### S. Swizzle-encoded byte sorting → in-place shuffle

- **Landed this session:** `enswizzle.<interp>` / `deswizzle.<interp>`
  (order-preserving bit flips: sign-bit for signed ints, IEEE monotone
  for floats, identity for unsigned) and `sort.bytes` (value-reordering
  unsigned byte sort on a flat Prim, no interp). The idiom
  `enswizzle.T sort.bytes deswizzle.T` sorts signed/float columns
  correctly without putting interp in the type system; the "order-form"
  invariant lives with the (eventual type-aware) emitter, same trust
  level as `+.f64`-on-u64. All three are first-class `SystemOp` variants.
  `sort.bytes` is currently a correctness-first `sort_unstable` on the
  backing words.
- **Engine reshape landed:** `sort_seq(order, things) -> (sorted
  things, order/group labels)` is now the data-returning engine face
  (principle 7); `sort` (`SortPoly`) routes through it, radix fast path
  preserved. `sort.perm` is marked the random-access escape hatch. The
  perm + `gather` is still the *internal* mechanism — the steps below
  replace it with in-place distribution under this unchanged contract.
- **Measured (idle machine, 1M u64, bench 13):** value radix beats
  pdqsort everywhere — scrambled full-width ~0.74–0.84× (1.2–1.35×
  faster), small-range (%1000, 2 effective bytes) ~0.52–0.55× (~2×).
  Never loses at idle; under memory contention its bandwidth appetite
  (8 passes + 2nd buffer) compresses the full-width margin. This is the
  floor step 3 must defend once payload/co-distribution enters.
- **Next — make sorting look like in-place shuffling (discrimination):**
  1. *Leaf radix.* Replace `sort.bytes`'s `sort_unstable` with an
     MSD/LSD byte radix that ping-pongs the value buffer (reorders words
     directly, no `(key,pos)` payload, no gather). Interp-free by
     construction — swizzle already did the encoding. Verify vs
     `sort_unstable` at all widths. This is the fast f64 path.
  2. *Value-shuffle vs perm.* `sort_prim_perm_top` builds `(key,pos)`
     pairs → perm → `SortPoly` gathers. When only sorted *values* are
     wanted (no payload), skip the pos payload and the gather, radix
     values in place (step 1). Keep `(key,pos)` only for `sort.perm` and
     payload-carrying compound sorts.
  3. *Compound: dispatch on shape.* **Fixed-width-Prim Prod landed
     (`sort_prod_packed`).** Sort the row "as one thing": pack each row
     into a single key (`order` label most significant, then fields in
     order, each sized to its *actual value range*), LSB-radix the keys,
     and DECODE the sorted keys back into the sorted columns. No index, no
     gather, no per-field labels — one radix + a decode. Falls back to
     perm+gather when a field isn't a flat Prim or the key would exceed
     128 bits. Validated vs perm+gather on data + labels
     (`prod_packed_matches_perm_gather`, incl. a u128-key full-range pair).
     **Measured (idle, bench 9/14, 1M):** packs only into a *u64* key
     (≤64 bits) — wider rows fall back, because a 128-bit u128 key lost
     ~2× to perm+gather. Pack/no-pack decided by cheap max-scans (no
     alloc), so the fallback is free. Prod[u64,u64] small-range
     (`%7,%100`): collie ~7.8ms vs perm+gather ~22ms (**~3× faster**);
     full-range: falls back, parity. (bench-14 Rust baseline omitted —
     the loop-invariant closure gets LICM-hoisted and reports bogus
     ~2.7ms times; compare vs perm+gather instead.)
     - *Why "as one thing":* a fixed-width tuple's whole row fits one key,
       so you skip per-field threading entirely (that was the ~82ms
       mistake — and the earlier `u128` hi/lo packing wasted ~9 empty
       passes on the zero byte-gap). Actual-range sizing keeps small-range
       keys to ~2 passes.
     - *Remaining (the non-packable shapes — dispatch the OTHER way):*
       tuples with a variable-length / sum field (`(u32, Vec<u8>, Enum)`),
       List (length-first), Sum (disc-first) — these can't pack into a
       fixed key, so they get datatoad-style discrimination: pack the
       fixed-width prefix, recurse / thread labels through the complex
       field. Still on perm+gather today.
     - `last` optimization is **liveness-gated** (skip returned labels only
       when the sort's labels/permutation output is dead — a `want_labels`
       signal from the caller, not a kernel assumption).
  - **Throughline:** dispatch on shape — pack-and-decode fixed-width rows
    (no index/gather); recurse/thread for variable/complex shapes. `gather`
    survives in `sort.perm` and the not-yet-converted complex shapes.

---

## Next

Prioritized order. Top is what to start with on a fresh session.

### 0. Apply the SEQ→SEQ operator recipe across the catalogue

- **What:** The operator-shape recipe (PRINCIPLES, Engineering
  policies) says every op has one signature `SEQ<T_in> → SEQ<T_out>`;
  internal dispatch on representation is fine, but polymorphism
  between *element types* (flat `SEQ<T>` vs per-row `SEQ<LIST<T>>`)
  is a violation. Several ops violate today:
  `reduce.+/*/min/max`, `any`, `all`, `cumsum.<i>`, `shift.<i>`,
  `intersect.<i>`, `search.<i>` — each dispatches at the op level on
  whether input is flat or List.
- **Why deferred:** The right cure (lower atom-form to
  `enlist <perrow> unlist`) needs a *shape-aware elaboration* step
  between parse and typecheck — the parser doesn't know shapes, so
  unconditional desugar breaks when input is already a `List<List>`.
  This dovetails with FOLLOWUPS §11 (surface→IR elaboration) and
  FOLLOWUPS §0's `RowAccess` (per-row kernels need to read `View<Prim>`
  inner values to match today's flat-path View-aware optimizations).
- **Prerequisite:** Teach per-row kernels (`fold_runs`, `cumsum_runs`,
  `shift_runs`) to read `View<Prim>` inner values. Self-contained;
  enables the desugar without performance regression on the
  `filter → reduce` mask-streaming path.
- **State this session:** Recipe articulated in PRINCIPLES.md;
  `length` (the atom-form alias for `enlist count`) **retired** — the
  SEQ-first `count` is the only token, flat total is `enlist count`.
  Example 07's `each` dropped onto the segment-aware reduce/count path.
  Broader sweep (reduce/any/all/cumsum/shift/intersect/search) still
  deferred pending the elaboration step.

### 0c. Segmented sort — expose the per-row form (SEQ-first)

- **What:** The engine's `sort_blocks` already sorts *within* groups
  (the recursive core refines per-block). But the only surface sort is
  the flat `sort` (`SEQ<T> → SEQ<T>`); there is no `sort.segmented`
  (`SEQ<LIST<T>> → SEQ<LIST<T>>`, sort within each row). So per-group
  sorting is only reachable via `each { sort }` — per-row interpreter
  dispatch over a kernel that is already segmented internally.
- **Recipe form:** per the SEQ→SEQ recipe, the segmented form is the
  primitive and the flat form is the lift: `sort ≡ enlist sort.segmented
  delist`. (Mirror of `count` being segmented with `enlist count` the
  flat lift — same shape, opposite default historically.) Need a
  `delist`/`unlist` to match.
- **Consumer:** example 15 (`grouped each { sort reverse 3u64 take }`),
  the last `each` in the curated examples. Fully retiring its `each`
  also needs per-group `take K` (segmented head) and per-group reverse
  (or descending sort). Land segmented sort first; it's the one whose
  kernel already exists.
- **State this session:** Identified, not started. `sort_blocks` is in
  `src/ops/sort.rs` (`mod perm`).

### 0b. Trie-walker kernel — shared substrate for polymorphic survey ops

- **What:** A per-layer batched driver that maintains a stateful
  `bounds: &mut [(usize, usize)]` (one range per prefix-equivalence
  class still in play) and exposes layer-level primitives:
  `advance` (push bounds down one layer of a value's shape),
  `intersect_with(other_walker)` (paired galloping intersection
  across two walkers), `refine_by_sort` (sort within each prefix
  range to produce new groups), `retain_lists` / `retain_items`.
  The walker's "layers" are the Value-shape stack: outer List
  bounds → Prod fields → Sum disc → lane → inner values.
- **Why:** This is the right shared kernel for `merge`, `diff`,
  generic `search`, polymorphic `sort` (which already implements
  this shape internally as `sort_blocks`), and `intersect`'s
  structured-key form. The naive "row-comparator" framing
  (`cmp_row(a, b, i, j) → Ordering`) is the *cursor-pair* model
  and fights collie's columnar layout — Sum's lane-local-index
  becomes a per-call prefix scan; per-element recursion hides
  batching opportunities. The trie-walker model does columnar
  work batched across N candidate prefixes per layer, matches
  what `sort_blocks` already does, and aligns with the broader
  columnar/ trie ecosystem.
- **Datatoad reference** — the canonical implementation lives at
  `~/Projects/datatoad/src/facts/trie.rs::layers`. The primitives
  worth porting in spirit (renamed to fit collie's naming):
  - `advance_bounds(layer, bounds)` — push (lower, upper) ranges
    one layer deeper.
  - `intersection(layer0, layer1, both0, both1) → (match0, match1)`
    — paired gallop, one range per prefix.
  - `retain_lists(layer, bounds)` / `retain_items(layer, bools)` —
    range / bool restriction.
  - `filter_items(layer, active, other, aligned)` — keep active
    indexes where value equals an aligned probe.
  - `sort_terms(layer, groups, indexs, last)` — group-aware sort
    that produces refined groups for the next layer.
  Atom executors (`src/rules/atoms/data.rs::count`) compose these
  into the per-rule survey logic. The rep flip we just landed
  (N+1 start-offsets) makes `advance_bounds`-style ops trivial —
  two index lookups, no carrier.
- **What lands first:** the walker is best motivated by a concrete
  consumer. The natural choice is `merge` (a single-walker emit
  pattern) or `intersect`-generic-keys (two-walker pattern). Pick
  one, build the walker around it, then bring the others over.
  Don't pre-design the walker abstraction in isolation — let the
  first consumer shape its API.
- **Relation to existing code:** `sort.rs::sort_blocks` is the
  closest existing thing to the walker — same layer-by-layer
  refinement, same stateful group structure. Likely the walker
  abstraction lives in a new `src/ops/walker.rs`, with
  `sort_blocks` later refactored to use it (not the reverse).
- **Known specialization the walker should accommodate:**
  *stride-width upgrade.* When a `List<Prim<u8>>` layer has
  `BoundsRepr::Stride { stride, count }` and `stride ∈ {1, 2, 4, 8}`,
  the byte payload is morally a `Prim<u_{8*stride}>` of length
  `count`. Reinterpreting (e.g. via `bytemuck::cast_slice`) at
  walker-advance time unlocks word-level fast paths in every
  consumer: radix sort, paired galloping merge/intersect, hash
  table lookup. Datatoad does exactly this in
  `~/Projects/datatoad/src/facts/trie.rs::sort_terms` via
  `upgrade_hint` + `upgrade::<N>` dispatch (line 1136 onward) —
  the stride=4 case routes to a dedicated `u32_sort` radix
  kernel; strides 1-3 use a generic `col_sort`. Designing the
  walker to surface "rectangular byte layer with stride S" as a
  recognized pattern from day one lets every consumer benefit
  without per-consumer plumbing. Pairs naturally with the
  `fl_squoze` flag from #0a — squeezing a wider Prim down to u8
  with appropriate stride is the producer of this layout.

### 0a. Sorted-order awareness (asc/dsc) — as an OPTIMIZER inference

**Investigated and re-shaped (don't re-litigate the CBQN-flags framing).**
The original idea was CBQN-style advisory bits on every `Value` (`fl_asc`,
`fl_dsc`, `fl_squoze`) set by producers and read by consumers. Two findings
killed that framing:

- **The carrier is expensive, not "a dozen points."** Bits must ride *with*
  the value (sort sets asc, a later search reads it), but `Value`/`Prim` are
  matched/constructed in ~166 / ~148 places and derive `PartialEq`. Adding the
  field ripples through ~150+ sites and forces a hand-written `PartialEq`/`Hash`
  that *ignores* the flags (advisory bits must never affect equality).
- **asc/dsc don't need runtime carriage at all — they're static.** The
  producers are statically identifiable ops (`iota`, `sort`, sort-merge);
  order is preserved by mask/filter/monotone maps and broken by arbitrary
  `gather`/non-monotone arith. So sortedness is a lattice property of the
  dataflow graph. CBQN needs runtime bits because it's a dynamic interpreter
  with no program graph; collie has one.

**Surviving actionable item:** an **order-analysis pass + rewrite rules** in
the optimizer. Seed `asc`/`dsc` at `iota`/`sort`/merge, propagate through
order-preservers, drop to `unknown` at breakers; then rewrite `sort(x) ⇒ x`
when `x` is known-asc, and `search` against a known-asc target → binary
search. No value carrier, no type change. Runtime flags would only add value
for *data-dependent* sortedness (external input sorted by contract); expose
that explicitly (`assert_asc`) if a workload ever needs it. **Next step:**
scope the pass against the existing optimizer surface (`eliminate_dead`/`cse`
host, or the e-graph task) before sizing it.

**`tighten`/`squeeze` dropped.** Its value is *data-dependent* width
narrowing (P64→P8 iff values fit), but collie pins static `Shape` width to
physical `Prim` width (`tc` produces `Prim(interp.width())` and requires
concrete widths, e.g. `reduce.+.u64` demands `P64`). A data-dependent output
width is untypeable without either an unsound type/storage mismatch or
decoupling logical-from-physical width across all ~150 width sites — not worth
it as a lone instance. The type-honest residue (a checked `tighten.W` that
errors if values don't fit) is just a non-truncating `as.W` and gives none of
the *automatic* narrowing that motivated it. Where a column is known narrow,
`as.<w>` already yields the static narrow type and the kernel fast paths fire.
Revisit only if a cluster of features wants logical/physical width decoupling.
- **Reference (if the optimizer pass is built):** CBQN `src/builtins/search.c`
  / `group.c` for the sorted-aware consumer fast paths.

### 1. Front-end language(s) above the IR

- **What:** Build one or more elaborator front-ends that lower to
  the IR. The IR stays as it is — the brutalist machine-friendly
  target. Surfaces are plural, not singular: candidates include the
  surface-IR sketch in `dev/SURFACE.md` (general-purpose, named
  fields / multi-name `let` / aggregate sugar), a relational /
  SQL-flavored layer, a datalog layer. Going long on a single
  unified surface risks a disorderly language; each front-end can
  pick its own idioms and elaborate independently.
- **Why:** The WCO triangle example (`examples/18_wco_lftj_def.col`)
  is the moment of truth. The kernels are tight but the surface
  reads like join-engine assembly — twenty-plus named bindings,
  `detuple.4` shuffling, parameter ordering by hand. The IR is
  fine; the language above it is the bottleneck. A reviewer
  framed this as "tighter than the equivalent Rust, but not where
  array languages are." We agree.
- **Sub-items the front-end work would enable:**
  - **`import "path.col"`** — multi-file `def` sharing. A front-end
    concern, not an IR one. Once a surface language exists, it can
    provide imports; in the meantime each script defines what it
    needs inline.
  - **Schemas with named fields.** `schema Edge = { src: u64,
    dst: u64 }` elaborates `.src` / `.dst` to `.0` / `.1` at parse
    time. Highest single-impact win for relational-shape ergonomics.
  - **Type-parametric `def`.** `def avg.<i> {| col | ... }` stamps
    out per-interpretation variants. Mechanically a parser-side
    template expansion.
  - **Pattern destructure.** `{| (a, b, c) | body }` as sugar over
    `detuple.3 >a >b >c`.
- **State this session:** Not started. SURFACE.md exists as one
  sketch among several possible front-ends.

### 2. First-class quotations (Joy-inspired)

- **What:** Promote `{ body }` blocks from syntactic forms (only
  allowed in body-bearing op positions) to first-class values that
  can be bound, passed to ops, composed, and consumed by higher-order
  ops. A `Value::Program(Vec<Op>)` variant in the value universe.
- **Why:** Adds programs-as-values without a constraint solver or a
  macro language. Higher-order ops (`apply`, `compose`, `dip`)
  become regular ops; `def` gets a runtime cousin (parse-time vs
  runtime binding); the line between control flow and data
  becomes uniform.
- **Design open question — runtime model:** Two flavors worth
  evaluating:
  - *Option A — quotations only.* Keep the current eval loop
    (borrow-and-iterate over `Vec<Box<dyn Op>>`). Add `Value::Program`.
    Body-bearing ops accept it; runtime still does recursive `eval()`
    for bodies. Smaller change. Body-bearing ops remain a special
    category.
  - *Option B — two-stack flip.* Restructure eval into a `todo`-stack
    pop loop (Tada's model: data and todo as parallel stacks; verbs
    that execute programs just push commands onto todo). Body-bearing
    ops disappear as a category — they're regular verbs that push
    quotation contents. Bigger runtime change; uniform dispatch;
    the principle-2 violation in `each` becomes structurally
    visible (the per-iteration loop is an explicit verb).
- **Why this matters now:** ties to BACKLOG #1 (front-end work) —
  quotations are most naturally introduced once a surface language
  exists, and the question of whether to two-stack-flip the IR is
  the kind of architectural decision worth resolving before
  building front-ends on top of the current model.
- **Reference:** Tada (Frank's Joy clone) explores the two-stack
  model directly — `eval` is just "push contents onto todo," `ifz`
  picks a branch and pushes the chosen quotation onto todo, no
  combinators have special interpreter privileges. ~150 LOC for a
  working concatenative semantics including `quine` and Fibonacci.
- **State:** Not started.

### 3. Principled spikes (diagnostic example surface)

- **What:** A second example surface that stretches the design rather
  than reflects it. Candidate: first ~50 Project Euler problems in
  `spikes/`. Each spike should either fit cleanly (validates a
  principle) or push back (surfaces a missing primitive or an
  awkward construct).
- **Why:** Existing examples are pedagogical (what collie can do);
  spikes are diagnostic (where collie strains). Together they're
  the two surfaces a fresh reader needs. Spikes also pressure-test
  principle drift — the silent failure mode where escape hatches
  (`each`, `reduce { body }`) accumulate without anyone noticing.
- **State this session:** Not started.

### 4. Retire SequenceRange — Task #53

- **What:** Stop constructing `Selector::SequenceRange` in `view.rs`;
  switch to `List<bounds, View<Prim, Runs>>`. Then delete the variant
  + dead match arms across the codebase (~10 sites).
- **State this session:** Consumer-side scaffolding landed —
  `RowAccess::Runs` variant in `join.rs` (alignment-checked
  `List<View<Runs>>`) and decompose-view handling for the same shape.
  Construction flip attempted but reverted because the extra
  cumulative-bounds `Vec<u64>` costs ~24MB at 3M anchors and WCO
  regressed 5-10%. The remaining work needs a bounds-lazy List
  variant (compute bounds on demand from the runs Arc) to close the
  alloc gap.
- **Why:** Restores orthogonality of selection vs reshape. The variant
  is documented as transitional.

*(E-graph optimizer pass was Task #50 — rolled back. The exploration
landed `fn arity(&self) -> Option<(usize, usize)>` on `PrimOp` and
implementations across all ops, which is kept as a general-purpose
stack-effect API. The egg dep, `egraph.rs` module, post-parse
optimization pass, and rule infrastructure were removed. The
`where gather → filter` fusion is back in the parser peephole. See
git log around 2026-05-23 for the design notes and barriers found.)*

---

## Smaller, anytime

Discrete items that don't sequence dependently — pick up between bigger
pieces.

- **Segmented `filter` (per-row filter) — the laggard of the SEQ→SEQ
  recipe.** `filter`/`where`/`gather` are flat-only; `reduce`/`count`/
  `cumsum`/`shift` all got per-row (List) forms, `filter` never did. So
  filtering grouped data means flatten → `where gather` → recount survivors
  per group → `cumsum` → prepend 0 → `nest` (the re-nest dance in example
  19's `lftj_lane_g`). A segmented `filter` (`List<T>` filtered by an aligned
  `List<bool>` → `List<T>`, keeping rows) collapses all of that to one op.
  This is BACKLOG #0 applied to `filter`, and #0b (trie-walker) is the
  structural version — *carry the per-group bounds through compute* so the
  flatten/re-nest never happens. Concrete witness: `19_wco_lftj_match.col`.
  A cheaper interim win: `nest`-by-counts (build a List from values + per-row
  counts, no manual `u64[0] … cumsum cat.2` to make bounds).
  **Kickoff doc + step plan: `dev/SEGMENTED.md`** (step 1 = element-wise
  cmp/arith preserve List; step 2 = segmented `filter`; step 3 = rewrite
  `lftj_lane_g`; step 4 = trie-walker). Started: no; next action noted in
  the doc (place the List branch in `cmp.rs`'s flat fall-through).

- **LIST and TUPLE entries in BAKEOFF** — bench collie vs BQN on
  ragged-list and Prod-of-columns workloads where collie's design
  center actually lives. Discussed in conversation; no ticket. ~1
  hour.
- **TPC-H Q3 validation** — 3-way join + group + order/limit. Probably
  exposes a missing primitive or two. ~1 day to write + benchmark +
  analyze.
- **Self-search family** — `∊` (mark firsts), `⊐` (classify / first
  occurrence index), `⊒` (occurrence count), `⍷` (dedupe without
  sort). The "what position did this first appear at?" /
  "is this a duplicate?" family. Complements `intersect`/`search`;
  natural for datalog-style workloads where sort-then-dedupe is
  wasteful. CBQN `src/builtins/selfsearch.c` is the implementation
  reference (with sortedness/squeeze fast paths layered on top of #0a).
- **`merge` — sorted-deduplicating union.** Two sorted columns →
  one sorted-deduped column. Polymorphic over the value universe.
  **Built on the trie-walker (#0b)**: two walkers, paired layer
  advance, layer-level merge that emits min-of-(a, b) per step
  with dedup. Maps to datatoad's `Layer::union` (see
  `~/Projects/datatoad/src/facts/trie.rs::union` around line 941
  and `layers::union` around line 893). Sets `fl_asc` + `fl_squoze`
  on output once #0a lands. Today's `cat.N` keeps duplicates and
  doesn't preserve order; this is the closing primitive for
  sorted-set algebra. Natural first consumer of #0b — single
  emit pattern, no preprocessing needed.
- **`diff` — sorted set difference / antijoin.** Given two sorted
  columns A, B: emit elements of A not in B. **Built on the
  trie-walker (#0b)**: same paired-walker setup as `merge`, but
  emits a only when a < b. Maps to datatoad's `Anti` atom kind
  (`~/Projects/datatoad/src/rules/atoms/anti.rs`). Composable
  today as `search → filter-where-no-match` but pays two extra
  ops and an extra allocation. Best landed after `merge` to
  validate the walker's two-side dispatch shape.
- **`permute.K` — Prod column reorder.** Sugar / surface op:
  `Prod[A, B, C] permute.3 [2, 0, 1] → Prod[C, A, B]`. Lowers at
  parse time to `detuple.K` + N `pickN` ops + `zipK`. Should
  recurse through `List<Prod[…]>` to give `List<Prod[…reordered]>`,
  matching the `.i` projection idiom (which already does the same
  for single-field selection). Cheap and high-leverage — column
  juggling appears in every multi-column pipeline.
- **Reduction overflow semantics — decide and document.** Today
  `reduce.+.<i>` wraps silently at the interp's width. CBQN sums
  integers associatively in blocks "as long as sum can't exceed
  ±2⁵³", widening on overflow. Pick one: keep wrap-silently (matches
  raw machine semantics, current behavior), widen-to-i64/f64
  (matches array-language expectations, costs an output-width
  promotion), or check-and-error (matches Rust integer semantics).
  Document the choice in PRINCIPLES or OPERATORS. ~30 min if no
  code change; ~2 hours if we adopt widening.
- ~~Cumulative scan / running totals~~ — **landed** as `cumsum.<interp>`
  (flat + per-row over Lists). Example 52. Generalization to `scan.<op>`
  (other associative ops like *, min, max) is a follow-up.
- ~~Sessionization / adjacent-row predicates~~ — **landed** as
  `shift.<interp>` (positive shift only, fills with default = 0). Flat
  and per-row variants. Example 53 shows the sessionization idiom
  (`gap = ts - shift(ts, 1); flag = gap > threshold; session_id = cumsum(flag)`).
  Negative shift (LEAD) and per-row variable n are follow-ups.

---

## Candidates worth investigation

Constructs in the codebase that appear to violate a principle but are
retained for now. Not "to do," but on the watch list — every use site
is a data point about whether the construct is load-bearing or just a
shortcut.

- **`each { body }`** — runs body per row. Violates principle 2
  (whole-collection-only dispatch). The body is columnar *within* one
  row, so the cost is per-row interpreter dispatch, not per-element.
  Watch list: every use is a signal we don't yet have the right
  columnar primitive. **Down to one use (example 15).** Example 07's
  `each { .{ length ; reduce.+ ; reduce.max } }` was removed: the
  reduce-family and `count` are already segment-aware on a List, so
  the cleave does the per-group work directly (the `each` only added
  dispatch + a vestigial length-1 `List<Prod>` nesting). Required the
  `length`→`count` swap — `length` was whole-collection (counted rows),
  `count` is per-row. **Example 15 (`each { sort reverse 3u64 take }`)
  is the honest remaining case** — it marks three missing segmented
  ops (per-group sort, per-group `take K`, per-group reverse); see
  "Segmented sort" in Next.
- **`Cleave { paths }`** — runs each `path` against a copy of the
  input value. Dispatch is bounded by `paths.len()`, not by N, so
  it's principle-2-aligned in spirit. But its purpose is not
  self-evident — flagged as unexplained complexity rather than
  unearned. Action: either explain what it's for in `PRINCIPLES.md`
  or remove if it doesn't pay for itself in the curated examples.
- **`Selector::SequenceRange`** — transitional per its own docstring.
  Retirement target is `List<View<Runs>>`. See item #4 (Task #53).
- **`Branch` with `BranchMode::{Strict, Clamp, Filter}`** — one op,
  three modes. May be three different ops in a trenchcoat.
  *(2026-05-23: clamp/filter deleted as zero-caller dead weight;
  only the strict default `branch.K` remains. Watch list closed.)*
- **Adaptive `where`/`filter` (sparse vs dense vs grouped)** —
  CBQN `slash.c` picks between three algorithms based on input
  statistics: sparse (`+´mask` small → enumerate set bits), grouped
  (`+´»⊸≠mask` small → emit run starts), dense (full scan). collie's
  `where`/`filter` are uniform. Wait until a workload pushes on
  either extreme; today the uniform path is fine.
- **Hash-table search for wide-key lookups** — CBQN uses Robin Hood
  hash tables for `search` on 32/64-bit elements (and full lookup
  tables for ≤16-bit). collie's `search` is uniform binary search.
  For wide-element columns with many queries, hashing wins big.
  Coordinates with columnar's `wip/rhh.rs` exploration. Wait for a
  workload that's many-queries-against-stable-target.
- **Fixpoint iteration (`fix { body }` / semi-naive evaluation).**
  Datatoad runs rules to fixpoint with delta tracking; `repeat N`
  in collie is fixed-iteration. A naive `fix { body }` (run body,
  compare output to previous, stop when equal) is straightforward
  but slow. The semi-naive version (track new facts between
  iterations, evaluate the body against the deltas, accumulate)
  needs an LSM-shaped story for incremental accumulation. Worth
  doing only if a datalog-shaped workload pushes on it; until then,
  fixed-iteration plus manual saturation checks (run, compare,
  loop in shell) suffice.
- **Generic `search` over arbitrary value shapes.** Today `search.<i>`
  requires Prim of width `<i>`. **Built on the trie-walker (#0b)**:
  the walker maintains per-query candidate ranges in the target;
  per layer galloping refines each range. Closes the "pack tuple
  keys into a single Prim" workaround idiom. Once #0b lands,
  this is mostly mechanical — a one-walker pattern where the
  "other side" is the query column rather than a second trie.
  Sharable preprocessing with `merge`/`diff`/`intersect` for
  Sum-keyed inputs (lane-local index column).

---

## Parked / future

Real items, intentionally deferred. Each has a reason.

- **§1 — Zero-copy decode (Borrowed Prim variant)** — no current
  workload demands it. Land when a real mmap-based pipeline appears.
- **§2 — Reuse columnar's containers** — explicit "do last" per the
  current roadmap. Trait gymnastics; big change; not urgent.
- **§4 — Outer-join semantics for `group`** — needed for SQL
  completeness. ~1 hour when a real SQL workload pushes on it.
- **§5 — Multi-output run()** — trivial; do whenever.
- **§7 — SIMD bench-1 ceiling investigation** — diminishing return;
  understand-but-don't-fix.
- **§8 — Stride-aware fast paths for count / reduce.+** — memory win
  for rectangular lists. Mechanical. Land when motivated.
- **§11 — Surface/IR split (full surface IR)** — big architectural
  move. The `>name` / `def` work picked up the lowest-hanging surface
  fruit; full split (schemas, named tags, structural destructuring)
  is a multi-week effort.
- **§12 — dd `interpreted/` integration** — real-world test, multi-week.

---

## Done recently

(Last ~10 items. Older history lives in commit messages.)

### AB. Example 19 tidied → renumbered 18; old 18 cut

`lftj_lane` params renamed to role-based `:[a b t_pos p_pos t_adj p_adj]`
(t_*/p_* = target searched / proposer enumerated); the `dup`/`swap`/stack-
leftover in the validation step replaced with explicit binds so `:hit`
reads as dataflow; lines compressed (one line = one complete thought, bind
to untangle cross-line operands); dead `disc` bind → `:[_ …]` (a normal
unused name the graph drops). Output byte-identical, still fully boils.
Then `18_wco_lftj_idiomatic.col` (redundant with the clean def-factored
version) was cut and `19_wco_lftj_def.col` renumbered to `18`; all refs +
counts updated. 18 examples, 114 tests.

Then a **match-based, order-preserving** counterpart was added as the new
example 19 (`19_wco_lftj_match.col`): branch only the positions, each lane
returns `List<c>` per anchor, `match` reassembles into original anchor
order, `(a,b)` attached afterward. Same 2,999,997 triples as 18; pairs with
it to teach flat-bag vs ordered-grouped (datatoad needs order for downstream
joins / deltas). 19 examples. **Surfaced a primitive gap** — see the new
"segmented filter" candidate. — 2026-05-25.

### AA. Binding notation → `:` ; retired `>name`/`name>`/`{| |}`

The binding sigil moved from `>` to `:`, freeing `>` to mean comparison
only. New surface: `:name` (bind top of stack), `:[a b c]` (bind top N,
deepest-to-top — reads forward, kills the reversed-peel order trap), and
`{ … }` as a standalone **scope block** that delimits where a `:`-bind
reaches (so `{| names | body }` became `{ :[names] body }`; `each {| n |}`
→ `each { :[n] … }`; `def name {| … |}` → `def name { :[…] … }`). The
explicit-take `name>` is gone — last-use is inferred and the graph derives
it via use-counting, so it was doubly vestigial. Tokenizer also treats
`,` as a separator (so `:[a, b]` ≡ `:[a b]`). All 19 examples migrated
(outputs byte-identical to baseline); parser arms for `>name`/`name>`/`{|`
removed; tests migrated (`pipeline::binding_clone_and_inferred_take`,
repl `brace_depth`). 114 tests + 19 examples green.

- **Design notes (this thread):** binding is purely front-end — `:`/`:[]`/
  `{}` lower to the same `Let`/`Ref` (boiled to edges) + existing ops; the
  core gained nothing. Two roughness axes identified: output-binding
  (positional `>name` peel → `:[…]` destructure) and argument-marshalling
  (positional op args / `detuple.N` → named args / records, future). `[]`
  means "take apart the stack", `()` means "take apart a tuple". Patterns
  destructure *static-arity* structure only (Prod + stack); List (dynamic
  length) and Sum (`split`/`match`) are out by nature.
- **Deferred — `:(…)` tuple-destructure pattern.** `:(a b c)` = destructure
  the Prod on top into fields (folds `detuple.N` into the bind), nestable as
  `:[(a b) c]`. Dropped for now on a symmetry argument (`detuple :[…]` and
  `split :[…]` are parallel; we didn't sugar `split`, so don't sugar
  `detuple` yet). Add when the `detuple.N` noise warrants it. (Supersedes
  the old `{| (a,b,c) |}` "pattern destructure" item under #1.)
- **Dead weight removed:** the `takens: Vec<bool>` use-after-take tracking
  in `parse.rs` (write-only once `>`/`name>` went) is gone — dropped from
  `parse()`, the `parse_block` signature, all call sites, and both bind arms;
  last-use scoping reads `scope_depth(scopes)` directly. Secondary docs still
  show old syntax
  (BAKEOFF.md, dev/SURFACE.md, dev/ONBOARDING.md, dev/FOLLOWUPS.md) — sweep
  when touched; the canonical refs (OPERATORS/PRINCIPLES/CLAUDE/README) are
  updated. — 2026-05-25.

### Z. `list>ranges` + example 19 cleanup (the Index is the List)

A `List` is already `(bounds, values)`, so a CSR-style "index" doesn't
need a heterogeneous record bundling degs/bnds/vals — those are all
projections of the List. New op `list>ranges` (`List → Prod[View<Range>,
View<Range>]`) exposes the per-row `(lower, upper)` offsets as two
zero-copy windows over the single N+1 bounds buffer (`lower = bounds[0..N]`,
`upper = bounds[1..N+1]`); degree = `upper - lower`, so `count` is
derivable. This recovers the logical length-N relation the N+1 layout
stores by sharing interior endpoints — the offset is the storage trick,
views are the view of it (principle 1 + 4). First-class `SystemOp::
ListRanges`; unit test `list_ranges_are_offset_views`.

Example 19 rewritten: `lftj_lane` now takes the two adjacency **Lists**
and derives bounds/degree/values inside via `list>ranges` + `flatten
drop`, instead of `def graph` precomputing loose degs/bnds/vals columns
threaded 6-at-a-time per lane. The two lanes collapse to
`lane_X detuple.4 [swap] · two Lists · lftj_lane`. **Measured (6× each,
steady state): performance-neutral** — prep ~128ms and total ~432–443ms
for both; the value `flatten` moves from prep into the lanes rather than
duplicating, and `list>ranges` is views + the same gathers (recursing the
Prod) + one subtract, so nothing materializes in the hot path. Confirms
`view.range`/`list>ranges` are genuinely zero-copy. (Earlier single-run
"~3% regression" was a cold first-run artifact — retracted.) Graph fully
boils (92 terms, 0 `let`/`ref`). 114 tests + 19 examples green.
— 2026-05-25.

### Y. `let`/`ref` boiled into graph edges (binding minimized, not removed)

Binding was an opaque barrier: `>name` lowers to `Let { body: rest-of-
block }`, which the graph treated as whole-stack-in/out and ran via
legacy eval — so in flat `>name` programs (the WCO examples) almost the
entire body was invisible to the optimizer. Now `lower.rs` boils `Let`
into a build-time `env: Vec<OutRef>` (mirrors the runtime env push/pop)
and resolves `Ref` to the bound producer's OutRef — no `let`/`ref` term.
Name resolution was already static (`Ref.idx`), and the graph's own
take-on-last-use (execute.rs) subsumes `Ref`'s `take` flag, so the boiled
form needs neither. **Gated, not total:** a `Let` whose body holds an
opaque body-bearing op (`each`/`repeat`/`match`/`cleave`) is kept Foreign
— those bodies run via legacy eval and read the runtime env at parse
indices, so boiled/kept regions never share an env (no re-indexing).
Verified: 17/18/19 now have 0 `let`/`ref` terms (38/84/84 flat terms);
15 (has `each`) keeps its 1 `let`. A/B vs `--legacy`: parity, and the
~4% example-19 conservative-arity gap is closed. No optimizer pass runs
on the flat graph yet — that's the unlocked follow-on (entuple/detuple
round-trip elision, CSE through former binding boundaries). The full
removal of binding is blocked by the same opaque body-bearing ops as
everything else; minimizing is as far as it goes until those move to
quotations/segmented primitives. 113 tests + 19 examples green. — 2026-05-25.

### X. `reduce { body }` removed (unused body-bearing per-row op)

The bare-body `reduce` (`list::Reduce`) mapped a row-collapsing body
over each row of a List (body gets the whole sub-list, must return
length-1) — `each`-shaped per-row dispatch with an output-arity assert,
*not* a fold. Zero example/test uses; the typed `reduce.+/min/max/*`
(separate `reduce_ops` kernels) cover real work. Removed: struct + both
impls (list.rs), the `"reduce"` parser arm, `has_body`/inference
downcasts, the now-orphaned `concat_rows` helper, and doc entries
(OPERATORS, ONBOARDING). `each` is now the sole "remove if a columnar
form appears" escape hatch. Add back if a workload reveals what it was
for. 113 tests + 19 examples green. — 2026-05-25.

### W. `length` retired + example 07's `each` removed (SEQ-first cleanup)

`length` was atom-form sugar (`enlist count`) competing with the
SEQ-first `count` (segmented; rejects a bare Prim). Retired: parser
desugar deleted, 6 WCO sites (`X_uniq length` → `X_uniq enlist count`),
catalogue entries, test repurposed (`enlist_count_is_flat_total`).
Example 07's `each { .{ length ; reduce.+ ; reduce.max } }` → bare
`.{ count ; reduce.+.u64 ; reduce.max.u64 }`: the reduce-family and
`count` are segment-aware on a List, so the `each` was pure per-row
dispatch and also produced a vestigial length-1 `List<Prod>` instead of
flat `Prod[c,t,m]`. `each` now appears in exactly one curated example
(15). Also fixed stale counts in CLAUDE.md/README (83→113 tests, "three"/
"six"→seven principles) and documented the `pipeline/` layer in CLAUDE.md.
113 tests + 19 examples green. — 2026-05-25.

### U. `search` / `intersect` → byte-gallop over order-form keys

The order-based merge/search half of the family is the laggard:
`search.<interp>` (and `intersect.<interp>`) require **Prim keys at a
fixed interp**, and **floats are unsupported** (no total order). Unlike
`sort`/`group`/`unique`, they didn't go polymorphic — because the gallop
hardcodes a typed-Prim `compare(query, target[mid])`.

**Fix (same direction as the rest):** make the comparison a plain
unsigned **byte** compare over *order-form* keys.
- interp + floats dissolve into the encoding (swizzle → byte-gallop),
- compound keys work once they're order-preserving-byte-encoded (the
  same packed-key encoding the `Prod` sort uses) — polymorphism and
  interp-freedom in one move.

**Wrinkle (and feature):** unlike `Sort`, `search`'s target arrives
*pre-sorted from the caller*, so a byte-gallop needs the target already
in order form — i.e. the **swizzled form persists** from the `sort` that
produced it into the `search` that consumes it (the "let swizzled form
persist" decision, paying off across a join pipeline:
`enswizzle → sort → search`, interp only at the boundary). It's a
*contract* shift (callers hold order-form data), not a transparent swap.
Frank's note: search likely has the same **column-at-a-time** structure
as sorting — work it out when we pick this up.

**Worked out + kernel landed:** `search = sort(queries) + galloping
forward merge + unsort`. `search_sort_merge` (join.rs): sort the queries,
sweep a single **monotonic forward cursor** through the target that
`gallop_to`s to each sorted query's lower bound (skipping the gap, never
rewinding), then unsort. So `N·log N` (sort) `+ N·log(M/N)` (gallops),
**interp-free** (unsigned word compare, width-dispatched) and
**sequential** (target in increasing order, ≤ one sweep) — the principle-7
form, same asymptotics as the random gallop but no random probes.
Validated `==` the gallop op (`search_sort_merge_matches_gallop`). Shares
the two-cursor shape with `sort_merge_intersect` — `search`/`intersect`/
joins differ only in the emit (position per query / match pairs / paired
rows). Gallop stays default for now (WCO untouched).
- **Crossover bench (15) — merge wins everywhere.** M=1M, N swept:
  N=1k 0.96× (tie), N=10k 0.49×, N=100k 0.31×, N=1M 0.32×. Gallop is flat
  ~75–81 ns/elem (every query a random DRAM miss); merge drops to
  ~24 ns/elem (sequential, amortizing). No regime where random gallop
  wins → it's subsumed.
- **Pivot started:** `Search`'s unrestricted **unsigned flat** path now
  routes to `search_sort_gallop` (interp-free, sequential). Views / signed
  / floats still use the per-query gallop. All `search.u64` examples
  (incl. WCO flat lookups) green.
- **Per-list (list-shaped) form done too:** `run_per_row` now does
  sort-then-gallop *per inner list* (sort that list's queries, one forward
  cursor over its target slice) instead of a per-query random restart.
  Interp-preserving for now; WCO examples green, no regression. Both flat
  and per-list are now sort-then-gallop.
- **Naming:** it's a **gallop** (sort queries + forward-skipping cursor),
  not a merge. `intersect` is the merge (symmetric two-cursor scan);
  `search` is the asymmetric gallop. Kernel renamed `search_sort_gallop`.
- **interp RETIRED → bare `search`.** Dropped the `interp` from the
  struct, `SystemOp::Search`, registry token, `tc` (now requires only
  equal width), and all error messages; `split_prim_view` no longer takes
  interp; all three paths (flat fast-lane, flat-view gallop, per-list
  sort-gallop) are width/unsigned-byte dispatched; `floats unsupported`
  removed. Signed/float order is now user-space `enswizzle … search`.
  Examples use bare `search`; all green.
- **`intersect` interp RETIRED → bare `intersect`** (parallel to search):
  `sort_merge_intersect` and the per-list intersect are width/unsigned-byte
  dispatched; `interp` gone from the struct, `tc` (equal-width only),
  `SystemOp::Intersect`, registry token; `floats unsupported` removed.
  Examples use bare `intersect`. So the **whole order/merge family is now
  interp-free**: `sort`, `sort.perm`, `group`, `unique`, `search`,
  `intersect` (+ `enswizzle`/`deswizzle` carry interp only as the swizzle
  selector). Signed/float order everywhere via order-form inputs.
- **Flat-View gallop unified (done).** The flat path now `flatten_to_prim`s
  each side (a flat View — Indices/Runs/Mask — gathers here, SequenceRange
  rejected with a clear error) and calls `search_sort_gallop` unconditionally;
  the per-query macro + `iter_queries` + `split_prim_view` are gone.
- **One gallop kernel (done).** Extracted `sort_gallop_slice<T>(target, queries,
  out)`; both `search_sort_gallop` (flat) and the per-list `per_row!` call it.
  Implemented as a shared slice kernel rather than literal `enlist search
  delist` — same single code path, no list-construction/unwrap overhead.
- **Per-list search amortization — TRIED, NOT ADOPTED.** Routed the per-list
  query sort onto the engine (`sort_blocks` with list-membership labels →
  blocks == lists, so it *can* take the packed-label radix). Measured neutral
  on the WCO examples (17/18/19, idle CPU): ~0.50/0.62/0.62 vs baseline
  ~0.50/0.65/0.65. Two reasons it can't help here: (1) queries are **u64**, so
  `sort_prim_blocks_packed` bails (`label_bits + 64 > 64`) and the engine just
  runs the same per-block `sort_unstable`; (2) the extra compaction copy +
  global perm alloc didn't even register, i.e. per-list query sorting isn't a
  meaningful slice of WCO runtime. Reverted — kept the single
  `sort_gallop_slice` kernel. Would only pay for ≤32-bit query keys with large
  lists; no such workload exists today.
- **`Cmp` interp RETIRED → bare `<` `<=` `=` `!=` `>` `>=`.** All four
  cmp kernels (`do_cmp` + 3 Mask fast paths) width-dispatched; `interp`
  gone from the struct, `tc` (equal-width only), `SystemOp::Cmp`,
  registry (bare tokens). **Ordering is unsigned-word; `=`/`!=` are
  bitwise.** Signed/float order via order-form inputs (mandelbrot's
  `<` stays correct — magnitudes are non-negative, noted inline). Float
  `=` is now bit-equality not IEEE (test updated; no example uses it).
  Parser `>name`-vs-`>` disambiguation unaffected (keys on identifier,
  not suffix). All 20 example comparisons + bench green.
- **`Cmp` Indices read-through (done).** Added `do_cmp_both_indices`: when
  both operands are `View<Prim, Indices>` of equal length, compare
  `src_a[idx_a[k]]` vs `src_b[idx_b[k]]` straight into a P8 mask, skipping the
  two gathered columns (fuses the gathers into the compare; the random reads
  happen either way). Mirrors the existing Mask fast paths. Example 19's two
  `gather`s before `<=` are now `view`s; WCO output unchanged.
- **Cosmetic rename (done).** `row`→`list` in join.rs: `RowAccess`→`ListAccess`,
  `is_row_shaped`→`is_list_shaped`, `n_rows`/`row_range`→`n_lists`/`list_range`,
  `run_per_row`→`run_per_list`, locals + comment prose. Surface vocabulary now
  says "list"/"sub-list" consistently.
- **Interp now fully removed from the order/compare/merge surface:**
  sort, sort.perm, group, unique, search, intersect, and all of cmp.
  `enswizzle`/`deswizzle` carry interp only as the swizzle selector.
- **Interp remains (genuinely load-bearing — arithmetic/convert):**
  `+`/`-`/`*`/`/`/`%`, `neg`/`abs`, `reduce.+/min/max/*`, `cumsum`,
  `shift`, `as`, `show`. interp here selects the actual operation
  (signed/unsigned/float math, conversions), not just order — not
  swizzle-retirable. (Also retired the `zipK` alias → `entuple.K`.)
- **Remaining to reach bare `search`:** signed/float via swizzle
  (order-form keys → drop interp from the flat path); views
  (Indices/Runs) → merge; the per-row (row-shaped/WCO) form → per-row
  merge; then retire the `search.<interp>` token for a bare `search`.

### V. Packed-label radix for the segmented (multi-block) Prim sort

`sort_prim_blocks` now keeps **two kernels** and dispatches on average
block size (`PACKED_AVG_BLOCK_MIN = 256`, heuristic/tunable):
- **few/large blocks** → `sort_prim_blocks_packed`: pack `(label, value)`
  into one u64 key (label high, value low), radix the whole column once
  carrying the index ("prepend the index, sort once"). Amortizes; also
  the natural kernel for the enlist'd single-list global sort. Falls back
  if the packed key would exceed 64 bits.
- **many tiny blocks** → per-block comparison sort (small, cache-hot).
A single `find_blocks` scan feeds both the gate and the per-block loop.
Measured: bench 10 (large position-blocks) ~11ms → ~9ms (radix wins);
bench 11 (tiny blocks) stays on per-block (the radix-always path had
regressed it ~2×); bench 12 neutral. Threshold is unmeasured — tune.


- **`SystemOp` operator vocabulary — stack/core factorization complete.**
  Graph `Term`s now hold a `SystemOp` enum (`src/pipeline/sysop.rs`)
  instead of opaque `Box<dyn Op>`. Every pure compute/reshape/structural
  op is a first-class, introspectable variant carrying its parameters
  (interp, arity, op-kind) — the optimizer pattern-matches on these.
  One `as_kernel` match reconstructs the kernel struct; `run`/`tc`/
  `name`/`arity` delegate through it (stage C will later lift kernel
  choice out). `promote()` is the stack front-end's token-op→variant
  map, by downcast during lowering. `Foreign(Box<dyn Op>)` is the
  escape hatch, now holding exactly the intended holdouts: body-bearing
  (`each`/`reduce {body}`/`repeat`/`match`/`cleave`), binding
  (`let`/`ref`), diagnostics (`time`/`profile.*`/`show`), literals
  (`lit.*`). Routing ops never reach `promote` — resolved into edges
  at lowering. 101 tests + 19 examples green.

- **Term graph IR substrate landed and wired as primary eval path** —
  new `src/ir/graph.rs` (~470 LOC). `Term { op, children, n_outputs }`,
  `OutRef { term, idx }`, `Graph { terms, roots }`. Build pass walks
  parsed op stream, hash-cons CSE during construction (side-effecting
  ops `time`/`show.*`/`profile.*` excluded). `eval_graph` walks terms
  in topo order with use-counting + take-on-last-use (preserves
  Arc-1 reuse semantics). Side-table `Vec<Vec<Shape>>` returned from
  build for shape information (not stored in `Graph`).
  Body-bearing ops (`each`, `reduce { body }`, `repeat`, `match`,
  `cleave`, `branch`, `let`) emit single Terms with bodies opaque
  inside the op struct — bodies execute via legacy eval inside
  `op.run`. Conservative dynamic arity for `let`/`repeat` (entire
  current stack in, entire current stack out — body could touch any
  depth) and for `split` (out count derived from input Sum's lane
  count). Added missing `arity()` impls to `ZipN`/`DetupleN`/
  `InjectN`/`PartitionN`/`Proj`. Also: `eliminate_dead` transform
  (forward reachability from roots; side-effecting + body-bearing
  ops kept alive).
  Wired into `main.rs` and `examples_runner.rs`. Legacy eval stays
  available via `--legacy` flag for A/B comparison. 14 graph-specific
  unit tests, 98 tests total (was 84), all 19 examples green.
  Perf A/B on WCO 17/18/19 + Q1: parity with legacy across all four
  (graph slightly slower on 19's scan by ~4%, within the cost of
  conservative Let-arity treatment). Sub-graph promotion for bodies,
  finer Let-arity inference, and the first real graph transforms
  (shape-aware recipe sweep, sort kernel choice) are the natural
  follow-ons. — 2026-05-24.

- **Bounds representation flipped to start+sentinel** — `BoundsRepr`
  now stores N+1 entries `[0, e₀, …, e_{N-1}]` everywhere (was N
  end-offsets only). New API: `iter_pairs()` is the preferred consumer
  interface (no `prev` carrier, no first-row special case). Producers
  use `bounds_var_from_ends` for the legacy "I have a Vec of N ends"
  pattern; the strict `bounds_var` takes the canonical N+1 form.
  Manual padding lines in examples 18/19 (`u64[0] adj_plus list>bounds
  cat.2`) collapsed to `adj_plus list>bounds` (the rep flip eliminated
  the pattern). Examples 12, 16 updated to canonical nest input.
  Touches: BoundsRepr/iter, bounds_as_u64, every per-row kernel
  (count, head, bounds>keys, fold_runs, cumsum_runs, shift_runs,
  reduce body, each, slice_value, concat_values, gather_list,
  merge_by_disc, view of List, RowAccess, xprod, sort_list_blocks).
  Bench within run-to-run noise; 84 tests + 19 examples green.
  CBQN-style advisory flags (`fl_asc`/`fl_dsc`/`fl_squoze`) and
  alternate bounds reps (Empty(n), Singleton(n), …) now cheap to add
  via the iter_pairs / iter_starts abstraction. — 2026-05-23.

- **Dead-weight pass + operator-shape recipe articulated** —
  fresh-eyes audit with kernel-vs-sugar framing. Removed:
  `BranchMode::{Clamp, Filter}` (zero callers; ~75 LOC + parse
  machinery), `Singleton` op (zero callers; doc/code disagreed on
  semantics), `Length` op (zero callers in `count` form;
  trivially `enlist count`), the stale `bounds` alias entry in
  OPERATORS.md (never registered), and the serialization
  round-trip from `demos.rs` (moved to a real `#[test]` in
  `serialize.rs`). Parser desugars `length` → `[Enlist, Count]`.
  `count`/`length` examined and `length` confirmed as sugar.
  PRINCIPLES.md gained an "Operator shape recipe" engineering
  policy formalizing the SEQ→SEQ contract + the
  representation-vs-element-type distinction. Broader catalog
  application (reduce.+/*/cumsum/shift/intersect/search) deferred
  to BACKLOG #0 pending a shape-aware elaboration pass. Tests
  83 → 84 (+1 round-trip), all 19 examples pass, bench within
  noise. — 2026-05-23.

- **Outside-review pass** — fresh-agent review prompted: README pitch
  + perf section dialed down (aspirational kernels, not benchmark
  claims; honest "this is closer to a writable IR than a finished
  array language" framing pointing to example 19 as the moment of
  truth). Principle 4 promoted from candidate to load-bearing ("all
  collections are sequences with optional restrictions"). SURFACE.md
  reframed as one possible front-end among several (multi-frontend
  IR architecture). README brief and BACKLOG Next reordered to put
  front-end work as the #1 next item. `LitNum` bug fixed (float
  literals were truncating to integer part). Module headers
  improved across `ops/*.rs` and `ir/shape.rs`. README sharpened
  with a Prod+Sum+List worked example (shapes). Examples curated
  48→19 with renumbering. Project renamed `colang` → `collie`.
  All 83 tests + 19 examples green. — 2026-05-23.

- **Clarity & simplicity pass (first wave)** — `PRINCIPLES.md` drafted
  with four principles + engineering policies + "candidates not yet
  promoted" footnote (the all-collections-are-sequences observation).
  README "Design principles" trimmed to pointer. Mechanical cleanups:
  removed unused `Id` op, three dead `BoundsRepr` fns, bogus
  `const _: PrimWidth`, `pretty_with_interp`; cleaned two stale
  `PrimOp` docstrings. Unified 10 `LitArr*` structs + `LitArrBool`
  into one `LitArr { tag, prim }`. Examples curated 48 → 19 (deleted
  29 redundant; renumbered 01-19 in teaching order); cross-refs in
  README, FOLLOWUPS, ONBOARDING, demos.rs, view.rs updated.
  Tests 83/83, examples 19/19. — 2026-05-23.

- **Mask-aware cmp + arith (hard cases)** — `View<Mask>` vs
  full-length Prim (paired walk through mask) and `View<Mask>` vs
  `View<Mask>` of equal logical length (two-cursor walk). Closes
  the BACKLOG follow-up from #56. — 2026-05-22.

- **`cumsum.<interp>`** — prefix sum / running total (flat + per-row
  over Lists). Example 52. — 2026-05-22.

- **`shift.<interp>`** — positive shift / lag (flat + per-row over
  Lists; positive n only, fills with default = 0). Example 53
  sessionization. — 2026-05-22.

- **`unique.<interp>`** — sort + dedup via `sort_perm_by_key` +
  `run_bounds` + `gather`. — 2026-05-22.

- **GROUP BY narrow-key fast path** — `sort_perm_by_key` detects
  identity-perm + u8/u16 unsigned and runs counting / 2-byte radix
  directly into Vec<usize>. 1M u8 GROUP BY 12.87 → 5.73 ns/elem —
  ~2× faster than naive Rust (12.13). — 2026-05-22.

- **E-graph optimizer (Phase 1 + thin-slice Phase 2)** — egg 0.11
  integrated. `Lang` enum, 3 starter rules (where-gather→filter,
  same-width cast collapse, flatten-nest cancel), `ColangCost`
  cost function, `optimize_op_sequence` demo. `optimize_program`
  wired into `parse()` pipeline as a post-parse pass; new rules
  go via `try_fuse_window` window table. Parser peephole stays
  in place for inside-nested-block coverage. — 2026-05-22.

- **#53 scaffolding (not flipped)** — `RowAccess::Runs` variant in
  join.rs, `BoundsRepr::Runs(Arc<Vec<(u64,u64)>>)` with
  cumulative-on-demand `.iter()` and semantic `PartialEq`,
  decompose-view handling for `List<View<Runs>>`. Construction flip
  attempted with both eager bounds and shared-Arc bounds — both
  showed ~10% WCO regression vs SequenceRange baseline (from the
  `(u64,u64)` access pattern in `RowAccess::Runs::row_range`).
  Reverted view.rs to construct SequenceRange. Scaffolding remains
  dormant pending a tuned RowAccess design. — 2026-05-22.

- **Task #52 — Sort module (Phase 1 + Phase 2 top-level fast path)** —
  Bundled `sort_blocks(labels, value) → (perm, new_labels)` core,
  recursive over Prim/Prod/List/Sum (Sum only refines by disc; no
  per-lane recursion yet). Polymorphic `sort.perm` and `sort` ops.
  Length-first List ordering. Top-level Prim fast path: u8 counting
  sort 5× win (1M elements 13ms → 2.5ms); u16 LSD radix 1.2×; u32/u64
  fall back to pdqsort (constant-factor parity, label scaffolding
  bypassed). Bench-1M perm: u8 2.5ms, u16 17ms, u32 22ms, u64 24ms.
  Existing `sort.<interp>` still works for explicit signed/float Prim
  sort. **Deferred to follow-ups:** Sum per-lane recursion, Bag/Set
  ordering, MSD radix for u32/u64, List<non-Prim> inners. — 2026-05-22.

- **Task #56 — Mask-aware cmp + arith (easy paths)** — `View<Prim, Mask>`
  + scalar fast paths added to cmp.rs and arith.rs. Streams source via
  mask, no materialization. Chained-filter benchmark: ~10ms → ~7ms (mixed
  cmp+arith) or ~6ms (pure cmp). Real workloads unchanged within noise.
  Both-Views and full-length-Prim cases stay materializing; documented as
  follow-up (now item 4 in Next). — 2026-05-22.
- **Task #55 — View<Mask> + Range retirement** — Combined cleanup.
  Added `Selector::Mask(Arc<Vec<u8>>)` as the new bitmap variant;
  retired `Selector::Range` in favor of `Runs([(start, end)])` (~60 LOC
  refactor). `filter` rewires to produce `View<source, Mask>` instead
  of materializing eagerly. Mask ∩ Mask composition via rank-aware AND.
  Single-filter terminal cases go from 1.6 ms → 0.3 ms (lazy); filter
  + reduce.+ streams through Mask (~1.1 ms at N=1M). Chained-filter
  win partial — cmp ops still materialize between (future: Mask-aware
  cmp). Real workloads unchanged within noise. Final selector enum:
  Indices | Runs | Mask | SequenceRange (last is transitional, #53
  retires). — 2026-05-22.
- **Task #54 — Automatic last-use Ref inference** — Parser pass marks
  the rightmost direct Ref to each Let's bound names as `take: true`.
  Uses trait upcasting (Rust 1.86+) via `Any` supertrait on PrimOp,
  recursive scan of Let/Each/Reduce/Repeat/Match/Cleave bodies.
  Conservative — only marks direct top-level Refs within the Let's
  own scope. Bare-name chain win matches explicit `name>` (~2.5ms at
  N=1M, vs ~6ms without inference). `name>` retained for source-visible
  documentation (compiler verifies it matches inferred liveness).
  — 2026-05-22.
- **Task #51 — `name>` (explicit take)** — Value gains Default impl
  (empty Prim sentinel). Ref gains `take: bool`; `mem::take`s env slot
  on take=true. Parser recognizes `name>` analogous to `>name`,
  enforces use-after-take at parse time. Composes with Arc-1 reuse
  (#49) for ~4× speedup on chained adds with distinct-binding args
  (~6ms → ~1.5ms at N=1M). Auto-inference (#54) is the natural
  follow-up. — 2026-05-22.
- **Selector::Runs variant** added; wired through materializers,
  shape, view ops. No consumer yet — plumbed for future `View<Mask>`
  work and e-graph rewrites. (#53 plans its retirement of
  SequenceRange.) — 2026-05-22.
- **Task #49 — Arc-1 buffer reuse + Storage::extract_arc** —
  Owning-path on Storage trait; arith kernels try Arc::try_unwrap on
  either arg, mutate in place when unique. ~2.5× on chain bench
  (anonymous intermediates). Win blocked by bindings until #51 lands.
  — 2026-05-22.
- **Task #48 — Predicate fusion** — `filter` op + parser peephole for
  `where gather`. ~2.3× on filter-with-values workloads. — 2026-05-21.
- **Task #47 — BQN bake-off** — CBQN built; comparisons documented in
  BAKEOFF.md. Identified the major perf gaps (filter inner loop,
  Arc-1 reuse, narrow-key GROUP BY). — 2026-05-21.
- **Task #46 — `.i` through List** — Proj recurses through
  `List<Prod[A,B,C]>` to give `List<field_i>`. Unblocks the
  cleave-on-grouped pattern. — 2026-05-21.
- **Task #45 — `>name` migration of binding-heavy examples** —
  Examples 41, 42, 46, 47, 48, 49 rewritten in Forth-style flat
  binding form. ~30-40% shorter, top-down readable. — 2026-05-21.
- **Task #44 — `>name` parser fix** — Restrict to valid identifiers;
  excludes `>=`, `>.u64`, etc. — 2026-05-21.
- **`def` parse-time macros** — `def name { body }` and
  `def name {| params | body }`. Block-scoped, late-binding name
  lookups, recursion guard. ~50 LOC in parser. — 2026-05-21.
- **Source restructure** — `src/{ir, ops, syntax, tools}/`; library +
  binary split via `lib.rs`. Made architecture visible in file layout.
  — 2026-05-21.

---

## How to use this file

- **Start of session:** read top-to-bottom. The Next list is the
  prioritized punch list. Pick one (probably #1 unless the user
  redirects).
- **During session:** move items from Next → Active when you start;
  Active → Done when you finish. If you discover a new item, add it
  with a status. If you defer something, move it to Parked with a
  reason.
- **End of session:** verify the file matches reality. Done items
  should reference what landed. Active should be empty (move
  in-progress work to Next with a note about state).
- **The task tracker is the system-level mirror.** Tasks created via
  TaskCreate persist across sessions and show up automatically in
  reminders. BACKLOG.md is the human-readable index over the same
  items, plus the FOLLOWUPS-anchored architectural ones.
