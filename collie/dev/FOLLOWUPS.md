# collie follow-ups

Things I sketched out a path for but didn't finish in this session. Ordered by leverage.

## −1. Convention: sequences by default

**Every value is `SEQUENCE<T>`** where `T` is one of `PRIM | PROD | SUM | LIST`
(the closed inductive content universe). The outer `SEQUENCE` axis is
universal — it's not in the type, it's the iteration container every value
carries. `LIST` is one of the element types, not the iteration axis itself.

So:
- `Prim<u64>` is `SEQUENCE<u64>` (column of scalars).
- A "list-of-u64" value is `SEQUENCE<LIST<u64>>` (column of sub-lists).
- `Prod[a, b]` is `SEQUENCE<(a, b)>`.

**Every op signature is `SEQUENCE<T0> -> SEQUENCE<T1>`** (or product of such
sequences). The outer SEQ never appears in op signatures. Atomic-feeling
behavior is recovered via `enlist` / `unlist`.

**Two layers of "polymorphism" that look similar but aren't:**

1. *Clean rank lift* (acceptable polymorphism): same op, lifted to higher
   rank. The output element type lifts in lockstep with the input element
   type. Example: `search.<i>` accepts `SEQ<T>² -> SEQ<u64>` (flat) AND
   `SEQ<LIST<T>>² -> SEQ<LIST<u64>>` (row-shaped). Both forms produce
   positions; the per-row form lifts the output element from `u64` to
   `LIST<u64>` along with the input lift.

2. *Conflated element-type signatures* (anti-pattern): same op name, but
   the flat and row-shaped forms have *different output element types*.
   Historical example: `intersect.<i>` originally had flat returning
   positions and the row form returning values — different output
   semantics, not a rank lift. Now reconciled: both forms return
   positions in source coordinates (a clean rank lift):

   - `intersect.<i>` flat : `SEQ<T>² -> SEQ<u64> × SEQ<u64>` — matched positions in each input.
   - `intersect.<i>` row : `SEQ<LIST<T>>² -> SEQ<LIST<u64>> × SEQ<LIST<u64>>` — per-row matched positions in each input's underlying source.

**The convention to apply when adding ops:**

- Default to writing the canonical row-shaped form.
- If the op naturally lifts (clean rank lift), make it polymorphic; both
  the flat and row-shaped forms share one name.
- If flat and row-shaped have *fundamentally different output element types*,
  use distinct names.
- For *flat ops applied to atoms*, the user wraps with `enlist` and unwraps
  with `unlist`. These are zero-cost — `enlist` is `nest` with one-row
  bounds; `unlist` is `flatten drop`.

This separates the IR's algebra (one signature per op) from the user's
convenience (atomic-looking applications via enlist/unlist).

## 0. View + row-shaped surveying ops — landed; honest WCO matches datatoad

`Value::View { source, selector }` storage-axis variant with three selector
shapes is now the spine of collie's per-row work:

- `Selector::Indices(P64)` — flat random subset (shape-transparent).
- `Selector::Range { start, end }` — flat contiguous slice (shape-transparent).
- `Selector::SequenceRange { los, his }` — per-row contiguous sub-slices of
  a shared source. Shape is `List<inner>` — looks like a List from outside,
  zero allocation of inner data inside.

**Operator algebra (closed, per axis):**

| | zero-cost intro | work-doing intro | zero-cost elim | work-doing elim |
|---|---|---|---|---|
| product | `zip` | — | `.i` | — |
| sum | `inject` | `partition` | `split` | `match` |
| list | `nest` | `group` | `flatten` | `reduce` |
| view | **`view`** (polymorphic on input shape) | `gather` | **`decompose-view`** | `pop()` materialize |

`view` dispatches on input:
- `(Prim<T>, P64)` → `View<Prim<T>, Indices>`.
- `(List<T>, P64)` → `View<Prim<T>, SequenceRange>` — per-row sub-list-view.

`decompose-view` dispatches on selector:
- `Indices/Range` → `(source, P64)`.
- `SequenceRange` → `(source, List<P64>)` of per-row position iotas in source
  coordinates. Back-projection by construction.

**Surveying ops that are row-shape-polymorphic:**

| Op | Flat form | Row-shaped form |
|---|---|---|
| `intersect.<interp>` | `(Prim, Prim) → (P64, P64)` matched positions | `(row-shaped, row-shaped) → List<T>` per-row matched values |
| `search.<interp>` | `(Prim target, Prim queries) → P64` positions | `(row-shaped, row-shaped) → List<P64>` per-row positions |

Both forms share inner kernels via a `RowAccess` abstraction that walks
per-row sub-slices regardless of whether the input is a real `List` or a
`View<Prim, SequenceRange>`. No per-row interpreter dispatch.

**Empirical scorecard (datatoad triangle stress test, N=1M, 3M-row arc):**

|  | datatoad | collie 42 |
|---|---|---|
| Data load | 112 ms | 85 ms |
| `tri` enumeration | 914 ms | **305 ms** |
| Output | 2,999,997 (a,b,c) triples | 2,999,997 (a,b,c) triples |

**No packed-key encoding, no symmetry trick, no parametric short-cut.**
Builds the unified arc, forward + backward adjacency, two per-anchor
`view` calls (zero alloc), one row-shaped `intersect.u64`, then `count` +
`spread` + `flatten` + `zip3` to assemble (a, b, c). The intersect kernel
runs sort-merge per row in tight Rust without leaving the interpreter.

The 1T-edge materialization that would have happened with eager `gather` is
genuinely avoided — the View source is shared by reference across all per-
anchor sub-list-views.

**Other ops with View-aware fast paths (correctness + alloc savings):**

| Op | When view pays off | Notes |
|---|---|---|
| `where` | View on col | `gather` composes selectors in one pass. |
| `reduce.+/min/max/*` (flat input) | View on a flat Prim | `for_each_prim` walks selector positions. |
| `any` / `all` (flat input) | View on a flat P8 | Same. |

Arithmetic / comparison stay flat (Indices selector would break SIMD).
Structural ops (`zip`, `match`, `inject`, etc.) don't need view fast paths.

**What `view` doesn't help (and shouldn't be expected to):**

- Bind-then-multi-use patterns. A `View` consumed by both `count` and
  `flatten` re-materializes twice. FOLLOWUPS §11's refcount-aware
  materialization would close this; not yet implemented. Workaround: use
  eager `gather` when you'll consume the result more than once.

**Future Selector variants (not yet implemented):**

- `Mask(Arc<Vec<u8>>)` — for `where`-style filtering where the predicate
  has been computed but the gather hasn't.
- `Reverse` — selector that reverses a source's positions.
- `Broadcast(usize)` / `Repeat(usize)` — first-class scalar-broadcast that
  survives bindings.
- `SequenceIndices(List<u64>)` — per-row arbitrary-position selectors (vs.
  `SequenceRange`'s per-row contiguous ranges). Needed for per-row outputs
  that aren't slices, e.g., a per-row `search` result wrapped as a view.

**Demos:**
- `examples/10_view_range.col` — `view.range`.
- `examples/17_wco_list_intersect.col` — **honest WCO without packed keys**,
  built on `view` (polymorphic) and row-shaped `intersect.u64`. Produces
  2,999,997 `(a, b, c)` triples in ~305 ms at N=1M; ~3× datatoad on this
  query.

## 0a. Convention: positions-canonical surveys

**Principle.** When an op's natural primitive is "select a subset of input
rows", expose the *positions* of the selected rows, not the values. Values
are derivable via `gather`; positions are strictly more informative.

**Why it matters.**

1. *Multi-column alignment.* One position vector feeds N parallel columns.
   Computing per-column-value forms throws away the index information that
   would let parallel columns stay aligned.
2. *Composes with `view`.* Positions feed `view` or `gather`
   interchangeably; selectors can chain (compose-then-materialize).
3. *No premature materialization.* Positions are 8 bytes/row regardless of
   element type. Returning values commits to a specific element layout
   and forces an allocation.
4. *Plays with WCO-style queries.* Navigation by index is the natural
   primitive for join algorithms; `intersect`, `search`, `where` all do
   it, and they compose only because they all agree on it.

**Current op status.**

| Op | Currently returns | Positions form? | Status |
|---|---|---|---|
| `where` (flat / row) | positions / per-row positions | yes (canonical) | done — see §39 |
| `search.<i>` (flat / row) | positions / per-row positions | yes (canonical) | done |
| `intersect.<i>` (flat / row) | positions / per-row positions | yes (canonical) | done — see §37 |
| `sort.<i>` | sorted values | **missing** | add `sort.permutation.<i>` |
| `group.<i>` | `(unique_keys, List<vals>)` | **missing** | add `group.positions.<i>` returning `(unique_keys, List<P64>)` |
| `partition.N` | N value columns | **missing** | add `partition.positions.N` returning N P64 columns |
| `reverse` | reversed values | **missing** | add `reverse.positions` (or a `Selector::Reverse` view variant, cf. §0) |
| `take` / `skip` | sliced values | structural — could be `view.range` | cheap as-is; leave |
| `head` | first row | n/a — single value | leave |
| `concat` / `cat.N` | concatenated values | n/a — structural | leave |
| `branch` / `match` | `Sum<…>` | n/a — structural (positions implicit in disc) | leave |
| `flatten` / `nest` | structural reshape | n/a | leave |
| `gather` / `view` | consumer of positions | n/a — sink, not source | leave |

**Top three implementation candidates** (by leverage):

1. **`group.positions.<i>`** — *major.* Today `group` does
   `sort_perm_by_key + gather(vals)`. Returning the per-group *positions*
   instead of `gather(vals)` lets the caller group multiple parallel
   columns by one key without a `zip`/`unzip` round-trip:

   ```
   keys vals1 group.<i>             # current: (uniq, List<vals1>)
   keys vals2 group.<i>             # current: redo the sort

   keys group.positions.<i>         # proposed: (uniq, List<P64>)
   {| uniq positions |
      vals1 positions gather        # apply once per column
      vals2 positions gather
      ...
   }
   ```

   Implementation: ~30 LOC — drop the `gather(&vals, &perm)`, return
   `List<P64>` of the perm bucketed by run-bounds. Don't even need `vals`
   on the stack (input becomes `keys` only). New op, doesn't break the
   existing `group`.

2. **`partition.positions.N`** — *major.* Same idea for lane-routing:
   today `partition.N` gathers each lane's values; the positions form
   would return N P64 columns of source positions per lane. The branch
   pattern in `47_wco_lftj_def.col` uses `branch + split` for a similar
   purpose; `partition.positions` is the cheaper sibling for when you
   want lane membership but don't yet want to materialize lane values.
   ~20 LOC.

3. **`sort.permutation.<i>`** — *moderate.* Returns the permutation as a
   P64. Sorting K parallel columns by one key becomes
   `sort.permutation.<i>` once + `gather` K times, instead of K
   independent (and unaligned) sorts. ~10 LOC: just push `perm` as P64
   instead of `gather(v, perm)`. New op.

**Where positions don't fit.**

- `take` / `skip` are already structural slicing; returning a `view.range`
  achieves the same composition. Could be aliased to `view.range` outright.
- `branch` / `match` route by discriminator; positions are implicit in the
  Sum's lane structure. The information is already there.
- `intersect` row-form COULD have returned values to match the flat form's
  values (the original anti-pattern in §−1); choosing positions for both
  forms restored the clean rank lift.

**Naming.**

Follow the established `search.positions.<i>` / `intersect.positions.<i>`
pattern (where it applies — currently only the canonical positions forms
exist, so `.positions.` isn't a needed suffix). For sort/group/partition
where the values-returning form is the established API, add the positions
variant as `<op>.positions.<i>` and leave the existing op untouched. This
matches the principle: positions form is *additive*, never a breaking
rename.

## 1. True zero-copy decode (Borrowed Value variant)

**Current state:** The serialized format admits zero-copy reads (the bytes are laid out as `bytemuck::cast_slice`-compatible primitive arrays), but `decode()` returns owned `Vec<T>` because `Value::U64(Arc<Vec<u64>>)` only knows how to hold owned data. We `bytemuck::try_cast_slice` to view the bytes and then `.to_vec()` — that copy is the gap.

**The change:** Indirect `Prim<T>` through a sum type:

```rust
enum Prim<T> {
    Owned(Arc<Vec<T>>),
    Borrowed {
        source: Arc<Vec<u8>>,        // keeps the byte buffer alive
        offset: usize,
        len: usize,
    },
}

impl<T: bytemuck::Pod> Prim<T> {
    fn as_slice(&self) -> &[T] {
        match self {
            Prim::Owned(v) => &v[..],
            Prim::Borrowed { source, offset, len } => {
                let bytes = &source[*offset..*offset + len * std::mem::size_of::<T>()];
                bytemuck::cast_slice(bytes)  // panics if misaligned; see below
            }
        }
    }
    fn len(&self) -> usize { ... }
}
```

The pattern matches `Value::U64(x) => ...` stay the same; the internals of `x` change. Every `&x[..]` becomes `x.as_slice()`. Mutating ops need `Arc::make_mut`-equivalent: if Borrowed, clone to Owned; if Owned, `Arc::make_mut` as before.

**Alignment gotcha:** the byte buffer's offset+8 may not be aligned to 8. Two options:
1. Pad the format so payloads start at 8-byte boundaries (changes wire format)
2. Have `as_slice` fall through to a slow path that materializes a `Vec` if alignment fails

Option 1 is the right answer for production; aligns naturally to columnar's existing convention.

**Estimated effort:** ~1 hour. Most of it is updating the ~30 sites that read primitive contents.

## 2. Reuse columnar's containers as the runtime backing

**Why:** The current `Value` is a separate mini-implementation. Switching to columnar's actual containers would inherit:
- `Strides` fast-path for rectangular nested lists (O(1) "is this regular?")
- `RankSelect`-backed Bools (compact storage, fast `where`/`filter`)
- Variable-int compression for usize-typed columns
- Dictionary coding for repeated values
- Bytes-view for zero-copy serialization (gets us #1 for free)

**The change:** Make `Value` parametric over the underlying primitive container types. Each variant wraps a columnar container instead of a `Prim<T>`:

```rust
enum Value {
    U64(columnar::primitive::U64s),    // Vec<u64>-backed by default
    Strided(columnar::primitive::Strides),
    // ... etc
}
```

The hard part: columnar's containers are statically typed by their inner contents (e.g., `Vecs<TC>` where `TC` is the inner container). Our dynamic Value needs to "forget" this. Likely: enumerate the practical combinations as Value variants, or use a trait-object boundary.

**Estimated effort:** several days. The trait gymnastics around `Borrow`, `AsBytes`, `FromBytes` would need careful handling.

## 3. Predicate fusion: `<cmp> where gather` in one pass — *partially landed*

**Original state:** `dup 4 >= where gather` did three passes: compare
(write 1M Bool mask), scan-and-collect indexes (write 0.5M usize), gather
(random-read source). Three allocations totaling ~9 MB on a 1M-row filter.

**Landed (May 2026):**
- New `filter` op (src/ops/list.rs) — one-pass mask-and-write, no
  positions vector.
- Parser peephole in src/syntax/parse.rs: when `gather` is pushed and the
  prior op was `where`, the pair is rewritten to `filter`. Existing
  examples (42, 48, etc.) benefit automatically.
- Bake-off result: filter-with-values goes from ~3.7 ns/elem to ~1.6 —
  closing the BQN gap from ~26× to ~11×. See BAKEOFF.md.

**What's left to close the remaining ~11× gap to BQN:**
- The inner-loop branch (`if mask[i] { push }`) resists autovectorization.
  BQN uses predicated stores / compress instructions.
- See §3a (selector-algebra direction) for the *durable* path that
  subsumes the current eager `filter` — keep the mask attached to data
  as a `View<Mask>` selector, defer materialization through chains.

## 3a. Selector algebra: View<Mask> + composable selector fusion — *partially landed*

**Status (May 2026):** Mask variant landed in #55; eager `filter` rewired
to produce `View<source, Mask>`; Mask ∩ Mask composition via rank-aware
AND. Single-filter terminals are now lazy; filter+reduce streams.
Chained filters still materialize between cmp ops (closing this needs
Mask-aware cmp paths, sketched at the bottom of this section).

**The thesis:** the eager `filter` op landed in §3 is the right *endpoint*
when the caller actually wants a fresh column, but the wrong *durable
representation*. Real columnar engines (Arrow, DuckDB, Polars, datatoad)
hold the mask attached to data and push selection forward as long as
possible; materialization only happens at the boundary where you need
random access or a fresh allocation.

For collie the natural fit is **a `Mask` variant on `Selector`** —
sketched but not yet implemented. The selector algebra (current + planned):

```
Indices         — sparse positions; canonical form, what `where` produces today
Range           — one [start, end); O(1) representation
Runs            — Vec<(lo, hi)>; flat union of intervals (landed May 2026)
Mask            — Arc<Vec<u8>>; bitmap over source positions (planned)
SequenceRange   — per-row sub-slices, rank-lifting (transitional; see #53)
```

**Why this is the durable predicate-fusion path:**

1. *Chained filters compose without materialization.* `x f1 filter f2
   filter` today allocates twice (each filter produces a fresh column).
   With `View<Mask>` and Mask-AND composition, the two masks combine,
   one materialization at the consumer.

2. *Selective ops stay cheap.* `reduce.+`, `count`, `where`, arith — all
   can stream through src + mask in one pass when the input is a
   `View<source, Mask>`. No intermediate column needed.

3. *Materialization heuristic.* When mask density drops below ~5%,
   materialize to Indices (gather is cheaper than streaming through
   sparse mask). The materialize boundary is one branch in the
   `pop_raw → materialize_top` path.

**The complexity discipline (Roaring-style):** keep the selector enum
*small and closed*. Four variants (Indices, Range, Runs, Mask) plus the
transitional SequenceRange. Composition between pairs is N²-ish at
worst; hand-write only the pairs that show up hot, lower everything else
to Indices via the canonical form. This is the same architectural move
Roaring uses with its 3 containers, and it's what prevents the selector
algebra from sprawling.

**Specific composition pairs worth hand-writing:**
- `Indices ∘ Indices` — array of positions composes via lookup.
  (Already done.)
- `Range ∘ Range` — sub-range of a sub-range. (Already done.)
- `Mask ∩ Mask` — bitwise AND when two filters compose. Cheap.
- `Mask ∘ Indices` — gather mask values at indices, returns Mask.

Everything else lowers to Indices first. The e-graph (§50) is the
natural home for the lowering / canonical-form discipline.

**Composition with `SequenceRange`:** intentionally limited. SequenceRange
is rank-lifting (List output); composing it with flat selectors doesn't
have a clean answer. The retirement path (task #53) replaces it with
`List<View<Runs>>`, which composes by composing the inner Runs selector
naturally. Until then, SequenceRange compositions force eager
materialization.

**Why not just add `Mask` now:**

Adding a `Selector::Mask` variant without consumers would be half a job
— it'd plumb through `len`, `to_usize_vec`, `compose_selectors`, but no
op would produce or consume it productively. The win comes from
*coupling* the variant addition with a consumer (`filter` becomes
"produce `View<Mask>` rather than fresh column"), an AND-composition
path, and a sparse-materialization heuristic. Best landed as one
coherent change of ~150 LOC.

**Estimated effort:** ~150 LOC plus benchmarking on the chained-filter
workload (no existing example exercises it; would need to be written).
Subsumes the current `filter` op as the eager endpoint.


## 4. Outer-join semantics for `group`

**Current state:** `group` produces `(K_unique, List<V>)` — no row for empty groups. SQL's LEFT/RIGHT/FULL OUTER joins need a way to express "for each known key in K, return the matching V-list (possibly empty)".

**The change:** Either a separate op (`outer_group <known_keys> <vals> <keys>`) or a post-processing step that materializes empties. The latter requires a "set difference on sorted columns" primitive, which we could add.

**Estimated effort:** ~1 hour including a demo.

## 5. Repeated multi-output programs (multi-return on the stack)

**Current state:** Each demo ends with one value on the stack. Some real queries want multiple outputs (e.g., "give me both the aggregated totals AND the unique keys"). The stack already supports this — just leave more than one value at the end. We just don't show it in run().

**The change:** Update `run()` to print all stack values, not assume single output. Already partially in place; the multi-output case isn't a real change, just exercising it.

**Estimated effort:** 5 minutes.

## 6. A REPL mode

**Current state:** Each demo is a one-shot program with hard-coded inputs. A REPL would let you push values, run ops, inspect intermediate stack state, and iterate.

**The change:** Add a `--repl` mode that reads a line at a time, parses it as a program, evaluates against a persistent stack/env, and prints the stack after each step. ~50 lines.

**Estimated effort:** 1 hour for a polished version (history, undo, error recovery).

## 7. SIMD investigation — bench 1 ceiling

**Current state:** Bench 1 is at 0.94 ns/el (2.7× Rust at 0.35 ns/el). The arith kernel is autovectorized (40 NEON instructions in `bin_arith`). So why isn't it closer to Rust?

Theory: Rust's `iter().zip().map().collect()` for two Vec inputs fits a known fast-path; the SpecFromIter machinery emits an unbranched loop with prefetch. My indexed-write loop should be similar but adds bounds-check elision dependence on LLVM. Worth measuring with `-emit asm` and comparing the two inner loops directly.

**Estimated effort:** 30 min to investigate, possibly small fix.

## 8. Stride-aware fast paths for `count` / `reduce.+` / `each` over rectangular lists

**Current state:** `BoundsRepr::Stride { stride, count }` represents bounds in 16 bytes regardless of row count. But list ops (`count`, `reduce.+`, `each`, `flatten>bounds`, etc.) all call `bounds_as_u64(&bounds)?.to_vec()` which materializes the stride into a length-`count` `Vec<u64>` — defeating most of the memory win and adding `O(count)` alloc per call.

**The change:** Plumb `&BoundsRepr` into the inner kernels and dispatch on the variant:

- `count`: for `Stride { stride, count }`, emit a length-`count` Prim filled with `stride`. Currently this is a sized-`count` Vec via `bounds_as_u64`-then-iter — same alloc, but the alloc is unnecessary if downstream just iterates.
- `reduce.+`, `each`: iterate via `bounds.iter()` (which is zero-alloc for Stride) instead of materializing.
- `each` could detect at build time: if all per-row outputs have length 1, emit a `BoundsRepr::Stride { stride: 1, count: N }` rather than `Var([1, 2, …, N])`.

**Why:** memory only — the materialized form for, say, a 1M-row rectangular list is 8MB vs 16 bytes. Performance impact small (the allocations are bulk and predictable), but for a real columnar pipeline that hands strided lists between ops the savings compound.

**Estimated effort:** 30–45 min. Mechanical — touches ~6 sites in `ops/list.rs` and `ops/helpers.rs`.

## 9. True WCO join — what's the right reusable primitive?

**Current state:** Demo 26's triangle is binary-join + packed-key membership test for the closing edge — `gather(adj, a) × gather(adj, b)` cartesian-then-filter. Correct, but the cartesian is `|N(a)| · |N(b)|` per edge, then filtered. Not WCO: runs in time proportional to candidates *before* the filter, not the triangle count.

The classical WCO triangle (Generic Join / Leapfrog Triejoin) wants the inner step to be a *sort-merge intersect* between `N(a)` and `N(b)`, with cost `O(min(|N(a)|, |N(b)|))` per pair instead of `O(|N(a)| · |N(b)|)`. That's the WCO leap.

**Update (implemented):** `search.<interp>` now exists — see `src/ops/join.rs` and `examples/09_search.col`. WCO triangle via composed intersect lives in `examples/17_wco_list_intersect.col` (sort-merge) and `examples/18_wco_lftj_idiomatic.col` (LFTJ).

**The original temptation:** add a single bespoke combinator like `intersect.per_row.<interp>` — takes `Prod[List<List<X>>, List<List<X>>]`, produces the per-row intersections. Solves the problem, ~40 lines. But this bundles "iterate rows" with "intersect inner lists" into one op, which is the *imperative* survey shape (à la datatoad's `extend`/`propose`). It's the right *algorithm*, but probably the wrong *factoring* for an array language.

**The data-parallel reframe** (Frank): the WCO inner step is "for each query in the smaller list, find its position in the larger (sorted) list, keep the matches." The reusable primitive is **search/lookup**, not intersect. Cost is `|small| · log(|large|)`. Properties:

- **Reusable everywhere lookup is useful.** Set membership, dictionary-style joins, finding insertion points for sorted inserts, range queries.
- **Composes with existing parts.** Search returns indices; we already have `gather` for indices and `where` for filtering. Intersection becomes `search → filter-where-found`, three small ops.
- **Doesn't presuppose the per-row axis.** A flat `search.<interp>` (one target, many queries) is the most useful version; per-row search comes from composing `flatten` / `bounds` / `each` with the flat primitive.

Actual shape as implemented:
```
search.<interp> : (target_sorted : Prim, queries : Prim) -> indices : P64
  // returns lower_bound position for each query in target (0..=len(target))
  // miss-handling: index == len(target), or target[index] != query
```

Composed intersect via search:
1. `search.u64` → positions
2. `positions target length <.u64` → in-range mask
3. `gather target at positions` → values at lookup positions
4. compare against queries → equality mask
5. `where` to filter

Less direct than a bespoke intersect, but the search half is independently useful (dictionary lookup, asymmetric joins where one side is much smaller).

**Algorithmically:** `search` lets us pick the asymmetric `|small| · log(|large|)` cost. Sort-merge gives symmetric `|small| + |large|`. Both are WCO-acceptable at the inner level — which one wins depends on size ratio. An array language should probably offer both as primitives and let the user/optimizer pick.

**What's still open** (genuinely — I shouldn't commit to a shape without Frank weighing in):

1. **What primitive(s) earn their keep?** Candidates:
   - `search.<interp>` (binary search / gallop) — `|small|·log(|large|)`, asymmetric.
   - Some sort-merge-shaped primitive that's *not* `intersect` per se — e.g., `merge.<interp>` returning the interleaved sequence with origin tags, from which intersect/union/diff fall out as filters. More general; cost is `|a|+|b|`.
   - Pure hash-based lookup — different cost profile (build cost amortized over many queries), useful for joins where one side is reused.
2. **Does per-row need special support, or does composition with `flatten`/`bounds`/`each` suffice?** Probably the latter, but we should attempt the triangle with just flat primitives + composition before adding a per-row form.
3. **Indexing adjacency by vertex** when the vertex set isn't dense `0..n-1`: needs either a sparse-key lookup (= `search` again, applied to a key column) or a "pad missing rows" helper. The former is more orthogonal.
4. **The k-ary case (k-cliques, longer paths):** leapfrog intersects k-1 streams. If we go with sort-merge `merge`, k-ary merge is a known generalization. If we go with `search`, k-ary becomes nested searches and the asymmetric cost compounds differently.

**Where to start:** Try to rewrite demo 26 using only `search.<interp>` (added as a single new op, ~30 lines: gallop in the helpers, op shell on top). If the triangle reads cleanly *and* the parts feel reusable for other queries — that's the signal we picked the right primitive. If the rewrite still wants a per-row combinator, that's a signal we need to think more about how data-parallel iteration interacts with the join structure.

**Worth checking against datatoad's experience:** datatoad's `extend`/`propose` is the imperative form — what's the data-parallel form that does the same work? Frank has implemented this and can probably name the primitives that *would* have made the array-language version natural, rather than the ones that did make the imperative version compact.

## 10. Relationship to the `columnar` crate (deferred, on purpose)

`columnar` and collie sit on opposite sides of the static/dynamic axis:
`columnar`'s main contribution is **strongly-typed wrappers** (a `Columnar`
derive that maps arbitrary Rust types to columnar storage with typed
accessors); collie is **the dynamic analogue** — width-tagged primitive
columns with interpretation in operators. Most of columnar's API surface
doesn't translate directly because we deliberately erased the type
information that columnar exists to preserve.

The integration vector that *does* survive is at the **storage** layer:

- **Stash for zero-copy from bytes.** Decode a Value from a memory-mapped
  byte buffer without copying; the Stash keeps the buffer alive. This pairs
  with FOLLOWUPS §1 (zero-copy decode) and would let us avoid building a
  `Borrowed` Prim variant from scratch.
- **Primitive container interop.** Backing `Prim::P64` with whatever
  columnar uses for owned `u64` columns would give cross-version
  serialization compat with the columnar ecosystem at zero semantic cost.

What we should *not* do: pull columnar's full type taxonomy (RLE,
dictionary, etc.) into `Value`. That would expand the closed inductive
universe principle in ways collie's design explicitly avoids. Encoding
awareness, if we ever want it, belongs at the operator layer (an
RLE-aware reducer, say), not in the value model.

**No urgency** — neither integration is on a critical path. Flag if/when
we hit the zero-copy bottleneck.

## 11. Pleasant concrete syntax vs brutalist IR — separate them

**The realization:** Looking at slap's named-tag unions next to collie's positional Sums, the instinct is to add named tags to collie. That's probably the wrong move at the *value* layer — it'd put strings in runtime values and violate the "interpretation lives in operators, not values" principle. But the *underlying instinct* (named tags read better for 5+ variant sums) is correct.

The right framing is layering: collie today *is* the brutalist IR. `partition2 inject2`, positional lanes, no field names, push/pop accounting — all of it is the right shape for the machine. The fact that humans find it dense and want named tags / field accessors / structural pattern matching is a signal that **a pleasant surface layer should compile *down* to it**, not that the IR itself should grow ergonomics.

This is a classic compiler-architecture move (LLVM IR vs C; Lean's surface vs core; Datalog source vs the relational algebra plan). Two layers, sharply distinct:

- **IR (today):** uniform, machine-friendly, minimal primitives. Positional everything. `partition2 inject2`. Stack juggling visible. Operators are the entire vocabulary. Optimizes well, runs fast, easy to typecheck. *What we have.*

- **Surface (future):** named fields, named tags, structural destructuring in let-bindings, `match` arms that bind by tag name not position, maybe `under .field { ... }` for in-place updates. Compiles to the IR. *What humans want to read and write.*

**Things that probably belong in the surface, not the IR:**

- Named lanes for Sums (slap-style `'ok` / `'err`).
- Named fields for Prods (`person.name` rather than `person .0`).
- Structural destructuring in `{| (x, y, z) | ... }` let-bindings.
- `under <path> { body }` for targeted in-place transforms — the lens-flavored sublanguage we sketched earlier.
- The cleave sugar `.{ p0 ; p1 }` is arguably surface; it compiles to `dup body0 swap body1 zip2` and could be lowered at parse time.
- Schemas (the sidecar that gives values their interpretive names — Person, Edge, Triangle) — surface annotations that elaborate to ordinary Shapes.

**Things that should stay in the IR:**

- The closed inductive value universe (Prim, Prod, Sum, List). No more cases.
- Width-tagged primitives; interpretation in operators.
- Stack discipline as the eval model.
- Positional `match` against lane indices.
- Composition via `eval(prog, stack)`.

**What this means for the codebase:** the existing four-layer architecture (value / op / typecheck / parse+registry) already separates the IR from the parser. We're missing a *surface-to-IR elaboration* pass between parse and typecheck. Today, "surface" and "IR" are the same — parse produces IR directly. The followup is: introduce a `Surface` AST, an `elaborate(surface) -> ir` pass, and let surface ergonomics grow without polluting the IR.

This isn't urgent — collie at IR-level is still usable for serious work, and the demos read OK once you've internalized the idioms. The trigger for adding the surface layer is when a real workload makes us reach for "this would be 5 lines if I could name things." The triangle query (demo 26) is the first program where I felt that strain.

**Worth noting** as a cross-reference to the design: this also resolves several other followup tensions cleanly. The "named lanes" question from slap goes in surface. The `under <path>` lens-style transforms go in surface. Structural destructuring goes in surface. The IR stays small and orthogonal.

## 12. Concrete target: `interpreted/` in differential-dataflow

A specific real-world workload collie should be able to serve: the
`interpreted/` subproject in `differential-dataflow`. Today that interpreter
operates on `Vec<i64>` with a handful of hard-coded methods. Generalizing it
along two axes — **shapes** (beyond flat Vec to nested / tagged / tupled) and
**operations** (beyond the hard-coded set to a registry) — is the natural
direction, and collie's design lands exactly there.

Why collie is a good fit:

- **Column-at-a-time evaluation amortizes shape-and-op interpretation.** The
  dispatch cost (which width, which interp, which op) is paid once per
  million rows, not per row. This is the whole reason a dynamic interpreter
  can be competitive with a compiled one at SQL-like workloads.
- **The closed value universe** (Prim/Prod/Sum/List) covers what dd's
  `interpreted/` would want without a long-tail of variant types.
- **Width-tagged primitives** with interpretation in operators matches how
  dd currently thinks about `i64`-as-anything — you can interpret a column
  as time, key, value, or count from the operator side without changing how
  it's stored.

What it would take to be a credible drop-in:

1. **`Vec<i64>` interop** — Storage / from_vec already cover this, but a
   bridge layer (e.g. `from_columnar_dd_thing(...)`) would let dd code stage
   its inputs without rewriting.
2. **The operation set dd currently needs** — likely group/reduce-style
   aggregations, joins, maybe iteration. Most of these exist; what's missing
   is whatever dd-specific verbs there are.
3. **Performance parity at dd's relevant scales** — should be a given since
   the per-element costs are already SIMD-ceiling-bound, but worth measuring
   on dd's actual workload before claiming.

This is a high-leverage validation target. Once collie can serve `interpreted/`
end-to-end, the "can it do real work" question is answered concretely. Worth
checking in with Frank on what dd's specific surface looks like — the
operation set there is the closest thing to a real customer requirements doc
we'd have.

## What I'd do next, in order

1. **#11 surface/IR split** — architectural move that unlocks named tags, named fields, `under`, structural binding. Big leverage but not urgent.
2. **#9 WCO search-style primitive** — unblocks honest worst-case-optimal joins.
3. **#3 predicate fusion** — small change, biggest remaining perf win on a real pattern.
4. **#1 zero-copy** — opens the door to streaming queries on memory-mapped files.
5. **#4 outer-join** — required to make `group` actually serve SQL workloads.
6. **#8 Stride fast paths** — memory-only win, mechanical change.
7. **#7 SIMD investigation** — diminishing return but worth understanding.
8. **#2 columnar containers** — the architectural move; do last.
9. **#5 multi-output** — trivial; do whenever.
10. **#10 columnar crate integration** — deferred; no urgency unless we hit zero-copy.
11. **#6 REPL** — done.
