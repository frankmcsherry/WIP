# Design principles

collie is a **concatenative, functional, interpreted columnar engine**
over flat, offset-addressed data. Each of the seven principles below
picks one side of a real design choice — there are credible languages
and engines that picked the other side. The framing here is "what
did we buy, what did we pay."

Ordered roughly: principles 1-3 are about *data* (what values are
and how their type info is factored); principle 4 is the *API
contract* (everything's a sequence); principle 5 is *operator design*
(structural ops are universally polymorphic in contents); principle 6
is *evaluation* (immutable values, pure ops); principle 7 is *access*
(sequential by default, sort to escape random). Idioms and engineering
policies follow.

---

## 1. Flat data, positionally addressable

Every value is one of five variants:

```
Value = Prim | Prod | Sum | List | View
```

Four content cases plus `View` as a storage-axis modifier. The four
content cases share a strong property: all data is **flat** — bytes
laid out contiguously, addressed by **offsets**. There are no
pointer-graph types. List is `(bounds, values)` with offsets into a
flat value column; Sum is `(disc, lanes)` with all lanes flat; Prod
is a vector of equal-length columns. No cons cells, no trees with
sharing, no object graphs.

A direct consequence: **positions are first-class.** Because data
lives at offsets, a row can be referred to by its index without
copying. This is what makes `View` work and what lets surveys like
`where`, `search`, `sort.perm`, `intersect` produce position columns
that other ops consume. It's a light step away from pure
value-semantics — a position is meaningful only against the column
it came from — but it's the table-stakes capability that
distinguishes columnar engines from value-only array languages.

**Trade.** The alternative is *pointer-graph data* — Lisp cons cells,
heap-allocated object graphs with cycles and sharing, parent
pointers, the data model most general-purpose languages assume.
Postgres, Arrow extension types, and Spark UDFs all share this
property: a new type can still be a pointer-graph thing as long as
the host system knows how to hold a reference to it.

We get SIMD-friendly inner loops (the kernel walks contiguous bytes),
mmap-friendly storage (the on-disk and in-memory forms are the
same), serialization that's only as complicated as any other
structural walk — `O(structure)` not `O(N × structure)`, with
`memcpy` at the leaves — no GC, no cycle detection, and
positions-as-references for free. We give up pointer-graph
structures entirely: a linked list, a tree with shared subtrees, a
cyclic graph must either be re-encoded as flat-and-offset data
(often awkwardly) or it's unrepresentable. Adding a new variant —
say, a `Sparse` representation — is a deep change every layer must
respond to.

---

## 2. Whole-collection-only dispatch

The interpreter dispatches once per op invocation. The unit of
dispatch is a *whole column*, not a row, not an iteration. Inner
loops are width-monomorphic Rust kernels that LLVM autovectorizes;
the per-op dispatch cost is amortized across N rows.

**Trade.** The alternative is *row-at-a-time compilation* — the
HyPer / Umbra / DuckDB-JIT / MonetDB-X100 family. A query plan is
codegen'd into a single pipelined loop that walks rows through many
operators without materializing between them. The "produce/consume"
model fuses 5+ stages into one tight loop.

We get an interpreter that's tractable to write and tightly
vectorized kernels per op — no JIT to maintain, no codegen to debug,
and rule-based optimization works directly on op sequences. We pay
*materialization between ops*: long pipelines spend memory bandwidth
that a compiled pipeline would avoid (`View` and Arc-1 reuse are
the main mitigations). The principle-2 *escape hatches are now all gone*:
the per-row `each` became segmented columnar ops (`sort.segmented`,
`reduce`, `filter`, …); `match`/`cleave` desugar to columnar ops in the
parser; and `repeat` was removed outright — collie is deliberately not
Turing-complete yet, so genuinely iterative algorithms (fixed-point,
escape-test loops) are simply out of scope rather than served by a
dispatch-violating hatch. Every op now dispatches once per whole column. A
loop can return later as a *dataflow* construct (see `dev/LAYERING.md`)
without reintroducing per-iteration interpreter dispatch.

---

## 3. Type information is layered

A `Prim` is bytes at a width (`P8`, `P16`, `P32`, `P64`) — and that
is *all* the storage layer knows. The same byte pattern is unsigned,
signed, or float depending on which operator reads it: `<.u32` vs
`<.i32` vs `<.f32`. Interpretation lives in the op token; richer
type information (sortedness, uniqueness, schema names) lives in
layers above. Width travels with the value because the kernel needs
it to walk the bytes; everything else is pushed up.

**Trade.** The alternative is *interpretation in storage* — every
value carries its full type. Statically typed array languages and
strict columnar engines work this way; SQL and Arrow track
`INT32`/`UINT32`/`FLOAT32` as distinct storage types and overload
operators over them.

The analogy is LLVM: registers and memory regions carry a width but
not an interpretation; instructions decide how to read them. The
win is the same one LLVM gets — storage-mode ops (`dup`, `gather`,
`view`, `nest`, `flatten`) need one implementation per width, four
arms total, instead of one per (width × interpretation), and the
kernel surface stays small. Type information *can* still exist; it
just lives where it earns its keep, which isn't always the value.

We pay: a `Prim<P32>` doesn't tell you whether it's signed; `+.u32`
accidentally applied to a column meant as `i32` won't trip a type
error, it just computes different numbers; debug output needs
`show.<interp>` to disambiguate; invariants that depend on
interpretation (sortedness, signedness) can't ride on the value
alone — they live in the calling code or wait for a typechecker
that tracks them.

(The consequence — that structural ops can carry no content type
at all — is its own principle; see principle 5.)

---

## 4. All collections are sequences with optional restrictions

Every collection — `Prim`, the values of a `List`, a `View` of either —
is logically a *sequence*: an ordered run of elements addressable by
position. Properties like sortedness, uniqueness, contiguity,
strided-ness exist as *restrictions* layered on top, not as separate
kinds of data. `View` is the structural form of this: any sequence
can be presented as a positional restriction of another.

The contract: every op signature is `SEQ<T0> → SEQ<T1>` (see
`dev/FOLLOWUPS.md` §−1). There is no parallel "scalar op" family —
a scalar is a length-1 sequence.

**Trade.** The alternative is *kind-stratified* data — distinct types
for scalars, vectors, sets, multisets, ordered/unordered, and a
parallel API per kind. SQL, K, and most statically-typed
relational languages have stratifications of this sort, and they
buy local type-safety: you can't accidentally pass an unsorted bag
to an op that needs sorted input.

We get uniform composition: anything that produces a sequence can
feed anything that consumes one; positions cross kinds; `View`
falls through to its source. We pay surface-level expressiveness
on the restriction axis — "this column is sorted" or "this set has
no duplicates" can't ride on the type. Restrictions live in
documentation, in op preconditions, or in selector variants
(`Indices` vs `Runs` vs `Mask`), not in the value's identity.

---

## 5. Operators are shape-polymorphic in their contents

Structural ops — `.0`, `zipN`, `gather`, `intersect`, `sort.perm`,
`group`, `match`, `nest`, `flatten`, etc. — operate on a shape
(pair, sum, list, view) without knowing or constraining what's
inside. `.0` works on any `Prod[a, b]`; `intersect` on any
`(K, V)` shape; `gather` on any column. Constraints on contents
land on the *next* op (e.g., `+.u64` is where "this column is
u64-shaped" is asserted) rather than on variables that flow through
structural code. Working with combinators and verbs doesn't require
spelling out the full type of what you're wrapping — though you do
still pay the performance cost of moving it.

**Trade.** The alternatives are *bounded polymorphism* (Haskell type
classes, Rust traits — every type variable carries a constraint bag
through inference, every op that does *anything* with the contents
needs a constraint named at the use site) and *row polymorphism*
(ML / PureScript / Ur — open records with row variables that need
unification). Both require a solver in the typechecker.

The closed value universe (principle 1) and op-carries-interp
(principle 3) together let us skip both machineries: shape dispatch
is decidable from finite case analysis, and constraints land
per-op rather than per-variable. The IR has no constraint solver
and no row-unification, and we don't lose expressivity for the
structural-op layer that does the heavy lifting.

We pay: no *content-aware* generic ops. There's no way to write a
single `print` or `equal` that handles "any totally-ordered type"
without per-content enumeration. Structural ops are universally
polymorphic; content-comparing ops are typed.

**The division of labor.** Principle 5 is part of a four-layer
story that together makes the language workable without any
metaprogramming machinery in the IR:

1. **Structural polymorphism handles contents.** Ops see shape,
   not interpretation. `.0`, `gather`, `intersect` are universal.
2. **Fixed-arity ops handle structure.** `join` is
   `(K, V) ⋈ (K, V') → (K, V, V')`, always — no arity-recursion.
   The op surface stays monomorphic.
3. **The surface (where present) handles packing.** Turning a
   user's "join on `k1, k2` values `v1, v2, v3`" into the
   fixed-arity form is parse-time mechanical sugar
   (`zip` / `detuple`), not an IR concern.
4. **Kernels handle any genuine shape recursion.** `sort` walks
   nested `List<Prod<Sum<…>>>` structures in Rust where the
   complexity is contained, not exposed at the op level.

Each layer has a clear job. None needs a constraint solver or a
general macro language.

---

## 6. Values are immutable; ops are pure (modulo diagnostics)

A value, once produced, is never modified. Ops consume values from
the stack and produce new ones; they never mutate in place. The
only side-effecting ops are diagnostic — `time`, `profile.start`,
`profile.print`, `show.<i>` — and are explicitly marked as such.

Combined with stack-based composition — programs are juxtapositions
of ops, point-free, with implicit value flow through the stack —
this puts collie in the same family as Joy, Cat, and Factor:
**concatenative and functional**.

**Trade.** The alternative is *in-place mutation*. Array languages
like K and APL freely modify columns; SQL `UPDATE` mutates rows;
most general-purpose languages default to mutable state. Mutation
is sometimes a performance win and is often what users ergonomically
expect.

We get equational reasoning (replacing an expression with its value
is always sound), no implicit aliasing (a column you've been
holding doesn't change underneath you), and a clean
parallelizability story (any two ops without data dependencies can
run concurrently without locks). We pay performance on the margin:
producing a new column when you could have modified an old one
costs an allocation. Arc-1 buffer reuse (`#49` in the backlog
history) recaptures this for the common case of single-owner
anonymous intermediates, but it's an *optimization invisible at
the semantic layer*, not the semantic model itself.

**Influence: Joy.** Manfred von Thun's [Joy](https://hypercubed.github.io/joy/joy.html)
is the closest direct ancestor — the language that made the
mathematical case for *concatenative + functional* with more rigor
than the broader Forth tradition. Joy's contributions worth carrying
forward: programs as first-class values (quoted programs), a
small combinator algebra (`i`, `dip`, `ifte`, `primrec`, `linrec`)
that covers a surprising amount of what other languages reach for
a macro system to do, and the discipline of treating every
operation as a stack-to-stack function. Where collie diverges:
columnar values rather than scalar, whole-collection dispatch,
the structural value universe with `Sum` and `List`. The kinship
is in *how the language composes*, not what it operates on.

---

## 7. Sequential access by default; sort to escape random access

The execution model prefers **sequential access**. Kernels walk
columns front-to-back; when an algorithm would otherwise need *random*
access — probing arbitrary positions of a large column — the answer is
to **sort** the data into the order that makes the access sequential,
not to issue the random reads. Distribution (counting / radix) is the
one primitive data-movement kernel; sort is recursive distribution,
and higher-level movement — `gather`, joins, `group` — reduces to
sort-merge rather than random probing. `gather` with already-ordered
indices (the `Range` / `Runs` / `Mask` selectors, which visit source
positions in order by construction) is sequential and effectively
free; `gather` with arbitrary `Indices` is *the* random-access escape
hatch, and producing arbitrary `Indices` is the signal that a sort
belongs there instead. A permutation is the fully-resolved special
case of a sort's order/group labels; reaching for one is reaching for
random access.

**Trade.** The alternative is *random access as a peer primitive* —
hash tables, hash joins, pointer-chasing, index probes that the cost
model treats as O(1) regardless of where they land. Most database
engines and general-purpose languages lean on hash-based operators
(hash join, hash aggregate) on exactly this assumption.

We get memory behavior the hardware is fastest at: contiguous,
prefetchable, bandwidth-bound streaming plus bounded-fan-out scatter
(radix's 256 cursors), instead of latency-bound random misses. It
compounds with flat data (principle 1) and whole-collection dispatch
(principle 2) — a kernel that walks bytes in order is the same kernel
that vectorizes and prefetches — and it gives one answer to "bring
data into the order I need" (sort) rather than a zoo of hash
structures.

We pay: when the working set fits in cache, a direct random gather is
cheaper than the two sorts that would make it sequential, so this is a
*default at scale*, not an absolute — small / in-cache random access
is fine, and the cost model has to know the threshold. And genuinely
sparse, one-shot lookups against a large structure are pessimized:
turning a handful of probes into a full sort is a loss. The principle
targets *bulk* movement, where sequential streaming dominates random
latency.

---

# Idioms

Patterns that fall out of the principles but aren't themselves
principles — habits, not constraints. The code mostly follows them;
exceptions exist and are fine.

- **Surveys return positions when natural; `gather` materializes.**
  Because positions are first-class (principle 1), surveys can
  produce them as their primary output. Several do: `where`,
  `search`, `sort.perm`, `intersect`, `iota`. Others bundle the
  gather for ergonomics (`filter`, `sort`, `unique`, `group`, `take`,
  `head`). Both shapes are fine; the idiom is just "when you have a
  natural position output, prefer returning it." Prefer *ordered*
  position outputs (`Range` / `Runs` / `Mask`) over arbitrary
  `Indices` — the former gather sequentially (principle 7); the latter
  is the random-access escape hatch.

- **Segmented compute preserves grouping.** A `List`'s per-row grouping is
  a *transparent restriction* on a flat sequence (principle 4), so compute
  ops dispatch on representation (flat vs `List`) to keep the grouping
  rather than forcing a `flatten` → compute → re-nest round-trip. Two
  cases, split by whether the op changes per-row counts:
  - **Element-wise** — `cmp`, `arith` (incl. `neg`/`abs`), `as`, boolean
    `and`/`or`/`not`. The op never crosses a row boundary, so the kernel
    runs on the flat inner values and **reattaches the same bounds**
    (`List<T> op List<T> → List<T'>`; equal bounds required, a length-1
    scalar broadcasts across all rows). Per-row counts are unchanged — the
    grouping rides through untouched.
  - **Count-changing** — `filter`. Per-row output length differs, so it
    needs a real segmented kernel: keep per-row the elements where an
    aligned `List<bool>` mask is true, deriving new bounds from the per-row
    survivor counts (the "nest by counts" step folded into the op).
  This is *representation dispatch* per the operator-shape recipe, **not** a
  parallel `*.seg` op family — same token, flat path unchanged, the `List`
  branch returns before it. The reduce-like ops (`reduce`/`count`/`cumsum`/
  `shift`) already work this way, though there the `List` is *semantic*
  (per-row vs whole-column) rather than a transparent restriction. The
  practical payoff: filtering grouped data no longer pays a manual
  `where gather` + recount + `cumsum` + `nest` re-grouping dance (see the
  `lftj_lane_g` lane in `examples/19_wco_lftj_match.col`).

---

# Engineering policies

These shape how we write code against the principles, but they are
not themselves design principles.

- **Composability before sugar.** Prefer fewer ops that compose well.
  Sugar (`:name`, `:[names]`, parser peepholes, `def`) is fine when it
  lowers to existing core ops without introducing new semantics.

- **No new primitive without a real workload.** Every op should
  answer "what query needed this?" The cost: workloads that *would*
  have benefited from a primitive go unwritten because the primitive
  isn't there; we accept this in exchange for surface clarity.

- **Layers are independently swappable.** `ir/` knows nothing of
  operators; `ops/` knows nothing of syntax. The parser in `syntax/`
  is one possible front-end. Adding an op is one file in `ops/` and
  a registry entry — nothing else changes.

- **No `unsafe` in user code paths.** Errors are messages, not
  panics. The interpreter never aborts on bad input. We give up
  some perf for the safety guarantee.

- **Verify before generalizing.** If two ops look similar, write the
  third before merging them. Many shape-similar ops have load-bearing
  semantic differences that only emerge under real use.

- **Optimizer passes are `Graph → Graph` and never load-bearing for
  execution.** The engine must run an un-optimized graph to the same
  result; an optimizer pass only ever *improves* a program, never enables
  it. (`--no-opt` runs the engine without the optimizer; the corpus
  equality test guards it.) This keeps the optimizer a cleanly removable
  layer rather than a fused part of execution — see `dev/LAYERING.md` and
  its three-question "smear test."

- **Operator shape recipe.** Every op has one signature
  `SEQ<T_in> → SEQ<T_out>`. Internal dispatch on *representation* —
  flat `Prim`, `List<Prim>`, `View<...>`, strided vs var bounds — is
  implementation, not semantics; the op may take any fast path it
  likes as long as the observable result matches the SEQ→SEQ contract.
  What's *not* allowed at one op is polymorphism between distinct
  *element types* (e.g. `SEQ<T>` and `SEQ<LIST<T>>` — flat vs
  per-row): those are different signatures and should be different
  ops, with the atom form lowered to `enlist <perrow> unlist`
  (or `enlist <perrow>` when the kernel's output is already
  scalar-shaped, like `count`). `View` would live more naturally as a
  representation axis of `SEQ` than as a `Value` variant; for now it's
  a variant, and ops dispatch internally on "non-trivial view or not."
  See FOLLOWUPS §−1 for the lifted/atom convention.

---

# Candidates not yet promoted

(None at the moment — the previous candidate, "all collections are
sequences with optional restrictions," was promoted to principle 4
above. New candidates land here when they look principle-shaped but
aren't yet load-bearing in the contract.)
