# Operators

The full operator surface, grouped by role. Each entry shows the
stack effect (`pop → push`) and a one-line semantics. For the design
choices behind these ops see `PRINCIPLES.md`.

**Conventions.** `<i>` is an interpretation tag — one of `u8`, `i8`,
`u16`, `i16`, `u32`, `i32`, `f32`, `u64`, `i64`, `f64`. `seq<T>` is
a column of `T`. `bool` means `seq<P8>` used as a 0/1 mask. `P64`
means `seq<Prim<P64>>` — a column of 64-bit positions. `Prod[A, B]`,
`Sum[A, B]`, `List[T]`, `View[T]` are the value-universe constructors.

Stack reads bottom-to-top: `a b → c` means `a` was pushed first, `b`
on top; `c` replaces both.

---

## 1. Per-element compute

Width-monomorphic kernels that walk a column once and produce a
column of the same length. Storage carries width; the op carries
interpretation (principle 3).

### Arithmetic

| Op | Stack | Notes |
|---|---|---|
| `+.<i>` | `seq<X> seq<X> → seq<X>` | per-element add |
| `-.<i>` | `seq<X> seq<X> → seq<X>` | per-element sub |
| `*.<i>` | `seq<X> seq<X> → seq<X>` | per-element mul |
| `/.<i>` | `seq<X> seq<X> → seq<X>` | per-element div (integer or float per `<i>`) |
| `%.<i>` | `seq<X> seq<X> → seq<X>` | per-element mod (integer interps only) |
| `neg.<i>` | `seq<X> → seq<X>` | unary negate |
| `abs.<i>` | `seq<X> → seq<X>` | unary absolute value |

Scalar right-hand sides broadcast: `seq.u64 5u64 +.u64` is
`x[i] + 5`. View-with-mask fast paths exist; see `src/ops/arith.rs`.

### Comparison

| Op | Stack | Notes |
|---|---|---|
| `<.<i>` | `seq<X> seq<X> → bool` | less than |
| `<=.<i>` | `seq<X> seq<X> → bool` | less or equal |
| `=.<i>` | `seq<X> seq<X> → bool` | equal |
| `!=.<i>` | `seq<X> seq<X> → bool` | not equal |
| `>=.<i>` | `seq<X> seq<X> → bool` | greater or equal |
| `>.<i>` | `seq<X> seq<X> → bool` | greater than |

Result is always a P8 mask column. Scalar broadcast as in arith.

### Conversion and display

| Op | Stack | Notes |
|---|---|---|
| `as.<i>` | `seq<W> → seq<W'>` | width-cast `seq` to the width of `<i>`; reinterprets bits when widths match |
| `show.<i>` | `seq<X> → seq<X>` | stack-shape-preserving; prints to stderr under interpretation `<i>` |

### Boolean

| Op | Stack | Notes |
|---|---|---|
| `not` | `bool → bool` | per-element NOT |
| `and` | `bool bool → bool` | per-element AND |
| `or` | `bool bool → bool` | per-element OR |
| `any` | `bool → bool` (1 element) | reduce-OR over the column |
| `all` | `bool → bool` (1 element) | reduce-AND over the column |

---

## 2. Constructors

| Op | Stack | Notes |
|---|---|---|
| `<i>[ v… ]` | `→ seq<X>` | column literal (e.g. `u64[1 2 3]`, `f32[1.0 -2.5]`, `bool[t f t]`) |
| `N<i>` | `→ seq<X>` (1 element) | scalar literal (e.g. `5u64`, `3.14f32`) |
| `iota` | `seq<P64> (1 elem) → seq<P64>` | `0..n` for the integer on top |
| `like` | `seq<T> seq<X> (1 elem) → seq<X>` | broadcast scalar to the shape of template |
| `spread` | `seq<T> seq<P64> → seq<T>` | repeat each element by its corresponding count |

---

## 3. Stack and binding

| Op | Stack | Notes |
|---|---|---|
| `dup` | `a → a a` | duplicate top |
| `drop` | `a →` | discard top |
| `swap` | `a b → b a` | swap top two |
| `over` | `a b → a b a` | copy second-from-top onto top |
| `rot` | `a b c → b c a` | rotate third-from-top to top |
| `pick N` | `… → … x` | copy the N-th-from-top |
| `roll N` | `… → …` | rotate the N-th-from-top to top |
| `{\| name … \| body }` | (Factor-style binding) | pops the named values, pushes them as locals available by name within `body` |
| `>name` | `a →` | bind top to `name` for the rest of the block |
| `name>` | `→ a` | reference `name`, taking ownership (last-use) |
| `name` | `→ a` | reference `name`, cloning |
| `def n { body }` | — | parse-time inline macro; `body` is spliced wherever `n` appears |

---

## 4. Structural shapes

### Tuples (Prod)

| Op | Stack | Notes |
|---|---|---|
| `zip2` … `zipN` | `a b → Prod[a, b]` (N inputs) | combine equal-length columns into a Prod |
| `detuple2` … `detupleN` | `Prod[a, b] → a b` | split a Prod into its columns |
| `.i` | `Prod[a, b, c] → seq<i-th>` | project field `i`; recurses through `List[Prod]` to `List[i-th]` |
| `entuple.K` | `… → Prod[…]` | tuple the top K values |

### Sums

| Op | Stack | Notes |
|---|---|---|
| `inject.N` | `… → Sum[a, b, …]` | build a Sum from a value + disc, or from per-lane values |
| `split` | `Sum[a, b, …] → seq<disc> a b …` | decompose a Sum into its discriminant + lane columns |
| `partition.N` | `seq<T> seq<disc> → Sum[…]` | scatter a single column into N lanes by `disc` |
| `branch / branch.K` | `seq<T> seq<disc> → Sum` | partitioning constructor; errors if disc value `>= K` |
| `match { -> arm0 -> arm1 … }` | `Sum[…] → merged` | run each arm on its lane; arms must produce the same shape; results merged in source order |

### Lists

| Op | Stack | Notes |
|---|---|---|
| `nest` | `seq<T> seq<P64> → List[T]` | `(values, bounds)` → list (bounds are run-end offsets) |
| `nest.stride` | `seq<T> seq<P64> (1 elem) → List[T]` | rectangular list (stride-based bounds) |
| `flatten` | `List[T] → seq<T> seq<P64>` | inverse of `nest` |
| `list>bounds` | `List[T] → seq<P64>` | extract the bounds column |
| `bounds>keys` | `seq<P64> → seq<P64>` | bounds → repeating row-key column (i appears `len(row i)` times) |
| `enlist` | `seq<T> → List[T]` (1 row containing the whole input) | promote to singleton list |
| `unlist` | `List[T] (1 row) → seq<T>` | inverse of enlist |
| `count` | `List[T] → seq<P64>` | per-row element count |
| `length` | `seq<T> → seq<P64>` (1 elem) | total length; surface sugar for `enlist count` |
| `head` | `List[T] → seq<T>` | first element of each row |

### Views

A `View` is a zero-copy positional selector over another value.
Materializing ops call `gather`; view-aware ops fast-path on the
selector variant (`Indices | Runs | Mask | SequenceRange`).

| Op | Stack | Notes |
|---|---|---|
| `view` | `source positions → View` | construct a view by positions |
| `view.range` | `source lo hi → View` | construct a view by half-open range |
| `decompose-view` | `View → source positions` | inverse |

---

## 5. Surveys (return positions)

Surveys produce position columns. To get values, follow with
`gather`. (Principle: positions are first-class; see `PRINCIPLES.md`
principle 1 and the Idioms section.)

| Op | Stack | Notes |
|---|---|---|
| `where` | `bool → P64` | positions where the mask is true |
| `search.<i>` | `target queries → P64` | for each query, lower-bound position in `target` (sorted) |
| `sort.perm` | `seq<T> → P64` | permutation that sorts the column ascending |
| `intersect.<i>` | `seq<X> seq<X> → P64 P64` | sort-merge intersect; returns positions in both inputs |

`iota` (in Constructors) and the bounds-shaped ops (in Lists) also
produce position-flavored output.

---

## 6. Materialization

| Op | Stack | Notes |
|---|---|---|
| `gather` | `seq<T> seq<P64> → seq<T>` | apply positions to a value column |

---

## 7. Aggregations and scans

Whole-column reductions and prefix-style scans. Per-row variants
operate on `List[T]` by dispatching once and looping over rows in a
Rust kernel.

| Op | Stack | Notes |
|---|---|---|
| `reduce.+.<i>` | `seq → seq (1 elem)` or `List → seq` | sum (flat or per-row) |
| `reduce.*.<i>` | same shape | product |
| `reduce.min.<i>` | same shape | min |
| `reduce.max.<i>` | same shape | max |
| `count` | `List[T] → seq<P64>` | per-row element count (also listed under Lists) |
| `length` | `seq<T> → seq<P64>` (1 elem) | total length |
| `cumsum.<i>` | `seq → seq` or `List → List` | prefix sum (flat or per-row) |
| `shift.<i>` | `seq scalar_n → seq` or `List scalar_n → List` | positive shift; fill with 0 |

---

## 8. Sort family

| Op | Stack | Notes |
|---|---|---|
| `sort.perm` | `seq<T> → P64` | (also listed under Surveys) ordering permutation |
| `sort` | `seq<T> → seq<T>` | sorted values; polymorphic over the value universe (Prim leaves treated as unsigned) |
| `sort.<i>` | `seq<X> → seq<X>` | sorted values under interpretation `<i>` |
| `group.<i>` | `vals keys → uniq_keys List[vals]` | sort by `keys`, group `vals` per unique key |
| `unique.<i>` | `seq<X> → seq<X>` | sort + dedup |

---

## 9. Slicing and concatenation

| Op | Stack | Notes |
|---|---|---|
| `take` | `seq<T> n → seq<T>` | first `n` elements |
| `skip` | `seq<T> n → seq<T>` | drop first `n` elements |
| `head` | `List[T] → seq<T>` | first element of each row (also under Lists) |
| `reverse` | `seq<T> → seq<T>` | reverse element order |
| `concat` | `List[T] → seq<T>` | concatenate the rows of a list |
| `cat.N` | `seq seq … → seq` | concatenate the top N columns |
| `filter` | `vals mask → seq<T>` | `where mask gather vals` fused (parser peephole) |

---

## 10. Joins

| Op | Stack | Notes |
|---|---|---|
| `intersect.<i>` | `seq<X> seq<X> → P64 P64` | sort-merge intersect (also under Surveys) |
| `search.<i>` | `target queries → P64` | binary search (also under Surveys) |
| `xprod` | `Prod[List[a], List[b]] → List[Prod[a, b]]` | per-row Cartesian product |

---

## 11. Escape hatches (body-bearing)

These ops violate or strain principle 2 (whole-collection dispatch).
They exist because some workloads don't decompose columnarly. Every
use is a signal that we may be missing a primitive.

| Op | Stack | Notes |
|---|---|---|
| `each { body }` | `List[T] → List[U]` | run `body` per row; body's stack must produce one value per row |
| `reduce { body }` | `List[T] → seq` | per-row reduction with an arbitrary body (generic version of `reduce.+` etc.) |
| `repeat { body }` | `seq → seq` (N times) | run `body` N times; body must be shape-preserving |
| `match { -> a0 -> a1 … }` | `Sum → merged` | (also under Sums) per-lane body dispatch; whole-lane, not per-row |
| `.{ p0 ; p1 ; … }` | `value → Prod[p0(v), p1(v), …]` | (`cleave`) run each path against a copy of the input |

---

## 12. Diagnostics

| Op | Stack | Notes |
|---|---|---|
| `time` | `→` | print elapsed since last `time`; reset clock |
| `profile.start` | `→` | enable per-op timing |
| `profile.print` | `→` | print per-op breakdown |

---

# Notes on the catalogue

**Width letter map.** `<i>` covers ten interpretations:
`u8 / i8 / u16 / i16 / u32 / i32 / f32 / u64 / i64 / f64`. Not every
op accepts every interpretation — e.g. arithmetic skips `bool` and
sort skips float interpretations where the order would be wrong.

**The ergonomic shortcuts.** `filter`, `sort`, `unique`, `group`,
`take`, `head`, `reverse` all bundle survey + materialize. Each has
a position-producing twin in spirit if not in name (`filter` =
`where` + `gather`; `sort` ≈ `sort.perm` + `gather`; `group` =
`sort.perm` + `bounds` + `gather`). See `PRINCIPLES.md` Idioms.

**The escape hatches.** `each` and `reduce { body }` are the two
constructs we'd most like to remove if a better columnar form
appeared. `repeat` is fine — iteration count is programmer-bounded,
not data-bounded. `match` and `cleave` dispatch a bounded number of
times (per Sum lane, per cleave path) and are aligned in spirit.

**Where to look in source.** Ops live in `src/ops/`, one file per
family: `arith.rs`, `cmp.rs`, `combinators.rs`, `convert.rs`,
`helpers.rs`, `join.rs`, `letbind.rs`, `list.rs`, `reduce_ops.rs`,
`sort.rs`, `sort_concat.rs`, `stack.rs`, `view.rs`. Op registry is
`src/syntax/registry.rs`.

**Operator-shape recipe & violators.** See PRINCIPLES.md ("Operator
shape recipe") for the rule: every op has one signature
`SEQ<T_in> → SEQ<T_out>`; internal dispatch on representation
(`Prim` / `List<Prim>` / `View<...>`) is fine; polymorphism between
element types (flat `SEQ<T>` vs per-row `SEQ<LIST<T>>`) is the
violation. Current violators — each dispatches at the op level on
whether input is flat or `List`:

- `reduce.+.<i>`, `reduce.*.<i>`, `reduce.min.<i>`, `reduce.max.<i>`
- `any`, `all`
- `cumsum.<i>`
- `shift.<i>`
- `intersect.<i>` (flat and per-row forms share one name)
- `search.<i>` (flat and per-row forms share one name)

The cure is to split each into a per-row kernel and lower the flat
form to `enlist <perrow> unlist` via a shape-aware elaboration step
between parse and typecheck. Tracked as BACKLOG #0. Until then, these
ops have polymorphic input contracts.

---

# Layer classification

Three buckets. Inclusion is by *what the op needs to know* — shape
ops see only structure; type ops need a width and an interpretation;
surface ops would lower cleanly to combinations of the first two.

**Shape (type-agnostic).** Sees structure only — no width, no
interpretation. The "structural polymorphism handles contents" layer
of principle 5.

- Stack: `pick N`, `roll N`
- Bindings: `let` (`{| names | body }`), `ref` (`name`, `name>`); the
  parser-only `def` is surface
- Prod: `zipN`, `detupleN`, `.i`
- Sum: `injectN`, `split`, `partitionN`, `branch`/`branch.K`, `match`
- List: `nest`, `nest.stride`, `flatten`, `list>bounds`,
  `bounds>keys`, `count`, `head`, `enlist`, `unlist`
- View: `view`, `view.range`, `decompose-view`
- Surveys (positions are structural): `where`, `sort.perm`
- Materialize: `gather`
- Joins (structural shape): `xprod`
- Slicing: `take`, `skip`, `concat`, `cat.N`, `reverse`
- Constructors (structural): `iota`, `spread`, `like`
- Body-bearing: `each`, `reduce { body }`, `repeat`

**Type (width + interpretation matters).** Fundamental kernels whose
behavior depends on how the bytes are read.

- Per-element compute: `+.<i>`, `-.<i>`, `*.<i>`, `/.<i>`, `%.<i>`,
  `neg.<i>`, `abs.<i>`
- Comparison: `<.<i>`, `<=.<i>`, `=.<i>`, `!=.<i>`, `>=.<i>`, `>.<i>`
- Boolean (P8-specific): `not`, `and`, `or`, `any` ¶, `all` ¶
- Width-cast / display: `as.<i>`, `show.<i>`
- Literals: `<i>[ … ]`, `N<i>`
- Aggregations / scans: `reduce.+/*/min/max.<i>` ¶, `cumsum.<i>` ¶,
  `shift.<i>` ¶
- Sort family: `sort` (polymorphic over universe), `sort.<i>`,
  `group.<i>`, `unique.<i>`
- Typed joins / surveys: `intersect.<i>` ¶, `search.<i>` ¶

**Surface (sugar; lowers to shape+type combinations).** Could live in
a separate surface IR; today these are recognized at parse time.

- Stack shortcuts: `dup`, `drop`, `swap`, `over`, `rot` (special
  cases of `pick`/`roll`)
- Binding sugar: `>name`, `name>`, `name`, `def name { body }`
- Compute sugar: `filter` (= `where gather`), `length`
  (= `enlist count`)
- Tuple sugar: `entuple.K` (= top-K `zipK`)
- Cleave: `.{ p0 ; p1 ; … }` (= `dup p0 swap dup p1 swap … zipN`)

**Side-band.** Effectful, classification doesn't quite fit.

- Diagnostics: `time`, `profile.start`, `profile.print`

`¶` marks recipe violators (see above).
