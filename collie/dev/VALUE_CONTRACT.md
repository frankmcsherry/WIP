# Value contract — the shared currency across the stack

**Purpose.** Several spikes across the wider stack (DD, DDIR, timely
diagnostics) are starting concurrently on the premise that collie's
`Value` is the *one* data interchange type. They parallelize safely only
if they agree, up front, on what `Value` is and what laws it obeys. This
file is that agreement. It is a contract, not a design sketch: if a spike
needs something not here, it amends this file first, then proceeds.

## What a `Value` is: a sequence over a shape

A `Value` is a **sequence of elements of some shape `T`**, where `T` is
the closed grammar

```
T ::= Prim(w)          w a width, u8 | u16 | u32 | u64   (the leaf)
    | Prod(T, …, T)    heterogeneous tuple
    | Sum (T, …, T)    tagged union (ENUM)
    | List(T)          variable-length nesting
```

The shape `T` *is* the value's type; the data is a column of `T`-shaped
elements. This is the `Content` axis of `src/ir/value.rs` (Prim / Prod /
Sum / List), and it is the entirety of the interchange contract.

**`View` is deliberately out of scope here.** `View` is the other axis
(`PositionStorage`) — a lazy-gather storage optimization that is
shape-transparent (`shape_of(View(s)) == shape_of(s)`). It is essential
*inside* collie to avoid continual rematerialization, but it is not part
of the cross-stack currency: **a `Value` is materialized to `Identity`
storage before it crosses a boundary** to DD / DDIR / a foreign op. Every
system in the table below therefore sees only the four `Content` cases,
never a `View`. (Should zero-copy *streaming* across a boundary ever
become a goal — distinct from *arranging*, which forces materialization
anyway — amend this scoping then.)

## Two forms of one algebra

The same value universe shows up in two shapes, and the systems we're
linking name them differently:

- **Collection form** — `value::Value` (`src/ir/value.rs`). A *column /
  batch*. `Prim` is many primitives; `Prod` is parallel columns sharing a
  row count; `Sum` is a tagged union over rows; `List` is variable-length
  nesting carved by `bounds`. This is the form collie operates on: per
  FOLLOWUPS §−1 every op is `SEQ<T0> → SEQ<T1>`, so `Value` *is* the `SEQ`.

- **Row form** — one logical element of a collection. This is what the
  other systems call their datum: DD's `Data`, DDIR's `Row`, an
  arrangement key/value. collie has no first-class row type; a row is the
  thing you'd get by indexing a `Value` at one position.

They are dual exactly as columnar's `Vec<T>` decomposes into columns: the
collection form is the **container**, the row form is the **element**.
This is the same owned/borrowed/container separation as the DD columnar
migration, drawn at the data level — not new vocabulary.

```
   collection form  (Value)        ──index one position──▶   row form
   the container, the SEQ, the                              the element, the
   columnar storage                                         DD datum / DDIR Row
   ◀──push / extend / collect──
```

## What each form must satisfy

**Row form — a *pair*, because DD's merge needs both** (DD spike, `trace/implementations/mod.rs:432`):
- `Owned: Clone + Ord` — the materialized single row, for key extraction.
- `ReadItem<'a>: Copy + Ord` — a *borrowed* row reference (a positional
  cursor into the columns) used for in-place merge comparison. `Copy` is
  the binding constraint: an `Arc`-wrapped single-row `Value` is **not**
  `Copy`, so the merge-boundary row form must be references-plus-index,
  not a materialized `Value`. (This is also *why* Content-only scoping is
  mandatory, not stylistic: `index()` must hand back a concrete positional
  `ReadItem`; a `View` would have to be materialized first.)
- `Ord` is the **load-bearing** obligation and it lives **here, on the
  row form** — arrangement keys and merge stepping
  (`ord_neu.rs:456,495`: `keys.index(i).cmp(keys.index(j))`) are row
  comparisons. The whole stack is keyed on this order.

**Collection form (`Value`)**
- The container surface (`push` / `extend` / `index` — DD's
  `BatchContainer`, the columnar `Container` role) so DD can accumulate it
  and a batch operator can produce it. `Value` fills DD's **Key/Val**
  columns only; DD owns the **Time** and **Diff** columns. `Value` is the
  *data-axis* container — never the `(D, T, R)` update-container. (`Prod`
  decomposes onto the Key/Val split natively: DD's key-major/val-minor
  storage order *is* the lexicographic `(K, V)` row order.)
- **No `Ord` on the collection enum.** Do *not* `#[derive(Ord)]` on
  `Value`: it orders by variant tag then structurally — comparing whole
  *columns* (`Prim`-column `<` `Prod`-column …), an independent order that
  is not the row order and does not give arrangeability. Arrangeability
  needs `Ord` on the row form, not the collection. If a whole-column order
  is ever wanted for its own sake, it must be the written
  sequence-lexicographic extension of the row order — never the derive.

**Coherence law (the part that actually matters) — two clauses, both required:**

1. **Order.** Sorting a `Value` by element position agrees with the row
   `Ord`: "sort the column = sort the rows." This makes a `Value`-backed
   column a well-defined sorted run, which `OrdValMerger` preserves under
   merge.
2. **Cancellation.** Row equality is coherent with row `Ord`:
   `cmp == Equal  ⟺  the rows cancel`. DD merge doesn't only sort, it
   *consolidates* (`merge_batcher.rs:312`: `plus_equals` then drop on
   `is_zero`). An incoherent `PartialEq`/`Ord` silently fails to cancel
   retractions — the worst kind of arrangement bug. Implied by a genuinely
   total row order, but stated because the failure mode is silent.

These are properties of the **row** form (`ReadItem`'s `Ord`/`Eq`), not of
the collection enum. A batch operator (spike D) must preserve both; a row
operator gets them for free.

## Who depends on which form

| Spike | Form it touches | What it assumes from this contract |
|---|---|---|
| **A. DD ⊇ `Value`** | `Value` = data-axis `BatchContainer` on Key/Val; row form = `{Owned, ReadItem<'a>}` | `Value` implements `BatchContainer` for Key/Val (DD owns Time/Diff); row form supplies `Owned: Clone+Ord` *and* `ReadItem: Copy+Ord`; both coherence clauses hold. **Known cost:** DD merge compares `ReadItem`s row-at-a-time through the column abstraction — columnar's deliberately-slow path — so "native" hinges on a cheap borrowed-row comparison, not on the type fit (which is clean). |
| **B. timely progress-as-data** | — | none (independent; produces capability/summary log streams) |
| **C. DDIR `Row` → `Value`** | `Row` becomes row form; DDIR collections become collection form | replaces the i64-only `RowLike`; needs row `Ord` for `Reduce`/`Join` keys |
| **D. DD additive batch op** | consumes/produces collection form | must preserve the coherence law across the batch transform |
| **E. collie foreign verbs + reachability** | operates on collection form natively; rows are logical timestamps | foreign `{less_equal, results_in, followed_by}` are row-level predicates lifted over the column |

## Open questions (resolve by amending this file)

1. *(Resolved by DD spike.)* Row form is **both** an owned `Owned: Clone+Ord`
   (key extraction) and a borrowed `ReadItem<'a>: Copy+Ord` (merge
   comparison) — not one or the other. The columnar crate already produces
   this borrowed-row shape; collie's `Arc<Vec<…>>` does not yet, and that
   borrowed-row-ref plus its `Ord` is the actual work item for spike A.
2. **`Send`/`'static` on `Value`.** Payloads are `Arc`-wrapped; confirm the
   container form crosses thread boundaries as DD requires.
