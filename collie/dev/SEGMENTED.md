# Segmented compute — kickoff doc

**Goal.** Let compute ops preserve `List` (per-row) grouping instead of
forcing a flatten. Today reshape ops (`nest`, `list>ranges`, `flatten`,
`view`, `bounds>keys`) move freely between flat and grouped, but the
per-element/compute ops (`=` `<` … , `+` `-` `*` `/` `%`, `as`, `neg`/`abs`,
`and`/`or`/`not`, and `where`/`gather`/`filter`) are **flat-only**. So any
grouped computation must `flatten` (discarding bounds) then rebuild the
grouping by hand. The witness is `examples/19_wco_lftj_match.col`'s
`lftj_lane_g`, whose tail is a re-nest dance:

```
queries hit where gather                       :surv     # flat survivors
p_adj p_pos view list>bounds                   :pbnds
hit as.u64 pbnds nest  reduce.+.u64            :cnts     # survivors per anchor
surv  u64[0] cnts cumsum.u64 cat.2  nest                 # rebuild List<c>
```

That entire tail exists only because compute flattened the grouping. With
segmented compute it collapses to roughly: keep `positions`/`queries` as
Lists, compute the mask per-row (`List<bool>`), then one segmented `filter`.

This is **BACKLOG #0** (the SEQ→SEQ recipe applied to compute/`filter`) made
concrete, with **#0b** (trie-walker) as the eventual structural form (carry
per-group bounds through compute so flatten/re-nest never happens).

## Two kinds of op (the key distinction)

- **Element-wise** (`cmp`, `arith`, `as`, boolean): grouping is a
  *transparent restriction* (principle 4). The op never crosses row
  boundaries, so per-row counts are unchanged: compute on the flat values,
  **reattach the same bounds**. Same op, dispatch on representation
  (List vs flat) — recipe-compliant, *easy*.
- **Count-changing** (`filter`/`where`): per-row output length differs.
  Needs a real segmented kernel (per-row survivor counts → new bounds).
  This is `filter`, the one survey op that never got a List form
  (`reduce`/`count`/`cumsum`/`shift` all did).

Reduce-like ops are NOT this work: `reduce`/`count` already have per-row
forms because there the List is *semantic* (per-row vs whole), not a
transparent restriction.

## Steps (each compiles + tests independently)

### Step 1 — element-wise compute preserves `List` (cmp, then arith)

Add a `List` branch to the binary element-wise ops. Approach: if both
operands are `Value::List { bounds: ba, values: va }` and `Value::List
{ bounds: bb, values: vb }` with **equal bounds**, run the existing flat
kernel on `va`/`vb` and rewrap with `ba` → `List`. Also support
`List` op length-1 scalar (broadcast the scalar across the flat values,
reattach bounds). Mismatched bounds → error ("segmented op: bounds differ").

- Files: `src/ops/cmp.rs` (`Cmp::run`), `src/ops/arith.rs`
  (`Arith::run`, `UnaryArith::run`). Inject the List branch *before* the
  existing view fast-paths / flat fall-through.
- Shared helper (suggest `src/ops/helpers.rs`):
  `list_elementwise2(a, b, |va, vb| inner) -> Option<Value>` and
  `list_elementwise1(a, |va| inner)` that peel/rewrap, returning `None`
  when not List-shaped so the caller falls through to its existing paths.
- `tc` side (`Typed`): `List<Prim> List<Prim> → List<Prim'>` element-wise;
  width/shape rules same as flat, bounds propagate. Equal-bounds-width.
- Unary `as` on a `List<Prim>` → `List<Prim'>` (trivial; same bounds).
- Boolean `and`/`or`/`not` (reduce_ops.rs) — same treatment if needed by
  the witness.
- **Verify:** `nest` two lists, compare → `List<u8>` with same bounds;
  e.g. `u64[1 2 3 4 5] u64[0 2 5] nest  u64[9 1 9 9 9] u64[0 2 5] nest  <`
  → `List<u8>` matching per-row. Add a unit test in `cmp.rs`/`arith.rs`.

### Step 2 — segmented `filter`

`List<T>` filtered by an aligned `List<bool>` (same bounds) → `List<T>`,
keeping per-row the elements where true. Internally: flat `where gather`
for the values, plus per-row survivor counts (sum the mask per row) →
new bounds. (The `nest`-by-counts idiom; consider also exposing a
`nest.counts` that builds a List from values + per-row counts, removing
the `u64[0] … cumsum cat.2` boilerplate.)

- File: `src/ops/list.rs` (new op next to `Filter`), register token.
- Decide the surface: a distinct token (e.g. `filter` already exists flat;
  could dispatch on List input like step 1 — preferred for recipe
  symmetry: `filter` on a `(List<T>, List<bool>)` → `List<T>`).
- `SystemOp` variant + `promote` + `as_kernel` (mirror `Filter`/`Bounds`).
- **Verify:** filter a `List<u64>` by a same-bounds `List<u8>` mask;
  per-row survivors match a hand-computed expectation.

### Step 3 — rewrite `lftj_lane_g` to stay grouped (validates 1+2)

Keep `positions`/`queries` as Lists; compute `in_range`/`hit` per-row
(step 1); produce `List<c>` with one segmented `filter` (step 2),
deleting the re-nest tail. Output of `examples/19_wco_lftj_match.col`
must stay byte-identical (set + order). Compare triple count (2,999,997)
and sorted column sums (99/98/97 at N=10) as before.

### Step 4 — (later, big) trie-walker (#0b)

Generalize "carry per-group bounds through compute" into a stateful
per-layer driver. Out of scope for this doc; see BACKLOG #0b.

## Constraints / watch-outs

- Element-wise on Lists requires **equal bounds** (same grouping). Don't
  silently broadcast a List against a different-bounds List.
- Respect the recipe: this is *representation dispatch* (same op, List vs
  flat), NOT a new `*.seg` op family. The flat path must stay unchanged.
- `View` inners: the witness uses `p_adj p_pos view` (a `List<View<…>>`).
  Step 1 should handle `List` whose values are `Prim` first; `View` inner
  values may need the `RowAccess`/read-through treatment (BACKLOG #0
  prerequisite). Start with `Prim` inners; note where Views are needed.
- Keep the boiling property: these are pure `SystemOp` variants (not
  body-bearing), so graphs stay boilable.

## State — Steps 1–3 LANDED (2026-05-25)

All three steps done; 121 tests (114 baseline + 7 new) + 19 examples green.

**Step 1 — element-wise compute preserves `List`.** Shared helpers
`list_elementwise2`/`list_elementwise1` in `helpers.rs` peel/rewrap a `List`
(equal-bounds Lists, or List op length-1 scalar; mismatched bounds → error
"segmented op: bounds differ"). Wired into the `run` of `cmp` (`Cmp`), `arith`
(`Arith` + `UnaryArith`), `as` (`convert::As`, cast factored into
`cast_prim`), and boolean `and`/`or`/`not` (`reduce_ops`). Each grew a
`List<Prim>` typecheck arm (cmp → `List<P8>`, arith → `List<Prim>`, etc.);
`shape::prim_width` is the shared peel helper. The flat/View paths are
unchanged — the List branch sits *before* them and returns early.

**Step 2 — segmented `filter`.** Representation dispatch on the existing
`Filter` op (no new token; `SystemOp::Filter` already wired). Keyed on the
**mask** being a `List<P8>`: materializes the (possibly row-shaped View) src
into a real List, checks equal bounds, keeps per-row the elements where the
mask is true, and computes new bounds from per-row survivor counts. Eager
(unlike flat `filter`'s lazy `View<Mask>`), since per-row output length
differs. No `nest.counts` op was needed — the count→bounds step is folded in.

**Step 3 — `lftj_lane_g` rewritten.** The 4-line survivor re-nest dance
(`where gather` + `list>bounds` + `nest` + `reduce.+` + `cumsum` + `cat` +
`nest`) collapsed to one segmented filter:
`queries  hit queries list>bounds nest  filter`. Output byte-identical
(2,999,997 triples; old-vs-new full-column fingerprint equal; truncated
displays identical at N=10 and N=1M).

### Findings worth keeping

- **The witness's *compute* stays flat, by nature.** `ok`/`slot`/`cand`/`hit`
  are gather-driven (`cand = t_adj flatten drop slot gather`), and `gather`
  is flat-indexed — so the grouping necessarily breaks at the gather and is
  only recoverable at the *end*. So Step 1's element-wise List ops, while a
  genuine catalogue-wide improvement (and unit-tested), are **not** the
  dominant pattern in this particular witness; the real win here is the
  segmented `filter` (Step 2) eliminating the count-changing re-nest. Forcing
  the compute grouped would *add* spread+nest ops (per-anchor scalar
  broadcast of `tlen`/`base`), not remove them — anti-simplification.
- **The View-inner wrinkle surfaced exactly where the doc predicted**, but in
  its mild form: `queries = p_adj p_pos view` is a `Value::View` with a
  row-shaped `SequenceRange` selector — shape `List`, value `View`. The fix
  was a runtime `materialize_top` in segmented `filter` (the old code also
  materialized via `flatten drop`), **not** the deeper `RowAccess`
  read-through (BACKLOG #0 prerequisite). That heavier work is still only
  needed if we want the *compute* (cmp/arith on `List<View<Prim>>` inners) to
  stay lazy — out of scope here.

### Not done (Step 4)

- Trie-walker (#0b) — out of scope per this doc; see BACKLOG #0b.
