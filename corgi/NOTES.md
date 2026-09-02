# corgi — notes & TODO

A minimal **columnar, single-input term-graph IR** with a layered op vocabulary, a structural
shape-checker, an `ml` front-end, and a small optimizer. Standalone lib crate (no deps).
`cargo test` green; `cargo clippy --all-targets` clean; `cargo run --example tour` walks the
language; `cargo bench --bench eval` measures throughput.

## Orientation (module map)

```
src/
  value.rs     Value (columnar data) + show. Leaf = Prim, a width-tagged Arc<Vec<uN>>
               (u8/u16/u32/u64) via the `prim!` macro. Sum = (Tags, variants), where Tags is the
               lane assignment: Const(tag, rows) or Column(u8 tags, within-lane offsets).
  engine.rs    row-movement primitives: gather, concat, fill + index generators
               (filter_mask / owner_ids / resolve_indices / expand_ranges).
  cmp.rs       the order machinery: compare_idx (bulk structural order over index pairs; compare_cols
               is the diagonal case) + the linear discrimination sort (sort_blocks / run_layout /
               segment_labels). compare2 is the scalar reference, now test-only. Consumers are the cmp ops.
  graph.rs     OpLike, NodeKind{Input,Tuple,Op(O)}, Graph<O>, Builder<O>, eval_graph / try_eval_graph,
               shape_of (= try_eval_graph on `Value::empty(shape)`), check. eval_graph CONSUMES its arg and MOVES values to last use (enables in-place).
  shape.rs     Shape (Prim(width) | Prod | Sum | List) + shape_of_value + Display.
  optimize.rs  cse / dce / peephole / fuse_maps / cancel_isos over Graph<NumOp>. OPT-IN: `run` evals
               the unoptimized graph; tested for semantic preservation on every corpus program, so the
               passes are latent, not dead.
  effect.rs    the effect layer as a REWRITE: `lower_effects` threads `Fail<T>` columns past the ops
               downstream by inserting `MapSum`-on-the-Ok-lane / `Lift` / `Hoist*` / `Squash`; `is_total`
               is the syntactic query. No second evaluator: the lowered graph is pure vocabulary.
  ops/
    core.rs    Op<L>: structure only, organized as the KERNEL MATRIX
                          intro          elim     map       capture
                   PROD   tuple (graph)  Field    —         —        (transparent; no witness column)
                   SUM    Branch/Inject  Unwrap   MapSum    CapSum   (witness: tag column)
                   LIST   Enlist         Get      MapList   CapList  (witness: bounds column)
               LIST elim: Get (U64,List<X>)->X is the point eliminator (`head` = sugar `Get 0`, so an
               empty row is Oob 0 — no non-emptiness proof). Fold (B,List<A>)->B is the accumulating
               elim; FoldScan (T,List<A>)->(T,List<R>) (mapAccumL — the scan kernel; `scan` is sugar =
               FoldScan with body (a,x)->(b,b), field 1). Get/Gather come in _uns (assert) and _try
               (total -> Sum{Oob|Found}) tiers; Gather is Get vectorized (a list of indices). Plus Unit
               (X -> Unit) and the typed numeric grid + named reductions in `numeric`.
               plus the structural isos — all three pairs present: List⊗Prod (Transpose/Zip),
               List⊗List (Flatten/Slices), List⊗Sum (Unweave/Weave) — and the fused forms/producers
               (Lit/Cast/Filter/Gather/Iota), each reducible to kernel+isos (the `law` corpus
               programs witness it), kept for the execution strategy the expansion loses. The
               boolean mask split is the idiom `Branch(2)`; a dedicated Partition op was removed. Body-generic over L; inherent
               eval/children; NOT OpLike. (Iota: U64->List<U64> data gen; MapSum: variadic match,
               Vec<(tag,body)>, unlisted variants pass through, disjoint tags so arms commute.)
    cmp.rs     CmpOp: Rel(Pred) + Gt + SortList/DedupList/GroupKey/Find. Kind-blind comparisons.
    numeric.rs NumOp { Core(Op<NumOp>), Cmp(CmpOp), Arith(ArithOp), Text(TextOp) } : OpLike. ArithOp = the
               (op × kind × width) grid + AddU64/ReduceSum + Shr/And (SIMD ÷2^k / mod 2^k). enc_i64/dec_i64.
    fail.rs    the failure family: `Fail<T> = Sum{Ok:T | Err:Unit}` as ordinary data. The `Try*` total
               per-row producers (get/gather/branch/zip/slices/filter/chunk), `Lift`/`Squash`, and the
               three distributive laws `HoistProd`/`HoistList`/`HoistSum` (Fail commuted out through each
               functor). The evals live here; `Op::eval` dispatches to them first.
    text.rs    TextOp: Split(u8) + ParseU64. Byte-leaf interpretations (a string is List<U8>); both
               total — ParseU64 returns Sum{Err: bytes | Ok: U64}, no data-dependent panic.
  frontend/
    mod.rs     the op-name resolve table (the whole vocabulary the surface reaches).
    ml.rs      the one surface: ML-flavoured (let / enum / juxtaposed stages / match / inject), lowering to Graph<NumOp>.
tests/  corpus (runs programs/*.col) · ml · typer · numeric · optimize · text   (no Builder-demo file —
        every surface example, algebraic law, and property test lives in the corpus.)
programs/  *.col — the self-generating example corpus (program + `# n =` seed + `# =` golden, or
           an equivalence via `(A, B) eq → [1]`). One source: tests/corpus.rs verifies, the tour displays.
examples/tour.rs   benches/eval.rs   dev/*-kickoff.md
```

## Structural completeness — the functor commutation table

The structural layer (PROD/SUM/LIST, leaving PRIM in its corner) is complete when every way the three
functors *commute* is either a named iso with a witnessing `law` program, or a documented hole with a
reason. The combinators are the **distributive laws** (functor-through-functor) and the **strengths**
(× through a functor); with three functors the table is finite and small. Two families carry it:

**Push LIST inward (the AoS→SoA transform — why corgi is columnar):**

| value | ↔ | columnar form | ops | law |
|-------|---|---------------|-----|-----|
| `List<(X,Y)>` | ↔ | `(List<X>, List<Y>)` | Transpose / Zip | programs/32 |
| `List<Sum{A\|B}>` | ↔ | `(tags, List<A>, List<B>)` | Unweave / Weave | programs/33 |
| `List<List<X>>` | ↔ | `(ranges, flat)` | Flatten / Slices | programs/17, 31 |

Note `List<Sum{A|B}>` is **not** `Sum{List<A>|List<B>}` — a list mixes variants, so the SoA form is the
*shredded* `(tag-list, per-variant payload columns)` (Dremel/Parquet record shredding). That is exactly
what Unweave produces; the "obvious" `List<Sum> → Sum<List>` does not exist.

**Distribute × into a functor (the CAPTURE / strength family):**

| value | ↔ | distributed | op | law |
|-------|---|-------------|-----|-----|
| `(X, Sum{A\|B})` | ↔ | `Sum{(X,A)\|(X,B)}` | CapSum (= `(X,A+B)≅(X,A)+(X,B)`) | programs/30 |
| `(X, List<Y>)` | → | `List<(X,Y)>` | CapList (List strength) | programs/40 |
| `(X, (A,B))` | ↔ | reassociation | Tuple/Field | trivial |

**The principled holes (no iso — documented, not missing):**
- **Sum over Prod** — `(A,B)+(C,D) ↛ (A+C, B+D)`: re-pairing loses the field correlation. Not iso.
- **Sum/List over List `→` Sum** — `List<Sum>` cannot become `Sum<List>` (mixed variants); this is the
  reason Unweave's target is the shredded triple, above.
- diagonals (Prod∘Prod, Sum∘Sum) — trivial reassociation, no op.

So the live cells (LIST-inward ×3, ×-into-{Sum,List} ×2) are all suite-checked; the rest are holes with
reasons. Adding a structural op means either filling a hole (and writing its law) or it is redundant.

## Design invariants (don't break)

- **Every semantic op is a unary `T0 -> T1`** (the 1:1 map), run by `eval`. `Input`/`Tuple` are the
  only non-ops — they're `graph::NodeKind`.
- **Shape = structure + leaf width, kind-blind.** Numeric kinds (signed/float) are an interpretation
  a layer encodes, never a Shape. `shape_of` is LITERALLY `eval` on a zero-row column: every op is
  total on zero rows, reports a mismatched operand as `Err` (the accessors `into_pair`/`into_list`/…
  carry the message), and builds an output of the shape it would at any length — so there is one
  vocabulary, one evaluator, and no type-level shadow of it to keep in sync. `eval_graph` unwraps
  (a typechecked program cannot fail); `try_eval_graph` is the fallible form the typer and the
  body-bearing ops use.
- **Layering = enum embedding via `OpLike` + body-generic `Op<L>`.** A layer is `{ Core(Op<Self>),
  <buckets> }` impl'ing `OpLike` by delegating; the graph machinery is unchanged across layers.
- **The core is numeric-blind.** Arithmetic is `ops/numeric`; comparison is `ops/cmp`. The leaf is
  stored order-preserving (signed = top-bit swizzle), so ONE kind-blind comparator serves `sort`,
  `find`, and `Rel` alike.
- **Float semantics are TOTAL-ORDER, not IEEE (deliberate).** `Kind::F` (widths 32/64) stores the
  IEEE bits under the total-order swizzle (`f64::total_cmp`: negatives flip all bits, others flip the
  sign bit). The kind-blind comparator then orders floats correctly with NO special case — *the
  swizzle never mis-orders two values IEEE orders; it only supplies a definite position where IEEE
  declines.* The deviations, all on the "naughty" sort/eq path: NaN is orderable (sorts to the top)
  and equals itself bit-for-bit; `-0 != +0` (distinct bits — no canonicalization, by choice). The
  win: no NaN-poisons-comparison surprise. *Arithmetic stays IEEE* (NaN/inf propagate; `x/0 -> ±inf`,
  `0/0 -> NaN` — total, no panic). A future `fXY_eq` can offer IEEE equality if needed. Floats enter
  via `to_f32`/`to_f64` (no float literal token: a constant is `lit_uN K to_fN`); the typed grid is
  reached by suffix (`add_i32`, `div_f64`, `lit_i16 N`, `signed`). Integer `div` is deferred (no NEON
  op; div-by-zero would panic) — `eval` rejects it.
- **All cardinality change lives inside `List`.** Filter/Group/Reduce are `List<X> -> …`; the SEQ
  level is always 1:1.
- **List rows carry a stride-aware `Bounds`.** `Value::List` holds `Bounds { Stride(stride, rows) |
  Offsets(Vec) }` — the dynamic mirror of `columnar`'s `Strides` (corgi is the *dynamic* columnar: it
  interprets programs that can't announce types ahead of time, so it can't adopt static `columnar`
  wholesale — it mirrors the layout and bridges at the serialization edge). Uniform-width rows are
  detected O(1) (`strided`), and the property PROPAGATES: every op that preserves the partition reuses
  the `Bounds`, so a uniform list keeps "I'm uniform" through a pipeline rather than re-deriving it.
  That lets uniform data recover dense / array-language kernels as a special case — `sort` of an
  equal-width byte list packs each record to a u64 key and radixes once (measured ~7×→~1.3× of the
  dense leaf sort at 1M/8M; `tests/kernel.rs::stride_sort_matches_offsets` pins the fast path to the
  structural result). `enlist` emits `Stride(1)`; `eq`/`hash`/`show` are by the partition, so a
  `Stride` and the equivalent `Offsets` are interchangeable. Array languages are the all-uniform case.
- **Sum rows carry a uniformity-aware `Tags`.** `Value::Sum` holds `Tags { Const(tag, rows) |
  Column(u8 tags, Arc<Vec<usize>> within-lane offsets) }` — the `Sum`-side twin of `Bounds`, and the
  dynamic mirror of `columnar`'s `Discriminant` (whose "homogeneous" state stores `[tag, count]` and
  synthesises identity offsets). `Const` is the ONE-TAG case: it costs two words at any row count,
  because row `i`'s offset in the single lane is `i`. It is what `inject` and `lift` build, and what
  a `Fail` column that has not actually failed stays in — so a fallible pipeline in its normal state
  carries no witness columns at all. Uniformity is O(1) to detect (`const_tag`) and it PROPAGATES: a
  gather of a one-tag sum is one, a lane map leaves the assignment alone, and `unwrap`/`sort`/
  `compare` each take a no-witness path on it. Equality and hash are by the ASSIGNMENT, so a `Const`
  and the equivalent `Column` are interchangeable — including on the wire, where the codec records
  which form the sender held (as it does for `Bounds`).
- **`Fail<T> = Sum{T | Unit}`, so "did anything fail" is a field read.** The Err lane carries no
  payload, only a length, so the failure count is O(1) on any fallible column. `into_fail`,
  `squash`, `hoist_prod` and `hoist_list` ask that FIRST and take a no-copy path when the answer is
  zero; materialising a `Vec<bool>` mask to discover it was the cost of the common case. The static
  optimizer cannot do this work instead: `lower_effects` inserts `Lift`/`Squash`/`Hoist*` only where
  a column genuinely CAN fail, so there are no trivially-cancellable pairs to peephole — whether it
  *did* fail is a runtime property, which is why the check lives in the ops.
- **Leaves are immutable Arc, cloned by refcount; eval moves to last use.** The last reader holds the
  sole Arc, so `into_*` move the buffer and pointwise ops are able to mutate in place (`AddU64` does).
  The WITNESS columns are Arc for the same reason — `Bounds::Offsets`, and a `Tags::Column`'s
  offsets — so a `Value` clone costs O(shape), not O(rows), at every shared edge in a graph.
  *Reuse policy:* an op that is elementwise AND same-width (`AddU64`/`Shr`/`And`, `bin_into`, `neg_into`,
  `lane_pick`, `xor_signbit`, the in-place fold scatter) consumes its operand and rewrites it under
  `Arc::get_mut`/`make_mut` when uniquely owned — take the reuse wherever the shape allows. The
  fresh-allocating leaf ops (`gather`/`gather_lanes` = permutation, `cast` = re-width, `rel`/`cmp_idx`/
  `sort_block` = a differently-typed result) allocate *by necessity*, not oversight — the access pattern
  or output type rules reuse out. (Cross-op intermediate elimination is the separate DPS backlog item.)
- **`Fold`/`FoldScan` are cross-row lockstep, `O(total elements)`.** A general (non-associative) fold is
  sequential *within* a row but vectorized *across* rows: round `t` folds in every still-active row's
  `t`-th element in one body call, so `#rounds = the longest row`, not the element count. The active
  set is maintained incrementally (`init_active` + per-round `retain(len > t)`), so per-round cost
  tracks the *active* rows — total work `O(total elements)`, asymptotically optimal (each element
  touched a constant number of times). The accumulator is scattered back **in place** for fixed-width
  `B` (a leaf or product of leaves); a `List`/`Sum` `B` falls back to the `gather_lanes` rebuild.
- **`FoldScan` (mapAccumL) is the scan kernel; `Fold` is its R=Unit specialization, kept for cost.**
  `FoldScan : (T,List<A>)->(T,List<R>)` by `(T,A)->(T,R)` threads a state and emits an output stream;
  `scan` lowers to it (body `(a,x)->(b,b)`, take field 1) — measured identical to a dedicated scan.
  `Fold` does NOT lower to it: `FoldScan` with `R=Unit, .0` measured ~3.4x slower, because the body is
  forced to emit a `(state, output)` PAIR each round (extra `Prod` build/teardown + it breaks the
  body's in-place accumulator mutation) and the lockstep records output positions even for a dead
  `Unit` stream — the `Unit` *values* are free, the *pairing* and *bookkeeping* are not. So `Fold` is
  the no-pair/no-recording path. (Equivalently an optimizer rule `FoldScan[R=Unit].0 -> Fold` would
  recover it — DCE the dead output, skip recording — which restores the in-place mutation.)
- **Named monoid reductions and scans** (`fold_add`/`mul`/`min`/`max`/`all`/`any` and the prefix `scan_add`/…) are the one-SIMD-pass fast
  paths for the associative case — prefer them; `Fold`/`FoldScan` are for non-monoid bodies. Remaining
  constant-factor lever (unbuilt): the all-active fast path (move `acc` through the body, skip the
  identity acc-gather + scatter) for the uniform-length regime where every row is active every round.

## Totality — partiality as data, threaded by a rewrite

The surface's fallible verbs (`get`/`head`, `gather`, `branch`, `zip`, `slices`, `filter`, `chunk`)
are TOTAL per-row producers: a row that would trip the partial kernel's assert lands in the Err lane
of `Fail<T> = Sum{ Ok: T | Err: Unit }` (`ops/fail.rs`). Everything downstream is written against `T`;
`effect::lower_effects` makes that well-typed by inserting ordinary ops — a pure op fed a `Fail<T>`
becomes `MapSum([(0, op)])` on the packed Ok lane, a second fallible op adds a `Squash`, a `Tuple`
with a fallible field `Lift`s the pure ones and `HoistProd`s, and a body-bearing op whose body fails
`HoistList`s / `HoistSum`s the per-element errors out to the row (all-or-nothing). `try` is the
identity on values: it marks where the program takes the `Sum{T | Unit}` up as data to `match` on.

So there is ONE evaluator and ONE typer. `Program::run_partial` = `eval_graph(lower_effects(g))`; the
corpus test types every lowered program with `shape_of`, which is what proves the discipline: an op
applied to a fallible column where lowering forgot to lift would be a shape error. `is_total` is the
separate syntactic query — total iff every fallible column meets a `try` before the output; `run`
refuses a partial program, `run_partial` returns its `Fail<T>` as the value.

The partial kernels (`Op::Get`, `Gather`, `Filter`, `Slices`, `Chunk`, `Zip`, `Branch`) stay in the enum for
a host holding a bounds proof (DDIR); they are not on the surface. `gather_try` is distinct: the
per-ELEMENT `List<Sum{Oob | Found}>`, a value the program handles itself, not a per-row effect.

**Audit rule, kept from the old gates:** an analysis threaded through a fixpoint (`Fold`/`FoldScan`'s
accumulator back-edge) must treat the fed-back value as unknown; the lowering does this by making the
accumulator itself a `Fail<B>`, so a row that errs on any round stays Err.

## Done (foundations in place)

The byte-width-leaf migration, the (op×kind×width) numeric grid, the linear discrimination sort,
Arc leaves + move-on-last-use + in-place `AddU64`, owned-arg `eval`, sum introduction (`Inject`),
the `CmpOp` bucket including the `Rel(Pred)` compare-to-mask family, and consolidation to the single
`ml` surface — all landed. The bench shows the streaming ops memory-bound and already
NEON-vectorized; `sort_list` is the lone compute-bound op.

Then the kernel-matrix session: `Op<L>` reorganized as (intro/elim/map/capture) × (Prod/Sum/List)
with `CapSum` closing the matrix and `Broadcast` renamed `CapList`; `Gather` (index-as-value) and
`Head` (the stratum drop) added; the three iso pairs completed (`Zip`, `Unweave`/`Weave`);
`Partition` removed as redundant with `Branch(2)`; `Find`/`Rel` check shape equality; `eval` rejects what it can't represent (`Cast` widths, sum
arities > 256). The law-program pattern (corpus 27–34) witnesses every embellishment's reduction to
kernel+isos, so the kernel's sufficiency is suite-checked.

Then the point-access factoring. `Index`/`Head` were retired in favour of one indexed-access concept
at two strata: the atom is the **scalar `Get (U64,List<X>)->X`** (one O(1) lookup per row, the genuine
list eliminator), `Gather` is its **vectorization** (the index arrives as a list), and `head` is **sugar
`Get 0`** — so `Op::Head` left the engine and a *total* head needs no non-emptiness proof (an empty row
is `Oob 0`). Each of get/gather carries a `_uns` tier (assert in-bounds) and a `_try` tier (total ->
`Sum{Oob|Found}`); the plain checked tier (a proven bound) is reserved. Why this direction and not
`Get = enlist;gather;head`: `gather` is list-*preserving* so it can't eliminate, and the only
irreducible piece is the bare-`X` outro — making `Get` the atom keeps one index kernel and yields the
total head for free, where the HEAD-atom route would have needed new non-emptiness analysis. (`Fold`/
`FoldScan` remain the accumulating eliminators; `head_try`→Option is expressible as a fold if wanted.)

## Live work — the DDIR consumer

The real consumer is **DDIR** (`../../differential-dataflow/interactive/`): DD hosts opaque `Value`
blobs and pushes commands (corgi `Graph`s) at them — `eval_graph(graph, value) -> value`. corgi is
the per-batch linear/expression engine; DD keeps Join/Reduce/Arrange/iteration. Three pieces:

1. **Data bridge (interop).** `columnar::Vecs<Vec<i64>>` ⇄ `Value`. MVP: copy at the boundary
   (transpose into fresh Vecs). The real fork is **align `Value` with `columnar` (DD-native, bespoke)
   vs Arrow/narrow (ecosystem interop)** — DDIR pulls toward `columnar`.
2. **Lowering.** DDIR's `Linear`/`FieldExpr`/`Condition` → corgi `Graph` (Field/arith/Rel/tuple;
   `And` = `Mul` of 0/1 masks). The host lowers to `Graph`; it never hand-builds.
3. **Compute gap — DONE.** `Rel(Pred)` (the six comparators) covers `Condition`.

## Backlog

- **Fusion / vector-at-a-time (the perf multiplier).** Single ops are memory-bound, so the lever is
  fewer passes, not SIMD. Tile execution ~1024 rows to L1 (the Polars/X100 model), composing the
  existing SIMD kernels — no per-row interpreter. Needs a slice-capable op path + a fusable-run pass.
- **Destination-passing style (DPS) — the intentional discipline for the fusion above (not yet built).**
  DPS (Shaikhha/Fitzgibbon/PeytonJones/Vytiniotis) is the calling convention that makes the tile fusion
  work: thread a destination buffer through a chain of ops, each writes into it, no intermediates. The
  seam is already corgi's central invariant — **DPS along the 1:1 SEQ spine** (pointwise/leaf/cast/
  select/fold-accumulator: `dest size = input size`, pre-sizable), **allocate-and-return at `List`
  introductions** (filter/group/iota/slices: data-dependent size). Relation to FBIP: corgi ALREADY does
  opportunistic refcount reuse (`get_mut`/`make_mut` in `bin_into`/`scatter_into`/`AddU64`/move-to-last-
  use) — that's reuse *discovered* at runtime; DPS makes it *intentional* (explicit destination →
  guaranteed in-place, AND it threads through a chain, which is what unlocks fusion; per-op reuse
  already works, so the new value is specifically cross-op intermediate elimination). The `None`
  destination = "output is dead" idiom collapses `FoldScan -> Fold` (skip the tags/off recording + DCE
  the dead output), which would retire `Op::Fold` the way `Op::Scan` was retired. THE fork to settle
  first is the ownership model: a true destination can't be a shared `Arc`, so DPS pressures the hot
  spine toward a linear/owned tile buffer (giving up free `Arc`-clone fan-out there) vs staying
  `Arc`-shared (free clones, no fusion). First spike: compile ONE stratum-stable run (e.g. `iota ; add
  ; mul ; gt`) to a single-tile DPS kernel and measure vs the per-op passes — that forces the ownership
  decision on a small surface before committing the convention.
- **Index-as-value — op DONE, rewrite pass open.** `Op::Gather` (row-relative point gather; `Slices`
  is the range form) makes indexes plain values; programs/26 (pointer jumping) and /27 (the law
  `gather(gather(v,i),j) = gather(v, gather(i,j))`) exercise it. Open: the optimizer rewrite applying
  that law, so gather chains become index math + one final gather. The lazy form (multiplicity View:
  0 = filter, ≥1 = repeat, range = slice) stays OUT of the representation — collie's `Selector`
  (4 variants × a composition matrix × per-op awareness) is the cautionary tale; laziness lives in
  the pass, where corgi can see the whole chain.
- **Vectorized abstract machine — the CPS connection (to discuss).** The term graph with let-sharing
  is already ANF (the "essence of CPS", Flanagan et al.), so CPS's bookkeeping benefits — named
  intermediates, explicit order, local rewrites — are built in. The deeper half, control flow
  becoming DATA, lands on the ADT machinery via Reynolds: with no function values, continuations
  defunctionalize to a Sum of "what remains to do" plus an apply dispatching on tags — and that pair
  IS Sum + MapSum. A sum column is a batch of suspended control decisions, the tag column a column
  of program counters; `match` runs each continuation once over the rows that chose it. Conclusion
  to pursue: a column of CEK-machine states stepped by MapSum is a vectorized interpreter — the same
  destination as the term-column/CSE thread (programs/28), reached from the control side. Arguably
  the shape of "interpreted columnar evaluation of programs".

- **Serialization / durability.** Leaves are flat typed buffers → write columns out/down,
  content-address. Latent strength (vs roto's statelessness); unbuilt. Option-as-`Sum` vs
  Arrow-validity-bitmap is the representation reconcile point.
- **Recursion / μ-types** — arbitrary-depth JSON; needs a recursive-column construct, and a
  length-carrying `Unit` for `null` / `Option` None.
- **JSONL / Extern** — `split`/`parse_u64` landed as the `text` bucket (typed, not `Op::Extern`); `parse_json` remains open and still wants `Extern` or recursion (μ-types below).
- **Named declarations — enum half DONE; struct half deliberately skipped.**
  `enum Name = V0 | V1 in …` is a parse-time table (variant-name → (tag, arity)); names erase at parse and the core stays positional.
  Use sites: `inject V` (the tag AND the whole sum's lane shapes off the declaration), `map_variant V`, named `match` arms, `branch Name` (arity by enum name).
  Payload shapes: `enum Node = Lit u64 | Add (u64, u64) | Str List(u8) | Wrap Other in …` — a variant may omit its shape unless the
  enum is ever `inject`ed (then every lane needs one, so the other lanes can be built as EMPTY columns of their shapes). Shapes nest by
  naming an earlier enum; no recursion (μ-types are the backlog item below). There is no `⊥`: every Sum lane, in values and in shapes,
  is concrete, so `shape::join` is gone and every merge (`Unwrap`/`Select`/`Find`/`Append`/fold state) is an equality check.
  Companions landed with it: lambda parameters take `let`-style tuple patterns (`map ((lo, hi) -> …)`), and pair-eating binaries accept an immediate (`x sub 1` ≡ `(x, x lit 1) sub`; the core's `And`/`Shr`/`AddU64`/`Gt` immediate kernels are untouched).
  Field-name projection (`s.a`) and record literals stay OUT: parse-time resolution would need globally-unique field names (a misapplied name silently projects the wrong index) or typed resolution, and destructuring covers the corpus without either.
  Mechanical closure capture (free vars threaded via `CapList`/`CapSum`) remains the open companion pass.
  Programs/28 exercises the whole bundle and the sum-heavy programs (09, 11, 18, 19, 23–25) use the named style; the numeric `inject tag arity` form is gone (a sum is only built from a declaration).

- **Kind-checking numeric front-end** — where `i32` / `f32` live; type-checks kinds, inserts
  swizzles, lowers to `NumOp`. Today's surface is kind-blind (emits `add` / `gt` / `lt` / …).
- **Length / stratum checker** — the one judgment `shape_of` skips (Tuple/Add same length; map body
  one stratum deeper). A pass beside `check`.
- **Sum random-access cost — RESOLVED (via representation).** A `Value::Sum` carries each row's
  offset WITHIN its lane, built once at construction and maintained by `gather`/`concat`.
  `compare_idx` reads it O(1), so `Rel`/`find` over sum-shaped data are linear (no per-call rank
  scan). The earlier plan was find-local block-starts *avoiding* a representation change; carrying
  the offset proved simpler and uniform.

## Conventions

Run after any `src/` change: `cargo test && cargo clippy --all-targets`. Dependency-free and tight.
Adding an op: ONE arm in `eval` (a failure-family op's lives in `ops/fail.rs`). There is no judge to keep in step: `eval` on a zero-row column IS the typer, so an op's shape rule is its accessor calls (`into_pair`/`into_list`/…, whose `Err` is the shape error) plus any explicit `same(..)` check it needs; the output shape is whatever it builds. Adding a layer:
an enum + `OpLike` + `From` impls; touch nothing below.
