# corgi — notes & TODO

A minimal **columnar, single-input term-graph IR** with a layered op vocabulary, a structural
shape-checker, an `ml` front-end, and a small optimizer. Standalone lib crate (no deps).
`cargo test` green; `cargo clippy --all-targets` clean; `cargo run --example tour` walks the
language; `cargo bench --bench eval` measures throughput.

## Orientation (module map)

```
src/
  value.rs     Value (columnar data) + show. Leaf = Prim, a width-tagged Arc<Vec<uN>>
               (u8/u16/u32/u64) via the `prim!` macro. Sum = (u8 tag column, carried within-variant
               offset per row, variants); build via Value::sum / sum_from_prim to keep the offset valid.
  engine.rs    row-movement primitives: gather, concat, fill + index generators
               (filter_mask / owner_ids / resolve_indices / expand_ranges).
  cmp.rs       the order machinery: compare_idx (bulk structural order over index pairs; compare_cols
               is the diagonal case) + the linear discrimination sort (sort_blocks / run_layout /
               segment_labels). compare2 is the scalar reference, now test-only. Consumers are the cmp ops.
  graph.rs     OpLike, NodeKind{Input,Tuple,Op(O)}, Graph<O>, Builder<O>, eval_graph, shape_of,
               check. eval_graph CONSUMES its arg and MOVES values to last use (enables in-place).
  shape.rs     Shape (Prim(width) | Prod | Sum | List) + shape_of_value + Display.
  optimize.rs  cse / dce / peephole / optimize over Graph<NumOp>.
  ops/
    core.rs    Op<L>: structure only, organized as the KERNEL MATRIX
                          intro          elim     map       capture
                   PROD   tuple (graph)  Field    —         —        (transparent; no witness column)
                   SUM    Branch/Inject  Unwrap   MapSum    CapSum   (witness: tag column)
                   LIST   Enlist         Head     MapList   CapList  (witness: bounds column)
               LIST also: Fold (B,List<A>)->B (seeded accumulating elim), Scan ->List<B> (Fold keeping
               each step), Index (total point elim -> Sum{Oob|Found}), Unit (X -> Unit). Plus the typed
               numeric grid + the named reductions in `numeric`.
               plus the structural isos — all three pairs present: List⊗Prod (Transpose/Zip),
               List⊗List (Flatten/Slices), List⊗Sum (Unweave/Weave) — and the fused forms/producers
               (Lit/Cast/Filter/Gather/Iota), each reducible to kernel+isos (the `law` corpus
               programs witness it), kept for the execution strategy the expansion loses. The
               boolean mask split is the idiom `Branch(2)`; a dedicated Partition op was removed. Body-generic over L; inherent
               eval/judge/children; NOT OpLike. (Iota: U64->List<U64> data gen; MapSum: variadic match,
               Vec<(tag,body)>, unlisted variants pass through, disjoint tags so arms commute.)
    cmp.rs     CmpOp: Rel(Pred) + Gt + SortList/DedupList/GroupKey/Find. Kind-blind comparisons.
    numeric.rs NumOp { Core(Op<NumOp>), Cmp(CmpOp), Arith(ArithOp), Text(TextOp) } : OpLike. ArithOp = the
               (op × kind × width) grid + AddU64/ReduceSum + Shr/And (SIMD ÷2^k / mod 2^k). enc_i64/dec_i64.
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
  a layer encodes, never a Shape. `shape_of` is `eval` lifted to shapes.
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
  op; div-by-zero would panic) — the judge rejects it.
- **All cardinality change lives inside `List`.** Filter/Group/Reduce are `List<X> -> …`; the SEQ
  level is always 1:1.
- **Leaves are immutable Arc, cloned by refcount; eval moves to last use.** The last reader holds the
  sole Arc, so `into_*` move the buffer and pointwise ops are able to mutate in place (`AddU64` does).
- **`Fold`/`Scan` are cross-row lockstep, `O(total elements)`.** A general (non-associative) fold is
  sequential *within* a row but vectorized *across* rows: round `t` folds in every still-active row's
  `t`-th element in one body call, so `#rounds = the longest row`, not the element count. The active
  set is maintained incrementally (`init_active` + per-round `retain(len > t)`), so per-round cost
  tracks the *active* rows — total work `O(total elements)`, asymptotically optimal (each element
  touched a constant number of times). The accumulator is scattered back **in place** for fixed-width
  `B` (a leaf or product of leaves); a `List`/`Sum` `B` falls back to the `gather_lanes` rebuild. The
  **named monoid reductions** (`reduce_sum`/`min`/`max`/`prod`/`all`/`any`) are the one-SIMD-pass fast
  paths for the associative case — prefer them; `Fold`/`Scan` are for non-monoid bodies. Remaining
  constant-factor lever (unbuilt): the all-active fast path (move `acc` through the body, skip the
  identity acc-gather + scatter) for the uniform-length regime where every row is active every round.

## Done (foundations in place)

The byte-width-leaf migration, the (op×kind×width) numeric grid, the linear discrimination sort,
Arc leaves + move-on-last-use + in-place `AddU64`, owned-arg `eval`, sum introduction (`Inject`),
the `CmpOp` bucket including the `Rel(Pred)` compare-to-mask family, and consolidation to the single
`ml` surface — all landed. The bench shows the streaming ops memory-bound and already
NEON-vectorized; `sort_list` is the lone compute-bound op.

Then the kernel-matrix session: `Op<L>` reorganized as (intro/elim/map/capture) × (Prod/Sum/List)
with `CapSum` closing the matrix and `Broadcast` renamed `CapList`; `Gather` (index-as-value) and
`Head` (the stratum drop) added; the three iso pairs completed (`Zip`, `Unweave`/`Weave`);
`Partition` removed as redundant with `Branch(2)`; `Find`/`Rel` now judge by `join` (⊥-laned probes
unify with committed operands); the judge rejects what eval can't represent (`Cast` widths, sum
arities > 256). The law-program pattern (corpus 27–34) witnesses every embellishment's reduction to
kernel+isos, so the kernel's sufficiency is suite-checked.

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
  Use sites: `inject V` (both numbers off the declaration), `map_variant V`, named `match` arms, `branch Name` (arity by enum name).
  Companions landed with it: lambda parameters take `let`-style tuple patterns (`map ((lo, hi) -> …)`), and pair-eating binaries accept an immediate (`x sub 1` ≡ `(x, x lit 1) sub`; the core's `And`/`Shr`/`AddU64`/`Gt` immediate kernels are untouched).
  Field-name projection (`s.a`) and record literals stay OUT: parse-time resolution would need globally-unique field names (a misapplied name silently projects the wrong index) or typed resolution, and destructuring covers the corpus without either.
  Mechanical closure capture (free vars threaded via `CapList`/`CapSum`) remains the open companion pass.
  Programs/28 exercises the whole bundle and the sum-heavy programs (09, 11, 18, 19, 23–25) use the named style; programs/10 deliberately keeps the numeric `inject 1 3` so both spellings stay exercised.

- **Kind-checking numeric front-end** — where `i32` / `f32` live; type-checks kinds, inserts
  swizzles, lowers to `NumOp`. Today's surface is kind-blind (emits `add` / `gt` / `lt` / …).
- **Length / stratum checker** — the one judgment `shape_of` skips (Tuple/Add same length; map body
  one stratum deeper). A pass beside `check`.
- **Sum random-access cost — RESOLVED (via representation).** A `Value::Sum` now carries the
  within-variant offset per row (`Sum(tags, offset, variants)`), built once at construction by
  `Value::sum`/`sum_from_prim` and maintained by `gather`/`concat`. `compare_idx` reads it O(1), so
  `Rel`/`find` over sum-shaped data are linear (no per-call rank scan). The earlier plan was find-local
  block-starts *avoiding* a representation change; carrying the offset proved simpler and uniform. The
  offset is a DERIVED field (droppable at serialization: content-address `(tags, variants)`, rebuild on
  load). Future optimization: skip computing it for sums a pass proves are never `find`/`Rel` operands.

## Conventions

Run after any `src/` change: `cargo test && cargo clippy --all-targets`. Dependency-free and tight.
Adding an op: one arm in `eval`, one in `judge` (exhaustiveness keeps them in sync). Adding a layer:
an enum + `OpLike` + `From` impls; touch nothing below.
