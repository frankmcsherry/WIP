# corgi — notes & TODO

A minimal **columnar, single-input term-graph IR** with a layered op vocabulary, a structural
shape-checker, an `ml` front-end, and a small optimizer. Standalone lib crate (no deps).
`cargo test` green; `cargo clippy --all-targets` clean; `cargo run --example tour` walks the
language; `cargo bench --bench eval` measures throughput.

## Orientation (module map)

```
src/
  value.rs     Value (columnar data) + show. Leaf = Prim, a width-tagged Arc<Vec<uN>>
               (u8/u16/u32/u64) via the `prim!` macro. Sum tags are a Prim u8 column.
  engine.rs    row-movement primitives: gather, concat, fill + index generators
               (split_by_mask / filter_mask / owner_ids / expand_ranges / tag_offsets).
  cmp.rs       the order machinery: compare2 (structural order) + the linear discrimination
               sort (sort_blocks / run_layout / segment_labels). Consumers are the cmp ops only.
  graph.rs     OpLike, NodeKind{Input,Tuple,Op(O)}, Graph<O>, Builder<O>, eval_graph, shape_of,
               check. eval_graph CONSUMES its arg and MOVES values to last use (enables in-place).
  shape.rs     Shape (Prim(width) | Prod | Sum | List) + shape_of_value + Display.
  optimize.rs  cse / dce / peephole / optimize over Graph<NumOp>.
  ops/
    core.rs    Op<L>: structure only (Lit/Cast/Field/Broadcast/Partition/Branch/Transpose/Filter/Slices/
               Flatten/Enlist/Iota/Unwrap/Inject/MapList/MapSum). Body-generic over L; inherent
               eval/judge/children; NOT OpLike. (Iota: U64->List<U64> data gen; MapSum: variadic match,
               Vec<(tag,body)>, unlisted variants pass through, disjoint tags so arms commute.)
    cmp.rs     CmpOp: Rel(Pred) + Gt + SortList/DedupList/GroupKey/Find. Kind-blind comparisons.
    numeric.rs NumOp { Core(Op<NumOp>), Cmp(CmpOp), Arith(ArithOp) } : OpLike. ArithOp = the
               (op × kind × width) grid + AddU64/ReduceSum + Shr/And (SIMD ÷2^k / mod 2^k). enc_i64/dec_i64.
  frontend/
    mod.rs     the op-name resolve table (the whole vocabulary the surface reaches).
    ml.rs      the one surface: ML-flavoured (let / |> / fun / match / inject), lowering to Graph<NumOp>.
tests/  corpus (runs programs/*.col) · ml · typer · numeric · optimize   (no Builder-demo file —
        every surface example, algebraic law, and property test lives in the corpus.)
programs/  *.col — the self-generating example corpus (program + `# n =` seed + `# =` golden, or
           an equivalence via `(A, B) |> eq → [1]`). One source: tests/corpus.rs verifies, the tour displays.
examples/tour.rs   benches/eval.rs   dev/*-kickoff.md
```

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
- **All cardinality change lives inside `List`.** Filter/Group/Reduce are `List<X> -> …`; the SEQ
  level is always 1:1.
- **Leaves are immutable Arc, cloned by refcount; eval moves to last use.** The last reader holds the
  sole Arc, so `into_*` move the buffer and pointwise ops are able to mutate in place (`AddU64` does).

## Done (foundations in place)

The byte-width-leaf migration, the (op×kind×width) numeric grid, the linear discrimination sort,
Arc leaves + move-on-last-use + in-place `AddU64`, owned-arg `eval`, sum introduction (`Inject`),
the `CmpOp` bucket including the `Rel(Pred)` compare-to-mask family, and consolidation to the single
`ml` surface — all landed. The bench shows the streaming ops memory-bound and already
NEON-vectorized; `sort_list` is the lone compute-bound op.

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
- **Index-as-value / VIEW multiplicity.** Composite ops are "generate index, then gather"; lift the
  index to a `Value` so the optimizer composes gathers (`gather(gather(v,i),j) = gather(v,gather(i,j))`)
  and fuses. The lazy form is a multiplicity View (0 = filter, ≥1 = repeat, range = slice).
- **Serialization / durability.** Leaves are flat typed buffers → write columns out/down,
  content-address. Latent strength (vs roto's statelessness); unbuilt. Option-as-`Sum` vs
  Arrow-validity-bitmap is the representation reconcile point.
- **Recursion / μ-types** — arbitrary-depth JSON; needs a recursive-column construct, and a
  length-carrying `Unit` for `null` / `Option` None.
- **JSONL / Extern** — `split(delim)`, `parse_int` / `parse_json` as `Op::Extern` (opaque bucket).
- **Kind-checking numeric front-end** — where `i32` / `f32` live; type-checks kinds, inserts
  swizzles, lowers to `NumOp`. Today's surface is kind-blind (emits `add` / `gt` / `lt` / …).
- **Length / stratum checker** — the one judgment `shape_of` skips (Tuple/Add same length; map body
  one stratum deeper). A pass beside `check`.
- **Sum random-access cost** — `compare2`'s Sum arm rank-scans (O(i)), so `find` over a sum-shaped
  haystack is O(n·m). The fix is find-local (sorted haystack ⇒ contiguous tags ⇒ block-start
  offset), not a representation change. See the COST NOTE in `cmp.rs`.

## Conventions

Run after any `src/` change: `cargo test && cargo clippy --all-targets`. Dependency-free and tight.
Adding an op: one arm in `eval`, one in `judge` (exhaustiveness keeps them in sync). Adding a layer:
an enum + `OpLike` + `From` impls; touch nothing below.
