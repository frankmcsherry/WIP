# Layering — committed direction (plan of record)

**Status:** agreed, not yet implemented. This doc is the plan; the next
step is to execute it. Sequencing + open checks are at the bottom.

## The model

```
[concatenative]─┐
[SQL-ish]       ─┼─ lower ─→ SystemOp Graph ─ opt* ─→ Graph ─→ engine
[datalog]       ─┘            (THE IR)
```

- **`SystemOp` (the term graph) is THE IR.** It is the closed, mechanical,
  kernel-mapped vocabulary: no stack-isms (`dup`/`pick`/`roll`), no binding
  *construct* (`let`/`ref` are boiled to edges in lowering), no front-end
  grammar. This is already largely true — `build_is_routing_free_by_construction`
  and `let_block_resolves_to_data_edges` prove the vocabulary is clean.
- **The op-stream (`Vec<Box<dyn Op>>`) is NOT a peer IR.** It is *the
  concatenative front end's surface AST*. It is name-identified and carries
  stack-isms and binding; like any front-end AST its job is to be lowered
  and forgotten. (`Vec<Op>` is unoptimizable precisely because ops are
  name-identified with no engine semantics — which is *why* `SystemOp`
  exists and is justified. The mistake was never `SystemOp`; it was keeping
  the op-stream executor as a peer.)
- **Front ends are plural and share nothing but the target IR.** Each is a
  `…source… → SystemOp Graph` lowering. The concatenative one is
  `parse → Vec<Op> → lower::build → Graph`. Stack-isms / binding / grammar
  are erased at *its* `lower` boundary and never reach the IR.
- **Optimization is `Graph → Graph`**, repeatable and composable, and must
  be *on the execution path* (passes individually toggleable).
- **The engine executes the Graph directly and is self-contained** — it
  never calls back into a front-end executor.

## The standing invariant (the "smear test")

Three deletion questions; the layering is honest only when all answer "yes":

1. Can the array language run without the graph? **Yes** — `--reference`
   runs the pre-graph interpreter (now the differential oracle, not a peer
   production path).
2. Can the optimizer be removed without touching execution? **Yes** (as of
   steps 6–7) — `optimize` is pure `Graph → Graph`, `--no-opt` runs the
   engine without it identically, and `optimize_corpus_preserves_results`
   proves it. Execution depends on no pass running.
3. Can a front end be added without touching another layer's lowering?
   **Yes** — by reframing. Binding-boiling lives in `lower.rs`, but that
   *is* the concatenative front end's lowering: a new front end (SQL,
   datalog) has its own lowering and resolves its own binding. Crucially,
   binding-boiling is **not** a removable optimization — `:name` binds the
   rest of its block, so an un-boiled `Let` would swallow the whole program
   into one legacy-eval blob. Boiling is what produces a dataflow graph from
   the concatenative front end's scoped binding; it belongs in lowering.
   (To make binding a *removable, shareable* pass would need a
   representation redesign — globally-unique slots + fine-grained
   `Bind`/`Ref` terms runnable via a slot-env — worth it only if a second
   front end ever wants shared binding. Recorded, not built.)

**New engineering rule (add to PRINCIPLES "Engineering policies"):** an
optimizer pass must take and return the IR (`Graph → Graph`) and must never
be load-bearing for execution. The day all three deletion questions answer
"yes" the layering is closed — **all three now answer yes** (see below).

## The keystone: close the IR under the engine — ✅ CLOSED

The smear was: `Foreign(Box<dyn Op>)` held body-bearing ops
(`each`/`match`/`repeat`/`cleave`) and `execute::eval_graph` ran them by
calling back into the legacy `ir::op::eval` loop — so the engine wasn't
self-contained and the optimizer couldn't see inside bodies.

**Resolved.** All four are gone: `each` → segmented ops, `match`/`cleave` →
parser desugaring, `repeat` → removed entirely (collie is deliberately not
Turing-complete yet; a loop can return later as a dataflow construct). No
body-bearing op remains, so `eval_graph` never calls legacy eval — it takes
no `env` at all. The engine is fully self-contained over the IR.

Precedent that pointed the way: the *same* problem was solved for binding —
`let`/`ref` boiled into IR structure (edges) during lowering. Body-bearing
ops were the same problem one level up.

### Op-by-op triage (verified against the code)

| op | verdict | route |
|---|---|---|
| `each` | per-row dispatch — the principle-2 violator | **eliminate** (columnar primitives) |
| `cleave` (`.{ }`) | sugar | **desugar** in parser |
| `match` | sugar | **desugar** in parser (needs surface `merge.K`) |
| `repeat` | genuine loop | **first-class loop term** (the one real scope) |

**`each` — eliminate, don't wrap.** Down to one curated use (15:
`each { sort reverse 3u64 take }`). Needs three segmented primitives:
per-group `sort` (BACKLOG 0c), per-group `take K`, per-group reverse. Build
them and `each` leaves the IR by deletion. This is the only route that also
fixes the *performance* smear (a body-as-subgraph still runs per-row).

**`cleave` — sugar (verified).** `Cleave::run` (combinators.rs:385) runs
each path on an isolated sub-stack `vec![v.clone()]`, net `1→1`. Desugar in
the parser:

```
.{ p0 ; p1 ; p2 }   ≡   :tmp  tmp p0  tmp p1  tmp p2  entuple.3
```

(last `tmp` is the take; `tmp`/the paths boil to edges, so paths become
inline and *visible to the optimizer*). Faithful for every current program
by construction: all paths pass the isolated-`1→1` typecheck today, and
example 06's `dup .1 swap .0 +.u64` confirms paths may use `dup`/`swap`
internally yet stay net-1→1 without digging below their input. *Isolation
is a safety net* (malformed path underflows a private stack vs. grabbing an
outer value), not semantics — lost gracefully (different error, same result
for well-formed programs). PRINCIPLES already flags `cleave` as
"explain-or-remove"; desugaring resolves it by dissolving it.

**`match` — sugar (verified).** `Match::run` (combinators.rs:312) is
mechanically: pop `Sum{disc, lanes}`, run `arm_i` on `lane_i` (isolated
sub-stack), enforce **length-preservation** (`out.len()==lane_len`,
combinators.rs:338-340, with a scalar→broadcast convenience at :335-336),
then `merge_by_disc`. Desugar:

```
match { -> a0 -> a1 }   ≡   split :[disc l0 l1]  l0 a0  l1 a1  disc merge.2
```

Requires a **surface `merge.K`** (expose `helpers::merge_by_disc`; pops disc
+ K lane outputs → merged column). Faithful for every current program:
length-preservation becomes `merge.K`'s precondition; arm-shape-agreement
becomes the typecheck of `merge.K`; and **no example relies on the
scalar→broadcast convenience** (all arms — `2u64 *.u64`, `lftj_lane_g` — are
already length-preserving), so it can be dropped or written explicitly with
`like`. The arms become inline and optimizer-visible.

**`repeat` — the one genuine scope.** The only sugar is *unrolling* (splice
the body N times), which makes graph size depend on a literal and blows up
for large N (Mandelbrot-style fixed iteration). So `repeat` becomes a
first-class **loop term**: holds a sub-graph + iteration count; `eval_graph`
runs the sub-graph N times over the value stack. This is the engine
recursing on its *own* IR (a nested `Graph`), not a callback to a foreign
interpreter — the one new engine capability, and it's contained.

## Residual `Foreign` cleanup (after the keystone)

Once bodies are gone, `Foreign` holds only literals + diagnostics + FFI.

- Promote literals → a `Const` IR variant; diagnostics → `Time`/`Show`
  variants. Mechanical.
- `Foreign` then survives only as an *honest* escape hatch (true FFI /
  unmodeled ops), not a parking lot.
- `promote()`'s downcast bridge can then shrink/vanish: have the registry
  factories construct `SystemOp` variants directly (sysop.rs already
  anticipates this — "shrinks as the parser learns to construct variants
  directly"). Each front end lowers straight to IR; no `Box<dyn Op>`
  round-trip. `as_kernel`'s reconstruct-and-delegate is fine to keep as the
  engine's dispatch table (mechanical duplication, not a smear); streamline
  to a direct `match` later if desired.

## Legacy evaluator → test oracle

`eval_graph` depends on legacy eval for *exactly one reason*: running
body-bearing `Foreign` ops. Remove the bodies and that callback has nothing
to call. Then:

- `eval_graph` is self-contained over the IR.
- Keep the legacy `eval` loop as a **differential-test oracle** — the
  `pipeline::tests::agree()` harness against a dead-simple reference
  interpreter is genuinely valuable and costs nothing off the production
  path. Rename `--legacy` → `--reference` so its role is honest (main.rs:18).

## Implementation sequencing (each step compiles + full suite green)

1. **`each` elimination. ✅ DONE.** Added per-row `sort.segmented` /
   `reverse.segmented` / `take.segmented` (sort.rs, sort_concat.rs) + their
   `SystemOp` variants; rewrote example 15 (byte-identical); removed `Each`
   (struct, parser arm, `has_body`/`body_has_opaque`/inference downcasts).
2. **Surface `mergeN`. ✅ DONE.** `combinators::MergeN` (dotless, arity-
   suffixed like inject/partition; bare `merge` reserved for sorted union)
   + `SystemOp::Merge { n }`. `merge_reproduces_match` test validates it.
3. **Desugar `match`. ✅ DONE.** Parser synthesizes
   `split :[g_disc g_l…] g_disc g_l0 <arm0> … mergeK` and re-parses it;
   `Match` op removed. Examples 05/19 verified (19 triple count 2,999,997).
4. **Desugar `cleave`. ✅ DONE.** Parser synthesizes
   `:[g_v] g_v <p0> g_v <p1> … entuple.K` and re-parses it; `Cleave` op
   removed. Examples 06/07/14 verified.
   → **Milestone:** all sugar (`each`/`match`/`cleave`) retired; the only
   body-bearing ops left are `repeat` (loop) and `let` (binding). 122 tests
   + 19 examples green.
5. **`repeat` — REMOVED entirely. ✅ DONE.** (Superseded Option B.) Rather
   than keep `repeat` as a scoped sub-interpreter, it was deleted: collie is
   deliberately not Turing-complete yet, and `repeat` was the last thing
   forcing legacy eval + a binding island. Removing it makes binding boil
   100% uniformly and lets `eval_graph` drop its `env` — the engine is now
   fully self-contained, a *cleaner* close than either Option A or B. Cost:
   the Mandelbrot example (its only user) was removed. A loop can return
   later as a dataflow construct (Option A) or via quotations (#2) — the
   removal forecloses neither. `body_has_opaque`/`has_body` and the
   Let/Repeat dynamic-arity handlers are gone with it.
6. **Close + demote. ✅ DONE.** `--legacy` → `--reference` (the differential
   oracle, not a peer path); the graph engine is the only top-level
   execution path. Added `optimize()` (`elide_routing → cse →
   eliminate_dead`, `Graph → Graph`) and **wired it onto the default path**
   (`run_script` + `examples_runner`), toggleable with `--no-opt`. Locked by
   `optimize_corpus_preserves_results` (full pipeline == reference on every
   example). **Deferred:** promoting residual `Foreign` (literals/
   diagnostics) to variants — under B, `Foreign` permanently holds
   `let`/`ref`/`repeat`, so it's cosmetic, not invariant-completing, and
   adds variants against B's "less work" spirit. Revisit with const-folding.
7. **Inspectability. ✅ DONE (pre-existing).** The `graph <path>` subcommand
   is the `--emit-ir` view; `graph <path> --elide` now runs the full
   `optimize` pipeline so the dump matches what the engine executes.

## Next structural item: split front-end AST from back-end kernels

The one remaining structural duplication is the `SystemOp` mirror — and
specifically that the back-end IR still *reconstructs* front-end op structs to
execute (`as_kernel`). The designed fix keeps `SystemOp` as the back-end IR
and *sharpens* the front/back split: front-end ops become metadata-only (no
`run`/`tc`), `SystemOp` owns the kernels, `Foreign` stays the plug-in hatch.
**Design + migration plan: `dev/SYSOP_KERNELS.md`.** Medium, optional.

## Single evaluator — the op-stream interpreter is DELETED

`ir::op::eval` (the op-stream interpreter) is **gone**. The graph engine
(`eval_graph`) is the only evaluator. Steps taken:

- **REPL removed** — the one interactive consumer (it needed incremental/
  seeded eval the engine doesn't do).
- **`bench` + `demos` migrated** to the engine. The engine can only run
  *closed* programs (no pre-pushed stack), and bench is *open* (it pushes a
  prebuilt input), so a new `SystemOp::Const(value)` source term + a
  `lower::build_seeded` helper bake the seed into a closed graph. (`Const` is
  also the promoted-literal home noted earlier.) Bench now measures the real
  engine path (build+optimize once, `eval_graph` in the timed loop).
- **`--reference` + the `agree()` differential oracle removed.** Its
  within-engine replacement: the optimizer-correctness tests now assert
  *optimized graph == unoptimized graph* (the right invariant for a
  `Graph → Graph` pass), and `optimize_corpus_preserves_results` checks it on
  every example. Unit tests assert explicit expected values.
- **`Let`/`Ref` are boiled in lowering**, so `Let::run` (which used to call
  `eval`) now errors as unreachable — never hit, since binding never becomes
  a term.

What was traded: the cross-check against a *second, independently-structured*
evaluator. Mitigation: optimized-vs-unoptimized still exercises the
graph-specific machinery (lower/optimize/execute), and the example corpus +
explicit-value unit tests cover correctness. A future REPL or dataflow loop
would seed inputs the same way `build_seeded` does (or via an `Input` term) —
no second evaluator needed.
