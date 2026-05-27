# Splitting front-end AST from back-end kernels

**Status:** core done (steps 0–3). `as_kernel` is **deleted**; every modeled
`SystemOp` variant dispatches `run`/`tc`/`name`/`arity` directly to a
per-op free-function kernel in `crate::ops::*` (e.g. `cmp::run`,
`list::group_run`, `combinators::zip_run`). The back-end IR is now
self-contained — no `pipeline → ops::structs`-for-execution dependency.
`Foreign` remains the sole kernel-carrying back-end node. 117 tests + 18
examples green throughout. Supersedes the loose "remove the SystemOp mirror"
phrasing — this *kept* `SystemOp` as the back-end IR and *sharpened* the
front-end/back-end split. Linked from `dev/LAYERING.md`.

**Deferred (steps 1, 4, 5):** the front-end op structs still carry thin
`run`/`tc` shims that delegate to the free fns (e.g. `Arith::run` →
`arith::run(self.op, self.interp, st)`). They were kept so the per-op unit
tests (which call `op.run`/`op.tc` directly) stay green without rewriting.
Making the front-end structs fully *inert* (default-erroring `run`, no `tc`,
and rewiring `lower`'s type threading off `op.tc`) is the remaining polish;
it buys enforcement-by-types but no capability, and is independent of this
refactor's payoff (which is already banked).

## The invariant we are protecting

`SystemOp` exists so the **system has a different IR than the front end**.
That property is sacred. The two representations:

1. **Front-end IR** — per-front-end. The concatenative one is the op-stream
   (`Box<dyn Op>`): name-identified AST with stack-isms (`dup`/`pick`/`roll`)
   and binding (`Let`/`Ref`). A future SQL/datalog front end has its own.
2. **Back-end IR** — `SystemOp` graph: closed, mechanical, no stack-isms, no
   binding constructs. Shared by all front ends.

`lower` (with `promote`) is the **one-way seam** between them. Nothing here
changes that. This refactor makes the split *truer*, not weaker.

## Current state — and the leak

Each modeled op is written in three places:

- an **op struct** in `ops/` (e.g. `arith::Arith { op, interp }`) carrying the
  **kernel** (`run`/`tc`, often wrapping a free fn like `do_arith` plus
  fast paths),
- a **`SystemOp` variant** (`SystemOp::Arith { op, interp }`),
- bridged by `promote()` (front struct → variant, downcast) and `as_kernel()`
  (variant → **a freshly reconstructed front struct**, to borrow its kernel).

The leak: `SystemOp::run`/`tc`/`name`/`arity` all go through
`as_kernel().unwrap()`, which does `Box::new(arith::Arith{…})` and calls *its*
`run`. So **the back-end IR executes by rebuilding front-end structs** — a
dependency pointing the wrong way (`pipeline → ops::structs`, only so the IR
can run). The IR is not self-contained.

(After the op-stream interpreter was deleted, the struct `run`/`tc` methods are
reached *only* via this reconstruction + `lower`'s `op.tc` shape-threading.
Nothing else runs a front-end struct.)

## Target state

| | Front-end op (struct) | Back-end op (`SystemOp`) |
|---|---|---|
| identity + params | ✅ (`Arith{op,interp}`) | ✅ (`Arith{op,interp}`) |
| metadata: `name`/`arity`/`routing_map` | ✅ | (derived) |
| **kernel: `run`/`tc`** | ❌ (none) | ✅ **owns it** |

- **Front-end ops keep only metadata** (`name`, `arity`, `routing_map`) — the
  data `lower` needs to translate and thread them. They **do not implement
  `run`/`tc`**. Mechanically: `PrimOp::run` gains a default impl that errors
  (`"modeled by SystemOp; not directly runnable"`); `Typed::tc` already
  defaults to `Ok(())`. System op structs simply *omit* `run`/`tc` and inherit
  the defaults. (They were dead anyway — only `as_kernel` reconstruction
  called them.)
- **`SystemOp` owns the kernels.** `SystemOp::run`/`tc` become a `match` over
  the variant that calls a **per-op free function** holding the relocated
  kernel body — e.g. `SystemOp::Arith{op,interp} => arith::run(op, interp,
  st)`. Many kernels are already free fns (`do_arith`, `do_cmp`); the rest
  (the `run` bodies, fast paths included) move to a sibling free fn. **No
  reconstruction. `as_kernel` is deleted.** The `pipeline → ops::structs`
  dependency-for-execution is severed.
- **`promote` stays — it is the seam.** Still a central downcast `match` in
  `lower`/`sysop`: front struct → `SystemOp` variant (reading the struct's
  params), or → `Foreign` for unmodeled ops. Keeping it central means the
  whole front→back mapping is readable in one place.
- **`Foreign` stays as the extensibility escape hatch** — the one back-end node
  that carries a kernel (`Box<dyn …>` with `run`/`tc`), for plug-in ops the
  system doesn't model (`ops_extra`'s `square`/`clamp`). So "back-end owns the
  kernels" has exactly one principled exception: externally-registered ops
  bring their own, and `Foreign` delegates to them (unchanged from today).

So: front-end = translatable structure; back-end = the doing; `Foreign` =
"unknown op, carries its own doing."

## What `lower` does with front-end ops that have no `run`/`tc`

`lower` interrogates front-end structure and threads types **without** calling
front-end kernels:

- **Routing** (`dup`/`pick`/…): already resolved via `routing_map` into edges;
  thread the type-stack by *applying the map* (pop `n_in` shapes, push the
  aliased ones) instead of calling `op.tc`. Pure aliasing — no kernel needed.
- **Binding** (`Let`/`Ref`): `boil_let` already threads shapes by hand
  (`split_off`/`extend` on the type-stack); `Ref` pushes `tenv[idx]` directly.
  Neither needs `op.tc`.
- **Compute ops**: `promote` → `SystemOp`, then call **`SystemOp::tc`** to
  thread output shapes and (for `Split`) derive dynamic arity. So the *only*
  `tc` that runs is the back-end's.

Result: no front-end op's `run`/`tc` is ever called. They genuinely carry no
semantics — exactly the target.

## What does *not* change (so the property is safe)

- Two distinct IRs. The op-stream keeps `Let`/`Ref`/`dup`/`pick` that never
  become `SystemOp`s. Front ends remain pluralizable, each with its own IR.
- `promote`/`lower` as the one-way translation boundary.
- `Foreign` extensibility.
- The params-in-both-places "duplication" (`Arith{op,interp}` in struct *and*
  variant) **stays** — that is the *inherent* cost of having two IRs, and it's
  the cost you chose by introducing `SystemOp`. We are not removing it. We are
  removing the *reconstruction* (`as_kernel`) and the dead front-end kernels.

## Migration plan (each step compiles + full suite green)

0. **Warm-up (independent, small). ✅ DONE.** `LitNum`/`LitArr` grew a
   `to_value()`; `promote` maps them to `SystemOp::Const(value)` instead of
   `Foreign`. Literals now lower to `const` terms (verified via `graph`
   dump), are CSE-able, and `Foreign` no longer carries them. 117 tests + 18
   examples green. Validated the "value/kernel as a `SystemOp` arm" shape.
1. **Give `PrimOp::run` a default erroring impl.** No behavior change yet
   (every op still implements `run`).
2. **Per ops/ module, extract the `run`/`tc` bodies into free fns** keyed by
   the op's params (e.g. `arith::run(op, interp, st)` / `arith::tc(...)`).
   Have `SystemOp::run`/`tc` call them directly via a `match`; drop those
   variants out of `as_kernel`. Do this family-by-family (arith, cmp, list,
   sort, …) — each family is self-contained and independently testable.
   **✅ DONE** — all families migrated: arith, cmp, convert, reduce_ops,
   list (incl. ReduceAdd/Cumsum/Shift), sort, sort_concat, swizzle, view,
   join, combinators. Each front-end struct keeps a one-line shim
   (`fn run(&self,…){ <op>_run(…) }`) delegating to its free fn.
3. **Once every modeled variant is in the `match`, delete `as_kernel`** and
   the `pipeline → ops::structs`-for-execution dependency. `name`/`arity`
   dispatch move to the same `match` or stay metadata-derived.
   **✅ DONE** — `as_kernel` deleted; `name`/`run`/`tc`/`arity` are
   exhaustive matches with `Const`/`Foreign` handled explicitly, no
   reconstruction fall-through.
4. **Strip `run`/`tc` from the system op structs** (they now inherit the
   default-error `run` and default `tc`); they keep `name`/`arity`/
   `routing_map`. External ops (`ops_extra`) keep `run`/`tc` for `Foreign`.
5. **Rewire `lower`'s type threading** for routing/binding to use
   `routing_map`/structural handling instead of `op.tc` (see above), so no
   front-end `tc` is called.

Reversible at each step; the differential safety net is gone, so lean on the
`optimize_corpus_preserves_results` + explicit-value unit tests after each
family moves.

## Cost / honesty

- **Medium refactor**, not cheap (I previously overstated this). ~58 ops'
  `run`/`tc` relocate. Mechanical but voluminous, and step 2 touches every
  `ops/` file.
- **Optional.** The system is correct as-is. The payoff is structural: the
  back-end IR becomes self-contained (no reconstruction, no
  back-end→front-end-struct dependency), the front-end becomes inert
  structure, and the front/back split is enforced by the type system rather
  than by convention. Worth doing *now that the optimizer the IR serves is
  live* — but it buys cleanliness, not capability.
- **Watch:** the trait split (metadata-only front-end vs kernel-bearing
  `Foreign`) is the one place to get the Rust ergonomics right; the
  default-erroring-`run` approach (step 1) avoids trait-object upcasting
  gymnastics by keeping one `Op` trait where system ops just don't override
  `run`/`tc`.
