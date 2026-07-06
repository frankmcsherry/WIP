# hobbes — a précis for the corgi/sprig/collie shelf

Morgan Stanley's [hobbes](https://github.com/morganstanley/hobbes) is a fourth point in
the design space these three projects explore: a scalar-logic engine that accepts typed
programs at runtime inside a compiled host, born of the same complaint (the host's
compile-deploy cycle is too slow to think with) and pointed at the same kind of work
(expression evaluation over structured data, live and stored). It is **not** a dataflow
or incremental engine — no times, no retractions, no maintenance — so it is read here
strictly as an analogue of the scalar layer, the way corgi/sprig/collie are candidates
for DDIR's. This file records its techniques so the best ones get considered on purpose,
even where we end up discarding them.

## Where it sits on the family's axes

| axis | corgi / sprig / collie | hobbes |
|---|---|---|
| data | flat SoA columns over integer leaves; Sum as tag column + lanes (corgi/collie), conventions over layered columns (sprig) | AoS, C-compatible layouts: records/variants/arrays/recursive types, host-native; zero-copy onto C++ structs and mmap'd files |
| types | shape inferred **from data** (corgi `shape_of_value`, collie's five variants); checked structurally at kernel boundaries | full static inference **from programs** (ML-family), structural records/variants via qualified-type constraints; checked before anything runs |
| execution | interpret a graph/program of vectorized kernels — amortize per **column** | LLVM JIT whole expressions to machine code — specialize per **expression**, run per-row loops |
| extension | new kernels in the engine | user-defined constraint solvers ("unqualifiers") — compiler plugins that discharge new constraint forms |
| scope | deliberately tiny IRs; syntax is a consumer concern | a whole language: comprehensions, pattern matching, LALR parser generation, typed RPC, REPL |

The load-bearing contrast: the family bets that **wide columns amortize
interpretation** (no JIT needed until per-row logic gets deep); hobbes bets that
**specialization beats vectorized interpretation** when expressions are complex and rows
arrive one at a time. Both are right in their regimes. Trading tick handlers are
row-at-a-time and logic-heavy; DDIR operators are column-at-a-time and logic-light.
That is why corgi wins its benches without a JIT — and why the JIT rung stays on the
ladder for the day the regime shifts (fold/case-heavy Terms, the eqsat corpus).

## Techniques worth considering

1. **Row/structural constraints that compile to offsets.** `(a.foo::b) => (a) -> b` is
   duck typing discharged at accept time into a fixed field-offset load — flexibility in
   the type system, zero dispatch at runtime. This is the declarative form of what
   `compile(term, shape)` does by construction: corgi resolves `Proj` against a `Shape`
   to `Field(i)`. The hobbes lesson is to keep that resolution **a checked constraint
   system rather than an ad-hoc pass**: the same machinery then rejects ill-shaped
   programs at intake (the DDIR server's missing type check) and documents exactly what
   a Term requires of its input shape. *Adapt: this is the shape-checker DDIR install
   wants; corgi's typer is already most of the way there.*

2. **Expression rewriting, not dictionary passing.** Hobbes resolves type-class
   constraints by rewriting the expression into specialized code — monomorphization
   on demand at JIT time, no runtime indirection, explicitly chosen over Haskell's
   dictionaries. As a discipline: **specialization is a rewrite of the term graph**, so
   it composes with every other rewrite (the optimizer, eqsat) in one representation,
   instead of living in a separate "codegen" phase. *Adopt as principle: corgi's
   shape-directed compile already behaves this way; keep it so as the op vocabulary
   grows.*

3. **Type descriptions persisted with the data.** Hobbes storage (fregion files, hog
   logs) embeds structural type descriptions in the data; readers match structurally,
   so mmap is zero-copy and schema evolution is structural compatibility rather than
   version negotiation. For DDIR: a trace/tap should carry its Shape — the server
   registry, content-addressed sources, and any record/replay tap want self-describing
   streams, and structural matching is the right compatibility rule for a structurally
   typed IR. *Adopt: cheap now, expensive to retrofit.*

4. **The numeric tower as constraints.** Mixed-width arithmetic
   (`0X01+2+3.0+4L+5S`) types by inference: widths and kinds are constraints solved
   once, not tags checked per op. Corgi's move — signed/float as KINDS encoded onto
   width-tagged `Prim`s, core stays kind-blind — is the columnar mirror. The hobbes
   version says where the kind knowledge should live: in the checker's solved types,
   so kernels stay monomorphic and the encoding never leaks into program text.
   *Consider when the numeric layer grows floats.*

5. **Match-table compilation.** Large pattern-match tables compile to decision
   trees/automata (including string-match tables to state machines), not per-row
   interpreted dispatch. The columnar analogue is corgi's Sum lanes — partition by tag
   column, run per-arm kernels — which handles wide sums; the hobbes technique matters
   when arms are many and *nested* (deep `Case` over AST-shaped data): compile the
   dispatch structure once per shape. *Keep on the shelf for the eqsat/datalog corpus.*

6. **Region allocation per evaluation.** Thread-local bump regions, deallocated en
   masse (`resetMemoryPool`) after each evaluation — allocation is a pointer bump,
   reclamation is free, nothing outlives its work unit. The family's analogues are
   ad hoc (per-retire pools in the reduce backend, reused scratch vecs). Worth stating
   as a family principle: **kernel scratch is region-scoped to the work unit**, which is
   also exactly the discipline the int_proxy `retire`/work-unit structure already
   implies. *Adopt as stated policy.*

7. **Typed logs with live tails, one macro from application code.**
   `DEFINE_STORAGE_GROUP`/`HSTORE` turn a C++ struct into a persistent, self-describing,
   real-time-queryable log (ring buffer → hog consumer, local or forwarded), and "live
   tails" fire callbacks on append. This is the tap/record/replay design plus the
   `subscribe` verb, with production-tested ergonomics: the instrumentation cost is one
   line at the data source. *Steal the ergonomics for the DDIR server's tap; the epochs
   and multi-source consistency are ours to add — hobbes logs are per-stream sequences
   with no common time domain.*

8. **Protocol negotiation by quoted expressions.** Hobbes RPC connects by exchanging
   types/quoted expressions and JIT-compiling the (de)serializers per session — the wire
   format is negotiated against structural types at connect time, not fixed at build
   time. The DDIR server's install/peek/subscribe wants the same shape: schemas checked
   at the boundary, codecs specialized per connection. *Consider when the server grows a
   wire protocol; pairs with (3).*

9. **The JIT rung itself.** Hobbes is a production proof that an embedded LLVM JIT with
   full inference is operable in low-latency environments — the top rung of the ladder
   `Term → tree interpreter → corgi graph → machine code`. The family's bet is that the
   third rung suffices; hobbes de-risks the fourth. *Defer, but keep the ladder's shape:
   nothing in corgi's design blocks compiling `Graph<NumOp>` kernels when a bench says
   the interpret-per-column overhead matters.*

## What to discard, and why

- **AoS, host-layout-native data.** Hobbes's zero-copy is onto C++ structs; the
  family's whole point is SoA columns that vectorize and gather. Its layout story
  answers a question we aren't asking (binding to someone else's structs); if that
  question arrives (MZ rows), CHUNK/payload genericity is our answer, not AoS.
- **The whole-language surface.** Comprehensions, LALR generation, a REPL, plugins —
  the language was hobbes's product. The family's minimalism (IR is the product,
  syntax is the consumer's) is deliberate and stays.
- **Efficiency over sandboxing.** Hobbes runs trusted code in-process by design. The
  DDIR server's posture is the opposite (validate at intake, workers must not panic),
  and multi-tenant serving needs it.
- **Nothing on time.** No incrementality, retractions, partial orders, or cross-stream
  consistency — scoped out by the framing here, but worth restating: every hobbes idea
  above is about the *value* half of the world; the *time* half is DD's, and the
  int_proxy seam is precisely the line between them.

## One-line summary

Hobbes is the statically-inferred, JIT'd, AoS point in the design space this shelf
explores columnar-and-interpreted; its durable exports are the constraint-discharge
compilation discipline (2), types-as-persisted-data (3), region-scoped scratch (6), and
an existence proof that accept-time inference (1) and an embedded JIT (9) hold up in
production — with everything about layout and language surface staying theirs.
