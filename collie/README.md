# collie

An interpreted columnar engine over a small flat-data universe with
**sum types** and **lists** as first-class values — not just
rectangular arrays. The aim is to express SQL-flavored joins,
factorized queries, and tagged-union data through ops that compose
the way array-language primitives do, while leaving room for efficient
kernels underneath.

What you're looking at today is closer to a **human-writable columnar
IR** than a finished array language: the inner-loop kernels are
tight, but the surface for relational shapes is still mechanical
(see the WCO triangle in `examples/19_wco_lftj_def.col` for the
honest "this could be terser" example). A surface lift is on the
roadmap; see `dev/SURFACE.md`. Treat the array-language framing as
where we're headed, not where we are.

## A taste

A column of mixed geometric shapes — each row is either a `Circle r`
or a `Rectangle w h`. We have six shapes split across two regions.
Compute the total area per region.

```col
u8[0 1 1 0 0 1]                            >disc      # 0 = Circle, 1 = Rectangle
f64[2.0 5.0 1.5]                           >circles   # 3 radii
f64[3.0 4.0 2.0]  f64[5.0 6.0 8.0]  zip2   >rects     # 3 (w, h) pairs
u64[3 6]                                   >bounds    # region 0: rows 0..3, region 1: 3..6

disc circles rects inject2                            # Sum<f64, Prod[f64, f64]>
match {
  -> dup *.f64  3.14159f64 *.f64                      # Circle:    π r²
  -> dup .0 swap .1 *.f64                             # Rectangle: w × h
}
bounds nest                                           # → List<f64>, one row per region
reduce.+.f64                                          # per-region total area
# → f64[51.566, 101.608]
```

A few things to notice:

- **`inject2`** builds a `Sum` column from a discriminant plus
  per-lane data. The circle lane is a flat `f64` column; the
  rectangle lane is a `Prod[f64, f64]` column. The same row index
  picks from different lanes depending on the discriminant.
- **`match`** runs each arm against its lane and reassembles the
  result in original row order. Each arm dispatches *once*, against
  a whole column — Circle's body sees `f64`; Rectangle's sees
  `Prod[f64, f64]`. No per-row interpreter overhead.
- **`nest`** carves the flat result into a `List<f64>` using
  `bounds` as run-end offsets. **`reduce.+.f64`** then sums per row.
- Every op is one Rust kernel walking columns. Sum, Prod, List
  compose naturally — they're all just shapes in the universe.

## Design

collie is a **concatenative, functional, interpreted columnar engine**.
Six principles, each picking one side of a real design trade-off:

1. **Flat data, positionally addressable** — `Value` is one of
   `Prim | Prod | Sum | List | View`. No pointer-graph types;
   positions into a column are first-class.
2. **Whole-collection-only dispatch** — the interpreter dispatches
   per column, not per row. Inner loops are autovectorized Rust.
3. **Type information is layered** — storage carries width;
   operators carry interpretation; richer types live above.
4. **All collections are sequences with optional restrictions** —
   every op signature is `SEQ<T0> → SEQ<T1>`; properties like
   sortedness or uniqueness layer on, they aren't separate kinds.
5. **Operators are shape-polymorphic in their contents** —
   structural ops (`.0`, `gather`, `intersect`, `match`, …) see
   shape, not contents. Constraints land on the next op.
6. **Values are immutable; ops are pure (modulo diagnostics)** —
   no mutation, no aliasing, equational reasoning. Same family as
   Joy / Cat / Factor.

See [`PRINCIPLES.md`](PRINCIPLES.md) for the full statement and the
trade each makes.

## Run

```
cargo run --release                                          # default demo
cargo run --release -- examples                              # all 19 examples
cargo run --release -- examples/17_wco_list_intersect.col    # one example
cargo run --release -- foo.col                               # any .col file
cargo run --release -- repl                                  # interactive REPL
cargo run --release -- bench                                 # microbenchmarks
cargo test  --release                                        # 83 unit tests
```

## What's where

```
src/
  ir/         language definition (value, stack, op, shape, typecheck)
  ops/        operators — one file per family
  syntax/     parser + registry
  tools/      binary-only (REPL, bench, pretty, serialize, demos)
examples/     19 .col files — tour from basics through WCO triangle
dev/          workshop notes (BACKLOG, FOLLOWUPS, SURFACE, ONBOARDING)
```

Layers compose upward only: `ir/` knows nothing of operators; `ops/`
knows nothing of syntax. Adding an operator is one file in `ops/`.

## Library use

```rust
use collie::ir::op::eval;
use collie::ir::value::Value;
use collie::syntax::{parse::parse, registry::OpRegistry};

let reg = OpRegistry::standard();
let prog = parse("u64[1 2 3] reduce.+.u64", &reg)?;
let mut stack: Vec<Value> = Vec::new();
let mut env: Vec<Value> = Vec::new();
eval(&prog, &mut stack, &mut env)?;
// stack now holds the result.
```

Custom ops: `impl PrimOp + Typed`, register with `reg.add(factory)` —
see `src/tools/ops_extra.rs` for an out-of-tree example.

## Where to go next

- **[`PRINCIPLES.md`](PRINCIPLES.md)** — the three principles + the
  trades they make, plus idioms and engineering policies.
- **[`OPERATORS.md`](OPERATORS.md)** — full operator catalogue,
  grouped by role (per-element compute, structural reshape, surveys,
  aggregations, sort family, escape hatches, …) with stack effects
  and one-line semantics.
- **`examples/`** — `01_reduce_sum` through `19_wco_lftj_def`. Read
  them in order; each one introduces one or two new ideas.
- **[`BAKEOFF.md`](BAKEOFF.md)** — head-to-head with K / BQN / Uiua
  on representative tasks. Where collie wins, where it pays for the
  sum-type machinery.
- **`dev/`** — workshop notes for contributors: `BACKLOG.md` is
  pending work; `FOLLOWUPS.md` is long-form architectural rationale;
  `SURFACE.md` sketches one possible front-end language above the IR
  (others — datalog, relational, etc. — are envisioned but not
  written); `ONBOARDING.md` is an older reading-order tour.

## Performance — the goal, not a claim

The design aim is **room for efficient kernels**, the way array
languages have. Inner loops are width-monomorphic Rust the compiler
autovectorizes; per-op interpreter cost amortizes across N rows.

Where this currently lands varies by workload. Some kernels (GROUP BY
on narrow keys, mask-aware filter chains) are close to or faster than
hand-written Rust; others (filter-with-values vs BQN's specialized
inner loop; per-row sum vs BQN) still lose by single-digit factors.
The honest cross-language numbers — with both wins and losses — are
in `BAKEOFF.md`, alongside the WCO triangle workload where collie
holds up against a compiled-Rust equivalent (datatoad).

The framing we'd prefer: collie is interested in not *foreclosing*
performance, not in winning benchmarks. If a kernel is slow, the
representation usually lets us fix it without rewriting consumers.

The interpreter runs at the SIMD ceiling for arith/cmp; allocation in
`where` and `gather` is the remaining overhead.
