# Reading collie — an onboarding tour

## What this is

A stack-based concatenative interpreter for column-oriented data, written in
~3K lines of Rust. The value universe is closed: `Prim` (width-tagged
primitive columns), `Prod` (heterogeneous tuples), `Sum` (tagged variants),
`List` (variable-length nesting). Operations work column-at-a-time, which
amortizes the cost of dynamic dispatch over many rows. Interpretation
(i32 vs u32 vs f32) lives in operators, not in values.

The pitch is "array-language++ for typed nested data" — fixes the
columnar/factorized representation question more carefully than K/BQN/Uiua,
adds Sum types (the genuinely novel piece), pays a small per-batch
interpretation cost for runtime flexibility.

## Four layers, each optional

```
Layer 4: Concrete syntax            parse.rs, registry.rs, repl.rs, main.rs
                                    (one possible surface; could be replaced)
            ↓
Layer 3: Types                       shape.rs, typecheck.rs
                                    (optional — could "trust the program")
            ↓
Layer 2: Operations                  ops/*.rs, ops_extra.rs
                                    (plug-in; each op is a struct)
            ↓
Layer 1: Repr-typed core             value.rs, stack.rs, op.rs
                                    (knows no operators, types, or syntax)
```

Each upward layer is independent. The core can be exercised programmatically
by anyone who imports value.rs + stack.rs + op.rs and writes their own ops.

## Suggested reading order (about an hour)

1. `src/value.rs`            — the value universe (the star). 5 minutes.
2. `src/shape.rs`            — its type-level mirror. 3 minutes.
3. `src/op.rs` + `src/typecheck.rs` — trait machinery. 5 minutes.
4. `src/ops/combinators.rs`  — `Match`, `Branch`, `Nest`, `Cleave`, `InjectN`,
                               `PartitionN`. Read one or two impls in full.
                               15 minutes.
5. `src/ops/list.rs`         — `Group`, `Each`, `Reduce`, `Count`. Where the
                               column-at-a-time idioms live. 10 minutes.
6. `src/parse.rs`            — turns source into ops via the registry.
                               5 minutes.
7. `examples/*.col`          — read in order; each adds one idiom.
                               30 minutes.
8. `FOLLOWUPS.md`            — where the design is going.

## The value model — `value.rs`

The file is laid out top-down: `Value` first, then submodules for `prim`,
`bounds`, `storage`. The submodules are intra-file `mod {}` blocks (so they
get their own privacy boundary) and their items are re-exported flat —
external code says `value::Prim`, not `value::prim::Prim`.

```rust
pub enum Value {
    Prim(Prim),                                 // the leaf
    Prod(Arc<Vec<Value>>),                       // tuple
    Sum  { disc: Prim, lanes: Arc<Vec<Value>> }, // tagged variants
    List { bounds: BoundsRepr, values: Arc<Value> }, // nested rows
}
```

All structural payloads are `Arc`-wrapped, so `Value::clone()` is O(1)
regardless of width or depth. Only Arc-counter bumps; no data copies.

`Prim` is the leaf — `Arc<Vec<u8/u16/u32/u64>>`, tagged by `PrimWidth`. The
key point: width is in the type; *interpretation* (signed vs unsigned vs
float) is in operators, via `Storage` (the bytemuck bridge that lets a Pod
numeric `T` be extracted from / wrapped into the right Prim variant).

`BoundsRepr` is `Var(Prim)` (general case — a P64 of run-end offsets) or
`Stride { stride, count }` (fast path for rectangular lists, 16 bytes
regardless of row count). Most ops call `bounds_as_u64` (returns
`Cow<[u64]>`) which is borrowed for Var, materialized for Stride.

Construction helpers: `prod()`, `sum(disc, lanes)`, `list(bounds, values)`,
`from_vec::<T>()`, `prim_p8()`, `prim_p64()`, `bounds_var()`,
`bounds_stride()`. Used everywhere; prefer them over enum-literal
construction.

## The op model — `op.rs`, `typecheck.rs`, `ops/*.rs`

Two traits, deliberately split:

```rust
// op.rs (Layer 1)
pub trait PrimOp: std::fmt::Debug {
    fn name(&self) -> &str;
    fn run(&self, st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String>;
}

// typecheck.rs (Layer 3)
pub trait Typed {
    fn tc(&self, st: &mut TypeStack, env: &mut TypeEnv) -> Result<(), String>;
}

pub trait Op: PrimOp + Typed {}  // combined; Layer 3 talks in Op, Layer 1 in PrimOp
```

An op is a struct. `Add { interp: Interp }`, `Search { interp: Interp }`,
`Match { arms: Vec<Vec<Box<dyn Op>>> }`, etc. The struct holds any
parameters (interp, K, body). `run` and `tc` are the only behavior.

**Registration**: each `ops/*.rs` file exposes a `pub fn register(reg: &mut
OpRegistry)`. The registry is a list of "factory closures" that map a
parsed token string to an `Option<Box<dyn Op>>`. The parser walks tokens
and asks the registry; first factory to return `Some` wins.

This is how `+.i32`, `reduce.max.u64`, `branch.3.clamp`, `zip5`, `.0` etc.
all work — they're not enumerated, they're parsed from suffix/prefix
patterns by factories.

To add a new op: write the struct + `impl PrimOp + Typed`, add a factory in
`register()`, done. `src/ops_extra.rs` shows the pattern from outside the
standard set.

## The eval model — `stack.rs`, `op.rs`

```rust
pub type Stack = Vec<Value>;
pub fn eval(prog: &[Box<O>], st: &mut Stack, env: &mut Vec<Value>) -> Result<(), String> {
    for op in prog { op.run(st, env)?; }
    Ok(())
}
```

The eval loop is one line. Each op pops its inputs, pushes its outputs.

`env` is the let-binding environment — `{| name |}` pops from stack and
pushes to env; binding references (`name` in source) look up the
corresponding env index. Lexically scoped via a `scopes: Vec<Vec<String>>`
in the parser. let-bindings cross combinator boundaries (you can reference
an outer-let name from inside a `match` arm or `each` body).

Body-bearing combinators (`each`, `reduce`, `match`, `cleave`, `branch`
arms) run their bodies in a sub-stack — the body sees only its row's input,
and the body's single output is incorporated back into the outer flow. This
is what lets `each { reduce.+.u64 }` work without per-row stack juggling.

## The surface vocabulary, by category

**Stack manipulation** (in `ops/stack.rs`):
- `dup drop swap over rot pick<N> roll<N> id`

**Primitives** (in `ops/arith.rs`, `cmp.rs`, `convert.rs`):
- `+.i32 -.f64 *.u64 /.f32` etc. (arith)
- `<.u64 <=.u64 ==.u64 !=.u64 >.u64 >=.u64` (cmp)
- `as.i32` (cast), `show.<interp>` (debug print with interpretation)
- Literals: `u64[1 2 3]`, `f32[1.0 2.5]`, `bool[t f t]`, `5u64`, `3.14f32`

**Product construction/destruction** (in `ops/combinators.rs`):
- `zipK` / `entuple.K` — pop K, bundle into Prod (`zip` and `entuple` alias)
- `detuple.K` — pop Prod, push K fields
- `.i` — project field i of a Prod

**Sum construction/destruction** (in `ops/combinators.rs`):
- `branch` / `branch.K` / `branch.K.<mode>` — `(col, disc) → Sum`. K
  defaults to 2; modes are `strict` (default), `clamp`, `filter`.
- `partitionK` + `injectK` — the brutalist construction (branch's IR form).
- `split` — `Sum → disc lane0 lane1 ...` (manual destruction).
- `match { -> arm0 -> arm1 -> ... }` — eliminate Sum with per-arm bodies,
  merged by disc back to a flat column.

**List construction/destruction** (in `ops/list.rs`, `combinators.rs`):
- `nest` — `(values, bounds) → List` (bounds validated).
- `nest.stride` — `(values, count) → List` with Stride bounds.
- `flatten` — `List → (inner_value, bounds)`.
- `count` — per-row inner length, as a Prim.
- `length` — total length (number of outer rows) as scalar.
- `head` — first element of each row.
- `singleton` — wrap each element as a length-1 row.
- `iota` — `Prim → [0, 1, …, n-1]`.
- `like` — broadcast a scalar to a column.

**Per-row work** (in `ops/list.rs`, `combinators.rs`):
- `each { body }` — body runs once per row. Works on `List<X>` or
  `Prod[List<A>, List<B>, …]`. **As of recently**, the Prod variant only
  requires matching outer counts; per-row inner lengths can differ.
- `reduce.+.<i>`, `reduce.max.<i>`, `reduce.min.<i>`, `reduce.any`,
  `reduce.all` — specialized reducers.
- `.{ p0 ; p1 ; … }` — cleave: each path runs against a fresh copy of TOS;
  results gathered into a Prod.

**Joins / lookups** (in `ops/join.rs`):
- `gather` — `(col, P64-idxs) → col`. Works on Prim/Prod/List (recursive).
- `intersect.<interp>` — sort-merge intersect; returns paired index arrays.
- `search.<interp>` — asymmetric lookup; returns lower_bound positions.
- `xprod` — per-row Cartesian on `Prod[List<X>, List<Y>]`.

**Aggregation**:
- `group.<interp>` — sort+collect-by-key. `(vals, keys) → (uniq_keys,
  list-of-vals-per-key)`. The data-driven `List` constructor.
- `where` — filter by P8 mask.
- `spread` — repeat each col element by per-element count.
- `bounds>keys` / `list>bounds` — bridge between bounds and row-id columns.

## Map of `src/`

```
value.rs          Layer 1: Value, Prim, BoundsRepr, Storage. Top-down with
                  intra-file mods.
stack.rs          Layer 1: trivial Vec<Value> type alias and `pop`.
op.rs             Layer 1: PrimOp trait + eval loop.

shape.rs          Layer 3: Shape mirror of Value; Interp tag for operators;
                  bounds_as_u64 helper.
typecheck.rs      Layer 3: Typed trait + Op combined trait + typecheck().

ops/
  arith.rs        +.<i> -.<i> *.<i> /.<i>
  cmp.rs          <.<i> <=.<i> ==.<i> !=.<i> >.<i> >=.<i>
  convert.rs      as.<i> show.<i> + numeric/array literals
  combinators.rs  ZipN/DetupleN/Proj, InjectN/Split/PartitionN, Match,
                  Branch, Cleave, Nest/NestStride/Flatten
  helpers.rs      gather, slice_value, concat_values, broadcast,
                  sort_perm_by_key, sort_merge_intersect, gallop_to,
                  merge_by_disc. Internal — no PrimOp impls.
  join.rs         Intersect, Search, Gather, XProd
  list.rs         Group, Reduce, ReduceAdd, Each, Bounds, BoundsToKeys,
                  Count, Length, Singleton, Like, Head, Iota, Spread, Where
  reduce_ops.rs   ReduceMax/Min/Any/All
  sort_concat.rs  Concat, Take, Drop
  stack.rs        Dup, Drop_, Swap, Over, Rot, Id, Pick, Roll
  letbind.rs      Let, Ref ({| ... | ... } and binding references)

registry.rs       OpRegistry — factory closures; suffix parsers.
parse.rs          Tokenizer + parse_block. Handles {|, .{, match arms,
                  array literals, # comments.
serialize.rs      encode/decode (self-describing binary).

bench.rs          Benchmarks against hand-rolled Rust.
demos.rs          Rust-glue demos (serialization round-trip, external ops).
examples_runner.rs  Walks examples/*.col, runs each.
pretty.rs         Compact REPL-friendly value display.
repl.rs           Interactive REPL with :load + multi-line + rollback.
main.rs           Dispatch: bench / repl / examples / <file.col> / default.

ops_extra.rs      Example of registering ops outside the standard set.
```

## How to run things

```
cargo run --release                              # all examples + Rust demos
cargo run --release -- examples                  # only file-based examples
cargo run --release -- examples/17_wco_list_intersect.col   # one example
cargo run --release -- foo.col                   # any .col file as script
cargo run --release -- repl                      # interactive REPL
cargo run --release -- bench                     # benchmarks
cargo test --release                             # 40 unit tests
```

REPL meta-commands: `:help`, `:stack` / `:s`, `:types` / `:t`, `:clear` / `:c`,
`:drop [N]` / `:d`, `:load <path>` / `:l`, `:quit` / `:q`.

Multi-line input: unclosed `{` or `[` prompts `...` for the next line.

## Worked example: adding an op

Goal: a `double.<i>` op that multiplies each element by 2.

```rust
// in src/ops_extra.rs (or any ops/*.rs)
use crate::op::PrimOp;
use crate::stack::{Stack, pop};
use crate::typecheck::{Op, Typed, TypeStack, TypeEnv, tc_pop};
use crate::value::{Value, Storage, from_vec};
use crate::shape::{Interp, Shape};

#[derive(Debug)]
pub struct Double { pub interp: Interp }

impl PrimOp for Double {
    fn name(&self) -> &str { "double" }
    fn run(&self, st: &mut Stack, _env: &mut Vec<Value>) -> Result<(), String> {
        let v = pop(st)?;
        let p = match v { Value::Prim(p) => p, other => return Err(format!("double: not a prim: {:?}", other)) };
        macro_rules! d { ($t:ty) => {{
            let xs = <$t as Storage>::extract(&p)?;
            from_vec::<$t>(xs.iter().map(|&x| x.wrapping_add(x)).collect())
        }};}
        let out = match self.interp {
            Interp::U64 => d!(u64),
            Interp::I64 => d!(i64),
            // … add the widths you care about
            _ => return Err(format!("double.{}: unsupported", self.interp)),
        };
        st.push(out);
        Ok(())
    }
}

impl Typed for Double {
    fn tc(&self, st: &mut TypeStack, _env: &mut TypeEnv) -> Result<(), String> {
        let v = tc_pop(st, "double")?;
        if v != Shape::Prim(self.interp.width()) { return Err(format!("double.{}: type", self.interp)); }
        st.push(v);
        Ok(())
    }
}

// In a register function the parser will call:
pub fn register(r: &mut crate::registry::OpRegistry) {
    use crate::registry::{parse_interp, split_suffix};
    r.add(|t: &str| -> Option<Box<dyn Op>> {
        let (head, sfx) = split_suffix(t)?;
        if head != "double" { return None; }
        let interp = parse_interp(sfx)?;
        Some(Box::new(Double { interp }))
    });
}
```

That's the whole pattern. About 30 lines for a stand-alone op. No core
changes; the typechecker, parser, and registry know nothing specific about
`double`.

## How to read an example

Each `examples/*.col` file is self-contained. The file is run on an empty
stack; the resulting stack is pretty-printed. `#` line comments are
stripped before parsing.

Suggested reading order from `examples/`:

1. `01_reduce_sum.col`              — the simplest one-liner.
2. `02_per_row_sum.col`             — Prod construction + projection.
3. `06_group_by.col`                — `group` as the data-driven List
                                      constructor.
4. `08_match_classify.col`          — `branch + match` for per-lane logic.
                                      This is the "what makes collie
                                      different" demo.
5. `14_factorized_aggregation.col`  — `group + each + cleave` for
                                      multi-aggregate per group.
6. `15_galen_chain_join.col`        — two-way join with named bindings.
7. `26_triangle_wco.col`            — WCO triangle (single relation).
8. `28_wco_triangle_count_three_relations.col` — WCO triangle counting
                                      across three relations with leapfrog
                                      dispatch.

Once you've done those, the rest of `examples/` is incremental — read
whatever fits your interest.

## Where the design is going

`FOLLOWUPS.md` has the full backlog with priorities. Headline themes:

- **Surface/IR split (§11)**. Today the stack-with-explicit-ops form *is*
  the surface; in time it should be the IR, with a richer surface (named
  fields, `under` lens transforms, structural destructuring, possibly a
  Datalog-flavored sublanguage) compiling down to it.
- **WCO realization (§9)**. The algorithm is expressible today; the *cost*
  is not (per-row slicing materializes). Needs zero-copy slicing
  (borrowed-Prim variant) and probably a range-aware merge primitive.
- **`columnar` integration (§10, §12)**. dd's `interactive/` subproject
  (`ddir_col` on the `ddir_optimization` branch) is a natural target —
  column layout is structurally identical. The bridge is at storage, not
  type-discipline.
- **Differential dataflow as the engine, collie as the operator body
  language (§12)**. The cleanest division: dd handles incrementality,
  sources, consolidation; collie handles the per-batch column-at-a-time
  operator semantics.

## Glossary

- **Prim**: width-tagged column of bytes (P8/P16/P32/P64). The leaf.
- **Prod**: tuple of co-bounded Values.
- **Sum**: tagged union; `disc[i]` tells which lane row `i` lives in.
- **List**: variable-length nesting; `bounds` carves flat `values`.
- **Shape**: type-level mirror of Value (no data, just structure + widths).
- **Interp**: per-op interpretation tag (i8/u8/.../f64) — width plus how
  to read the bytes.
- **BoundsRepr**: `Var(Prim)` (general) or `Stride{stride,count}` (regular).
- **Storage**: bytemuck-backed bridge between a Pod numeric type and its
  Prim carrier.
- **Op / PrimOp**: an operator struct. PrimOp = layer-1 trait (run);
  Op = PrimOp + Typed = layer-3 combined.
- **Registry**: ordered list of factory closures that parse token strings
  to ops.
- **Cleave (`.{ ; }`)**: structural fan-out; each path runs on a fresh
  copy of TOS, results bundled into a Prod.
- **Branch (`branch.K.<mode>`)**: construct a Sum from a (col, disc) pair.
- **WCO**: worst-case-optimal join algorithm; per-attribute intersect
  rather than cartesian-then-filter at the inner step.
- **Factorized output**: a query result that preserves group structure
  rather than flattening to row-tuples.

## A final pointer

If you find yourself wishing for a verb that doesn't exist, check
`ops/helpers.rs` first — many candidates live there as internal helpers
without being exposed as ops. The pattern `make it a helper, see if it's
useful in multiple ops, then promote to a registered op` is the working
discipline.
