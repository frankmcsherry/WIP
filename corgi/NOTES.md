# corgi — notes & TODO

## Goal

Corgi is a vectorized interpreter for columns of algebraic data: products, sums, lists, with shapes defined at run time.
A program is a graph of operations over whole columns, so interpretation costs per operation rather than per row.

Corgi is built in layers.
The core is a small set of operations that mirror what one CPU core does well with SIMD:
element-wise arithmetic and comparison, scans and reductions, selection by mask and by position, ordering and search, and the structure of products, sums and lists.
Every core operation is total, and takes and returns its natural data: a sort returns its order, not only the sorted values.
Layers above add opinions:

- words written in Corgi over the core, which expand into the graph so the optimizer sees through them;
- the representations the engine chooses (an integer's storage, list bounds, tags);
- and languages that check what the core leaves unchecked (types, totality, unwrapping).

**Goals:**

- Small enough to understand and reason about.
- Few largely orthogonal operations, each capable: new work is built from what exists, and the core grows only when recombining cannot reach the cost.
- A new word must behave as what it replaces, not only equal it: the same answers at about the same cost.
- Near (or better than) hand-written Rust on bulk columnar work.
- Failure as data.
- Interpreted, with no code generation.

**Non-goals:**

- matching per-row scalar loops;
- a user-facing language inside this crate;
- specialized kernels added to rescue an abstraction that should have been reshaped.
- a stable interface while evolving.

**Motivation:**
Several consumers exist, which span a few use cases.

- DDIR uses Corgi as an interpreted expression language, and applies it to collection of independent rows.
- Datatoad hand-writes column-oriented logic, and would like to use a simpler and more direct language.
- Other projects use DDIR as an interactive sandbox, and exercise the breadth of its coverage and find holes.
- There is not currently a DuckDB-like batch processor, but that hypothetical tool also fits here.

What corgi does well, what it could do better, and what it accepts doing badly is in
[`performance.md`](performance.md).

Judge additions against this section. Anything below that conflicts with it is stale; fix the
text below, not this.

## Overview

A minimal **columnar, single-input term-graph IR** with a layered op vocabulary, a structural
shape-checker, an `ml` front-end, and a small optimizer. Standalone lib crate (no dependencies;
`serde` is optional).
`cargo test` green; `cargo clippy --all-targets` clean; `cargo run --example tour` walks the
language; `cargo bench --bench eval` measures throughput.

## Where things stand (2026-10-12)

A dated snapshot for whoever picks this up next. Numbers are from master at `6275a5d` on an M4 mini.

**Benchmarks.** Each has a README with per-query or per-program times and what dominates each:
- ClickBench, 43 queries on 1M rows: 2.1× DuckDB on one thread (geometric mean over q01–q42; 5.3×
  on 2026-10-03). [`algorithms/clickbench/README.md`](algorithms/clickbench/README.md).
- TPC-H, 8 of the 22 queries as 11 programs, scale factor 0.2: 0.92× DuckDB on one thread (2.0× on
  2026-10-03). [`algorithms/tpch/README.md`](algorithms/tpch/README.md).
- The algorithms corpus, 48 programs: 0.9–19× hand-written Rust, geometric mean 4.3×.
  [`algorithms/README.md`](algorithms/README.md).
- Single operations against idiomatic Rust: [`performance.md`](performance.md).

**Landed since one Int (#74):** checked ops as words (#79); `Builder::graft` (#80); folds run rows
longest first (#83, #84); Float reductions in row order (#82); exact tuple patterns (#86); the cheap
forms' shortcuts (#87); list equality reads lengths first (#88) and compares with a literal in place
(#89); a faster dependency-free hash for lists of numbers (#91); `hash_group`, a grouping word
written in corgi that sorts 64-bit hashes and checks the groups are exact, with `all`/`any` (#93);
`select` over lists copying each row whole (#94); filter choosing between compress and gather by
how many rows it keeps (#97).

**Open PRs:**
- #95: TPC-H's one-character flags stored as their byte. Ready. q01 goes from 1.8× to 1.4× DuckDB;
  existing TPC-H data directories need `prepare.py` run again.
- #96: list rows move as ranges of elements (one copy per run of consecutive rows, at every level).
  Ready; the decision is whether it is worth its code. ClickBench 0.93 of master's time (q24–q26 3–4×,
  q28 1.38×); three corpus programs that repeat one short row are 9–17% slower; +151 −77 lines.
- #50: reused leaf buffers, and evaluation bookkeeping once per graph. Conflicts with #97 and needs
  a rebase. Open question: whether the ~21 ns per run that installing the pool costs is acceptable
  (one-op programs on 8K rows are 7–15% slower). DDIR calls `eval_graph` directly, so it would get
  the bookkeeping saving but not the pool.
- #76 (draft): integer storages u8 through i64, with ops that keep results narrow. Parked; needs a
  rebase over #79–#97 before its numbers mean anything.
- #27: a fold over a product of monoids becomes one reduction per field. From 2026-09; can close.

**Decided:**
- No substring-search word: a host that needs LIKE provides it. ClickBench q20–q23 keep
  split-and-compare as a record of the cost.
- TPC-H grows only by queries that translate directly (q07, q08, q11, q15, q18 are candidates), not
  ones that would measure planning (correlated subqueries, EXISTS, outer joins).
- `hash`'s values may change: DDIR depends on its quality, not its values. A dependency for a faster
  hash is undecided. The candidate is `foldhash`'s `quality` variant pinned to 0.2.0: 0.76 of master's
  time over the fourteen `hash_group` queries, about 0.08 s of build, no dependencies of its own, the
  same avalanche quality as rapidhash, and the same values only on 64-bit little-endian machines.
  `foldhash::fast` fails avalanche on keys of 8 bytes or fewer; ahash's output depends on the CPU
  and costs 1.4 s of build. A hash that reads a row as bytes must not depend on storage: a row is
  hashed as one byte per element when every element is in 0..=255, whatever holds it, and otherwise
  as 8-byte words under another seed. (corgi's own byte hash today has a mild weakness: its last
  step is a multiply, so its low output bits are less mixed.)

**Tried and set aside:**
- 32-bit engine indexes (row ends, positions, offsets) with a 64-bit fallback chosen per array:
  0.98–0.99 of master's time for about 285 lines and 10% more build time. Master had already
  removed most of the index-heavy paths (byte masks, literal fills, element-wise append and select).
- Choosing between range copies and positions from a sample of the first rows: dropped from #96 as
  too complicated for what it bought (two corpus programs).
- Filter by positions always: q01 16% and q12 14% slower than #97's choice by count.
- Filter reading its mask as 64-row bit words (skip empty words, copy full ones): even with master
  overall, and twice as slow where kept rows are scattered and copied as runs. Masks are bytes, so
  it needs a packing pass, and number columns already compress at about memory speed.
- `hash_group` for a few distinct short keys (ClickBench q10, q11; TPC-H q01, q12): 1.1–1.7× slower
  than `group`, which sorts them directly.

**Where the remaining time goes:**
- ClickBench: sorting 1M 64-bit keys (integer GROUP BY and COUNT DISTINCT, and the sort inside
  `hash_group`: about nine queries); the hash and the exactness check on long string keys (q33–q39);
  substring search (left to the host); predicate chains, one pass per comparison, and DuckDB's
  block skipping (q19); q29's ninety passes.
- TPC-H: q01's grouping into four groups (a reduce by a small key, which corgi has no op for); `find`
  where keys are unique; re-sorting tables that arrive sorted.
- The corpus: loops written as folds and scans, interpreted once per element (3–16× Rust).

## Orientation (module map)

```
src/
  value.rs     Value (columnar data) + show. Leaf = Prim, an Arc<Vec<_>> per storage via the
               `prim!` macro: U8 and I64 hold integers (the same value at either; equality and hash
               go by value, and two integer leaves at different storages meet at I64), F64 floats
               (as their total-order keys). Sum = (Tags, variants), where Tags is the lane
               assignment: Const(tag, rows) or Column(u8 tags, within-lane offsets).
  engine.rs    row-movement primitives: gather, concat, fill + index generators
               (filter_mask / owner_ids / index_plan / expand_ranges).
  lib.rs       re-exports, plus `arrange`: the sort/survey/gather surface DDIR's backend calls.
  bytes.rs     the byte codec: a column to and from a self-describing, 8-byte-aligned byte string,
               for shipping columns between processes.
  hash.rs      structural hashing, one stable 64-bit word per row, over the same structure the comparator reads.
  graph.rs     OpLike, NodeKind{Input,Tuple,Op(O)}, Graph<O>, Builder<O>, eval_graph / try_eval_graph,
               shape_of (= try_eval_graph on `Value::empty(shape)`), check. eval_graph CONSUMES its arg and MOVES values to last use (enables in-place).
               Value::Ref(Arc<list>, Arc<rows>) = referenced LIST ROWS (shape Ref<List<T>>, Rust's &[T]):
               row j is row rows[j] of the shared list (the arena). A lossy gather out of range names an
               empty row of the arena: its last when that is empty, else a new arena with one more row end
               (the values shared), a copy paid only where a panic once stood. `ref` takes them (through products and sums; bounded rows stay by value), `clone` copies out (deep; the
               only copy of referenced rows), `gather` moves row numbers only; `gather_lanes` too over one
               arena, and over distinct arenas copies the named rows once into a new arena (live rows only,
               never once per reference).
               `Rows` = the reader's borrowed view (List or Ref) via `rows_of`; `into_list` takes only a
               List, so a Ref elsewhere is the shape error "clone first". Compare/sort read through refs.
  shape.rs     Shape (Int | Float | Prod | Sum | List | Ref) + shape_of_value + Display.
  optimize.rs  cse / dce / peephole / fuse_maps / cancel_isos over Graph<NumOp>. OPT-IN: `run` evals
               the unoptimized graph; tested for semantic preservation on every corpus program, so the
               passes are latent, not dead.
  ops/
    core.rs    Op<L>: structure only, organized as the KERNEL MATRIX
                          intro          elim     map       capture
                   PROD   tuple (graph)  Field    —         —        (transparent; no witness column)
                   SUM    Branch/Inject  Unwrap   MapSum    CapSum   (witness: tag column)
                   LIST   Enlist         Gather   MapList   CapList  (witness: bounds column)
               LIST elim: Gather (P, List<X>) -> P[X] replaces every integer leaf of P (any shape:
               one position, a list, nested lists, products, sums) by the row's element there;
               `get` is Gather on one position per row (`head` = get 0, so an
               empty row reads the zero of the element's shape). Fold (B,List<A>)->B is the accumulating
               elim; FoldScan (T,List<A>)->(T,List<R>) (mapAccumL — the scan kernel; `scan` is sugar =
               FoldScan with body (a,x)->(b,b), field 1). Gather (positions per row, nested or flat;
               the result keeps their structure) is the one fetch kernel; `slices` = map(range); gather. Plus Unit
               (X -> Unit) and the arithmetic + named reductions in `numeric`.
               plus the structural isos — all three pairs present: List⊗Prod (Transpose/Zip),
               List⊗List (Flatten / the word `slices`), List⊗Sum (Unweave/Weave) — and the fused forms/producers
               (Lit/Filter/Gather/Iota/Range), each reducible to kernel+isos (the `law` corpus
               programs witness it), kept for the execution strategy the expansion loses. The
               boolean mask split is the idiom `Branch(2)`; a dedicated Partition op was removed. Body-generic over L; inherent
               eval/children; NOT OpLike. (Iota: Int->List<Int> data gen; MapSum: variadic match,
               Vec<(tag,body)>, unlisted variants pass through, disjoint tags so arms commute.)
    cmp.rs     CmpOp: Rel(Pred) + SortBy/SortLimit/Find/Adjacent. Comparisons by value. SortBy is
               the sort's own output (stable by key, payload carried, runs numbered;
               dev/indexed-sort.md); `sort`/`dedup`/`group` are words over it (ml.rs::sort_word).
               SortLimit sorts the order's levels one at a time, keeping per row the first k and the
               ties at the k-th after each; it is not a word yet (dev/indexed-sort.md says why).
               Adjacent marks where runs of equal elements start (a structural compare of each
               element with the one before it); with `Cut` it splits a row into those runs without
               sorting, the cheap form of `group` when the order is already known.
               `find` on leaves (`cmp/search.rs`): a dense row of needles in order is walked into
               its haystack row (galloping, `survey::walk_ranges`); every other needle gets a
               branch-free binary search, sixteen in lockstep. Structured values: `batched_bound`.
    cmp/       the order machinery the cmp ops reduce to. order.rs: compare_idx (bulk structural
               order over index pairs; compare_cols is the diagonal case), block labels, group_bounds.
               sort.rs: the indexed discrimination sort (dev/indexed-sort.md). survey.rs: the merge
               kernel, rank at a time (dev/lane-survey.md).
    host.rs    host kernels: `NumOp::Host`, an op whose eval is supplied from outside corgi. A program
               reaches one by name with `call`, through the host's `Scope` (frontend/ml.rs).
    numeric.rs NumOp { Core(Op<NumOp>), Cmp(CmpOp), Arith(ArithOp), Text(TextOp) } : OpLike. ArithOp =
               arithmetic on two Ints or two Floats (the kind from the operands) + the `_b64` and
               bitwise ops (an Int as its 64-bit word) + shifts + the named reductions and scans.
    text.rs    TextOp: Split(u8) + ParseInt. A string is a List<Int> of its bytes (held as bytes); both
               total — ParseInt returns Sum{Ok: Int | Err: ()}, no data-dependent panic.
  frontend/
    mod.rs     the op-name resolve table (the whole vocabulary the surface reaches, apart from the
               words a host binds in a `Scope`).
    ml.rs      the one surface: ML-flavoured (let / enum / juxtaposed stages / match / inject), lowering to Graph<NumOp>.
               The words are built here: sort/dedup/group over sort_by, hash_group (written in
               corgi itself, as source grafted in), all/any over a written tuple, slices, head, and the
               checked ops try_get/try_gather/try_zip/try_chunk (check, branch, lossy op on lane 0).
               `Scope`: what a program names without defining it — the host's words (graphs, a host
               kernel being one node), grafted by `x call name`, and enums declared once for every
               program parsed in it (`parse_ml_in`).
    program.rs `Program`: parse, type and run in one path (compile_ml / shape / run).
tests/  corpus (runs programs/*.col) · ml · typer · numeric · optimize · text · effect · kernel · ref ·
        fail_allocations   (no Builder-demo file —
        every surface example, algebraic law, and property test lives in the corpus.)
programs/  *.col — the self-generating example corpus (program + `# n =` seed + `# =` golden, or
           an equivalence via `(A, B) eq → [1]`). One source: tests/corpus.rs verifies, the tour displays.
algorithms/  *.col — algorithms written plainly; benches/algorithms checks and times them against
           Rust, and algorithms/README.md lists the rewrites they call for.
examples/ (tour.rs)   benches/ (eval, gaps, idioms, algorithms)   dev/ (design notes)
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
| `List<List<X>>` | ↔ | `(ranges, flat)` | Flatten / `slices` (a word) | programs/17, 31 |

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
- **Shape = structure over two leaves, `Int` and `Float`.** An integer's storage (a byte, an `i64`)
  is the engine's choice and never part of its shape or its value. `shape_of` is LITERALLY `eval` on a zero-row column: every op is
  total on zero rows, reports a mismatched operand as `Err` (the accessors `into_pair`/`into_list`/…
  carry the message), and builds an output of the shape it would at any length — so there is one
  vocabulary, one evaluator, and no type-level shadow of it to keep in sync. `eval_graph` unwraps
  (a typechecked program cannot fail); `try_eval_graph` is the fallible form the typer and the
  body-bearing ops use.
- **Layering = enum embedding via `OpLike` + body-generic `Op<L>`.** A layer is `{ Core(Op<Self>),
  <buckets> }` impl'ing `OpLike` by delegating; the graph machinery is unchanged across layers.
- **The core is numeric-blind.** Arithmetic is `ops/numeric`; comparison is `ops/cmp`. Every leaf
  orders by value (an integer as a signed number, a float by its total order), so ONE comparator
  serves `sort`, `find`, and `Rel` alike; the sort radixes each storage's order keys.
- **One integer type.** `Int` is an integer, exact within `i64` and wrapping past it (the one
  documented edge, until exact wide integers). Integer `div` truncates and is total: `x/0 = 0`,
  `x%0 = x`, and `MIN/-1` wraps. Code that treats an integer as a 64-bit word (hashing, bit banging)
  says so with the `_b64` verbs (`add_b64`, `mul_b64`, `shr_b64` the logical shift, rotates) and the
  bitwise ops, each defined on the integer's low 64 bits read back as an `i64`; they will keep their
  meaning when plain arithmetic becomes exact. There is no integer shift: dividing by a power of
  two is `div` (run as a shift), so the word's logical shift and integer division can't be
  mistaken for each other. Text, masks and sum tags are held as bytes, as are integers brought in
  with `Value::int` (constants, parsed or fed data) when every one fits; every other integer is
  held as `i64`. A reduction of an empty row reads the zero of an integer: `fold_min` and
  `fold_max` give 0 (not an identity, which unbounded integers would not have), and a program that
  wants another default tests `len` and `select`s it.
- **Float semantics are TOTAL-ORDER, not IEEE (deliberate).** A `Float` is an `f64`, stored as its
  total-order key (`f64::total_cmp`: negatives flip all bits, others flip the sign bit), so the
  comparator orders floats with NO special case — *the key never mis-orders two values IEEE orders;
  it only supplies a definite position where IEEE declines.* The deviations, all on the "naughty" sort/eq path: NaN is orderable (sorts to the top)
  and equals itself bit-for-bit; `-0 != +0` (distinct bits — no canonicalization, by choice). The
  win: no NaN-poisons-comparison surprise. *Arithmetic stays IEEE* (NaN/inf propagate; `x/0 -> ±inf`,
  `0/0 -> NaN` — total, no panic). Arithmetic, negation, sums and products canonicalize computed
  NaNs to `0x7ff8_0000_0000_0000`, discarding their sign and payload. Inputs, selections and host
  results keep their bits. A future IEEE `eq` can be offered if needed. Floats enter via
  `to_float` or a float literal (`1.5`, `1e3`: a fraction or exponent); an Int literal is `5` or
  `-3`. Arithmetic takes its kind from its operands, and an Int meeting a Float is a shape error.
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
  because row `i`'s offset in the single lane is `i`. It is what `inject` builds, and what `branch` builds when every
  row goes to one lane — so a checked op whose rows all fit (a `try_` word) carries no witness
  columns at all. Uniformity is O(1) to detect (`const_tag`) and it PROPAGATES: a
  gather of a one-tag sum is one, a lane map leaves the assignment alone, and `unwrap`/`sort`/
  `compare` each take a no-witness path on it. Equality and hash are by the ASSIGNMENT, so a `Const`
  and the equivalent `Column` are interchangeable — including on the wire, where the codec records
  which form the sender held (as it does for `Bounds`).
- **Leaves are immutable Arc, cloned by refcount; eval moves to last use.** The last reader holds the
  sole Arc, so `into_*` move the buffer and pointwise ops are able to mutate in place (the shifts do).
  The WITNESS columns are Arc for the same reason — `Bounds::Offsets`, and a `Tags::Column`'s
  offsets — so a `Value` clone costs O(shape), not O(rows), at every shared edge in a graph.
  *Reuse policy:* an op that is elementwise AND same-storage (shifts, `bin_into`, `map_into`,
  `lane_pick`) consumes its operand and rewrites it under
  `Arc::get_mut`/`make_mut` when uniquely owned — take the reuse wherever the shape allows. The
  fresh-allocating leaf ops (`gather`/`gather_lanes` = permutation, a byte leaf widening, `rel`/`cmp_idx`/
  the sort's pulled keys = a differently-typed result) allocate *by necessity*, not oversight — the access pattern
  or output type rules reuse out. (Cross-op intermediate elimination is the separate DPS backlog item.)
- **`Fold`/`FoldScan` are cross-row lockstep, `O(total elements)`.** A general (non-associative) fold is
  sequential *within* a row but vectorized *across* rows: round `t` folds in every row that has a
  `t`-th element, in one body call, so `#rounds = the longest row`, not the element count. The rows
  are ranked once by length, longest first, so the rows still running are always the first ranks: a
  round's states are the previous round's output as it stands, less the rows that just finished,
  which are its last rows. Those are copied out once, and the state is cut in place (a prefix of
  rows is a prefix of every buffer, a list's ends and a sum's tags included). One `gather_lanes`
  puts the finished rows back in row order. A stride needs no ranking: every row runs every round.
- **`FoldScan` (mapAccumL) is the scan kernel; `Fold` is its R=Unit specialization, kept for cost.**
  `FoldScan : (T,List<A>)->(T,List<R>)` by `(T,A)->(T,R)` threads a state and emits an output stream;
  `scan` lowers to it (body `(a,x)->(b,b)`, take field 1) — measured identical to a dedicated scan.
  `Fold` does NOT lower to it: `FoldScan` with `R=Unit, .0` measures about 1.6x slower on ragged
  rows, because the body is forced to emit a `(state, output)` PAIR each round, and the outputs are
  put back in list order even when they are a dead `Unit` stream — the `Unit` *values* are free, the
  *pairing* and *bookkeeping* are not. So `Fold` is the no-pair/no-recording path. (Equivalently an optimizer rule `FoldScan[R=Unit].0 -> Fold` would
  recover it — DCE the dead output, skip recording — which restores the in-place mutation.)
- **Named monoid reductions and scans** (`fold_add`/`mul`/`min`/`max`/`all`/`any` and the prefix `scan_add`/…) are the one-SIMD-pass fast
  paths for the associative case — prefer them; `Fold`/`FoldScan` are for non-monoid bodies. Over
  Floats a sum or product runs in row order, so it is the `fold` of `add` or `mul` bit for bit
  (`all`/`any` read masks, which are Ints).
- **Keyed reductions**, `scatter_<op>`, take `(keys, values)` and give one `(key, total)` per
  distinct key: `group` followed by monoid folds, with no rows moved. The pairs' order is not
  promised (the same input gives the same order), so a program that wants key order sorts them, and
  `sort` costs one pass when they came out in it. Each row's distinct keys are numbered and each
  value added into its number's slot. Int keys spanning a narrow range in every row are numbered by
  a table over the range, which gives key order. Other keys are hashed, and the hashes numbered as
  first seen by one table that grows as it fills; once it outgrows the cache, the rest are numbered
  in parts by their top bits, each by a table of its own. Each key is checked against the first key
  with its number, in place (not leaves: a leaf's hash is exact); a row where two keys share a hash
  is numbered by sorting its keys. Prefer `scatter` to `group` when a program wants totals per key;
  it is the consolidation of `(data, diff)` pairs.

## Failure: lossy ops, and checked ones as words

The ops that can lose something are lossy under their plain names, and nothing in corgi panics on
data: `gather`/`get`/`head`/`slices` read the zero of the element's shape for a position outside its
row (zero bits, the empty list, a sum's lane 0, so a sum must have at least one lane); `zip` keeps
each row's shortest column; `chunk` drops a row's remainder. `branch` is total (a tag of n-1 or more
goes to the last lane).

Each has a checked form, a WORD built in `frontend/ml.rs` (`try_word`), not a kernel: `try_get`,
`try_gather`, `try_zip`, `try_chunk`. A word marks each row its op would lose something on, routes
the input with `branch 2` on the mark, runs the lossy op on lane 0 and makes lane 1 `()`. The result
is `Sum{T | ()}`, and the program takes it up as data — usually with a `match`. Nothing threads it
past later ops implicitly: a program that wants the error carried further says so with ordinary
ops (a `map_variant 0`, or a `branch` on the combined tags of two checked results).

A word is only as fast as the primitives it is spelled with, so writing these as words is also how
primitive gaps show up. `branch` and `cap_sum` return at once when every row goes to one lane (the
no-error case of every checked op); `len` reads lengths as differences of the ends. With no errors
`try_chunk` measures at the old kernel's speed, `try_get` 1.3x, `try_zip` 4x (two `i64` length
columns where the kernel compared the bounds in place) and `try_gather` 2.6x (its check is two
per-row reductions, where the kernel checked as it gathered).

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
is an error row). Each of get/gather carries a `_uns` tier (assert in-bounds) and a `_try` tier (total ->
`Sum{Found|Missing}`); the plain checked tier (a proven bound) is reserved. Why this direction and not
`Get = enlist;gather;head`: `gather` is list-*preserving* so it can't eliminate, and the only
irreducible piece is the bare-`X` outro — making `Get` the atom keeps one index kernel and yields the
total head for free, where the HEAD-atom route would have needed new non-emptiness analysis. (`Fold`/
`FoldScan` remain the accumulating eliminators; `head_try`→Option is expressible as a fold if wanted.)

Reversed 2026-10-02 (the totality design note): `gather` is the one fetch kernel, and it takes
positions of ANY shape — every integer leaf is replaced by the element it names, so the result has
the positions' shape. That dissolves the bare-`X` outro the paragraph above names: a scalar position
gives a scalar element, so `get` is `gather` on one position per row (no enlist, no delist), `head` is
`get 0`, and `slices` is the word `map(range); gather` (`Range` is iota with a start). Fewer kernels
was the reason: `Get`, `Slices` and their `Try` forms leave the core, and sub-list references (a
`slices` view of a referenced list) go with `Slices`.

Then one integer type (2026-10): the leaf types are `Int` and `Float`, and an integer's width is
its storage (bytes or `i64`), never its value or shape. The typed grid (`add_i64`, `div_f64`), the
literal suffixes, `cast`, `signed` and the sign-flip encoding are gone; arithmetic takes its kind
from its operands; the `_b64` verbs and the bitwise ops are how a program asks for 64-bit word
arithmetic. Unsigned 64-bit values above 2^63 have no storage of their own: a hash is the full
64-bit word read as an `i64`, and exact wide integers are the backlog item that will hold the rest.

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
  opportunistic refcount reuse (`get_mut`/`make_mut` in `bin_into`/`map_into`/move-to-last-use) — that's reuse *discovered* at runtime; DPS makes it *intentional* (explicit destination →
  guaranteed in-place, AND it threads through a chain, which is what unlocks fusion; per-op reuse
  already works, so the new value is specifically cross-op intermediate elimination). The `None`
  destination = "output is dead" idiom collapses `FoldScan -> Fold` (skip the tags/off recording + DCE
  the dead output), which would retire `Op::Fold` the way `Op::Scan` was retired. THE fork to settle
  first is the ownership model: a true destination can't be a shared `Arc`, so DPS pressures the hot
  spine toward a linear/owned tile buffer (giving up free `Arc`-clone fan-out there) vs staying
  `Arc`-shared (free clones, no fusion). First spike: run ONE stratum-stable run (e.g. `iota ; add
  ; mul ; gt`) a tile at a time through the existing kernels into reused buffers, and measure vs the
  per-op passes — that forces the ownership decision on a small surface before committing the
  convention. (Composing existing kernels, not generating code: see Goal.)
- **Index-as-value — op DONE, rewrite pass open.** `Op::Gather` (row-relative point gather; the word
  `slices` is the range form) makes indexes plain values; programs/26 (pointer jumping) and /27 (the law
  `gather(gather(v,i),j) = gather(v, gather(i,j))`) exercise it. Open: the optimizer rewrite applying
  that law, so gather chains become index math + one final gather. The lazy form (multiplicity View:
  0 = filter, ≥1 = repeat, range = slice) stays OUT of `List` — collie's `Selector` (4 variants × a
  composition matrix × per-op awareness) is the cautionary tale. What IS in the representation is
  the REFERENCE, as its own shape: `Ref<List<T>>` (`ref`/`clone`), because a List row is the only
  unbounded-size row and capturing it by copy is `elements × length` where a closure pays `elements`
  (pointer chasing through a list every row copied: 2088× → 3.7×). Only list rows are referenced: a first cut also had thin
  `&T` row refs into any column, whose one use (a referenced tuple read for its scalar, K2) is Field
  pushdown's job; they come back with a consumer (the μ-type knot). The choice is explicit in the program — `(ctx ref, ys)
  cap_list` is one reference per element, `(ctx, ys) cap_list` copies — and the only copy of
  referenced data is `clone`. A first spike put the same spans inside `Bounds` with a second gather
  and a second accessor deciding reference-vs-copy implicitly (branch `corgi-spans`); rejected as
  sneaky. Next: Field pushdown through `cap_list`/`cap_sum`, then the mechanical closure-capture
  pass (which inserts the `ref`/`clone`). What that pass should know: a reference pays for LOOKUP
  (`get`/`gather`/`find`/`slices`), not ITERATION. A body that iterates a captured list (`map`,
  `fold_add`, `cap_list` over it) must `clone` it per element, which is the by-copy cost, and reading
  through spans would not change that: iterating per element is elements × length however the list
  is held. The win there is HOISTING — an iteration that does not depend on the element (`c fold_add`)
  runs once per owner row before the capture, and the scalar is captured instead — i.e. the pass
  asks what the use needs and copies (or computes) only that. And the GROWING-STATE
  fold (a List accumulator is rebuilt per round, O(k²)/row, 4567× at k=4096): let
  a `FoldScan` state hold a `Ref` into its own emitted-so-far output — append-only and immutable, so
  the ref is stable and `get` through it is O(1) — which turns a self-referential recurrence / stack
  machine into O(k) per row. (The unconditional collect already has the linear `foldscan` spelling.)
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
- **JSONL / Extern** — `split`/`parse_int` landed as the `text` bucket (typed, not `Op::Extern`); `parse_json` remains open and still wants `Extern` or recursion (μ-types below).
- **Named declarations — enum half DONE; struct half deliberately skipped.**
  `enum Name = V0 | V1 in …` is a parse-time table (variant-name → (tag, arity)); names erase at parse and the core stays positional.
  Two enums may declare the same variant name; a use of it then names its enum, `Foo::Bar` (nothing is typed while parsing, so the
  name must pick the tag by itself), and an unqualified use of a shared name is a parse error that lists the choices.
  Use sites: `inject V` (the tag AND the other lanes' shapes off the declaration), `map_variant V`, named `match` arms, `branch Name` (arity by enum name).
  Payload shapes: `enum Node = Lit int | Add (int, int) | Str List(int) | Wrap Other in …` — a variant may omit its shape unless
  another variant is `inject`ed (the lanes `inject` does not fill are built as EMPTY columns of their shapes; the lane it fills, if
  left open, takes its payload's, so `enum Option = None () | Some` serves `3 inject Some` and `"dog" inject Some`). Shapes nest by
  naming an earlier enum; no recursion (μ-types are the backlog item below). There is no `⊥`: every Sum lane, in values and in shapes,
  is concrete, so `shape::join` is gone and every merge (`Unwrap`/`Select`/`Find`/`Append`/fold state) is an equality check.
  Companions landed with it: lambda parameters take `let`-style tuple patterns (`map ((lo, hi) -> …)`), and constant operands are literals (`(x, 1) sub`; the core's `And`/`Shr`/`Gt` immediate kernels are ops with a parameter, `x shr 3`; `shr` later retired for `shr_b64` and `div`).
  Field-name projection (`s.a`) and record literals stay OUT: parse-time resolution would need globally-unique field names (a misapplied name silently projects the wrong index) or typed resolution, and destructuring covers the corpus without either.
  A host declares enums once for many programs with `Scope::declare("enum Size = Small int | Big (int, int)")`; each program parsed in
  the scope uses them as if it had declared them. Enum names are unique across the scope and its programs; variant names may be shared.
  Mechanical closure capture (free vars threaded via `CapList`/`CapSum`) remains the open companion pass.
  Programs/28 exercises the whole bundle and the sum-heavy programs (09, 11, 18, 19, 23–25) use the named style; the numeric `inject tag arity` form is gone (a sum is only built from a declaration).

- **Exact wide integers** — `Int` wraps at the `i64` edge for now. Next: an `i128` storage (holds
  any `i64` or `u64`, and the sums that leave 64 bits, e.g. ClickBench q03's AVG of ids), then
  arbitrary precision held apart as a sum. Narrower storages (`i8` for DD diffs, 32-bit for offsets
  and ids) come in behind the same `Int`, each measured as it lands.
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

Optimizer rewrites live in `src/optimize/passes/`. `optimize(&Graph) -> Graph`
normalizes without input shapes; scalar folding runs before immediate lowering in the optimizer.
`compile(Graph, &Shape) -> Result<Compiled, String>` checks the whole graph at its input
type, strips checked patterns (including in bodies), then normalizes. `Compiled`
keeps the graph and checked input type together, with read-only accessors.
`analyze_demand` reports requests in original field positions without rebuilding.
`narrow(Compiled, &Demand) -> Result<Narrowed, String>` rebuilds each producer
at its readers' request and normalizes. `Narrowed` returns the graph and the
requests on its original input and actual output, including wider references.
`Demand::project` compacts a value at a request, matching `projected_shape`.
Analysis, shape-aware union and rebuilding normalize identity requests recursively:
nonempty products of All, sums of All, List(All), and Ref(All) become All. None stays
None, even on unit; a retained unit field and a dropped field are distinct.
Products of unread fields retain their slot when nested and stay explicit.
Call `compile` first; `narrow` consumes that graph and its recorded type, and does
not strip patterns. `Demand::All` asks for everything
the caller reads. Existing references keep their available payload, even when the
requested output asks for less.

`Program::from_graph` wraps its graph unchanged. ML compilation calls `optimize`
explicitly; callers supplying graphs choose their own normalization step.

A Builder tuple with no fields evaluates to unit at its graph input's row count,
including inside map and fold bodies. A literal empty product also fills to unit
at its anchor's row count. Explicit `Value::Prod(vec![])` values passed as data
still have zero rows.
