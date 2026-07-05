# sprig

A columnar Joy.

sprig is the smallest Joy/Forth that still grows, in its own language, up to a worst-case-optimal join.

It keeps tada's two stacks, `data` and `todo`, and tada's trick: a combinator is not a blessed object that executes programs, it is a command that pushes more commands onto `todo`.
Running a program is `todo.extend(body)`, nothing more.

What changes from tada is the noun.
A sprig noun is a quoted program or a `Data`: a tree of `i64` columns, where each node carries a flat column of `values` and a list of child `layers`.
There is no List, Product, Sum, or Trie type.
Those are conventions built over `Data` in the language, not cases in the kernel.

## The kernel

Six shapes of information motion, and nothing else:

- `map` — elementwise scalar ops over a column: `add sub mul eq lt shr and`
- `scan` — prefix-reduce a column by the `+` monoid: `scan sum`
- `gather` — read a column at a column of indices: `gather`
- `scatter` — write at a column of indices, gather's dual: `scatter` (`1 scatter` is bincount)
- `sort` — the permutation that stably orders a column: `sort`
- `find` — the seek: batched membership in a sorted segment, the join's core

plus `iota`/`len` to make and measure columns, `layer`/`unlayer`/`cat` to nest and append columns, `print` to show one, and tada's Joy spine for control flow: `dup pop swap rot concat cons cadr eval ifz`.

A scalar is a column of length one, so the old scalar Joy programs run unchanged.

Values can be named. `=x` binds the top of the stack to `x`; `$x` recalls it. This is the one thing the two stacks could not give: a column computed once and reused by name, instead of re-derived or juggled positionally. It is a small value environment beside the two stacks, not a shape of motion.

Columns are `Arc`-shared, so `dup` and `$x` are O(1) — they bump a refcount, not copy. The few verbs that write a column in place clone it first only when it is shared (copy-on-write), so a write through one name never disturbs another. This makes naming and aliasing free; it does not speed up the triangle join, which is bound by sorting and gathering, not by copying.

## Run it

```
cargo run --bin sprig
> { 3 1 2 } dup sort gather
data: { 1 2 3 }
> 10 fib
data: { 55 }
> { 1 2 3 } { 4 5 6 } mul sum
data: { 32 }
> { 1 2 3 4 5 } { 2 4 7 } isect      # gallop A into sorted B, count hits
data: { 2 }
> 3 triangles                        # count directed triangles in arc(3)
data: { 6 }
> 3 tris                             # produce them, as columns a / b / c
data: { 0 0 2 2 3 1 { 1 2 3 0 0 2 } { 2 3 0 1 2 0 } }
> 1000000 triangles                  # arc(1M): 2,999,997, in a few seconds
data: { 2999997 }
```

## The triangle join

sprig grows, in its own words, up to a worst-case-optimal triangle join. The count of directed triangles is the sum over edges `a->b` of `|out(b) ∩ in(a)|`, each intersection sought *smaller-into-larger* so a degree-N hub costs `log N` per probe, not `N`. Built as words:

- trie layer: `trielevel` — distinct keys of a sorted column, with group-start offsets as a child layer.
- intersect: `isect` — `find` every element of one sorted list in another and count the hits, at `|A| log |B|`.
- the join: `triangles` (or `tri2`, with let-bindings) counts; `tris2` produces the relation `(a,b,c)` columnar. `arc(3)` is 6; `arc(1,000,000)` is 2,999,997. With bindings, counting one million is ~0.45 s and producing all three million tuples is ~0.49 s — producing costs almost nothing over counting.

The roadmap predicted "the first operation that genuinely cannot be written as a word" would be either a new motion-shape or a sign the project changed. Both happened, and neither was what we guessed:

- The **seek** (`find`) is a verb, not a word: a word-level binary search would thread the whole haystack through a loop, and `dup` is still a copy. It is the foretold new motion-shape — the sixth.
- The real wall was not the seek but having **no way to name a value**. The join needs ~15 columns live at once, and a stack with only `dup`/`swap`/`rot` cannot hold them.

Two versions of the count live side by side, and the gap between them is the lesson:

- `triangles` makes every column an accessor *word* that re-derives itself from `N`. Fully in-language with no naming, but each access recomputes its whole subtree: 2.5 s at one million.
- `tri2` names each column with `=` and recalls it with `$`, computing each one once. It reads top-to-bottom as the dataflow it is, and runs in 0.45 s — 5.5x faster, and within a small factor of the hand-written reference.

So the binding earned its place: positional access (`pick`/`roll`) would have given the same single-evaluation but with indices that shift on every push and fragments that cannot be factored into words. A name is stable and composes.

The measured reference (`cargo run --release --bin triangle [N]`) computes the same answer three ways — per-edge ground truth, the batched columnar pipeline, and the closed form `3(N-1)` — and can produce all 3M tuples in about 0.2 s.

## A reusable WCOJ vocabulary

The triangle words above are fused for one query. The same machinery factors into a small vocabulary, mirroring datatoad's worst-case-optimal join constructs, so other cyclic queries compose from the same words:

- `index` — a relation keyed by one column, its neighbours sorted within each key, with offsets and degrees, bundled as one columnar value (datatoad's *Forest*).
- `propval` — for a batch of rows, propose candidate values from the smaller index and validate each against the larger by `find` (datatoad's join + semijoin).
- `meet` — extend a new variable bound by two indexes: count both sides per row, route each row to its smaller side, `propval` each, and union. Returns `(rowid, value)` — for every surviving extension, which input row it came from and the new value.

`rowid` is the point: it links each result back to its input row, so any payload is recovered with one `gather`, and the new variable's two sources may be two different indexes or the *same* index twice. The triangle is then `tri3` (count) / `tri3p` (produce): build `out` and `in` indexes of `arc`, then `meet` them keyed by `b` and `a`. It runs within a few percent of the hand-fused version — the abstraction is free. A `meet` over two atoms is the worst-case-optimal core; a query binding a variable with *k* atoms folds `meet` over them.

Relations enter from outside with `.load <name> <path>`, which reads a CSV of integers and binds each column to the environment as `name0`, `name1`, …. That is enough to run a second, real cyclic query — GALEN's rule 6, `q(x,e,o) :- q(x,y,z), q(z,u,o), r(y,u,e)`, a triangle on `(y,z,u)` over three relation instances (`q` twice, `r` once). Its join core is the same `meet`, keyed off two indexes built from the loaded relations; `gr6core` reproduces the reference survivor count. The vocabulary that solved the triangle solved a different cyclic join unchanged — which was the point of having a vocabulary.

## Recursion: a Datalog fixpoint

The last step is iteration. A relation of pairs is one composite column, and three more words give set-at-a-time recursion:

- `merge` — sorted union with dedup (the LSM union).
- `diff` — antijoin, `a \ b`, via the `find` seek.
- `extend` — the one-index join: propose each prefix's matches and expand.

Transitive closure is then semi-naive: `tcstep` extends the frontier along edges, `diff`s against the closure so far to keep only new pairs, and `merge`s them in. The loop is tada's `iter`, a while-loop driven by the size of the new frontier — when it reaches zero, the fixpoint is reached. `tc` returns the closure size: a directed path of `N` nodes gives `N(N-1)/2`, a `k`-cycle gives `k²`. Recursion falls out of the `todo` stack as a word, with no new machinery in the kernel — which is the whole reason the concatenative spine was worth keeping.

## The whole thing: GALEN

To hold a closure without re-sorting it each round, it becomes a list of sorted runs — a `Data` whose layers are the runs — merged log-structured (`consolidate`, the binary-counter cascade). Ternary relations are one composite integer; sparse pair-keyed lookups use `locate` (a `searchsorted` returning the position) over a sorted index (`pindex`/`plook`). With those, the entire GALEN ontology benchmark — six mutually-recursive rules over a binary `p` and a ternary `q`, three of them cyclic worst-case-optimal joins — runs as a few dozen words. `galeng` seeds the closures from the loaded relations and iterates all six rules to the joint fixpoint. It returns p=7,560,179 and q=16,595,494, matching datatoad exactly. The kernel never grew an IR; every step is a word over the same handful of motion verbs.

That first `tc` re-sorts the whole closure each round. A real LSM does not: it keeps the closure as a list of sorted runs — a `Data` whose layers are the runs — and only merges runs of the same size. Appending a round's new facts is one O(1) `layer`, never a merge of the small batch into the big closure. Membership (`member`) folds a query over the runs with `iter`; `consolidate` is the binary-counter cascade that merges the top two runs while the smaller is more than half the larger, so the survivors at least double going down and the run count stays O(log N). `tcL` is the same transitive closure over this structure: it returns the same sizes, and the closure of a 125,000-pair relation is held in nine runs, not five hundred. The kernel grew by one verb for all of it — `nlayers`, to count the runs.

## The incremental index

The first GALEN fixpoint rebuilds an index from the whole closure every round. Each round flattens the run-list back to a column and re-sorts it into a fresh `index`/`pindex` — work proportional to the *closure*, not to the round's new facts, repeated for every round until convergence. That is the cost that makes the interpreted fixpoint slow.

The fix is to notice that the closure runs *are* the index. A run sorted by the composite `a·K+b` already groups every tuple with first field `y` into the contiguous range `[y·K, (y+1)·K)`. So a query for "all `b` with `a=y`" is a `locate` of that range in each run and a gather of the values, folded over the runs — no flatten, no rebuild. `probe` does exactly this: it reads a batch of query keys against a whole run-list and returns `(rid, val)`, the matching second field with a row-id back into the query batch. It is `member` and the segment-expand of `extend` fused. Because `K` rides on the stack, `probe` is field-agnostic: a ternary `q` packed as `x·K²+r·K+z` is probed by `x` with radix `K²` (yielding the packed `(r,z)`) or by the pair `(x,r)` with radix `K` (yielding `z`). The closure is held in the few orderings the rules need — `p` by either column, `q` by its first column, by its first pair, and two distinct projections — and each ordering is one run-list, appended and `consolidate`d on its own schedule.

With the index gone, the worst-case-optimal `meet` follows: `meetr` is `meet` over run-lists, using `degr` (the per-key degree summed across runs, `probe`'s counting twin) to route each row to its smaller side, `probe` to propose, and a composite `member` to validate. No bundle is built. Every cyclic rule then becomes a `meetr` driven by a *delta* — the round's new facts — rather than by the full closure. The whole of GALEN, all six mutually-recursive rules, runs this way as `galeng`: each round's cost is proportional to the new facts it derives, not to the closure it has accumulated. It returns p=7,560,179 and q=16,595,494, matching datatoad exactly, in about 50 seconds — against 150 for the closure-rebuilding semi-naive and 770 for the naive fixpoint.

One invariant earns a comment in the code and a mention here, because it is the kind of bug that hides until scale. A cross-product that indexes a payload by `offset + local` assumes that payload's `probe` output is laid out contiguously per row — but `probe` concatenates its runs, so a multi-run closure's output is not contiguous. The offset-indexed side of a cross-product must therefore be a single-run (static) relation; the multi-run closure side is recovered by gather instead. With one run the two are indistinguishable, so the rule passes every small test and the single-relation reference, then over-derives only once a closure has grown enough runs to interleave. The fix is to put the static relation on the offset-indexed side; the symptom that found it was a fixpoint that converged to a strict superset of the right answer.

## Performance

Full GALEN runs in about 32 seconds and returns the exact answer. The naive fixpoint took 770, the closure-rebuilding semi-naive 150; datatoad does it in about 12, so sprig is roughly 3x off a compiled engine, as an interpreter with no fusion. A sampling profile is the place to look, and its first result is the important one: the interpreter itself — the two-stack evaluator, the command dispatch — is about *zero percent* of self time. Every operation is bulk over a column, so the per-op interpreter cost divides by the column length and vanishes. The language is not the bottleneck; the kernels are. That is the whole bet of a columnar Joy, and the profile is where it pays off.

What the kernels spend time on, and what datatoad does differently:

- **Binary-search seeks.** sprig answers a key lookup with an independent binary search into the closure. datatoad gallops sorted streams (leapfrog), skipping rather than searching, and its trie's first level is a dense key-to-offset table. Two changes narrowed this. First, the closures are collapsed to a single sorted run rather than the log-structured pile of runs the append-optimised LSM leaves: a Datalog fixpoint is *query*-heavy — it probes its closures far more than it appends to them — so one run that every seek folds over once beats twenty runs each seek must search. `merge2`, a linear merge of two already-sorted runs, makes collapsing cheap. Second, the two seeks per key (the bucket's start and end) are not symmetric: the end is only a bucket-width past the start, so `gallop` finds it by exponential search outward rather than bisecting the whole closure. Together: 50s to 32s.
- **Sorting.** After those, the largest single cost is `argsort`. sprig generates join results in expansion order, then sorts them to dedup and to merge into the closure; datatoad generates them already ordered. Sorting unsorted candidates each round is the price of the simple "produce then sort" shape.

The two clean wins are taken. What remains needs a deeper change in kind, not degree: producing join output already sorted (to retire the per-round `argsort`), or leapfrogging the seeks over sorted query batches (to retire the per-key binary search) — both moving sprig toward datatoad's incremental trie, and both a deliberate redesign rather than a local tweak.
