# collie vs K / BQN / Uiua — what does the sum-type machinery actually buy?

Per-task comparison. K snippets are k9-flavor; BQN and Uiua use their standard glyphs. I've kept the analogues honest: where an array language has a clean idiom, I use it; where the task would require heroics or simply isn't expressible, I say so.

## Tasks where the array languages are more concise

### Per-row sum of two fields

| lang | program |
|---|---|
| collie | `dup .0 swap .1 +` |
| K | `+/x` (if x is a 2×N matrix) |
| BQN | `+˝x` |
| Uiua | `+` (if two arrays on the stack) |

collie's product-unpacking tax is real. The array languages assume "two values on the stack/in a matrix" is the natural shape; collie requires you to spell out the projection.

### Per-row average of an inner list

| lang | program |
|---|---|
| collie | `dup reduce.+ swap count as.f64 /` |
| K | `(+/'x)%#'x` |
| BQN | `(+´¨)÷(≠¨)` |
| Uiua | `÷⊃⧻/+` |

The array languages have polished glyphs for the common "do X to each row" idiom. collie requires explicit `reduce.+`, `count`, and an `as.f64` cast (because K et al. coerce numerics aggressively).

### GROUP BY

| lang | program |
|---|---|
| collie | `dup .1 swap .0 group reduce.+ zip2` |
| K | `+/'sales group region` (using k9's `group`) |
| BQN | `+´∘⊔` |
| Uiua | `⊕+` |

K, BQN, and Uiua all have first-class GROUP BY. collie's `group` is competitive in expressiveness, but pays a token tax for the `zip2` at the end. Uiua's `⊕+` is the brevity champion.

### Filter

| lang | program |
|---|---|
| collie | `dup 4 >= where` |
| K | `x@&x>=4` |
| BQN | `(≥4)⊸/x` |
| Uiua | `▽≥4.` |

Roughly tied; collie's `dup … where` matches the array languages' "duplicate, predicate, filter" pattern.

## Tasks where collie is in a category of one

### JSON-ish variant sizes (Object: #keys, Array: #elems, Number: 1)

| lang | program |
|---|---|
| collie | `match { -> count -> count -> drop 1u64 } as.u64` |
| K | (no clean expression — see below) |
| BQN | (no clean expression) |
| Uiua | (no clean expression) |

This is the headline gap. None of K/BQN/Uiua have a first-class discriminated-union type. To do the equivalent you'd need to manually maintain (a) a discriminant array, (b) a separate per-variant data array for each variant, and (c) the merge logic that re-injects per-variant results into row order. All three are *implementation* of what collie's `Sum` value does natively.

A K user attacking this would typically reach for:
- A separate per-row branch (`if[…;…;…]`-style), giving up the data-parallel story
- Or a tagged dictionary representation, where each "variant" is a (tag, value) pair and the whole thing is a list of pairs — at which point the lookups are O(1) but the *type system* gives no help, and there's no equivalent of `match`'s exhaustiveness or arm-output-shape-unification check.

collie's `match` does this in one operator and keeps the columnar promise: each arm runs on a dense per-variant slab; nothing branches per row.

### Condition-to-sum (Tall/Short)

| lang | program |
|---|---|
| collie | `dup 6 >= as.u8 dup rot swap partition2 inject2` → `Sum<U64 \| U64>` |
| K / BQN / Uiua | can produce `(disc; vals)` pairs, but not a *typed* sum |

The array languages can compute the discriminant and either partition the values or tag them. What they can't do is express the *output type* as a discriminated union that subsequent operations can dispatch on. If a downstream consumer wants to handle Tall and Short differently, that consumer has to re-discover the structure.

### Hoist `(a, Sum<b,c>) → Sum<(a,b),(a,c)>`

| lang | program |
|---|---|
| collie | 13 tokens (with stack juggling) → `Sum<(U64,U64) \| (U64,F64)>` |
| K / BQN / Uiua | **inexpressible** — no value can simultaneously contain a U64 array and an F64 array as variant lanes |

This is the algebraic move I think justifies the whole project. The output type has *heterogeneous lanes*: lane 0 is (U64, U64), lane 1 is (U64, F64). The array languages don't have a single value that can hold both; you'd have to maintain two parallel structures and convince yourself they stay in sync.

collie's type checker actually reports the output: `Sum<(U64, U64) | (U64, F64)>`. The types stay coherent through the program; subsequent ops can `match` on this sum and operate per-variant.

### Explode `[(a, [b])] → [[(a, b)]]`

| lang | program |
|---|---|
| collie | `xprod` (1 token, primitive) |
| K | `as ,\:' bs` (each-left/each-right cross) |
| BQN | `as∾¨⊸·bs` (similar shape) |
| Uiua | similar |

A draw if you have `xprod` as a primitive. Without it, the array languages have a slight edge because their cross-product idioms are well-trodden. (Without `xprod`, collie's expression is the 18-token program I wrote in demo 4.)

### Sort-merge equijoin

| lang | program |
|---|---|
| collie | ~20 tokens with picks |
| K | `((lk?rk)…)`-style, ~6 tokens — but hash join, not sort-merge |
| BQN | derivable cleanly |
| Uiua | derivable cleanly |

The array languages typically use hash-based joins (`?` in K finds indexes by value). collie's `intersect` is the explicit sort-merge primitive. For *sorted* inputs (datatoad's case), sort-merge has better cache behavior and supports gallop for asymmetric workloads. For unsorted inputs, hash usually wins.

## Performance (collie vs hand-written Rust at 1M elements, release mode)

After SIMD lift + Arc + scalar-broadcast inline:

| task | collie | rust | slowdown |
|---|---|---|---|
| Per-row sum of two U64 fields | 0.42 ns/el | 0.33 ns/el | **1.3×** |
| Filter (keep ≥ 50) | 0.83 ns/el | 0.49 ns/el | **1.7×** |
| GROUP BY (100 regions, sum) | **6.4 ns/el** | 12.1 ns/el | **0.5×** (collie faster) |
| Sort-merge equijoin (L=R=100K) | 1.97 ns/el | 1.14 ns/el | **1.7×** |
| Add (N=1024 L1-resident) | 0.21 ns/el | 0.17 ns/el | **1.3×** |

What this tells you:

- The interpreter dispatch is amortized over thousands of elements per op, so the overhead per *op* is irrelevant when the per-op work is non-trivial (GROUP BY: 1.2× because both arms spend most time in sort).
- Three big wins along the way:
  - **Lifted op dispatch**: match on the op outside the inner loop instead of inside; LLVM autovectorizes the monomorphic inner kernels (40 NEON vector ops in `bin_arith` on ARM64).
  - **Arc-backed primitive columns**: `dup` is now `Arc::clone` instead of a Vec copy. For bench 1 (which dup's a 16MB Prod) this was a 35% win.
  - **Scalar-broadcast inline**: `cmp` and `arith` kernels handle `(scalar, vec)` and `(vec, scalar)` cases without materializing the broadcast. This took filter from 3.4 ns/el to 1.1 ns/el (3×).

Remaining overhead is mostly output Vec allocation. Closing the last gap would need either output-buffer pooling or borrowed Value variants (so e.g. compare-then-where can write directly into the filtered output without an intermediate Bool mask).

## The short version

- For **flat array work**, K/BQN/Uiua are more concise and similarly fast. collie isn't trying to win this fight.
- For **per-row nested work**, collie's `each` matches K's `'` rank operator in expressiveness; the array languages have slicker glyphs.
- For **sum types** — variant-aware control flow, partition/inject/match, heterogeneous lanes — **no other language in this family even tries**. This is what the whole columnar type-universe pitch was for, and it's the most defensible chunk of territory.
- For **relational** work, both collie and the array languages can express joins; collie's typed shape system means the result type stays coherent through complex pipelines.

The right way to think about collie: *array language for data with shape*. K/BQN/Uiua assume your data is a rectangle (with concessions for ragged lists); collie assumes your data is a closed inductive type built from {primitive, product, sum, list}, and gives you the algebra to navigate it.

## Let-bindings: brevity vs structure

With Factor-style named bindings, the demos that were dominated by stack juggling now read structurally. Comparison of demo 6 (hoist) before and after:

Before (17 tokens, all juggling):
```
dup .0 swap .1 split roll 3 pick 3 partition2 swap roll 3 zip2 swap roll 2 zip2 inject2
```

After (named bindings make the algebraic structure visible):
```
{| p |
  p .1 split
  {| disc B C |
    p .0 disc partition2
    {| A0 A1 |
      disc A0 B zip2 A1 C zip2 inject2
    }
  }
}
```

The token count is similar; the *structure* is now in the text. No `pick`/`roll`/`swap drop` appears anywhere. K/BQN/Uiua have lambda equivalents (BQN's `𝕩`/`𝕨`, Uiua's `⟨…⟩`) but none has clean syntax for variant-aware sum decomposition that this enables.

## Related work outside the array family

The K/BQN/Uiua comparison above is the array-language axis. There's another axis worth naming — the **typed concatenative stack** family — where the closest relative is:

### slap ([github.com/surprisetalk/slap](https://github.com/surprisetalk/slap))

A stack-based concatenative language with static type inference and **linear types**, ~3000 LOC of C99. Same surface family as collie (postfix, structurally-delimited code blocks, types inferred not annotated), but with three structural differences worth comparing:

1. **Linear vs free `dup`.** slap enforces that each "box" be consumed exactly once — no `dup` of a linear value. This catches resource-management errors at type-check time (great for graphics / IO / file handles). collie has free `dup` because columnar values are `Arc`-shared and duplication is O(1); linearity would be the wrong constraint for our data-parallel model.

2. **Named tags vs positional lanes for sums.** slap declares unions as `{'ok int 'no str} union`, constructs with `42 'ok tag`, pattern-matches with `{'ok (...) 'no (...)} case`. Tags are *symbols carried at runtime*. collie's Sums are positional — `lane 0`, `lane 1` — and `match { -> ... -> ... }` runs each arm against its lane index. slap's reads more self-documentingly for 5+ variant unions; collie's is more uniform and column-dense. (The right way to bridge this is probably *not* runtime tags — see FOLLOWUPS on surface-vs-IR layering.)

3. **Row-at-a-time vs column-at-a-time.** slap's `tag` operation wraps one value. collie's `partition2 inject2` builds an entire Sum *column* from a mask in one shot. This is the scalar-vs-columnar split that everything else flows from.

For someone choosing between them: slap is the right tool when you want **type-safe resource management for scalar code**; collie is the right tool when you want **column-at-a-time work over typed nested data**. Almost no workload genuinely overlaps.

Other relatives worth a sentence:

- **Joy / Factor** — older typed concatenative ancestors. Factor's `{| names | body }` syntax is what collie's let-bindings are borrowed from.
- **Cat** — Christopher Diggins' typed concatenative; similar typing goals, never widely adopted.
- **Forth** — the spiritual ancestor of stack langs, untyped.

None of the array languages above sit in this family — K is APL-flavored functional, BQN is tacit functional, Uiua is stack-but-untyped.

## Performance bake-off — collie vs CBQN (May 2026)

CBQN built fresh from `dzaima/CBQN` master, single-threaded. collie `cargo
build --release`. Numbers are ns per *input* element (lower is better);
warmed up, multiple runs averaged.

| Task | collie | BQN | Rust baseline | collie / BQN |
|---|---|---|---|---|
| Per-row sum of two u64 cols, N=1M | 0.42 | 0.21 | 0.33 | 2.0× |
| Filter `≥ 50`, positions only, N=1M | 0.83 | — | 0.49 | (n/a) |
| Filter `(x≥50)/x` with values, N=1M (pre-fusion) | ~3.7 | 0.14 | ~0.45 | ~26× |
| Filter `(x≥50)/x` with values, N=1M (post-fusion) | ~1.6 | 0.14 | ~0.45 | **~11×** |
| GROUP BY 100 regions + sum, N=1M | **6.4** | 9.34 | 12.13 | **0.69×** (collie faster) |
| u64 add, N=1024 (L1-resident) | 0.21 | 0.12 | 0.17 | 1.7× |
| u64 add kernel-only, N=1024 | 0.17 | — | 0.17 | (parity) |

### Where the gaps are

1. **Filter-with-values was the loudest gap — partially closed.** collie
   originally spelled `(x≥50)/x` as `dup 50u64 >=.u64 where gather` —
   three passes plus one over a position vector. **Landed in this
   session:** a `filter` op (one-pass mask-and-write) plus a parser
   peephole that auto-fuses `where gather` → `filter`. Got from ~3.7
   to ~1.6 ns/elem (~2.3× speedup). Remaining ~11× gap vs BQN is the
   inner-loop branch — `if mask[i] != 0 { push }` resists
   autovectorization; BQN uses predicated stores. Further closing would
   need either a SIMD-aware kernel (manual intrinsics or Rust portable
   SIMD) or RankSelect-style mask compression.

2. **Per-row sum and L1-resident add: ~1.7× slow — partially closed.**
   Kernel-only collie matches Rust within 5%. The remaining caller-level
   gap was fresh allocation. **Landed this session:** Storage trait
   grows `extract_arc(p: Prim) → Arc<Vec<backing>>` for owned access;
   arith kernels try `Arc::try_unwrap` on either arg, mutate in place
   when one is unique. Bench 1 doesn't show the win because its input
   Prim is shared with the cached outer `pair` (refcount always > 1),
   but chained arith with anonymous intermediates — e.g., `x dup dup
   +.u64 +.u64` — gets ~2.5× on the intermediates. Real workloads with
   sequential arith pipelines (column derivations like
   `quantity 100 *.u64 price_raw +.u64`) benefit.

   *Compose with `name>`:* let-bindings would otherwise block Arc-1
   reuse because env holds a copy throughout the binding's scope.
   **Landed in this session:** `name>` parser form (#51) — emits
   `Ref { take: true }` which `mem::take`s the value out of env,
   restoring refcount=1 for the next op. On a chain of 4 distinct
   binding-consuming adds at N=1M:

   | spelling | time |
   |---|---|
   | `a b +.u64 >ab` (clone, refcount=2 throughout) | ~6.5 ms |
   | `a b> +.u64 >ab` (take, refcount=1 at the add) | **~1.6 ms** |

   ~4× speedup. **And now landed (#54): auto-inference.** Bare `name`
   at its last reference auto-marks as take. Same chain without any
   `name>` annotation runs at ~2.5 ms — equivalent to the explicit
   version. The user no longer needs to write `name>` for perf; the
   annotation is now purely for source-visible documentation of
   resource lifetimes (compiler verifies the annotation matches
   inferred liveness).

3. **GROUP BY: 1.4× slow.** Dominated by sort-by-key (same comparison
   sort in both). BQN probably has tighter inner kernels via Singeli
   SIMD. Less obvious how to close — possible win from radix-sort for
   narrow keys (the u8 region key here is the obvious target).

4. **Where collie is already competitive.** Kernel arith hits Rust
   within 5%. This is the SIMD ceiling, we're at it. The structure tax
   (Prod projection, Arc cloning, op dispatch) accounts for everything
   between kernel and full-program timing.

### Honest workload caveats

- **BQN can't cleanly express collie's headline workloads** — nested-
  list joins, WCO triangle, intersect.u64, view-based per-anchor work.
  The comparisons above are only on what BQN can *idiomatically*
  express. The WCO triangle (datatoad stress test) is the appropriate
  perf comparison for collie's design center, not these microbenches.

- **Small N favors collie relatively.** BQN has interpreter dispatch
  overhead too; we'd expect collie to look relatively better at
  N=10K and below. The benchmarks above are at N=1M which favors
  BQN's per-element kernel work and amortizes dispatch.

### Update — May 2026 overnight session

Landed this night:

**Polymorphic sort.** New `sort` and `sort.perm` operators dispatch by
shape. Prim leaves go through specialized kernels: u8 counting (one pass,
256 buckets), u16 LSD byte-radix (two passes), u32/u64 fall through to
Rust's pdqsort (`sort_unstable_by_key`). Per-shape recursion handles Prod
(sequential field sorts, label-refined), List (length-first then per-
position), Sum (disc-refined then per-lane recursion). The bundled
`(labels, value) → (perm, new_labels)` signature lets multi-column sorts
operate in a single sweep.

| task | collie (was) | collie (now) | notes |
|---|---|---|---|
| sort.perm 1M u8 | 12.9 ms | **2.5 ms** | counting-sort fast path |
| sort.perm 1M u16 | ~20 ms | **11 ms** | MSD byte-radix (2 passes) |
| sort.perm 1M u32 | 27 ms | **15 ms** | MSD byte-radix (4 passes) |
| sort.perm 1M u64 | — | **14 ms** | MSD byte-radix (8 passes) |
| sort + take 100 from 1M u64 | — | **~20 ms** | bench 6 (top-K via sort/take) |
| GROUP BY 1M u8 keys + sum | 12.87 ms | **7.0 ms** | counting-sort fast path inside `group` (faster than BQN's 9.34 ms) |
| cumsum 1M u64 | — | **~2.3 ms** | new prefix-sum op |
| sessionization, 1M timestamps | — | **~6 ms** | `shift` + `cumsum` idiom (example 53) |

**Group fast path for narrow keys.** `sort_perm_by_key` now detects the
identity-perm + unsigned u8/u16 case and switches to counting/byte-radix.
GROUP BY at N=1M with u8 region key: ~9.3 ms → **~5.4 ms** (1.7× on the
sort step alone).

**Mask-aware cmp + arith (hard cases).** `View<Mask>` vs full-length Prim
(paired walk through mask) and `View<Mask>` vs another `View<Mask>`
(two-cursor walk) now stream without materialization, completing the set
sketched in the BACKLOG.

**E-graph spike (rolled back).** A subsequent session explored
integrating `egg` for declarative rule-based optimization. The arity
API on `PrimOp` (`fn arity() -> Option<(usize, usize)>`) is kept; the
egg dep, module, and rule infrastructure were removed. The `where
gather → filter` fusion is back in the parser peephole. See git log
2026-05-23 for design notes and the barriers we hit (DAG-to-stack
scheduling, op-with-data reconstruction, multi-output ops).

### Action priorities from this bake-off

1. **Predicate fusion (FOLLOWUPS §3)** — single biggest win, one
   concrete pattern (`<cmp> where gather`) recognized at parse time
   → fused IR op. Should bump filter-with-values from ~26× slow to
   ~3× slow.

2. **Arc-1 buffer reuse (sketched in let-binding thread)** — closes
   the kernel-call gap. Should bump add / per-row-sum from 1.7× to
   ~1.1×.

3. **Investigate group-by sort kernel.** Probably radix-sort for narrow
   key types (u8 region keys here) — same approach datatoad uses.
   Probably ~2× on this specific workload.

Combined: 1+2+3 likely closes most of the BQN gap on the workloads BQN
can express. Past that, expressivity (sums, lists, WCO joins) is where
collie differentiates and BQN can't follow.

Sources:
- [CBQN repo](https://github.com/dzaima/CBQN)
- [BQN •_timed documentation](https://github.com/mlochbaum/bencharray/blob/master/measure/time.bqn)
