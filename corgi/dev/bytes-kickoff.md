# Width-aligned leaves — migration kickoff

## Goal (the real driver: ALIGNMENT, not numeric types)
Give each leaf its natural storage and **alignment**: text/bytes = `Vec<u8>` (1-aligned),
8-byte values = `Vec<u64>` (8-aligned), etc. A typed `Vec<uN>` column *is* a native `&[uN]`
— aligned loads, SIMD over lanes. This is what width-distinguished leaves buy. Also unlocks
narrow ints and strings (`List<U8>`); numeric *kinds* (signed/float) ride on top via a swizzle.

## Decision: B (per-width native), NOT A (flat bytes)
- **B — `Prim { U8(Vec<u8>), U16(Vec<u16>), U32(Vec<u32>), U64(Vec<u64>) }`** nested under
  `Value::Leaf(Prim)`. Each width keeps its naturally-aligned `Vec<uN>`. Compare/gather/concat/
  fill dispatch on width (macro over the four), native moves and native compares. Order is
  native-per-width; signed/float order is a **swizzle** in the numeric layer — which corgi
  already does (`enc_i64` = xor the sign bit). Unsigned is native end-to-end.
- **A — flat `Bytes{width, Vec<u8>}` + byte-lex + big-endian** — REJECTED. Byte-lex would
  force *unaligned* reads of every wide value (the buffer is 1-aligned), block `&[u64]` views
  and SIMD, and require encoding every number to big-endian. A's only win (one comparator) is
  not worth losing alignment, which is the entire point.

## Roles (who owns what, after)
- **core (`value`/`engine`/`cmp`/`graph`):** leaves are typed `Vec<uN>`. gather/concat/fill =
  per-width native moves; compare2/discrimination = per-width native compare; radix later.
  Structural arms (Prod/Sum/List) are UNCHANGED — only the leaf arm gains width-dispatch.
- **`shape`:** `Shape::Leaf(width)`; `judge` propagates width by equality.
- **numeric layer (`ops/numeric`):** native arith per width; swizzle (`enc_i64`-style) for
  signed/float order. The only place that knows a leaf is a *number*.
- **front-ends + `show`:** `lit`/`gt` pick a width; `show` formats per width.

## Current code → migration map (thread width through; do NOT re-encode storage)
- `value.rs`: `Value::U64(Vec<u64>)` → `Value::Leaf(Prim)`; `len()` per width; record accessors.
- `engine.rs`: `fill`/`gather`/`concat` leaf arms → macro over widths (native moves).
- `cmp.rs`: compare2 leaf arm + discrimination leaf sort → per-width native (macro); the order
  is native, so signed stays on the existing `enc_i64` swizzle.
- `ops/core.rs`: `Op::Lit` carries a width; `GtU64` → per-width (or generic) compare; mask
  accessors are a width (1 or 8).
- `ops/numeric.rs`: arith per width (native); `enc_i64` is the signed swizzle, generalize per width.
- `shape.rs`: `Shape::U64` → `Shape::Leaf(width)`.
- front-ends: `lit N` / `gt N` choose a width.

## Steps (each compiles + 47 tests green)
1. **Encapsulate the leaf.** Route every `Value::U64(..)` construction/match through
   constructors/accessors. No behavior change — shrinks step 2's blast radius.
2. **Introduce `Prim`, wire only U64.** `Value::Leaf(Prim::U64(Vec<u64>))` replaces
   `Value::U64`. Leaf ops match `Leaf(p)` then dispatch on `p` (only U64 populated). Behavior
   identical, all green. THE structural step.
3. **Shape carries width.** `Shape::Leaf(8)`; `judge` propagates. Still width 8.
4. **Per-width leaf ops via macro.** gather/concat/fill/compare across U8/U16/U32/U64; radix
   later. Still only U64 produced.
5. **Producers + kinds.** ops/front-end to make narrow widths; strings = `List<Leaf(U8)>`;
   generalize the signed/float swizzle per width.

## Settled: `Value::Prim(Prim)`, type-level width
`Prim = U8(Vec<u8>) | U16(Vec<u16>) | U32(Vec<u32>) | U64(Vec<u64>)`, nested under one
`Value::Prim` variant — keeps the Value-level Prod/Sum/List recursion untouched (only the
leaf arm dispatches width), each width naturally aligned (`Vec<u64>` 8-aligned, `Vec<u8>`
1-aligned), native access and native wrap.

Weighed and rejected: **runtime width** (one `Vec<u64>` + a `width` field, `bytemuck`-cast to
`&[uN]`). It's real — the u64 backing is 8-aligned so the casts are native slices, text stays
compact, no shift/mask. But the width must be threaded through every op AND kept consistent
with the data by hand (a width/data mismatch is a runtime bug the type can't catch), and every
output must be allocated u64-backed. The `Prim` enum is the same runtime branch with the
bookkeeping moved into the type, which enforces it — and needs no casts. Its only loss is
uniform buffer ownership (one `Arc<Vec<u64>>` vs per-width `Vec<uN>`), a macro-able cost.

## Open questions
- `GtU64`: per-width compare-to-constant, or one generic byte-compare? (Lean: per-width, mirrors arith.)
- Where the swizzle lives: numeric layer only, applied around sort/compare (collie's
  `enswizzle … sort … deswizzle`) vs stored swizzled. (Lean: numeric layer, around the boundary.)
