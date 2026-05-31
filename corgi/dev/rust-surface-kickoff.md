# A Rust-flavoured front-end — structs, enums, match

A third surface (`frontend/rust.rs`, beside `pipeline`/`ml`, sharing `resolve`/`str_value`) that
reads like Rust: `struct`/`enum` declarations with named fields/variants, struct/enum literals,
`s.field` access, `match`, and `let x = expr;` sequencing. The thesis: **almost all of it is a
naming layer over the positional core** — names resolve to `Field` indices, tag indices, and graph
node ids, and are *erased* before the IR. The core stays anonymous. Exactly one core op is missing.

## Roles (what crosses into the IR, what doesn't)
- **Declarations** (`struct S { a: A, b: B }`, `enum E { Foo(A), Bar(B) }`) — a compile-time table:
  field-name↔index, variant-name↔tag, and the field/variant *shapes*. Never reaches the core.
- **Names** (fields, variants, `let` bindings) — resolved by the front-end; erased at lowering.
- **Core IR** — stays positional. One addition: a sum *introducer* (`Inject`, below).

## Feature → lowering (the naming layer)
| surface | lowers to |
| --- | --- |
| `let x = e; rest` | lower `e` → node id, bind `x`→id, lower `rest` (shared edge — like `ml`'s `Let`) |
| `S { a: e0, b: e1 }` | `tuple([lower e0, lower e1])` **in declared field order** |
| `s.a` | `Field(index_of(a))` |
| `match e { Foo(x)=>b0, Bar(y)=>b1 }` | `MapSum([(0, λx.b0), (1, λy.b1)]); Unwrap` |
| `E::Foo(e)` | `Inject(tag_of Foo, variant_shapes E)` over `lower e` |

`match` is the satisfying case: an arm per variant **is** `MapSum`'s `Vec<(tag, body)>`, and when all
arms return the same type the sum is homogeneous, so `Unwrap` collapses it. **Exhaustiveness is
enforced for free** — a missing arm leaves an unmapped variant at its original type, the sum isn't
homogeneous, and `Unwrap`'s typer rejects it. So `match` = *total* `MapSum`; `if let` = *partial*.
(Depends on the `MapSum` workstream; until it lands, lower to a `MapVariant` chain — same behavior,
less optimizer-legible.)

## The Inject gap — sum introduction
The core has product introduction (`tuple`, the N-ary fan-in) but **no sum introduction**: the only
sum-builder is `Partition` (mask-driven, 2-way, homogeneous). A Rust enum constructor `E::Foo(x)` —
put `x` in variant `tag`, leave the others empty — has no op. Add the unary dual of `tuple`:

```
Op::Inject(usize, Vec<Shape>)   // (tag, all variant shapes) ;  shapes[tag] -> Sum(shapes)
```

- **eval**: input column `X` (n rows) → `Sum` with `tags = [tag; n]`, `variants[tag] = X`, and every
  other `variants[j] =` a **zero-row value of `shapes[j]`**. Needs a small recursive
  `Value::empty(&Shape) -> Value` (empty `Prim::Uw`, empty `Prod` of empties, empty `List`, …).
- **judge**: require `input == shapes[tag]`; emit `Sum(shapes)`. The op carries the other variants'
  shapes because the forward typer can't invent them — same reason `Lit` carries a `Value`. (Full
  vec is mildly redundant with the input shape; harmless, and an enum decl has all shapes anyway.)
- **cost**: O(n + Σ empty shapes) — just the tag run; the payload column is moved, not copied. Cheap,
  like `tuple` (shares columns) and unlike `gather` (copies).

The duality is the interesting part: a **product is "all of"** → `tuple` takes K inputs; a **sum is
"one of"** → `Inject` takes 1 input plus a tag. That K-vs-1 asymmetry is the product/sum duality
itself. Seen together, there are three sum introducers, and only the first two exist:
`Partition` (per-row, data-driven tag), `Inject` (uniform, constant tag), and a general
**N-way partition by an explicit discriminant column** (per-row tag from data) — deferred; composes
from nested `Partition` + `Focus`/`Unfocus`, or becomes its own op if construction-heavy code wants it.

## Steps (each compiles independently)
1. **Core, no surface.** `Value::empty(&Shape)`; `Op::Inject(usize, Vec<Shape>)` (eval/judge/children).
   Unit-test `Inject` directly (build a sum, check shape + eval). Crate compiles; front-end untouched.
2. **Decls + symbol table.** Lexer/parser for `struct`/`enum`; a declaration table (name↔index/tag,
   shapes). No lowering yet — just parse + resolve, tested on the table.
3. **Expressions.** `let;` sequencing, struct/enum literals, `s.field`, `match`. Lower per the table
   above (`match` → `MapSum`+`Unwrap`, or `MapVariant`-chain if `MapSum` hasn't landed).
4. **Tests.** Round-trip a small struct/enum/match program through parse→eval and assert it matches
   the equivalent `pipeline`/`ml` program (the front-ends already agree on a shared `resolve`).

## Deferred
- N-way partition by a discriminant column; sum-merge / per-row re-interleave (the unstateable
  reachability gap from the commutation audit — only if a program actually needs it).
- Recursive types (no `Trees` in core yet), generics, traits, methods beyond field access.
- A kind-checking numeric layer (`i32`/`f32` literals) — shared with the numerics deferral.
