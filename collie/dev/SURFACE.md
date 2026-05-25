# Surface language(s) — design sketch

The IR (today's collie) is the brutalist machine-friendly target. Humans
want named tags, named fields, multi-name bindings, and aggregate-spec
sugar. This document proposes *one possible* surface language — a
general-purpose elaboration to the existing IR. **No IR changes**; any
surface is a parse-time front-end.

The architecture envisions *multiple* such front-ends rather than a
single canonical surface — a relational/SQL-flavored front-end, a
datalog front-end, the array-language form we already have, and
others as workloads call for them. Each elaborates to the same IR.
Going long on a single unified surface would risk a disorderly
language; the IR is what they share.

## Principle

Two sharply distinct layers:

- **IR** (current): uniform primitives, positional everything, stack
  discipline, plug-in ops, easy to typecheck and run. Designed for
  composition and for compiler back-ends.

- **Surface** (proposed): humane syntax that elaborates to the IR. Adds
  multi-name `let`, schema declarations with named-field projection,
  multi-key group, aggregate-spec sugar. Compiles away; nothing
  reaches the typechecker that the typechecker didn't already accept.

The architecture already separates parse from typecheck; this adds an
**`elaborate(surface) -> ir`** pass in between.

```
source  --parse-->  Surface AST  --elaborate-->  IR ops  --typecheck-->  ...
```

Surface and IR forms **mix freely** in the same source file — the
elaborator only rewrites surface forms it recognizes; everything else
passes through. This lets us add surface features one at a time and
migrate examples incrementally.

## Pain points (from examples 48 and 49)

### Pain 1: Binding tower for column construction

Six columns means six nested `{| name | ... }`. Example 48 (lines
42–51):

```col
N iota
{| seed |
   seed 3u64 mod.u64
   seed 3u64 /.u64  2u64 mod.u64
   seed 50u64 mod.u64  1u64 +.u64
   seed 100u64 mod.u64
   seed 1000u64 mod.u64
   seed 365u64 mod.u64
   {| returnflag linestatus quantity price_raw discount shipdate |
      ...
   }
}
```

The bindings work but you can't tell at a glance which expression
produces which name. They're stacked by position, decoded by counting.

### Pain 2: Positional field access

```col
grouped each {
   .{ .0 reduce.+.u64
    ; .1 reduce.+.u64
    ; .2 reduce.+.u64
    }
}
```

`.0` / `.1` / `.2` reads as "first / second / third", not "qty / price /
price_adj". A reader has to look up what was zipped to know what each
index means.

### Pain 3: Multi-key group

```col
returnflag as.u64 2u64 *.u64  linestatus as.u64 +.u64
{| key |
   ...
   key group.u64
   {| uniq_keys grouped |
      ...
      # at the end:
      uniq_keys 2u64 /.u64  as.u8
      uniq_keys 2u64 mod.u64  as.u8
      {| rf_out ls_out | ... }
   }
}
```

Packing two keys into one is mechanical (key = a * cardinality(b) + b).
The pack and unpack add maybe 8 lines of bookkeeping plus an extra
binding pair, none of which says anything about the actual query.

### Pain 4: Cleave-in-each for multi-aggregate

```col
grouped each {
   .{ .0 reduce.+.u64
    ; .1 reduce.+.u64
    ; .2 reduce.+.u64
    }
}
flatten drop detuple.3
```

This is the only sensible way to do "sum each field of a grouped Prod"
today, but the spelling is hostile. Three sub-branches that all do the
same thing on different fields; then `flatten drop detuple.3` to unpack
the resulting length-1-per-row Prod. A reader has to walk through it
piece by piece to see "we're computing three sums."

## Surface v1: minimal additions

Four constructs, all elaborate to existing IR. Each can land
independently.

### 1. Multi-name `let` block

Surface:

```
let name1 = expr1,
    name2 = expr2,
    ...
    nameK = exprK
in body
```

Each `exprI` is a stack-shaped subexpression that pushes one value, and
**later expressions can reference earlier names** (sequential scoping).
The elaborator translates to a chain of single-name `{| ... |}` bindings:

```
LET RULE
  let n1 = E1, …, nK = EK in B
  ↦  E1 ; {| n1 |  E2 ; {| n2 |  …  EK ; {| nK | B } … }}
```

The IR-level nesting depth is the same as today; the surface eliminates
the brace-counting and lets you name each value next to the expression
that produces it.

Example 48 lines 43–51 collapse to:

```
N iota
let seed     = id,
    rf       = seed 3u64 mod.u64,
    ls       = seed 3u64 /.u64  2u64 mod.u64,
    qty      = seed 50u64 mod.u64  1u64 +.u64,
    price_raw = seed 100u64 mod.u64,
    discount = seed 1000u64 mod.u64,
    shipdate = seed 365u64 mod.u64
in
   ...
```

(`id` is the identity op — the first binding captures the value on the
stack with no further computation.)

The first binding pattern (capture the value already on the stack) is
common enough to deserve its own form:

```
LET-WITH RULE
  let n1 from-stack, n2 = E2, … in B
  ↦  {| n1 | E2 ; … ; B' }
```

So the rewritten lines become:

```
N iota
let seed from-stack,
    rf = seed 3u64 mod.u64,
    ls = seed 3u64 /.u64 2u64 mod.u64,
    ...
in body
```

Implementation: ~80 LOC parser change. The `let` block becomes a single
Let op at IR level, identical to what `{| ... |}` produces today.

### 2. Schema declarations + named-field projection

Surface:

```
schema Lineitem {
  returnflag: u8,
  linestatus: u8,
  qty: u64,
  price: u64,
  discount: u64,
  shipdate: u64,
}
```

This is a parse-time declaration: registers `Lineitem` as a Prod shape
with named fields at positions 0..5.

Construction tags an existing Prod as `Lineitem`:

```
returnflag linestatus qty price discount shipdate
zip6 as Lineitem
```

Once tagged, dotted projection elaborates:

```
LINEITEM.QTY RULE  (when Lineitem is in scope)
  expr.qty  ↦  expr .2          # 2 = index of `qty` in Lineitem
```

`.foo` without a schema context falls through unchanged (numeric `.0`
behavior preserved).

Schemas are **erased at elaboration time**. The IR sees plain Prods with
positional fields. The `as Schema` annotation just registers a name → 
field-positions map in the elaborator's scope.

Implementation: ~120 LOC. Parser captures `schema` declarations;
elaborator maintains a schema scope; rewrites `expr.fieldname` to
`expr .i`.

### 3. Multi-key `group.by`

Surface:

```
vals (k1, k2, ...) group.by.u64
```

Elaborates to: pack each key column into a positional u64 hash (based on
its declared cardinality, or via a generic structural hash), call
`group.u64`, return `(uniq_keys_unpacked, grouped_vals)`.

For low-cardinality columns (small int type), packing is just `k1 *
card(k2) * card(k3) + k2 * card(k3) + k3`. For general columns, lex-sort
+ run-length-encode (datatoad-style).

```
GROUP.BY RULE  (when cardinalities known)
  vals (k1, k2) group.by.u64
  ↦  vals
     k1 as.u64 card(k2) *.u64  k2 as.u64 +.u64
     group.u64
     {| uniq_keys grouped |
        uniq_keys card(k2) /.u64 as.<width(k1)>
        uniq_keys card(k2) mod.u64 as.<width(k2)>
        {| k1_out k2_out |  (k1_out, k2_out, grouped) }
     }
```

Cardinalities come from either:
- Explicit annotation: `(rf : 3, ls : 2)`.
- Schema-declared narrow types (e.g. `u8` ⇒ cardinality 256).

When cardinality is unknown, fall back to structural lex-sort grouping.

Implementation: ~80 LOC. Parser recognizes `group.by`; elaborator emits
the pack+unpack chain.

### 4. Aggregate-spec sugar

Surface:

```
grouped aggregate {
  count,
  sum qty,
  sum price,
  sum price_adj,
}
```

(`qty`, `price`, `price_adj` are field names on the grouped row's Prod
type, requiring a schema or positional indices.)

Elaborates to:

```
grouped count                              # per-group count column
grouped each {
  .{ .qty reduce.+.u64
   ; .price reduce.+.u64
   ; .price_adj reduce.+.u64
   }
}
flatten drop detuple.3
{| sum_qty sum_price sum_price_adj counts | (counts, sum_qty, sum_price, sum_price_adj) }
```

Aggregate kinds (initial set): `count`, `sum F`, `max.<i> F`, `min.<i>
F`, `avg.<i> F`. Each maps to a known reducer; `avg` is `sum / count`.

`count` is special-cased to come from list bounds (no per-row reduce).

Implementation: ~120 LOC. Parser recognizes `aggregate { … }`; elaborator
emits the cleave-in-each + post-processing chain.

## Worked example — example 48 in surface form

Before (current IR, lines 43–106 of 48; 64 lines of body):

```col
N iota
{| seed |
   seed 3u64 mod.u64
   seed 3u64 /.u64  2u64 mod.u64
   ...
   {| returnflag linestatus quantity price_raw discount shipdate |
      quantity 100u64 *.u64  price_raw +.u64
      dup  10000u64 discount -.u64  *.u64  10000u64 /.u64
      {| price price_adj |
         shipdate 250u64 <=.u64
         {| ship_mask |
            returnflag as.u64 2u64 *.u64  linestatus as.u64 +.u64
            {| key |
               key quantity price price_adj zip4
               ship_mask where gather
               detuple.4
               {| k qty pr pr_adj |
                  qty pr pr_adj zip3  k group.u64
                  {| uniq_keys grouped |
                     grouped count
                     grouped each {
                        .{ .0 reduce.+.u64
                         ; .1 reduce.+.u64
                         ; .2 reduce.+.u64
                         }
                     }
                     flatten drop detuple.3
                     {| sum_qty sum_pr sum_pr_adj counts |
                        uniq_keys 2u64 /.u64  as.u8
                        uniq_keys 2u64 mod.u64  as.u8
                        {| rf_out ls_out |
                           rf_out ls_out counts sum_qty sum_pr sum_pr_adj entuple.6
                        }
                     }
                  }
               }
            }
         }
      }
   }
}
```

After (surface, ~22 lines of body):

```col
schema Lineitem {
  returnflag: u8, linestatus: u8, qty: u64,
  price: u64, discount: u64, shipdate: u64,
}

N iota
let seed      from-stack,
    rf        = seed 3u64 mod.u64,
    ls        = seed 3u64 /.u64 2u64 mod.u64,
    qty       = seed 50u64 mod.u64 1u64 +.u64,
    price     = qty 100u64 *.u64  seed 100u64 mod.u64 +.u64,
    discount  = seed 1000u64 mod.u64,
    shipdate  = seed 365u64 mod.u64,
    price_adj = price 10000u64 discount -.u64 *.u64 10000u64 /.u64,
    lineitem  = rf ls qty price discount shipdate zip6 as Lineitem,
    kept      = lineitem  lineitem.shipdate 250u64 <=.u64  where gather
in
   kept (kept.returnflag : 3, kept.linestatus : 2) group.by.u64
   aggregate {
     count,
     sum qty,
     sum price,
     sum price_adj,
   }
```

A single `let ... in` block holds the whole data declaration + filter;
the query body is three lines. The `as Lineitem` is the *only*
introduction of named structure; everything else follows from it
(`.shipdate`, `.qty`, `.returnflag`).

Two surface conventions are at work:

1. `let n from-stack` captures whatever's already on the stack. Used
   here for `seed` (= `N iota`'s result) and for `aggregate`'s implicit
   input (the grouped value from `group.by.u64`).
2. `(field1 : card1, field2 : card2)` annotates a tuple of grouping
   keys with cardinalities so `group.by.u64` can pack them without
   needing to lex-sort.

## What stays in the IR (unchanged)

- The closed inductive value universe (Prim, Prod, Sum, List, View).
- Width-tagged primitives; interpretation in operators.
- Stack discipline, `eval(prog, stack)`.
- Positional `match` against lane indices.
- `{| ... |}` let-bindings (the elaboration target for `let … in`).
- All existing ops, unchanged.

## What's deliberately NOT in v1

- **Expression-style arith** (`a + b` infix). Big lift — separate parser
  precedence layer. Could land as v2.
- **Named lanes for Sums** (`'ok` / `'err` style). Needs more thought
  about how schemas register variant names. Defer.
- **Structural destructuring in let-bindings** (`let (x, y, z) = expr`).
  Useful but secondary. Defer.
- **`under .field { … }`** for in-place updates. The lens sublanguage.
  Defer.
- **First-class procedures** (callable values). Defer indefinitely —
  parse-time `def` covers the use cases.

## Implementation estimate

Total: ~400 LOC, one module `src/syntax/elaborate.rs` plus parser
additions. Per feature:

| Feature | LOC | Independent? |
|---|---|---|
| Multi-name `let` block | ~80 | Yes (no schema dependency) |
| `schema` + named-field projection | ~120 | Yes (no aggregate dependency) |
| `group.by` (multi-key group) | ~80 | Needs schemas for cardinality inference; standalone with explicit annotations |
| `aggregate { ... }` sugar | ~120 | Needs schemas for field names; standalone with positional `aggregate { sum .0, sum .1 }` |

Land in order: let-block (cheapest, biggest readability win) → schemas
(unlocks the rest) → aggregate sugar → group.by.

## What this is *not* solving

- Performance. Elaboration is parse-time; the emitted IR is identical to
  what a human would write today. No speedups; no regressions.
- Verification. The IR's typechecker still runs after elaboration;
  surface annotations don't add proofs.
- Programmatic emission. The surface is for humans. A SQL→collie
  compiler can target either layer, and probably should target the IR
  directly (less surface to track).

## Next step

Land multi-name `let` first. It's the most-used surface feature (every
example would benefit), it has no dependencies, and the elaborator
mechanism it requires generalizes to the others.
