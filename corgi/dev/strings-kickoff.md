# Producer + strings — scope (step 5a/5b)

The width-ready core (steps 1–4) can store/sort/group/gather/compare any-width leaves, but
nothing in the *language* mints a narrow width. This adds the two surface producers — a
numeric width-cast and string literals — plus the rendering to make strings usable. It does
NOT add per-width arithmetic or numeric kinds (the deferred grid).

## What "for free" already works
A string is `List<U8>` (a list whose leaf is a `u8` column — compact, 1-aligned). The
width-generic core already sorts/groups/dedups/finds `List<U8>` and lists-of-strings, and
`Op::Lit` already broadcasts a `List` value via `fill`→`gather`. So once strings can be
*produced* and *rendered*, sort/search/group come along at no extra cost.

**Decided to ignore:** string order is length-first (shorter first), not dictionary — a
consequence of the length-first list decision. Not what the language is about; left as is.

## Piece A — numeric producer: a `Cast(bits)` core op
Re-widths a leaf at the BYTE level, kind-blind: narrowing keeps the low bytes
(`x as u8`), widening zero-pads (`x as u64`). So `lit 300 | cast 8` yields a `u8` column.
- core op `Op::Cast(u32)`; eval re-widths the `Prim`; judge `Prim(_) => Prim(target)`.
- front-end keyword `cast N` (shared `resolve`, takes a numeric arg).
- Signed/sign-extending widen is a numeric-layer concern (deferred); core `Cast` is unsigned/byte.

## Piece B — string literals → `List<U8>`
- lexer: add a `"…"` token to both front-ends; lower to `Op::Lit(Value::List([len], u8(bytes)))`.
  (Both surfaces, for parity — small per-lexer addition.)
- `Lit` already fills a `List` value to the input's length, so a string literal broadcasts
  like any constant — no new eval path.

## Piece C — rendering: DECIDED, keep bytes
`show` keeps rendering `List<U8>` as byte-numbers. Text rendering isn't what the language is
about — no kind-leak into the core, no front-end render path. Strings print as their bytes.

## Steps (each compiles + tests green)
1. `Cast(bits)` core op + `cast N` keyword + judge; test narrowing a u64 and sorting it.
2. String-literal lexing in both front-ends → `Op::Lit(List<U8>)`; test parse + eval.
3. `show` renders `List<U8>` as text (decision C); test `"hello"` round-trips, and a
   list-of-strings sorts (length-first) and `find`s.
4. A tour demo: a column of strings, sorted and searched.

## Deferred (explicitly out)
- `split(delim): List<U8> -> List<List<U8>>` and other string ops (the JSONL-grind feature;
  field-by-key already reduces to `find`+`slices`).
- Dictionary (lexicographic) string order.
- Per-width arithmetic and numeric kinds (signed/float) — the `(op × kind × width)` grid.
