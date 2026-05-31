# Numerics — the (op × kind × width) grid

Generalize the u64-only arithmetic to a macro-generated grid, the way `prim!` did for the
leaf. Keep it tight: list the widths ONCE, enumerate (op, kind) explicitly, and the macro
expands the width dispatch — no hand-written 504-line sea.

## Representation: parameterize, don't enumerate variants
`ArithOp` carries the cell, not a variant per cell:
```
enum BinOp { Add, Sub, Mul }
enum Kind  { U, I }                 // unsigned (native); signed (order-preserving swizzle)
enum ArithOp {
    Bin(BinOp, Kind, u32),          // binary leaf arith at a bit-width
    Neg(Kind, u32),                 // unary negate
    AddU64(u64),                    // kept: add-a-u64-constant sugar
    ReduceSum,                      // kept: List<U64> -> U64 (unsigned reduce)
}
```

## Kinds = how the bytes are interpreted (all stored as `Prim::Uw`)
- **U**: the `uN` bytes ARE the value. native `wrapping_{add,sub,mul}`.
- **I**: the `uN` bytes are a signed value, order-preserving **swizzled** (XOR the sign bit —
  the per-width generalization of `enc_i64`). Arith = deswizzle → native `iN` op → reswizzle.
- **F** (deferred): bit-reinterpret `uN`↔`fN`. Float ordering (NaN) is its own story; skip.

So the leaf is always `Prim::Uw`; signed/float are interpretations, exactly as the core's
"numeric-blind" principle wants — only this layer knows.

## The macro (mirrors `prim!`, plus a kind knob)
One `grid!` over the widths generates `bin_eval`/`neg_eval`; a tiny `wrap!` does the
op-match so no `num_traits` dependency:
```
grid!{ U8 => u8:i8, U16 => u16:i16, U32 => u32:i32, U64 => u64:i64 }
```
The swizzle is the top bit `!(<$u>::MAX >> 1)`, computed per width inside the macro — no
`enc_i64`-per-width to hand-write.

## What gets populated
- Binary: {Add, Sub, Mul} × {U, I} × {8,16,32,64} = 24 cells. Unary Neg × {U,I} × 4 = 8.
- All via the macro — adding a width is one line in `grid!`.
- `enc_i64`/`dec_i64` stay public (tests build signed columns with them); the grid's swizzle
  is the same trick generalized.

## Surface exposure (kind-blind front-end, so: the U64 slice)
The front-end is kind-blind, so it reaches only the u64-unsigned row by keyword:
`add`/`sub`/`mul` → `Bin(_, U, 64)`. The signed and narrow cells are reachable from the
`Builder` (and the eventual kind-checking front-end, still deferred). No 24-keyword sprawl.

## Steps
1. `into_prim` accessor on `Value`.
2. numeric.rs: the enums, the `wrap!`/`grid!` macros, `eval`/`judge` dispatch; keep
   `AddU64`/`ReduceSum`/`enc_i64`/`dec_i64`.
3. export `BinOp`/`Kind`; `add`/`sub`/`mul` keywords → `Bin(_, U, 64)`.
4. update `demos.rs`/`tests/numeric.rs` off the old `Add`/`SubI64` names; add grid tests
   (narrow add, signed sub/mul, neg) verifying native wrap and order-preserving signedness.

## Deferred
- Float kind (bit-reinterpret + NaN order). Per-width/kind `ReduceSum`, `AddU64`.
- The kind-checking numeric front-end (where `i32`/`f32` literals live) — reaches the whole grid.
