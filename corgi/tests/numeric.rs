//! The numeric layer: signed arithmetic built over the core, and proof that the
//! core's (kind-blind) sort/shape machinery serves it unchanged.

use corgi::{
    dec_i64, enc_i64, eval_graph, parse_ml, shape_of, shape_of_value, ArithOp, BinOp, Builder, CmpOp,
    Kind, NumOp, Op, Pred, Shape, Value,
};

/// a leaf column of signed integers, stored order-preserving.
fn i64col(xs: &[i64]) -> Value {
    Value::u64(xs.iter().map(|&x| enc_i64(x)).collect())
}

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

fn dec_col(v: Value) -> Vec<i64> {
    v.into_u64("dec_col").unwrap().iter().map(|&u| dec_i64(u)).collect()
}

#[test]
fn signed_subtraction_mixes_core_and_arith() {
    // (a - b) over two signed columns: Field/Tuple are core ops, signed Sub is the layer's.
    let input = Value::Prod(vec![i64col(&[5, -3, 10]), i64col(&[2, 4, -1])]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let a = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let bb = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![a, bb]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Sub, Kind::I, 64)), vec![pair]);
    let g = b.finish(out);
    g.check();
    // the SAME shape-checker types it — Arith judges as a plain leaf op, kind-blind:
    assert_eq!(shape_of(&g, &shape_of_value(&input)).unwrap(), Shape::Prim(64));
    assert_eq!(dec_col(eval_graph(&g, input)), vec![3, -7, 11]);
}

#[test]
fn core_sort_orders_signed_values() {
    // the headline: SortList is a kind-blind CMP op (u64/byte order). Because the layer
    // encoded the integers order-preserving, the sort comes out in *signed* order.
    let input = Value::List(vec![4].into(), Box::new(i64col(&[5, -3, 10, -8])));
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(NumOp::Cmp(CmpOp::SortList), vec![inp]);
    let g = b.finish(out);
    let result = eval_graph(&g, input);
    let inner = match result {
        Value::List(_, v) => *v,
        _ => panic!("expected a list"),
    };
    assert_eq!(dec_col(inner), vec![-8, -3, 5, 10]); // signed order, not raw u64 order
}

#[test]
fn negate() {
    let input = i64col(&[5, -3, 0]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(NumOp::Arith(ArithOp::Neg(Kind::I, 64)), vec![inp]);
    let g = b.finish(out);
    assert_eq!(dec_col(eval_graph(&g, input)), vec![-5, 3, 0]);
}

#[test]
fn arith_shape_errors_are_caught() {
    // signed subtraction on a non-pair is a shape error, via the core machinery.
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Sub, Kind::I, 64)), vec![inp]);
    let g = b.finish(out);
    assert!(shape_of(&g, &Shape::Prim(64)).is_err());
}

#[test]
fn relational_compare_to_mask() {
    // two leaf columns -> a 0/1 U64 mask. The op is the leaf compare DDIR's `Condition` needs, and
    // its result is a VALUE in the host's world, not just a control mask — see `CmpOp::Rel`.
    let rel = |pred| {
        let mut b = Builder::<NumOp>::default();
        let inp = b.input();
        let out = b.add(CmpOp::Rel(pred), vec![inp]);
        b.finish(out)
    };
    let pair = |a, b| Value::Prod(vec![a, b]);

    // unsigned: 1<2, 5<5 (no), 3<1 (no)
    assert_eq!(eval_graph(&rel(Pred::Lt), pair(u64(&[1, 5, 3]), u64(&[2, 5, 1]))), u64(&[1, 0, 0]));
    // equality / ge over the same columns
    assert_eq!(eval_graph(&rel(Pred::Ge), pair(u64(&[1, 5, 3]), u64(&[2, 5, 1]))), u64(&[0, 1, 1]));

    // kind-blind: i64 columns stored order-preserving compare by VALUE under a plain (unsigned) lane
    // compare — -3 < 1 holds, 2 < -5 does not — exactly as for SortList.
    assert_eq!(eval_graph(&rel(Pred::Lt), pair(i64col(&[-3, 2]), i64col(&[1, -5]))), u64(&[1, 0]));
}

/// run a binary grid cell on two leaf columns.
fn bin(op: BinOp, kind: Kind, w: u32, a: Value, b: Value) -> Value {
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(ArithOp::Bin(op, kind, w), vec![inp]);
    eval_graph(&bld.finish(out), Value::Prod(vec![a, b]))
}

#[test]
fn grid_unsigned_narrow_wraps() {
    // u8 add wraps at 256 (300 -> 44); u16 mul fits (300*200 = 60000).
    assert_eq!(bin(BinOp::Add, Kind::U, 8, Value::u8(vec![200]), Value::u8(vec![100])), Value::u8(vec![44]));
    assert_eq!(bin(BinOp::Mul, Kind::U, 16, Value::u16(vec![300]), Value::u16(vec![200])), Value::u16(vec![60000]));
}

#[test]
fn grid_signed_is_order_preserving_at_any_width() {
    // i16 stored order-preserving: enc(v) = (v as u16) ^ 0x8000 (the grid's per-width swizzle).
    let enc = |v: i16| (v as u16) ^ 0x8000;
    // (a - b): [-5 - 3, 10 - 20] = [-8, -10]
    assert_eq!(
        bin(BinOp::Sub, Kind::I, 16, Value::u16(vec![enc(-5), enc(10)]), Value::u16(vec![enc(3), enc(20)])),
        Value::u16(vec![enc(-8), enc(-10)])
    );
    // unary signed neg
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(ArithOp::Neg(Kind::I, 16), vec![inp]);
    assert_eq!(eval_graph(&bld.finish(out), Value::u16(vec![enc(7), enc(-3)])), Value::u16(vec![enc(-7), enc(3)]));
}

#[test]
fn no_float_literal_token() {
    // `lit_f32 N` would store the raw bits N, not the float N.0 — `lit_value` only encodes integers.
    // So a float-literal token is rejected (unknown op); the float path is `lit_uN K to_fN`.
    assert!(parse_ml("input lit_f32 3").is_err());
    assert!(parse_ml("input lit_f64 3").is_err());
    // the integer literal tokens still resolve, and the documented float path parses.
    assert!(parse_ml("input lit_u32 3").is_ok());
    assert!(parse_ml("input lit_i16 3").is_ok());
    assert!(parse_ml("input lit_u32 3 to_f32").is_ok());
}

/// `Rem` on the unsigned row, including the total `x % 0 = x`. The zero divisor is
/// deliberately defined rather than rejected: a caller that already knows the modulus is
/// positive (DDIR's `hash(bound, ..)` guards `bound > 0`) should not pay for a branch, and
/// "no reduction" is the only reading of a zero modulus that keeps the op total.
#[test]
fn unsigned_rem_is_total_at_a_zero_divisor() {
    let input = Value::Prod(vec![u64(&[17, 100, 7, 42]), u64(&[5, 97, 7, 0])]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let x = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let y = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![x, y]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Rem, Kind::U, 64)), vec![pair]);
    let g = b.finish(out);
    g.check();
    assert_eq!(shape_of(&g, &shape_of_value(&input)).unwrap(), Shape::Prim(64));
    assert_eq!(eval_graph(&g, input).into_u64("rem").unwrap(), vec![2, 3, 0, 42]);
}

/// `Rem` on the signed row: the sign follows the DIVIDEND (Rust's `%`), the operands are
/// read through the order-preserving encoding, and `i64::MIN % -1` does not overflow.
#[test]
fn signed_rem_follows_the_dividend() {
    let input = Value::Prod(vec![i64col(&[-17, 17, -17, i64::MIN]), i64col(&[5, -5, -5, -1])]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let x = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let y = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![x, y]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Rem, Kind::I, 64)), vec![pair]);
    let g = b.finish(out);
    g.check();
    assert_eq!(dec_col(eval_graph(&g, input)), vec![-2, 2, -2, 0]);
}

/// The signed zero divisor is the encoded zero, not the raw-bit zero — a `Kind::I` lane
/// stores 0 as the flipped sign bit, so a naive `y == 0` test would miss it and divide.
#[test]
fn signed_rem_is_total_at_a_zero_divisor() {
    let input = Value::Prod(vec![i64col(&[-17, 9]), i64col(&[0, 0])]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let x = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let y = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![x, y]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Rem, Kind::I, 64)), vec![pair]);
    let g = b.finish(out);
    g.check();
    assert_eq!(dec_col(eval_graph(&g, input)), vec![-17, 9]);
}

/// The judge rejects a float `Rem` (integer-only), the mirror of its integer-`Div` rejection.
#[test]
fn float_rem_is_rejected() {
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let x = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let y = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![x, y]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Rem, Kind::F, 64)), vec![pair]);
    let g = b.finish(out);
    let shape = Shape::Prod(vec![Shape::Prim(64), Shape::Prim(64)]);
    assert!(shape_of(&g, &shape).is_err(), "float Rem must not type");
}

/// run an immediate grid cell on one leaf column.
fn bin_imm(op: BinOp, kind: Kind, w: u32, a: Value, c: u64) -> Value {
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(ArithOp::BinImm(op, kind, w, c), vec![inp]);
    eval_graph(&bld.finish(out), a)
}

/// the shape error an immediate cell reports — `shape_of` is `eval` on a zero-row column, so a
/// rejected (op, kind, width) surfaces there exactly as it would at any length.
fn bin_imm_err(op: BinOp, kind: Kind, w: u32, shape: Shape, c: u64) -> String {
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(ArithOp::BinImm(op, kind, w, c), vec![inp]);
    shape_of(&bld.finish(out), &shape).unwrap_err()
}

/// A column of `n` copies of the constant, at the given width — the operand the PAIR form has to
/// build before it can start, and which the immediate cell exists to not build.
fn broadcast(w: u32, c: u64, n: usize) -> Value {
    match w {
        8 => Value::u8(vec![c as u8; n]),
        16 => Value::u16(vec![c as u16; n]),
        32 => Value::u32(vec![c as u32; n]),
        _ => Value::u64(vec![c; n]),
    }
}

fn col(w: u32, xs: &[u64]) -> Value {
    match w {
        8 => Value::u8(xs.iter().map(|&x| x as u8).collect()),
        16 => Value::u16(xs.iter().map(|&x| x as u16).collect()),
        32 => Value::u32(xs.iter().map(|&x| x as u32).collect()),
        _ => Value::u64(xs.to_vec()),
    }
}

/// The immediate cell must agree with the pair form EXACTLY, at every (op, kind, width) the grid
/// defines — it is the same cell reached without broadcasting the constant, not a second definition
/// of the arithmetic.
#[test]
fn every_immediate_cell_agrees_with_the_pair_form() {
    let xs = [0u64, 1, 2, 7, 100, 127];
    for w in [8u32, 16, 32, 64] {
        for kind in [Kind::U, Kind::I] {
            for op in [BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Rem] {
                for c in [0u64, 1, 3, 100] {
                    let a = col(w, &xs);
                    let want = bin(op, kind, w, a.clone(), broadcast(w, c, xs.len()));
                    assert_eq!(bin_imm(op, kind, w, a, c), want, "{w} {c}");
                }
            }
            if matches!(kind, Kind::U) {
                for op in [BinOp::Shl, BinOp::Shr, BinOp::And, BinOp::Or, BinOp::Xor] {
                    for c in [0u64, 1, 3, 7] {
                        let a = col(w, &xs);
                        let want = bin(op, kind, w, a.clone(), broadcast(w, c, xs.len()));
                        assert_eq!(bin_imm(op, kind, w, a, c), want, "{w} {c}");
                    }
                }
            }
        }
    }
    // float, at the two widths it has: the immediate takes the constant's STORED (total-order
    // encoded) form, so it is the same bits the pair form's `lit` column would hold.
    for w in [32u32, 64] {
        let enc = |v: f64| match w {
            32 => {
                let b = (v as f32).to_bits();
                (if b >> 31 == 1 { !b } else { b ^ (1 << 31) }) as u64
            }
            _ => {
                let b = v.to_bits();
                if b >> 63 == 1 { !b } else { b ^ (1 << 63) }
            }
        };
        let a = col(w, &[enc(1.0), enc(-2.5), enc(0.0)]);
        for c in [enc(2.0), enc(-0.5)] {
            let want = bin(BinOp::Add, Kind::F, w, a.clone(), broadcast(w, c, 3));
            assert_eq!(bin_imm(BinOp::Add, Kind::F, w, a.clone(), c), want, "f{w}");
        }
    }
}

/// The bitwise cells read STORED bytes, so they are unsigned-only: on a signed or float leaf the
/// bytes are an order-preserving swizzle and a bit op on them is not the bit op on the value. Both
/// forms have to refuse, and for the same reason — they share one check.
#[test]
fn bitwise_is_unsigned_only_in_both_forms() {
    for op in [BinOp::Shl, BinOp::Shr, BinOp::And, BinOp::Or, BinOp::Xor] {
        for kind in [Kind::I, Kind::F] {
            let err = bin_imm_err(op, kind, 64, Shape::Prim(64), 1);
            assert!(err.contains("unsigned-only"), "{err}");
        }
    }
    // ...and they work, column by column, on an unsigned leaf — which nothing could say before:
    // `Shr`/`And` existed only against a constant, and `Shl`/`Or`/`Xor` did not exist at all.
    assert_eq!(
        bin(BinOp::Xor, Kind::U, 8, Value::u8(vec![0b1100, 0b1010]), Value::u8(vec![0b1010, 0b0110])),
        Value::u8(vec![0b0110, 0b1100])
    );
    assert_eq!(
        bin(BinOp::Shl, Kind::U, 16, Value::u16(vec![1, 3]), Value::u16(vec![4, 2])),
        Value::u16(vec![16, 12])
    );
}

/// Shifts are TOTAL: an amount at or past the width wraps modulo the width rather than panicking,
/// the same discipline `Rem` follows on a zero divisor.
#[test]
fn shifts_are_total() {
    assert_eq!(bin_imm(BinOp::Shr, Kind::U, 8, Value::u8(vec![255]), 8), Value::u8(vec![255]));
    assert_eq!(bin_imm(BinOp::Shl, Kind::U, 64, u64(&[1]), 64), u64(&[1]));
}

/// `RelImm` compares against the leaf's STORED form, so a constant that does not fit the width is a
/// shape error rather than a silent truncation to a different value.
#[test]
fn relational_immediate_rejects_a_constant_it_cannot_hold() {
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(CmpOp::RelImm(Pred::Gt, 300), vec![inp]);
    let g = bld.finish(out);
    assert!(shape_of(&g, &Shape::Prim(8)).unwrap_err().contains("does not fit"));
    // at a width that holds it, every predicate answers as the pair form would.
    for (pred, want) in [
        (Pred::Gt, vec![0u64, 0, 1]),
        (Pred::Lt, vec![1, 0, 0]),
        (Pred::Eq, vec![0, 1, 0]),
        (Pred::Ne, vec![1, 0, 1]),
        (Pred::Le, vec![1, 1, 0]),
        (Pred::Ge, vec![0, 1, 1]),
    ] {
        let mut bld = Builder::<NumOp>::default();
        let inp = bld.input();
        let out = bld.add(CmpOp::RelImm(pred, 300), vec![inp]);
        let g = bld.finish(out);
        assert_eq!(eval_graph(&g, Value::u16(vec![7, 300, 900])), u64(&want), "{pred:?}");
    }
}

/// The surface's immediate stages reach the grid's immediate cells, and mean exactly what the
/// lit-pair spelling means.
#[test]
fn surface_immediates_match_the_pair_spelling() {
    let src = Value::u64(vec![0, 1, 5, 40]);
    for (imm, pair) in [
        ("input mul 3", "let x = input in (x, x lit 3) mul"),
        ("input sub 1", "let x = input in (x, x lit 1) sub"),
        ("input add 7", "let x = input in (x, x lit 7) add"),
        ("input rem 3", "let x = input in (x, x lit 3) rem"),
        ("input lt 5", "let x = input in (x, x lit 5) lt"),
        ("input eq 5", "let x = input in (x, x lit 5) eq"),
        ("input min 5", "let x = input in (x, x lit 5) min"),
        ("input shr 1", "let x = input in (x, x lit 1) shr"),
        ("input and 3", "let x = input in (x, x lit 3) and"),
        ("input xor 3", "let x = input in (x, x lit 3) xor"),
    ] {
        let a = eval_graph(&parse_ml(imm).unwrap(), src.clone());
        let b = eval_graph(&parse_ml(pair).unwrap(), src.clone());
        assert_eq!(a, b, "{imm}");
    }
}
