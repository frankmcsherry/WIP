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
    // two leaf columns -> a 0/1 U64 mask. The op is the leaf compare DDIR's `Condition` needs.
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


/// run a graph of one op on one input.
fn one(op: impl Into<NumOp>, input: Value) -> Result<Value, String> {
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let out = b.add(op, vec![inp]);
    let g = b.finish(out);
    shape_of(&g, &shape_of_value(&input))?;
    Ok(eval_graph(&g, input))
}

fn list(ends: Vec<usize>, vals: Value) -> Value {
    Value::List(ends.into(), Box::new(vals))
}

/// The reductions take any leaf width. The accumulating ones (`add`, `mul`, `all`, `any`) come
/// out at u64, since a sum of bytes is not a byte; the order ones (`min`, `max`) come out at the
/// element's width, since the answer is an element.
#[test]
fn reductions_take_any_width_and_accumulate_at_u64() {
    use corgi::Red;
    let bytes = list(vec![3, 3, 5], Value::u8(vec![200, 100, 7, 9, 3]));
    assert_eq!(one(ArithOp::Reduce(Red::Add), bytes.clone()).unwrap(), u64(&[307, 0, 12]));
    assert_eq!(one(ArithOp::Reduce(Red::Mul), bytes.clone()).unwrap(), u64(&[140000, 1, 27]));
    assert_eq!(one(ArithOp::Reduce(Red::Min), bytes.clone()).unwrap(), Value::u8(vec![7, 255, 3]));
    assert_eq!(one(ArithOp::Reduce(Red::Max), bytes.clone()).unwrap(), Value::u8(vec![200, 0, 9]));
    assert_eq!(one(ArithOp::Reduce(Red::All), bytes.clone()).unwrap(), u64(&[1, 1, 1]));
    // the scans follow the same split: a running sum widens, a running minimum stays a byte.
    assert_eq!(
        one(ArithOp::Scan(Red::Add), bytes.clone()).unwrap(),
        list(vec![3, 3, 5], u64(&[200, 300, 307, 9, 12]))
    );
    assert_eq!(
        one(ArithOp::Scan(Red::Min), bytes).unwrap(),
        list(vec![3, 3, 5], Value::u8(vec![200, 100, 7, 9, 3]))
    );
    // u64 in, u64 out, unchanged.
    let wide = list(vec![2], u64(&[u64::MAX, 2]));
    assert_eq!(one(ArithOp::Reduce(Red::Add), wide.clone()).unwrap(), u64(&[1]));
    assert_eq!(one(ArithOp::Reduce(Red::Max), wide).unwrap(), u64(&[u64::MAX]));
}

/// A NARROWER unsigned operand widens to a cell's declared width: the declared width is the
/// result's. A wider operand does not fit, and a signed or float kind, whose encoding is
/// width-dependent, must already match.
#[test]
fn a_narrower_unsigned_operand_widens_to_the_cell() {
    let pair = |a, b| Value::Prod(vec![a, b]);
    assert_eq!(
        one(ArithOp::Bin(BinOp::Mul, Kind::U, 64), pair(u64(&[7, 9]), Value::u8(vec![1, 0]))).unwrap(),
        u64(&[7, 0])
    );
    assert_eq!(
        one(ArithOp::Bin(BinOp::Add, Kind::U, 32), pair(Value::u8(vec![200]), Value::u16(vec![1000]))).unwrap(),
        Value::u32(vec![1200])
    );
    let err = one(ArithOp::Bin(BinOp::Add, Kind::U, 8), pair(Value::u8(vec![1]), u64(&[1]))).unwrap_err();
    assert!(err.contains("does not fit"), "{err}");
    let err = one(ArithOp::Bin(BinOp::Add, Kind::I, 64), pair(Value::u8(vec![1]), u64(&[1]))).unwrap_err();
    assert!(err.contains("cannot widen"), "{err}");
}

/// A filter mask reads at any leaf width, and `arrange::mask_positions` is the same index list.
#[test]
fn a_mask_reads_at_any_width() {
    let data = list(vec![3, 5], u64(&[10, 20, 30, 40, 50]));
    for mask in [
        list(vec![3, 5], Value::u8(vec![1, 0, 1, 0, 1])),
        list(vec![3, 5], Value::u16(vec![7, 0, 7, 0, 7])),
        list(vec![3, 5], u64(&[1, 0, 1, 0, 1])),
    ] {
        assert_eq!(
            one(Op::Filter, Value::Prod(vec![data.clone(), mask])).unwrap(),
            list(vec![2, 3], u64(&[10, 30, 50]))
        );
    }
    assert_eq!(corgi::arrange::mask_positions(&Value::u8(vec![0, 3, 0, 1])), Some(vec![1, 3]));
    assert_eq!(corgi::arrange::mask_positions(&Value::Unit(2)), None);
}
