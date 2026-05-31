//! The numeric layer: signed arithmetic built over the core, and proof that the
//! core's (kind-blind) sort/shape machinery serves it unchanged.

use corgi::{
    dec_i64, enc_i64, eval_graph, shape_of, shape_of_value, ArithOp, BinOp, Builder, CmpOp, Kind,
    NumOp, Op, Pred, Shape, Value,
};

/// a leaf column of signed integers, stored order-preserving.
fn i64col(xs: &[i64]) -> Value {
    Value::u64(xs.iter().map(|&x| enc_i64(x)).collect())
}

fn u64(xs: &[u64]) -> Value {
    Value::u64(xs.to_vec())
}

fn dec_col(v: Value) -> Vec<i64> {
    v.into_u64("dec_col").iter().map(|&u| dec_i64(u)).collect()
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
    let input = Value::List(vec![4], Box::new(i64col(&[5, -3, 10, -8])));
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
