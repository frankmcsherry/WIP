use super::*;
use crate::value::Bounds;

fn eval(op: ArithOp, input: Value) -> Prim {
    op.eval(input).unwrap().into_prim("test").unwrap()
}

/// Each integer op computes at the storage its operands plan (by their values, for a narrow leaf)
/// and stores its results where they fit; the results are the integers they always were.
#[test]
fn arithmetic_is_planned_from_its_operands() {
    use Storage::*;
    let pair = |a: Value, b: Value| Value::Prod(vec![a, b]);
    let add = |a, b| eval(ArithOp::Bin(BinOp::Add), pair(a, b));
    // a sum of bytes that stays within a byte is bytes; one past it is i16
    let r = add(Value::u8(vec![1, 100]), Value::u8(vec![2, 100]));
    assert_eq!((r.storage(), r.ints()), (Some(U8), vec![3, 200]));
    let r = add(Value::u8(vec![1, 200]), Value::u8(vec![2, 100]));
    assert_eq!((r.storage(), r.ints()), (Some(I16), vec![3, 300]));
    // a difference that goes below zero
    let r = eval(ArithOp::BinImm(BinOp::Sub, Scalar::Int(48)), Value::u8(vec![48, 57, 32]));
    assert_eq!((r.storage(), r.ints()), (Some(I8), vec![0, 9, -16]));
    // the least i8 over -1 is 128
    let r = eval(ArithOp::Bin(BinOp::Div), pair(Value::i8(vec![-128, 7]), Value::i8(vec![-1, 2])));
    assert_eq!((r.storage(), r.ints()), (Some(I16), vec![128, 3]));
    // a remainder by a constant is smaller than it, from any storage
    let r = eval(ArithOp::BinImm(BinOp::Rem, Scalar::Int(10)), Value::i64(vec![i64::MIN, 12345, -7]));
    assert_eq!((r.storage(), r.ints()), (Some(I8), vec![-8, 5, -7]));
    // `and` with a byte is a byte, even of an i64
    let r = eval(ArithOp::BitsImm(BitOp::And, 255), Value::i64(vec![-1, 1 << 40, 300]));
    assert_eq!((r.storage(), r.ints()), (Some(U8), vec![255, 0, 44]));
    // a sum with an i64 is an i64, and wraps
    let r = add(Value::i64(vec![i64::MAX]), Value::u8(vec![1]));
    assert_eq!((r.storage(), r.ints()), (Some(I64), vec![i64::MIN]));
    // negating the least i8
    let r = eval(ArithOp::Neg, Value::i8(vec![-128, 5]));
    assert_eq!((r.storage(), r.ints()), (Some(I16), vec![128, -5]));
    // a row's sum is planned from the longest row
    let rows = Value::List(Bounds::offsets(vec![2, 3]), Box::new(Value::u8(vec![200, 200, 7])));
    let r = eval(ArithOp::Reduce(Red::Add), rows);
    assert_eq!((r.storage(), r.ints()), (Some(I16), vec![400, 7]));
}
