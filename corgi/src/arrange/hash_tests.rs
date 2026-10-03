use crate::value::{Bounds, Value};

/// ONE id function: the `hash` op's column is the `hash` function's ids, wrapped. There
/// were once two folds with different constants that disagreed about whether a `u8 5` and
/// a `u64 5` are the same value — exactly the question the boundary id exists to answer —
/// so this is pinned to keep a second one from quietly reappearing.
#[test]
fn the_hash_op_is_the_hash_function() {
    let shapes = [
        Value::u64(vec![5, 7, 5]),
        Value::Prod(vec![Value::u8(vec![1, 2, 3]), Value::u32(vec![9, 9, 8])]),
        Value::sum(vec![0, 1, 0], vec![Value::u16(vec![4, 6]), Value::u64(vec![7])]),
        Value::List(Bounds::offsets(vec![1, 1, 4]), Box::new(Value::u8(vec![3, 4, 5, 6]))),
        Value::Unit(3),
    ];
    for v in shapes {
        let op = crate::ops::Op::<crate::NumOp>::Hash.eval(v.clone()).unwrap();
        assert_eq!(crate::hash::hash(&v), op.into_u64("hash op").unwrap());
    }
}
