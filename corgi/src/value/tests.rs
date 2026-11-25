use super::*;

/// An integer is the same value whatever storage holds it: a byte leaf and an `i64` leaf of the
/// same integers are equal and hash alike, and a float leaf is never equal to an integer one.
#[test]
fn integers_are_equal_across_storages() {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let h = |v: &Value| {
        let mut s = DefaultHasher::new();
        v.hash(&mut s);
        s.finish()
    };
    let (bytes, wide) = (Value::u8(vec![0, 7, 255]), Value::i64(vec![0, 7, 255]));
    assert_eq!(bytes, wide);
    assert_eq!(h(&bytes), h(&wide));
    assert_ne!(Value::u8(vec![1]), Value::i64(vec![-255]));
    assert_ne!(Value::i64(vec![0]), Value::f64(vec![0.0]));
}

/// `Value::int` holds integers at the narrowest storage that holds every one (an empty column as
/// bytes), and each is the value `Value::i64` of the same integers is.
#[test]
fn int_picks_the_narrowest_storage() {
    let width = |v: &Value| match v { Value::Prim(p) => p.int_width(), _ => None };
    for (xs, w) in [
        (vec![0, 7, 255], IntWidth::U8),
        (vec![], IntWidth::U8),
        (vec![0, 256, 3], IntWidth::U16),
        (vec![65536], IntWidth::U32),
        (vec![u32::MAX as i64 + 1], IntWidth::I64),
        (vec![-1, 3], IntWidth::I8),
        (vec![-1, 200], IntWidth::I16),
        (vec![-1, 40000], IntWidth::I32),
        (vec![i32::MIN as i64 - 1], IntWidth::I64),
    ] {
        let v = Value::int(xs.clone());
        assert_eq!(width(&v), Some(w), "{xs:?}");
        assert_eq!(v, Value::i64(xs.clone()));
        assert_eq!(crate::hash::hash(&v), crate::hash::hash(&Value::i64(xs)));
    }
}

/// Two integer storages meet at the narrowest storage holding both, with no detour through `i64`.
#[test]
fn narrow_storages_meet_at_the_wider() {
    let (a, b) = Prim::meet(Prim::U8(Arc::new(vec![1, 2])), Prim::ints_at(IntWidth::U16, [300, 4].into_iter()));
    assert_eq!((a.int_width(), b.int_width()), (Some(IntWidth::U16), Some(IntWidth::U16)));
    assert_eq!(a, Prim::U8(Arc::new(vec![1, 2])));
    let (a, b) = Prim::meet(Prim::ints_at(IntWidth::U32, [70000].into_iter()), Prim::I64(Arc::new(vec![-1])));
    assert_eq!((a.int_width(), b.int_width()), (Some(IntWidth::I64), Some(IntWidth::I64)));
    // unsigned meets signed at the narrowest storage holding both ranges
    let (a, b) = Prim::meet(Prim::U8(Arc::new(vec![200])), Prim::ints_at(IntWidth::I8, [-1].into_iter()));
    assert_eq!((a.int_width(), b.int_width()), (Some(IntWidth::I16), Some(IntWidth::I16)));
    assert_eq!(IntWidth::U32.join(IntWidth::I8), IntWidth::I64);
    assert_eq!(IntWidth::U16.join(IntWidth::I32), IntWidth::I32);
}

/// Signed narrow keys sort in value order, and pack at their width.
#[test]
fn signed_keys_order_by_value() {
    let xs = [-128i64, -1, 0, 1, 127];
    let p = Prim::ints_at(IntWidth::I8, xs.into_iter());
    let keys: Vec<u64> = (0..xs.len()).map(|i| p.key_at(i)).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]) && keys.iter().all(|&k| k < 256), "{keys:?}");
    assert_eq!(p.like_keys(&keys), p);
}

/// Two integer leaves at different storages meet at `i64`; one storage meets as it is.
#[test]
fn meet_widens_bytes() {
    let (a, b) = Prim::meet(Prim::U8(Arc::new(vec![1, 2])), Prim::I64(Arc::new(vec![-1, 3])));
    assert!(matches!((&a, &b), (Prim::I64(_), Prim::I64(_))));
    assert_eq!(a, Prim::I64(Arc::new(vec![1, 2])));
    let xs = Arc::new(vec![5u8]);
    let (a, _) = Prim::meet(Prim::U8(xs.clone()), Prim::U8(Arc::new(vec![6])));
    assert!(matches!(a, Prim::U8(v) if Arc::ptr_eq(&v, &xs)), "one storage meets without a copy");
}

/// The `i64` words a position reader sees are the integers' two's complement, in the same buffer.
#[test]
fn words_round_trip() {
    let xs = vec![-1i64, 0, 5, i64::MIN];
    let w = words_of_i64s(xs.clone());
    assert_eq!(w, vec![u64::MAX, 0, 5, 1 << 63]);
    assert_eq!(i64s_of_words(w), xs);
}

/// A `Value` clone must be a refcount bump, not a column copy: `eval_graph` clones at every
/// shared edge. A `List`'s partition is the same size as a `u64` payload column, so a bare
/// `Vec` here made a shared edge cost as much again as the data it carried.
#[test]
fn cloning_a_list_shares_its_partition() {
    let list = Value::List(Bounds::offsets(vec![1, 3, 6]), Box::new(Value::i64(vec![0; 6])));
    let copy = list.clone();
    let (Value::List(Bounds::Offsets(a), _), Value::List(Bounds::Offsets(b), _)) = (&list, &copy)
    else {
        panic!("expected two offset-partitioned lists")
    };
    assert!(Arc::ptr_eq(a, b), "clone copied the partition");
}

/// Equality is still by the PARTITION — the shared-buffer check is only a fast path, so two
/// distinct buffers describing one partition stay equal, as do a `Stride` and its offsets.
#[test]
fn equality_is_by_partition_not_buffer() {
    assert_eq!(Bounds::offsets(vec![1, 3, 6]), Bounds::offsets(vec![1, 3, 6]));
    assert_eq!(Bounds::offsets(vec![2, 4, 6]), Bounds::Stride(2, 3));
    assert_ne!(Bounds::offsets(vec![1, 3, 6]), Bounds::offsets(vec![1, 3, 5]));
}

/// `inject` assigns every row one tag, so the assignment is two words: no discriminant column
/// and no offset column, at any row count. This is the `Sum`-side twin of a uniform `Bounds`
/// becoming a `Stride`, and it is the state a `Fail` column that has not failed stays in.
#[test]
fn one_tag_throughout_costs_no_columns() {
    let t = Tags::from_tags(vec![2, 2, 2, 2], 3);
    assert_eq!(t.const_tag(), Some(2));
    assert!(matches!(t, Tags::Const(2, 4)));
    // ...and every row still reads back the same as the column form would answer.
    assert_eq!(t.tags_iter().collect::<Vec<_>>(), vec![2, 2, 2, 2]);
    assert_eq!((0..4).map(|i| t.offset_at(i)).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
}

/// Equality and hash are by the ASSIGNMENT, so the two representations are interchangeable —
/// the property that lets `Const` appear anywhere a `Column` would without being observable.
#[test]
fn const_and_column_assignments_agree() {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let konst = Tags::Const(1, 3);
    let column = Tags::Column(Prim::U8(Arc::new(vec![1, 1, 1])), Arc::new(vec![0, 1, 2]));
    assert_eq!(konst, column);
    let h = |t: &Tags| {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    };
    assert_eq!(h(&konst), h(&column));
    // a mixed assignment is not equal to either.
    assert_ne!(konst, Tags::from_tags(vec![1, 0, 1], 2));
}

/// A gather over integer leaves at different storages reads each source in place: the result is
/// at `i64`, and holds each row as its source held it.
#[test]
fn gather_lanes_reads_mixed_storages_in_place() {
    let (bytes, wide) = (Prim::U8(Arc::new(vec![1, 2, 255])), Prim::I64(Arc::new(vec![-1, 300])));
    let (tags, off) = ([1usize, 0, 0, 1, 0], [1usize, 2, 0, 0, 1]);
    let out = Prim::gather_lanes(&[&bytes, &wide], &tags, &off);
    assert!(matches!(&out, Prim::I64(v) if **v == vec![300, 255, 1, -1, 2]));
    let flipped = Prim::gather_lanes(&[&wide, &bytes], &[0usize, 1], &[0, 2]);
    assert!(matches!(&flipped, Prim::I64(v) if **v == vec![-1, 255]));
}

/// Copying spans from integer leaves at different storages reads each source in place: the
/// result is at `i64`, the spans in turn.
#[test]
fn copy_spans_reads_mixed_storages_in_place() {
    let (bytes, wide) = (Prim::U8(Arc::new(vec![1, 2, 255])), Prim::I64(Arc::new(vec![-1, 300, 7])));
    let out = Prim::copy_spans(&[&bytes, &wide], &[(1, 1, 3), (0, 0, 2), (0, 3, 3), (1, 0, 1)]);
    assert!(matches!(&out, Prim::I64(v) if **v == vec![300, 7, 1, 2, -1]));
}
