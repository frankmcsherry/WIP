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
    // the signed storages: the same values, at every storage that holds them
    let all = [
        Value::u8(vec![0, 7, 127]), Value::i8(vec![0, 7, 127]), Value::i16(vec![0, 7, 127]),
        Value::i32(vec![0, 7, 127]), Value::i64(vec![0, 7, 127]),
    ];
    for a in &all {
        for b in &all {
            assert_eq!(a, b);
            assert_eq!(h(a), h(b));
        }
    }
    let neg = [Value::i8(vec![-1, -128]), Value::i16(vec![-1, -128]), Value::i32(vec![-1, -128]), Value::i64(vec![-1, -128])];
    for a in &neg {
        assert_eq!(a, &neg[3]);
        assert_eq!(h(a), h(&neg[3]));
    }
    assert_ne!(Value::u8(vec![255]), Value::i8(vec![-1]));
}

/// A storage's keys order its values and fit its width, which the sort's packing relies on.
#[test]
fn keys_order_and_fit_the_storage() {
    fn check<T: Elem + Ord + Copy>(xs: &[T]) {
        for &x in xs {
            assert!(T::BITS == 64 || x.key() < 1 << T::BITS, "key past the width");
            assert_eq!(T::from_key(x.key()), x);
            for &y in xs {
                assert_eq!(x.cmp(&y), x.key().cmp(&y.key()));
            }
        }
    }
    check(&[0u8, 1, 127, 128, 255]);
    check(&[i8::MIN, -1, 0, 1, i8::MAX]);
    check(&[i16::MIN, -1, 0, 1, i16::MAX]);
    check(&[i32::MIN, -1, 0, 1, i32::MAX]);
    check(&[i64::MIN, -1, 0, 1, i64::MAX]);
    assert_eq!((-1i8).word(), u64::MAX, "a word is the sign-extended two's complement");
    assert_eq!(255u8.word(), 255);
}

/// The narrowest storage that holds a range; two storages join at the narrowest holding both.
#[test]
fn storages_hold_and_join() {
    use Storage::*;
    assert_eq!(Storage::holding(0, 255), U8);
    assert_eq!(Storage::holding(-1, 0), I8);
    assert_eq!(Storage::holding(-1, 128), I16);
    assert_eq!(Storage::holding(0, 1 << 20), I32);
    assert_eq!(Storage::holding(i64::MIN, 0), I64);
    assert_eq!(Storage::join(U8, I8), I16);
    assert_eq!(Storage::join(U8, I16), I16);
    assert_eq!(Storage::join(I8, I32), I32);
    assert_eq!(Storage::join(U8, I64), I64);
    for s in [U8, I8, I16, I32, I64] {
        assert_eq!(Storage::join(s, s), s);
        let (lo, hi) = s.range();
        assert_eq!(Storage::holding(lo, hi), s);
    }
}

/// Two integer leaves at different storages meet at the narrowest storage holding both; one
/// storage meets as it is.
#[test]
fn meet_widens_to_the_join() {
    let (a, b) = Prim::meet(Prim::U8(Arc::new(vec![1, 2])), Prim::I64(Arc::new(vec![-1, 3])));
    assert!(matches!((&a, &b), (Prim::I64(_), Prim::I64(_))));
    assert_eq!(a, Prim::I64(Arc::new(vec![1, 2])));
    let (a, b) = Prim::meet(Prim::U8(Arc::new(vec![200])), Prim::I8(Arc::new(vec![-1])));
    assert!(matches!((&a, &b), (Prim::I16(x), Prim::I16(y)) if x[0] == 200 && y[0] == -1));
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
    assert_eq!(words_of(&xs), &[u64::MAX, 0, 5, 1 << 63]);
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
