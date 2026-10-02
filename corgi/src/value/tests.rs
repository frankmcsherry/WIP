use super::*;

/// `cast` to the width a leaf already has is the identity, and must not copy the column: the
/// stored bytes ARE the result's bytes, so the result shares the buffer (an `Arc` bump).
#[test]
fn identity_cast_reuses_the_buffer() {
    let xs = leaf(vec![10u32, 20, 30]);
    let p = Prim::U32(xs.clone());
    let Prim::U32(out) = p.cast(32) else { panic!("cast(32) must stay a U32 leaf") };
    assert!(Arc::ptr_eq(&out, &xs), "same-width cast copied the column");
}

/// A genuine re-width keeps the low bytes (narrowing) or zero-extends (widening), for every
/// (source, destination) pair the `prim!` grid generates.
#[test]
fn rewidth_keeps_the_low_bytes() {
    let wide = Prim::U64(leaf(vec![0x0102_0304_0506_0708, 0xff, 0x1_0000]));
    assert_eq!(wide.cast(8), Prim::U8(leaf(vec![0x08, 0xff, 0x00])));
    assert_eq!(wide.cast(16), Prim::U16(leaf(vec![0x0708, 0x00ff, 0x0000])));
    assert_eq!(wide.cast(32), Prim::U32(leaf(vec![0x0506_0708, 0xff, 0x1_0000])));

    let narrow = Prim::U8(leaf(vec![0, 1, 255]));
    assert_eq!(narrow.cast(16), Prim::U16(leaf(vec![0, 1, 255])));
    assert_eq!(narrow.cast(64), Prim::U64(leaf(vec![0, 1, 255])));
}

/// A `Value` clone must be a refcount bump, not a column copy: `eval_graph` clones at every
/// shared edge. A `List`'s partition is the same size as a `u64` payload column, so a bare
/// `Vec` here made a shared edge cost as much again as the data it carried.
#[test]
fn cloning_a_list_shares_its_partition() {
    let list = Value::List(Bounds::offsets(vec![1, 3, 6]), Box::new(Value::u64(vec![0; 6])));
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
    let column = Tags::Column(Prim::U8(leaf(vec![1, 1, 1])), Arc::new(vec![0, 1, 2]));
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

/// Narrowing then widening back is `mod 2^bits` — the documented truncating semantics, not a
/// round trip. Pinned so a future "make cast lossless" change has to face the corpus.
#[test]
fn narrow_then_widen_truncates() {
    let wide = Prim::U64(leaf(vec![0x1_0000, 0x1_0001]));
    assert_eq!(wide.cast(16).cast(64), Prim::U64(leaf(vec![0, 1])));
}
