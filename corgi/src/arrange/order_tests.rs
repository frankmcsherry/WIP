use super::{gather, group_bounds, segment_labels, sort_blocks};
use crate::value::{Bounds, Value};

#[test]
fn sort_blocks_segmented_stable_argmin() {
    // two segments (labels [0,0,0, 1,1,1]) over rows; segment 0 has duplicate 3s (stability),
    // segment 1 has duplicate 5s. Segments must stay contiguous, sort within, argmin at start.
    let labels = vec![0u64, 0, 0, 1, 1, 1];
    let v = Value::i64(vec![3, 1, 3, 5, 2, 5]);
    let (perm, _refined) = sort_blocks(&labels, &v);

    // segment 0 occupies output [0,3), segment 1 [3,6); each perm entry stays in its segment's
    // index range (rows contiguous, in segment order).
    for &p in &perm[0..3] {
        assert!(p < 3, "segment 0 pulled a row from segment 1");
    }
    for &p in &perm[3..6] {
        assert!((3..6).contains(&p), "segment 1 pulled a row from segment 0");
    }

    // sorted WITHIN each segment.
    let sorted = gather(&v, &perm).into_i64("sorted").unwrap();
    assert_eq!(&sorted[0..3], &[1, 3, 3]);
    assert_eq!(&sorted[3..6], &[2, 5, 5]);

    // perm[segment_start] is the segment's argmin (original position of its minimum).
    assert_eq!(perm[0], 1); // min of [3,1,3] is at index 1
    assert_eq!(perm[3], 4); // min of [5,2,5] is at index 4

    // stability: the two equal 3s (indices 0 and 2) keep input order.
    assert_eq!(&perm[1..3], &[0, 2]);
}

#[test]
fn segment_labels_offsets_and_stride_agree() {
    // Offsets([2,4,6]) and the equivalent Stride(2,3) describe the same 3-row partition
    // (rows of width 2), so per-element segment labels are identical.
    let off = Bounds::offsets(vec![2, 4, 6]);
    let stride = Bounds::Stride(2, 3);
    assert_eq!(segment_labels(&off), vec![0, 0, 1, 1, 2, 2]);
    assert_eq!(segment_labels(&off), segment_labels(&stride));
}

#[test]
fn roundtrip_matches_the_sort_word() {
    // build List<u64> with ragged rows, segmented-sort it via the arrange surface, and check
    // it reproduces exactly what the ML `sort` produces on the same list.
    let bounds = Bounds::offsets(vec![3, 3, 6]); // rows [3,1,2], [], [5,0,4]
    let vals = Value::i64(vec![3, 1, 2, 5, 0, 4]);
    let list = Value::List(bounds.clone(), Box::new(vals.clone()));

    // arrange surface: seed segment labels from bounds, segmented-sort, gather by perm.
    let labels = segment_labels(&bounds);
    let (perm, _refined) = sort_blocks(&labels, &vals);
    let ours = Value::List(bounds.clone(), Box::new(gather(&vals, &perm)));

    // the ML word.
    let theirs = crate::Program::compile_ml("input sort").unwrap().run(list);
    assert_eq!(ours, theirs);
}

#[test]
fn group_bounds_runs() {
    // exclusive ends of equal-value runs: [1,1,2,3,3,3] → groups [0,2),[2,3),[3,6).
    assert_eq!(group_bounds(&Value::i64(vec![1, 1, 2, 3, 3, 3])), vec![2, 3, 6]);
    // all distinct → one end per row; all equal → a single group; empty → no ends.
    assert_eq!(group_bounds(&Value::i64(vec![1, 2, 3])), vec![1, 2, 3]);
    assert_eq!(group_bounds(&Value::i64(vec![4, 4, 4])), vec![3]);
    assert!(group_bounds(&Value::i64(vec![])).is_empty());
    // the runs of a column with repeats.
    assert_eq!(group_bounds(&Value::i64(vec![10, 10, 20, 20, 20, 30])), vec![2, 5, 6]);
}

#[test]
fn native_integer_lanes_borrow_only_wide_integer_products() {
    use super::borrowed_int_lanes;
    let a = Value::i64(vec![1, 2, 3]);
    let b = Value::i64(vec![4, 5, 6]);
    let nested = Value::Prod(vec![a.clone(), Value::Prod(vec![Value::Unit(3), b.clone()])]);
    let lanes = borrowed_int_lanes(&nested).unwrap();
    assert_eq!(lanes, vec![&[1, 2, 3][..], &[4, 5, 6][..]]);
    assert_eq!(lanes[0].as_ptr(), a.as_i64("test").unwrap().as_ptr());
    assert_eq!(lanes[1].as_ptr(), b.as_i64("test").unwrap().as_ptr());
    assert_eq!(borrowed_int_lanes(&Value::Prod(vec![a.clone()])).unwrap(), vec![&[1, 2, 3][..]]);
    assert_eq!(borrowed_int_lanes(&Value::Unit(3)), Some(vec![]));
    assert_eq!(borrowed_int_lanes(&Value::Prod(vec![])), Some(vec![]));
    assert_eq!(borrowed_int_lanes(&Value::i64(vec![])), Some(vec![&[][..]]));

    let narrow = Value::u8(vec![1, 2, 3]);
    let list = Value::List(Bounds::Stride(1, 3), Box::new(a.clone()));
    for rejected in [
        narrow.clone(), Value::f64(vec![1.0, 2.0, 3.0]), list.clone(),
        crate::engine::take_ref(list), Value::sum(vec![0; 3], vec![a.clone()]),
        Value::Prod(vec![a.clone(), narrow.clone()]),
        Value::Prod(vec![narrow, a]),
    ] {
        assert!(borrowed_int_lanes(&rejected).is_none());
    }
}

/// Interleaved source pairs must retain their row order after grouping: otherwise contiguous
/// requests lose the dense comparison path. Use sizes beyond the small-sort threshold.
#[test]
fn pair_grouping_preserves_contiguous_spans() {
    for n in [0, 1, 16, 64, 257, 4096] {
        let pairs: Vec<_> = (0..n).map(|i| {
            let (column, row) = (i % 4, i / 4);
            ((column, row + 7), (column + 4, row + 19))
        }).collect();
        let order = super::pair_order(&pairs);
        let mut seen = vec![false; n];
        for &i in &order { assert!(!seen[i]); seen[i] = true; }
        assert!(seen.into_iter().all(|present| present));
        for adjacent in order.windows(2) {
            let (a, b) = (pairs[adjacent[0]], pairs[adjacent[1]]);
            assert!((a.0.0, a.1.0) <= (b.0.0, b.1.0));
            if (a.0.0, a.1.0) == (b.0.0, b.1.0) {
                assert_eq!((b.0.1, b.1.1), (a.0.1 + 1, a.1.1 + 1));
            }
        }
        let single: Vec<_> = (0..n).map(|i| ((1, i + 3), (0, i + 11))).collect();
        assert_eq!(super::pair_order(&single), (0..n).collect::<Vec<_>>());
    }
}
