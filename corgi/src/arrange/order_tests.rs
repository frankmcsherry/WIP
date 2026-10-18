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
