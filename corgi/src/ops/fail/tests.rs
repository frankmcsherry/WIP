use super::*;
use std::sync::Arc;

#[test]
fn squash_matches_rowwise_failures() {
    // Each row succeeds, fails outside, or fails inside. Include empty,
    // all-Ok, all-Err and interleaved columns on both fast and general paths.
    for n in 0..=4 {
        for mut pattern in 0..3usize.pow(n) {
            let mut outer = Vec::new();
            let mut inner = Vec::new();
            let mut errors = Vec::new();
            let mut values = Vec::new();
            for row in 0..n {
                let state = pattern % 3;
                pattern /= 3;
                outer.push(state == 1);
                if state != 1 { inner.push(state == 2); }
                errors.push(state != 0);
                if state == 0 { values.push(row as i64); }
            }
            let ok = Value::i64(values);
            let nested = fail(&outer, fail(&inner, ok.clone()));
            assert_eq!(squash(nested).unwrap(), fail(&errors, ok));
        }
    }
}

#[test]
fn hoist_sum_preserves_pure_lanes_and_packed_order() {
    for mask in 0..8 {
        let err: Vec<_> = (0..3).map(|i| mask & (1 << i) != 0).collect();
        let ok = Value::i64((0..3).filter(|&i| !err[i]).map(|i| 10 + i as i64).collect());
        let pure = Value::u8(vec![20, 21]);
        let input = Value::sum(vec![0, 1, 0, 1, 0], vec![fail(&err, ok.clone()), pure.clone()]);
        let errors = [err[0], false, err[1], false, err[2]];
        let tags = [0, 1, 0, 1, 0].into_iter().zip(errors)
            .filter_map(|(t, e)| (!e).then_some(t)).collect();
        let expected = fail(&errors, Value::sum(tags, vec![ok, pure]));
        assert_eq!(hoist_sum(&[0], input).unwrap(), expected);
    }
    let pure = Value::sum(vec![0, 1, 0], vec![Value::i64(vec![1, 2]), Value::Unit(1)]);
    assert_eq!(hoist_sum(&[], pure.clone()).unwrap(), lift(pure));
    let empty = Value::sum(vec![], vec![lift(Value::i64(vec![])), Value::Unit(0)]);
    let expected = lift(Value::sum(vec![], vec![Value::i64(vec![]), Value::Unit(0)]));
    assert_eq!(hoist_sum(&[0], empty).unwrap(), expected);
}

#[test]
fn squash_retains_a_noncanonical_all_ok_assignment() {
    for n in [0, 3] {
        // Bypass producer compaction: an all-Ok Column is valid too. Preserve
        // its representation and buffers, but require the same value as Lift.
        let tags = Arc::new(vec![0; n]);
        let offsets = Arc::new((0..n).collect::<Vec<_>>());
        let values = Arc::new((0..n).map(|i| 10 + i as i64).collect::<Vec<_>>());
        let ok = Value::Prim(Prim::I64(values.clone()));
        let inner = Value::sum_tagged(
            Tags::Column(Prim::U8(tags.clone()), offsets.clone()),
            vec![ok.clone(), Value::Unit(0)],
        );
        assert!(no_errors(&inner));
        let result = squash(lift(inner)).unwrap();
        assert_eq!(result, lift(ok));
        let Value::Sum(Tags::Column(Prim::U8(t), o), lanes) = result else { panic!("retained assignment") };
        assert!(Arc::ptr_eq(&t, &tags));
        assert!(Arc::ptr_eq(&o, &offsets));
        let Value::Prim(Prim::I64(v)) = &lanes[0] else { panic!("retained payload") };
        assert!(Arc::ptr_eq(v, &values));
    }
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "Fail: lane lengths disagree with the assignment")]
fn fail_parts_checks_lane_lengths() {
    let bad = Value::sum_tagged(Tags::Const(0, 3), vec![Value::Unit(2), Value::Unit(0)]);
    let _ = fail_parts(bad, "test");
}

#[test]
fn fast_paths_keep_shape_errors_even_on_empty_columns() {
    for n in [0, 2] {
        assert!(squash(Value::Unit(n)).unwrap_err().contains("Squash:"));
        assert!(squash(lift(Value::Unit(n))).unwrap_err().contains("Squash inner:"));
        let bad = Value::sum(vec![0; n], vec![Value::Unit(n)]);
        assert!(hoist_sum(&[0], bad.clone()).unwrap_err().contains("HoistSum lane:"));
        assert!(hoist_sum(&[1], bad).unwrap_err().contains("HoistSum: no lane 1"));
    }
}

fn one_row(idx: Vec<i64>, hay: &Arc<Vec<i64>>) -> Value {
    Value::Prod(vec![
        Value::List(vec![idx.len()].into(), Box::new(Value::i64(idx))),
        Value::List(vec![hay.len()].into(), Box::new(Value::Prim(Prim::I64(hay.clone())))),
    ])
}

/// The state a fallible pipeline spends most of its time in: a `Fail` column that CAN fail but
/// has not. Its assignment is constant, so it carries no discriminant column and no offset
/// column, and "did anything fail" is a field read rather than a mask to build and scan.
#[test]
fn a_fail_that_has_not_failed_carries_no_witness_columns() {
    let hay = Arc::new(vec![10, 20, 30]);
    let ok = try_gather::<crate::ops::NumOp>(one_row(vec![2, 0, 1], &hay)).unwrap();
    assert!(no_errors(&ok));
    let Value::Sum(tags, _) = &ok else { panic!("a Fail is a Sum") };
    assert_eq!(tags.const_tag(), Some(0), "no failures: one tag throughout");

    // ...and it survives the plumbing `lower_effects` wraps around it. `Lift` is constant by
    // construction, and hoisting a product of un-failed columns stays constant.
    let lifted = lift(Value::i64(vec![1, 2, 3]));
    assert!(no_errors(&lifted));
    let paired = hoist_prod(Value::Prod(vec![lifted, lift(Value::i64(vec![4, 5, 6]))])).unwrap();
    assert!(no_errors(&paired));
    let Value::Sum(tags, _) = &paired else { panic!("a Fail is a Sum") };
    assert_eq!(tags.const_tag(), Some(0), "hoisting un-failed fields stays constant");
}

/// A row that DOES fail forces the general assignment — the two forms have to agree on what
/// they mean, so the mask read back is the same either way.
#[test]
fn a_failure_forces_the_general_assignment() {
    let hay = Arc::new(vec![10, 20, 30]);
    let bad = try_gather::<crate::ops::NumOp>(one_row(vec![7], &hay)).unwrap();
    assert!(!no_errors(&bad));
    let (err, _) = into_fail(bad, "t").unwrap();
    assert_eq!(err, vec![true]);
}

#[test]
fn one_row_identity_gather_reuses_the_haystack_leaf() {
    let hay = Arc::new(vec![10, 20, 30]);
    let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(one_row(vec![0, 1, 2], &hay)).unwrap(), "t").unwrap();
    assert_eq!(err, vec![false]);
    let (_, vals) = ok.into_list("identity gather result").unwrap();
    let Prim::I64(out) = vals.into_prim("identity gather result").unwrap() else { panic!("expected U64") };
    assert!(Arc::ptr_eq(&out, &hay));
}

#[test]
fn one_row_u64_gather_discards_a_partially_rewritten_failure() {
    let hay = Arc::new(vec![10, 20, 30]);
    let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(one_row(vec![1, 3, 0], &hay)).unwrap(), "t").unwrap();
    assert_eq!(err, vec![true]);
    assert_eq!(ok.len(), 0);
    assert_eq!(hay.as_slice(), &[10, 20, 30]);
}

#[test]
fn one_row_nonidentity_u64_gather_returns_values_and_normalizes_bounds() {
    let hay = Arc::new(vec![10, 20, 30]);
    let input = Value::Prod(vec![
        Value::List(Bounds::offsets(vec![3]), Box::new(Value::i64(vec![2, 0, 1]))),
        Value::List(vec![3].into(), Box::new(Value::Prim(Prim::I64(hay.clone())))),
    ]);
    let (err, ok) = into_fail(try_gather::<crate::ops::NumOp>(input).unwrap(), "t").unwrap();
    assert_eq!(err, vec![false]);
    let (bounds, vals) = ok.into_list("nonidentity gather result").unwrap();
    assert_eq!(bounds.strided(), Some(3));
    let Prim::I64(out) = vals.into_prim("nonidentity gather result").unwrap() else { panic!("expected U64") };
    assert_eq!(out.as_slice(), &[30, 10, 20]);
    assert!(!Arc::ptr_eq(&out, &hay));
}

#[test]
fn raw_one_row_primitive_gather_reads_zero_out_of_range() {
    use crate::ops::{NumOp, Op};
    use crate::graph::OpLike;
    let got = NumOp::Core(Op::Gather).eval(Value::Prod(vec![
        Value::List(vec![2].into(), Box::new(Value::i64(vec![0, 3]))),
        Value::List(vec![2].into(), Box::new(Value::i64(vec![10, 20]))),
    ]));
    assert_eq!(got.unwrap(), Value::List(vec![2].into(), Box::new(Value::i64(vec![10, 0]))));
}
