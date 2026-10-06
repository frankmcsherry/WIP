use corgi::{arrange, bytes, eval_graph, hash, lower_effects, shape_of, shape_of_value,
    Builder, CmpOp, Integer, IntegerBinary as B, IntegerEncoding as E,
    IntegerFrame as F, IntegerOp, NumOp, Op, Program, Shape, Value};
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}, sync::Arc};

fn encodings(xs: &[i128]) -> Vec<Integer> {
    let mut encs = vec![E::Bits, E::Wide];
    for width in [8, 16, 32, 64] {
        for frame in [F::Zero, F::Biased] { encs.push(E::Native { width, frame }); }
    }
    encs.into_iter().filter_map(|e| Integer::with_encoding(xs.to_vec(), e).ok()).collect()
}
fn logical(v: &Value) -> Vec<i128> {
    let Value::Int(i) = v else { panic!("expected integer: {v:?}") };
    i.to_vec()
}
fn ok(v: Value) -> Value {
    let Value::Sum(tags, mut lanes) = v else { panic!("expected Fail") };
    assert!(lanes[1].is_empty());
    assert!(tags.is_empty() || tags.const_tag() == Some(0));
    lanes.remove(0)
}
fn one(op: impl Into<NumOp>) -> corgi::Graph<NumOp> {
    let mut b = Builder::default(); let x = b.input(); let y = b.add(op, vec![x]); b.finish(y)
}

#[test]
fn packing_and_byte_contracts() {
    for (xs, enc) in [
        (vec![0, 1, 1], E::Bits),
        (vec![0, 127, 128, 255], E::Native { width: 8, frame: F::Zero }),
        (vec![-128, -1, 0, 127], E::Native { width: 8, frame: F::Biased }),
        (vec![-1, 255], E::Native { width: 16, frame: F::Biased }),
        (vec![u64::MAX as i128], E::Native { width: 64, frame: F::Zero }),
        (vec![-1, u64::MAX as i128], E::Wide),
    ] {
        let i = Integer::new(xs.clone()); assert_eq!(i.encoding(), enc); assert_eq!(i.to_vec(), xs);
    }
    let raw = Arc::new(vec![0u8, 128, 255]);
    let col = Integer::from_bytes(raw.clone());
    assert!(Arc::ptr_eq(&raw, &col.to_bytes().unwrap()));
    assert_eq!(Integer::new(vec![0, 1]).to_bytes().unwrap().as_slice(), &[0, 1]);
    assert!(Integer::new(vec![-1]).to_bytes().is_err());
    assert!(Integer::new(vec![256]).to_bytes().is_err());
    assert!(Integer::with_encoding(vec![0], E::Native { width: 0, frame: F::Zero }).is_err());
    let signed = eval_graph(&one(IntegerOp::FromSigned), Value::u8(vec![0, 127, 128, 255]));
    assert_eq!(logical(&signed), vec![-128, -1, 0, 127]);
    let unsigned = eval_graph(&one(IntegerOp::FromUnsigned), Value::u8(vec![0, 127, 128, 255]));
    assert_eq!(logical(&unsigned), vec![0, 127, 128, 255]);
}

#[test]
fn layout_matrix_matches_independent_arithmetic_and_comparison() {
    let n = 2053; // two full tiles and a tail
    let samples = [
        (0..n).map(|i| (i % 2) as i128).collect::<Vec<_>>(),
        (0..n).map(|i| (i % 256) as i128).collect(),
        (0..n).map(|i| (i % 256) as i128 - 128).collect(),
        vec![0, 256, 65535, 65536, u32::MAX as i128, u64::MAX as i128],
        vec![i64::MIN as i128, -1, 0, i64::MAX as i128, 2, 17],
        vec![i128::MIN, i128::MAX, 0, -1, 1, 2],
    ];
    for ax in &samples { for bx in &samples {
        if ax.len() != bx.len() { continue; }
        for a in encodings(ax) { for b in encodings(bx) {
            let expected: Vec<_> = ax.iter().zip(bx).map(|(x, y)| (x > y) as i8 - (x < y) as i8).collect();
            assert_eq!(a.compare(&b).unwrap(), expected);
            for take_max in [false, true] {
                let expected: Vec<_> = ax.iter().zip(bx).map(|(&x, &y)| if take_max { x.max(y) } else { x.min(y) }).collect();
                assert_eq!(a.pick(&b, take_max).unwrap().to_vec(), expected);
            }
            for op in [B::Add, B::Sub, B::Mul] {
                let expected: Option<Vec<_>> = ax.iter().zip(bx).map(|(&x, &y)| match op {
                    B::Add => x.checked_add(y), B::Sub => x.checked_sub(y), B::Mul => x.checked_mul(y),
                }).collect();
                match (a.binary(&b, op), expected) {
                    (Ok(out), Some(expected)) => assert_eq!(out.to_vec(), expected, "{op:?} {:?} {:?}", a.encoding(), b.encoding()),
                    (Err(_), None) => {},
                    mismatch => panic!("{op:?} {:?} {:?}: {mismatch:?}", a.encoding(), b.encoding()),
                }
            }
        } }
    } }
}

#[test]
fn arity_is_not_a_layout_dispatch_axis() {
    let xs = [-2, 0, 3, 7]; let ys = [0, 1, 127, 255]; let zs = [1, -1, 4, 9];
    for a in encodings(&xs) { for b in encodings(&ys) { for c in encodings(&zs) {
        assert_eq!(Integer::sum(&[&a, &b, &c]).unwrap().to_vec(), vec![-1, 0, 134, 271]);
    } } }
    // Cancellation may make the output smaller than individual inputs.
    let a = Integer::new(vec![255, -128]); let b = Integer::new(vec![-255, 128]);
    assert_eq!(Integer::sum(&[&a, &b]).unwrap().compact().encoding(), E::Bits);
    let c = Integer::new(vec![1]);
    assert_eq!(Integer::sum(&[&c; 17]).unwrap().to_vec(), vec![17]);
}

#[test]
fn identity_is_independent_of_frame_width_and_batch_neighbors() {
    let xs = [0, 1];
    for a in encodings(&xs) { for b in encodings(&xs) {
        assert_eq!(a, b); assert_eq!(a.hashes(), b.hashes());
        let fingerprint = |i: &Integer| { let mut h = DefaultHasher::new(); i.hash(&mut h); h.finish() };
        assert_eq!(fingerprint(&a), fingerprint(&b));
        assert_eq!(shape_of_value(&Value::Int(a.clone())), Shape::Int);
        assert_eq!(hash(&Value::Int(a.clone())), hash(&Value::Int(b.clone())));
    } }
    for x in [-1, 0, 5, 255, u64::MAX as i128] {
        let isolated = Integer::new(vec![x]);
        let widened = Integer::new(vec![x, i128::MIN]);
        assert_eq!(isolated.hashes()[0], widened.hashes()[0]);
        assert_eq!(isolated.compare_at(0, &widened, 0), std::cmp::Ordering::Equal);
    }
    assert_ne!(Integer::new(vec![-1]).hashes(), Integer::new(vec![u64::MAX as i128]).hashes());
}

#[test]
fn core_sort_search_merge_gather_and_append_accept_mixed_layouts() {
    let xs: Vec<_> = (0..2053).map(|i| ((i * 73) % 127) as i128 - 63).collect();
    let mut expected: Vec<_> = (0..xs.len()).collect(); expected.sort_by_key(|&i| xs[i]);
    for col in encodings(&xs) {
        let v = Value::Int(col.clone());
        assert_eq!(arrange::sort_perm(&v), expected);
        let sorted = arrange::gather(&v, &expected);
        assert!(logical(&sorted).windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(hash(&arrange::gather(&v, &[7, 0])), vec![col.hashes()[7], col.hashes()[0]]);
    }
    for xs in [vec![1, 0, 1, 0, 0], vec![i128::MAX, -1, i128::MIN, 0, u64::MAX as i128]] {
        let v = Value::integer(xs.clone());
        let mut perm: Vec<_> = (0..xs.len()).collect(); perm.sort_by_key(|&i| xs[i]);
        assert_eq!(arrange::sort_perm(&v), perm);
    }
    let a = Value::Int(Integer::with_encoding(vec![-1, 0, 1, 5, 5], E::Native { width: 8, frame: F::Biased }).unwrap());
    let b = Value::Int(Integer::with_encoding(vec![0, 1, 5, 7], E::Native { width: 64, frame: F::Zero }).unwrap());
    assert_eq!(arrange::find_ranges(&b, &a), (vec![1, 2, 3, 5], vec![2, 3, 5, 5]));
    let runs = arrange::survey(&a, &b);
    assert!(runs.iter().any(|r| matches!(r, arrange::Run::Both(_, _))));
    let mixed = arrange::gather_lanes(&[Some(&a), Some(&b)], &[0, 1, 0, 1], &[0, 0, 4, 3]);
    assert_eq!(logical(&mixed), vec![-1, 0, 5, 7]);
    let list = |v| Value::List(vec![2].into(), Box::new(v));
    let aa = arrange::gather(&a, &[0, 4]); let bb = arrange::gather(&b, &[0, 3]);
    let out = eval_graph(&one(Op::Append), Value::Prod(vec![list(aa), list(bb)]));
    let Value::List(_, vals) = out else { panic!() }; assert_eq!(logical(&vals), vec![-1, 5, 0, 7]);
}

#[test]
fn nested_structural_order_hash_and_refs_keep_integer_identity() {
    let native = Integer::with_encoding(vec![-1, 5, 5, 8], E::Native { width: 8, frame: F::Biased }).unwrap();
    let wide = Integer::with_encoding(native.to_vec(), E::Wide).unwrap();
    let nest = |i| Value::Prod(vec![Value::List(vec![2, 4].into(), Box::new(Value::Int(i))), Value::u8(vec![9, 9])]);
    let (a, b) = (nest(native), nest(wide));
    assert_eq!(hash(&a), hash(&b));
    assert_eq!(arrange::compare_idx(&a, &b, &[0, 1], &[0, 1]), vec![0, 0]);
    let refs = eval_graph(&one(Op::Ref), a.clone());
    assert_eq!(hash(&refs), hash(&a));
    assert_eq!(eval_graph(&one(Op::Clone), refs), a);
}

#[test]
fn codec_roundtrips_layout_and_rejects_corrupt_integer_headers() {
    for xs in [vec![0, 1, 0], vec![-1, 5], vec![0, 128, 255], vec![i128::MIN, i128::MAX]] {
        for i in encodings(&xs) {
            let v = Value::Int(i.clone()); let mut data = Vec::new();
            bytes::write_to(&v, &mut data).unwrap(); assert_eq!(data.len(), bytes::length_in_bytes(&v));
            let (out, read) = bytes::read_from(&data).unwrap(); assert_eq!(read, data.len());
            assert_eq!(out, v); let Value::Int(out) = out else { panic!() };
            assert_eq!(out.encoding(), i.encoding()); assert_eq!(out.hashes(), i.hashes());
            for n in 0..data.len() { assert!(bytes::read_from(&data[..n]).is_err()); }
        }
    }
    let wire = |frame, width, len, payload: &[u64]| [vec![6, frame, width, len], payload.to_vec()].concat()
        .into_iter().flat_map(u64::to_le_bytes).collect::<Vec<_>>();
    for data in [wire(2, 8, 0, &[]), wire(1, 1, 0, &[]), wire(0, 7, 0, &[]),
        wire(0, 1, u64::MAX, &[]), wire(0, 128, u64::MAX, &[]), wire(0, 1, 1, &[2])] {
        assert!(bytes::read_from(&data).is_err());
    }
}

#[test]
fn graphs_infer_logical_shape_and_overflow_is_per_row_effect_data() {
    let g = one(IntegerOp::Binary(B::Add));
    let pair = Shape::Prod(vec![Shape::Int, Shape::Int]);
    assert_eq!(shape_of(&g, &pair).unwrap(), Shape::Sum(vec![Shape::Int, Shape::Unit]));
    let out = eval_graph(&g, Value::Prod(vec![Value::integer(vec![i128::MAX, 250, -1]), Value::integer(vec![1, 10, 2])]));
    let Value::Sum(tags, lanes) = out else { panic!() };
    assert_eq!((0..tags.len()).map(|i| tags.tag_at(i)).collect::<Vec<_>>(), vec![1, 0, 0]);
    assert_eq!(logical(&lanes[0]), vec![260, 1]); assert_eq!(lanes[1], Value::Unit(1));
    let narrow = Integer::new(vec![250]);
    let wide = Integer::with_encoding(vec![250], E::Wide).unwrap();
    let ten = Integer::new(vec![10]);
    for a in [narrow, wide] {
        assert_eq!(a.binary(&ten, B::Add).unwrap().to_vec(), vec![260]);
        assert_eq!(a.wrapping(&ten, B::Add, 8).unwrap().to_vec(), vec![4]);
    }
    let mut b = Builder::<NumOp>::default(); let p = b.input();
    let sum = b.add(IntegerOp::Binary(B::Add), vec![p]);
    let two = b.add(Op::Lit(Value::integer(vec![2])), vec![p]);
    let args = b.tuple(vec![sum, two]); let mul = b.add(IntegerOp::Binary(B::Mul), vec![args]);
    let g = lower_effects(&b.finish(mul));
    let out = eval_graph(&g, Value::Prod(vec![Value::integer(vec![i128::MAX, 250]), Value::integer(vec![1, 10])]));
    let Value::Sum(tags, lanes) = out else { panic!() };
    assert_eq!(tags.tag_at(0), 1); assert_eq!(logical(&lanes[0]), vec![520]);
}

#[test]
fn ml_surface_and_fold_backedge_do_not_spell_storage_widths() {
    let p = Program::compile_ml("(input lit_int 250, input lit_int 10) int_add").unwrap();
    assert_eq!(logical(&ok(p.run_partial(Value::u64(vec![1])))), vec![260]);
    let p = Program::compile_ml("(input lit_int 250, input lit_int 10) wrap_add 8").unwrap();
    assert_eq!(logical(&p.run_partial(Value::u64(vec![1]))), vec![4]);
    assert!(Program::compile_ml("input wrap_add 4294967304").is_err());
    let p = Program::compile_ml("(input lit_int 0, input iota map (x -> x integer)) fold ((a, x) -> (a, x) int_add)").unwrap();
    p.shape(&Shape::Prim(64)).unwrap();
    assert_eq!(logical(&ok(p.run_partial(Value::u64(vec![30])))), vec![435]);
}

#[test]
fn byte_range_failure_and_sort_dedup_preserve_list_structure() {
    let out = eval_graph(&one(IntegerOp::ToBytes), Value::integer(vec![-1, 0, 255, 256]));
    let Value::Sum(tags, lanes) = out else { panic!() };
    assert_eq!((0..4).map(|i| tags.tag_at(i)).collect::<Vec<_>>(), vec![1, 0, 0, 1]);
    assert_eq!(lanes[0], Value::u8(vec![0, 255]));
    let rows = Value::List(vec![3, 6].into(), Box::new(Value::integer(vec![3, -1, 3, 8, 2, 2])));
    let out = eval_graph(&one(CmpOp::DedupList), rows);
    let Value::List(bounds, vals) = out else { panic!() };
    assert_eq!(bounds.to_vec(), vec![2, 4]); assert_eq!(logical(&vals), vec![-1, 3, 2, 8]);
}

#[test]
fn comparison_ops_accept_independent_integer_layouts() {
    let a = Value::integer(vec![-1, 0, 100, u64::MAX as i128]);
    let b = Value::Int(Integer::with_encoding(vec![0, -1, 256, -1], E::Wide).unwrap());
    assert_eq!(logical(&eval_graph(&one(CmpOp::Min), Value::Prod(vec![a.clone(), b.clone()]))), vec![-1, -1, 100, -1]);
    assert_eq!(logical(&eval_graph(&one(CmpOp::Max), Value::Prod(vec![a.clone(), b]))), vec![0, 0, 256, u64::MAX as i128]);
    assert_eq!(eval_graph(&one(CmpOp::Gt(0)), a), Value::u64(vec![0, 0, 1, 1]));
}
