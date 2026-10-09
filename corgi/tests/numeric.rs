//! The numeric layer: Int and Float arithmetic built over the core, the `_b64` word verbs and the
//! bitwise ops, and proof that an Int's storage (bytes or `i64`) never shows in its value.

use corgi::{
    eval_graph, parse_ml, shape_of, shape_of_value, ArithOp, BinOp, Builder, CmpOp, NumOp, Op, Pred,
    Program, Shape, Value,
};

fn int(xs: &[i64]) -> Value {
    Value::i64(xs.to_vec())
}

/// run a binary op on two leaf columns.
fn bin(op: BinOp, a: Value, b: Value) -> Value {
    let mut bld = Builder::<NumOp>::default();
    let inp = bld.input();
    let out = bld.add(ArithOp::Bin(op), vec![inp]);
    eval_graph(&bld.finish(out), Value::Prod(vec![a, b]))
}

/// through `Program`, so a constant operand runs as an immediate.
fn run(src: &str, input: Value) -> Value {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}")).run(input)
}

/// the bits of a Float column, so `-0.0` and `0.0` compare apart.
fn float_bits(v: Value) -> Vec<u64> {
    v.as_f64("float_bits").unwrap().iter().map(|x| x.to_bits()).collect()
}

#[test]
fn subtraction_mixes_core_and_arith() {
    // (a - b) over two Int columns: Field/Tuple are core ops, Sub is the layer's.
    let input = Value::Prod(vec![int(&[5, -3, 10]), int(&[2, 4, -1])]);
    let mut b = Builder::<NumOp>::default();
    let inp = b.input();
    let a = b.add(NumOp::Core(Op::Field(0)), vec![inp]);
    let bb = b.add(NumOp::Core(Op::Field(1)), vec![inp]);
    let pair = b.tuple(vec![a, bb]);
    let out = b.add(NumOp::Arith(ArithOp::Bin(BinOp::Sub)), vec![pair]);
    let g = b.finish(out);
    g.check();
    // the SAME shape-checker types it — Arith judges as a plain leaf op:
    assert_eq!(shape_of(&g, &shape_of_value(&input)).unwrap(), Shape::Int);
    assert_eq!(eval_graph(&g, input), int(&[3, -7, 11]));
}

#[test]
fn core_sort_orders_by_value() {
    // the core sort orders Ints as signed integers and Floats in their total order (-0.0 < 0.0).
    let g = parse_ml("input sort").unwrap();
    let sorted = |v: Value| match eval_graph(&g, Value::List(vec![v.len()].into(), Box::new(v))) {
        Value::List(_, v) => *v,
        _ => panic!("expected a list"),
    };
    assert_eq!(sorted(int(&[5, -3, i64::MAX, 10, i64::MIN, -8])), int(&[i64::MIN, -8, -3, 5, 10, i64::MAX]));
    let xs = [2.5, 0.0, f64::NEG_INFINITY, -0.0, 1e300, -1e-300, f64::INFINITY];
    let want = [f64::NEG_INFINITY, -1e-300, -0.0, 0.0, 2.5, 1e300, f64::INFINITY];
    assert_eq!(float_bits(sorted(Value::f64(xs.to_vec()))), want.map(f64::to_bits));
}

#[test]
fn arith_shape_errors_are_caught() {
    let one = |op: ArithOp| {
        let mut b = Builder::<NumOp>::default();
        let inp = b.input();
        let out = b.add(NumOp::Arith(op), vec![inp]);
        b.finish(out)
    };
    let (i, f) = (Shape::Int, Shape::Float);
    // subtraction on a non-pair is a shape error, via the core machinery.
    assert!(shape_of(&one(ArithOp::Bin(BinOp::Sub)), &i).is_err());
    // two Ints or two Floats; an Int and a Float is a shape error, whichever side.
    assert_eq!(shape_of(&one(ArithOp::Bin(BinOp::Add)), &Shape::Prod(vec![f.clone(), f.clone()])).unwrap(), f);
    assert!(shape_of(&one(ArithOp::Bin(BinOp::Add)), &Shape::Prod(vec![i.clone(), f.clone()])).is_err());
    assert!(shape_of(&one(ArithOp::Bin(BinOp::Div)), &Shape::Prod(vec![f.clone(), i.clone()])).is_err());
    // remainder is Int-only.
    assert!(shape_of(&one(ArithOp::Bin(BinOp::Rem)), &Shape::Prod(vec![i.clone(), i.clone()])).is_ok());
    assert!(shape_of(&one(ArithOp::Bin(BinOp::Rem)), &Shape::Prod(vec![f.clone(), f])).is_err(), "float Rem must not type");
    // and on the surface, as a pair and as the immediate a constant operand becomes.
    for src in ["(input, 0.5) add", "(0.5, input) mul", "(input, 2.0) lt", "(input, input to_float) sub"] {
        assert!(shape_of(&parse_ml(src).unwrap(), &i).is_err(), "{src}");
        assert!(Program::compile_ml(src).unwrap().shape(&i).is_err(), "{src}");
    }
}

#[test]
fn relational_compare_to_mask() {
    // two leaf columns -> a 0/1 Int mask. The op is the leaf compare DDIR's `Condition` needs.
    let rel = |pred| {
        let mut b = Builder::<NumOp>::default();
        let inp = b.input();
        let out = b.add(CmpOp::Rel(pred), vec![inp]);
        b.finish(out)
    };
    let pair = |a, b| Value::Prod(vec![a, b]);
    // 1<2, 5<5 (no), 3<1 (no); and ge over the same columns
    assert_eq!(eval_graph(&rel(Pred::Lt), pair(int(&[1, 5, 3]), int(&[2, 5, 1]))), int(&[1, 0, 0]));
    assert_eq!(eval_graph(&rel(Pred::Ge), pair(int(&[1, 5, 3]), int(&[2, 5, 1]))), int(&[0, 1, 1]));
    // by value, as `sort` orders them: -3 < 1 holds, 2 < -5 does not; -0.5 < 0.25, -0.0 < 0.0.
    assert_eq!(eval_graph(&rel(Pred::Lt), pair(int(&[-3, 2]), int(&[1, -5]))), int(&[1, 0]));
    let floats = pair(Value::f64(vec![-0.5, 1.0, -0.0]), Value::f64(vec![0.25, -1.0, 0.0]));
    assert_eq!(eval_graph(&rel(Pred::Lt), floats), int(&[1, 0, 1]));
}

#[test]
fn int_arithmetic_wraps_past_i64() {
    // exact within i64; past it, the documented edge: the result wraps.
    let (max, min) = (i64::MAX, i64::MIN);
    assert_eq!(bin(BinOp::Add, int(&[max, -5]), int(&[1, 3])), int(&[min, -2]));
    assert_eq!(bin(BinOp::Sub, int(&[min, 7]), int(&[1, 9])), int(&[max, -2]));
    assert_eq!(bin(BinOp::Mul, int(&[max, -4]), int(&[2, 6])), int(&[-2, -24]));
    assert_eq!(run("input neg", int(&[5, -3, 0, min])), int(&[-5, 3, 0, min]));
    // the immediate forms agree
    assert_eq!(run("((input, 1) add, (input, 2) mul)", int(&[max])), Value::Prod(vec![int(&[min]), int(&[-2])]));
}

/// Division truncates toward zero, and is total: `x / 0 = 0` and `MIN / -1` wraps to `MIN`. `Rem`'s
/// sign follows the DIVIDEND (Rust's `%`), `MIN % -1 = 0`, and `x % 0 = x`. The zero divisor is
/// deliberately defined rather than rejected: a caller that already knows the modulus is positive
/// (DDIR's `hash(bound, ..)` guards `bound > 0`) should not pay for a branch, and "no reduction" is
/// the only reading of a zero modulus that keeps the op total.
#[test]
fn int_division_truncates_and_is_total() {
    let min = i64::MIN;
    let x = int(&[-17, 17, -17, min, 9, -9, 0, min, 42]);
    let y = int(&[5, -5, -5, -1, 0, 0, 0, min, 0]);
    assert_eq!(bin(BinOp::Div, x.clone(), y.clone()), int(&[-3, -3, 3, min, 0, 0, 0, 1, 0]));
    assert_eq!(bin(BinOp::Rem, x, y), int(&[-2, 2, -2, 0, 9, -9, 0, 0, 42]));
    // the surface spelling reaches the same kernels, and so do the immediates.
    let g = parse_ml("input div").unwrap();
    let input = Value::Prod(vec![int(&[-17]), int(&[5])]);
    assert_eq!(shape_of(&g, &shape_of_value(&input)).unwrap(), Shape::Int);
    assert_eq!(eval_graph(&g, input), int(&[-3]));
    let src = "((input, -1) div, (input, 0) div, (input, 0) rem, (input, -5) rem)";
    let want = Value::Prod(vec![int(&[17, min]), int(&[0, 0]), int(&[-17, min]), int(&[-2, -3])]);
    assert_eq!(run(src, int(&[-17, min])), want);
}

#[test]
fn float_arithmetic_is_ieee() {
    let xs = vec![0.5, 1e300, -3.0, 1.0, -2.0, -0.0];
    let ys = vec![0.25, 1e300, 2.0, 0.0, 0.0, 1.0];
    let f = |v: &Vec<f64>| Value::f64(v.clone());
    for (op, rust) in [
        (BinOp::Add, (|a, b| a + b) as fn(f64, f64) -> f64),
        (BinOp::Sub, |a, b| a - b),
        (BinOp::Mul, |a, b| a * b),
        (BinOp::Div, |a, b| a / b),
    ] {
        let want: Vec<u64> = xs.iter().zip(&ys).map(|(&a, &b)| rust(a, b).to_bits()).collect();
        assert_eq!(float_bits(bin(op, f(&xs), f(&ys))), want, "{op:?}");
    }
    assert_eq!(float_bits(run("input neg", f(&xs))), xs.iter().map(|x| (-x).to_bits()).collect::<Vec<_>>());
    // a float literal is the float: `to_float` of an Int gives the same value.
    for src in ["(3.0, 3 to_float) eq", "(-3.0, -3 to_float) eq", "(1e3, 1000 to_float) eq"] {
        assert_eq!(run(src, int(&[0])), int(&[1]), "{src}");
    }
}

/// a splitmix64 step, the reference for the mixer below.
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
fn word_verbs_match_u64_arithmetic() {
    // splitmix64 written with the `_b64` verbs: the constants are the u64 ones read as i64, and the
    // result is Rust's u64 wrapping result read as i64.
    let src = "let z = (input, -7046029254386353131) add_b64 in \
               let z = ((z, z shr_b64 30) xor, -4658895280553007687) mul_b64 in \
               let z = ((z, z shr_b64 27) xor, -7723592293110705685) mul_b64 in \
               (z, z shr_b64 31) xor";
    let xs = [0, 1, -1, 12345, i64::MAX, i64::MIN];
    let want: Vec<i64> = xs.iter().map(|&x| splitmix64(x as u64) as i64).collect();
    assert_eq!(run(src, int(&xs)), Value::i64(want));
    // `shr_b64` is the logical shift of the word; dividing by a power of two is `div` (toward
    // zero, as a shift), and its remainder takes the dividend's sign. Plain `shr` is retired.
    let xs = int(&[-8, -7, 7, i64::MIN]);
    assert_eq!(run("input shr_b64 1", xs.clone()), int(&[i64::MAX - 3, i64::MAX - 3, 3, 1 << 62]));
    assert_eq!(run("input shr_b64 64", xs.clone()), int(&[0; 4]));
    assert_eq!(run("((input, 2) div, (input, 2) rem)", xs.clone()),
        Value::Prod(vec![int(&[-4, -3, 3, i64::MIN / 2]), int(&[0, -1, 1, 0])]));
    let big = int(&[-1 << 40, (-1 << 40) - 1, (1 << 40) + 5, i64::MAX, i64::MIN]);
    let c = 1i64 << 32;
    let want = |f: fn(i64, i64) -> i64| int(&[-1 << 40, (-1 << 40) - 1, (1 << 40) + 5, i64::MAX, i64::MIN].map(|x| f(x, c)));
    assert_eq!(run("(input, 4294967296) div", big.clone()), want(|x, c| x / c));
    assert_eq!(run("(input, 4294967296) rem", big.clone()), want(|x, c| x % c));
    assert!(corgi::parse_ml("input shr 1").unwrap_err().contains("shr_b64"));
    // shifts left drop the bits past 64; rotates turn by k mod 64.
    assert_eq!(run("(input shl_b64 63, input rotl_b64 1, input rotr_b64 1, input rotl_b64 65)", int(&[1, i64::MIN])),
        Value::Prod(vec![int(&[i64::MIN, 0]), int(&[2, 1]), int(&[i64::MIN, 1 << 62]), int(&[2, 1])]));
    // bitwise ops are two's complement; `x and m` takes its mask as a constant.
    let (a, b) = (int(&[-1, 12, i64::MIN]), int(&[255, 10, -1]));
    assert_eq!(run("input and 255", a.clone()), int(&[255, 12, 0]));
    assert_eq!(run("input and", Value::Prod(vec![a.clone(), b.clone()])), int(&[255, 8, i64::MIN]));
    assert_eq!(run("input or", Value::Prod(vec![a.clone(), b.clone()])), int(&[-1, 14, -1]));
    assert_eq!(run("input xor", Value::Prod(vec![a, b])), int(&[-256, 6, i64::MAX]));
    // the word products agree with plain arithmetic wherever plain arithmetic is exact
    assert_eq!(run("((input, -3) mul_b64, (input, 5) sub_b64)", int(&[7])), Value::Prod(vec![int(&[-21]), int(&[2])]));
}

#[test]
fn storage_is_invisible() {
    // the same integers held as bytes and as i64s: equal, hashed alike, compared, sorted, searched
    // and computed on alike — the storage is the engine's, never the value's.
    let held = |xs: &[u8], bytes: bool| {
        if bytes { Value::u8(xs.to_vec()) } else { Value::i64(xs.iter().map(|&x| x as i64).collect()) }
    };
    let vals = [3, 1, 2, 1, 0, 255];
    let (bytes, wide) = (held(&vals, true), held(&vals, false));
    assert_eq!(bytes, wide);
    assert_eq!(corgi::hash(&bytes), corgi::hash(&wide));
    assert_eq!(run("input hash", bytes.clone()), run("input hash", wide.clone()));
    let pair = Value::Prod(vec![bytes.clone(), wide.clone()]);
    assert_eq!(run("input eq", pair.clone()), int(&[1; 6]));
    assert_eq!(run("input lt", pair.clone()), int(&[0; 6]));
    assert_eq!(run("input add", pair), int(&[6, 2, 4, 2, 0, 510]));
    // a list of either sorts, dedups and groups alike.
    let list = |v: Value| Value::List(vec![v.len()].into(), Box::new(v));
    for op in ["sort", "dedup", "map (x -> (x, x)) group"] {
        let src = format!("input {op}");
        assert_eq!(run(&src, list(bytes.clone())), run(&src, list(wide.clone())), "{op}");
    }
    // a join: byte needles in an i64 haystack, and i64 needles in a byte haystack, find what i64s find.
    let (needles, hay) = ([1, 255, 7], [0, 1, 1, 2, 3, 255]);
    let find = |n, h| run("input find", Value::Prod(vec![list(held(&needles, n)), list(held(&hay, h))]));
    assert_eq!(find(true, false), find(false, false));
    assert_eq!(find(false, true), find(false, false));
}
