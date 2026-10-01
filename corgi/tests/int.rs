//! Integer columns whose width is an encoding (`src/int.rs`, `dev/integers.md`), checked against
//! `i128` arithmetic and Rust's own sort and search, through programs and the public API. Every
//! check builds its columns several ways — narrowed, adopted at 64 bits, decoded as a window of a
//! shared buffer, at a constant — since the encoding must never be observable.

use corgi::bytes::{read_from, read_from_words, write_to};
use corgi::{hash, show, Bounds, Int, Program, Value, Width};
use std::sync::Arc;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

/// `n` integers from `[lo, lo + range)`.
fn draw(rng: &mut Rng, n: usize, lo: i128, range: u64) -> Vec<i128> {
    (0..n).map(|_| lo + rng.below(range) as i128).collect()
}

/// the same values as an `Int` column, encoded every way a column can be.
fn encodings(vals: &[i128]) -> Vec<Value> {
    let narrow = Int::from_i128s(vals).unwrap();
    let mut out = vec![Value::Int(narrow.clone())];
    if vals.iter().all(|&v| (0..=u64::MAX as i128).contains(&v)) {
        let u: Vec<u64> = vals.iter().map(|&v| v as u64).collect();
        out.push(Value::int_adopt(u.clone()));
        out.push(Value::int_u64(u));
    }
    // a window of a decoded message: two columns written together, the second viewed in place
    let msg = Value::Prod(vec![Value::Int(narrow.clone()), Value::Int(narrow)]);
    let words = encode_words(&msg);
    let (back, _) = read_from_words(&words).unwrap();
    let Value::Prod(mut cols) = back else { panic!("a product") };
    out.push(cols.pop().unwrap());
    out
}

fn encode_words(v: &Value) -> Arc<Vec<u64>> {
    let mut bytes = Vec::new();
    write_to(v, &mut bytes).unwrap();
    Arc::new(bytes.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect())
}

fn ints(v: &Value) -> Vec<i128> {
    match v {
        Value::Int(c) => c.values(),
        other => panic!("expected an Int column, got {}", show(other)),
    }
}

fn one_list(v: Value) -> Value {
    Value::List(Bounds::offsets(vec![v.len()]), Box::new(v))
}

fn run(src: &str, input: Value) -> Value {
    let p = Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}"));
    p.run(input).unwrap_or_else(|e| panic!("{src}: {e}"))
}

fn list_ints(v: &Value) -> Vec<i128> {
    match v {
        Value::List(_, vals) => ints(vals),
        other => panic!("expected a list, got {}", show(other)),
    }
}

const FRAMES: &[(i128, u64)] = &[
    (0, 1),
    (0, 200),
    (-100, 50),
    (5_000, 60_000),
    (-(1 << 40), 1 << 30),
    (1 << 50, u32::MAX as u64 * 4),
    (0, u64::MAX),
    (i64::MIN as i128, u64::MAX / 3),
];

#[test]
fn sort_and_dedup_agree_with_rust() {
    let mut rng = Rng(17);
    for &(lo, range) in FRAMES {
        for n in [0usize, 1, 2, 40, 3000] {
            let vals = draw(&mut rng, n, lo, range);
            let mut want = vals.clone();
            want.sort();
            let mut uniq = want.clone();
            uniq.dedup();
            for v in encodings(&vals) {
                assert_eq!(list_ints(&run("input sort", one_list(v.clone()))), want);
                assert_eq!(list_ints(&run("input dedup", one_list(v))), uniq);
            }
        }
    }
}

#[test]
fn product_keys_pack_by_span_and_sort_lexicographically() {
    let mut rng = Rng(23);
    for &(la, ra) in FRAMES {
        for &(lb, rb) in &FRAMES[..5] {
            let n = 500;
            let (a, b) = (draw(&mut rng, n, la, ra), draw(&mut rng, n, lb, rb));
            let c: Vec<u64> = (0..n).map(|_| rng.below(5)).collect();
            let mut want: Vec<(i128, i128, u64)> = (0..n).map(|i| (a[i], b[i], c[i])).collect();
            want.sort();
            for va in encodings(&a) {
                for vb in encodings(&b).into_iter().take(2) {
                    let triple = Value::Prod(vec![va.clone(), vb, Value::u64(c.clone())]);
                    let out = run("input sort", one_list(triple));
                    let Value::List(_, vals) = out else { panic!("a list") };
                    let Value::Prod(cols) = *vals else { panic!("a product") };
                    let (sa, sb) = (ints(&cols[0]), ints(&cols[1]));
                    let Value::Prim(_) = &cols[2] else { panic!("the u64 field stays a u64 leaf") };
                    let got: Vec<(i128, i128)> = sa.into_iter().zip(sb).collect();
                    assert_eq!(got, want.iter().map(|t| (t.0, t.1)).collect::<Vec<_>>());
                }
            }
        }
    }
}

#[test]
fn group_by_an_integer_key() {
    let mut rng = Rng(5);
    let n = 400;
    let k = draw(&mut rng, n, -7, 9);
    let v: Vec<u64> = (0..n as u64).collect();
    for kv in encodings(&k) {
        let out = run("input group", one_list(Value::Prod(vec![kv, Value::u64(v.clone())])));
        let Value::List(_, inner) = out else { panic!() };
        let Value::Prod(cols) = *inner else { panic!() };
        let mut want = k.clone();
        want.sort();
        want.dedup();
        assert_eq!(ints(&cols[0]), want);
    }
}

/// equal_range of every needle in the haystack, as Rust computes it.
fn ranges(hay: &[i128], needles: &[i128]) -> Vec<(u64, u64)> {
    needles
        .iter()
        .map(|x| (hay.partition_point(|h| h < x) as u64, hay.partition_point(|h| h <= x) as u64))
        .collect()
}

#[test]
fn find_across_encodings() {
    let mut rng = Rng(41);
    for &(lh, rh) in FRAMES {
        for &(ln, rn) in FRAMES {
            let mut hay = draw(&mut rng, 300, lh, rh);
            hay.sort();
            // needles: some drawn from the haystack, some from their own frame
            let mut needles = draw(&mut rng, 100, ln, rn);
            needles.extend((0..100).map(|_| hay[rng.below(300) as usize]));
            if Int::from_i128s(&needles).is_err() {
                continue; // the two frames together spread past 64 bits: not one column
            }
            let want = ranges(&hay, &needles);
            for h in encodings(&hay).into_iter().take(3) {
                for nd in encodings(&needles).into_iter().take(2) {
                    let out = run("input find", Value::Prod(vec![one_list(nd), one_list(h.clone())]));
                    let Value::List(_, inner) = out else { panic!() };
                    let Value::Prod(cols) = *inner else { panic!() };
                    let (lo, hi) = (cols[0].clone(), cols[1].clone());
                    let (Value::Prim(_), Value::Prim(_)) = (&lo, &hi) else { panic!() };
                    let lo = show(&lo);
                    let hi = show(&hi);
                    let want_lo = format!("{:?}", want.iter().map(|r| r.0).collect::<Vec<_>>());
                    let want_hi = format!("{:?}", want.iter().map(|r| r.1).collect::<Vec<_>>());
                    assert_eq!((lo, hi), (want_lo, want_hi), "hay {lh}+{rh}, needles {ln}+{rn}");
                }
            }
        }
    }
}

#[test]
fn codec_round_trips_and_views() {
    let mut rng = Rng(3);
    for &(lo, range) in FRAMES {
        let vals = draw(&mut rng, 777, lo, range);
        for v in encodings(&vals) {
            let nested = Value::List(Bounds::offsets(vec![300, 777]), Box::new(Value::Prod(vec![v.clone(), Value::u64(vec![1; 777])])));
            let mut bytes = Vec::new();
            write_to(&nested, &mut bytes).unwrap();
            assert_eq!(bytes.len(), corgi::bytes::length_in_bytes(&nested));
            let (copied, used) = read_from(&bytes).unwrap();
            assert_eq!(used, bytes.len());
            assert_eq!(copied, nested);
            let words = encode_words(&nested);
            let (viewed, _) = read_from_words(&words).unwrap();
            assert_eq!(viewed, nested);
            // the viewed leaf points into the message
            let Value::List(_, inner) = &viewed else { panic!() };
            let Value::Prod(cols) = &**inner else { panic!() };
            let Value::Int(c) = &cols[0] else { panic!() };
            if c.width() != Width::W0 {
                let range = words.as_ptr() as usize..words.as_ptr() as usize + 8 * words.len();
                assert!(range.contains(&(c.buffer_ptr() as usize)), "decoded leaf was copied");
            }
        }
    }
    // a declared span the offsets break is refused, not believed
    let c = Value::int_u64(vec![1, 2, 300]);
    let mut bytes = Vec::new();
    write_to(&c, &mut bytes).unwrap();
    bytes[24] = 5; // span word: 299 -> 5 (low byte)
    bytes[25] = 0;
    assert!(read_from(&bytes).is_err());
}

#[test]
fn hashes_and_equality_ignore_the_encoding() {
    let mut rng = Rng(12);
    for &(lo, range) in FRAMES {
        let vals = draw(&mut rng, 100, lo, range);
        let encs = encodings(&vals);
        let h0 = hash(&encs[0]);
        for e in &encs {
            assert_eq!(hash(e), h0);
            assert_eq!(e, &encs[0]);
        }
        if vals.iter().all(|&v| (0..=u64::MAX as i128).contains(&v)) {
            let u = Value::u64(vals.iter().map(|&v| v as u64).collect());
            assert_eq!(hash(&u), h0, "an integer hashes as the u64 of its value");
        }
    }
}

#[test]
fn select_and_minmax_across_encodings() {
    let a = Value::int_i64(&[-5, 10, 300, 7]);
    let b = Value::int_u64(vec![70_000, 3, 4, 7]);
    let out = run("let (a, b) = input in ((a, b) lt, a, b) select", Value::Prod(vec![a.clone(), b.clone()]));
    assert_eq!(ints(&out), vec![-5, 3, 4, 7]);
    assert_eq!(ints(&run("input min", Value::Prod(vec![a.clone(), b.clone()]))), vec![-5, 3, 4, 7]);
    assert_eq!(ints(&run("input max", Value::Prod(vec![a, b]))), vec![70_000, 10, 300, 7]);
}

#[test]
fn narrowing_reuses_and_picks_widths() {
    let c = Int::from_u64s((0..1000u64).map(|i| 1 << 40 | i).collect());
    assert_eq!((c.width(), c.base()), (Width::W16, 1 << 40));
    let wide = Int::adopt_u64s((0..1000u64).collect());
    assert_eq!(wide.width(), Width::W64);
    let n = wide.narrow();
    assert_eq!((n.width(), n.span()), (Width::W16, 999));
    assert_eq!(n, wide);
}

#[test]
fn merges_survey_integers_as_their_values() {
    // the merge kernel over two sorted integer columns in different encodings reports what it
    // reports for the same values held as u64 leaves.
    let mut rng = Rng(31);
    for &(lo, range) in &FRAMES[..6] {
        let mut a = draw(&mut rng, 500, lo.max(0), range);
        let mut b = draw(&mut rng, 400, lo.max(0) + range as i128 / 3, range);
        a.sort();
        b.sort();
        let u = |v: &[i128]| Value::u64(v.iter().map(|&x| x as u64).collect());
        let want = corgi::arrange::survey_groups(&u(&a), &u(&b));
        for ea in encodings(&a) {
            for eb in encodings(&b) {
                assert_eq!(corgi::arrange::survey_groups(&ea, &eb), want);
                let pair = |x: Value, y: &[i128]| Value::Prod(vec![x, u(y)]);
                assert_eq!(
                    corgi::arrange::survey_groups(&pair(ea.clone(), &a), &pair(eb.clone(), &b)),
                    corgi::arrange::survey_groups(&pair(u(&a), &a), &pair(u(&b), &b))
                );
            }
        }
    }
}
