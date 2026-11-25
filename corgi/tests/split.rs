//! `split` at a mask against its definition: the marked elements dropped, a piece closed at each
//! and at each row's end. Elements of several shapes, rows around the kernel's word and short-row
//! edges, and masks from empty to full, so both the plain pass and the pass for few marks run.

use corgi::arrange::gather;
use corgi::{Program, Value};

/// a deterministic LCG.
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

/// `n` elements of each shape tried: integers, bytes, pairs, and lists of up to three integers.
fn shapes(n: usize) -> Vec<(&'static str, Value)> {
    let ints: Vec<i64> = (0..n as i64).map(|i| i * 7 - 3).collect();
    let mut ends = Vec::new();
    let mut flat = Vec::new();
    for i in 0..n {
        flat.extend((0..i % 4).map(|j| (i * 10 + j) as i64));
        ends.push(flat.len());
    }
    vec![
        ("integers", Value::i64(ints.clone())),
        ("bytes", Value::u8((0..n).map(|i| i as u8).collect())),
        ("pairs", Value::Prod(vec![Value::i64(ints), Value::u8((0..n).map(|i| (i % 5) as u8).collect())])),
        ("lists", Value::List(ends.into(), Box::new(Value::i64(flat)))),
    ]
}

fn check(ends: Vec<usize>, mask: Vec<u8>, what: &str) {
    let n = mask.len();
    assert_eq!(ends.last().copied().unwrap_or(0), n, "{what}: the rows cover the mask");
    // the definition, an element at a time
    let (mut outer, mut inner, mut kept, mut start) = (Vec::new(), Vec::new(), Vec::new(), 0);
    for &end in &ends {
        for (k, &m) in mask.iter().enumerate().take(end).skip(start) {
            if m != 0 { inner.push(kept.len()) } else { kept.push(k) }
        }
        inner.push(kept.len());
        outer.push(inner.len());
        start = end;
    }
    let split = Program::compile_ml("input split").expect("split");
    for (shape, vals) in shapes(n) {
        let want = Value::List(outer.clone().into(), Box::new(Value::List(inner.clone().into(), Box::new(gather(&vals, &kept)))));
        let input = Value::List(ends.clone().into(), Box::new(Value::Prod(vec![Value::u8(mask.clone()), vals])));
        assert_eq!(split.run(input), want, "{what}, elements as {shape}");
    }
}

#[test]
fn split_matches_its_definition() {
    let mut rng = Rng(0x5eed);
    // one mark per `one_in` elements; 0 is no marks at all. Marks are any nonzero byte.
    for one_in in [0u64, 1, 2, 6, 20, 40, 100, 1000] {
        let mut mask = |n: usize| -> Vec<u8> {
            (0..n).map(|_| if one_in != 0 && rng.below(one_in) == 0 { 1 + rng.below(255) as u8 } else { 0 }).collect()
        };
        for n in (0..36).chain([63, 64, 65, 255, 256, 257, 2000]) {
            check(vec![n], mask(n), &format!("one row of {n}, one in {one_in}"));
        }
        for max in [1usize, 3, 8, 17, 40, 200] {
            let mut rows = Rng(max as u64);
            let (mut ends, mut end) = (Vec::new(), 0);
            for _ in 0..200 {
                end += rows.below(max as u64 + 1) as usize;
                ends.push(end);
            }
            check(ends, mask(end), &format!("rows of up to {max}, one in {one_in}"));
        }
    }
}

/// n marks make n+1 pieces: marks side by side and at a row's ends leave empty pieces, and a row
/// of no elements is one empty piece.
#[test]
fn split_edges() {
    check(vec![], vec![], "no rows");
    check(vec![0, 0, 0], vec![], "empty rows");
    check(vec![4, 4, 8], vec![1, 0, 0, 1, 1, 1, 0, 0], "marks at row ends and side by side");
    check(vec![20, 40], vec![1; 40], "every element marked");
    // few marks overall, all in one short row among long unmarked ones
    let mut mask = vec![0u8; 4000];
    mask[2000..2040].fill(7);
    check(vec![2000, 2040, 4000], mask, "one dense row in a sparse column");
}

/// `s split ","` is the mask set where the byte is, and nothing else: a delimiter of another
/// length is refused.
#[test]
fn split_at_a_byte_is_split_at_its_mask() {
    let text = "a,,b|,x,||no commas|,";
    let ends: Vec<usize> = text.match_indices('|').map(|(i, _)| i).chain([text.len()]).collect();
    let rows = Value::List(ends.into(), Box::new(Value::u8(text.as_bytes().to_vec())));
    let word = Program::compile_ml("input split \",\"").unwrap();
    let mask = Program::compile_ml("input map (c -> ((c, 44) eq, c)) split").unwrap();
    assert_eq!(word.run(rows.clone()), mask.run(rows));
    assert!(Program::compile_ml("input split \", \"").is_err());
    assert!(Program::compile_ml("input split \"\"").is_err());
}
