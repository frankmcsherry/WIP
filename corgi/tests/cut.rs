//! `cut` against its definition: a piece starts at a row's first element and at each marked
//! element after it. Rows of every length around the kernel's word and short-row edges, masks
//! from empty to full, so both the plain pass and the pass that skips unmarked words are checked.

use corgi::{Program, Value};

/// a deterministic LCG.
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

/// the definition, an element at a time: each row's piece ends, and each row's end among them.
fn pieces(ends: &[usize], mask: &[u8]) -> (Vec<usize>, Vec<usize>) {
    let (mut outer, mut inner, mut start) = (Vec::new(), Vec::new(), 0);
    for &end in ends {
        for (k, &m) in mask.iter().enumerate().take(end).skip(start + 1) {
            if m != 0 {
                inner.push(k);
            }
        }
        if end > start {
            inner.push(end);
        }
        outer.push(inner.len());
        start = end;
    }
    (outer, inner)
}

fn check(ends: Vec<usize>, mask: Vec<u8>, what: &str) {
    let n = mask.len();
    assert_eq!(ends.last().copied().unwrap_or(0), n, "{what}: the rows cover the mask");
    let vals = Value::i64((0..n as i64).collect());
    let (outer, inner) = pieces(&ends, &mask);
    let want = Value::List(outer.into(), Box::new(Value::List(inner.into(), Box::new(vals.clone()))));
    let cut = Program::compile_ml("input cut").expect("cut");
    // the mask as bytes (what a comparison writes), and as wide integers
    let wide = Value::i64(mask.iter().map(|&m| m as i64 * 1_000_003).collect());
    for (storage, m) in [("bytes", Value::u8(mask.clone())), ("wide", wide)] {
        let input = Value::List(ends.clone().into(), Box::new(Value::Prod(vec![m, vals.clone()])));
        assert_eq!(cut.run(input), want, "{what}, mask as {storage}");
    }
}

#[test]
fn cut_matches_its_definition() {
    let mut rng = Rng(0x5eed);
    // one mark per `one_in` elements; 0 is no marks at all. Marks are any nonzero byte.
    for one_in in [0u64, 1, 2, 6, 20, 40, 100, 1000] {
        let mut mask = |n: usize| -> Vec<u8> {
            (0..n).map(|_| if one_in != 0 && rng.below(one_in) == 0 { 1 + rng.below(255) as u8 } else { 0 }).collect()
        };
        // one row of each length across the edges of the eight-wide words
        for n in (0..40).chain([63, 64, 65, 255, 256, 257, 4096]) {
            check(vec![n], mask(n), &format!("one row of {n}, one in {one_in}"));
        }
        // many rows: empty ones, single elements, and lengths either side of the short-row cutoff
        for max in [1usize, 3, 8, 17, 40, 200] {
            let mut rows = Rng(max as u64);
            let mut ends = Vec::new();
            let mut end = 0;
            for _ in 0..300 {
                end += rows.below(max as u64 + 1) as usize;
                ends.push(end);
            }
            check(ends, mask(end), &format!("rows of up to {max}, one in {one_in}"));
        }
    }
}

/// A mark on a row's first element starts no new piece, in any row, and neither does a column of
/// no rows or of empty rows.
#[test]
fn cut_edges() {
    check(vec![], vec![], "no rows");
    check(vec![0, 0, 0], vec![], "empty rows");
    check(vec![4, 4, 8], vec![1, 0, 0, 0, 1, 0, 0, 0], "marks on first elements only");
    check(vec![20, 40], vec![1; 40], "every element marked");
    // sparse overall, with one dense row among long unmarked ones
    let mut mask = vec![0u8; 4000];
    mask[2000..2040].fill(7);
    check(vec![2000, 2040, 4000], mask, "one dense row in a sparse column");
}
