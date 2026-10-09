//! Lists order lexicographically: `sort`, `sort_limit` and `dedup` agree with Rust's slice order
//! on strings, nested lists and lists of pairs.

use corgi::{Bounds, Program, Value};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// one row holding all of `strs`, as `List<List<U8>>`.
fn one_row(strs: &[Vec<u8>]) -> Value {
    let mut ends = Vec::new();
    let mut bytes = Vec::new();
    for s in strs {
        bytes.extend_from_slice(s);
        ends.push(bytes.len());
    }
    let inner = Value::List(ends.into(), Box::new(Value::u8(bytes)));
    Value::List(Bounds::offsets(vec![strs.len()]), Box::new(inner))
}

fn rows_of(v: &Value) -> Vec<Vec<u8>> {
    let Value::List(_, inner) = v else { panic!("a list") };
    let Value::List(b, bytes) = &**inner else { panic!("a list of strings") };
    let bytes = bytes.as_u8("bytes").unwrap();
    let ends = b.to_vec();
    (0..ends.len()).map(|i| bytes[if i == 0 { 0 } else { ends[i - 1] }..ends[i]].to_vec()).collect()
}

#[test]
fn strings_sort_lexicographically() {
    let mut rng = Rng(7);
    for trial in 0..40 {
        let n = 1 + rng.below(300) as usize;
        let alphabet: &[u8] = if trial % 2 == 0 { b"\x00ab\xff" } else { b"ab" };
        let strs: Vec<Vec<u8>> = (0..n)
            .map(|_| (0..rng.below(if trial % 3 == 0 { 20 } else { 5 })).map(|_| alphabet[rng.below(alphabet.len() as u64) as usize]).collect())
            .collect();
        let mut want = strs.clone();
        want.sort();
        let arg = one_row(&strs);
        let sorted = Program::compile_ml("input sort").unwrap().run(arg.clone());
        assert_eq!(rows_of(&sorted), want, "sort, trial {trial}");
        for k in [1usize, 3, 10, 1000] {
            let got = Program::compile_ml(&format!("input sort_limit {k}")).unwrap().run(arg.clone());
            assert_eq!(rows_of(&got), want[..k.min(n)].to_vec(), "sort_limit {k}, trial {trial}");
        }
        let mut dedup = want.clone();
        dedup.dedup();
        let got = Program::compile_ml("input dedup").unwrap().run(arg.clone());
        assert_eq!(rows_of(&got), dedup, "dedup, trial {trial}");
    }
}

/// rows of `List<U64>` lists, several rows: the general (non-byte) path, per-row blocks.
#[test]
fn number_lists_sort_lexicographically_per_row() {
    let mut rng = Rng(11);
    for trial in 0..40 {
        let rows: Vec<Vec<Vec<u64>>> = (0..1 + rng.below(5))
            .map(|_| (0..rng.below(80)).map(|_| (0..rng.below(6)).map(|_| rng.below(3)).collect()).collect())
            .collect();
        let (mut outer, mut inner, mut vals) = (Vec::new(), Vec::new(), Vec::new());
        for row in &rows {
            for l in row {
                vals.extend_from_slice(l);
                inner.push(vals.len());
            }
            outer.push(inner.len());
        }
        let arg = Value::List(Bounds::offsets(outer), Box::new(Value::List(inner.into(), Box::new(Value::u64(vals)))));
        let flat = |v: &Value| -> Vec<Vec<u64>> {
            let Value::List(_, inner) = v else { panic!("a list") };
            let Value::List(b, xs) = &**inner else { panic!("lists") };
            let p = xs.as_u64("numbers").unwrap();
            let ends = b.to_vec();
            (0..ends.len()).map(|i| (if i == 0 { 0 } else { ends[i - 1] }..ends[i]).map(|j| p[j]).collect()).collect()
        };
        let k = 4;
        let (mut want_sort, mut want_lim) = (Vec::new(), Vec::new());
        for row in &rows {
            let mut r = row.clone();
            r.sort();
            want_lim.extend(r.iter().take(k).cloned());
            want_sort.extend(r);
        }
        let got = Program::compile_ml("input sort").unwrap().run(arg.clone());
        assert_eq!(flat(&got), want_sort, "sort, trial {trial}");
        let got = Program::compile_ml(&format!("input sort_limit {k}")).unwrap().run(arg.clone());
        assert_eq!(flat(&got), want_lim, "sort_limit, trial {trial}");
    }
}
