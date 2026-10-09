//! Three small text programs: counting a pattern's occurrences, the decimal digits of an Int that
//! is not negative, and the three most frequent words.

use crate::common::{bytes_col, run_case, Cfg, Rng};
use corgi::{Bounds, Value};

fn substring_count(text: &[u8], pat: &[u8]) -> i64 {
    if pat.is_empty() {
        return text.len() as i64 + 1;
    }
    text.windows(pat.len()).filter(|w| *w == pat).count() as i64
}

fn word_topk(text: &[u8]) -> Vec<(Vec<u8>, i64)> {
    let mut counts = std::collections::BTreeMap::<&[u8], i64>::new();
    for w in text.split(|&b| b == b' ').filter(|w| !w.is_empty()) {
        *counts.entry(w).or_default() += 1;
    }
    let mut v: Vec<(&[u8], i64)> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    v.into_iter().take(3).map(|(w, c)| (w.to_vec(), c)).collect()
}

pub fn run(cfg: &Cfg) {
    let mut rng = Rng::new(3);

    // substring_count: texts of 0-40 bytes over 3 letters, patterns of 0-3 bytes.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..cfg.rows)
        .map(|_| {
            let (n, m) = (rng.below(41) as usize, rng.below(4) as usize);
            (rng.string(n, b"abc"), rng.string(m, b"abc"))
        })
        .collect();
    let input = Value::Prod(vec![
        bytes_col(&pairs.iter().map(|p| &p.0).collect::<Vec<_>>()),
        bytes_col(&pairs.iter().map(|p| &p.1).collect::<Vec<_>>()),
    ]);
    let expected = Value::i64(pairs.iter().map(|(t, p)| substring_count(t, p)).collect());
    let rust = || pairs.iter().map(|(t, p)| substring_count(t, p)).collect::<Vec<i64>>();
    run_case(cfg, "substring_count", "text 0-40 bytes, pattern 0-3, 3 letters", include_str!("../../algorithms/substring_count.col"), input.clone(), expected.clone(), rust);
    run_case(cfg, "substring_count_ref", "text 0-40 bytes, pattern 0-3, 3 letters", include_str!("../../algorithms/substring_count_ref.col"), input, expected, rust);

    // itoa: non-negative Ints of every magnitude.
    let xs: Vec<i64> = (0..cfg.rows).map(|_| (rng.next() >> 1 >> rng.below(63)) as i64).collect();
    let input = Value::i64(xs.clone());
    let expected = bytes_col(&xs.iter().map(|x| x.to_string().into_bytes()).collect::<Vec<_>>());
    let rust = || xs.iter().map(|x| x.to_string().into_bytes()).collect::<Vec<_>>();
    run_case(cfg, "itoa", "non-negative ints of every magnitude", include_str!("../../algorithms/itoa.col"), input, expected, rust);

    // word_topk: 0-12 words of 1-3 letters over 3 letters, single spaces.
    let texts: Vec<Vec<u8>> = (0..cfg.rows)
        .map(|_| {
            let words: Vec<Vec<u8>> = (0..rng.below(13)).map(|_| { let l = 1 + rng.below(3) as usize; rng.string(l, b"abc") }).collect();
            words.join(&b' ')
        })
        .collect();
    let input = bytes_col(&texts);
    let tops: Vec<Vec<(Vec<u8>, i64)>> = texts.iter().map(|t| word_topk(t)).collect();
    let mut ends = Vec::new();
    let mut words = Vec::new();
    let mut counts = Vec::new();
    for t in &tops {
        for (w, c) in t {
            words.push(w.clone());
            counts.push(*c);
        }
        ends.push(words.len());
    }
    let expected = Value::List(Bounds::offsets(ends), Box::new(Value::Prod(vec![bytes_col(&words), Value::i64(counts)])));
    let rust = || texts.iter().map(|t| word_topk(t)).collect::<Vec<_>>();
    run_case(cfg, "word_topk", "0-12 words of 1-3 letters over 3 letters", include_str!("../../algorithms/word_topk.col"), input, expected, rust);
}
