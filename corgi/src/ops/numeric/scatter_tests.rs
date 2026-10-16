//! `hash_scatter` where keys share hashes: the hash cut to two bits, so most distinct keys collide,
//! and the rows that collide are numbered by sorting them by (hash, key).

use super::*;
use crate::frontend::Program;
use crate::value::Bounds;

fn strings(words: &[String]) -> Value {
    let (mut ends, mut bytes) = (Vec::new(), Vec::new());
    for w in words {
        bytes.extend(w.bytes());
        ends.push(bytes.len());
    }
    Value::List(ends.into(), Box::new(Value::u8(bytes)))
}

#[test]
fn keys_that_share_a_hash_are_told_apart() {
    let short = |v: &Value| crate::hash::hash(v).into_iter().map(|h| h >> 62).collect::<Vec<u64>>();
    // two rows: one of strings drawn from 40, one empty, one of a single repeated string
    let mut words: Vec<String> = (0..500).map(|k| format!("s{}", (k * 7919) % 40)).collect();
    words.extend((0..5).map(|_| "only".to_string()));
    let keys = strings(&words);
    let kb: Bounds = vec![500, 500, 505].into();
    let vals = Value::i64((0..505).map(|k| (k % 9) as i64 - 4).collect());
    let got = hash_scatter_rows(&kb, &keys, &kb, Value::Prod(vec![Value::Unit(505), vals.clone()]), Red::Add, &short).unwrap();
    // the same totals as `group` and the folds, once put in key order
    let input = Value::Prod(vec![Value::List(kb.clone(), Box::new(keys.clone())), Value::List(kb.clone(), Box::new(vals))]);
    let grouped = Program::compile_ml("let (k, v) = input in (k, v) zip group map ((k, vs) -> (k, (vs len, vs fold_add)))").unwrap().run(input);
    let sorted = Program::compile_ml("input sort").unwrap().run(got.clone());
    assert_eq!(crate::show(&sorted), crate::show(&grouped));
    // and in the order of the cut-short hashes, keys that share one in key order
    let Value::List(ends, pairs) = got else { panic!("a list") };
    let Value::Prod(cols) = *pairs else { panic!("pairs") };
    let hs = short(&cols[0]);
    let mut start = 0;
    for end in ends.ends() {
        for k in start + 1..end {
            let order = (hs[k - 1], crate::ops::cmp::order::compare_at(&cols[0], k - 1, &cols[0], k));
            assert!(order.0 < hs[k] || (order.0 == hs[k] && order.1 == std::cmp::Ordering::Less), "row order at {k}");
        }
        start = end;
    }
}
