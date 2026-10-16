//! `scatter` by hashes where keys share them: the hash cut to two bits, so most distinct keys
//! collide, and the rows that collide are numbered by sorting their keys.

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
    let got = scatter_by_hash(&kb, &keys, &kb, Value::Prod(vec![Value::u8(vec![1; 505]), vals.clone()]), Red::Add, &short, false).unwrap();
    // the same totals as `group` and the folds
    let input = Value::Prod(vec![Value::List(kb.clone(), Box::new(keys.clone())), Value::List(kb.clone(), Box::new(vals))]);
    let grouped = Program::compile_ml("let (k, v) = input in (k, v) zip group map ((k, vs) -> (k, (vs len, vs fold_add)))").unwrap().run(input);
    let sorted = Program::compile_ml("input sort").unwrap().run(got);
    assert_eq!(crate::show(&sorted), crate::show(&grouped));
}
