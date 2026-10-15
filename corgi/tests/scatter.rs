//! `scatter_<op>`: per row, one `(id, total)` pair per distinct id, in id order. These check it
//! against a direct reference on random rows whose ids take each of the kernel's three numberings
//! (a range, a hash table, a sort), against `group` and a fold, and on the cases its definition
//! names: element order, units that count, products, empty rows, and the errors.

use corgi::{show, Program, Value};
use std::collections::BTreeMap;

struct Rng(u64);
impl Rng {
    fn below(&mut self, m: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % m
    }
}

fn run(src: &str, input: Value) -> Value {
    Program::compile_ml(src).unwrap_or_else(|e| panic!("{src}: {e}")).run(input)
}

fn list(ends: &[usize], v: Value) -> Value {
    Value::List(ends.to_vec().into(), Box::new(v))
}

/// each op's reduction, directly: per row, a map from id to the fold of its values.
#[allow(clippy::type_complexity)]
fn reference(op: &str, ends: &[usize], ids: &[i64], vals: &[i64]) -> (Vec<usize>, Vec<i64>, Vec<i64>) {
    let (unit, f): (i64, fn(i64, i64) -> i64) = match op {
        "add" => (0, i64::wrapping_add),
        "mul" => (1, i64::wrapping_mul),
        "min" => (i64::MAX, i64::min),
        "max" => (i64::MIN, i64::max),
        _ => (0, |a, x| a | (x != 0) as i64),
    };
    let (mut out_ends, mut out_ids, mut out_vals) = (Vec::new(), Vec::new(), Vec::new());
    let mut start = 0;
    for &end in ends {
        let mut m: BTreeMap<i64, i64> = BTreeMap::new();
        for k in start..end {
            let a = m.entry(ids[k]).or_insert(unit);
            *a = f(*a, vals[k]);
        }
        for (id, v) in m {
            out_ids.push(id);
            out_vals.push(v);
        }
        out_ends.push(out_ids.len());
        start = end;
    }
    (out_ends, out_ids, out_vals)
}

#[test]
fn scatter_matches_a_direct_reduction() {
    let mut rng = Rng(0x2545F4914F6CDD1D);
    for trial in 0..120 {
        let (mut ends, mut ids, mut vals) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..1 + rng.below(5) {
            let len = if rng.below(3) == 0 { 100 + rng.below(300) } else { rng.below(9) };
            let spread = rng.below(3);
            for _ in 0..len {
                ids.push(match spread {
                    0 => rng.below(12) as i64 - 3,                    // a range
                    1 => rng.below(5000) as i64 * 1_000_003 - 7,      // spread, few: a hash table
                    _ => (rng.below(u64::MAX) as i64) / 3,             // spread over i64
                });
                vals.push(rng.below(256) as i64 - if trial % 2 == 0 { 0 } else { 128 });
            }
            ends.push(ids.len());
        }
        let as_bytes = vals.iter().all(|&v| (0..256).contains(&v));
        let leaf = if as_bytes { Value::u8(vals.iter().map(|&v| v as u8).collect()) } else { Value::i64(vals.clone()) };
        let input = Value::Prod(vec![list(&ends, Value::i64(ids.clone())), list(&ends, leaf)]);
        for op in ["add", "mul", "min", "max", "any"] {
            let (we, wi, wv) = reference(op, &ends, &ids, &vals);
            let wv = if op == "any" { Value::u8(wv.iter().map(|&x| x as u8).collect()) } else { Value::i64(wv) };
            let want = list(&we, Value::Prod(vec![Value::i64(wi), wv]));
            assert_eq!(run(&format!("input scatter_{op}"), input.clone()), want, "trial {trial}, scatter_{op}");
        }
    }
}

/// past the hash table's limit (64K distinct ids spread wide), ids are numbered by sorting.
#[test]
fn many_spread_ids_are_numbered_by_sorting() {
    let mut rng = Rng(7);
    let pool: Vec<i64> = (0..80_000).map(|_| rng.below(u64::MAX) as i64 / 2).collect();
    let ids: Vec<i64> = (0..200_000).map(|_| pool[rng.below(80_000) as usize]).collect();
    let vals: Vec<i64> = (0..200_000).map(|_| rng.below(1000) as i64).collect();
    let ends = [ids.len()];
    let (we, wi, wv) = reference("add", &ends, &ids, &vals);
    let input = Value::Prod(vec![list(&ends, Value::i64(ids)), list(&ends, Value::i64(vals))]);
    assert_eq!(run("input scatter_add", input), list(&we, Value::Prod(vec![Value::i64(wi), Value::i64(wv)])));
}

/// `scatter_add` gives what `group` and `fold_add` give, without moving the rows.
#[test]
fn scatter_add_is_group_then_fold() {
    let mut rng = Rng(11);
    for keys in [10u64, 3000, 1 << 40] {
        let ids: Vec<i64> = (0..20_000).map(|_| (rng.below(keys) as i64) - 5).collect();
        let vals: Vec<i64> = (0..20_000).map(|_| rng.below(100) as i64).collect();
        let input = Value::Prod(vec![list(&[20_000], Value::i64(ids)), list(&[20_000], Value::i64(vals))]);
        assert_eq!(
            show(&run("let (i, v) = input in (i, v) scatter_add", input.clone())),
            show(&run("let (i, v) = input in (i, v) zip group map ((k, vs) -> (k, vs fold_add))", input)),
            "{keys} keys",
        );
    }
}

#[test]
fn scatter_as_defined() {
    let ids = list(&[5, 5, 8], Value::i64(vec![3, -1, 3, 0, -1, 7, 7, 7]));
    let ints = list(&[5, 5, 8], Value::i64(vec![10, 20, 30, 40, 50, 1, 2, 3]));
    // one pair per distinct id, in id order; an empty row has none
    assert_eq!(show(&run("input scatter_add", Value::Prod(vec![ids.clone(), ints.clone()]))), "List ends=[3, 3, 4] <([-1, 0, 3, 7], [70, 40, 40, 6])>");
    assert_eq!(show(&run("input scatter_min", Value::Prod(vec![ids.clone(), ints.clone()]))), "List ends=[3, 3, 4] <([-1, 0, 3, 7], [20, 40, 10, 1])>");
    // a unit field counts, and a product's fields are reduced each on its own
    let src = "let (i, v) = input in (i, v map (x -> ((), x, (x, 2) mul))) scatter_max";
    assert_eq!(show(&run(src, Value::Prod(vec![ids.clone(), ints]))), "List ends=[3, 3, 4] <([-1, 0, 3, 7], ([2, 1, 2, 3], [50, 40, 30, 3], [100, 80, 60, 6]))>");
    // a Float sum adds in element order: 1e16, then -1e16, then 1.0 is 1.0, not 0.0
    let floats = list(&[5, 5, 8], Value::f64(vec![1e16, 0.5, -1e16, 2.0, 0.25, 1.0, 1.0, 1.0]));
    let sums = run("input scatter_add", Value::Prod(vec![ids.clone(), floats]));
    let ids3 = list(&[3], Value::i64(vec![3, 3, 3]));
    assert_eq!(show(&run("input scatter_add", Value::Prod(vec![ids3, list(&[3], Value::f64(vec![1e16, -1e16, 1.0]))]))), "List ends=[1] <([3], [1.0])>");
    assert_eq!(show(&sums), "List ends=[3, 3, 4] <([-1, 0, 3, 7], [0.75, 2.0, 0.0, 3.0])>");
    // the ids and values pair up by position; the longer list's extra elements are dropped
    let short = list(&[2, 2, 2], Value::i64(vec![5, 5]));
    let long = list(&[3, 3, 4], Value::i64(vec![1, 2, 3, 4]));
    assert_eq!(show(&run("input scatter_add", Value::Prod(vec![short, long]))), "List ends=[1, 1, 1] <([5], [3])>");
}

#[test]
fn scatter_errors() {
    let ids = list(&[2], Value::i64(vec![0, 1]));
    let err = |src: &str, v: Value| Program::compile_ml(src).unwrap().shape(&corgi::shape_of_value(&v)).unwrap_err();
    let floats = Value::Prod(vec![ids.clone(), list(&[2], Value::f64(vec![1.0, 2.0]))]);
    assert!(err("input scatter_any", floats).contains("a mask"));
    let lists = Value::Prod(vec![ids, list(&[2], list(&[1, 2], Value::i64(vec![1, 2])))]);
    assert!(err("input scatter_add", lists).contains("expected leaves, units, or a product of them"));
}
