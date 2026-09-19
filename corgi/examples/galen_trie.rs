//! GALEN in corgi, trie-shaped: the layered driver.
//!
//! `cargo run --release --example galen_trie -- /Users/mcsherry/Projects/datasets/flowlog/galen`
//!
//! Where `examples/galen.rs` keeps a rule's partial tuples as flat rows and probes them one tuple
//! at a time, this driver keeps them the way datatoad does: as a TRIE in the order of the atom it
//! is about to consult — one sorted column per layer, every distinct prefix once — and intersects
//! trie against trie one layer at a time. A relation's index for a column permutation is a trie
//! whose layers below the first are columns of fat `Ref`s into that layer's flat keys, so walking
//! down it moves references, never keys. The intersection at each layer is `find`, which on sorted
//! needles is the galloping merge of one column against one column; the counts of the WCO step are
//! the lengths of the matched leaf lists, read through the refs; the extension clones exactly the
//! leaves it produces. Row ids ride in the leaves so the per-row argmin across candidates is a
//! merge on ids rather than a scatter (corgi's one structural disadvantage here: datatoad writes
//! its counts into a mutable notes column).
use corgi::{Program, Value};
use std::collections::HashMap;
use std::time::Instant;

// ---------------------------------------------------------------------------------------------
// values

fn one_row(cols: Vec<Vec<u64>>) -> Value {
    let n = cols.first().map_or(0, |c| c.len());
    Value::List(vec![n].into(), Box::new(Value::Prod(cols.into_iter().map(Value::u64).collect())))
}

/// rows of a one-row list
fn count(v: &Value) -> usize {
    match v {
        Value::List(_, vals) => vals.len(),
        other => other.len(),
    }
}

/// a one-row `List<X>` as its payload column (the `X` rows themselves)
fn unnest(v: Value) -> Value {
    match v {
        Value::List(_, vals) => *vals,
        other => panic!("unnest: expected a list, got {}", corgi::show(&other)),
    }
}

/// a column of `n` rows as a one-row `List` of them
fn nest(v: Value) -> Value {
    let n = v.len();
    Value::List(vec![n].into(), Box::new(v))
}

fn unfail(v: Value) -> Value {
    match v {
        Value::Sum(_, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_)) => {
            if let Value::Unit(n) = lanes[1] {
                assert_eq!(n, 0, "a corgi stage failed on {n} rows");
            }
            lanes.into_iter().next().unwrap()
        }
        other => other,
    }
}

fn load(path: &str, arity: usize) -> Value {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let mut cols = vec![Vec::new(); arity];
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        for (i, f) in line.split(',').enumerate() {
            cols[i].push(f.trim().parse::<u64>().unwrap());
        }
    }
    one_row(cols)
}

struct Progs(HashMap<String, Program>);
impl Progs {
    fn run(&mut self, text: &str, input: Value) -> Value {
        let p = self.0.entry(text.to_string()).or_insert_with(|| {
            Program::compile_ml(text).unwrap_or_else(|e| panic!("compile {text:?}: {e}"))
        });
        unfail(p.run_partial(input))
    }
    fn run2(&mut self, text: &str, a: Value, b: Value) -> Value {
        self.run(text, Value::Prod(vec![a, b]))
    }
}

fn tuple_expr(cols: &[usize]) -> String {
    if cols.len() == 1 {
        format!("t.{}", cols[0])
    } else {
        format!("({})", cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", "))
    }
}

/// attach a fresh id column (the last column) to flat tuples
fn with_ids(progs: &mut Progs, tuples: Value, arity: usize) -> Value {
    let n = count(&tuples);
    let ids = Value::List(vec![n].into(), Box::new(Value::u64((0..n as u64).collect())));
    let fields: Vec<String> = (0..arity).map(|i| format!("p.0.{i}")).collect();
    let text = format!("let (t, i) = input in (t, i) zip map (p -> ({}, p.1))", fields.join(", "));
    progs.run(&text, Value::Prod(vec![tuples, ids]))
}

// ---------------------------------------------------------------------------------------------
// index tries: one per (batch, key columns, value columns)

/// a relation batch as a trie in key order. `keys[0]` is a one-row `List<K1>`; `keys[l]` for
/// `l >= 1` is a one-row list of fat refs, one per layer-`l` item, each a `Ref<List<K(l+1)>>`;
/// `vals` is nested per parent: for depth 1 a one-row `List<Ref<List<V>>>` (one ref per key), for
/// depth 2 a one-row `List<List<Ref<List<V>>>>` (per key1, the refs of its key2 items).
struct Index {
    keys: Vec<Value>,
    vals: Value,
}

fn build_index(progs: &mut Progs, tuples: &Value, key: &[usize], val: &[usize]) -> Index {
    match key.len() {
        1 => {
            let kv = progs.run(&format!("input map (t -> ({}, {})) group transpose", tuple_expr(key), tuple_expr(val)), tuples.clone());
            let Value::Prod(mut c) = kv else { panic!() };
            let children = c.pop().unwrap();
            let keys1 = c.pop().unwrap();
            let vals = progs.run("input map (l -> l ref)", children);
            Index { keys: vec![keys1], vals }
        }
        2 => {
            let kv = progs.run(
                &format!("input map (t -> (t.{}, (t.{}, {}))) group transpose", key[0], key[1], tuple_expr(val)),
                tuples.clone(),
            );
            let Value::Prod(mut c) = kv else { panic!() };
            let rest = c.pop().unwrap(); // List<List<(K2, V)>>
            let keys1 = c.pop().unwrap();
            // per key1: group its (K2, V) list by K2 -> (List<K2>, List<List<V>>)
            let inner = progs.run("input map (l -> l group transpose) transpose", rest);
            let Value::Prod(mut c) = inner else { panic!() };
            let vals_nested = c.pop().unwrap(); // List<List<List<V>>>: per key1, per key2, the V list
            let keys2_lists = c.pop().unwrap(); // List<List<K2>>: per key1, its sorted K2s
            let keys2 = progs.run("input map (l -> l ref)", keys2_lists);
            let vals = progs.run("input map (m -> m map (l -> l ref))", vals_nested);
            Index { keys: vec![keys1, keys2], vals }
        }
        d => panic!("index depth {d} not supported"),
    }
}

struct Batch {
    tuples: Value,
    indexes: HashMap<(Vec<usize>, Vec<usize>), Index>,
}
impl Batch {
    fn new(tuples: Value) -> Batch {
        Batch { tuples, indexes: HashMap::new() }
    }
    fn index(&mut self, progs: &mut Progs, key: &[usize], val: &[usize]) -> &Index {
        let k = (key.to_vec(), val.to_vec());
        if !self.indexes.contains_key(&k) {
            let ix = build_index(progs, &self.tuples, key, val);
            self.indexes.insert(k.clone(), ix);
        }
        &self.indexes[&k]
    }
}

struct Rel {
    stable: Vec<Batch>,
    recent: Option<Batch>,
}
impl Rel {
    fn total(&self) -> usize {
        self.stable.iter().map(|b| count(&b.tuples)).sum::<usize>() + self.recent.as_ref().map_or(0, |b| count(&b.tuples))
    }
}

// ---------------------------------------------------------------------------------------------
// the salad trie and its intersection with an index trie

/// the matched paths of a salad against an index: aligned one-row lists — per matched leaf-key
/// path, the salad's list of full tuples under it, and the index's `Ref<List<V>>` under it.
struct Matched {
    leaves: Value, // one-row List<List<T>>  (T = the full flat tuple with its id)
    vals: Value,   // one-row List<Ref<List<V>>>
}

/// group flat tuples (with ids) into a trie by `key` (1 or 2 columns); the leaves are the full
/// tuples. `group` is a stable key-only discrimination that yields sorted keys — no `sort` first,
/// which would order every column (the whole profile of the first version was those sorts). Depth 1: `List<(K1, List<T>)>`; depth 2: `List<(K1, List<(K2, List<T>)>)>`.
fn salad_trie(progs: &mut Progs, tuples: Value, key: &[usize]) -> Value {
    match key.len() {
        1 => progs.run(&format!("input map (t -> (t.{}, t)) group", key[0]), tuples),
        2 => progs.run(
            &format!("input map (t -> (t.{}, (t.{}, t))) group map (p -> (p.0, p.1 group))", key[0], key[1]),
            tuples,
        ),
        d => panic!("salad depth {d}"),
    }
}

fn intersect(progs: &mut Progs, salad: Value, ix: &Index) -> Matched {
    // layer 1: distinct salad keys against the index keys — sorted needles, a merge.
    let keys1 = progs.run("input map (p -> p.0)", salad.clone());
    let subs1 = progs.run("input map (p -> p.1)", salad);
    let r1 = progs.run2("let (n, h) = input in (n, h) find", keys1, ix.keys[0].clone());
    let m1 = progs.run("input map (x -> (x.0, x.1) lt)", r1.clone());
    let los1 = progs.run2("let (r, m) = input in (r map (x -> x.0), m) filter", r1, m1.clone());
    let kept1 = progs.run2("let (s, m) = input in (s, m) filter", subs1, m1);
    if ix.keys.len() == 1 {
        let vals = progs.run2("let (i, h) = input in (i, h) gather", los1, ix.vals.clone());
        return Matched { leaves: kept1, vals };
    }
    // layer 2: per matched key1, the salad's K2 list against the index's K2 list (a ref) — one
    // merge per matched key, all at once, rows aligned.
    let idx_k2 = unnest(progs.run2("let (i, h) = input in (i, h) gather", los1.clone(), ix.keys[1].clone())); // M rows of Ref<List<K2>>
    let idx_v2 = unnest(progs.run2("let (i, h) = input in (i, h) gather", los1, ix.vals.clone())); // M rows of List<Ref<List<V>>>
    let needles2 = unnest(progs.run("input map (l -> l map (p -> p.0))", kept1.clone())); // M rows of List<K2>
    let leaves2 = unnest(progs.run("input map (l -> l map (p -> p.1))", kept1)); // M rows of List<List<T>>
    let r2 = progs.run2("let (n, h) = input in (n, h) find", needles2, idx_k2); // M rows of List<(lo,hi)>
    let m2 = progs.run("input map (x -> (x.0, x.1) lt)", r2.clone());
    let los2 = progs.run2("let (r, m) = input in (r map (x -> x.0), m) filter", r2, m2.clone());
    let vals2 = progs.run2("let (i, h) = input in (i, h) gather", los2, idx_v2); // M rows of List<Ref<List<V>>>
    let leaves_m = progs.run2("let (l, m) = input in (l, m) filter", leaves2, m2);
    // flatten the M rows into one row of matched paths (the per-key1 boundaries are not needed).
    Matched { leaves: nest(unnest(leaves_m)), vals: nest(unnest(vals2)) }
}

// ---------------------------------------------------------------------------------------------
// rules

struct Atom {
    rel: &'static str,
    vars: Vec<&'static str>,
}
struct Rule {
    head: Atom,
    body: Vec<Atom>,
}
fn atom(rel: &'static str, vars: &[&'static str]) -> Atom {
    Atom { rel, vars: vars.to_vec() }
}

/// the key/value column plan for consulting atom `j` from tuples over `vars`
struct Plan {
    key_pos: Vec<usize>,   // key columns in the current tuple
    idx_key: Vec<usize>,   // the same variables' positions in the atom
    idx_val: Vec<usize>,   // the atom's other variables' positions
    val_vars: Vec<&'static str>,
    semijoin: bool,
}

fn plan(rule: &Rule, vars: &[&'static str], j: usize) -> Plan {
    let a = &rule.body[j];
    let key_vars: Vec<&str> = vars.iter().copied().filter(|v| a.vars.contains(v)).collect();
    assert!(!key_vars.is_empty(), "cross product in rule {}", rule.head.rel);
    let val_vars: Vec<&'static str> = a.vars.iter().copied().filter(|v| !key_vars.contains(v)).collect();
    let key_pos = key_vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap()).collect();
    let idx_key: Vec<usize> = key_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect();
    let semijoin = val_vars.is_empty();
    let idx_val = if semijoin { vec![idx_key[0]] } else { val_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect() };
    Plan { key_pos, idx_key, idx_val, val_vars, semijoin }
}

/// the batches of atom `j`'s relation that the delta convention allows the stage to see
fn batches<'a>(rels: &'a mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, j: usize) -> Vec<&'a mut Batch> {
    let rel = rels.get_mut(rule.body[j].rel).unwrap();
    let use_recent = j > seed && rel.recent.is_some();
    let mut out: Vec<&mut Batch> = rel.stable.iter_mut().collect();
    if use_recent {
        out.push(rel.recent.as_mut().unwrap());
    }
    out
}

/// per current row (by id), the fan-out an extension by atom `j` would produce, summed over the
/// allowed batches: counts at the matched leaves, expanded over their tuples, aligned by a merge
/// on ids. `n` rows; a row with no match counts 0.
fn fanout(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, tuples: &Value, vars: &[&'static str], j: usize) -> Value {
    let p = plan(rule, vars, j);
    let n = count(tuples);
    let id_col = vars.len(); // the id is the last column of the flat tuple
    let salad = salad_trie(progs, tuples.clone(), &p.key_pos);
    let zeros = Value::List(vec![n].into(), Box::new(Value::u64(vec![0; n])));
    let mut total = zeros.clone();
    let ids_all = Value::List(vec![n].into(), Box::new(Value::u64((0..n as u64).collect())));
    for b in batches(rels, rule, seed, j) {
        let ix = b.index(progs, &p.idx_key, &p.idx_val);
        let m = intersect(progs, salad.clone(), ix);
        // (count, id) per matched tuple, sorted by id
        let pairs = progs.run2(
            &format!(
                "let (l, v) = input in let c = v map (r -> r len) in \
                 (((c, l) zip map (q -> (q.0, q.1) cap_list)) flatten).1 map (q -> (q.1.{id_col}, q.0)) sort"
            ),
            m.leaves,
            m.vals,
        );
        // align to 0..n: ids present take their count (the sentinel 0 at the end covers the rest)
        let per_row = progs.run2(
            "let (ids, pairs) = input in let r = (ids, pairs map (q -> q.0)) find in \
             let cs = (pairs map (q -> q.1), ids map (i -> i lit 0)) append in \
             let m = r map (x -> (x.0, x.1) lt) in \
             ((r map (x -> x.0), cs) gather, m) zip map (p -> (p.0, p.1) mul)",
            ids_all.clone(),
            pairs,
        );
        total = progs.run2("let (a, b) = input in (a, b) zip map (q -> (q.0, q.1) add)", total, per_row);
    }
    total
}

/// join `tuples` (flat, with ids) with atom `j`: new flat tuples over `vars ++ val_vars` (ids
/// re-issued), or None if nothing matched.
fn join_atom(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, tuples: Value, vars: &[&'static str], j: usize) -> Option<(Value, Vec<&'static str>)> {
    let ts = Instant::now();
    let p = plan(rule, vars, j);
    let n_in = count(&tuples);
    let salad = salad_trie(progs, tuples, &p.key_pos);
    let arity = vars.len();
    let vc = p.idx_val.len();
    let mut acc: Option<Value> = None;
    for b in batches(rels, rule, seed, j) {
        let ix = b.index(progs, &p.idx_key, &p.idx_val);
        let m = intersect(progs, salad.clone(), ix);
        let out = if p.semijoin {
            // the matched leaves ARE the result (drop the id, re-issued below)
            let fields: Vec<String> = (0..arity).map(|i| format!("t.{i}")).collect();
            progs.run(&format!("(input flatten).1 map (t -> ({}))", fields.join(", ")), m.leaves)
        } else {
            // cross each leaf list with its clone of the matched values: (t, v) rows, flattened
            let mut fields: Vec<String> = (0..arity).map(|i| format!("q.1.{i}")).collect();
            if vc == 1 { fields.push("q.0".to_string()); } else { for i in 0..vc { fields.push(format!("q.0.{i}")); } }
            progs.run2(
                &format!(
                    "let (l, v) = input in \
                     let rows = ((l, v) zip map (p -> (((p.0, p.1 clone) cap_list) map (w -> (w.1, w.0) cap_list) flatten).1) flatten).1 in \
                     rows map (q -> ({}))",
                    fields.join(", ")
                ),
                m.leaves,
                m.vals,
            )
        };
        acc = Some(match acc {
            None => out,
            Some(prev) => progs.run2("let (a, b) = input in (a, b) append", prev, out),
        });
    }
    let out = acc?;
    let mut new_vars: Vec<&'static str> = vars.to_vec();
    if !p.semijoin {
        new_vars.extend(p.val_vars.iter().copied());
    }
    let out = with_ids(progs, out, new_vars.len());
    if std::env::var("GALEN_TRACE").is_ok() {
        let a = &rule.body[j];
        eprintln!("    {}[seed {seed}] ⋈ {}({}) key {:?}: {} rows -> {} rows in {:.2}s", rule.head.rel, a.rel, a.vars.join(","), p.key_pos, n_in, count(&out), ts.elapsed().as_secs_f64());
    }
    Some((out, new_vars))
}

/// extend flat tuples (with ids) over `vars` by the atoms in `remaining`, to head tuples.
fn extend(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, mut tuples: Value, mut vars: Vec<&'static str>, mut remaining: Vec<usize>) -> Option<Value> {
    loop {
        let bound = remaining.iter().copied().find(|&j| rule.body[j].vars.iter().all(|v| vars.contains(v)));
        let Some(j) = bound else { break };
        remaining.retain(|&k| k != j);
        let (out, nv) = join_atom(progs, rels, rule, seed, tuples, &vars, j)?;
        tuples = out;
        vars = nv;
        if count(&tuples) == 0 {
            return None;
        }
    }
    if remaining.is_empty() {
        let head_pos: Vec<usize> = rule.head.vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap()).collect();
        return Some(progs.run(&format!("input map (t -> {})", tuple_expr(&head_pos)), tuples));
    }
    if remaining.len() == 1 {
        let (out, nv) = join_atom(progs, rels, rule, seed, tuples, &vars, remaining[0])?;
        return extend(progs, rels, rule, seed, out, nv, vec![]);
    }
    assert_eq!(remaining.len(), 2, "WCO step over more than two candidates is not implemented");
    let tw = Instant::now();
    let (ja, jb) = (remaining[0], remaining[1]);
    let ca = fanout(progs, rels, rule, seed, &tuples, &vars, ja);
    let cb = fanout(progs, rels, rule, seed, &tuples, &vars, jb);
    let masks = progs.run2(
        "let (a, b) = input in (a, b) zip map (p -> \
           let nz = ((p.0, p.0 lit 0) ne, (p.1, p.1 lit 0) ne) mul in \
           let pa = (p.0, p.1) le in ((pa, nz) mul, ((pa lit 1, pa) sub, nz) mul)) transpose",
        ca,
        cb,
    );
    let Value::Prod(mut mm) = masks else { panic!() };
    let mask_b = mm.pop().unwrap();
    let mask_a = mm.pop().unwrap();
    let lane_a = progs.run2("let (l, m) = input in (l, m) filter", tuples.clone(), mask_a);
    let lane_b = progs.run2("let (l, m) = input in (l, m) filter", tuples, mask_b);
    if std::env::var("GALEN_TRACE").is_ok() {
        eprintln!("    {}[seed {seed}] wco shard: {} via {}, {} via {} in {:.2}s", rule.head.rel, count(&lane_a), rule.body[ja].rel, count(&lane_b), rule.body[jb].rel, tw.elapsed().as_secs_f64());
    }
    let mut acc: Option<Value> = None;
    for (lane, j, other) in [(lane_a, ja, jb), (lane_b, jb, ja)] {
        if count(&lane) == 0 {
            continue;
        }
        let Some((out, nv)) = join_atom(progs, rels, rule, seed, lane, &vars, j) else { continue };
        if let Some(res) = extend(progs, rels, rule, seed, out, nv, vec![other]) {
            acc = Some(match acc {
                None => res,
                Some(prev) => progs.run2("let (a, b) = input in (a, b) append", prev, res),
            });
        }
    }
    acc
}

fn eval_rule(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize) -> Option<Value> {
    let seed_atom = &rule.body[seed];
    let seed_tuples = rels.get(seed_atom.rel)?.recent.as_ref()?.tuples.clone();
    let tuples = with_ids(progs, seed_tuples, seed_atom.vars.len());
    let remaining: Vec<usize> = (0..rule.body.len()).filter(|&j| j != seed).collect();
    extend(progs, rels, rule, seed, tuples, seed_atom.vars.clone(), remaining)
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/Users/mcsherry/Projects/datasets/flowlog/galen".to_string());
    let t0 = Instant::now();
    let mut progs = Progs(HashMap::new());
    let mut rels: HashMap<&'static str, Rel> = HashMap::new();
    for (name, arity) in [("c", 3), ("p", 2), ("q", 3), ("r", 3), ("s", 2), ("u", 3)] {
        let raw = load(&format!("{dir}/{}.csv", name.to_uppercase()), arity);
        let tuples = progs.run("input sort dedup", raw);
        let idb = name == "p" || name == "q";
        let (stable, recent) = if idb { (vec![], Some(Batch::new(tuples))) } else { (vec![Batch::new(tuples)], None) };
        rels.insert(name, Rel { stable, recent });
    }
    println!("loaded in {:.2}s", t0.elapsed().as_secs_f64());

    let rules = vec![
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("p", &["x", "y"]), atom("p", &["y", "z"])] },
        Rule { head: atom("q", &["x", "r", "z"]), body: vec![atom("p", &["x", "y"]), atom("q", &["y", "r", "z"])] },
        Rule { head: atom("q", &["x", "q2", "z"]), body: vec![atom("q", &["x", "r", "z"]), atom("s", &["r", "q2"])] },
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("p", &["y", "w"]), atom("u", &["w", "r", "z"]), atom("q", &["x", "r", "y"])] },
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("c", &["y", "w", "z"]), atom("p", &["x", "w"]), atom("p", &["x", "y"])] },
        Rule { head: atom("q", &["x", "e", "o"]), body: vec![atom("q", &["x", "y", "z"]), atom("q", &["z", "u", "o"]), atom("r", &["y", "u", "e"])] },
    ];

    let t1 = Instant::now();
    let mut round = 0;
    loop {
        round += 1;
        let tr = Instant::now();
        let mut derived: HashMap<&'static str, Vec<Value>> = HashMap::new();
        for rule in &rules {
            for seed in 0..rule.body.len() {
                if let Some(out) = eval_rule(&mut progs, &mut rels, rule, seed) {
                    derived.entry(rule.head.rel).or_default().push(out);
                }
            }
        }
        let mut any = false;
        let mut report = Vec::new();
        for name in ["p", "q"] {
            let outs = derived.remove(name).unwrap_or_default();
            let mut new: Option<Value> = None;
            for o in outs {
                new = Some(match new {
                    None => o,
                    Some(prev) => progs.run2("let (a, b) = input in (a, b) append", prev, o),
                });
            }
            let rel = rels.get_mut(name).unwrap();
            let ta = Instant::now();
            if let Some(recent) = rel.recent.take() {
                let mut batch = recent;
                while let Some(last) = rel.stable.last() {
                    if count(&last.tuples) <= 2 * count(&batch.tuples) {
                        let last = rel.stable.pop().unwrap();
                        let merged = progs.run2("let (a, b) = input in (a, b) append sort dedup", last.tuples, batch.tuples);
                        batch = Batch::new(merged);
                    } else {
                        break;
                    }
                }
                rel.stable.push(batch);
            }
            let t_lsm = ta.elapsed().as_secs_f64();
            let tb = Instant::now();
            let raw_rows = new.as_ref().map_or(0, count);
            let mut next = new.map(|v| progs.run("input sort dedup", v));
            let t_dedup = tb.elapsed().as_secs_f64();
            let tc = Instant::now();
            if let Some(mut v) = next.take() {
                for b in &rel.stable {
                    v = progs.run2(
                        "let (new, old) = input in let r = (new, old) find in (new, r map (x -> (x.0, x.1) eq)) filter",
                        v,
                        b.tuples.clone(),
                    );
                }
                if count(&v) > 0 {
                    any = true;
                    next = Some(v);
                }
            }
            if std::env::var("GALEN_TRACE").is_ok() {
                eprintln!("    advance {name}: lsm {t_lsm:.2}s ({} batches), sort+dedup {t_dedup:.2}s ({raw_rows} raw), antijoin {:.2}s", rel.stable.len(), tc.elapsed().as_secs_f64());
            }
            rel.recent = next.map(Batch::new);
            report.push(format!("{name}: total {} (+{})", rel.total(), rel.recent.as_ref().map_or(0, |b| count(&b.tuples))));
        }
        println!("round {round:>3} {:>6.2}s  {}", tr.elapsed().as_secs_f64(), report.join("  "));
        if !any {
            break;
        }
    }
    println!("fixpoint in {:.2}s ({} rounds); p = {}, q = {}", t1.elapsed().as_secs_f64(), round, rels["p"].total(), rels["q"].total());
}
