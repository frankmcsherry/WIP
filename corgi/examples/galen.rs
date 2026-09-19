//! GALEN in corgi: a semi-naive Datalog driver whose data plane is corgi programs.
//!
//! `cargo run --release --example galen -- /Users/mcsherry/Projects/datasets/flowlog/galen`
//!
//! Relations are one-row `List<Prod<u64,..>>` values kept as an LSM of sorted, deduplicated
//! batches (datatoad's `FactLSM`), each batch carrying the sorted `(key, values)` index for every
//! column permutation a rule needs (datatoad's `Permute-*` forms), built on demand. A rule is a chain
//! of binary joins from its delta atom; every join is ONE generic corgi program — `find` the sorted
//! left keys in the index keys (a galloping merge), `slices` the matched value ranges out of the
//! index BY REFERENCE, and `cap_list` each left row with its clone of them. The fixpoint loop, the
//! delta convention (atoms before the seed see stable, at/after see stable+recent), the LSM tidy
//! policy and the join order are the host's, as they are in datatoad.
use corgi::{Program, Value};
use std::collections::HashMap;
use std::time::Instant;

// ---------------------------------------------------------------------------------------------
// values

fn one_row(cols: Vec<Vec<u64>>) -> Value {
    let n = cols.first().map_or(0, |c| c.len());
    Value::List(vec![n].into(), Box::new(Value::Prod(cols.into_iter().map(Value::u64).collect())))
}

fn count(v: &Value) -> usize {
    match v {
        Value::List(_, vals) => vals.len(),
        other => other.len(),
    }
}

/// strip a `Fail` wrapper, asserting no row failed.
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

// ---------------------------------------------------------------------------------------------
// programs, compiled once per text

struct Progs(HashMap<String, Program>);
impl Progs {
    fn run(&mut self, text: &str, input: Value) -> Value {
        let p = self.0.entry(text.to_string()).or_insert_with(|| {
            Program::compile_ml(text).unwrap_or_else(|e| panic!("compile {text:?}: {e}"))
        });
        unfail(p.run_partial(input))
    }
}

/// `t.i` or `(t.i, t.j, ..)` over a flat tuple `t`.
fn tuple_expr(cols: &[usize]) -> String {
    if cols.len() == 1 {
        format!("t.{}", cols[0])
    } else {
        format!("({})", cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", "))
    }
}

// ---------------------------------------------------------------------------------------------
// relations: LSM of sorted batches with per-permutation indexes

struct Index {
    keys: Value, // one-row List<K>, sorted
    vals: Value, // Ref<List<V>>: the value column by reference (one fat ref over the whole column)
}

struct Batch {
    tuples: Value, // one-row List<Prod>, sorted + deduplicated
    indexes: HashMap<(Vec<usize>, Vec<usize>), Index>,
}

impl Batch {
    fn new(tuples: Value) -> Batch {
        Batch { tuples, indexes: HashMap::new() }
    }
    fn index(&mut self, progs: &mut Progs, key: &[usize], val: &[usize]) -> &Index {
        let k = (key.to_vec(), val.to_vec());
        if !self.indexes.contains_key(&k) {
            let text = format!("input map (t -> ({}, {})) sort transpose", tuple_expr(key), tuple_expr(val));
            let kv = progs.run(&text, self.tuples.clone());
            let Value::Prod(mut cols) = kv else { panic!("index: expected (keys, vals)") };
            let vals = cols.pop().unwrap();
            let keys = cols.pop().unwrap();
            let vals = progs.run("input ref", vals);
            self.indexes.insert(k.clone(), Index { keys, vals });
        }
        &self.indexes[&k]
    }
}

struct Rel {
    #[allow(dead_code)]
    arity: usize,
    stable: Vec<Batch>,
    recent: Option<Batch>,
}

impl Rel {
    fn total(&self) -> usize {
        self.stable.iter().map(|b| count(&b.tuples)).sum::<usize>() + self.recent.as_ref().map_or(0, |b| count(&b.tuples))
    }
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

/// the generic binary join: `(left: List<(K,A)>, keys: List<K>, vals: Ref<List<B>>)`, left sorted by
/// K, to a flat tuple list ordered K, A, B. `kc`/`ac`/`bc` are the arities of K, A, B (a 1-arity
/// component is a scalar, not a 1-tuple).
fn join_text(kc: usize, ac: usize, bc: usize) -> String {
    let field = |base: &str, n: usize, i: usize| if n == 1 { base.to_string() } else { format!("{base}.{i}") };
    let mut out = Vec::new();
    for i in 0..kc { out.push(field("p.0.0", kc, i)); }
    for i in 0..ac { out.push(field("p.0.1", ac, i)); }
    for i in 0..bc { out.push(field("p.1", bc, i)); }
    format!(
        "let (left, keys, vals) = input in \
         let r = (left map (t -> t.0), keys) find in \
         let ok = r map (x -> (x.0, x.1) lt) in \
         let l2 = (left, ok) filter in let r2 = (r, ok) filter in \
         let sl = (r2, vals) slices in \
         let rows = (((l2, sl) zip map (q -> (q.0, q.1 clone) cap_list)) flatten).1 in \
         rows map (p -> ({}))",
        out.join(", ")
    )
}

/// one binary join of `left` (flat tuples over `vars`) with atom `j` of the rule on their shared
/// variables, against the batches the delta convention allows (`j > seed` sees recent). Returns the
/// flat K, A, B tuples and their variable names, or None if nothing to join against.
fn join_atom(
    progs: &mut Progs,
    rels: &mut HashMap<&'static str, Rel>,
    rule: &Rule,
    seed: usize,
    left: Value,
    vars: &[&'static str],
    j: usize,
) -> Option<(Value, Vec<&'static str>)> {
    let a = &rule.body[j];
    let ts = Instant::now();
    let key_vars: Vec<&str> = vars.iter().copied().filter(|v| a.vars.contains(v)).collect();
    assert!(!key_vars.is_empty(), "cross product in rule {}", rule.head.rel);
    let rest_vars: Vec<&str> = vars.iter().copied().filter(|v| !key_vars.contains(v)).collect();
    let mut val_vars: Vec<&str> = a.vars.iter().copied().filter(|v| !key_vars.contains(v)).collect();
    let semijoin = val_vars.is_empty();
    if semijoin {
        val_vars.push("_dummy");
    }
    let key_pos: Vec<usize> = key_vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap()).collect();
    let rest_pos: Vec<usize> = rest_vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap()).collect();
    let idx_key: Vec<usize> = key_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect();
    let idx_val: Vec<usize> = if semijoin {
        vec![idx_key[0]]
    } else {
        val_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect()
    };
    let rest_expr = if rest_pos.is_empty() { tuple_expr(&key_pos[..1]) } else { tuple_expr(&rest_pos) };
    let ac = rest_pos.len().max(1);
    let ka = progs.run(&format!("input map (t -> ({}, {})) sort", tuple_expr(&key_pos), rest_expr), left);
    let rel = rels.get_mut(a.rel).unwrap();
    let use_recent = j > seed && rel.recent.is_some();
    let text = join_text(key_pos.len(), ac, idx_val.len());
    let mut acc: Option<Value> = None;
    let n_stable = rel.stable.len();
    for b in 0..n_stable + use_recent as usize {
        let batch = if b < n_stable { &mut rel.stable[b] } else { rel.recent.as_mut().unwrap() };
        let ix = batch.index(progs, &idx_key, &idx_val);
        let out = progs.run(&text, Value::Prod(vec![ka.clone(), ix.keys.clone(), ix.vals.clone()]));
        acc = Some(match acc {
            None => out,
            Some(prev) => progs.run("let (a, b) = input in (a, b) append", Value::Prod(vec![prev, out])),
        });
    }
    let out = acc?;
    assert!(count(&out) < 200_000_000, "join blow-up: {} rows", count(&out));
    if std::env::var("GALEN_TRACE").is_ok() {
        eprintln!("    {}[seed {seed}] ⋈ {}({}) key {:?}: {} left rows -> {} rows in {:.2}s", rule.head.rel, a.rel, a.vars.join(","), key_vars, count(&ka), count(&out), ts.elapsed().as_secs_f64());
    }
    let mut new_vars = key_vars.clone();
    if rest_pos.is_empty() { new_vars.push("_ignored"); } else { new_vars.extend(rest_vars); }
    new_vars.extend(val_vars);
    Some((out, new_vars))
}

/// per left row, the number of rows of atom `j` matching its key (the fan-out an extension by `j`
/// would produce), summed over the batches the delta convention allows. Unsorted needles.
fn fanout(
    progs: &mut Progs,
    rels: &mut HashMap<&'static str, Rel>,
    rule: &Rule,
    seed: usize,
    left: &Value,
    vars: &[&'static str],
    j: usize,
) -> Value {
    let a = &rule.body[j];
    let key_vars: Vec<&str> = vars.iter().copied().filter(|v| a.vars.contains(v)).collect();
    let key_pos: Vec<usize> = key_vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap()).collect();
    let idx_key: Vec<usize> = key_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect();
    let val_vars: Vec<&str> = a.vars.iter().copied().filter(|v| !key_vars.contains(v)).collect();
    let idx_val: Vec<usize> = if val_vars.is_empty() { vec![idx_key[0]] } else { val_vars.iter().map(|v| a.vars.iter().position(|w| w == v).unwrap()).collect() };
    let needles = progs.run(&format!("input map (t -> {})", tuple_expr(&key_pos)), left.clone());
    let rel = rels.get_mut(a.rel).unwrap();
    let use_recent = j > seed && rel.recent.is_some();
    let n_stable = rel.stable.len();
    let mut acc: Option<Value> = None;
    for b in 0..n_stable + use_recent as usize {
        let batch = if b < n_stable { &mut rel.stable[b] } else { rel.recent.as_mut().unwrap() };
        let ix = batch.index(progs, &idx_key, &idx_val);
        let c = progs.run(
            "let (needles, keys) = input in (needles, keys) find map (x -> (x.1, x.0) sub)",
            Value::Prod(vec![needles.clone(), ix.keys.clone()]),
        );
        acc = Some(match acc {
            None => c,
            Some(prev) => progs.run("let (a, b) = input in (a, b) zip map (p -> (p.0, p.1) add)", Value::Prod(vec![prev, c])),
        });
    }
    acc.unwrap_or_else(|| progs.run("input map (t -> t lit 0)", needles))
}

/// extend `left` (flat tuples over `vars`) by the atoms in `remaining`, to head tuples.
/// Fully-bound atoms validate (semijoin). One partially-bound atom joins. Two or more: the
/// worst-case-optimal step — count each row's fan-out in every candidate, extend each row by
/// its smallest candidate (shard, join, recurse), so no intermediate exceeds the true output
/// times the number of candidates. datatoad's count / shard / propose / validate.
fn extend(
    progs: &mut Progs,
    rels: &mut HashMap<&'static str, Rel>,
    rule: &Rule,
    seed: usize,
    mut left: Value,
    mut vars: Vec<&'static str>,
    mut remaining: Vec<usize>,
) -> Option<Value> {
    // validate against every atom that is already fully bound.
    loop {
        let bound: Option<usize> = remaining.iter().copied().find(|&j| rule.body[j].vars.iter().all(|v| vars.contains(v)));
        let Some(j) = bound else { break };
        remaining.retain(|&k| k != j);
        let (out, nv) = join_atom(progs, rels, rule, seed, left, &vars, j)?;
        left = out;
        vars = nv;
        if count(&left) == 0 {
            return None;
        }
    }
    if remaining.is_empty() {
        let head_pos: Vec<usize> = rule.head.vars.iter().map(|v| vars.iter().position(|w| w == v).unwrap_or_else(|| panic!("head var {v} unbound"))).collect();
        return Some(progs.run(&format!("input map (t -> {})", tuple_expr(&head_pos)), left));
    }
    if remaining.len() == 1 {
        let j = remaining[0];
        let (out, nv) = join_atom(progs, rels, rule, seed, left, &vars, j)?;
        return extend(progs, rels, rule, seed, out, nv, vec![]);
    }
    assert_eq!(remaining.len(), 2, "WCO step over more than two candidates is not implemented");
    let tw = Instant::now();
    let (ja, jb) = (remaining[0], remaining[1]);
    let ca = fanout(progs, rels, rule, seed, &left, &vars, ja);
    let cb = fanout(progs, rels, rule, seed, &left, &vars, jb);
    // rows with count 0 in either candidate produce nothing; the rest go to the smaller side.
    let pick_a = progs.run(
        "let (a, b) = input in (a, b) zip map (p -> (p.0, p.1) le)",
        Value::Prod(vec![ca.clone(), cb.clone()]),
    );
    let nonzero = progs.run(
        "let (a, b) = input in (a, b) zip map (p -> ((p.0, p.0 lit 0) ne, (p.1, p.1 lit 0) ne) mul)",
        Value::Prod(vec![ca, cb]),
    );
    let mask_a = progs.run("let (a, b) = input in (a, b) zip map (p -> (p.0, p.1) mul)", Value::Prod(vec![pick_a.clone(), nonzero.clone()]));
    let mask_b = progs.run(
        "let (a, b) = input in (a, b) zip map (p -> ((p.0 lit 1, p.0) sub, p.1) mul)",
        Value::Prod(vec![pick_a, nonzero]),
    );
    let lane_a = progs.run("let (l, m) = input in (l, m) filter", Value::Prod(vec![left.clone(), mask_a]));
    let lane_b = progs.run("let (l, m) = input in (l, m) filter", Value::Prod(vec![left, mask_b]));
    if std::env::var("GALEN_TRACE").is_ok() {
        eprintln!("    {}[seed {seed}] wco shard: {} rows -> {} via {}, {} via {} in {:.2}s", rule.head.rel, count(&lane_a) + count(&lane_b), count(&lane_a), rule.body[ja].rel, count(&lane_b), rule.body[jb].rel, tw.elapsed().as_secs_f64());
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
                Some(prev) => progs.run("let (a, b) = input in (a, b) append", Value::Prod(vec![prev, res])),
            });
        }
    }
    acc
}

/// evaluate one rule with the delta at body position `seed`: head tuples (unsorted, with
/// duplicates), or None if the seed relation has no recent batch.
fn eval_rule(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize) -> Option<Value> {
    let seed_atom = &rule.body[seed];
    let seed_tuples = rels.get(seed_atom.rel)?.recent.as_ref()?.tuples.clone();
    let remaining: Vec<usize> = (0..rule.body.len()).filter(|&j| j != seed).collect();
    extend(progs, rels, rule, seed, seed_tuples, seed_atom.vars.clone(), remaining)
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/Users/mcsherry/Projects/datasets/flowlog/galen".to_string());
    let t0 = Instant::now();
    let mut progs = Progs(HashMap::new());
    let mut rels: HashMap<&'static str, Rel> = HashMap::new();
    for (name, arity) in [("c", 3), ("p", 2), ("q", 3), ("r", 3), ("s", 2), ("u", 3)] {
        let raw = load(&format!("{dir}/{}.csv", name.to_uppercase()), arity);
        let tuples = progs.run("input sort dedup", raw);
        // EDB relations start stable; the IDB relations p and q start as the first recent batch.
        let idb = name == "p" || name == "q";
        let (stable, recent) = if idb { (vec![], Some(Batch::new(tuples))) } else { (vec![Batch::new(tuples)], None) };
        rels.insert(name, Rel { arity, stable, recent });
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
        // derive
        let mut derived: HashMap<&'static str, Vec<Value>> = HashMap::new();
        for rule in &rules {
            for seed in 0..rule.body.len() {
                if let Some(out) = eval_rule(&mut progs, &mut rels, rule, seed) {
                    derived.entry(rule.head.rel).or_default().push(out);
                }
            }
        }
        // advance: recent -> stable (LSM), new - known -> recent
        let mut any = false;
        let mut report = Vec::new();
        for name in ["p", "q"] {
            let outs = derived.remove(name).unwrap_or_default();
            let mut new: Option<Value> = None;
            for o in outs {
                new = Some(match new {
                    None => o,
                    Some(prev) => progs.run("let (a, b) = input in (a, b) append", Value::Prod(vec![prev, o])),
                });
            }
            let rel = rels.get_mut(name).unwrap();
            let ta = Instant::now();
            // roll recent into stable, tidying geometrically
            if let Some(recent) = rel.recent.take() {
                let mut batch = recent;
                while let Some(last) = rel.stable.last() {
                    if count(&last.tuples) <= 2 * count(&batch.tuples) {
                        let last = rel.stable.pop().unwrap();
                        let merged = progs.run("let (a, b) = input in (a, b) append sort dedup", Value::Prod(vec![last.tuples, batch.tuples]));
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
            let mut next = match new {
                None => None,
                Some(v) => Some(progs.run("input sort dedup", v)),
            };
            let t_dedup = tb.elapsed().as_secs_f64();
            let tc = Instant::now();
            if let Some(mut v) = next.take() {
                for b in &rel.stable {
                    v = progs.run(
                        "let (new, old) = input in let r = (new, old) find in (new, r map (x -> (x.0, x.1) eq)) filter",
                        Value::Prod(vec![v, b.tuples.clone()]),
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
