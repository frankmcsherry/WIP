//! GALEN in corgi with a persistent salad: the seed's trie is walked, never re-sorted.
//!
//! `cargo run --release --example galen_salad -- /Users/mcsherry/Projects/datasets/flowlog/galen`
//!
//! A rule with the delta at atom S and up to two other atoms A, B (GALEN's shape: each other
//! atom's key is ONE seed variable, both share one extension variable, each has at most one extra
//! variable that only the head sees) is evaluated on the seed's trie in the seed's own column order:
//!  * layer l of the trie is the sorted distinct values of seed variable l under each layer-(l-1)
//!    item — built by `group` once per delta, no sort;
//!  * at the layer holding A's key, one broadcast `find` (the layer's lists against A's index keys)
//!    gives every item its `Ref` to A's sub-trie — its extension list and, per extension value, a
//!    `Ref` to its extras — and those refs ride down to the deeper items by `cap_list`, so an item
//!    holds references, never copies;
//!  * at the deepest key layer every item has both refs: the WCO step is `len` of each, the argmin,
//!    and one `find` per item with the smaller extension list cloned as needles and the larger
//!    searched through its ref — min·log max per item, datatoad's intersection;
//!  * the head tuples are the cross product of the matched extras, materialized once, at the end.
//! Nothing is sorted between the delta's `group` and the output's dedup.
use corgi::{Program, Refs, Value};

// datatoad runs under mimalloc; every corgi pass allocates its output column, so the allocator is
// a sixth of this driver's time (32.6s -> 27.5s). Parity for the comparison.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

fn one_row(cols: Vec<Vec<u64>>) -> Value {
    let n = cols.first().map_or(0, |c| c.len());
    // store the terms at the narrowest width the data fits, as datatoad does (`upgrade_hint`);
    // corgi's kernels are width-generic, so only the leaves change. GALEN_U64 forces 8 bytes.
    let fits32 = cols.iter().all(|c| c.iter().all(|&x| x <= u32::MAX as u64));
    let narrow = fits32 && std::env::var("GALEN_U64").is_err();
    let cols: Vec<Value> = cols
        .into_iter()
        .map(|c| if narrow { Value::u32(c.into_iter().map(|x| x as u32).collect()) } else { Value::u64(c) })
        .collect();
    Value::List(vec![n].into(), Box::new(Value::Prod(cols)))
}
fn count(v: &Value) -> usize {
    match v {
        Value::List(_, vals) => vals.len(),
        other => other.len(),
    }
}
fn unnest(v: Value) -> Value {
    match v {
        Value::List(_, vals) => *vals,
        other => panic!("unnest: expected a list, got {}", corgi::show(&other)),
    }
}
fn nest(v: Value) -> Value {
    let n = v.len();
    Value::List(vec![n].into(), Box::new(v))
}
/// a one-row list broadcast to `n` rows: `n` fat refs to its one payload
fn broadcast(v: &Value, n: usize) -> Value {
    match v {
        Value::List(_, vals) => Value::Ref(Arc::new((**vals).clone()), Refs::Fat(Arc::new(vec![(0, vals.len()); n]))),
        other => panic!("broadcast: expected a list, got {}", corgi::show(other)),
    }
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

struct Progs(HashMap<String, Program>, u64, u64, HashMap<String, (u64, u64, f64)>); // programs, runs, rows, per-text (runs, rows, secs)
impl Progs {
    fn run(&mut self, text: &str, input: Value) -> Value {
        self.1 += 1;
        let rows = match &input {
            Value::Prod(cols) => cols.iter().map(|c| count(c) as u64).max().unwrap_or(0),
            v => count(v) as u64,
        };
        self.2 += rows;
        let t = Instant::now();
        let out = self.run_inner(text, input);
        let e = self.3.entry(text.to_string()).or_insert((0, 0, 0.0));
        e.0 += 1;
        e.1 += rows;
        e.2 += t.elapsed().as_secs_f64();
        out
    }
    fn run_inner(&mut self, text: &str, input: Value) -> Value {
        let p = self.0.entry(text.to_string()).or_insert_with(|| {
            Program::compile_ml(text).unwrap_or_else(|e| panic!("compile {text:?}: {e}"))
        });
        unfail(p.run_partial(input))
    }
    fn run2(&mut self, text: &str, a: Value, b: Value) -> Value {
        self.run(text, Value::Prod(vec![a, b]))
    }
}
/// concatenate many one-row lists in ONE pass: the multi-source gather reads each row from its
/// source (`arrange::gather_lanes`), where a fold of `append`s copies every row once per level.
fn concat_all(_progs: &mut Progs, mut pieces: Vec<Value>) -> Option<Value> {
    pieces.retain(|v| count(v) > 0);
    match pieces.len() {
        0 => None,
        1 => pieces.pop(),
        _ => {
            let payloads: Vec<Value> = pieces.into_iter().map(unnest).collect();
            let total: usize = payloads.iter().map(|p| p.len()).sum();
            let (mut tags, mut off) = (Vec::with_capacity(total), Vec::with_capacity(total));
            for (t, p) in payloads.iter().enumerate() {
                tags.extend(std::iter::repeat_n(t, p.len()));
                off.extend(0..p.len());
            }
            let srcs: Vec<Option<&Value>> = payloads.iter().map(Some).collect();
            Some(nest(corgi::arrange::gather_lanes(&srcs, &tags, &off)))
        }
    }
}

/// the UNION of two sorted, deduplicated one-row lists, sorted and deduplicated, in one merge and
/// one gather: `survey_groups` reports the exclusive runs of each side and the equal classes
/// (datatoad's `union`), and the multi-source gather materializes them in merged order. Linear.
fn union(a: Value, b: Value) -> Value {
    use corgi::arrange::GroupRun;
    if count(&a) == 0 {
        return b;
    }
    if count(&b) == 0 {
        return a;
    }
    let (pa, pb) = (unnest(a), unnest(b));
    let runs = corgi::arrange::survey_groups(&pa, &pb);
    let mut tags = Vec::with_capacity(pa.len() + pb.len());
    let mut off = Vec::with_capacity(pa.len() + pb.len());
    for r in runs {
        match r {
            GroupRun::A(lo, hi) => {
                tags.extend(std::iter::repeat_n(0usize, hi - lo));
                off.extend(lo..hi);
            }
            GroupRun::B(lo, hi) => {
                tags.extend(std::iter::repeat_n(1usize, hi - lo));
                off.extend(lo..hi);
            }
            GroupRun::Both(alo, _ahi, _blo, _bhi) => {
                // equal rows on both sides: the inputs are deduplicated, so one of them
                tags.push(0);
                off.push(alo);
            }
        }
    }
    nest(corgi::arrange::gather_lanes(&[Some(&pa), Some(&pb)], &tags, &off))
}

/// sort + dedup each piece (only its ungrouped columns), then union them pairwise in a balanced
/// tree (every level a linear merge)
fn sorted_union(progs: &mut Progs, pieces: Vec<Piece>) -> Option<Value> {
    let mut sorted: Vec<Value> = pieces.into_iter().filter(|p| count(&p.rows) > 0).map(|p| piece_sorted(progs, p)).collect();
    while sorted.len() > 1 {
        let mut next = Vec::with_capacity(sorted.len().div_ceil(2));
        let mut it = sorted.into_iter();
        while let Some(a) = it.next() {
            match it.next() {
                Some(b) => next.push(union(a, b)),
                None => next.push(a),
            }
        }
        sorted = next;
    }
    sorted.pop()
}

fn tuple_expr(cols: &[usize]) -> String {
    if cols.len() == 1 {
        format!("t.{}", cols[0])
    } else {
        format!("({})", cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", "))
    }
}

// ---------------------------------------------------------------------------------------------
// index tries: keys (in seed order) -> ext -> extras, one Ref column per layer


/// A relation batch as a trie over `key` (d columns, in the seed's order), then `ext` (if any),
/// then `extra` (any number of columns, one tuple). `keys0` is the sorted distinct first keys;
/// `next[l]` holds, per layer-`l` item, a fat ref to its children's key list (for `l = d-1`, to its
/// ext list); `starts[l]` the absolute index of each item's first child in layer `l+1`; `xrefs`,
/// per full-key item, a fat ref to the list of refs to its ext items' extras. Everything below
/// `keys0` is references: walking down moves 16 bytes per item, never keys.
struct Index {
    keys0: Value,
    next: Vec<Value>,
    starts: Vec<Value>,
    xrefs: Option<Value>,
}

fn nested_group_program(depth: usize, key: &[usize], ext: Option<usize>, extra: &[usize]) -> String {
    // t -> (k1, (k2, (... (kd, (e, X)) ...)))
    let mut inner = if let Some(e) = ext {
        format!("(t.{e}, {})", tuple_expr(extra))
    } else {
        format!("t.{}", key[depth - 1])
    };
    let kd = if ext.is_some() { depth } else { depth - 1 };
    for i in (0..kd).rev() {
        inner = format!("(t.{}, {inner})", key[i]);
    }
    // successive groups: level 0 on the whole, then `map (p -> (p.0, p.1 group))` nested per level
    let mut prog = format!("input map (t -> {inner}) group");
    let levels = if ext.is_some() { depth } else { depth - 1 }; // groupings below the first
    for lvl in 1..=levels {
        // build the nested map that applies `group` at level `lvl`
        let mut m = "l group".to_string();
        for _ in 1..lvl {
            m = format!("l map (q -> (q.0, let l = q.1 in {m}))");
        }
        prog = format!("{prog} map (p -> (p.0, let l = p.1 in {m}))");
    }
    prog
}

fn build_index(progs: &mut Progs, tuples: &Value, key: &[usize], ext: Option<usize>, extra: &[usize]) -> Index {
    let d = key.len();
    if d == 1 && ext.is_none() {
        // a validation-only atom on one key: its distinct keys are the whole index
        let keys0 = progs.run(&format!("input map (t -> t.{}) sort dedup", key[0]), tuples.clone());
        return Index { keys0, next: Vec::new(), starts: Vec::new(), xrefs: None };
    }
    let grouped = progs.run(&nested_group_program(d, key, ext, extra), tuples.clone());
    let mut next = Vec::new();
    let mut starts = Vec::new();
    let keys0 = progs.run("input map (p -> p.0)", grouped.clone());
    let mut cur = grouped; // rows = parents, elements = (key, children)
    let layers_with_children = if ext.is_some() { d } else { d - 1 };
    for l in 0..layers_with_children {
        // children key lists by reference, and where each item's children start
        let refs = progs.run("input map (p -> (p.1 map (q -> q.0)) ref)", cur.clone());
        let lens = progs.run("input map (p -> p.1 len)", cur.clone());
        let st = progs.run("let s = input scan_add in (s, input) zip map (q -> (q.0, q.1) sub)", lens);
        next.push(nest(unnest(refs)));
        starts.push(nest(unnest(st)));
        if l + 1 == layers_with_children && ext.is_some() {
            // the full-key items: per item, a ref to the list of refs to each ext item's extras
            let x = progs.run("input map (p -> (p.1 map (q -> q.1 ref)) ref)", cur.clone());
            return Index { keys0, next, starts, xrefs: Some(nest(unnest(x))) };
        }
        cur = unnest(progs.run("input map (p -> p.1)", cur));
    }
    Index { keys0, next, starts, xrefs: None }
}

struct Batch {
    tuples: Value,
    indexes: HashMap<(Vec<usize>, Option<usize>, Vec<usize>), Index>,
}
impl Batch {
    fn new(tuples: Value) -> Batch {
        Batch { tuples, indexes: HashMap::new() }
    }
    fn index(&mut self, progs: &mut Progs, key: &[usize], ext: Option<usize>, extra: &[usize]) -> &Index {
        let k = (key.to_vec(), ext, extra.to_vec());
        if !self.indexes.contains_key(&k) {
            let ix = build_index(progs, &self.tuples, key, ext, extra);
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
// rules and their shape

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

/// how a non-seed atom attaches to the seed: its key variables (the ones it shares with the seed,
/// in SEED order, each at a seed layer), the variable it extends by (none for a validation-only
/// atom), and its extra variables (the rest; only the head sees them).
struct Cand {
    j: usize,
    key_layers: Vec<usize>, // seed layer (1-based) of each key variable, in seed order
    idx_key: Vec<usize>,    // the same variables' positions in the atom
    idx_ext: Option<usize>,
    idx_extra: Vec<usize>,
    extra_vars: Vec<&'static str>,
}

fn cand(rule: &Rule, seed: usize, j: usize, ext_var: Option<&'static str>) -> Cand {
    let s = &rule.body[seed];
    let a = &rule.body[j];
    let keys: Vec<&str> = s.vars.iter().copied().filter(|v| a.vars.contains(v)).collect();
    assert!(!keys.is_empty(), "rule {}: atom {} shares no variable with the seed", rule.head.rel, a.rel);
    let extra_vars: Vec<&'static str> = a.vars.iter().copied().filter(|v| !keys.contains(v) && Some(*v) != ext_var).collect();
    let idx_key: Vec<usize> = keys.iter().map(|k| a.vars.iter().position(|v| v == k).unwrap()).collect();
    let idx_ext = ext_var.map(|e| a.vars.iter().position(|v| *v == e).unwrap());
    let idx_extra: Vec<usize> = if extra_vars.is_empty() { vec![idx_key[0]] } else { extra_vars.iter().map(|x| a.vars.iter().position(|v| v == x).unwrap()).collect() };
    Cand { j, key_layers: keys.iter().map(|k| s.vars.iter().position(|v| v == k).unwrap() + 1).collect(), idx_key, idx_ext, idx_extra, extra_vars }
}

fn batches<'a>(rels: &'a mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, j: usize) -> Vec<&'a mut Batch> {
    let rel = rels.get_mut(rule.body[j].rel).unwrap();
    let use_recent = j > seed && rel.recent.is_some();
    let mut out: Vec<&mut Batch> = rel.stable.iter_mut().collect();
    if use_recent {
        out.push(rel.recent.as_mut().unwrap());
    }
    out
}

fn seed_layers(progs: &mut Progs, tuples: Value, depth: usize, key_depth: usize) -> (Vec<Value>, Option<Value>) {
    let mut layers = Vec::new();
    let mut cur = tuples;
    for l in 0..key_depth {
        if l + 1 == depth {
            layers.push(cur);
            return (layers, None);
        }
        let rest: Vec<usize> = (1..depth - l).collect();
        let g = progs.run(&format!("input map (t -> (t.0, {})) group", tuple_expr(&rest)), cur);
        let keys = progs.run("input map (p -> p.0)", g.clone());
        let subs = unnest(progs.run("input map (p -> p.1)", g));
        layers.push(keys);
        cur = subs;
    }
    (layers, Some(cur))
}

fn zip_flat(progs: &mut Progs, a: Value, na: usize, b: Value, nb: usize) -> Value {
    let sel: Vec<String> = (0..na).map(|i| if na == 1 { "p.0".to_string() } else { format!("p.0.{i}") })
        .chain((0..nb).map(|i| if nb == 1 { "p.1".to_string() } else { format!("p.1.{i}") }))
        .collect();
    progs.run2(&format!("let (a, b) = input in (a, b) zip map (p -> ({}))", sel.join(", ")), a, b)
}

/// the (candidate, batch) columns carried per item: a presence mask and an absolute position in
/// the index's current key layer
struct Carried {
    mask: usize,
    pos: usize,
}

fn eval_rule(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize) -> Option<Piece> {
    let s = &rule.body[seed];
    let tuples = rels.get(s.rel)?.recent.as_ref()?.tuples.clone();
    let depth = s.vars.len();
    let ts = Instant::now();
    // the stage: extend by the unbound variable shared by the most atoms (datatoad's
    // most-constrained-first); atoms bound by the seed alone validate; atoms without it wait
    // (not needed by GALEN's rules, asserted).
    let others: Vec<usize> = (0..rule.body.len()).filter(|&j| j != seed).collect();
    let mut unbound: Vec<&'static str> = Vec::new();
    for &j in &others {
        for v in &rule.body[j].vars {
            if !s.vars.contains(v) && !unbound.contains(v) {
                unbound.push(v);
            }
        }
    }
    let ext_var: Option<&'static str> = unbound
        .iter()
        .copied()
        .max_by_key(|v| others.iter().filter(|&&j| rule.body[j].vars.contains(v)).count());
    let cands: Vec<Cand> = others
        .iter()
        .map(|&j| {
            let has_ext = ext_var.map_or(false, |e| rule.body[j].vars.contains(&e));
            cand(rule, seed, j, if has_ext { ext_var } else { None })
        })
        .collect();
    for c in &cands {
        let a = &rule.body[c.j];
        let bound_after = a.vars.iter().all(|v| s.vars.contains(v) || Some(*v) == ext_var || c.extra_vars.contains(v));
        assert!(bound_after, "rule {}: atom {} needs a second stage (not implemented)", rule.head.rel, a.rel);
    }
    let deepest = cands.iter().flat_map(|c| c.key_layers.iter().copied()).max().unwrap();
    let (layers, leaves) = seed_layers(progs, tuples, depth, deepest);
    let leaf_arity = depth - deepest;

    // the walk: per item a flat tuple of the seed values so far and, per (candidate, batch), its
    // (mask, position); `fields` names the tuple's positions
    let mut item: Option<Value> = None;
    let mut parents: Option<Value> = None; // the previous layer's items, as one row
    let mut fields: Vec<String> = Vec::new();
    let mut carried: Vec<Vec<Carried>> = cands.iter().map(|_| Vec::new()).collect(); // [cand][batch]
    let n_batches: Vec<usize> = cands.iter().map(|c| batches(rels, rule, seed, c.j).len()).collect();
    for l in 0..deepest {
        let layer = layers[l].clone();
        let nrows = layer.len();
        let mut it = match item.take() {
            None => layer.clone(),
            Some(d) => {
                let na = fields.len();
                let sel: Vec<String> = (0..na).map(|i| if na == 1 { "p.0".to_string() } else { format!("p.0.{i}") }).chain(std::iter::once("p.1".to_string())).collect();
                progs.run(&format!("input map (p -> ({}))", sel.join(", ")), d)
            }
        };
        fields.push(s.vars[l].to_string());
        let key_field = fields.len() - 1;
        for (ci, c) in cands.iter().enumerate() {
            let Some(j) = c.key_layers.iter().position(|&kl| kl == l + 1) else { continue };
            let needles = if fields.len() == 1 { layer.clone() } else { progs.run(&format!("input map (p -> p.{key_field})"), it.clone()) };
            for b in 0..n_batches[ci] {
                // the haystack rows: the first key layer broadcast, a deeper one gathered through
                // the parent's position into the previous key layer's children refs
                let (hay, base) = {
                    let bs = batches(rels, rule, seed, c.j);
                    let ix = bs.into_iter().nth(b).unwrap().index(progs, &c.idx_key, c.idx_ext, &c.idx_extra);
                    if j == 0 {
                        (broadcast(&ix.keys0, nrows), None)
                    } else {
                        // per PARENT (the rows of this layer): its position in the previous key
                        // layer, hence its children's key list (a ref) and where they start
                        let pf = carried[ci][b].pos;
                        let prev = progs.run(&format!("input map (p -> p.{pf})"), parents.clone().unwrap());
                        let kids = progs.run2("let (i, h) = input in (i, h) gather", prev.clone(), broadcast(&ix.next[j - 1], 1));
                        let st = progs.run2("let (i, h) = input in (i, h) gather", prev, broadcast(&ix.starts[j - 1], 1));
                        // the start, per ITEM of this layer (replicated over each parent's items)
                        let base = progs.run2("let (b, l) = input in (b, l) cap_list map (q -> q.0)", unnest(st), layer.clone());
                        (unnest(kids), Some(base))
                    }
                };
                let r = progs.run2("let (n, h) = input in (n, h) find", needles.clone(), hay);
                let mp = progs.run("input map (x -> let m = (x.0, x.1) lt in (m, (x.0, m) mul)) transpose", r);
                let Value::Prod(mut cc) = mp else { panic!() };
                let lo = cc.pop().unwrap();
                let m = cc.pop().unwrap();
                let pair = if let Some(base) = base {
                    // absolute position = the parent's children start + the row-relative match;
                    // carry the mask conjunction with the parent's
                    let mf = carried[ci][b].mask;
                    let pm = progs.run(&format!("input map (p -> p.{mf})"), it.clone());
                    progs.run(
                        "let (m, lo, base, pm) = input in (((m, pm) zip, (lo, base) zip) zip) map (q -> ((q.0.0, q.0.1) mul, (q.1.0, q.1.1) add))",
                        Value::Prod(vec![m, lo, base, pm]),
                    )
                } else {
                    progs.run2("let (m, p) = input in (m, p) zip", m, lo)
                };
                it = zip_flat(progs, it, fields.len(), pair, 2);
                let mf = fields.len();
                fields.push(format!("m{ci}_{b}"));
                fields.push(format!("pos{ci}_{b}"));
                if j == 0 {
                    carried[ci].push(Carried { mask: mf, pos: mf + 1 });
                } else {
                    carried[ci][b] = Carried { mask: mf, pos: mf + 1 };
                }
            }
        }
        if l + 1 < deepest {
            parents = Some(nest(unnest(it.clone())));
            item = Some(progs.run2("let (d, ch) = input in (d, ch) cap_list", unnest(it), layers[l + 1].clone()));
        } else {
            item = Some(it);
        }
    }
    let mut items = nest(unnest(item.unwrap()));
    let has_leaf = deepest < depth;
    if has_leaf {
        items = zip_flat(progs, items, fields.len(), nest(leaves.unwrap()), 1);
        fields.push("leaves".to_string());
    }
    let leaf_idx = if has_leaf { Some(fields.len() - 1) } else { None };
    let nf = fields.len();

    // ---- the stage kernel over batch combinations ----
    let extending: Vec<usize> = (0..cands.len()).filter(|&ci| cands[ci].idx_ext.is_some()).collect();
    let combos: Vec<Vec<usize>> = {
        let mut acc: Vec<Vec<usize>> = vec![vec![]];
        for ci in 0..cands.len() {
            let mut next = Vec::new();
            for prefix in &acc {
                for b in 0..n_batches[ci] {
                    let mut v = prefix.clone();
                    v.push(b);
                    next.push(v);
                }
            }
            acc = next;
        }
        acc
    };
    let prefix_expr: String = if deepest == 1 { "it.0".to_string() } else { format!("({})", (0..deepest).map(|i| format!("it.{i}")).collect::<Vec<_>>().join(", ")) };
    // head projection over an output row `(P, (e, (x_0, (x_1, ... )), leaf))`
    let head_of = || -> String {
        let pos = |v: &str| -> String {
            if let Some(i) = s.vars.iter().position(|w| *w == v) {
                if i >= deepest {
                    if leaf_arity == 1 { "r.1.2".to_string() } else { format!("r.1.2.{}", i - deepest) }
                } else if deepest == 1 {
                    "r.0".to_string()
                } else {
                    format!("r.0.{i}")
                }
            } else if Some(v) == ext_var {
                "r.1.0".to_string()
            } else {
                // extras: nested pairs (x_0, (x_1, (x_2, ..))) in extending-candidate order
                let mut path = "r.1.1".to_string();
                for (k, &ci) in extending.iter().enumerate() {
                    let c = &cands[ci];
                    if let Some(xi) = c.extra_vars.iter().position(|x| *x == v) {
                        let last = k + 1 == extending.len();
                        let here = if last { path.clone() } else { format!("{path}.0") };
                        return if c.extra_vars.len() == 1 { here } else { format!("{here}.{xi}") };
                    }
                    path = format!("{path}.1");
                }
                panic!("head var {v} unbound")
            }
        };
        let sel: Vec<String> = rule.head.vars.iter().map(|v| pos(v)).collect();
        format!("r -> ({})", sel.join(", "))
    };
    let mut pieces: Vec<Value> = Vec::new();
    for combo in &combos {
        // presence on every candidate for this batch combination
        let mask_fields: Vec<usize> = (0..cands.len()).map(|ci| carried[ci][combo[ci]].mask).collect();
        let pres = mask_fields.iter().map(|f| format!("it.{f}")).collect::<Vec<_>>().join(", ");
        let both = progs.run(&format!("(input, input map (it -> {})) filter", fold_mul(&pres, mask_fields.len())), items.clone());
        if count(&both) == 0 {
            continue;
        }
        // resolve the extending candidates' ext refs and extras refs through their positions
        let mut cur = both;
        let mut nfields = nf;
        let mut ext_field: HashMap<usize, (usize, usize)> = HashMap::new(); // ci -> (ext, xref)
        for &ci in &extending {
            let c = &cands[ci];
            let pf = carried[ci][combo[ci]].pos;
            let pos = progs.run(&format!("input map (it -> it.{pf})"), cur.clone());
            let (e, x) = {
                let bs = batches(rels, rule, seed, c.j);
                let ix = bs.into_iter().nth(combo[ci]).unwrap().index(progs, &c.idx_key, c.idx_ext, &c.idx_extra);
                let e = progs.run2("let (i, h) = input in (i, h) gather", pos.clone(), broadcast(ix.next.last().unwrap(), 1));
                let x = progs.run2("let (i, h) = input in (i, h) gather", pos, broadcast(ix.xrefs.as_ref().unwrap(), 1));
                (e, x)
            };
            let pair = progs.run2("let (e, x) = input in (e, x) zip", e, x);
            cur = zip_flat(progs, cur, nfields, pair, 2);
            ext_field.insert(ci, (nfields, nfields + 1));
            nfields += 2;
        }
        if extending.is_empty() {
            // validation only: the surviving items (crossed with their leaves) are the result; the
            // row shape `(P, (e, xs, leaf))` is kept with placeholders so the head projection is shared
            let leafrows = match leaf_idx {
                Some(li) => format!("it.{li} map (lv -> (lv, lv, lv))"),
                None => "(it.0 enlist) map (lv -> (lv, lv, lv))".to_string(),
            };
            let prog = format!("(input map (it -> ({prefix_expr}, {leafrows}) cap_list) flatten).1 map ({})", head_of());
            pieces.push(progs.run(&prog, cur));
            continue;
        }
        // nonzero extension lists on every extending candidate, then the argmin
        let lens: Vec<String> = extending.iter().map(|ci| format!("it.{} len", ext_field[ci].0)).collect();
        let nz = lens.iter().map(|l| format!("({l}, {l} lit 0) ne")).collect::<Vec<_>>().join(", ");
        let alive = progs.run(&format!("(input, input map (it -> {})) filter", fold_mul(&nz, lens.len())), cur);
        if count(&alive) == 0 {
            continue;
        }
        for (k, &ci) in extending.iter().enumerate() {
            // lane k: candidate ci has the smallest extension list (ties to the earliest)
            let mine = &lens[k];
            let cond: Vec<String> = extending
                .iter()
                .enumerate()
                .filter(|&(k2, _)| k2 != k)
                .map(|(k2, _)| if k2 < k { format!("({mine}, {}) lt", lens[k2]) } else { format!("({mine}, {}) le", lens[k2]) })
                .collect();
            let lane = if cond.is_empty() {
                alive.clone()
            } else {
                progs.run(&format!("(input, input map (it -> {})) filter", fold_mul(&cond.join(", "), cond.len())), alive.clone())
            };
            if count(&lane) == 0 {
                continue;
            }
            // needles = my ext list cloned; validate against every other extending candidate
            let (ne, nx) = ext_field[&ci];
            let mut prog = format!("(input map (it -> let n = it.{ne} clone in let m0 = n map (v -> v lit 1) in ");
            let mut matched_lo: Vec<String> = Vec::new();
            let mut mask_terms = vec!["m0".to_string()];
            for (k2, &o) in extending.iter().enumerate() {
                if k2 == k {
                    continue;
                }
                let (he, _) = ext_field[&o];
                prog.push_str(&format!("let r{k2} = (n, it.{he}) find in let m{k2} = r{k2} map (x -> (x.0, x.1) lt) in "));
                mask_terms.push(format!("m{k2}"));
                matched_lo.push(format!("r{k2}"));
            }
            // combined mask
            let mut mexpr = mask_terms[0].clone();
            for t in &mask_terms[1..] {
                mexpr = format!("(({mexpr}, {t}) zip map (q -> (q.0, q.1) mul))");
            }
            prog.push_str(&format!("let m = {mexpr} in let es = (n, m) filter in "));
            // extras refs per matched e, per extending candidate in order: mine by filter, others by gather
            let mut xs: Vec<String> = Vec::new();
            for (k2, &o) in extending.iter().enumerate() {
                let (_, ox) = ext_field[&o];
                if k2 == k {
                    prog.push_str(&format!("let x{k2} = (it.{nx} clone, m) filter in "));
                } else {
                    prog.push_str(&format!("let x{k2} = ((r{k2} map (x -> x.0), m) filter, it.{ox}) gather in "));
                }
                xs.push(format!("x{k2}"));
            }
            // per matched e: (e, cross) -> pairs; zip e with its per-e extras lists first
            prog.push_str("let per_e = ");
            // build per-e tuple (e, x0ref, x1ref, ...) as nested zips
            let mut z = "es".to_string();
            for xk in &xs {
                z = format!("({z}, {xk}) zip");
            }
            prog.push_str(&z);
            prog.push_str(" in ");
            // per e: rows (e, (cross of clones)); the cross for k extending candidates:
            let mut per_e_body = String::new();
            // access paths inside the nested zip tuple q: es = q(.0)^k, x_i = ...
            let depth_z = xs.len();
            let e_path = { let mut p = "q".to_string(); for _ in 0..depth_z { p.push_str(".0"); } p };
            // extras refs for this e: x_i at path q(.0)^(k-i)(.1) — compute explicitly
            let xpath = |i: usize| -> String { let mut p = "q".to_string(); for _ in 0..(depth_z - 1 - i) { p.push_str(".0"); } format!("{p}.1") };
            // cross product innermost-first
            let mut cross2 = format!("{} clone", xpath(depth_z - 1));
            for i in (0..depth_z - 1).rev() {
                cross2 = format!("((({} clone, {cross2}) cap_list) map (w -> (w.1, w.0) cap_list) flatten).1 map (w -> (w.1, w.0))", xpath(i));
            }
            per_e_body.push_str(&format!("q -> ({e_path}, {cross2}) cap_list"));
            prog.push_str(&format!("let rows = ((per_e map ({per_e_body})) flatten).1 in "));
            // leaves: cross each row with the item's leaf tuples
            let rows_with_leaf = match leaf_idx {
                None => "rows map (q -> (q.0, q.1, q.0))".to_string(),
                Some(li) => format!("(((((it.{li}, rows) cap_list) map (w -> (w.1, w.0) cap_list)) flatten).1 map (w -> (w.0.0, w.0.1, w.1)))"),
            };
            prog.push_str(&format!("({prefix_expr}, {rows_with_leaf}) cap_list) flatten).1 map ({})", head_of()));
            // the lane's needles are candidate ci's; the extras order in `head_of` is extending
            // order regardless of which candidate is the needle side (x_i are indexed by k2).
            let out = progs.run(&prog, lane);
            pieces.push(out);
        }
    }
    let out = concat_all(progs, pieces)?;
    // the head's leading variables that are the seed's leading layers, in order: the output rows
    // come out grouped by them (trie order), so their dedup need only sort the rest within groups
    let k = rule.head.vars.iter().zip(&s.vars).take(deepest).take_while(|(h, sv)| h == sv).count();
    if std::env::var("GALEN_TRACE").is_ok() {
        eprintln!("    {}[seed {seed}]: {} -> {} rows (grouped on {k}) in {:.2}s", rule.head.rel, count(&layers[0]), count(&out), ts.elapsed().as_secs_f64());
    }
    Some(Piece { rows: out, grouped: k, arity: rule.head.vars.len() })
}

/// a rule-seed's output: flat head rows, grouped (in order) by their first `grouped` columns
struct Piece {
    rows: Value,
    grouped: usize,
    arity: usize,
}

/// a piece as a sorted, deduplicated flat list: the grouped prefix is already in order, so
/// `group` is linear on it and only the remaining columns sort, within each group (datatoad's
/// sort of the new layers only); a piece with no grouped prefix sorts whole.
fn piece_sorted(progs: &mut Progs, p: Piece) -> Value {
    let (k, a) = (p.grouped, p.arity);
    // Measured on GALEN: the grouped path costs what the flat sort costs (corgi's sort already
    // refines field by field, and the group / cap_list / flatten passes eat the saving), so it is
    // opt-in (GALEN_GROUPED=1); it is the first half of a trie-shaped output side, kept as such.
    if k == 0 || k >= a || std::env::var("GALEN_GROUPED").is_err() {
        return progs.run("input sort dedup", p.rows);
    }
    let key: Vec<usize> = (0..k).collect();
    let rest: Vec<usize> = (k..a).collect();
    let flat: Vec<String> = (0..k).map(|i| if k == 1 { "q.0".to_string() } else { format!("q.0.{i}") })
        .chain((0..a - k).map(|i| if a - k == 1 { "q.1".to_string() } else { format!("q.1.{i}") }))
        .collect();
    progs.run(
        &format!(
            "let g = input map (t -> ({}, {})) group in \
             ((g map (p -> (p.0, p.1 sort dedup) cap_list)) flatten).1 map (q -> ({}))",
            tuple_expr(&key), tuple_expr(&rest), flat.join(", ")
        ),
        p.rows,
    )
}

/// `(a, b, c) -> ((a, b) mul, c) mul` over a comma-separated list of 0/1 expressions
fn fold_mul(list: &str, n: usize) -> String {
    if n == 1 {
        return list.to_string();
    }
    let parts: Vec<&str> = split_top(list);
    let mut e = parts[0].to_string();
    for p in &parts[1..] {
        e = format!("({e}, {p}) mul");
    }
    e
}
/// split a comma-separated list at depth 0
fn split_top(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0i32, 0usize);
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(s[start..].trim());
    out
}

/// semi-naive fixpoint of `rules` over `rels` (IDB relations named in `idb`); returns rounds
fn fixpoint(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rules: &[Rule], idb: &[&'static str]) -> usize {
    let mut round = 0;
    loop {
        round += 1;
        let tr = Instant::now();
        let mut derived: HashMap<&'static str, Vec<Piece>> = HashMap::new();
        for rule in rules {
            for seed in 0..rule.body.len() {
                if let Some(out) = eval_rule(progs, rels, rule, seed) {
                    derived.entry(rule.head.rel).or_default().push(out);
                }
            }
        }
        let mut any = false;
        let mut report = Vec::new();
        for &name in idb {
            let outs = derived.remove(name).unwrap_or_default();
            let raw_rows: usize = outs.iter().map(|p| count(&p.rows)).sum();
            let tb = Instant::now();
            let new: Option<Value> = sorted_union(progs, outs);
            let t_dedup = tb.elapsed().as_secs_f64();
            let rel = rels.get_mut(name).unwrap();
            let ta = Instant::now();
            if let Some(recent) = rel.recent.take() {
                let mut batch = recent;
                while let Some(last) = rel.stable.last() {
                    if count(&last.tuples) <= 2 * count(&batch.tuples) {
                        let last = rel.stable.pop().unwrap();
                        batch = Batch::new(union(last.tuples, batch.tuples));
                    } else {
                        break;
                    }
                }
                rel.stable.push(batch);
            }
            let t_lsm = ta.elapsed().as_secs_f64();
            let mut next = new;
            let tc = Instant::now();
            if let Some(mut v) = next.take() {
                for b in &rel.stable {
                    v = progs.run2("let (new, old) = input in let r = (new, old) find in (new, r map (x -> (x.0, x.1) eq)) filter", v, b.tuples.clone());
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
        if std::env::var("GALEN_QUIET").is_err() {
            println!("round {round:>3} {:>6.2}s  {}", tr.elapsed().as_secs_f64(), report.join("  "));
        }
        if !any {
            return round;
        }
    }
}

/// all tuples of a relation, as sorted rows (host-side, for checking)
fn rows_of(progs: &mut Progs, rel: &Rel) -> Vec<Vec<u64>> {
    let mut all: Vec<Value> = rel.stable.iter().map(|b| b.tuples.clone()).collect();
    if let Some(r) = &rel.recent {
        all.push(r.tuples.clone());
    }
    let Some(v) = concat_all(progs, all) else { return Vec::new() };
    let flat = progs.run("input sort dedup", v);
    // read back through the surface: one column at a time via `map (t -> t.i)` and show
    let n = count(&flat);
    let arity = match &flat { Value::List(_, p) => match &**p { Value::Prod(c) => c.len(), _ => 1 }, _ => 1 };
    let mut cols: Vec<Vec<u64>> = Vec::new();
    for i in 0..arity {
        let c = if arity == 1 { flat.clone() } else { progs.run(&format!("input map (t -> t.{i})"), flat.clone()) };
        let text = corgi::show(&c);
        let inner = text.trim_start_matches("List ends=[").split("] <").nth(1).unwrap_or("").trim_end_matches('>');
        let vals: Vec<u64> = inner.trim_matches(|c| c == '[' || c == ']').split(", ").filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect();
        assert_eq!(vals.len(), n);
        cols.push(vals);
    }
    (0..n).map(|r| cols.iter().map(|c| c[r]).collect()).collect()
}

/// naive fixpoint: nested loops over all atoms, to a set, until nothing changes
fn naive(rules: &[Rule], rels: &HashMap<&'static str, Vec<Vec<u64>>>, idb: &[&'static str]) -> HashMap<&'static str, std::collections::BTreeSet<Vec<u64>>> {
    use std::collections::BTreeSet;
    let mut db: HashMap<&'static str, BTreeSet<Vec<u64>>> = rels.iter().map(|(k, v)| (*k, v.iter().cloned().collect())).collect();
    loop {
        let mut added = false;
        for rule in rules {
            let mut env: Vec<HashMap<&'static str, u64>> = vec![HashMap::new()];
            for a in &rule.body {
                let mut next = Vec::new();
                for e in &env {
                    for t in &db[a.rel] {
                        let mut e2 = e.clone();
                        let mut ok = true;
                        for (v, &x) in a.vars.iter().zip(t) {
                            match e2.get(v) {
                                Some(&y) if y != x => { ok = false; break; }
                                _ => { e2.insert(v, x); }
                            }
                        }
                        if ok { next.push(e2); }
                    }
                }
                env = next;
            }
            for e in env {
                let t: Vec<u64> = rule.head.vars.iter().map(|v| e[v]).collect();
                if db.get_mut(rule.head.rel).unwrap().insert(t) { added = true; }
            }
        }
        if !added { break; }
    }
    let _ = idb;
    db
}

fn selftest() {
    let mut x = 0x2545F4914F6CDD1Du64;
    let mut next = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
    let mut gen = |arity: usize, n: usize, range: u64| -> Vec<Vec<u64>> {
        (0..n).map(|_| (0..arity).map(|_| next() % range).collect()).collect()
    };
    let base: HashMap<&'static str, Vec<Vec<u64>>> = HashMap::from([
        ("a", gen(2, 60, 12)), ("b", gen(3, 80, 12)), ("c", gen(2, 60, 12)), ("d", gen(3, 80, 12)), ("s", gen(1, 6, 12)),
        ("t", Vec::new()), ("u", Vec::new()), ("v", Vec::new()), ("w", Vec::new()), ("k", Vec::new()), ("m", Vec::new()),
    ]);
    let rules = vec![
        Rule { head: atom("t", &["x", "z"]), body: vec![atom("a", &["x", "y"]), atom("b", &["y", "z", "q"])] },          // binary, extras
        Rule { head: atom("u", &["x", "z"]), body: vec![atom("a", &["x", "y"]), atom("b", &["x", "y", "z"])] },          // two-layer key
        Rule { head: atom("v", &["x", "w"]), body: vec![atom("a", &["x", "y"]), atom("c", &["y", "w"]), atom("c", &["x", "w"])] }, // two candidates
        Rule { head: atom("w", &["x", "w"]), body: vec![atom("a", &["x", "y"]), atom("c", &["y", "w"]), atom("c", &["x", "w"]), atom("d", &["x", "y", "w"])] }, // three, one with a 2-layer key
        Rule { head: atom("k", &["x", "y"]), body: vec![atom("a", &["x", "y"]), atom("s", &["x"])] },                    // validation only
        Rule { head: atom("m", &["z", "x", "y"]), body: vec![atom("b", &["x", "y", "z"]), atom("a", &["x", "q"])] },      // leaves in the head
        Rule { head: atom("t", &["x", "z"]), body: vec![atom("t", &["x", "y"]), atom("t", &["y", "z"])] },               // recursion
    ];
    let idb: Vec<&'static str> = vec!["t", "u", "v", "w", "k", "m"];
    let expect = naive(&rules, &base, &idb);
    let mut progs = Progs(HashMap::new(), 0, 0, HashMap::new());
    let mut rels: HashMap<&'static str, Rel> = HashMap::new();
    for (name, rows) in &base {
        let arity = if rows.is_empty() { rules.iter().find(|r| r.head.rel == *name).unwrap().head.vars.len() } else { rows[0].len() };
        let mut cols = vec![Vec::new(); arity];
        for r in rows { for (i, &v) in r.iter().enumerate() { cols[i].push(v); } }
        let tuples = progs.run("input sort dedup", one_row(cols));
        let is_idb = idb.contains(name);
        // an IDB relation seeded by facts starts as a recent batch; an empty one has nothing
        let (stable, recent) = if is_idb { (vec![], (!rows.is_empty()).then(|| Batch::new(tuples))) } else { (vec![Batch::new(tuples)], None) };
        rels.insert(name, Rel { stable, recent });
    }
    // IDB relations with no facts still need a recent batch to seed the first round: the EDB
    // rules fire from the EDB atoms' recent batches — so mark every EDB relation recent for round 1
    for (name, rel) in rels.iter_mut() {
        if !idb.contains(name) {
            if let Some(b) = rel.stable.pop() { rel.recent = Some(b); }
        }
    }
    let rounds = fixpoint(&mut progs, &mut rels, &rules, &idb);
    let mut bad = 0;
    for name in &idb {
        let got: std::collections::BTreeSet<Vec<u64>> = rows_of(&mut progs, &rels[name]).into_iter().collect();
        let want = &expect[name];
        if &got != want {
            bad += 1;
            println!("MISMATCH {name}: got {} rows, want {}; missing {:?}; extra {:?}", got.len(), want.len(),
                want.difference(&got).take(3).collect::<Vec<_>>(), got.difference(want).take(3).collect::<Vec<_>>());
        } else {
            println!("ok {name}: {} rows", got.len());
        }
    }
    println!("selftest: {} rounds, {} mismatches", rounds, bad);
    if bad > 0 { std::process::exit(1); }
}

fn main() {
    if std::env::var("GALEN_SELFTEST").is_ok() {
        std::env::set_var("GALEN_QUIET", "1");
        selftest();
        return;
    }
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/Users/mcsherry/Projects/datasets/flowlog/galen".to_string());
    let t0 = Instant::now();
    let mut progs = Progs(HashMap::new(), 0, 0, HashMap::new());
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
    let round = fixpoint(&mut progs, &mut rels, &rules, &["p", "q"]);
    println!("fixpoint in {:.2}s ({} rounds); p = {}, q = {}", t1.elapsed().as_secs_f64(), round, rels["p"].total(), rels["q"].total());
    println!("corgi programs run: {} ({} distinct), input rows summed over runs: {}M", progs.1, progs.0.len(), progs.2 / 1_000_000);
    let mut by: Vec<(&String, &(u64, u64, f64))> = progs.3.iter().collect();
    by.sort_by(|a, b| b.1 .2.partial_cmp(&a.1 .2).unwrap());
    println!("top programs by time (secs, runs, Mrows, ns/row):");
    for (text, (runs, rows, secs)) in by.iter().take(14) {
        let short: String = text.chars().take(110).collect();
        println!("  {secs:>6.2}s {runs:>6} {:>7.1}M {:>6.1}  {short}", *rows as f64 / 1e6, secs * 1e9 / (*rows as f64).max(1.0));
    }
}
