//! Datalog over corgi, one variable at a time: a semi-naive driver whose whole data plane is corgi
//! programs, and whose Rust is only the plan.
//!
//! `cargo run --release --example toad` runs GALEN; `TOAD_SELFTEST=1` checks seven rule shapes
//! against a naive evaluator on random relations.
//!
//! A rule with the delta at atom S is evaluated as a sequence of STAGES from the seed's tuples.
//! A stage binds one more variable v (the unbound one shared by the most atoms that already have a
//! bound key — datatoad's most-constrained-first): every atom containing v with a bound key is a
//! candidate; per tuple, the candidate's key is looked up (one `find`, layered inside corgi for a
//! multi-column key) and yields a REF to the distinct v's under it; the tuple extends by the
//! candidate with the fewest — cloned, that is the worst-case-optimal budget — and each proposed v
//! is validated by a `find` into every other candidate's ref. The stage's output, the tuples with
//! v appended, sorted, is the next stage's salad. Atoms bound by the seed alone validate first; a
//! candidate is validated by the stage that binds its last variable. The head is projected last.
//! Relations are LSMs of sorted batches; a stage consults each batch (the delta convention), sums
//! counts across them, and unions a winner's lists across them.
use corgi::{Program, Value};
use std::collections::HashMap;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---- values ---------------------------------------------------------------------------------
fn count(v: &Value) -> usize {
    match v { Value::List(_, vals) => vals.len(), other => other.len() }
}
fn unnest(v: Value) -> Value {
    match v { Value::List(_, vals) => *vals, other => panic!("unnest: {}", corgi::show(&other)) }
}
fn nest(v: Value) -> Value {
    let n = v.len();
    Value::List(vec![n].into(), Box::new(v))
}
fn unfail(v: Value) -> Value {
    match v {
        Value::Sum(_, lanes) if lanes.len() == 2 && matches!(lanes[1], Value::Unit(_)) => {
            if let Value::Unit(n) = lanes[1] { assert_eq!(n, 0, "a corgi stage failed on {n} rows"); }
            lanes.into_iter().next().unwrap()
        }
        other => other,
    }
}
fn one_row(cols: Vec<Vec<u64>>) -> Value {
    let n = cols.first().map_or(0, |c| c.len());
    let narrow = cols.iter().all(|c| c.iter().all(|&x| x <= u32::MAX as u64)) && std::env::var("TOAD_U64").is_err();
    let cols = cols.into_iter().map(|c| if narrow { Value::u32(c.into_iter().map(|x| x as u32).collect()) } else { Value::u64(c) }).collect();
    Value::List(vec![n].into(), Box::new(Value::Prod(cols)))
}
/// concatenate one-row lists in one multi-source gather
fn concat(mut pieces: Vec<Value>) -> Option<Value> {
    pieces.retain(|v| count(v) > 0);
    if pieces.len() < 2 { return pieces.pop(); }
    let payloads: Vec<Value> = pieces.into_iter().map(unnest).collect();
    let (mut tags, mut off) = (Vec::new(), Vec::new());
    for (t, p) in payloads.iter().enumerate() { tags.extend(std::iter::repeat_n(t, p.len())); off.extend(0..p.len()); }
    let srcs: Vec<Option<&Value>> = payloads.iter().map(Some).collect();
    Some(nest(corgi::arrange::gather_lanes(&srcs, &tags, &off)))
}
/// the union of two sorted deduplicated one-row lists: one merge, one gather
fn union(a: Value, b: Value) -> Value {
    use corgi::arrange::GroupRun;
    if count(&a) == 0 { return b; }
    if count(&b) == 0 { return a; }
    let (pa, pb) = (unnest(a), unnest(b));
    let (mut tags, mut off) = (Vec::new(), Vec::new());
    for r in corgi::arrange::survey_groups(&pa, &pb) {
        match r {
            GroupRun::A(lo, hi) => { tags.extend(std::iter::repeat_n(0usize, hi - lo)); off.extend(lo..hi); }
            GroupRun::B(lo, hi) => { tags.extend(std::iter::repeat_n(1usize, hi - lo)); off.extend(lo..hi); }
            GroupRun::Both(alo, ..) => { tags.push(0); off.push(alo); }
        }
    }
    nest(corgi::arrange::gather_lanes(&[Some(&pa), Some(&pb)], &tags, &off))
}
fn union_all(mut vs: Vec<Value>) -> Option<Value> {
    while vs.len() > 1 {
        let mut next = Vec::new();
        let mut it = vs.into_iter();
        while let Some(a) = it.next() { next.push(match it.next() { Some(b) => union(a, b), None => a }); }
        vs = next;
    }
    vs.pop()
}

struct Progs(HashMap<String, (Program, u64, u64, f64)>); // per text: program, runs, rows, secs
impl Progs {
    fn run(&mut self, text: &str, input: Value) -> Value {
        let rows = match &input { Value::Prod(c) => c.iter().map(|v| count(v) as u64).max().unwrap_or(0), v => count(v) as u64 };
        let e = self.0.entry(text.to_string()).or_insert_with(|| (Program::compile_ml(text).unwrap_or_else(|e| panic!("compile {text:?}: {e}")), 0, 0, 0.0));
        let t = Instant::now();
        let out = unfail(e.0.run_partial(input));
        e.1 += 1; e.2 += rows; e.3 += t.elapsed().as_secs_f64();
        out
    }
    fn report(&self) {
        let mut by: Vec<_> = self.0.iter().collect();
        by.sort_by(|a, b| b.1 .3.partial_cmp(&a.1 .3).unwrap());
        for (text, (_, runs, rows, secs)) in by.iter().take(12) {
            println!("  {secs:>6.2}s {runs:>6} {:>7.1}M {:>6.1} ns/row  {}", *rows as f64 / 1e6, secs * 1e9 / (*rows as f64).max(1.0), text.chars().take(100).collect::<String>());
        }
    }
}
/// `t.i` or `(t.i, t.j, ..)`
fn tup(base: &str, cols: &[usize]) -> String {
    if cols.len() == 1 { format!("{base}.{}", cols[0]) } else { format!("({})", cols.iter().map(|c| format!("{base}.{c}")).collect::<Vec<_>>().join(", ")) }
}

// ---- relations ------------------------------------------------------------------------------
/// an index of a batch for a stage: sorted distinct keys and, per key, a ref to the sorted
/// distinct extension values under it. One empty list is appended as the target of a miss, so a
/// tuple whose key is absent proposes nothing and validates nothing — no masks anywhere.
struct Index { keys: Value, exts: Value }
struct Batch { tuples: Value, perms: HashMap<Vec<usize>, Value>, indexes: HashMap<(Vec<usize>, Option<usize>), Index> }
impl Batch {
    fn new(tuples: Value) -> Batch { Batch { tuples, perms: HashMap::new(), indexes: HashMap::new() } }
    /// the tuples sorted by `cols` (datatoad's `Permute-*` form): one sort per column order, kept
    fn perm(&mut self, progs: &mut Progs, cols: &[usize]) -> Value {
        if !self.perms.contains_key(cols) {
            let v = progs.run(&format!("((input map (t -> ({}, t)) group map (p -> (p.0, p.1) cap_list)) flatten).1 map (q -> q.1)", tup("t", cols)), self.tuples.clone());
            self.perms.insert(cols.to_vec(), v);
        }
        self.perms[cols].clone()
    }
    fn index(&mut self, progs: &mut Progs, key: &[usize], ext: Option<usize>) -> &Index {
        let k = (key.to_vec(), ext);
        if !self.indexes.contains_key(&k) {
            // off the copy sorted by (key, ext) the group is linear (runs of equal keys) and each
            // group's ext values are already in order
            let order: Vec<usize> = key.iter().copied().chain(ext).collect();
            let sorted = self.perm(progs, &order);
            let ix = match ext {
                None => Index { keys: progs.run(&format!("input map (t -> {}) dedup", tup("t", key)), sorted), exts: Value::Unit(0) },
                Some(e) => {
                    let g = progs.run(&format!("let g = input map (t -> ({}, t.{e})) group in (g map (p -> p.0), g map (p -> p.1 dedup))", tup("t", key)), sorted);
                    let Value::Prod(mut c) = g else { panic!() };
                    let lists = c.pop().unwrap();
                    let keys = c.pop().unwrap();
                    let inner = match corgi::shape_of_value(&lists) { corgi::Shape::List(s) => match *s { corgi::Shape::List(p) => *p, _ => panic!() }, _ => panic!() };
                    let empty_row = nest(Value::List(vec![0usize].into(), Box::new(Value::empty(&inner))));
                    let lists = progs.run("let (a, b) = input in (a, b) append", Value::Prod(vec![lists, empty_row]));
                    Index { keys, exts: progs.run("input map (l -> l ref)", lists) }
                }
            };
            self.indexes.insert(k.clone(), ix);
        }
        &self.indexes[&k]
    }
}
struct Rel { stable: Vec<Batch>, recent: Option<Batch> }
impl Rel {
    fn total(&self) -> usize { self.stable.iter().map(|b| count(&b.tuples)).sum::<usize>() + self.recent.as_ref().map_or(0, |b| count(&b.tuples)) }
}
/// the batches of atom `j`'s relation a stage may see: stable, plus recent for atoms after the seed
fn batches<'a>(rels: &'a mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, j: usize) -> Vec<&'a mut Batch> {
    let rel = rels.get_mut(rule.body[j].rel).unwrap();
    let mut out: Vec<&mut Batch> = rel.stable.iter_mut().collect();
    if j > seed { if let Some(r) = rel.recent.as_mut() { out.push(r); } }
    out
}

// ---- rules ----------------------------------------------------------------------------------
struct Atom { rel: &'static str, vars: Vec<&'static str> }
struct Rule { head: Atom, body: Vec<Atom> }
fn atom(rel: &'static str, vars: &[&'static str]) -> Atom { Atom { rel, vars: vars.to_vec() } }

/// the salad: sorted distinct tuples over `vars`, in that column order
struct Salad { tuples: Value, vars: Vec<&'static str> }

/// the shared-with-salad columns of atom `j`, as (salad positions, atom positions), in salad order
fn key_of(salad: &Salad, a: &Atom) -> (Vec<usize>, Vec<usize>) {
    let mut sp = Vec::new();
    let mut ap = Vec::new();
    for (i, v) in salad.vars.iter().enumerate() {
        if let Some(p) = a.vars.iter().position(|w| w == v) { sp.push(i); ap.push(p); }
    }
    (sp, ap)
}

/// evaluate one rule with the delta at `seed`: head tuples, unsorted, or None
fn eval_rule(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize) -> Option<Value> {
    let s = &rule.body[seed];
    let tuples = rels.get(s.rel)?.recent.as_ref()?.tuples.clone();
    let ts = Instant::now();
    let n0 = count(&tuples);
    let mut salad = Salad { tuples, vars: s.vars.clone() };
    let mut pending: Vec<usize> = (0..rule.body.len()).filter(|&j| j != seed).collect();
    loop {
        // atoms bound by what is already in the salad: validate (a semijoin per batch, any match keeps)
        let bound: Vec<usize> = pending.iter().copied().filter(|&j| rule.body[j].vars.iter().all(|v| salad.vars.contains(v))).collect();
        for j in bound {
            pending.retain(|&k| k != j);
            let (sp, ap) = key_of(&salad, &rule.body[j]);
            let mut keep: Option<Value> = None;
            for b in batches(rels, rule, seed, j) {
                let ix = b.index(progs, &ap, None);
                let m = progs.run(&format!("let (t, keys) = input in (t map (q -> {}), keys) find map (x -> (x.0, x.1) lt)", tup("q", &sp)), Value::Prod(vec![salad.tuples.clone(), ix.keys.clone()]));
                keep = Some(match keep { None => m, Some(prev) => progs.run("let (a, b) = input in (a, b) zip map (x -> (x.0, x.1) max)", Value::Prod(vec![prev, m])) });
            }
            salad.tuples = progs.run("let (t, m) = input in (t, m) filter", Value::Prod(vec![salad.tuples, keep?]));
            if count(&salad.tuples) == 0 { return None; }
        }
        if pending.is_empty() && rule.head.vars.iter().all(|v| salad.vars.contains(v)) { break; }
        // the next variable: unbound, proposable (some atom containing it has a bound key), shared by the most atoms
        let mut unbound: Vec<&'static str> = Vec::new();
        for &j in &pending { for v in &rule.body[j].vars { if !salad.vars.contains(v) && !unbound.contains(v) { unbound.push(v); } } }
        let proposable = |v: &&'static str| pending.iter().any(|&j| rule.body[j].vars.contains(v) && rule.body[j].vars.iter().any(|w| salad.vars.contains(w)));
        let v = unbound.iter().copied().filter(proposable).max_by_key(|v| pending.iter().filter(|&&j| rule.body[j].vars.contains(v)).count())
            .unwrap_or_else(|| panic!("rule {}: no proposable variable (a cross product)", rule.head.rel));
        let cands: Vec<usize> = pending.iter().copied().filter(|&j| rule.body[j].vars.contains(&v) && rule.body[j].vars.iter().any(|w| salad.vars.contains(w))).collect();
        salad = stage(progs, rels, rule, seed, salad, v, &cands);
        if count(&salad.tuples) == 0 { return None; }
        // a candidate whose variables are now all bound was validated by this stage
        pending.retain(|&j| !(cands.contains(&j) && rule.body[j].vars.iter().all(|w| salad.vars.contains(w))));
    }
    let head: Vec<usize> = rule.head.vars.iter().map(|v| salad.vars.iter().position(|w| w == v).unwrap()).collect();
    let out = progs.run(&format!("input map (t -> {})", tup("t", &head)), salad.tuples);
    if std::env::var("TOAD_TRACE").is_ok() { eprintln!("    {}[seed {seed}]: {n0} -> {} rows in {:.2}s", rule.head.rel, count(&out), ts.elapsed().as_secs_f64()); }
    Some(out)
}

/// one stage: bind `v` to the salad through `cands` (atoms containing v with a bound key)
fn stage(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rule: &Rule, seed: usize, salad: Salad, v: &'static str, cands: &[usize]) -> Salad {
    let n = salad.vars.len();
    // the output's shape (the salad's columns plus one), for an empty result
    let out_shape = {
        use corgi::Shape;
        let cols: Vec<Shape> = match corgi::shape_of_value(&salad.tuples) { Shape::List(inner) => match *inner { Shape::Prod(c) => c, other => vec![other] }, _ => panic!() };
        let mut all = cols.clone();
        all.push(cols[0].clone());
        Shape::List(Box::new(Shape::Prod(all)))
    };
    // align the salad to the first candidate's key (datatoad's `align_to`): that candidate's
    // lookups then merge instead of search; the others search
    let (sp0, _) = key_of(&salad, &rule.body[cands[0]]);
    let mut t = progs.run(&format!("((input map (t -> ({}, t)) group map (p -> (p.0, p.1) cap_list)) flatten).1 map (q -> q.1)", tup("t", &sp0)), salad.tuples);
    // attach, per (candidate, batch), a ref to the distinct v's under the tuple's key (empty on a miss)
    let mut refs: Vec<Vec<usize>> = Vec::new(); // refs[c] = the field of each batch's ref
    let mut nf = n;
    for &j in cands {
        let a = &rule.body[j];
        let (sp, ap) = key_of(&salad_ref(&t, &salad.vars), a);
        let e = a.vars.iter().position(|w| *w == v).unwrap();
        let nb = batches(rels, rule, seed, j).len();
        let mut fs = Vec::new();
        for b in 0..nb {
            let (keys, exts) = { let bs = batches(rels, rule, seed, j); let ix = bs.into_iter().nth(b).unwrap().index(progs, &ap, Some(e)); (ix.keys.clone(), ix.exts.clone()) };
            let flat: Vec<String> = (0..nf).map(|i| format!("p.0.{i}")).chain(["p.1".to_string()]).collect();
            // a miss resolves to position `keys len`, the appended empty list (the count is data,
            // not program text, so one program serves every batch)
            t = progs.run(
                &format!(
                    "let (t, keys, exts) = input in let r = ((keys len, t) cap_list, (t map (q -> {}), keys) find) zip in \
                     let pos = r map (x -> let lo = x.1.0 in let m = (lo, x.1.1) lt in ((lo, m) mul, ((m lit 1, m) sub, x.0.0) mul) add) in \
                     (t, (pos, exts) gather) zip map (p -> ({}))",
                    tup("q", &sp), flat.join(", ")
                ),
                Value::Prod(vec![t, keys, exts]),
            );
            fs.push(nf);
            nf += 1;
        }
        refs.push(fs);
    }
    // a candidate with no visible batch (the delta convention) proposes nothing: the stage is empty
    if refs.iter().any(|fs| fs.is_empty()) {
        return Salad { tuples: nest(Value::empty(&out_shape)), vars: salad.vars.iter().copied().chain([v]).collect() };
    }
    // per tuple, each candidate's count (its lists' lengths summed over batches); every candidate must propose
    let cnt: Vec<String> = refs.iter().map(|fs| fs.iter().map(|f| format!("it.{f} len")).reduce(|a, b| format!("({a}, {b}) add")).unwrap()).collect();
    let nz = cnt.iter().map(|c| format!("({c}, {c} lit 0) ne")).reduce(|a, b| format!("({a}, {b}) mul")).unwrap();
    t = progs.run(&format!("(input, input map (it -> {nz})) filter"), t);
    let vars_out: Vec<&'static str> = salad.vars.iter().copied().chain([v]).collect();
    let mut pieces = Vec::new();
    for (k, fs) in refs.iter().enumerate() {
        // lane k: candidate k has the fewest (ties to the earliest)
        let cond = cnt.iter().enumerate().filter(|&(k2, _)| k2 != k).map(|(k2, c)| format!("({}, {c}) {}", cnt[k], if k2 < k { "lt" } else { "le" })).reduce(|a, b| format!("({a}, {b}) mul"));
        let lane = match cond { Some(c) => progs.run(&format!("(input, input map (it -> {c})) filter"), t.clone()), None => t.clone() };
        if count(&lane) == 0 { continue; }
        // proposals: the winner's lists cloned and merged across its batches; each proposal must be
        // in some batch of every other candidate (a find into its ref; an empty list matches nothing)
        let needles = fs.iter().map(|f| format!("it.{f} clone")).reduce(|a, b| format!("({a}, {b}) append")).unwrap();
        let needles = if fs.len() > 1 { format!("({needles}) sort dedup") } else { needles };
        let mut prog = format!("(input map (it -> let n = {needles} in ");
        let mut mask = "n map (x -> x lit 1)".to_string();
        for (k2, fs2) in refs.iter().enumerate() {
            if k2 == k { continue; }
            let any = fs2.iter().map(|f| format!("(n, it.{f}) find map (x -> (x.0, x.1) lt)")).reduce(|a, b| format!("(({a}, {b}) zip map (x -> (x.0, x.1) max))")).unwrap();
            mask = format!("(({mask}, {any}) zip map (x -> (x.0, x.1) mul))");
        }
        let cols: Vec<String> = (0..n).map(|i| if n == 1 { "q.0".to_string() } else { format!("q.0.{i}") }).chain(["q.1".to_string()]).collect();
        prog.push_str(&format!("({}, (n, {mask}) filter) cap_list) flatten).1 map (q -> ({}))", tup("it", &(0..n).collect::<Vec<_>>()), cols.join(", ")));
        pieces.push(progs.run(&prog, lane));
    }
    // lanes partition the tuples, so their pieces concatenate; the next stage re-aligns anyway
    let tuples = concat(pieces).unwrap_or_else(|| nest(Value::empty(&out_shape)));
    Salad { tuples, vars: vars_out }
}
/// a salad view over tuples that have extra fields appended (only `vars` positions are consulted)
fn salad_ref(t: &Value, vars: &[&'static str]) -> Salad { Salad { tuples: t.clone(), vars: vars.to_vec() } }

// ---- the fixpoint -----------------------------------------------------------------------------
fn fixpoint(progs: &mut Progs, rels: &mut HashMap<&'static str, Rel>, rules: &[Rule], idb: &[&'static str]) -> usize {
    let mut round = 0;
    loop {
        round += 1;
        let tr = Instant::now();
        let mut derived: HashMap<&'static str, Vec<Value>> = HashMap::new();
        for rule in rules {
            for seed in 0..rule.body.len() {
                if let Some(out) = eval_rule(progs, rels, rule, seed) { derived.entry(rule.head.rel).or_default().push(out); }
            }
        }
        let mut any = false;
        let mut report = Vec::new();
        // every relation's recent batch becomes stable (an EDB relation is recent in round 1 only)
        let mut names: Vec<&'static str> = rels.keys().copied().collect();
        names.sort();
        for name in names {
            let outs = derived.remove(name).unwrap_or_default();
            let sorted: Vec<Value> = outs.into_iter().filter(|v| count(v) > 0).map(|v| progs.run("input sort dedup", v)).collect();
            let new = union_all(sorted);
            let rel = rels.get_mut(name).unwrap();
            if let Some(recent) = rel.recent.take() {
                let mut batch = recent;
                while let Some(last) = rel.stable.last() {
                    if count(&last.tuples) > 2 * count(&batch.tuples) { break; }
                    let last = rel.stable.pop().unwrap();
                    batch = Batch::new(union(last.tuples, batch.tuples));
                }
                rel.stable.push(batch);
            }
            let mut next = new;
            if let Some(mut v) = next.take() {
                for b in &rel.stable {
                    v = progs.run("let (new, old) = input in let r = (new, old) find in (new, r map (x -> (x.0, x.1) eq)) filter", Value::Prod(vec![v, b.tuples.clone()]));
                }
                if count(&v) > 0 { any = true; next = Some(v); }
            }
            rel.recent = next.map(Batch::new);
            if idb.contains(&name) { report.push(format!("{name}: total {} (+{})", rel.total(), rel.recent.as_ref().map_or(0, |b| count(&b.tuples)))); }
        }
        if std::env::var("TOAD_QUIET").is_err() { println!("round {round:>3} {:>6.2}s  {}", tr.elapsed().as_secs_f64(), report.join("  ")); }
        if !any { return round; }
    }
}

// ---- checking ---------------------------------------------------------------------------------
fn rows_of(progs: &mut Progs, rel: &Rel) -> std::collections::BTreeSet<Vec<u64>> {
    let mut all: Vec<Value> = rel.stable.iter().map(|b| b.tuples.clone()).collect();
    if let Some(r) = &rel.recent { all.push(r.tuples.clone()); }
    let Some(v) = concat(all) else { return Default::default() };
    let flat = progs.run("input sort dedup", v);
    let n = count(&flat);
    let arity = match &flat { Value::List(_, p) => match &**p { Value::Prod(c) => c.len(), _ => 1 }, _ => 1 };
    let cols: Vec<Vec<u64>> = (0..arity).map(|i| {
        let c = if arity == 1 { flat.clone() } else { progs.run(&format!("input map (t -> t.{i})"), flat.clone()) };
        let text = corgi::show(&c);
        let inner = text.split("] <").nth(1).unwrap_or("").trim_end_matches('>');
        inner.trim_matches(|c| c == '[' || c == ']').split(", ").filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect()
    }).collect();
    (0..n).map(|r| cols.iter().map(|c| c[r]).collect()).collect()
}
fn naive(rules: &[Rule], base: &HashMap<&'static str, Vec<Vec<u64>>>) -> HashMap<&'static str, std::collections::BTreeSet<Vec<u64>>> {
    let mut db: HashMap<&'static str, std::collections::BTreeSet<Vec<u64>>> = base.iter().map(|(k, v)| (*k, v.iter().cloned().collect())).collect();
    loop {
        let mut added = false;
        for rule in rules {
            let mut env: Vec<HashMap<&'static str, u64>> = vec![HashMap::new()];
            for a in &rule.body {
                let mut next = Vec::new();
                for e in &env { for t in &db[a.rel] {
                    let mut e2 = e.clone();
                    if a.vars.iter().zip(t).all(|(v, &x)| match e2.insert(v, x) { Some(y) => y == x, None => true }) { next.push(e2); }
                } }
                env = next;
            }
            for e in env { if db.get_mut(rule.head.rel).unwrap().insert(rule.head.vars.iter().map(|v| e[v]).collect()) { added = true; } }
        }
        if !added { return db; }
    }
}
fn load_rels(progs: &mut Progs, base: &HashMap<&'static str, Vec<Vec<u64>>>, arities: &HashMap<&'static str, usize>, idb: &[&'static str]) -> HashMap<&'static str, Rel> {
    let mut rels = HashMap::new();
    for (&name, rows) in base {
        let arity = arities[name];
        let mut cols = vec![Vec::new(); arity];
        for r in rows { for (i, &x) in r.iter().enumerate() { cols[i].push(x); } }
        let tuples = progs.run("input sort dedup", one_row(cols));
        // every relation with facts starts as a recent batch, so rules seeded from it fire in round 1
        let has = !rows.is_empty();
        let _ = idb;
        rels.insert(name, Rel { stable: vec![], recent: has.then(|| Batch::new(tuples)) });
    }
    rels
}
fn selftest() {
    let mut x = 0x2545F4914F6CDD1Du64;
    let mut next = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
    let mut gen = |arity: usize, n: usize, range: u64| -> Vec<Vec<u64>> { (0..n).map(|_| (0..arity).map(|_| next() % range).collect()).collect() };
    let base: HashMap<&'static str, Vec<Vec<u64>>> = HashMap::from([
        ("a", gen(2, 60, 12)), ("b", gen(3, 80, 12)), ("c", gen(2, 60, 12)), ("d", gen(3, 80, 12)), ("s", gen(1, 6, 12)),
        ("t", vec![]), ("u", vec![]), ("v", vec![]), ("w", vec![]), ("k", vec![]), ("m", vec![]), ("g", vec![]),
    ]);
    let rules = vec![
        Rule { head: atom("t", &["x", "z"]), body: vec![atom("a", &["x", "y"]), atom("b", &["y", "z", "q"])] },
        Rule { head: atom("u", &["x", "z"]), body: vec![atom("a", &["x", "y"]), atom("b", &["x", "y", "z"])] },
        Rule { head: atom("v", &["x", "w"]), body: vec![atom("a", &["x", "y"]), atom("c", &["y", "w"]), atom("c", &["x", "w"])] },
        Rule { head: atom("w", &["x", "w"]), body: vec![atom("a", &["x", "y"]), atom("c", &["y", "w"]), atom("c", &["x", "w"]), atom("d", &["x", "y", "w"])] },
        Rule { head: atom("k", &["x", "y"]), body: vec![atom("a", &["x", "y"]), atom("s", &["x"])] },
        Rule { head: atom("m", &["z", "x", "y"]), body: vec![atom("b", &["x", "y", "z"]), atom("a", &["x", "q"])] },
        Rule { head: atom("t", &["x", "z"]), body: vec![atom("t", &["x", "y"]), atom("t", &["y", "z"])] },
        // two stages: y then z, with an atom that needs both stages before it can validate
        Rule { head: atom("g", &["x", "z"]), body: vec![atom("a", &["x", "y"]), atom("b", &["y", "z", "q"]), atom("d", &["x", "z", "y"])] },
    ];
    let idb = ["t", "u", "v", "w", "k", "m", "g"];
    let mut arities: HashMap<&'static str, usize> = base.iter().filter(|(_, r)| !r.is_empty()).map(|(k, r)| (*k, r[0].len())).collect();
    for r in &rules { arities.insert(r.head.rel, r.head.vars.len()); }
    let expect = naive(&rules, &base);
    let mut progs = Progs(HashMap::new());
    let mut rels = load_rels(&mut progs, &base, &arities, &idb);
    let rounds = fixpoint(&mut progs, &mut rels, &rules, &idb);
    let mut bad = 0;
    for name in idb {
        let got = rows_of(&mut progs, &rels[name]);
        let want = &expect[name];
        if &got != want { bad += 1; println!("MISMATCH {name}: got {} want {}; missing {:?} extra {:?}", got.len(), want.len(), want.difference(&got).take(3).collect::<Vec<_>>(), got.difference(want).take(3).collect::<Vec<_>>()); }
        else { println!("ok {name}: {} rows", got.len()); }
    }
    println!("selftest: {rounds} rounds, {bad} mismatches");
    if bad > 0 { std::process::exit(1); }
}

fn main() {
    if std::env::var("TOAD_SELFTEST").is_ok() { std::env::set_var("TOAD_QUIET", "1"); selftest(); return; }
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/Users/mcsherry/Projects/datasets/flowlog/galen".to_string());
    let mut progs = Progs(HashMap::new());
    let mut base: HashMap<&'static str, Vec<Vec<u64>>> = HashMap::new();
    let mut arities = HashMap::new();
    for (name, arity) in [("c", 3), ("p", 2), ("q", 3), ("r", 3), ("s", 2), ("u", 3)] {
        let text = std::fs::read_to_string(format!("{dir}/{}.csv", name.to_uppercase())).unwrap();
        base.insert(name, text.lines().filter(|l| !l.trim().is_empty()).map(|l| l.split(',').map(|f| f.trim().parse().unwrap()).collect()).collect());
        arities.insert(name, arity);
    }
    let rules = vec![
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("p", &["x", "y"]), atom("p", &["y", "z"])] },
        Rule { head: atom("q", &["x", "r", "z"]), body: vec![atom("p", &["x", "y"]), atom("q", &["y", "r", "z"])] },
        Rule { head: atom("q", &["x", "q2", "z"]), body: vec![atom("q", &["x", "r", "z"]), atom("s", &["r", "q2"])] },
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("p", &["y", "w"]), atom("u", &["w", "r", "z"]), atom("q", &["x", "r", "y"])] },
        Rule { head: atom("p", &["x", "z"]), body: vec![atom("c", &["y", "w", "z"]), atom("p", &["x", "w"]), atom("p", &["x", "y"])] },
        Rule { head: atom("q", &["x", "e", "o"]), body: vec![atom("q", &["x", "y", "z"]), atom("q", &["z", "u", "o"]), atom("r", &["y", "u", "e"])] },
    ];
    let idb = ["p", "q"];
    let mut rels = load_rels(&mut progs, &base, &arities, &idb);
    let t = Instant::now();
    let rounds = fixpoint(&mut progs, &mut rels, &rules, &idb);
    println!("fixpoint in {:.2}s ({rounds} rounds); p = {}, q = {}; {} distinct programs", t.elapsed().as_secs_f64(), rels["p"].total(), rels["q"].total(), progs.0.len());
    progs.report();
}
