//! Ground truth for GALEN rule 5, the cyclic one:
//!
//!     p(x,z) :- c(y,w,z), p(x,w), p(x,y).
//!
//! A triangle on (x,w,y): p gives edges (x,w) and (x,y), c gives (w,y) and
//! carries payload z. Run "the wrong way" (join p and c on y first) it forms a
//! ~2-billion-tuple intermediate; the worst-case-optimal way seeds from c and,
//! per tuple, intersects in_p(w) and in_p(y) smaller-into-larger -- ~|c| seeks.
//!
//! This computes rule 5 once over the base relations, both ways, to fix the
//! output size and confirm the blowup. It is the reference the sprig words must
//! match. Run with:  cargo run --release --bin galen [dir]

use std::collections::HashMap;
use std::time::Instant;

fn read_csv(path: &str, arity: usize) -> Vec<Vec<i64>> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {}", path, e));
    text.lines().map(|line| {
        let row: Vec<i64> = line.split(',').map(|f| f.trim().parse().unwrap()).collect();
        assert_eq!(row.len(), arity, "bad arity in {}", path);
        row
    }).collect()
}

/// GALEN rule 6, the cyclic q-twice-plus-r rule:
///
///     q(x,e,o) :- q(x,y,z), q(z,u,o), r(y,u,e).
///
/// A triangle on (y,z,u): q gives edges (y,z) and (z,u), r gives (y,u). Three
/// distinct relation instances, with payloads x, o, e cross-producting into the
/// head. WCO core is a 2-atom meet: seed with q's (x,y,z), extend u in
/// q-by-z intersect r-by-y; then expand payloads o (from q at z,u) and e (from
/// r at y,u), and dedup. Returns the distinct (x,e,o) count.
fn rule6(q: &[Vec<i64>], r: &[Vec<i64>]) -> (usize, usize) {
    let key = |a: i64, b: i64| (a << 21) | b;             // pair key, ids < 2^21
    let sorted_distinct = |rows: &[Vec<i64>], k: usize, v: usize| {
        let mut m: HashMap<i64, Vec<i64>> = HashMap::new();
        for t in rows { m.entry(t[k]).or_default().push(t[v]); }
        for vs in m.values_mut() { vs.sort_unstable(); vs.dedup(); }
        m
    };
    let idx_zu = sorted_distinct(q, 0, 1);    // z -> { u : q(z,u,*) }
    let idx_yu = sorted_distinct(r, 0, 1);    // y -> { u : r(y,u,*) }
    let mut q_o: HashMap<i64, Vec<i64>> = HashMap::new();  // (z,u) -> { o }
    for t in q { q_o.entry(key(t[0], t[1])).or_default().push(t[2]); }
    let mut r_e: HashMap<i64, Vec<i64>> = HashMap::new();  // (y,u) -> { e }
    for t in r { r_e.entry(key(t[0], t[1])).or_default().push(t[2]); }

    let empty = Vec::new();
    let mut out: Vec<u64> = Vec::new();
    let mut core = 0usize;                         // (x,y,z,u) survivors, pre-payload
    for t in q {                                  // q1: (x,y,z)
        let (x, y, z) = (t[0], t[1], t[2]);
        let az = idx_zu.get(&z).unwrap_or(&empty);
        let ay = idx_yu.get(&y).unwrap_or(&empty);
        let (small, large) = if az.len() <= ay.len() { (az, ay) } else { (ay, az) };
        for &u in small {
            if large.binary_search(&u).is_ok() {
                core += 1;
                let os = q_o.get(&key(z, u)).unwrap_or(&empty);
                let es = r_e.get(&key(y, u)).unwrap_or(&empty);
                for &o in os { for &e in es {
                    out.push(((x as u64) << 42) | ((e as u64) << 21) | o as u64);
                } }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    (core, out.len())
}

/// Transitive closure of base `p` (GALEN rule 1: p(x,z):-p(x,y),p(y,z)), semi-naive.
/// Returns the closure size -- the target/scale for running `tcL` on real data.
fn tc_base_p(p: &[Vec<i64>]) -> usize {
    let mut succ: HashMap<i64, Vec<i64>> = HashMap::new();
    for e in p { succ.entry(e[0]).or_default().push(e[1]); }
    for v in succ.values_mut() { v.sort_unstable(); v.dedup(); }
    let pack = |x: i64, z: i64| ((x as u64) << 21) | z as u64;
    let mut closure: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut delta: Vec<(i64, i64)> = Vec::new();
    for e in p { if closure.insert(pack(e[0], e[1])) { delta.push((e[0], e[1])); } }
    let empty = Vec::new();
    let mut rounds = 0;
    while !delta.is_empty() {
        rounds += 1;
        let mut nd = Vec::new();
        for &(x, y) in &delta {
            for &z in succ.get(&y).unwrap_or(&empty) {
                if closure.insert(pack(x, z)) { nd.push((x, z)); }
            }
        }
        delta = nd;
    }
    println!("tc(base p): {} edges -> {} closure pairs, {} rounds", p.len(), closure.len(), rounds);
    closure.len()
}

fn main() {
    let dir = std::env::args().nth(1)
        .unwrap_or_else(|| "/Users/mcsherry/Projects/datasets/flowlog/galen".into());

    // rule 6 (non-degenerate on base data; datatoad says 25753).
    let q = read_csv(&format!("{}/Q.csv", dir), 3);
    let r = read_csv(&format!("{}/R.csv", dir), 3);
    let t = Instant::now();
    let (core, n6) = rule6(&q, &r);
    println!("rule6  q={} r={}   core (x,y,z,u) survivors = {}   -> {} distinct (x,e,o)   ({:.0} ms)   [datatoad: 25753]",
             q.len(), r.len(), core, n6, t.elapsed().as_secs_f64() * 1e3);

    let t = Instant::now();
    let p = read_csv(&format!("{}/P.csv", dir), 2);
    let c = read_csv(&format!("{}/C.csv", dir), 3);
    println!("loaded p={} c={}   ({:.0} ms)", p.len(), c.len(), t.elapsed().as_secs_f64() * 1e3);

    let t = Instant::now();
    tc_base_p(&p);
    println!("  ({:.0} ms)", t.elapsed().as_secs_f64() * 1e3);

    // in_p[v] = sorted { x : p(x,v) } -- p indexed by its second column (target).
    let t = Instant::now();
    let mut in_p: HashMap<i64, Vec<i64>> = HashMap::new();
    for e in &p { in_p.entry(e[1]).or_default().push(e[0]); }
    for v in in_p.values_mut() { v.sort_unstable(); v.dedup(); }
    println!("built in_p index   ({:.0} ms)", t.elapsed().as_secs_f64() * 1e3);

    // WCO: seed c, per (y,w,z) intersect in_p(w) & in_p(y) smaller-first, emit (x,z).
    let empty: Vec<i64> = Vec::new();
    let t = Instant::now();
    let mut out: Vec<u64> = Vec::new();           // packed (x,z), x and z fit in 32 bits
    let mut probes = 0u64;
    for row in &c {
        let (y, w, z) = (row[0], row[1], row[2]);
        let aw = in_p.get(&w).unwrap_or(&empty);
        let ay = in_p.get(&y).unwrap_or(&empty);
        let (small, large) = if aw.len() <= ay.len() { (aw, ay) } else { (ay, aw) };
        probes += small.len() as u64;
        for &x in small {
            if large.binary_search(&x).is_ok() {
                out.push(((x as u64) << 32) | z as u64);
            }
        }
    }
    let emitted = out.len();
    out.sort_unstable();
    out.dedup();
    let wco_ms = t.elapsed().as_secs_f64() * 1e3;

    println!("WCO   {:>8.0} ms   {} probes   {} (x,z) emitted   {} distinct",
             wco_ms, probes, emitted, out.len());
    println!("wrong-way p|c-on-y intermediate would be ~2.08e9 tuples (measured by awk)");
}
