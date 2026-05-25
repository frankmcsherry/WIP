//! Stage B — graph-to-graph optimization passes.
//!
//! Each pass takes a [`Graph`] and returns a [`Graph`], composable in any
//! order (though some orderings expose more: `elide_routing` before `cse`
//! lets CSE see shared producers the routing terms were hiding). All
//! passes are order-preserving — surviving terms keep their relative
//! positions — so they don't change evaluation order or lifetimes.

use crate::pipeline::graph::{Graph, Term, OutRef};
use crate::pipeline::sysop::SystemOp;

/// Cheap key for hash-cons: op-debug-repr captures op identity +
/// parameters baked into the variant (`Cat { n }`'s n, `Proj { i }`'s i).
/// `None` for side-effecting ops, which must not be merged — re-running
/// them changes observable behavior.
fn cse_key_for(op: &SystemOp) -> Option<String> {
    if op.is_side_effecting() {
        return None;
    }
    Some(format!("{:?}", op))
}

/// Common-subexpression elimination. Merges terms with the same op key
/// and the same (already-canonicalized) children, rewiring consumers.
/// Returns the new graph and the number of merges. Most effective after
/// `elide_routing`.
pub fn cse(g: Graph) -> (Graph, usize) {
    use std::collections::HashMap;
    let mut memo: HashMap<(String, Vec<OutRef>), usize> = HashMap::new();
    let mut new_id: Vec<usize> = vec![usize::MAX; g.terms.len()];
    let mut new_terms: Vec<Term> = Vec::with_capacity(g.terms.len());
    let mut hits = 0usize;

    for (old_id, mut term) in g.terms.into_iter().enumerate() {
        for ch in term.children.iter_mut() {
            ch.term = new_id[ch.term];
        }
        match cse_key_for(&term.op) {
            Some(key) => {
                let k = (key, term.children.clone());
                if let Some(&existing) = memo.get(&k) {
                    new_id[old_id] = existing;
                    hits += 1;
                } else {
                    let id = new_terms.len();
                    memo.insert(k, id);
                    new_id[old_id] = id;
                    new_terms.push(term);
                }
            }
            None => {
                let id = new_terms.len();
                new_id[old_id] = id;
                new_terms.push(term);
            }
        }
    }
    let new_roots = g.roots.iter()
        .map(|r| OutRef { term: new_id[r.term], idx: r.idx })
        .collect();
    (Graph { terms: new_terms, roots: new_roots }, hits)
}

/// Resolve pure stack-routing ops (`dup`/`drop`/`swap`/`over`/`rot`/
/// `pick`/`roll`) into direct edges, using each op's `routing_map`.
/// Consumers are rewired straight to the aliased producer and the routing
/// term is dropped. `drop` (empty map) leaves its input with one fewer
/// consumer — run `eliminate_dead` afterward to collect orphans.
pub fn elide_routing(g: Graph) -> Graph {
    use std::collections::HashMap;
    let mut subst: HashMap<OutRef, OutRef> = HashMap::new();
    let mut new_id: Vec<Option<usize>> = vec![None; g.terms.len()];
    let mut new_terms: Vec<Term> = Vec::with_capacity(g.terms.len());

    // Resolve an old-id OutRef to its post-elision, reindexed form. Topo
    // order guarantees the referent was already processed.
    let resolve = |subst: &HashMap<OutRef, OutRef>, new_id: &[Option<usize>], r: OutRef| -> OutRef {
        match subst.get(&r) {
            Some(&s) => s,
            None => OutRef { term: new_id[r.term].expect("graph: child term neither kept nor substituted"), idx: r.idx },
        }
    };

    for (old_id, mut term) in g.terms.into_iter().enumerate() {
        for ch in term.children.iter_mut() {
            *ch = resolve(&subst, &new_id, *ch);
        }
        match term.op.routing_map() {
            Some(map) => {
                for (out_i, &in_i) in map.iter().enumerate() {
                    subst.insert(OutRef { term: old_id, idx: out_i }, term.children[in_i]);
                }
            }
            None => {
                new_id[old_id] = Some(new_terms.len());
                new_terms.push(term);
            }
        }
    }
    let new_roots = g.roots.iter()
        .map(|r| resolve(&subst, &new_id, *r))
        .collect();
    Graph { terms: new_terms, roots: new_roots }
}

/// Dead-term elimination. Keeps only terms reachable from `roots`, plus
/// side-effecting and body-bearing ops (whose execution is observable
/// beyond their data outputs). Reindexes survivors.
pub fn eliminate_dead(g: Graph) -> Graph {
    let n = g.terms.len();
    let mut live: Vec<bool> = vec![false; n];

    for r in &g.roots { live[r.term] = true; }
    for (i, t) in g.terms.iter().enumerate() {
        if t.op.is_side_effecting() || t.op.has_body() {
            live[i] = true;
        }
    }

    let mut changed = true;
    while changed {
        changed = false;
        for (i, t) in g.terms.iter().enumerate() {
            if !live[i] { continue; }
            for ch in &t.children {
                if !live[ch.term] {
                    live[ch.term] = true;
                    changed = true;
                }
            }
        }
    }

    let mut new_id: Vec<usize> = vec![usize::MAX; n];
    let mut new_terms: Vec<Term> = Vec::with_capacity(n);
    for (old_id, term) in g.terms.into_iter().enumerate() {
        if !live[old_id] { continue; }
        new_id[old_id] = new_terms.len();
        new_terms.push(term);
    }
    for term in new_terms.iter_mut() {
        for ch in term.children.iter_mut() {
            ch.term = new_id[ch.term];
        }
    }
    let new_roots: Vec<OutRef> = g.roots.iter()
        .map(|r| OutRef { term: new_id[r.term], idx: r.idx })
        .collect();
    Graph { terms: new_terms, roots: new_roots }
}
