//! The merge kernel, rank at a time: two structurally sorted columns surveyed as the maximal
//! runs exclusive to one side and the equal classes common to both, level by level.
//!
//! [`survey_groups`] walks the two columns together by the same descent the sort makes: the
//! leading leaf decides most of the interleaving with one gallop, and every class it leaves equal
//! is refined by the next level at once — a product's next field, a sum's lane at the carried
//! offsets, a list's next element position — never one pair at a time. A level reads its keys
//! through index lists into the two columns; the reports form a tree, an equal class holding its
//! refinement as children, flattened once at the end. A level's work is proportional to the rows
//! of the classes it refines — a full pass over both inputs while a leading field leaves most
//! rows tied, and nothing once it does not — and never to the reports already made. Nothing is
//! gathered, and the shape is walked once per level rather than once per comparison. Layout, top
//! down: the reports, the entry point, the level walk, the leaf merges, the report tree.

use crate::value::{Bounds, Prim, Value};

/// One report from [`survey_groups`]: a maximal range from one side that comes next in merged
/// order, or an equal class as the maximal ranges on both sides.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GroupRun {
    /// Rows `a[lo..hi)` come next, all strictly less than the next row of `b`.
    A(usize, usize),
    /// Rows `b[lo..hi)` come next, all strictly less than the next row of `a`.
    B(usize, usize),
    /// Rows `a[alo..ahi)` and `b[blo..bhi)` are all structurally equal, and maximal on both sides.
    Both(usize, usize, usize, usize),
}

// ---- entry point ----------------------------------------------------------------------------

/// Survey `a` and `b`, both in structural order. Ensures: the `A` ranges and the `a` halves of
/// the `Both` classes partition `0..a.len()` in order without gap or overlap, likewise `b`;
/// expanding the reports to their rows yields a non-decreasing structural sequence; every row of
/// a `Both` class on either side is structurally equal to every other, and the class is maximal;
/// adjacent reports never share a side.
pub fn survey_groups(a: &Value, b: &Value) -> Vec<GroupRun> {
    let mut tree = Tree { nodes: vec![Node::Class { alo: 0, ahi: a.len(), blo: 0, bhi: b.len(), la: 0, lb: 0, kids: None }] };
    level(a, b, Identity, Identity, &[0], &mut tree);
    tree.flatten()
}

// ---- the level walk -------------------------------------------------------------------------

/// How a level finds the row behind a local position: the identity at the top, an index list
/// below. Generic so the top level pays no indirection.
trait Rows: Copy {
    fn row(self, j: usize) -> usize;
}
#[derive(Copy, Clone)]
struct Identity;
impl Rows for Identity {
    #[inline]
    fn row(self, j: usize) -> usize {
        j
    }
}
impl Rows for &[usize] {
    #[inline]
    fn row(self, j: usize) -> usize {
        self[j]
    }
}

/// Refine the open classes `open` (node ids) at this level of the shape. `ia` and `ib` are the
/// index lists the classes' local offsets refer to; each class's rows are `ia[la..la + len]`.
fn level<IA: Rows, IB: Rows>(a: &Value, b: &Value, ia: IA, ib: IB, open: &[usize], tree: &mut Tree) {
    match (a, b) {
        (Value::Prim(pa), Value::Prim(pb)) => leaf(pa, pb, ia, ib, open, tree),
        (Value::Unit(_), Value::Unit(_)) => merge(|_| 0u8, |_| 0u8, open, tree),
        (Value::Prod(ca), Value::Prod(cb)) => {
            assert_eq!(ca.len(), cb.len(), "survey: product arity");
            // a run of leaf lanes is one level keyed by their tuple, as a structural compare
            // reads them; the first level reads the level's own lists, each later one the rows
            // of the classes the level before left equal.
            let mut cur: Vec<usize> = open.to_vec();
            let (mut sa, mut sb): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
            let mut first = true;
            let mut f = 0;
            while f < ca.len() && !cur.is_empty() {
                let (lanes, g) = leaf_run(ca, cb, f);
                let next = if g > f { g } else { f + 1 };
                if g > f && lanes.is_empty() {
                    f = next; // units only: nothing to decide
                    continue;
                }
                if first {
                    if g > f { lanes_level(&lanes, ia, ib, &cur, tree) } else { level(&ca[f], &cb[f], ia, ib, &cur, tree) }
                } else {
                    if g > f { lanes_level(&lanes, &sa[..], &sb[..], &cur, tree) } else { level(&ca[f], &cb[f], &sa[..], &sb[..], &cur, tree) }
                }
                // The report tree is already complete after the last field.
                // Row lists and local offsets are only needed by a next field;
                // an enclosing level reconstructs its own lists from the tree.
                if next == ca.len() { break; }
                (cur, sa, sb) = if first { tree.refined(&cur, ia, ib) }
                    else { tree.refined(&cur, &sa[..], &sb[..]) };
                first = false;
                f = next;
            }
        }
        (Value::Sum(ta, va), Value::Sum(tb, vb)) => {
            assert_eq!(va.len(), vb.len(), "survey: sum arity");
            // the tag first, then each lane over its classes at the carried offsets.
            merge(|j| ta.tag_at(ia.row(j)), |j| tb.tag_at(ib.row(j)), open, tree);
            let (cur, ra, rb) = tree.refined(open, ia, ib);
            let mut by_lane: Vec<Vec<usize>> = vec![Vec::new(); va.len()];
            for &c in &cur {
                let (_, oa, _) = tree.class(c);
                by_lane[ta.tag_at(ra[oa])].push(c);
            }
            for (t, cs) in by_lane.iter().enumerate() {
                if cs.is_empty() {
                    continue;
                }
                let (mut la, mut lb) = (Vec::new(), Vec::new());
                for &c in cs {
                    let (cl, oa, ob) = tree.class(c);
                    tree.set_offsets(c, la.len(), lb.len());
                    la.extend(ra[oa..oa + cl.ahi - cl.alo].iter().map(|&r| ta.offset_at(r)));
                    lb.extend(rb[ob..ob + cl.bhi - cl.blo].iter().map(|&r| tb.offset_at(r)));
                }
                level(&va[t], &vb[t], &la[..], &lb[..], cs, tree);
            }
        }
        (Value::List(ba, va), Value::List(bb, vb)) => {
            // the length first, then element by element over the classes still that long.
            let len = |bounds: &Bounds, r: usize| {
                let (s, e) = bounds.span(r);
                e - s
            };
            merge(|j| len(ba, ia.row(j)), |j| len(bb, ib.row(j)), open, tree);
            let (mut cur, mut ra, mut rb) = tree.refined(open, ia, ib);
            let mut pos = 0;
            while !cur.is_empty() {
                // the classes whose rows are longer than `pos` refine at it; the rest are equal
                // throughout and stay as they are.
                let (mut live, mut la, mut lb, mut live_ra, mut live_rb) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
                let (mut oa, mut ob) = (0usize, 0usize);
                for &c in &cur {
                    let (cl, _, _) = tree.class(c);
                    let (na, nb) = (cl.ahi - cl.alo, cl.bhi - cl.blo);
                    // one row a side over leaf elements: the rest of the two spans decide at
                    // once, rather than a level per remaining position.
                    if na == 1 && nb == 1 {
                        if let Some(ord) = cmp_spans(va, vb, ba.span(ra[oa]), bb.span(rb[ob]), pos) {
                            let first = tree.nodes.len();
                            match ord {
                                std::cmp::Ordering::Less => {
                                    tree.push(Node::A(cl.alo, cl.ahi));
                                    tree.push(Node::B(cl.blo, cl.bhi));
                                }
                                std::cmp::Ordering::Greater => {
                                    tree.push(Node::B(cl.blo, cl.bhi));
                                    tree.push(Node::A(cl.alo, cl.ahi));
                                }
                                std::cmp::Ordering::Equal => {}
                            }
                            if ord != std::cmp::Ordering::Equal {
                                tree.set_kids(c, first, tree.nodes.len());
                            }
                            oa += na;
                            ob += nb;
                            continue;
                        }
                    }
                    if len(ba, ra[oa]) > pos {
                        live.push(c);
                        tree.set_offsets(c, la.len(), lb.len());
                        la.extend(ra[oa..oa + na].iter().map(|&r| ba.span(r).0 + pos));
                        lb.extend(rb[ob..ob + nb].iter().map(|&r| bb.span(r).0 + pos));
                        live_ra.extend_from_slice(&ra[oa..oa + na]);
                        live_rb.extend_from_slice(&rb[ob..ob + nb]);
                    }
                    oa += na;
                    ob += nb;
                }
                if live.is_empty() {
                    break;
                }
                level(va, vb, &la[..], &lb[..], &live, tree);
                (cur, ra, rb) = tree.refined(&live, &live_ra[..], &live_rb[..]);
                pos += 1;
            }
        }
        _ => panic!("survey: shape mismatch"),
    }
}

// ---- the leaf merges ------------------------------------------------------------------------

/// A leaf level: one width dispatch, then the merge over every open class with the keys read
/// through the index.
fn leaf<IA: Rows, IB: Rows>(pa: &Prim, pb: &Prim, ia: IA, ib: IB, open: &[usize], tree: &mut Tree) {
    macro_rules! go {
        ($($V:ident),*) => {
            match (pa, pb) {
                $( (Prim::$V(va), Prim::$V(vb)) => merge(|j| va[ia.row(j)], |j| vb[ib.row(j)], open, tree), )*
                _ => panic!("survey: leaf width mismatch"),
            }
        };
    }
    go!(U8, U16, U32, U64)
}

/// The order of the elements `sa + from..ea` of `va` against `sb + from..eb` of `vb`, when both
/// are leaves: one slice comparison. `None` for any other element shape.
fn cmp_spans(va: &Value, vb: &Value, (sa, ea): (usize, usize), (sb, eb): (usize, usize), from: usize) -> Option<std::cmp::Ordering> {
    let (Value::Prim(pa), Value::Prim(pb)) = (va, vb) else { return None };
    macro_rules! go {
        ($($V:ident),*) => {
            match (pa, pb) {
                $( (Prim::$V(x), Prim::$V(y)) => Some(x[sa + from..ea].cmp(&y[sb + from..eb])), )*
                _ => panic!("survey: leaf width mismatch"),
            }
        };
    }
    go!(U8, U16, U32, U64)
}

/// The leaf lanes of `a` and `b`, in structural order, when the shape is nothing but leaves,
/// units and products of those; `false` otherwise.
fn leaves<'a>(a: &'a Value, b: &'a Value, out: &mut Vec<(&'a Prim, &'a Prim)>) -> bool {
    match (a, b) {
        (Value::Prim(pa), Value::Prim(pb)) => {
            out.push((pa, pb));
            true
        }
        (Value::Unit(_), Value::Unit(_)) => true,
        (Value::Prod(xa), Value::Prod(xb)) => xa.len() == xb.len() && xa.iter().zip(xb).all(|(x, y)| leaves(x, y, out)),
        _ => false,
    }
}

/// From field `f` on, the run of fields that are leaves, units, or products of those, as their
/// lanes in order, at most four; returns the lanes and the first field not taken. A field whose
/// lanes would not fit ends the run, so an empty run with `g == f` means the field is structured.
fn leaf_run<'a>(ca: &'a [Value], cb: &'a [Value], f: usize) -> (Vec<(&'a Prim, &'a Prim)>, usize) {
    let mut lanes = Vec::new();
    let mut g = f;
    while g < ca.len() {
        let mut more = Vec::new();
        if !leaves(&ca[g], &cb[g], &mut more) || lanes.len() + more.len() > 4 {
            break;
        }
        lanes.extend(more);
        g += 1;
    }
    (lanes, g)
}

/// A level keyed by a run of leaf lanes, compared as a tuple: what a structural comparison of
/// those fields does, in one gallop. Every lane `u64` reads the leaves directly.
/// The lanes as `u64` slices, when every one is a `u64` leaf: the common shape, read directly.
fn wide<'a>(lanes: &[(&'a Prim, &'a Prim)]) -> Option<Vec<(&'a [u64], &'a [u64])>> {
    lanes
        .iter()
        .map(|(pa, pb)| match (pa, pb) {
            (Prim::U64(va), Prim::U64(vb)) => Some((&va[..], &vb[..])),
            _ => None,
        })
        .collect()
}

fn lanes_level<IA: Rows, IB: Rows>(lanes: &[(&Prim, &Prim)], ia: IA, ib: IB, open: &[usize], tree: &mut Tree) {
    if let Some(w) = wide(lanes) {
        return match w.len() {
            1 => merge(|j| w[0].0[ia.row(j)], |j| w[0].1[ib.row(j)], open, tree),
            2 => merge(|j| { let r = ia.row(j); (w[0].0[r], w[1].0[r]) }, |j| { let r = ib.row(j); (w[0].1[r], w[1].1[r]) }, open, tree),
            3 => merge(|j| { let r = ia.row(j); (w[0].0[r], w[1].0[r], w[2].0[r]) }, |j| { let r = ib.row(j); (w[0].1[r], w[1].1[r], w[2].1[r]) }, open, tree),
            _ => merge(|j| { let r = ia.row(j); (w[0].0[r], w[1].0[r], w[2].0[r], w[3].0[r]) }, |j| { let r = ib.row(j); (w[0].1[r], w[1].1[r], w[2].1[r], w[3].1[r]) }, open, tree),
        };
    }
    let at = |x: usize, j: usize| lanes[x].0.usize_at(ia.row(j));
    let bt = |x: usize, j: usize| lanes[x].1.usize_at(ib.row(j));
    match lanes.len() {
        1 => leaf(lanes[0].0, lanes[0].1, ia, ib, open, tree),
        2 => merge(|j| (at(0, j), at(1, j)), |j| (bt(0, j), bt(1, j)), open, tree),
        3 => merge(|j| (at(0, j), at(1, j), at(2, j)), |j| (bt(0, j), bt(1, j), bt(2, j)), open, tree),
        _ => merge(|j| (at(0, j), at(1, j), at(2, j), at(3, j)), |j| (bt(0, j), bt(1, j), bt(2, j), bt(3, j)), open, tree),
    }
}

/// Merge every open class on one key: `ka(j)` and `kb(j)` are the keys at local positions `j`
/// of the level's index lists. Within a class the walk gallops: an exclusive run is found by
/// doubling then bisecting, and an equal class by galloping past its duplicates on each side, so
/// a long run costs its logarithm. The reports become the class's children.
fn merge<T: Ord + Copy>(ka: impl Fn(usize) -> T, kb: impl Fn(usize) -> T, open: &[usize], tree: &mut Tree) {
    for &c in open {
        let (cl, la, lb) = tree.class(c);
        let (ea, eb) = (la + cl.ahi - cl.alo, lb + cl.bhi - cl.blo);
        let (mut i, mut j) = (la, lb);
        let top_a = |i: usize| cl.alo + i - la;
        let top_b = |j: usize| cl.blo + j - lb;
        // the class's children are the nodes pushed from here on, contiguous.
        let first = tree.nodes.len();
        while i < ea && j < eb {
            match ka(i).cmp(&kb(j)) {
                std::cmp::Ordering::Less => {
                    let s = i;
                    i += 1;
                    gallop(&mut i, ea, |k| ka(k) < kb(j));
                    tree.push(Node::A(top_a(s), top_a(i)));
                }
                std::cmp::Ordering::Greater => {
                    let s = j;
                    j += 1;
                    gallop(&mut j, eb, |k| kb(k) < ka(i));
                    tree.push(Node::B(top_b(s), top_b(j)));
                }
                std::cmp::Ordering::Equal => {
                    let key = ka(i);
                    let (si, sj) = (i, j);
                    i += 1;
                    gallop(&mut i, ea, |k| ka(k) == key);
                    j += 1;
                    gallop(&mut j, eb, |k| kb(k) == key);
                    tree.push(Node::Class { alo: top_a(si), ahi: top_a(i), blo: top_b(sj), bhi: top_b(j), la: si, lb: sj, kids: None });
                }
            }
        }
        if i < ea {
            tree.push(Node::A(top_a(i), top_a(ea)));
        }
        if j < eb {
            tree.push(Node::B(top_b(j), top_b(eb)));
        }
        tree.set_kids(c, first, tree.nodes.len());
    }
}

/// Advance `idx` while `pred` holds, by doubling steps then bisection: `O(log gap)` probes.
fn gallop(idx: &mut usize, hi: usize, pred: impl Fn(usize) -> bool) {
    if *idx < hi && pred(*idx) {
        let mut step = 1;
        while *idx + step < hi && pred(*idx + step) {
            *idx += step;
            step <<= 1;
        }
        step >>= 1;
        while step > 0 {
            if *idx + step < hi && pred(*idx + step) {
                *idx += step;
            }
            step >>= 1;
        }
        *idx += 1;
    }
}

// ---- the report tree ------------------------------------------------------------------------

/// An equal-so-far class as ranges of top-level positions.
#[derive(Copy, Clone)]
struct Class {
    alo: usize,
    ahi: usize,
    blo: usize,
    bhi: usize,
}

/// The reports as a tree: an exclusive run, or an equal class with its refinement as children,
/// a contiguous range of nodes. `kids` is `None` until a level has refined the class, which is
/// what makes it equal throughout when the tree is read; a refined class may have no children
/// at all (two empty inputs), which is not the same thing. `la`/`lb` are the class's offsets in
/// the index lists of the level that made it.
enum Node {
    A(usize, usize),
    B(usize, usize),
    Class { alo: usize, ahi: usize, blo: usize, bhi: usize, la: usize, lb: usize, kids: Option<(usize, usize)> },
}

struct Tree {
    nodes: Vec<Node>,
}

impl Tree {
    fn push(&mut self, n: Node) {
        self.nodes.push(n);
    }
    fn set_kids(&mut self, c: usize, first: usize, end: usize) {
        match &mut self.nodes[c] {
            Node::Class { kids, .. } => *kids = Some((first, end)),
            _ => unreachable!("only a class is refined"),
        }
    }
    /// a class node's ranges and local offsets.
    fn class(&self, c: usize) -> (Class, usize, usize) {
        match &self.nodes[c] {
            Node::Class { alo, ahi, blo, bhi, la, lb, .. } => (Class { alo: *alo, ahi: *ahi, blo: *blo, bhi: *bhi }, *la, *lb),
            _ => unreachable!("only a class is refined"),
        }
    }
    /// point class `c` at its rows in the lists the next level will read.
    fn set_offsets(&mut self, c: usize, la: usize, lb: usize) {
        match &mut self.nodes[c] {
            Node::Class { la: a, lb: b, .. } => {
                *a = la;
                *b = lb;
            }
            _ => unreachable!("only a class is refined"),
        }
    }
    /// After a level has refined the classes `open`: the classes now equal throughout under
    /// them, in order, and their rows read from the lists `open`'s offsets refer to — a
    /// refinement only partitions, so a class's rows are a sub-range of its ancestor's segment.
    /// The returned classes' offsets now name their places in the lists returned.
    fn refined<IA: Rows, IB: Rows>(&mut self, open: &[usize], ia: IA, ib: IB) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        let (mut cur, mut sa, mut sb) = (Vec::new(), Vec::new(), Vec::new());
        let mut stack = Vec::new();
        for &p in open {
            let (top, la, lb) = self.class(p);
            stack.push(p);
            while let Some(n) = stack.pop() {
                if let Node::Class { alo, ahi, blo, bhi, kids, .. } = &self.nodes[n] {
                    if let Some((s, e)) = *kids {
                        stack.extend((s..e).rev());
                    } else {
                        let (oa, ob) = (la + alo - top.alo, lb + blo - top.blo);
                        let (na, nb) = (ahi - alo, bhi - blo);
                        cur.push(n);
                        let at = (sa.len(), sb.len());
                        sa.extend((oa..oa + na).map(|j| ia.row(j)));
                        sb.extend((ob..ob + nb).map(|j| ib.row(j)));
                        self.set_offsets(n, at.0, at.1);
                    }
                }
            }
        }
        (cur, sa, sb)
    }
    /// the reports in merged order, adjacent runs of one side joined.
    fn flatten(&self) -> Vec<GroupRun> {
        let mut out: Vec<GroupRun> = Vec::new();
        let mut stack = vec![0usize];
        while let Some(n) = stack.pop() {
            match &self.nodes[n] {
                Node::A(lo, hi) => match out.last_mut() {
                    Some(GroupRun::A(_, end)) if *end == *lo => *end = *hi,
                    _ => out.push(GroupRun::A(*lo, *hi)),
                },
                Node::B(lo, hi) => match out.last_mut() {
                    Some(GroupRun::B(_, end)) if *end == *lo => *end = *hi,
                    _ => out.push(GroupRun::B(*lo, *hi)),
                },
                Node::Class { alo, ahi, blo, bhi, kids, .. } => match *kids {
                    Some((s, e)) => stack.extend((s..e).rev()),
                    None => out.push(GroupRun::Both(*alo, *ahi, *blo, *bhi)),
                },
            }
        }
        out
    }
}

#[cfg(test)]
mod tests;
