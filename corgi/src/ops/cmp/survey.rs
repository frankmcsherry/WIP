//! The merge kernel, rank at a time: two structurally sorted columns surveyed as the maximal
//! runs exclusive to one side and the equal classes common to both, level by level.
//!
//! [`survey_groups`] walks the two columns together by the same descent the sort makes: the
//! leading leaf decides most of the interleaving with one gallop, and every class it leaves equal
//! is refined by the next level at once — a product's next field, a sum's lane at the carried
//! offsets, a list's next element position — never one pair at a time. A level reads its keys
//! through index lists into the two columns; the reports form a tree, an equal class holding its
//! refinement as children, flattened once at the end, so a level's work is proportional to the
//! classes it refines and not to the reports already made. Nothing is gathered, and the shape is
//! walked once per level rather than once per comparison. [`survey`] is the older pairwise
//! report: a leaf pair is walked directly, anything else is derived from the groups. Layout, top
//! down: the reports, the entry points, the level walk, the leaf merges, the report tree.

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

/// One report from [`survey`]: an exclusive range, or a single matched pair.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Run {
    A(usize, usize),
    B(usize, usize),
    /// Row `a[ia]` and row `b[ib]` are structurally equal.
    Both(usize, usize),
}

// ---- entry points ---------------------------------------------------------------------------

/// Survey `a` and `b`, both in structural order. Ensures: the `A` ranges and the `a` halves of
/// the `Both` classes partition `0..a.len()` in order without gap or overlap, likewise `b`;
/// expanding the reports to their rows yields a non-decreasing structural sequence; every row of
/// a `Both` class on either side is structurally equal to every other, and the class is maximal;
/// adjacent reports never share a side.
pub fn survey_groups(a: &Value, b: &Value) -> Vec<GroupRun> {
    let mut tree = Tree { nodes: vec![Node::Class { alo: 0, ahi: a.len(), blo: 0, bhi: b.len(), la: 0, lb: 0, kids: (0, 0) }] };
    level(a, b, Identity, Identity, &[0], &mut tree);
    tree.flatten()
}

/// [`survey_groups`] as pairwise reports: a class of `k` rows against `l` matches the first
/// `min(k, l)` of each side pair by pair, and the excess joins the exclusive run that follows it,
/// which is what a two-pointer walk reports. A leaf pair is walked directly.
pub fn survey(a: &Value, b: &Value) -> Vec<Run> {
    if let (Value::Prim(pa), Value::Prim(pb)) = (a, b) {
        macro_rules! go {
            ($($V:ident),*) => {
                match (pa, pb) {
                    $( (Prim::$V(va), Prim::$V(vb)) => return merge_pairs(|i| va[i], |j| vb[j], va.len(), vb.len()), )*
                    _ => panic!("survey: leaf width mismatch"),
                }
            };
        }
        go!(U8, U16, U32, U64)
    }
    let mut out: Vec<Run> = Vec::new();
    let mut push = |r: Run| match (out.last_mut(), r) {
        (Some(Run::A(_, hi)), Run::A(lo, nhi)) if *hi == lo => *hi = nhi,
        (Some(Run::B(_, hi)), Run::B(lo, nhi)) if *hi == lo => *hi = nhi,
        _ => out.push(r),
    };
    for g in survey_groups(a, b) {
        match g {
            GroupRun::A(lo, hi) => push(Run::A(lo, hi)),
            GroupRun::B(lo, hi) => push(Run::B(lo, hi)),
            GroupRun::Both(alo, ahi, blo, bhi) => {
                let k = (ahi - alo).min(bhi - blo);
                for i in 0..k {
                    push(Run::Both(alo + i, blo + i));
                }
                if alo + k < ahi {
                    push(Run::A(alo + k, ahi));
                }
                if blo + k < bhi {
                    push(Run::B(blo + k, bhi));
                }
            }
        }
    }
    out
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
            let mut fields = ca.iter().zip(cb);
            let Some((x0, y0)) = fields.next() else {
                return merge(|_| 0u8, |_| 0u8, open, tree);
            };
            // the first field reads the level's own lists; each later field, the rows of the
            // classes the field before left equal.
            level(x0, y0, ia, ib, open, tree);
            let (mut cur, mut sa, mut sb) = tree.refined(open, ia, ib);
            for (x, y) in fields {
                if cur.is_empty() {
                    break;
                }
                level(x, y, &sa[..], &sb[..], &cur, tree);
                (cur, sa, sb) = tree.refined(&cur, &sa[..], &sb[..]);
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
                    tree.push(Node::Class { alo: top_a(si), ahi: top_a(i), blo: top_b(sj), bhi: top_b(j), la: si, lb: sj, kids: (0, 0) });
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

/// The pairwise walk over two leaves, as [`survey`] reports it: a match is one pair, and the
/// walk goes on from the next row of each side.
fn merge_pairs<T: Ord + Copy>(ka: impl Fn(usize) -> T, kb: impl Fn(usize) -> T, na: usize, nb: usize) -> Vec<Run> {
    let (mut i, mut j) = (0usize, 0usize);
    let mut out = Vec::new();
    while i < na && j < nb {
        match ka(i).cmp(&kb(j)) {
            std::cmp::Ordering::Less => {
                let s = i;
                i += 1;
                gallop(&mut i, na, |k| ka(k) < kb(j));
                out.push(Run::A(s, i));
            }
            std::cmp::Ordering::Equal => {
                out.push(Run::Both(i, j));
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Greater => {
                let s = j;
                j += 1;
                gallop(&mut j, nb, |k| kb(k) < ka(i));
                out.push(Run::B(s, j));
            }
        }
    }
    if i < na {
        out.push(Run::A(i, na));
    }
    if j < nb {
        out.push(Run::B(j, nb));
    }
    out
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
/// a contiguous range of nodes, an empty range meaning equal throughout. `la`/`lb` are the
/// class's offsets in the index lists of the level that made it.
enum Node {
    A(usize, usize),
    B(usize, usize),
    Class { alo: usize, ahi: usize, blo: usize, bhi: usize, la: usize, lb: usize, kids: (usize, usize) },
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
            Node::Class { kids, .. } => *kids = (first, end),
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
                    if kids.0 == kids.1 {
                        let (oa, ob) = (la + alo - top.alo, lb + blo - top.blo);
                        let (na, nb) = (ahi - alo, bhi - blo);
                        cur.push(n);
                        let at = (sa.len(), sb.len());
                        sa.extend((oa..oa + na).map(|j| ia.row(j)));
                        sb.extend((ob..ob + nb).map(|j| ib.row(j)));
                        self.set_offsets(n, at.0, at.1);
                    } else {
                        stack.extend((kids.0..kids.1).rev());
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
                Node::Class { alo, ahi, blo, bhi, kids, .. } => {
                    if kids.0 == kids.1 {
                        if *alo < *ahi || *blo < *bhi {
                            out.push(GroupRun::Both(*alo, *ahi, *blo, *bhi));
                        }
                    } else {
                        stack.extend((kids.0..kids.1).rev());
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::gather;
    use crate::ops::cmp::order::compare_at;
    use crate::ops::cmp::sort::sort_values;
    use std::cmp::Ordering;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// a random column of `rows` rows over a small value space, so that two draws share rows.
    fn random_value(rng: &mut Rng, rows: usize, depth: usize) -> Value {
        if depth == 0 {
            return Value::u64((0..rows).map(|_| rng.below(4) as u64).collect());
        }
        match rng.below(6) {
            0 => Value::u8((0..rows).map(|_| rng.below(3) as u8).collect()),
            1 => Value::Prod((0..1 + rng.below(3)).map(|_| random_value(rng, rows, depth - 1)).collect()),
            2 => {
                let arity = 1 + rng.below(3);
                let tags: Vec<usize> = (0..rows).map(|_| rng.below(arity)).collect();
                let lanes = (0..arity)
                    .map(|t| random_value(rng, tags.iter().filter(|&&x| x == t).count(), depth - 1))
                    .collect();
                Value::sum(tags, lanes)
            }
            3 => {
                let mut ends = Vec::with_capacity(rows);
                let mut total = 0;
                for _ in 0..rows {
                    total += rng.below(3);
                    ends.push(total);
                }
                Value::List(ends.into(), Box::new(random_value(rng, total, depth - 1)))
            }
            4 => Value::List(Bounds::Stride(2, rows), Box::new(random_value(rng, rows * 2, depth - 1))),
            _ => Value::Unit(rows),
        }
    }

    /// the same shape, two independent draws, each sorted.
    fn two_sorted(rng: &mut Rng, na: usize, nb: usize, depth: usize) -> (Value, Value) {
        let both = random_value(rng, na + nb, depth);
        let a = gather(&both, &(0..na).collect::<Vec<_>>());
        let b = gather(&both, &(na..na + nb).collect::<Vec<_>>());
        let (_, _, a) = sort_values(&[], &a);
        let (_, _, b) = sort_values(&[], &b);
        (a, b)
    }

    /// the group oracle: a two-pointer walk with `compare_at`, classes galloped by scanning.
    fn naive_groups(a: &Value, b: &Value) -> Vec<GroupRun> {
        let (na, nb) = (a.len(), b.len());
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < na && j < nb {
            match compare_at(a, i, b, j) {
                Ordering::Less => {
                    let s = i;
                    while i < na && compare_at(a, i, b, j) == Ordering::Less {
                        i += 1;
                    }
                    out.push(GroupRun::A(s, i));
                }
                Ordering::Greater => {
                    let s = j;
                    while j < nb && compare_at(b, j, a, i) == Ordering::Less {
                        j += 1;
                    }
                    out.push(GroupRun::B(s, j));
                }
                Ordering::Equal => {
                    let (si, sj) = (i, j);
                    while i < na && compare_at(a, i, b, sj) == Ordering::Equal {
                        i += 1;
                    }
                    while j < nb && compare_at(b, j, a, si) == Ordering::Equal {
                        j += 1;
                    }
                    out.push(GroupRun::Both(si, i, sj, j));
                }
            }
        }
        if i < na {
            out.push(GroupRun::A(i, na));
        }
        if j < nb {
            out.push(GroupRun::B(j, nb));
        }
        out
    }

    #[test]
    fn groups_agree_with_the_scalar_walk_on_random_shapes() {
        for seed in 1..120u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let (na, nb) = (rng.below(30), rng.below(30));
            let (a, b) = two_sorted(&mut rng, na, nb, 3);
            assert_eq!(survey_groups(&a, &b), naive_groups(&a, &b), "\n{}\n{}", crate::value::show(&a), crate::value::show(&b));
        }
    }

    #[test]
    fn groups_are_maximal_and_equal() {
        let mut rng = Rng(5);
        let (a, b) = two_sorted(&mut rng, 400, 300, 3);
        let (mut ca, mut cb) = (0, 0);
        for r in survey_groups(&a, &b) {
            match r {
                GroupRun::A(lo, hi) => {
                    assert_eq!(lo, ca);
                    assert!(hi > lo);
                    ca = hi;
                }
                GroupRun::B(lo, hi) => {
                    assert_eq!(lo, cb);
                    assert!(hi > lo);
                    cb = hi;
                }
                GroupRun::Both(alo, ahi, blo, bhi) => {
                    assert_eq!((alo, blo), (ca, cb));
                    assert!((alo..ahi).all(|k| compare_at(&a, k, &b, blo) == Ordering::Equal));
                    assert!((blo..bhi).all(|k| compare_at(&b, k, &a, alo) == Ordering::Equal));
                    assert!(ahi == a.len() || compare_at(&a, ahi, &a, alo) != Ordering::Equal, "not maximal in a");
                    assert!(bhi == b.len() || compare_at(&b, bhi, &b, blo) != Ordering::Equal, "not maximal in b");
                    ca = ahi;
                    cb = bhi;
                }
            }
        }
        assert_eq!((ca, cb), (a.len(), b.len()));
    }

    #[test]
    fn one_side_empty_is_one_run() {
        let (a, e) = (Value::u64(vec![1, 2, 2]), Value::u64(vec![]));
        assert_eq!(survey_groups(&a, &e), vec![GroupRun::A(0, 3)]);
        assert_eq!(survey_groups(&e, &a), vec![GroupRun::B(0, 3)]);
        assert!(survey_groups(&e, &e).is_empty());
    }
}
