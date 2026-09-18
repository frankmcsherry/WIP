//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `gather_lanes` (its multi-source form) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in the `cmp` op bucket's `order` submodule.

use crate::shape::shape_of_value;
use crate::value::{Bounds, Prim, Refs, Rows, Value};
use std::sync::Arc;

pub(crate) use generators::*;

pub(crate) fn row_span(b: &Bounds, i: usize) -> (usize, usize) {
    b.span(i)
}

/// lift a single-row constant to a column of length `n` (its stratum): `n` copies of `row`'s row 0. Total
/// over every shape — it is `gather` at the all-zero index, so `Op::Lit` (which accepts any value's
/// shape) and `eval` agree. (A boxed constant is `n` references, since `gather` on a Box moves refs.)
pub(crate) fn fill(row: &Value, n: usize) -> Value {
    gather(row, &vec![0usize; n])
}

/// TAKE REFERENCES: `Box` = box every row of `v`. A list's rows become spans of its payload (the
/// payload is the arena); any other column becomes its own arena with the identity refs. O(rows),
/// nothing copied — the by-reference half of the pair.
pub(crate) fn boxed(v: Value) -> Value {
    match v {
        Value::List(bounds, payload) => {
            let spans = (0..bounds.len()).map(|i| bounds.span(i)).collect();
            Value::Box(Arc::new(*payload), Refs::Spans(spans))
        }
        other => {
            let n = other.len();
            Value::Box(Arc::new(other), Refs::Rows((0..n).collect()))
        }
    }
}

/// COPY OUT: `Unbox` = the referenced rows as a fresh by-value column (the ONE place referenced data
/// is copied). Rows refs are a `gather` of the arena; span refs rebuild a partition over the
/// referenced elements. A non-Box comes back as it is.
pub(crate) fn unbox(v: Value) -> Value {
    match v {
        Value::Box(arena, Refs::Rows(rows)) => gather(&arena, &rows),
        Value::Box(payload, Refs::Spans(spans)) => materialize_spans(&spans, &payload),
        other => other,
    }
}

mod generators {
    //! Index generators — the `gather`-family currency. Each composite op is "make an index (and sometimes
    //! re-segmented bounds), then `gather`": mask→survivors (`Filter`), bounds→owner-ids (`CapList`),
    //! point-resolve (`Gather`), range-expand (`Slices`). The index math lives here; the op bodies in
    //! `ops::core` just generate, gather, and re-wrap. (`Unwrap` reads the Sum's carried offset via
    //! `gather_lanes` — no generator; `Branch` groups by tag inline.)

    use super::*;

    /// the mask family: over a list's `bounds` and a per-element 0/1 `mask`, the surviving (nonzero)
    /// positions AND the re-counted per-row bounds, in one pass. Pairs with `gather` to realise `Filter`.
    pub(crate) fn filter_mask(bounds: &Bounds, mask: &[u64]) -> (Vec<usize>, Vec<usize>) {
        let mut idx = Vec::new();
        let mut nb = Vec::with_capacity(bounds.len());
        let mut start = 0;
        for end in bounds.ends() {
            for (off, &b) in mask[start..end].iter().enumerate() {
                if b != 0 {
                    idx.push(start + off);
                }
            }
            nb.push(idx.len()); // cumulative survivors = this row's end offset
            start = end;
        }
        (idx, nb)
    }

    /// the capture family: expand a list's `bounds` to the owner row of each element — `[2,3,6]` →
    /// `[0,0,1,2,2,2]`. Pairs with `gather` to replicate the context side of `CapList`. (The inverse of
    /// `bounds`: position → segment.)
    pub(crate) fn owner_ids(bounds: &Bounds) -> Vec<usize> {
        let mut idx = Vec::with_capacity(bounds.total());
        for i in 0..bounds.len() {
            let (s, e) = row_span(bounds, i);
            idx.extend(std::iter::repeat_n(i, e - s));
        }
        idx
    }

    /// the point family: each index RELATIVE to its haystack row (rows spanned by `hay`) becomes the
    /// absolute haystack position it names. Pairs with `gather` to realise `Gather` — the point sibling
    /// of `range_spans` below. An index outside its row's span is a (data-dependent) panic. `hay` may
    /// be a list or a boxed list (`Rows`): rows are read through `span`.
    pub(crate) fn resolve_indices(outer: &Bounds, idx: &[u64], hay: &Rows) -> Vec<usize> {
        let mut abs = Vec::with_capacity(idx.len());
        for r in 0..outer.len() {
            let (os, oe) = outer.span(r);
            let (hs, he) = hay.span(r);
            for &x in &idx[os..oe] {
                let p = hs + x as usize;
                assert!(p < he, "Gather: index {x} out of row {r}'s bounds");
                abs.push(p);
            }
        }
        abs
    }

    /// the range family: `(lo,hi)` pairs grouped by `outer` into rows, each pair RELATIVE to its haystack row
    /// (rows spanned by `hay`). Emits each pair as an ABSOLUTE span of the haystack payload. `Slices` on
    /// a list copies those spans out into a partition (the materialising inverse of `Flatten`); on a
    /// BOXED list it hands them back as references. A range outside its row is a (data-dependent)
    /// panic; `TrySlices` is the total form.
    pub(crate) fn range_spans(outer: &Bounds, lo: &[u64], hi: &[u64], hay: &Rows) -> Vec<(usize, usize)> {
        let mut spans = Vec::with_capacity(lo.len());
        for r in 0..outer.len() {
            let (os, oe) = outer.span(r);
            let (hs, he) = hay.span(r);
            for k in os..oe {
                let (a, b) = (lo[k] as usize, hi[k] as usize);
                assert!(a <= b && b <= he - hs, "Slices: range ({a}, {b}) outside row {r} of {} elements", he - hs);
                spans.push((hs + a, hs + b));
            }
        }
        spans
    }

    /// spans over a payload as a by-value list: the partition of their lengths over a gather of
    /// exactly the spanned elements. What `Slices` on a list (and `unbox` of span refs) builds.
    pub(crate) fn materialize_spans(spans: &[(usize, usize)], payload: &Value) -> Value {
        let mut elem = Vec::with_capacity(spans.iter().map(|(s, e)| e - s).sum());
        let mut nb = Vec::with_capacity(spans.len());
        for &(s, e) in spans {
            elem.extend(s..e);
            nb.push(elem.len());
        }
        Value::List(nb.into(), Box::new(gather(payload, &elem)))
    }

}

/// build a column whose row j is `v`'s row `idx[j]`; recurses through every shape.
pub(crate) fn gather(v: &Value, idx: &[usize]) -> Value {
    match v {
        Value::Prim(p) => Value::Prim(p.gather(idx)),
        Value::Prod(cols) => Value::Prod(cols.iter().map(|c| gather(c, idx)).collect()),
        Value::List(bounds, vals) => {
            let mut elem = Vec::new();
            let mut nb = Vec::with_capacity(idx.len());
            let mut acc = 0;
            for &i in idx {
                let (s, e) = row_span(bounds, i);
                elem.extend(s..e);
                acc += e - s;
                nb.push(acc);
            }
            Value::List(nb.into(), Box::new(gather(vals, &elem)))
        }
        Value::Sum(tags, within, variants) => {
            // `within` is the carried within-variant offset — read, not recomputed. Each selected row
            // lands in its variant's lane at that offset; `sum_from_prim` rebuilds the result's offset.
            let Prim::U8(tag_vec) = tags else { unreachable!("gather: sum discriminants are u8 columns") };
            let new_tags = tags.gather(idx); // the discriminant moves like any leaf column
            let mut per = vec![Vec::new(); variants.len()];
            for &i in idx {
                per[tag_vec[i] as usize].push(within[i]); // read in place: a gather of k rows is O(k)
            }
            let nv = variants.iter().zip(&per).map(|(v, s)| gather(v, s)).collect();
            Value::sum_from_prim(new_tags, nv)
        }
        Value::Unit(_) => Value::Unit(idx.len()), // no payload to move — just the new row count
        // a reference column: move the refs, never the arena. This one arm is the entire cost model
        // of capture-by-reference — `CapList`/`CapSum`/`Lit` are gathers, so on a Box they are free.
        Value::Box(arena, refs) => Value::Box(arena.clone(), refs.gather(idx)),
    }
}

/// multi-source gather: result row `i` is row `off[i]` of source `srcs[tags[i]]` — all sources sharing
/// one shape. The multi-source generalisation of [`gather`] (the 1-source case) and the fused
/// inverse of `Inject`: `Unwrap` is `gather_lanes(variants, tags, offset)`, reading each row straight from
/// its variant instead of materialising `concat(variants)` first. `off` is the carried within-variant offset.
pub(crate) fn gather_lanes(srcs: &[Option<&Value>], tags: &[usize], off: &[usize]) -> Value {
    // a `None` source is one `tags` never names; fill it with a zero-row value of the witness (first
    // present) shape to hold its slot for tag-indexing. At least one source must be present.
    let witness = srcs.iter().flatten().next().copied().expect("gather_lanes: no committed source");
    let ws = shape_of_value(witness);
    let filled: Vec<Value> = srcs.iter().map(|s| s.map_or_else(|| Value::empty(&ws), |v| v.clone())).collect();
    match &filled[0] {
        Value::Prim(_) => {
            let prims: Vec<&Prim> = filled
                .iter()
                .map(|v| match v {
                    Value::Prim(p) => p,
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            Value::Prim(Prim::gather_lanes(&prims, tags, off))
        }
        Value::Prod(c0) => Value::Prod(
            (0..c0.len())
                .map(|f| {
                    let fields: Vec<Option<&Value>> = filled
                        .iter()
                        .map(|v| match v {
                            Value::Prod(c) => Some(&c[f]),
                            _ => panic!("gather_lanes: shape mismatch"),
                        })
                        .collect();
                    gather_lanes(&fields, tags, off)
                })
                .collect(),
        ),
        Value::List(..) => {
            // each output row is a source row's span; expand to element-level (source, pos) pairs.
            let lists: Vec<(&Bounds, &Value)> = filled
                .iter()
                .map(|v| match v {
                    Value::List(b, vv) => (b, &**vv),
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            let mut nb = Vec::with_capacity(tags.len());
            let (mut etags, mut eoff) = (Vec::new(), Vec::new());
            let mut acc = 0;
            for (&t, &o) in tags.iter().zip(off) {
                let (s, e) = row_span(lists[t].0, o);
                for p in s..e {
                    etags.push(t);
                    eoff.push(p);
                }
                acc += e - s;
                nb.push(acc);
            }
            let vals: Vec<Option<&Value>> = lists.iter().map(|l| Some(l.1)).collect();
            Value::List(nb.into(), Box::new(gather_lanes(&vals, &etags, &eoff)))
        }
        Value::Sum(..) => {
            // pick each output row's tagged payload: build the output tag column, then per output-tag
            // gather that variant from the sources at the carried within-offset.
            // (tags, within-offsets, lanes) borrowed from each source sum.
            type SumView<'a> = (&'a Prim, &'a [usize], &'a [Value]);
            let sums: Vec<SumView> = filled
                .iter()
                .map(|v| match v {
                    Value::Sum(t, o, vs) => (t, o.as_slice(), vs.as_slice()),
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            let tag_prims: Vec<&Prim> = sums.iter().map(|s| s.0).collect();
            let out_tags = Prim::gather_lanes(&tag_prims, tags, off);
            let out_tag_vec = out_tags.usize_vec();
            // every source has the same shape, hence the same arity (there is no uncommitted lane
            // for sources to disagree by); a mismatch is the caller's shape error.
            let arity = sums[0].2.len();
            assert!(sums.iter().all(|sm| sm.2.len() == arity), "gather_lanes: sum sources differ in arity");
            let out_vars: Vec<Value> = (0..arity)
                .map(|s| {
                    let (mut s_t, mut s_o) = (Vec::new(), Vec::new());
                    for (i, &os) in out_tag_vec.iter().enumerate() {
                        if os == s {
                            let (t, o) = (tags[i], off[i]);
                            s_t.push(t);
                            s_o.push(sums[t].1[o]); // carried offset = position in the source's variant s
                        }
                    }
                    let vsrcs: Vec<Option<&Value>> = sums.iter().map(|sm| Some(&sm.2[s])).collect();
                    gather_lanes(&vsrcs, &s_t, &s_o)
                })
                .collect();
            Value::sum_from_prim(out_tags, out_vars)
        }
        Value::Unit(_) => Value::Unit(tags.len()), // all sources unit -> one unit row per pick
        // sources may reference different arenas, so their refs cannot be merged: copy out, pick,
        // re-box. (A merge of same-arena boxes would be refs-only; not needed yet.)
        Value::Box(..) => {
            let owned: Vec<Value> = filled.iter().map(|v| unbox(v.clone())).collect();
            let refs: Vec<Option<&Value>> = owned.iter().map(Some).collect();
            boxed(gather_lanes(&refs, tags, off))
        }
    }
}

/// concatenate same-shape columns end to end, re-basing witnesses. The pre-`gather_lanes` realization,
/// kept as the reference the `gather_lanes` test validates against — no production op reduces to it.
#[cfg(test)]
pub(crate) fn concat(parts: &[Value]) -> Value {
    match &parts[0] {
        Value::Prim(_) => {
            let prims: Vec<&Prim> = parts
                .iter()
                .map(|p| match p {
                    Value::Prim(pp) => pp,
                    _ => panic!("concat: shape mismatch"),
                })
                .collect();
            Value::Prim(Prim::concat(&prims))
        }
        Value::Prod(c0) => Value::Prod(
            (0..c0.len())
                .map(|c| {
                    let sub: Vec<Value> = parts
                        .iter()
                        .map(|p| match p {
                            Value::Prod(cols) => cols[c].clone(),
                            _ => panic!("concat: shape mismatch"),
                        })
                        .collect();
                    concat(&sub)
                })
                .collect(),
        ),
        Value::List(..) => {
            let mut nb = Vec::new();
            let mut base = 0;
            let mut vp = Vec::new();
            for p in parts {
                match p {
                    Value::List(b, vals) => {
                        nb.extend(b.ends().map(|x| base + x));
                        base += b.total();
                        vp.push((**vals).clone());
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            Value::List(nb.into(), Box::new(concat(&vp)))
        }
        Value::Sum(_, _, v0) => {
            let mut tag_parts: Vec<&Prim> = Vec::new();
            let mut per: Vec<Vec<Value>> = vec![Vec::new(); v0.len()]; // committed contributions per lane
            for p in parts {
                match p {
                    Value::Sum(t, _, v) => {
                        tag_parts.push(t);
                        for (i, c) in v.iter().enumerate() {
                            per[i].push(c.clone());
                        }
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            // the concatenated tags fix the offset, so it's rebuilt rather than spliced.
            let lanes = per.iter().map(|ps| concat(ps)).collect();
            Value::sum_from_prim(Prim::concat(&tag_parts), lanes)
        }
        Value::Unit(_) => Value::Unit(parts.iter().map(Value::len).sum()),
        Value::Box(..) => {
            let owned: Vec<Value> = parts.iter().map(|v| unbox(v.clone())).collect();
            boxed(concat(&owned))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn u(xs: &[u64]) -> Value {
        Value::u64(xs.to_vec())
    }

    /// reference for `gather_lanes`: index into `concat(variants)` by lane-start + offset.
    fn oracle(variants: &[Value], tags: &[usize], off: &[usize]) -> Value {
        let mut start = vec![0usize; variants.len()];
        let mut acc = 0;
        for (t, v) in variants.iter().enumerate() {
            start[t] = acc;
            acc += v.len();
        }
        let idx: Vec<usize> = tags.iter().zip(off).map(|(&t, &o)| start[t] + o).collect();
        gather(&concat(variants), &idx)
    }

    /// `gather_lanes` must match the concat+gather oracle; `off` is the within-variant rank.
    fn check(tags: &[usize], variants: Vec<Value>) {
        let mut cur = vec![0usize; variants.len()];
        let off: Vec<usize> = tags.iter().map(|&t| { let p = cur[t]; cur[t] += 1; p }).collect();
        let refs: Vec<Option<&Value>> = variants.iter().map(Some).collect();
        assert_eq!(gather_lanes(&refs, tags, &off), oracle(&variants, tags, &off));
    }

    /// Two sources of one sum shape, each using only one of its lanes (the other is an empty
    /// column of the declared shape): the gather reads each row from its lane and the result
    /// carries both lanes, whichever source comes first.
    #[test]
    fn gather_lanes_sums_using_different_lanes() {
        let a = Value::sum(vec![0, 0], vec![u(&[10, 11]), u(&[])]);
        let b = Value::sum(vec![1, 1], vec![u(&[]), u(&[20, 21])]);
        let (tags, off) = (vec![0usize, 1, 0, 1], vec![0usize, 0, 1, 1]);
        let out = gather_lanes(&[Some(&a), Some(&b)], &tags, &off);
        match &out {
            Value::Sum(t, _, lanes) => {
                assert_eq!(lanes.len(), 2);
                assert_eq!(t.usize_vec(), vec![0, 1, 0, 1]);
                assert_eq!(lanes[0], u(&[10, 11]));
                assert_eq!(lanes[1], u(&[20, 21]));
            }
            other => panic!("expected a Sum, got {other:?}"),
        }
        let flipped = gather_lanes(&[Some(&b), Some(&a)], &tags, &off);
        assert_eq!(flipped.len(), 4);
    }

    /// `boxed`/`unbox` round-trip by content; `gather` on a Box moves refs only (the arena `Arc`
    /// is the same allocation); `fill` of a boxed row is `n` references.
    #[test]
    fn box_gather_moves_refs_not_the_arena() {
        let list = Value::List(vec![2, 3, 6].into(), Box::new(u(&[1, 2, 3, 4, 5, 6])));
        let b = boxed(list.clone());
        assert_eq!(unbox(b.clone()), list);
        let Value::Box(arena, Refs::Spans(s)) = &b else { panic!("a boxed list holds spans") };
        assert_eq!(*s, vec![(0, 2), (2, 3), (3, 6)]);
        let g = gather(&b, &[2, 0, 2, 2, 1]);
        let Value::Box(arena2, Refs::Spans(s2)) = &g else { panic!() };
        assert!(Arc::ptr_eq(arena, arena2), "gather on a Box must not touch the arena");
        assert_eq!(*s2, vec![(3, 6), (0, 2), (3, 6), (3, 6), (2, 3)]);
        assert_eq!(unbox(g), gather(&list, &[2, 0, 2, 2, 1]));
        // non-list rows: row refs into the arena
        let prod = Value::Prod(vec![u(&[10, 20]), list]);
        let bp = boxed(prod.clone());
        assert!(matches!(&bp, Value::Box(_, Refs::Rows(r)) if *r == vec![0, 1]));
        assert_eq!(unbox(gather(&bp, &[1, 1, 0])), gather(&prod, &[1, 1, 0]));
        // the broadcast
        let row = boxed(Value::List(vec![3].into(), Box::new(u(&[4, 5, 6]))));
        let f = fill(&row, 4);
        assert!(matches!(&f, Value::Box(_, Refs::Spans(s)) if *s == vec![(0, 3); 4]));
    }

    #[test]
    fn gather_lanes_matches_concat_gather() {
        let tags = [0usize, 1, 0, 1, 0]; // t0 ×3, t1 ×2
        // leaf
        check(&tags, vec![u(&[10, 20, 30]), u(&[40, 50])]);
        // product
        check(
            &tags,
            vec![
                Value::Prod(vec![u(&[1, 2, 3]), u(&[4, 5, 6])]),
                Value::Prod(vec![u(&[7, 8]), u(&[9, 10])]),
            ],
        );
        // list payload (ragged spans, the recursive value gather)
        check(
            &tags,
            vec![
                Value::List(vec![2, 3, 6].into(), Box::new(u(&[1, 2, 3, 4, 5, 6]))),
                Value::List(vec![1, 3].into(), Box::new(u(&[7, 8, 9]))),
            ],
        );
        // sum payload (nested tags + within-offset remap)
        check(
            &tags,
            vec![
                Value::sum(vec![0, 1, 0], vec![u(&[1, 2]), u(&[3])]),
                Value::sum(vec![1, 0], vec![u(&[4]), u(&[5])]),
            ],
        );
        // empty
        check(&[], vec![u(&[]), u(&[])]);
    }
}
