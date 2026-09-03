//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `gather_lanes` (its multi-source form) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in the `cmp` op bucket's `order` submodule.

use crate::shape::shape_of_value;
use std::sync::Arc;
use crate::value::{Bounds, Prim, Tags, Value};

pub(crate) use generators::*;

pub(crate) fn row_span(b: &Bounds, i: usize) -> (usize, usize) {
    b.span(i)
}

/// lift a single-row constant to a column of length `n` (its stratum): `n` copies of `row`'s row 0. Total
/// over every shape — it is `gather` at the all-zero index, so `Op::Lit` (which accepts any value's
/// shape) and `eval` agree.
pub(crate) fn fill(row: &Value, n: usize) -> Value {
    match row {
        // a FIXED-WIDTH row broadcasts directly: one `vec![x; n]` per leaf, and no index column
        // to describe an index that is constant. (`Op::Lit` is the caller, and a literal is
        // overwhelmingly a leaf or a product of them.)
        Value::Prim(p) => Value::Prim(p.repeat(0, n)),
        Value::Prod(cols) => Value::Prod(cols.iter().map(|c| fill(c, n)).collect()),
        Value::Unit(_) => Value::Unit(n),
        // a VARIABLE-WIDTH row (a `List` span, a `Sum` lane) is a row move, which is what a
        // `gather` at the all-zero index already is; there is no cheaper form of it here.
        Value::List(..) | Value::Sum(..) => gather(row, &vec![0usize; n]),
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
    ///
    /// The mask's WIDTH is the producer's choice — the core's idiom is "nonzero is true" and a leaf
    /// is width-tagged — so this dispatches it once, above the pass, rather than making the caller
    /// normalize (which would be a pass over the mask to save nothing). `Rel` produces a byte mask,
    /// so the `U8` arm is the common one and no other arm costs anything to have.
    pub(crate) fn filter_mask(bounds: &Bounds, mask: &Prim) -> (Vec<usize>, Vec<usize>) {
        fn scan<T: Copy + Default + PartialEq>(bounds: &Bounds, m: &[T]) -> (Vec<usize>, Vec<usize>) {
            let mut idx = Vec::new();
            let mut nb = Vec::with_capacity(bounds.len());
            let mut start = 0;
            for end in bounds.ends() {
                for (off, b) in m[start..end].iter().enumerate() {
                    if *b != T::default() {
                        idx.push(start + off);
                    }
                }
                nb.push(idx.len()); // cumulative survivors = this row's end offset
                start = end;
            }
            (idx, nb)
        }
        match mask {
            Prim::U8(v) => scan(bounds, v),
            Prim::U16(v) => scan(bounds, v),
            Prim::U32(v) => scan(bounds, v),
            Prim::U64(v) => scan(bounds, v),
        }
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
    /// of `expand_ranges` below. An index outside its row's span is a (data-dependent) panic.
    pub(crate) fn resolve_indices(outer: &Bounds, idx: &[u64], hay: &Bounds) -> Vec<usize> {
        let mut abs = Vec::with_capacity(idx.len());
        let (mut os, mut hs) = (0, 0);
        for r in 0..outer.len() {
            let (oe, he) = (outer.end(r), hay.end(r));
            for &x in &idx[os..oe] {
                let p = hs + x as usize;
                assert!(p < he, "Gather: index {x} out of row {r}'s bounds");
                abs.push(p);
            }
            os = oe;
            hs = he;
        }
        abs
    }

    /// the range family: `(lo,hi)` pairs grouped by `outer` into rows, each pair RELATIVE to its haystack row
    /// (rows spanned by `hay`). Emits the absolute haystack positions each pair names and the per-pair inner
    /// bounds. Pairs with `gather` to realise `Slices` — the materialising inverse of `Flatten`.
    pub(crate) fn expand_ranges(outer: &Bounds, lo: &[u64], hi: &[u64], hay: &Bounds) -> (Vec<usize>, Vec<usize>) {
        let mut idx = Vec::new();
        let mut inner = Vec::new();
        let mut acc = 0;
        let (mut os, mut hs) = (0, 0);
        for r in 0..outer.len() {
            let (oe, he) = (outer.end(r), hay.end(r));
            for k in os..oe {
                let (a, b) = (hs + lo[k] as usize, hs + hi[k] as usize);
                idx.extend(a..b);
                acc += b - a;
                inner.push(acc);
            }
            os = oe;
            hs = he;
        }
        (idx, inner)
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
        Value::Sum(tags, variants) => {
            // one tag throughout: the selected rows are lane `t`'s rows at exactly `idx` (a `Const`
            // assignment's offset IS the row index), so this is one lane gather and no witness work.
            if let Some(t) = tags.const_tag() {
                let mut lanes: Vec<Value> = variants.iter().map(|v| gather(v, &[])).collect();
                lanes[t] = gather(&variants[t], idx);
                return Value::Sum(Tags::constant(t, idx.len()), lanes);
            }
            // Otherwise build the result's assignment in the SAME pass that routes the rows: a row's
            // new offset is the size its lane had when the row arrived, so nothing is recomputed
            // afterwards. Both reads are in place — a gather of k rows is O(k), not O(column).
            let mut per = vec![Vec::new(); variants.len()];
            let (mut new_tags, mut new_off) = (Vec::with_capacity(idx.len()), Vec::with_capacity(idx.len()));
            for &i in idx {
                let t = tags.tag_at(i);
                new_tags.push(t as u8);
                new_off.push(per[t].len());
                per[t].push(tags.offset_at(i));
            }
            let nv = variants.iter().zip(&per).map(|(v, s)| gather(v, s)).collect();
            Value::Sum(Tags::column(Prim::U8(Arc::new(new_tags)), new_off), nv)
        }
        Value::Unit(_) => Value::Unit(idx.len()), // no payload to move — just the new row count
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
            type SumView<'a> = (&'a Tags, &'a [Value]);
            let sums: Vec<SumView> = filled
                .iter()
                .map(|v| match v {
                    Value::Sum(t, vs) => (t, vs.as_slice()),
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            // each output row takes its source row's tag, read in place from that source.
            let out_tag: Vec<usize> =
                tags.iter().zip(off).map(|(&t, &o)| sums[t].0.tag_at(o)).collect();
            // every source has the same shape, hence the same arity (there is no uncommitted lane
            // for sources to disagree by); a mismatch is the caller's shape error.
            let arity = sums[0].1.len();
            assert!(sums.iter().all(|sm| sm.1.len() == arity), "gather_lanes: sum sources differ in arity");
            let out_vars: Vec<Value> = (0..arity)
                .map(|s| {
                    let (mut s_t, mut s_o) = (Vec::new(), Vec::new());
                    for (i, &os) in out_tag.iter().enumerate() {
                        if os == s {
                            let (t, o) = (tags[i], off[i]);
                            s_t.push(t);
                            s_o.push(sums[t].0.offset_at(o)); // carried offset within the source's lane s
                        }
                    }
                    let vsrcs: Vec<Option<&Value>> = sums.iter().map(|sm| Some(&sm.1[s])).collect();
                    gather_lanes(&vsrcs, &s_t, &s_o)
                })
                .collect();
            Value::Sum(Tags::from_tags(out_tag, arity), out_vars)
        }
        Value::Unit(_) => Value::Unit(tags.len()), // all sources unit -> one unit row per pick
    }
}

/// per-row blend: row `i` of the result is row `i` of `then` where `mask[i]` is nonzero, else row
/// `i` of `els` (both same-shape columns at the mask's stratum) — what [`crate::ops::Op::Select`] is.
///
/// A FIXED-WIDTH level blends LANE-WISE: one pass reading both sides at the same position, which is
/// a select instruction. A VARIABLE-WIDTH level (a `List` row is a span, a `Sum` row is a lane
/// position) has no constant slot to blend into, so it falls back to the two-source [`gather_lanes`]
/// — the same split `scatter` makes, and for the same reason. The split is per LEVEL, not per value:
/// a product blends each leaf field directly and only gathers the fields that need it.
pub(crate) fn blend(mask: &[u8], then: Value, els: Value) -> Value {
    match (then, els) {
        (Value::Prim(t), Value::Prim(e)) => Value::Prim(t.blend(e, mask)),
        (Value::Prod(ts), Value::Prod(es)) => {
            Value::Prod(ts.into_iter().zip(es).map(|(t, e)| blend(mask, t, e)).collect())
        }
        (Value::Unit(_), Value::Unit(_)) => Value::Unit(mask.len()),
        // row `i` from lane `mask[i] != 0`, at its own position — the offsets are the identity.
        (t, e) => {
            let tags: Vec<usize> = mask.iter().map(|&m| (m != 0) as usize).collect();
            let off: Vec<usize> = (0..tags.len()).collect();
            gather_lanes(&[Some(&e), Some(&t)], &tags, &off)
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
        Value::Sum(_, v0) => {
            let mut all_tags: Vec<usize> = Vec::new();
            let mut per: Vec<Vec<Value>> = vec![Vec::new(); v0.len()]; // contributions per lane
            for p in parts {
                match p {
                    Value::Sum(t, v) => {
                        all_tags.extend(t.tags_iter());
                        for (i, c) in v.iter().enumerate() {
                            per[i].push(c.clone());
                        }
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            // the concatenated tags fix the offset, so it's rebuilt rather than spliced.
            let lanes = per.iter().map(|ps| concat(ps)).collect();
            let arity = v0.len();
            Value::Sum(Tags::from_tags(all_tags, arity), lanes)
        }
        Value::Unit(_) => Value::Unit(parts.iter().map(Value::len).sum()),
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

    /// `blend` takes a lane-wise path at fixed-width levels and the `gather_lanes` path elsewhere.
    /// The two must agree exactly, including inside a MIXED product where one field takes each.
    #[test]
    fn blend_matches_the_gather_lanes_path() {
        // the general path, written out: row i from lane `mask[i] != 0`, at its own position.
        fn oracle(mask: &[u8], then: &Value, els: &Value) -> Value {
            let tags: Vec<usize> = mask.iter().map(|&m| (m != 0) as usize).collect();
            let off: Vec<usize> = (0..tags.len()).collect();
            gather_lanes(&[Some(els), Some(then)], &tags, &off)
        }
        let mask = [1u8, 0, 0, 1];
        let list = |ends: Vec<usize>, xs: &[u64]| Value::List(ends.into(), Box::new(u(xs)));

        // leaf, product of leaves, unit — the lane-wise path.
        for (t, e) in [
            (u(&[1, 2, 3, 4]), u(&[10, 20, 30, 40])),
            (
                Value::Prod(vec![u(&[1, 2, 3, 4]), Value::u8(vec![5, 6, 7, 8])]),
                Value::Prod(vec![u(&[9, 8, 7, 6]), Value::u8(vec![1, 2, 3, 4])]),
            ),
            (Value::Unit(4), Value::Unit(4)),
        ] {
            assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));
        }

        // a list (ragged rows: no constant slot) — the fallback path.
        let (t, e) = (list(vec![1, 3, 3, 6], &[1, 2, 3, 4, 5, 6]), list(vec![2, 2, 5, 5], &[7, 8, 9, 1, 2]));
        assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));

        // a MIXED product: field 0 blends lane-wise, field 1 falls back, and the result is the same.
        let t = Value::Prod(vec![u(&[1, 2, 3, 4]), list(vec![1, 3, 3, 6], &[1, 2, 3, 4, 5, 6])]);
        let e = Value::Prod(vec![u(&[9, 8, 7, 6]), list(vec![2, 2, 5, 5], &[7, 8, 9, 1, 2])]);
        assert_eq!(blend(&mask, t.clone(), e.clone()), oracle(&mask, &t, &e));
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
            Value::Sum(t, lanes) => {
                assert_eq!(lanes.len(), 2);
                assert_eq!(t.tags_iter().collect::<Vec<_>>(), vec![0, 1, 0, 1]);
                assert_eq!(lanes[0], u(&[10, 11]));
                assert_eq!(lanes[1], u(&[20, 21]));
            }
            other => panic!("expected a Sum, got {other:?}"),
        }
        let flipped = gather_lanes(&[Some(&b), Some(&a)], &tags, &off);
        assert_eq!(flipped.len(), 4);
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
