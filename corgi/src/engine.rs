//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `gather_lanes` (its multi-source form) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in the `cmp` op bucket's `order` submodule.

use crate::shape::shape_of_value;
use crate::value::{Prim, Value};

pub(crate) use generators::*;

pub(crate) fn row_span(b: &[usize], i: usize) -> (usize, usize) {
    (if i == 0 { 0 } else { b[i - 1] }, b[i])
}

/// lift a single-row constant to a column of length `n` (its stratum): `n` copies of `row`'s row 0. Total
/// over every shape — it is `gather` at the all-zero index, so `Op::Lit`'s `judge` (which accepts any value's
/// shape) and `eval` agree.
pub(crate) fn fill(row: &Value, n: usize) -> Value {
    gather(row, &vec![0usize; n])
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
    pub(crate) fn filter_mask(bounds: &[usize], mask: &[u64]) -> (Vec<usize>, Vec<usize>) {
        let mut idx = Vec::new();
        let mut nb = Vec::with_capacity(bounds.len());
        let mut start = 0;
        for &end in bounds {
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
    pub(crate) fn owner_ids(bounds: &[usize]) -> Vec<usize> {
        let mut idx = Vec::with_capacity(bounds.last().copied().unwrap_or(0));
        for i in 0..bounds.len() {
            let (s, e) = row_span(bounds, i);
            idx.extend(std::iter::repeat_n(i, e - s));
        }
        idx
    }

    /// the point family: each index RELATIVE to its haystack row (rows spanned by `hay`) becomes the
    /// absolute haystack position it names. Pairs with `gather` to realise `Gather` — the point sibling
    /// of `expand_ranges` below. An index outside its row's span is a (data-dependent) panic.
    pub(crate) fn resolve_indices(outer: &[usize], idx: &[u64], hay: &[usize]) -> Vec<usize> {
        let mut abs = Vec::with_capacity(idx.len());
        let (mut os, mut hs) = (0, 0);
        for r in 0..outer.len() {
            let (oe, he) = (outer[r], hay[r]);
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
    pub(crate) fn expand_ranges(outer: &[usize], lo: &[u64], hi: &[u64], hay: &[usize]) -> (Vec<usize>, Vec<usize>) {
        let mut idx = Vec::new();
        let mut inner = Vec::new();
        let mut acc = 0;
        let (mut os, mut hs) = (0, 0);
        for r in 0..outer.len() {
            let (oe, he) = (outer[r], hay[r]);
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
            Value::List(nb, Box::new(gather(vals, &elem)))
        }
        Value::Sum(tags, within, variants) => {
            // `within` is the carried within-variant offset — read, not recomputed. Each selected row
            // lands in its variant's lane at that offset; `sum_from_prim` rebuilds the result's offset.
            let tag_vec = tags.usize_vec();
            let new_tags = tags.gather(idx); // the discriminant moves like any leaf column
            let mut per = vec![Vec::new(); variants.len()];
            for &i in idx {
                per[tag_vec[i]].push(within[i]);
            }
            // gather each committed lane (possibly to fewer rows); a ⊥ lane stays ⊥.
            let nv = variants.iter().zip(&per).map(|(v, s)| v.as_ref().map(|vv| gather(vv, s))).collect();
            Value::sum_from_prim(new_tags, nv)
        }
    }
}

/// multi-source gather: result row `i` is row `off[i]` of source `srcs[tags[i]]` — all sources sharing
/// one shape (up to `⊥`). The multi-source generalisation of [`gather`] (the 1-source case) and the fused
/// inverse of `Inject`: `Unwrap` is `gather_lanes(variants, tags, offset)`, reading each row straight from
/// its variant instead of materialising `concat(variants)` first. `off` is the carried within-variant offset.
pub(crate) fn gather_lanes(srcs: &[Option<&Value>], tags: &[usize], off: &[usize]) -> Value {
    // `None` sources are ⊥ lanes — `tags` never names one (no row carries an uncommitted variant's tag),
    // so they're never read. Fill each with a zero-row value of the witness (first committed) shape to hold
    // its slot for tag-indexing. Every sum has ≥1 committed lane, so a witness exists.
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
            let lists: Vec<(&[usize], &Value)> = filled
                .iter()
                .map(|v| match v {
                    Value::List(b, vv) => (b.as_slice(), &**vv),
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
            Value::List(nb, Box::new(gather_lanes(&vals, &etags, &eoff)))
        }
        Value::Sum(..) => {
            // pick each output row's tagged payload: build the output tag column, then per output-tag
            // gather that variant from the sources at the carried within-offset.
            // (tags, within-offsets, lanes) borrowed from each source sum.
            type SumView<'a> = (&'a Prim, &'a [usize], &'a [Option<Value>]);
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
            let arity = sums[0].2.len();
            let out_vars: Vec<Option<Value>> = (0..arity)
                .map(|s| {
                    // out lane `s` is ⊥ only if no source committed it.
                    if sums.iter().all(|sm| sm.2[s].is_none()) {
                        return None;
                    }
                    let (mut s_t, mut s_o) = (Vec::new(), Vec::new());
                    for (i, &os) in out_tag_vec.iter().enumerate() {
                        if os == s {
                            let (t, o) = (tags[i], off[i]);
                            s_t.push(t);
                            s_o.push(sums[t].1[o]); // carried offset = position in the source's variant s
                        }
                    }
                    let vsrcs: Vec<Option<&Value>> = sums.iter().map(|sm| sm.2[s].as_ref()).collect();
                    Some(gather_lanes(&vsrcs, &s_t, &s_o))
                })
                .collect();
            Value::sum_from_prim(out_tags, out_vars)
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
                        nb.extend(b.iter().map(|&x| base + x));
                        base += b.last().copied().unwrap_or(0);
                        vp.push((**vals).clone());
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            Value::List(nb, Box::new(concat(&vp)))
        }
        Value::Sum(_, _, v0) => {
            let mut tag_parts: Vec<&Prim> = Vec::new();
            let mut per: Vec<Vec<Value>> = vec![Vec::new(); v0.len()]; // committed contributions per lane
            for p in parts {
                match p {
                    Value::Sum(t, _, v) => {
                        tag_parts.push(t);
                        for (i, c) in v.iter().enumerate() {
                            if let Some(cv) = c {
                                per[i].push(cv.clone());
                            }
                        }
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            // a lane committed by any part is committed in the result (the ⊥ parts add no rows); the
            // concatenated tags fix the offset, so it's rebuilt rather than spliced.
            let lanes = per.into_iter().map(|ps| (!ps.is_empty()).then(|| concat(&ps))).collect();
            Value::sum_from_prim(Prim::concat(&tag_parts), lanes)
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
                Value::List(vec![2, 3, 6], Box::new(u(&[1, 2, 3, 4, 5, 6]))),
                Value::List(vec![1, 3], Box::new(u(&[7, 8, 9]))),
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
