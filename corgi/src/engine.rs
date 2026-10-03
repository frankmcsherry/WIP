//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `gather_lanes` (its multi-source form) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in the `cmp` op bucket's `order` submodule.

use crate::shape::shape_of_value;
use std::sync::Arc;
use crate::value::{Bounds, Prim, Rows, Tags, Value};

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
        // `gather` at the all-zero index already is; there is no cheaper form of it here. (A Ref
        // row broadcasts as `n` references, since `gather` on a Ref moves refs.)
        Value::List(..) | Value::Sum(..) | Value::Ref(..) => gather(row, &vec![0usize; n]),
    }
}

/// TAKE REFERENCES: `Ref` = reference every top-level list row of `v` — a list's rows become spans
/// of its payload (the payload is the arena). Passes through products and sums, and leaves bounded
/// rows (leaves, units, rows already referenced) by value: only a list row is unbounded, so only a
/// list row is worth a reference. O(rows), nothing copied — the by-reference half of the pair.
pub(crate) fn take_ref(v: Value) -> Value {
    match v {
        Value::List(bounds, payload) => {
            let spans = (0..bounds.len()).map(|i| bounds.span(i)).collect();
            Value::Ref(Arc::new(*payload), Arc::new(spans))
        }
        Value::Prod(cols) => Value::Prod(cols.into_iter().map(take_ref).collect()),
        Value::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(take_ref).collect()),
        bounded @ (Value::Prim(_) | Value::Unit(_) | Value::Ref(..)) => bounded,
    }
}

/// CLONE OUT: `Clone` = `v` with every reference in it replaced by the rows it names, as a fresh
/// by-value list (the ONE place referenced data is copied). Deep, so the result holds no `Ref` and
/// `clone` undoes `ref`; a value without references comes back as it is.
pub(crate) fn clone_ref(v: Value) -> Value {
    match v {
        Value::Ref(payload, spans) => clone_ref(materialize_spans(&spans, &payload)),
        Value::List(bounds, vals) => Value::List(bounds, Box::new(clone_ref(*vals))),
        Value::Prod(cols) => Value::Prod(cols.into_iter().map(clone_ref).collect()),
        Value::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(clone_ref).collect()),
        leaf @ (Value::Prim(_) | Value::Unit(_)) => leaf,
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
    /// be a list or a referenced list (`Rows`): rows are read through `span`.
    pub(crate) fn resolve_indices(outer: &Bounds, idx: &[u64], hay: Rows) -> Vec<usize> {
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
    /// REFERENCED list it hands them back as references. A range outside its row is a (data-dependent)
    /// panic; `TrySlices` is the total form.
    pub(crate) fn range_spans(outer: &Bounds, lo: &[u64], hi: &[u64], hay: Rows) -> Vec<(usize, usize)> {
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
    /// exactly the spanned elements. What `Slices` on a list (and `clone` of a Ref) builds.
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
        Value::Sum(tags, variants) => {
            // one tag throughout: the selected rows are lane `t`'s rows at exactly `idx` (a `Const`
            // assignment's offset IS the row index), so this is one lane gather and no witness work.
            if let Some(t) = tags.const_tag() {
                let mut lanes: Vec<Value> = variants.iter().map(|v| gather(v, &[])).collect();
                lanes[t] = gather(&variants[t], idx);
                return Value::Sum(Tags::Const(t, idx.len()), lanes);
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
        // a reference column: move the spans, never the arena. This one arm is the entire cost model
        // of capture-by-reference — `CapList`/`CapSum`/`Lit` are gathers, so on a Ref they are free.
        Value::Ref(payload, spans) => Value::Ref(payload.clone(), Arc::new(idx.iter().map(|&i| spans[i]).collect())),
    }
}

/// `filter`'s values in one pass per leaf: the rows whose mask element is nonzero, in order, for a
/// leaf, a unit, or a product of those. `None` for a list, sum or reference, which keep the
/// positions-then-gather path.
pub(crate) fn compress(v: &Value, mask: &[u64]) -> Option<Value> {
    match v {
        Value::Prim(p) => Some(Value::Prim(p.compress(mask))),
        Value::Prod(fields) => fields.iter().map(|f| compress(f, mask)).collect::<Option<_>>().map(Value::Prod),
        Value::Unit(_) => Some(Value::Unit(mask.iter().filter(|&&b| b != 0).count())),
        Value::List(..) | Value::Sum(..) | Value::Ref(..) => None,
    }
}

/// `Unwrap` for lanes made of leaves (a leaf, or products of leaves): each row read from its lane
/// with the sum's own `u8` discriminants, in place, rather than widened into a `usize` column first.
/// `None` when a lane holds a list, sum, reference or unit, which take [`gather_lanes`].
pub(crate) fn unwrap_leaves(lanes: &[&Value], tags: &[u8], off: &[usize]) -> Option<Value> {
    match lanes[0] {
        Value::Prim(_) => {
            let prims: Vec<&Prim> = lanes
                .iter()
                .map(|v| match v {
                    Value::Prim(p) => Some(p),
                    _ => None,
                })
                .collect::<Option<_>>()?;
            Some(Value::Prim(Prim::gather_lanes(&prims, tags, off)))
        }
        Value::Prod(c0) => {
            let mut fields = Vec::with_capacity(c0.len());
            for f in 0..c0.len() {
                let lanes_f: Vec<&Value> = lanes
                    .iter()
                    .map(|v| match v {
                        Value::Prod(c) => c.get(f),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                fields.push(unwrap_leaves(&lanes_f, tags, off)?);
            }
            Some(Value::Prod(fields))
        }
        _ => None,
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
        // pick spans, never elements. Sources over ONE arena (by pointer) merge spans only — the
        // case of a loop state or a branch that keeps what it was handed. Over distinct arenas, each
        // arena contributes only what the result still references: the union of its picked spans,
        // copied once and rebased. So the result holds live rows only (a fold rebuilding its state
        // every round does not accumulate dead arenas), and a row referenced many times is still
        // copied once. (An empty span names nothing, so neither it nor an empty source's arena —
        // often a fresh `Value::empty` — counts.)
        Value::Ref(..) => {
            let parts: Vec<_> = filled
                .iter()
                .map(|v| match v {
                    Value::Ref(p, s) => (p, &s[..]),
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            let picked = |i: usize| parts[tags[i]].1[off[i]];
            let mut arenas: Vec<&Arc<Value>> = Vec::new();
            let mut arena_of = vec![0usize; parts.len()];
            for (k, (p, s)) in parts.iter().enumerate() {
                if s.is_empty() {
                    continue;
                }
                arena_of[k] = arenas.iter().position(|a| Arc::ptr_eq(a, p)).unwrap_or_else(|| {
                    arenas.push(p);
                    arenas.len() - 1
                });
            }
            if arenas.len() <= 1 {
                let payload = arenas.first().copied().unwrap_or(parts[0].0).clone();
                return Value::Ref(payload, Arc::new((0..tags.len()).map(picked).collect()));
            }
            // per arena, the union of the picked non-empty spans as disjoint sorted intervals.
            let mut used: Vec<Vec<(usize, usize)>> = vec![Vec::new(); arenas.len()];
            for i in 0..tags.len() {
                let (lo, hi) = picked(i);
                if lo < hi {
                    used[arena_of[tags[i]]].push((lo, hi));
                }
            }
            // each interval's elements are gathered into the new payload; `(lo, hi, base)` rebases.
            let (mut atags, mut aoff) = (Vec::new(), Vec::new());
            let mut kept: Vec<Vec<(usize, usize, usize)>> = Vec::with_capacity(arenas.len());
            for (a, spans) in used.iter_mut().enumerate() {
                spans.sort_unstable();
                let mut ivs: Vec<(usize, usize, usize)> = Vec::new();
                for &(lo, hi) in spans.iter() {
                    match ivs.last_mut() {
                        Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
                        _ => ivs.push((lo, hi, 0)),
                    }
                }
                for iv in ivs.iter_mut() {
                    iv.2 = atags.len();
                    atags.extend(std::iter::repeat_n(a, iv.1 - iv.0));
                    aoff.extend(iv.0..iv.1);
                }
                kept.push(ivs);
            }
            let srcs: Vec<Option<&Value>> = arenas.iter().map(|a| Some(&***a)).collect();
            let payload = Arc::new(gather_lanes(&srcs, &atags, &aoff));
            let spans = (0..tags.len())
                .map(|i| {
                    let (lo, hi) = picked(i);
                    if lo == hi {
                        return (0, 0);
                    }
                    let ivs = &kept[arena_of[tags[i]]];
                    let (ilo, _, base) = ivs[ivs.partition_point(|iv| iv.0 <= lo) - 1];
                    (base + lo - ilo, base + hi - ilo)
                })
                .collect();
            Value::Ref(payload, Arc::new(spans))
        }
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
pub(crate) fn blend(mask: &[u64], then: Value, els: Value) -> Value {
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
        Value::Ref(..) => {
            let owned: Vec<Value> = parts.iter().map(|v| clone_ref(v.clone())).collect();
            take_ref(concat(&owned))
        } // (test-only reference: by value, then re-referenced)
    }
}

#[cfg(test)]
mod tests;
