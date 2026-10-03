//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `gather_lanes` (its multi-source form) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in the `cmp` op bucket's `order` submodule.

use crate::shape::shape_of_value;
use std::sync::Arc;
use crate::value::{arena, zero_row, Bounds, Prim, Rows, Tags, Value};

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

/// TAKE REFERENCES: `Ref` = reference every top-level list row of `v` — a list becomes the arena,
/// and each of its rows a reference to itself by number. Passes through products and sums, and
/// leaves bounded rows (leaves, units, rows already referenced) by value: only a list row is
/// unbounded, so only a list row is worth a reference. O(rows), nothing copied — the
/// by-reference half of the pair.
pub(crate) fn take_ref(v: Value) -> Value {
    match v {
        list @ Value::List(..) => {
            let rows = (0..list.len()).collect();
            Value::Ref(Arc::new(arena(list)), Arc::new(rows))
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
        // the named rows, copied (the zero reference names the arena's empty row).
        Value::Ref(list, rows) => clone_ref(gather(&list, &rows)),
        Value::List(bounds, vals) => Value::List(bounds, Box::new(clone_ref(*vals))),
        Value::Prod(cols) => Value::Prod(cols.into_iter().map(clone_ref).collect()),
        Value::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(clone_ref).collect()),
        leaf @ (Value::Prim(_) | Value::Unit(_)) => leaf,
    }
}

mod generators {
    //! Index generators — the `gather`-family currency. Each composite op is "make an index (and sometimes
    //! re-segmented bounds), then `gather`": mask→survivors (`Filter`), bounds→owner-ids (`CapList`),
    //! positions of any shape (`Gather`, via `index_plan`). The index math lives here; the op bodies in
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

    /// the top-level row of each element of a value inside an index structure: the identity at the
    /// top (and through products), a column once lists or sums have rearranged the elements.
    pub(crate) enum Owners {
        Identity,
        Rows(Vec<usize>),
    }

    impl Owners {
        fn get(&self, j: usize) -> usize {
            match self {
                Owners::Identity => j,
                Owners::Rows(rows) => rows[j],
            }
        }
    }

    /// an index value with every integer leaf resolved to absolute haystack positions: the same
    /// structure (bounds and tags kept), each leaf column replaced by the positions it names.
    /// [`IndexPlan::fill`] gathers them.
    pub(crate) enum IndexPlan {
        Leaf(Vec<usize>),
        Unit(usize),
        Prod(Vec<IndexPlan>),
        List(Bounds, Box<IndexPlan>),
        Sum(Tags, Vec<IndexPlan>),
    }

    /// resolve an index value of any shape (`gather`'s positions): every integer leaf is a position
    /// relative to its element's TOP-level row of the haystack (`hay`). Clears `ok[r]` when top-level
    /// row `r` names a position outside its row; such a position is recorded as `usize::MAX`, which
    /// [`IndexPlan::fill_or_zero`] reads as zero and [`IndexPlan::fill`] must never see.
    pub(crate) fn index_plan(index: &Value, owners: &Owners, hay: Rows, ok: &mut [bool]) -> Result<IndexPlan, String> {
        // a position `x` in owner row `r`, whose span is `(hs, he)`: its place in the payload, or
        // `usize::MAX` (and `r` marked) when it lies outside the row.
        fn resolve(ok: &mut [bool], r: usize, (hs, he): (usize, usize), x: u64, pos: &mut Vec<usize>) {
            let inside = x < (he - hs) as u64;
            ok[r] &= inside;
            pos.push(if inside { hs + x as usize } else { usize::MAX });
        }
        Ok(match index {
            Value::Prim(p) => {
                let mut pos = Vec::with_capacity(p.len());
                match p {
                    Prim::U64(xs) => xs.iter().enumerate().for_each(|(j, &x)| {
                        let r = owners.get(j);
                        resolve(ok, r, hay.span(r), x, &mut pos)
                    }),
                    _ => (0..p.len()).for_each(|j| {
                        let r = owners.get(j);
                        resolve(ok, r, hay.span(r), p.u64_at(j), &mut pos)
                    }),
                }
                IndexPlan::Leaf(pos)
            }
            Value::Unit(n) => IndexPlan::Unit(*n),
            Value::Prod(fields) => {
                IndexPlan::Prod(fields.iter().map(|f| index_plan(f, owners, hay, ok)).collect::<Result<_, _>>()?)
            }
            // a list of positions (the common case) resolves row by row with no owner column; a list
            // of anything else hands its elements their rows.
            Value::List(bounds, vals) => match &**vals {
                Value::Prim(Prim::U64(xs)) => {
                    let mut pos = Vec::with_capacity(xs.len());
                    // one owner per row of positions, so its span is read once per row.
                    for i in 0..bounds.len() {
                        let r = owners.get(i);
                        let (s, e) = bounds.span(i);
                        let span = hay.span(r);
                        xs[s..e].iter().for_each(|&x| resolve(ok, r, span, x, &mut pos));
                    }
                    IndexPlan::List(bounds.clone(), Box::new(IndexPlan::Leaf(pos)))
                }
                inner => {
                    let mut rows = Vec::with_capacity(inner.len());
                    for i in 0..bounds.len() {
                        let (s, e) = bounds.span(i);
                        rows.extend(std::iter::repeat_n(owners.get(i), e - s));
                    }
                    IndexPlan::List(bounds.clone(), Box::new(index_plan(inner, &Owners::Rows(rows), hay, ok)?))
                }
            },
            // a lane's elements are its rows in row order, so each lane takes its rows' owners.
            Value::Sum(tags, lanes) => {
                let mut rows: Vec<Vec<usize>> = vec![Vec::new(); lanes.len()];
                for (i, t) in tags.tags_iter().enumerate() {
                    rows[t].push(owners.get(i));
                }
                let lanes = lanes
                    .iter()
                    .zip(rows)
                    .map(|(lane, rows)| index_plan(lane, &Owners::Rows(rows), hay, ok))
                    .collect::<Result<_, _>>()?;
                IndexPlan::Sum(tags.clone(), lanes)
            }
            Value::Ref(..) => return Err("gather: positions can't be a referenced list; clone first".into()),
        })
    }

    impl IndexPlan {
        /// as [`IndexPlan::fill`], with a position outside its row reading the zero of the element's
        /// shape (see [`gather_or_zero`]).
        pub(crate) fn fill_or_zero(self, hvals: &Value) -> Result<Value, String> {
            Ok(match self {
                IndexPlan::Leaf(pos) => gather_or_zero(hvals, &pos)?,
                IndexPlan::Unit(n) => Value::Unit(n),
                IndexPlan::Prod(fields) => Value::Prod(fields.into_iter().map(|f| f.fill_or_zero(hvals)).collect::<Result<_, _>>()?),
                IndexPlan::List(bounds, inner) => Value::List(bounds, Box::new(inner.fill_or_zero(hvals)?)),
                IndexPlan::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(|l| l.fill_or_zero(hvals)).collect::<Result<_, _>>()?),
            })
        }

        /// the index value with each leaf replaced by the haystack elements its positions name.
        pub(crate) fn fill(self, hvals: &Value) -> Value {
            match self {
                IndexPlan::Leaf(pos) => gather(hvals, &pos),
                IndexPlan::Unit(n) => Value::Unit(n),
                IndexPlan::Prod(fields) => Value::Prod(fields.into_iter().map(|f| f.fill(hvals)).collect()),
                IndexPlan::List(bounds, inner) => Value::List(bounds, Box::new(inner.fill(hvals))),
                IndexPlan::Sum(tags, lanes) => Value::Sum(tags, lanes.into_iter().map(|l| l.fill(hvals)).collect()),
            }
        }
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
        // a reference column: move the row numbers, never the arena. This one arm is the entire cost
        // model of capture-by-reference — `CapList`/`CapSum`/`Lit` are gathers, so on a Ref they move
        // one number per row.
        Value::Ref(list, rows) => Value::Ref(list.clone(), Arc::new(idx.iter().map(|&i| rows[i]).collect())),
    }
}

/// `gather` where a position past `v`'s rows reads the ZERO of the element's shape: zero bits for a
/// leaf, the empty list, a unit, lane 0 (holding its own zero) for a sum. A sum of no lanes has no
/// zero, so it is an error. Every in-range position reads as `gather` would.
pub(crate) fn gather_or_zero(v: &Value, idx: &[usize]) -> Result<Value, String> {
    Ok(match v {
        Value::Prim(p) => Value::Prim(p.gather_or_zero(idx)),
        Value::Prod(cols) => Value::Prod(cols.iter().map(|c| gather_or_zero(c, idx)).collect::<Result<_, _>>()?),
        Value::Unit(_) => Value::Unit(idx.len()),
        Value::List(bounds, vals) => {
            let (mut elem, mut nb) = (Vec::new(), Vec::with_capacity(idx.len()));
            for &i in idx {
                if i < bounds.len() {
                    let (s, e) = row_span(bounds, i);
                    elem.extend(s..e);
                }
                nb.push(elem.len());
            }
            Value::List(nb.into(), Box::new(gather(vals, &elem)))
        }
        Value::Sum(tags, lanes) => {
            if lanes.is_empty() {
                return Err("gather: a sum of no lanes has no zero to read out of range".into());
            }
            // a position past the rows goes to lane 0, which reads it as its own zero.
            let mut per = vec![Vec::new(); lanes.len()];
            let (mut new_tags, mut new_off) = (Vec::with_capacity(idx.len()), Vec::with_capacity(idx.len()));
            for &i in idx {
                let (t, at) = if i < tags.len() { (tags.tag_at(i), tags.offset_at(i)) } else { (0, usize::MAX) };
                new_tags.push(t as u8);
                new_off.push(per[t].len());
                per[t].push(at);
            }
            let mut nv = Vec::with_capacity(lanes.len());
            for (k, (lane, at)) in lanes.iter().zip(&per).enumerate() {
                nv.push(if k == 0 { gather_or_zero(lane, at)? } else { gather(lane, at) });
            }
            Value::Sum(Tags::column(Prim::U8(Arc::new(new_tags)), new_off), nv)
        }
        // out of range: the arena's empty row, which every arena keeps for this.
        Value::Ref(list, rows) => {
            let zero = zero_row(list);
            Value::Ref(list.clone(), Arc::new(idx.iter().map(|&i| rows.get(i).copied().unwrap_or(zero)).collect()))
        }
    })
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
        // pick row numbers, never elements. Sources over ONE arena (by pointer) merge row numbers
        // only — the case of a loop state or a branch that keeps what it was handed. Over distinct
        // arenas, each arena contributes only what the result still references: the rows it names,
        // copied once into one new arena, and the numbers rebased. So the result holds live rows only
        // (a fold rebuilding its state every round does not accumulate dead arenas), and a row
        // referenced many times is still copied once, and the new arena ends in its own empty row.
        // (A source naming no rows — often a fresh `Value::empty` — doesn't count.)
        Value::Ref(..) => {
            let parts: Vec<(&Arc<Value>, &[usize])> = filled
                .iter()
                .map(|v| match v {
                    Value::Ref(list, rows) => (list, &rows[..]),
                    _ => panic!("gather_lanes: shape mismatch"),
                })
                .collect();
            let picked = |i: usize| parts[tags[i]].1[off[i]];
            let mut arenas: Vec<&Arc<Value>> = Vec::new();
            let mut arena_of = vec![0usize; parts.len()];
            for (k, (list, rows)) in parts.iter().enumerate() {
                if rows.is_empty() {
                    continue;
                }
                arena_of[k] = arenas.iter().position(|a| Arc::ptr_eq(a, list)).unwrap_or_else(|| {
                    arenas.push(list);
                    arenas.len() - 1
                });
            }
            if arenas.len() <= 1 {
                let list = arenas.first().copied().unwrap_or(parts[0].0).clone();
                return Value::Ref(list, Arc::new((0..tags.len()).map(picked).collect()));
            }
            // per arena, the distinct rows the result names, in order; the new arena holds each
            // arena's rows in turn, so a row's new number is its arena's base plus its rank.
            let mut used: Vec<Vec<usize>> = vec![Vec::new(); arenas.len()];
            for i in 0..tags.len() {
                used[arena_of[tags[i]]].push(picked(i));
            }
            let (mut atags, mut aoff, mut base) = (Vec::new(), Vec::new(), Vec::with_capacity(arenas.len()));
            for (a, rows) in used.iter_mut().enumerate() {
                rows.sort_unstable();
                rows.dedup();
                base.push(atags.len());
                atags.extend(std::iter::repeat_n(a, rows.len()));
                aoff.extend(rows.iter().copied());
            }
            let srcs: Vec<Option<&Value>> = arenas.iter().map(|a| Some(&***a)).collect();
            // the new arena ends in its own empty row, as every arena does.
            let list = Arc::new(arena(gather_lanes(&srcs, &atags, &aoff)));
            let rows = (0..tags.len())
                .map(|i| {
                    let a = arena_of[tags[i]];
                    base[a] + used[a].binary_search(&picked(i)).expect("a picked row is used")
                })
                .collect();
            Value::Ref(list, Arc::new(rows))
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
