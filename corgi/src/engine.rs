//! The engine: the row-movement primitives every shape op reduces to — `gather` (move rows by index) and
//! `concat` (append same-shape columns) — plus the bound helpers and `mod generators` (the `gather`-family
//! index currency). The structural comparator lives in [`crate::cmp`].

use crate::value::{Prim, Value};

pub use generators::*;

pub fn count_at(b: &[usize], i: usize) -> usize {
    b[i] - if i == 0 { 0 } else { b[i - 1] }
}

pub fn row_span(b: &[usize], i: usize) -> (usize, usize) {
    (if i == 0 { 0 } else { b[i - 1] }, b[i])
}

/// lift a single-row constant to a column of length `n` (its stratum): `n` copies of `row`'s row 0. Total
/// over every shape — it is `gather` at the all-zero index, so `Op::Lit`'s `judge` (which accepts any value's
/// shape) and `eval` agree.
pub fn fill(row: &Value, n: usize) -> Value {
    gather(row, &vec![0usize; n])
}

mod generators {
    //! Index generators — the `gather`-family currency. Each composite op is "make an index (and sometimes
    //! re-segmented bounds), then `gather`": mask→survivors (`Filter`/`Partition`), bounds→owner-ids
    //! (`Broadcast`), range-expand (`Slices`), tag→offset (`Unwrap`). The index math lives here; the op bodies
    //! in `ops::core` just generate, gather, and re-wrap.

    use super::*;

    /// the mask family: split element positions by a 0/1 `mask` into `(mask==0 positions, mask!=0 positions)`,
    /// each ascending. The shared atom under `Filter` (gathers the nonzero set, re-segmented) and `Partition`
    /// (gathers both into a Sum). Filter ⊂ Partition: Filter's survivors are exactly Partition's nonzero lane.
    pub fn split_by_mask(mask: &[u64]) -> (Vec<usize>, Vec<usize>) {
        let mut falses = Vec::new();
        let mut trues = Vec::new();
        for (j, &b) in mask.iter().enumerate() {
            if b != 0 { trues.push(j) } else { falses.push(j) }
        }
        (falses, trues)
    }

    /// the mask family, segmented: over a list's `bounds` and a per-element 0/1 `mask`, the surviving (nonzero)
    /// positions AND the re-counted per-row bounds, in one pass. Pairs with `gather` to realise `Filter`. The
    /// seq-level sibling is `split_by_mask` (Partition's); Filter needs the segmentation, so it has its own —
    /// the two share only the "mask nonzero" predicate, not a generator.
    pub fn filter_mask(bounds: &[usize], mask: &[u64]) -> (Vec<usize>, Vec<usize>) {
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

    /// the broadcast family: expand a list's `bounds` to the owner row of each element — `[2,3,6]` →
    /// `[0,0,1,2,2,2]`. Pairs with `gather` to replicate the scalar side of `Broadcast`. (The inverse of
    /// `bounds`: position → segment.)
    pub fn owner_ids(bounds: &[usize]) -> Vec<usize> {
        let mut idx = Vec::with_capacity(bounds.last().copied().unwrap_or(0));
        for i in 0..bounds.len() {
            idx.extend(std::iter::repeat_n(i, count_at(bounds, i)));
        }
        idx
    }

    /// the range family: `(lo,hi)` pairs grouped by `outer` into rows, each pair RELATIVE to its haystack row
    /// (rows spanned by `hay`). Emits the absolute haystack positions each pair names and the per-pair inner
    /// bounds. Pairs with `gather` to realise `Slices` — the materialising inverse of `Flatten`.
    pub fn expand_ranges(outer: &[usize], lo: &[u64], hi: &[u64], hay: &[usize]) -> (Vec<usize>, Vec<usize>) {
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

    /// the sum family: for a tagged column whose lanes have lengths `lane_lens`, the position of each row
    /// within `concat(lanes)` — lane start plus a running per-lane cursor. Pairs with `gather ∘ concat` to
    /// realise `Unwrap`. The same per-lane-cursor pass `gather`/`compare2`'s Sum arms (re)compute by hand.
    pub fn tag_offsets(tags: &[usize], lane_lens: &[usize]) -> Vec<usize> {
        let mut cursor = Vec::with_capacity(lane_lens.len());
        let mut acc = 0;
        for &l in lane_lens {
            cursor.push(acc);
            acc += l;
        }
        tags.iter().map(|&t| { let p = cursor[t]; cursor[t] += 1; p }).collect()
    }
}

/// build a column whose row j is `v`'s row `idx[j]`; recurses through every shape.
pub fn gather(v: &Value, idx: &[usize]) -> Value {
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
        Value::Sum(tags, variants) => {
            let tag_vec = tags.usize_vec();
            let mut within = vec![0usize; tag_vec.len()];
            let mut counts = vec![0usize; variants.len()];
            for (i, &t) in tag_vec.iter().enumerate() {
                within[i] = counts[t];
                counts[t] += 1;
            }
            let new_tags = tags.gather(idx); // the discriminant moves like any leaf column
            let mut per = vec![Vec::new(); variants.len()];
            for &i in idx {
                per[tag_vec[i]].push(within[i]);
            }
            let nv = variants.iter().zip(&per).map(|(v, s)| gather(v, s)).collect();
            Value::Sum(new_tags, nv)
        }
    }
}

/// concatenate same-shape columns end to end, re-basing witnesses.
pub fn concat(parts: &[Value]) -> Value {
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
        Value::Sum(_, v0) => {
            let mut tag_parts: Vec<&Prim> = Vec::new();
            let mut per = vec![Vec::new(); v0.len()];
            for p in parts {
                match p {
                    Value::Sum(t, v) => {
                        tag_parts.push(t);
                        for (i, c) in v.iter().enumerate() {
                            per[i].push(c.clone());
                        }
                    }
                    _ => panic!("concat: shape mismatch"),
                }
            }
            Value::Sum(Prim::concat(&tag_parts), per.iter().map(|ps| concat(ps)).collect())
        }
    }
}
