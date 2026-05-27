//! Cross-op helpers: gather, broadcast, slicing, concat, sort-merge intersect.
//! These don't implement PrimOp themselves; they're called by ops that need them.

use std::sync::Arc;
use crate::ir::value::{Value, Prim, PrimWidth, Selector, Storage, bounds_var_from_ends, prod, list, compose_selectors};
use crate::ir::shape::{Interp, bounds_as_u64};

/// Materialize a `Value::View` by gathering source through selector. Returns
/// non-View values unchanged. Use this when an op needs an unrestricted
/// Value and doesn't want to opt into View-aware fast paths.
pub fn materialize(v: Value) -> Result<Value, String> {
    match v {
        Value::View { source, selector } => {
            let idxs = selector.to_usize_vec();
            gather(&source, &idxs)
        }
        other => Ok(other),
    }
}

/// Materialize a `&Value::View` without consuming. Returns `Cow::Borrowed`
/// for non-View, `Cow::Owned` for View. Helpful when an op already has a
/// `&Value` borrow and doesn't want to clone.
///
/// Delegates to `stack::materialize_top` for the work (handles all selector
/// variants including SequenceRange).
pub fn materialize_ref(v: &Value) -> Result<std::borrow::Cow<'_, Value>, String> {
    use std::borrow::Cow;
    match v {
        Value::View { .. } => Ok(Cow::Owned(crate::ir::stack::materialize_top(v.clone())?)),
        other => Ok(Cow::Borrowed(other)),
    }
}

/// Visit every logical element of a Prim-shaped value as a `T`, regardless
/// of whether the storage is direct or viewed. Fast path for ops that just
/// want to iterate (`reduce.+`, `min`/`max`, scalar `where`, etc.) without
/// allocating an intermediate gathered buffer.
///
/// The `f` closure is called once per logical element, in selector order
/// (or source order for non-View values).
///
/// Returns an error if the value isn't a Prim of the expected width, or if
/// it's a View whose source isn't a Prim (which shouldn't happen given the
/// smart constructor, but we keep the path safe).
pub fn for_each_prim<T: Storage, F: FnMut(T)>(v: &Value, mut f: F) -> Result<(), String> {
    match v {
        Value::Prim(p) => {
            let xs = T::extract(p)?;
            for &x in xs { f(x); }
            Ok(())
        }
        Value::View { source, selector } => {
            let src_p = match source.as_ref() {
                Value::Prim(p) => p,
                other => return Err(format!("for_each_prim: View source isn't Prim: {:?}", other)),
            };
            let xs = T::extract(src_p)?;
            match selector {
                crate::ir::value::Selector::Indices(idxs) => {
                    for &i in idxs.iter() { f(xs[i as usize]); }
                }
                crate::ir::value::Selector::Runs(runs) => {
                    for (lo, hi) in runs.iter() {
                        for i in *lo as usize..*hi as usize { f(xs[i]); }
                    }
                }
                crate::ir::value::Selector::Mask(m) => {
                    // Streaming through mask — the hot path for chained
                    // filters consumed by reduce/count/arith without
                    // materialization.
                    for (i, &b) in m.iter().enumerate() {
                        if b != 0 { f(xs[i]); }
                    }
                }
                crate::ir::value::Selector::SequenceRange { .. } => {
                    return Err("for_each_prim: SequenceRange view is row-shaped, not flat".into());
                }
            }
            Ok(())
        }
        other => Err(format!("for_each_prim: expected Prim, got {:?}", other)),
    }
}


/// Per-Prim gather (width-monomorphic inner kernel).
pub fn gather_prim(p: &Prim, idxs: &[usize]) -> Prim {
    macro_rules! g { ($v:expr, $ctor:ident) => {{
        let xs: &[_] = $v;
        Prim::$ctor(Arc::new(idxs.iter().map(|&i| xs[i]).collect()))
    }};}
    match p {
        Prim::P8 (x) => g!(x, P8),
        Prim::P16(x) => g!(x, P16),
        Prim::P32(x) => g!(x, P32),
        Prim::P64(x) => g!(x, P64),
    }
}

/// Recursive gather over any Value shape.
///
/// When the input is a `View`, we compose the existing selector with the new
/// indices and materialize through the source — keeping the V-as-storage
/// invariant ("gather always returns a non-View result") and avoiding a
/// double allocation.
pub fn gather(v: &Value, idxs: &[usize]) -> Result<Value, String> {
    Ok(match v {
        Value::Prim(p) => Value::Prim(gather_prim(p, idxs)),
        Value::Prod(fs) => {
            let mut out = Vec::with_capacity(fs.len());
            for f in fs.iter() { out.push(gather(f, idxs)?); }
            prod(out)
        }
        Value::Sum { disc, lanes } => {
            // For each requested position, the corresponding Sum value
            // lives in lane `disc[i]` at the running cursor for that
            // disc among positions [0, i). Build:
            //   - new_disc: gather disc at idxs.
            //   - per-lane gather indices: for each output k that lands
            //     in lane L, the source position is the lane-L cursor
            //     of the original disc.
            let disc_bytes = match disc {
                Prim::P8(d) => d.as_slice(),
                _ => return Err("gather on Sum: disc must be P8".into()),
            };
            let n_src = disc_bytes.len();
            let mut orig_cursors = vec![0u64; n_src];
            let mut counts = vec![0u64; lanes.len()];
            for i in 0..n_src {
                let k = disc_bytes[i] as usize;
                if k >= lanes.len() {
                    return Err(format!("gather on Sum: disc[{}]={} but only {} lanes", i, k, lanes.len()));
                }
                orig_cursors[i] = counts[k];
                counts[k] += 1;
            }
            let new_disc: Vec<u8> = idxs.iter().map(|&i| disc_bytes[i]).collect();
            let mut per_lane_idxs: Vec<Vec<usize>> = vec![Vec::new(); lanes.len()];
            for &i in idxs {
                let k = disc_bytes[i] as usize;
                per_lane_idxs[k].push(orig_cursors[i] as usize);
            }
            let mut new_lanes: Vec<Value> = Vec::with_capacity(lanes.len());
            for (k, lane_idxs) in per_lane_idxs.iter().enumerate() {
                new_lanes.push(gather(&lanes[k], lane_idxs)?);
            }
            Value::Sum { disc: Prim::P8(Arc::new(new_disc)), lanes: Arc::new(new_lanes) }
        }
        Value::List { bounds, values } => {
            let bnds = bounds_as_u64(bounds)?;
            let mut new_bounds = Vec::with_capacity(idxs.len());
            let mut flat_idxs: Vec<usize> = Vec::new();
            for &i in idxs {
                let lo = bnds[i] as usize;
                let hi = bnds[i + 1] as usize;
                for j in lo..hi { flat_idxs.push(j); }
                new_bounds.push(flat_idxs.len() as u64);
            }
            let new_vals = gather(values, &flat_idxs)?;
            list(bounds_var_from_ends(new_bounds), new_vals)
        }
        Value::View { source, selector } => match selector {
            // Flat selectors: compose with idxs and recurse against source.
            Selector::Indices(_) | Selector::Runs(_) | Selector::Mask(_) => {
                let new_sel = Selector::from_indices(idxs.iter().map(|&i| i as u64).collect());
                let composed = compose_selectors(selector, &new_sel);
                let composed_usize = composed.to_usize_vec();
                gather(source, &composed_usize)?
            }
            // SequenceRange: gather *rows* of the per-row view. idxs[i]
            // picks row idxs[i] of the view, which corresponds to source
            // slice [los[idxs[i]], his[idxs[i]]). We materialize the
            // selected rows into a real List.
            Selector::SequenceRange { los, his } => {
                let new_los: Vec<u64> = idxs.iter().map(|&i| los[i]).collect();
                let new_his: Vec<u64> = idxs.iter().map(|&i| his[i]).collect();
                crate::ir::stack::materialize_top(Value::View {
                    source: source.clone(),
                    selector: Selector::SequenceRange {
                        los: Arc::new(new_los),
                        his: Arc::new(new_his),
                    },
                })?
            }
        }
    })
}

pub fn broadcast_prim(p: &Prim, n: usize) -> Prim {
    macro_rules! b { ($v:expr, $ctor:ident) => {
        Prim::$ctor(Arc::new(vec![$v[0]; n]))
    };}
    match p {
        Prim::P8 (x) => b!(x, P8),
        Prim::P16(x) => b!(x, P16),
        Prim::P32(x) => b!(x, P32),
        Prim::P64(x) => b!(x, P64),
    }
}

pub fn broadcast(v: &Value, n: usize) -> Value {
    match v {
        Value::Prim(p) => Value::Prim(broadcast_prim(p, n)),
        _ => panic!("broadcast: only primitives supported"),
    }
}

pub fn slice_prim(p: &Prim, lo: usize, hi: usize) -> Prim {
    macro_rules! s { ($v:expr, $ctor:ident) => {
        Prim::$ctor(Arc::new($v[lo..hi].to_vec()))
    };}
    match p {
        Prim::P8 (x) => s!(x, P8),
        Prim::P16(x) => s!(x, P16),
        Prim::P32(x) => s!(x, P32),
        Prim::P64(x) => s!(x, P64),
    }
}

pub fn slice_value(v: &Value, lo: usize, hi: usize) -> Result<Value, String> {
    Ok(match v {
        Value::Prim(p) => Value::Prim(slice_prim(p, lo, hi)),
        Value::Prod(fs) => {
            let mut out = Vec::with_capacity(fs.len());
            for f in fs.iter() { out.push(slice_value(f, lo, hi)?); }
            prod(out)
        }
        Value::List { bounds, values } => {
            let bnds = bounds_as_u64(bounds)?;
            // Canonical N+1 starts: row i's slice is [bnds[i], bnds[i+1]).
            let inner_lo = bnds[lo] as usize;
            let inner_hi = bnds[hi] as usize;
            let new_inner = slice_value(values, inner_lo, inner_hi)?;
            // bnds[lo..=hi] is N+1 entries covering rows lo..hi; rebase to start at 0.
            let new_bounds: Vec<u64> = bnds[lo..=hi].iter().map(|&b| b - inner_lo as u64).collect();
            list(crate::ir::value::bounds_var(new_bounds), new_inner)
        }
        other => return Err(format!("slice: unsupported {:?}", other)),
    })
}

/// Concat several Values of the same shape (used by `cat.N`).
pub fn concat_values(parts: &[Value]) -> Result<Value, String> {
    if parts.is_empty() { return Err("concat_values: empty".into()); }
    let first = &parts[0];
    if let Value::Prim(p0) = first {
        let width = p0.width();
        macro_rules! cp { ($ctor:ident, $t:ty) => {{
            let mut out: Vec<$t> = Vec::new();
            for p in parts {
                if let Value::Prim(Prim::$ctor(v)) = p { out.extend_from_slice(v); }
                else { return Err("concat_values: mixed types".into()); }
            }
            Ok(Value::Prim(Prim::$ctor(Arc::new(out))))
        }};}
        return match width {
            PrimWidth::W8  => cp!(P8,  u8),
            PrimWidth::W16 => cp!(P16, u16),
            PrimWidth::W32 => cp!(P32, u32),
            PrimWidth::W64 => cp!(P64, u64),
        };
    }
    match first {
        Value::Prod(_) => {
            let arity = if let Value::Prod(fs) = first { fs.len() } else { unreachable!() };
            let mut fields: Vec<Vec<Value>> = vec![Vec::new(); arity];
            for p in parts {
                if let Value::Prod(fs) = p {
                    if fs.len() != arity { return Err("concat_values: prod arity mismatch".into()); }
                    for (i, f) in fs.iter().enumerate() { fields[i].push(f.clone()); }
                } else { return Err("concat_values: mixed types".into()); }
            }
            let mut combined = Vec::with_capacity(arity);
            for fs in fields { combined.push(concat_values(&fs)?); }
            Ok(prod(combined))
        }
        Value::List { .. } => {
            let mut inners: Vec<Value> = Vec::new();
            let mut new_bounds: Vec<u64> = Vec::new();  // end-offsets per row
            let mut shift: u64 = 0;
            for p in parts {
                if let Value::List { bounds, values } = p {
                    inners.push((**values).clone());
                    for (_, hi) in bounds.iter_pairs() { new_bounds.push(hi + shift); }
                    shift = *new_bounds.last().unwrap_or(&shift);
                } else { return Err("concat_values: mixed types".into()); }
            }
            let new_inner = concat_values(&inners)?;
            Ok(list(bounds_var_from_ends(new_bounds), new_inner))
        }
        other => Err(format!("concat_values: unsupported {:?}", other)),
    }
}

/// Merge per-variant lane outputs back into one column by walking disc.
///
/// Supported lane shapes:
///   - `Prim<T>`: emit one element per disc position from the corresponding
///     lane's cursor.
///   - `List<Prim<T>>`: emit one row per disc position from the
///     corresponding lane's per-row cursor; concatenate inner values and
///     produce cumulative bounds.
///
/// All lanes must share the same shape. Lane lengths must equal the count
/// of their disc value in `disc`.
pub fn merge_by_disc(disc: &[u8], lanes: &[Value]) -> Result<Value, String> {
    match &lanes[0] {
        Value::Prim(first_prim) => {
            let width = first_prim.width();
            let mut cursors = vec![0usize; lanes.len()];
            macro_rules! merge_prim { ($ctor:ident, $t:ty) => {{
                let mut out: Vec<$t> = Vec::with_capacity(disc.len());
                for &d in disc {
                    let k = d as usize;
                    if let Value::Prim(Prim::$ctor(v)) = &lanes[k] {
                        out.push(v[cursors[k]]);
                        cursors[k] += 1;
                    } else {
                        return Err(format!("merge: lane {} has wrong type", k));
                    }
                }
                Ok(Value::Prim(Prim::$ctor(Arc::new(out))))
            }};}
            match width {
                PrimWidth::W8  => merge_prim!(P8,  u8),
                PrimWidth::W16 => merge_prim!(P16, u16),
                PrimWidth::W32 => merge_prim!(P32, u32),
                PrimWidth::W64 => merge_prim!(P64, u64),
            }
        }
        Value::List { values: first_values, .. } => {
            let inner_prim = match first_values.as_ref() {
                Value::Prim(p) => p,
                _ => return Err("merge_by_disc: list lanes must have Prim inners".into()),
            };
            let width = inner_prim.width();
            macro_rules! merge_list { ($ctor:ident, $t:ty) => {{
                // Pre-extract lane data.
                let mut lane_bnds: Vec<Vec<u64>> = Vec::with_capacity(lanes.len());
                let mut lane_vals: Vec<Vec<$t>> = Vec::with_capacity(lanes.len());
                for lane in lanes {
                    let (bounds, values) = match lane {
                        Value::List { bounds, values } => (bounds, values),
                        _ => return Err("merge_by_disc: lane not a list".into()),
                    };
                    let bnds = bounds_as_u64(bounds)?.into_owned();
                    let vals_prim = match values.as_ref() {
                        Value::Prim(p) => p,
                        _ => return Err("merge_by_disc: list inner must be Prim".into()),
                    };
                    let vals_slice = <$t as Storage>::extract(vals_prim)?.to_vec();
                    lane_bnds.push(bnds);
                    lane_vals.push(vals_slice);
                }
                // lane_bnds are canonical N+1 starts; cursor `k` advances
                // through pairs (lane_bnds[k][cur], lane_bnds[k][cur+1]).
                let mut cursors = vec![0usize; lanes.len()];
                let mut bounds_out: Vec<u64> = Vec::with_capacity(disc.len());
                let mut values_out: Vec<$t> = Vec::new();
                let mut acc = 0u64;
                for &d in disc {
                    let k = d as usize;
                    let cur = cursors[k];
                    let bnds = &lane_bnds[k];
                    let vals = &lane_vals[k];
                    let lo = bnds[cur] as usize;
                    let hi = bnds[cur + 1] as usize;
                    for j in lo..hi { values_out.push(vals[j]); }
                    acc += (hi - lo) as u64;
                    bounds_out.push(acc);
                    cursors[k] += 1;
                }
                Ok(Value::List {
                    bounds: bounds_var_from_ends(bounds_out),
                    values: Arc::new(crate::ir::value::from_vec::<$t>(values_out)),
                })
            }};}
            match width {
                PrimWidth::W8  => merge_list!(P8,  u8),
                PrimWidth::W16 => merge_list!(P16, u16),
                PrimWidth::W32 => merge_list!(P32, u32),
                PrimWidth::W64 => merge_list!(P64, u64),
            }
        }
        other => Err(format!(
            "merge_by_disc: lane shape not supported ({:?}); expected Prim or List<Prim>",
            other
        )),
    }
}

pub fn sum_runs(values: &Value, bounds: &[u64], interp: Interp) -> Result<Value, String> {
    let p = match values {
        Value::Prim(p) => p,
        _ => return Err("sum_runs: expected Prim".into()),
    };
    macro_rules! run { ($t:ty) => {{
        let xs = <$t as Storage>::extract(p)?;
        let mut out: Vec<$t> = Vec::with_capacity(bounds.len().saturating_sub(1));
        for w in bounds.windows(2) {
            let lo = w[0] as usize;
            let hi = w[1] as usize;
            let mut s: $t = <$t as Default>::default();
            for &x in &xs[lo..hi] { s = s + x; }
            out.push(s);
        }
        Ok(crate::ir::value::from_vec::<$t>(out))
    }};}
    match interp {
        Interp::U8  => run!(u8),
        Interp::I8  => run!(i8),
        Interp::U16 => run!(u16),
        Interp::I16 => run!(i16),
        Interp::U32 => run!(u32),
        Interp::I32 => run!(i32),
        Interp::U64 => run!(u64),
        Interp::I64 => run!(i64),
        Interp::F32 => run!(f32),
        Interp::F64 => run!(f64),
    }
}

/// Sum a flat (non-List) value into a single scalar. Accepts either a plain
/// `Prim` or a `View` over a Prim. The View case dispatches on selector
/// variant in `for_each_prim` and avoids materializing an intermediate.
pub fn sum_whole(v: &Value, interp: Interp) -> Result<Value, String> {
    macro_rules! one { ($t:ty) => {{
        let mut acc: $t = <$t as Default>::default();
        for_each_prim::<$t, _>(v, |x| acc = acc + x)?;
        Ok(crate::ir::value::from_vec::<$t>(vec![acc]))
    }};}
    match interp {
        Interp::U8  => one!(u8),
        Interp::I8  => one!(i8),
        Interp::U16 => one!(u16),
        Interp::I16 => one!(i16),
        Interp::U32 => one!(u32),
        Interp::I32 => one!(i32),
        Interp::U64 => one!(u64),
        Interp::I64 => one!(i64),
        Interp::F32 => one!(f32),
        Interp::F64 => one!(f64),
    }
}

/// Sort-merge intersection over a Prim, interpreted as `T`. Returns matched
/// position pairs (ia, ib).
/// Interp-free (byte/unsigned-word compare; signed/float order via
/// order-form inputs). Dispatches on width.
pub fn sort_merge_intersect(a: &Value, b: &Value) -> Result<(Vec<usize>, Vec<usize>), String> {
    let pa = match a { Value::Prim(p) => p, _ => return Err("intersect: not a prim".into()) };
    let pb = match b { Value::Prim(p) => p, _ => return Err("intersect: not a prim".into()) };
    macro_rules! intersect_t { ($t:ty) => {{
        let av = <$t as Storage>::extract(pa)?;
        let bv = <$t as Storage>::extract(pb)?;
        let mut ia = 0usize; let mut ib = 0usize;
        let mut ra: Vec<usize> = Vec::new();
        let mut rb: Vec<usize> = Vec::new();
        while ia < av.len() && ib < bv.len() {
            use std::cmp::Ordering::*;
            match av[ia].cmp(&bv[ib]) {
                Less => {
                    let target = bv[ib];
                    ia = gallop_to(av, ia + 1, |x| *x < target);
                }
                Equal => {
                    let v = av[ia];
                    let mut ja = ia + 1;
                    while ja < av.len() && av[ja] == v { ja += 1; }
                    let mut jb = ib + 1;
                    while jb < bv.len() && bv[jb] == v { jb += 1; }
                    for i in ia..ja {
                        for j in ib..jb { ra.push(i); rb.push(j); }
                    }
                    ia = ja; ib = jb;
                }
                Greater => {
                    let target = av[ia];
                    ib = gallop_to(bv, ib + 1, |x| *x < target);
                }
            }
        }
        Ok((ra, rb))
    }};}
    match pa.width() {
        PrimWidth::W8  => intersect_t!(u8),
        PrimWidth::W16 => intersect_t!(u16),
        PrimWidth::W32 => intersect_t!(u32),
        PrimWidth::W64 => intersect_t!(u64),
    }
}

pub fn gallop_to<T, F: Fn(&T) -> bool>(s: &[T], mut lower: usize, pred: F) -> usize {
    if lower >= s.len() || !pred(&s[lower]) { return lower; }
    let mut step = 1usize;
    while lower + step < s.len() && pred(&s[lower + step]) {
        lower += step;
        step <<= 1;
    }
    step >>= 1;
    while step > 0 {
        if lower + step < s.len() && pred(&s[lower + step]) { lower += step; }
        step >>= 1;
    }
    lower + 1
}

pub fn extract_prim(v: &Value, ctx: &str) -> Result<Prim, String> {
    match v {
        Value::Prim(p) => Ok(p.clone()),
        other => Err(format!("{}: expected Prim, got {:?}", ctx, other)),
    }
}

/// Segmented element-wise binary op (principle 4: grouping is a transparent
/// restriction). Dispatches the List representation so that compute ops keep
/// per-row grouping instead of forcing a flatten:
///
///   - both sides `List` with **equal bounds**: run `f` on the inner values
///     and rewrap under the shared bounds (`List<T> List<T> → List<T'>`),
///   - `List` op length-1 scalar `Prim` (either side): broadcast the scalar
///     across the inner values — the inner kernel already broadcasts length-1,
///     so we just hand it the scalar Prim and reattach the List's bounds,
///   - neither side a `List`: return `None` so the caller falls through to its
///     flat / View fast paths.
///
/// Mismatched-bounds Lists are an error ("segmented op: bounds differ") — we
/// never silently broadcast one grouping against another.
pub fn list_elementwise2<F>(a: &Value, b: &Value, f: F) -> Option<Result<Value, String>>
where F: Fn(&Value, &Value) -> Result<Value, String>
{
    match (a, b) {
        (Value::List { bounds: ba, values: va }, Value::List { bounds: bb, values: vb }) => {
            if ba != bb {
                return Some(Err("segmented op: bounds differ".into()));
            }
            Some(f(va, vb).map(|inner| list(ba.clone(), inner)))
        }
        (Value::List { bounds, values }, Value::Prim(p)) if p.len() == 1 => {
            Some(f(values, b).map(|inner| list(bounds.clone(), inner)))
        }
        (Value::Prim(p), Value::List { bounds, values }) if p.len() == 1 => {
            Some(f(a, values).map(|inner| list(bounds.clone(), inner)))
        }
        _ => None,
    }
}

/// Segmented element-wise unary op — the unary sibling of
/// [`list_elementwise2`]. Runs `f` on a `List`'s inner values and reattaches
/// the same bounds; `None` when the input isn't a `List`.
pub fn list_elementwise1<F>(a: &Value, f: F) -> Option<Result<Value, String>>
where F: Fn(&Value) -> Result<Value, String>
{
    match a {
        Value::List { bounds, values } => Some(f(values).map(|inner| list(bounds.clone(), inner))),
        _ => None,
    }
}
