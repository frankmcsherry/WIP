//! The operator vocabulary. Every *semantic* op is a unary `T0 -> T1` evaluated by
//! `eval`; the input's type carries the shape requirement (no `arity()`). The two
//! structural nodes (`Input`, `Tuple`) are handled by the evaluator, not here.

use crate::engine::{
    expand_ranges, fill, filter_mask, gather, gather_lanes, owner_ids, resolve_indices,
};
use crate::graph::{eval_graph, shape_of, Graph, OpLike};
use crate::shape::{join, shape_of_value, Shape};
use crate::value::Value;

/// overwrite `acc`'s rows at positions `active` (in order) with `new`'s rows — the scatter inverse of
/// `gather`, the accumulator update for `Fold`/`Scan`.
///
/// FIXED-WIDTH `acc` (a leaf, or a product of leaves) is mutated IN PLACE: each row has a constant
/// slot, so we overwrite only the `active` rows, reusing the buffer round-to-round — no per-round
/// allocation, no copying of the unchanged rows. VARIABLE-WIDTH `acc` (containing a `List`/`Sum`,
/// whose rows differ in size) can't be slot-overwritten, so it falls back to the rebuild via
/// two-source `gather_lanes` (correct, the prior behaviour; an uncommon accumulator shape).
fn scatter(mut acc: Value, active: &[usize], new: Value) -> Value {
    if fixed_width(&acc) {
        scatter_fixed(&mut acc, active, &new);
        acc
    } else {
        let n = acc.len();
        let mut tags = vec![0usize; n];
        let mut off: Vec<usize> = (0..n).collect();
        for (slot, &r) in active.iter().enumerate() {
            tags[r] = 1;
            off[r] = slot;
        }
        gather_lanes(&[Some(&acc), Some(&new)], &tags, &off)
    }
}

/// every row occupies a constant byte slot — true for a leaf and a product of fixed-width fields,
/// false once a `List` or `Sum` (variable-size rows) appears. Checked WHOLE before any mutation, so
/// the in-place pass below can't half-write a value that turns out to be variable-width deeper down.
fn fixed_width(v: &Value) -> bool {
    match v {
        Value::Prim(_) | Value::Unit(_) => true, // a unit row is a (zero-byte) constant slot
        Value::Prod(cs) => cs.iter().all(fixed_width),
        Value::List(..) | Value::Sum(..) => false,
    }
}

/// in-place scatter for a fixed-width `acc` (precondition: `fixed_width(acc)`), recursing products to
/// the leaves where the actual write happens.
fn scatter_fixed(acc: &mut Value, active: &[usize], new: &Value) {
    match (acc, new) {
        (Value::Prim(d), Value::Prim(s)) => d.scatter_into(active, s),
        (Value::Prod(ca), Value::Prod(cn)) => {
            for (fa, fb) in ca.iter_mut().zip(cn) {
                scatter_fixed(fa, active, fb);
            }
        }
        (Value::Unit(_), Value::Unit(_)) => {} // no payload to overwrite (length is unchanged)
        _ => unreachable!("scatter_fixed: fixed_width guarantees Prim/Prod/Unit"),
    }
}

/// the lockstep work-list for `Fold`/`Scan`, maintained incrementally. `(row, start, len)` for each
/// non-empty row, in row order. A row stays until round `len`; after each round the caller `retain`s
/// `len > t`, so the per-round cost tracks the *active* rows, not all N (the win for ragged lengths —
/// vs rescanning every row's bounds each round, which was O(N·maxlen)). `row`/`start` give the gather
/// targets: row `r`'s round-`t` element is at flat index `start + t`.
fn init_active(bounds: &[usize]) -> Vec<(usize, usize, usize)> {
    let mut v = Vec::with_capacity(bounds.len());
    let mut start = 0;
    for (r, &end) in bounds.iter().enumerate() {
        if end > start {
            v.push((r, start, end - start));
        }
        start = end;
    }
    v
}

/// the core op vocabulary: structure only — comparison/order is the `cmp` bucket (`ops::cmp`) and
/// arithmetic the `numeric` layer. Generic over `L`, the layer used for body sub-graphs, so a higher layer's
/// `map` bodies can use the higher vocabulary. `Op<L>` is not itself `OpLike` — the layer enum is (e.g.
/// `NumOp`), delegating to these inherent methods.
///
/// Organized as the KERNEL MATRIX — (intro / elim / map / capture) × (Prod / Sum / List) — plus
/// two further tiers: the structural isos (re-slicings the columnar layout stores for free) and
/// the fused forms / producers (each reducible to the kernel where the isos allow — the `law`
/// corpus programs witness it — but kept for the execution strategy the expansion would lose).
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Op<L> {
    // ---- the kernel matrix ------------------------------------------------------------------
    // PROD — intro is the graph-structural `Tuple`. Products are transparent (fixed arity, no
    // witness column), so map is projection+rebuild and capture is `tuple` itself: no ops needed.
    Field(usize),   // elim:  (.., X_i, ..) -> X_i
    // SUM — witness: the tag column.
    Branch(usize),  // intro: (X, U64-tags) -> Sum{X × n}  data-driven demux: row i -> variant
                    //        tags[i]. (The boolean split is the idiom `Branch(2)` on a 0/1 mask.)
                    //        PARTIAL: panics on tag >= n. The clean kernel; surface-usable once a
                    //        range pass proves tags < n (else write TryBranch).
    TryBranch(usize), // intro: (X, U64-tags) -> Sum{Oob:U64 | X × n}  TOTAL Branch: an out-of-range
                    // tag lands in lane 0 (Oob) carrying the bad tag; tag t<n lands in lane t+1. The
                    // FOO_TRY of Branch — usable with no proof (the index/ParseU64 discipline).
    Inject(usize, usize), // intro: X -> Sum{tag: X, ⊥..} — the constant-tag Branch; the other
                    //        lanes are ⊥ (empty, shape uncommitted, pinned later by `join`).
    Unwrap,         // elim:  homogeneous (up to ⊥) Sum -> payload
    MapSum(Vec<(usize, Graph<L>)>), // map: closed bodies on chosen variants; unlisted variants
                    // pass through. The Vec breaks the type recursion, so no Box. A variadic
                    // match — disjoint indices keep the arms independent (the optimizer relies
                    // on this; `judge` rejects duplicates).
    CapSum,         // capture: (X, Sum{A | B | ..}) -> Sum{(X,A) | (X,B) | ..} — distribute a
                    // context into each lane (⊥ lanes stay ⊥); lets a `match` arm see an outer
                    // value (closure capture, made explicit).
    // LIST — witness: the bounds column.
    Enlist,         // intro: X -> List<X>  each element its own length-1 list (list-monad unit)
    MapList(Box<Graph<L>>), // map: closed body on a list's values (in the *layer* L)
    Fold(Box<Graph<L>>), // elim: (B, List<A>) -> B  seeded left fold of a binary body (B,A)->B along
                    // each row's list. The accumulator-carrying list eliminator (Head is the 0-step
                    // case; the named monoid reductions like ReduceSum are the SIMD fast paths). Body
                    // runs once per ROUND, vectorized across rows: round t folds in every row's t-th
                    // element in lockstep, so #invocations = the longest row, not the element count.
    FoldScan(Box<Graph<L>>), // (T, List<A>) -> (T, List<R>)  the mapAccumL / Mealy-machine fold: a body
                    // (T,A)->(T,R) threads a STATE T and emits an output R per element; returns the final
                    // state AND the output stream. The unifying scan kernel: `scan` is sugar for this
                    // (body `(a,x)->b` becomes `(a,x)->(b,b)`, take field 1). Expresses stateful maps a
                    // plain scan can't (running deltas, indexing, RLE). `Fold` is kept separate — the
                    // R=Unit specialization, ~3x cheaper than FoldScan (no output pair, no recording).
    CapList,        // capture: (X, List<Y>) -> List<(X,Y)> — pair a context with every element
                    // (né Broadcast); the list-side closure capture.

    // ---- structural isos: de-/re-structure between nestings the layout already stores; linear
    // bounds work at most, no per-element compute. Three pairs: List⊗Prod (Transpose/Zip),
    // List⊗List (Flatten/Slices), List⊗Sum (Unweave/Weave).
    Transpose,      // List<(X,Y,..)> -> (List<X>, List<Y>, ..)
    Zip,            // (List<X>, List<Y>, ..) -> List<(X,Y,..)>  Transpose's inverse; bounds must
                    // agree (asserted). A pure rewrap — no data moves. PARTIAL: panics on differing
                    // bounds; surface-usable once the size pass proves the bounds agree (else TryZip).
    TryZip,         // (List<X>, List<Y>) -> Sum{Err:(List<X>,List<Y>) | Ok:List<(X,Y)>}  TOTAL Zip,
                    // per row: a row whose two inner lists have equal length zips into Ok; a mismatched
                    // row lands in Err carrying its two inner lists. (Pair-only for now; N-ary later.)
    Flatten,        // List<List<X>> -> (List<(lo,hi)>, List<X>)  destructure: ranges + flat values
    Slices,         // (List<(lo,hi)>, haystack:List<T>) -> List<List<T>>  materialize each range —
                    // Flatten's inverse and the range form of Gather.
    Unweave,        // List<Sum{A|B|..}> -> (tags:List<U64>, List<A>, List<B>, ..)  destructure a
                    // sum column: the tag list plus each lane re-sliced per outer row. Lanes are
                    // already stored packed in row order, so only bounds are computed; ⊥ lanes
                    // are rejected (a standalone List<⊥> has no shape).
    Weave,          // (tags:List<U64>, List<A>, List<B>, ..) -> List<Sum{A|B|..}>  Unweave's
                    // inverse: interleave the lanes per the tags. Per-row tag counts must match
                    // each lane's row length (asserted); the lanes' flat storage is the Sum's.

    // ---- fused forms & producers -------------------------------------------------------------
    Lit(Value),     // a constant element, filled to the input's length (anchored)
    Cast(u32),      // leaf -> leaf  re-width to N bits (low bytes / zero-pad), kind-blind
    Filter,         // (List<X>, List<U64-mask>) -> List<X>  keep mask-nonzero elements in one
                    // pass (the kernel expansion is zip; map(branch); unweave; field — see the law)
    // point access — fetch a haystack element by index. The atom is the SCALAR `Get` (one O(1)
    // lookup per row); `Gather` is its vectorization (the index arrives as a list), and `head` is
    // sugar for `Get 0` (an empty row is `Oob 0`, so a TOTAL head needs no non-emptiness proof).
    // Each comes in a `_uns` (assert in-bounds, panic) and a `_try` (total, Oob lane) tier; the plain
    // checked tier (`get`/`gather`, a proven bound) is reserved for later.
    Get,            // (idx:U64, haystack:List<T>) -> T  scalar point access — one index per row into
                    // that row's list. PARTIAL (panics out of bounds): the unchecked fast path.
    GetTry,         // (idx:U64, haystack:List<T>) -> Sum{Oob:U64 | Found:T}  TOTAL scalar access: an
                    // out-of-bounds index lands in Oob carrying the bad index, in-bounds in Found
                    // (the ParseU64 discipline — failure is a committed lane, never a panic).
    Gather,         // (idx:List<U64>, haystack:List<T>) -> List<T>  the vector form: a list of indices.
                    // The engine primitive surfaced; chains compose in-language —
                    // gather(gather(v,i),j) = gather(v, gather(i,j)), so index math stays index math.
                    // PARTIAL (panics out of bounds): the unchecked fast path. `Get` is the 1-index case.
    GatherTry,      // (idx:List<U64>, haystack:List<T>) -> List<Sum{Oob:U64 | Found:T}>  TOTAL vector
                    // access — `GetTry` lifted over a list of indices. A bounds-proof pass demotes
                    // `GatherTry` to `Gather` + `inject Found` when the Oob lane is provably empty.
    Iota,           // U64 -> List<U64>  per row [0,1,…,n-1] — a List-introducer / data generator
    Unit,           // X -> Unit  forget the payload, keep the length — how a column becomes the `None`
                    // lane of `Option = Sum{Unit | T}` (e.g. `branch 2 map_variant 1 (x -> x unit)`).
    Select,         // (mask:U64, then:T, else:T) -> T  branchless per-row blend (the SIMD bitselect):
                    // row i takes `then` if mask[i] != 0 else `else`. The dual of `Branch(2)`+`Weave` —
                    // Branch avoids computing the unused side, Select avoids the partition; cheap bodies
                    // favour Select. Shape-generic: it IS `gather_lanes([else, then], mask, identity)`.
}

impl<L: OpLike> Op<L> {
    pub(crate) fn eval(&self, input: Value) -> Value {
        match self {
            Op::Lit(v) => fill(v, input.len()),

            Op::Field(i) => {
                let mut cols = input.into_prod("Field");
                cols.swap_remove(*i)
            }

            Op::Transpose => {
                let (bounds, vals) = input.into_list("Transpose");
                let cols = vals.into_prod("Transpose values");
                Value::Prod(
                    cols.into_iter()
                        .map(|c| Value::List(bounds.clone(), Box::new(c)))
                        .collect(),
                )
            }

            // Transpose's inverse: parallel lists with identical bounds rewrap as one list of
            // products. No data moves — the columns simply become the product's fields.
            Op::Zip => {
                let cols = input.into_prod("Zip");
                let mut bounds: Option<Vec<usize>> = None;
                let mut inner = Vec::with_capacity(cols.len());
                for c in cols {
                    let (b, v) = c.into_list("Zip column");
                    match &bounds {
                        None => bounds = Some(b),
                        Some(prev) => assert_eq!(prev, &b, "Zip: column bounds differ"),
                    }
                    inner.push(v);
                }
                Value::List(bounds.expect("Zip: empty product"), Box::new(Value::Prod(inner)))
            }

            // total Zip (pair): each SEQ row whose two inner lists have equal length zips into the Ok
            // lane (a List<(X,Y)>); a length-mismatched row goes to Err carrying its two inner lists.
            // Failure is per-row and a committed lane — never the whole-column panic Zip would raise.
            Op::TryZip => {
                let (lx, ly) = input.into_pair("TryZip");
                let (bx, vx) = lx.into_list("TryZip lhs");
                let (by, vy) = ly.into_list("TryZip rhs");
                assert_eq!(bx.len(), by.len(), "TryZip: row count"); // outer agreement (scope length)
                let mut tags = Vec::with_capacity(bx.len());
                let (mut ok_outer, mut ok_x, mut ok_y) = (Vec::new(), Vec::new(), Vec::new());
                let (mut ex_outer, mut ex_idx) = (Vec::new(), Vec::new());
                let (mut ey_outer, mut ey_idx) = (Vec::new(), Vec::new());
                let (mut sx, mut sy) = (0, 0);
                let (mut oka, mut exa, mut eya) = (0, 0, 0);
                for r in 0..bx.len() {
                    let (ex, ey) = (bx[r], by[r]);
                    let (lenx, leny) = (ex - sx, ey - sy);
                    if lenx == leny {
                        tags.push(1); // Ok
                        for k in 0..lenx {
                            ok_x.push(sx + k);
                            ok_y.push(sy + k);
                        }
                        oka += lenx;
                        ok_outer.push(oka);
                    } else {
                        tags.push(0); // Err
                        for k in 0..lenx {
                            ex_idx.push(sx + k);
                        }
                        for k in 0..leny {
                            ey_idx.push(sy + k);
                        }
                        exa += lenx;
                        ex_outer.push(exa);
                        eya += leny;
                        ey_outer.push(eya);
                    }
                    sx = ex;
                    sy = ey;
                }
                let ok = Value::List(
                    ok_outer,
                    Box::new(Value::Prod(vec![gather(&vx, &ok_x), gather(&vy, &ok_y)])),
                );
                let err = Value::Prod(vec![
                    Value::List(ex_outer, Box::new(gather(&vx, &ex_idx))),
                    Value::List(ey_outer, Box::new(gather(&vy, &ey_idx))),
                ]);
                Value::sum(tags, vec![err, ok]) // lane 0 = Err, lane 1 = Ok
            }

            // destructure a sum column: the tag list plus each lane re-sliced per outer row. A
            // lane's elements are stored packed in row order, so each lane keeps its values and
            // only gains bounds (per-row cumulative tag counts) — no data moves but the tag widen.
            Op::Unweave => {
                let (bounds, vals) = input.into_list("Unweave");
                let (tags, _offset, lanes) = vals.into_sum("Unweave");
                let mut lane_bounds = vec![Vec::with_capacity(bounds.len()); lanes.len()];
                let mut counts = vec![0usize; lanes.len()];
                let mut start = 0;
                for &end in &bounds {
                    for &t in &tags[start..end] {
                        counts[t] += 1;
                    }
                    for (lb, &c) in lane_bounds.iter_mut().zip(&counts) {
                        lb.push(c);
                    }
                    start = end;
                }
                let tag_list =
                    Value::List(bounds, Box::new(Value::u64(tags.iter().map(|&t| t as u64).collect())));
                let mut out = vec![tag_list];
                for (o, lb) in lanes.into_iter().zip(lane_bounds) {
                    let lane = o.expect("Unweave: ⊥ lane (judge rejects these)");
                    out.push(Value::List(lb, Box::new(lane)));
                }
                Value::Prod(out)
            }

            // Unweave's inverse: interleave the lanes per the tag list. The lanes' flat row-major
            // storage IS the Sum's lane storage, so after validating per-row tag counts against
            // each lane's row lengths, the Sum is built without moving lane data.
            Op::Weave => {
                let mut cols = input.into_prod("Weave");
                let rest = cols.split_off(1);
                let (tb, tv) = cols.pop().expect("Weave: empty product").into_list("Weave tags");
                let tags = tv.into_u64("Weave tags");
                let mut lanes = Vec::with_capacity(rest.len());
                let mut lane_bounds = Vec::with_capacity(rest.len());
                for l in rest {
                    let (b, v) = l.into_list("Weave lane");
                    assert_eq!(b.len(), tb.len(), "Weave: lane/tags row count");
                    lane_bounds.push(b);
                    lanes.push(v);
                }
                let mut counts = vec![0usize; lanes.len()];
                let mut start = 0;
                for (r, &end) in tb.iter().enumerate() {
                    for &t in &tags[start..end] {
                        assert!((t as usize) < lanes.len(), "Weave: tag {t} out of range");
                        counts[t as usize] += 1;
                    }
                    for (t, (lb, &c)) in lane_bounds.iter().zip(&counts).enumerate() {
                        assert_eq!(lb[r], c, "Weave: row {r} lane {t} length/tag-count mismatch");
                    }
                    start = end;
                }
                let sum = Value::sum(tags.iter().map(|&t| t as usize).collect(), lanes);
                Value::List(tb, Box::new(sum))
            }

            Op::CapList => {
                let (x, list) = input.into_pair("CapList");
                let (bounds, y) = list.into_list("CapList list");
                let idx = owner_ids(&bounds);
                Value::List(bounds, Box::new(Value::Prod(vec![gather(&x, &idx), y])))
            }

            // capture into a sum: row i's context pairs with its payload inside variant tags[i].
            // Lane t's rows are the tag-t rows in tag order, so gathering the context at those
            // positions aligns with the carried within-variant offsets.
            Op::CapSum => {
                let (x, s) = input.into_pair("CapSum");
                let (tags, _offset, lanes) = s.into_sum("CapSum sum");
                assert_eq!(x.len(), tags.len(), "CapSum: context/sum length");
                let mut per = vec![Vec::new(); lanes.len()];
                for (i, &t) in tags.iter().enumerate() {
                    per[t].push(i);
                }
                let new = lanes
                    .into_iter()
                    .zip(&per)
                    .map(|(o, rows)| o.map(|lane| Value::Prod(vec![gather(&x, rows), lane])))
                    .collect();
                Value::sum_opt(tags, new)
            }

            Op::Cast(bits) => match input {
                Value::Prim(p) => Value::Prim(p.cast(*bits)),
                _ => panic!("Cast: expected a leaf"),
            },

            Op::Filter => {
                let (data, mask) = input.into_pair("Filter");
                let (bounds, vals) = data.into_list("Filter data");
                let (mb, mv) = mask.into_list("Filter mask");
                assert_eq!(bounds, mb, "Filter: data/mask bounds differ");
                let m = mv.into_u64("Filter mask");
                let (idx, nb) = filter_mask(&bounds, &m);
                Value::List(nb, Box::new(gather(&vals, &idx)))
            }

            // N-way partition: the discriminant `tags` routes each row of `data` to its variant. The
            // tags ARE the sum's tag column; each variant gathers its rows in order (so the implicit
            // within-variant offset matches `Value::sum`).
            Op::Branch(n) => {
                let (data, tags_v) = input.into_pair("Branch");
                let tags = tags_v.into_u64("Branch tags");
                assert_eq!(data.len(), tags.len(), "Branch: payload/discriminant length");
                let mut groups: Vec<Vec<usize>> = vec![Vec::new(); *n];
                for (i, &t) in tags.iter().enumerate() {
                    assert!((t as usize) < *n, "Branch: tag {t} out of range (n={n})");
                    groups[t as usize].push(i);
                }
                let variants = groups.iter().map(|idx| gather(&data, idx)).collect();
                Value::sum(tags.iter().map(|&t| t as usize).collect(), variants)
            }

            // total Branch: tag t<n routes to lane t+1 (payload), tag>=n to lane 0 (Oob, carrying the
            // bad tag). No panic — out-of-range is a committed lane, exactly like Index's Oob.
            Op::TryBranch(n) => {
                let (data, tags_v) = input.into_pair("TryBranch");
                let tags = tags_v.into_u64("TryBranch tags");
                assert_eq!(data.len(), tags.len(), "TryBranch: payload/discriminant length");
                let mut groups: Vec<Vec<usize>> = vec![Vec::new(); *n + 1];
                let mut oob = Vec::new();
                let mut lanes = Vec::with_capacity(tags.len());
                for (i, &t) in tags.iter().enumerate() {
                    if (t as usize) < *n {
                        lanes.push(t as usize + 1); // payload lanes are 1..=n
                        groups[t as usize + 1].push(i);
                    } else {
                        lanes.push(0); // the Oob lane
                        groups[0].push(i);
                        oob.push(t);
                    }
                }
                let mut variants = vec![Value::u64(oob)]; // lane 0 = the out-of-range tags
                variants.extend(groups[1..].iter().map(|idx| gather(&data, idx)));
                Value::sum(lanes, variants)
            }

            Op::Unwrap => {
                // each row's payload, read straight from its variant by the carried within-offset —
                // the fused inverse of `Inject` (no `concat(variants)` temporary).
                let (tags, offset, variants) = input.into_sum("Unwrap");
                let refs: Vec<Option<&Value>> = variants.iter().map(|o| o.as_ref()).collect();
                gather_lanes(&refs, &tags, &offset)
            }

            // sum introduction: every row goes to variant `tag` (a constant tag run), the
            // payload column fills that lane, the others are zero-row. The unary dual of `tuple`.
            Op::Inject(tag, arity) => {
                let n = input.len();
                let mut variants = vec![None; *arity];
                variants[*tag] = Some(input);
                Value::sum_opt(vec![*tag; n], variants)
            }

            Op::MapList(body) => {
                let (bounds, inner) = input.into_list("MapList");
                Value::List(bounds, Box::new(eval_graph(body, inner)))
            }

            // seeded left fold, vectorized across rows. `acc` is a column of one accumulator per row
            // (starts as the seed). Round t pairs every still-long-enough row's acc with its t-th
            // element, runs the body once over those active rows, scatters the results back. Rounds =
            // the longest row; empty rows never become active, so it is total (empty list -> seed).
            Op::Fold(body) => {
                let (seed, list) = input.into_pair("Fold");
                let (bounds, vals) = list.into_list("Fold list");
                let mut acc = seed;
                let mut active = init_active(&bounds);
                let mut t = 0;
                while !active.is_empty() {
                    let rows: Vec<usize> = active.iter().map(|&(r, _, _)| r).collect();
                    let elem: Vec<usize> = active.iter().map(|&(_, s, _)| s + t).collect();
                    let acc_active = gather(&acc, &rows);
                    let elt = gather(&vals, &elem);
                    let updated = eval_graph(body, Value::Prod(vec![acc_active, elt]));
                    assert_eq!(updated.len(), rows.len(), "Fold body changed the row count");
                    acc = scatter(acc, &rows, updated);
                    t += 1;
                    active.retain(|&(_, _, len)| len > t); // rows that just ran out drop here
                }
                acc
            }

            // mapAccumL: the body returns a PAIR (new state, output R). We thread field 0
            // (the state) into `acc` and record field 1 (R) into the chunks; return (final state, [R]).
            Op::FoldScan(body) => {
                let (seed, list) = input.into_pair("FoldScan");
                let (bounds, vals) = list.into_list("FoldScan list");
                let total = vals.len();
                let mut acc = seed;
                let mut chunks: Vec<Value> = Vec::new();
                let (mut tags, mut off) = (vec![0usize; total], vec![0usize; total]);
                let mut active = init_active(&bounds);
                let mut t = 0;
                while !active.is_empty() {
                    let rows: Vec<usize> = active.iter().map(|&(r, _, _)| r).collect();
                    let elem: Vec<usize> = active.iter().map(|&(_, s, _)| s + t).collect();
                    let acc_active = gather(&acc, &rows);
                    let elt = gather(&vals, &elem);
                    let (new_state, r) =
                        eval_graph(body, Value::Prod(vec![acc_active, elt])).into_pair("FoldScan body");
                    assert_eq!(new_state.len(), rows.len(), "FoldScan body changed the row count");
                    for (slot, &pos) in elem.iter().enumerate() {
                        tags[pos] = chunks.len();
                        off[pos] = slot;
                    }
                    acc = scatter(acc, &rows, new_state);
                    chunks.push(r);
                    t += 1;
                    active.retain(|&(_, _, len)| len > t);
                }
                // empty (no rounds): an empty R-shaped column, obtained by running the body on zero rows
                // (R may differ from the state, so we can't reuse `acc`). Else stitch the recorded chunks.
                let out_vals = if chunks.is_empty() {
                    let z = eval_graph(body, Value::Prod(vec![gather(&acc, &[]), gather(&vals, &[])]));
                    z.into_pair("FoldScan body").1
                } else {
                    let refs: Vec<Option<&Value>> = chunks.iter().map(Some).collect();
                    gather_lanes(&refs, &tags, &off)
                };
                Value::Prod(vec![acc, Value::List(bounds, Box::new(out_vals))])
            }

            Op::MapSum(arms) => {
                let (tags, _offset, mut variants) = input.into_sum("MapSum");
                for (k, body) in arms {
                    // take the lane so the body's `Input` owns it (refcount 1 ⇒ in-place). A ⊥ lane
                    // (`None`) has no rows and a deferred shape, so the body can't run on it — leave it
                    // ⊥ (the judge defers its type the same way).
                    let Some(lane) = variants[*k].take() else { continue };
                    let lane_len = lane.len();
                    let res = eval_graph(body, lane);
                    assert_eq!(res.len(), lane_len, "MapSum changed a variant's length");
                    variants[*k] = Some(res);
                }
                Value::sum_opt(tags, variants)
            }

            // materialize: replace each (lo,hi) range with the haystack-row slice it
            // names. List<(lo,hi)> -> List<List<T>>; reuses `gather`. A list-introducer.
            Op::Slices => {
                let (lohi, haystack) = input.into_pair("Slices");
                let (lb, lvals) = lohi.into_list("Slices ranges");
                let (hb, hvals) = haystack.into_list("Slices haystack");
                let (lo, hi) = lvals.into_pair("Slices lo_hi");
                let (lo_c, hi_c) = (lo.into_u64("Slices lo"), hi.into_u64("Slices hi"));
                assert_eq!(lb.len(), hb.len(), "Slices: row count");
                let (idx, inner_bounds) = expand_ranges(&lb, &lo_c, &hi_c, &hb);
                let inner = Value::List(inner_bounds, Box::new(gather(&hvals, &idx)));
                Value::List(lb, Box::new(inner))
            }

            // scalar point access: one index per row into that row's haystack list. Output is a
            // bare T column (the list stratum is DROPPED — this is the eliminator). Partial: an
            // out-of-bounds index is a data-dependent panic, like Gather's bounds check.
            Op::Get => {
                let (idx, haystack) = input.into_pair("Get");
                let idxs = idx.into_u64("Get index");
                let (hb, hvals) = haystack.into_list("Get haystack");
                assert_eq!(idxs.len(), hb.len(), "Get: index/haystack row count");
                let mut abs = Vec::with_capacity(idxs.len());
                let mut hs = 0;
                for (r, &he) in hb.iter().enumerate() {
                    let x = idxs[r];
                    assert!((x as usize) < he - hs, "Get: index {x} out of row {r}'s bounds");
                    abs.push(hs + x as usize);
                    hs = he;
                }
                gather(&hvals, &abs)
            }

            // total scalar access: each index either names its row's element (Found) or is out of
            // bounds (Oob, carrying the bad index). The empty-row case is just Oob 0 — which is why
            // `head = GetTry 0` is total with no non-emptiness proof. Output is a bare Sum column.
            Op::GetTry => {
                let (idx, haystack) = input.into_pair("GetTry");
                let idxs = idx.into_u64("GetTry index");
                let (hb, hvals) = haystack.into_list("GetTry haystack");
                assert_eq!(idxs.len(), hb.len(), "GetTry: index/haystack row count");
                let mut tags = Vec::with_capacity(idxs.len());
                let mut abs = Vec::new(); // absolute haystack positions of the Found elements (lane 1)
                let mut oob = Vec::new(); // the out-of-bounds index values (lane 0)
                let mut hs = 0;
                for (r, &he) in hb.iter().enumerate() {
                    let x = idxs[r];
                    if (x as usize) < he - hs {
                        tags.push(1);
                        abs.push(hs + x as usize);
                    } else {
                        tags.push(0);
                        oob.push(x);
                    }
                    hs = he;
                }
                Value::sum(tags, vec![Value::u64(oob), gather(&hvals, &abs)])
            }

            // vector point gather: each row-relative index becomes the haystack element it names.
            // Output bounds are the index list's bounds (the indices decide the cardinality).
            Op::Gather => {
                let (idx, haystack) = input.into_pair("Gather");
                let (ib, ivals) = idx.into_list("Gather indices");
                let (hb, hvals) = haystack.into_list("Gather haystack");
                assert_eq!(ib.len(), hb.len(), "Gather: indices/haystack row count");
                let abs = resolve_indices(&ib, &ivals.into_u64("Gather indices"), &hb);
                Value::List(ib, Box::new(gather(&hvals, &abs)))
            }

            // total vector access: each index either names a haystack-row element (Found) or is out of
            // that row's bounds (Oob, carrying the bad index). The per-element test is branchless (a
            // comparison to a u64); only the routing into the two lanes is data-dependent. Output is a
            // list (the index list's bounds) of Sum{Oob:U64 | Found:T}.
            Op::GatherTry => {
                let (idx, haystack) = input.into_pair("GatherTry");
                let (ib, ivals) = idx.into_list("GatherTry indices");
                let (hb, hvals) = haystack.into_list("GatherTry haystack");
                assert_eq!(ib.len(), hb.len(), "GatherTry: indices/haystack row count");
                let idxs = ivals.into_u64("GatherTry indices");
                let mut tags = Vec::with_capacity(idxs.len());
                let mut abs = Vec::new(); // absolute haystack positions of the Found elements (lane 1)
                let mut oob = Vec::new(); // the out-of-bounds index values (lane 0)
                let (mut is, mut hs) = (0, 0);
                for r in 0..ib.len() {
                    let (ie, he) = (ib[r], hb[r]);
                    let rowlen = he - hs;
                    for &x in &idxs[is..ie] {
                        if (x as usize) < rowlen {
                            tags.push(1);
                            abs.push(hs + x as usize);
                        } else {
                            tags.push(0);
                            oob.push(x);
                        }
                    }
                    is = ie;
                    hs = he;
                }
                let sum = Value::sum(tags, vec![Value::u64(oob), gather(&hvals, &abs)]);
                Value::List(ib, Box::new(sum))
            }

            // DESTRUCTURE one list layer: return the per-inner-list ranges (relative to
            // each top row's flattened span) AND the one-level-flattened values. Both
            // outputs are lists at the SAME top stratum, so they bundle as a Prod, and
            // `Slices` is the exact inverse — hence MapList(MapList(b)) == Flatten; b; Slices.
            Op::Flatten => {
                let (ob, inner) = input.into_list("Flatten");
                let (ib, vals) = inner.into_list("Flatten inner");
                let new_ob: Vec<usize> =
                    ob.iter().map(|&e| if e == 0 { 0 } else { ib[e - 1] }).collect();
                let mut lo_c = Vec::with_capacity(ib.len());
                let mut hi_c = Vec::with_capacity(ib.len());
                let mut prev = 0;
                for &e in &ob {
                    let base = if prev == 0 { 0 } else { ib[prev - 1] }; // top row's flat start
                    for kk in prev..e {
                        let g_lo = if kk == 0 { 0 } else { ib[kk - 1] };
                        lo_c.push((g_lo - base) as u64);
                        hi_c.push((ib[kk] - base) as u64);
                    }
                    prev = e;
                }
                let ranges = Value::List(
                    ob,
                    Box::new(Value::Prod(vec![Value::u64(lo_c), Value::u64(hi_c)])),
                );
                let flat = Value::List(new_ob, Box::new(vals));
                Value::Prod(vec![ranges, flat])
            }

            // wrap each element in its own length-1 list (the list-monad unit). Values
            // unchanged; bounds become [1,2,..,n]. `Flatten` of an `Enlist` is identity.
            Op::Enlist => {
                let n = input.len();
                Value::List((1..=n).collect(), Box::new(input))
            }

            // generate a range per row: element n_i becomes the list [0,1,…,n_i-1]. Cardinality
            // lands inside the new List (SEQ stays 1:1). Lets a program build its own input data.
            Op::Iota => {
                let ns = input.into_u64("Iota");
                let mut bounds = Vec::with_capacity(ns.len());
                let mut vals = Vec::new();
                let mut end = 0usize;
                for &n in &ns {
                    vals.extend(0..n);
                    end += n as usize;
                    bounds.push(end);
                }
                Value::List(bounds, Box::new(Value::u64(vals)))
            }

            // forget the payload, keep the row count — the constructor for unit/`None` columns.
            Op::Unit => Value::Unit(input.len()),

            // branchless blend: a two-source `gather_lanes` reading each row's own position from the
            // lane its mask selects (`then` when nonzero). Both operands are full columns, so the
            // identity offset reads row i from row i — the whole "computed both sides, pick per lane".
            Op::Select => {
                let mut cols = input.into_prod("Select");
                assert_eq!(cols.len(), 3, "Select: expected (mask, then, else)");
                let els = cols.pop().unwrap();
                let then = cols.pop().unwrap();
                let mask = cols.pop().unwrap().into_u64("Select mask");
                let tags: Vec<usize> = mask.iter().map(|&m| (m != 0) as usize).collect();
                let off: Vec<usize> = (0..tags.len()).collect();
                gather_lanes(&[Some(&els), Some(&then)], &tags, &off)
            }
        }
    }

    /// the type-level shadow of `eval`: given the input's shape, return the output's
    /// shape (or a structural error). Pattern-matches the input exactly like `eval`,
    /// so adding an op means one rule here and one in `eval`. `Input`/`Tuple` are
    /// handled by `graph::shape_of`, the analogue of `eval_graph`.
    pub(crate) fn judge(&self, input: &Shape) -> Result<Shape, String> {
        use Shape::*;
        let err = |what: &str| Err(format!("{what}, got {input}"));
        Ok(match self {
            Op::Lit(v) => shape_of_value(v),

            Op::Field(i) => match input {
                Prod(ts) if *i < ts.len() => ts[*i].clone(),
                _ => return err(&format!("Field({i}) expects a product with > {i} fields")),
            },

            Op::Transpose => match input {
                List(inner) => match inner.as_ref() {
                    Prod(ts) => Prod(ts.iter().map(|t| List(Box::new(t.clone()))).collect()),
                    _ => return err("Transpose expects List<product>"),
                },
                _ => return err("Transpose expects a list"),
            },

            Op::Zip => match input {
                Prod(ts) if !ts.is_empty() => {
                    let inners = ts
                        .iter()
                        .map(|t| match t {
                            List(x) => Ok((**x).clone()),
                            _ => Err(format!("Zip expects a product of lists, got {input}")),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    List(Box::new(Prod(inners)))
                }
                _ => return err("Zip expects a nonempty product of lists"),
            },

            // total Zip (pair): Sum{ Err: (List<X>, List<Y>) | Ok: List<(X,Y)> }.
            Op::TryZip => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(x), List(y)) => Sum(vec![
                        Some(Prod(vec![List(x.clone()), List(y.clone())])),
                        Some(List(Box::new(Prod(vec![(**x).clone(), (**y).clone()])))),
                    ]),
                    _ => return err("TryZip expects (List<X>, List<Y>)"),
                },
                _ => return err("TryZip expects a pair of lists"),
            },

            // every lane must be committed: a ⊥ lane would need a standalone List<⊥>, and ⊥ lives
            // only inside Sum shapes. (Merge the sum first if a lane is uncommitted.)
            Op::Unweave => match input {
                List(inner) => match inner.as_ref() {
                    Sum(ls) => {
                        let mut out = vec![List(Box::new(Prim(64)))];
                        for (k, o) in ls.iter().enumerate() {
                            match o {
                                Some(s) => out.push(List(Box::new(s.clone()))),
                                None => return err(&format!("Unweave: lane {k} is ⊥ (uncommitted)")),
                            }
                        }
                        Prod(out)
                    }
                    _ => return err("Unweave expects List<Sum>"),
                },
                _ => return err("Unweave expects a list"),
            },

            Op::Weave => match input {
                Prod(ts) if ts.len() >= 2 => {
                    if !matches!(&ts[0], List(t) if **t == Prim(64)) {
                        return err("Weave expects (List<U64> tags, List<A>, ..)");
                    }
                    // the tag column is u8-backed, so the lane count must fit (eval's
                    // `Value::sum` asserts the same bound — keep judge and eval in agreement).
                    if ts.len() - 1 > 256 {
                        return err("Weave: arity exceeds the u8 tag width");
                    }
                    let lanes = ts[1..]
                        .iter()
                        .map(|t| match t {
                            List(x) => Ok(Some((**x).clone())),
                            _ => Err(format!("Weave expects lane lists, got {input}")),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    List(Box::new(Sum(lanes)))
                }
                _ => return err("Weave expects (List<U64> tags, List<A>, ..)"),
            },

            Op::CapList => match input {
                Prod(ts) if ts.len() == 2 => match &ts[1] {
                    List(y) => List(Box::new(Prod(vec![ts[0].clone(), (**y).clone()]))),
                    _ => return err("CapList expects (X, List<Y>)"),
                },
                _ => return err("CapList expects a pair"),
            },

            // the context shape distributes into each committed lane; a ⊥ lane stays ⊥ (no rows,
            // nothing to pair — the same deferral as MapSum's).
            Op::CapSum => match input {
                Prod(ts) if ts.len() == 2 => match &ts[1] {
                    Sum(ls) => Sum(ls
                        .iter()
                        .map(|o| o.as_ref().map(|l| Prod(vec![ts[0].clone(), l.clone()])))
                        .collect()),
                    _ => return err("CapSum expects (X, Sum)"),
                },
                _ => return err("CapSum expects a pair"),
            },

            Op::Branch(n) => match input {
                Prod(ts) if ts.len() == 2 && ts[1] == Prim(64) => {
                    // the tag column is u8-backed (same bound as Weave's; eval's `Value::sum` asserts it).
                    if *n > 256 {
                        return err(&format!("Branch: arity {n} exceeds the u8 tag width"));
                    }
                    Sum(vec![Some(ts[0].clone()); *n])
                }
                _ => return err("Branch expects (X, U64-tags)"),
            },

            // total Branch: an extra leading Oob:U64 lane, then the n payload lanes (n+1 total, so the
            // u8 tag bound is n+1).
            Op::TryBranch(n) => match input {
                Prod(ts) if ts.len() == 2 && ts[1] == Prim(64) => {
                    if *n + 1 > 256 {
                        return err(&format!("TryBranch: arity {n}+1 exceeds the u8 tag width"));
                    }
                    let mut lanes = vec![Some(Prim(64))];
                    lanes.extend(std::iter::repeat_n(Some(ts[0].clone()), *n));
                    Sum(lanes)
                }
                _ => return err("TryBranch expects (X, U64-tags)"),
            },

            // only the widths the `prim!` macro generates exist; any other would judge fine and
            // panic at eval — the typer owns the rejection.
            Op::Cast(bits) => match input {
                Prim(_) if matches!(*bits, 8 | 16 | 32 | 64) => Prim(*bits),
                Prim(_) => return err(&format!("Cast: unsupported width {bits}")),
                _ => return err("Cast expects a leaf"),
            },

            Op::Filter => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(x), List(m)) if **m == Prim(64) => List(x.clone()),
                    _ => return err("Filter expects (List<X>, List<U64>)"),
                },
                _ => return err("Filter expects a pair"),
            },

            Op::Slices => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(lh), List(t)) if **lh == Prod(vec![Prim(64), Prim(64)]) => {
                        List(Box::new(List(t.clone())))
                    }
                    _ => return err("Slices expects (List<(U64,U64)>, List<T>)"),
                },
                _ => return err("Slices expects a pair"),
            },

            // scalar access: (U64, List<T>) -> T — the index is a leaf column, the list stratum drops.
            Op::Get => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (Prim(64), List(t)) => (**t).clone(),
                    _ => return err("Get expects (U64, List<T>)"),
                },
                _ => return err("Get expects a pair"),
            },

            // total scalar access: (U64, List<T>) -> Sum{Oob: U64 | Found: T} — Oob carries the index.
            Op::GetTry => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (Prim(64), List(t)) => Sum(vec![Some(Prim(64)), Some((**t).clone())]),
                    _ => return err("GetTry expects (U64, List<T>)"),
                },
                _ => return err("GetTry expects a pair"),
            },

            Op::Gather => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(i), List(t)) if **i == Prim(64) => List(t.clone()),
                    _ => return err("Gather expects (List<U64>, List<T>)"),
                },
                _ => return err("Gather expects a pair"),
            },

            // total vector access: List<Sum{Oob: U64 | Found: T}> — the Oob lane carries the bad index.
            Op::GatherTry => match input {
                Prod(ts) if ts.len() == 2 => match (&ts[0], &ts[1]) {
                    (List(i), List(t)) if **i == Prim(64) => {
                        List(Box::new(Sum(vec![Some(Prim(64)), Some((**t).clone())])))
                    }
                    _ => return err("GatherTry expects (List<U64>, List<T>)"),
                },
                _ => return err("GatherTry expects a pair"),
            },

            Op::Flatten => match input {
                List(inner) => match inner.as_ref() {
                    List(x) => Prod(vec![List(Box::new(Prod(vec![Prim(64), Prim(64)]))), List(x.clone())]),
                    _ => return err("Flatten expects List<List<X>>"),
                },
                _ => return err("Flatten expects a list"),
            },

            Op::Enlist => List(Box::new(input.clone())),

            Op::Iota => match input {
                Prim(64) => List(Box::new(Prim(64))),
                _ => return err("Iota expects U64"),
            },

            Op::Unit => Unit, // any shape -> Unit

            // (U64 mask, T, T) -> T. The two branches join (a ⊥ lane adopts its sibling, as in Unwrap);
            // eval never reads a branch the mask doesn't select, so the join is total.
            Op::Select => match input {
                Prod(ts) if ts.len() == 3 && ts[0] == Prim(64) => join(&ts[1], &ts[2])?,
                _ => return err("Select expects (U64 mask, T, T)"),
            },

            // homogeneous up to `⊥`: fold the committed (`Some`) lanes by `join`, so `⊥` lanes adopt the
            // concrete payload and a genuine clash of two concrete payloads is the type error.
            Op::Unwrap => match input {
                Sum(ts) => {
                    let mut present = ts.iter().flatten();
                    let first = present
                        .next()
                        .ok_or_else(|| format!("Unwrap: sum has no committed variant, got {input}"))?
                        .clone();
                    present.try_fold(first, |a, t| join(&a, t))?
                }
                _ => return err("Unwrap expects a sum"),
            },

            // sum intro: the input lands in variant `tag`; the other `arity - 1` lanes are `⊥` (`None`,
            // shape uncommitted) and adopt their type wherever the sum is later merged.
            Op::Inject(tag, arity) => {
                if *tag >= *arity {
                    return err(&format!("Inject: tag {tag} out of range for arity {arity}"));
                }
                // u8 tag width, as in Branch/Weave — eval's `Value::sum_opt` asserts the same bound.
                if *arity > 256 {
                    return err(&format!("Inject: arity {arity} exceeds the u8 tag width"));
                }
                let mut variants = vec![None; *arity];
                variants[*tag] = Some(input.clone());
                Sum(variants)
            }

            Op::MapList(body) => match input {
                List(x) => List(Box::new(shape_of(body, x)?)),
                _ => return err("MapList expects a list"),
            },

            // (B, List<A>) -> B. The body, on (B, A), must again yield B (joined ⊥-tolerantly with the
            // seed shape). Heterogeneous: the accumulator B and element A need not match.
            Op::Fold(body) => match input {
                Prod(ts) if ts.len() == 2 => match &ts[1] {
                    List(a) => {
                        let body_out = shape_of(body, &Prod(vec![ts[0].clone(), (**a).clone()]))?;
                        join(&ts[0], &body_out)?
                    }
                    _ => return err("Fold expects (B, List<A>)"),
                },
                _ => return err("Fold expects a pair"),
            },

            // (T, List<A>) -> (T, List<R>). The body, on (T, A), must return (T', R) with T' joining the
            // state T; the result is the final state and the list of emitted R.
            Op::FoldScan(body) => match input {
                Prod(ts) if ts.len() == 2 => match &ts[1] {
                    List(a) => {
                        let out = shape_of(body, &Prod(vec![ts[0].clone(), (**a).clone()]))?;
                        match out {
                            Prod(os) if os.len() == 2 => {
                                let state = join(&ts[0], &os[0])?;
                                Prod(vec![state, List(Box::new(os[1].clone()))])
                            }
                            _ => return err("FoldScan body must return (T, R)"),
                        }
                    }
                    _ => return err("FoldScan expects (T, List<A>)"),
                },
                _ => return err("FoldScan expects a pair"),
            },

            Op::MapSum(arms) => match input {
                Sum(ts) => {
                    let mut out = ts.clone();
                    for (i, (k, body)) in arms.iter().enumerate() {
                        if *k >= ts.len() {
                            return err(&format!("MapSum: no variant {k}"));
                        }
                        // disjoint indices keep the arms independent (so they commute).
                        if arms[..i].iter().any(|(j, _)| j == k) {
                            return err(&format!("MapSum: duplicate variant {k}"));
                        }
                        // a ⊥ (`None`) lane can't be shaped through the body — defer it (stays ⊥).
                        out[*k] = match &ts[*k] {
                            Some(s) => Some(shape_of(body, s)?),
                            None => None,
                        };
                    }
                    Sum(out)
                }
                _ => return err("MapSum expects a sum"),
            },

        })
    }

    pub(crate) fn children(&self) -> Vec<&Graph<L>> {
        match self {
            Op::MapList(b) => vec![b],
            Op::Fold(b) | Op::FoldScan(b) => vec![b],
            Op::MapSum(arms) => arms.iter().map(|(_, b)| b).collect(),
            _ => Vec::new(),
        }
    }
}
