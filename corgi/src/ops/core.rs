//! The operator vocabulary. Every *semantic* op is a unary `T0 -> T1` evaluated by
//! `eval`; the input's type carries the shape requirement (no `arity()`). The two
//! structural nodes (`Input`, `Tuple`) are handled by the evaluator, not here.

use crate::engine::{
    blend, clone_ref, compress, fill, filter_mask, gather, gather_lanes, index_plan, owner_ids, take_ref,
    unwrap_leaves, Owners,
};
use crate::graph::{try_eval_graph, Graph, OpLike};
use crate::shape::{same, shape_of_value, Shape};
use crate::value::{Bounds, Prim, Rows, Tags, Value};
use std::sync::Arc;

/// overwrite `acc`'s rows at positions `active` (in order) with `new`'s rows — the scatter inverse of
/// `gather`, the accumulator update for `Fold`/`Scan`.
///
/// FIXED-WIDTH `acc` (a leaf, or a product of leaves) is mutated IN PLACE: each row has a constant
/// slot, so we overwrite only the `active` rows, reusing the buffer round-to-round — no per-round
/// allocation, no copying of the unchanged rows. VARIABLE-WIDTH `acc` (containing a `List`/`Sum`,
/// whose rows differ in size) can't be slot-overwritten, so it falls back to the rebuild via
/// two-source `gather_lanes` (correct, the prior behaviour; an uncommon accumulator shape).
pub(crate) fn scatter(mut acc: Value, active: &[usize], new: Value) -> Value {
    if fixed_width(&acc, &new) {
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
/// false once a `List` or `Sum` (variable-size rows) appears. A `Ref` row is a row number, so it
/// counts when `acc` and `new` reference ONE arena (a row of another arena can't be named in
/// `acc`'s). Checked WHOLE before any mutation, so the in-place pass below can't half-write a
/// value that turns out to be variable-width deeper down.
fn fixed_width(acc: &Value, new: &Value) -> bool {
    match (acc, new) {
        (Value::Prim(_), _) | (Value::Unit(_), _) => true, // a unit row is a (zero-byte) constant slot
        (Value::Prod(ca), Value::Prod(cn)) => ca.iter().zip(cn).all(|(a, n)| fixed_width(a, n)),
        (Value::Ref(pa, _), Value::Ref(pn, _)) => Arc::ptr_eq(pa, pn),
        _ => false,
    }
}

/// in-place scatter for a fixed-width `acc` (precondition: `fixed_width(acc, new)`), recursing
/// products to the leaves (and row numbers) where the actual write happens.
fn scatter_fixed(acc: &mut Value, active: &[usize], new: &Value) {
    match (acc, new) {
        (Value::Prim(d), Value::Prim(s)) => d.scatter_into(active, s),
        (Value::Prod(ca), Value::Prod(cn)) => {
            for (fa, fb) in ca.iter_mut().zip(cn) {
                scatter_fixed(fa, active, fb);
            }
        }
        (Value::Unit(_), Value::Unit(_)) => {} // no payload to overwrite (length is unchanged)
        (Value::Ref(_, sa), Value::Ref(_, sn)) => {
            let sa = Arc::make_mut(sa);
            for (slot, &r) in active.iter().enumerate() {
                sa[r] = sn[slot];
            }
        }
        _ => unreachable!("scatter_fixed: fixed_width guarantees Prim/Prod/Unit/Ref"),
    }
}

/// the lockstep work-list for `Fold`/`Scan`, maintained incrementally. `(row, start, len)` for each
/// non-empty row, in row order. A row stays until round `len`; after each round the caller `retain`s
/// `len > t`, so the per-round cost tracks the *active* rows, not all N (the win for ragged lengths —
/// vs rescanning every row's bounds each round, which was O(N·maxlen)). `row`/`start` give the gather
/// targets: row `r`'s round-`t` element is at flat index `start + t`.
pub(crate) fn init_active(bounds: &Bounds) -> Vec<(usize, usize, usize)> {
    let mut v = Vec::with_capacity(bounds.len());
    let mut start = 0;
    for (r, end) in bounds.ends().enumerate() {
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Op<L> {
    // ---- the kernel matrix ------------------------------------------------------------------
    // PROD — intro is the graph-structural `Tuple`. Products are transparent (fixed arity, no
    // witness column), so map is projection+rebuild and capture is `tuple` itself: no ops needed.
    Field(usize),   // elim:  (.., X_i, ..) -> X_i
    // SUM — witness: the tag column.
    Branch(usize),  // intro: (X, Int-tags) -> Sum{X × n}  data-driven demux: row i -> variant
                    //        tags[i]. (The boolean split is the idiom `Branch(2)` on a 0/1 mask.)
                    //        Total: a tag of n-1 or more goes to the last lane, so on a mask any
                    //        nonzero tag is "true", as `filter` reads masks.
    Inject(usize, Vec<Shape>), // intro: X -> Sum{..} — the constant-tag Branch: the input fills lane
                    //        `tag` of the declared sum shape (whose lane `tag` must be X); the other
                    //        lanes are empty columns of their declared shapes.
    Unwrap,         // elim:  homogeneous Sum -> payload
    MapSum(Vec<(usize, Graph<L>)>), // map: closed bodies on chosen variants; unlisted variants
                    // pass through. The Vec breaks the type recursion, so no Box. A variadic
                    // match — disjoint indices keep the arms independent (the optimizer relies
                    // on this; `eval` rejects duplicates).
    CapSum,         // capture: (X, Sum{A | B | ..}) -> Sum{(X,A) | (X,B) | ..} — distribute a
                    // context into each lane; lets a `match` arm see an outer
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
                    // (né Broadcast); the list-side closure capture. Copies X per element unless X
                    // is referenced — then it is one reference per element (a closure's `&ctx`).
    // REF — referenced list rows. The explicit by-reference/by-value pair: everything that moves
    // rows (`gather`, hence the capture family, `Lit`, and the merges) moves only row numbers on a Ref, and
    // nothing copies referenced rows except `Clone`. The readers `Gather`/`Find`/`Len`
    // accept a referenced list haystack; every other op on a Ref is the shape error "clone first".
    Ref,            // T -> T'           every top-level List<X> in T becomes Ref<List<X>> (through
                    //                   products and sums; O(rows), nothing copied)
    Clone,          // T -> T'           every Ref<List<X>> in T becomes List<X> (the ONLY copy of
                    //                   referenced rows; `clone` undoes `ref`)

    // ---- structural isos: de-/re-structure between nestings the layout already stores; linear
    // bounds work at most, no per-element compute. Pairs: List⊗Prod (Transpose/Zip), List⊗Sum
    // (Unweave/Weave); List⊗List's inverse of Flatten is the word `slices` (map(range); gather).
    Transpose,      // List<(X,Y,..)> -> (List<X>, List<Y>, ..)
    Zip,            // (List<X>, List<Y>, ..) -> List<(X,Y,..)>  Transpose's inverse. With agreeing
                    // bounds a pure rewrap — no data moves. Total but lossy: a row whose columns
                    // differ in length keeps the shortest (only then is data copied). `TryZip`, the
                    // surface `zip`, reports such a row as an error instead.
    Flatten,        // List<List<X>> -> (List<(lo,hi)>, List<X>)  destructure: ranges + flat values (its
                    // inverse is the word `slices`: map(range); gather)
    Unweave,        // List<Sum{A|B|..}> -> (tags:List<U8>, List<A>, List<B>, ..)  destructure a
                    // sum column: the tag list plus each lane re-sliced per outer row. Lanes are
                    // already stored packed in row order, so only bounds are computed.
    Weave,          // (tags:List<U8>, List<A>, List<B>, ..) -> List<Sum{A|B|..}>  Unweave's
                    // inverse: interleave the lanes per the tags. Per-row tag counts must match
                    // each lane's row length (asserted); the lanes' flat storage is the Sum's.

    // ---- fused forms & producers -------------------------------------------------------------
    Lit(Value),     // a constant element, filled to the input's length (anchored)
    Hash,           // X -> Int   stable content hash of each row (all 64 bits, read as an i64):
                    //        structural, by value, one pass
                    // (the boundary id function — see [`crate::hash`]). TOTAL over any shape.
    Filter,         // List<(Int-mask, X)> -> List<X>  keep the elements whose mask is nonzero, in one
                    // pass. Total by construction: a list of pairs can't disagree in length. (The
                    // kernel expansion is map(branch); unweave; field — see the law.)
    // point access — fetch haystack elements by position. `Gather` is the one fetch kernel: per row,
    // positions of any shape into that row's haystack, each integer leaf replaced by the element it
    // names. `get` is `Gather` on one position per row, `head` is `get 0`, and `slices` is the word
    // `map(range); gather` (see `frontend::ml`).
    Gather,         // (idx:P, haystack:List<T>) -> P[T]  P any shape whose leaves are integers (one
                    // position, a list of them, nested lists, products, sums), each a position in its
                    // top-level row; the result has P's structure with every leaf replaced by its
                    // element. Chains compose in-language — gather(gather(v,i),j) = gather(v,
                    // gather(i,j)), so index math stays index math.
                    // Total but lossy: a position outside its row reads the ZERO of the element's
                    // shape (zero bits, the empty list, a sum's lane 0). `TryGather` reports it as an
                    // error row instead; when positions are proven in range the two agree.
    Range,          // (lo:Int, hi:Int) -> List<Int>  per row [lo, hi), empty when lo >= hi: `iota` with
                    // a start. Total.
    GatherTry,      // (idx:List<Int>, haystack:List<T>) -> List<Sum{Found:T | Missing}>  TOTAL vector
                    // access: each index found or missing, the failure shape `Fail<T>` has (Ok first,
                    // the misses only counted). A bounds-proof pass demotes `GatherTry` to `Gather` +
                    // `inject 0` when the Missing lane is provably empty.
    Iota,           // Int -> List<Int>  per row [0,1,…,n-1] (empty for n <= 0) — a List-introducer / data generator
    Unit,           // X -> Unit  forget the payload, keep the length — how a column becomes the `None`
                    // lane of `Option = Sum{Unit | T}` (e.g. `branch 2 map_variant 1 (x -> x unit)`).
    Select,         // (mask:Int, then:T, else:T) -> T  branchless per-row blend (the SIMD bitselect):
                    // row i takes `then` if mask[i] != 0 else `else`. The dual of `Branch(2)`+`Weave` —
                    // Branch avoids computing the unused side, Select avoids the partition; cheap bodies
                    // favour Select. Shape-generic: it IS `gather_lanes([else, then], mask, identity)`.

    // the List monoid + measure (both 1:1 on the SEQ — cardinality stays inside the list).
    Append,         // (List<X>, List<X>) -> List<X>   row-wise concat: row i = a[i] ++ b[i] (the ⊕ of
                    // the list monoid, [] its unit). Same-shape elements, as in Zip.
    Len,            // List<X> -> Int                  each row's element count, read straight off the
                    // bounds (O(1) — the count the structure already holds, not a fold over the row).
    Cut,            // List<(Int-mask, X)> -> List<List<X>>  cut each row into pieces: a piece starts
                    // at each marked element and at the row's first. Only bounds are written; the
                    // values don't move. With `adjacent`'s marks, the pieces are runs of equal elements.
    Chunk(usize),   // List<X> -> List<List<X>>        partition each row into fixed `k`-wide sub-rows
                    // (the uniform inverse of Flatten): a pure re-partition — values don't move, the
                    // new inner list is a `Stride(k)`. The surface PRODUCER of wide strides, so a
                    // chunked record stream feeds the stride fast paths. Total but lossy: a row that
                    // doesn't divide by `k` drops its remainder (and only then are values copied).
                    // `TryChunk`, the surface `chunk`, reports such a row as an error instead.

    // ---- the failure family (see `ops::fail`) — failure as data: `Fail<T> = Sum{Ok:T | Err:Unit}`.
    // The `Try*` ops are the checked forms of the lossy kernels above (a row the kernel would read
    // zeros for, truncate or cut short lands in Err); `Lift`/`Squash`/`Hoist*` are the plumbing
    // `effect::lower_effects` inserts so pure programs run on the Ok lane. All ordinary ops, with one
    // eval each.
    TryGather,      // (idx:P, haystack:List<T>) -> Fail<P[T]>              per row all-or-nothing
    TryChunk(usize),// List<X> -> Fail<List<List<X>>>                       row length divides by k
    TryZip,         // (List<X>, List<Y>) -> Fail<List<(X,Y)>>              inner lengths agree
    Lift,           // X -> Fail<X>                                         every row Ok
    Squash,         // Fail<Fail<T>> -> Fail<T>                             the monad join
    HoistProd,      // (Fail<A>, Fail<B>, ..) -> Fail<(A, B, ..)>           errs if ANY field errs
    HoistList,      // List<Fail<T>> -> Fail<List<T>>                       errs if ANY element errs
    HoistSum(Vec<usize>), // Sum{.. Fail<A> ..} -> Fail<Sum{.. A ..}>       the listed lanes are Fail
    Try,            // the TRY marker: identity on values. The lowering reads it as "handled here" —
                    // the point past which a fallible column is ordinary data the program matches on.
}

impl<L: OpLike> Op<L> {
    /// run the op on a column. `Err` is a SHAPE error — the operand is not what the op consumes —
    /// which is what makes this the typer when run on zero rows (see `graph::shape_of`). No op fails
    /// on data: a value of the right shape always has a result.
    pub(crate) fn eval(&self, input: Value) -> Result<Value, String> {
        // the failure family lives in `ops::fail`; everything else is below.
        if super::fail::is_family(self) {
            return super::fail::eval(self, input);
        }
        Ok(match self {
            Op::Lit(v) => fill(v, input.len()),

            Op::Field(i) => {
                let mut cols = input.into_prod("Field")?;
                if *i >= cols.len() {
                    return Err(format!("Field({i}) expects a product with > {i} fields, got {}", Shape::Prod(cols.iter().map(shape_of_value).collect())));
                }
                cols.swap_remove(*i)
            }

            Op::Ref => take_ref(input),
            Op::Clone => clone_ref(input),

            Op::Transpose => {
                let (bounds, vals) = input.into_list("Transpose")?;
                let cols = vals.into_prod("Transpose values")?;
                Value::Prod(
                    cols.into_iter()
                        .map(|c| Value::List(bounds.clone(), Box::new(c)))
                        .collect(),
                )
            }

            // Transpose's inverse: parallel lists with identical bounds rewrap as one list of
            // products. No data moves — the columns simply become the product's fields.
            Op::Zip => {
                let cols = input.into_prod("Zip")?;
                let mut bounds: Option<Bounds> = None;
                let mut inner = Vec::with_capacity(cols.len());
                let mut differ = Vec::new(); // each column's bounds, kept only once two disagree
                for c in cols {
                    let (b, v) = c.into_list("Zip column")?;
                    match &bounds {
                        None => bounds = Some(b),
                        Some(prev) if differ.is_empty() && *prev == b => {}
                        Some(prev) => {
                            if differ.is_empty() {
                                differ.resize(inner.len(), prev.clone());
                            }
                            differ.push(b);
                        }
                    }
                    inner.push(v);
                }
                let bounds = bounds.ok_or("Zip expects a nonempty product of lists")?;
                // every column on the first one's bounds: a pure rewrap. Otherwise each row keeps
                // the shortest column's length (cold: only when two columns disagree).
                if !differ.is_empty() {
                    return Ok(zip_shortest(differ, inner));
                }
                Value::List(bounds, Box::new(Value::Prod(inner)))
            }

            // destructure a sum column: the tag list plus each lane re-sliced per outer row. A
            // lane's elements are stored packed in row order, so each lane keeps its values and
            // only gains bounds (per-row cumulative tag counts) — no data moves but the tag widen.
            Op::Unweave => {
                let (bounds, vals) = input.into_list("Unweave")?;
                let (tags, lanes) = vals.into_sum("Unweave")?;
                // each lane's bounds: per row, how many of its elements the lane holds, as
                // running ends. One row owns every element, so its counts are the lane lengths;
                // one tag throughout gives that lane every row's elements and the others none.
                // A sum's tag column is always one byte per row (every constructor and the codec
                // make it so).
                let lane_bounds: Vec<Vec<usize>> = match &tags {
                    _ if bounds.len() == 1 => lanes.iter().map(|l| vec![l.len()]).collect(),
                    Tags::Const(t, _) => (0..lanes.len())
                        .map(|l| if l == *t { bounds.ends().collect() } else { vec![0; bounds.len()] })
                        .collect(),
                    Tags::Column(Prim::U8(ts), _) => {
                        let mut lane_bounds = vec![Vec::with_capacity(bounds.len()); lanes.len()];
                        let mut counts = vec![0usize; lanes.len()];
                        let mut start = 0;
                        for end in bounds.ends() {
                            for &t in &ts[start..end] {
                                counts[t as usize] += 1;
                            }
                            for (lb, &c) in lane_bounds.iter_mut().zip(&counts) {
                                lb.push(c);
                            }
                            start = end;
                        }
                        lane_bounds
                    }
                    Tags::Column(..) => unreachable!("a sum's tags are one byte per row"),
                };
                // the tags are a byte per row, and stay one: the sum's own tag column is shared,
                // not copied, so a program that projects the tags away pays nothing for them.
                let narrow = match &tags {
                    Tags::Const(t, rows) => Prim::U8(Arc::new(vec![*t as u8; *rows])),
                    Tags::Column(p @ Prim::U8(_), _) => p.clone(),
                    Tags::Column(..) => unreachable!("a sum's tags are one byte per row"),
                };
                let tag_list = Value::List(bounds, Box::new(Value::Prim(narrow)));
                let mut out = vec![tag_list];
                for (lane, lb) in lanes.into_iter().zip(lane_bounds) {
                    out.push(Value::List(lb.into(), Box::new(lane)));
                }
                Value::Prod(out)
            }

            // Unweave's inverse: interleave the lanes per the tag list. The lanes' flat row-major
            // storage IS the Sum's lane storage, so after validating per-row tag counts against
            // each lane's row lengths, the Sum is built without moving lane data.
            Op::Weave => {
                let mut cols = input.into_prod("Weave")?;
                let rest = cols.split_off(1);
                if rest.is_empty() || rest.len() > 256 {
                    return Err(format!("Weave expects 1..=256 lanes, got {}", rest.len()));
                }
                let (tb, tv) = cols.pop().ok_or("Weave expects (List<U8> tags, List<A>, ..)")?.into_list("Weave tags")?;
                // tags are integers at any storage: bytes, the usual one, are read in place, and
                // any other is read by value (a tag past a byte names no lane; there are at most 256)
                let tags: std::borrow::Cow<[u8]> = match tv.as_u8("Weave tags") {
                    Ok(t) => t.into(),
                    Err(_) => tv
                        .as_i64("Weave tags")?
                        .iter()
                        .map(|&t| u8::try_from(t).map_err(|_| format!("Weave: tag {t} out of range")))
                        .collect::<Result<_, _>>()?,
                };
                let mut lanes = Vec::with_capacity(rest.len());
                let mut lane_bounds = Vec::with_capacity(rest.len());
                for l in rest {
                    let (b, v) = l.into_list("Weave lane")?;
                    assert_eq!(b.len(), tb.len(), "Weave: lane/tags row count");
                    lane_bounds.push(b);
                    lanes.push(v);
                }
                // one pass validates the per-row tag counts AND builds the Sum's assignment: a
                // row's within-lane offset is its lane's running count, which this loop already has.
                let mut counts = vec![0usize; lanes.len()];
                let (mut tag8, mut off) = (Vec::with_capacity(tags.len()), Vec::with_capacity(tags.len()));
                let mut start = 0;
                for (r, end) in tb.ends().enumerate() {
                    for &t in &tags[start..end] {
                        assert!((t as usize) < lanes.len(), "Weave: tag {t} out of range");
                        tag8.push(t);
                        off.push(counts[t as usize]);
                        counts[t as usize] += 1;
                    }
                    for (t, (lb, &c)) in lane_bounds.iter().zip(&counts).enumerate() {
                        assert_eq!(lb.end(r), c, "Weave: row {r} lane {t} length/tag-count mismatch");
                    }
                    start = end;
                }
                let sum = Value::sum_tagged(Tags::column(Prim::U8(Arc::new(tag8)), off), lanes);
                Value::List(tb, Box::new(sum))
            }

            Op::CapList => {
                let (x, list) = input.into_pair("CapList")?;
                let (bounds, y) = list.into_list("CapList list")?;
                let idx = owner_ids(&bounds);
                Value::List(bounds, Box::new(Value::Prod(vec![gather(&x, &idx), y])))
            }

            // capture into a sum: row i's context pairs with its payload inside variant tags[i].
            // Lane t's rows are the tag-t rows in tag order, so gathering the context at those
            // positions aligns with the carried within-variant offsets.
            Op::CapSum => {
                let (x, s) = input.into_pair("CapSum")?;
                let Value::Sum(tags, lanes) = s else {
                    return Err(format!("CapSum expects (X, Sum), got (.., {})", shape_of_value(&s)));
                };
                assert_eq!(x.len(), tags.len(), "CapSum: context/sum length");
                let mut per = vec![Vec::new(); lanes.len()];
                for (i, t) in tags.tags_iter().enumerate() {
                    per[t].push(i);
                }
                let new = lanes
                    .into_iter()
                    .zip(&per)
                    .map(|(lane, rows)| Value::Prod(vec![gather(&x, rows), lane]))
                    .collect();
                // the assignment is unchanged: each lane keeps its rows, now paired.
                Value::Sum(tags, new)
            }

            // stable structural hash: one 64-bit word per row, read as an i64, over any shape (see
            // `crate::hash`).
            Op::Hash => Value::i64(crate::value::i64s_of_words(crate::hash::hash(&input))),

            Op::Filter => {
                let (bounds, pairs) = input.into_list("Filter")?;
                let (mask, vals) = pairs.into_pair("Filter element")?;
                let m = mask.as_mask("Filter mask")?;
                let m = &m[..];
                // leaves (and products of them) compress in one pass each; the new row ends are a
                // count of each row's kept elements. Lists, sums and references build positions
                // and gather them.
                match compress(&vals, m) {
                    Some(kept) => {
                        let mut nb = Vec::with_capacity(bounds.len());
                        let (mut acc, mut start) = (0usize, 0usize);
                        for end in bounds.ends() {
                            acc += m[start..end].iter().filter(|&&b| b != 0).count();
                            nb.push(acc);
                            start = end;
                        }
                        Value::List(nb.into(), Box::new(kept))
                    }
                    None => {
                        let (idx, nb) = filter_mask(&bounds, m);
                        Value::List(nb.into(), Box::new(gather(&vals, &idx)))
                    }
                }
            }

            // row-wise append: row r's output is a's row r elements followed by b's. A multi-source
            // gather over the two value columns ([a, b]) reuses the engine's `gather_lanes`, so it
            // works for any element shape X (leaf / product / list / sum).
            Op::Append => {
                let (a, b) = input.into_pair("Append")?;
                let (ab, av) = a.into_list("Append lhs")?;
                let (bb, bv) = b.into_list("Append rhs")?;
                same(&shape_of_value(&av), &shape_of_value(&bv)).map_err(|e| format!("Append: {e}"))?;
                // both are SEQ columns, hence equal row count by the product invariant (defensive).
                assert_eq!(ab.len(), bb.len(), "Append: row count mismatch");
                let cap = av.len() + bv.len();
                let mut nb = Vec::with_capacity(ab.len());
                let (mut tags, mut off) = (Vec::with_capacity(cap), Vec::with_capacity(cap));
                let (mut acc, mut sa, mut sb) = (0usize, 0usize, 0usize);
                for r in 0..ab.len() {
                    let (ea, eb) = (ab.end(r), bb.end(r));
                    for p in sa..ea { tags.push(0); off.push(p); } // a's row-r elements ...
                    for p in sb..eb { tags.push(1); off.push(p); } // ... then b's
                    acc += (ea - sa) + (eb - sb);
                    nb.push(acc);
                    sa = ea;
                    sb = eb;
                }
                Value::List(nb.into(), Box::new(gather_lanes(&[Some(&av), Some(&bv)], &tags, &off)))
            }

            // each row's length, read off the bounds in one pass (no per-element work).
            Op::Len => {
                let (rows, _vals) = input.rows_of("Len")?;
                let lens = (0..rows.len()).map(|r| { let (s, e) = rows.span(r); (e - s) as i64 }).collect();
                Value::i64(lens)
            }

            // re-partition each row into k-wide sub-rows. Pure: the values never move — only the bounds
            // change, the new inner being a `Stride(k)` (the surface producer of wide strides).
            Op::Cut => {
                let (bounds, pairs) = input.into_list("Cut")?;
                let (mask, vals) = pairs.into_pair("Cut element")?;
                cut(&bounds, &mask.as_mask("Cut mask")?, vals)
            }

            Op::Chunk(k) => {
                if *k == 0 {
                    return Err("Chunk width must be positive".into());
                }
                let (bounds, vals) = input.into_list("Chunk")?;
                chunk(bounds, vals, *k)
            }

            // N-way partition: the discriminant `tags` routes each row of `data` to its variant. The
            // tags ARE the sum's tag column; each variant gathers its rows in order (so the implicit
            // within-variant offset matches `Value::sum`).
            Op::Branch(n) => {
                let (data, tags_v) = input.into_pair("Branch")?;
                // a tag is read as its word, so a negative one is past every lane and goes to the last
                let tags = tags_v.as_words("Branch tags")?;
                assert_eq!(data.len(), tags.len(), "Branch: payload/discriminant length");
                if *n > 256 {
                    return Err(format!("Branch: arity {n} exceeds the u8 tag width"));
                }
                if *n == 0 {
                    return Err("Branch: a sum of no lanes has nowhere to put a row".into());
                }
                // one pass builds the tag column, each lane's row list, AND the within-variant offset
                // (a row's offset is its lane's size when it arrives) — no decode/recompute afterwards.
                let mut groups: Vec<Vec<usize>> = vec![Vec::new(); *n];
                let mut tag8 = Vec::with_capacity(tags.len());
                let mut off = Vec::with_capacity(tags.len());
                let last = n.saturating_sub(1) as u64;
                for (i, &t) in tags.iter().enumerate() {
                    let t = t.min(last) as usize;
                    tag8.push(t as u8);
                    off.push(groups[t].len());
                    groups[t].push(i);
                }
                let variants = groups.iter().map(|idx| gather(&data, idx)).collect();
                Value::sum_tagged(Tags::column(Prim::U8(Arc::new(tag8)), off), variants)
            }

            Op::Unwrap => {
                // each row's payload, read straight from its variant by the carried within-offset —
                // the fused inverse of `Inject` (no `concat(variants)` temporary).
                let (tags, variants) = input.into_sum("Unwrap")?;
                let first = variants.first().ok_or("Unwrap: empty sum")?;
                let first_shape = shape_of_value(first);
                for v in &variants[1..] {
                    same(&first_shape, &shape_of_value(v)).map_err(|e| format!("Unwrap: {e}"))?;
                }
                // every row in one lane, in row order: that lane already IS the answer. This is the
                // `unwrap(inject x) = x` case, and it costs nothing.
                if let Some(t) = tags.const_tag() {
                    return Ok(variants.into_iter().nth(t).expect("tag names a lane"));
                }
                let Tags::Column(tp, os) = &tags else { unreachable!("const handled above") };
                // lanes of leaves read each row with the u8 discriminants in place.
                if let Prim::U8(t8) = tp {
                    let lanes: Vec<&Value> = variants.iter().collect();
                    if let Some(v) = unwrap_leaves(&lanes, t8, os) {
                        return Ok(v);
                    }
                }
                // otherwise the carried offsets are already the `&[usize]` `gather_lanes` wants;
                // only the u8 discriminants widen.
                let ts: Vec<usize> = tags.tags_iter().collect();
                let refs: Vec<Option<&Value>> = variants.iter().map(Some).collect();
                gather_lanes(&refs, &ts, os)
            }

            // sum introduction: every row goes to variant `tag` (a constant tag run), the
            // payload column fills that lane, the others are zero-row columns of their declared
            // shapes. The unary dual of `tuple`.
            Op::Inject(tag, shapes) => {
                let n = input.len();
                if *tag >= shapes.len() {
                    return Err(format!("Inject: tag {tag} out of range for arity {}", shapes.len()));
                }
                if shapes.len() > 256 {
                    return Err(format!("Inject: arity {} exceeds the u8 tag width", shapes.len()));
                }
                if shapes[*tag] != shape_of_value(&input) {
                    return Err(format!("Inject: lane {tag} is declared {}, got {}", shapes[*tag], shape_of_value(&input)));
                }
                let mut variants: Vec<Value> = shapes.iter().map(Value::empty).collect();
                variants[*tag] = input;
                // a constant tag run: the assignment is two words, and the within-lane offset IS
                // the row index — neither column is materialised (see `Tags::Const`).
                Value::sum_tagged(Tags::Const(*tag, n), variants)
            }

            Op::MapList(body) => {
                let (bounds, inner) = input.into_list("MapList")?;
                Value::List(bounds, Box::new(try_eval_graph(body, inner)?))
            }

            // seeded left fold, vectorized across rows. `acc` is a column of one accumulator per row
            // (starts as the seed). Round t pairs every still-long-enough row's acc with its t-th
            // element, runs the body once over those active rows, scatters the results back. Rounds =
            // the longest row; empty rows never become active, so it is total (empty list -> seed).
            Op::Fold(body) => {
                let (seed, list) = input.into_pair("Fold")?;
                let (bounds, vals) = list.into_list("Fold list")?;
                // the body must hand back the seed's shape: checked on the first round — or, when no
                // row has an element (no rounds at all, the typer's zero-row run included), on a
                // zero-row run of the body.
                let seed_shape = shape_of_value(&seed);
                let check = |updated: &Value| {
                    same(&seed_shape, &shape_of_value(updated)).map(drop).map_err(|e| format!("Fold body: {e}"))
                };
                if bounds.total() == 0 {
                    let z = try_eval_graph(body, Value::Prod(vec![gather(&seed, &[]), gather(&vals, &[])]))?;
                    check(&z)?;
                    return Ok(seed);
                }
                // Strided fast path: every row has length k, so every row is active in every
                // round — no worklist, no per-round accumulator gather/scatter (the whole
                // accumulator IS the active set); one strided element gather per round.
                if let Some(k) = bounds.strided() {
                    let n = bounds.len();
                    let mut acc = seed;
                    let mut elem: Vec<usize> = Vec::with_capacity(n);
                    for t in 0..k {
                        elem.clear();
                        elem.extend((0..n).map(|r| r * k + t));
                        let elt = gather(&vals, &elem);
                        let updated = try_eval_graph(body, Value::Prod(vec![acc, elt]))?;
                        if t == 0 {
                            check(&updated)?;
                        }
                        assert_eq!(updated.len(), n, "Fold body changed the row count");
                        acc = updated;
                    }
                    return Ok(acc);
                }
                let mut acc = seed;
                let mut active = init_active(&bounds);
                let mut t = 0;
                while !active.is_empty() {
                    let rows: Vec<usize> = active.iter().map(|&(r, _, _)| r).collect();
                    let elem: Vec<usize> = active.iter().map(|&(_, s, _)| s + t).collect();
                    let acc_active = gather(&acc, &rows);
                    let elt = gather(&vals, &elem);
                    let updated = try_eval_graph(body, Value::Prod(vec![acc_active, elt]))?;
                    if t == 0 {
                        check(&updated)?;
                    }
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
                let (seed, list) = input.into_pair("FoldScan")?;
                let (bounds, vals) = list.into_list("FoldScan list")?;
                // the body's new state must have the seed's shape (checked as in `Fold`).
                let seed_shape = shape_of_value(&seed);
                let check = |state: &Value| {
                    same(&seed_shape, &shape_of_value(state)).map(drop).map_err(|e| format!("FoldScan body: {e}"))
                };
                let total = vals.len();
                // Strided fast path — as in `Fold`: no worklist, no scatter; round t's outputs
                // land at positions r*k + t, chunk t, slot r.
                if let Some(k) = bounds.strided() {
                    let n = bounds.len();
                    let mut acc = seed;
                    let mut chunks: Vec<Value> = Vec::with_capacity(k);
                    let (mut tags, mut off) = (vec![0usize; total], vec![0usize; total]);
                    let mut elem: Vec<usize> = Vec::with_capacity(n);
                    for t in 0..k {
                        elem.clear();
                        elem.extend((0..n).map(|r| r * k + t));
                        let elt = gather(&vals, &elem);
                        let (new_state, r) =
                            try_eval_graph(body, Value::Prod(vec![acc, elt]))?.into_pair("FoldScan body")?;
                        if t == 0 {
                            check(&new_state)?;
                        }
                        assert_eq!(new_state.len(), n, "FoldScan body changed the row count");
                        for (slot, &pos) in elem.iter().enumerate() {
                            tags[pos] = t;
                            off[pos] = slot;
                        }
                        acc = new_state;
                        chunks.push(r);
                    }
                    let out_vals = if chunks.is_empty() {
                        let z = try_eval_graph(body, Value::Prod(vec![gather(&acc, &[]), gather(&vals, &[])]))?;
                        let (state, r) = z.into_pair("FoldScan body")?;
                        check(&state)?;
                        r
                    } else {
                        let refs: Vec<Option<&Value>> = chunks.iter().map(Some).collect();
                        gather_lanes(&refs, &tags, &off)
                    };
                    return Ok(Value::Prod(vec![acc, Value::List(bounds, Box::new(out_vals))]));
                }
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
                        try_eval_graph(body, Value::Prod(vec![acc_active, elt]))?.into_pair("FoldScan body")?;
                    if t == 0 {
                        check(&new_state)?;
                    }
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
                    let z = try_eval_graph(body, Value::Prod(vec![gather(&acc, &[]), gather(&vals, &[])]))?;
                    let (state, r) = z.into_pair("FoldScan body")?;
                    check(&state)?;
                    r
                } else {
                    let refs: Vec<Option<&Value>> = chunks.iter().map(Some).collect();
                    gather_lanes(&refs, &tags, &off)
                };
                Value::Prod(vec![acc, Value::List(bounds, Box::new(out_vals))])
            }

            Op::MapSum(arms) => {
                // the tag and within-offset columns are untouched by a lane map (each lane keeps its
                // row count), so move them through rather than decode + recompute them.
                let Value::Sum(tags, mut variants) = input else {
                    return Err(format!("MapSum expects a sum, got {}", shape_of_value(&input)));
                };
                for (i, (k, body)) in arms.iter().enumerate() {
                    if *k >= variants.len() {
                        return Err(format!("MapSum: no variant {k}"));
                    }
                    // disjoint indices keep the arms independent (so they commute).
                    if arms[..i].iter().any(|(j, _)| j == k) {
                        return Err(format!("MapSum: duplicate variant {k}"));
                    }
                    // take the lane so the body's `Input` owns it (refcount 1 ⇒ in-place).
                    let lane = std::mem::replace(&mut variants[*k], Value::Unit(0));
                    let lane_len = lane.len();
                    let res = try_eval_graph(body, lane)?;
                    assert_eq!(res.len(), lane_len, "MapSum changed a variant's length");
                    variants[*k] = res;
                }
                Value::Sum(tags, variants)
            }

            Op::Gather => {
                let (idx, haystack) = input.into_pair("Gather")?;
                let (hb, hvals) = haystack.rows_of("Gather haystack")?;
                assert_eq!(idx.len(), hb.len(), "Gather: indices/haystack row count");
                // the one-row leaf fast path indexes the payload directly, so row 0 must BE the
                // payload (a partition); a referenced haystack takes the general path below.
                if let (Value::List(ib, ivals), Value::Prim(p), Rows::Part(_)) = (&idx, hvals, hb) {
                    if ib.len() == 1 && matches!(**ivals, Value::Prim(Prim::I64(_))) {
                        // Raw Gather reads zero out of range, not an all-or-nothing error row: a
                        // clamped read and a select, no separate scan. This is the one path that
                        // CONSUMES the indices — it rewrites that buffer into the result — so it is
                        // also the only one that takes ownership.
                        let p = p.clone();
                        let Value::List(ib, ivals) = idx else { unreachable!() };
                        let idxs = ivals.into_words("Gather indices")?;
                        return Ok(Value::List(ib, Box::new(Value::Prim(p.gather_words_owned(idxs)))));
                    }
                }
                let mut ok = vec![true; idx.len()];
                index_plan(&idx, &Owners::Identity, hb, &mut ok)?.fill_or_zero(hvals)?
            }

            // total vector access: each index either names a haystack-row element (Found) or is out of
            // that row's bounds (Missing, only counted). The per-element test is branchless (a
            // comparison to a u64); only the routing into the two lanes is data-dependent. Output is a
            // list (the index list's bounds) of Sum{Found:T | Missing}.
            Op::GatherTry => {
                let (idx, haystack) = input.into_pair("GatherTry")?;
                let (ib, ivals) = idx.into_list("GatherTry indices")?;
                let (hb, hvals) = haystack.rows_of("GatherTry haystack")?;
                assert_eq!(ib.len(), hb.len(), "GatherTry: indices/haystack row count");
                let idxs = ivals.as_words("GatherTry indices")?;
                // the clean case first: one branch-free pass resolves every index and notes whether
                // any is out of its row. Only when one is does the routing below run.
                let mut pos = Vec::with_capacity(idxs.len());
                let mut ok = true;
                for r in 0..ib.len() {
                    let (is, ie) = ib.span(r);
                    let (hs, he) = hb.span(r);
                    let rowlen = (he - hs) as u64;
                    for &x in &idxs[is..ie] {
                        ok &= x < rowlen;
                        pos.push(hs.wrapping_add(x as usize));
                    }
                }
                if ok && !pos.is_empty() {
                    let found = Value::sum_tagged(Tags::Const(0, pos.len()), vec![gather(hvals, &pos), Value::Unit(0)]);
                    return Ok(Value::List(ib, Box::new(found)));
                }
                // one pass routes each index AND records its within-lane offset — the size its
                // lane had when it arrived — so the assignment needs no second pass to derive.
                let (mut tags, mut off) = (Vec::with_capacity(idxs.len()), Vec::with_capacity(idxs.len()));
                let mut abs = Vec::new(); // absolute haystack positions of the found elements (lane 0)
                let mut missing = 0usize; // how many indices are out of their row (lane 1)
                for r in 0..ib.len() {
                    let (is, ie) = ib.span(r);
                    let (hs, he) = hb.span(r);
                    let rowlen = he - hs;
                    for &x in &idxs[is..ie] {
                        if (x as usize) < rowlen {
                            tags.push(0u8);
                            off.push(abs.len());
                            abs.push(hs + x as usize);
                        } else {
                            tags.push(1u8);
                            off.push(missing);
                            missing += 1;
                        }
                    }
                }
                let lanes = vec![gather(hvals, &abs), Value::Unit(missing)];
                let sum = Value::sum_tagged(Tags::column(Prim::U8(Arc::new(tags)), off), lanes);
                Value::List(ib, Box::new(sum))
            }

            // DESTRUCTURE one list layer: return the per-inner-list ranges (relative to
            // each top row's flattened span) AND the one-level-flattened values. Both
            // outputs are lists at the SAME top stratum, so they bundle as a Prod, and the
            // word `slices` is the exact inverse — hence MapList(MapList(b)) == Flatten; b; slices.
            Op::Flatten => {
                let (ob, inner) = input.into_list("Flatten")?;
                let (ib, vals) = inner.into_list("Flatten inner")?;
                let new_ob: Vec<usize> =
                    ob.ends().map(|e| if e == 0 { 0 } else { ib.end(e - 1) }).collect();
                let mut lo_c = Vec::with_capacity(ib.len());
                let mut hi_c = Vec::with_capacity(ib.len());
                let mut prev = 0;
                for e in ob.ends() {
                    let base = if prev == 0 { 0 } else { ib.end(prev - 1) }; // top row's flat start
                    for kk in prev..e {
                        let g_lo = if kk == 0 { 0 } else { ib.end(kk - 1) };
                        lo_c.push((g_lo - base) as i64);
                        hi_c.push((ib.end(kk) - base) as i64);
                    }
                    prev = e;
                }
                let ranges = Value::List(
                    ob,
                    Box::new(Value::Prod(vec![Value::i64(lo_c), Value::i64(hi_c)])),
                );
                let flat = Value::List(new_ob.into(), Box::new(vals));
                Value::Prod(vec![ranges, flat])
            }

            // wrap each element in its own length-1 list (the list-monad unit). Values
            // unchanged; bounds become [1,2,..,n]. `Flatten` of an `Enlist` is identity.
            Op::Enlist => {
                let n = input.len();
                Value::List(Bounds::Stride(1, n), Box::new(input)) // uniform width 1 — a stride, not offsets
            }

            // generate a range per row: element n_i becomes the list [0,1,…,n_i-1]. Cardinality
            // lands inside the new List (SEQ stays 1:1). Lets a program build its own input data.
            Op::Iota => {
                let ns = input.as_i64("Iota")?;
                let mut bounds = Vec::with_capacity(ns.len());
                let mut vals = Vec::new();
                for &n in ns.iter() {
                    vals.extend(0..n); // empty when n <= 0
                    bounds.push(vals.len());
                }
                Value::List(bounds.into(), Box::new(Value::i64(vals)))
            }

            // per row [lo, hi): iota with a start, empty when lo >= hi.
            Op::Range => {
                let (lo, hi) = input.into_pair("Range")?;
                let (lo, hi) = (lo.as_i64("Range lo")?, hi.as_i64("Range hi")?);
                assert_eq!(lo.len(), hi.len(), "Range: lo/hi row count");
                let mut bounds = Vec::with_capacity(lo.len());
                let mut vals = Vec::new();
                for (&a, &z) in lo.iter().zip(hi.iter()) {
                    vals.extend(a..z.max(a));
                    bounds.push(vals.len());
                }
                Value::List(bounds.into(), Box::new(Value::i64(vals)))
            }

            // forget the payload, keep the row count — the constructor for unit/`None` columns.
            Op::Unit => Value::Unit(input.len()),

            // TRY is the identity on values; the effect lowering reads it as the handling point.
            Op::Try => input,

            // the failure family was dispatched to `ops::fail::eval` above.
            Op::TryGather | Op::TryChunk(_)
            | Op::TryZip | Op::Lift | Op::Squash | Op::HoistProd | Op::HoistList | Op::HoistSum(_) => unreachable!("ops::fail::eval handles the failure family"),

            // branchless blend: a two-source `gather_lanes` reading each row's own position from the
            // lane its mask selects (`then` when nonzero). Both operands are full columns, so the
            // identity offset reads row i from row i — the whole "computed both sides, pick per lane".
            Op::Select => {
                let mut cols = input.into_prod("Select")?;
                if cols.len() != 3 {
                    return Err("Select expects (Int mask, T, T)".into());
                }
                let els = cols.pop().unwrap();
                let then = cols.pop().unwrap();
                let mask_col = cols.pop().unwrap();
                let mask = mask_col.as_mask("Select mask")?;
                same(&shape_of_value(&then), &shape_of_value(&els)).map_err(|e| format!("Select: {e}"))?;
                blend(&mask, then, els)
            }
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

/// `Zip` on columns whose rows disagree in length: each row keeps the shortest column's length, the
/// rest of each longer row dropped.
#[cold]
#[inline(never)]
fn zip_shortest(bounds: Vec<Bounds>, cols: Vec<Value>) -> Value {
    let rows = bounds[0].len();
    let mut keep = Vec::with_capacity(rows);
    for r in 0..rows {
        keep.push(bounds.iter().map(|b| { let (s, e) = b.span(r); e - s }).min().unwrap_or(0));
    }
    let mut ends = Vec::with_capacity(rows);
    let mut acc = 0;
    for &n in &keep {
        acc += n;
        ends.push(acc);
    }
    let inner = bounds
        .iter()
        .zip(&cols)
        .map(|(b, v)| {
            let idx: Vec<usize> = (0..rows).flat_map(|r| { let s = b.span(r).0; s..s + keep[r] }).collect();
            gather(v, &idx)
        })
        .collect();
    Value::List(ends.into(), Box::new(Value::Prod(inner)))
}

/// `Chunk(k)`: each row split into `k`-wide sub-rows; a row that doesn't divide by `k` drops its
/// remainder, and then the kept elements are no longer contiguous, so they are gathered (cold).
/// `Cut`: each row's pieces end where the next marked element starts one, and at the row's end.
fn cut(bounds: &Bounds, mask: &[u8], vals: Value) -> Value {
    let mut outer = Vec::with_capacity(bounds.len());
    let mut inner = Vec::new();
    let mut start = 0usize;
    for end in bounds.ends() {
        for (k, &m) in mask.iter().enumerate().take(end).skip(start + 1) {
            if m != 0 {
                inner.push(k);
            }
        }
        if end > start {
            inner.push(end);
        }
        outer.push(inner.len());
        start = end;
    }
    Value::List(outer.into(), Box::new(Value::List(inner.into(), Box::new(vals))))
}

fn chunk(bounds: Bounds, vals: Value, k: usize) -> Value {
    let mut outer = Vec::with_capacity(bounds.len());
    let (mut total, mut prev, mut ragged) = (0usize, 0usize, false);
    for end in bounds.ends() {
        let len = end - prev;
        if len % k != 0 {
            ragged = true;
        }
        total += len / k;
        outer.push(total);
        prev = end;
    }
    let vals = if ragged { chunk_kept(&bounds, &vals, k) } else { vals };
    Value::List(outer.into(), Box::new(Value::List(Bounds::Stride(k, total), Box::new(vals))))
}

/// `Chunk(k)`'s values when some row doesn't divide by `k`: each row's first `len / k * k` elements.
#[cold]
#[inline(never)]
fn chunk_kept(bounds: &Bounds, vals: &Value, k: usize) -> Value {
    let idx: Vec<usize> = (0..bounds.len())
        .flat_map(|r| { let (s, e) = bounds.span(r); s..s + (e - s) / k * k })
        .collect();
    gather(vals, &idx)
}

