//! Caller-side boundary helpers. These construct explicit input adapters and consumers.
//! Optimizer entry points return demand directly and do not invoke these helpers.

use crate::demand::{input_demand, Demand};
use crate::graph::{shape_of, Builder, Graph};
use crate::ops::{NumOp, Op};
use crate::optimize::{dce, fuse_maps, peephole};
use crate::optimize::passes::demand::prune_graph;
use crate::shape::Shape;

/// An input operation that replaces unread payloads with `()`, if any exist.
///
/// `graph` must typecheck on `input`.
/// The returned graph accepts `input` and produces a simpler shape on which `graph` still typechecks and has the same output for every value.
/// Other operations are treated as reading their whole input until their demand rules are known.
pub fn demand_projection(
    graph: &Graph<NumOp>,
    input: &Shape,
) -> Result<Option<Graph<NumOp>>, String> {
    let (need, output) = input_demand(graph, input, Demand::All)?;
    let projected = need.to_shape(input)?;
    if projected == *input {
        return Ok(None);
    }
    let mut builder = Builder::default();
    let root = builder.input();
    let result = builder.add(Op::Project(projected.clone()), vec![root]);
    let projection = builder.finish(result);
    // An op may have a shape constraint beyond the values it reads.
    // Refuse the projection unless the subject still typechecks and keeps its original output shape.
    if shape_of(graph, &projected).as_ref() != Ok(&output) {
        return Ok(None);
    }
    Ok(Some(projection))
}

/// A rewritten expression and its input demand.
/// The optional projection graph describes the boundary transformation. Column-aware callers
/// use `input_demand` to construct the input directly instead of evaluating this adapter.
pub struct DemandTransform {
    pub graph: Graph<NumOp>,
    pub input_projection: Option<Graph<NumOp>>,
    /// The demand embodied by `input_projection`, or `All` when no projection is returned.
    pub input_demand: Demand,
}

/// Remove computations behind output lanes erased by `output_projection`, then extract input demand.
///
/// For every valid input `x`, the result satisfies `Project(output_projection)(graph(x)) = result.graph(result.input_projection(x))`.
/// An absent input projection means that `result.graph` accepts the original input unchanged.
/// `input_demand` describes the returned input projection and can be unioned with other consumers' demands on the same source.
/// Each revised consumer must still accept the unioned shape or receive its own input projection.
/// The original graph remains unchanged.
pub fn demand_transform(
    graph: &Graph<NumOp>,
    input: &Shape,
    output_projection: &Shape,
) -> Result<DemandTransform, String> {
    let rewritten = prune_graph(graph, input, output_projection)?;
    let input_projection = demand_projection(&rewritten, input)?;
    let input_demand = match &input_projection {
        Some(projection) => Demand::from_projection(&shape_of(projection, input)?),
        None => Demand::All,
    };
    Ok(DemandTransform { graph: rewritten, input_projection, input_demand })
}

/// A projection that removes `()` product positions and its revised consumer.
pub struct UnitCompaction {
    pub input_projection: Graph<NumOp>,
    pub input_shape: Shape,
    pub graph: Graph<NumOp>,
}

/// Remove `()` product positions from any typed lambda input.
///
/// For every input `x`, `graph(x) = result.graph(result.input_projection(x))`.
/// Products inside lists and sums are compacted; references retain their shape.
/// An all-`()` product becomes `Unit` so that its row count survives.
/// This changes the lambda boundary only; callers decide whether projecting before an enclosing operator preserves its semantics.
pub fn compact_unit_input(graph: &Graph<NumOp>, input: &Shape) -> Result<Option<UnitCompaction>, String> {
    compact_to_shape(graph, input, compact_unit_shape(input), None)
}

/// Remove `()` positions inside selected fields of a product input.
///
/// For every input `x`, `graph(x) = result.graph(result.input_projection(x))`.
/// Fields outside `selected` retain their exact shape, so a join key can be left untouched.
/// Products inside selected lists and sums are also compacted; references retain their shape.
/// An all-`()` product becomes `Unit` so that its row count survives.
/// This can follow `demand_transform`, after its input projection has introduced `()` fields.
pub fn compact_unit_fields(
    graph: &Graph<NumOp>,
    input: &Shape,
    selected: &[usize],
) -> Result<Option<UnitCompaction>, String> {
    let Shape::Prod(fields) = input else { return Err("unit compaction expects a product input".into()) };
    if selected.iter().any(|&i| i >= fields.len()) {
        return Err("unit compaction selected a field outside the input product".into());
    }
    let mut choose = vec![false; fields.len()];
    for &i in selected { choose[i] = true; }
    let compacted: Vec<Shape> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| if choose[i] { compact_unit_shape(field) } else { field.clone() })
        .collect();
    compact_to_shape(graph, input, Shape::Prod(compacted), Some(&choose))
}

fn compact_to_shape(
    graph: &Graph<NumOp>,
    input: &Shape,
    input_shape: Shape,
    selected: Option<&[bool]>,
) -> Result<Option<UnitCompaction>, String> {
    let output = shape_of(graph, input)?;
    if input_shape == *input {
        return Ok(None);
    }

    let mut builder = Builder::default();
    let root = builder.input();
    let narrowed = if let (Some(selected), Shape::Prod(fields), Shape::Prod(compacted)) = (selected, input, &input_shape) {
        let projected = fields
            .iter()
            .zip(compacted)
            .enumerate()
            .map(|(i, (old, new))| {
                let field = builder.add(Op::Field(i), vec![root]);
                if selected[i] { project_compact_shape(&mut builder, field, old, new) } else { field }
            })
            .collect();
        builder.tuple(projected)
    } else {
        project_compact_shape(&mut builder, root, input, &input_shape)
    };
    let input_projection = peephole(&builder.finish(narrowed));

    let mut builder = Builder::default();
    let root = builder.input();
    let restored = if let (Some(selected), Shape::Prod(fields), Shape::Prod(compacted)) = (selected, input, &input_shape) {
        let restored = fields
            .iter()
            .zip(compacted)
            .enumerate()
            .map(|(i, (old, new))| {
                let field = builder.add(Op::Field(i), vec![root]);
                if selected[i] { inflate_compact_shape(&mut builder, field, old, new) } else { field }
            })
            .collect();
        builder.tuple(restored)
    } else {
        inflate_compact_shape(&mut builder, root, input, &input_shape)
    };
    let output_node = builder.graft(graph, restored);
    let graph = dce(&peephole(&fuse_maps(&peephole(&builder.finish(output_node)))));

    if shape_of(&input_projection, input).as_ref() != Ok(&input_shape)
        || shape_of(&graph, &input_shape).as_ref() != Ok(&output)
    {
        return Err("unit compaction did not preserve the graph's shapes".into());
    }
    Ok(Some(UnitCompaction { input_projection, input_shape, graph }))
}

fn compact_unit_shape(shape: &Shape) -> Shape {
    match shape {
        Shape::Prod(fields) => {
            let kept: Vec<_> = fields.iter().map(compact_unit_shape).filter(|field| *field != Shape::Unit).collect();
            if kept.is_empty() { Shape::Unit } else { Shape::Prod(kept) }
        }
        Shape::List(element) => Shape::List(Box::new(compact_unit_shape(element))),
        Shape::Sum(lanes) => Shape::Sum(lanes.iter().map(compact_unit_shape).collect()),
        other => other.clone(),
    }
}

fn project_compact_shape(builder: &mut Builder<NumOp>, source: usize, old: &Shape, new: &Shape) -> usize {
    if old == new {
        return source;
    }
    match (old, new) {
        (Shape::Prod(_), Shape::Unit) => builder.add(Op::Unit, vec![source]),
        (Shape::Prod(fields), Shape::Prod(_)) => {
            let kept = fields
                .iter()
                .enumerate()
                .filter_map(|(i, old)| {
                    let new = compact_unit_shape(old);
                    (new != Shape::Unit).then(|| {
                        let field = builder.add(Op::Field(i), vec![source]);
                        project_compact_shape(builder, field, old, &new)
                    })
                })
                .collect();
            builder.tuple(kept)
        }
        (Shape::List(old), Shape::List(new)) => {
            let mut body = Builder::default();
            let element = body.input();
            let projected = project_compact_shape(&mut body, element, old, new);
            builder.add(Op::MapList(Box::new(body.finish(projected))), vec![source])
        }
        (Shape::Sum(olds), Shape::Sum(news)) => {
            let arms = olds
                .iter()
                .zip(news)
                .enumerate()
                .filter(|(_, (old, new))| old != new)
                .map(|(tag, (old, new))| {
                    let mut body = Builder::default();
                    let lane = body.input();
                    let projected = project_compact_shape(&mut body, lane, old, new);
                    (tag, body.finish(projected))
                })
                .collect();
            builder.add(Op::MapSum(arms), vec![source])
        }
        _ => source,
    }
}

fn inflate_compact_shape(builder: &mut Builder<NumOp>, source: usize, old: &Shape, new: &Shape) -> usize {
    if old == new {
        return source;
    }
    match (old, new) {
        (Shape::Prod(_), Shape::Unit) => restore_erased_product(builder, source, old),
        (Shape::Prod(fields), Shape::Prod(_)) => {
            let mut next = 0;
            let restored = fields
                .iter()
                .map(|field| {
                    let compact = compact_unit_shape(field);
                    if compact == Shape::Unit {
                        restore_erased_product(builder, source, field)
                    } else {
                        let retained = builder.add(Op::Field(next), vec![source]);
                        next += 1;
                        inflate_compact_shape(builder, retained, field, &compact)
                    }
                })
                .collect();
            builder.tuple(restored)
        }
        (Shape::List(old), Shape::List(new)) => {
            let mut body = Builder::default();
            let element = body.input();
            let restored = inflate_compact_shape(&mut body, element, old, new);
            builder.add(Op::MapList(Box::new(body.finish(restored))), vec![source])
        }
        (Shape::Sum(olds), Shape::Sum(news)) => {
            let arms = olds
                .iter()
                .zip(news)
                .enumerate()
                .filter(|(_, (old, new))| old != new)
                .map(|(tag, (old, new))| {
                    let mut body = Builder::default();
                    let lane = body.input();
                    let restored = inflate_compact_shape(&mut body, lane, old, new);
                    (tag, body.finish(restored))
                })
                .collect();
            builder.add(Op::MapSum(arms), vec![source])
        }
        _ => source,
    }
}

fn restore_erased_product(builder: &mut Builder<NumOp>, source: usize, old: &Shape) -> usize {
    match old {
        Shape::Unit => builder.add(Op::Unit, vec![source]),
        Shape::Prod(fields) => {
            let restored = fields.iter().map(|field| restore_erased_product(builder, source, field)).collect();
            builder.tuple(restored)
        }
        _ => unreachable!("only payload-free product fields can be erased"),
    }
}

