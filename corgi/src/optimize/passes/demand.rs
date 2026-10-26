//! Producer pruning. Analysis supplies the demand; this module rewrites the graph.

use crate::demand::{input_demand, movement_demand, Demand};
use crate::graph::{node_shapes, shape_of, shape_of_op, Graph, Node, NodeKind};
use crate::ops::{NumOp, Op};
use crate::shape::Shape;
use super::dce;

/// Prune once without adding runtime projections. Input omission belongs to the caller.
pub(crate) fn prune_producers(graph: &Graph<NumOp>, input: &Shape, output: &Shape) -> Result<Graph<NumOp>, String> {
    if !graph.nodes.iter().any(|node| matches!(node.kind, NodeKind::Input)) {
        return Ok(graph.clone());
    }
    prune_graph_mode(graph, input, output, false).map_err(|error| error.to_string())
}

pub(crate) fn prune_graph(graph: &Graph<NumOp>, input: &Shape, output_projection: &Shape) -> Result<Graph<NumOp>, String> {
    prune_graph_mode(graph, input, output_projection, true).map_err(|error| error.to_string())
}

#[derive(Debug)]
enum PruneError {
    Unavailable,
    Invalid(String),
}

impl From<String> for PruneError {
    fn from(error: String) -> Self { Self::Invalid(error) }
}

impl From<&str> for PruneError {
    fn from(error: &str) -> Self { Self::Invalid(error.into()) }
}

impl std::fmt::Display for PruneError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("demand cannot reach a producer without projection"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

fn prune_graph_mode(graph: &Graph<NumOp>, input: &Shape, output_projection: &Shape, allow_projection: bool) -> Result<Graph<NumOp>, PruneError> {
    // The first pass discards dead branches without specializing any shared producer.
    // The second pass can then count only uses that survived.
    let live = match prune_graph_once(graph, input, output_projection, false, allow_projection) {
        Ok(live) => live,
        // A nested body can require a producer rewrite even before dead uses
        // disappear. Keep its original use counts when the first pass cannot
        // express the requested shape without inserting a projection.
        Err(PruneError::Unavailable) => graph.clone(),
        Err(error) => return Err(error),
    };
    prune_graph_once(&live, input, output_projection, true, allow_projection)
}

fn prune_graph_once(graph: &Graph<NumOp>, input: &Shape, output_projection: &Shape, specialize_maps: bool, allow_projection: bool) -> Result<Graph<NumOp>, PruneError> {
    let shapes = node_shapes(graph, input)?;
    let output = shapes.get(graph.output).ok_or("graph has no output")?;
    let projected = shape_of_op(&NumOp::Core(Op::Project(output_projection.clone())), output)?;
    if projected != *output_projection {
        return Err("output projection produced the wrong shape".into());
    }
    let input_node = graph
        .nodes
        .iter()
        .position(|node| matches!(node.kind, NodeKind::Input))
        .ok_or("graph has no input")?;

    let mut follows_input = Vec::with_capacity(graph.nodes.len());
    let mut uses = vec![0usize; graph.nodes.len()];
    for node in &graph.nodes {
        for &edge in &node.inputs {
            uses[edge] += 1;
        }
        follows_input.push(match &node.kind {
            NodeKind::Input => true,
            NodeKind::Tuple => node.inputs.first().is_some_and(|&i| follows_input[i]),
            NodeKind::Op(_) => follows_input[node.inputs[0]],
        });
    }
    uses[graph.output] += 1;

    let mut nodes = graph.nodes.clone();
    let unit = nodes.len();
    nodes.push(Node { kind: NodeKind::Op(Op::Unit.into()), inputs: vec![input_node] });
    let result = Pruner {
        nodes: &mut nodes, shapes: &shapes, follows_input: &follows_input,
        uses: &uses, unit, allow_projection,
    }.prune(graph.output, output_projection, specialize_maps)?;
    let mut rewritten = dce(&Graph { nodes, output: result });
    // DCE can remove the formal input of a constant graph; keep it for input-demand extraction.
    if !rewritten.nodes.iter().any(|node| matches!(node.kind, NodeKind::Input)) {
        rewritten.nodes.insert(0, Node { kind: NodeKind::Input, inputs: Vec::new() });
        for node in rewritten.nodes.iter_mut().skip(1) {
            for edge in &mut node.inputs {
                *edge += 1;
            }
        }
        rewritten.output += 1;
    }
    if shape_of(&rewritten, input).as_ref() != Ok(output_projection) {
        return Err("rewritten graph does not produce the requested shape".into());
    }
    Ok(rewritten)
}

/// Context fixed for one graph walk; only node, target and specialization vary.
struct Pruner<'a> {
    nodes: &'a mut Vec<Node<NumOp>>,
    shapes: &'a [Shape],
    follows_input: &'a [bool],
    uses: &'a [usize],
    unit: usize,
    allow_projection: bool,
}

impl Pruner<'_> {
    fn prune(&mut self, node: usize, target: &Shape, specialize_maps: bool) -> Result<usize, PruneError> {
        match self.prune_inner(node, target, specialize_maps) {
            Err(PruneError::Unavailable) if self.shapes[node] == *target => Ok(node),
            result => result,
        }
    }

    fn prune_inner(&mut self, node: usize, target: &Shape, specialize_maps: bool) -> Result<usize, PruneError> {
        // Even a cheap shared tuple/field can hide a single-use expensive producer. Visiting
        // that producer once per consumer would duplicate it, so stop specialization here too.
        let specialize_maps = specialize_maps && self.uses[node] <= 1;
        if *target == Shape::Unit {
            // An already-unit input is its own row-count witness, including inside a sum arm.
            if matches!(self.nodes[node].kind, NodeKind::Input) && self.shapes[node] == Shape::Unit {
                return Ok(node);
            }
            if self.follows_input[node] {
                return Ok(self.unit);
            }
            let result = self.nodes.len();
            self.nodes.push(Node { kind: NodeKind::Op(Op::Unit.into()), inputs: vec![node] });
            return Ok(result);
        }
        if let NodeKind::Op(NumOp::Core(Op::Project(projected))) = &self.nodes[node].kind {
            if projected == target {
                let source = self.nodes[node].inputs[0];
                return self.prune(source, target, specialize_maps);
            }
        }
        if let NodeKind::Op(NumOp::Core(Op::Field(field))) = &self.nodes[node].kind {
            let field = *field;
            let source = self.nodes[node].inputs[0];
            if matches!(self.nodes[source].kind, NodeKind::Tuple) {
                let selected = self.nodes[source].inputs[field];
                return self.prune(selected, target, specialize_maps);
            }
            if specialize_maps && self.uses[source] <= 1 && !matches!(self.nodes[source].kind, NodeKind::Input) {
                let Shape::Prod(fields) = &self.shapes[source] else { unreachable!() };
                let mut targets = vec![Shape::Unit; fields.len()];
                targets[field] = target.clone();
                let narrowed = self.prune(source, &Shape::Prod(targets), specialize_maps)?;
                // A Project left after the same producer saves no work over the original Field.
                // Keep the direct path unless the projection actually reached a producer's input.
                let only_projected = matches!(&self.nodes[narrowed].kind, NodeKind::Op(NumOp::Core(Op::Project(_))))
                    && self.nodes[narrowed].inputs[0] == source;
                if narrowed != source && !only_projected {
                    let result = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(Op::Field(field).into()), inputs: vec![narrowed] });
                    return Ok(result);
                }
            }
        }
        if let (NodeKind::Tuple, Shape::Prod(targets)) = (&self.nodes[node].kind, target) {
            let inputs = self.nodes[node].inputs.clone();
            if inputs.len() == targets.len() {
                let rewritten: Vec<_> = inputs
                    .iter()
                    .zip(targets)
                    .map(|(&edge, target)| self.prune(edge, target, specialize_maps))
                    .collect::<Result<Vec<_>, _>>()?;
                if rewritten == inputs && self.shapes[node] == *target {
                    return Ok(node);
                }
                let result = self.nodes.len();
                self.nodes.push(Node { kind: NodeKind::Tuple, inputs: rewritten });
                return Ok(result);
            }
        }
        if specialize_maps {
            if let (NodeKind::Op(NumOp::Core(Op::Enlist)), Shape::List(element_target)) = (&self.nodes[node].kind, target) {
                let source = self.nodes[node].inputs[0];
                let narrowed = self.prune(source, element_target, specialize_maps)?;
                if narrowed != source {
                    let result = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(Op::Enlist.into()), inputs: vec![narrowed] });
                    return Ok(result);
                }
            }
            if let NodeKind::Op(NumOp::Core(op)) = &self.nodes[node].kind {
                let source = self.nodes[node].inputs[0];
                if let Some(need) = movement_demand(op, &self.shapes[source], Demand::from_projection(target)) {
                    let input_target = need.to_shape(&self.shapes[source])?;
                    if input_target != self.shapes[source] {
                        let op = NumOp::Core(op.clone());
                        let output_shape = shape_of_op(&op, &input_target)?;
                        let narrowed = self.prune(source, &input_target, specialize_maps)?;
                        let result = self.nodes.len();
                        self.nodes.push(Node { kind: NodeKind::Op(op), inputs: vec![narrowed] });
                        if output_shape == *target {
                            return Ok(result);
                        }
                        // Different position leaves can request different payload fields. Gather
                        // uses their union, then each result leaf keeps just its own demand.
                        if !self.allow_projection { return Err(PruneError::Unavailable); }
                        let projected = self.nodes.len();
                        self.nodes.push(Node { kind: NodeKind::Op(Op::Project(target.clone()).into()), inputs: vec![result] });
                        return Ok(projected);
                    }
                }
            }
            if let (NodeKind::Op(NumOp::Core(Op::MapList(body))), Shape::List(element_target)) = (&self.nodes[node].kind, target) {
                let source = self.nodes[node].inputs[0];
                let Shape::List(element_input) = &self.shapes[source] else { unreachable!() };
                let rewritten = prune_graph_mode(body, element_input, element_target, self.allow_projection)?;
                let body_changed = rewritten != **body;
                let (need, _) = input_demand(&rewritten, element_input, Demand::All)?;
                let needed_element = need.to_shape(element_input)?;
                let mut narrowed = source;
                if needed_element != **element_input && self.uses[source] <= 1
                    && !matches!(self.nodes[source].kind, NodeKind::Input)
                    && shape_of(&rewritten, &needed_element).as_ref() == Ok(element_target)
                {
                    let input_target = Shape::List(Box::new(needed_element));
                    let projected = match self.prune(source, &input_target, specialize_maps) {
                        Ok(projected) => projected,
                        Err(PruneError::Unavailable) => source,
                        Err(error) => return Err(error),
                    };
                    let only_projected = matches!(&self.nodes[projected].kind, NodeKind::Op(NumOp::Core(Op::Project(_))))
                        && self.nodes[projected].inputs[0] == source;
                    if !only_projected {
                        narrowed = projected;
                    }
                }
                if body_changed || narrowed != source {
                    let result = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(Op::MapList(Box::new(rewritten)).into()), inputs: vec![narrowed] });
                    return Ok(result);
                }
            }
            if let (NodeKind::Op(NumOp::Core(Op::MapSum(arms))), Shape::Sum(targets)) = (&self.nodes[node].kind, target) {
                let source = self.nodes[node].inputs[0];
                let Shape::Sum(lanes) = &self.shapes[source] else { unreachable!() };
                let mut changed = false;
                let mut rewritten = Vec::with_capacity(arms.len());
                for (tag, body) in arms {
                    let body_rewrite = prune_graph_mode(body, &lanes[*tag], &targets[*tag], self.allow_projection)?;
                    changed |= body_rewrite != *body;
                    rewritten.push((*tag, body_rewrite));
                }
                if changed {
                    let result = self.nodes.len();
                    let op = NumOp::Core(Op::MapSum(rewritten));
                    let output_shape = shape_of_op(&op, &self.shapes[source])?;
                    self.nodes.push(Node { kind: NodeKind::Op(op), inputs: vec![source] });
                    if output_shape == *target {
                        return Ok(result);
                    }
                    if !self.allow_projection { return Err(PruneError::Unavailable); }
                    let projected = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(Op::Project(target.clone()).into()), inputs: vec![result] });
                    return Ok(projected);
                }
            }
            // An opaque consumer still permits rewrites inside its input expression, provided
            // that expression keeps its complete shape. This finds Field(Gather(..)) under
            // arithmetic, for example, without claiming an arithmetic demand rule.
            if let NodeKind::Op(op) = &self.nodes[node].kind {
                let mut op = op.clone();
                let source = self.nodes[node].inputs[0];
                // A fold's state is fed back on the next iteration. Keep its complete shape while
                // pruning work inside the body, just as for an opaque consumer's input expression.
                if let NumOp::Core(Op::Fold(body) | Op::FoldScan(body)) = &mut op {
                    let Shape::Prod(pair) = &self.shapes[source] else { unreachable!() };
                    let Shape::List(element) = &pair[1] else { unreachable!() };
                    let body_input = Shape::Prod(vec![pair[0].clone(), *element.clone()]);
                    let body_output = shape_of(body, &body_input)?;
                    **body = prune_graph_mode(body, &body_input, &body_output, self.allow_projection)?;
                }
                let rewritten = if self.uses[source] <= 1 {
                    self.prune(source, &self.shapes[source], specialize_maps)?
                } else {
                    source
                };
                if rewritten != source || self.nodes[node].kind != NodeKind::Op(op.clone()) {
                    let result = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(op), inputs: vec![rewritten] });
                    if self.shapes[node] == *target {
                        return Ok(result);
                    }
                    if !self.allow_projection { return Err(PruneError::Unavailable); }
                    let projected = self.nodes.len();
                    self.nodes.push(Node { kind: NodeKind::Op(Op::Project(target.clone()).into()), inputs: vec![result] });
                    return Ok(projected);
                }
            }
        }
        if self.shapes[node] == *target {
            return Ok(node);
        }
        if !self.allow_projection { return Err(PruneError::Unavailable); }
        let result = self.nodes.len();
        self.nodes.push(Node { kind: NodeKind::Op(Op::Project(target.clone()).into()), inputs: vec![node] });
        Ok(result)
    }
}
