//! The core IR, generic over an op vocabulary `O: OpLike`. The graph owns the two
//! *structural* node kinds — `Input` (the parameter) and `Tuple` (the sole fan-in) —
//! and every other node delegates to `O`. A higher layer is simply a richer `O`
//! (e.g. `enum NumOp { Core(CoreOp), Arith(..) }` that embeds this one); the graph,
//! `eval_graph`, and `shape_of` are unchanged across layers.

use crate::shape::{shape_of_value, Shape};
use crate::value::Value;

/// an op vocabulary: a value-level `eval` — whose `Err` is the SHAPE error, so that `eval` run on a
/// zero-row column is the typer — and any body sub-graphs it carries (so structural passes like
/// `check` can recurse).
pub trait OpLike: Sized {
    fn eval(&self, input: Value) -> Result<Value, String>;
    fn children(&self) -> Vec<&Graph<Self>> {
        Vec::new()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum NodeKind<O> {
    Input,  // arity 0: the graph's parameter (stratum root)
    Tuple,  // arity N: the sole fan-in
    Op(O),  // a unary op
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Node<O> {
    pub(crate) kind: NodeKind<O>,
    pub(crate) inputs: Vec<usize>, // indices of earlier nodes — the edges
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Graph<O> {
    pub(crate) nodes: Vec<Node<O>>,
    pub(crate) output: usize,
}

impl<O: OpLike> Graph<O> {
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// structural well-formedness: `Input` 0 edges, `Tuple` any, ops exactly 1; every
    /// edge backward; recurse into body sub-graphs.
    pub fn check(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            let ok = match &node.kind {
                NodeKind::Input => node.inputs.is_empty(),
                NodeKind::Tuple => true,
                NodeKind::Op(_) => node.inputs.len() == 1,
            };
            assert!(ok, "node {i}: wrong edge count for its kind");
            assert!(node.inputs.iter().all(|&e| e < i), "node {i}: non-backward edge");
            if let NodeKind::Op(o) = &node.kind {
                for child in o.children() {
                    child.check();
                }
            }
        }
    }
}

/// evaluate the graph on one argument, CONSUMING it. `Input` takes it, `Tuple` gathers its edges
/// into a product, every op delegates to `O::eval`.
///
/// A node's value is MOVED to its last consumer and only cloned for earlier ones, so the final
/// reader holds the sole `Arc` to each leaf — `into_*` can move the buffer out (refcount 1) and an
/// op can `Arc::make_mut` in place. Taking `arg` by value extends that to the FIRST op: `Input`
/// moves the argument in rather than cloning it, so a caller that hands off sole ownership pays no
/// input copy. Backward edges (see [`Graph::check`]) make the per-node consumer count a single pass.
pub fn eval_graph<O: OpLike>(g: &Graph<O>, arg: Value) -> Value {
    try_eval_graph(g, arg).unwrap_or_else(|e| panic!("eval_graph: {e}"))
}

/// [`eval_graph`] with the shape error surfaced: the form the typer and body-bearing ops use.
pub fn try_eval_graph<O: OpLike>(g: &Graph<O>, arg: Value) -> Result<Value, String> {
    Prepared::new(g).run(arg)
}

/// A graph with everything that does not change between runs computed once: the per-node consumer
/// counts, and the two buffers a run needs.
///
/// [`try_eval_graph`] is `Prepared::new(g).run(arg)`, which is right for a graph run once — but
/// `Fold` and `FoldScan` run their body once per ROUND, and on one long row a round is one
/// element. There the setup is the work: two allocations and a pass over the nodes, per element.
/// Preparing once outside the round loop removes both.
///
/// This is also where a physical plan would live if corgi grows one — which strategy an op should
/// take, which tile schedule to run — since those are properties of the graph, not of a call.
pub(crate) struct Prepared<'g, O> {
    graph: &'g Graph<O>,
    /// consumer count per node, including the output's own use: the template `live` is reset from.
    uses: Vec<usize>,
    live: Vec<usize>,
    vals: Vec<Option<Value>>,
}

impl<'g, O: OpLike> Prepared<'g, O> {
    pub(crate) fn new(graph: &'g Graph<O>) -> Self {
        let mut uses = vec![0usize; graph.nodes.len()];
        for node in &graph.nodes {
            for &i in &node.inputs {
                uses[i] += 1;
            }
        }
        // the returned value is a use too, so a consumer can't move it out first.
        uses[graph.output] += 1;
        Prepared {
            live: uses.clone(),
            uses,
            vals: Vec::with_capacity(graph.nodes.len()),
            graph,
        }
    }

    /// Evaluate on one argument, CONSUMING it. A node's value is MOVED to its last consumer and
    /// only cloned for earlier ones, so the final reader holds the sole `Arc` to each leaf —
    /// `into_*` can move the buffer out (refcount 1) and an op can `Arc::make_mut` in place.
    pub(crate) fn run(&mut self, arg: Value) -> Result<Value, String> {
        let g = self.graph;
        self.live.copy_from_slice(&self.uses);
        self.vals.clear();

        // take node `i`'s value: move it out on its last use, else clone (a cheap `Arc` bump).
        let take = |vals: &mut Vec<Option<Value>>, live: &mut [usize], i: usize| -> Value {
            live[i] -= 1;
            if live[i] == 0 { vals[i].take().unwrap() } else { vals[i].as_ref().unwrap().clone() }
        };

        let mut arg = Some(arg); // moved into the (single) `Input` node
        for node in &g.nodes {
            let v = match &node.kind {
                NodeKind::Input => arg.take().expect("graph has more than one Input node"),
                NodeKind::Tuple => Value::Prod(
                    node.inputs
                        .iter()
                        .map(|&i| take(&mut self.vals, &mut self.live, i))
                        .collect(),
                ),
                NodeKind::Op(o) => o.eval(take(&mut self.vals, &mut self.live, node.inputs[0]))?,
            };
            self.vals.push(Some(v));
        }
        Ok(self.vals[g.output].take().unwrap())
    }
}

/// shape-check the graph given the input's shape: `eval` on a ZERO-ROW column of that shape. Every
/// op is total on zero rows (no data-dependent work remains), reports a mismatched operand as an
/// `Err`, and produces an output of the shape it would at any length — so the value's shape is the
/// program's, and there is no second, type-level copy of the vocabulary to keep in step.
pub fn shape_of<O: OpLike>(g: &Graph<O>, input: &Shape) -> Result<Shape, String> {
    try_eval_graph(g, Value::empty(input)).map(|v| shape_of_value(&v))
}

pub struct Builder<O> {
    nodes: Vec<Node<O>>,
}

impl<O: OpLike> Default for Builder<O> {
    fn default() -> Self {
        Builder { nodes: Vec::new() }
    }
}

impl<O: OpLike> Builder<O> {
    fn push(&mut self, kind: NodeKind<O>, inputs: Vec<usize>) -> usize {
        self.nodes.push(Node { kind, inputs });
        self.nodes.len() - 1
    }
    /// the graph's parameter (one per graph).
    pub fn input(&mut self) -> usize {
        self.push(NodeKind::Input, vec![])
    }
    /// fan-in: collect edges into a product.
    pub fn tuple(&mut self, inputs: Vec<usize>) -> usize {
        self.push(NodeKind::Tuple, inputs)
    }
    /// a unary op consuming earlier nodes by index. `impl Into<O>` lets a layer's
    /// sub-vocabularies (e.g. `Op<NumOp>` or `ArithOp`) be passed without wrapping.
    pub fn add(&mut self, op: impl Into<O>, inputs: Vec<usize>) -> usize {
        self.push(NodeKind::Op(op.into()), inputs)
    }
    pub fn finish(self, output: usize) -> Graph<O> {
        Graph { nodes: self.nodes, output }
    }
}
