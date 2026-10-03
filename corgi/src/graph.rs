//! The core IR, generic over an op vocabulary `O: OpLike`. The graph owns the two
//! *structural* node kinds — `Input` (the parameter) and `Tuple` (the sole fan-in) —
//! and every other node delegates to `O`. A higher layer is simply a richer `O`
//! (e.g. `enum NumOp { Core(CoreOp), Arith(..) }` that embeds this one); the graph,
//! `eval_graph`, and `shape_of` are unchanged across layers.

use crate::shape::{shape_of_value, Shape};
use crate::value::Value;
use std::cell::RefCell;

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
    /// how many times each node's value is read: once per edge into it, and once more for the
    /// output. It depends only on `nodes` and `output`, so it is computed once, when the graph is
    /// built ([`Graph::new`]), rather than on every evaluation. (So a graph's nodes are not edited
    /// in place after it is built; every pass builds a new graph.)
    uses: Box<[usize]>,
}

impl<O> Graph<O> {
    /// a graph from its nodes and its output node, with each node's read count worked out once.
    pub(crate) fn new(nodes: Vec<Node<O>>, output: usize) -> Self {
        let mut uses = vec![0usize; nodes.len()];
        for node in &nodes {
            for &i in &node.inputs {
                // an edge that is not backward is `check`'s error to report, not this one's.
                if let Some(u) = uses.get_mut(i) {
                    *u += 1;
                }
            }
        }
        if let Some(u) = uses.get_mut(output) {
            *u += 1; // the returned value is a use too, so a consumer can't move it out first
        }
        Graph { nodes, output, uses: uses.into() }
    }
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
/// input copy. Backward edges (see [`Graph::check`]) make the per-node consumer count a single pass,
/// made once when the graph is built.
pub fn eval_graph<O: OpLike>(g: &Graph<O>, arg: Value) -> Value {
    try_eval_graph(g, arg).unwrap_or_else(|e| panic!("eval_graph: {e}"))
}

/// [`eval_graph`] with the shape error surfaced: the form the typer and body-bearing ops use.
pub(crate) fn try_eval_graph<O: OpLike>(g: &Graph<O>, arg: Value) -> Result<Value, String> {
    // the working state comes from this thread's stack of spare slot vectors, and goes back to it
    // empty, so an evaluation allocates nothing for its own bookkeeping once the stack has warmed.
    let mut slots = SLOTS.try_with(|s| s.borrow_mut().pop()).ok().flatten().unwrap_or_default();
    let out = eval_slots(g, arg, &mut slots);
    slots.clear(); // drops whatever an early error left behind
    let _ = SLOTS.try_with(|s| s.borrow_mut().push(slots));
    out
}

/// one node's working state during an evaluation: the reads of its value still to come, and the
/// value itself once computed (until its last read moves it out).
struct Slot {
    uses: usize,
    val: Option<Value>,
}

thread_local! {
    /// spare slot vectors, kept empty between evaluations. A stack rather than one vector, because a
    /// body's evaluation runs inside its parent's; one vector per level of nesting is ever in use.
    static SLOTS: RefCell<Vec<Vec<Slot>>> = const { RefCell::new(Vec::new()) };
}

fn eval_slots<O: OpLike>(g: &Graph<O>, arg: Value, slots: &mut Vec<Slot>) -> Result<Value, String> {
    // take node `i`'s value: move it out on its last use, else clone (a cheap `Arc` bump).
    fn take(slots: &mut [Slot], i: usize) -> Value {
        let s = &mut slots[i];
        s.uses -= 1;
        if s.uses == 0 { s.val.take().unwrap() } else { s.val.as_ref().unwrap().clone() }
    }

    let mut arg = Some(arg); // moved into the (single) `Input` node; `take` errors on a second one
    slots.reserve(g.nodes.len());
    for (node, &uses) in g.nodes.iter().zip(&g.uses[..]) {
        let v = match &node.kind {
            NodeKind::Input => arg.take().expect("graph has more than one Input node"),
            NodeKind::Tuple => Value::Prod(node.inputs.iter().map(|&i| take(slots, i)).collect()),
            NodeKind::Op(o) => o.eval(take(slots, node.inputs[0]))?,
        };
        slots.push(Slot { uses, val: Some(v) });
    }
    Ok(slots[g.output].val.take().unwrap())
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
        Graph::new(self.nodes, output)
    }
}
