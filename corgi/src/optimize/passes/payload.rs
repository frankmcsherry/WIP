//! Move compact, nonduplicating structural wiring ahead of a single-use filter.
use crate::graph::{node_shapes, shape_of, shape_of_op, Builder, Graph, Node, NodeKind};
use crate::ops::{NumOp, Op};
use crate::shape::Shape;

#[derive(Clone)]
enum Wire {
    Path(Vec<usize>, Shape),
    Tuple(Vec<Wire>),
    Unit,
}
impl Wire {
    fn shape(&self) -> Shape {
        match self {
            Self::Path(_, s) => s.clone(),
            Self::Tuple(xs) => Shape::Prod(xs.iter().map(Self::shape).collect()),
            Self::Unit => Shape::Unit,
        }
    }
    fn field(&self, i: usize) -> Option<Self> {
        match self {
            Self::Tuple(xs) => xs.get(i).cloned(),
            Self::Path(path, Shape::Prod(xs)) => {
                let mut path = path.clone(); path.push(i);
                Some(Self::Path(path, xs.get(i)?.clone()))
            }
            _ => None,
        }
    }
    fn erase(&self, target: &Shape) -> Option<Self> {
        if self.shape() == *target { return Some(self.clone()); }
        if *target == Shape::Unit { return Some(Self::Unit); }
        let Shape::Prod(fields) = target else { return None; };
        let Shape::Prod(source) = self.shape() else { return None; };
        if fields.len() != source.len() { return None; }
        Some(Self::Tuple(fields.iter().enumerate().map(|(i, s)| self.field(i)?.erase(s)).collect::<Option<_>>()?))
    }
    fn paths(&self, paths: &mut Vec<Vec<usize>>) -> bool {
        match self {
            Self::Path(path, _) => {
                if paths.iter().any(|p| p.starts_with(path) || path.starts_with(p)) { return false; }
                paths.push(path.clone()); true
            }
            Self::Tuple(xs) => xs.iter().all(|x| x.paths(paths)),
            Self::Unit => true,
        }
    }
    fn emit(&self, b: &mut Builder<NumOp>, input: usize, rows: usize) -> usize {
        match self {
            Self::Path(path, _) => path.iter().fold(input, |x, &i| b.add(Op::Field(i), vec![x])),
            Self::Unit => b.add(Op::Unit, vec![rows]),
            Self::Tuple(xs) => {
                let fields = xs.iter().map(|x| x.emit(b, input, rows)).collect(); b.tuple(fields)
            }
        }
    }
}

// Empty products have exceptional row-count behavior. Leave these to the ordinary evaluator.
fn regular(s: &Shape) -> bool {
    match s {
        Shape::Prod(xs) => !xs.is_empty() && xs.iter().all(regular),
        Shape::Sum(xs) => xs.iter().all(regular),
        Shape::List(x) | Shape::Ref(x) => regular(x),
        _ => true,
    }
}
fn size(s: &Shape) -> usize {
    match s {
        Shape::Prod(xs) | Shape::Sum(xs) => 1 + xs.iter().map(size).sum::<usize>(),
        Shape::List(x) | Shape::Ref(x) => 1 + size(x),
        Shape::Unit => 0,
        _ => 1,
    }
}
fn wiring(g: &Graph<NumOp>, input: &Shape) -> Option<Wire> {
    if !regular(input) { return None; }
    let g = super::dce(&super::peephole(g));
    let mut wires: Vec<Wire> = Vec::new();
    for n in &g.nodes {
        let w = match &n.kind {
            NodeKind::Input => Wire::Path(vec![], input.clone()),
            NodeKind::Tuple => Wire::Tuple(n.inputs.iter().map(|&i| wires[i].clone()).collect()),
            NodeKind::Op(NumOp::Core(Op::Field(i))) => wires[n.inputs[0]].field(*i)?,
            NodeKind::Op(NumOp::Core(Op::Unit)) => Wire::Unit,
            NodeKind::Op(NumOp::Core(Op::Project(s))) => wires[n.inputs[0]].erase(s)?,
            _ => return None,
        };
        if !regular(&w.shape()) { return None; }
        wires.push(w);
    }
    let w = wires[g.output].clone();
    (size(&w.shape()) < size(input) && w.paths(&mut Vec::new())).then_some(w)
}

/// Compact a filter's payload at its producer. No projection nodes or discarded
/// tuple placeholders are introduced, and repeated selected fields are left after
/// the filter so they continue sharing the filtered column.
pub fn compact_filter_payloads(g: &Graph<NumOp>, input: &Shape) -> Result<Graph<NumOp>, String> {
    let old_shapes = node_shapes(g, input)?;
    let expected = &old_shapes[g.output];
    let mut uses = vec![0usize; g.nodes.len()];
    for n in &g.nodes { for &i in &n.inputs { uses[i] += 1; } }
    uses[g.output] += 1;
    let mut nodes: Vec<Node<NumOp>> = Vec::new();
    let mut shapes: Vec<Shape> = Vec::new();
    let mut remap = Vec::new();
    for (old, n) in g.nodes.iter().enumerate() {
        let inputs: Vec<usize> = n.inputs.iter().map(|&i| remap[i]).collect();
        let mut kind = n.kind.clone();
        if let NodeKind::Op(NumOp::Core(op)) = &mut kind {
            let source = &shapes[inputs[0]];
            match op {
                Op::MapList(body) => {
                    let Shape::List(element) = source else { unreachable!() };
                    **body = compact_filter_payloads(body, element)?;
                }
                Op::MapSum(arms) => {
                    let Shape::Sum(lanes) = source else { unreachable!() };
                    for (tag, body) in arms { *body = compact_filter_payloads(body, &lanes[*tag])?; }
                }
                Op::Fold(body) | Op::FoldScan(body) => {
                    let Shape::Prod(pair) = source else { unreachable!() };
                    let Shape::List(element) = &pair[1] else { unreachable!() };
                    **body = compact_filter_payloads(body, &Shape::Prod(vec![pair[0].clone(), *element.clone()]))?;
                }
                _ => {}
            }
        }
        let shape = old_shapes[old].clone();
        let mut replacement = None;
        if let NodeKind::Op(NumOp::Core(Op::MapList(body))) = &kind {
            let filtered = inputs[0];
            let old_filter = n.inputs[0];
            let producer_is_private = g.nodes[old_filter].inputs.first().is_some_and(|&source| uses[source] == 1);
            if uses[old_filter] == 1 && producer_is_private
                && matches!(nodes[filtered].kind, NodeKind::Op(NumOp::Core(Op::Filter))) {
                let Shape::List(payload) = &shapes[filtered] else { unreachable!() };
                if let Some(w) = wiring(body, payload) {
                    let mut b = Builder::default();
                    let x = b.input();
                    let mask = b.add(Op::Field(0), vec![x]);
                    let value = b.add(Op::Field(1), vec![x]);
                    let compact = w.emit(&mut b, value, mask);
                    let out = b.tuple(vec![mask, compact]);
                    let producer = nodes[filtered].inputs[0];
                    let op = NumOp::Core(Op::MapList(Box::new(b.finish(out))));
                    let mapped_shape = shape_of_op(&op, &shapes[producer])?;
                    let mapped = nodes.len();
                    nodes.push(Node { kind: NodeKind::Op(op), inputs: vec![producer] });
                    shapes.push(mapped_shape);
                    replacement = Some(Node { kind: NodeKind::Op(Op::Filter.into()), inputs: vec![mapped] });
                }
            }
        }
        remap.push(nodes.len());
        nodes.push(replacement.unwrap_or(Node { kind, inputs }));
        shapes.push(shape);
    }
    let result = super::cleanup(&super::fuse_maps(&super::dce(&Graph { nodes, output: remap[g.output] })));
    if shape_of(&result, input)? != *expected { return Err("filter payload compaction changed output shape".into()); }
    Ok(result)
}
