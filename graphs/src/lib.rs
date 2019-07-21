pub type Node = u32;
pub type Edge<N> = (N,N);
pub type Time = ();
pub type Iter = u32;
pub type Diff = isize;

pub mod computations;

use std::rc::Rc;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdKeyBatch;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;

type EdgeTrace = Spine<Node, Node, (), Diff, Rc<OrdValBatch<Node, Node, (), Diff>>>;
type NodeTrace = Spine<Node, (), (), Diff, Rc<OrdKeyBatch<Node, (), Diff>>>;
type EdgeArrangement<G> = Arranged<G, TraceAgent<EdgeTrace>>;
type NodeArrangement<G> = Arranged<G, TraceAgent<NodeTrace>>;

// type TraceHandle = TraceAgent<GraphTrace>;

pub fn load_graph(filename: &str, index: usize, peers: usize) -> Vec<Edge<Node>> {
    let mut results = Vec::new();
    use graph_map::GraphMMap;
    let graph = GraphMMap::new(&filename);
    for node in 0 .. graph.nodes() {
        if node % peers == index {
            for &edge in graph.edges(node) {
                results.push((node as Node, edge as Node));
            }
        }
    }
    results
}