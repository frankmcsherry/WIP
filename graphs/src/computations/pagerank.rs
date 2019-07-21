use crate::{Node, Edge, Iter, Diff, NodeArrangement, EdgeArrangement};

use timely::dataflow::operators::filter::Filter;
use timely::order::Product;
use differential_dataflow::Collection;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Count;

// Returns a weighted collection in which the weight of each node is proportional
// to its PageRank in the input graph `edges`.
pub fn pagerank<G>(
    iters: Iter, 
    nodes: &NodeArrangement<G>, 
    edges: &EdgeArrangement<G>,
) -> Collection<G, Node, Diff>
where
    G: timely::dataflow::scopes::Scope<Timestamp=()>,
{
    // snag out-degrees for each node.
    let degrs = 
    edges
        .as_collection(|src, _dst| src.clone())
        .count();

    edges.stream.scope().iterative::<Iter,_,_>(|inner| {

        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.as_collection(|src, &()| src.clone()).explode(|node| Some((node, 6_000_000)));
        let reset = nodes.as_collection(|src, &()| src.clone()).explode(|node| Some((node, 1_000_000)));

        // Define a recursive variable to track surfers.
        // We start from `inits` and cycle only `iters`.
        let ranks = Variable::new_from(inits, Product::new(Default::default(), 1));

        // Match each surfer with the degree, scale numbers down.
        let to_push =
        degrs.semijoin(&ranks)
             .threshold(|(_node, degr), rank| (5 * rank) / (6 * degr))
             .map(|(node, _degr)| node);

        // Propagate surfers along links, blend in reset surfers.
        let mut pushed =
        edges.semijoin(&to_push)
             .map(|(_node, dest)| dest)
             .concat(&reset)
             .consolidate();

        if iters > 0 {
            pushed =
            pushed
             .inner
             .filter(move |(_d,t,_r)| t.inner < iters)
             .as_collection();
        }

        // Bind the recursive variable, return its limit.
        ranks.set(&pushed);
        pushed.leave()
    })
}
