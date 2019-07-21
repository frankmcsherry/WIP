use crate::{Node, Edge, Iter, EdgeArrangement};

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::consolidate::Consolidate;

pub fn neighborhoods<G>(edges: &EdgeArrangement<G>, source: Collection<G, (Node, Iter)>) -> Collection<G, (Node, Node)> 
where
    G: timely::dataflow::scopes::Scope<Timestamp=()>,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    let source = source.map(|(node,steps)| (node, (node, steps)));

    source
        .iterate(|inner| {
            let edges = edges.enter(&inner.scope());
            let source = source.enter(&inner.scope());
            inner
                .filter(|(_node,(_root, steps))| steps > &0)
                // .join_map(&edges, |_node, &(source,steps), &dest| (dest, (source, steps-1)))
                .join_core(&edges, |_node, &(source,steps), &dest| Some((dest, (source, steps-1))))
                .concat(&source)
                .distinct()
        })
        .map(|(dest,(node,_))| (node, dest))
        .distinct()
}