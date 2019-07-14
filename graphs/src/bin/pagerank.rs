use graphs::{Node, Edge, Iter, Diff};

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::filter::Filter;
use timely::order::Product;
use differential_dataflow::Collection;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Count;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let filename = std::env::args().nth(1).expect("Must supply filename");
        let inspect = std::env::args().nth(2).expect("Must supply inspect").parse::<bool>().expect("Could not parse inspect");

        let index = worker.index();
        let peers = worker.peers();
        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\tLoaded {} edges", timer.elapsed(), edges.len());

        let mut probe = timely::dataflow::ProbeHandle::new();
        
        worker.dataflow(|scope| {

            let timer = timer.clone();

            let edges =
            edges
                .to_stream(scope)
                .map(|edge| (edge, 0, 1))
                .as_collection()
                .map(|(src,dst)| if src < dst { (src,dst) } else { (dst,src) })
                .distinct()
                ;

            pagerank(20, &edges)
                .filter(move |_| inspect)
                .map(|_| ())
                .consolidate()
                .inspect(move |x| println!("{:?}\tTriangles {:?}", timer.elapsed(), x))
                .probe_with(&mut probe)
                ;
        });

        while worker.step() { }

        println!("{:?}\tComputation stable", timer.elapsed());

    }).expect("Timely computation failed to start");

}

// Returns a weighted collection in which the weight of each node is proportional
// to its PageRank in the input graph `edges`.
fn pagerank<G>(iters: Iter, edges: &Collection<G, Edge<Node>, Diff>) -> Collection<G, Node, Diff>
where
    G: timely::dataflow::scopes::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    // initialize many surfers at each node.
    let nodes =
    edges.flat_map(|(x,y)| Some(x).into_iter().chain(Some(y)))
         .distinct();

    // snag out-degrees for each node.
    let degrs = edges.map(|(src,_dst)| src)
                     .count();

    edges.scope().iterative::<Iter,_,_>(|inner| {

        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.explode(|node| Some((node, 6_000_000)));
        let reset = nodes.explode(|node| Some((node, 1_000_000)));

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
