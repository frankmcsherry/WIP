extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let mut result = bfs(&graph, &roots);

            if !inspect {
                result = result.filter(|_| false);
            }

            result.map(|(_,l)| l)
                  .consolidate()
                  .inspect(|x| println!("\t{:?}", x))
                  .probe_with(&mut probe);

            (root_input, edge_input)
        });

        let mut seed = [0u8; 32];
        seed[0] = worker.index() as u8;
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        roots.advance_to(1); roots.flush();
        graph.advance_to(1); graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        let mut prev = None;
        for round in 0 .. rounds {

            if let Some(src) = prev {
                roots.remove(src);
            }
            roots.insert(round);
            prev = Some(round);

            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
                graph.advance_to(2 + round * batch + element);
            }
            graph.flush();
            roots.advance_to(*graph.time());
            roots.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}