extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

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
        let (mut goals, mut graph) = worker.dataflow(|scope| {

            let (goal_input, goals) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            use differential_dataflow::algorithms::graphs::bijkstra::bidijkstra;

            let mut result = bidijkstra(&graph, &goals);

            if !inspect {
                result = result.filter(|_| false);
            }

            result
                // .map(|(_,l)| l)
                .consolidate()
                .inspect(|x| println!("\t{:?}", x))
                .probe_with(&mut probe);

            (goal_input, edge_input)
        });

        let mut seed = [0u8; 32];
        seed[0] = worker.index() as u8;
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        // roots.insert(0);
        // roots.close();

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        goals.advance_to(1); goals.flush();
        graph.advance_to(1); graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        let mut prev = None;
        for round in 0 .. rounds {

            if let Some((src,tgt)) = prev {
                goals.remove((src,tgt));
            }
            goals.insert((round,round+1));
            prev = Some((round,round+1));

            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
                graph.advance_to(2 + round * batch + element);
            }
            graph.flush();
            goals.advance_to(*graph.time());
            goals.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}