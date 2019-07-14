use graphs::Node;

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::input::Input;
use differential_dataflow::collection::AsCollection;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let filename = std::env::args().nth(1).expect("Must supply filename");
        let source = std::env::args().nth(2).expect("Must supply source").parse::<Node>().expect("Could not parse source");
        let target = std::env::args().nth(3).expect("Must supply target").parse::<Node>().expect("Could not parse target");
        let inspect = std::env::args().nth(4).expect("Must supply inspect").parse::<bool>().expect("Could not parse inspect");

        let index = worker.index();
        let peers = worker.peers();
        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\tLoaded {} edges", timer.elapsed(), edges.len());

        let mut probe = timely::dataflow::ProbeHandle::new();
        

        let mut goals =
        worker.dataflow(|scope| {

            let timer = timer.clone();

            let edges =
            edges
                .to_stream(scope)
                .map(|edge| (edge, 0, 1))
                .as_collection();

            let (handle, goals) = scope.new_collection();

            differential_dataflow::algorithms::graphs::bijkstra::bidijkstra(&edges, &goals)
                .filter(move |_| inspect)
                .inspect(move |x| println!("{:?}\tGoals {:?}", timer.elapsed(), x))
                .probe_with(&mut probe)
                ;

            handle
        });

        goals.advance_to(1);
        goals.flush();

        while probe.less_than(goals.time()) {
            worker.step();
        }

        println!("{:?}\tComputation stable", timer.elapsed());

        goals.insert((source, target));
        goals.advance_to(2);
        goals.flush();

        while probe.less_than(goals.time()) {
            worker.step();
        }

        println!("{:?}\tQuery complete", timer.elapsed());

    }).expect("Timely computation failed to start");

}