use graphs::{Node, Edge, Iter};

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let filename = std::env::args().nth(1).expect("Must supply filename");
        let steps = std::env::args().nth(2).expect("Must supply steps").parse::<Iter>().expect("Could not parse steps");
        let source = std::env::args().nth(3).expect("Must supply source").parse::<Node>().expect("Could not parse source");
        let inspect = std::env::args().nth(4).expect("Must supply inspect").parse::<bool>().expect("Could not parse inspect");

        let index = worker.index();
        let peers = worker.peers();
        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\tLoaded {} edges", timer.elapsed(), edges.len());

        let mut probe = timely::dataflow::ProbeHandle::new();
        
        let mut sources =
        worker.dataflow(|scope| {

            let timer = timer.clone();

            let edges =
            edges
                .to_stream(scope)
                .map(|edge| (edge, (), 1))
                .as_collection()
                .arrange_by_key();

            let (handle, source) = scope.new_collection();

            graphs::computations::neighborhoods::neighborhoods(&edges, source)
                .filter(move |_| inspect)
                .map(|(node,_)| node)
                .consolidate()
                .inspect(move |x| println!("{:?}\tNeighbors {:?}", timer.elapsed(), x))
                .probe_with(&mut probe)
                ;

            handle
        });
        
        sources.insert((source, steps));

        while worker.step() {}

        println!("{:?}\tQuery complete", timer.elapsed());

    }).expect("Timely computation failed to start");

}