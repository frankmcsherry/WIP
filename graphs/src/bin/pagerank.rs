
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;

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
                .map(|edge| (edge, (), 1))
                .as_collection()
                .arrange_by_key()
                ;

            let nodes = 
            edges
                .as_collection(|src, _dst| *src)
                .distinct()
                .arrange_by_self();

            graphs::computations::pagerank::pagerank(20, &nodes, &edges)
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