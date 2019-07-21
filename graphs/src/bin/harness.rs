use graphs::{Node, Edge, Iter};

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::Collection;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::reduce::Reduce;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let filename = std::env::args().nth(1).expect("Must supply filename");

        let index = worker.index();
        let peers = worker.peers();
        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\tLoaded {} edges", timer.elapsed(), edges.len());

        // let mut probe = timely::dataflow::ProbeHandle::new();
        
        let (mut as_self, mut forward, mut reverse, mut nodes) =
        worker.dataflow(|scope| {

            // let timer = timer.clone();
            let edges =
            edges
                .to_stream(scope)
                .map(|edge| (edge, (), 1))
                .as_collection()
                ;

            // arrange the edge relation three ways.
            let as_self = edges.arrange_by_self();
            let forward = edges.arrange_by_key();
            let reverse = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                               .arrange_by_key();

            // TODO: Look into getting output arrangement from `distinct`.
            let nodes = forward.as_collection(|src,_| *src).distinct().arrange_by_self();

            // as_self.stream.probe_with(&mut probe);
            // forward.stream.probe_with(&mut probe);
            // reverse.stream.probe_with(&mut probe);

            (as_self.trace, forward.trace, reverse.trace, nodes.trace)
        });

        // Run until graph is fully loaded.
        while worker.step() { }

        println!("{:?}\tData indexed", timer.elapsed());

        worker.dataflow(|scope| {
            let nodes = nodes.import(scope);
            let edges = forward.import(scope);
            graphs::computations::pagerank::pagerank(20, &nodes, &edges);
        });

        while worker.step() { }
        println!("{:?}\tPagerank computed", timer.elapsed());

        worker.dataflow(|scope| {
            let steps = std::env::args().nth(2).expect("Must supply steps").parse::<Iter>().expect("Could not parse steps");
            let source = std::env::args().nth(3).expect("Must supply source").parse::<Node>().expect("Could not parse source");

            let edges = forward.import(scope);
            let source = Some(((source, steps), (), 1)).to_stream(scope).as_collection();
            graphs::computations::neighborhoods::neighborhoods(&edges, source)
                .map(|(node,_)| node)
                .consolidate()
                .inspect(move |x| println!("{:?}\tNeighbors {:?}", timer.elapsed(), x));
        });

        while worker.step() { }
        println!("{:?}\tNeighborhoods computed", timer.elapsed());

        worker.dataflow(|scope| {
            use differential_dataflow::operators::reduce::Count;
            let nodes = nodes.import(scope);
            let forward = forward.import(scope);
            let reverse = reverse.import(scope);
            // let timer = timer.clone();
            graphs::computations::connected::connected(&nodes, &forward, &reverse)
                .map(|(_src,lbl)| lbl)
                .count()
                .map(|(_lbl,cnt)| cnt)
                .consolidate()
                .inspect(move |x| println!("{:?}\t\tComponent size: {:?}", timer.elapsed(), x));
        });

        while worker.step() { }
        println!("{:?}\tComponents computed", timer.elapsed());

    }).expect("Timely computation failed to start");
}