use graphs::{Node, Edge, Iter};

use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::map::Map;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let filename = std::env::args().nth(1).expect("Must supply filename");

        let index = worker.index();
        let peers = worker.peers();
        let timer = worker.timer();

        let edges = graphs::load_graph(&filename, index, peers);

        println!("{:?}\tLoaded {} edges", timer.elapsed(), edges.len());

        // let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut as_self, mut forward, mut reverse, mut f_degree, mut r_degree, mut nodes) =
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

            let f_degree = forward.as_collection(|k,_| *k).arrange_by_self();
            let r_degree = reverse.as_collection(|k,_| *k).arrange_by_self();

            // TODO: Look into getting output arrangement from `distinct`.
            let nodes = forward.as_collection(|src,_| *src).distinct().arrange_by_self();

            // as_self.stream.probe_with(&mut probe);
            // forward.stream.probe_with(&mut probe);
            // reverse.stream.probe_with(&mut probe);

            (as_self.trace, forward.trace, reverse.trace, f_degree.trace, r_degree.trace, nodes.trace)
        });

        // Run until graph is fully loaded.
        while worker.step() { }

        println!("{:?}\tData indexed", timer.elapsed());

        use std::io::BufRead;
        let input = std::io::stdin();
        for line in input.lock().lines().map(|x| x.unwrap()) {

            let elts = line[..].split_whitespace().collect::<Vec<_>>();
            match elts[0] {
                "pagerank" => {
                    println!("{:?}\tPagerank started", timer.elapsed());
                    worker.dataflow(|scope| {
                        let nodes = nodes.import(scope);
                        let edges = forward.import(scope);
                        graphs::computations::pagerank::pagerank(20, &nodes, &edges);
                    });
                    while worker.step() { }
                    println!("{:?}\tPagerank computed", timer.elapsed());
                },

                "neighborhood" => {
                    println!("{:?}\tNeighborhoods started", timer.elapsed());
                    let root: Node = elts[1].parse().expect("couldn't parse node");
                    let steps: Iter = elts[2].parse().expect("couldn't parse steps");

                    worker.dataflow(|scope| {
                        let edges = forward.import(scope);
                        let roots = Some(((root, steps), (), 1)).to_stream(scope).as_collection();
                        graphs::computations::neighborhoods::neighborhoods(&edges, roots)
                            .map(|(node,_)| node)
                            .consolidate()
                            .inspect(move |x| println!("{:?}\tNeighbors {:?}", timer.elapsed(), x));
                    });
                    while worker.step() { }
                    println!("{:?}\tNeighborhoods computed", timer.elapsed());
                },

                "components" => {
                    println!("{:?}\tComponents started", timer.elapsed());
                    worker.dataflow(|scope| {
                        use differential_dataflow::operators::reduce::Count;
                        let nodes = nodes.import(scope);
                        let forward = forward.import(scope);
                        let reverse = reverse.import(scope);
                        graphs::computations::connected::connected(&nodes, &forward, &reverse)
                            .map(|(_src,lbl)| lbl)
                            .count()
                            .map(|(_lbl,cnt)| cnt)
                            .consolidate()
                            .inspect(move |x| println!("{:?}\t\tComponent size: {:?}", timer.elapsed(), x));
                    });
                    while worker.step() { }
                    println!("{:?}\tComponents computed", timer.elapsed());
                },

                "reachable" | "sssp" => {
                    println!("{:?}\tReachability started", timer.elapsed());
                    worker.dataflow(|scope| {
                        let source: Node = elts[1].parse().expect("couldn't parse source");
                        let target: Node = elts[2].parse().expect("couldn't parse target");
                        let goals = Some(((source, target), (), 1)).to_stream(scope).as_collection();
                        let forward = forward.import(scope);
                        let reverse = reverse.import(scope);
                        differential_dataflow::algorithms::graphs::bijkstra::bidijkstra_arranged(&forward, &reverse, &goals)
                            .inspect(|x| println!("Goal reached: {:?}", x));
                    });
                    while worker.step() { }
                    println!("{:?}\tReachability computed", timer.elapsed());
                },

                "triangles" => {

                    use dogsdogsdogs::operators::{count, propose, validate};

                    println!("{:?}\tTriangles started", timer.elapsed());
                    worker.dataflow(|scope| {

                        let forward = forward.import(scope);
                        let f_degree = f_degree.import(scope);
                        let as_self = as_self.import(scope);

                        let counts = forward.as_collection(|k,v| ((k.clone(),v.clone()), usize::max_value(), usize::max_value()));

                        let counts = count(&counts, f_degree.clone(), |(a,_b)| *a, 0);
                        let counts = count(&counts, f_degree.clone(), |(_a,b)| *b, 1);

                        use timely::dataflow::operators::partition::Partition;
                        let parts = counts.inner.partition(2, |((p, _, i),t,d)| (i as u64, (p,t,d)));

                        let props0 = propose(&parts[0].as_collection(), forward.clone(), |(a,_b)| *a);
                        let props0 = validate(&props0, as_self.clone(), |(_a,b)| *b);

                        let props1 = propose(&parts[1].as_collection(), forward.clone(), |(_a,b)| *b);
                        let props1 = validate(&props1, as_self.clone(), |(a,_b)| *a);

                        props0.concat(&props1).map(|_| ()).consolidate().inspect(|x| println!("Triangles: {:?}", x));

                    });
                    while worker.step() { }
                    println!("{:?}\tTriangles computed", timer.elapsed());
                },

                x => {
                    println!("Mode {:?} unrecognized; options are:\n
                        pagerank
                        neighborhood <node> <steps>
                        components
                        reachable <source> <target>
                        sssp <source> <target>
                        triangles", x);
                }
            }
        }



    }).expect("Timely computation failed to start");
}