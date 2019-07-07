type Node = u32;

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let timer = worker.timer();

        let mut args = std::env::args();
        args.next();

        let filename = args.next().expect("Must supply a filename");
        let batch_size = args.next().expect("Must supply a batch size").parse::<usize>().expect("Batch size must be an integer");
        let inspect = args.next().expect("Must indicate inspect-y-ness").parse::<bool>().expect("Inspect-y-ness must be a Boolean");
        let round_times = args.next().expect("Must indicate round-y-ness").parse::<bool>().expect("Round-y-ness must be a Boolean");

        let batch_size = batch_size / worker.peers() + if batch_size % worker.peers() > worker.index() { 1 } else { 0 };

        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let mut edges = Vec::new();
        let file = BufReader::new(File::open(filename).expect("Could not open file!"));
        let mut lines = file.lines();
        lines.next();
        lines.next();
        for (count, readline) in lines.enumerate() {
            if count % worker.peers() == worker.index() {
                if let Ok(line) = readline {
                    let mut fields = line.split_whitespace();
                    let src: Node = fields.next().unwrap().parse().unwrap();
                    let dst: Node = fields.next().unwrap().parse().unwrap();
                    fields.next();
                    let time: usize = fields.next().unwrap().parse().unwrap();
                    edges.push((time, src, dst));
                }
            }
        }

        if worker.index() == 0 {
            println!("{:?}\tedges loaded ({})", timer.elapsed(), edges.len());
        }
        edges.sort();
        if worker.index() == 0 {
            println!("{:?}\tedges sorted", timer.elapsed());
        }

        let mut probe = timely::dataflow::ProbeHandle::new();

        // Compute triangles from edges!
        let mut input =
        worker.dataflow(|scope| {

            use differential_dataflow::input::Input;
            use differential_dataflow::operators::consolidate::Consolidate;

            let (handle, collection) = scope.new_collection();

            use differential_dataflow::operators::reduce::Threshold;

            let collection =
            collection
                .map(|(src,dst)| if src < dst { (src, dst) } else { (dst, src) })
                .distinct();

            truss(collection)
                .map(|(_src_dst,count)| count)
                .consolidate()
                .filter(move |_| inspect)
                .inspect(move |x| println!("{:?}\tseen: {:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            handle
        });

        for (count, (time, src, dst)) in edges.into_iter().enumerate() {
            if !round_times { input.advance_to(time); }
            input.insert((src, dst));
            if count % batch_size == 0 {
                if round_times { input.advance_to(count / batch_size); }
                input.flush();
                while probe.less_than(input.time()) {
                    worker.step();
                }
                if worker.index() == 0 {
                    println!("{:?}\tround {} complete\n", timer.elapsed(), count);
                }
            }
        }

        input.close();
        while worker.step() { }
        if worker.index() == 0 {
            println!("{:?}\tcomputation complete", timer.elapsed());
        }

    }).expect("Timely computation failed to start");

}

type Edge = (Node, Node);

use differential_dataflow::Collection;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

// Count, for each edge, of the number of triangles in which it participates.
// Assume edges (src, dst) satisfy src < dst.
fn triangles<G>(edges: Collection<G, Edge>) -> Collection<G, (Edge, Node)>
where
    G: timely::dataflow::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{

    use differential_dataflow::operators::join::Join;
    use differential_dataflow::operators::reduce::Count;

    // join (a,b) and (a,c)
    edges
        .join(&edges)
        .map(|(a,(b,c))| (a,b,c))
        .filter(|(_a,b,c)| b < c)
        .map(|(a,b,c)| ((b,c),a))
        .semijoin(&edges)
        .flat_map(|((b,c),a)|
            vec![(a,b), (a,c), (b,c)]
        )
        .count()
        .map(|(edge, count)| (edge, count as Node))

}

// Determines for each edge a number k such that the edge participates
// in at least k triangles with other edges with label k.
fn truss<G>(edges: Collection<G, Edge>) -> Collection<G, (Edge, Node)>
where
    G: timely::dataflow::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    use differential_dataflow::operators::iterate::Iterate;
    use differential_dataflow::operators::join::Join;
    use differential_dataflow::operators::reduce::Reduce;
    use differential_dataflow::operators::consolidate::Consolidate;

    // TODO: Optimize triangle computation.
    let triangles =
    edges
        .join(&edges)
        .map(|(a,(b,c))| (a,b,c))
        .filter(|(_a,b,c)| b < c)
        .map(|(a,b,c)| ((b,c),a))
        .semijoin(&edges);

    edges
        .map(|edge| (edge, Node::max_value()))
        .iterate(|labels| {

            // labels.map(|(_,lbl)| lbl).consolidate().inspect(|x| println!("\t{:?}", x));

            let triangles = triangles.enter(&labels.scope()).map(|((b,c),a)| (a,b,c));

            triangles.inspect(|x| println!("TRIANGLE: {:?}", x));
            labels.inspect(|x| println!("LABEL: {:?}", x));

            // (a,b,c,min(lbl)) := triangle(a,b,c), label((a,b),lbl), label((a,c),lbl), label((b,c),lbl)
            //
            // d(a,b,c,min(lbl)) :=
            //
            //      d(triangle(a,b,c)), label((a,b),lbl), label((a,c),lbl), label((b,c),lbl)
            //      d(label((a,b),lbl)), triangle(a,b,c), label((a,c),lbl), label((b,c),lbl)
            //      d(label((a,c),lbl)), triangle(a,b,c), label((a,b),lbl), label((b,c),lbl)
            //      d(label((b,c),lbl)), triangle(a,b,c), label((a,b),lbl), label((a,c),lbl)

            use dogsdogsdogs::altneu::AltNeu;
            use dogsdogsdogs::operators::propose;
            // use dogsdogsdogs::operators::validate;

            let labels_by_edge = labels.arrange_by_key();
            let triangles_ab = triangles.map(|(a,b,c)| ((a,b),c)).arrange_by_key();
            let triangles_ac = triangles.map(|(a,b,c)| ((a,c),b)).arrange_by_key();
            let triangles_bc = triangles.map(|(a,b,c)| ((b,c),a)).arrange_by_key();

            let triangle_labels_old =
            triangles
                .map(|(a,b,c)| ((b,c),a))
                .join_map(&labels, |&(b,c),&a,&lbl| ((a,b),(c,lbl)))
                .join_map(&labels, |&(a,b),&(c,lbl1),&lbl2| ((a,c),(b,std::cmp::min(lbl1,lbl2))))
                .join_map(&labels, |&(a,c),&(b,lbl1),&lbl2| ((a,b,c),std::cmp::min(lbl1,lbl2)));

            use timely::dataflow::scopes::Scope;

            let triangle_labels =
            labels
                .scope()
                .scoped::<AltNeu<_>,_,_>("DeltaQuery (Triangles)", |inner| {

                    let labels_by_edge_alt = labels_by_edge.enter_at(inner, |_,_,t| AltNeu::alt(t.clone()));
                    let labels_by_edge_neu = labels_by_edge.enter_at(inner, |_,_,t| AltNeu::neu(t.clone()));

                    let triangles_ab = triangles_ab.enter(inner);
                    let triangles_ac = triangles_ac.enter(inner);
                    let triangles_bc = triangles_bc.enter(inner);

                    //  d(triangle(a,b,c)), label((a,b),lbl), label((a,c),lbl), label((b,c),lbl)
                    let rule0 = triangles_ab.as_collection(|&(a,b),&c| (a,b,c));
                    let rule0 = propose(&rule0, labels_by_edge_neu.clone(), |&(a,b,_)| (a,b));
                    let rule0 = propose(&rule0, labels_by_edge_neu.clone(), |&((a,_,c),_)| (a,c));
                    let rule0 = rule0.map(|(((a,b,c),l1),l2)| ((a,b,c), std::cmp::min(l1,l2)));
                    let rule0 = propose(&rule0, labels_by_edge_neu.clone(), |&((_,b,c),_)| (b,c));
                    let rule0 = rule0.map(|(((a,b,c),l1),l2)| ((a,b,c), std::cmp::min(l1,l2)));

                    //  d(label((a,b),lbl)), triangle(a,b,c), label((a,c),lbl), label((b,c),lbl)
                    let rule1 = labels_by_edge_alt.as_collection(|&(a,b),&l| ((a,b),l));
                    let rule1 = propose(&rule1, triangles_ab, |&((a,b),_)| (a,b));
                    let rule1 = rule1.map(|(((a,b),l),c)| ((a,b,c),l));
                    let rule1 = propose(&rule1, labels_by_edge_neu.clone(), |&((a,_,c),_)| (a,c));
                    let rule1 = rule1.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));
                    let rule1 = propose(&rule1, labels_by_edge_neu.clone(), |&((_,b,c),_)| (b,c));
                    let rule1 = rule1.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));

                    //  d(label((a,c),lbl)), triangle(a,b,c), label((a,b),lbl), label((b,c),lbl)
                    let rule2 = labels_by_edge_alt.as_collection(|&(a,c),&l| ((a,c),l));
                    let rule2 = propose(&rule2, triangles_ac, |&((a,c),_)| (a,c));
                    let rule2 = rule2.map(|(((a,c),l),b)| ((a,b,c),l));
                    let rule2 = propose(&rule2, labels_by_edge_alt.clone(), |&((a,b,_),_)| (a,b));
                    let rule2 = rule2.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));
                    let rule2 = propose(&rule2, labels_by_edge_neu.clone(), |&((_,b,c),_)| (b,c));
                    let rule2 = rule2.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));

                    //  d(label((b,c),lbl)), triangle(a,b,c), label((a,b),lbl), label((a,c),lbl)
                    let rule3 = labels_by_edge_alt.as_collection(|&(b,c),&l| ((b,c),l));
                    let rule3 = propose(&rule3, triangles_bc, |&((b,c),_)| (b,c));
                    let rule3 = rule3.map(|(((b,c),l),a)| ((a,b,c),l));
                    let rule3 = propose(&rule3, labels_by_edge_alt.clone(), |&((a,b,_),_)| (a,b));
                    let rule3 = rule3.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));
                    let rule3 = propose(&rule3, labels_by_edge_alt.clone(), |&((a,_,c),_)| (a,c));
                    let rule3 = rule3.map(|(((a,b,c),l1),l2)| ((a,b,c),std::cmp::min(l1,l2)));

                    rule0.concat(&rule1).concat(&rule2).concat(&rule3).leave()
            })
                .consolidate()
                .inspect(|x| println!("NEW: {:?}", x));


            triangle_labels_old
                .consolidate()
                .inspect(|x| println!("OLD: {:?}", x))
                .negate()
                .concat(&triangle_labels)
                .consolidate()
                // .map(|_| ())
                .inspect(|x| println!("ERROR: {:?}", x));

            triangle_labels_old
                .flat_map(|((a,b,c),lbl)| {
                    vec![((a,b),lbl), ((a,c),lbl), ((b,c),lbl)]
                })
                .reduce(|_edge, input, output| {

                    let mut total = 0;
                    for (label, count) in input.iter().rev() {
                        // total not >= previous label.
                        if total >= **label {
                            output.push((total, 1));
                            return;
                        }
                        total += *count as Node;
                        if total >= **label {
                            output.push((**label, 1));
                            return;
                        }
                    }
                    output.push((total, 1));

                })

        })

}



