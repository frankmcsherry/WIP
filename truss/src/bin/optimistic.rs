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

// Determines for each edge a number k such that the edge participates
// in at least k triangles with other edges with label k.
fn truss<G>(edges: Collection<G, Edge>) -> Collection<G, (Edge, Node)>
where
    G: timely::dataflow::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{
    use differential_dataflow::operators::iterate::Iterate;
    // use differential_dataflow::operators::join::Join;
    use differential_dataflow::operators::reduce::Reduce;
    // use differential_dataflow::operators::consolidate::Consolidate;

    // TODO: Optimize triangle computation.
    let triangles = triangles(&edges).map(|(a,b,c)| ((b,c),a));

    edges
        .map(|edge| (edge, Node::max_value()))
        .iterate(|labels| {


            use differential_dataflow::operators::join::JoinCore;

            let labels_by_edge = labels.arrange_by_key();

            triangles
                .enter(&labels.scope())
                .join_core(&labels_by_edge, |&(b,c),&a,&lbl| Some(((a,b),(c,lbl))))
                .join_core(&labels_by_edge, |&(a,b),&(c,lbl1),&lbl2| Some(((a,c),(b,std::cmp::min(lbl1,lbl2)))))
                .join_core(&labels_by_edge, |&(a,c),&(b,lbl1),&lbl2| Some(((a,b,c),std::cmp::min(lbl1,lbl2))))
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

fn triangles<G>(edges: &Collection<G, Edge>) -> Collection<G, (Node, Node, Node)>
where
    G: timely::dataflow::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
    // G::Timestamp: Lattice+Hash+Ord,
{

use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
use differential_dataflow::operators::reduce::Reduce;

    // only use forward-pointing edges.
    let edges = edges.filter(|&(src, dst)| src < dst);

    // arrange the edge relation three ways.
    let as_self = edges.arrange_by_self();
    let forward = edges.arrange_by_key();
    let reverse = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                       .arrange_by_key();

    // arrange the count of extensions from each source.
    let counts = edges.map(|(src, _dst)| src)
                      .arrange_by_self();

    // extract ((src, dst), idx) tuples with weights equal to the number of extensions.
    let cand_count1 = forward.join_core(&counts, |&src, &dst, &()| Some(((src, dst), 1)));
    let cand_count2 = reverse.join_core(&counts, |&dst, &src, &()| Some(((src, dst), 2)));

    // determine for each (src, dst) tuple which index would propose the fewest extensions.
    let winners = cand_count1.concat(&cand_count2)
                             .reduce(|_srcdst, counts, output| {
                                 if counts.len() == 2 {
                                     let mut min_cnt = isize::max_value();
                                     let mut min_idx = usize::max_value();
                                     for &(&idx, cnt) in counts.iter() {
                                         if min_cnt > cnt {
                                             min_idx = idx;
                                             min_cnt = cnt;
                                         }
                                     }
                                     output.push((min_idx, 1));
                                 }
                             });

    // select tuples with the first relation minimizing the proposals, join, then intersect.
    let winners1 = winners.flat_map(|((src, dst), index)| if index == 1 { Some((src, dst)) } else { None })
                          .join_core(&forward, |&src, &dst, &ext| Some(((dst, ext), src)))
                          .join_core(&as_self, |&(dst, ext), &src, &()| Some(((dst, ext), src)))
                          .map(|((dst, ext), src)| (src, dst, ext));

    // select tuples with the second relation minimizing the proposals, join, then intersect.
    let winners2 = winners.flat_map(|((src, dst), index)| if index == 2 { Some((dst, src)) } else { None })
                          .join_core(&forward, |&dst, &src, &ext| Some(((src, ext), dst)))
                          .join_core(&as_self, |&(src, ext), &dst, &()| Some(((src, ext), dst)))
                          .map(|((src, ext), dst)| (src, dst, ext));

    // collect and return results.
    winners1.concat(&winners2)
}
