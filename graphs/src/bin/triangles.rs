use graphs::{Node, Edge};

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

            triangles(&edges)
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


fn triangles<G>(edges: &Collection<G, Edge<Node>>) -> Collection<G, (Node, Node, Node)>
where 
    G: timely::dataflow::scopes::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice+std::hash::Hash+Ord 
{
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
