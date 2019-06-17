extern crate differential_dataflow;
extern crate timely;

use timely::order::Product;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::Collection;

type Node = usize;  // identifies a person.
type Text = usize;  // identifies a post or comment.
type Forum = usize; // identifies a forum.
type Edge = (Node, Node);

fn main() {

    let mut args = std::env::args();
    args.next(); // remove binary name.

    let path = args.next().expect("Must specify path to data files");
    let inspect: bool = args.next().expect("Must indicate inspect-y-ness").parse().expect("Inspect argument must be boolean");

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = worker.timer();
        let mut probe = Handle::new();

        let (mut query, mut knows, mut posts, mut comms, mut forum) = worker.dataflow(|scope| {

            // Create various input handles and collections.
            let (query_input, query) = scope.new_collection::<(Node,Node,usize,usize),isize>();
            let (knows_input, knows) = scope.new_collection::<Edge,isize>();
            let (posts_input, posts) = scope.new_collection::<(Text, (Node, Forum)),isize>();
            let (comms_input, comms) = scope.new_collection::<(Text, (Node, Text)),isize>();
            let (forum_input, forum) = scope.new_collection::<(Forum, usize),isize>();

            // 1. Determine edges in shortest paths, for each query.
            let goals = query.map(|(src,dst,_,_)| (src,dst));
            let shortest_edges: Collection<_,((Node, Node), Edge)>
                = shortest_paths(&knows, &goals)
                    .inspect(|x| println!("SHORTEDGE: {:?}", x))
                ;

            // 2. Score each edge, broken down by the root post.
            let relevant_edges = shortest_edges.map(|(_,x)| x).distinct();
            let mut edge_scores: Collection<_, (Edge, Text)>
                = score_edges(&relevant_edges, &posts, &comms)
                    .inspect(|x| println!("EDGESCORE: {:?}", x))
                ;

            // // 3. Merge queries and scores, filter by start and end dates.
            // let filtered_edges: Collection<_, ((Node, Node), (Edge, usize))>
            //     = unimplemented!();

            // // 4. Reconstruct paths and scores.
            // let scored_paths: Collection<_, ((Node, Node), (Vec<Node>, usize))>
            //     = unimplemented!();

            // 5. Announce massive success!
            // shortest_edges.inspect(|x| println!("WOW:\t{:?}", x));

            let mut result = edge_scores.clone();
            if !inspect {
                result = result.filter(|_| false);
            }

            result
                .inspect(move |x| println!("{:?}\tWOW\t{:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            (query_input, knows_input, posts_input, comms_input, forum_input)
        });

        let filename = format!("{}comment_0_0.csv", path);
        for comm in read_comms(&filename) {
            comms.insert(comm);
        }

        let filename = format!("{}post_0_0.csv", path);
        for post in read_posts(&filename) {
            posts.insert(post);
        }

        


        posts.close();
        comms.close();
        forum.close();

        let mut sources = std::collections::HashSet::new();
        let mut targets = std::collections::HashSet::new();
        if worker.index() == 0 {
            let filename = format!("{}person_knows_person_0_0.csv", path);
            let edges = read_edges(&filename);
            let edges_len = edges.len();
            for (src,dst) in edges {
                sources.insert(src);
                targets.insert(dst);
                knows.insert((src,dst));
            }
            println!("performing Bi-directional Dijkstra on ({},{}) nodes, {} edges:", sources.len(), targets.len(), edges_len);
        }

        println!("{:?}\tloaded", timer.elapsed());

        query.advance_to(1); query.flush();
        knows.advance_to(1); knows.flush();
        worker.step_while(|| probe.less_than(query.time()));

        println!("{:?}\tstable", timer.elapsed());

        let mut prev = None;
        for (round, (src, tgt)) in sources.into_iter().zip(targets).enumerate() {
            println!("{:?}\tquery ({} -> {})", timer.elapsed(), src, tgt);
            query.insert((src, tgt,0,0));
            if let Some((src,tgt)) = prev {
                query.remove((src,tgt,0,0));
            }
            prev = Some((src, tgt));
            query.advance_to(round + 2); query.flush();
            knows.advance_to(round + 2); knows.flush();
            worker.step_while(|| probe.less_than(query.time()));
            println!("{:?}\tround {}", timer.elapsed(), round);
        }

        println!("finished; elapsed: {:?}", timer.elapsed());
    })
    .unwrap();
}

/// Describes shortest paths between query node pairs.
///
/// The `bijkstra` method takes as input a collection of edges (described as pairs
/// of source and target node) and a collection of query node pairs (again, source
/// and target nodes). Its intent is to describe the set of shortest paths between
/// each pair of query nodes.
///
/// The method's output describes a set of directed edges for each input query pair.
/// The set of directed edges are the directed acyclic graph of edges on shortest
/// paths from the source of the query pair to the target of the query pair.
/// There may be multiple paths, and the paths may fork apart and merge together.
fn shortest_paths<G>(
    edges: &Collection<G, Edge>,
    goals: &Collection<G, (Node, Node)>,
) -> Collection<G, ((Node, Node), (Node, Node))>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    // Iteratively develop reachability information.
    edges.scope().iterative::<u64, _, _>(|inner| {

        // Our plan is to start evolving distances from both sources and destinations.
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        let goals = goals.enter(inner);
        let edges = edges.enter(inner);

        // forward: ((mid1,mid2), (src, len)) can be read as
        //      src -len-> mid1 -> mid2 is a shortest path from src to mid2.
        let forward = Variable::new(inner, Product::new(Default::default(), 1));
        // reverse: ((mid1,mid2), (dst, len)) can be read as
        //      mid1 -> mid2 -len-> dst is a shortest path from mid1 to dst.
        let reverse = Variable::new(inner, Product::new(Default::default(), 1));

        // reached((src, dst), (mid1, mid2)) can be read as
        //      src -*-> mid1 -> mid2 -*-> dst is a shortest path.
        let reached: Collection<_, ((Node, Node), (Node, Node))> =
        forward
            .join_map(&reverse, |&(m1,m2), &(src,len1), &(dst,len2)| {
                ((src, dst), (len1 + len2, (m1,m2)))
            })
            .semijoin(&goals)
            .reduce(|&_src_dst, source, target| {
                let min_len = (source[0].0).0;
                for &(&(len,edge),_wgt) in source.iter() {
                    if len == min_len {
                        target.push((edge, 1));
                    }
                }
            });

        // reached.inspect(|x| println!("\tREACHED\t{:?}", x));

        // Subtract from goals any goal pairs that can reach each other.
        let active =
        reached
            .map(|((src,dst),_mid)| (src,dst))
            .distinct()
            .negate()
            .concat(&goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x, _y)| x).distinct();
        let forward_next = forward
            .map(|((_mid0,mid1), (src, len))| (src, (mid1, len)))
            .semijoin(&forward_active)
            .map(|(src, (mid1, len))| (mid1, (src, len)))
            .join_map(&edges, |&mid1, &(src, len), &mid2| {
                ((mid1,mid2), (src, len + 1))
            })
            .concat(&*forward)
            .map(|((mid1,mid2),(src,len))| ((mid2,src),(len,mid1)))
            .reduce(|_key, s, t| {
                let min_len = (s[0].0).0;
                for (&(len,mid1), _weight) in s.iter() {
                    if len == min_len {
                        t.push(((len, mid1), 1));
                    }
                }
            })
            .map(|((mid2, src), (len, mid1))| ((mid1,mid2),(src,len)))
            ;

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x, y)| y).distinct();
        let reverse_next = reverse
            .map(|((mid1,_mid2), (rev, len))| (rev, (mid1, len)))
            .semijoin(&reverse_active)
            .map(|(rev, (mid1, len))| (mid1, (rev, len)))
            .join_map(&edges.map(|(x, y)| (y, x)), |&mid1, &(rev, len), &mid0| {
                ((mid0,mid1), (rev, len + 1))
            })
            .concat(&reverse)
            .map(|((mid0,mid1),(rev,len))| ((mid0,rev),(len,mid1)))
            // .map(|(edge, (rev, len))| ((edge, rev), len))
            .reduce(|_key, s, t| {
                let min_len = (s[0].0).0;
                for (&(len,mid1), _weight) in s.iter() {
                    if len == min_len {
                        t.push(((len, mid1), 1));
                    }
                }
            })
            .map(|((mid0, rev), (len, mid1))| ((mid0,mid1), (rev, len)));

        // `reached` are edges on a shortest path;
        // we want to unwind them back to their roots.

        // reached((src, dst), (mid1, mid2)) means that
        //      src -*-> mid1 -> mid2 -*-> dst is a shortest path.

        // forward_back(src, dst, )
        let shortest = Variable::new(inner, Product::new(Default::default(), 1));

        let forward_dag = forward.map(|((mid1,mid2),(src,_len))| ((src,mid2),mid1));
        let reverse_dag = reverse.map(|((mid1,mid2),(dst,_len))| ((dst,mid1),mid2));

        let short_forward =
        shortest
            .map(|((src,dst),(mid1,_mid2))| ((src,mid1),dst))
            .join_map(&forward_dag, |&(src,mid1),&dst,&mid0| ((src,dst),(mid0,mid1)));

        let short_reverse =
        shortest
            .map(|((src,dst),(_mid0,mid1))| ((dst,mid1),src))
            .join_map(&reverse_dag, |&(dst,mid1),&src,&mid2| ((src,dst),(mid1,mid2)));

        let short =
        short_forward
            .concat(&short_reverse)
            .concat(&reached)
            .distinct();

        shortest.set(&short);

        forward.set(&forward_next.concat(&goals.map(|(x, _)| ((x,x),(x,0)))));
        reverse.set(&reverse_next.concat(&goals.map(|(_, y)| ((y,y),(y,0)))));

        short.leave()
    })
}

/// Assigns a score to each edge, based on posts and comments.
///
/// This method assigns an integer score to each edge, where an edge (src, tgt) gets
/// one point for each reply by either src or tgt to a comment of the other, and two
/// points for each reply by either src or tgt to a post of the other.
///
/// The result type is a multiset of (Edge, Post), where the multiplicity of
/// each record indicates the score the edge derives from the post.
/// One may filter by post and accumulate to get a final score.
fn score_edges<G>(
    edges: &Collection<G, Edge>,                    // (source, target)
    posts: &Collection<G, (Text, (Node, Forum))>,    // (id, author, in_forum)
    comms: &Collection<G, (Text, (Node, Text))>,    // (id, author, reply_to)
) -> Collection<G, (Edge, Forum)>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    // We have a cyclic join to perform:
    // Counts(post1, post2) :=
    //   edges(auth1, auth2), post(post1, auth1, _), post(post2, auth2, Some(post1)),
    //   edges(auth2, auth1), post(post1, auth1, _), post(post2, auth2, Some(post1)).

    // We "know" that the link relation is a primary key, and so will not expand the
    // set of results. Therefore, we first join on it, and later join with edges.

    let comm_post: Collection<_,(Edge,Forum)> =
    comms.map(|(id, (auth, link))| (link, (id, auth)))
         .join_map(&posts, |_post, &(_id,auth_c), &(auth_p, forum)|
             ((auth_c, auth_p), forum)
         )
         .semijoin(&edges);

    let comm_comm: Collection<_,(Edge,Text)> =
    comms.map(|(id, (auth, link))| (link, (id, auth)))
         .join_map(&comms, |_comm, &(_id,auth_c), &(auth_p,link)|
             ((auth_c, auth_p), link)
         )
         .semijoin(&edges);

    let links =
    comms.map(|(id, (_, link))| (id, link))
         .concat(&posts.map(|(id, _)| (id, id)));

    let scores =
    comm_comm
        .map(|(edge, link)| (link, edge))
        .iterate(|scores|
            links.enter(&scores.scope())
                 .join_map(&scores, |_src, &dst, &edge| (dst, edge))
        )
        .join_map(&posts, |_post,&edge,&(_, forum)| (edge, forum))
        .concat(&comm_post)
        .concat(&comm_post)
        ;

    scores
}

/// Reads lines of text into pairs of integers.
fn read_edges(filename: &str) -> Vec<(Node, Node)> {

    // let mut mapping = std::collections::HashMap::new();

    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut graph = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    let mut lines = file.lines();
    lines.next();
    for readline in lines {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let mut elts = line[..].split('|');
            let src: Node = elts.next().expect("line missing src field").parse().expect("malformed src");
            let dst: Node = elts.next().expect("line missing dst field").parse().expect("malformed dst");
            // let len = mapping.len();
            // let src = *mapping.entry(src).or_insert(len);
            // let len = mapping.len();
            // let dst = *mapping.entry(dst).or_insert(len);
            graph.push((src, dst));
        }
    }
    println!("knows: read {} lines", graph.len());
    graph
}

/// Reads lines of text into pairs of integers.
fn read_comms(filename: &str) -> Vec<(Text, (Node, Text))> {

    // Field zero is "id"
    // Field six is "creator"
    // Fields 8/9 reply_of_post and reply_of_comment 

    // let mut mapping = std::collections::HashMap::new();

    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut comms = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    let mut lines = file.lines();
    lines.next();
    for readline in lines {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let mut elts = line[..].split('|');
            let id: Text = elts.next().expect("line missing id field").parse().expect("malformed id");
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            let creator: Node = elts.next().expect("line missing author field").parse().expect("malformed author");
            elts.next();
            let post = elts.next().expect("line missing post field");
            let comment = elts.next().expect("line missing comment field");

            let link =
            if let Ok(post) = post.parse::<Text>() {
                post
            } else if let Ok(comment) = comment.parse::<Text>() {
                comment
            }
            else {
                panic!("Failed to parse either parent post or comment");
            };

            comms.push((id, (creator, link)));
        }
    }
    println!("comms: read {} lines", comms.len());
    comms
}

/// Reads lines of text into pairs of integers.
fn read_posts(filename: &str) -> Vec<(Text, (Node, Forum))> {

    // Field zero is "id"
    // Field 8 is "creator"
    // Field 9 is "forum" 
    
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut posts = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    let mut lines = file.lines();
    lines.next();
    for readline in lines {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let mut elts = line[..].split('|');
            let id: Text = elts.next().expect("line missing id field").parse().expect("malformed id");
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            elts.next();
            let creator: Node = elts.next().expect("line missing author field").parse().expect("malformed author");
            let forum: Forum = elts.next().expect("line missing post field").parse().expect("malformed forum");

            posts.push((id, (creator, forum)));
        }
    }
    println!("posts: read {} lines", posts.len());
    posts
}
