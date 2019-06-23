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
type Time = usize;

enum Event {
    Query(((Node, Node),(Time,Time))),
    Knows(Edge),
    Comms((Text, (Node, Text))),
    Posts((Text, (Node, Forum))),
    Forum((Forum, Time)),
}

fn main() {

    let mut args = std::env::args();
    args.next(); // remove binary name.

    let path = args.next().expect("Must specify path to data files");
    let batch: usize = args.next().expect("Must specify a batch size").parse().expect("Batch size must be an integer");

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = Handle::new();

        let (mut query, mut knows, mut posts, mut comms, mut forum) = worker.dataflow(|scope| {

            // Create various input handles and collections.
            let (query_input, query) = scope.new_collection::<((Node,Node),(Time,Time)),isize>();
            let (knows_input, knows) = scope.new_collection::<Edge,isize>();
            let (posts_input, posts) = scope.new_collection::<(Text, (Node, Forum)),isize>();
            let (comms_input, comms) = scope.new_collection::<(Text, (Node, Text)),isize>();
            let (forum_input, forum) = scope.new_collection::<(Forum, Time),isize>();

            // 1. Determine edges in shortest paths, for each query.
            let goals = query.map(|(goal,_bounds)| goal).distinct();
            let shortest_edges: Collection<_,((Node, Node), Edge)>
                = shortest_paths(&knows, &goals);

            // 2. Score each edge, broken down by the root post.
            let oriented_edges = shortest_edges.map(|(_,(x,y))| {
                let min = std::cmp::min(x, y);
                let max = std::cmp::max(x, y);
                (min, max)
            }).distinct();
            let edge_scores: Collection<_, (Edge, Text)>
                = score_edges(&oriented_edges, &posts, &comms);

            // orient edges in both directions
            let edge_scores = edge_scores.map(|((x,y),f)| ((y,x),f)).concat(&edge_scores);

            // 3. Merge queries and scores, filter by start and end dates.
            let _scored_edges =
            query
                .join_map(&shortest_edges, |&goal, &bounds, &edge| (edge, (goal, bounds)))
                .join_map(&edge_scores, |&edge, &(goal, bounds), &forum| (forum, (goal, edge, bounds)))
                .join_map(&forum, |_forum, &(goal, edge, bounds), &time| 
                    (time >= bounds.0 && time <= bounds.1, goal, edge)
                )
                .filter(|x| x.0)
                .map(|(_, goal, edge)| (goal, edge))
                .concat(&shortest_edges)
                .count()
                .map(|(x,c)| (x,c-1))
                .inspect(|x| println!("SCORED: {:?}", x))
                .probe_with(&mut probe)
                ;

            // let filtered_edges: Collection<_, ((Node, Node), (Edge, usize))>
            //     = unimplemented!();

            // // 4. Reconstruct paths and scores.
            // let scored_paths: Collection<_, ((Node, Node), (Vec<Node>, usize))>
            //     = unimplemented!();

            // 5. Announce massive success!
            // shortest_edges.inspect(|x| println!("WOW:\t{:?}", x));

            (query_input, knows_input, posts_input, comms_input, forum_input)
        });

        let query_data = input::read_query(&format!("{}../substitution_parameters/bi_25_param.txt", path), index, peers);
        let knows_data = input::read_knows(&format!("{}person_knows_person_0_0.csv", path), index, peers);
        let comms_data = input::read_comms(&format!("{}comment_0_0.csv", path), index, peers);
        let posts_data = input::read_posts(&format!("{}post_0_0.csv", path), index, peers);
        let forum_data = input::read_forum(&format!("{}forum_0_0.csv", path), index, peers);

        let mut events = Vec::new();
        for (query, time, diff) in query_data { events.push((Event::Query(query), time, diff)); }
        for (knows, time, diff) in knows_data { events.push((Event::Knows(knows), time, diff)); }
        for (comms, time, diff) in comms_data { events.push((Event::Comms(comms), time, diff)); }
        for (posts, time, diff) in posts_data { events.push((Event::Posts(posts), time, diff)); }
        for (forum, time, diff) in forum_data { events.push((Event::Forum(forum), time, diff)); }
        events.sort_by(|x,y| x.1.cmp(&y.1));

        println!("{:?}\tloaded", timer.elapsed());

        // Advance inputs and settle computation.
        query.advance_to(1); query.flush();
        knows.advance_to(1); knows.flush();
        comms.advance_to(1); comms.flush();
        posts.advance_to(1); posts.flush();
        forum.advance_to(1); forum.flush();
        worker.step_while(|| probe.less_than(query.time()));

        println!("{:?}\tstable", timer.elapsed());

        for (steps, (event, time, diff)) in events.into_iter().enumerate() {

            // Periodically advance and flush inputs.
            if steps % batch == (batch-1) {
                query.advance_to(time); query.flush();
                knows.advance_to(time); knows.flush();
                comms.advance_to(time); comms.flush();
                posts.advance_to(time); posts.flush();
                forum.advance_to(time); forum.flush();
                while probe.less_than(&time) { worker.step(); }
                println!("{:?}\tProcessed {} events", timer.elapsed(), steps);
            }

            // Insert the new events
            match event {
                Event::Query(q) => query.update_at(q, time, diff),
                Event::Knows(k) => knows.update_at(k, time, diff),
                Event::Comms(c) => comms.update_at(c, time, diff),
                Event::Posts(p) => posts.update_at(p, time, diff),
                Event::Forum(f) => forum.update_at(f, time, diff),
            };
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

        short
            .filter(|(_,(x,y))| x != y)
            .leave()
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
    posts: &Collection<G, (Text, (Node, Forum))>,   // (id, author, in_forum)
    comms: &Collection<G, (Text, (Node, Text))>,    // (id, author, reply_to)
) -> Collection<G, (Edge, Forum)>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    // Perhaps a comment links to a post ...
    let comm_post: Collection<_,(Edge,Forum)> =
    comms.map(|(id, (auth, link))| (link, (id, auth)))
         .join_map(&posts, |_post, &(_id,auth_c), &(auth_p, forum)| {
             let min = std::cmp::min(auth_c, auth_p);
             let max = std::cmp::max(auth_c, auth_p);
             ((min, max), forum)
         })
         .semijoin(&edges);

    // Perhaps a comment links to a comment ...
    let comm_comm: Collection<_,(Edge,Text)> =
    comms.map(|(id, (auth, link))| (link, (id, auth)))
         .join_map(&comms, |_comm, &(_id,auth_c), &(auth_p,link)| {
             let min = std::cmp::min(auth_c, auth_p);
             let max = std::cmp::max(auth_c, auth_p);
             ((min, max), link)
         })
         .semijoin(&edges);

    // All comment -> parent links.
    let links =
    comms.map(|(id, (_, link))| (id, link))
         .concat(&posts.map(|(id, _)| (id, id)));

    // unroll 
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
        .consolidate()
        ;

    scores
}

/// Methods for reading LDBC input.
mod input {

    use super::{Node, Text, Edge, Forum, Time};

    /// Reads lines of text into pairs of integers.
    pub fn read_query(filename: &str, index: usize, peers: usize) -> Vec<(((Node, Node),(Time,Time)), Time, isize)> {

        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let mut query = Vec::new();
        let file = BufReader::new(File::open(filename).unwrap());
        let mut lines = file.lines();
        lines.next();
        for (count, readline) in lines.enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split('|');
                    let src: Node = elts.next().expect("line missing src field").parse().expect("malformed src");
                    let dst: Node = elts.next().expect("line missing dst field").parse().expect("malformed dst");
                    let date = elts.next().expect("line missing start date");
                    let start_date = date.parse::<usize>().expect("failed to parse milliseconds");
                    // let mut date = date.to_string();
                    // let len = date.len();
                    // date.insert(len - 2, ':');
                    // println!("Q1PARSING DATE: {:?}", date);
                    // let start_date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
                    let date = elts.next().expect("line missing end date");
                    // let mut date = date.to_string();
                    // let len = date.len();
                    // date.insert(len - 2, ':');
                    // println!("Q2PARSING DATE: {:?}", date);
                    // let end_date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
                    let end_date = date.parse::<usize>().expect("failed to parse milliseconds");
                    query.push((((src, dst), (start_date, end_date)), 1, 1));
                }
            }
        }
        println!("query: read {} lines", query.len());
        query.sort_by(|x,y| x.1.cmp(&y.1));
        query
    }

    /// Reads lines of text into pairs of integers.
    pub fn read_knows(filename: &str, index: usize, peers: usize) -> Vec<(Edge, Time, isize)> {

        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let mut knows = Vec::new();
        let file = BufReader::new(File::open(filename).unwrap());
        let mut lines = file.lines();
        lines.next();
        for (count, readline) in lines.enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split('|');
                    let src: Node = elts.next().expect("line missing src field").parse().expect("malformed src");
                    let dst: Node = elts.next().expect("line missing dst field").parse().expect("malformed dst");
                    let date = elts.next().expect("line missing creation date");
                    let mut date = date.to_string();
                    let len = date.len();
                    date.insert(len - 2, ':');
                    // println!("KPARSING DATE: {:?}", date);
                    let date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
                    knows.push(((src, dst), date, 1));
                }
            }
        }
        println!("knows: read {} lines", knows.len());
        knows.sort_by(|x,y| x.1.cmp(&y.1));
        knows
    }

    /// Reads lines of text into pairs of integers.
    pub fn read_comms(filename: &str, index: usize, peers: usize) -> Vec<((Text, (Node, Text)), Time, isize)> {

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
        for (count, readline) in lines.enumerate() {
            if count % peers == index {
            let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split('|');
                    let id: Text = elts.next().expect("line missing id field").parse().expect("malformed id");
                    let date = elts.next().expect("line missing creation date");
                    let mut date = date.to_string();
                    let len = date.len();
                    date.insert(len - 2, ':');
                    // println!("CPARSING DATE: {:?}", date);
                    let date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
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

                    comms.push(((id, (creator, link)), date, 1));
                }
            }
        }
        println!("comms: read {} lines", comms.len());
        comms.sort_by(|x,y| x.1.cmp(&y.1));
        comms
    }

    /// Reads lines of text into pairs of integers.
    pub fn read_posts(filename: &str, index: usize, peers: usize) -> Vec<((Text, (Node, Forum)), Time, isize)> {

        // Field zero is "id"
        // Field 8 is "creator"
        // Field 9 is "forum" 
        
        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let mut posts = Vec::new();
        let file = BufReader::new(File::open(filename).unwrap());
        let mut lines = file.lines();
        lines.next();
        for (count, readline) in lines.enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split('|');
                    let id: Text = elts.next().expect("line missing id field").parse().expect("malformed id");
                    elts.next();
                    let date = elts.next().expect("line missing creation date");
                    let mut date = date.to_string();
                    let len = date.len();
                    date.insert(len - 2, ':');
                    // println!("PPARSING DATE: {:?}", date);
                    let date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
                    elts.next();
                    elts.next();
                    elts.next();
                    elts.next();
                    elts.next();
                    let creator: Node = elts.next().expect("line missing author field").parse().expect("malformed author");
                    let forum: Forum = elts.next().expect("line missing post field").parse().expect("malformed forum");

                    posts.push(((id, (creator, forum)), date, 1));
                }
            }
        }
        println!("posts: read {} lines", posts.len());
        posts.sort_by(|x,y| x.1.cmp(&y.1));
        posts
    }

    /// Reads lines of text into pairs of integers.
    pub fn read_forum(filename: &str, index: usize, peers: usize) -> Vec<((Forum, Time), Time, isize)> {

        // Field zero is "id"
        // Field two is creation date.
        
        use std::io::{BufRead, BufReader};
        use std::fs::File;

        let mut forum = Vec::new();
        let file = BufReader::new(File::open(filename).unwrap());
        let mut lines = file.lines();
        lines.next();
        for (count, readline) in lines.enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split('|');
                    let id: Text = elts.next().expect("line missing id field").parse().expect("malformed id");
                    elts.next();
                    let date = elts.next().expect("line missing creation date");
                    let mut date = date.to_string();
                    let len = date.len();
                    date.insert(len - 2, ':');
                    // println!("FPARSING DATE: {:?}", date);
                    let date = chrono::DateTime::parse_from_rfc3339(&date).expect("DateTime parse error").timestamp_millis() as usize;
                    forum.push(((id, date), date, 1));
                }
            }
        }
        println!("forum: read {} lines", forum.len());
        forum.sort_by(|x,y| x.1.cmp(&y.1));
        forum
    }
}