pub type Node = u32;
pub type Edge<N> = (N,N);
pub type Time = ();
pub type Iter = u32;
pub type Diff = isize;

pub fn load_graph(filename: &str, index: usize, peers: usize) -> Vec<Edge<Node>> {
    let mut results = Vec::new();
    use graph_map::GraphMMap;
    let graph = GraphMMap::new(&filename);
    for node in 0 .. graph.nodes() {
        if node % peers == index {
            for &edge in graph.edges(node) {
                results.push((node as Node, edge as Node));
            }
        }
    }
    results
}