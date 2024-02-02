use std::collections::HashMap;

#[derive(Default)]
struct Graph {
    adj_list: HashMap<i32, Vec<i32>>,
    in_degree: HashMap<i32, i32>,
}

impl Graph {
    fn new(edges: &[(i32, i32)], n: usize) -> Self {
        let mut graph = Graph::default();
        for i in 0..n {
            graph.adj_list.insert(i as i32, vec![]);
            graph.in_degree.insert(i as i32, 0);
        }
        for &(src, dest) in edges {
            graph.adj_list.get_mut(&src).unwrap().push(dest);
            *graph.in_degree.get_mut(&dest).unwrap() += 1;
        }
        graph
    }
}
