pub struct Graph {
    pub adj_list: Vec<Vec<usize>>,
    pub indegree: Vec<usize>,
}

impl Graph {
    pub fn new(edges: &[(usize, usize)], n: usize) -> Self {
        let mut adj_list = vec![vec![]; n];
        let mut indegree = vec![0; n];

        for &(src, dest) in edges {
            adj_list[src].push(dest);
            indegree[dest] += 1;
        }

        Graph { adj_list, indegree }
    }
}
