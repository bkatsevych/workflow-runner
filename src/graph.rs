pub struct Graph {
    pub adj_list: Vec<Vec<isize>>,
    pub indegree: Vec<isize>,
}

impl Graph {
    pub fn new(edges: &[(isize, isize)], n: usize) -> Self {
        let mut adj_list = vec![vec![]; n];
        let mut indegree = vec![0; n];

        for &(src, dest) in edges {
            adj_list[src as usize].push(dest);
            indegree[dest as usize] += 1;
        }

        Graph { adj_list, indegree }
    }
}
