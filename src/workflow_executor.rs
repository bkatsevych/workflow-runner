use crate::arguments::Arguments;
use crate::graph::Graph;
use crate::logger::Logger;
use crate::utils::load_json;
use regex::Regex;
use serde_json::Map;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
// use std::iter::FromIterator;
use std::process;

#[derive(Debug)]
struct DAGProperties {
    nexttasks: HashMap<usize, Vec<usize>>,
    weights: Vec<(i32, usize)>,
    topological_ordering: Vec<Vec<usize>>,
}

pub struct WorkflowExecutor {
    arguments: Arguments,
    is_production_mode: bool,
    workflow_file: String,
    action_logger: Logger,
    metric_logger: Logger,
    workflow_spec: Value,
    global_init: Map<String, Value>,
}

impl WorkflowExecutor {
    pub fn new(arguments: Arguments, action_logger: Logger, metric_logger: Logger) -> Self {
        let is_production_mode = arguments.production_mode;
        let workflow_file: String = arguments.workflow_file.clone();
        let mut workflow_spec = load_json(workflow_file.as_str()).unwrap();
        let global_init = extract_global_environment(&mut workflow_spec);

        for (e, value) in global_init["env"].as_object().unwrap().iter() {
            if env::var(e).is_err() {
                action_logger.info(&format!(
                    "Applying global environment from init section {} : {}",
                    e, value
                ));
                env::set_var(e, value.to_string()); // Convert value to String before setting as environment variable
            }
        }

        // only keep those tasks that are necessary to be executed based on user's filters
        workflow_spec = filter_workflow(
            workflow_spec,
            arguments.target_tasks.clone(),
            arguments.target_labels.clone(),
        );

        if workflow_spec["stages"].is_null() {
            if !arguments.target_tasks.is_empty() {
                println!("Apparently some of the chosen target tasks are not in the workflow");
                process::exit(0);
            }
            println!("Workflow is empty. Nothing to do");
            process::exit(0);
        }

        // // construct the DAG, compute task weights
        let workflow = build_dag_properties(&workflow_spec);

        println!("Workflow: {:?}", workflow);

        WorkflowExecutor {
            arguments,
            is_production_mode,
            workflow_file,
            action_logger,
            metric_logger,
            workflow_spec,
            global_init,
        }
    }
}

fn extract_global_environment(workflow_spec: &mut Value) -> Map<String, Value> {
    let init_index = 0; // this has to be the first task in the workflow
    let mut global_env = serde_json::Map::new();
    let mut init_cmd = None;

    if workflow_spec["stages"][init_index]["name"] == "__global_init_task__" {
        if let Some(env) = workflow_spec["stages"][init_index].get("env") {
            if let Some(env_map) = env.as_object() {
                for (key, value) in env_map {
                    global_env.insert(key.clone(), value.clone());
                }
            }
        }

        if let Some(cmd) = workflow_spec["stages"][init_index].get("cmd") {
            if cmd != "NO-COMMAND" {
                init_cmd = Some(cmd.clone());
            }
        }

        workflow_spec["stages"]
            .as_array_mut()
            .unwrap()
            .remove(init_index);
    }

    let mut result = serde_json::Map::new();
    result.insert("env".to_string(), Value::Object(global_env));
    result.insert("cmd".to_string(), init_cmd.unwrap_or(Value::Null));
    result
}

fn can_be_done(
    t: &Value,
    workflow_spec: &Value,
    tasknametoid: &HashMap<String, usize>,
    cache: &mut HashMap<String, bool>,
) -> bool {
    let c = cache.get(&t["name"].to_string());
    if let Some(cached) = c {
        return *cached;
    }
    let mut ok = true;
    for r in t["needs"].as_array().unwrap() {
        let taskid = tasknametoid.get(&r.to_string());
        if let Some(id) = taskid {
            if !can_be_done(
                &workflow_spec["stages"].as_array().unwrap()[*id],
                workflow_spec,
                tasknametoid,
                cache,
            ) {
                ok = false;
                break;
            }
        } else {
            ok = false;
            break;
        }
    }
    cache.insert(t["name"].to_string(), ok);
    ok
}

fn get_all_requirements<'a>(
    t: &'a Value,
    workflow_spec: &'a Value,
    tasknametoid: &'a HashMap<String, usize>,
) -> Vec<&'a Value> {
    let mut _l: Vec<&'a Value> = Vec::new();
    for r in t["needs"].as_array().unwrap() {
        let fulltask = &workflow_spec["stages"].as_array().unwrap()[tasknametoid[&r.to_string()]];
        _l.push(fulltask);
        _l.extend(get_all_requirements(fulltask, workflow_spec, tasknametoid));
    }
    _l
}

fn filter_workflow(
    mut workflow_spec: Value,
    target_tasks: Vec<String>,
    target_labels: Vec<String>,
) -> Value {
    if target_tasks.is_empty() {
        return workflow_spec;
    }
    if target_labels.is_empty() && target_tasks.len() == 1 && target_tasks[0] == "*" {
        return workflow_spec;
    }

    let task_matches = |t: &str| {
        for filt in &target_tasks {
            if filt == "*" {
                return true;
            }
            if Regex::new(filt).unwrap().is_match(t) {
                return true;
            }
        }
        false
    };

    let task_matches_labels = |t: &Value| {
        if target_labels.is_empty() {
            return true;
        }

        for l in t["labels"].as_array().unwrap() {
            if target_labels.contains(&l.to_string()) {
                return true;
            }
        }
        false
    };

    let mut tasknametoid: HashMap<String, usize> = HashMap::new();
    for (i, t) in workflow_spec["stages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        tasknametoid.insert(t["name"].to_string(), i);
    }

    let mut cache: HashMap<String, bool> = HashMap::new();

    let full_target_list: Vec<&Value> = workflow_spec["stages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            task_matches(&t["name"].to_string())
                && task_matches_labels(t)
                && can_be_done(t, &workflow_spec, &tasknametoid, &mut cache)
        })
        .collect();

    let full_target_name_list: Vec<String> = full_target_list
        .iter()
        .map(|t| t["name"].to_string())
        .collect();

    let full_requirements_list: Vec<Vec<&Value>> = full_target_list
        .iter()
        .map(|t| get_all_requirements(t, &workflow_spec, &tasknametoid))
        .collect();

    let full_requirements_name_list: Vec<String> = full_requirements_list
        .iter()
        .flat_map(|sublist| sublist.iter().map(|item| item["name"].to_string()))
        .collect();

    let needed_by_targets = |name: &str| -> bool {
        full_target_name_list.contains(&name.to_string())
            || full_requirements_name_list.contains(&name.to_string())
    };

    workflow_spec["stages"] = Value::Array(
        workflow_spec["stages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|l| needed_by_targets(&l["name"].to_string()))
            .cloned()
            .collect(),
    );

    workflow_spec
}

fn find_all_topological_orders(
    graph: &mut Graph,
    path: &mut Vec<usize>,
    discovered: &mut Vec<bool>,
    n: usize,
    all_paths: &mut Vec<Vec<usize>>,
    max_number: usize,
) {
    if all_paths.len() >= max_number {
        return;
    }

    for v in 0..n {
        if graph.indegree[v] == 0 && !discovered[v] {
            for &u in &graph.adj_list[v] {
                graph.indegree[u] -= 1;
            }

            path.push(v);
            discovered[v] = true;

            find_all_topological_orders(graph, path, discovered, n, all_paths, max_number);

            for &u in &graph.adj_list[v] {
                graph.indegree[u] += 1;
            }

            path.pop();
            discovered[v] = false;
        }
    }

    if path.len() == n {
        all_paths.push(path.clone());
    }
}

fn print_all_topological_orders(graph: &mut Graph, max_number: usize) -> Vec<Vec<usize>> {
    let n = graph.adj_list.len();
    let mut discovered = vec![false; n];
    let mut path = vec![];
    let mut all_paths = vec![];

    find_all_topological_orders(
        graph,
        &mut path,
        &mut discovered,
        n,
        &mut all_paths,
        max_number,
    );

    all_paths
}

fn analyse_graph(
    edges: &[(usize, usize)],
    nodes: &mut Vec<usize>,
) -> (Vec<Vec<usize>>, HashMap<usize, Vec<usize>>) {
    let n = nodes.len();
    let mut next_job_trivial = HashMap::new();
    next_job_trivial.insert(-1i32, nodes.clone());

    for &(src, dest) in edges {
        next_job_trivial
            .entry(src as i32)
            .or_insert(vec![])
            .push(dest);
        if let Some(pos) = next_job_trivial
            .get_mut(&-1i32)
            .unwrap()
            .iter()
            .position(|&x| x == dest)
        {
            next_job_trivial.get_mut(&-1i32).unwrap().remove(pos);
        }
    }

    let mut graph = Graph::new(edges, n);
    let orderings = print_all_topological_orders(&mut graph, 1);

    let next_job_trivial: HashMap<usize, Vec<usize>> = next_job_trivial
        .into_iter()
        .map(|(k, v)| (k as usize, v))
        .collect();

    (orderings, next_job_trivial)
}

fn build_graph(task_universe: &[(serde_json::Value, usize)]) -> (Vec<(usize, usize)>, Vec<usize>) {
    let task_to_id: HashMap<String, usize> = task_universe
        .iter()
        .enumerate()
        .map(|(i, (t, _))| (t["name"].as_str().unwrap().to_string(), i))
        .collect();

    let mut nodes = vec![];
    let mut edges = vec![];

    for (t, _) in task_universe {
        let node = task_to_id[t["name"].as_str().unwrap()];
        nodes.push(node);

        if let Some(needs) = t["needs"].as_array() {
            for n in needs {
                let edge = (task_to_id[n.as_str().unwrap()], node);
                edges.push(edge);
            }
        }
    }

    (edges, nodes)
}

fn find_all_dependent_tasks(
    possiblenexttask: &HashMap<usize, Vec<usize>>,
    tid: usize,
    cache: &mut HashMap<usize, Vec<usize>>,
) -> Vec<usize> {
    if let Some(c) = cache.get(&tid) {
        return c.clone();
    }

    let mut daughterlist = vec![tid];
    if let Some(next_tasks) = possiblenexttask.get(&tid) {
        for &n in next_tasks {
            let mut c = if let Some(cached) = cache.get(&n) {
                cached.clone()
            } else {
                find_all_dependent_tasks(possiblenexttask, n, cache)
            };
            daughterlist.append(&mut c);
            cache.insert(n, c.clone());
        }
    }

    cache.insert(tid, daughterlist.clone());
    daughterlist.sort();
    daughterlist.dedup();
    daughterlist
}

fn build_dag_properties(workflow_spec: &Value) -> DAGProperties {
    let mut globaltaskuniverse = Vec::new();

    if let Some(stages) = workflow_spec["stages"].as_array() {
        for (i, l) in stages.iter().enumerate() {
            globaltaskuniverse.push((l.clone(), i + 1));
        }
    }

    let (edges, nodes) = build_graph(&globaltaskuniverse);
    let tup = analyse_graph(&edges, &mut nodes.clone());

    let global_next_tasks = tup.1;
    let mut dependency_cache = HashMap::new();

    fn get_weight(
        tid: usize,
        globaltaskuniverse: &Vec<(Value, usize)>,
        global_next_tasks: &HashMap<usize, Vec<usize>>,
        dependency_cache: &mut HashMap<usize, Vec<usize>>,
    ) -> (i32, usize) {
        let timeframe = globaltaskuniverse[tid].0["timeframe"].as_i64().unwrap() as i32;
        let dependent_tasks = find_all_dependent_tasks(global_next_tasks, tid, dependency_cache);
        (timeframe, dependent_tasks.len())
    }

    let task_weights: Vec<(i32, usize)> = (0..globaltaskuniverse.len())
        .map(|tid| {
            get_weight(
                tid,
                &globaltaskuniverse,
                &global_next_tasks,
                &mut dependency_cache,
            )
        })
        .collect();

    for tid in 0..globaltaskuniverse.len() {
        println!(
            "Score for {} is {:?}",
            globaltaskuniverse[tid].0["name"], task_weights[tid]
        );
    }

    DAGProperties {
        nexttasks: global_next_tasks,
        weights: task_weights,
        topological_ordering: tup.0,
    }
}
