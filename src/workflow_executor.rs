use crate::arguments::Arguments;
use crate::graph::Graph;
use crate::logger::Logger;
use crate::utils::load_json;
use regex::Regex;
use serde_json::Map;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
use std::iter::FromIterator;
use std::process;

pub struct WorkflowExecutor {
    arguments: Arguments,
    is_production_mode: bool,
    workflow_file: String,
    action_logger: Logger,
    metric_logger: Logger,
    workflow_spec: Value,
    global_init: HashMap<String, Value>,
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

        // construct the DAG, compute task weights
        let workflow = build_dag_properties(&workflow_spec, &action_logger);

        println!("{:?}", workflow);

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

fn extract_global_environment(workflow_spec: &mut Value) -> HashMap<String, Value> {
    let init_index = 0; // this has to be the first task in the workflow
    let mut globalenv = HashMap::new();
    let mut initcmd = None;

    if workflow_spec["stages"][init_index]["name"] == "__global_init_task__" {
        let env = workflow_spec["stages"][init_index].get("env").cloned();
        if let Some(e) = env {
            if let Some(e) = e.as_object() {
                globalenv = e.clone().into_iter().collect();
            }
        }
        let cmd = workflow_spec["stages"][init_index].get("cmd").cloned();
        if cmd != Some(Value::String("NO-COMMAND".to_string())) {
            initcmd = cmd;
        }

        workflow_spec["stages"]
            .as_array_mut()
            .unwrap()
            .remove(init_index);
    }

    let mut result = HashMap::from_iter(globalenv.clone().into_iter());
    result.insert(
        "env".to_string(),
        Value::Object(serde_json::Map::from_iter(globalenv.clone().into_iter())),
    );
    if let Some(cmd) = initcmd {
        result.insert("cmd".to_string(), cmd);
    }

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

    let mut full_target_list: Vec<&Value> = workflow_spec["stages"]
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

fn find_all_dependent_tasks(
    possiblenexttask: &HashMap<String, Vec<String>>,
    tid: &str,
    cache: &mut HashMap<String, Vec<String>>,
) -> Vec<String> {
    if let Some(c) = cache.get(tid) {
        return c.clone();
    }

    let mut daughterlist = vec![tid.to_string()];
    if let Some(tasks) = possiblenexttask.get(tid) {
        for n in tasks {
            let mut c = if let Some(cached) = cache.get(n) {
                cached.clone()
            } else {
                find_all_dependent_tasks(possiblenexttask, n, cache)
            };
            daughterlist.append(&mut c);
            cache.insert(n.to_string(), c);
        }
    }

    cache.insert(tid.to_string(), daughterlist.clone());
    daughterlist.sort();
    daughterlist.dedup();
    daughterlist
}

fn find_all_topological_orders(
    graph: &mut Graph,
    path: &mut Vec<usize>,
    discovered: &mut Vec<bool>,
    n: usize,
    allpaths: &mut Vec<Vec<usize>>,
    maxnumber: usize,
) {
    if allpaths.len() >= maxnumber {
        return;
    }

    for v in 0..n {
        if graph.indegree[v] == 0 && !discovered[v] {
            for u in &graph.adj_list[v] {
                graph.indegree[*u] -= 1;
            }

            path.push(v);
            discovered[v] = true;

            find_all_topological_orders(graph, path, discovered, n, allpaths, maxnumber);

            for u in &graph.adj_list[v] {
                graph.indegree[*u] += 1;
            }

            path.pop();
            discovered[v] = false;
        }
    }

    if path.len() == n {
        allpaths.push(path.clone());
    }
}

fn print_all_topological_orders(graph: &mut Graph, maxnumber: usize) -> Vec<Vec<usize>> {
    let n = graph.adj_list.len();
    let mut discovered = vec![false; n];
    let mut path = Vec::new();
    let mut allpaths = Vec::new();

    find_all_topological_orders(
        graph,
        &mut path,
        &mut discovered,
        n,
        &mut allpaths,
        maxnumber,
    );

    allpaths
}

fn analyse_graph(
    edges: &[(usize, usize)],
    nodes: &[usize],
) -> (Vec<Vec<usize>>, HashMap<usize, Vec<usize>>) {
    let n = nodes.len();
    let mut nextjobtrivial = HashMap::new();
    nextjobtrivial.insert(usize::MAX, nodes.to_vec());

    for &(src, dest) in edges {
        nextjobtrivial
            .entry(src)
            .or_insert_with(Vec::new)
            .push(dest);
        if let Some(vec) = nextjobtrivial.get_mut(&usize::MAX) {
            vec.retain(|&x| x != dest);
        }
    }

    let mut graph = Graph::new(edges, n);
    let orderings = print_all_topological_orders(&mut graph, 1);

    (orderings, nextjobtrivial)
}

fn build_graph(taskuniverse: &[(Value, usize)]) -> (Vec<(usize, usize)>, Vec<usize>) {
    let tasktoid: HashMap<String, usize> = taskuniverse
        .iter()
        .map(|(l, i)| (l["name"].as_str().unwrap().to_string(), *i))
        .collect();

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for (l, _) in taskuniverse {
        let name = l["name"].as_str().unwrap();
        nodes.push(tasktoid[name]);

        if let Some(needs) = l["needs"].as_array() {
            for n in needs {
                let n_str = n.as_str().unwrap();
                edges.push((tasktoid[n_str], tasktoid[name]));
            }
        }
    }

    (edges, nodes)
}

fn build_dag_properties(workflowspec: &Value, action_logger: &Logger) -> HashMap<String, Value> {
    let globaltaskuniverse: Vec<(Value, usize)> = workflowspec["stages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, l)| (l.clone(), i))
        .collect();

    let (edges, nodes) = build_graph(&globaltaskuniverse);
    let (orderings, global_next_tasks) = analyse_graph(&edges, &nodes);

    let global_next_tasks_str: HashMap<String, Vec<String>> = global_next_tasks
        .iter()
        .map(|(k, v)| (k.to_string(), v.iter().map(|i| i.to_string()).collect()))
        .collect();

    let mut dependency_cache = HashMap::new();

    fn getweight(
        tid: usize,
        globaltaskuniverse: &[(Value, usize)],
        global_next_tasks: &HashMap<String, Vec<String>>,
        dependency_cache: &mut HashMap<String, Vec<String>>,
    ) -> (Value, usize) {
        let timeframe = globaltaskuniverse[tid].0["timeframe"].clone();
        let dependent_tasks =
            find_all_dependent_tasks(global_next_tasks, &tid.to_string(), dependency_cache);
        (timeframe, dependent_tasks.len())
    }

    let task_weights: Vec<(Value, usize)> = (0..globaltaskuniverse.len())
        .map(|tid| {
            getweight(
                tid,
                &globaltaskuniverse,
                &global_next_tasks_str,
                &mut dependency_cache,
            )
        })
        .collect();

    for tid in 0..globaltaskuniverse.len() {
        action_logger.info(&format!(
            "Score for {} is {:?}",
            globaltaskuniverse[tid].0["name"], task_weights[tid]
        ));
    }

    let mut result = HashMap::new();
    result.insert(
        "nexttasks".to_string(),
        serde_json::to_value(global_next_tasks_str).unwrap(),
    );
    result.insert(
        "weights".to_string(),
        serde_json::to_value(task_weights).unwrap(),
    );
    result.insert(
        "topological_ordering".to_string(),
        serde_json::to_value(orderings).unwrap(),
    );

    result
}
