use crate::arguments::Arguments;
use crate::graph::Graph;
use crate::logger::Logger;
use crate::resource_manager::ResourceManager;
use crate::utils::load_json;
use regex::Regex;
use serde_json::Map;
pub use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
use std::process;
use std::process::Command;
use std::time::Instant;

#[derive(Debug)]
struct DagProperties {
    next_tasks: HashMap<isize, Vec<isize>>,
    weights: Vec<(i32, i32)>,
    topological_ordering: Vec<Vec<isize>>,
}

pub struct WorkflowExecutor<'a> {
    arguments: Arguments,
    is_production_mode: bool,
    workflow_file: String,
    action_logger: Logger,
    metric_logger: Logger,
    workflow_spec: Value,
    global_init: Map<String, Value>,
    possiblenexttask: HashMap<isize, Vec<isize>>,
    taskweights: Vec<(i32, i32)>,
    topological_orderings: Vec<Vec<isize>>,
    taskuniverse: Vec<String>,
    id_to_task: Vec<String>,
    task_to_id: HashMap<String, usize>,
    resource_manager: ResourceManager,
    procstatus: HashMap<usize, &'a str>,
    taskneeds: HashMap<String, HashSet<String>>,
    stop_on_failure: bool,
    scheduling_iteration: u32,
    process_list: Vec<String>,
    backfill_process_list: Vec<String>,
    pid_to_psutilsproc: HashMap<u32, String>,
    pid_to_files: HashMap<u32, String>,
    pid_to_connections: HashMap<u32, String>,
    internal_monitor_counter: i32,
    internal_monitor_id: i32,
    tids_marked_to_retry: Vec<i32>,
    retry_counter: Vec<i32>,
    task_retries: Vec<i64>,
    alternative_envs: HashMap<String, HashMap<String, String>>,
    start_time: Option<Instant>,
}

impl<'a> WorkflowExecutor<'a> {
    pub fn new(arguments: Arguments, action_logger: Logger, metric_logger: Logger) -> Self {
        let is_production_mode = arguments.production_mode.clone();
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

        // TODO:
        // visualize workflow

        let possiblenexttask = workflow.next_tasks;
        let taskweights = workflow.weights;
        let topological_orderings = workflow.topological_ordering;
        let taskuniverse: Vec<String> = workflow_spec["stages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["name"].as_str().unwrap().to_string())
            .collect();

        // construct task ID <-> task name lookup
        let mut id_to_task: Vec<String> = vec!["".to_string(); taskuniverse.len()];
        let mut task_to_id: HashMap<String, usize> = HashMap::new();

        for (i, name) in taskuniverse.iter().enumerate() {
            task_to_id.insert(name.clone(), i);
            id_to_task[i] = name.clone();
        }

        if arguments.update_resources.is_some() {
            update_resource_estimates(
                &mut workflow_spec,
                &arguments.update_resources,
                &action_logger,
            );
        }

        // construct the object that is in charge of resource management...
        let mut resource_manager = ResourceManager::new(
            arguments.cpu_limit.clone(),
            arguments.mem_limit.clone(),
            arguments.maxjobs.clone(),
            arguments.dynamic_resources.clone(),
            arguments.optimistic_resources.clone(),
        );

        if let Some(stages) = workflow_spec["stages"].as_array() {
            for task in stages {
                // ...and add all initial resource estimates
                let task = task.as_object().unwrap();
                let name = task["name"].as_str().unwrap();
                let global_task_name = get_global_task_name(name);
                let cpu = task["resources"]["cpu"].as_f64().unwrap();
                let mem = task["resources"]["mem"].as_f64().unwrap();
                let semaphore = task.get("semaphore").map_or("", |v| v.as_str().unwrap());
                resource_manager.add_task_resources(name, &global_task_name, cpu, mem, semaphore);
            }
        }

        let mut procstatus: HashMap<usize, &str> = HashMap::new();

        if let Some(stages) = workflow_spec["stages"].as_array() {
            for (tid, _) in stages.iter().enumerate() {
                procstatus.insert(tid, "ToDo");
            }
        }

        let mut taskneeds = HashMap::new();
        for t in &taskuniverse {
            let requirements = get_requirements(t, &workflow_spec, &task_to_id);
            let requirements_set: HashSet<_> = requirements.into_iter().collect();
            taskneeds.insert(t.clone(), requirements_set);
        }

        let stop_on_failure = !arguments.keep_going.clone();

        print!("Stop on failure: {}", stop_on_failure);

        let mut scheduling_iteration: u32 = 0; // count how often it was tried to schedule new tasks
        let mut process_list: Vec<String> = Vec::new(); // list of currently scheduled tasks with normal priority
        let mut backfill_process_list: Vec<String> = Vec::new(); // list of currently scheduled tasks with low backfill priority (not sure this is needed)
        let mut pid_to_psutilsproc: HashMap<u32, String> = HashMap::new(); // cache of putilsproc for resource monitoring
        let mut pid_to_files: HashMap<u32, String> = HashMap::new(); // we can auto-detect what files are produced by which task (at least to some extent)
        let mut pid_to_connections: HashMap<u32, String> = HashMap::new(); // we can auto-detect what connections are opened by which task (at least to some extent)
        let mut internal_monitor_counter: i32 = 0; // internal use
        let mut internal_monitor_id: i32 = 0; // internal use
        let mut tids_marked_to_retry: Vec<i32> = Vec::new(); // sometimes we might want to retry a failed task (simply because it was "unlucky") and we put them here
        let mut retry_counter: Vec<i32> = vec![0; taskuniverse.len()];
        let task_retries: Vec<i64> = taskuniverse
            .iter()
            .enumerate()
            .map(|(tid, _)| {
                workflow_spec["stages"][tid]
                    .get("retry_count")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0)
            })
            .collect();

        let mut alternative_envs: HashMap<String, HashMap<String, String>> = HashMap::new();
        init_alternative_software_environments(&workflow_spec, &mut alternative_envs);

        WorkflowExecutor {
            arguments,
            is_production_mode,
            workflow_file,
            action_logger,
            metric_logger,
            workflow_spec,
            global_init,
            possiblenexttask,
            taskweights,
            topological_orderings,
            taskuniverse,
            id_to_task,
            task_to_id,
            resource_manager,
            procstatus,
            taskneeds,
            stop_on_failure,
            scheduling_iteration,
            process_list,
            backfill_process_list,
            pid_to_psutilsproc,
            pid_to_files,
            pid_to_connections,
            internal_monitor_counter,
            internal_monitor_id,
            tids_marked_to_retry,
            retry_counter,
            task_retries,
            alternative_envs,
            start_time: None,
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

fn build_graph(taskuniverse: Vec<(serde_json::Value, usize)>) -> (Vec<(isize, isize)>, Vec<isize>) {
    let mut tasktoid: std::collections::HashMap<String, isize> = std::collections::HashMap::new();
    for (i, t) in taskuniverse.iter().enumerate() {
        if let Some(name) = t.0["name"].as_str() {
            tasktoid.insert(name.to_string(), i as isize);
        }
    }

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    for t in &taskuniverse {
        if let Some(name) = t.0["name"].as_str() {
            nodes.push(*tasktoid.get(name).unwrap());
            if let Some(needs) = t.0["needs"].as_array() {
                for n in needs {
                    if let Some(n_str) = n.as_str() {
                        edges.push((tasktoid[n_str], *tasktoid.get(name).unwrap()));
                    }
                }
            }
        }
    }

    (edges, nodes)
}

fn find_all_topological_orders(
    graph: &mut Graph,
    path: &mut Vec<isize>,
    discovered: &mut Vec<bool>,
    n: usize,
    all_paths: &mut Vec<Vec<isize>>,
    max_number: usize,
) {
    if all_paths.len() >= max_number {
        return;
    }

    for v in 0..n {
        if graph.indegree[v] == 0 && !discovered[v] {
            for &u in &graph.adj_list[v] {
                graph.indegree[u as usize] -= 1;
            }

            path.push(v as isize);
            discovered[v] = true;

            find_all_topological_orders(graph, path, discovered, n, all_paths, max_number);

            for &u in &graph.adj_list[v] {
                graph.indegree[u as usize] += 1;
            }

            path.pop();
            discovered[v] = false;
        }
    }

    if path.len() == n {
        all_paths.push(path.clone());
    }
}

fn print_all_topological_orders(
    edges: &[(isize, isize)],
    n: usize,
    max_number: usize,
) -> Vec<Vec<isize>> {
    let mut graph = Graph::new(edges, n);
    let mut discovered = vec![false; n];
    let mut path = vec![];
    let mut all_paths = vec![];

    find_all_topological_orders(
        &mut graph,
        &mut path,
        &mut discovered,
        n,
        &mut all_paths,
        max_number,
    );

    all_paths
}

fn analyse_graph(
    edges: &mut Vec<(isize, isize)>,
    nodes: &mut Vec<isize>,
) -> (Vec<Vec<isize>>, HashMap<isize, Vec<isize>>) {
    let n = nodes.len();

    let mut next_job_trivial: HashMap<isize, Vec<isize>> = HashMap::new();
    for n in nodes.iter() {
        next_job_trivial.insert(*n, Vec::new());
    }

    // start nodes
    let start_nodes: Vec<isize> = nodes.clone();
    next_job_trivial.insert(-1, start_nodes);

    for e in edges.iter() {
        next_job_trivial.get_mut(&e.0).unwrap().push(e.1);
        if let Some(start_nodes) = next_job_trivial.get_mut(&-1) {
            start_nodes.retain(|&x| x != e.1);
        }
    }

    let orderings = print_all_topological_orders(&edges, n, 1);

    (orderings, next_job_trivial)
}

fn find_all_dependent_tasks(
    possible_next_task: &HashMap<isize, Vec<isize>>,
    tid: isize,
    cache: &mut HashMap<isize, Vec<isize>>,
) -> Vec<isize> {
    if let Some(c) = cache.get(&tid) {
        return c.clone();
    }

    let mut daughter_list = vec![tid];
    if let Some(next_tasks) = possible_next_task.get(&tid) {
        for &n in next_tasks {
            let c = if let Some(cached) = cache.get(&n) {
                cached.clone()
            } else {
                find_all_dependent_tasks(possible_next_task, n, cache)
            };
            daughter_list.extend(c.clone());
            cache.insert(n, c);
        }
    }

    cache.insert(tid, daughter_list.clone());
    daughter_list.sort();
    daughter_list.dedup();
    daughter_list
}

fn build_dag_properties(workflow_spec: &Value, action_logger: &Logger) -> DagProperties {
    let global_task_universe: Vec<(Value, usize)> = workflow_spec["stages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, l)| (l.clone(), i))
        .collect();

    let (mut edges, nodes) = build_graph(global_task_universe.clone());
    let (topological_ordering, global_next_tasks) = analyse_graph(&mut edges, &mut nodes.clone());

    let mut dependency_cache = HashMap::new();

    let task_weights: Vec<(i32, i32)> = (0..global_task_universe.len())
        .map(|tid| {
            let timeframe = global_task_universe[tid].0["timeframe"]
                .as_i64()
                .unwrap_or(0) as i32;
            let dependent_tasks =
                find_all_dependent_tasks(&global_next_tasks, tid as isize, &mut dependency_cache)
                    .len() as i32;
            (timeframe, dependent_tasks)
        })
        .collect();
    for tid in 0..global_task_universe.len() {
        action_logger.info(&format!(
            "Score for {} is {:?}",
            global_task_universe[tid].0["name"], task_weights[tid]
        ));
    }

    DagProperties {
        next_tasks: global_next_tasks,
        weights: task_weights,
        topological_ordering,
    }
}

fn update_resource_estimates(
    workflow: &mut Value,
    resource_json: &Option<String>,
    action_logger: &Logger,
) {
    if let Some(resource_json) = resource_json {
        let resource_dict: HashMap<String, Value> = serde_json::from_str(resource_json).unwrap();
        let stages = workflow["stages"].as_array_mut().unwrap();

        for task in stages.iter_mut() {
            let timeframe = task["timeframe"].as_f64().unwrap();
            let mut name = task["name"].as_str().unwrap().to_string();

            if timeframe >= 1.0 {
                let split_name: Vec<&str> = name.split('_').collect();
                name = split_name[..split_name.len() - 1].join("_");
            }

            if let Some(new_resources) = resource_dict.get(&name) {
                // memory
                if let Some(newmem) = new_resources.get("mem") {
                    let oldmem = task["resources"]["mem"].as_f64().unwrap();
                    action_logger.info(&format!(
                        "Updating mem estimate for {} from {} to {}",
                        name, oldmem, newmem
                    ));
                    task["resources"]["mem"] = newmem.clone();
                }

                // cpu
                if let Some(newcpu) = new_resources.get("cpu") {
                    let mut newcpu = newcpu.as_f64().unwrap();
                    let oldcpu = task["resources"]["cpu"].as_f64().unwrap();
                    if let Some(rel_cpu) = task["resources"]["relative_cpu"].as_f64() {
                        newcpu *= rel_cpu;
                    }
                    action_logger.info(&format!(
                        "Updating cpu estimate for {} from {} to {}",
                        name, oldcpu, newcpu
                    ));
                    task["resources"]["cpu"] = Value::from(newcpu);
                }
            }
        }
    }
}

fn get_global_task_name(name: &str) -> String {
    let tokens: Vec<&str> = name.split("_").collect();
    match tokens.last().unwrap().parse::<i32>() {
        Ok(_) => tokens[..tokens.len() - 1].join("_"),
        Err(_) => name.to_string(),
    }
}

fn get_requirements(
    task_name: &str,
    workflowspec: &Value,
    tasktoid: &HashMap<String, usize>,
) -> Vec<String> {
    let mut l = Vec::new();
    let task_id = tasktoid.get(task_name).unwrap();
    let required_tasks = workflowspec["stages"][*task_id]["needs"]
        .as_array()
        .unwrap();

    for required_task in required_tasks {
        let required_task_name = required_task.as_str().unwrap().to_string();
        l.push(required_task_name.clone());
        l.extend(get_requirements(
            &required_task_name,
            workflowspec,
            tasktoid,
        ));
    }

    l
}

fn get_alienv_software_environment(
    packagestring: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let cmd = format!("/cvmfs/alice.cern.ch/bin/alienv printenv {}", packagestring);
    let output = Command::new("sh").arg("-c").arg(&cmd).output()?;

    if !output.stderr.is_empty() {
        println!("{}", String::from_utf8_lossy(&output.stderr));
        return Err("Command execution failed".into());
    }

    let envstring = String::from_utf8(output.stdout)?;
    let tokens: Vec<&str> = envstring.split(";").collect();
    let mut envmap = HashMap::new();

    for t in tokens {
        if t.contains("=") {
            let assignment: Vec<&str> = t.trim().split("=").collect();
            envmap.insert(assignment[0].to_string(), assignment[1].to_string());
        } else if t.contains("export") {
            let variable = t.split_whitespace().nth(1).unwrap();
            if !envmap.contains_key(variable) {
                envmap.insert(variable.to_string(), "".to_string());
            }
        }
    }

    Ok(envmap)
}

fn init_alternative_software_environments(
    workflowspec: &Value,
    alternative_envs: &mut HashMap<String, HashMap<String, String>>,
) {
    let mut environment_cache = HashMap::new();

    if let Value::Object(stages) = &workflowspec["stages"] {
        for (taskid, stage) in stages {
            let packagestr = stage
                .get("alternative_alienv_package")
                .and_then(Value::as_str);
            if let Some(packagestr) = packagestr {
                if !environment_cache.contains_key(packagestr) {
                    let env = get_alienv_software_environment(packagestr).unwrap();
                    environment_cache.insert(packagestr.to_string(), env);
                }
                alternative_envs.insert(taskid.to_string(), environment_cache[packagestr].clone());
            }
        }
    }
}
