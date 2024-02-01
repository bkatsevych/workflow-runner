use crate::arguments::Arguments;
use crate::logger::Logger;
use crate::utils::load_json;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::process;

pub struct WorkflowExecutor {
    arguments: Arguments,
    is_production_mode: bool,
    workflow_file: String,
    action_logger: Logger,
    metric_logger: Logger,
    workflow_spec: Value,
    global_env: HashMap<String, Value>,
    // possible_next_task: Vec<String>,
    // task_weights: Vec<f32>,
    // topological_orderings: Vec<String>,
    // taskuniverse: Vec<String>,
    // id_to_task: Vec<String>,
    // task_toid: HashMap<String, usize>,
    // resource_manager: ResourceManager,
    // proc_status: HashMap<usize, String>,
    // task_needs: HashMap<String, Vec<String>>,
    // stop_on_failure: bool,
    // scheduling_iteration: usize,
    // process_list: Vec<String>,
    // backfill_process_list: Vec<String>,
    // pid_to_psutilsproc: HashMap<usize, String>,
    // pid_to_files: HashMap<usize, String>,
    // pid_to_connections: HashMap<usize, String>,
    // internal_monitor_counter: usize,
    // internal_monitor_id: usize,
    // tids_marked_to_retry: Vec<usize>,
    // retry_counter: Vec<usize>,
    // task_retries: Vec<usize>,
    // alternative_envs: HashMap<usize, String>,
}

impl WorkflowExecutor {
    pub fn new(arguments: Arguments, action_logger: Logger, metric_logger: Logger) -> Self {
        let is_production_mode = arguments.production_mode;
        let workflow_file: String = arguments.workflow_file.clone();
        let mut workflow_spec = load_json(workflow_file.as_str()).unwrap();
        let init_index = 0; // this has to be the first task in the workflow

        // initialize global environment settings
        let mut global_env = HashMap::new();

        if workflow_spec["stages"][init_index]["name"] == "__global_init_task__" {
            if let Some(env) = workflow_spec["stages"][init_index].get("env") {
                if env.is_object() {
                    for (key, value) in env.as_object().unwrap() {
                        global_env.insert(key.clone(), value.clone());
                    }
                }
            }
            workflow_spec["stages"]
                .as_array_mut()
                .unwrap()
                .remove(init_index);
        }

        for (e, value) in &global_env {
            if env::var(e).is_err() {
                action_logger.info(&format!(
                    "Applying global environment from init section {} : {}",
                    e, value
                ));
                env::set_var(e, value.to_string());
            }
        }

        let target_tasks_as_str = arguments.target_tasks.iter().map(|s| s.as_str()).collect();
        let target_labels_as_str = arguments.target_labels.iter().map(|s| s.as_str()).collect();

        // only keep those tasks that are necessary to be executed based on user's filters
        let tranformed_workflow_spec = filter_workflow(
            &mut workflow_spec,
            target_tasks_as_str,
            target_labels_as_str,
        );

        workflow_spec = tranformed_workflow_spec.clone();

        if workflow_spec["stages"].as_array().unwrap().is_empty() {
            if arguments.target_tasks.is_empty() {
                println!("Apparently some of the chosen target tasks are not in the workflow");
                process::exit(0);
            }
            println!("Workflow is empty. Nothing to do");
            process::exit(0);
        }

        WorkflowExecutor {
            arguments,
            is_production_mode,
            workflow_file,
            action_logger,
            metric_logger,
            workflow_spec,
            global_env,
            // possible_next_task: workflow.nexttasks,
            // task_weights: workflow.weights,
            // topological_orderings: workflow.topological_ordering,
            // taskuniverse,
            // id_to_task,
            // task_to_id,
            // resource_manager,
            // proc_status,
            // task_needs,
            // stop_on_failure,
            // scheduling_iteration: 0,
            // process_list: vec![],
            // backfill_process_list: vec![],
            // pid_to_psutilsproc: HashMap::new(),
            // pid_to_files: HashMap::new(),
            // pid_to_connections: HashMap::new(),
            // internal_monitor_counter: 0,
            // internal_monitor_id: 0,
            // tids_marked_to_retry: vec![],
            // retry_counter,
            // task_retries,
            // alternative_envs,
        }
    }
}

fn filter_workflow<'a>(
    workflow_spec: &'a mut Value,
    target_tasks: Vec<&str>,
    target_labels: Vec<&str>,
) -> &'a mut Value {
    if target_tasks.is_empty() {
        return workflow_spec;
    }
    if target_labels.is_empty() && target_tasks.len() == 1 && target_tasks[0] == "*" {
        return workflow_spec;
    }

    let mut task_name_to_id: HashMap<&str, usize> = HashMap::new();
    let stages = workflow_spec["stages"].as_array().unwrap();
    for (i, t) in stages.iter().enumerate() {
        let name = t["name"].as_str().unwrap();
        task_name_to_id.insert(name, i);
    }

    let mut ok_cache: HashMap<&str, bool> = HashMap::new();
    let full_target_list: Vec<&Value> = stages
        .iter()
        .filter(|t| {
            let name = t["name"].as_str().unwrap();
            task_matches(name, &target_tasks)
                && task_matches_labels(t, &target_labels)
                && can_be_done(t, &task_name_to_id, &mut ok_cache, &workflow_spec)
        })
        .collect();

    let full_target_name_list: Vec<&str> = full_target_list
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    let full_requirements_list: Vec<Vec<&Value>> = full_target_list
        .iter()
        .map(|t| get_all_requirements(t, &task_name_to_id, &workflow_spec))
        .collect();

    let full_requirements_name_list: Vec<&str> = full_requirements_list
        .into_iter()
        .flatten()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    workflow_spec["stages"] = Value::Array(
        stages
            .iter()
            .filter(|l| {
                needed_by_targets(
                    l["name"].as_str().unwrap(),
                    &full_target_name_list,
                    &full_requirements_name_list,
                )
            })
            .cloned()
            .collect(),
    );

    workflow_spec
}

fn task_matches(t: &str, target_tasks: &Vec<&str>) -> bool {
    target_tasks
        .iter()
        .any(|&filt| filt == "*" || Regex::new(filt).unwrap().is_match(t))
}

fn task_matches_labels(t: &Value, target_labels: &Vec<&str>) -> bool {
    if target_labels.is_empty() {
        return true;
    }

    let labels = t["labels"].as_array().unwrap();
    labels
        .iter()
        .any(|l| target_labels.contains(&l.as_str().unwrap()))
}

fn can_be_done<'a>(
    t: &'a Value,
    task_name_to_id: &HashMap<&str, usize>,
    ok_cache: &mut HashMap<&'a str, bool>,
    workflow_spec: &'a Value,
) -> bool {
    let name = t["name"].as_str().unwrap();
    if let Some(&ok) = ok_cache.get(name) {
        return ok;
    }

    let needs = t["needs"].as_array().unwrap();
    let ok = needs.iter().all(|r| {
        let task_id = task_name_to_id.get(r.as_str().unwrap());
        if let Some(&task_id) = task_id {
            can_be_done(
                &workflow_spec["stages"][task_id],
                task_name_to_id,
                ok_cache,
                workflow_spec,
            )
        } else {
            false
        }
    });

    ok_cache.insert(name, ok);
    ok
}

fn get_all_requirements<'a>(
    t: &'a Value,
    task_name_to_id: &'a HashMap<&'a str, usize>,
    workflow_spec: &'a Value,
) -> Vec<&'a Value> {
    let mut l = Vec::new();
    let needs = t["needs"].as_array().unwrap();
    for r in needs {
        let fulltask = &workflow_spec["stages"][task_name_to_id[r.as_str().unwrap()]];
        l.push(fulltask);
        l.extend(get_all_requirements(
            fulltask,
            task_name_to_id,
            workflow_spec,
        ));
    }
    l
}

fn needed_by_targets(
    name: &str,
    full_target_name_list: &Vec<&str>,
    full_requirements_name_list: &Vec<&str>,
) -> bool {
    full_target_name_list.contains(&name) || full_requirements_name_list.contains(&name)
}
