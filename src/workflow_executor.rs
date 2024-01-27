use crate::arguments::Arguments;
use crate::resource_manager::ResourceManager;
use crate::utils::load_json;
use crate::workflow_spec::WorkflowSpec;
use std::collections::HashMap;

pub struct WorkflowExecutor {
    args: Arguments,
    is_production_mode: bool,
    workflow_file: String,
    workflow_spec: WorkflowSpec,
    global_env: HashMap<String, String>,
    possible_next_task: Vec<String>,
    task_weights: Vec<f32>,
    topological_orderings: Vec<String>,
    task_universe: Vec<String>,
    id_to_task: Vec<String>,
    task_toid: HashMap<String, usize>,
    resource_manager: ResourceManager,
    proc_status: HashMap<usize, String>,
    task_needs: HashMap<String, Vec<String>>,
    stop_on_failure: bool,
    scheduling_iteration: usize,
    process_list: Vec<String>,
    backfill_process_list: Vec<String>,
    pid_to_psutilsproc: HashMap<usize, String>,
    pid_to_files: HashMap<usize, String>,
    pid_to_connections: HashMap<usize, String>,
    internal_monitor_counter: usize,
    internal_monitor_id: usize,
    tids_marked_to_retry: Vec<usize>,
    retry_counter: Vec<usize>,
    task_retries: Vec<usize>,
    alternative_envs: HashMap<usize, String>,
}

impl WorkflowExecutor {
    // pub fn new(args: Args, workflowfile: String, jmax: u16) -> Self {}
}
