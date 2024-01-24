use std::collections::HashMap;
use std::env;
use std::process;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::args::Args;
use crate::resource_manager::ResourceManager;
use crate::utils::load_json;
use crate::workflow_spec::WorkflowSpec;

pub struct WorkflowExecutor {
    args: Args,
    is_productionmode: bool,
    workflowfile: String,
    workflowspec: WorkflowSpec,
    globalenv: HashMap<String, String>,
    possiblenexttask: Vec<String>,
    taskweights: Vec<f32>,
    topological_orderings: Vec<String>,
    taskuniverse: Vec<String>,
    idtotask: Vec<String>,
    tasktoid: HashMap<String, usize>,
    resource_manager: ResourceManager,
    procstatus: HashMap<usize, String>,
    taskneeds: HashMap<String, Vec<String>>,
    stoponfailure: bool,
    scheduling_iteration: usize,
    process_list: Vec<String>,
    backfill_process_list: Vec<String>,
    pid_to_psutilsproc: HashMap<usize, String>,
    pid_to_files: HashMap<usize, String>,
    pid_to_connections: HashMap<usize, String>,
    internalmonitorcounter: usize,
    internalmonitorid: usize,
    tids_marked_toretry: Vec<usize>,
    retry_counter: Vec<usize>,
    task_retries: Vec<usize>,
    alternative_envs: HashMap<usize, String>,
}

impl WorkflowExecutor {
    pub fn new(args: Args, workflowfile: String, jmax: u16) -> Self {}
}
