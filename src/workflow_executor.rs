use crate::arguments::Arguments;
use crate::graph::Graph;
use crate::logger::Logger;
use crate::resource_manager::ResourceManager;
use crate::utils::load_json;
use libc::{getpriority, setpriority, PRIO_PROCESS};
use regex::Regex;
use serde_json::Map;
pub use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::Path;
use std::process;
use std::process::Child;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use sysinfo::{CpuRefreshKind, RefreshKind, System};
use tar::Builder;

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
    process_list: Vec<(usize, Child)>,
    backfill_process_list: Vec<String>,
    pid_to_psutilsproc: HashMap<u32, String>,
    pid_to_files: HashMap<u32, String>,
    pid_to_connections: HashMap<u32, String>,
    internal_monitor_counter: i32,
    internal_monitor_id: i32,
    tids_marked_to_retry: Vec<usize>,
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

        println!("Stop on failure: {}", stop_on_failure);

        let mut scheduling_iteration: u32 = 0; // count how often it was tried to schedule new tasks
        let mut process_list: Vec<(usize, Child)> = Vec::new(); // list of currently scheduled tasks with normal priority
        let mut backfill_process_list: Vec<String> = Vec::new(); // list of currently scheduled tasks with low backfill priority (not sure this is needed)
        let mut pid_to_psutilsproc: HashMap<u32, String> = HashMap::new(); // cache of putilsproc for resource monitoring
        let mut pid_to_files: HashMap<u32, String> = HashMap::new(); // we can auto-detect what files are produced by which task (at least to some extent)
        let mut pid_to_connections: HashMap<u32, String> = HashMap::new(); // we can auto-detect what connections are opened by which task (at least to some extent)
        let mut internal_monitor_counter: i32 = 0; // internal use
        let mut internal_monitor_id: i32 = 0; // internal use
        let mut tids_marked_to_retry: Vec<usize> = Vec::new(); // sometimes we might want to retry a failed task (simply because it was "unlucky") and we put them here
        let mut retry_counter: Vec<i32> = vec![0; taskuniverse.len()];
        let mut task_retries: Vec<i64> = taskuniverse
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

    fn get_logfile(&self, tid: usize) -> String {
        let name = self.workflow_spec["stages"][tid]["name"].as_str().unwrap();
        let workdir = self.workflow_spec["stages"][tid]["cwd"].as_str().unwrap();
        let path = Path::new(workdir).join(format!("{}.log", name));
        path.to_str().unwrap().to_string()
    }

    fn get_done_filename(&self, tid: usize) -> String {
        format!("{}_done", self.get_logfile(tid))
    }

    fn ok_to_skip(&self, tid: usize) -> bool {
        let done_filename = self.get_done_filename(tid);

        Path::new(&done_filename).is_file()
    }

    fn execute_globalinit_cmd(&self, cmd: &Value) -> bool {
        self.action_logger
            .info(&format!("Executing global setup cmd {:?}", cmd));

        let output = Command::new("/bin/bash")
            .arg("-c")
            .arg(cmd.as_str().unwrap())
            .output()
            .expect("Failed to execute command");

        if output.status.success() {
            self.action_logger
                .info(&format!("{}", String::from_utf8_lossy(&output.stdout)));
            true
        } else {
            self.action_logger
                .error(&format!("Error executing global init function"));
            false
        }
    }

    fn remove_done_flag(&self, list_of_task_ids: Vec<isize>) {
        for tid in list_of_task_ids {
            let done_filename = self.get_done_filename(tid as usize); // Convert tid from isize to usize
            let tid = tid as usize; // Convert tid from isize to usize
            let name = self.workflow_spec["stages"][tid]["name"].as_str().unwrap();
            if self.arguments.dry_run {
                println!("Would mark task {} as to be done again", name);
            } else {
                println!("Marking task {} as to be done again", name);
                if Path::new(&done_filename).exists() {
                    fs::remove_file(done_filename).unwrap();
                }
            }
        }
    }

    fn submit(&mut self, tid: usize, nice: i32) -> Option<Child> {
        self.action_logger.debug(&format!(
            "Submitting task {} with nice value {}",
            self.id_to_task[tid], nice
        ));
        let c = self.workflow_spec["stages"][tid]["cmd"].as_str().unwrap();
        let workdir = self.workflow_spec["stages"][tid]["cwd"].as_str().unwrap();
        if !workdir.is_empty() {
            if fs::metadata(workdir).is_ok() && !fs::metadata(workdir).unwrap().is_dir() {
                self.action_logger.error(&format!(
                    "Cannot create working dir ... some other resource exists already"
                ));
                return None;
            }

            if fs::metadata(workdir).is_err() {
                fs::create_dir_all(workdir).unwrap();
            }
        }

        self.procstatus.insert(tid, "Running");
        if self.arguments.dry_run {
            let drycommand = format!(
                "echo \' {} : would do {}\'",
                self.scheduling_iteration, self.workflow_spec["stages"][tid]["name"]
            );

            return Some(
                Command::new("/bin/bash")
                    .arg("-c")
                    .arg(drycommand)
                    .current_dir(workdir)
                    .spawn()
                    .unwrap(),
            );
        }

        let mut taskenv = std::env::vars().collect::<HashMap<String, String>>();
        // add task specific environment
        if self.workflow_spec["stages"][tid].get("env").is_some() {
            for (key, value) in self.workflow_spec["stages"][tid]["env"]
                .as_object()
                .unwrap()
            {
                taskenv.insert(key.clone(), value.as_str().unwrap().to_string());
            }
        }

        // apply specific (non-default) software version, if any
        // (this was setup earlier)
        if let Some(alternative_env) = self.alternative_envs.get(&tid.to_string()) {
            self.action_logger.info(&format!(
                "Applying alternative software environment to task {}",
                self.id_to_task[tid]
            ));
            for (key, value) in alternative_env {
                taskenv.insert(key.clone(), value.clone());
            }
        }

        // this is actually spawning the new process
        let p = Command::new("/bin/bash")
            .arg("-c")
            .arg(c)
            .current_dir(workdir)
            .envs(&taskenv)
            .spawn()
            .unwrap();

        unsafe {
            setpriority(PRIO_PROCESS, p.id() as u32, nice as i32);
        }

        Some(p)
    }

    fn noprogress_errormsg(&self) {
        let msg = "Scheduler runtime error: The scheduler is not able to make progress although we have a non-zero candidate set.

Explanation: This is typically the case because the **ESTIMATED** resource requirements for some tasks
in the workflow exceed the available number of CPU cores or the memory (as explicitly or implicitly determined from the
--cpu-limit and --mem-limit options). Often, this might be the case on laptops with <=16GB of RAM if one of the tasks
is demanding ~16GB. In this case, one could try to tell the scheduler to use a slightly higher memory limit
with an explicit --mem-limit option (for instance `--mem-limit 20000` to set to 20GB). This might work whenever the
**ACTUAL** resource usage of the tasks is smaller than anticipated (because only small test cases are run).

In addition it might be worthwhile running the workflow without this resource aware, dynamic scheduler.
This is possible by converting the json workflow into a linearized shell script and by directly executing the shell script.
Use the `--produce-script myscript.sh` option for this.";

        eprintln!("{}", msg);
    }

    pub fn execute(&mut self) -> bool {
        self.start_time = Some(Instant::now());
        let mut s =
            System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));

        // Wait a bit because CPU usage is based on diff.
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        // Refresh CPUs again.
        s.refresh_cpu();

        // for cpu in s.cpus() {
        //     println!("CPU: {}%", cpu.cpu_usage());
        // }

        env::set_var("JOBUTILS_SKIPDONE", "ON");

        let mut errorencountered = false;

        // TODO:
        // speed up ROOT init

        // we make our own "tmp" folder
        // where we can put stuff such as tmp socket files etc (for instance DPL FAIR-MQ sockets)
        // (In case of running within docker/singularity, this may not be so important)
        let dir = "./.tmp";

        if !Path::new(dir).exists() {
            fs::create_dir(dir).unwrap();
        }

        if env::var("FAIRMQ_IPC_PREFIX").is_err() {
            let socketpath = format!("{}/.tmp", env::current_dir().unwrap().display());
            self.action_logger
                .info(&format!("Setting FAIRMQ socket path to {}", socketpath));
            env::set_var("FAIRMQ_IPC_PREFIX", socketpath);
        }

        // some maintenance / init work
        if self.arguments.list_tasks {
            println!("List of tasks in this workflow:");
            if let Some(stages) = self.workflow_spec["stages"].as_array() {
                for (i, t) in stages.iter().enumerate() {
                    let name = t["name"].as_str().unwrap_or("");
                    let labels = t["labels"].to_string();
                    let to_do = !self.ok_to_skip(i);
                    println!("{}  ({}) ToDo: {}", name, labels, to_do);
                }
            }
            process::exit(0);
        }

        // TODO:
        // Produce_script()

        // execute the user-given global init cmd for this workflow
        let globalinitcmd = self.global_init.get("cmd");

        if let Some(cmd) = globalinitcmd {
            if !self.execute_globalinit_cmd(cmd) {
                std::process::exit(1);
            }
        }

        if let Some(rerun_from) = &self.arguments.rerun_from {
            let mut rerun_task_found = false;

            if let Some(stages) = self.workflow_spec.get("stages").and_then(|v| v.as_array()) {
                for task in stages {
                    if let Some(task_name) = task.get("name").and_then(|v| v.as_str()) {
                        if Regex::new(&rerun_from).unwrap().is_match(task_name) {
                            rerun_task_found = true;
                            if let Some(task_id) = self.task_to_id.get(task_name) {
                                let dependent_tasks = find_all_dependent_tasks(
                                    &self.possiblenexttask,
                                    *task_id as isize,
                                    &mut None,
                                );
                                self.remove_done_flag(dependent_tasks);
                            }
                        }
                    }
                }
            }
            if !rerun_task_found {
                println!(
                    "No task matching {} found; cowardly refusing to do anything",
                    rerun_from
                );
                process::exit(1);
            }
        }

        ////////////////////
        // MAIN CONTROL LOOP
        ////////////////////
        let candidates: Vec<isize> = match self.possiblenexttask.get(&-1) {
            Some(v) => v.clone(),
            None => Vec::new(),
        };
        let mut finished_tasks: Vec<usize> = Vec::new(); // Vector of finished tasks

        println!("candidates: {:?}", candidates);

        // sort candidate list according to task weights
        let mut candidates: Vec<(isize, (i32, i32))> = candidates
            .iter()
            .map(|&tid| (tid, self.taskweights[tid as usize]))
            .collect();

        candidates.sort_by(|a, b| {
            let a_key = (a.1 .0, -(a.1 .1));
            let b_key = (b.1 .0, -(b.1 .1));
            a_key.cmp(&b_key)
        });

        let mut candidates_without_weights: Vec<isize> =
            candidates.into_iter().map(|(tid, _)| tid).collect();

        loop {
            let mut finished: Vec<isize> = Vec::new();

            //let sorted_candidates: Vec<_> = candidates_without_weights
            //    .iter()
            //    .map(|&c| (c, self.id_to_task[c as usize].clone()))
            //    .collect();

            //self.action_logger.debug(&format!(
            //    "Sorted current candidates: {:?}",
            //   sorted_candidates
            //));

            println!(
                "Sorted current candidates: {:?}",
                candidates_without_weights
            );

            self.try_jobs_from_candidates(&mut candidates_without_weights, &mut finished);

            if !candidates_without_weights.is_empty() && self.process_list.is_empty() {
                self.noprogress_errormsg();
                send_webhook(
                    &self.arguments.webhook,
                    "Unable to make further progress: Quitting",
                );
                errorencountered = true;
                break;
            }

            let mut finished_from_started: Vec<usize> = Vec::new();
            let mut failing: Vec<usize> = Vec::new();

            while self.wait_for_any(&mut finished_from_started, &mut failing) {
                if !self.arguments.dry_run {
                    // TODO:
                    // self.monitor();
                    thread::sleep(Duration::from_secs(1));
                } else {
                    thread::sleep(Duration::from_millis(1));
                }
            }

            finished.extend(finished_from_started.iter().map(|&x| x as isize));

            self.action_logger
                .debug(&format!("Finished now: {:?}", &finished_from_started));

            finished_tasks.extend(finished.iter().map(|&x| x as usize));

            if self.is_production_mode {
                for _t in &finished_from_started {
                    self.end_of_task_hook(_t);
                }
            }

            if !failing.is_empty() {
                errorencountered = true;
                for t in &failing {
                    finished.retain(|&x| x != *t as isize);
                    finished_tasks.retain(|&x| x != *t);
                }
            }

            if !self.tids_marked_to_retry.is_empty() {
                for t in &self.tids_marked_to_retry {
                    finished.retain(|&x| x != *t as isize);
                    finished_tasks.retain(|&x| x != *t);
                }

                for t in &self.tids_marked_to_retry {
                    candidates_without_weights.push(*t as isize);
                }
                self.tids_marked_to_retry.clear();
            }

            for tid in &finished {
                if let Some(potential_candidates) = self.possiblenexttask.get(tid) {
                    let potential_candidates = potential_candidates.clone();
                    for candid in potential_candidates {
                        if self.is_good_candidate(candid, &finished_tasks)
                            && !candidates_without_weights.contains(&candid)
                        {
                            candidates_without_weights.push(candid);
                        }
                    }
                }
            }

            self.action_logger
                .debug(&format!("New candidates {:?}", candidates_without_weights));

            if candidates_without_weights.is_empty() && self.process_list.is_empty() {
                break;
            }
        }
        let mut status_msg = "success".to_string();

        if errorencountered {
            status_msg = "with failures".to_string();
        }

        let elapsed = self.start_time.unwrap().elapsed();
        let global_runtime = elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 * 1e-9;

        println!(
            "\n**** Pipeline done {} (global_runtime : {:.3?}s) *****\n",
            status_msg, global_runtime
        );
        println!("global_runtime : {:.3?}s", global_runtime);
        errorencountered
    }

    fn is_good_candidate(&self, candid: isize, finishedtasks: &Vec<usize>) -> bool {
        if *self.procstatus.get(&(candid as usize)).unwrap_or(&"") != "ToDo" {
            return false;
        }
        let task_id = self.id_to_task.get(candid as usize).unwrap();
        let needs: HashSet<usize> = self
            .taskneeds
            .get(task_id)
            .unwrap()
            .iter()
            .filter_map(|t| self.task_to_id.get(t).cloned())
            .collect();
        let finishedtasks: HashSet<usize> = finishedtasks.iter().cloned().collect(); // Convert to HashSet of values
        if finishedtasks.intersection(&needs).count() == needs.len() {
            return true;
        }
        false
    }

    fn end_of_task_hook(&self, tid: &usize) {
        self.action_logger
            .debug(&format!("Cleaning up log files for task {}", tid));

        let logf = self.get_logfile(*tid);
        let donef = self.get_done_filename(*tid);
        let timef = format!("{}_time", logf);

        let tar_path = "pipeline_log_archive.log.tar";
        let mut tar_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(tar_path)
            .unwrap();

        let mut tar_builder = Builder::new(tar_file);

        tar_builder.append_path(logf.clone()).unwrap();
        tar_builder.append_path(donef.clone()).unwrap();
        tar_builder.append_path(timef.clone()).unwrap();

        tar_builder.into_inner().unwrap();

        fs::remove_file(logf).unwrap();
        fs::remove_file(donef).unwrap();
        fs::remove_file(timef).unwrap();
    }

    // WIP !!!
    // fn monitor(&mut self) {
    //     self.internal_monitor_counter += 1;
    //     if self.internal_monitor_counter % 5 != 0 {
    //         return;
    //     }

    //     self.internal_monitor_id += 1;

    //     let global_cpu: f64 = 0.0;
    //     let global_pss: f64 = 0.0;
    //     // resources_per_task = {}

    //     for (tid, proc) in &self.process_list {
    //         let pid = proc.child.id();

    //         if self.pid_to_files.get(&pid).is_none() {
    //             self.pid_to_files.insert(pid, String::new());
    //             self.pid_to_connections.insert(pid, String::new());
    //         }
    //         let mut child_procs: Vec<&ChildWithOptionalNice> = Vec::new();
    //         child_procs.push(proc);
    //     }
    // }

    fn wait_for_any(&mut self, finished: &mut Vec<usize>, failingtasks: &mut Vec<usize>) -> bool {
        let mut failure_detected = false;
        let mut failing_pids: Vec<u32> = Vec::new();

        println!(
            "Waiting for process. Current process list has length {} ",
            self.process_list.len()
        );
        if self.process_list.is_empty() {
            return false;
        }

        let mut i = self.process_list.len();
        while i != 0 {
            i -= 1;
            let p = &mut self.process_list[i];
            let pid = p.1.id();
            let tid = p.0;
            let mut return_code = -1;

            if !self.arguments.dry_run {
                //let r1: Result<Option<ExitStatus>, Error> = p.1.child.try_wait();
                //println!("# Exit status of {} {}", p.1.child.id(), r1);

                match p.1.try_wait() {
                    Ok(Some(status)) => return_code = status.code().unwrap_or(0),
                    Ok(None) => return_code = -1,
                    Err(_) => return_code = 1,
                };
                println!("Exit status of {} {}", p.1.id(), return_code)
            }

            if return_code != -1 {
                self.action_logger.info(&format!(
                    "Task {} {}: {} finished with status {}",
                    pid, tid, self.id_to_task[tid], return_code
                ));
                self.resource_manager.unbook(tid);
                self.procstatus.insert(tid, "Done");
                finished.push(tid);
                self.process_list.remove(i);

                if return_code != 0 {
                    println!("{} failed ... checking retry", self.id_to_task[tid]);
                    if self.is_worth_retrying(tid)
                        && ((self.retry_counter[tid] < self.arguments.retry_on_failure as i32)
                            || (self.retry_counter[tid] < self.task_retries[tid] as i32))
                    {
                        println!("{} to be retried", self.id_to_task[tid]);
                        self.action_logger.info(&format!(
                            "Task {} failed but marked to be retried ",
                            self.id_to_task[tid]
                        ));
                        self.tids_marked_to_retry.push(tid);
                        self.retry_counter[tid] += 1;
                    } else {
                        failure_detected = true;
                        failing_pids.push(pid);
                        failingtasks.push(tid);
                    }
                }
            }
        }

        if failure_detected && self.stop_on_failure {
            self.action_logger.info(&format!(
                "Stopping pipeline due to failure in stages with PID {:?}",
                failing_pids
            ));

            // if self.args.stdout_on_failure:
            //     self.cat_logfiles_tostdout(failingtasks)
            // self.send_checkpoint(failingtasks, self.args.checkpoint_on_failure)

            self.stop_pipeline_and_exit();
        }

        finished.len() == 0
    }

    fn stop_pipeline_and_exit(&mut self) {
        for p in &mut self.process_list {
            p.1.kill().unwrap();
        }

        std::process::exit(1);
    }

    fn is_worth_retrying(&self, tid: usize) -> bool {
        /// WIP
        // # This checks for some signatures in logfiles that indicate that a retry of this task
        // # might have a chance.
        // # Ideally, this should be made user configurable. Either the user could inject a lambda
        // # or a regular expression to use.
        // logfile = self.get_logfile(tid);
        true
    }

    fn try_jobs_from_candidates(
        &mut self,
        task_candidates: &mut Vec<isize>,
        finished: &mut Vec<isize>,
    ) {
        self.scheduling_iteration += 1;

        for tid in task_candidates.clone() {
            if self.ok_to_skip(tid as usize) {
                finished.push(tid);
                task_candidates.retain(|&x| x != tid);
                self.action_logger
                    .info(&format!("Skipping task {}", self.id_to_task[tid as usize]))
            }
        }

        for (tid, nice_value) in self.resource_manager.ok_to_submit(task_candidates) {
            if let Some(task) = self.id_to_task.get(tid) {
                self.action_logger
                    .debug(&format!("trying to submit {}:{}", tid, task));
            }
            if let Some(p) = self.submit(tid, nice_value) {
                unsafe {
                    self.resource_manager
                        .book(tid, getpriority(PRIO_PROCESS, p.id() as u32));
                    self.process_list.push((tid, p));
                }

                // we need to remove the index corresponding to tid
                let tmp: isize = tid.try_into().unwrap();
                if let Some(index) = task_candidates.iter().position(|&r| r == tmp) {
                    task_candidates.remove(index);
                } else {
                    println!("Element {} not found in the Vec.", tid);
                }

                thread::sleep(Duration::from_millis(100));
            }
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
    cache: &mut Option<HashMap<isize, Vec<isize>>>,
) -> Vec<isize> {
    if let Some(c) = cache.as_ref().and_then(|c| c.get(&tid)) {
        return c.clone();
    }

    let mut daughter_list = vec![tid];
    if let Some(next_tasks) = possible_next_task.get(&tid) {
        for &n in next_tasks {
            let c = if let Some(cached) = cache.as_ref().and_then(|c| c.get(&n)) {
                cached.clone()
            } else {
                find_all_dependent_tasks(possible_next_task, n, cache)
            };
            daughter_list.extend(c.clone());
            if let Some(cache_map) = cache {
                cache_map.insert(n, c);
            }
        }
    }

    if let Some(cache_map) = cache {
        cache_map.insert(tid, daughter_list.clone());
    }
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

    let mut dependency_cache = Some(HashMap::new());

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

pub fn update_resource_estimates(
    workflow: &mut Value,
    resource_json: &Option<String>,
    action_logger: &Logger,
) {
    let resource_dict = load_json(resource_json.as_ref().unwrap()).unwrap();
    let stages = workflow["stages"].as_array_mut().unwrap();

    for task in stages {
        let name = if task["timeframe"].as_f64().unwrap() >= 1.0 {
            let mut split_name = task["name"]
                .as_str()
                .unwrap()
                .split("_")
                .collect::<Vec<&str>>();
            split_name.pop();
            split_name.join("_")
        } else {
            String::from(task["name"].as_str().unwrap())
        };

        if !resource_dict.as_object().unwrap().contains_key(&name) {
            continue;
        }

        let new_resources = resource_dict[&name].as_object().unwrap();

        // memory
        match new_resources.get("mem") {
            Some(newmem) => {
                let oldmem = task["resources"]["mem"].as_f64().unwrap();
                action_logger.info(&format!(
                    "Updating mem estimate for {} from {} to {}",
                    task["name"], oldmem, newmem
                ));
                task["resources"]["mem"] = newmem.clone();
            }
            None => {}
        }

        // cpu
        match new_resources.get("cpu") {
            Some(newcpu) => {
                let mut newcpu = newcpu.as_f64().unwrap();
                let oldcpu = task["resources"]["cpu"].as_f64().unwrap();
                let rel_cpu = task["resources"]["relative_cpu"].as_f64();

                if let Some(rel_cpu) = rel_cpu {
                    // respect the relative CPU settings
                    // By default, the CPU value in the workflow is already scaled if relative_cpu is given.
                    // The new estimate on the other hand is not yet scaled so it needs to be done here.
                    newcpu *= rel_cpu;
                }

                action_logger.info(&format!(
                    "Updating cpu estimate for {} from {} to {}",
                    task["name"], oldcpu, newcpu
                ));
                task["resources"]["cpu"] = Value::from(newcpu);
            }
            None => {}
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

fn send_webhook(hook: &Option<String>, t: &str) {
    if let Some(hook) = hook {
        let command = format!("curl -X POST -H 'Content-type: application/json' --data '{{\"text\":\"{}\"}}' {} &> /dev/null", t, hook);
        let _output = Command::new("sh")
            .arg("-c")
            .arg(&command)
            .output()
            .expect("Failed to execute command");
    }
}
