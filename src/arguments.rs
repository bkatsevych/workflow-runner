use clap::Parser;
use sysinfo::System;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Arguments {
    /// Input workflow file name
    #[arg(short = 'f', long)]
    pub workflow_file: String,

    /// Number of maximal parallel tasks
    #[arg(long, default_value_t = 100)]
    pub maxjobs: u16,

    /// Keep executing the pipeline as far possibe (not stopping on first failure).
    #[arg(short, long)]
    pub keep_going: bool,

    ///Show what you would do
    #[arg(long)]
    pub dry_run: bool,

    ///Saves a graph visualization of workflow
    #[arg(long)]
    pub visualize_workflow: bool,

    #[arg(
        long,
        num_args = ..,
        help = "Runs the pipeline by target labels (example \"TPC\" or \"DIGI\").\nThis condition is used as logical AND together with --target-tasks."
    )]
    pub target_labels: Vec<String>,

    #[arg(
        long,
        default_value = "*",
        num_args = .., 
        help ="Runs the pipeline by target tasks (example \"tpcdigi\").\nBy default everything in the graph is run. Regular expressions supported."
    )]
    pub target_tasks: Vec<String>,

    /// Produces a shell script that runs the workflow in serialized manner and quits
    #[arg(long)]
    pub produce_script: Option<String>,

    /// Reruns the workflow starting from given task (or pattern). All dependent jobs will be rerun.
    #[arg(long)]
    pub rerun_from: Option<String>,

    /// Simply list all tasks by name and quit
    #[arg(long)]
    pub list_tasks: bool,

    // Resources
    /// Read resource estimates from a JSON and apply where possible.
    #[arg(long)]
    pub update_resources: Option<String>,

    /// Update reources estimates of task based on finished related tasks
    #[arg(long)]
    pub dynamic_resources: bool,

    /// Try to run workflow even though resource limits might underestimate resource needs of some tasks
    #[arg(long)]
    pub optimistic_resources: bool,

    #[arg(long, default_value_t = 1)]
    pub n_backfill: u16,

    /// Set memory limit as scheduling constraint (in MB)
    #[arg(long, default_value_t = default_mem_limit())]
    pub mem_limit: u32,

    /// Set CPU limit (core count)
    #[arg(long, default_value_t = 8)]
    pub cpu_limit: u16,

    #[arg(
        long,
        help = "Execute pipeline under a given cgroup (e.g., 8coregrid) emulating resource constraints.\nThis must exist and the tasks file must be writable to with the current user."
    )]
    pub cgroup: Option<String>,

    // run control, webhooks
    /// Print log files of failing tasks to stdout,
    #[arg(long)]
    pub stdout_on_failure: bool,

    #[arg(long, hide = true)]
    pub webhook: Option<String>,

    #[arg(long, hide = true)]
    pub checkpoint_on_failure: Option<String>,

    #[arg(long, hide = true, default_value_t = 0)]
    pub retry_on_failure: u16,

    #[arg(long, hide = true)]
    pub no_rootinit_speedup: bool,

    // Logging
    /// Logfilename for action logs. If none given, pipeline_action_#PID.log will be used
    #[arg(long)]
    pub action_logfile: Option<String>,

    /// Logfilename for metric logs. If none given, pipeline_metric_#PID.log will be used
    #[arg(long)]
    pub metric_logfile: Option<String>,

    /// Production mode
    #[arg(long)]
    pub production_mode: bool,
}

fn default_mem_limit() -> u32 {
    let system = System::new_all();
    let max_system_mem = system.total_memory() as f64;

    (0.9 * max_system_mem / 1024. / 1024.) as u32
}
