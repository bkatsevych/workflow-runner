use clap::{arg, value_parser, ArgAction, Command};
use sysinfo::System;

fn main() {
    let system = System::new_all();
    let max_system_mem = system.total_memory() as f64;

    // defining command line options
    let _matches = Command::new("parser")
        .about("Parallel execution of a (O2-DPG) DAG data/job pipeline under resource contraints")
        .arg(
            arg!(
                -f --"workflow-file" <FILE> "Input workflow file name."
            )
            .required(true),
        )
        .arg(
            arg!(
                <MAX_JOBS> 
            )
            .value_parser(value_parser!(u16))
            .default_value("100")
            .required(false),
        )
        .arg(
            arg!(
                -k --"keep-going" "Keep executing the pipeline as far possibe (not stopping on first failure)."
            )
            .action(ArgAction::SetTrue),
        )
        .arg(arg!(--"dry-run" "Show what you would do.").action(ArgAction::SetTrue))
        .arg(arg!(--"visualize-workflow" "Saves a graph visualization of workflow.")
            .action(ArgAction::SetTrue))
        .arg(arg!(--"target-labels" "Runs the pipeline by target labels (example \"TPC\" or \"DIGI\").\nThis condition is used as logical AND together with --target-tasks.")
            .action(ArgAction::Append)
            .default_value("")
        )
        .arg(arg!(--"target-tasks" "Runs the pipeline by target tasks (example \"tpcdigi\").\nBy default everything in the graph is run. Regular expressions supported.")
            .action(ArgAction::Append)
            .default_value("*")
        )
        .arg(arg!(--"produce-script" "Produces a shell script that runs the workflow in serialized manner and quits."))
        .arg(arg!(--"rerun-from" "Reruns the workflow starting from given task (or pattern). All dependent jobs will be rerun."))
        .arg(arg!(--"list-tasks" "Simply list all tasks by name and quit.")
            .action(ArgAction::Append))

        // resources
        .arg(arg!(--"update-resources" "Read resource estimates from a JSON and apply where possible."))
        .arg(arg!(--"dynamic-resources" "Update reources estimates of task based on finished related tasks")
            .action(ArgAction::Append))
        .arg(arg!(--"optimistic-resources" "Try to run workflow even though resource limits might underestimate resource needs of some tasks")
            .action(ArgAction::Append))
        .arg(arg!(<BACKFILL>)
            .value_parser(value_parser!(u16))
            .default_value("1")
            .required(false),
        )
        .arg(arg!(<MEM_LIMIT>)
            .value_parser(value_parser!(f64))
            .default_value((0.9 * max_system_mem / 1024. / 1024.).to_string())
            .required(false),
        )
        .arg(arg!(<CPU_LIMIT>)
            .value_parser(value_parser!(u16))
            .default_value("8")
            .required(false),
        )
        .arg(arg!(--"cgroup" "Execute pipeline under a given cgroup (e.g., 8coregrid) emulating resource constraints.\nThis must exist and the tasks file must be writable to with the current user."))
        // run control, webhooks
        .arg(arg!(--"stdout-on-failure" "Print log files of failing tasks to stdout")
            .action(ArgAction::SetTrue))
        .arg(arg!(--"webhook")
            .hide(true))
        .arg(arg!(--"checkpoint-on-failure")
            .hide(true))
        .arg(arg!(<RETRY_ON_FAILURE>)
            .value_parser(value_parser!(u16))
            .default_value("0")
            .hide(true)
            .required(false),
        )
        .arg(arg!(--"no-rootinit-speedup")
            .action(ArgAction::SetTrue)
            .hide(true))
        // Logging
        .arg(arg!(--"action-logfile" "Logfilename for action logs. If none given, pipeline_action_#PID.log will be used"))
        .arg(arg!(--"metric-logfile" "Logfilename for metric logs. If none given, pipeline_metric_#PID.log will be used"))
        .arg(arg!(--"production-mode" "Production mode")
            .action(ArgAction::SetTrue))
        .get_matches();

   
}
