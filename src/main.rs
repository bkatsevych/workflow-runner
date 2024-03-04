use crate::utils::kill_child_procs;
use arguments::Arguments;
use clap::Parser;
use logger::{LogLevel, Logger};
use std::process;
use std::process::{exit, Command};
use workflow_executor::WorkflowExecutor;
mod arguments;
mod graph;
mod logger;
mod resource_manager;
mod utils;
mod workflow_executor;

fn main() {
    ctrlc::set_handler(move || {
        println!("Ctrl-C received, exiting");
        kill_child_procs();
    })
    .expect("Error setting Ctrl-C handler");

    // defining command line options
    let arguments = Arguments::parse();

    // first file logger
    let action_logger = Logger::new(
        format!("pipeline_action_{}.log", process::id()),
        LogLevel::INFO,
    );

    // second file logger
    let metric_logger = Logger::new(
        format!("pipeline_metric_{}.log", process::id()),
        LogLevel::INFO,
    );

    if let Some(cgroup) = &arguments.cgroup {
        let my_pid = std::process::id().to_string();
        // cgroups such as /sys/fs/cgroup/cpuset/<cgroup-name>/tasks
        // or              /sys/fs/cgroup/cpu/<cgroup-name>/tasks
        let command = format!("echo {} > {}", my_pid, cgroup);

        action_logger.info(&format!("Try running in cgroup {}", cgroup));

        let output = Command::new("sh")
            .arg("-c")
            .arg(&command)
            .output()
            .expect("failed to execute process");

        if !output.status.success() {
            action_logger.error("Could not apply cgroup");
            exit(output.status.code().unwrap_or(1));
        }

        action_logger.info("Running in cgroup");
    }

    // instantiating workflow executor
    let mut workflow_executor = WorkflowExecutor::new(arguments, action_logger, metric_logger);

    process::exit(workflow_executor.execute() as i32);
}
