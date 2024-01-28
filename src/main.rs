use arguments::Arguments;
use clap::Parser;
use logger::{LogLevel, Logger};
use std::fs::File;
use std::io::Write;
use std::process;
use std::process::{exit, Command};

mod arguments;
mod logger;
mod resource_manager;
mod utils;
mod workflow_executor;
mod workflow_spec;

fn main() {
    let arguments = Arguments::parse();

    println!("{:?}", arguments);

    let action_logger = Logger::new(
        format!("pipeline_action_{}.log", process::id()),
        LogLevel::INFO,
    );

    let metric_logger = Logger::new(
        format!("pipeline_metric_{}.log", process::id()),
        LogLevel::INFO,
    );

    if let Some(cgroup) = arguments.cgroup {
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
}
