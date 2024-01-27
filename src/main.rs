use args::Args;
use clap::Parser;
use std::process;

use crate::logger::{LogLevel, Logger};

mod args;
mod logger;
mod resource_manager;
mod utils;
mod workflow_executor;
mod workflow_spec;

fn main() {
    let args = Args::parse();

    println!("{:?}", args);

    let actionlogger = Logger::new(
        format!("pipeline_action_{}.log", process::id()),
        LogLevel::INFO,
    );

    let metriclogger = Logger::new(
        format!("pipeline_metric_{}.log", process::id()),
        LogLevel::INFO,
    );

    // actionlogger.info("Score for task is 10");
    // metriclogger.info("Resources per task is 5");
    // actionlogger.debug("Task has not enough points (< 3) to sample resources, setting to previously assigned values.");
    // actionlogger.warning("Sampled CPU (2.5) exceeds assigned CPU limit (2.0)");
}
