use arguments::Arguments;
use clap::Parser;
use std::process;

use logger::{LogLevel, Logger};

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

    // action_logger.info("Score for task is 10");
    // metric_logger.info("Resources per task is 5");
    // action_logger.debug("Task has not enough points (< 3) to sample resources, setting to previously assigned values.");
    // action_logger.warning("Sampled CPU (2.5) exceeds assigned CPU limit (2.0)");
}
