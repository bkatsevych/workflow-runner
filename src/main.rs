use args::Args;
use clap::Parser;

mod args;
mod resource_manager;
mod utils;
mod workflow_executor;
mod workflow_spec;

fn main() {
    let args = Args::parse();

    println!("{:?}", args);
}
