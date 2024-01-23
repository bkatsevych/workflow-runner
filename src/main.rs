mod args;

use args::Cli;
use clap::Parser;

fn main() {
    let cli = Cli::parse();

    println!("{:?}", cli);
}
