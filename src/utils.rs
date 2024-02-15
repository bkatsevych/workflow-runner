use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::io::Error;
use std::process;
use sysinfo::{Pid, Process, Signal, System};

pub fn load_json(workflow_file: &str) -> Result<Value, Error> {
    let file = File::open(workflow_file)?;
    let reader = BufReader::new(file);
    let workflow_spec: Value = serde_json::from_reader(reader)?;
    Ok(workflow_spec)
}

pub fn kill_child_procs() {
    let mut system = System::new_all();
    system.refresh_all();

    let current_pid = process::id();
    println!("Signal {} caught", current_pid);
    let processes = system.processes();

    for (&pid, proc) in processes {
        if proc.parent() == Some(sysinfo::Pid::from_u32(current_pid as u32)) {
            println!("Terminating {}", pid);
            proc.kill_with(Signal::Term);
            println!("Waiting for {} termination", pid);
            proc.wait();
            println!("{} exited", pid);
            println!("Killing {}", pid);
            proc.kill();
        }
    }

    process::exit(1);
}
