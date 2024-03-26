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
    println!("Signal caught for {} ", current_pid);
    let processes = system.processes();

    for (&pid, proc) in processes {
        if proc.parent() == Some(sysinfo::Pid::from_u32(current_pid)) {
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

// pub fn children(pid: u32, ) -> Vec<u32> {
//     let system: &System = System::new_all();
//     let mut children: Vec<u32>= Vec::new();
//     let processes = system.processes();

//     for (&_p, proc) in processes {
//         if proc.parent() == Some(Pid::from_u32(pid)) {
//             children.push(proc);
//         }
//     }

//     children
// }

// Function to recursively determine all child processes with `pid` as the root
pub fn find_child_processes_recursive(system: &System, pid: Pid) -> Vec<Pid> {
    // let mut system = System::new_all();
    let mut child_pids = Vec::new();

    // Recursively find child processes
    fn find_children(system: &System, pid: Pid, child_pids: &mut Vec<Pid>) {
        // Iterate through all processes
        for (process_id, process) in system.processes() {
            // Check if the process has `pid` as its parent
            if let Some(parent_pid) = process.parent() {
                if parent_pid == pid {
                    // If so, add it to the list of child processes
                    let child_pid = *process_id;
                    child_pids.push(child_pid);
                    // Recursively find children of the current process
                    find_children(system, child_pid, child_pids);
                }
            }
        }
    }

    // Start the recursive search from the root process
    find_children(system, pid, &mut child_pids);

    child_pids
}