use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::io::Error;
use std::io::{self, BufRead};
use std::path::Path;
use std::process;
use sysinfo::{Pid, System};

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

    let child_pids = find_child_processes_recursive(&system, sysinfo::Pid::from_u32(current_pid));

    for pid in child_pids {
        if let Some(proc) = system.process(pid) {
            println!("Killing child process {} ", pid);
            proc.kill();
        }
    }

    process::exit(1);
}

pub fn find_child_processes_recursive(system: &System, pid: Pid) -> Vec<Pid> {
    let mut child_pids = Vec::new();

    fn find_children(system: &System, pid: Pid, child_pids: &mut Vec<Pid>) {
        for (process_id, process) in system.processes() {
            if let Some(parent_pid) = process.parent() {
                if parent_pid == pid {
                    let child_pid = *process_id;
                    child_pids.push(child_pid);
                    find_children(system, child_pid, child_pids);
                }
            }
        }
    }

    find_children(system, pid, &mut child_pids);

    child_pids
}

// more info about /proc/PID/smaps file: https://shorturl.at/ituHO
fn get_metric(pid: u32, metrics: &[&str]) -> io::Result<u64> {
    let path = format!("/proc/{}/smaps", pid);
    let file = File::open(Path::new(&path))?;
    let reader = io::BufReader::new(file);

    let mut total = 0;

    for line in reader.lines() {
        let line = line?;
        for metric in metrics {
            if line.starts_with(metric) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(metric_str) = parts.get(1) {
                    let metric_value: u64 = metric_str.parse().unwrap_or(0);
                    total += metric_value;
                }
            }
        }
    }

    Ok(total)
}

pub fn get_pss(pid: u32) -> io::Result<u64> {
    get_metric(pid, &["Pss:"])
}

pub fn get_swap(pid: u32) -> io::Result<u64> {
    get_metric(pid, &["Swap:"])
}

pub fn get_uss(pid: u32) -> io::Result<u64> {
    get_metric(pid, &["Private_Clean:", "Private_Dirty:"])
}
