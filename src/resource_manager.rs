use rustix::process::nice;
use std::collections::HashMap;

struct ResourceBoundaries {
    cpu_limit: u16,
    mem_limit: u32,
    dynamic_resources: bool,
    optimistic_resources: bool,
}

impl ResourceBoundaries {
    fn new(
        cpu_limit: u16,
        mem_limit: u32,
        dynamic_resources: bool,
        optimistic_resources: bool,
    ) -> Self {
        Self {
            cpu_limit,
            mem_limit,
            dynamic_resources,
            optimistic_resources,
        }
    }
}

pub struct ResourceManager {
    resources: Vec<String>, // Assuming resources are represented as strings
    resources_related_tasks_dict: HashMap<String, String>, // Assuming key-value pairs are strings
    semaphore_dict: HashMap<String, String>, // Assuming key-value pairs are strings
    resource_boundaries: ResourceBoundaries,
    cpu_booked: u16,
    mem_booked: u32,
    n_procs: u16,
    cpu_booked_backfill: u16,
    mem_booked_backfill: u32,
    n_procs_backfill: u16,
    procs_parallel_max: u16,
    nice_default: i32,
    nice_backfill: i32,
}

impl ResourceManager {
    pub fn new(
        cpu_limit: u16,
        mem_limit: u32,
        procs_parallel_max: u16,
        dynamic_resources: bool,
        optimistic_resources: bool,
    ) -> Self {
        let nice_default = nice(0).unwrap();
        let nice_backfill = nice_default + 19;
        Self {
            resources: Vec::new(),
            resources_related_tasks_dict: HashMap::new(),
            semaphore_dict: HashMap::new(),
            resource_boundaries: ResourceBoundaries::new(
                cpu_limit,
                mem_limit,
                dynamic_resources,
                optimistic_resources,
            ),
            cpu_booked: 0,
            mem_booked: 0,
            n_procs: 0,
            cpu_booked_backfill: 0,
            mem_booked_backfill: 0,
            n_procs_backfill: 0,
            procs_parallel_max,
            nice_default,
            nice_backfill,
        }
    }
}
