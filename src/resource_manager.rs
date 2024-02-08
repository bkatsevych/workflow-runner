use rustix::process::nice;
use serde_json::de;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Semaphore {
    locked: bool,
}

impl Semaphore {
    pub fn new() -> Self {
        Self { locked: false }
    }

    pub fn lock(&mut self) {
        self.locked = true;
    }

    pub fn unlock(&mut self) {
        self.locked = false;
    }
}

#[derive(Clone, Debug)]
pub struct TaskResources {
    tid: usize,
    name: String,
    cpu_assigned_original: f64,
    mem_assigned_original: f64,
    cpu_assigned: f64,
    mem_assigned: f64,
    resource_boundaries: ResourceBoundaries,
    cpu_sampled: Option<f64>,
    mem_sampled: Option<f64>,
    walltime: Option<f64>,
    cpu_taken: Option<f64>,
    mem_taken: Option<f64>,
    time_collect: Vec<f64>,
    cpu_collect: Vec<f64>,
    mem_collect: Vec<f64>,
    related_tasks: Option<Vec<TaskResources>>,
    semaphore: Option<Semaphore>,
    nice_value: Option<i32>,
    booked: bool,
}

impl TaskResources {
    pub fn new(
        tid: usize,
        name: &str,
        cpu: f64,
        mem: f64,
        resource_boundaries: ResourceBoundaries,
    ) -> Self {
        Self {
            tid,
            name: name.to_string(),
            cpu_assigned_original: cpu,
            mem_assigned_original: mem,
            cpu_assigned: cpu,
            mem_assigned: mem,
            resource_boundaries,
            cpu_sampled: None,
            mem_sampled: None,
            walltime: None,
            cpu_taken: None,
            mem_taken: None,
            time_collect: vec![],
            cpu_collect: vec![],
            mem_collect: vec![],
            related_tasks: None,
            semaphore: None,
            nice_value: None,
            booked: false,
        }
    }

    pub fn is_done(&self) -> bool {
        !self.time_collect.is_empty() && !self.booked
    }
}

#[derive(Clone, Debug)]
pub struct ResourceBoundaries {
    cpu_limit: u16,
    mem_limit: u32,
    dynamic_resources: bool,
    optimistic_resources: bool,
}

impl ResourceBoundaries {
    pub fn new(
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
    resources: Vec<TaskResources>,
    resources_related_tasks_dict: HashMap<String, Vec<TaskResources>>,
    semaphore_dict: HashMap<String, Semaphore>,
    resource_boundaries: ResourceBoundaries,
    cpu_booked: f64,
    mem_booked: f64,
    n_procs: u16,
    cpu_booked_backfill: f64,
    mem_booked_backfill: f64,
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
            resources: vec![],
            resources_related_tasks_dict: HashMap::new(),
            semaphore_dict: HashMap::new(),
            resource_boundaries: ResourceBoundaries::new(
                cpu_limit,
                mem_limit,
                dynamic_resources,
                optimistic_resources,
            ),
            cpu_booked: 0.0,
            mem_booked: 0.0,
            n_procs: 0,
            cpu_booked_backfill: 0.0,
            mem_booked_backfill: 0.0,
            n_procs_backfill: 0,
            procs_parallel_max,
            nice_default,
            nice_backfill,
        }
    }

    pub fn add_task_resources(
        &mut self,
        name: &str,
        related_tasks_name: &String,
        cpu: f64,
        mem: f64,
        semaphore_string: &str,
    ) {
        let resources = TaskResources::new(
            self.resources.len(),
            name,
            cpu,
            mem,
            self.resource_boundaries.clone(),
        );
        if cpu > self.resource_boundaries.cpu_limit as f64
            || mem > self.resource_boundaries.mem_limit as f64
        {
            println!("Resource estimates of id {} overestimates limits, CPU limit: {}, MEM limit: {}; might not run", self.resources.len(), self.resource_boundaries.cpu_limit, self.resource_boundaries.mem_limit);
            if !self.resource_boundaries.optimistic_resources {
                panic!("We don't dare to try");
            }
            println!("We will try to run this task anyway with maximum available resources");
        }

        self.resources.push(resources.clone());
        if !semaphore_string.is_empty() {
            if !self.semaphore_dict.contains_key(semaphore_string) {
                self.semaphore_dict
                    .insert(semaphore_string.to_string(), Semaphore::new());
            }
            self.resources.last_mut().unwrap().semaphore =
                Some(self.semaphore_dict.get(semaphore_string).unwrap().clone());
        }

        if !related_tasks_name.is_empty() {
            if !self
                .resources_related_tasks_dict
                .contains_key(related_tasks_name)
            {
                self.resources_related_tasks_dict
                    .insert(related_tasks_name.clone(), vec![]);
            }
            self.resources_related_tasks_dict
                .get_mut(related_tasks_name)
                .unwrap()
                .push(resources.clone());
            self.resources.last_mut().unwrap().related_tasks = Some(
                self.resources_related_tasks_dict
                    .get(related_tasks_name)
                    .unwrap()
                    .clone(),
            );
        }
    }
}
