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

    pub fn sample_resources(&mut self) {
        if !self.is_done() {
            return;
        }

        if self.time_collect.len() < 3 {
            self.cpu_sampled = Some(self.cpu_assigned);
            self.mem_sampled = Some(self.mem_assigned);
            println!("Task {} has not enough points (< 3) to sample resources, setting to previously assigned values.", self.name);
        } else {
            let time_deltas: Vec<f64> = self.time_collect.windows(2).map(|w| w[1] - w[0]).collect();
            let cpu: f64 = self.cpu_collect[1..]
                .iter()
                .zip(time_deltas.iter())
                .filter(|&(cpu, _)| *cpu >= 0.0)
                .map(|(cpu, time_delta)| cpu * time_delta)
                .sum();
            self.cpu_sampled = Some(cpu / time_deltas.iter().sum::<f64>());
            self.mem_sampled = self
                .mem_collect
                .iter()
                .cloned()
                .fold(None, |max, x| max.map_or(Some(x), |max| Some(x.max(max))));
        }

        let mut mem_sampled = 0.0;
        let mut cpu_sampled = vec![];
        if let Some(related_tasks) = &self.related_tasks {
            for res in related_tasks {
                if res.is_done() {
                    mem_sampled = f64::max(mem_sampled, res.mem_sampled.unwrap_or(0.0));
                    cpu_sampled.push(res.cpu_sampled.unwrap_or(0.0));
                }
            }
        }
        let mut cpu_sampled = cpu_sampled.iter().sum::<f64>() / cpu_sampled.len() as f64;

        if cpu_sampled > self.resource_boundaries.cpu_limit as f64 {
            println!(
                "Sampled CPU ({:.2}) exceeds assigned CPU limit ({:.2})",
                cpu_sampled, self.resource_boundaries.cpu_limit
            );
            cpu_sampled = self.resource_boundaries.cpu_limit as f64;
        }
        if mem_sampled > self.resource_boundaries.mem_limit as f64 {
            println!(
                "Sampled MEM ({:.2}) exceeds assigned MEM limit ({:.2})",
                mem_sampled, self.resource_boundaries.mem_limit
            );
            mem_sampled = self.resource_boundaries.mem_limit as f64;
        }

        if mem_sampled <= 0.0 {
            println!(
                "Sampled memory for {} is {:.2} <= 0, setting to previously assigned value {:.2}",
                self.name, mem_sampled, self.mem_assigned
            );
            mem_sampled = self.mem_assigned;
        }
        if cpu_sampled < 0.0 {
            println!(
                "Sampled CPU for {} is {:.2} < 0, setting to previously assigned value {:.2}",
                self.name, cpu_sampled, self.cpu_assigned
            );
            cpu_sampled = self.cpu_assigned;
        }
        if let Some(related_tasks) = &mut self.related_tasks {
            for res in related_tasks {
                if res.is_done() || res.booked {
                    continue;
                }
                res.cpu_assigned = cpu_sampled;
                res.mem_assigned = mem_sampled;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResourceBoundaries {
    cpu_limit: u16,
    pub mem_limit: u64,
    dynamic_resources: bool,
    optimistic_resources: bool,
}

impl ResourceBoundaries {
    pub fn new(
        cpu_limit: u16,
        mem_limit: u64,
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
    pub resource_boundaries: ResourceBoundaries,
    cpu_booked: f64,
    mem_booked: f64,
    n_procs: u16,
    cpu_booked_backfill: f64,
    mem_booked_backfill: f64,
    n_procs_backfill: u16,
    procs_parallel_max: u16,
    pub nice_default: i32,
    nice_backfill: i32,
}

impl ResourceManager {
    pub fn new(
        cpu_limit: u16,
        mem_limit: u64,
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

    pub fn ok_to_submit(&mut self, tids: &mut Vec<isize>) -> Option<(usize, i32)> {
        let tids_copy = tids.clone();

        let ok_to_submit_default = |res: &TaskResources| -> Option<i32> {
            let okcpu =
                (self.cpu_booked + res.cpu_assigned <= self.resource_boundaries.cpu_limit as f64);
            let okmem =
                (self.mem_booked + res.mem_assigned <= self.resource_boundaries.mem_limit as f64);
            if okcpu && okmem {
                Some(self.nice_default)
            } else {
                None
            }
        };

        let ok_to_submit_backfill = |res: &TaskResources| -> Option<i32> {
            if self.n_procs_backfill >= self.procs_parallel_max {
                return None;
            }

            if res.cpu_assigned > 0.9 * self.resource_boundaries.cpu_limit as f64
                || res.mem_assigned / self.resource_boundaries.cpu_limit as f64 >= 1900.0
            {
                return None;
            }

            let okcpu = (self.cpu_booked_backfill + res.cpu_assigned
                <= self.resource_boundaries.cpu_limit as f64);
            let okcpu = okcpu
                && (self.cpu_booked + self.cpu_booked_backfill + res.cpu_assigned
                    <= 1.5 * self.resource_boundaries.cpu_limit as f64);
            let okmem = (self.mem_booked + self.mem_booked_backfill + res.mem_assigned
                <= 1.5 * self.resource_boundaries.mem_limit as f64);

            if okcpu && okmem {
                Some(self.nice_backfill)
            } else {
                None
            }
        };

        if self.n_procs + self.n_procs_backfill >= self.procs_parallel_max {
            return None;
        }

        for (ok_to_submit_impl, should_break) in vec![
            (
                Box::new(ok_to_submit_default) as Box<dyn Fn(&TaskResources) -> Option<i32>>,
                true,
            ),
            (
                Box::new(ok_to_submit_backfill) as Box<dyn Fn(&TaskResources) -> Option<i32>>,
                false,
            ),
        ] {
            let mut tid_index = 0;
            while tid_index < tids_copy.len() {
                let tid = tids_copy[tid_index];
                let res = &mut self.resources[tid as usize];

                tid_index += 1;

                if let Some(semaphore) = &res.semaphore {
                    if semaphore.locked || res.booked {
                        continue;
                    }
                }

                if let Some(nice_value) = ok_to_submit_impl(res) {
                    res.nice_value = Some(nice_value);
                    return Some((tid as usize, nice_value));
                } else if should_break {
                    break;
                }
            }
        }

        None
    }

    pub fn book(&mut self, tid: usize, nice_value: i32) {
        let res = &mut self.resources[tid];
        let previous_nice_value = res.nice_value;

        match previous_nice_value {
            None => {
                println!(
                    "Task ID {} has never been checked for resources. Treating as backfill",
                    tid
                );
                res.nice_value = Some(self.nice_backfill);
            }
            Some(value) if value != nice_value => {
                println!("Task ID {} was last time checked for a different nice value ({}) but is now submitted with ({}).", tid, value, nice_value);
            }
            _ => {}
        }

        res.nice_value = Some(nice_value);
        res.booked = true;
        if let Some(semaphore) = &mut res.semaphore {
            semaphore.lock();
        }
        if nice_value != self.nice_default {
            self.n_procs_backfill += 1;
            self.cpu_booked_backfill += res.cpu_assigned;
            self.mem_booked_backfill += res.mem_assigned;
        } else {
            self.n_procs += 1;
            self.cpu_booked += res.cpu_assigned;
            self.mem_booked += res.mem_assigned;
        }
    }

    pub fn unbook(&mut self, tid: usize) {
        let res = &mut self.resources[tid];
        res.booked = false;
        if self.resource_boundaries.dynamic_resources {
            res.sample_resources();
        }
        if let Some(semaphore) = &mut res.semaphore {
            semaphore.unlock();
        }
        if res.nice_value != Some(self.nice_default) {
            self.cpu_booked_backfill -= res.cpu_assigned;
            self.mem_booked_backfill -= res.mem_assigned;
            self.n_procs_backfill -= 1;
            if self.n_procs_backfill <= 0 {
                self.cpu_booked_backfill = 0.0;
                self.mem_booked_backfill = 0.0;
            }
            return;
        }
        self.n_procs -= 1;
        self.cpu_booked -= res.cpu_assigned;
        self.mem_booked -= res.mem_assigned;
        if self.n_procs <= 0 {
            self.cpu_booked = 0.0;
            self.mem_booked = 0.0;
        }
    }

    pub fn add_monitored_resources(
        &mut self,
        tid: &usize,
        time_delta_since_start: i32,
        cpu: f32,
        mem: u64,
    ) {
        let res = &mut self.resources[*tid];
        res.time_collect.push(time_delta_since_start as f64);
        res.cpu_collect.push(cpu as f64);
        res.mem_collect.push(mem as f64);
    }
}
