use chrono::{DateTime, Local};
use std::fs::OpenOptions;
use std::io::prelude::*;

#[derive(Debug)]
pub enum LogLevel {
    INFO,
    DEBUG,
    WARNING,
    ERROR,
}

pub struct Logger {
    log_file: String,
    level: LogLevel,
}

impl Logger {
    pub fn new(log_file: String, level: LogLevel) -> Self {
        Self { log_file, level }
    }

    pub fn log(&self, level: &LogLevel, message: &str) {
        let now: DateTime<Local> = Local::now();
        let formatted_time = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = format!("{:?} {:?} {}\n", formatted_time, level, message);

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.log_file)
            .unwrap();

        if let Err(e) = writeln!(file, "{}", log_message) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }

    pub fn info(&self, message: &str) {
        self.log(&LogLevel::INFO, message);
    }

    pub fn debug(&self, message: &str) {
        self.log(&LogLevel::DEBUG, message);
    }

    pub fn warning(&self, message: &str) {
        self.log(&LogLevel::WARNING, message);
    }

    pub fn error(&self, message: &str) {
        self.log(&LogLevel::ERROR, message);
    }
}
