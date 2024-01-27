use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::io::Error;

pub fn load_json(workflowfile: &str) -> Result<Value, Error> {
    let file = File::open(workflowfile)?;
    let reader = BufReader::new(file);
    let workflow_spec: Value = serde_json::from_reader(reader)?;
    Ok(workflow_spec)
}
