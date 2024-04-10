> _This is a draft document expressing ideas. Everything is to be understood as prototype and open for changes/adaptions._

# Purpose

Execution of workflows under resource constraints, where possibly tasks will be scheduled in parallel. In principle, this can serve to schedule any kind of directed acycling graph (DAG) workflow. The tool takes care of how something is executed, not what is executed or how it is configured.

# More detailed description

The tool provides features of a typical data/task pipelining environment using a DAG approach. It allows to separate the concerns of workflow setup and workflow running - and as such allows to optimize workflow deployment during execution.

Typical workflows targeted by the tool are complex bash scripts of interdependent sections, with a mix of DPL workflows, transport simulation, QA, file-operations, validation steps, etc.

# Example usage

### General help

```
$ cargo run -- --help
```

### Run in a Linux environment

-   Simply build an image and run a container from it. You can set arguments at runtime (see the example below).
-   Log files will be stored in the current directory (pwd), making it easy to check them. However, keep in mind that `process::id()` will always be `1`, so it's better to provide a name for each logger explicitly. Otherwise, new logging information will be appended to the previous one each time you run the app.

```
docker build -t your_image_name .

docker run --rm -v "$(pwd)":/usr/src/app your_image_name -f workflow_fake.json --mem-limit 16000 --target-tasks grpcreate --action-logfile docker_action_1 --metric-logfile docker_metric_1
```

# To do

-   Graph visualization
-   Speed up ROOT init
-   Produce script
