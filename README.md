# Benchmark Runner

This repository contains a benchmark runner for executing one or more solver
commands over a list of benchmark instances. The runner uses a client/server
architecture:

- `benchmark-server` owns the queue, resource limits, job execution, and
  server-side output directories.
- `benchmark` is the user-facing client. It reads the benchmark list locally,
  submits the batch to the server, displays progress, and receives result files
  back from the server when the batch finishes.

The server executes submitted solver commands as the server OS user. This is a
trusted-user tool; there is no authentication or sandboxing in the protocol.

## Building

From the runner directory:

```sh
cd runner
dune build
```

When installed, the commands are:

```sh
benchmark-server
benchmark
```

For local development, Dune also builds executables under `runner/_build`.

## Testing

Run the automated test harness from the runner directory:

```sh
cd runner
dune runtest
```

The harness includes:

- protocol encode/decode and base64 round-trip tests
- shared output directory and result file naming tests
- client CLI validation tests
- local client/server integration tests
- normal submit/wait with result transfer
- detach and reconnect
- relative benchmark list and solver paths
- server and client `-runN` output directory collision behavior
- max-memory rejection
- timeout classification

The integration tests start a temporary local TCP server on `127.0.0.1`, so the
test environment must allow loopback bind/connect.

## Starting A Server

Start a server before submitting jobs:

```sh
benchmark-server -cores 8 -max-memory 64 -output-root /path/to/server/results
```

Common server options:

- `-host HOST`
  Bind address. Default is `127.0.0.1`. Use `0.0.0.0` only for trusted networks.
- `-port PORT`
  TCP port. Default is `8765`.
- `-cores N`
  Maximum number of concurrently running jobs.
- `-max-memory GB`
  Total memory budget across concurrent jobs.
- `-output-root DIR`
  Directory where the server writes batch output directories.
- `-server-log FILE`
  Append server activity messages to a file.

The server keeps its queue and reconnect table in memory. If the server exits,
queued/running job state and reconnect job ids are lost, although files already
written on the server disk remain there.

## Submitting A Batch

The normal client invocation is:

```sh
benchmark [options] BENCHMARK_LIST SOLVER [SOLVER ...]
```

Example:

```sh
benchmark -server 127.0.0.1:8765 -timeout 60 -memory 8 -excel benchmarks.txt ./solver-a ./solver-b
```

The client reads `BENCHMARK_LIST` locally and sends the list contents to the
server. The file containing the list does not need to exist on the server.

Client options:

- `-server HOST:PORT`
  Server endpoint. Defaults to `BENCHMARK_SERVER`, then `127.0.0.1:8765`.
- `-timeout SECONDS`
  Per-job timeout. Default is `300`.
- `-memory GB`
  Per-job memory limit.
- `-timesort wall|user`
  Sort output rows by wall or user time.
- `-generations N`
  Repeat each solver/benchmark job `N` times. Server mode requires `N >= 1`.
- `-excel`
  Also produce and transfer `results.xls`.
- `-xml`
  Deprecated alias for `-excel`.
- `-detach`
  Submit the batch and exit after server acceptance.
- `-reconnect JOB_ID`
  Reconnect to an existing server-side batch.

`-cores` is a server option. If passed to `benchmark`, the client exits with a
migration message.

## Paths

The client sends:

- its current working directory
- the benchmark list path
- the benchmark prefix, normally the benchmark list file's directory
- the benchmark list contents
- solver command paths

The server resolves relative benchmark entries and relative solver commands
using the submitted client path information. This works when the client and
server share the same filesystem layout or compatible mounts.

The benchmark list file itself may be local to the client, but the actual
benchmark instance paths and solver command paths must be visible and executable
from the server.

## Queueing And Progress

The server maintains an in-memory FIFO queue of submitted batches. It schedules
jobs while both constraints hold:

- running jobs are fewer than `-cores`
- reserved memory does not exceed `-max-memory`, if set

If `-max-memory` is set and a submitted job has no per-job `-memory`, that job
reserves the entire server memory budget.

The client displays progress from server events:

- a red `prior` bar for unfinished jobs from older batches that were ahead of
  this batch at acceptance time
- a cyan `job` bar for this batch's own jobs

If there are no prior jobs, only the cyan job bar is shown.

## Job IDs, Detach, And Reconnect

Every accepted batch gets a job id:

```text
accepted job id batch-000001 ...
```

Use that id to reconnect:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001
```

Reconnect behavior:

- If the batch is still running, the client attaches and receives remaining
  progress events.
- If the batch is already finished, the client receives the output files and the
  final event immediately.
- If the server has restarted or the id is unknown, reconnect fails clearly.

Detached clients and killed clients do not receive output files at completion
time, because there is no live TCP connection. Reconnect later to download the
files, as long as the server process is still alive.

## Output Files

For a batch with `S` solver commands, the server writes:

- `log`
- one `<solver>.csv` per solver
- `results.xls` if `-excel` or `-xml` was used

So the batch produces:

- `S + 1` files without Excel
- `S + 2` files with Excel

The `log` file is a diagnostic execution log. It records the command and
interpreted result for each run. Full captured stdout/stderr are included only
for diagnostic cases such as timeout, crash, unreadable stdout, or unreadable
timing output. Successful `sat` and `unsat` runs do not store full stdout/stderr.

## Server And Client Output Directories

The server writes each batch under:

```text
OUTPUT_ROOT/<digest>
```

If that directory already exists, the shared fresh-directory rule appends
`-runN`:

```text
<digest>
<digest>-run1
<digest>-run2
```

The client uses the same rule when it receives files from the server. It creates
a local directory in the client's current working directory, based on the server
output directory's basename. If a local directory already exists, the client
also uses `-runN`.

Only directories get `-runN`. File names transferred from server to client do
not contain counting suffixes. They remain exactly:

- `log`
- `<solver>.csv`
- `results.xls`

## Result Transfer

At the end of a connected run, the server sends the output files over the
existing TCP connection. The client writes them locally and prints:

```text
downloaded output files to /path/to/local/output-dir
```

Reconnect to a completed job also transfers the files again into a fresh local
directory.

## Result Classification

Each solver invocation is wrapped by `wtime`, which enforces timeout and memory
limits and reports timing data. The runner classifies results as:

- `sat`
- `unsat`
- `timeout`
- `memout`
- `crash`
- `inconsistent`

Repeated generations are averaged when answers agree. Conflicting answers across
generations become `inconsistent`.

## Examples

Run a private local server:

```sh
benchmark-server -cores 4 -max-memory 32 -output-root /tmp/benchmark-results
```

Submit and wait:

```sh
benchmark -server 127.0.0.1:8765 -timeout 120 -memory 4 examples.txt ./solver
```

Submit and detach:

```sh
benchmark -server 127.0.0.1:8765 -timeout 120 -detach examples.txt ./solver
```

Reconnect later:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001
```

Generate Excel output:

```sh
benchmark -server 127.0.0.1:8765 -excel examples.txt ./solver-a ./solver-b
```

Expose a server on a trusted network:

```sh
benchmark-server -host 0.0.0.0 -port 9000 -cores 16 -max-memory 128 -output-root /srv/bench-results
```

Then submit from a client:

```sh
benchmark -server server.example.org:9000 examples.txt /shared/solvers/yices
```

The benchmark instance paths and solver path in this example must be meaningful
on the server.

## Current Limitations

- Queue state is in memory only.
- There is no authentication.
- The server executes arbitrary submitted commands as the server OS user.
- `-generations 0` infinite mode is not supported in server mode.
- Reconnect works only while the original server process is still alive.
