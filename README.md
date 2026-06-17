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
- reconnect to running and finished batches
- reconnect with and without result download
- detach and later reconnect
- kill and reconnect-to-killed-batch behavior
- pause, unpause, and reconnect-after-unpause behavior
- reconnect by server-side result folder, including relative folders, absolute
  folders, duplicate imports, direct folder downloads, and empty-folder errors
- server-side benchmark and executable roots for relative job paths
- server and client `-runN` output directory collision behavior
- max-memory rejection
- timeout classification
- solver suffix preservation in CSV names
- spreadsheet `Totals`, `Clashes`, and per-solver tab generation

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
  Directory where the server writes batch output directories. If `DIR` is
  relative, it is relative to the server process's current working directory.
  Default is `.`.
- `-server-log FILE`
  Append server activity messages to a file. If `FILE` is relative, it is
  relative to the server process's current working directory. By default, no
  server log file is written; messages go to the server trace output.
- `-state-file FILE`
  JSON file used to persist batch ids, imported-folder mappings, queued work,
  status, and completed aggregate results. If omitted, the default is
  `benchmark-server-state.json` under `-output-root`.
- `-state-save-interval SECONDS`
  Minimum interval between result checkpoint writes while jobs are completing.
  Default is `30`. Submits, pause/kill/unpause operations, and batch completion
  still force a state write.

State persistence is always enabled. On startup, the server reloads the state
file if it exists. Finished and imported batches can be reconnected to by their
old batch ids. Unfinished batches are requeued from the most recent saved
aggregate result counts; solver processes that were actually running at the
moment of shutdown or crash, and completed jobs newer than the last result
checkpoint, are not resumed, so their benchmarks are run again.

## Submitting A Batch

The normal client invocation is:

```sh
benchmark [options] BENCHMARK_LIST SOLVER [SOLVER ...]
```

Example:

```sh
benchmark \
  -server 127.0.0.1:8765 \
  -server-benchmark-root /server/benchmarks \
  -server-exe-root /server/solvers \
  -timeout 60 \
  -memory 8 \
  -excel \
  benchmarks.txt \
  solver-a solver-b
```

The client reads `BENCHMARK_LIST` locally and sends the list contents to the
server. The file containing the list does not need to exist on the server.

Client options:

- `-server HOST:PORT`
  Server endpoint. Defaults to `BENCHMARK_SERVER`, then `127.0.0.1:8765`.
- `-server-benchmark-root DIR`
  Server-visible root for relative benchmark entries. If `DIR` is relative, it
  is interpreted by the server relative to the server process's current working
  directory. If omitted, the client sends the local directory containing
  `BENCHMARK_LIST`, made absolute from the client process's current working
  directory when needed.
- `-server-exe-root DIR`
  Server-visible root for relative solver commands. If `DIR` is relative, it is
  interpreted by the server relative to the server process's current working
  directory. If omitted, the client sends the client process's current working
  directory.
- `-timeout SECONDS`
  Per-job timeout. Default is `300`.
- `-memory GB`
  Per-job memory limit.
- `-timesort wall|user`
  Sort output rows by wall or user time.
- `-generations N`
  Repeat each solver/benchmark job `N` times. Server mode requires `N >= 1`.
- `-excel`
  Also produce and transfer `results.xlsx`.
- `-xml`
  Deprecated alias for `-excel`.
- `-detach`
  Submit the batch and exit after server acceptance.
- `-reconnect JOB_ID_OR_FOLDER`
  Reconnect to an existing server-side batch id. If the id is unknown, the
  server treats the argument as a result folder to import.
- `-download`
  Download output files for `-reconnect` or `-aggregate`.
- `-aggregate PREFIX`
  Aggregate all server-side result CSV files in folders matching `PREFIX*` and
  write `PREFIX.xlsx`.
- `-state`
  Continuously display the server's current unfinished batches. The client
  redraws one full-width progress line per batch until interrupted, including
  total jobs, benchmark/solver/generation counts, and queued/running job counts.
- `-kill JOB_ID`
  Kill a queued or running batch.
- `-pause JOB_ID`
  Pause a queued or running batch without cancelling currently running jobs.
- `-unpause JOB_ID`
  Resume a paused batch.

`-cores` is a server option. If passed to `benchmark`, the client exits with a
migration message.

## Paths

The benchmark list file itself is read locally by the client. It does not need
to exist on the server.

Path inputs are interpreted as follows:

| Input | Interpreted by | If relative |
| --- | --- | --- |
| server `-output-root DIR` | server | relative to the server process's current working directory |
| server `-server-log FILE` | server | relative to the server process's current working directory |
| server `-state-file FILE` | server | relative to the server process's current working directory |
| client `BENCHMARK_LIST` argument | client | relative to the client process's current working directory |
| client `-server-benchmark-root DIR` | server | relative to the server process's current working directory |
| client `-server-exe-root DIR` | server | relative to the server process's current working directory |
| benchmark entry inside `BENCHMARK_LIST` | server | relative to `-server-benchmark-root` |
| solver command argument | server | relative to `-server-exe-root` |
| `-reconnect FOLDER` folder import | server | relative to server `-output-root` |
| `-aggregate PREFIX` | server | relative to server `-output-root` |
| downloaded result directory | client | when downloading, created in the client process's current working directory |

The paths used for execution are server-side paths:

- relative benchmark entries are resolved under `-server-benchmark-root`
- relative solver commands are resolved under `-server-exe-root`
- absolute benchmark entries and absolute solver commands are used as explicit
  server-side absolute paths
- relative `-server-benchmark-root` and `-server-exe-root` values are
  themselves interpreted relative to the server process's current working
  directory

The client working directory is not used for server-side execution path
resolution. This allows the client and server to run on different machines with
different filesystem layouts.

Example:

```sh
benchmark \
  -server host:8765 \
  -server-benchmark-root /srv/smtlib \
  -server-exe-root /opt/solvers \
  local-list.txt \
  yices-smt2
```

If `local-list.txt` contains:

```text
QF_BV/foo.smt2
```

the server runs:

```text
/opt/solvers/yices-smt2 /srv/smtlib/QF_BV/foo.smt2
```

If either path is absolute, it is treated as an absolute path on the server.

For local single-machine use, if roots are omitted the client defaults them to
local paths based on the benchmark list directory and current working directory.
For multi-machine use, pass both roots explicitly.

When `-server-benchmark-root` is omitted, the client sends the local benchmark
list directory as the server benchmark root. If the list path was relative, the
client first makes that directory absolute using the client process's current
working directory. When `-server-exe-root` is omitted, the client sends the
client process's current working directory as the server executable root. These
defaults are only appropriate when those local paths are also valid on the
server.

Default path behavior summary:

- server `-output-root` omitted: server uses `.` under the server process's
  current working directory
- server `-server-log` omitted: no log file is opened
- server `-state-file` omitted: server writes
  `benchmark-server-state.json` under `-output-root`
- client `-server-benchmark-root` omitted: client sends the benchmark list
  directory, made absolute on the client if necessary
- client `-server-exe-root` omitted: client sends the client process's current
  working directory
- downloaded result directory: when files are downloaded, created under the
  client process's current working directory using the server output directory
  basename, with `-runN` if needed

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

Use `-state` for a continuous server-wide view:

```sh
benchmark -server 127.0.0.1:8765 -state
```

The state view subscribes to server updates and redraws one terminal-width
progress bar per unfinished batch, including batches that are accepted but have
not started yet. Batches are ordered from oldest to newest. Each line includes
job progress, total benchmark/solver/generation counts, and queued/running job
counts. It does not download files or attach to any single batch.

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
- If the batch is already finished, the client receives the accepted and final
  events immediately.
- Plain reconnect does not download files or create a local output directory.
- Use `-reconnect JOB_ID_OR_FOLDER -download` to reconnect and download files
  at the end.
- If the id is unknown, the server tries to interpret the argument as a
  server-side result folder. If that also fails, reconnect fails clearly.

Detached clients and killed clients do not receive output files at completion
time, because there is no live TCP connection. Use `-reconnect JOB_ID` to view
status, or `-reconnect JOB_ID -download` to download files, as long as the
server process is still alive.

### Reconnect By Batch Id

Use plain reconnect as a status/visualization operation:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001
```

Use reconnect with download when you want a local copy of the server output
files:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001 -download
```

The server tracks whether each connected client asked for downloads. A regular
non-detached submission is equivalent to a connection with download enabled.
Plain reconnect is a connection with download disabled.

### Reconnect By Result Folder

If a batch id is unknown, `-reconnect` can import an existing server-side result
folder:

```sh
benchmark -server 127.0.0.1:8765 -reconnect QF_NRA5-123456-t300
```

The folder argument is interpreted by the server:

- a relative folder is resolved under server `-output-root`
- an absolute folder is used as an absolute server-side path
- the folder only needs to contain result CSV files
- `log` and `results.xlsx` are optional

Import behavior:

- the server reads all `*.csv` files in the folder
- `results.xlsx` is generated or overwritten in that folder
- a new finished batch id is created and printed
- reconnecting to the same canonical folder reuses the same batch id
- `-reconnect FOLDER -download` works during the folder reconnect, and
  `-reconnect JOB_ID -download` works during a later reconnect to the new batch id

Examples:

```sh
benchmark -server 127.0.0.1:8765 -reconnect QF_NRA5-123456-t300
benchmark -server 127.0.0.1:8765 -reconnect QF_NRA5-123456-t300 -download
benchmark -server 127.0.0.1:8765 -reconnect /srv/old-results/QF_NRA5 -download
```

Folder import is intended for completed CSV data. It does not recover queued or
running jobs after a server restart.

### Aggregate Result Folders

Use `-aggregate PREFIX` to combine CSV files from several result folders that
share a folder-name prefix:

```sh
benchmark -server 127.0.0.1:8765 -aggregate QF_NRA5
```

For a relative prefix, the server resolves paths under `-output-root`, reads all
CSV files matching `PREFIX*/*.csv`, and writes `PREFIX.xlsx` next to the
matching folders. This behaves like importing a temporary directory containing
the matching CSV files, except no temporary directory is created.

Add `-download` to transfer the generated workbook into a fresh local download
directory:

```sh
benchmark -server 127.0.0.1:8765 -aggregate QF_NRA5 -download
```

## Killing A Batch

Kill a queued or running batch by id:

```sh
benchmark -server 127.0.0.1:8765 -kill batch-000001
```

The server removes pending jobs for that batch and cancels running jobs. Killing
a batch does not download files. Reconnecting to a killed batch reports the
killed status.

## Pausing A Batch

Pause a queued or running batch by id:

```sh
benchmark -server 127.0.0.1:8765 -pause batch-000001
```

The server keeps the batch unfinished and does not cancel currently running
jobs. Those jobs are allowed to finish normally, but queued jobs from the paused
batch are not scheduled while the batch is paused.

Resume the batch with:

```sh
benchmark -server 127.0.0.1:8765 -unpause batch-000001
```

Pausing and unpausing do not download files. Use `-reconnect JOB_ID` to wait for
completion, or `-reconnect JOB_ID -download` to download files when the batch
finishes.

## Output Files

For a batch with `S` solver commands, the server writes:

- `log`
- one `<solver>.csv` per solver
- `results.xlsx` if `-excel` or `-xml` was used

So a normal server-produced batch produces:

- `S + 1` files without Excel
- `S + 2` files with Excel

Imported result folders may contain only CSV files initially. During import,
the server writes `results.xlsx` into the imported folder. If there is no `log`
file in the imported folder, downloads from that imported batch omit `log`.

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

The server output directory path in the `accepted` event may be absolute or
relative depending on how the server was configured. The client does not use
that path as a destination path. It only uses the basename to choose a local
download directory under the client process's current working directory.

Only directories get `-runN`. File names transferred from server to client do
not contain counting suffixes. They remain exactly:

- `log`
- `<solver>.csv`
- `results.xlsx`

## Spreadsheet Output

`results.xlsx` is a standard Excel workbook. It contains:

- `Totals` as the first tab, with formulas over whole columns of each solver tab
- `Clashes` as the second tab, listing benchmarks where at least one solver says
  `sat` and another says `unsat`
- one tab per solver after that, mirroring the corresponding CSV file

Solver tabs do not include a header row. In `Clashes`, column A is the benchmark
name and columns B onward are solver names. Each clash row includes the result
reported by every solver for that benchmark.

## Result Transfer

At the end of a regular non-detached submission, the server sends the output
files over the existing TCP connection. The client writes them locally and
prints:

```text
downloaded output files to /path/to/local/output-dir
```

Plain reconnect never downloads files. `-reconnect JOB_ID_OR_FOLDER -download`
transfers the files into a fresh local directory.

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
benchmark \
  -server 127.0.0.1:8765 \
  -server-benchmark-root /tmp/benchmarks \
  -server-exe-root /tmp/solvers \
  -timeout 120 \
  -memory 4 \
  examples.txt \
  solver
```

Submit and detach:

```sh
benchmark \
  -server 127.0.0.1:8765 \
  -server-benchmark-root /tmp/benchmarks \
  -server-exe-root /tmp/solvers \
  -timeout 120 \
  -detach \
  examples.txt \
  solver
```

Reconnect later:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001
```

Reconnect later and download files:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001 -download
```

Watch the whole server queue:

```sh
benchmark -server 127.0.0.1:8765 -state
```

Import an existing server result folder without downloading:

```sh
benchmark -server 127.0.0.1:8765 -reconnect old-result-folder
```

Import an existing server result folder and download:

```sh
benchmark -server 127.0.0.1:8765 -reconnect /srv/bench-results/old-result-folder -download
```

Kill a running batch:

```sh
benchmark -server 127.0.0.1:8765 -kill batch-000001
```

Generate Excel output:

```sh
benchmark \
  -server 127.0.0.1:8765 \
  -server-benchmark-root /tmp/benchmarks \
  -server-exe-root /tmp/solvers \
  -excel \
  examples.txt \
  solver-a solver-b
```

Expose a server on a trusted network:

```sh
benchmark-server -host 0.0.0.0 -port 9000 -cores 16 -max-memory 128 -output-root /srv/bench-results
```

Then submit from a client:

```sh
benchmark \
  -server server.example.org:9000 \
  -server-benchmark-root /srv/smtlib \
  -server-exe-root /opt/solvers \
  local-list.txt \
  yices
```

The benchmark list file in this example is local to the client. The benchmark
entries in that file are resolved under `/srv/smtlib` on the server, and the
relative solver command `yices` is resolved under `/opt/solvers` on the server.

## Current Limitations

- There is no authentication.
- The server executes arbitrary submitted commands as the server OS user.
- `-generations 0` infinite mode is not supported in server mode.
- State persistence is not a live process checkpoint: running solver processes
  are lost on server exit and their unfinished jobs are requeued on restart.
  Recently completed jobs may also be requeued if they finished after the last
  `-state-save-interval` checkpoint.
- State files are written by one server process. Do not run two benchmark
  servers against the same `-state-file`.
- Reconnect by folder can recreate a finished batch id from CSV files when no
  state file is available, but folder import by itself cannot recover
  queued/running work.
