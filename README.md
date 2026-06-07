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

The server keeps its queue, live batch table, and imported-folder table in
memory. If the server exits, queued/running job state and batch ids are lost,
although files already written on the server disk remain there. A completed
CSV result folder can be imported later with `benchmark -reconnect FOLDER` or
`benchmark -download FOLDER`.

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
- `-download JOB_ID_OR_FOLDER`
  Reconnect to an existing server-side batch id or import a result folder, then
  download output files when the reconnect finishes.
- `-kill JOB_ID`
  Kill a queued or running batch.

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
| client `BENCHMARK_LIST` argument | client | relative to the client process's current working directory |
| client `-server-benchmark-root DIR` | server | relative to the server process's current working directory |
| client `-server-exe-root DIR` | server | relative to the server process's current working directory |
| benchmark entry inside `BENCHMARK_LIST` | server | relative to `-server-benchmark-root` |
| solver command argument | server | relative to `-server-exe-root` |
| `-reconnect FOLDER` / `-download FOLDER` folder import | server | relative to server `-output-root` |
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
- Use `-download JOB_ID_OR_FOLDER` to reconnect and download files at the end.
- If the id is unknown, the server tries to interpret the argument as a
  server-side result folder. If that also fails, reconnect fails clearly.

Detached clients and killed clients do not receive output files at completion
time, because there is no live TCP connection. Use `-reconnect JOB_ID` to view
status, or `-download JOB_ID` to download files, as long as the
server process is still alive.

### Reconnect By Batch Id

Use plain reconnect as a status/visualization operation:

```sh
benchmark -server 127.0.0.1:8765 -reconnect batch-000001
```

Use reconnect with download when you want a local copy of the server output
files:

```sh
benchmark -server 127.0.0.1:8765 -download batch-000001
```

The server tracks whether each connected client asked for downloads. A regular
non-detached submission is equivalent to a connection with download enabled.
Plain reconnect is a connection with download disabled.

### Reconnect By Result Folder

If a batch id is unknown, `-reconnect` and `-download` can import an existing
server-side result folder:

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
- `-download FOLDER` works during the folder reconnect, and
  `-download JOB_ID` works during a later reconnect to the new batch id

Examples:

```sh
benchmark -server 127.0.0.1:8765 -reconnect QF_NRA5-123456-t300
benchmark -server 127.0.0.1:8765 -download QF_NRA5-123456-t300
benchmark -server 127.0.0.1:8765 -download /srv/old-results/QF_NRA5
```

Folder import is intended for completed CSV data. It does not recover queued or
running jobs after a server restart.

## Killing A Batch

Kill a queued or running batch by id:

```sh
benchmark -server 127.0.0.1:8765 -kill batch-000001
```

The server removes pending jobs for that batch and cancels running jobs. Killing
a batch does not download files. Reconnecting to a killed batch reports the
killed status.

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

Plain reconnect never downloads files. `-download JOB_ID_OR_FOLDER` transfers
the files into a fresh local directory.

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
benchmark -server 127.0.0.1:8765 -download batch-000001
```

Import an existing server result folder without downloading:

```sh
benchmark -server 127.0.0.1:8765 -reconnect old-result-folder
```

Import an existing server result folder and download:

```sh
benchmark -server 127.0.0.1:8765 -download /srv/bench-results/old-result-folder
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

- Queue state is in memory only.
- There is no authentication.
- The server executes arbitrary submitted commands as the server OS user.
- `-generations 0` infinite mode is not supported in server mode.
- Reconnect by batch id works only while the server process that knows that id
  is still alive.
- Reconnect by folder can recreate a finished batch id from CSV files, but it
  cannot recover queued/running work.
