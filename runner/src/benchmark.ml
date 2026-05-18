open Eio
open Runner_lib

let args = ref []
let timeout = ref 300
let memory = ref None
let sort = ref "alpha"
let generations = ref 1
let excel = ref false
let detach = ref false
let server = ref (Sys.getenv_opt "BENCHMARK_SERVER" |> Option.value ~default:"127.0.0.1:8765")
let cores_was_used = ref None

let parse_sort = function
  | "wall" -> sort := "wall"
  | "user" -> sort := "user"
  | _ ->
      prerr_endline "benchmark: -timesort requires argument \"wall\" or \"user\"";
      exit 2

let parse_server endpoint =
  match String.rindex_opt endpoint ':' with
  | None ->
      prerr_endline "benchmark: -server must have form HOST:PORT";
      exit 2
  | Some i ->
      let host = String.sub endpoint 0 i in
      let port_s = String.sub endpoint (i + 1) (String.length endpoint - i - 1) in
      if host = "" || port_s = "" then (
        prerr_endline "benchmark: -server must have form HOST:PORT";
        exit 2);
      let port =
        try int_of_string port_s
        with Failure _ ->
          prerr_endline "benchmark: server port must be an integer";
          exit 2
      in
      (host, port)

let validate_input benchmark_file commands =
  if Option.is_some !cores_was_used then (
    prerr_endline
      "benchmark: -cores is now a benchmark-server option. Start benchmark-server with \
       -cores N, then submit with benchmark -server HOST:PORT ...";
    exit 2);
  if !timeout < 1 then (
    prerr_endline "benchmark: -timeout must be at least 1";
    exit 2);
  Option.iter
    (fun memory ->
      if memory < 1 then (
        prerr_endline "benchmark: -memory must be at least 1";
        exit 2))
    !memory;
  if !generations < 1 then (
    prerr_endline
      "benchmark: server mode requires -generations to be at least 1; infinite batches \
       cannot be queued";
    exit 2);
  if commands = [] then (
    prerr_endline "benchmark: at least one solver command is required";
    exit 2);
  if not (Sys.file_exists benchmark_file) then (
    prerr_endline ("benchmark: benchmark file does not exist: " ^ benchmark_file);
    exit 2)

let send_line writer line =
  Buf_write.string writer line;
  Buf_write.char writer '\n';
  Buf_write.flush writer

let print_event = function
  | Protocol.Accepted { batch_id; output_dir; total_jobs } ->
      Printf.printf "accepted %s (%d jobs), output %s\n%!" batch_id total_jobs output_dir
  | Protocol.Job_finished { completed; total; solver; benchmark; result; _ } ->
      Printf.printf "[%d/%d] %s %s -> %s\n%!" completed total solver benchmark result
  | Protocol.Batch_finished { batch_id; output_dir } ->
      Printf.printf "finished %s, output %s\n%!" batch_id output_dir
  | Protocol.Batch_failed { batch_id; message } ->
      let prefix = if batch_id = "" then "batch failed" else "batch " ^ batch_id ^ " failed" in
      Printf.eprintf "%s: %s\n%!" prefix message

let submit request =
  let host, port = parse_server !server in
  Eio_main.run @@ fun env ->
  Eio.Net.with_tcp_connect ~host ~service:(string_of_int port) (Stdenv.net env) @@ fun flow ->
  let reader = Buf_read.of_flow flow ~max_size:1_000_000 in
  Buf_write.with_flow flow @@ fun writer ->
  send_line writer (Protocol.encode_submit request);
  let rec read_loop () =
    let event = Buf_read.line reader |> Protocol.decode_event in
    print_event event;
    match event with
    | Protocol.Accepted _ when request.detach -> 0
    | Protocol.Batch_finished _ -> 0
    | Protocol.Batch_failed _ -> 1
    | _ -> read_loop ()
  in
  read_loop ()

let options =
  [
    ( "-cores",
      Arg.Int (fun n -> cores_was_used := Some n),
      "Deprecated on the client; use benchmark-server -cores N" );
    ("-timeout", Arg.Int (fun u -> timeout := u), "Timeout for each task in seconds");
    ("-memory", Arg.Int (fun u -> memory := Some u), "Maximum memory per job in Gb");
    ( "-timesort",
      Arg.String parse_sort,
      "Sort output by increasing solve time, with argument \"wall\" or \"user\"" );
    ("-generations", Arg.Int (fun u -> generations := u), "Number of generations");
    ("-excel", Arg.Set excel, "Also output an Excel-readable spreadsheet results.xls");
    ("-xml", Arg.Set excel, "Deprecated alias for -excel");
    ("-server", Arg.Set_string server, "Benchmark server endpoint HOST:PORT");
    ("-detach", Arg.Set detach, "Submit the batch and exit after server acceptance");
  ]

let description =
  "Client for benchmark-server.\n\
   First argument: ASCII file containing list of benchmarks (1 per line)\n\
   Next arguments: executables to process each benchmark"

let () =
  Arg.parse options (fun a -> args := a :: !args) description;
  match List.rev !args with
  | benchmark_file :: commands ->
      validate_input benchmark_file commands;
      let lines = CCIO.(with_in benchmark_file read_lines_l) in
      if lines = [] then (
        prerr_endline "benchmark: benchmark file is empty";
        exit 2);
      let cwd = Sys.getcwd () in
      let request : Protocol.submit_request =
        {
          cwd;
          benchmark_file;
          benchmark_prefix = Filename.dirname benchmark_file;
          benchmark_name = Common.prune benchmark_file;
          lines;
          commands;
          timeout = !timeout;
          memory = !memory;
          generations = !generations;
          sort = !sort;
          excel = !excel;
          detach = !detach;
        }
      in
      exit (submit request)
  | _ ->
      Printf.printf "%s\n" (Arg.usage_string options "Usage: benchmark [options] BENCHMARK_FILE COMMAND...")
