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
let reconnect = ref None
let kill = ref None
let server_benchmark_root = ref None
let server_exe_root = ref None

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

let local_default_root path =
  let dir = Filename.dirname path in
  if Filename.is_relative dir then Filename.concat (Sys.getcwd ()) dir else dir

let require_server_roots benchmark_file =
  let benchmark_root =
    match !server_benchmark_root with
    | Some root -> root
    | None -> local_default_root benchmark_file
  in
  let exe_root =
    match !server_exe_root with
    | Some root -> root
    | None -> Sys.getcwd ()
  in
  (benchmark_root, exe_root)

let send_line writer line =
  Buf_write.string writer line;
  Buf_write.char writer '\n';
  Buf_write.flush writer

let print_event = function
  | Protocol.Accepted { batch_id; output_dir; total_jobs; prior_jobs; _ } ->
      let waiting =
        if prior_jobs > 0 then Format.sprintf ", waiting for %d prior jobs" prior_jobs else ""
      in
      Printf.printf "accepted job id %s (%d jobs%s), output %s\n%!" batch_id total_jobs
        waiting output_dir
  | Protocol.Queue_progress _ -> ()
  | Protocol.Output_file _ -> ()
  | Protocol.Job_finished { completed; total; solver; benchmark; result; _ } ->
      Printf.printf "[%d/%d] %s %s -> %s\n%!" completed total solver benchmark result
  | Protocol.Batch_finished { batch_id; output_dir } ->
      Printf.printf "finished %s, output %s\n%!" batch_id output_dir
  | Protocol.Batch_failed { batch_id; message } ->
      let prefix = if batch_id = "" then "batch failed" else "batch " ^ batch_id ^ " failed" in
      Printf.eprintf "%s: %s\n%!" prefix message
  | Protocol.Batch_killed { batch_id; message } ->
      Printf.printf "killed %s: %s\n%!" batch_id message

let progress_section ?bar_width ~color ~label total =
  let open Progress in
  let open Line in
  let width = String.length (string_of_int total) in
  let count = sum ~pp:(Printer.int ~width) ~width () in
  let style = `Custom (Bar_style.(with_color (Color.ansi color) utf8)) in
  let rendered_bar =
    match bar_width with
    | None -> bar ~style total
    | Some width -> bar ~width ~style total
  in
  const label
  ++ const " "
  ++ rendered_bar
  ++ percentage_of total
  ++ const " "
  ++ parens (count ++ const ("/" ^ string_of_int total))

let progress_bar ~prior_total ~job_total =
  let open Progress.Line in
  if prior_total > 0 then
    let prior = progress_section ~bar_width:(`Fixed 20) ~color:`red ~label:"prior" prior_total in
    let job = progress_section ~bar_width:(`Fixed 20) ~color:`cyan ~label:"job" job_total in
    elapsed () ++ const " " ++ pair ~sep:(const "  ") prior job
  else
    let job = progress_section ~color:`cyan ~label:"job" job_total in
    elapsed () ++ const " " ++ using snd job

let print_final_event = function
  | Protocol.Batch_finished { batch_id; output_dir } ->
      Printf.printf "finished %s, output %s\n%!" batch_id output_dir
  | Protocol.Batch_failed { batch_id; message } ->
      let prefix = if batch_id = "" then "batch failed" else "batch " ^ batch_id ^ " failed" in
      Printf.eprintf "%s: %s\n%!" prefix message
  | Protocol.Batch_killed { batch_id; message } ->
      Printf.printf "killed %s: %s\n%!" batch_id message
  | event -> print_event event

let ensure_local_output_dir local_output_dir server_output_dir =
  match !local_output_dir with
  | Some dir -> dir
  | None ->
      let dir = Common.fresh_output_dir_native ~root:(Sys.getcwd ()) ~output_dir:server_output_dir in
      local_output_dir := Some dir;
      dir

let write_output_file local_output_dir ~name ~contents =
  let dir =
    match !local_output_dir with
    | Some dir -> dir
    | None -> ensure_local_output_dir local_output_dir "benchmark-output"
  in
  let path = Filename.concat dir (Common.safe_output_file_name name) in
  Common.write_file_binary path (Protocol.base64_decode contents)

let print_download_dir = function
  | Some dir -> Printf.printf "downloaded output files to %s\n%!" dir
  | None -> ()

let wait_with_progress reader ~local_output_dir ~initial_prior_completed ~prior_total
    ~initial_completed ~total =
  let prior_completed = ref 0 in
  let completed = ref 0 in
  let apply_prior_completed update_progress new_completed =
    let new_completed = max 0 (min prior_total new_completed) in
    let delta = new_completed - !prior_completed in
    if delta > 0 then (
      update_progress (delta, 0);
      prior_completed := new_completed)
  in
  let apply_completed update_progress new_completed =
    let new_completed = max 0 (min total new_completed) in
    let delta = new_completed - !completed in
    if delta > 0 then (
      update_progress (0, delta);
      completed := new_completed)
  in
  let code, final_event =
    Progress.with_reporter (progress_bar ~prior_total ~job_total:total) @@ fun update_progress ->
    apply_prior_completed update_progress initial_prior_completed;
    apply_completed update_progress initial_completed;
    let rec loop () =
      let event = Buf_read.line reader |> Protocol.decode_event in
      match event with
      | Protocol.Queue_progress { completed = new_completed; _ } ->
          apply_prior_completed update_progress new_completed;
          loop ()
      | Protocol.Output_file { name; contents; _ } ->
          write_output_file local_output_dir ~name ~contents;
          loop ()
      | Protocol.Job_finished { completed = new_completed; _ } ->
          apply_completed update_progress new_completed;
          loop ()
      | Protocol.Batch_finished _ ->
          apply_completed update_progress total;
          (0, event)
      | Protocol.Batch_failed _ -> (1, event)
      | Protocol.Batch_killed _ -> (0, event)
      | Protocol.Accepted
          {
            completed = new_completed;
            prior_completed = new_prior_completed;
            _;
          } ->
          apply_prior_completed update_progress new_prior_completed;
          apply_completed update_progress new_completed;
          loop ()
    in
    loop ()
  in
  print_final_event final_event;
  (match final_event with
  | Protocol.Batch_finished _ -> print_download_dir !local_output_dir
  | _ -> ());
  code

let submit ~detach_after_accept request =
  let host, port = parse_server !server in
  Eio_main.run @@ fun env ->
  Eio.Net.with_tcp_connect ~host ~service:(string_of_int port) (Stdenv.net env) @@ fun flow ->
  let reader = Buf_read.of_flow flow ~max_size:Protocol_limits.max_json_line_size in
  Buf_write.with_flow flow @@ fun writer ->
  send_line writer (Protocol.encode_request request);
  let first_event = Buf_read.line reader |> Protocol.decode_event in
  print_event first_event;
  let local_output_dir = ref None in
  (match first_event with
  | Protocol.Accepted { output_dir; _ } when not detach_after_accept ->
      ignore (ensure_local_output_dir local_output_dir output_dir)
  | _ -> ());
  match first_event with
  | Protocol.Accepted _ when detach_after_accept -> 0
  | Protocol.Accepted { total_jobs; completed; prior_jobs; prior_completed; _ } ->
      wait_with_progress reader ~local_output_dir ~initial_prior_completed:prior_completed
        ~prior_total:prior_jobs ~initial_completed:completed ~total:total_jobs
  | Protocol.Batch_finished _ -> 0
  | Protocol.Batch_failed _ -> 1
  | Protocol.Batch_killed _ -> 0
  | Protocol.Job_finished { completed; total; _ } ->
      wait_with_progress reader ~local_output_dir ~initial_prior_completed:0 ~prior_total:0
        ~initial_completed:completed ~total
  | Protocol.Queue_progress { completed; total; _ } ->
      wait_with_progress reader ~local_output_dir ~initial_prior_completed:completed
        ~prior_total:total ~initial_completed:0 ~total:1
  | Protocol.Output_file { name; contents; _ } ->
      write_output_file local_output_dir ~name ~contents;
      wait_with_progress reader ~local_output_dir ~initial_prior_completed:0 ~prior_total:0
        ~initial_completed:0 ~total:1

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
    ( "-server-benchmark-root",
      Arg.String (fun root -> server_benchmark_root := Some root),
      "Server-visible root for relative benchmark entries" );
    ( "-server-exe-root",
      Arg.String (fun root -> server_exe_root := Some root),
      "Server-visible root for relative solver commands" );
    ("-detach", Arg.Set detach, "Submit the batch and exit after server acceptance");
    ("-reconnect", Arg.String (fun id -> reconnect := Some id), "Reconnect to an existing server job id");
    ("-kill", Arg.String (fun id -> kill := Some id), "Kill an existing server job id");
  ]

let description =
  "Client for benchmark-server.\n\
   First argument: ASCII file containing list of benchmarks (1 per line)\n\
   Next arguments: executables to process each benchmark"

let () =
  Arg.parse options (fun a -> args := a :: !args) description;
  match !reconnect, !kill, List.rev !args with
  | Some _, Some _, _ ->
      prerr_endline "benchmark: -reconnect and -kill are mutually exclusive";
      exit 2
  | Some batch_id, None, [] ->
      exit (submit ~detach_after_accept:false (Protocol.Reconnect { batch_id }))
  | Some _, None, _ ->
      prerr_endline "benchmark: -reconnect does not take a benchmark file or solver command";
      exit 2
  | None, Some batch_id, [] ->
      exit (submit ~detach_after_accept:false (Protocol.Kill { batch_id }))
  | None, Some _, _ ->
      prerr_endline "benchmark: -kill does not take a benchmark file or solver command";
      exit 2
  | None, None, benchmark_file :: commands ->
      validate_input benchmark_file commands;
      let lines = CCIO.(with_in benchmark_file read_lines_l) in
      if lines = [] then (
        prerr_endline "benchmark: benchmark file is empty";
        exit 2);
      let server_benchmark_root, server_exe_root = require_server_roots benchmark_file in
      let request : Protocol.submit_request =
        {
          benchmark_file;
          benchmark_name = Common.prune benchmark_file;
          server_benchmark_root;
          server_exe_root;
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
      exit (submit ~detach_after_accept:!detach (Protocol.Submit request))
  | None, None, _ ->
      Printf.printf "%s\n" (Arg.usage_string options "Usage: benchmark [options] BENCHMARK_FILE COMMAND...")
