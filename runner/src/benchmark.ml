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
let pause = ref None
let unpause = ref None
let download = ref None
let state_view = ref false
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
  | Protocol.Batch_paused { batch_id; message } ->
      Printf.printf "paused %s: %s\n%!" batch_id message
  | Protocol.Batch_unpaused { batch_id; message } ->
      Printf.printf "unpaused %s: %s\n%!" batch_id message
  | Protocol.State_snapshot _ -> ()

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
  | Protocol.Batch_paused { batch_id; message } ->
      Printf.printf "paused %s: %s\n%!" batch_id message
  | Protocol.Batch_unpaused { batch_id; message } ->
      Printf.printf "unpaused %s: %s\n%!" batch_id message
  | event -> print_event event

let terminal_width () =
  match Terminal.Size.get_columns () with
  | Some width when width >= 40 -> width
  | _ -> 80

let fit_to_width width s =
  let len = String.length s in
  if len = width then s
  else if len < width then s ^ String.make (width - len) ' '
  else String.sub s 0 width

let ellipsize width s =
  let len = String.length s in
  if len <= width then s
  else if width <= 3 then String.sub s 0 (max 0 width)
  else String.sub s 0 (width - 3) ^ "..."

let render_batch_summary width (batch : Protocol.batch_summary) =
  let total = max 1 batch.total_jobs in
  let completed = max 0 (min batch.completed batch.total_jobs) in
  let pct = (completed * 100) / total in
  let status =
    if batch.paused then "paused"
    else if batch.running_jobs > 0 then Format.sprintf "%d running" batch.running_jobs
    else if batch.queued_jobs > 0 then "queued"
    else "waiting"
  in
  let suffix =
    Format.sprintf " %d/%d jobs %3d%% | %db %ds x%d | q%d r%d %s"
      completed batch.total_jobs pct batch.total_benchmarks batch.total_solvers
      batch.generations batch.queued_jobs batch.running_jobs status
  in
  let raw_prefix = Format.sprintf "%s %s" batch.batch_id batch.benchmark_name in
  let min_bar_width = 10 in
  let prefix_width = max 0 (width - String.length suffix - min_bar_width - 3) in
  let prefix = ellipsize prefix_width raw_prefix in
  let bar_width = max 1 (width - String.length prefix - String.length suffix - 3) in
  let filled = if batch.total_jobs <= 0 then 0 else (bar_width * completed) / batch.total_jobs in
  let empty = bar_width - filled in
  fit_to_width width
    (Format.sprintf "%s [%s%s]%s" prefix (String.make filled '#') (String.make empty '.')
       suffix)

let render_state_snapshot previous_lines batches =
  let width = terminal_width () in
  let lines =
    match batches with
    | [] -> [ fit_to_width width "queue empty" ]
    | batches -> List.map (render_batch_summary width) batches
  in
  if Unix.isatty Unix.stdout then (
    let old_lines = !previous_lines in
    let new_lines = List.length lines in
    if old_lines > 0 then Printf.printf "\027[%dA%!" old_lines;
    let rows = max old_lines new_lines in
    for i = 0 to rows - 1 do
      let line = if i < new_lines then List.nth lines i else "" in
      Printf.printf "\r\027[2K%s\n%!" line
    done;
    if old_lines > new_lines then Printf.printf "\027[%dA%!" (old_lines - new_lines);
    previous_lines := new_lines)
  else (
    List.iter print_endline lines;
    print_endline "";
    flush stdout)

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

let wait_with_progress reader ~download_outputs ~local_output_dir ~initial_prior_completed ~prior_total
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
          if download_outputs then write_output_file local_output_dir ~name ~contents;
          loop ()
      | Protocol.State_snapshot _ -> loop ()
      | Protocol.Job_finished { completed = new_completed; _ } ->
          apply_completed update_progress new_completed;
          loop ()
      | Protocol.Batch_finished _ ->
          apply_completed update_progress total;
          (0, event)
      | Protocol.Batch_failed _ -> (1, event)
      | Protocol.Batch_killed _ -> (0, event)
      | Protocol.Batch_paused _ | Protocol.Batch_unpaused _ -> loop ()
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
  | Protocol.Batch_finished _ when download_outputs -> print_download_dir !local_output_dir
  | _ -> ());
  code

let submit ~detach_after_accept ~download_outputs request =
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
  | Protocol.Accepted { output_dir; _ } when not detach_after_accept && download_outputs ->
      ignore (ensure_local_output_dir local_output_dir output_dir)
  | _ -> ());
  match first_event with
  | Protocol.Accepted _ when detach_after_accept -> 0
  | Protocol.Accepted { total_jobs; completed; prior_jobs; prior_completed; _ } ->
      wait_with_progress reader ~download_outputs ~local_output_dir
        ~initial_prior_completed:prior_completed ~prior_total:prior_jobs
        ~initial_completed:completed ~total:total_jobs
  | Protocol.Batch_finished _ -> 0
  | Protocol.Batch_failed _ -> 1
  | Protocol.Batch_killed _ -> 0
  | Protocol.Batch_paused _ -> 0
  | Protocol.Batch_unpaused _ -> 0
  | Protocol.Job_finished { completed; total; _ } ->
      wait_with_progress reader ~download_outputs ~local_output_dir ~initial_prior_completed:0
        ~prior_total:0 ~initial_completed:completed ~total
  | Protocol.Queue_progress { completed; total; _ } ->
      wait_with_progress reader ~download_outputs ~local_output_dir
        ~initial_prior_completed:completed ~prior_total:total ~initial_completed:0 ~total:1
  | Protocol.Output_file { name; contents; _ } ->
      if download_outputs then write_output_file local_output_dir ~name ~contents;
      wait_with_progress reader ~download_outputs ~local_output_dir ~initial_prior_completed:0
        ~prior_total:0 ~initial_completed:0 ~total:1
  | Protocol.State_snapshot _ -> 1

let show_state () =
  let host, port = parse_server !server in
  Eio_main.run @@ fun env ->
  Eio.Net.with_tcp_connect ~host ~service:(string_of_int port) (Stdenv.net env) @@ fun flow ->
  let reader = Buf_read.of_flow flow ~max_size:Protocol_limits.max_json_line_size in
  Buf_write.with_flow flow @@ fun writer ->
  send_line writer (Protocol.encode_request Protocol.State);
  let previous_lines = ref 0 in
  let rec loop () =
    match Buf_read.line reader |> Protocol.decode_event with
    | Protocol.State_snapshot { batches } ->
        render_state_snapshot previous_lines batches;
        loop ()
    | Protocol.Batch_failed { batch_id; message } ->
        print_event (Protocol.Batch_failed { batch_id; message });
        1
    | _ -> loop ()
  in
  loop ()

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
    ("-excel", Arg.Set excel, "Also output an Excel spreadsheet results.xlsx");
    ("-xml", Arg.Set excel, "Deprecated alias for -excel");
    ("-server", Arg.Set_string server, "Benchmark server endpoint HOST:PORT");
    ( "-server-benchmark-root",
      Arg.String (fun root -> server_benchmark_root := Some root),
      "Server-visible root for relative benchmark entries" );
    ( "-server-exe-root",
      Arg.String (fun root -> server_exe_root := Some root),
      "Server-visible root for relative solver commands" );
    ("-detach", Arg.Set detach, "Submit the batch and exit after server acceptance");
    ( "-reconnect",
      Arg.String (fun id -> reconnect := Some id),
      "Reconnect to an existing server job id or import a server-side result folder" );
    ( "-download",
      Arg.String (fun id -> download := Some id),
      "Reconnect to an existing server job id or import a server-side result folder, then download \
       output files" );
    ("-state", Arg.Set state_view, "Continuously display server queue state");
    ("-kill", Arg.String (fun id -> kill := Some id), "Kill an existing server job id");
    ("-pause", Arg.String (fun id -> pause := Some id), "Pause an existing server job id");
    ("-unpause", Arg.String (fun id -> unpause := Some id), "Unpause an existing server job id");
  ]

let description =
  "Client for benchmark-server.\n\
   First argument: ASCII file containing list of benchmarks (1 per line)\n\
   Next arguments: executables to process each benchmark"

let () =
  Arg.parse options (fun a -> args := a :: !args) description;
  let args = List.rev !args in
  let selected_actions =
    List.filter_map
      (fun action -> action)
      [
        Option.map (fun id -> (`Reconnect, id)) !reconnect;
        Option.map (fun id -> (`Download, id)) !download;
        Option.map (fun id -> (`Kill, id)) !kill;
        Option.map (fun id -> (`Pause, id)) !pause;
        Option.map (fun id -> (`Unpause, id)) !unpause;
      ]
  in
  if !state_view then (
    if selected_actions <> [] || args <> [] then (
      prerr_endline
        "benchmark: -state is mutually exclusive with -reconnect, -download, -kill, -pause, \
         -unpause, and benchmark submission";
      exit 2);
    exit (show_state ()));
  (match !reconnect, !download, !kill, !pause, !unpause with
  | Some _, Some _, _, _, _ ->
      prerr_endline "benchmark: -reconnect and -download are mutually exclusive";
      exit 2
  | Some _, None, Some _, _, _ ->
      prerr_endline "benchmark: -reconnect and -kill are mutually exclusive";
      exit 2
  | None, Some _, Some _, _, _ ->
      prerr_endline "benchmark: -download and -kill are mutually exclusive";
      exit 2
  | _ when List.length selected_actions > 1 ->
      prerr_endline
        "benchmark: -reconnect, -download, -kill, -pause, and -unpause are mutually exclusive";
      exit 2
  | _ -> ());
  match selected_actions, args with
  | [ (`Reconnect, batch_id) ], [] ->
      exit
        (submit ~detach_after_accept:false ~download_outputs:false
           (Protocol.Reconnect { batch_id; download = false }))
  | [ (`Download, batch_id) ], [] ->
      exit
        (submit ~detach_after_accept:false ~download_outputs:true
           (Protocol.Reconnect { batch_id; download = true }))
  | [ (`Kill, batch_id) ], [] ->
      exit (submit ~detach_after_accept:false ~download_outputs:false (Protocol.Kill { batch_id }))
  | [ (`Pause, batch_id) ], [] ->
      exit (submit ~detach_after_accept:false ~download_outputs:false (Protocol.Pause { batch_id }))
  | [ (`Unpause, batch_id) ], [] ->
      exit
        (submit ~detach_after_accept:false ~download_outputs:false
           (Protocol.Unpause { batch_id }))
  | [ (action, _) ], _ ->
      let name =
        match action with
        | `Reconnect -> "reconnect"
        | `Download -> "download"
        | `Kill -> "kill"
        | `Pause -> "pause"
        | `Unpause -> "unpause"
      in
      prerr_endline
        ("benchmark: -" ^ name ^ " does not take a benchmark file or solver command");
      exit 2
  | [], benchmark_file :: commands ->
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
      exit
        (submit ~detach_after_accept:!detach ~download_outputs:true
           (Protocol.Submit request))
  | [], _ ->
      Printf.printf "%s\n" (Arg.usage_string options "Usage: benchmark [options] BENCHMARK_FILE COMMAND...")
  | _ -> assert false
