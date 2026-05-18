open Eio
open Runner_lib
open Protocol

type job = {
  batch : batch;
  solver : string;
  benchmark : string;
  reserve_memory : int;
}

and batch = {
  sequence : int;
  id : string;
  request : Protocol.submit_request;
  out_dir : string;
  htbl : (int * Common.aggregate) Common.HStrings.t Common.HStrings.t;
  log : string Stream.t;
  log_done : bool ref;
  log_drained : Eio.Condition.t;
  mutable log_closed : bool;
  watchers : Protocol.event Stream.t list ref;
  total_jobs : int;
  prior_jobs : int;
  mutable prior_completed : int;
  mutable completed : int;
  mutable status : batch_status;
}

and batch_status =
  | Running
  | Finished
  | Failed of string

type state = {
  cores : int;
  max_memory : int option;
  output_root : string;
  mutable next_batch : int;
  mutable pending : job list;
  mutable running : int;
  mutable reserved_memory : int;
  batches : (string, batch) Hashtbl.t;
  wake_scheduler : Eio.Condition.t;
  log_server : string -> unit;
}

let send_line writer line =
  Buf_write.string writer line;
  Buf_write.char writer '\n';
  Buf_write.flush writer

let send_event writer event = send_line writer (Protocol.encode_event event)

let accepted_event batch =
  Accepted
    {
      batch_id = batch.id;
      output_dir = batch.out_dir;
      total_jobs = batch.total_jobs;
      completed = batch.completed;
      prior_jobs = batch.prior_jobs;
      prior_completed = batch.prior_completed;
    }

let add_watcher batch =
  let events = Stream.create max_int in
  batch.watchers := events :: !(batch.watchers);
  events

let remove_watcher batch events =
  batch.watchers := List.filter (fun existing -> existing != events) !(batch.watchers)

let publish batch event =
  List.iter (fun events -> Stream.add events event) !(batch.watchers)

let output_file_names batch =
  Common.result_file_names ~excel:batch.request.excel batch.htbl

let output_file_events batch =
  output_file_names batch
  |> List.filter_map (fun name ->
         let name = Common.safe_output_file_name name in
         let path = Filename.concat batch.out_dir name in
         if Sys.file_exists path then
           Some
             (Output_file
                {
                  batch_id = batch.id;
                  name;
                  contents = Protocol.base64_encode (Common.read_file_binary path);
                })
         else None)

let publish_output_files batch =
  List.iter (publish batch) (output_file_events batch)

let send_output_files writer batch =
  List.iter (send_event writer) (output_file_events batch)

let unfinished_jobs batch =
  match batch.status with
  | Running -> max 0 (batch.total_jobs - batch.completed)
  | Finished | Failed _ -> 0

let prior_jobs state =
  Hashtbl.fold (fun _ batch total -> total + unfinished_jobs batch) state.batches 0

let publish_prior_progress state finished_batch =
  Hashtbl.iter
    (fun _ batch ->
      if batch.sequence > finished_batch.sequence && batch.prior_completed < batch.prior_jobs then (
        batch.prior_completed <- min batch.prior_jobs (batch.prior_completed + 1);
        publish batch
          (Queue_progress
             {
               batch_id = batch.id;
               completed = batch.prior_completed;
               total = batch.prior_jobs;
             })))
    state.batches

let server_addr host port =
  let ip =
    match host with
    | "0.0.0.0" -> Eio.Net.Ipaddr.V4.any
    | "127.0.0.1" | "localhost" -> Eio.Net.Ipaddr.V4.loopback
    | _ ->
        prerr_endline
          "benchmark-server: -host currently accepts 127.0.0.1, localhost, or 0.0.0.0";
        exit 2
  in
  `Tcp (ip, port)

let reserved_memory max_memory per_job_memory =
  match max_memory, per_job_memory with
  | Some max_memory, None -> max_memory
  | _, Some memory -> memory
  | None, None -> 0

let resources_fit state job =
  state.running < state.cores
  &&
  match state.max_memory with
  | None -> true
  | Some max_memory -> state.reserved_memory + job.reserve_memory <= max_memory

let take_runnable state =
  let rec aux prefix = function
    | [] -> None
    | job :: rest when resources_fit state job ->
        state.pending <- List.rev_append prefix rest;
        Some job
    | job :: rest -> aux (job :: prefix) rest
  in
  aux [] state.pending

let result_sort batch = Common.sort_of_name batch.request.sort

let finish_batch state batch =
  match batch.status with
  | Failed _ | Finished -> ()
  | Running -> (
    try
      Common.hashtables_to_files_native batch.out_dir batch.htbl (result_sort batch);
      if batch.request.excel then
        Common.hashtables_to_excel_native batch.out_dir batch.htbl Common.cmp_user;
      batch.log_done := true;
      Stream.add batch.log "";
      while not batch.log_closed do
        Eio.Condition.await_no_mutex batch.log_drained
      done;
      batch.status <- Finished;
      publish_output_files batch;
      publish batch (Batch_finished { batch_id = batch.id; output_dir = batch.out_dir });
      state.log_server
        (Format.sprintf "batch %s finished in %s" batch.id batch.out_dir)
    with exn ->
      let message = Printexc.to_string exn in
      batch.status <- Failed message;
      publish batch (Batch_failed { batch_id = batch.id; message });
      state.log_server (Format.sprintf "batch %s failed: %s" batch.id message))

let run_job proc_mgr state job =
  let batch = job.batch in
  let request = batch.request in
  let solver = Common.solver_path ~root:request.server_exe_root job.solver in
  let instance = Common.benchmark_path ~root:request.server_benchmark_root job.benchmark in
  let result =
    Common.with_timing proc_mgr ~timeout:request.timeout ?memory:request.memory
      ~log:batch.log [ solver; instance ]
  in
  let solver_name = Common.prune job.solver in
  Common.add_result ~htbl:batch.htbl ~nb_benchmarks:(List.length request.lines) solver_name
    job.benchmark result;
  batch.completed <- batch.completed + 1;
  publish_prior_progress state batch;
  publish batch
    (Job_finished
       {
         batch_id = batch.id;
         completed = batch.completed;
         total = batch.total_jobs;
         solver = solver_name;
         benchmark = job.benchmark;
         result = Common.output_to_string result;
       });
  state.running <- state.running - 1;
  state.reserved_memory <- state.reserved_memory - job.reserve_memory;
  if batch.completed = batch.total_jobs then finish_batch state batch;
  Eio.Condition.broadcast state.wake_scheduler

let rec scheduler proc_mgr state sw =
  match take_runnable state with
  | Some job ->
      state.running <- state.running + 1;
      state.reserved_memory <- state.reserved_memory + job.reserve_memory;
      Fiber.fork ~sw (fun () -> run_job proc_mgr state job);
      scheduler proc_mgr state sw
  | None ->
      Eio.Condition.await_no_mutex state.wake_scheduler;
      scheduler proc_mgr state sw

let validate_request state (request : Protocol.submit_request) =
  let fail message = Error message in
  if request.timeout < 1 then fail "-timeout must be at least 1"
  else if request.generations < 1 then
    fail "-generations 0 is not supported by server mode; submit a finite batch"
  else if request.lines = [] then fail "benchmark file is empty"
  else if request.commands = [] then fail "at least one solver command is required"
  else
    match request.memory, state.max_memory with
    | Some memory, _ when memory < 1 -> fail "-memory must be at least 1"
    | Some memory, Some max_memory when memory > max_memory ->
        fail
          (Format.sprintf
             "per-job memory limit %i exceeds server -max-memory %i" memory max_memory)
    | _ -> Ok ()

let create_batch state (request : Protocol.submit_request) =
  let sequence =
    state.next_batch <- state.next_batch + 1;
    state.next_batch
  in
  let id = Format.sprintf "batch-%06d" sequence in
  let digest =
    Common.digest ~benchmark_file:request.benchmark_name ~lines:request.lines
      ~timeout:request.timeout ?memory:request.memory ()
  in
  let out_dir = Common.fresh_output_dir_native ~root:state.output_root ~output_dir:digest in
  let total_jobs = List.length request.lines * List.length request.commands * request.generations in
  let prior_jobs = prior_jobs state in
  let batch =
    {
      sequence;
      id;
      request;
      out_dir;
      htbl = Common.HStrings.create (List.length request.commands);
      log = Stream.create max_int;
      log_done = ref false;
      log_drained = Eio.Condition.create ();
      log_closed = false;
      watchers = ref [];
      total_jobs;
      prior_jobs;
      prior_completed = 0;
      completed = 0;
      status = Running;
    }
  in
  Hashtbl.add state.batches id batch;
  (batch, total_jobs)

let enqueue_jobs state batch =
  let request = batch.request in
  let reserve_memory = reserved_memory state.max_memory request.memory in
  let new_jobs =
    List.init request.generations (fun i -> i + 1)
    |> List.concat_map (fun _generation ->
           List.concat_map
             (fun benchmark ->
               List.map
                 (fun solver -> { batch; solver; benchmark; reserve_memory })
                 request.commands)
             request.lines)
  in
  state.pending <- state.pending @ new_jobs;
  Eio.Condition.broadcast state.wake_scheduler

let start_log_drain sw batch =
  Fiber.fork_daemon ~sw (fun () ->
      let path = Filename.concat batch.out_dir "log" in
      let ch = open_out_gen [ Open_wronly; Open_creat; Open_excl ] 0o640 path in
      let rec loop () =
        let s = Stream.take batch.log in
        output_string ch s;
        flush ch;
        if !(batch.log_done) && Stream.is_empty batch.log then (
          close_out ch;
          batch.log_closed <- true;
          Eio.Condition.broadcast batch.log_drained)
        else loop ()
      in
      loop ();
      `Stop_daemon)

let handle_submit state sw request =
  match validate_request state request with
  | Error message -> Error message
  | Ok () ->
      let batch, total_jobs = create_batch state request in
      let events = if request.detach then None else Some (add_watcher batch) in
      start_log_drain sw batch;
      enqueue_jobs state batch;
      state.log_server
        (Format.sprintf "accepted %s with %i jobs into %s" batch.id total_jobs batch.out_dir);
      Ok (batch, events)

let rec drain_events writer events =
  let event = Stream.take events in
  send_event writer event;
  match event with
  | Batch_finished _ | Batch_failed _ -> ()
  | _ -> drain_events writer events

let watch_stream writer batch events =
  Fun.protect
    ~finally:(fun () -> remove_watcher batch events)
    (fun () ->
      send_event writer (accepted_event batch);
      match batch.status with
      | Running -> drain_events writer events
      | Finished ->
          send_output_files writer batch;
          send_event writer
            (Batch_finished { batch_id = batch.id; output_dir = batch.out_dir })
      | Failed message -> send_event writer (Batch_failed { batch_id = batch.id; message }))

let watch_batch writer batch =
  let events = add_watcher batch in
  watch_stream writer batch events

let handle_client state sw flow _addr =
  let reader = Buf_read.of_flow flow ~max_size:16_000_000 in
  let request =
    try Buf_read.line reader |> Protocol.decode_request
    with exn ->
      invalid_arg ("invalid request: " ^ Printexc.to_string exn)
  in
  Buf_write.with_flow flow @@ fun writer ->
  match request with
  | Submit submit_request -> (
      match handle_submit state sw submit_request with
      | Error message -> send_event writer (Batch_failed { batch_id = ""; message })
      | Ok (batch, events) ->
          if submit_request.detach then send_event writer (accepted_event batch)
          else
            match events with
            | None -> assert false
            | Some events -> watch_stream writer batch events)
  | Reconnect { batch_id } -> (
      match Hashtbl.find_opt state.batches batch_id with
      | None ->
          send_event writer
            (Batch_failed
               { batch_id; message = "unknown job id; queued jobs are only kept in server memory" })
      | Some batch -> watch_batch writer batch)

let parse_positive name value =
  if value < 1 then (
    prerr_endline (Format.sprintf "benchmark-server: %s must be at least 1" name);
    exit 2);
  value

let ends_with ~suffix s =
  let ls = String.length s in
  let lf = String.length suffix in
  ls >= lf && String.sub s (ls - lf) lf = suffix

let parse_memory_gb name value =
  let value = String.trim value in
  let fail () =
    prerr_endline
      (Format.sprintf
         "benchmark-server: %s expects a positive integer GB value, optionally suffixed with G, GB, T, or TB"
         name);
    exit 2
  in
  let number, multiplier =
    let upper = String.uppercase_ascii value in
    if ends_with ~suffix:"TB" upper then
      (String.sub value 0 (String.length value - 2), 1024)
    else if ends_with ~suffix:"T" upper then
      (String.sub value 0 (String.length value - 1), 1024)
    else if ends_with ~suffix:"GB" upper then
      (String.sub value 0 (String.length value - 2), 1)
    else if ends_with ~suffix:"G" upper then
      (String.sub value 0 (String.length value - 1), 1)
    else (value, 1)
  in
  try parse_positive name (int_of_string (String.trim number) * multiplier)
  with Failure _ -> fail ()

let () =
  let host = ref "127.0.0.1" in
  let port = ref 8765 in
  let cores = ref 1 in
  let max_memory = ref None in
  let output_root = ref "." in
  let server_log = ref None in
  let options =
    [
      ("-host", Arg.Set_string host, "Host/interface to bind (default 127.0.0.1)");
      ("-port", Arg.Set_int port, "TCP port to bind (default 8765)");
      ("-cores", Arg.Set_int cores, "Maximum concurrent jobs");
      ( "-max-memory",
        Arg.String (fun m -> max_memory := Some (parse_memory_gb "-max-memory" m)),
        "Total memory budget in GB; suffixes G, GB, T, and TB are accepted" );
      ("-output-root", Arg.Set_string output_root, "Directory where batch output directories are created");
      ("-server-log", Arg.String (fun path -> server_log := Some path), "Append server activity to this file");
    ]
  in
  Arg.parse options
    (fun arg ->
      prerr_endline ("benchmark-server: unexpected argument " ^ arg);
      exit 2)
    "Usage: benchmark-server [options]";
  let cores = parse_positive "-cores" !cores in
  let port = parse_positive "-port" !port in
  Option.iter (fun m -> ignore (parse_positive "-max-memory" m)) !max_memory;
  let log_server =
    match !server_log with
    | None -> fun msg -> traceln "%s" msg
    | Some path ->
        fun msg ->
          let ch = open_out_gen [ Open_creat; Open_wronly; Open_append ] 0o640 path in
          Fun.protect
            ~finally:(fun () -> close_out ch)
            (fun () ->
              output_string ch (msg ^ "\n");
              flush ch)
  in
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let state =
    {
      cores;
      max_memory = !max_memory;
      output_root = !output_root;
      next_batch = 0;
      pending = [];
      running = 0;
      reserved_memory = 0;
      batches = Hashtbl.create 64;
      wake_scheduler = Eio.Condition.create ();
      log_server;
    }
  in
  let proc_mgr = Stdenv.process_mgr env in
  Fiber.fork ~sw (fun () -> scheduler proc_mgr state sw);
  let addr = server_addr !host port in
  let socket =
    Eio.Net.listen ~reuse_addr:true ~backlog:128 ~sw (Stdenv.net env) addr
  in
  log_server
    (Format.sprintf "benchmark-server listening on %s:%i with %i cores" !host port cores);
  Eio.Net.run_server socket ~on_error:(fun exn ->
      log_server ("connection error: " ^ Printexc.to_string exn))
    (handle_client state sw)
