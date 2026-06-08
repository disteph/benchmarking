open Eio
open Runner_lib
open Protocol

type job = {
  batch : batch;
  solver : string;
  benchmark : string;
  reserve_memory : int;
}

and watcher = {
  events : Protocol.event Stream.t;
  download : bool;
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
  watchers : watcher list ref;
  running_switches : Switch.t list ref;
  total_jobs : int;
  prior_jobs : int;
  mutable prior_completed : int;
  mutable completed : int;
  mutable status : batch_status;
}

and batch_status =
  | Running
  | Paused
  | Finished
  | Failed of string
  | Killed of string

type state = {
  cores : int;
  max_memory : int option;
  output_root : string;
  state_file : string option;
  mutable next_batch : int;
  mutable pending : job list;
  mutable running : int;
  mutable reserved_memory : int;
  batches : (string, batch) Hashtbl.t;
  folder_batches : (string, batch) Hashtbl.t;
  state_watchers : Protocol.event Stream.t list ref;
  wake_scheduler : Eio.Condition.t;
  log_server : string -> unit;
}

let json_member name fields =
  match List.assoc_opt name fields with
  | Some value -> value
  | None -> invalid_arg ("missing JSON field: " ^ name)

let json_string = function
  | `String s -> s
  | _ -> invalid_arg "expected JSON string"

let json_int = function
  | `Int i -> i
  | _ -> invalid_arg "expected JSON integer"

let json_float = function
  | `Float f -> f
  | `Int i -> float_of_int i
  | _ -> invalid_arg "expected JSON float"

let json_list = function
  | `List xs -> xs
  | _ -> invalid_arg "expected JSON list"

let times_to_yojson { Common.wall; user; system } =
  `Assoc [ ("wall", `Float wall); ("user", `Float user); ("system", `Float system) ]

let times_of_yojson = function
  | `Assoc fields ->
      {
        Common.wall = json_float (json_member "wall" fields);
        user = json_float (json_member "user" fields);
        system = json_float (json_member "system" fields);
      }
  | _ -> invalid_arg "expected JSON object"

let answer_to_string = function
  | Common.Sat -> "sat"
  | Common.Unsat -> "unsat"

let answer_of_string = function
  | "sat" -> Common.Sat
  | "unsat" -> Common.Unsat
  | answer -> invalid_arg ("unknown answer: " ^ answer)

let crash_to_yojson = function
  | `UnreadableOut -> `String "unreadable_out"
  | `UnreadableTimes -> `String "unreadable_times"
  | `Exception -> `String "exception"

let crash_of_yojson json =
  match json_string json with
  | "unreadable_out" -> `UnreadableOut
  | "unreadable_times" -> `UnreadableTimes
  | "exception" -> `Exception
  | crash -> invalid_arg ("unknown crash kind: " ^ crash)

let aggregate_to_yojson : Common.aggregate -> Yojson.Safe.t = function
  | `Answer (answer, times) ->
      `Assoc
        [
          ("result", `String "answer");
          ("answer", `String (answer_to_string answer));
          ("times", times_to_yojson times);
        ]
  | `Crash (crash, message) ->
      `Assoc
        [
          ("result", `String "crash");
          ("crash", crash_to_yojson crash);
          ("message", `String message);
        ]
  | `Timeout -> `Assoc [ ("result", `String "timeout") ]
  | `Memout -> `Assoc [ ("result", `String "memout") ]
  | `Inconsistent -> `Assoc [ ("result", `String "inconsistent") ]

let aggregate_of_yojson = function
  | `Assoc fields -> (
      match json_string (json_member "result" fields) with
      | "answer" ->
          `Answer
            ( answer_of_string (json_string (json_member "answer" fields)),
              times_of_yojson (json_member "times" fields) )
      | "crash" ->
          `Crash
            ( crash_of_yojson (json_member "crash" fields),
              json_string (json_member "message" fields) )
      | "timeout" -> `Timeout
      | "memout" -> `Memout
      | "inconsistent" -> `Inconsistent
      | result -> invalid_arg ("unknown result: " ^ result))
  | _ -> invalid_arg "expected JSON object"

let results_to_yojson htbl =
  Common.HStrings.to_list htbl
  |> List.sort (fun (a, _) (b, _) -> String.compare a b)
  |> List.map (fun (command, command_tbl) ->
         let rows =
           Common.HStrings.to_list command_tbl
           |> List.sort (fun (a, _) (b, _) -> String.compare a b)
           |> List.map (fun (benchmark, (count, aggregate)) ->
                  `Assoc
                    [
                      ("benchmark", `String benchmark);
                      ("count", `Int count);
                      ("result", aggregate_to_yojson aggregate);
                    ])
         in
         `Assoc [ ("command", `String command); ("benchmarks", `List rows) ])
  |> fun commands -> `List commands

let results_of_yojson json =
  let htbl = Common.HStrings.create 16 in
  json_list json
  |> List.iter (function
       | `Assoc fields ->
           let command = json_string (json_member "command" fields) in
           let command_tbl = Common.HStrings.create 16 in
           json_list (json_member "benchmarks" fields)
           |> List.iter (function
                | `Assoc fields ->
                    let benchmark = json_string (json_member "benchmark" fields) in
                    let count = json_int (json_member "count" fields) in
                    let aggregate = aggregate_of_yojson (json_member "result" fields) in
                    Common.HStrings.replace command_tbl benchmark (count, aggregate)
                | _ -> invalid_arg "expected JSON object");
           Common.HStrings.replace htbl command command_tbl
       | _ -> invalid_arg "expected JSON object");
  htbl

let status_to_yojson = function
  | Running -> `Assoc [ ("status", `String "running") ]
  | Paused -> `Assoc [ ("status", `String "paused") ]
  | Finished -> `Assoc [ ("status", `String "finished") ]
  | Failed message -> `Assoc [ ("status", `String "failed"); ("message", `String message) ]
  | Killed message -> `Assoc [ ("status", `String "killed"); ("message", `String message) ]

let status_of_yojson = function
  | `Assoc fields -> (
      match json_string (json_member "status" fields) with
      | "running" -> Running
      | "paused" -> Paused
      | "finished" -> Finished
      | "failed" -> Failed (json_string (json_member "message" fields))
      | "killed" -> Killed (json_string (json_member "message" fields))
      | status -> invalid_arg ("unknown batch status: " ^ status))
  | _ -> invalid_arg "expected JSON object"

let batch_to_yojson batch =
  `Assoc
    [
      ("sequence", `Int batch.sequence);
      ("id", `String batch.id);
      ("request", Protocol.submit_to_yojson batch.request);
      ("out_dir", `String batch.out_dir);
      ("total_jobs", `Int batch.total_jobs);
      ("prior_jobs", `Int batch.prior_jobs);
      ("prior_completed", `Int batch.prior_completed);
      ("completed", `Int batch.completed);
      ("status", status_to_yojson batch.status);
      ("results", results_to_yojson batch.htbl);
    ]

let batch_of_yojson = function
  | `Assoc fields ->
      let status = status_of_yojson (json_member "status" fields) in
      let running =
        match status with
        | Running | Paused -> true
        | Finished | Failed _ | Killed _ -> false
      in
      {
        sequence = json_int (json_member "sequence" fields);
        id = json_string (json_member "id" fields);
        request = Protocol.submit_of_yojson (json_member "request" fields);
        out_dir = json_string (json_member "out_dir" fields);
        htbl = results_of_yojson (json_member "results" fields);
        log = Stream.create max_int;
        log_done = ref (not running);
        log_drained = Eio.Condition.create ();
        log_closed = not running;
        watchers = ref [];
        running_switches = ref [];
        total_jobs = json_int (json_member "total_jobs" fields);
        prior_jobs = json_int (json_member "prior_jobs" fields);
        prior_completed = json_int (json_member "prior_completed" fields);
        completed = json_int (json_member "completed" fields);
        status;
      }
  | _ -> invalid_arg "expected JSON object"

let folder_batches_to_yojson state =
  Hashtbl.fold
    (fun folder batch rows ->
      `Assoc [ ("folder", `String folder); ("batch_id", `String batch.id) ] :: rows)
    state.folder_batches []
  |> List.sort (fun a b -> compare a b)
  |> fun rows -> `List rows

let server_state_to_yojson state =
  let batches =
    Hashtbl.to_seq_values state.batches |> List.of_seq
    |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
    |> List.map batch_to_yojson
  in
  `Assoc
    [
      ("version", `Int 1);
      ("next_batch", `Int state.next_batch);
      ("batches", `List batches);
      ("folder_batches", folder_batches_to_yojson state);
    ]

let write_json_file_atomic path json =
  Common.mkdir_p (Filename.dirname path);
  let tmp = path ^ ".tmp" in
  let ch = open_out_gen [ Open_creat; Open_wronly; Open_trunc ] 0o640 tmp in
  Fun.protect
    ~finally:(fun () -> close_out_noerr ch)
    (fun () ->
      output_string ch (Yojson.Safe.pretty_to_string json);
      output_char ch '\n';
      flush ch);
  Unix.rename tmp path

let save_state state =
  match state.state_file with
  | None -> ()
  | Some path -> (
      try write_json_file_atomic path (server_state_to_yojson state)
      with exn ->
        state.log_server
          (Format.sprintf "could not save server state to %s: %s" path (Printexc.to_string exn)))

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

let add_watcher batch ~download =
  let events = Stream.create max_int in
  let watcher = { events; download } in
  batch.watchers := watcher :: !(batch.watchers);
  watcher

let remove_watcher batch watcher =
  batch.watchers := List.filter (fun existing -> existing != watcher) !(batch.watchers)

let publish batch event =
  List.iter
    (fun watcher ->
      match event with
      | Output_file _ when not watcher.download -> ()
      | _ -> Stream.add watcher.events event)
    !(batch.watchers)

let unfinished_batch_summaries state =
  let queued_jobs batch =
    List.fold_left
      (fun count job -> if job.batch == batch then count + 1 else count)
      0 state.pending
  in
  Hashtbl.to_seq_values state.batches |> List.of_seq
  |> List.filter (fun batch ->
         match batch.status with
         | Running | Paused -> true
         | Finished | Failed _ | Killed _ -> false)
  |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
  |> List.map (fun batch : Protocol.batch_summary ->
         {
           batch_id = batch.id;
           benchmark_name = batch.request.benchmark_name;
           output_dir = batch.out_dir;
           total_benchmarks = List.length batch.request.lines;
           total_solvers = List.length batch.request.commands;
           generations = batch.request.generations;
           total_jobs = batch.total_jobs;
           completed = batch.completed;
           queued_jobs = queued_jobs batch;
           running_jobs = List.length !(batch.running_switches);
           paused = (match batch.status with Paused -> true | _ -> false);
         })

let state_snapshot_event state =
  State_snapshot { batches = unfinished_batch_summaries state }

let publish_state_snapshot state =
  let event = state_snapshot_event state in
  List.iter (fun watcher -> Stream.add watcher event) !(state.state_watchers)

let add_state_watcher state =
  let events = Stream.create max_int in
  state.state_watchers := events :: !(state.state_watchers);
  Stream.add events (state_snapshot_event state);
  events

let remove_state_watcher state events =
  state.state_watchers := List.filter (fun existing -> existing != events) !(state.state_watchers)

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
  | Paused -> List.length !(batch.running_switches)
  | Finished | Failed _ | Killed _ -> 0

let prior_jobs state =
  Hashtbl.fold (fun _ batch total -> total + unfinished_jobs batch) state.batches 0

let next_batch_identity state =
  let sequence =
    state.next_batch <- state.next_batch + 1;
    state.next_batch
  in
  (sequence, Format.sprintf "batch-%06d" sequence)

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
    | job :: rest when job.batch.status = Paused -> aux (job :: prefix) rest
    | job :: rest when resources_fit state job ->
        state.pending <- List.rev_append prefix rest;
        Some job
    | job :: rest -> aux (job :: prefix) rest
  in
  aux [] state.pending

let result_sort batch = Common.sort_of_name batch.request.sort

let finish_batch state batch =
  match batch.status with
  | Failed _ | Finished | Killed _ -> ()
  | Running | Paused -> (
    try
      Common.hashtables_to_files_native ~overwrite:true batch.out_dir batch.htbl
        (result_sort batch);
      if batch.request.excel then
        Common.hashtables_to_excel_native ~timeout:batch.request.timeout ~overwrite:true
          batch.out_dir batch.htbl Common.cmp_user;
      batch.log_done := true;
      Stream.add batch.log "";
      while not batch.log_closed do
        Eio.Condition.await_no_mutex batch.log_drained
      done;
      batch.status <- Finished;
      save_state state;
      publish_state_snapshot state;
      publish_output_files batch;
      publish batch (Batch_finished { batch_id = batch.id; output_dir = batch.out_dir });
      state.log_server
        (Format.sprintf "batch %s finished in %s" batch.id batch.out_dir)
    with exn ->
      let message = Printexc.to_string exn in
      batch.status <- Failed message;
      save_state state;
      publish_state_snapshot state;
      publish batch (Batch_failed { batch_id = batch.id; message });
      state.log_server (Format.sprintf "batch %s failed: %s" batch.id message))

let run_job proc_mgr state job =
  let batch = job.batch in
  let request = batch.request in
  let solver = Common.solver_path ~root:request.server_exe_root job.solver in
  let instance = Common.benchmark_path ~root:request.server_benchmark_root job.benchmark in
  Fun.protect
    ~finally:(fun () ->
      state.running <- state.running - 1;
      state.reserved_memory <- state.reserved_memory - job.reserve_memory;
      Eio.Condition.broadcast state.wake_scheduler)
    (fun () ->
      let result =
        Common.with_timing proc_mgr ~timeout:request.timeout ?memory:request.memory
          ~log:batch.log [ solver; instance ]
      in
      match batch.status with
      | Running | Paused ->
          let solver_name = Common.solver_output_name job.solver in
          Common.add_result ~htbl:batch.htbl ~nb_benchmarks:(List.length request.lines)
            solver_name job.benchmark result;
          batch.completed <- batch.completed + 1;
          save_state state;
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
          if batch.completed = batch.total_jobs then finish_batch state batch
          else publish_state_snapshot state
      | Finished | Failed _ | Killed _ -> ())

let rec scheduler proc_mgr state sw =
  match take_runnable state with
  | Some job ->
      state.running <- state.running + 1;
      state.reserved_memory <- state.reserved_memory + job.reserve_memory;
      Fiber.fork ~sw (fun () ->
          try
            Switch.run ~name:("job " ^ job.batch.id) @@ fun job_sw ->
            job.batch.running_switches := job_sw :: !(job.batch.running_switches);
            Fun.protect
              ~finally:(fun () ->
                job.batch.running_switches :=
                  List.filter (fun existing -> existing != job_sw) !(job.batch.running_switches))
              (fun () -> run_job proc_mgr state job)
          with _ -> ());
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
  let sequence, id = next_batch_identity state in
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
      running_switches = ref [];
      total_jobs;
      prior_jobs;
      prior_completed = 0;
      completed = 0;
      status = Running;
    }
  in
  Hashtbl.add state.batches id batch;
  (batch, total_jobs)

let canonical_directory path =
  if not (Sys.file_exists path && Sys.is_directory path) then
    Error (Format.sprintf "not a directory: %s" path)
  else
    try Ok (Unix.realpath path)
    with Unix.Unix_error (error, _, _) ->
      Error (Format.sprintf "cannot resolve %s: %s" path (Unix.error_message error))

let rec path_is_under ~root path =
  String.equal root path
  ||
  let parent = Filename.dirname path in
  not (String.equal parent path) && path_is_under ~root parent

let resolve_reconnect_folder state folder =
  if Filename.is_relative folder then
    match canonical_directory state.output_root with
    | Error message -> Error message
    | Ok root -> (
        let path = Filename.concat root folder in
        match canonical_directory path with
        | Error message -> Error message
        | Ok path ->
            if path_is_under ~root path then Ok path
            else Error (Format.sprintf "relative folder escapes output root: %s" folder))
  else canonical_directory folder

let htbl_commands htbl =
  Common.HStrings.to_list htbl |> List.map fst |> List.sort String.compare

let htbl_benchmarks htbl =
  Common.HStrings.to_list htbl
  |> List.concat_map (fun (_, command_tbl) ->
         Common.HStrings.to_list command_tbl |> List.map fst)
  |> List.sort_uniq String.compare

let htbl_completed_runs htbl command benchmark =
  match Common.HStrings.get htbl command with
  | None -> 0
  | Some command_tbl -> (
      match Common.HStrings.get command_tbl benchmark with
      | None -> 0
      | Some (n, _) -> n)

let htbl_completed_count htbl =
  Common.HStrings.to_list htbl
  |> List.fold_left
       (fun total (_, command_tbl) ->
         total
         + (Common.HStrings.to_list command_tbl
           |> List.fold_left (fun subtotal (_, (n, _)) -> subtotal + n) 0))
       0

let create_imported_batch state ~folder ~htbl ~total_jobs =
  let sequence, id = next_batch_identity state in
  let request : Protocol.submit_request =
    {
      benchmark_file = folder;
      benchmark_name = Filename.basename folder;
      server_benchmark_root = "";
      server_exe_root = "";
      lines = htbl_benchmarks htbl;
      commands = htbl_commands htbl;
      timeout = 1;
      memory = None;
      generations = 1;
      sort = "alpha";
      excel = true;
      detach = false;
    }
  in
  let batch =
    {
      sequence;
      id;
      request;
      out_dir = folder;
      htbl;
      log = Stream.create max_int;
      log_done = ref true;
      log_drained = Eio.Condition.create ();
      log_closed = true;
      watchers = ref [];
      running_switches = ref [];
      total_jobs;
      prior_jobs = 0;
      prior_completed = 0;
      completed = total_jobs;
      status = Finished;
    }
  in
  Hashtbl.add state.batches id batch;
  Hashtbl.add state.folder_batches folder batch;
  batch

let batch_of_reconnect_folder state folder =
  match resolve_reconnect_folder state folder with
  | Error message -> Error message
  | Ok folder -> (
      match Hashtbl.find_opt state.folder_batches folder with
      | Some batch -> Ok batch
      | None -> (
          try
            let htbl, total_jobs = Common.load_csv_dir_native folder in
            Common.hashtables_to_excel_native ~overwrite:true folder htbl Common.cmp_user;
            let batch = create_imported_batch state ~folder ~htbl ~total_jobs in
            save_state state;
            publish_state_snapshot state;
            state.log_server
              (Format.sprintf "imported %s as %s with %i CSV rows" folder batch.id total_jobs);
            Ok batch
          with exn -> Error (Printexc.to_string exn)))

let enqueue_jobs state batch =
  let request = batch.request in
  let reserve_memory = reserved_memory state.max_memory request.memory in
  let new_jobs =
    List.concat_map
      (fun benchmark ->
        List.concat_map
          (fun solver ->
            let solver_name = Common.solver_output_name solver in
            let completed = htbl_completed_runs batch.htbl solver_name benchmark in
            let remaining = max 0 (request.generations - completed) in
            List.init remaining (fun _ -> { batch; solver; benchmark; reserve_memory }))
          request.commands)
      request.lines
  in
  state.pending <- state.pending @ new_jobs;
  Eio.Condition.broadcast state.wake_scheduler

let kill_batch state batch_id =
  match Hashtbl.find_opt state.batches batch_id with
  | None ->
      Batch_failed { batch_id; message = "unknown job id; queued jobs are only kept in server memory" }
  | Some batch -> (
      match batch.status with
      | Finished ->
          Batch_failed { batch_id; message = "batch already finished" }
      | Failed message ->
          Batch_failed { batch_id; message = "batch already failed: " ^ message }
      | Killed message ->
          Batch_killed { batch_id; message }
      | Running | Paused ->
          let removed = ref 0 in
          state.pending <-
            List.filter
              (fun job ->
                if job.batch == batch then (
                  incr removed;
                  false)
                else true)
              state.pending;
          let running = List.length !(batch.running_switches) in
          let message =
            Format.sprintf "removed %d pending jobs and cancelled %d running jobs" !removed running
          in
          batch.status <- Killed message;
          save_state state;
          publish_state_snapshot state;
          List.iter
            (fun sw -> Switch.fail sw (Failure ("killed " ^ batch_id)))
            !(batch.running_switches);
          publish batch (Batch_killed { batch_id; message });
          Eio.Condition.broadcast state.wake_scheduler;
          state.log_server (Format.sprintf "killed %s: %s" batch_id message);
          Batch_killed { batch_id; message })

let pending_jobs_for_batch state batch =
  List.fold_left
    (fun count job -> if job.batch == batch then count + 1 else count)
    0 state.pending

let pause_batch state batch_id =
  match Hashtbl.find_opt state.batches batch_id with
  | None ->
      Batch_failed { batch_id; message = "unknown job id; queued jobs are only kept in server memory" }
  | Some batch -> (
      match batch.status with
      | Finished ->
          Batch_failed { batch_id; message = "batch already finished" }
      | Failed message ->
          Batch_failed { batch_id; message = "batch already failed: " ^ message }
      | Killed message ->
          Batch_failed { batch_id; message = "batch already killed: " ^ message }
      | Paused ->
          let message = "batch already paused" in
          Batch_paused { batch_id; message }
      | Running ->
          let pending = pending_jobs_for_batch state batch in
          let running = List.length !(batch.running_switches) in
          let message =
            Format.sprintf "paused with %d pending jobs and %d running jobs" pending running
          in
          batch.status <- Paused;
          save_state state;
          publish_state_snapshot state;
          publish batch (Batch_paused { batch_id; message });
          Eio.Condition.broadcast state.wake_scheduler;
          state.log_server (Format.sprintf "paused %s: %s" batch_id message);
          Batch_paused { batch_id; message })

let unpause_batch state batch_id =
  match Hashtbl.find_opt state.batches batch_id with
  | None ->
      Batch_failed { batch_id; message = "unknown job id; queued jobs are only kept in server memory" }
  | Some batch -> (
      match batch.status with
      | Finished ->
          Batch_failed { batch_id; message = "batch already finished" }
      | Failed message ->
          Batch_failed { batch_id; message = "batch already failed: " ^ message }
      | Killed message ->
          Batch_failed { batch_id; message = "batch already killed: " ^ message }
      | Running ->
          let message = "batch already running" in
          Batch_unpaused { batch_id; message }
      | Paused ->
          let pending = pending_jobs_for_batch state batch in
          let message = Format.sprintf "unpaused with %d pending jobs" pending in
          batch.status <- Running;
          save_state state;
          publish_state_snapshot state;
          publish batch (Batch_unpaused { batch_id; message });
          Eio.Condition.broadcast state.wake_scheduler;
          state.log_server (Format.sprintf "unpaused %s: %s" batch_id message);
          Batch_unpaused { batch_id; message })

let start_log_drain ?(append = false) sw batch =
  Fiber.fork_daemon ~sw (fun () ->
      let path = Filename.concat batch.out_dir "log" in
      let create_mode = if append then Open_append else Open_excl in
      let ch = open_out_gen [ Open_wronly; Open_creat; create_mode ] 0o640 path in
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

let load_state_file path =
  match Yojson.Safe.from_file path with
  | `Assoc fields ->
      let version = json_int (json_member "version" fields) in
      if version <> 1 then invalid_arg (Format.sprintf "unsupported state version: %d" version);
      let next_batch = json_int (json_member "next_batch" fields) in
      let batches = json_list (json_member "batches" fields) |> List.map batch_of_yojson in
      let folder_batches =
        match List.assoc_opt "folder_batches" fields with
        | None -> []
        | Some json ->
            json_list json
            |> List.map (function
                 | `Assoc fields ->
                     ( json_string (json_member "folder" fields),
                       json_string (json_member "batch_id" fields) )
                 | _ -> invalid_arg "expected JSON object")
      in
      (next_batch, batches, folder_batches)
  | _ -> invalid_arg "expected JSON object"

let restore_state state sw =
  match state.state_file with
  | None -> ()
  | Some path ->
      if Sys.file_exists path then (
        let next_batch, batches, folder_batches =
          try load_state_file path
          with exn ->
            prerr_endline
              (Format.sprintf "benchmark-server: could not load state file %s: %s" path
                 (Printexc.to_string exn));
            exit 2
        in
        let max_sequence = ref 0 in
        List.iter
          (fun batch ->
            batch.completed <- min batch.total_jobs (htbl_completed_count batch.htbl);
            max_sequence := max !max_sequence batch.sequence;
            Hashtbl.replace state.batches batch.id batch)
          batches;
        List.iter
          (fun (folder, batch_id) ->
            match Hashtbl.find_opt state.batches batch_id with
            | Some batch -> Hashtbl.replace state.folder_batches folder batch
            | None ->
                state.log_server
                  (Format.sprintf "ignoring persisted folder mapping %s -> %s" folder batch_id))
          folder_batches;
        state.next_batch <- max next_batch !max_sequence;
        let running_batches =
          batches
          |> List.filter (fun batch ->
                 match batch.status with
                 | Running | Paused -> true
                 | Finished | Failed _ | Killed _ -> false)
          |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
        in
        List.iter (fun batch -> start_log_drain ~append:true sw batch) running_batches;
        List.iter
          (fun batch ->
            if batch.completed >= batch.total_jobs then finish_batch state batch
            else enqueue_jobs state batch)
          running_batches;
        state.log_server
          (Format.sprintf "loaded %d batches from %s" (List.length batches) path))

let handle_submit state sw request =
  match validate_request state request with
  | Error message -> Error message
  | Ok () ->
      let batch, total_jobs = create_batch state request in
      let watcher = if request.detach then None else Some (add_watcher batch ~download:true) in
      start_log_drain sw batch;
      save_state state;
      enqueue_jobs state batch;
      publish_state_snapshot state;
      state.log_server
        (Format.sprintf "accepted %s with %i jobs into %s" batch.id total_jobs batch.out_dir);
      Ok (batch, watcher)

let rec drain_events writer events =
  let event = Stream.take events in
  send_event writer event;
  match event with
  | Batch_finished _ | Batch_failed _ | Batch_killed _ -> ()
  | _ -> drain_events writer events

let watch_stream writer batch watcher =
  Fun.protect
    ~finally:(fun () -> remove_watcher batch watcher)
    (fun () ->
      send_event writer (accepted_event batch);
      match batch.status with
      | Running | Paused -> drain_events writer watcher.events
      | Finished ->
          if watcher.download then send_output_files writer batch;
          send_event writer
            (Batch_finished { batch_id = batch.id; output_dir = batch.out_dir })
      | Failed message -> send_event writer (Batch_failed { batch_id = batch.id; message })
      | Killed message -> send_event writer (Batch_killed { batch_id = batch.id; message }))

let watch_batch writer batch ~download =
  let watcher = add_watcher batch ~download in
  watch_stream writer batch watcher

let rec drain_state_events writer events =
  Stream.take events |> send_event writer;
  drain_state_events writer events

let watch_state writer state =
  let events = add_state_watcher state in
  Fun.protect
    ~finally:(fun () -> remove_state_watcher state events)
    (fun () -> drain_state_events writer events)

let handle_client state sw flow _addr =
  let reader = Buf_read.of_flow flow ~max_size:Protocol_limits.max_json_line_size in
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
      | Ok (batch, watcher) ->
          if submit_request.detach then send_event writer (accepted_event batch)
          else
            match watcher with
            | None -> assert false
            | Some watcher -> watch_stream writer batch watcher)
  | Reconnect { batch_id; download } -> (
      match Hashtbl.find_opt state.batches batch_id with
      | None -> (
          match batch_of_reconnect_folder state batch_id with
          | Ok batch -> watch_batch writer batch ~download
          | Error message ->
              send_event writer
                (Batch_failed
                   {
                     batch_id;
                     message =
                       "unknown job id and could not import output folder: " ^ message;
                   }))
      | Some batch -> watch_batch writer batch ~download)
  | Kill { batch_id } -> send_event writer (kill_batch state batch_id)
  | Pause { batch_id } -> send_event writer (pause_batch state batch_id)
  | Unpause { batch_id } -> send_event writer (unpause_batch state batch_id)
  | State -> watch_state writer state

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
  let state_file = ref None in
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
      ( "-state-file",
        Arg.String (fun path -> state_file := Some path),
        "JSON file used to persist server batch state" );
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
  let state_file =
    Some (Option.value !state_file ~default:(Filename.concat !output_root "benchmark-server-state.json"))
  in
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
      state_file;
      next_batch = 0;
      pending = [];
      running = 0;
      reserved_memory = 0;
      batches = Hashtbl.create 64;
      folder_batches = Hashtbl.create 64;
      state_watchers = ref [];
      wake_scheduler = Eio.Condition.create ();
      log_server;
    }
  in
  let proc_mgr = Stdenv.process_mgr env in
  restore_state state sw;
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
