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
  id : string;
  request : Protocol.submit_request;
  out_dir : string;
  htbl : (int * Common.aggregate) Common.HStrings.t Common.HStrings.t;
  log : string Stream.t;
  log_done : bool ref;
  events : Protocol.event Stream.t option;
  total_jobs : int;
  mutable completed : int;
  mutable failed : bool;
}

type state = {
  cores : int;
  max_memory : int option;
  output_root : string;
  mutable next_batch : int;
  mutable pending : job list;
  mutable running : int;
  mutable reserved_memory : int;
  wake_scheduler : Eio.Condition.t;
  log_server : string -> unit;
}

let send_line writer line =
  Buf_write.string writer line;
  Buf_write.char writer '\n';
  Buf_write.flush writer

let send_event writer event = send_line writer (Protocol.encode_event event)

let publish batch event =
  match batch.events with
  | None -> ()
  | Some events -> Stream.add events event

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
  if not batch.failed then (
    try
      Common.hashtables_to_files_native batch.out_dir batch.htbl (result_sort batch);
      if batch.request.excel then
        Common.hashtables_to_excel_native batch.out_dir batch.htbl Common.cmp_user;
      batch.log_done := true;
      Stream.add batch.log "";
      publish batch (Batch_finished { batch_id = batch.id; output_dir = batch.out_dir });
      state.log_server
        (Format.sprintf "batch %s finished in %s" batch.id batch.out_dir)
    with exn ->
      batch.failed <- true;
      let message = Printexc.to_string exn in
      publish batch (Batch_failed { batch_id = batch.id; message });
      state.log_server (Format.sprintf "batch %s failed: %s" batch.id message))

let run_job proc_mgr state job =
  let batch = job.batch in
  let request = batch.request in
  let solver = Common.solver_path ~cwd:request.cwd job.solver in
  let instance = Common.benchmark_path ~cwd:request.cwd ~prefix:request.benchmark_prefix job.benchmark in
  let result =
    Common.with_timing proc_mgr ~timeout:request.timeout ?memory:request.memory
      ~log:batch.log [ solver; instance ]
  in
  let solver_name = Common.prune job.solver in
  Common.add_result ~htbl:batch.htbl ~nb_benchmarks:(List.length request.lines) solver_name
    job.benchmark result;
  batch.completed <- batch.completed + 1;
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

let create_batch state (request : Protocol.submit_request) events =
  let id =
    state.next_batch <- state.next_batch + 1;
    Format.sprintf "batch-%06d" state.next_batch
  in
  let digest =
    Common.digest ~benchmark_file:request.benchmark_name ~lines:request.lines
      ~timeout:request.timeout ?memory:request.memory ()
  in
  let out_dir = Common.fresh_dir_native ~root:state.output_root ~dir:digest in
  let total_jobs = List.length request.lines * List.length request.commands * request.generations in
  let batch =
    {
      id;
      request;
      out_dir;
      htbl = Common.HStrings.create (List.length request.commands);
      log = Stream.create max_int;
      log_done = ref false;
      events;
      total_jobs;
      completed = 0;
      failed = false;
    }
  in
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
        if !(batch.log_done) && Stream.is_empty batch.log then close_out ch else loop ()
      in
      loop ();
      `Stop_daemon)

let handle_submit state sw request =
  match validate_request state request with
  | Error message -> Error message
  | Ok () ->
      let events = if request.detach then None else Some (Stream.create max_int) in
      let batch, total_jobs = create_batch state request events in
      start_log_drain sw batch;
      enqueue_jobs state batch;
      state.log_server
        (Format.sprintf "accepted %s with %i jobs into %s" batch.id total_jobs batch.out_dir);
      Ok (batch, Accepted { batch_id = batch.id; output_dir = batch.out_dir; total_jobs })

let handle_client state sw flow _addr =
  let reader = Buf_read.of_flow flow ~max_size:16_000_000 in
  let request =
    try Buf_read.line reader |> Protocol.decode_submit
    with exn ->
      invalid_arg ("invalid submit request: " ^ Printexc.to_string exn)
  in
  Buf_write.with_flow flow @@ fun writer ->
  match handle_submit state sw request with
  | Error message ->
      send_event writer (Batch_failed { batch_id = ""; message })
  | Ok (batch, accepted) ->
      send_event writer accepted;
      if not request.detach then (
        let rec loop () =
          match batch.events with
          | None -> ()
          | Some events -> (
              let event = Stream.take events in
              send_event writer event;
              match event with
              | Batch_finished _ | Batch_failed _ -> ()
              | _ -> loop ())
        in
        loop ())

let parse_positive name value =
  if value < 1 then (
    prerr_endline (Format.sprintf "benchmark-server: %s must be at least 1" name);
    exit 2);
  value

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
      ("-max-memory", Arg.Int (fun m -> max_memory := Some m), "Total memory budget in Gb");
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
