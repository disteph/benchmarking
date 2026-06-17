type submit_request = {
  benchmark_file : string;
  benchmark_name : string;
  server_benchmark_root : string;
  server_exe_root : string;
  lines : string list;
  commands : string list;
  timeout : int;
  memory : int option;
  generations : int;
  sort : string;
  excel : bool;
  detach : bool;
}

type request =
  | Submit of submit_request
  | Reconnect of { batch_id : string; download : bool }
  | Aggregate of { prefix : string; download : bool }
  | Kill of { batch_id : string }
  | Pause of { batch_id : string }
  | Unpause of { batch_id : string }
  | State

type batch_summary = {
  batch_id : string;
  benchmark_name : string;
  output_dir : string;
  total_benchmarks : int;
  total_solvers : int;
  generations : int;
  total_jobs : int;
  completed : int;
  queued_jobs : int;
  running_jobs : int;
  paused : bool;
}

type event =
  | Accepted of {
      batch_id : string;
      output_dir : string;
      total_jobs : int;
      completed : int;
      prior_jobs : int;
      prior_completed : int;
    }
  | Queue_progress of { batch_id : string; completed : int; total : int }
  | Job_finished of {
      batch_id : string;
      completed : int;
      total : int;
      solver : string;
      benchmark : string;
      result : string;
    }
  | Output_file of { batch_id : string; name : string; contents : string }
  | Batch_finished of { batch_id : string; output_dir : string }
  | Batch_failed of { batch_id : string; message : string }
  | Batch_killed of { batch_id : string; message : string }
  | Batch_paused of { batch_id : string; message : string }
  | Batch_unpaused of { batch_id : string; message : string }
  | State_snapshot of { batches : batch_summary list }

let base64_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

let base64_encode s =
  let len = String.length s in
  let out = Buffer.create (((len + 2) / 3) * 4) in
  let emit i = Buffer.add_char out base64_alphabet.[i land 0x3f] in
  let rec loop i =
    if i < len then (
      let b0 = Char.code s.[i] in
      let b1 = if i + 1 < len then Char.code s.[i + 1] else 0 in
      let b2 = if i + 2 < len then Char.code s.[i + 2] else 0 in
      emit (b0 lsr 2);
      emit (((b0 land 0x03) lsl 4) lor (b1 lsr 4));
      if i + 1 < len then emit (((b1 land 0x0f) lsl 2) lor (b2 lsr 6))
      else Buffer.add_char out '=';
      if i + 2 < len then emit b2 else Buffer.add_char out '=';
      loop (i + 3))
  in
  loop 0;
  Buffer.contents out

let base64_value = function
  | 'A' .. 'Z' as c -> Char.code c - Char.code 'A'
  | 'a' .. 'z' as c -> 26 + Char.code c - Char.code 'a'
  | '0' .. '9' as c -> 52 + Char.code c - Char.code '0'
  | '+' -> 62
  | '/' -> 63
  | '=' -> -1
  | c -> invalid_arg (Printf.sprintf "invalid base64 character: %C" c)

let base64_decode s =
  let len = String.length s in
  if len mod 4 <> 0 then invalid_arg "invalid base64 length";
  let out = Buffer.create ((len / 4) * 3) in
  let rec loop i =
    if i < len then (
      let a = base64_value s.[i] in
      let b = base64_value s.[i + 1] in
      let c = base64_value s.[i + 2] in
      let d = base64_value s.[i + 3] in
      if a < 0 || b < 0 then invalid_arg "invalid base64 padding";
      Buffer.add_char out (Char.chr ((a lsl 2) lor (b lsr 4)));
      if c >= 0 then (
        Buffer.add_char out (Char.chr (((b land 0x0f) lsl 4) lor (c lsr 2)));
        if d >= 0 then Buffer.add_char out (Char.chr (((c land 0x03) lsl 6) lor d)));
      loop (i + 4))
  in
  loop 0;
  Buffer.contents out

let member name fields =
  match List.assoc_opt name fields with
  | Some v -> v
  | None -> invalid_arg ("missing JSON field: " ^ name)

let as_string = function
  | `String s -> s
  | _ -> invalid_arg "expected JSON string"

let as_int = function
  | `Int i -> i
  | _ -> invalid_arg "expected JSON integer"

let as_bool = function
  | `Bool b -> b
  | _ -> invalid_arg "expected JSON boolean"

let as_string_list = function
  | `List xs -> List.map as_string xs
  | _ -> invalid_arg "expected JSON string list"

let opt_int = function
  | `Null -> None
  | `Int i -> Some i
  | _ -> invalid_arg "expected nullable JSON integer"

let submit_to_yojson r =
  `Assoc
    [
      ("type", `String "submit");
      ("benchmark_file", `String r.benchmark_file);
      ("benchmark_name", `String r.benchmark_name);
      ("server_benchmark_root", `String r.server_benchmark_root);
      ("server_exe_root", `String r.server_exe_root);
      ("lines", `List (List.map (fun s -> `String s) r.lines));
      ("commands", `List (List.map (fun s -> `String s) r.commands));
      ("timeout", `Int r.timeout);
      ("memory", (match r.memory with Some m -> `Int m | None -> `Null));
      ("generations", `Int r.generations);
      ("sort", `String r.sort);
      ("excel", `Bool r.excel);
      ("detach", `Bool r.detach);
    ]

let reconnect_to_yojson batch_id download =
  `Assoc
    [
      ("type", `String "reconnect");
      ("batch_id", `String batch_id);
      ("download", `Bool download);
    ]

let aggregate_to_yojson prefix download =
  `Assoc
    [
      ("type", `String "aggregate");
      ("prefix", `String prefix);
      ("download", `Bool download);
    ]

let kill_to_yojson batch_id =
  `Assoc [ ("type", `String "kill"); ("batch_id", `String batch_id) ]

let pause_to_yojson batch_id =
  `Assoc [ ("type", `String "pause"); ("batch_id", `String batch_id) ]

let unpause_to_yojson batch_id =
  `Assoc [ ("type", `String "unpause"); ("batch_id", `String batch_id) ]

let state_to_yojson = `Assoc [ ("type", `String "state") ]

let submit_of_yojson = function
  | `Assoc fields ->
      if as_string (member "type" fields) <> "submit" then invalid_arg "expected submit";
      {
        benchmark_file = as_string (member "benchmark_file" fields);
        benchmark_name = as_string (member "benchmark_name" fields);
        server_benchmark_root =
          (match List.assoc_opt "server_benchmark_root" fields with
          | Some value -> as_string value
          | None -> as_string (member "benchmark_prefix" fields));
        server_exe_root =
          (match List.assoc_opt "server_exe_root" fields with
          | Some value -> as_string value
          | None -> as_string (member "cwd" fields));
        lines = as_string_list (member "lines" fields);
        commands = as_string_list (member "commands" fields);
        timeout = as_int (member "timeout" fields);
        memory = opt_int (member "memory" fields);
        generations = as_int (member "generations" fields);
        sort = as_string (member "sort" fields);
        excel = as_bool (member "excel" fields);
        detach = as_bool (member "detach" fields);
      }
  | _ -> invalid_arg "expected JSON object"

let request_to_yojson = function
  | Submit request -> submit_to_yojson request
  | Reconnect { batch_id; download } -> reconnect_to_yojson batch_id download
  | Aggregate { prefix; download } -> aggregate_to_yojson prefix download
  | Kill { batch_id } -> kill_to_yojson batch_id
  | Pause { batch_id } -> pause_to_yojson batch_id
  | Unpause { batch_id } -> unpause_to_yojson batch_id
  | State -> state_to_yojson

let request_of_yojson = function
  | `Assoc fields as json -> (
      match as_string (member "type" fields) with
      | "submit" -> Submit (submit_of_yojson json)
      | "reconnect" ->
          Reconnect
            {
              batch_id = as_string (member "batch_id" fields);
              download =
                (match List.assoc_opt "download" fields with
                | Some value -> as_bool value
                | None -> false);
            }
      | "aggregate" ->
          Aggregate
            {
              prefix = as_string (member "prefix" fields);
              download =
                (match List.assoc_opt "download" fields with
                | Some value -> as_bool value
                | None -> false);
            }
      | "kill" -> Kill { batch_id = as_string (member "batch_id" fields) }
      | "pause" -> Pause { batch_id = as_string (member "batch_id" fields) }
      | "unpause" -> Unpause { batch_id = as_string (member "batch_id" fields) }
      | "state" -> State
      | request_type -> invalid_arg ("unknown request: " ^ request_type))
  | _ -> invalid_arg "expected JSON object"

let batch_summary_to_yojson
    {
      batch_id;
      benchmark_name;
      output_dir;
      total_benchmarks;
      total_solvers;
      generations;
      total_jobs;
      completed;
      queued_jobs;
      running_jobs;
      paused;
    } =
  `Assoc
    [
      ("batch_id", `String batch_id);
      ("benchmark_name", `String benchmark_name);
      ("output_dir", `String output_dir);
      ("total_benchmarks", `Int total_benchmarks);
      ("total_solvers", `Int total_solvers);
      ("generations", `Int generations);
      ("total_jobs", `Int total_jobs);
      ("completed", `Int completed);
      ("queued_jobs", `Int queued_jobs);
      ("running_jobs", `Int running_jobs);
      ("paused", `Bool paused);
    ]

let batch_summary_of_yojson = function
  | `Assoc fields ->
      let optional_int name default =
        match List.assoc_opt name fields with
        | Some value -> as_int value
        | None -> default
      in
      {
        batch_id = as_string (member "batch_id" fields);
        benchmark_name = as_string (member "benchmark_name" fields);
        output_dir = as_string (member "output_dir" fields);
        total_benchmarks = optional_int "total_benchmarks" 0;
        total_solvers = optional_int "total_solvers" 0;
        generations = optional_int "generations" 0;
        total_jobs = as_int (member "total_jobs" fields);
        completed = as_int (member "completed" fields);
        queued_jobs = optional_int "queued_jobs" 0;
        running_jobs = optional_int "running_jobs" 0;
        paused =
          (match List.assoc_opt "paused" fields with
          | Some value -> as_bool value
          | None -> false);
      }
  | _ -> invalid_arg "expected JSON object"

let event_to_yojson = function
  | Accepted { batch_id; output_dir; total_jobs; completed; prior_jobs; prior_completed } ->
      `Assoc
        [
          ("event", `String "accepted");
          ("batch_id", `String batch_id);
          ("output_dir", `String output_dir);
          ("total_jobs", `Int total_jobs);
          ("completed", `Int completed);
          ("prior_jobs", `Int prior_jobs);
          ("prior_completed", `Int prior_completed);
        ]
  | Queue_progress { batch_id; completed; total } ->
      `Assoc
        [
          ("event", `String "queue_progress");
          ("batch_id", `String batch_id);
          ("completed", `Int completed);
          ("total", `Int total);
        ]
  | Job_finished { batch_id; completed; total; solver; benchmark; result } ->
      `Assoc
        [
          ("event", `String "job_finished");
          ("batch_id", `String batch_id);
          ("completed", `Int completed);
          ("total", `Int total);
          ("solver", `String solver);
          ("benchmark", `String benchmark);
          ("result", `String result);
        ]
  | Output_file { batch_id; name; contents } ->
      `Assoc
        [
          ("event", `String "output_file");
          ("batch_id", `String batch_id);
          ("name", `String name);
          ("contents", `String contents);
        ]
  | Batch_finished { batch_id; output_dir } ->
      `Assoc
        [
          ("event", `String "batch_finished");
          ("batch_id", `String batch_id);
          ("output_dir", `String output_dir);
        ]
  | Batch_failed { batch_id; message } ->
      `Assoc
        [
          ("event", `String "batch_failed");
          ("batch_id", `String batch_id);
          ("message", `String message);
        ]
  | Batch_killed { batch_id; message } ->
      `Assoc
        [
          ("event", `String "batch_killed");
          ("batch_id", `String batch_id);
          ("message", `String message);
        ]
  | Batch_paused { batch_id; message } ->
      `Assoc
        [
          ("event", `String "batch_paused");
          ("batch_id", `String batch_id);
          ("message", `String message);
        ]
  | Batch_unpaused { batch_id; message } ->
      `Assoc
        [
          ("event", `String "batch_unpaused");
          ("batch_id", `String batch_id);
          ("message", `String message);
        ]
  | State_snapshot { batches } ->
      `Assoc
        [
          ("event", `String "state_snapshot");
          ("batches", `List (List.map batch_summary_to_yojson batches));
        ]

let event_of_yojson = function
  | `Assoc fields -> (
      match as_string (member "event" fields) with
      | "accepted" ->
          Accepted
            {
              batch_id = as_string (member "batch_id" fields);
              output_dir = as_string (member "output_dir" fields);
              total_jobs = as_int (member "total_jobs" fields);
              completed =
                (match List.assoc_opt "completed" fields with
                | Some value -> as_int value
                | None -> 0);
              prior_jobs =
                (match List.assoc_opt "prior_jobs" fields with
                | Some value -> as_int value
                | None -> 0);
              prior_completed =
                (match List.assoc_opt "prior_completed" fields with
                | Some value -> as_int value
                | None -> 0);
            }
      | "queue_progress" ->
          Queue_progress
            {
              batch_id = as_string (member "batch_id" fields);
              completed = as_int (member "completed" fields);
              total = as_int (member "total" fields);
            }
      | "job_finished" ->
          Job_finished
            {
              batch_id = as_string (member "batch_id" fields);
              completed = as_int (member "completed" fields);
              total = as_int (member "total" fields);
              solver = as_string (member "solver" fields);
              benchmark = as_string (member "benchmark" fields);
              result = as_string (member "result" fields);
            }
      | "output_file" ->
          Output_file
            {
              batch_id = as_string (member "batch_id" fields);
              name = as_string (member "name" fields);
              contents = as_string (member "contents" fields);
            }
      | "batch_finished" ->
          Batch_finished
            {
              batch_id = as_string (member "batch_id" fields);
              output_dir = as_string (member "output_dir" fields);
            }
      | "batch_failed" ->
          Batch_failed
            {
              batch_id = as_string (member "batch_id" fields);
              message = as_string (member "message" fields);
            }
      | "batch_killed" ->
          Batch_killed
            {
              batch_id = as_string (member "batch_id" fields);
              message = as_string (member "message" fields);
            }
      | "batch_paused" ->
          Batch_paused
            {
              batch_id = as_string (member "batch_id" fields);
              message = as_string (member "message" fields);
            }
      | "batch_unpaused" ->
          Batch_unpaused
            {
              batch_id = as_string (member "batch_id" fields);
              message = as_string (member "message" fields);
            }
      | "state_snapshot" ->
          State_snapshot
            {
              batches =
                (match member "batches" fields with
                | `List batches -> List.map batch_summary_of_yojson batches
                | _ -> invalid_arg "expected JSON batch summary list");
            }
      | event -> invalid_arg ("unknown event: " ^ event))
  | _ -> invalid_arg "expected JSON object"

let encode_request r = Yojson.Safe.to_string (request_to_yojson r)
let decode_request s = Yojson.Safe.from_string s |> request_of_yojson
let encode_submit r = encode_request (Submit r)
let decode_submit s =
  match decode_request s with
  | Submit r -> r
  | Reconnect _ | Aggregate _ | Kill _ | Pause _ | Unpause _ | State ->
      invalid_arg "expected submit"
let encode_event e = Yojson.Safe.to_string (event_to_yojson e)
let decode_event s = Yojson.Safe.from_string s |> event_of_yojson
