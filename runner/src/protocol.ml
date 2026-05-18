type submit_request = {
  cwd : string;
  benchmark_file : string;
  benchmark_prefix : string;
  benchmark_name : string;
  lines : string list;
  commands : string list;
  timeout : int;
  memory : int option;
  generations : int;
  sort : string;
  excel : bool;
  detach : bool;
}

type event =
  | Accepted of { batch_id : string; output_dir : string; total_jobs : int }
  | Job_finished of {
      batch_id : string;
      completed : int;
      total : int;
      solver : string;
      benchmark : string;
      result : string;
    }
  | Batch_finished of { batch_id : string; output_dir : string }
  | Batch_failed of { batch_id : string; message : string }

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
      ("cwd", `String r.cwd);
      ("benchmark_file", `String r.benchmark_file);
      ("benchmark_prefix", `String r.benchmark_prefix);
      ("benchmark_name", `String r.benchmark_name);
      ("lines", `List (List.map (fun s -> `String s) r.lines));
      ("commands", `List (List.map (fun s -> `String s) r.commands));
      ("timeout", `Int r.timeout);
      ("memory", (match r.memory with Some m -> `Int m | None -> `Null));
      ("generations", `Int r.generations);
      ("sort", `String r.sort);
      ("excel", `Bool r.excel);
      ("detach", `Bool r.detach);
    ]

let submit_of_yojson = function
  | `Assoc fields ->
      if as_string (member "type" fields) <> "submit" then invalid_arg "expected submit";
      {
        cwd = as_string (member "cwd" fields);
        benchmark_file = as_string (member "benchmark_file" fields);
        benchmark_prefix = as_string (member "benchmark_prefix" fields);
        benchmark_name = as_string (member "benchmark_name" fields);
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

let event_to_yojson = function
  | Accepted { batch_id; output_dir; total_jobs } ->
      `Assoc
        [
          ("event", `String "accepted");
          ("batch_id", `String batch_id);
          ("output_dir", `String output_dir);
          ("total_jobs", `Int total_jobs);
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

let event_of_yojson = function
  | `Assoc fields -> (
      match as_string (member "event" fields) with
      | "accepted" ->
          Accepted
            {
              batch_id = as_string (member "batch_id" fields);
              output_dir = as_string (member "output_dir" fields);
              total_jobs = as_int (member "total_jobs" fields);
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
      | event -> invalid_arg ("unknown event: " ^ event))
  | _ -> invalid_arg "expected JSON object"

let encode_submit r = Yojson.Safe.to_string (submit_to_yojson r)
let decode_submit s = Yojson.Safe.from_string s |> submit_of_yojson
let encode_event e = Yojson.Safe.to_string (event_to_yojson e)
let decode_event s = Yojson.Safe.from_string s |> event_of_yojson
