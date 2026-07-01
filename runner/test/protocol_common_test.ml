open Runner_lib
open Protocol

let fail msg = failwith msg

let assert_bool msg b = if not b then fail msg
let assert_equal msg a b = if a <> b then fail (Printf.sprintf "%s: expected %S, got %S" msg a b)
let assert_int msg a b = if a <> b then fail (Printf.sprintf "%s: expected %d, got %d" msg a b)

let assert_raises msg f =
  match f () with
  | exception _ -> ()
  | _ -> fail msg

let submit_request =
  {
    Protocol.benchmark_file = "lists/benchmarks.txt";
    benchmark_name = "benchmarks";
    server_benchmark_root = "/server/benchmarks";
    server_exe_root = "/server/solvers";
    lines = [ "a.smt2"; "b.smt2" ];
    commands = [ "./solver"; "/bin/echo" ];
    timeout = 30;
    memory = Some 4;
    generations = 2;
    sort = "wall";
    excel = true;
    detach = false;
  }

let test_submit_roundtrip () =
  match Protocol.encode_request (Submit submit_request) |> Protocol.decode_request with
  | Submit r ->
      assert_equal "benchmark_file" submit_request.benchmark_file r.benchmark_file;
      assert_equal "server_benchmark_root" submit_request.server_benchmark_root
        r.server_benchmark_root;
      assert_equal "server_exe_root" submit_request.server_exe_root r.server_exe_root;
      assert_int "timeout" submit_request.timeout r.timeout;
      assert_int "generations" submit_request.generations r.generations;
      assert_bool "excel" r.excel
  | Reconnect _ -> fail "decoded submit as reconnect"
  | Aggregate _ -> fail "decoded submit as aggregate"
  | Kill _ -> fail "decoded submit as kill"
  | Pause _ -> fail "decoded submit as pause"
  | Unpause _ -> fail "decoded submit as unpause"
  | State -> fail "decoded submit as state"

let test_legacy_submit_defaults () =
  let json =
    `Assoc
      [
        ("benchmark_file", `String "lists/benchmarks.txt");
        ("benchmark_prefix", `String "/old/benchmarks");
        ("cwd", `String "/old/solvers");
        ("lines", `List [ `String "a.smt2" ]);
        ("commands", `List [ `String "solver" ]);
      ]
  in
  let r = Protocol.submit_of_yojson json in
  assert_equal "legacy benchmark root" "/old/benchmarks" r.server_benchmark_root;
  assert_equal "legacy exe root" "/old/solvers" r.server_exe_root;
  assert_equal "legacy benchmark name" "benchmarks" r.benchmark_name;
  assert_int "legacy timeout default" 300 r.timeout;
  assert_int "legacy generations default" 1 r.generations;
  assert_bool "legacy excel default" (not r.excel);
  assert_bool "legacy detach default" (not r.detach)

let test_reconnect_roundtrip () =
  match
    Protocol.encode_request (Reconnect { batch_id = "batch-000123"; download = true })
    |> Protocol.decode_request
  with
  | Reconnect { batch_id; download } ->
      assert_equal "batch_id" "batch-000123" batch_id;
      assert_bool "download" download
  | Aggregate _ -> fail "decoded reconnect as aggregate"
  | Kill _ -> fail "decoded reconnect as kill"
  | Pause _ -> fail "decoded reconnect as pause"
  | Unpause _ -> fail "decoded reconnect as unpause"
  | Submit _ -> fail "decoded reconnect as submit"
  | State -> fail "decoded reconnect as state"

let test_legacy_reconnect_defaults_no_download () =
  match Protocol.decode_request {|{"type":"reconnect","batch_id":"batch-000123"}|} with
  | Reconnect { batch_id; download } ->
      assert_equal "batch_id" "batch-000123" batch_id;
      assert_bool "download default" (not download)
  | Aggregate _ -> fail "decoded reconnect as aggregate"
  | Kill _ -> fail "decoded reconnect as kill"
  | Pause _ -> fail "decoded reconnect as pause"
  | Unpause _ -> fail "decoded reconnect as unpause"
  | Submit _ -> fail "decoded reconnect as submit"
  | State -> fail "decoded reconnect as state"

let test_aggregate_roundtrip () =
  match
    Protocol.encode_request (Aggregate { prefix = "QF_NRA5"; download = true })
    |> Protocol.decode_request
  with
  | Aggregate { prefix; download } ->
      assert_equal "prefix" "QF_NRA5" prefix;
      assert_bool "download" download
  | Submit _ -> fail "decoded aggregate as submit"
  | Reconnect _ -> fail "decoded aggregate as reconnect"
  | Kill _ -> fail "decoded aggregate as kill"
  | Pause _ -> fail "decoded aggregate as pause"
  | Unpause _ -> fail "decoded aggregate as unpause"
  | State -> fail "decoded aggregate as state"

let test_kill_roundtrip () =
  match Protocol.encode_request (Kill { batch_id = "batch-000123" }) |> Protocol.decode_request with
  | Kill { batch_id } -> assert_equal "batch_id" "batch-000123" batch_id
  | Submit _ -> fail "decoded kill as submit"
  | Reconnect _ -> fail "decoded kill as reconnect"
  | Aggregate _ -> fail "decoded kill as aggregate"
  | Pause _ -> fail "decoded kill as pause"
  | Unpause _ -> fail "decoded kill as unpause"
  | State -> fail "decoded kill as state"

let test_pause_roundtrip () =
  match
    Protocol.encode_request (Pause { batch_id = "batch-000123" }) |> Protocol.decode_request
  with
  | Pause { batch_id } -> assert_equal "batch_id" "batch-000123" batch_id
  | Submit _ -> fail "decoded pause as submit"
  | Reconnect _ -> fail "decoded pause as reconnect"
  | Aggregate _ -> fail "decoded pause as aggregate"
  | Kill _ -> fail "decoded pause as kill"
  | Unpause _ -> fail "decoded pause as unpause"
  | State -> fail "decoded pause as state"

let test_unpause_roundtrip () =
  match
    Protocol.encode_request (Unpause { batch_id = "batch-000123" }) |> Protocol.decode_request
  with
  | Unpause { batch_id } -> assert_equal "batch_id" "batch-000123" batch_id
  | Submit _ -> fail "decoded unpause as submit"
  | Reconnect _ -> fail "decoded unpause as reconnect"
  | Aggregate _ -> fail "decoded unpause as aggregate"
  | Kill _ -> fail "decoded unpause as kill"
  | Pause _ -> fail "decoded unpause as pause"
  | State -> fail "decoded unpause as state"

let test_state_roundtrip () =
  match Protocol.encode_request State |> Protocol.decode_request with
  | State -> ()
  | Submit _ -> fail "decoded state as submit"
  | Reconnect _ -> fail "decoded state as reconnect"
  | Aggregate _ -> fail "decoded state as aggregate"
  | Kill _ -> fail "decoded state as kill"
  | Pause _ -> fail "decoded state as pause"
  | Unpause _ -> fail "decoded state as unpause"

let test_event_roundtrip () =
  let events =
    [
      Protocol.Accepted
        {
          batch_id = "batch-1";
          output_dir = "/server/out";
          total_jobs = 10;
          completed = 3;
          prior_jobs = 7;
          prior_completed = 2;
        };
      Queue_progress { batch_id = "batch-1"; completed = 4; total = 7 };
      Job_finished
        {
          batch_id = "batch-1";
          completed = 5;
          total = 10;
          solver = "solver";
          benchmark = "case.smt2";
          result = "sat";
        };
      Output_file { batch_id = "batch-1"; name = "solver.csv"; contents = "YWJj" };
      Batch_finished { batch_id = "batch-1"; output_dir = "/server/out" };
      Batch_failed { batch_id = "batch-1"; message = "failed" };
      Batch_killed { batch_id = "batch-1"; message = "killed" };
      Batch_paused { batch_id = "batch-1"; message = "paused" };
      Batch_unpaused { batch_id = "batch-1"; message = "unpaused" };
      State_snapshot
        {
          batches =
            [
              {
                batch_id = "batch-1";
                benchmark_name = "benchmarks";
                output_dir = "/server/out";
                total_benchmarks = 2;
                total_solvers = 5;
                generations = 1;
                total_jobs = 10;
                completed = 3;
                queued_jobs = 6;
                running_jobs = 1;
                paused = true;
              };
            ];
        };
    ]
  in
  List.iter
    (fun event ->
      let decoded = Protocol.encode_event event |> Protocol.decode_event in
      if decoded <> event then fail "event roundtrip changed value")
    events

let test_legacy_state_snapshot_defaults_unpaused () =
  match
    Protocol.decode_event
      {|{"event":"state_snapshot","batches":[{"batch_id":"batch-1","benchmark_name":"benchmarks","output_dir":"/server/out","total_jobs":10,"completed":3}]}|}
  with
  | State_snapshot { batches = [ batch ] } ->
      assert_bool "legacy state snapshot defaults to unpaused" (not batch.paused)
  | State_snapshot _ -> fail "legacy state snapshot decoded wrong batch count"
  | _ -> fail "legacy state snapshot decoded as non-state event"

let test_base64_roundtrip () =
  let binary = String.init 256 Char.chr in
  let encoded = Protocol.base64_encode binary in
  let decoded = Protocol.base64_decode encoded in
  if decoded <> binary then fail "base64 roundtrip changed binary data"

let with_tmp_dir f =
  let root =
    Filename.concat (Filename.get_temp_dir_name ())
      (Printf.sprintf "runner-protocol-common-test-%d" (Unix.getpid ()))
  in
  if Sys.file_exists root then fail ("temporary directory already exists: " ^ root);
  Unix.mkdir root 0o700;
  Fun.protect ~finally:(fun () -> ()) (fun () -> f root)

let test_fresh_output_dirs () =
  with_tmp_dir @@ fun root ->
  let first = Common.fresh_output_dir_native ~root ~output_dir:"server/path/results" in
  let second = Common.fresh_output_dir_native ~root ~output_dir:"server/path/results" in
  assert_equal "first basename" "results" (Filename.basename first);
  assert_equal "second basename" "results-run1" (Filename.basename second);
  assert_bool "first exists" (Sys.is_directory first);
  assert_bool "second exists" (Sys.is_directory second)

let test_safe_output_file_names () =
  assert_equal "safe file" "log" (Common.safe_output_file_name "log");
  assert_raises "reject slash" (fun () -> ignore (Common.safe_output_file_name "a/b"));
  assert_raises "reject dotdot" (fun () -> ignore (Common.safe_output_file_name ".."));
  assert_raises "reject empty" (fun () -> ignore (Common.safe_output_file_name ""))

let test_solver_output_name () =
  assert_equal "relative solver suffix" "solver.mcsat" (Common.solver_output_name "solver.mcsat");
  assert_equal "absolute solver suffix" "solver.cdclT"
    (Common.solver_output_name "/tmp/install/solver.cdclT")

let test_result_file_names () =
  let htbl = Common.HStrings.create 3 in
  Common.HStrings.add htbl "solver-b" (Common.HStrings.create 1);
  Common.HStrings.add htbl "solver-a" (Common.HStrings.create 1);
  let names = Common.result_file_names ~excel:true htbl in
  if names <> [ "log"; "solver-a.csv"; "solver-b.csv"; "results.xlsx" ] then
    fail "unexpected result file names"

let test_massage_out_requires_answer_line () =
  begin
    match Common.massage_out "sat\n" with
    | `Sat -> ()
    | _ -> fail "sat line was not classified as sat"
  end;
  begin
    match Common.massage_out "unsat\n" with
    | `Unsat -> ()
    | _ -> fail "unsat line was not classified as unsat"
  end;
  begin
    match Common.massage_out "(error \"mcsat: unsupported theory\")\n" with
    | `Crash (`UnreadableOut, _) -> ()
    | _ -> fail "mcsat error was misclassified as an answer"
  end

let () =
  test_submit_roundtrip ();
  test_legacy_submit_defaults ();
  test_reconnect_roundtrip ();
  test_legacy_reconnect_defaults_no_download ();
  test_aggregate_roundtrip ();
  test_kill_roundtrip ();
  test_pause_roundtrip ();
  test_unpause_roundtrip ();
  test_state_roundtrip ();
  test_event_roundtrip ();
  test_legacy_state_snapshot_defaults_unpaused ();
  test_base64_roundtrip ();
  test_fresh_output_dirs ();
  test_safe_output_file_names ();
  test_solver_output_name ();
  test_result_file_names ();
  test_massage_out_requires_answer_line ();
  print_endline "protocol/common tests passed"
