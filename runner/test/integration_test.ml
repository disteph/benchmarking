type command_result = {
  status : Unix.process_status;
  output : string;
}

let fail msg = failwith msg

let assert_bool msg b = if not b then fail msg

let starts_with ~prefix s =
  let lp = String.length prefix in
  String.length s >= lp && String.sub s 0 lp = prefix

let contains ~needle haystack =
  let ln = String.length needle in
  let lh = String.length haystack in
  let rec loop i =
    i + ln <= lh && (String.sub haystack i ln = needle || loop (i + 1))
  in
  ln = 0 || loop 0

let lines s = String.split_on_char '\n' s

let find_line ~prefix s =
  match List.find_opt (starts_with ~prefix) (lines s) with
  | Some line -> line
  | None -> fail ("missing output line with prefix: " ^ prefix ^ "\noutput:\n" ^ s)

let field n line =
  let fields = String.split_on_char ' ' line |> List.filter (( <> ) "") in
  try List.nth fields n
  with Failure _ -> fail ("not enough fields in line: " ^ line)

let batch_id_of_output output =
  find_line ~prefix:"accepted job id " output |> field 3

let download_dir_of_output output =
  let prefix = "downloaded output files to " in
  let line = find_line ~prefix output in
  String.sub line (String.length prefix) (String.length line - String.length prefix)

let exit_code = function
  | Unix.WEXITED n -> n
  | Unix.WSIGNALED n -> fail (Printf.sprintf "process signaled: %d" n)
  | Unix.WSTOPPED n -> fail (Printf.sprintf "process stopped: %d" n)

let assert_exit msg expected result =
  let actual = exit_code result.status in
  if actual <> expected then
    fail
      (Printf.sprintf "%s: expected exit %d, got %d\noutput:\n%s" msg expected actual
         result.output)

let mkdir_p path =
  let rec loop path =
    if path = "" || path = Filename.dirname path || Sys.file_exists path then ()
    else (
      loop (Filename.dirname path);
      Unix.mkdir path 0o700)
  in
  loop path

let write_file path contents =
  mkdir_p (Filename.dirname path);
  let ch = open_out path in
  Fun.protect ~finally:(fun () -> close_out ch) (fun () -> output_string ch contents)

let chmod_x path = Unix.chmod path 0o755

let read_file path =
  let ch = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in ch)
    (fun () -> really_input_string ch (in_channel_length ch))

let readdir_names path =
  Sys.readdir path |> Array.to_list |> List.sort String.compare

let fresh_root () =
  let root =
    Filename.concat (Filename.get_temp_dir_name ())
      (Printf.sprintf "runner-integration-%d-%d" (Unix.getpid ()) (Random.bits ()))
  in
  Unix.mkdir root 0o700;
  root

let free_port () =
  let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close sock)
    (fun () ->
      Unix.setsockopt sock Unix.SO_REUSEADDR true;
      Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
      match Unix.getsockname sock with
      | Unix.ADDR_INET (_, port) -> port
      | Unix.ADDR_UNIX _ -> assert false)

let read_fd_all fd =
  let buf = Bytes.create 4096 in
  let out = Buffer.create 4096 in
  let rec loop () =
    match Unix.read fd buf 0 (Bytes.length buf) with
    | 0 -> Buffer.contents out
    | n ->
        Buffer.add_subbytes out buf 0 n;
        loop ()
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> loop ()
  in
  loop ()

let run_capture ?cwd prog args =
  let null = Unix.openfile "/dev/null" [ Unix.O_RDONLY ] 0 in
  let read_fd, write_fd = Unix.pipe () in
  let old_cwd = Unix.getcwd () in
  let pid =
    Fun.protect
      ~finally:(fun () ->
        (match cwd with Some _ -> Unix.chdir old_cwd | None -> ());
        Unix.close null;
        Unix.close write_fd)
      (fun () ->
        Option.iter Unix.chdir cwd;
        Unix.create_process prog (Array.of_list (prog :: args)) null write_fd write_fd)
  in
  let output =
    Fun.protect ~finally:(fun () -> Unix.close read_fd) (fun () -> read_fd_all read_fd)
  in
  let _, status = Unix.waitpid [] pid in
  { status; output }

let start_server ~server ~port ~output_root ~cores ?max_memory log_path =
  let log_fd =
    Unix.openfile log_path [ Unix.O_CREAT; Unix.O_TRUNC; Unix.O_WRONLY ] 0o600
  in
  let null = Unix.openfile "/dev/null" [ Unix.O_RDONLY ] 0 in
  let args =
    [
      server;
      "-port";
      string_of_int port;
      "-cores";
      string_of_int cores;
      "-output-root";
      output_root;
    ]
    @
    match max_memory with
    | None -> []
    | Some memory -> [ "-max-memory"; string_of_int memory ]
  in
  let pid =
    Fun.protect
      ~finally:(fun () ->
        Unix.close log_fd;
        Unix.close null)
      (fun () -> Unix.create_process server (Array.of_list args) null log_fd log_fd)
  in
  pid

let wait_for_server port =
  let deadline = Unix.gettimeofday () +. 5.0 in
  let rec loop () =
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let connected =
      match Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_loopback, port)) with
      | () -> true
      | exception Unix.Unix_error ((Unix.ECONNREFUSED | Unix.EPERM), _, _) -> false
      | exception Unix.Unix_error (Unix.EINTR, _, _) -> false
    in
    Unix.close sock;
    if connected then ()
    else if Unix.gettimeofday () > deadline then fail "server did not start listening"
    else (
      Unix.sleepf 0.05;
      loop ())
  in
  loop ()

let stop_server pid =
  (try Unix.kill pid Sys.sigterm with Unix.Unix_error (Unix.ESRCH, _, _) -> ());
  match Unix.waitpid [] pid with
  | _, _ -> ()
  | exception Unix.Unix_error (Unix.ECHILD, _, _) -> ()

let make_solver path body =
  write_file path ("#!/bin/sh\n" ^ body);
  chmod_x path

let assert_files_exact dir expected =
  let actual = readdir_names dir in
  if actual <> expected then
    fail
      (Printf.sprintf "unexpected files in %s: expected [%s], got [%s]" dir
         (String.concat "; " expected) (String.concat "; " actual))

let assert_csv_contains_sat dir solver_name =
  let csv = read_file (Filename.concat dir (solver_name ^ ".csv")) in
  assert_bool "CSV should contain sat result" (contains ~needle:"\tsat" csv)

let run_client ~client ~client_cwd ~port args =
  run_capture ~cwd:client_cwd client ("-server" :: ("127.0.0.1:" ^ string_of_int port) :: args)

let with_server_roots ~bench_root ~exe_root args =
  "-server-benchmark-root" :: bench_root :: "-server-exe-root" :: exe_root :: args

let test_client_cores_rejected ~client ~client_cwd list_path solver =
  let result = run_capture ~cwd:client_cwd client [ "-cores"; "2"; list_path; solver ] in
  assert_exit "client -cores rejection" 2 result;
  assert_bool "client -cores message" (contains ~needle:"-cores is now" result.output)

let test_normal_submit_and_transfer ~client ~client_cwd ~port ~bench_root ~exe_root list_path
    solver =
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root
         [ "-timeout"; "5"; "-excel"; list_path; solver ])
  in
  assert_exit "normal submit" 0 result;
  assert_bool "accepted id printed" (contains ~needle:"accepted job id batch-" result.output);
  let dir = download_dir_of_output result.output in
  assert_files_exact dir [ "log"; "results.xls"; "sat_solver.csv" ];
  assert_csv_contains_sat dir "sat_solver";
  dir

let test_detach_and_reconnect ~client ~client_cwd ~port ~bench_root ~exe_root list_path solver =
  let detach =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root [ "-timeout"; "5"; "-detach"; list_path; solver ])
  in
  assert_exit "detach submit" 0 detach;
  let batch_id = batch_id_of_output detach.output in
  let reconnect = run_client ~client ~client_cwd ~port [ "-reconnect"; batch_id ] in
  assert_exit "reconnect" 0 reconnect;
  let dir = download_dir_of_output reconnect.output in
  assert_files_exact dir [ "log"; "sat_solver.csv" ];
  assert_csv_contains_sat dir "sat_solver";
  dir

let test_server_root_relative_paths ~client ~client_cwd ~port ~bench_root ~exe_root =
  let list_dir = Filename.concat client_cwd "relative-list" in
  let list_path = Filename.concat list_dir "benchmarks.txt" in
  let case_path = Filename.concat bench_root "nested/case.smt2" in
  let solver_path = Filename.concat exe_root "relative_solver.sh" in
  write_file case_path "(set-logic QF_UF)\n";
  write_file list_path "nested/case.smt2\n";
  make_solver solver_path "test -f \"$1\" || exit 43\necho sat\n";
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root
         [ "-timeout"; "5"; "relative-list/benchmarks.txt"; "relative_solver.sh" ])
  in
  assert_exit "server root relative paths" 0 result;
  let dir = download_dir_of_output result.output in
  assert_files_exact dir [ "log"; "relative_solver.csv" ];
  assert_csv_contains_sat dir "relative_solver"

let test_absolute_server_paths ~client ~client_cwd ~port ~bench_root ~exe_root =
  let absolute_case = Filename.concat bench_root "absolute-case.smt2" in
  let absolute_solver = Filename.concat exe_root "absolute_solver.sh" in
  let list_path = Filename.concat client_cwd "absolute-list.txt" in
  write_file absolute_case "(set-logic QF_UF)\n";
  write_file list_path (absolute_case ^ "\n");
  make_solver absolute_solver "test -f \"$1\" || exit 44\necho sat\n";
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root:"/no/such/benchmark/root" ~exe_root:"/no/such/exe/root"
         [ "-timeout"; "5"; list_path; absolute_solver ])
  in
  assert_exit "absolute server paths" 0 result;
  let dir = download_dir_of_output result.output in
  assert_files_exact dir [ "absolute_solver.csv"; "log" ];
  assert_csv_contains_sat dir "absolute_solver"

let test_memory_rejection ~client ~client_cwd ~port ~bench_root ~exe_root list_path solver =
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root
         [ "-timeout"; "5"; "-memory"; "3"; list_path; solver ])
  in
  assert_exit "memory rejection" 1 result;
  assert_bool "memory rejection message"
    (contains ~needle:"exceeds server -max-memory" result.output)

let test_unknown_reconnect ~client ~client_cwd ~port =
  let result = run_client ~client ~client_cwd ~port [ "-reconnect"; "batch-missing" ] in
  assert_exit "unknown reconnect" 1 result;
  assert_bool "unknown reconnect message" (contains ~needle:"unknown job id" result.output)

let test_timeout_classification ~client ~client_cwd ~port ~bench_root ~exe_root list_path
    timeout_solver =
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root [ "-timeout"; "1"; list_path; timeout_solver ])
  in
  assert_exit "timeout classification" 0 result;
  let dir = download_dir_of_output result.output in
  let csv = read_file (Filename.concat dir "timeout_solver.csv") in
  assert_bool "CSV should contain timeout" (contains ~needle:"\ttimeout" csv)

let test_server_output_collision server_root =
  let dirs =
    readdir_names server_root
    |> List.filter (fun name -> contains ~needle:"run1" name)
  in
  assert_bool "server should create at least one -run1 output directory" (dirs <> [])

let absolute_path path =
  if Filename.is_relative path then Filename.concat (Unix.getcwd ()) path else path

let () =
  Random.self_init ();
  let client, server, _wtime =
    match Array.to_list Sys.argv with
    | [ _; client; server; wtime ] ->
        (absolute_path client, absolute_path server, absolute_path wtime)
    | _ -> fail "usage: integration_test CLIENT SERVER WTIME"
  in
  let root = fresh_root () in
  let client_cwd = Filename.concat root "client" in
  let bench_root = Filename.concat root "server-benchmarks" in
  let server_root = Filename.concat root "server-output" in
  let solver_dir = Filename.concat root "solvers" in
  mkdir_p client_cwd;
  mkdir_p bench_root;
  mkdir_p server_root;
  mkdir_p solver_dir;
  let sat_solver = Filename.concat solver_dir "sat_solver.sh" in
  let timeout_solver = Filename.concat solver_dir "timeout_solver.sh" in
  make_solver sat_solver "echo sat\n";
  make_solver timeout_solver "sleep 3\necho sat\n";
  let list_path = Filename.concat client_cwd "benchmarks.txt" in
  write_file list_path "sat-case-1.smt2\nsat-case-2.smt2\n";
  write_file (Filename.concat bench_root "sat-case-1.smt2") "(set-logic QF_UF)\n";
  write_file (Filename.concat bench_root "sat-case-2.smt2") "(set-logic QF_UF)\n";
  let port = free_port () in
  let server_log = Filename.concat root "server.log" in
  let server_pid = start_server ~server ~port ~output_root:server_root ~cores:1 ~max_memory:2 server_log in
  Fun.protect
    ~finally:(fun () -> stop_server server_pid)
    (fun () ->
      wait_for_server port;
      test_client_cores_rejected ~client ~client_cwd list_path sat_solver;
      let first_dir =
        test_normal_submit_and_transfer ~client ~client_cwd ~port ~bench_root
          ~exe_root:solver_dir list_path "sat_solver.sh"
      in
      let reconnect_dir =
        test_detach_and_reconnect ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir
          list_path "sat_solver.sh"
      in
      assert_bool "client reconnect download should use -runN"
        (starts_with ~prefix:(Filename.basename first_dir ^ "-run") (Filename.basename reconnect_dir));
      test_server_root_relative_paths ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir;
      test_absolute_server_paths ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir;
      test_memory_rejection ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir list_path
        "sat_solver.sh";
      test_unknown_reconnect ~client ~client_cwd ~port;
      test_timeout_classification ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir
        list_path "timeout_solver.sh";
      test_server_output_collision server_root);
  print_endline "integration tests passed"
