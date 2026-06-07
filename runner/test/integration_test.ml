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

let index_of ~needle haystack =
  let ln = String.length needle in
  let lh = String.length haystack in
  let rec loop i =
    if i + ln > lh then None
    else if String.sub haystack i ln = needle then Some i
    else loop (i + 1)
  in
  if ln = 0 then Some 0 else loop 0

let count_occurrences ~needle haystack =
  let ln = String.length needle in
  let lh = String.length haystack in
  let rec loop i count =
    if ln = 0 || i + ln > lh then count
    else if String.sub haystack i ln = needle then loop (i + ln) (count + 1)
    else loop (i + 1) count
  in
  loop 0 0

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

let read_zip_entry path name =
  let zf = Zip.open_in path in
  Fun.protect
    ~finally:(fun () -> Zip.close_in zf)
    (fun () -> Zip.read_entry zf (Zip.find_entry zf name))

let assert_zip_entries path expected =
  let zf = Zip.open_in path in
  Fun.protect
    ~finally:(fun () -> Zip.close_in zf)
    (fun () ->
      let actual = Zip.entries zf |> List.map (fun entry -> entry.Zip.filename) |> List.sort String.compare in
      if actual <> expected then
        fail
          (Printf.sprintf "unexpected entries in %s: expected [%s], got [%s]" path
             (String.concat "; " expected) (String.concat "; " actual)))

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

let start_server ~server ~port ~output_root ~cores ?max_memory ?state_file log_path =
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
    | Some memory -> [ "-max-memory"; memory ]
  in
  let args =
    args
    @
    match state_file with
    | None -> []
    | Some path -> [ "-state-file"; path ]
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

let read_line_from_fd fd =
  let buffer = Buffer.create 256 in
  let byte = Bytes.create 1 in
  let rec loop () =
    match Unix.read fd byte 0 1 with
    | 0 -> fail "server closed connection before sending a line"
    | _ ->
        let c = Bytes.get byte 0 in
        if c = '\n' then Buffer.contents buffer
        else (
          Buffer.add_char buffer c;
          loop ())
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> loop ()
  in
  loop ()

let request_first_event ~port request =
  let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close sock)
    (fun () ->
      Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_loopback, port));
      let line = Runner_lib.Protocol.encode_request request ^ "\n" in
      ignore (Unix.write_substring sock line 0 (String.length line));
      read_line_from_fd sock |> Runner_lib.Protocol.decode_event)

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

let test_reconnect_download_conflict ~client ~client_cwd ~port =
  let result =
    run_client ~client ~client_cwd ~port
      [ "-reconnect"; "batch-missing"; "-download"; "batch-missing" ]
  in
  assert_exit "client -reconnect/-download rejection" 2 result;
  assert_bool "client -reconnect/-download message"
    (contains ~needle:"-reconnect and -download are mutually exclusive" result.output)

let test_state_rejects_submission ~client ~client_cwd ~port list_path solver =
  let result = run_client ~client ~client_cwd ~port [ "-state"; list_path; solver ] in
  assert_exit "client -state submission rejection" 2 result;
  assert_bool "client -state message"
    (contains ~needle:"-state is mutually exclusive" result.output)

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
  assert_files_exact dir [ "log"; "results.xlsx"; "sat_solver.sh.csv" ];
  assert_csv_contains_sat dir "sat_solver.sh";
  (batch_id_of_output result.output, dir)

let test_finished_batch_reconnect_modes ~client ~client_cwd ~port batch_id =
  let reconnect = run_client ~client ~client_cwd ~port [ "-reconnect"; batch_id ] in
  assert_exit "finished batch reconnect without download" 0 reconnect;
  assert_bool "finished batch reconnect should finish"
    (contains ~needle:("finished " ^ batch_id) reconnect.output);
  assert_bool "finished batch reconnect should not download"
    (not (contains ~needle:"downloaded output files to " reconnect.output));
  let download = run_client ~client ~client_cwd ~port [ "-download"; batch_id ] in
  assert_exit "finished batch reconnect with download" 0 download;
  let dir = download_dir_of_output download.output in
  assert_files_exact dir [ "log"; "results.xlsx"; "sat_solver.sh.csv" ];
  assert_csv_contains_sat dir "sat_solver.sh";
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
  assert_bool "plain reconnect should not download files"
    (not (contains ~needle:"downloaded output files to " reconnect.output));
  let reconnect_with_download =
    run_client ~client ~client_cwd ~port [ "-download"; batch_id ]
  in
  assert_exit "reconnect with download" 0 reconnect_with_download;
  let dir = download_dir_of_output reconnect_with_download.output in
  assert_files_exact dir [ "log"; "sat_solver.sh.csv" ];
  assert_csv_contains_sat dir "sat_solver.sh";
  dir

let test_running_reconnect_without_download ~client ~client_cwd ~port ~bench_root ~exe_root
    list_path solver =
  let detach =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root [ "-timeout"; "20"; "-detach"; list_path; solver ])
  in
  assert_exit "running reconnect detach submit" 0 detach;
  let batch_id = batch_id_of_output detach.output in
  let reconnect = run_client ~client ~client_cwd ~port [ "-reconnect"; batch_id ] in
  assert_exit "running reconnect without download" 0 reconnect;
  assert_bool "running reconnect should finish"
    (contains ~needle:("finished " ^ batch_id) reconnect.output);
  assert_bool "running reconnect without download should not download"
    (not (contains ~needle:"downloaded output files to " reconnect.output))

let test_server_state_stream ~client ~client_cwd ~port ~bench_root ~exe_root list_path solver =
  let detach =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root [ "-timeout"; "20"; "-detach"; list_path; solver ])
  in
  assert_exit "state stream detach submit" 0 detach;
  let batch_id = batch_id_of_output detach.output in
  let event = request_first_event ~port Runner_lib.Protocol.State in
  (match event with
  | Runner_lib.Protocol.State_snapshot { batches } -> (
      match
        List.find_opt
          (fun (b : Runner_lib.Protocol.batch_summary) -> b.batch_id = batch_id)
          batches
      with
      | Some batch ->
          assert_bool "state stream should report benchmark name"
            (String.equal "benchmarks" batch.benchmark_name);
          assert_bool "state stream should report benchmark count" (batch.total_benchmarks = 2);
          assert_bool "state stream should report solver count" (batch.total_solvers = 1);
          assert_bool "state stream should report generations" (batch.generations = 1);
          assert_bool "state stream should report total jobs" (batch.total_jobs = 2);
          assert_bool "state stream completed count should be bounded"
            (batch.completed >= 0 && batch.completed <= batch.total_jobs);
          assert_bool "state stream should include queued/running jobs"
            (batch.queued_jobs + batch.running_jobs + batch.completed <= batch.total_jobs)
      | None -> fail ("state stream missing batch " ^ batch_id))
  | _ -> fail "state stream returned non-state event");
  let killed = run_client ~client ~client_cwd ~port [ "-kill"; batch_id ] in
  assert_exit "state stream cleanup kill" 0 killed

let test_kill_batch ~client ~client_cwd ~port ~bench_root ~exe_root list_path solver =
  let detach =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root [ "-timeout"; "60"; "-detach"; list_path; solver ])
  in
  assert_exit "kill detach submit" 0 detach;
  let batch_id = batch_id_of_output detach.output in
  let killed = run_client ~client ~client_cwd ~port [ "-kill"; batch_id ] in
  assert_exit "kill batch" 0 killed;
  assert_bool "kill message" (contains ~needle:("killed " ^ batch_id) killed.output);
  assert_bool "kill should not download"
    (not (contains ~needle:"downloaded output files to " killed.output));
  let reconnect = run_client ~client ~client_cwd ~port [ "-reconnect"; batch_id ] in
  assert_exit "reconnect killed batch" 0 reconnect;
  assert_bool "reconnect killed message" (contains ~needle:("killed " ^ batch_id) reconnect.output)

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
  assert_files_exact dir [ "log"; "relative_solver.sh.csv" ];
  assert_csv_contains_sat dir "relative_solver.sh"

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
  assert_files_exact dir [ "absolute_solver.sh.csv"; "log" ];
  assert_csv_contains_sat dir "absolute_solver.sh"

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
  let csv = read_file (Filename.concat dir "timeout_solver.sh.csv") in
  assert_bool "CSV should contain timeout" (contains ~needle:"\ttimeout" csv)

let test_solver_suffixes_preserved ~client ~client_cwd ~port ~bench_root ~exe_root =
  let list_path = Filename.concat client_cwd "suffix-list.txt" in
  let case_path = Filename.concat bench_root "suffix-case.smt2" in
  let mcsat_solver = Filename.concat exe_root "same_stem.mcsat" in
  let cdclt_solver = Filename.concat exe_root "same_stem.cdclT" in
  write_file case_path "(set-logic QF_UF)\n";
  write_file list_path "suffix-case.smt2\n";
  make_solver mcsat_solver "echo sat\n";
  make_solver cdclt_solver "echo unsat\n";
  let result =
    run_client ~client ~client_cwd ~port
      (with_server_roots ~bench_root ~exe_root
         [ "-timeout"; "5"; "-excel"; list_path; "same_stem.mcsat"; "same_stem.cdclT" ])
  in
  assert_exit "solver suffixes preserved" 0 result;
  let dir = download_dir_of_output result.output in
  assert_files_exact dir
    [ "log"; "results.xlsx"; "same_stem.cdclT.csv"; "same_stem.mcsat.csv" ];
  assert_csv_contains_sat dir "same_stem.mcsat";
  let cdclt_csv = read_file (Filename.concat dir "same_stem.cdclT.csv") in
  assert_bool "cdclT CSV should contain unsat result" (contains ~needle:"\tunsat" cdclt_csv);
  let xlsx_path = Filename.concat dir "results.xlsx" in
  assert_zip_entries xlsx_path
    [
      "[Content_Types].xml";
      "_rels/.rels";
      "xl/_rels/workbook.xml.rels";
      "xl/workbook.xml";
      "xl/worksheets/sheet1.xml";
      "xl/worksheets/sheet2.xml";
      "xl/worksheets/sheet3.xml";
      "xl/worksheets/sheet4.xml";
    ];
  let workbook = read_zip_entry xlsx_path "xl/workbook.xml" in
  let totals = read_zip_entry xlsx_path "xl/worksheets/sheet1.xml" in
  let clashes = read_zip_entry xlsx_path "xl/worksheets/sheet2.xml" in
  let sheet3 = read_zip_entry xlsx_path "xl/worksheets/sheet3.xml" in
  let sheet4 = read_zip_entry xlsx_path "xl/worksheets/sheet4.xml" in
  assert_bool "results.xlsx should include mcsat suffix"
    (contains ~needle:"same_stem.mcsat" sheet4);
  assert_bool "results.xlsx should include cdclT suffix"
    (contains ~needle:"same_stem.cdclT" sheet3);
  assert_bool "results.xlsx should have Totals worksheet"
    (contains ~needle:"<sheet name=\"Totals\"" workbook);
  assert_bool "results.xlsx should have Clashes worksheet"
    (contains ~needle:"<sheet name=\"Clashes\"" workbook);
  assert_bool "results.xlsx should have first solver worksheet"
    (contains ~needle:"<sheet name=\"Sheet1\"" workbook);
  assert_bool "results.xlsx should have second solver worksheet"
    (contains ~needle:"<sheet name=\"Sheet2\"" workbook);
  assert_bool "results.xlsx should use whole result column in COUNTIF"
    (contains ~needle:"COUNTIF(Sheet1!C:C,&quot;unsat&quot;)" totals);
  assert_bool "results.xlsx should use whole time column in SUMIF"
    (contains ~needle:"SUMIF(Sheet1!C:C,&quot;sat&quot;,Sheet1!E:E)" totals);
  assert_bool "results.xlsx should not use old HTML header"
    (not (contains ~needle:"<tr><th>Solver</th>" workbook));
  let worksheet_index name =
    match index_of ~needle:("<sheet name=\"" ^ name ^ "\"") workbook with
    | Some i -> i
    | None -> fail ("missing worksheet " ^ name)
  in
  assert_bool "Clashes worksheet should be after Totals"
    (worksheet_index "Totals" < worksheet_index "Clashes");
  assert_bool "Clashes worksheet should be before solver sheets"
    (worksheet_index "Clashes" < worksheet_index "Sheet1");
  assert_bool "Clashes header should list solver names"
    (contains
       ~needle:
         "<row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>Benchmark</t></is></c><c \
          r=\"B1\" t=\"inlineStr\"><is><t>same_stem.cdclT</t></is></c><c \
          r=\"C1\" t=\"inlineStr\"><is><t>same_stem.mcsat</t></is></c></row>"
       clashes);
  let clash_row =
    "<row r=\"2\"><c r=\"A2\" t=\"inlineStr\"><is><t>suffix-case.smt2</t></is></c><c \
     r=\"B2\" t=\"inlineStr\"><is><t>unsat</t></is></c><c r=\"C2\" \
     t=\"inlineStr\"><is><t>sat</t></is></c></row>"
  in
  assert_bool "Clashes worksheet should contain exactly one row for the clashing benchmark"
    (count_occurrences ~needle:clash_row clashes = 1)

let test_reconnect_folder_import ~client ~client_cwd ~port ~server_root =
  let import_dir = Filename.concat server_root "imported-csvs" in
  mkdir_p import_dir;
  write_file (Filename.concat import_dir "solver_a.csv")
    "solver_a\t\"case-1.smt2\"\tsat\t1.000000\t2.000000\t3.000000\n\
     solver_a\t\"case-2.smt2\"\ttimeout\n";
  write_file (Filename.concat import_dir "solver_b.csv")
    "solver_b\t\"case-1.smt2\"\tunsat\t4.000000\t5.000000\t6.000000\n";
  let import = run_client ~client ~client_cwd ~port [ "-reconnect"; "imported-csvs" ] in
  assert_exit "folder reconnect import" 0 import;
  assert_bool "folder reconnect should print accepted id"
    (contains ~needle:"accepted job id batch-" import.output);
  assert_bool "folder reconnect should finish"
    (contains ~needle:"finished batch-" import.output);
  assert_bool "folder reconnect without download should not download"
    (not (contains ~needle:"downloaded output files to " import.output));
  assert_bool "folder reconnect should produce results.xlsx on server"
    (Sys.file_exists (Filename.concat import_dir "results.xlsx"));
  let batch_id = batch_id_of_output import.output in
  let same_folder = run_client ~client ~client_cwd ~port [ "-reconnect"; "imported-csvs" ] in
  assert_exit "folder reconnect reuse" 0 same_folder;
  let reused_batch_id = batch_id_of_output same_folder.output in
  assert_bool "folder reconnect should reuse batch id" (String.equal batch_id reused_batch_id);
  let download = run_client ~client ~client_cwd ~port [ "-download"; batch_id ] in
  assert_exit "folder reconnect download by batch id" 0 download;
  let dir = download_dir_of_output download.output in
  assert_files_exact dir [ "results.xlsx"; "solver_a.csv"; "solver_b.csv" ];
  let direct_download =
    run_client ~client ~client_cwd ~port [ "-download"; "imported-csvs" ]
  in
  assert_exit "folder reconnect direct download" 0 direct_download;
  let direct_batch_id = batch_id_of_output direct_download.output in
  assert_bool "direct folder download should reuse batch id"
    (String.equal batch_id direct_batch_id);
  let direct_dir = download_dir_of_output direct_download.output in
  assert_files_exact direct_dir [ "results.xlsx"; "solver_a.csv"; "solver_b.csv" ];
  let xlsx_path = Filename.concat dir "results.xlsx" in
  let workbook = read_zip_entry xlsx_path "xl/workbook.xml" in
  let clashes = read_zip_entry xlsx_path "xl/worksheets/sheet2.xml" in
  assert_bool "imported spreadsheet should have Totals worksheet"
    (contains ~needle:"<sheet name=\"Totals\"" workbook);
  assert_bool "imported spreadsheet should have Clashes worksheet"
    (contains ~needle:"<sheet name=\"Clashes\"" workbook);
  assert_bool "imported spreadsheet should include clashing benchmark"
    (contains ~needle:"case-1.smt2" clashes);
  batch_id

let test_reconnect_absolute_folder_import ~client ~client_cwd ~port root =
  let import_dir = Filename.concat root "absolute-imported-csvs" in
  mkdir_p import_dir;
  write_file (Filename.concat import_dir "solver_abs.csv")
    "solver_abs\t\"abs-case.smt2\"\tsat\t7.000000\t8.000000\t9.000000\n";
  let import = run_client ~client ~client_cwd ~port [ "-download"; import_dir ] in
  assert_exit "absolute folder reconnect import" 0 import;
  assert_bool "absolute folder reconnect should print accepted id"
    (contains ~needle:"accepted job id batch-" import.output);
  let dir = download_dir_of_output import.output in
  assert_files_exact dir [ "results.xlsx"; "solver_abs.csv" ];
  assert_csv_contains_sat dir "solver_abs"

let test_state_restore_after_restart ~client ~client_cwd ~port ~batch_id ~imported_batch_id =
  let restored = run_client ~client ~client_cwd ~port [ "-download"; batch_id ] in
  assert_exit "state restore batch reconnect" 0 restored;
  assert_bool "restored batch should keep batch id"
    (contains ~needle:("finished " ^ batch_id) restored.output);
  let restored_dir = download_dir_of_output restored.output in
  assert_files_exact restored_dir [ "log"; "results.xlsx"; "sat_solver.sh.csv" ];
  assert_csv_contains_sat restored_dir "sat_solver.sh";
  let restored_import = run_client ~client ~client_cwd ~port [ "-download"; "imported-csvs" ] in
  assert_exit "state restore imported folder reconnect" 0 restored_import;
  let restored_import_id = batch_id_of_output restored_import.output in
  assert_bool "restored folder mapping should reuse imported batch id"
    (String.equal imported_batch_id restored_import_id);
  let restored_import_dir = download_dir_of_output restored_import.output in
  assert_files_exact restored_import_dir [ "results.xlsx"; "solver_a.csv"; "solver_b.csv" ]

let test_state_restore_requeues_running_batch ~client ~server ~client_cwd ~bench_root
    ~solver_dir root list_path =
  let output_root = Filename.concat root "restore-running-output" in
  let out_dir = Filename.concat output_root "restored-running" in
  let state_file = Filename.concat output_root "state.json" in
  let port = free_port () in
  mkdir_p out_dir;
  write_file state_file
    (Printf.sprintf
       {|
{
  "version": 1,
  "next_batch": 1,
  "batches": [
    {
      "sequence": 1,
      "id": "batch-000001",
      "request": {
        "type": "submit",
        "benchmark_file": %S,
        "benchmark_name": "benchmarks",
        "server_benchmark_root": %S,
        "server_exe_root": %S,
        "lines": ["sat-case-1.smt2"],
        "commands": ["sat_solver.sh"],
        "timeout": 5,
        "memory": null,
        "generations": 1,
        "sort": "alpha",
        "excel": true,
        "detach": true
      },
      "out_dir": %S,
      "total_jobs": 1,
      "prior_jobs": 0,
      "prior_completed": 0,
      "completed": 0,
      "status": {"status": "running"},
      "results": []
    }
  ],
  "folder_batches": []
}
|}
       list_path bench_root solver_dir out_dir);
  let server_log = Filename.concat root "restore-running-server.log" in
  let server_pid =
    start_server ~server ~port ~output_root ~cores:1 ~state_file server_log
  in
  Fun.protect
    ~finally:(fun () -> stop_server server_pid)
    (fun () ->
      wait_for_server port;
      let restored = run_client ~client ~client_cwd ~port [ "-download"; "batch-000001" ] in
      assert_exit "running state restore reconnect" 0 restored;
      assert_bool "running state restore should finish"
        (contains ~needle:"finished batch-000001" restored.output);
      let restored_dir = download_dir_of_output restored.output in
      assert_files_exact restored_dir [ "log"; "results.xlsx"; "sat_solver.sh.csv" ];
      assert_csv_contains_sat restored_dir "sat_solver.sh")

let test_reconnect_empty_folder_rejected ~client ~client_cwd ~port ~server_root =
  let empty_dir = Filename.concat server_root "empty-import" in
  mkdir_p empty_dir;
  let result = run_client ~client ~client_cwd ~port [ "-reconnect"; "empty-import" ] in
  assert_exit "empty folder reconnect rejection" 1 result;
  assert_bool "empty folder reconnect message"
    (contains ~needle:"no CSV result rows found" result.output)

let test_server_output_collision server_root =
  let dirs =
    readdir_names server_root
    |> List.filter (fun name -> contains ~needle:"run1" name)
  in
  assert_bool "server should create at least one -run1 output directory" (dirs <> [])

let test_server_accepts_terabyte_memory ~server root =
  let port = free_port () in
  let output_root = Filename.concat root "server-output-1t" in
  let server_log = Filename.concat root "server-1t.log" in
  mkdir_p output_root;
  let server_pid =
    start_server ~server ~port ~output_root ~cores:1 ~max_memory:"1T" server_log
  in
  Fun.protect
    ~finally:(fun () -> stop_server server_pid)
    (fun () -> wait_for_server port)

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
  let slow_solver = Filename.concat solver_dir "slow_solver.sh" in
  let kill_solver = Filename.concat solver_dir "kill_solver.sh" in
  make_solver sat_solver "echo sat\n";
  make_solver timeout_solver "sleep 3\necho sat\n";
  make_solver slow_solver "sleep 2\necho sat\n";
  make_solver kill_solver "sleep 30\necho sat\n";
  let list_path = Filename.concat client_cwd "benchmarks.txt" in
  write_file list_path "sat-case-1.smt2\nsat-case-2.smt2\n";
  write_file (Filename.concat bench_root "sat-case-1.smt2") "(set-logic QF_UF)\n";
  write_file (Filename.concat bench_root "sat-case-2.smt2") "(set-logic QF_UF)\n";
  test_server_accepts_terabyte_memory ~server root;
  test_state_restore_requeues_running_batch ~client ~server ~client_cwd ~bench_root
    ~solver_dir root list_path;
  let port = free_port () in
  let server_log = Filename.concat root "server.log" in
  let state_file = Filename.concat server_root "server-state.json" in
  let server_pid =
    start_server ~server ~port ~output_root:server_root ~cores:1 ~max_memory:"2G"
      ~state_file server_log
  in
  Fun.protect
    ~finally:(fun () -> stop_server server_pid)
    (fun () ->
      wait_for_server port;
      test_client_cores_rejected ~client ~client_cwd list_path sat_solver;
      test_reconnect_download_conflict ~client ~client_cwd ~port;
      test_state_rejects_submission ~client ~client_cwd ~port list_path "sat_solver.sh";
      let first_batch_id, first_dir =
        test_normal_submit_and_transfer ~client ~client_cwd ~port ~bench_root
          ~exe_root:solver_dir list_path "sat_solver.sh"
      in
      ignore (test_finished_batch_reconnect_modes ~client ~client_cwd ~port first_batch_id);
      let reconnect_dir =
        test_detach_and_reconnect ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir
          list_path "sat_solver.sh"
      in
      assert_bool "client reconnect download should use -runN"
        (starts_with ~prefix:(Filename.basename first_dir ^ "-run") (Filename.basename reconnect_dir));
      test_running_reconnect_without_download ~client ~client_cwd ~port ~bench_root
        ~exe_root:solver_dir list_path "slow_solver.sh";
      test_server_state_stream ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir
        list_path "slow_solver.sh";
      test_kill_batch ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir list_path
        "kill_solver.sh";
      test_server_root_relative_paths ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir;
      test_absolute_server_paths ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir;
      test_memory_rejection ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir list_path
        "sat_solver.sh";
      test_unknown_reconnect ~client ~client_cwd ~port;
      test_timeout_classification ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir
        list_path "timeout_solver.sh";
      test_solver_suffixes_preserved ~client ~client_cwd ~port ~bench_root ~exe_root:solver_dir;
      let imported_batch_id = test_reconnect_folder_import ~client ~client_cwd ~port ~server_root in
      test_reconnect_absolute_folder_import ~client ~client_cwd ~port root;
      test_reconnect_empty_folder_rejected ~client ~client_cwd ~port ~server_root;
      assert_bool "server state file should exist" (Sys.file_exists state_file);
      stop_server server_pid;
      let restarted_log = Filename.concat root "server-restarted.log" in
      let restarted_pid =
        start_server ~server ~port ~output_root:server_root ~cores:1 ~max_memory:"2G"
          ~state_file restarted_log
      in
      Fun.protect
        ~finally:(fun () -> stop_server restarted_pid)
        (fun () ->
          wait_for_server port;
          test_state_restore_after_restart ~client ~client_cwd ~port ~batch_id:first_batch_id
            ~imported_batch_id);
      test_server_output_collision server_root);
  print_endline "integration tests passed"
