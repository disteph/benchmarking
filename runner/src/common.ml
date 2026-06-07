open Containers
open Eio

type crash = [ `UnreadableOut | `UnreadableTimes | `Exception ]
type answer = Sat | Unsat [@@deriving eq]
type times = { wall : float; user : float; system : float }
type output =
  [ `Answer of answer * times
  | `Crash of crash * string
  | `Timeout
  | `Memout
  ]

type result = [ output | `Cancelled ]
type aggregate = [ output | `Inconsistent ]

module HStrings = CCHashtbl.Make (String)

let massage_out stdout =
  let (!=) a b = not (Int.equal a b) in
  if String.rfind ~sub:"unsat" stdout != -1 then `Unsat
  else if String.rfind ~sub:"sat" stdout != -1 then `Sat
  else `Crash (`UnreadableOut, stdout)

let massage_err stderr =
  let rec last_three = function
    | _ :: b :: c :: d :: tl -> last_three (b :: c :: d :: tl)
    | lines -> lines
  in
  let lines =
    String.lines stderr
    |> List.filter (fun line -> not (String.equal "" (String.trim line)))
    |> last_three
  in
  match lines with
  | [ wall; user; system ] -> (
      try
        `Times
          {
            wall = float_of_string wall;
            user = float_of_string user;
            system = float_of_string system;
          }
      with _ -> `Crash (`UnreadableTimes, stderr))
  | _ -> `Crash (`UnreadableTimes, stderr)

let write_cs file command s =
  Stream.add file
    (Format.sprintf "@[<v>%a@,%s@]@,@,"
       (List.pp ~pp_sep:(fun fmt () -> Format.fprintf fmt " ") String.pp)
       command s)

let write_coe file command interpretation stdout stderr =
  Stream.add file
    (Format.sprintf "@[<v>%a@,Interpreted as %s@,STDOUT@,%s@,STDERR@,%s@]@,@,"
       (List.pp ~pp_sep:(fun fmt () -> Format.fprintf fmt " ") String.pp)
       command interpretation stdout stderr)

let wtime_command =
  let dir = Filename.dirname Sys.executable_name in
  let candidates = [ Filename.concat dir "wtime"; Filename.concat dir "wtime.exe" ] in
  match List.find_opt CCIO.File.exists candidates with
  | Some command -> command
  | None -> "wtime"

let with_timing proc_mgr ~timeout ?memory ~log task =
  let timeout = string_of_int timeout in
  let memory = Option.map string_of_int memory in
  let timer = Timer.create "timer" in
  let command =
    wtime_command :: "-timeout" :: timeout
    :: (match memory with
       | Some memory -> "-memory" :: memory :: task
       | None -> task)
  in
  let aux () =
    Switch.run @@ fun sw ->
    let out_r, out_w = Process.pipe proc_mgr ~sw in
    let err_r, err_w = Process.pipe proc_mgr ~sw in
    try
      let child = Process.spawn ~sw proc_mgr ~stdout:out_w ~stderr:err_w command in
      Flow.close out_w;
      Flow.close err_w;
      Timer.start timer;
      let output =
        (fun () ->
          let read_all flow =
            Fun.protect
              ~finally:(fun () -> Flow.close flow)
              (fun () -> Buf_read.parse_exn Buf_read.take_all flow ~max_size:max_int)
          in
          let out = ref None in
          let err = ref None in
          Fiber.both
            (fun () -> out := Some (read_all out_r))
            (fun () -> err := Some (read_all err_r));
          let out = Option.get_exn_or "missing stdout" !out in
          let err = Option.get_exn_or "missing stderr" !err in
          match Process.await child with
          | `Exited 0 -> (
              match massage_out out, massage_err err with
              | `Unsat, `Times times ->
                  write_cs log command "unsat";
                  `Answer (Unsat, times)
              | `Sat, `Times times ->
                  write_cs log command "sat";
                  `Answer (Sat, times)
              | (`Crash (`UnreadableOut, _) as r), _ ->
                  write_coe log command "Crash: Unreadable out" out err;
                  r
              | _, (`Crash (`UnreadableTimes, _) as r) ->
                  write_coe log command "Crash: Unreadable err as times" out err;
                  r)
          | `Exited 124 ->
              write_coe log command "Timeout" out err;
              `Timeout
          | `Exited code ->
              let interpretation = Format.sprintf "Crash: exited %i" code in
              write_coe log command interpretation out err;
              `Crash (`Exception, interpretation)
          | `Signaled signal ->
              let interpretation = Format.sprintf "Crash: signaled %i" signal in
              write_coe log command interpretation out err;
              `Crash (`Exception, interpretation))
          ()
      in
      Timer.stop timer;
      output
    with Exn.Io _ as ex ->
      let bt = Printexc.get_raw_backtrace () in
      Exn.reraise_with_context ex bt "running command: %a" Process.pp_args task
  in
  match aux () with
  | output -> output
  | exception (Cancel.Cancelled _) ->
      write_cs log command "Cancelled";
      `Cancelled
  | exception exc ->
      let s = "Exception: " ^ Printexc.to_string exc in
      write_cs log command s;
      `Crash (`Exception, s)

let prune filename = Filename.(filename |> basename |> remove_extension)

let solver_output_name filename = Filename.basename filename

let resolve_server_path ~root path =
  if Filename.is_relative path then Filename.concat root path else path

let benchmark_path ~root instance = resolve_server_path ~root instance

let solver_path ~root command = resolve_server_path ~root command

let digest ~benchmark_file ~lines ~timeout ?memory () =
  let filename = prune benchmark_file in
  match memory with
  | Some m ->
      Format.sprintf "%s-%i-t%i-m%i" filename (Hash.list Hash.string lines) timeout m
  | None -> Format.sprintf "%s-%i-t%i" filename (Hash.list Hash.string lines) timeout

let rec mkdir_p path =
  if String.equal path "" || String.equal path (Filename.dirname path) then ()
  else if Sys.file_exists path then ()
  else (
    mkdir_p (Filename.dirname path);
    Unix.mkdir path 0o750)

let fresh_dir_native ~root ~dir =
  mkdir_p root;
  let rec create_fresh i =
    let dirname = if Int.equal i 0 then dir else Format.sprintf "%s-run%i" dir i in
    let out_path = Filename.concat root dirname in
    try
      Unix.mkdir out_path 0o750;
      out_path
    with Unix.Unix_error (Unix.EEXIST, _, _) -> create_fresh (i + 1)
  in
  create_fresh 0

let output_dir_name_from_path path =
  match Filename.basename path with
  | "" | "." | ".." -> "benchmark-output"
  | name -> name

let fresh_output_dir_native ~root ~output_dir =
  fresh_dir_native ~root ~dir:(output_dir_name_from_path output_dir)

let safe_output_file_name name =
  if
    String.equal name ""
    || String.equal name "."
    || String.equal name ".."
    || not (String.equal (Filename.basename name) name)
  then invalid_arg ("unsafe output file name: " ^ name);
  name

let result_file_names ~excel htbl =
  let csvs =
    HStrings.to_list htbl
    |> List.map (fun (command, _) -> safe_output_file_name (command ^ ".csv"))
    |> List.sort String.compare
  in
  [ "log" ] @ csvs @ if excel then [ "results.xlsx" ] else []

let strip_suffix ~suffix s =
  let ls = String.length s in
  let lf = String.length suffix in
  if ls >= lf && String.equal (String.sub s (ls - lf) lf) suffix then
    Some (String.sub s 0 (ls - lf))
  else None

let read_file_binary path =
  let ch = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in ch)
    (fun () -> really_input_string ch (in_channel_length ch))

let write_file_binary path contents =
  let ch = open_out_bin path in
  Fun.protect
    ~finally:(fun () -> close_out ch)
    (fun () -> output_string ch contents)

let rec drain_stream f stream =
  if Fiber.is_cancelled () then
    match Stream.take_nonblocking stream with
    | Some t ->
        f t;
        drain_stream f stream
    | None -> ()
  else
    let t = Stream.take stream in
    f t;
    drain_stream f stream

let add_aggregate_result ~htbl ~nb_benchmarks command bench (msg : aggregate) =
  let command_tbl =
    HStrings.get_or_add htbl ~f:(fun _ -> HStrings.create nb_benchmarks) ~k:command
  in
  let value =
    match HStrings.get command_tbl bench with
    | None -> (1, msg)
    | Some (n, previous) ->
        ( n + 1,
          match previous, msg with
          | `Inconsistent, _ | _, `Inconsistent -> `Inconsistent
          | `Crash msg, _ | _, `Crash msg -> `Crash msg
          | `Timeout, _ | _, `Timeout -> `Timeout
          | `Memout, _ | _, `Memout -> `Memout
          | `Answer (a, t1), `Answer (b, t2) ->
              if equal_answer a b then
                `Answer
                  ( a,
                    {
                      wall = t1.wall +. t2.wall;
                      user = t1.user +. t2.user;
                      system = t1.system +. t2.system;
                    } )
              else `Inconsistent )
  in
  HStrings.replace command_tbl bench value

let add_result ~htbl ~nb_benchmarks command bench (msg : result) =
  match msg with
  | `Cancelled -> ()
  | #output as msg ->
      add_aggregate_result ~htbl ~nb_benchmarks command bench (msg :> aggregate)

let csv_unquote s =
  let len = String.length s in
  if len >= 2 && Char.equal s.[0] '"' && Char.equal s.[len - 1] '"' then
    String.sub s 1 (len - 2)
  else s

let csv_aggregate_of_fields ~path ~line_no fields =
  let fail message =
    invalid_arg (Format.sprintf "%s:%d: %s" path line_no message)
  in
  match fields with
  | [ command; bench; result; wall; user; system ] -> (
      let times =
        try
          {
            wall = float_of_string wall;
            user = float_of_string user;
            system = float_of_string system;
          }
        with Failure _ -> fail "invalid time value"
      in
      match result with
      | "sat" -> (command, csv_unquote bench, `Answer (Sat, times))
      | "unsat" -> (command, csv_unquote bench, `Answer (Unsat, times))
      | _ -> fail ("result with times must be sat or unsat, got " ^ result))
  | [ command; bench; result ] ->
      let aggregate =
        match result with
        | "inconsistent" -> `Inconsistent
        | "timeout" -> `Timeout
        | "memout" -> `Memout
        | "crash" -> `Crash (`Exception, "crash")
        | "sat" | "unsat" -> fail ("missing time columns for " ^ result)
        | _ -> fail ("unknown result: " ^ result)
      in
      (command, csv_unquote bench, aggregate)
  | _ -> fail "expected 3 or 6 tab-separated fields"

let load_csv_file_into_hashtables ~htbl path =
  let rows = ref 0 in
  let ch = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in ch)
    (fun () ->
      let line_no = ref 0 in
      (try
         while true do
           let line = input_line ch in
           incr line_no;
           if not (String.equal "" (String.trim line)) then (
             let command, bench, aggregate =
               csv_aggregate_of_fields ~path ~line_no:!line_no (String.split_on_char '\t' line)
             in
             incr rows;
             add_aggregate_result ~htbl ~nb_benchmarks:16 command bench aggregate)
         done
       with End_of_file -> ());
      !rows)

let load_csv_dir_native dir =
  if not (Sys.file_exists dir && Sys.is_directory dir) then
    invalid_arg ("not a directory: " ^ dir);
  let htbl = HStrings.create 16 in
  let total_rows =
    Sys.readdir dir |> Array.to_list |> List.sort String.compare
    |> List.filter_map (fun name ->
           match strip_suffix ~suffix:".csv" name with
           | Some _ -> Some (Filename.concat dir name)
           | None -> None)
    |> List.fold_left
         (fun total path -> total + load_csv_file_into_hashtables ~htbl path)
         0
  in
  if total_rows = 0 then invalid_arg ("no CSV result rows found in " ^ dir);
  (htbl, total_rows)

let stream_to_hashtables ~htbl ~nb_benchmarks stream =
  drain_stream
    (function command, bench, msg -> add_result ~htbl ~nb_benchmarks command bench msg)
    stream

let average = function
  | instance, (n, `Answer (a, { wall; user; system })) ->
      let n = float_of_int n in
      ( instance,
        `Answer (a, { wall = wall /. n; user = user /. n; system = system /. n }) )
  | instance, (_, a) -> (instance, a)

let cmp_wall_times { wall = w1; _ } { wall = w2; _ } = Float.compare w1 w2
let cmp_user_times { user = u1; _ } { user = u2; _ } = Float.compare u1 u2

let cmp cmp_times (i1, (r1 : aggregate)) (i2, (r2 : aggregate)) =
  match r1, r2 with
  | `Answer (_, t1), `Answer (_, t2) -> Ord.pair cmp_times String.compare (t1, i1) (t2, i2)
  | `Answer _, _ -> -1
  | _, `Answer _ -> 1
  | _ -> String.compare i1 i2

let cmp_alpha (i1, _) (i2, _) = String.compare_natural i1 i2
let cmp_wall = cmp cmp_wall_times
let cmp_user = cmp cmp_user_times

let output_label : [ output | `Inconsistent ] -> string = function
  | `Answer (Unsat, _) -> "unsat"
  | `Answer (Sat, _) -> "sat"
  | `Inconsistent -> "inconsistent"
  | `Timeout -> "timeout"
  | `Memout -> "memout"
  | `Crash _ -> "crash"

let hashtables_to_files_native ?(overwrite = false) out_path htbl sort =
  let out_seq = HStrings.to_seq htbl in
  let process (command, res) =
    let pp_pair fmt (instance, res) =
      let wtimes s { wall; user; system } =
        Format.fprintf fmt "@[%s\t\"%s\"\t%s\t%f\t%f\t%f@]@," command instance s wall
          user system
      in
      let wotimes s = Format.fprintf fmt "@[%s\t\"%s\"\t%s@]@," command instance s in
      match res with
      | `Answer (Unsat, times) -> wtimes "unsat" times
      | `Answer (Sat, times) -> wtimes "sat" times
      | `Inconsistent -> wotimes "inconsistent"
      | `Timeout -> wotimes "timeout"
      | `Memout -> wotimes "memout"
      | `Crash _msg -> wotimes "crash"
    in
    let pp_htable fmt command_tbl =
      HStrings.to_list command_tbl |> List.map average |> List.sort sort
      |> List.iter (pp_pair fmt)
    in
    let filename = Filename.concat out_path (command ^ ".csv") in
    let create_mode = if overwrite then Open_trunc else Open_excl in
    let ch = open_out_gen [ Open_wronly; Open_creat; create_mode ] 0o640 filename in
    Fun.protect
      ~finally:(fun () -> close_out ch)
      (fun () ->
        let fmt = Format.formatter_of_out_channel ch in
        Format.fprintf fmt "%a" pp_htable res;
        Format.pp_print_flush fmt ())
  in
  Seq.iter process out_seq

let hashtables_to_excel_native ?(overwrite = false) out_path htbl sort =
  let project command (instance, r) =
    match r with
    | `Answer (Unsat, { wall; user; system }) ->
        (command, instance, "unsat", Some (wall, user, system))
    | `Answer (Sat, { wall; user; system }) ->
        (command, instance, "sat", Some (wall, user, system))
    | `Inconsistent -> (command, instance, "inconsistent", None)
    | `Timeout -> (command, instance, "timeout", None)
    | `Memout -> (command, instance, "memout", None)
    | `Crash _msg -> (command, instance, "crash", None)
  in
  let result_label command_tbl benchmark =
    match HStrings.get command_tbl benchmark with
    | Some (_, aggregate) -> output_label aggregate
    | None -> ""
  in
  let has_sat_and_unsat outputs =
    List.exists (String.equal "sat") outputs && List.exists (String.equal "unsat") outputs
  in
  let commands =
    HStrings.to_list htbl |> List.sort (fun (c1, _) (c2, _) -> String.compare_natural c1 c2)
  in
  let clashes =
    let benchmarks =
      commands
      |> List.concat_map (fun (_, command_tbl) ->
             HStrings.to_list command_tbl |> List.map fst)
      |> Stdlib.List.sort_uniq String.compare_natural
    in
    benchmarks
    |> List.filter_map (fun benchmark ->
           let outputs =
             List.map (fun (_, command_tbl) -> result_label command_tbl benchmark) commands
           in
           if has_sat_and_unsat outputs then Some { Excel.benchmark; outputs } else None)
  in
  let sheets =
    commands
    |> List.mapi (fun i (command, command_tbl) ->
           let rows =
             HStrings.to_list command_tbl |> List.map average |> List.sort sort
             |> List.map (project command)
           in
           { Excel.name = Format.sprintf "Sheet%i" (i + 1); rows })
  in
  let filename = Filename.concat out_path "results.xlsx" in
  if (not overwrite) && Sys.file_exists filename then
    invalid_arg ("output file already exists: " ^ filename);
  Excel.write_workbook filename (sheets, clashes)

let sort_of_name = function
  | "alpha" -> cmp_alpha
  | "wall" -> cmp_wall
  | "user" -> cmp_user
  | s -> invalid_arg ("unknown sort mode: " ^ s)

let output_to_string : result -> string = function
  | `Cancelled -> "cancelled"
  | #output as output -> output_label output
