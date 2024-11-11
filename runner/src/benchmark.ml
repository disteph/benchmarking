open Containers

open Eio

type crash = [`UnreadableOut | `UnreadableTimes | `Exception ]
type answer = Sat | Unsat [@@deriving eq]
type times = { wall : float; user : float; system : float}
type output = [ `Answer of answer*times
              | `Crash of crash*string
              | `Timeout
              | `Memout ]

let massage_out stdout =
  let (!=) a b = not(Int.equal a b) in
  if String.rfind ~sub:"unsat" stdout != -1
  then `Unsat
  else if String.rfind ~sub:"sat" stdout != -1
  then `Sat
  else `Crash(`UnreadableOut, stdout)

let massage_err stderr =
  match String.lines stderr with
  | [wall; user; system] ->
     (try
        `Times{ wall = float_of_string wall;
                user = float_of_string user;
                system = float_of_string system}
      with _ -> `Crash(`UnreadableTimes, stderr))
  | _  -> `Crash(`UnreadableTimes, stderr)

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

let with_timing proc_mgr ~timeout ?memory ~log task =
  let timeout = string_of_int timeout in
  let memory  = Option.map string_of_int memory in
  let timer   = Timer.create "timer" in
  let command =
    "wtime"::
      "-timeout"::timeout::
        match memory with
        | Some memory -> "-memory"::memory::task
        | None -> task
  in
  let aux () =
    Switch.run @@
      fun sw ->
      let out_r, out_w = Process.pipe proc_mgr ~sw in
      let err_r, err_w = Process.pipe proc_mgr ~sw in
      try
        let child = Process.spawn ~sw proc_mgr ~stdout:out_w ~stderr:err_w command in
        Flow.close out_w;
        Flow.close err_w;
        Timer.start timer;
        let output =
          (fun () ->
            let out = Buf_read.parse_exn Buf_read.take_all out_r ~max_size:max_int in
            let err = Buf_read.parse_exn Buf_read.take_all err_r ~max_size:max_int in
            Flow.close out_r;
            Flow.close err_r;
            match Process.await child with
            | `Exited 0 ->
               begin
                 match massage_out out, massage_err err with
                 | `Unsat, `Times times ->
                    write_cs log command "unsat";
                    `Answer(Unsat,times)
                 | `Sat, `Times times   ->
                    write_cs log command "sat";
                    `Answer(Sat,  times)
                 | (`Crash(`UnreadableOut, _) as r), _   ->
                    write_coe log command "Crash: Unreadable out" out err;
                    r
                 | _, (`Crash(`UnreadableTimes, _) as r) ->
                    write_coe log command "Crash: Unreadable err as times" out err;
                    r
               end
            | _ -> if Float.(Timer.read timer > float_of_string timeout)
                   then (write_coe log command "Timeout" out err; `Timeout)
                   else (write_coe log command "Memout" out err; `Memout)
          )();
        in
        Timer.stop timer;
        output
      with Exn.Io _ as ex ->
        let bt = Printexc.get_raw_backtrace () in
        Exn.reraise_with_context ex bt "running command: %a" Process.pp_args task
  in
  match aux() with
  | output -> output
  | exception (Cancel.Cancelled _) ->
     write_cs log command "Cancelled";
     `Cancelled
  | exception exc ->
     let s = "Exception: "^ Printexc.to_string exc in
     write_cs log command s;
     `Crash(`Exception, s)

let bar ~generations total =
  let open Progress in
  let open Line in
  let ( +++ ) a b = a ++ const " " ++ const " " ++ b in
  let totaltotal =
    if generations > 0 then generations * total else total
  in
  let total_string = string_of_int total in
  let width = String.length total_string in
  let pp = Printer.using ~f:(fun c -> c mod total) (Printer.int ~width) in
  let count = sum ~pp ~width () in
  let pp   = Printer.using ~f:(fun c -> if c/total > 99 then ">" else " ") (Printer.string ~width:1) in
  let more = sum ~pp ~width () in
  let pp   = Printer.using ~f:(fun c -> c/total) (Printer.int ~width:2) in
  let gen  = sum ~pp ~width () in
  let style = `Custom(Bar_style.(with_color (Color.ansi `cyan) utf8)) in
  elapsed()
  ++ (parens @@ const "eta" ++ spinner () ++ eta totaltotal)
  ++ bar ~style totaltotal
  ++ percentage_of totaltotal
  +++ (parens @@ count ++ const ("/"^total_string))
  ++ more
  ++ if generations = 1 then noop () else gen ++ const "gen"
  
exception Stop
  
let run protect ~clock ~generations ~interrupted ~nb_jobs input_seq output_stream =
  try
    let counter = ref 0 in
    let generation = ref 1 in
    Progress.with_reporter (bar ~generations nb_jobs) @@
      fun update_progress ->
      let aux task =
        let r = task () |> Stream.add output_stream in
        incr counter;
        if Int.equal !counter nb_jobs then (counter := 0; incr generation);
        update_progress 1;
        r
      in
      Switch.run @@
        fun sw ->
        (* Daemon to make the clock move in the terminal even when nothing else moves *)
        Fiber.fork_daemon ~sw @@
          (fun () -> 
            while true do Time.sleep clock 1.0; update_progress 0 done;
            Fiber.await_cancel());
        (* Daemon to await user's ctr-C *)
        Fiber.fork_daemon ~sw @@
          (fun () ->
            Eio.Condition.await_no_mutex interrupted;
            traceln "Cancelled at user's request.";
            Switch.fail sw Stop;
            `Stop_daemon);
        (* Scheduling the actual tasks *)
        let rec iter l () =
          match l () with
          | Seq.Nil -> ()
          | Seq.Cons (x, l') ->
             Fiber.fork ~sw @@ protect (iter l');
             aux x
        in
        protect (iter input_seq) ()
  with Stop -> ()

let run_semaphore ~clock ~generations ~interrupted ~cores ~nb_jobs input_seq output_stream =
  (* traceln "Created semaphore"; *)
  let semaphore = Semaphore.make cores in
  let protect task () =
    Semaphore.acquire semaphore;
    (* traceln "acquired"; *)
    let answer = task() in
    Semaphore.release semaphore;
    (* traceln "released"; *)
    answer
  in
  run protect ~clock ~generations ~interrupted ~nb_jobs input_seq output_stream

(* let run_resources ~resources ~total input_seq output_stream = *)
(*   let aux () = *)
(*     let cores = List.length resources in *)
(*     let local = ref resources in *)
(*     let get_next () = *)
(*       let hd,tl = List.hd_tl !local in *)
(*       local := tl; *)
(*       hd *)
(*     in *)
(*     let pool = Pool.create cores get_next in *)
(*     Pool.use pool *)
(*   in *)
(*   run aux ~total input_seq output_stream *)

let prune filename = Filename.(filename |> basename |> remove_extension)
  
let tasks proc_mgr generations ~timeout ?memory commands ~prefix ~log lines =
  let cycle =
    if generations > 0 then fun seq -> Seq.(seq |> repeat ~n:generations |> flatten) 
    else Seq.cycle
  in
  let aux ~prefix instance command () =
    let command  = Filename.(concat current_dir_name) command in
    let full_instance = Filename.concat prefix instance in
    let r = with_timing proc_mgr ~timeout ?memory ~log [command; full_instance] in
    prune command, instance, r
  in
  let aux instance = Seq.of_list commands |> Seq.map (aux ~prefix instance) in
  let aux          = Seq.of_list lines |> Seq.map aux |> Seq.flatten in 
  cycle aux

let rec rename ~sep src_path dir basename i suffix =
  let path = Path.(dir / (basename ^ sep ^ string_of_int i ^ suffix)) in
  try
    if CCIO.File.exists (Path.native_exn path)
    then rename ~sep path dir basename (i+1) suffix;
    Path.rename src_path path
  with _ -> ()

let rename_if_exists dir basename suffix =
  let path = Path.(dir / (basename ^ suffix)) in
  try
    if CCIO.File.exists (Path.native_exn path)
    then rename ~sep:"-" path dir basename 0 suffix
  with _ -> ()

let rename_sheet_if_exists dir =
  let path = Path.(dir / "sheet3.xml") in
  try
    if CCIO.File.exists (Path.native_exn path)
    then rename ~sep:"" path dir "sheet" 4 ".xml"
  with _ -> ()

let use_dir ~pwd ~dir commands =
  let out_path = Path.(pwd / dir) in
  begin
    try Path.mkdir ~perm:0o750 out_path
    with _ ->
      (* If path already exists, rename relevant files in it that would be overwritten *)
      (rename_if_exists out_path "log" "";
       List.iter
         (fun command -> rename_if_exists out_path (prune command) ".csv")
         commands)
  end;
  out_path

let rec drain_stream f stream =
  if Fiber.is_cancelled ()
  then (* Emptying the stream and stop *)
    match Stream.take_nonblocking stream with
    | Some t -> f t; drain_stream f stream
    | None -> ()
  else (* Consuming the stream while waiting for tokens *)
    let t = Stream.take stream in
    f t;
    drain_stream f stream

module HStrings = CCHashtbl.Make(String)

(* Pouring the stream into hashtables *)
let stream_to_hashtables ~htbl ~nb_benchmarks stream =
  let add_command_out command bench (msg : [ output | `Cancelled ]) =
    match msg with
    | `Cancelled -> ()
    | #output as msg ->
       let command_tbl = HStrings.(get_or_add htbl ~f:(fun _ -> create nb_benchmarks) ~k:command) in
       let value =
         match HStrings.get command_tbl bench with
         | None -> 1, (msg :> [ output | `Inconsistent ])
         | Some(n, previous) ->
            n+1,
            match previous, msg with
            | `Inconsistent, _                    -> `Inconsistent
            | `Crash msg, _    | _, `Crash msg    -> `Crash msg
            | `Timeout, _      | _, `Timeout      -> `Timeout
            | `Memout, _       | _, `Memout       -> `Memout
            | `Answer(a, t1), `Answer(b, t2) ->
               if equal_answer a b
               then `Answer(a, { wall   = t1.wall   +. t2.wall;
                                 user   = t1.user   +. t2.user;
                                 system = t1.system +. t2.system })
               else `Inconsistent
       in
       HStrings.replace command_tbl bench value
  in
  drain_stream (function (command, bench, msg) -> add_command_out command bench msg) stream

let average = function
  | instance,(n, `Answer(a,{wall; user; system})) ->
     let n = float_of_int n in
     instance, `Answer(a,{wall = wall /. n; user = user /. n; system = system /. n})
  | instance,(_, a) -> instance, a

(* Pouring the hashtables into files *)
let hashtables_to_files out_path htbl sort =
  let out_seq = HStrings.to_seq htbl in
  let process (command, res) =
    let pp_pair fmt (instance,res) =
      let wtimes s {wall;user;system} = 
        Format.fprintf fmt "@[%s\t\"%s\"\t%s\t%f\t%f\t%f@]@,"
          command instance s wall user system
      in
      let wotimes s = Format.fprintf fmt "@[%s\t\"%s\"\t%s@]@," command instance s in
      match res with
      | `Answer(Unsat, times) -> wtimes "unsat" times
      | `Answer(Sat,   times) -> wtimes "sat" times
      | `Inconsistent -> wotimes "inconsistent"
      | `Timeout      -> wotimes "timeout"
      | `Memout       -> wotimes "memout"
      | `Crash _msg   -> wotimes "crash"
    in
    let pp_htable fmt command_tbl =
      let l = HStrings.to_list command_tbl
              |> List.map average
              |> List.sort sort
      in
      l |> List.iter (pp_pair fmt)
    in
    Switch.run @@
      fun sw ->
      let out =
        Path.(open_out ~sw ~create:(`Exclusive 0o640) (out_path / (command ^ ".csv")))
      in
      Buf_write.(with_flow out @@fun b -> string b @@ Format.sprintf "%a" pp_htable res)
  in
  Seq.iter process out_seq

(* Pouring the hashtables into XML files *)
let hashtables_to_xml out_path htbl sort =
  let out_seq = HStrings.to_seq htbl in
  let process (command, res) =
    let project (instance, r) =
      match r with
      | `Answer(Unsat, {wall; user; system}) ->
         command, instance, "unsat", Some(wall, user, system)
      | `Answer(Sat,   {wall; user; system}) ->
         command, instance, "sat", Some(wall, user, system)
      | `Inconsistent -> command, instance, "inconsistent", None
      | `Timeout      -> command, instance, "timeout", None
      | `Memout       -> command, instance, "memout", None
      | `Crash _msg   -> command, instance, "crash", None
    in
    let pp_htable fmt command_tbl =
      let l = HStrings.to_list command_tbl
              |> List.map average
              |> List.sort sort
              |> List.map project
      in
      Excel.pp_table fmt l
    in
    rename_sheet_if_exists out_path;
    let filename = "sheet3.xml" in
    Switch.run @@
      fun sw ->
      let out =
        Path.(open_out ~sw ~create:(`Exclusive 0o640) (out_path / filename))
      in
      Buf_write.(with_flow out @@fun b -> string b @@ Format.sprintf "%a" pp_htable res)
  in
  Seq.iter process out_seq


let catch_sig signal_code =
  let condition = Eio.Condition.create () in
  let handle (_signum : int) = Eio.Condition.broadcast condition in
  Sys.signal signal_code (Signal_handle handle), condition

let args    = ref []
let cores   = ref 1
let timeout = ref 300
let memory  = ref None
let sort    = ref (fun (i1,_) (i2,_) -> String.compare_natural i1 i2)
let generations = ref 1
let xml = ref false
let description = "Multitask runner.
                   First argument: ASCII file containing list of benchmarks (1 per line)
                   Next arguments: Executables to process each of the benchmarks"

let cmp_wall { wall = w1; _} { wall = w2; _} = Float.compare w1 w2
let cmp_user { user = u1; _} { user = u2; _} = Float.compare u1 u2

let cmp cmp_times (i1,r1) (i2,r2) =
  match r1, r2 with
  | `Answer(_,t1), `Answer(_,t2) -> Ord.pair cmp_times String.compare (t1,i1) (t2,i2)
  | `Answer _, _ -> -1
  | _, `Answer _ -> +1
  | _ -> String.compare i1 i2

let cmp_wall = cmp cmp_wall
let cmp_user = cmp cmp_user
    
let parse_sort = function
  | "wall" -> sort := cmp_wall
  | "user" -> sort := cmp_user
  | _ -> failwith "timesort requires argument \"wall\" or \"user\""

let options = [
    ("-cores",   Arg.Int(fun u -> cores := u), "Number of cores (default is 1)");
    ("-timeout", Arg.Int(fun u -> timeout := u), "Timeout for each task in seconds (default is 300)");
    ("-memory",  Arg.Int(fun u -> memory  := Some u), "Maximum memory in Gb (default is Infinity)");
    ("-timesort",  Arg.String parse_sort, "Sort output by increasing solve time, with argument \"wall\" or \"user\" (default is sort alphabetically)");
    ("-generations", Arg.Int(fun u -> generations := u), "Number of generations (default is 1); use 0 for infinite (stop the runs by sending SIGINT signal, usually with Ctr-C)");
    ("-xml", Arg.Set xml, "Also output in xml for xlxs (default is false)");
  ];;

Arg.parse options (fun a->args := a::!args) description;;

match List.rev !args with
| benchmark_file::commands ->
   print_endline (Format.sprintf "PID is %i" (Unix.getpid()));
   let cores            = !cores in
   let timeout          = !timeout in
   let memory           = !memory in
   let prefix           = Filename.dirname benchmark_file in
   let lines            = CCIO.(with_in benchmark_file read_lines_l) in
   let nb_benchmarks    = List.length lines in
   let nb_solvers       = List.length commands in
   let nb_jobs          = nb_benchmarks * nb_solvers in
   let benchmark_digest =
     let filename = prune benchmark_file in
     match memory with
     | Some m ->
        Format.sprintf "%s-%i-t%i-m%i"
          filename
          (Hash.list Hash.string lines)
          timeout
          m
     | None ->
        Format.sprintf "%s-%i-t%i"
          filename
          (Hash.list Hash.string lines)
          timeout
   in
   Eio_main.run @@
     fun env ->

     (* Handling interruption signal and usr1 signal *)
     let original_interrupt, interrupted = catch_sig Sys.sigint in
     let original_usr1, usr1             = catch_sig Sys.sigusr1 in

     (* Environment utilities and input/output paths/files *)
     let out_path = use_dir ~pwd:(Stdenv.fs env) ~dir:benchmark_digest commands in
     
     (* Defining the jobs to do, creating a hashtable to store the results *)
     let proc_mgr = Stdenv.process_mgr env in
     let clock    = Stdenv.clock env in
     let htbl     = HStrings.create nb_solvers in

     Switch.run @@
       (fun sw ->
         let log_file = Path.(open_out ~sw ~create:(`Exclusive 0o640) (out_path / "log")) in
         let log   = Stream.create max_int in
         let tasks =
           tasks proc_mgr !generations ~timeout ?memory ~prefix ~log commands lines
         in
         Fiber.fork_daemon ~sw @@ (* Daemon to drain log into file *)
           (fun () ->
             Buf_write.(with_flow log_file @@fun b -> drain_stream (string b) log);
             `Stop_daemon);
         Fiber.fork_daemon ~sw @@ (* Daemon to await user signal1 *)
           (fun () ->
             let i = ref 0 in
             while true do
               Eio.Condition.await_no_mutex usr1;
               traceln "Pouring current state into files";
               let out_path =
                 use_dir ~pwd:(Stdenv.fs env)
                   ~dir:(benchmark_digest^"-snapshot_"^string_of_int !i)
                   commands
               in
               hashtables_to_files out_path htbl !sort;
               incr i;
             done;
             `Stop_daemon);
         Fiber.fork ~sw @@
           fun () ->
           let stream = Stream.create max_int in
           Fiber.first
             (* Processing the jobs, putting the results into the stream *)
             (fun () ->
               run_semaphore
                 ~clock
                 ~generations:!generations
                 ~interrupted
                 ~cores
                 ~nb_jobs
                 tasks
                 stream)
             (* Concurrently pouring the stream into hashtables *)
             (fun () -> stream_to_hashtables ~htbl ~nb_benchmarks stream)
       );

     (* Restoring original handling of interruption signal *)
     Sys.set_signal Sys.sigint  original_interrupt;
     Sys.set_signal Sys.sigusr1 original_usr1;

     (* Pouring the hashtables into files *)
     hashtables_to_files out_path htbl !sort;
     if !xml then  hashtables_to_xml out_path htbl cmp_user;
     traceln "DONE!"
       
| _ -> print_endline(Arg.usage_string options "Usage is: benchmark [benchmark_file] [command]")
