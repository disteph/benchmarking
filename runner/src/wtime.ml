let timeout = ref None
let memory  = ref None
let args = ref []

let options = [
    ("-timeout", Arg.Int(fun u -> timeout := Some u), "Timeout for each task (default is none)");
    ("-memory",  Arg.Int(fun u -> memory  := Some(1073741824 * u)), "Maximum memory in Gb (default is none)");
  ];;

let usage () = Arg.usage options "Usage is: wtime [options] [prog] [args]" 

let set_args_from argv i =
  let len = Array.length argv in
  args := List.init (len - i) (fun j -> argv.(i + j))

let rec parse_args argv i =
  if i >= Array.length argv then ()
  else
    match argv.(i) with
    | "-timeout" ->
       if i + 1 >= Array.length argv then (usage (); exit 2);
       timeout := Some(int_of_string argv.(i + 1));
       parse_args argv (i + 2)
    | "-memory" ->
       if i + 1 >= Array.length argv then (usage (); exit 2);
       memory := Some(1073741824 * int_of_string argv.(i + 1));
       parse_args argv (i + 2)
    | "-help" | "--help" ->
       usage ();
       exit 0
    | "--" ->
       set_args_from argv (i + 1)
    | arg when String.length arg > 0 && Char.equal arg.[0] '-' ->
       Format.eprintf "wtime: unknown option %s@." arg;
       usage ();
       exit 2
    | _ ->
       set_args_from argv i

let () = parse_args Sys.argv 1

let set_cpu_limit seconds =
  let open Core_unix.RLimit in
  let limit = get cpu_seconds in
  set cpu_seconds { limit with cur = Limit(Int64.of_int seconds) }

let set_memory_limit bytes =
  let open Core_unix.RLimit in
  let vm = Core.Or_error.ok_exn virtual_memory in
  let limit = get vm in
  set vm { limit with cur = Limit(Int64.of_int bytes) }

let status_code = function
  | Unix.WEXITED n -> n
  | Unix.WSIGNALED s -> if s = Sys.sigxcpu then 124 else 128 + abs s
  | Unix.WSTOPPED s -> 128 + abs s

let kill_child_group pid =
  try Unix.kill (-pid) Sys.sigkill
  with Unix.Unix_error _ ->
    try Unix.kill pid Sys.sigkill with Unix.Unix_error _ -> ()

let wait_with_timeout pid timeout =
  let deadline = Option.map (fun seconds -> Unix.gettimeofday () +. float seconds) timeout in
  let rec loop () =
    match Unix.waitpid [Unix.WNOHANG] pid with
    | 0, _ ->
       begin
         match deadline with
         | Some deadline when Unix.gettimeofday () >= deadline ->
            kill_child_group pid;
            let _, _ = Unix.waitpid [] pid in
            124
         | _ ->
            Unix.sleepf 0.01;
            loop ()
       end
    | _, status -> status_code status
  in
  loop ()

let print_times started before =
  let wall_time = Timer.read started in
  let after = Unix.times () in
  Format.eprintf "@\n@[<v>%f@,%f@,%f@]@."
    wall_time
    (after.Unix.tms_cutime -. before.Unix.tms_cutime)
    (after.Unix.tms_cstime -. before.Unix.tms_cstime)

let () =
match !args with
| prog::argv ->
   let wall_timer = Timer.create "wall time" in
   let before = Unix.times () in
   Timer.start wall_timer;
   let child = Unix.fork () in
   if child = 0 then
     begin
       try
         Option.iter set_cpu_limit !timeout;
         Option.iter set_memory_limit !memory;
         ignore (Unix.setsid ());
         Unix.execvp prog (Array.of_list (prog :: argv))
       with exn ->
         Format.eprintf "wtime: could not execute %s: %s@." prog (Printexc.to_string exn);
         Unix._exit 127
     end
   else
     let code = wait_with_timeout child !timeout in
     print_times wall_timer before;
     exit code

| _ -> usage ()
