open Containers

let timeout = ref None
let memory  = ref None
let args = ref []
let description = "Timeout implem";;

let options = [
    ("-timeout", Arg.Int(fun u -> timeout := Some u), "Timeout for each task (default is none)");
    ("-memory",  Arg.Int(fun u -> memory  := Some(1073741824 * u)), "Maximum memory in Gb (default is none)");
  ];;

Arg.parse options (fun a->args := a::!args) description;;

match List.rev !args with
| prog::argv ->
   let open Core_unix.RLimit in
   let set_timeout timeout =
     let limit = get cpu_seconds in
     let cur = Limit(Int64.of_string_exn timeout) in
     set cpu_seconds { limit with cur }
   in
   Option.iter set_timeout (Option.map string_of_int !timeout);
   let set_memory memory =
     let vm = Core.Or_error.ok_exn virtual_memory in
     let limit = get vm in
     let cur = Limit(Int64.of_string_exn memory) in
     set vm { limit with cur }
   in
   Option.iter set_memory (Option.map string_of_int !memory);
   let open Shexp_process in
   let open Infix in
   let task = spawn prog argv >>= wait in
   let wall_timer = Timer.create "wall time" in
   Timer.start wall_timer;
   let i = eval task in
   let wall_time = Timer.read wall_timer in
   let times = Core_unix.times() in
   (match i with
    | Exited 0 ->
       Format.(fprintf stderr) "@[<v>%f@,%f@,%f@]"
         wall_time times.tms_cutime times.tms_cstime;
       exit 0
    | _        -> exit 1)

| _ -> print_endline(Arg.usage_string options "Usage is: wtime [prog] [args]")
