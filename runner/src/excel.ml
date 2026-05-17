let escape_html s =
  let b = Buffer.create (String.length s) in
  String.iter
    (function
      | '&' -> Buffer.add_string b "&amp;"
      | '<' -> Buffer.add_string b "&lt;"
      | '>' -> Buffer.add_string b "&gt;"
      | '"' -> Buffer.add_string b "&quot;"
      | '\'' -> Buffer.add_string b "&#39;"
      | c -> Buffer.add_char b c)
    s;
  Buffer.contents b

let pp_cell fmt s =
  Format.fprintf fmt "<td>%s</td>" (escape_html s)

let pp_optional_float fmt = function
  | Some f -> Format.fprintf fmt "<td>%.6f</td>" f
  | None -> Format.fprintf fmt "<td></td>"

let pp_line fmt (solver, bench, result, times) =
  let wall, user, system =
    match times with
    | Some (wall, user, system) -> Some wall, Some user, Some system
    | None -> None, None, None
  in
  Format.fprintf fmt
    "<tr>%a%a%a%a%a%a</tr>@."
    pp_cell solver
    pp_cell bench
    pp_cell result
    pp_optional_float wall
    pp_optional_float user
    pp_optional_float system

let pp_table fmt rows =
  Format.fprintf fmt
    "<html><head><meta charset=\"utf-8\"></head><body>@.\
     <table border=\"1\">@.\
     <tr><th>Solver</th><th>Benchmark</th><th>Result</th><th>Wall</th><th>User</th><th>System</th></tr>@.\
     %a\
     </table></body></html>@."
    (fun fmt -> List.iter (pp_line fmt))
    rows
