type row = string * string * string * (float * float * float) option
type sheet = { name : string; rows : row list }
type clash = { benchmark : string; outputs : string list }

let escape_xml s =
  let b = Buffer.create (String.length s) in
  String.iter
    (function
      | '&' -> Buffer.add_string b "&amp;"
      | '<' -> Buffer.add_string b "&lt;"
      | '>' -> Buffer.add_string b "&gt;"
      | '"' -> Buffer.add_string b "&quot;"
      | '\'' -> Buffer.add_string b "&apos;"
      | c -> Buffer.add_char b c)
    s;
  Buffer.contents b

let pp_string_cell fmt s =
  Format.fprintf fmt "<Cell><Data ss:Type=\"String\">%s</Data></Cell>" (escape_xml s)

let pp_number_cell fmt f =
  Format.fprintf fmt "<Cell><Data ss:Type=\"Number\">%.6f</Data></Cell>" f

let pp_formula_cell fmt formula =
  Format.fprintf fmt "<Cell ss:Formula=\"%s\"/>" (escape_xml formula)

let pp_solver_row fmt (solver, bench, result, times) =
  Format.fprintf fmt "<Row>%a%a%a" pp_string_cell solver pp_string_cell bench pp_string_cell
    result;
  (match times with
  | Some (wall, user, system) ->
      Format.fprintf fmt "%a%a%a" pp_number_cell wall pp_number_cell user pp_number_cell system
  | None -> ());
  Format.fprintf fmt "</Row>@."

let pp_totals_header fmt =
  Format.fprintf fmt "<Row>%a%a%a%a%a%a%a</Row>@." pp_string_cell "Solver" pp_string_cell
    "unsat" pp_string_cell "time unsat" pp_string_cell "sat" pp_string_cell "time sat"
    pp_string_cell "all" pp_string_cell "time all"

let pp_totals_row fmt row sheet =
  let sheet = sheet.name in
  Format.fprintf fmt
    "<Row>%a%a%a%a%a%a%a</Row>@."
    pp_formula_cell ("=" ^ sheet ^ "!A1")
    pp_formula_cell ("=COUNTIF(" ^ sheet ^ "!C:C,\"unsat\")")
    pp_formula_cell ("=SUMIF(" ^ sheet ^ "!C:C,\"unsat\"," ^ sheet ^ "!E:E)")
    pp_formula_cell ("=COUNTIF(" ^ sheet ^ "!C:C,\"sat\")")
    pp_formula_cell ("=SUMIF(" ^ sheet ^ "!C:C,\"sat\"," ^ sheet ^ "!E:E)")
    pp_formula_cell ("=B" ^ string_of_int row ^ "+D" ^ string_of_int row)
    pp_formula_cell ("=C" ^ string_of_int row ^ "+E" ^ string_of_int row)

let pp_totals_sheet fmt sheets =
  Format.fprintf fmt "<Worksheet ss:Name=\"Totals\"><Table>@.";
  pp_totals_header fmt;
  List.iteri (fun i sheet -> pp_totals_row fmt (i + 2) sheet) sheets;
  Format.fprintf fmt "</Table></Worksheet>@."

let pp_clash_header fmt sheets =
  Format.fprintf fmt "<Row>%a" pp_string_cell "Benchmark";
  List.iter
    (fun sheet ->
      match sheet.rows with
      | (solver, _, _, _) :: _ -> Format.fprintf fmt "%a" pp_string_cell solver
      | [] -> Format.fprintf fmt "%a" pp_string_cell sheet.name)
    sheets;
  Format.fprintf fmt "</Row>@."

let pp_clash_row fmt clash =
  Format.fprintf fmt "<Row>%a" pp_string_cell clash.benchmark;
  List.iter (fun output -> Format.fprintf fmt "%a" pp_string_cell output) clash.outputs;
  Format.fprintf fmt "</Row>@."

let pp_clashes_sheet fmt sheets clashes =
  Format.fprintf fmt "<Worksheet ss:Name=\"Clashes\"><Table>@.";
  pp_clash_header fmt sheets;
  List.iter (pp_clash_row fmt) clashes;
  Format.fprintf fmt "</Table></Worksheet>@."

let pp_solver_sheet fmt sheet =
  Format.fprintf fmt "<Worksheet ss:Name=\"%s\"><Table>@." (escape_xml sheet.name);
  List.iter (pp_solver_row fmt) sheet.rows;
  Format.fprintf fmt "</Table></Worksheet>@."

let pp_workbook fmt (sheets, clashes) =
  Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>@.\
     <?mso-application progid=\"Excel.Sheet\"?>@.\
     <Workbook xmlns=\"urn:schemas-microsoft-com:office:spreadsheet\" \
     xmlns:o=\"urn:schemas-microsoft-com:office:office\" \
     xmlns:x=\"urn:schemas-microsoft-com:office:excel\" \
     xmlns:ss=\"urn:schemas-microsoft-com:office:spreadsheet\">@.";
  pp_totals_sheet fmt sheets;
  pp_clashes_sheet fmt sheets clashes;
  List.iter (pp_solver_sheet fmt) sheets;
  Format.fprintf fmt "</Workbook>@."
