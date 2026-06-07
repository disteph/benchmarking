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

let buffer_to_string pp value =
  let b = Buffer.create 4096 in
  let fmt = Format.formatter_of_buffer b in
  pp fmt value;
  Format.pp_print_flush fmt ();
  Buffer.contents b

let column_name n =
  if n < 1 then invalid_arg "column index must be positive";
  let rec loop n acc =
    if n = 0 then acc
    else
      let n = n - 1 in
      let c = Char.chr (Char.code 'A' + (n mod 26)) in
      loop (n / 26) (String.make 1 c :: acc)
  in
  String.concat "" (loop n [])

let cell_ref col row = column_name col ^ string_of_int row

let pp_inline_string_cell fmt ~row ~col s =
  Format.fprintf fmt "<c r=\"%s\" t=\"inlineStr\"><is><t>%s</t></is></c>"
    (cell_ref col row) (escape_xml s)

let pp_number_cell fmt ~row ~col f =
  Format.fprintf fmt "<c r=\"%s\"><v>%.6f</v></c>" (cell_ref col row) f

let pp_formula_cell fmt ~row ~col formula =
  Format.fprintf fmt "<c r=\"%s\"><f>%s</f></c>" (cell_ref col row)
    (escape_xml formula)

let pp_row fmt ~row cells =
  Format.fprintf fmt "<row r=\"%d\">" row;
  List.iteri (fun i pp_cell -> pp_cell fmt ~row ~col:(i + 1)) cells;
  Format.fprintf fmt "</row>@."

let string_cell value fmt ~row ~col = pp_inline_string_cell fmt ~row ~col value
let number_cell value fmt ~row ~col = pp_number_cell fmt ~row ~col value
let formula_cell value fmt ~row ~col = pp_formula_cell fmt ~row ~col value

let pp_worksheet fmt pp_rows =
  Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
     <worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">@.\
     <sheetData>@.";
  pp_rows fmt;
  Format.fprintf fmt "</sheetData>@.</worksheet>@."

let pp_totals_header fmt =
  pp_row fmt ~row:1
    [
      string_cell "Solver";
      string_cell "unsat";
      string_cell "time unsat";
      string_cell "sat";
      string_cell "time sat";
      string_cell "all";
      string_cell "time all";
    ]

let pp_totals_row fmt row sheet =
  let sheet = sheet.name in
  pp_row fmt ~row
    [
      formula_cell (sheet ^ "!A1");
      formula_cell ("COUNTIF(" ^ sheet ^ "!C:C,\"unsat\")");
      formula_cell ("SUMIF(" ^ sheet ^ "!C:C,\"unsat\"," ^ sheet ^ "!E:E)");
      formula_cell ("COUNTIF(" ^ sheet ^ "!C:C,\"sat\")");
      formula_cell ("SUMIF(" ^ sheet ^ "!C:C,\"sat\"," ^ sheet ^ "!E:E)");
      formula_cell ("B" ^ string_of_int row ^ "+D" ^ string_of_int row);
      formula_cell ("C" ^ string_of_int row ^ "+E" ^ string_of_int row);
    ]

let totals_worksheet sheets =
  buffer_to_string
    (fun fmt () ->
      pp_worksheet fmt (fun fmt ->
          pp_totals_header fmt;
          List.iteri (fun i sheet -> pp_totals_row fmt (i + 2) sheet) sheets))
    ()

let solver_name_of_sheet sheet =
  match sheet.rows with
  | (solver, _, _, _) :: _ -> solver
  | [] -> sheet.name

let pp_clash_header fmt sheets =
  pp_row fmt ~row:1
    (string_cell "Benchmark" :: List.map (fun sheet -> string_cell (solver_name_of_sheet sheet)) sheets)

let pp_clash_row fmt row clash =
  pp_row fmt ~row
    (string_cell clash.benchmark :: List.map (fun output -> string_cell output) clash.outputs)

let clashes_worksheet sheets clashes =
  buffer_to_string
    (fun fmt () ->
      pp_worksheet fmt (fun fmt ->
          pp_clash_header fmt sheets;
          List.iteri (fun i clash -> pp_clash_row fmt (i + 2) clash) clashes))
    ()

let pp_solver_row fmt row (solver, bench, result, times) =
  let cells =
    [
      string_cell solver;
      string_cell bench;
      string_cell result;
    ]
    @
    match times with
    | Some (wall, user, system) -> [ number_cell wall; number_cell user; number_cell system ]
    | None -> []
  in
  pp_row fmt ~row cells

let solver_worksheet sheet =
  buffer_to_string
    (fun fmt () ->
      pp_worksheet fmt (fun fmt ->
          List.iteri (fun i row -> pp_solver_row fmt (i + 1) row) sheet.rows))
    ()

let workbook_sheets sheets =
  { name = "Totals"; rows = [] } :: { name = "Clashes"; rows = [] } :: sheets

let pp_content_types fmt worksheet_count =
  Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
     <Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">@.\
     <Default Extension=\"rels\" \
     ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>@.\
     <Default Extension=\"xml\" ContentType=\"application/xml\"/>@.\
     <Override PartName=\"/xl/workbook.xml\" \
     ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>@.";
  for i = 1 to worksheet_count do
    Format.fprintf fmt
      "<Override PartName=\"/xl/worksheets/sheet%d.xml\" \
       ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>@."
      i
  done;
  Format.fprintf fmt "</Types>@."

let content_types worksheet_count = buffer_to_string pp_content_types worksheet_count

let root_relationships =
  "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
   <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n\
   <Relationship Id=\"rId1\" \
   Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" \
   Target=\"xl/workbook.xml\"/>\n\
   </Relationships>\n"

let pp_workbook fmt sheets =
  let workbook_sheets = workbook_sheets sheets in
  Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
     <workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" \
     xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">@.\
     <sheets>@.";
  List.iteri
    (fun i sheet ->
      let n = i + 1 in
      Format.fprintf fmt "<sheet name=\"%s\" sheetId=\"%d\" r:id=\"rId%d\"/>@."
        (escape_xml sheet.name) n n)
    workbook_sheets;
  Format.fprintf fmt
    "</sheets>@.\
     <calcPr calcId=\"0\" fullCalcOnLoad=\"1\" forceFullCalc=\"1\"/>@.\
     </workbook>@."

let workbook sheets = buffer_to_string pp_workbook sheets

let pp_workbook_relationships fmt worksheet_count =
  Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
     <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">@.";
  for i = 1 to worksheet_count do
    Format.fprintf fmt
      "<Relationship Id=\"rId%d\" \
       Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" \
       Target=\"worksheets/sheet%d.xml\"/>@."
      i i
  done;
  Format.fprintf fmt "</Relationships>@."

let workbook_relationships worksheet_count =
  buffer_to_string pp_workbook_relationships worksheet_count

let workbook_entries sheets clashes =
  let worksheets =
    [ totals_worksheet sheets; clashes_worksheet sheets clashes ]
    @ List.map solver_worksheet sheets
  in
  let worksheet_count = List.length worksheets in
  [
    ("[Content_Types].xml", content_types worksheet_count);
    ("_rels/.rels", root_relationships);
    ("xl/workbook.xml", workbook sheets);
    ("xl/_rels/workbook.xml.rels", workbook_relationships worksheet_count);
  ]
  @ List.mapi
      (fun i worksheet -> (Format.sprintf "xl/worksheets/sheet%d.xml" (i + 1), worksheet))
      worksheets

let write_workbook filename (sheets, clashes) =
  let zf = Zip.open_out filename in
  Fun.protect
    ~finally:(fun () -> Zip.close_out zf)
    (fun () ->
      List.iter
        (fun (name, contents) -> Zip.add_entry contents zf name)
        (workbook_entries sheets clashes))
