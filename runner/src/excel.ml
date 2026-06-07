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

let pp_sparse_row fmt ~row cells =
  if cells <> [] then (
    Format.fprintf fmt "<row r=\"%d\">" row;
    List.iter (fun (col, pp_cell) -> pp_cell fmt ~row ~col) cells;
    Format.fprintf fmt "</row>@.")

let string_cell value fmt ~row ~col = pp_inline_string_cell fmt ~row ~col value
let number_cell value fmt ~row ~col = pp_number_cell fmt ~row ~col value
let formula_cell value fmt ~row ~col = pp_formula_cell fmt ~row ~col value

let pp_worksheet ?drawing ?hidden_columns fmt pp_rows =
  let rels =
    match drawing with
    | None -> ""
    | Some _ -> " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\""
  in
  (Format.fprintf fmt
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
     <worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"%s>@.\
     "
     rels);
  Option.iter
    (fun (first_col, last_col) ->
      Format.fprintf fmt
        "<cols><col min=\"%d\" max=\"%d\" hidden=\"1\" width=\"10\" customWidth=\"1\"/></cols>@."
        first_col last_col)
    hidden_columns;
  Format.fprintf fmt "<sheetData>@.";
  pp_rows fmt;
  Format.fprintf fmt "</sheetData>@.";
  Option.iter (Format.fprintf fmt "<drawing r:id=\"%s\"/>@.") drawing;
  Format.fprintf fmt "</worksheet>@."

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

let plot_sheet_name = "Plots"
let plot_helper_start_col = 24

type plot_series = {
  series_name : string;
  series_col : int;
  series_values : float array;
}

type plot_chart = {
  chart_index : int;
  chart_title : string;
  chart_series : plot_series list;
}

let row_user_time result_filter (_, _, result, times) =
  match times, result_filter with
  | Some (_, user, _), None -> Some user
  | Some (_, user, _), Some expected when String.equal result expected -> Some user
  | _ -> None

let make_plot_charts sheets =
  let specs =
    [
      ("SAT+UNSAT solving time", None);
      ("UNSAT solving time", Some "unsat");
      ("SAT solving time", Some "sat");
    ]
  in
  let solver_count = List.length sheets in
  specs
  |> List.mapi (fun chart_i (chart_title, result_filter) ->
         let chart_series =
           sheets
           |> List.mapi (fun sheet_i sheet ->
                  let values =
                    sheet.rows |> List.filter_map (row_user_time result_filter) |> Array.of_list
                  in
                  {
                    series_name = solver_name_of_sheet sheet;
                    series_col = plot_helper_start_col + (chart_i * solver_count) + sheet_i;
                    series_values = values;
                  })
         in
         { chart_index = chart_i + 1; chart_title; chart_series })

let all_plot_series charts = List.concat_map (fun chart -> chart.chart_series) charts

let plot_helper_columns charts =
  match all_plot_series charts with
  | [] -> None
  | series ->
      let last_col =
        List.fold_left (fun max_col series -> max max_col series.series_col) 0 series
      in
      Some (plot_helper_start_col, last_col)

let max_plot_rows charts =
  all_plot_series charts
  |> List.fold_left
       (fun max_rows series -> max max_rows (Array.length series.series_values))
       0

let pp_plot_rows fmt charts =
  let series = all_plot_series charts in
  pp_sparse_row fmt ~row:1
    ((1, string_cell "Solving time plots")
    :: List.map (fun series -> (series.series_col, string_cell series.series_name)) series);
  for row_index = 0 to max_plot_rows charts - 1 do
    let cells =
      series
      |> List.filter_map (fun series ->
             if row_index < Array.length series.series_values then
               Some (series.series_col, number_cell series.series_values.(row_index))
             else None)
    in
    pp_sparse_row fmt ~row:(row_index + 2) cells
  done

let plot_worksheet charts =
  buffer_to_string
    (fun fmt () ->
      pp_worksheet ~drawing:"rId1" ?hidden_columns:(plot_helper_columns charts) fmt
        (fun fmt -> pp_plot_rows fmt charts))
    ()

let plot_sheet_relationships =
  "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
   <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n\
   <Relationship Id=\"rId1\" \
   Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing\" \
   Target=\"../drawings/drawing1.xml\"/>\n\
   </Relationships>\n"

let drawing_relationships charts =
  buffer_to_string
    (fun fmt () ->
      Format.fprintf fmt
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
         <Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">@.";
      List.iter
        (fun chart ->
          Format.fprintf fmt
            "<Relationship Id=\"rId%d\" \
             Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/chart\" \
             Target=\"../charts/chart%d.xml\"/>@."
            chart.chart_index chart.chart_index)
        charts;
      Format.fprintf fmt "</Relationships>@.")
    ()

let pp_drawing_anchor fmt chart =
  let row = 1 + ((chart.chart_index - 1) * 33) in
  Format.fprintf fmt
    "<xdr:twoCellAnchor>@.\
     <xdr:from><xdr:col>0</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>%d</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:from>@.\
     <xdr:to><xdr:col>20</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>%d</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:to>@.\
     <xdr:graphicFrame macro=\"\"><xdr:nvGraphicFramePr><xdr:cNvPr id=\"%d\" name=\"%s\"/><xdr:cNvGraphicFramePr/></xdr:nvGraphicFramePr>@.\
     <xdr:xfrm><a:off x=\"0\" y=\"0\"/><a:ext cx=\"0\" cy=\"0\"/></xdr:xfrm>@.\
     <a:graphic><a:graphicData uri=\"http://schemas.openxmlformats.org/drawingml/2006/chart\"><c:chart \
     xmlns:c=\"http://schemas.openxmlformats.org/drawingml/2006/chart\" \
     xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" \
     r:id=\"rId%d\"/></a:graphicData></a:graphic>@.\
     </xdr:graphicFrame><xdr:clientData/></xdr:twoCellAnchor>@."
    row (row + 31) (chart.chart_index + 1) (escape_xml chart.chart_title)
    chart.chart_index

let drawing charts =
  buffer_to_string
    (fun fmt () ->
      Format.fprintf fmt
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
         <xdr:wsDr xmlns:xdr=\"http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing\" \
         xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\">@.";
      List.iter (pp_drawing_anchor fmt) charts;
      Format.fprintf fmt "</xdr:wsDr>@.")
    ()

let pp_chart_title fmt title =
  Format.fprintf fmt
    "<c:title><c:tx><c:rich><a:bodyPr/><a:lstStyle/><a:p><a:r><a:t>%s</a:t></a:r></a:p></c:rich></c:tx><c:layout/><c:overlay \
     val=\"0\"/></c:title>"
    (escape_xml title)

let pp_chart_series fmt i series =
  let last_row = Array.length series.series_values + 1 in
  if Array.length series.series_values > 0 then
    let col = column_name series.series_col in
    Format.fprintf fmt
      "<c:ser><c:idx val=\"%d\"/><c:order val=\"%d\"/><c:tx><c:strRef><c:f>%s!$%s$1</c:f></c:strRef></c:tx><c:marker><c:symbol \
       val=\"none\"/></c:marker><c:val><c:numRef><c:f>%s!$%s$2:$%s$%d</c:f></c:numRef></c:val><c:smooth \
       val=\"0\"/></c:ser>@."
      i i plot_sheet_name col plot_sheet_name col col last_row

let chart ?timeout plot_chart =
  let y_axis_title =
    match timeout with
    | None -> "Solving time (seconds - log scale)"
    | Some timeout -> Format.sprintf "Solving time (seconds - log scale) %ds timeout" timeout
  in
  buffer_to_string
    (fun fmt () ->
      Format.fprintf fmt
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>@.\
         <c:chartSpace xmlns:c=\"http://schemas.openxmlformats.org/drawingml/2006/chart\" \
         xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\" \
         xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">@.\
         <c:chart>@.";
      pp_chart_title fmt plot_chart.chart_title;
      Format.fprintf fmt
        "<c:plotArea><c:layout/><c:lineChart><c:grouping val=\"standard\"/><c:varyColors \
         val=\"0\"/>@.";
      plot_chart.chart_series
      |> List.filter (fun series -> Array.length series.series_values > 0)
      |> List.iteri (pp_chart_series fmt);
      Format.fprintf fmt
        "<c:axId val=\"1279802063\"/><c:axId val=\"1279794207\"/></c:lineChart>@.\
         <c:catAx><c:axId val=\"1279802063\"/><c:scaling><c:orientation \
         val=\"minMax\"/></c:scaling><c:delete val=\"0\"/><c:axPos val=\"b\"/>";
      pp_chart_title fmt "Benchmarks ranked by increasing solving time";
      Format.fprintf fmt
        "<c:numFmt formatCode=\"General\" sourceLinked=\"1\"/><c:majorTickMark \
         val=\"out\"/><c:minorTickMark val=\"none\"/><c:tickLblPos val=\"nextTo\"/><c:crossAx \
         val=\"1279794207\"/><c:crosses val=\"autoZero\"/><c:auto val=\"1\"/><c:lblAlgn \
         val=\"ctr\"/><c:lblOffset val=\"100\"/></c:catAx>@.\
         <c:valAx><c:axId val=\"1279794207\"/><c:scaling><c:logBase val=\"10\"/><c:orientation \
         val=\"minMax\"/></c:scaling><c:delete val=\"0\"/><c:axPos val=\"l\"/>";
      pp_chart_title fmt y_axis_title;
      Format.fprintf fmt
        "<c:numFmt formatCode=\"General\" sourceLinked=\"1\"/><c:majorTickMark \
         val=\"out\"/><c:minorTickMark val=\"none\"/><c:tickLblPos val=\"nextTo\"/><c:crossAx \
         val=\"1279802063\"/><c:crosses val=\"autoZero\"/><c:crossBetween \
         val=\"between\"/></c:valAx></c:plotArea>@.\
         <c:legend><c:legendPos val=\"b\"/><c:layout/><c:overlay val=\"0\"/></c:legend>@.\
         <c:plotVisOnly val=\"0\"/><c:dispBlanksAs val=\"gap\"/>@.\
         </c:chart>@.</c:chartSpace>@.")
    ()

let workbook_sheets sheets =
  { name = plot_sheet_name; rows = [] }
  :: { name = "Totals"; rows = [] }
  :: { name = "Clashes"; rows = [] }
  :: sheets

let pp_content_types fmt worksheet_count chart_count =
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
  Format.fprintf fmt
    "<Override PartName=\"/xl/drawings/drawing1.xml\" \
     ContentType=\"application/vnd.openxmlformats-officedocument.drawing+xml\"/>@.";
  for i = 1 to chart_count do
    Format.fprintf fmt
      "<Override PartName=\"/xl/charts/chart%d.xml\" \
       ContentType=\"application/vnd.openxmlformats-officedocument.drawingml.chart+xml\"/>@."
      i
  done;
  Format.fprintf fmt "</Types>@."

let content_types worksheet_count chart_count =
  buffer_to_string (fun fmt () -> pp_content_types fmt worksheet_count chart_count) ()

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

let workbook_entries ?timeout sheets clashes =
  let plot_charts = make_plot_charts sheets in
  let worksheets =
    [ plot_worksheet plot_charts; totals_worksheet sheets; clashes_worksheet sheets clashes ]
    @ List.map solver_worksheet sheets
  in
  let worksheet_count = List.length worksheets in
  [
    ("[Content_Types].xml", content_types worksheet_count (List.length plot_charts));
    ("_rels/.rels", root_relationships);
    ("xl/workbook.xml", workbook sheets);
    ("xl/_rels/workbook.xml.rels", workbook_relationships worksheet_count);
    ("xl/worksheets/_rels/sheet1.xml.rels", plot_sheet_relationships);
    ("xl/drawings/drawing1.xml", drawing plot_charts);
    ("xl/drawings/_rels/drawing1.xml.rels", drawing_relationships plot_charts);
  ]
  @ List.map
      (fun plot_chart ->
        (Format.sprintf "xl/charts/chart%d.xml" plot_chart.chart_index, chart ?timeout plot_chart))
      plot_charts
  @ List.mapi
      (fun i worksheet -> (Format.sprintf "xl/worksheets/sheet%d.xml" (i + 1), worksheet))
      worksheets

let write_workbook ?timeout filename (sheets, clashes) =
  let zf = Zip.open_out filename in
  Fun.protect
    ~finally:(fun () -> Zip.close_out zf)
    (fun () ->
      List.iter
        (fun (name, contents) -> Zip.add_entry contents zf name)
        (workbook_entries ?timeout sheets clashes))
