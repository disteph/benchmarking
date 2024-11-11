open Containers

let prefix = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>
              <worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\" mc:Ignorable=\"x14ac xr xr2 xr3\" xmlns:x14ac=\"http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac\" xmlns:xr=\"http://schemas.microsoft.com/office/spreadsheetml/2014/revision\" xmlns:xr2=\"http://schemas.microsoft.com/office/spreadsheetml/2015/revision2\" xmlns:xr3=\"http://schemas.microsoft.com/office/spreadsheetml/2016/revision3\" xr:uid=\"{B5C9F1FA-7775-9C48-A02B-F44496B467AA}\"><dimension ref=\"A1:I2\"/><sheetViews><sheetView tabSelected=\"1\" workbookViewId=\"0\"><selection activeCell=\"I2\" sqref=\"I2\"/></sheetView></sheetViews><sheetFormatPr baseColWidth=\"10\" defaultRowHeight=\"16\" x14ac:dyDescent=\"0.2\"/><cols><col min=\"1\" max=\"1\" width=\"57.1640625\" bestFit=\"1\" customWidth=\"1\"/><col min=\"2\" max=\"2\" width=\"80.6640625\" bestFit=\"1\" customWidth=\"1\"/><col min=\"3\" max=\"3\" width=\"8.1640625\" bestFit=\"1\" customWidth=\"1\"/><col min=\"4\" max=\"5\" width=\"11.1640625\" bestFit=\"1\" customWidth=\"1\"/><col min=\"6\" max=\"6\" width=\"9.1640625\" bestFit=\"1\" customWidth=\"1\"/></cols><sheetData>"

let pp_string col row fmt s =
  Format.fprintf fmt "<c r=\"%s%i\" t=\"s\"><v>%s</v></c>" col row s
  
let pp_solver = pp_string "A"
let pp_bench  = pp_string "B"
let pp_result = pp_string "C"

let pp_float col row fmt f =
  Format.fprintf fmt "<c r=\"%s%i\" t=\"s\"><v>%f</v></c>" col row f

let pp_wall = pp_float "D"
let pp_user = pp_float "E"
let pp_sys  = pp_float "F"

let pp_times row fmt times =
  match times with
  | Some(wall,user,sys) ->
     Format.fprintf fmt "%a%a%a"
       (pp_wall row) wall
       (pp_user row) user
       (pp_sys row) sys
  | None -> ()

let pp_satunsat fmt row =
  Format.fprintf fmt
    "<c r=\"G%i\" t=\"b\"><f>OR((C%i=\"unsat\"), (C%i=\"sat\"))</f><v>1</v></c>"
     row row row
  
let pp_suffix fmt row =
  if Int.equal row 1 then
    Format.fprintf fmt "<c r=\"H1\"><v>1</v></c><c r=\"I1\"><v>0</v></c>"
  else  
    Format.fprintf fmt
      "<c r=\"H%i\"><f>IF(G%i,H%i+1,0)</f><v>2</v></c><c r=\"I%i\"><f>IF(G%i,I%i+E%i,0)</f><v>0</v></c>"
      row row (row-1) row row (row-1) row

let pp_line fmt row (solver,bench,result,times) =
  let row = row+1 in (* OCaml counts form 0, Excel from 1 *)
  Format.fprintf fmt
    "<row r=\"%i\" spans=\"1:9\" x14ac:dyDescent=\"0.2\">%a%a%a%a%a%a</row>"
    row
    (pp_solver row) solver
    (pp_bench row) bench
    (pp_result row) result
    (pp_times row) times
    pp_satunsat row
    pp_suffix row

let suffix = "</sheetData><pageMargins left=\"0.7\" right=\"0.7\" top=\"0.75\" bottom=\"0.75\" header=\"0.3\" footer=\"0.3\"/></worksheet>"

let pp_table fmt l =
  Format.fprintf fmt
    "%s%a%s"
    prefix
    (fun fmt -> List.iteri (pp_line fmt))
    l
    suffix
