curl -X POST "https://sap-bom-material-trace.onrender.com/api/upload" ^
  -H "accept: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ^
  -H "Content-Type: multipart/form-data" ^
  -F "issue_file=@工單耗用.xlsx" ^
  -F "workorder_file=@工單生產.xlsx" ^
  --output sap_workorder_material_trace.xlsx
