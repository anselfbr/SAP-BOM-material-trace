from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io

app = FastAPI(title="SAP BOM Material Trace API")

def read_table(upload: UploadFile) -> pd.DataFrame:
    """Read CSV/XLSX into DataFrame with common SAP export encodings."""
    filename = (upload.filename or "").lower()
    data = upload.file.read()

    if filename.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(data))
    if filename.endswith(".csv"):
        for enc in ["utf-8-sig", "utf-8", "cp950", "big5", "latin1"]:
            try:
                return pd.read_csv(io.BytesIO(data), encoding=enc)
            except Exception:
                pass
        raise ValueError("CSV encoding not supported (tried utf-8/cp950/big5).")

    raise ValueError("Unsupported file type. Please upload .csv or .xlsx")

@app.get("/")
def home():
    return {"ok": True, "message": "SAP Material Trace API running"}

@app.post("/api/upload")
async def upload_files(
    issue_file: UploadFile = File(..., description="SAP 發料/領料檔 (CSV/XLSX)"),
    workorder_file: UploadFile = File(..., description="SAP 生產工單檔 (CSV/XLSX)"),
):
    try:
        issue_df = read_table(issue_file)
        wo_df = read_table(workorder_file)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    issue_df.columns = [str(c).strip() for c in issue_df.columns]
    wo_df.columns = [str(c).strip() for c in wo_df.columns]

    return {
        "ok": True,
        "issue": {
            "filename": issue_file.filename,
            "rows": int(issue_df.shape[0]),
            "columns": list(issue_df.columns),
            "preview": issue_df.head(5).fillna("").to_dict(orient="records"),
        },
        "workorder": {
            "filename": workorder_file.filename,
            "rows": int(wo_df.shape[0]),
            "columns": list(wo_df.columns),
            "preview": wo_df.head(5).fillna("").to_dict(orient="records"),
        },
        "next_step": "把兩份檔案的 columns 貼給我，我就把『工單→成品→原物料用量』歸集邏輯接上。",
    }
