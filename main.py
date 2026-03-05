from fastapi import FastAPI, UploadFile, File
import pandas as pd

app = FastAPI()

@app.get("/")
def home():
    return {"message": "SAP Material Trace API Running"}

@app.post("/upload")
async def upload_files(
    issue_file: UploadFile = File(...),
    workorder_file: UploadFile = File(...)
):

    issue_df = pd.read_csv(issue_file.file)
    wo_df = pd.read_csv(workorder_file.file)

    result = issue_df.merge(
        wo_df,
        on="WorkOrder",
        how="left"
    )

    return {
        "rows": len(result),
        "columns": list(result.columns)
    }
