from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re

app = FastAPI(title="SAP BOM Material Trace API")

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 去掉欄名空白、統一大小寫、把多餘空白壓成一個
    df = df.copy()
    df.columns = [
        re.sub(r"\s+", " ", str(c).strip())
        for c in df.columns
    ]
    return df

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            return cols[key]
    return ""

def _read_excel(upload: UploadFile) -> pd.DataFrame:
    # 只支援 xlsx/xls
    name = (upload.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls")):
        raise HTTPException(status_code=400, detail=f"File must be .xlsx/.xls: {upload.filename}")

    try:
        # 注意：UploadFile.file 是 file-like，可直接給 pandas
        df = pd.read_excel(upload.file, dtype=str)  # dtype=str 避免料號被轉科學記號
        df = _normalize_columns(df)
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel {upload.filename}: {e}")

@app.get("/")
def home():
    return {"message": "SAP BOM Material Trace API Running"}

@app.post("/api/trace")
async def trace_materials(
    issue_file: UploadFile = File(...),      # 工單耗用
    workorder_file: UploadFile = File(...),   # 工單生產
):
    # 1) 讀檔
    issue_df = _read_excel(issue_file)
    wo_df = _read_excel(workorder_file)

    # 2) 找欄位（容錯：不同公司匯出欄名可能略有差）
    issue_order_col = _find_col(issue_df, ["Order", "order", "工單"])
    issue_material_col = _find_col(issue_df, ["Material", "material", "原物料", "Component"])
    issue_qty_col = _find_col(issue_df, [
        "Quantity withdrawn (EINHEIT)",
        "Quantity withdrawn",
        "Qty withdrawn",
        "quantity withdrawn",
        "耗用量",
        "Issued quantity"
    ])

    wo_order_col = _find_col(wo_df, ["Order", "order", "工單"])
    wo_product_col = _find_col(wo_df, ["Material Number", "Material number", "material number", "產品料號", "Material"])

    missing = []
    if not issue_order_col: missing.append("issue_file: Order")
    if not issue_material_col: missing.append("issue_file: Material")
    if not wo_order_col: missing.append("workorder_file: Order")
    if not wo_product_col: missing.append("workorder_file: Material Number (product)")

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing required columns",
                "missing": missing,
                "issue_columns": list(issue_df.columns),
                "workorder_columns": list(wo_df.columns),
            },
        )

    # 3) 清理資料型別 / 去空白
    issue_df[issue_order_col] = issue_df[issue_order_col].astype(str).str.strip()
    wo_df[wo_order_col] = wo_df[wo_order_col].astype(str).str.strip()

    # quantity（如果有）轉成數字方便彙總
    if issue_qty_col:
        issue_df[issue_qty_col] = (
            issue_df[issue_qty_col]
            .astype(str).str.replace(",", "", regex=False).str.strip()
        )
        issue_df["_qty_num"] = pd.to_numeric(issue_df[issue_qty_col], errors="coerce")
    else:
        issue_df["_qty_num"] = pd.NA

    # 4) 只留需要欄位
    issue_keep = [issue_order_col, issue_material_col]
    if issue_qty_col:
        issue_keep.append(issue_qty_col)
    issue_keep.append("_qty_num")
    issue_small = issue_df[issue_keep].copy()

    wo_small = wo_df[[wo_order_col, wo_product_col]].copy()

    # 5) 用 Order join：每個工單的產品料號 對上 該工單耗用的原物料
    merged = wo_small.merge(
        issue_small,
        left_on=wo_order_col,
        right_on=issue_order_col,
        how="left",
        suffixes=("_wo", "_issue"),
    )

    # 6) 整理輸出欄位名稱
    merged_out = merged.rename(columns={
        wo_order_col: "Order",
        wo_product_col: "Product Material Number",
        issue_material_col: "Input Material",
    })

    if issue_qty_col:
        merged_out = merged_out.rename(columns={issue_qty_col: "Input Qty (raw)"})
    else:
        merged_out["Input Qty (raw)"] = ""

    merged_out["Input Qty (num)"] = merged_out["_qty_num"]
    merged_out = merged_out.drop(columns=[c for c in merged_out.columns if c in [issue_order_col, "_qty_num"]], errors="ignore")

    # 7) Summary：每個 Order + 產品，彙總原物料用量（如果有數量）
    if issue_qty_col:
        summary = (
            merged_out
            .dropna(subset=["Input Material"])
            .groupby(["Order", "Product Material Number", "Input Material"], as_index=False)["Input Qty (num)"]
            .sum()
        )
    else:
        summary = (
            merged_out
            .dropna(subset=["Input Material"])
            .groupby(["Order", "Product Material Number", "Input Material"], as_index=False)
            .size()
            .rename(columns={"size": "Rows"})
        )

    # 8) 寫 Excel 到記憶體並回傳下載
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        merged_out.to_excel(writer, index=False, sheet_name="trace_detail")
        summary.to_excel(writer, index=False, sheet_name="trace_summary")

    output.seek(0)
    filename = "sap_bom_material_trace.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
