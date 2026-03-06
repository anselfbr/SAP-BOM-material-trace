from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re

app = FastAPI(title="SAP Work Order Material Trace API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df


def find_required_col(df: pd.DataFrame, target_name: str) -> str:
    """
    大小寫不敏感、前後空白不敏感地找欄位
    """
    normalized = {str(c).strip().lower(): c for c in df.columns}
    key = target_name.strip().lower()
    if key in normalized:
        return normalized[key]

    raise HTTPException(
        status_code=400,
        detail=f"缺少必要欄位: {target_name}；實際欄位: {list(df.columns)}"
    )


def read_excel_file(upload: UploadFile) -> pd.DataFrame:
    filename = (upload.filename or "").lower()

    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        raise HTTPException(
            status_code=400,
            detail=f"檔案格式錯誤，請上傳 Excel 檔 (.xlsx/.xls)：{upload.filename}"
        )

    try:
        df = pd.read_excel(upload.file, dtype=str)
        return normalize_columns(df)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"無法讀取 Excel 檔 {upload.filename}：{str(e)}"
        )


@app.get("/")
def home():
    return {"message": "SAP Work Order Material Trace API Running"}


@app.post(
    "/api/upload",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
            "description": "Download Excel result",
        }
    },
)
async def trace_materials(
    issue_file: UploadFile = File(..., description="工單耗用檔：請上傳工單耗用 Excel"),
    workorder_file: UploadFile = File(..., description="工單生產檔：請上傳工單生產 Excel"),
):
    try:
        # 1) 讀檔
        issue_df = read_excel_file(issue_file)
        wo_df = read_excel_file(workorder_file)

        # 2) 工單耗用檔欄位
        issue_order_col = find_required_col(issue_df, "Order")
        issue_plant_col = find_required_col(issue_df, "Plant")
        issue_material_col = find_required_col(issue_df, "Material")
        issue_material_desc_col = find_required_col(issue_df, "Material Description")
        issue_req_qty_col = find_required_col(issue_df, "Requirement quantity (EINHEIT)")
        issue_withdrawn_qty_col = find_required_col(issue_df, "Quantity withdrawn (EINHEIT)")
        issue_uom_col = find_required_col(issue_df, "Base Unit of Measure (=EINHEIT)")

        # 3) 工單生產檔欄位
        wo_order_col = find_required_col(wo_df, "Order")
        wo_plant_col = find_required_col(wo_df, "Plant")
        wo_product_col = find_required_col(wo_df, "Material Number")
        wo_product_desc_col = find_required_col(wo_df, "Material description")
        wo_order_qty_col = find_required_col(wo_df, "Order quantity (GMEIN)")
        wo_delivered_qty_col = find_required_col(wo_df, "Delivered quantity (GMEIN)")

        # 4) 清理工單號
        issue_df[issue_order_col] = issue_df[issue_order_col].astype(str).str.strip()
        wo_df[wo_order_col] = wo_df[wo_order_col].astype(str).str.strip()

        # 5) 數量欄轉數字
        issue_df["原物料需求量(數值)"] = pd.to_numeric(
            issue_df[issue_req_qty_col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )

        issue_df["原物料實際耗用量(數值)"] = pd.to_numeric(
            issue_df[issue_withdrawn_qty_col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )

        wo_df["工單需求數量(數值)"] = pd.to_numeric(
            wo_df[wo_order_qty_col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )

        wo_df["實際完工數量(數值)"] = pd.to_numeric(
            wo_df[wo_delivered_qty_col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )

        # 6) 只保留需要欄位
        issue_small = issue_df[
            [
                issue_order_col,
                issue_plant_col,
                issue_material_col,
                issue_material_desc_col,
                issue_req_qty_col,
                issue_withdrawn_qty_col,
                issue_uom_col,
                "原物料需求量(數值)",
                "原物料實際耗用量(數值)",
            ]
        ].copy()

        wo_small = wo_df[
            [
                wo_order_col,
                wo_plant_col,
                wo_product_col,
                wo_product_desc_col,
                wo_order_qty_col,
                wo_delivered_qty_col,
                "工單需求數量(數值)",
                "實際完工數量(數值)",
            ]
        ].copy()

        # 7) 用 Order 關聯
        merged = wo_small.merge(
            issue_small,
            left_on=wo_order_col,
            right_on=issue_order_col,
            how="left",
            suffixes=("_wo", "_issue"),
        )

        # 8) 輸出明細表
        merged_out = pd.DataFrame({
            "Order": merged[wo_order_col],
            "Plant": merged[wo_plant_col].fillna(merged[issue_plant_col]),
            "Product Material Number": merged[wo_product_col],
            "Product Description": merged[wo_product_desc_col],
            "工單需求數量": merged[wo_order_qty_col],
            "工單需求數量(數值)": merged["工單需求數量(數值)"],
            "實際完工數量": merged[wo_delivered_qty_col],
            "實際完工數量(數值)": merged["實際完工數量(數值)"],
            "Input Material": merged[issue_material_col],
            "Input Material Description": merged[issue_material_desc_col],
            "原物料需求量": merged[issue_req_qty_col],
            "原物料需求量(數值)": merged["原物料需求量(數值)"],
            "原物料實際耗用量": merged[issue_withdrawn_qty_col],
            "原物料實際耗用量(數值)": merged["原物料實際耗用量(數值)"],
            "UoM": merged[issue_uom_col],
        })

        merged_out = merged_out.sort_values(
            by=["Order", "Product Material Number", "Input Material"],
            na_position="last"
        )

        # 9) 原物料彙總
        trace_summary = (
            merged_out
            .dropna(subset=["Input Material"])
            .groupby(
                [
                    "Order",
                    "Plant",
                    "Product Material Number",
                    "Product Description",
                    "Input Material",
                    "Input Material Description",
                    "UoM",
                ],
                as_index=False
            )[["原物料需求量(數值)", "原物料實際耗用量(數值)"]]
            .sum()
        )

        # 10) 成品工單彙總
        product_summary = (
            merged_out[
                [
                    "Order",
                    "Plant",
                    "Product Material Number",
                    "Product Description",
                    "工單需求數量",
                    "工單需求數量(數值)",
                    "實際完工數量",
                    "實際完工數量(數值)",
                ]
            ]
            .drop_duplicates()
            .sort_values(by=["Order", "Product Material Number"])
        )

        # 11) 輸出 Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            merged_out.to_excel(writer, index=False, sheet_name="trace_detail")
            trace_summary.to_excel(writer, index=False, sheet_name="trace_summary")
            product_summary.to_excel(writer, index=False, sheet_name="product_summary")

        output.seek(0)

        # 12) 回傳下載
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": 'attachment; filename="sap_workorder_material_trace.xlsx"'
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
