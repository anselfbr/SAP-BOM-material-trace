from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import re
import traceback

app = FastAPI(title="SAP Work Order Material Trace API - Debug")

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


def find_required_col(df: pd.DataFrame, target_name: str) -> str:
    col_map = {str(c).strip().lower(): c for c in df.columns}
    key = target_name.strip().lower()

    if key in col_map:
        return col_map[key]

    raise HTTPException(
        status_code=400,
        detail={
            "error": f"缺少必要欄位: {target_name}",
            "actual_columns": list(df.columns)
        }
    )


@app.get("/")
def home():
    return {"message": "API is running"}


@app.post("/api/upload")
async def debug_upload(
    issue_file: UploadFile = File(..., description="工單耗用檔"),
    workorder_file: UploadFile = File(..., description="工單生產檔"),
):
    try:
        print("=== ENTER /api/upload ===")
        print("issue_file:", issue_file.filename)
        print("workorder_file:", workorder_file.filename)

        issue_df = read_excel_file(issue_file)
        wo_df = read_excel_file(workorder_file)

        print("issue columns:", list(issue_df.columns))
        print("workorder columns:", list(wo_df.columns))

        # 工單耗用檔欄位
        issue_order_col = find_required_col(issue_df, "Order")
        issue_plant_col = find_required_col(issue_df, "Plant")
        issue_material_col = find_required_col(issue_df, "Material")
        issue_material_desc_col = find_required_col(issue_df, "Material Description")
        issue_req_qty_col = find_required_col(issue_df, "Requirement quantity (EINHEIT)")
        issue_withdrawn_qty_col = find_required_col(issue_df, "Quantity withdrawn (EINHEIT)")
        issue_uom_col = find_required_col(issue_df, "Base Unit of Measure (=EINHEIT)")

        # 工單生產檔欄位
        wo_order_col = find_required_col(wo_df, "Order")
        wo_plant_col = find_required_col(wo_df, "Plant")
        wo_product_col = find_required_col(wo_df, "Material Number")
        wo_product_desc_col = find_required_col(wo_df, "Material description")
        wo_order_qty_col = find_required_col(wo_df, "Order quantity (GMEIN)")
        wo_delivered_qty_col = find_required_col(wo_df, "Delivered quantity (GMEIN)")

        # 只做最小 merge 測試
        issue_small = issue_df[
            [
                issue_order_col,
                issue_plant_col,
                issue_material_col,
                issue_material_desc_col,
                issue_req_qty_col,
                issue_withdrawn_qty_col,
                issue_uom_col,
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
            ]
        ].copy()

        issue_small = issue_small.rename(columns={
            issue_order_col: "Order",
            issue_plant_col: "Issue Plant",
            issue_material_col: "Input Material",
            issue_material_desc_col: "Input Material Description",
            issue_req_qty_col: "Requirement Qty",
            issue_withdrawn_qty_col: "Withdrawn Qty",
            issue_uom_col: "UoM",
        })

        wo_small = wo_small.rename(columns={
            wo_order_col: "Order",
            wo_plant_col: "Plant",
            wo_product_col: "Product Material Number",
            wo_product_desc_col: "Product Description",
            wo_order_qty_col: "Order Qty",
            wo_delivered_qty_col: "Delivered Qty",
        })

        merged = wo_small.merge(issue_small, on="Order", how="left")

        print("merged rows:", len(merged))
        print("=== EXIT /api/upload SUCCESS ===")

        return {
            "ok": True,
            "issue_filename": issue_file.filename,
            "workorder_filename": workorder_file.filename,
            "issue_columns": list(issue_df.columns),
            "workorder_columns": list(wo_df.columns),
            "issue_rows": len(issue_df),
            "workorder_rows": len(wo_df),
            "merged_rows": len(merged),
            "preview": merged.head(10).fillna("").to_dict(orient="records"),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("=== UNHANDLED ERROR START ===")
        traceback.print_exc()
        print("=== UNHANDLED ERROR END ===")
        raise HTTPException(status_code=500, detail=str(e))
