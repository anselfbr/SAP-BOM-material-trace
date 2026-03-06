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

        # 2) 寫死對應 SAP 欄位名稱（依你提供的表頭）
        # 工單耗用檔
        issue_required_cols = [
            "Order",
            "Plant",
            "Material",
            "Material Description",
            "Requirement quantity (EINHEIT)",
            "Quantity withdrawn (EINHEIT)",
            "Base Unit of Measure (=EINHEIT)",
        ]

        # 工單生產檔
        wo_required_cols = [
            "Order",
            "Plant",
            "Material Number",
            "Material description",
            "Order quantity (GMEIN)",
            "Delivered quantity (GMEIN)",
        ]

        missing_issue = [c for c in issue_required_cols if c not in issue_df.columns]
        missing_wo = [c for c in wo_required_cols if c not in wo_df.columns]

        if missing_issue or missing_wo:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Excel 欄位名稱不符合預期",
                    "工單耗用檔缺少欄位": missing_issue,
                    "工單生產檔缺少欄位": missing_wo,
                    "工單耗用檔實際欄位": list(issue_df.columns),
                    "工單生產檔實際欄位": list(wo_df.columns),
                },
            )

        # 3) 清理工單號
        issue_df["Order"] = issue_df["Order"].astype(str).str.strip()
        wo_df["Order"] = wo_df["Order"].astype(str).str.strip()

        # 4) 數量轉數字
        issue_df["Requirement Qty Num"] = pd.to_numeric(
            issue_df["Requirement quantity (EINHEIT)"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

        issue_df["Withdrawn Qty Num"] = pd.to_numeric(
            issue_df["Quantity withdrawn (EINHEIT)"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

        wo_df["Order Qty Num"] = pd.to_numeric(
            wo_df["Order quantity (GMEIN)"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

        wo_df["Delivered Qty Num"] = pd.to_numeric(
            wo_df["Delivered quantity (GMEIN)"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )

        # 5) 只保留需要欄位
        issue_small = issue_df[
            [
                "Order",
                "Plant",
                "Material",
                "Material Description",
                "Requirement quantity (EINHEIT)",
                "Quantity withdrawn (EINHEIT)",
                "Base Unit of Measure (=EINHEIT)",
                "Requirement Qty Num",
                "Withdrawn Qty Num",
            ]
        ].copy()

        wo_small = wo_df[
            [
                "Order",
                "Plant",
                "Material Number",
                "Material description",
                "Order quantity (GMEIN)",
                "Delivered quantity (GMEIN)",
                "Order Qty Num",
                "Delivered Qty Num",
            ]
        ].copy()

        # 6) 用 Order 關聯
        merged = wo_small.merge(
            issue_small,
            on="Order",
            how="left",
            suffixes=("_wo", "_issue"),
        )

        # 7) 統一輸出欄位
        merged_out = pd.DataFrame({
            "Order": merged["Order"],
            "Plant": merged["Plant_wo"].fillna(merged["Plant_issue"]),
            "Product Material Number": merged["Material Number"],
            "Product Description": merged["Material description"],
            "工單需求數量": merged["Order quantity (GMEIN)"],
            "工單需求數量(數值)": merged["Order Qty Num"],
            "實際完工數量": merged["Delivered quantity (GMEIN)"],
            "實際完工數量(數值)": merged["Delivered Qty Num"],
            "Input Material": merged["Material"],
            "Input Material Description": merged["Material Description"],
            "原物料需求量": merged["Requirement quantity (EINHEIT)"],
            "原物料需求量(數值)": merged["Requirement Qty Num"],
            "原物料實際耗用量": merged["Quantity withdrawn (EINHEIT)"],
            "原物料實際耗用量(數值)": merged["Withdrawn Qty Num"],
            "UoM": merged["Base Unit of Measure (=EINHEIT)"],
        })

        # 8) 明細表排序
        merged_out = merged_out.sort_values(
            by=["Order", "Product Material Number", "Input Material"],
            na_position="last"
        )

        # 9) 彙總表：每工單/成品/原物料彙總實際耗用量
        summary = (
            merged_out
            .dropna(subset=["Input Material"])
            .groupby(
                ["Order", "Plant", "Product Material Number", "Product Description", "Input Material", "Input Material Description", "UoM"],
                as_index=False
            )[["原物料需求量(數值)", "原物料實際耗用量(數值)"]]
            .sum()
        )

        # 10) 產品層級彙總：每工單對應成品資訊
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
            summary.to_excel(writer, index=False, sheet_name="trace_summary")
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
