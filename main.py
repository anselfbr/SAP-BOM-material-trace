from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re

app = FastAPI(title="SAP BOM Material Trace API")

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


def find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    col_map = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in col_map:
            return col_map[key]
    return ""


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
    return {"message": "SAP BOM Material Trace API Running"}


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
        issue_df = read_excel_file(issue_file)
        wo_df = read_excel_file(workorder_file)

        issue_order_col = find_col(issue_df, ["Order", "order", "工單"])
        issue_material_col = find_col(issue_df, ["Material", "material", "原物料", "Component"])
        issue_desc_col = find_col(issue_df, [
            "Material Description", "material description", "Description", "description", "物料說明"
        ])
        issue_qty_col = find_col(issue_df, [
            "Quantity withdrawn (EINHEIT)",
            "Quantity withdrawn",
            "Qty withdrawn",
            "quantity withdrawn",
            "Issued quantity",
            "Requirement quantity (EINHEIT)",
            "Requirement quantity",
            "耗用量",
            "需求量"
        ])
        issue_uom_col = find_col(issue_df, [
            "Base Unit of Measure (=EINHEIT)",
            "Base Unit of Measure",
            "UoM",
            "Unit",
            "單位"
        ])
        issue_vendor_col = find_col(issue_df, ["Vendor", "vendor", "供應商"])
        issue_plant_col = find_col(issue_df, ["Plant", "plant", "工廠"])

        wo_order_col = find_col(wo_df, ["Order", "order", "工單"])
        wo_product_col = find_col(wo_df, [
            "Material Number",
            "Material number",
            "material number",
            "Material",
            "material",
            "產品料號",
            "成品料號"
        ])
        wo_product_desc_col = find_col(wo_df, [
            "Material Description", "material description", "Description", "description", "產品說明", "成品說明"
        ])
        wo_plant_col = find_col(wo_df, ["Plant", "plant", "工廠"])

        missing = []
        if not issue_order_col:
            missing.append("工單耗用檔缺少欄位：Order")
        if not issue_material_col:
            missing.append("工單耗用檔缺少欄位：Material")
        if not wo_order_col:
            missing.append("工單生產檔缺少欄位：Order")
        if not wo_product_col:
            missing.append("工單生產檔缺少欄位：Material Number / Material")

        if missing:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "缺少必要欄位",
                    "missing": missing,
                    "工單耗用檔欄位": list(issue_df.columns),
                    "工單生產檔欄位": list(wo_df.columns),
                },
            )

        issue_df[issue_order_col] = issue_df[issue_order_col].astype(str).str.strip()
        wo_df[wo_order_col] = wo_df[wo_order_col].astype(str).str.strip()

        if issue_qty_col:
            issue_df["_qty_num"] = pd.to_numeric(
                issue_df[issue_qty_col].astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce"
            )
        else:
            issue_df["_qty_num"] = pd.NA

        issue_keep = [issue_order_col, issue_material_col]
        if issue_desc_col:
            issue_keep.append(issue_desc_col)
        if issue_qty_col:
            issue_keep.append(issue_qty_col)
        if issue_uom_col:
            issue_keep.append(issue_uom_col)
        if issue_vendor_col:
            issue_keep.append(issue_vendor_col)
        if issue_plant_col:
            issue_keep.append(issue_plant_col)
        issue_keep.append("_qty_num")

        wo_keep = [wo_order_col, wo_product_col]
        if wo_product_desc_col:
            wo_keep.append(wo_product_desc_col)
        if wo_plant_col:
            wo_keep.append(wo_plant_col)

        issue_small = issue_df[issue_keep].copy()
        wo_small = wo_df[wo_keep].copy()

        merged = wo_small.merge(
            issue_small,
            left_on=wo_order_col,
            right_on=issue_order_col,
            how="left",
            suffixes=("_wo", "_issue"),
        )

        rename_map = {
            wo_order_col: "Order",
            wo_product_col: "Product Material Number",
            issue_material_col: "Input Material",
        }
        if wo_product_desc_col:
            rename_map[wo_product_desc_col] = "Product Description"
        if issue_desc_col:
            rename_map[issue_desc_col] = "Input Material Description"
        if issue_qty_col:
            rename_map[issue_qty_col] = "Input Qty (raw)"
        if issue_uom_col:
            rename_map[issue_uom_col] = "UoM"
        if issue_vendor_col:
            rename_map[issue_vendor_col] = "Vendor"
        if wo_plant_col:
            rename_map[wo_plant_col] = "Plant"
        elif issue_plant_col:
            rename_map[issue_plant_col] = "Plant"

        merged_out = merged.rename(columns=rename_map)

        if "_qty_num" in merged_out.columns:
            merged_out["Input Qty (num)"] = merged_out["_qty_num"]

        drop_cols = []
        if "_qty_num" in merged_out.columns:
            drop_cols.append("_qty_num")
        if issue_order_col != "Order" and issue_order_col in merged_out.columns:
            drop_cols.append(issue_order_col)

        merged_out = merged_out.drop(columns=drop_cols, errors="ignore")

        preferred_order = [
            "Order",
            "Plant",
            "Product Material Number",
            "Product Description",
            "Input Material",
            "Input Material Description",
            "Input Qty (raw)",
            "Input Qty (num)",
            "UoM",
            "Vendor",
        ]
        final_cols = [c for c in preferred_order if c in merged_out.columns] + [
            c for c in merged_out.columns if c not in preferred_order
        ]
        merged_out = merged_out[final_cols]

        summary_group_cols = [
            c for c in ["Order", "Plant", "Product Material Number", "Input Material", "UoM"]
            if c in merged_out.columns
        ]

        if not summary_group_cols:
            raise HTTPException(
                status_code=500,
                detail=f"找不到 summary 分組欄位，現有欄位: {list(merged_out.columns)}"
            )

        if "Input Qty (num)" in merged_out.columns:
            summary = (
                merged_out
                .dropna(subset=["Input Material"])
                .groupby(summary_group_cols, as_index=False)["Input Qty (num)"]
                .sum()
            )
        else:
            summary = (
                merged_out
                .dropna(subset=["Input Material"])
                .groupby(summary_group_cols, as_index=False)
                .size()
                .rename(columns={"size": "Rows"})
            )

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            merged_out.to_excel(writer, index=False, sheet_name="trace_detail")
            summary.to_excel(writer, index=False, sheet_name="trace_summary")

        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="sap_bom_material_trace.xlsx"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
