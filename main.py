from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re
import traceback

app = FastAPI(title="SAP Work Order Material Trace API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_columns(df: pd.DataFrame):
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df


def read_excel(upload: UploadFile):

    filename = (upload.filename or "").lower()

    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        raise HTTPException(status_code=400, detail="請上傳Excel")

    try:
        df = pd.read_excel(upload.file, dtype=str)
        return normalize_columns(df)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def find_col(df, name):

    col_map = {c.lower(): c for c in df.columns}

    if name.lower() in col_map:
        return col_map[name.lower()]

    raise HTTPException(
        status_code=400,
        detail=f"缺少欄位 {name}"
    )


@app.get("/")
def home():
    return {"message": "SAP Work Order Material Trace API Running"}


@app.post("/api/upload")
async def trace_materials(

    issue_file: UploadFile = File(...),
    workorder_file: UploadFile = File(...)
):

    try:

        issue = read_excel(issue_file)
        wo = read_excel(workorder_file)

        # 工單耗用
        issue_order = find_col(issue, "Order")
        issue_plant = find_col(issue, "Plant")
        issue_material = find_col(issue, "Material")
        issue_desc = find_col(issue, "Material Description")
        issue_req = find_col(issue, "Requirement quantity (EINHEIT)")
        issue_withdraw = find_col(issue, "Quantity withdrawn (EINHEIT)")
        issue_uom = find_col(issue, "Base Unit of Measure (=EINHEIT)")

        # 工單生產
        wo_order = find_col(wo, "Order")
        wo_plant = find_col(wo, "Plant")
        wo_product = find_col(wo, "Material Number")
        wo_desc = find_col(wo, "Material description")
        wo_order_qty = find_col(wo, "Order quantity (GMEIN)")
        wo_delivered_qty = find_col(wo, "Delivered quantity (GMEIN)")

        issue_small = issue[
            [
                issue_order,
                issue_plant,
                issue_material,
                issue_desc,
                issue_req,
                issue_withdraw,
                issue_uom
            ]
        ].copy()

        wo_small = wo[
            [
                wo_order,
                wo_plant,
                wo_product,
                wo_desc,
                wo_order_qty,
                wo_delivered_qty
            ]
        ].copy()

        issue_small = issue_small.rename(columns={

            issue_order: "Order",
            issue_plant: "Issue Plant",
            issue_material: "Material",
            issue_desc: "Material Description",
            issue_req: "Requirement Qty",
            issue_withdraw: "Withdrawn Qty",
            issue_uom: "Base Unit of Measure"

        })

        wo_small = wo_small.rename(columns={

            wo_order: "Order",
            wo_plant: "Plant",
            wo_product: "Product Material Number",
            wo_desc: "Product Description",
            wo_order_qty: "Order Qty",
            wo_delivered_qty: "Delivered Qty"

        })

        merged = wo_small.merge(issue_small, on="Order", how="left")

        trace_detail = pd.DataFrame({

            "Order": merged["Order"],
            "Plant": merged["Plant"],
            "Product Material Number": merged["Product Material Number"],
            "Product Description": merged["Product Description"],
            "Order Qty": merged["Order Qty"],
            "Delivered Qty": merged["Delivered Qty"],
            "Material": merged["Material"],
            "Material Description": merged["Material Description"],
            "Requirement Qty": merged["Requirement Qty"],
            "Withdrawn Qty": merged["Withdrawn Qty"],
            "Base Unit of Measure": merged["Base Unit of Measure"]

        })

        trace_detail = trace_detail.sort_values(
            by=["Order", "Product Material Number", "Material"]
        )

        trace_summary = (
            trace_detail
            .groupby(
                [
                    "Order",
                    "Plant",
                    "Product Material Number",
                    "Material",
                    "Material Description",
                    "Base Unit of Measure"
                ],
                as_index=False
            )[["Requirement Qty", "Withdrawn Qty"]]
            .sum()
        )

        product_summary = (
            trace_detail[
                [
                    "Order",
                    "Plant",
                    "Product Material Number",
                    "Product Description",
                    "Order Qty",
                    "Delivered Qty"
                ]
            ]
            .drop_duplicates()
        )

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:

            trace_detail.to_excel(writer, index=False, sheet_name="trace_detail")
            trace_summary.to_excel(writer, index=False, sheet_name="trace_summary")
            product_summary.to_excel(writer, index=False, sheet_name="product_summary")

        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition":
                "attachment; filename=sap_workorder_material_trace.xlsx"
            }
        )

    except Exception as e:

        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
