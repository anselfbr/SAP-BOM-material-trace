from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re
import zipfile
import traceback
from collections import defaultdict

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
        raise HTTPException(status_code=400, detail="請上傳 Excel 檔")

    try:
        df = pd.read_excel(upload.file, dtype=str)
        return normalize_columns(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Excel 讀取失敗: {str(e)}")


def find_col(df: pd.DataFrame, name: str) -> str:
    col_map = {str(c).strip().lower(): c for c in df.columns}
    key = name.strip().lower()
    if key in col_map:
        return col_map[key]
    raise HTTPException(status_code=400, detail=f"缺少欄位 {name}")


def safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
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
                "application/zip": {}
            },
            "description": "Download ZIP result"
        }
    }
)
async def trace_materials(
    issue_file: UploadFile = File(..., description="工單耗用檔"),
    workorder_file: UploadFile = File(..., description="工單生產檔")
):
    try:
        # -----------------------------
        # 1) 讀檔
        # -----------------------------
        issue = read_excel(issue_file)
        wo = read_excel(workorder_file)

        # 工單耗用欄位
        issue_order = find_col(issue, "Order")
        issue_plant = find_col(issue, "Plant")
        issue_material = find_col(issue, "Material")
        issue_desc = find_col(issue, "Material Description")
        issue_req = find_col(issue, "Requirement quantity (EINHEIT)")
        issue_withdraw = find_col(issue, "Quantity withdrawn (EINHEIT)")
        issue_uom = find_col(issue, "Base Unit of Measure (=EINHEIT)")

        # 工單生產欄位
        wo_order = find_col(wo, "Order")
        wo_plant = find_col(wo, "Plant")
        wo_product = find_col(wo, "Material Number")
        wo_prod_desc = find_col(wo, "Material description")
        wo_order_qty = find_col(wo, "Order quantity (GMEIN)")
        wo_delivered_qty = find_col(wo, "Delivered quantity (GMEIN)")

        issue[issue_order] = issue[issue_order].astype(str).str.strip()
        wo[wo_order] = wo[wo_order].astype(str).str.strip()

        issue["Requirement Qty Num"] = safe_num(issue[issue_req])
        issue["Withdrawn Qty Num"] = safe_num(issue[issue_withdraw])
        wo["Order Qty Num"] = safe_num(wo[wo_order_qty])
        wo["Delivered Qty Num"] = safe_num(wo[wo_delivered_qty])

        # -----------------------------
        # 2) 整理第一階資料
        # -----------------------------
        issue_small = issue[
            [
                issue_order,
                issue_plant,
                issue_material,
                issue_desc,
                issue_req,
                issue_withdraw,
                issue_uom,
                "Requirement Qty Num",
                "Withdrawn Qty Num"
            ]
        ].copy()

        wo_small = wo[
            [
                wo_order,
                wo_plant,
                wo_product,
                wo_prod_desc,
                wo_order_qty,
                wo_delivered_qty,
                "Order Qty Num",
                "Delivered Qty Num"
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
            wo_prod_desc: "Product Description",
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
            "Base Unit of Measure": merged["Base Unit of Measure"],
            "Order Qty Num": merged["Order Qty Num"],
            "Delivered Qty Num": merged["Delivered Qty Num"],
            "Requirement Qty Num": merged["Requirement Qty Num"],
            "Withdrawn Qty Num": merged["Withdrawn Qty Num"],
        })

        trace_detail = trace_detail.sort_values(
            by=["Order", "Product Material Number", "Material"],
            na_position="last"
        )

        trace_detail_export = trace_detail[
            [
                "Order",
                "Plant",
                "Product Material Number",
                "Product Description",
                "Order Qty",
                "Delivered Qty",
                "Material",
                "Material Description",
                "Requirement Qty",
                "Withdrawn Qty",
                "Base Unit of Measure"
            ]
        ].copy()

        trace_summary = (
            trace_detail
            .dropna(subset=["Material"])
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
            )[["Requirement Qty Num", "Withdrawn Qty Num"]]
            .sum()
            .rename(columns={
                "Requirement Qty Num": "Requirement Qty",
                "Withdrawn Qty Num": "Withdrawn Qty"
            })
        )

        product_summary = (
            trace_detail[
                [
                    "Order",
                    "Plant",
                    "Product Material Number",
                    "Product Description",
                    "Order Qty",
                    "Delivered Qty",
                    "Order Qty Num",
                    "Delivered Qty Num"
                ]
            ]
            .drop_duplicates()
            .sort_values(by=["Order", "Product Material Number"])
        )

        # -----------------------------
        # 3) 建立半品集合
        #    規則：Material 若也出現在 Product Material Number，就視為半品
        # -----------------------------
        semifinished_set = set(
            wo_small["Product Material Number"].dropna().astype(str).str.strip().unique()
        )

        # -----------------------------
        # 4) 建立半品「每1單位」實際用料比例
        #    全部用實際量：
        #    Unit Actual Usage = Withdrawn Qty Num / Delivered Qty Num
        # -----------------------------
        bom_ratio_source = trace_detail.copy()

        # 只保留有 Material、有 Product、有完工數量的資料
        bom_ratio_source = bom_ratio_source.dropna(
            subset=["Plant", "Product Material Number", "Material", "Delivered Qty Num", "Withdrawn Qty Num"]
        ).copy()

        # 避免除以 0
        bom_ratio_source = bom_ratio_source[
            (bom_ratio_source["Delivered Qty Num"].notna()) &
            (bom_ratio_source["Delivered Qty Num"] != 0)
        ].copy()

        bom_group = (
            bom_ratio_source
            .groupby(
                [
                    "Plant",
                    "Product Material Number",
                    "Material",
                    "Material Description",
                    "Base Unit of Measure"
                ],
                as_index=False
            )[["Withdrawn Qty Num", "Delivered Qty Num"]]
            .sum()
        )

        bom_group["Unit Actual Usage"] = bom_group["Withdrawn Qty Num"] / bom_group["Delivered Qty Num"]
        bom_group["Unit Actual Usage"] = bom_group["Unit Actual Usage"].fillna(0)

        # mapping: (Plant, 半品料號) -> 下階用料列表
        bom_map = defaultdict(list)
        for _, r in bom_group.iterrows():
            key = (str(r["Plant"]).strip(), str(r["Product Material Number"]).strip())
            bom_map[key].append({
                "Material": "" if pd.isna(r["Material"]) else str(r["Material"]).strip(),
                "Material Description": "" if pd.isna(r["Material Description"]) else str(r["Material Description"]).strip(),
                "Base Unit of Measure": "" if pd.isna(r["Base Unit of Measure"]) else str(r["Base Unit of Measure"]).strip(),
                "Unit Actual Usage": 0 if pd.isna(r["Unit Actual Usage"]) else float(r["Unit Actual Usage"]),
            })

        # -----------------------------
        # 5) 遞迴展開
        # -----------------------------
        exploded_rows = []

        def explode_material(
            root_order: str,
            root_plant: str,
            root_product: str,
            root_product_desc: str,
            parent_material: str,
            parent_material_desc: str,
            current_material: str,
            current_desc: str,
            current_uom: str,
            level: int,
            parent_actual_qty: float,
            path: set
        ):
            # 記錄本層
            is_semifinished = current_material in semifinished_set

            exploded_rows.append({
                "Order": root_order,
                "Plant": root_plant,
                "Top Product Material Number": root_product,
                "Top Product Description": root_product_desc,
                "Level": level,
                "Parent Material": parent_material,
                "Parent Material Description": parent_material_desc,
                "Material": current_material,
                "Material Description": current_desc,
                "Parent Actual Qty": parent_actual_qty,
                "Unit Actual Usage": None,
                "Exploded Actual Qty": parent_actual_qty,
                "Base Unit of Measure": current_uom,
                "Is SemiFinished": "Y" if is_semifinished else "N",
            })

            # 不是半品就停止
            if not is_semifinished:
                return

            # 自己領自己 -> 停止，避免無限迴圈
            if current_material == parent_material:
                return

            cycle_key = (root_plant, current_material)
            if cycle_key in path:
                return

            # 找該半品的下階 BOM
            child_key = (root_plant, current_material)
            children = bom_map.get(child_key, [])

            # 同 plant 找不到時，退而求其次只看料號
            if not children:
                fallback_keys = [k for k in bom_map.keys() if k[1] == current_material]
                if fallback_keys:
                    children = bom_map[fallback_keys[0]]

            if not children:
                return

            new_path = set(path)
            new_path.add(cycle_key)

            for child in children:
                unit_usage = child["Unit Actual Usage"]
                exploded_actual_qty = parent_actual_qty * unit_usage

                exploded_rows.append({
                    "Order": root_order,
                    "Plant": root_plant,
                    "Top Product Material Number": root_product,
                    "Top Product Description": root_product_desc,
                    "Level": level + 1,
                    "Parent Material": current_material,
                    "Parent Material Description": current_desc,
                    "Material": child["Material"],
                    "Material Description": child["Material Description"],
                    "Parent Actual Qty": parent_actual_qty,
                    "Unit Actual Usage": unit_usage,
                    "Exploded Actual Qty": exploded_actual_qty,
                    "Base Unit of Measure": child["Base Unit of Measure"],
                    "Is SemiFinished": "Y" if child["Material"] in semifinished_set else "N",
                })

                # 若下階仍是半品，繼續遞迴
                if child["Material"] in semifinished_set and child["Material"] != current_material:
                    explode_material(
                        root_order=root_order,
                        root_plant=root_plant,
                        root_product=root_product,
                        root_product_desc=root_product_desc,
                        parent_material=current_material,
                        parent_material_desc=current_desc,
                        current_material=child["Material"],
                        current_desc=child["Material Description"],
                        current_uom=child["Base Unit of Measure"],
                        level=level + 1,
                        parent_actual_qty=exploded_actual_qty,
                        path=new_path
                    )

        # 從第一階開始：第一階的 parent_actual_qty 就是實際領用量 Withdrawn Qty Num
        first_level = trace_detail.dropna(subset=["Material"]).copy()

        for _, row in first_level.iterrows():
            root_order = "" if pd.isna(row["Order"]) else str(row["Order"]).strip()
            root_plant = "" if pd.isna(row["Plant"]) else str(row["Plant"]).strip()
            root_product = "" if pd.isna(row["Product Material Number"]) else str(row["Product Material Number"]).strip()
            root_product_desc = "" if pd.isna(row["Product Description"]) else str(row["Product Description"]).strip()

            current_material = "" if pd.isna(row["Material"]) else str(row["Material"]).strip()
            current_desc = "" if pd.isna(row["Material Description"]) else str(row["Material Description"]).strip()
            current_uom = "" if pd.isna(row["Base Unit of Measure"]) else str(row["Base Unit of Measure"]).strip()
            parent_actual_qty = 0 if pd.isna(row["Withdrawn Qty Num"]) else float(row["Withdrawn Qty Num"])

            if current_material == "":
                continue

            # 先記錄第一階
            exploded_rows.append({
                "Order": root_order,
                "Plant": root_plant,
                "Top Product Material Number": root_product,
                "Top Product Description": root_product_desc,
                "Level": 1,
                "Parent Material": root_product,
                "Parent Material Description": root_product_desc,
                "Material": current_material,
                "Material Description": current_desc,
                "Parent Actual Qty": parent_actual_qty,
                "Unit Actual Usage": 1.0,
                "Exploded Actual Qty": parent_actual_qty,
                "Base Unit of Measure": current_uom,
                "Is SemiFinished": "Y" if current_material in semifinished_set else "N",
            })

            # 若第一階是半品，再往下拆
            if current_material in semifinished_set:
                explode_material(
                    root_order=root_order,
                    root_plant=root_plant,
                    root_product=root_product,
                    root_product_desc=root_product_desc,
                    parent_material=root_product,
                    parent_material_desc=root_product_desc,
                    current_material=current_material,
                    current_desc=current_desc,
                    current_uom=current_uom,
                    level=1,
                    parent_actual_qty=parent_actual_qty,
                    path=set()
                )

        bom_explosion_all_levels = pd.DataFrame(exploded_rows)

        # 去掉第一階遞迴重複記錄（因為我們先 append 一次，再 explode 內又 append 一次）
        if not bom_explosion_all_levels.empty:
            bom_explosion_all_levels = bom_explosion_all_levels.drop_duplicates()

            bom_explosion_all_levels = bom_explosion_all_levels.sort_values(
                by=["Order", "Top Product Material Number", "Level", "Parent Material", "Material"],
                na_position="last"
            )

        # 只保留最底層原物料
        bom_explosion_leaf_only = bom_explosion_all_levels[
            bom_explosion_all_levels["Is SemiFinished"] == "N"
        ].copy()

        if not bom_explosion_leaf_only.empty:
            bom_explosion_leaf_only = (
                bom_explosion_leaf_only
                .groupby(
                    [
                        "Order",
                        "Plant",
                        "Top Product Material Number",
                        "Top Product Description",
                        "Material",
                        "Material Description",
                        "Base Unit of Measure"
                    ],
                    as_index=False
                )[["Exploded Actual Qty"]]
                .sum()
            )

        # -----------------------------
        # 6) 輸出 ZIP
        # -----------------------------
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("trace_detail.csv", trace_detail_export.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("trace_summary.csv", trace_summary.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("product_summary.csv", product_summary.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("bom_explosion_all_levels.csv", bom_explosion_all_levels.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("bom_explosion_leaf_only.csv", bom_explosion_leaf_only.to_csv(index=False, encoding="utf-8-sig"))

        zip_buffer.seek(0)

        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": 'attachment; filename="sap_workorder_bom_explosion_actual.zip"'
            }
        )

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
