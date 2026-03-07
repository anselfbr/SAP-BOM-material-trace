from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd
import os
import io
import re
import uuid
import json
import zipfile
import traceback
from threading import Thread, Lock
from collections import defaultdict
from datetime import datetime

app = FastAPI(title="SAP BOM Material Trace SaaS")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "job_data")
os.makedirs(DATA_DIR, exist_ok=True)

JOBS_FILE = os.path.join(DATA_DIR, "jobs.json")
jobs_lock = Lock()


def load_jobs():
    if not os.path.exists(JOBS_FILE):
        return {}
    with open(JOBS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_jobs(jobs):
    with open(JOBS_FILE, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)


def update_job(job_id, **kwargs):
    with jobs_lock:
        jobs = load_jobs()
        if job_id not in jobs:
            jobs[job_id] = {}
        jobs[job_id].update(kwargs)
        save_jobs(jobs)


def normalize_columns(df: pd.DataFrame):
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]
    return df


def read_excel_path(path: str):
    df = pd.read_excel(path, dtype=str)
    return normalize_columns(df)


def find_col(df: pd.DataFrame, name: str) -> str:
    col_map = {str(c).strip().lower(): c for c in df.columns}
    key = name.strip().lower()
    if key in col_map:
        return col_map[key]
    raise ValueError(f"缺少欄位 {name}")


def safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )


def process_job(job_id: str, issue_path: str, workorder_path: str, output_zip_path: str):
    try:
        update_job(job_id, status="processing", message="讀取 Excel 中...")

        issue = read_excel_path(issue_path)
        wo = read_excel_path(workorder_path)

        update_job(job_id, status="processing", message="辨識欄位中...")

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

        update_job(job_id, status="processing", message="建立第一階 BOM 資料...")

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
                "Withdrawn Qty Num",
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
                "Delivered Qty Num",
            ]
        ].copy()

        issue_small = issue_small.rename(columns={
            issue_order: "Order",
            issue_plant: "Issue Plant",
            issue_material: "Material",
            issue_desc: "Material Description",
            issue_req: "Requirement Qty",
            issue_withdraw: "Withdrawn Qty",
            issue_uom: "Base Unit of Measure",
        })

        wo_small = wo_small.rename(columns={
            wo_order: "Order",
            wo_plant: "Plant",
            wo_product: "Product Material Number",
            wo_prod_desc: "Product Description",
            wo_order_qty: "Order Qty",
            wo_delivered_qty: "Delivered Qty",
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
                "Base Unit of Measure",
            ]
        ].copy()

        update_job(job_id, status="processing", message="建立第一階彙總...")

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
                    "Base Unit of Measure",
                ],
                as_index=False
            )[["Requirement Qty Num", "Withdrawn Qty Num"]]
            .sum()
            .rename(columns={
                "Requirement Qty Num": "Requirement Qty",
                "Withdrawn Qty Num": "Withdrawn Qty",
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
                    "Delivered Qty Num",
                ]
            ]
            .drop_duplicates()
            .sort_values(by=["Order", "Product Material Number"])
        )

        update_job(job_id, status="processing", message="建立半品 map...")

        semifinished_set = set(
            wo_small["Product Material Number"].dropna().astype(str).str.strip().unique()
        )

        bom_ratio_source = trace_detail.dropna(
            subset=["Plant", "Product Material Number", "Material", "Delivered Qty Num", "Withdrawn Qty Num"]
        ).copy()

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
                    "Base Unit of Measure",
                ],
                as_index=False
            )[["Withdrawn Qty Num", "Delivered Qty Num"]]
            .sum()
        )

        bom_group["Unit Actual Usage"] = bom_group["Withdrawn Qty Num"] / bom_group["Delivered Qty Num"]
        bom_group["Unit Actual Usage"] = bom_group["Unit Actual Usage"].fillna(0)

        bom_map = defaultdict(list)
        for _, r in bom_group.iterrows():
            key = (str(r["Plant"]).strip(), str(r["Product Material Number"]).strip())
            bom_map[key].append({
                "Material": "" if pd.isna(r["Material"]) else str(r["Material"]).strip(),
                "Material Description": "" if pd.isna(r["Material Description"]) else str(r["Material Description"]).strip(),
                "Base Unit of Measure": "" if pd.isna(r["Base Unit of Measure"]) else str(r["Base Unit of Measure"]).strip(),
                "Unit Actual Usage": 0 if pd.isna(r["Unit Actual Usage"]) else float(r["Unit Actual Usage"]),
            })

        update_job(job_id, status="processing", message="進行多階 BOM 展開...")

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
            if current_material not in semifinished_set:
                return

            if current_material == parent_material:
                return

            cycle_key = (root_plant, current_material)
            if cycle_key in path:
                return

            child_key = (root_plant, current_material)
            children = bom_map.get(child_key, [])

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

        first_level = trace_detail.dropna(subset=["Material"]).copy()

        total_rows = len(first_level)
        for idx, (_, row) in enumerate(first_level.iterrows(), start=1):
            if idx % 5000 == 0:
                update_job(
                    job_id,
                    status="processing",
                    message=f"多階展開中... {idx}/{total_rows}"
                )

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

        update_job(job_id, status="processing", message="整理展開結果...")

        bom_explosion_all_levels = pd.DataFrame(exploded_rows)

        if not bom_explosion_all_levels.empty:
            bom_explosion_all_levels = bom_explosion_all_levels.drop_duplicates()
            bom_explosion_all_levels = bom_explosion_all_levels.sort_values(
                by=["Order", "Top Product Material Number", "Level", "Parent Material", "Material"],
                na_position="last"
            )

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
                        "Base Unit of Measure",
                    ],
                    as_index=False
                )[["Exploded Actual Qty"]]
                .sum()
            )

        update_job(job_id, status="processing", message="寫出結果檔...")

        with zipfile.ZipFile(output_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("trace_detail.csv", trace_detail_export.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("trace_summary.csv", trace_summary.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("product_summary.csv", product_summary.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("bom_explosion_all_levels.csv", bom_explosion_all_levels.to_csv(index=False, encoding="utf-8-sig"))
            zf.writestr("bom_explosion_leaf_only.csv", bom_explosion_leaf_only.to_csv(index=False, encoding="utf-8-sig"))

        update_job(
            job_id,
            status="finished",
            message="完成",
            output_file=os.path.basename(output_zip_path),
            finished_at=datetime.now().isoformat()
        )

    except Exception as e:
        traceback.print_exc()
        update_job(
            job_id,
            status="failed",
            message=str(e),
            finished_at=datetime.now().isoformat()
        )


@app.get("/api/jobs")
def list_jobs():
    return load_jobs()


@app.post("/api/upload")
async def upload_files(
    issue_file: UploadFile = File(..., description="工單耗用檔"),
    workorder_file: UploadFile = File(..., description="工單生產檔")
):
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(DATA_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    issue_path = os.path.join(job_dir, issue_file.filename)
    workorder_path = os.path.join(job_dir, workorder_file.filename)
    output_zip_path = os.path.join(job_dir, f"{job_id}_result.zip")

    with open(issue_path, "wb") as f:
        f.write(await issue_file.read())

    with open(workorder_path, "wb") as f:
        f.write(await workorder_file.read())

    update_job(
        job_id,
        status="queued",
        message="已上傳，等待背景處理",
        created_at=datetime.now().isoformat(),
        issue_file=issue_file.filename,
        workorder_file=workorder_file.filename,
        output_file=None
    )

    t = Thread(
        target=process_job,
        args=(job_id, issue_path, workorder_path, output_zip_path),
        daemon=True
    )
    t.start()

    return {
        "ok": True,
        "job_id": job_id,
        "status_url": f"/api/status/{job_id}",
        "download_url": f"/api/download/{job_id}"
    }


@app.get("/api/status/{job_id}")
def job_status(job_id: str):
    jobs = load_jobs()
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="job not found")
    return jobs[job_id]


@app.get("/api/download/{job_id}")
def download_result(job_id: str):
    jobs = load_jobs()
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="job not found")

    job = jobs[job_id]
    if job.get("status") != "finished":
        raise HTTPException(status_code=400, detail="job not finished")

    job_dir = os.path.join(DATA_DIR, job_id)
    output_file = job.get("output_file")
    file_path = os.path.join(job_dir, output_file)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="result file not found")

    return FileResponse(
        path=file_path,
        media_type="application/zip",
        filename=output_file
    )
