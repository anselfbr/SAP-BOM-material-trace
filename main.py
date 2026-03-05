from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI(title="SAP BOM Material Trace API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- helpers ----------

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    """Pick the first existing column from candidates."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    raise KeyError(f"Missing column. Tried: {candidates}. Existing: {list(df.columns)}")

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def to_number(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except:
        return None

# ---------- routes ----------

@app.get("/")
def home():
    return {"message": "SAP Material Trace API Running"}

@app.post("/api/upload")
async def upload_files(
    issue_file: UploadFile = File(...),      # 工單耗用(領料)
    workorder_file: UploadFile = File(...),  # 工單生產(產品)
):
    try:
        # 讀 Excel
        issue_df = pd.read_excel(issue_file.file)
        wo_df = pd.read_excel(workorder_file.file)
        issue_df = normalize_cols(issue_df)
        wo_df = normalize_cols(wo_df)

        # ---- 欄位對應（耗用檔）----
        issue_order_col = pick_col(issue_df, ["Order", "order", "Production order", "Prod. order"])
        issue_mat_col   = pick_col(issue_df, ["Material", "Material Number", "Component", "Component material"])
        issue_desc_col  = None
        for cand in ["Material Description", "Description", "Material description"]:
            if cand in issue_df.columns:
                issue_desc_col = cand
                break

        # 你截圖看到有 Quantity withdrawn / Base Unit of Measure
        issue_qty_col = None
        for cand in ["Quantity withdrawn (EINHEIT)", "Quantity withdrawn", "Withdrawn qty", "Qty withdrawn"]:
            if cand in issue_df.columns:
                issue_qty_col = cand
                break

        issue_uom_col = None
        for cand in ["Base Unit of Measure (=EINHEIT)", "Base Unit of Measure", "UoM", "Unit"]:
            if cand in issue_df.columns:
                issue_uom_col = cand
                break

        issue_plant_col = None
        for cand in ["Plant", "plant"]:
            if cand in issue_df.columns:
                issue_plant_col = cand
                break

        # ---- 欄位對應（生產檔）----
        wo_order_col = pick_col(wo_df, ["Order", "order", "Production order", "Prod. order"])
        # 你說的：Material Number = 產品料號
        wo_product_col = None
        for cand in ["Material Number", "Material", "Product", "Header material", "Material number"]:
            if cand in wo_df.columns:
                wo_product_col = cand
                break
        if not wo_product_col:
            raise KeyError(f"Cannot find product column in workorder file. Existing: {list(wo_df.columns)}")

        wo_plant_col = None
        for cand in ["Plant", "plant"]:
            if cand in wo_df.columns:
                wo_plant_col = cand
                break

        # ---- 只保留需要欄位 ----
        issue_keep = [issue_order_col, issue_mat_col]
        if issue_desc_col: issue_keep.append(issue_desc_col)
        if issue_qty_col: issue_keep.append(issue_qty_col)
        if issue_uom_col: issue_keep.append(issue_uom_col)
        if issue_plant_col: issue_keep.append(issue_plant_col)

        wo_keep = [wo_order_col, wo_product_col]
        if wo_plant_col: wo_keep.append(wo_plant_col)

        issue_s = issue_df[issue_keep].copy()
        wo_s = wo_df[wo_keep].copy()

        # ---- 統一欄位名稱 ----
        issue_s.rename(columns={
            issue_order_col: "Order",
            issue_mat_col: "InputMaterial",
            **({issue_desc_col: "InputDescription"} if issue_desc_col else {}),
            **({issue_qty_col: "QtyWithdrawn"} if issue_qty_col else {}),
            **({issue_uom_col: "UoM"} if issue_uom_col else {}),
            **({issue_plant_col: "Plant"} if issue_plant_col else {}),
        }, inplace=True)

        wo_s.rename(columns={
            wo_order_col: "Order",
            wo_product_col: "ProductMaterial",
            **({wo_plant_col: "Plant"} if wo_plant_col else {}),
        }, inplace=True)

        # ---- Order 清洗 ----
        issue_s["Order"] = issue_s["Order"].apply(safe_str)
        wo_s["Order"] = wo_s["Order"].apply(safe_str)

        # ---- 建立 Order -> Inputs list ----
        inputs_by_order = {}
        for _, r in issue_s.iterrows():
            order = safe_str(r.get("Order"))
            if not order:
                continue
            item = {
                "Material": safe_str(r.get("InputMaterial")),
                "Description": safe_str(r.get("InputDescription", "")),
                "QtyWithdrawn": to_number(r.get("QtyWithdrawn", None)),
                "UoM": safe_str(r.get("UoM", "")),
            }
            # 可選：Plant
            if "Plant" in issue_s.columns:
                item["Plant"] = safe_str(r.get("Plant"))
            inputs_by_order.setdefault(order, []).append(item)

        # ---- 組合結果：每張工單（Order）-> 產品料號 + 投入原物料 ----
        results = []
        for _, r in wo_s.iterrows():
            order = safe_str(r.get("Order"))
            if not order:
                continue
            obj = {
                "Order": order,
                "ProductMaterial": safe_str(r.get("ProductMaterial")),
                "Inputs": inputs_by_order.get(order, []),
            }
            # Plant：優先生產檔，其次耗用檔
            if "Plant" in wo_s.columns and safe_str(r.get("Plant")):
                obj["Plant"] = safe_str(r.get("Plant"))
            elif inputs_by_order.get(order):
                # 從第一筆 input 取 plant
                p = inputs_by_order[order][0].get("Plant")
                if p:
                    obj["Plant"] = p

            results.append(obj)

        return {
            "summary": {
                "workorders": len(wo_s),
                "issue_rows": len(issue_s),
                "matched_orders": sum(1 for o in wo_s["Order"].unique() if o in inputs_by_order),
            },
            "data": results[:200],  # 先回傳前200筆，避免太大；需要可改成全部或加分頁
        }

    except KeyError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {e}")
