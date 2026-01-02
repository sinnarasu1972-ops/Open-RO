import os
import re
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# ==================== CONFIGURATION ====================

PORT = int(os.getenv("PORT", 8000))
HOST = "0.0.0.0"

# ==================== GLOBAL DATA ====================

df_global = None
df_landed_cost = None
df_billable_type = None
df_model_group = None
ro_remarks_list = []  # Standard RO Remarks

# ==================== DATE HELPERS ====================

def parse_date_any(date_value):
    """
    Robust date parser for RO Date values coming from Excel.
    Supports datetime / Timestamp, ISO, DD/MM/YYYY, DD-MM-YYYY etc.
    Returns pandas.Timestamp or NaT.
    """
    if date_value is None or pd.isna(date_value):
        return pd.NaT

    if isinstance(date_value, (pd.Timestamp, datetime)):
        try:
            return pd.to_datetime(date_value)
        except:
            return pd.NaT

    s = str(date_value).strip()
    if s in ["", "-", "nan", "NaT", "None"]:
        return pd.NaT

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%b-%Y", "%d %b %Y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except:
            pass

    try:
        return pd.to_datetime(s, errors="coerce", dayfirst=True)
    except:
        return pd.NaT


def ensure_ro_date_dt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds/refreshes helper datetime column RO_DATE_DT used for sorting/filtering.
    Keeps existing 'RO Date' as-is for display.
    """
    if df is None or df.empty:
        return df

    if "RO Date" in df.columns:
        df["RO_DATE_DT"] = df["RO Date"].apply(parse_date_any)
    else:
        df["RO_DATE_DT"] = pd.NaT
    return df


def sort_by_ro_date(df: pd.DataFrame, ascending: bool = False) -> pd.DataFrame:
    """
    Sort by RO Date using RO_DATE_DT, not by string.
    ascending=False => latest first
    """
    if df is None or df.empty:
        return df
    if "RO_DATE_DT" not in df.columns:
        df = ensure_ro_date_dt(df)
    return df.sort_values("RO_DATE_DT", ascending=ascending, na_position="last")


# ==================== RO REMARK HELPERS ====================

def load_ro_remarks_dynamically():
    """Load RO remarks dynamically from Excel file - returns latest remarks"""
    try:
        remark_file = None
        for fn in ["RO Remark.xlsx", "RO_Remark.xlsx", "ro_remark.xlsx"]:
            if os.path.exists(fn):
                remark_file = fn
                break

        if remark_file:
            df_remarks = pd.read_excel(remark_file)
            remarks_col = df_remarks.columns[0]
            remarks = [str(x).strip() for x in df_remarks[remarks_col].dropna().unique()]
            return remarks
        return []
    except Exception as e:
        print(f"Warning: Error loading RO remarks dynamically: {e}")
        return []


def get_all_ro_remarks_for_dropdown():
    """Get all RO remarks for dropdown - both standard and those found in data"""
    try:
        standard_remarks = load_ro_remarks_dynamically()

        if df_global is not None and (not df_global.empty) and ("ro_remark_mapped" in df_global.columns):
            mapped_remarks = [
                str(x) for x in df_global["ro_remark_mapped"].dropna().unique()
                if str(x).strip() != "" and str(x) != "Not Assigned"
            ]
            all_remarks = list(set(standard_remarks + mapped_remarks))
            return sorted(all_remarks) if all_remarks else ["All"]
        else:
            return sorted(standard_remarks) if standard_remarks else ["All"]
    except Exception:
        return ["All"]


def map_ro_remark(remark):
    """
    Map RO Remarks to standard codes (case-insensitive).
    Searches for any standard remark code within the remarks text.
    Returns first match found, or "Not Assigned".
    """
    global ro_remarks_list

    if pd.isna(remark) or str(remark).strip() in ["", "-"]:
        return "Not Assigned"

    remarks_list = load_ro_remarks_dynamically()
    if not remarks_list:
        remarks_list = ro_remarks_list  # fallback

    remark_str = str(remark).strip().upper()
    for standard_remark in remarks_list:
        su = str(standard_remark).upper()
        if su and su in remark_str:
            return standard_remark

    return "Not Assigned"


# ==================== LOAD DATA ====================

def load_data():
    """Load Excel files and merge data"""
    global df_global, df_landed_cost, df_billable_type, df_model_group, ro_remarks_list
    try:
        # Load Standard RO Remarks
        remark_file = None
        for fn in ["RO Remark.xlsx", "RO_Remark.xlsx", "ro_remark.xlsx"]:
            if os.path.exists(fn):
                remark_file = fn
                break

        if remark_file:
            df_remarks = pd.read_excel(remark_file)
            remarks_col = df_remarks.columns[0]
            ro_remarks_list = [str(x).strip() for x in df_remarks[remarks_col].dropna().unique()]
        else:
            ro_remarks_list = []

        # Load Model Group mapping
        model_file = None
        for fn in ["Model Group.xlsx", "Model_Group.xlsx", "model_group.xlsx"]:
            if os.path.exists(fn):
                model_file = fn
                break

        if model_file:
            df_model_group = pd.read_excel(model_file)
        else:
            df_model_group = pd.DataFrame()

        # Load Open RO data
        excel_file = None
        for fn in ["Open RO.xlsx", "Open_RO.xlsx", "open_ro.xlsx"]:
            if os.path.exists(fn):
                excel_file = fn
                break

        if excel_file is None:
            df_global = pd.DataFrame()
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            return

        df_global = pd.read_excel(excel_file)

        # Create RO_DATE_DT helper
        df_global = ensure_ro_date_dt(df_global)

        # RO Remark mapping column
        if "RO Remarks" in df_global.columns:
            df_global["ro_remark_mapped"] = df_global["RO Remarks"].apply(map_ro_remark)
        else:
            df_global["ro_remark_mapped"] = "Not Assigned"

        # Merge model group
        if df_model_group is not None and (not df_model_group.empty):
            try:
                if "Model Group" in df_global.columns and "Model Code" in df_model_group.columns:
                    segment_mapping = dict(zip(df_model_group["Model Code"], df_model_group.get("Segment", pd.Series()).fillna("Unknown")))
                    mg_mapping = dict(zip(df_model_group["Model Code"], df_model_group.get("Model Group", df_model_group["Model Code"])))

                    df_global["segment"] = df_global["Model Group"].map(segment_mapping)
                    df_global["model_group_mapped"] = df_global["Model Group"].map(mg_mapping)

                    df_global["segment"] = df_global["segment"].fillna("Unknown")
                    df_global["model_group_mapped"] = df_global["model_group_mapped"].fillna(df_global["Model Group"])

                    df_global["Model Group"] = df_global["model_group_mapped"]
                else:
                    df_global["segment"] = "Unknown"
            except Exception:
                df_global["segment"] = "Unknown"
        else:
            df_global["segment"] = "Unknown"

        # Landed cost + billable type
        parts_file = None
        for fn in ["Part Issue But Not Bill.xlsx", "Part_Issue_But_Not_Bill.xlsx", "part_issue_but_not_bill.xlsx"]:
            if os.path.exists(fn):
                parts_file = fn
                break

        if parts_file is None:
            df_global["total_landed_cost"] = 0.0
            df_global["billable_type"] = "Not Billed"
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
        else:
            df_parts = pd.read_excel(parts_file)

            df_landed_cost = df_parts.groupby("RO Number")["Landed Cost (Total)"].sum().reset_index()
            df_landed_cost.columns = ["RO ID", "total_landed_cost"]

            df_billable_type = df_parts.groupby("RO Number")["Billable Type"].first().reset_index()
            df_billable_type.columns = ["RO ID", "billable_type"]

            df_global = df_global.merge(df_landed_cost, on="RO ID", how="left")
            df_global["total_landed_cost"] = df_global["total_landed_cost"].fillna(0)

            df_global = df_global.merge(df_billable_type, on="RO ID", how="left")
            df_global["billable_type"] = df_global["billable_type"].fillna("Not Billed")

        # Default sort: Latest first
        df_global = sort_by_ro_date(df_global, ascending=False)

    except Exception as e:
        print(f"ERROR load_data: {e}")
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()
        df_billable_type = pd.DataFrame()


load_data()

# ==================== OTHER HELPERS ====================

def parse_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    s = str(date_str).strip()
    if s in ["", "-", "nan", "NaT"]:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except:
            pass
    try:
        return pd.to_datetime(s, errors="coerce", dayfirst=True)
    except:
        return None


def extract_mjobs(remark):
    if pd.isna(remark) or remark in ["-", ""]:
        return None
    remark_str = str(remark).strip()
    matches = re.findall(r"\bM/?[1-4]\b", remark_str)
    return matches if matches else None


def convert_row(row) -> Dict[str, Any]:
    def safe_date_parse(date_value):
        if pd.notna(date_value) and str(date_value).strip() not in ["-", "", "nan", "NaT"]:
            try:
                return pd.Timestamp(date_value).strftime("%Y-%m-%d")
            except:
                return "-"
        return "-"

    return {
        "ro_id": str(row.get("RO ID", "-")).strip() if pd.notna(row.get("RO ID")) else "-",
        "branch": str(row.get("Branch", "-")).strip() if pd.notna(row.get("Branch")) else "-",
        "ro_status": str(row.get("RO Status", "-")).strip() if pd.notna(row.get("RO Status")) else "-",
        "age_bucket": str(row.get("Age Bucket", "-")).strip() if pd.notna(row.get("Age Bucket")) else "-",
        "service_category": str(row.get("SERVC_CATGRY_DESC", "-")).strip() if pd.notna(row.get("SERVC_CATGRY_DESC")) else "-",
        "service_type": str(row.get("SERVC_TYPE_DESC", "-")).strip() if pd.notna(row.get("SERVC_TYPE_DESC")) else "-",
        "vehicle_model": str(row.get("Family", "-")).strip() if pd.notna(row.get("Family")) else "-",
        "model_group": str(row.get("Model Group", "-")).strip() if pd.notna(row.get("Model Group")) else "-",
        "segment": str(row.get("segment", "Unknown")).strip() if pd.notna(row.get("segment")) else "Unknown",
        "reg_number": str(row.get("Reg. Number", "-")).strip() if pd.notna(row.get("Reg. Number")) else "-",
        "ro_date": safe_date_parse(row.get("RO Date")),
        "vehicle_ready_date": safe_date_parse(row.get("Vehicle  Ready Date")),
        "ro_remarks": str(row.get("RO Remarks", "-")).strip() if pd.notna(row.get("RO Remarks")) else "-",
        "ro_remark_mapped": str(row.get("ro_remark_mapped", "Not Assigned")).strip() if pd.notna(row.get("ro_remark_mapped")) else "Not Assigned",
        "km": int(row.get("KM", 0)) if pd.notna(row.get("KM")) else 0,
        "days": int(row.get("Days", 0)) if pd.notna(row.get("Days")) else 0,
        "days_open": int(row.get("[No of Visits (In last 90 days)]", 0)) if pd.notna(row.get("[No of Visits (In last 90 days)]")) else 0,
        "service_adviser": str(row.get("Service Adviser Name", "-")).strip() if pd.notna(row.get("Service Adviser Name")) else "-",
        "vin": str(row.get("VIN", "-")).strip() if pd.notna(row.get("VIN")) else "-",
        "pendncy_resn_desc": str(row.get("PENDNCY_RESN_DESC", "-")).strip() if pd.notna(row.get("PENDNCY_RESN_DESC")) else "-",
        "total_landed_cost": round(float(row.get("total_landed_cost", 0.0)), 2) if pd.notna(row.get("total_landed_cost")) else 0.0,
        "billable_type": str(row.get("billable_type", "Not Billed")).strip() if pd.notna(row.get("billable_type")) else "Not Billed",
    }


def apply_filters(
    df,
    branch,
    ro_status,
    age_bucket,
    mjob=None,
    billable_type=None,
    reg_number=None,
    service_type=None,
    sa_name=None,
    segment=None,
    ro_remark=None,
    pending_reason=None,
    from_date=None,
    to_date=None
):
    result = df.copy()

    if branch and branch != "All":
        result = result[result["Branch"] == branch]

    if ro_status and ro_status != "All":
        result = result[result["RO Status"] == ro_status]

    if age_bucket and age_bucket != "All":
        result = result[result["Age Bucket"] == age_bucket]

    if billable_type and billable_type != "All":
        result = result[result["billable_type"] == billable_type]

    if service_type and service_type != "All":
        result = result[result["SERVC_TYPE_DESC"] == service_type]

    if sa_name and sa_name != "All":
        result = result[result["Service Adviser Name"] == sa_name]

    if segment and segment != "All":
        result = result[result["segment"] == segment]

    if ro_remark and ro_remark != "All":
        result = result[result["ro_remark_mapped"] == ro_remark]

    if pending_reason and pending_reason != "All":
        result = result[result["PENDNCY_RESN_DESC"] == pending_reason]

    if "RO_DATE_DT" not in result.columns:
        result = ensure_ro_date_dt(result)

    if from_date:
        fd = parse_date(from_date)
        if fd is not None and not pd.isna(fd):
            result = result[result["RO_DATE_DT"] >= fd]

    if to_date:
        td = parse_date(to_date)
        if td is not None and not pd.isna(td):
            result = result[result["RO_DATE_DT"] <= td]

    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result["RO Remarks"].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result["RO Remarks"].apply(
                lambda x: any(m.upper() in [search_mjob, search_mjob.replace("/", "")]
                              for m in (extract_mjobs(x) or []))
            )]

    if reg_number and reg_number.strip() != "":
        search_reg = reg_number.strip().upper()
        result = result[result["Reg. Number"].astype(str).str.upper().str.contains(search_reg, na=False)]

    return result


# ==================== APP ====================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== CATEGORY FILTER ====================

def df_by_category(service_category: str):
    """
    service_category:
      mechanical, bodyshop, accessories, presale, all
    """
    if df_global is None or df_global.empty:
        return pd.DataFrame()

    if service_category == "mechanical":
        return df_global[df_global["SERVC_CATGRY_DESC"].isin(["Repair", "Paid Service", "Free Service"])].copy()
    if service_category == "bodyshop":
        return df_global[df_global["SERVC_CATGRY_DESC"] == "Bodyshop"].copy()
    if service_category == "accessories":
        return df_global[df_global["SERVC_CATGRY_DESC"] == "Accessories"].copy()
    if service_category == "presale":
        return df_global[df_global["SERVC_CATGRY_DESC"] == "Pre-Sale/PDI"].copy()

    return df_global.copy()


# ==================== STATS ====================

@app.get("/api/dashboard/statistics")
async def statistics():
    try:
        if df_global is None or df_global.empty:
            return {
                "total_vehicles": 0,
                "mechanical_count": 0,
                "bodyshop_count": 0,
                "accessories_count": 0,
                "presale_count": 0,
                "total_landed_cost": 0.0
            }

        return {
            "total_vehicles": int(len(df_global)),
            "mechanical_count": int(len(df_by_category("mechanical"))),
            "bodyshop_count": int(len(df_by_category("bodyshop"))),
            "accessories_count": int(len(df_by_category("accessories"))),
            "presale_count": int(len(df_by_category("presale"))),
            "total_landed_cost": float(df_global["total_landed_cost"].sum()) if "total_landed_cost" in df_global.columns else 0.0
        }
    except Exception:
        return {"total_vehicles": 0, "mechanical_count": 0, "bodyshop_count": 0, "accessories_count": 0, "presale_count": 0, "total_landed_cost": 0.0}


# ==================== VEHICLES (TABLE) ====================

@app.get("/api/vehicles")
async def get_vehicles(
    service_category: Optional[str] = Query("all"),  # mechanical/bodyshop/accessories/presale/all
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    skip: int = Query(0),
    limit: int = Query(50),
):
    """
    IMPORTANT:
    - limit=0 means ALL rows (dropdown >50)
    - service_category controls which tab is active; if all, shows all (Clear All)
    """
    try:
        df = df_by_category(service_category)
        if df.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}

        total = len(df)

        # Only bodyshop uses mjob filter; others ignore it safely
        use_mjob = mjob if service_category == "bodyshop" else None

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            mjob=use_mjob,
            billable_type=billable_type,
            reg_number=reg_number,
            service_type=service_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            pending_reason=pending_reason,
            from_date=from_date,
            to_date=to_date
        )

        df = sort_by_ro_date(df, ascending=False)

        filtered = len(df)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]
        # else limit=0 => all rows

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}

    except Exception as e:
        print(f"Error get_vehicles: {e}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}


# ==================== EXPORT (SAME AS TABLE) ====================

@app.get("/api/export")
async def export_vehicles(
    service_category: Optional[str] = Query("all"),
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    service_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark: Optional[str] = Query("All"),
    pending_reason: Optional[str] = Query("All"),
    reg_number: Optional[str] = Query(""),
    from_date: Optional[str] = Query(""),
    to_date: Optional[str] = Query(""),
    skip: int = Query(0),
    limit: int = Query(0),   # 0 = ALL
):
    """
    Export follows SAME logic as table:
    - service_category decides which tab
    - limit=0 means export ALL filtered rows (dropdown >50)
    - if limit>0 export only those rows
    """
    try:
        df = df_by_category(service_category)
        if df.empty:
            return {"vehicles": []}

        use_mjob = mjob if service_category == "bodyshop" else None

        df = apply_filters(
            df, branch, ro_status, age_bucket,
            mjob=use_mjob,
            billable_type=billable_type,
            reg_number=reg_number,
            service_type=service_type,
            sa_name=sa_name,
            segment=segment,
            ro_remark=ro_remark,
            pending_reason=pending_reason,
            from_date=from_date,
            to_date=to_date
        )

        df = sort_by_ro_date(df, ascending=False)

        if limit and limit > 0:
            df = df.iloc[skip: skip + limit]

        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}

    except Exception as e:
        print(f"Error export_vehicles: {e}")
        return {"vehicles": []}


# ==================== DASHBOARD PAGE ====================

@app.get("/")
async def dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}


@app.get("/health")
async def health():
    return {"status": "healthy", "records": int(len(df_global)) if df_global is not None and not df_global.empty else 0}


# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"Running on http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
