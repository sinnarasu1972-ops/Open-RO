import os
import pandas as pd
import re
import traceback
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

# ==================== CONFIGURATION ====================

PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'

# ==================== DATA ====================

df_global = None

def load_data():
    """Load Excel file"""
    global df_global
    try:
        for fn in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(fn):
                print(f"✓ Loading: {fn}")
                df_global = pd.read_excel(fn)
                print(f"✓ Loaded {len(df_global)} rows")
                return
        print("⚠ Excel file not found")
        df_global = pd.DataFrame()
    except Exception as e:
        print(f"✗ Error loading Excel: {str(e)}")
        df_global = pd.DataFrame()

load_data()

# ==================== HELPERS ====================

def convert_row(row):
    """Convert pandas row to dict"""
    try:
        return {
            'ro_id': str(row['RO ID']).strip() if pd.notna(row['RO ID']) else '-',
            'branch': str(row['Branch']).strip() if pd.notna(row['Branch']) else '-',
            'ro_status': str(row['RO Status']).strip() if pd.notna(row['RO Status']) else '-',
            'age_bucket': str(row['Age Bucket']).strip() if pd.notna(row['Age Bucket']) else '-',
            'service_category': str(row['SERVC_CATGRY_DESC']).strip() if pd.notna(row['SERVC_CATGRY_DESC']) else '-',
            'vehicle_model': str(row['Family']).strip() if pd.notna(row['Family']) else '-',
            'model_group': str(row['Model Group']).strip() if pd.notna(row['Model Group']) else '-',
            'reg_number': str(row['Reg. Number']).strip() if pd.notna(row['Reg. Number']) else '-',
            'ro_date': pd.Timestamp(row['RO Date']).strftime('%Y-%m-%d') if pd.notna(row['RO Date']) else '-',
            'ro_remarks': str(row['RO Remarks']).strip() if pd.notna(row['RO Remarks']) else '-',
            'km': int(row['KM']) if pd.notna(row['KM']) else 0,
            'days': int(row['Days']) if pd.notna(row['Days']) else 0,
            'days_open': int(row['[No of Visits (In last 90 days)]']) if pd.notna(row['[No of Visits (In last 90 days)]']) else 0,
            'service_adviser': str(row['Service Adviser Name']).strip() if pd.notna(row['Service Adviser Name']) else '-',
            'vin': str(row['VIN']).strip() if pd.notna(row['VIN']) else '-',
            'pendncy_resn_desc': str(row['PENDNCY_RESN_DESC']).strip() if pd.notna(row['PENDNCY_RESN_DESC']) else '-',
        }
    except Exception as e:
        print(f"ERROR in convert_row: {str(e)}")
        print(traceback.format_exc())
        raise

def apply_filters(df, branch, ro_status, age_bucket):
    """Apply filters"""
    result = df.copy()
    if branch and branch != "All":
        result = result[result['Branch'] == branch]
    if ro_status and ro_status != "All":
        result = result[result['RO Status'] == ro_status]
    if age_bucket and age_bucket != "All":
        result = result[result['Age Bucket'] == age_bucket]
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

# ==================== API ====================

@app.get("/api/dashboard/statistics")
async def statistics():
    """Statistics"""
    try:
        if df_global.empty:
            return {"total_vehicles": 0, "mechanical_count": 0, "bodyshop_count": 0, "accessories_count": 0}
        return {
            "total_vehicles": int(len(df_global)),
            "mechanical_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])])),
            "bodyshop_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'])),
            "accessories_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']))
        }
    except Exception as e:
        print(f"ERROR in statistics: {str(e)}")
        return {"error": str(e)}

@app.get("/api/filter-options/mechanical")
async def mech_filters():
    """Mechanical filters"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()])
        }
    except Exception as e:
        print(f"ERROR in mech_filters: {str(e)}")
        return {"error": str(e)}

@app.get("/api/filter-options/bodyshop")
async def bs_filters():
    """Bodyshop filters"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"]}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()]),
            "mjobs": ["All", "Not Categorized"]
        }
    except Exception as e:
        print(f"ERROR in bs_filters: {str(e)}")
        return {"error": str(e)}

@app.get("/api/filter-options/accessories")
async def acc_filters():
    """Accessories filters"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()])
        }
    except Exception as e:
        print(f"ERROR in acc_filters: {str(e)}")
        return {"error": str(e)}

@app.get("/api/vehicles/mechanical")
async def get_mechanical(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get mechanical vehicles"""
    try:
        print(f"DEBUG: get_mechanical called with branch={branch}, ro_status={ro_status}, age_bucket={age_bucket}")
        
        if df_global.empty:
            print("DEBUG: df_global is empty!")
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        print(f"DEBUG: df_global has {len(df_global)} rows")
        
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
        print(f"DEBUG: After service filter: {len(df)} rows")
        
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket)
        print(f"DEBUG: After apply_filters: {len(df)} rows")
        
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        print(f"DEBUG: After pagination: {len(df)} rows")
        
        vehicles = []
        for idx, row in df.iterrows():
            try:
                v = convert_row(row)
                vehicles.append(v)
            except Exception as e:
                print(f"ERROR converting row {idx}: {str(e)}")
                raise
        
        print(f"DEBUG: Successfully converted {len(vehicles)} vehicles")
        return {"total_count": int(total), "filtered_count": int(filtered), "vehicles": vehicles}
    except Exception as e:
        print(f"ERROR in get_mechanical: {str(e)}")
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc(), "total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/vehicles/bodyshop")
async def get_bodyshop(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get bodyshop vehicles"""
    try:
        print(f"DEBUG: get_bodyshop called")
        
        if df_global.empty:
            print("DEBUG: df_global is empty!")
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        print(f"DEBUG: After service filter: {len(df)} rows")
        
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        
        vehicles = []
        for idx, row in df.iterrows():
            try:
                v = convert_row(row)
                vehicles.append(v)
            except Exception as e:
                print(f"ERROR converting row {idx}: {str(e)}")
                raise
        
        print(f"DEBUG: Successfully converted {len(vehicles)} vehicles")
        return {"total_count": int(total), "filtered_count": int(filtered), "vehicles": vehicles}
    except Exception as e:
        print(f"ERROR in get_bodyshop: {str(e)}")
        print(traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc(), "total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/vehicles/accessories")
async def get_accessories(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get accessories vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"total_count": int(total), "filtered_count": int(filtered), "vehicles": vehicles}
    except Exception as e:
        print(f"ERROR in get_accessories: {str(e)}")
        return {"error": str(e), "total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/export/mechanical")
async def export_mech(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/export/bodyshop")
async def export_bs(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All"), mjob: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/export/accessories")
async def export_acc(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        return {"error": str(e)}

@app.get("/")
async def dashboard():
    """Serve dashboard"""
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "records": len(df_global) if not df_global.empty else 0}

# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Running on http://0.0.0.0:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
