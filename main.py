import os
import pandas as pd
import re
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any

# ==================== CONFIGURATION ====================

PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'

# ==================== GLOBAL DATA ====================

df_global = None

def load_data():
    """Load Excel file"""
    global df_global
    try:
        excel_file = None
        for fn in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(fn):
                excel_file = fn
                break
        
        if excel_file is None:
            print("⚠ Excel file not found")
            df_global = pd.DataFrame()
            return
        
        print(f"✓ Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"✓ Loaded {len(df_global)} rows, {len(df_global.columns)} cols")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        df_global = pd.DataFrame()

load_data()

# ==================== HELPER FUNCTIONS ====================

def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or remark == '-' or remark == '':
        return None
    matches = re.findall(r'\bM[1-4]\b', str(remark))
    return matches if matches else None

def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict - TESTED LOCALLY"""
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
        print(f"Error converting row: {str(e)}")
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

# ==================== API ENDPOINTS ====================

@app.get("/api/dashboard/statistics")
async def statistics():
    """Dashboard statistics"""
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
        print(f"Error in statistics: {str(e)}")
        return {"total_vehicles": 0, "mechanical_count": 0, "bodyshop_count": 0, "accessories_count": 0}

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
    except:
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}

@app.get("/api/filter-options/bodyshop")
async def bs_filters():
    """Bodyshop filters"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"]}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        mjobs = set(['Not Categorized'])
        for r in df['RO Remarks']:
            m = extract_mjobs(r)
            if m:
                mjobs.update(m)
        
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()]),
            "mjobs": ["All"] + sorted(list(mjobs))
        }
    except:
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"]}

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
    except:
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}

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
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in mechanical: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

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
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket)
        
        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                df = df[df['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
            else:
                df = df[df['RO Remarks'].apply(lambda x: mjob in (extract_mjobs(x) or []))]
        
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in bodyshop: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

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
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in accessories: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/export/mechanical")
async def export_mech(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except:
        return {"vehicles": []}

@app.get("/api/export/bodyshop")
async def export_bs(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All"), mjob: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        df = apply_filters(df, branch, ro_status, age_bucket)
        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                df = df[df['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
            else:
                df = df[df['RO Remarks'].apply(lambda x: mjob in (extract_mjobs(x) or []))]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except:
        return {"vehicles": []}

@app.get("/api/export/accessories")
async def export_acc(branch: Optional[str] = Query("All"), ro_status: Optional[str] = Query("All"), age_bucket: Optional[str] = Query("All")):
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except:
        return {"vehicles": []}

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
