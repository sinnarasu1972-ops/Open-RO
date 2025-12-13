import os
import pandas as pd
import re
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime

# ==================== CONFIGURATION ====================

PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'

# ==================== DATA LOADING ====================

df_global = None

def load_data():
    """Load data from Excel file"""
    global df_global
    try:
        excel_file = None
        for filename in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(filename):
                excel_file = filename
                break
        
        if excel_file is None:
            print("⚠ Excel file not found in current directory")
            df_global = pd.DataFrame()
            return
        
        print(f"✓ Loading Excel file: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"✓ Loaded {len(df_global)} records")
        print(f"✓ Columns: {list(df_global.columns)}")
    except Exception as e:
        print(f"✗ Error loading Excel file: {str(e)}")
        df_global = pd.DataFrame()

load_data()

# ==================== HELPER FUNCTIONS ====================

def extract_mjobs(remark):
    """Extract MJobs from RO Remarks"""
    if pd.isna(remark) or remark == '-':
        return None
    matches = re.findall(r'\bM[1-4]\b', str(remark))
    return matches if matches else None

def safe_str(val):
    """Safely convert value to string"""
    if pd.isna(val):
        return '-'
    s = str(val).strip()
    return s if s else '-'

def format_date(val):
    """Format date to string"""
    if pd.isna(val):
        return '-'
    try:
        if isinstance(val, str):
            return val
        return pd.Timestamp(val).strftime('%Y-%m-%d')
    except:
        return str(val)

def convert_row_to_dict(row) -> Dict[str, Any]:
    """Convert dataframe row to dictionary"""
    try:
        return {
            'ro_id': safe_str(row['RO ID']),
            'branch': safe_str(row['Branch']),
            'ro_status': safe_str(row['RO Status']),
            'age_bucket': safe_str(row['Age Bucket']),
            'service_category': safe_str(row['SERVC_CATGRY_DESC']),
            'vehicle_model': safe_str(row['Family']),
            'model_group': safe_str(row['Model Group']),
            'reg_number': safe_str(row['Reg. Number']),
            'ro_date': format_date(row['RO Date']),
            'ro_remarks': safe_str(row['RO Remarks']),
            'km': int(row['KM']) if pd.notna(row['KM']) else 0,
            'days': int(row['Days']) if pd.notna(row['Days']) else 0,
            'days_open': int(row['[No of Visits (In last 90 days)]']) if pd.notna(row['[No of Visits (In last 90 days)]']) else 0,
            'service_adviser': safe_str(row['Service Adviser Name']),
            'vin': safe_str(row['VIN']),
            'pendncy_resn_desc': safe_str(row['PENDNCY_RESN_DESC']),
        }
    except Exception as e:
        print(f"Error converting row: {str(e)}")
        raise

def apply_filters(df, branch, ro_status, age_bucket):
    """Apply filters to dataframe"""
    result_df = df.copy()
    if branch and branch != "All":
        result_df = result_df[result_df['Branch'] == branch]
    if ro_status and ro_status != "All":
        result_df = result_df[result_df['RO Status'] == ro_status]
    if age_bucket and age_bucket != "All":
        result_df = result_df[result_df['Age Bucket'] == age_bucket]
    return result_df

# ==================== FASTAPI APP ====================

app = FastAPI(title="Open RO Dashboard API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== STATISTICS ENDPOINTS ====================

@app.get("/api/dashboard/statistics")
async def get_statistics():
    """Get dashboard statistics"""
    try:
        if df_global.empty:
            return {
                "total_vehicles": 0,
                "mechanical_count": 0,
                "bodyshop_count": 0,
                "accessories_count": 0
            }
        
        return {
            "total_vehicles": len(df_global),
            "mechanical_count": len(df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]),
            "bodyshop_count": len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']),
            "accessories_count": len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'])
        }
    except Exception as e:
        print(f"Error in get_statistics: {str(e)}")
        return {
            "total_vehicles": 0,
            "mechanical_count": 0,
            "bodyshop_count": 0,
            "accessories_count": 0
        }

# ==================== FILTER OPTIONS ====================

@app.get("/api/filter-options/mechanical")
async def get_mechanical_filter_options():
    """Get available filter options for Mechanical page"""
    try:
        if df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"]
            }
        
        mechanical_df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        return {
            "branches": ["All"] + sorted(mechanical_df['Branch'].unique().tolist()),
            "ro_statuses": ["All"] + sorted(mechanical_df['RO Status'].unique().tolist()),
            "age_buckets": ["All"] + sorted(mechanical_df['Age Bucket'].unique().tolist())
        }
    except Exception as e:
        print(f"Error in get_mechanical_filter_options: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}

@app.get("/api/filter-options/bodyshop")
async def get_bodyshop_filter_options():
    """Get available filter options for Bodyshop page"""
    try:
        if df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"],
                "mjobs": ["All"]
            }
        
        bodyshop_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        mjobs_set = set()
        mjobs_set.add('Not Categorized')
        for remarks in bodyshop_df['RO Remarks']:
            mjob_list = extract_mjobs(remarks)
            if mjob_list:
                mjobs_set.update(mjob_list)
        
        return {
            "branches": ["All"] + sorted(bodyshop_df['Branch'].unique().tolist()),
            "ro_statuses": ["All"] + sorted(bodyshop_df['RO Status'].unique().tolist()),
            "age_buckets": ["All"] + sorted(bodyshop_df['Age Bucket'].unique().tolist()),
            "mjobs": ["All"] + sorted(list(mjobs_set))
        }
    except Exception as e:
        print(f"Error in get_bodyshop_filter_options: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"]}

@app.get("/api/filter-options/accessories")
async def get_accessories_filter_options():
    """Get available filter options for Accessories page"""
    try:
        if df_global.empty:
            return {
                "branches": ["All"],
                "ro_statuses": ["All"],
                "age_buckets": ["All"]
            }
        
        accessories_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        return {
            "branches": ["All"] + sorted(accessories_df['Branch'].unique().tolist()),
            "ro_statuses": ["All"] + sorted(accessories_df['RO Status'].unique().tolist()),
            "age_buckets": ["All"] + sorted(accessories_df['Age Bucket'].unique().tolist())
        }
    except Exception as e:
        print(f"Error in get_accessories_filter_options: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}

# ==================== VEHICLE ENDPOINTS ====================

@app.get("/api/vehicles/mechanical")
async def get_mechanical_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Mechanical vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        mechanical_df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
        total_count = len(mechanical_df)
        filtered_df = apply_filters(mechanical_df, branch, ro_status, age_bucket)
        filtered_count = len(filtered_df)
        paginated_df = filtered_df.iloc[skip:skip + limit]
        vehicles = [convert_row_to_dict(row) for _, row in paginated_df.iterrows()]
        
        return {"total_count": total_count, "filtered_count": filtered_count, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_mechanical_vehicles: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": [], "error": str(e)}

@app.get("/api/vehicles/bodyshop")
async def get_bodyshop_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Bodyshop vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        bodyshop_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        total_count = len(bodyshop_df)
        filtered_df = apply_filters(bodyshop_df, branch, ro_status, age_bucket)
        
        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                filtered_df = filtered_df[
                    filtered_df['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)
                ]
            else:
                filtered_df = filtered_df[
                    filtered_df['RO Remarks'].apply(lambda x: mjob in (extract_mjobs(x) or []))
                ]
        
        filtered_count = len(filtered_df)
        paginated_df = filtered_df.iloc[skip:skip + limit]
        vehicles = [convert_row_to_dict(row) for _, row in paginated_df.iterrows()]
        
        return {"total_count": total_count, "filtered_count": filtered_count, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_bodyshop_vehicles: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": [], "error": str(e)}

@app.get("/api/vehicles/accessories")
async def get_accessories_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Accessories vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        accessories_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'].copy()
        total_count = len(accessories_df)
        filtered_df = apply_filters(accessories_df, branch, ro_status, age_bucket)
        filtered_count = len(filtered_df)
        paginated_df = filtered_df.iloc[skip:skip + limit]
        vehicles = [convert_row_to_dict(row) for _, row in paginated_df.iterrows()]
        
        return {"total_count": total_count, "filtered_count": filtered_count, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_accessories_vehicles: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": [], "error": str(e)}

# ==================== EXPORT ENDPOINTS ====================

@app.get("/api/export/bodyshop")
async def export_bodyshop_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All")
):
    """Export bodyshop data"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        
        bodyshop_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        filtered_df = apply_filters(bodyshop_df, branch, ro_status, age_bucket)
        
        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                filtered_df = filtered_df[
                    filtered_df['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)
                ]
            else:
                filtered_df = filtered_df[
                    filtered_df['RO Remarks'].apply(lambda x: mjob in (extract_mjobs(x) or []))
                ]
        
        export_list = []
        for _, row in filtered_df.iterrows():
            row_dict = row.to_dict()
            row_dict = {k: (str(v) if pd.notna(v) else '-') for k, v in row_dict.items()}
            export_list.append(row_dict)
        
        return {"vehicles": export_list}
    except Exception as e:
        print(f"Error in export_bodyshop_data: {str(e)}")
        return {"vehicles": []}

@app.get("/api/export/mechanical")
async def export_mechanical_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export mechanical data"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        
        mechanical_df = df_global[
            df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])
        ]
        filtered_df = apply_filters(mechanical_df, branch, ro_status, age_bucket)
        
        export_list = []
        for _, row in filtered_df.iterrows():
            row_dict = row.to_dict()
            row_dict = {k: (str(v) if pd.notna(v) else '-') for k, v in row_dict.items()}
            export_list.append(row_dict)
        
        return {"vehicles": export_list}
    except Exception as e:
        print(f"Error in export_mechanical_data: {str(e)}")
        return {"vehicles": []}

@app.get("/api/export/accessories")
async def export_accessories_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export accessories data"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        
        accessories_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        filtered_df = apply_filters(accessories_df, branch, ro_status, age_bucket)
        
        export_list = []
        for _, row in filtered_df.iterrows():
            row_dict = row.to_dict()
            row_dict = {k: (str(v) if pd.notna(v) else '-') for k, v in row_dict.items()}
            export_list.append(row_dict)
        
        return {"vehicles": export_list}
    except Exception as e:
        print(f"Error in export_accessories_data: {str(e)}")
        return {"vehicles": []}

# ==================== SERVE DASHBOARD ====================

@app.get("/")
async def serve_dashboard():
    """Serve dashboard.html at root URL"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_path = os.path.join(current_dir, "dashboard.html")
    
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path, media_type="text/html")
    else:
        return {"error": "dashboard.html not found"}

# ==================== HEALTH CHECK ====================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "total_records": len(df_global) if df_global is not None else 0
    }

# ==================== MAIN ====================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Dashboard running on: http://0.0.0.0:{PORT}")
    print(f"📊 Open in browser: http://localhost:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
