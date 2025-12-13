import os
import pandas as pd
import re
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

# ==================== CONFIGURATION ====================

# Get port from environment variable (Render uses PORT env var)
PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'  # Required for Render

# ==================== DATA LOADING ====================

df_global = None

def load_data():
    """Load data from Excel file"""
    global df_global
    try:
        # Try to find Excel file in current directory
        excel_file = None
        for filename in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(filename):
                excel_file = filename
                break
        
        if excel_file is None:
            print("⚠ Excel file not found in current directory")
            print("  Looking for: Open RO.xlsx, Open_RO.xlsx, open_ro.xlsx")
            # Create a dummy dataframe for testing
            df_global = pd.DataFrame()
            return
        
        print(f"✓ Loading Excel file: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"✓ Loaded {len(df_global)} records")
    except Exception as e:
        print(f"✗ Error loading Excel file: {str(e)}")
        df_global = pd.DataFrame()

# Load data on startup
load_data()

# ==================== DATA MODELS ====================

class VehicleDetail(BaseModel):
    ro_id: str
    branch: str
    ro_status: str
    age_bucket: str
    service_category: str
    vehicle_model: str
    model_group: Optional[str] = None
    reg_number: str
    ro_date: str
    ro_remarks: str
    km: int
    days: int
    days_open: int
    service_adviser: Optional[str] = None
    vin: Optional[str] = None
    pendncy_resn_desc: Optional[str] = None
    mjob: Optional[List[str]] = None

class PageData(BaseModel):
    total_count: int
    filtered_count: int
    vehicles: List[VehicleDetail]

class FilterOptions(BaseModel):
    branches: List[str]
    ro_statuses: List[str]
    age_buckets: List[str]
    mjobs: Optional[List[str]] = None

# ==================== HELPER FUNCTIONS ====================

def extract_mjobs(remark):
    """Extract MJobs (M1, M2, M3, M4, etc.) from RO Remarks"""
    if pd.isna(remark) or remark == '-':
        return None
    matches = re.findall(r'\bM[1-4]\b', str(remark))
    return matches if matches else None

def prepare_vehicle_data(row, include_mjob=False):
    """Convert a dataframe row to VehicleDetail"""
    # Get Service Adviser Name from available columns
    sa_name = '-'
    if 'Service Adviser Name' in row and pd.notna(row['Service Adviser Name']):
        sa_name = str(row['Service Adviser Name'])
    elif 'SA Name' in row and pd.notna(row['SA Name']):
        sa_name = str(row['SA Name'])
    elif 'Adviser Name' in row and pd.notna(row['Adviser Name']):
        sa_name = str(row['Adviser Name'])
    
    # Get VIN/Chassis number
    vin = '-'
    if 'VIN' in row and pd.notna(row['VIN']):
        vin = str(row['VIN'])
    elif 'Chassis No' in row and pd.notna(row['Chassis No']):
        vin = str(row['Chassis No'])
    
    # Get Pending Reason Description
    pendncy_resn = '-'
    if 'PENDNCY_RESN_DESC' in row and pd.notna(row['PENDNCY_RESN_DESC']):
        pendncy_resn = str(row['PENDNCY_RESN_DESC'])
    
    # Get Model Group from available columns
    model_grp = '-'
    if 'Model Group' in row and pd.notna(row['Model Group']):
        model_grp = str(row['Model Group'])
    elif 'VehType' in row and pd.notna(row['VehType']):
        model_grp = str(row['VehType'])
    elif 'Vehicle Type' in row and pd.notna(row['Vehicle Type']):
        model_grp = str(row['Vehicle Type'])
    
    vehicle = VehicleDetail(
        ro_id=str(row['RO ID']),
        branch=str(row['Branch']),
        ro_status=str(row['RO Status']),
        age_bucket=str(row['Age Bucket']),
        service_category=str(row['SERVC_CATGRY_DESC']),
        vehicle_model=str(row['Family']),
        model_group=model_grp,
        reg_number=str(row['Reg. Number']),
        ro_date=str(row['RO Date']),
        ro_remarks=str(row['RO Remarks']),
        km=int(row['KM']) if pd.notna(row['KM']) else 0,
        days=int(row['Days']) if pd.notna(row['Days']) else 0,
        days_open=int(row['[No of Visits (In last 90 days)]']) if pd.notna(row['[No of Visits (In last 90 days)]']) else 0,
        service_adviser=sa_name,
        vin=vin,
        pendncy_resn_desc=pendncy_resn,
    )
    if include_mjob:
        vehicle.mjob = extract_mjobs(row['RO Remarks'])
    return vehicle

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

# ==================== FILTER OPTIONS ====================

@app.get("/api/filter-options/mechanical", response_model=FilterOptions)
async def get_mechanical_filter_options():
    """Get available filter options for Mechanical page"""
    if df_global.empty:
        return FilterOptions(branches=["All"], ro_statuses=["All"], age_buckets=["All"])
    
    mechanical_df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
    return FilterOptions(
        branches=["All"] + sorted(mechanical_df['Branch'].unique().tolist()),
        ro_statuses=["All"] + sorted(mechanical_df['RO Status'].unique().tolist()),
        age_buckets=["All"] + sorted(mechanical_df['Age Bucket'].unique().tolist())
    )

@app.get("/api/filter-options/bodyshop", response_model=FilterOptions)
async def get_bodyshop_filter_options():
    """Get available filter options for Bodyshop page"""
    if df_global.empty:
        return FilterOptions(branches=["All"], ro_statuses=["All"], age_buckets=["All"], mjobs=["All"])
    
    bodyshop_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
    
    # Extract all unique MJobs
    mjobs_set = set()
    mjobs_set.add('Not Categorized')  # Always include Not Categorized
    for remarks in bodyshop_df['RO Remarks']:
        mjob_list = extract_mjobs(remarks)
        if mjob_list:
            mjobs_set.update(mjob_list)
    
    return FilterOptions(
        branches=["All"] + sorted(bodyshop_df['Branch'].unique().tolist()),
        ro_statuses=["All"] + sorted(bodyshop_df['RO Status'].unique().tolist()),
        age_buckets=["All"] + sorted(bodyshop_df['Age Bucket'].unique().tolist()),
        mjobs=["All"] + sorted(list(mjobs_set))
    )

@app.get("/api/filter-options/accessories", response_model=FilterOptions)
async def get_accessories_filter_options():
    """Get available filter options for Accessories page"""
    if df_global.empty:
        return FilterOptions(branches=["All"], ro_statuses=["All"], age_buckets=["All"])
    
    accessories_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
    return FilterOptions(
        branches=["All"] + sorted(accessories_df['Branch'].unique().tolist()),
        ro_statuses=["All"] + sorted(accessories_df['RO Status'].unique().tolist()),
        age_buckets=["All"] + sorted(accessories_df['Age Bucket'].unique().tolist())
    )

# ==================== VEHICLE ENDPOINTS ====================

@app.get("/api/vehicles/mechanical", response_model=PageData)
async def get_mechanical_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Mechanical vehicles"""
    if df_global.empty:
        return PageData(total_count=0, filtered_count=0, vehicles=[])
    
    mechanical_df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
    total_count = len(mechanical_df)
    filtered_df = apply_filters(mechanical_df, branch, ro_status, age_bucket)
    filtered_count = len(filtered_df)
    paginated_df = filtered_df.iloc[skip:skip + limit]
    vehicles = [prepare_vehicle_data(row) for _, row in paginated_df.iterrows()]
    
    return PageData(total_count=total_count, filtered_count=filtered_count, vehicles=vehicles)

@app.get("/api/vehicles/bodyshop", response_model=PageData)
async def get_bodyshop_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Bodyshop vehicles with MJob details"""
    if df_global.empty:
        return PageData(total_count=0, filtered_count=0, vehicles=[])
    
    bodyshop_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
    total_count = len(bodyshop_df)
    filtered_df = apply_filters(bodyshop_df, branch, ro_status, age_bucket)
    
    # Filter by MJob if specified
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
    vehicles = [prepare_vehicle_data(row, include_mjob=True) for _, row in paginated_df.iterrows()]
    
    return PageData(total_count=total_count, filtered_count=filtered_count, vehicles=vehicles)

@app.get("/api/vehicles/accessories", response_model=PageData)
async def get_accessories_vehicles(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Accessories vehicles"""
    if df_global.empty:
        return PageData(total_count=0, filtered_count=0, vehicles=[])
    
    accessories_df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'].copy()
    total_count = len(accessories_df)
    filtered_df = apply_filters(accessories_df, branch, ro_status, age_bucket)
    filtered_count = len(filtered_df)
    paginated_df = filtered_df.iloc[skip:skip + limit]
    vehicles = [prepare_vehicle_data(row) for _, row in paginated_df.iterrows()]
    
    return PageData(total_count=total_count, filtered_count=filtered_count, vehicles=vehicles)

# ==================== EXPORT ENDPOINTS ====================

@app.get("/api/export/bodyshop")
async def export_bodyshop_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All")
):
    """Export all filtered bodyshop data with complete columns"""
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

@app.get("/api/export/mechanical")
async def export_mechanical_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export all filtered mechanical data with complete columns"""
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

@app.get("/api/export/accessories")
async def export_accessories_data(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export all filtered accessories data with complete columns"""
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
