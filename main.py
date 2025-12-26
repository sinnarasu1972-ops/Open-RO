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
df_landed_cost = None
df_billable_type = None
df_model_group = None
ro_remark_codes = []  # NEW: Store RO Remark codes

KNOWN_RO_CODES = ['WIP', 'RBND', 'PNA', 'WCA', 'WNS', 'MGP', 'CNA', 'WFC', 'WFCA']

# ==================== HELPER FUNCTIONS ====================

def extract_remark_code(remark):
    """Extract RO Remark code (WIP, RBND, PNA, etc.) from remarks"""
    if pd.isna(remark) or remark == '-' or remark == '':
        return None
    
    remark_str = str(remark).strip().upper()
    
    for code in KNOWN_RO_CODES:
        if code in remark_str:
            return code
    return None

def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or remark == '-' or remark == '':
        return None
    
    remark_str = str(remark).strip()
    matches = re.findall(r'\bM/?[1-4]\b', remark_str)
    return matches if matches else None

def get_mjob_category(remark):
    """Categorize a remark as M1, M2, M3, M4, M/4, or "Not Categorized" """
    mjobs = extract_mjobs(remark)
    if mjobs:
        return mjobs[0]
    return "Not Categorized"

def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict"""
    try:
        # Helper function to parse dates safely
        def safe_date_parse(date_value):
            if pd.notna(date_value) and str(date_value).strip() not in ['-', '', 'nan', 'NaT']:
                try:
                    return pd.Timestamp(date_value).strftime('%Y-%m-%d')
                except:
                    return '-'
            return '-'
        
        return {
            'ro_id': str(row['RO ID']).strip() if pd.notna(row['RO ID']) else '-',
            'branch': str(row['Branch']).strip() if pd.notna(row['Branch']) else '-',
            'ro_status': str(row['RO Status']).strip() if pd.notna(row['RO Status']) else '-',
            'age_bucket': str(row['Age Bucket']).strip() if pd.notna(row['Age Bucket']) else '-',
            'service_category': str(row['SERVC_CATGRY_DESC']).strip() if pd.notna(row['SERVC_CATGRY_DESC']) else '-',
            'service_type': str(row['SERVC_TYPE_DESC']).strip() if pd.notna(row['SERVC_TYPE_DESC']) else '-',
            'vehicle_model': str(row['Family']).strip() if pd.notna(row['Family']) else '-',
            'model_group': str(row.get('Model Group', '-')).strip() if pd.notna(row.get('Model Group')) else '-',
            'segment': str(row.get('segment', 'Unknown')).strip() if pd.notna(row.get('segment')) else 'Unknown',
            'reg_number': str(row['Reg. Number']).strip() if pd.notna(row['Reg. Number']) else '-',
            'ro_date': safe_date_parse(row['RO Date']),
            'vehicle_ready_date': safe_date_parse(row['Vehicle  Ready Date']),
            'ro_remarks': str(row['RO Remarks']).strip() if pd.notna(row['RO Remarks']) else '-',
            'km': int(row['KM']) if pd.notna(row['KM']) else 0,
            'days': int(row['Days']) if pd.notna(row['Days']) else 0,
            'days_open': int(row['[No of Visits (In last 90 days)]']) if pd.notna(row['[No of Visits (In last 90 days)]']) else 0,
            'service_adviser': str(row['Service Adviser Name']).strip() if pd.notna(row['Service Adviser Name']) else '-',
            'vin': str(row['VIN']).strip() if pd.notna(row['VIN']) else '-',
            'pendncy_resn_desc': str(row['PENDNCY_RESN_DESC']).strip() if pd.notna(row['PENDNCY_RESN_DESC']) else '-',
            'total_landed_cost': round(float(row['total_landed_cost']), 2) if pd.notna(row['total_landed_cost']) else 0.00,
            'billable_type': str(row['billable_type']).strip() if pd.notna(row['billable_type']) else 'Not Billed',
            'ro_remark_code': str(row.get('ro_remark_code', '-')) if pd.notna(row.get('ro_remark_code')) else '-',  # NEW
        }
    except Exception as e:
        print(f"Error converting row: {str(e)}")
        raise

def apply_filters(df, branch, ro_status, age_bucket, mjob=None, billable_type=None, reg_number=None, service_type=None, sa_name=None, segment=None, ro_remark_code=None):
    """Apply filters to dataframe"""
    result = df.copy()
    
    if branch and branch != "All":
        result = result[result['Branch'] == branch]
    
    if ro_status and ro_status != "All":
        result = result[result['RO Status'] == ro_status]
    
    if age_bucket and age_bucket != "All":
        result = result[result['Age Bucket'] == age_bucket]
    
    if billable_type and billable_type != "All":
        result = result[result['billable_type'] == billable_type]
    
    if service_type and service_type != "All":
        result = result[result['SERVC_TYPE_DESC'] == service_type]
    
    if sa_name and sa_name != "All":
        result = result[result['Service Adviser Name'] == sa_name]
    
    if segment and segment != "All":
        result = result[result['segment'] == segment]
    
    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result['RO Remarks'].apply(
                lambda x: any(m.upper() in [search_mjob, search_mjob.replace('/', '')] 
                            for m in (extract_mjobs(x) or []))
            )]
    
    if reg_number and reg_number.strip() != "":
        search_reg = reg_number.strip().upper()
        result = result[result['Reg. Number'].astype(str).str.upper().str.contains(search_reg, na=False)]
    
    # NEW: Filter by RO Remark Code
    if ro_remark_code and ro_remark_code != "All":
        result = result[result['ro_remark_code'] == ro_remark_code]
    
    return result

def load_data():
    """Load Excel files and merge data"""
    global df_global, df_landed_cost, df_billable_type, df_model_group, ro_remark_codes
    try:
        # Load Model Group mapping
        model_file = None
        for fn in ['Model Group.xlsx', 'Model_Group.xlsx', 'model_group.xlsx']:
            if os.path.exists(fn):
                model_file = fn
                break
        
        if model_file:
            print(f"[OK] Loading: {model_file}")
            df_model_group = pd.read_excel(model_file)
            print(f"[OK] Loaded {len(df_model_group)} model group records")
        else:
            print("⚠ Model Group Excel file not found")
            df_model_group = pd.DataFrame()
        
        # Load Open RO data
        excel_file = None
        for fn in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
            if os.path.exists(fn):
                excel_file = fn
                break
        
        if excel_file is None:
            print("⚠ Open RO Excel file not found")
            df_global = pd.DataFrame()
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
            return
        
        print(f"[OK] Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"[OK] Loaded {len(df_global)} rows")
        
        # Merge Model Group data
        if not df_model_group.empty:
            print(f"[OK] Merging Model Group data...")
            try:
                if 'Model Group' in df_global.columns:
                    segment_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Segment']))
                    mg_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Model Group']))
                    
                    df_global['segment'] = df_global['Model Group'].map(segment_mapping)
                    df_global['model_group_mapped'] = df_global['Model Group'].map(mg_mapping)
                    
                    matched = df_global['segment'].notna().sum()
                    unmatched = df_global['segment'].isna().sum()
                    print(f"  Mapping complete: {matched} matched, {unmatched} unmatched")
                    
                    df_global['segment'] = df_global['segment'].fillna('Unknown')
                    df_global['model_group_mapped'] = df_global['model_group_mapped'].fillna(df_global['Model Group'])
                    df_global['Model Group'] = df_global['model_group_mapped']
                else:
                    df_global['segment'] = 'Unknown'
            except Exception as e:
                print(f"  Error during mapping: {str(e)}")
                df_global['segment'] = 'Unknown'
            
            print(f"[OK] Model Group and Segment data enriched")
        else:
            df_global['segment'] = 'Unknown'
        
        # NEW: Extract RO Remark codes
        print(f"[OK] Extracting RO Remark codes...")
        df_global['ro_remark_code'] = df_global['RO Remarks'].apply(extract_remark_code)
        
        found_codes = df_global['ro_remark_code'].dropna().unique()
        ro_remark_codes = sorted([str(code) for code in found_codes])
        print(f"[OK] Found {len(ro_remark_codes)} unique RO codes: {ro_remark_codes}")
        
        # Load and aggregate Landed Cost data
        parts_file = None
        for fn in ['Part Issue But Not Bill.xlsx', 'Part_Issue_But_Not_Bill.xlsx']:
            if os.path.exists(fn):
                parts_file = fn
                break
        
        if parts_file is None:
            print("⚠ Part Issue file not found")
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
        else:
            print(f"[OK] Loading: {parts_file}")
            df_parts = pd.read_excel(parts_file)
            
            df_landed_cost = df_parts.groupby('RO Number')['Landed Cost (Total)'].sum().reset_index()
            df_landed_cost.columns = ['RO ID', 'total_landed_cost']
            
            df_billable_type = df_parts.groupby('RO Number')['Billable Type'].first().reset_index()
            df_billable_type.columns = ['RO ID', 'billable_type']
            
            df_global = df_global.merge(df_landed_cost, on='RO ID', how='left')
            df_global['total_landed_cost'] = df_global['total_landed_cost'].fillna(0)
            
            df_global = df_global.merge(df_billable_type, on='RO ID', how='left')
            df_global['billable_type'] = df_global['billable_type'].fillna('Not Billed')
            
            print(f"[OK] Merged landed cost data")
        
    except Exception as e:
        print(f"[ERROR] Error: {str(e)}")
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()
        df_billable_type = pd.DataFrame()

load_data()

# ==================== APP ====================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== FILTER OPTIONS WITH RO REMARK CODE ====================

@app.get("/api/filter-options/{category}")
async def get_filter_options(category: str, branch: Optional[str] = Query("All")):
    """Get filter options including RO Remark Code"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "segments": ["All"], "billable_types": ["All"], "sa_names": ["All"], "ro_remark_codes": ["All"]}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        
        if branch and branch != "All":
            df_branch = df[df['Branch'] == branch]
            sa_names = ['All'] + sorted([str(x) for x in df_branch['Service Adviser Name'].dropna().unique().tolist()])
        else:
            sa_names = ['All'] + sorted([str(x) for x in df['Service Adviser Name'].dropna().unique().tolist()])
        
        if 'segment' in df.columns:
            segments = ['All'] + sorted([str(x) for x in df['segment'].dropna().unique().tolist() if x != 'Unknown'])
        else:
            segments = ['All', 'Unknown']
        
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()]),
            "segments": segments,
            "billable_types": ['All'] + sorted([str(x) for x in df['billable_type'].dropna().unique().tolist()]),
            "sa_names": sa_names,
            "ro_remark_codes": ['All'] + sorted(ro_remark_codes)  # NEW
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "segments": ["All"], "billable_types": ["All"], "sa_names": ["All"], "ro_remark_codes": ["All"]}

# ==================== VEHICLE ENDPOINTS WITH RO REMARK CODE ====================

@app.get("/api/vehicles/mechanical")
async def get_mechanical(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark_code: Optional[str] = Query("All"),  # NEW
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get mechanical vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket, billable_type=billable_type, sa_name=sa_name, segment=segment, ro_remark_code=ro_remark_code)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/vehicles/bodyshop")
async def get_bodyshop(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark_code: Optional[str] = Query("All"),  # NEW
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get bodyshop vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket, billable_type=billable_type, sa_name=sa_name, segment=segment, ro_remark_code=ro_remark_code)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/vehicles/accessories")
async def get_accessories(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark_code: Optional[str] = Query("All"),  # NEW
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get accessories vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket, billable_type=billable_type, sa_name=sa_name, segment=segment, ro_remark_code=ro_remark_code)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/vehicles/presale")
async def get_presale(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark_code: Optional[str] = Query("All"),  # NEW
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Get Pre-Sale/PDI vehicles"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Pre-Sale/PDI'].copy()
        total = len(df)
        df = apply_filters(df, branch, ro_status, age_bucket, billable_type=billable_type, sa_name=sa_name, segment=segment, ro_remark_code=ro_remark_code)
        filtered = len(df)
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

# ==================== REMAINING ENDPOINTS (kept from original) ====================

@app.get("/api/dashboard/statistics")
async def statistics():
    try:
        if df_global.empty:
            return {"total_vehicles": 0, "mechanical_count": 0, "bodyshop_count": 0, "accessories_count": 0, "presale_count": 0, "total_landed_cost": 0.0}
        
        return {
            "total_vehicles": int(len(df_global)),
            "mechanical_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])])),
            "bodyshop_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'])),
            "accessories_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'])),
            "presale_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Pre-Sale/PDI'])),
            "total_landed_cost": float(df_global['total_landed_cost'].sum())
        }
    except Exception as e:
        return {"total_vehicles": 0, "mechanical_count": 0, "bodyshop_count": 0, "accessories_count": 0, "presale_count": 0, "total_landed_cost": 0.0}

@app.get("/api/dashboard/division-stats-v2")
async def division_stats_v2(service_category: str = Query("mechanical")):
    try:
        if df_global.empty:
            return {"divisions": [], "total_open": 0, "total_closed_not_billed": 0}
        
        if service_category == "mechanical":
            df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        elif service_category == "bodyshop":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        elif service_category == "accessories":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        elif service_category == "presale":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Pre-Sale/PDI']
        else:
            df = df_global
        
        branches = df['Branch'].unique()
        divisions = []
        total_open = 0
        total_closed_not_billed = 0
        
        for branch in sorted(branches):
            df_branch = df[df['Branch'] == branch]
            open_count = len(df_branch[df_branch['RO Status'] == 'Open'])
            closed_not_billed_count = len(df_branch[df_branch['RO Status'] == 'Closed but not billed'])
            
            total_open += open_count
            total_closed_not_billed += closed_not_billed_count
            
            divisions.append({
                'branch': branch,
                'open_count': open_count,
                'closed_not_billed_count': closed_not_billed_count,
                'total': open_count + closed_not_billed_count
            })
        
        divisions = sorted(divisions, key=lambda x: x['total'], reverse=True)
        
        return {
            "service_category": service_category,
            "total_open": total_open,
            "total_closed_not_billed": total_closed_not_billed,
            "divisions": divisions
        }
    except Exception as e:
        return {"service_category": service_category, "total_open": 0, "total_closed_not_billed": 0, "divisions": []}

@app.get("/api/export/{category}")
async def export_data(
    category: str,
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    billable_type: Optional[str] = Query("All"),
    sa_name: Optional[str] = Query("All"),
    segment: Optional[str] = Query("All"),
    ro_remark_code: Optional[str] = Query("All")  # NEW
):
    try:
        if df_global.empty:
            return {"vehicles": []}
        
        if category == "mechanical":
            df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        elif category == "bodyshop":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        elif category == "accessories":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        elif category == "presale":
            df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Pre-Sale/PDI']
        else:
            df = df_global
        
        df = apply_filters(df, branch, ro_status, age_bucket, billable_type=billable_type, sa_name=sa_name, segment=segment, ro_remark_code=ro_remark_code)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        return {"vehicles": vehicles}
    except Exception as e:
        return {"vehicles": []}

@app.get("/")
async def dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return {"error": "dashboard.html not found"}

@app.get("/health")
async def health():
    return {"status": "healthy", "records": len(df_global) if not df_global.empty else 0}

if __name__ == "__main__":
    import uvicorn
    print(f"Running on http://0.0.0.0:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
