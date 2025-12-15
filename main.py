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

def load_data():
    """Load Excel files and merge data"""
    global df_global, df_landed_cost
    try:
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
            return
        
        print(f"✓ Loading: {excel_file}")
        df_global = pd.read_excel(excel_file)
        print(f"✓ Loaded {len(df_global)} rows, {len(df_global.columns)} cols")
        print(f"✓ Columns: {list(df_global.columns)}")
        
        # Load and aggregate Landed Cost data
        parts_file = None
        for fn in ['Part Issue But Not Bill.xlsx', 'Part_Issue_But_Not_Bill.xlsx', 'part_issue_but_not_bill.xlsx']:
            if os.path.exists(fn):
                parts_file = fn
                break
        
        if parts_file is None:
            print("⚠ Part Issue file not found - Landed Cost will not be available")
            df_landed_cost = pd.DataFrame()
        else:
            print(f"✓ Loading: {parts_file}")
            df_parts = pd.read_excel(parts_file)
            print(f"✓ Loaded {len(df_parts)} part records")
            
            # Aggregate Landed Cost by RO Number
            df_landed_cost = df_parts.groupby('RO Number')['Landed Cost (Total)'].sum().reset_index()
            df_landed_cost.columns = ['RO ID', 'total_landed_cost']
            print(f"✓ Aggregated {len(df_landed_cost)} unique RO Numbers with landed cost")
            
            # Merge with main dataframe
            df_global = df_global.merge(df_landed_cost, on='RO ID', how='left')
            df_global['total_landed_cost'] = df_global['total_landed_cost'].fillna(0)
            print(f"✓ Merged landed cost data into main dataframe")
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()

load_data()

# ==================== HELPER FUNCTIONS ====================

def extract_mjobs(remark):
    """Extract MJob codes from remarks"""
    if pd.isna(remark) or remark == '-' or remark == '':
        return None
    
    remark_str = str(remark).strip()
    matches = re.findall(r'\bM/?[1-4]\b', remark_str)
    return matches if matches else None

def get_mjob_category(remark):
    """Categorize a remark as M1, M2, M3, M4, M/4, or "Not Categorized"
    """
    mjobs = extract_mjobs(remark)
    if mjobs:
        return mjobs[0]
    return "Not Categorized"

def convert_row(row) -> Dict[str, Any]:
    """Convert pandas row to JSON-safe dict"""
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
            'total_landed_cost': float(row['total_landed_cost']) if pd.notna(row['total_landed_cost']) else 0.0,
        }
    except Exception as e:
        print(f"Error converting row: {str(e)}")
        raise

def apply_filters(df, branch, ro_status, age_bucket, mjob=None):
    """Apply filters to dataframe"""
    result = df.copy()
    
    if branch and branch != "All":
        result = result[result['Branch'] == branch]
    
    if ro_status and ro_status != "All":
        result = result[result['RO Status'] == ro_status]
    
    if age_bucket and age_bucket != "All":
        result = result[result['Age Bucket'] == age_bucket]
    
    if mjob and mjob != "All":
        if mjob == "Not Categorized":
            result = result[result['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
        else:
            search_mjob = mjob.upper()
            result = result[result['RO Remarks'].apply(
                lambda x: any(m.upper() in [search_mjob, search_mjob.replace('/', '')] 
                            for m in (extract_mjobs(x) or []))
            )]
    
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
    """Dashboard statistics - total counts"""
    try:
        if df_global.empty:
            return {
                "total_vehicles": 0, 
                "mechanical_count": 0, 
                "bodyshop_count": 0, 
                "accessories_count": 0,
                "total_landed_cost": 0.0
            }
        
        return {
            "total_vehicles": int(len(df_global)),
            "mechanical_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])])),
            "bodyshop_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'])),
            "accessories_count": int(len(df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories'])),
            "total_landed_cost": float(df_global['total_landed_cost'].sum())
        }
    except Exception as e:
        print(f"Error in statistics: {str(e)}")
        return {
            "total_vehicles": 0, 
            "mechanical_count": 0, 
            "bodyshop_count": 0, 
            "accessories_count": 0,
            "total_landed_cost": 0.0
        }

# ========== CHANGED: Enhanced filtered statistics with better debugging ==========
@app.get("/api/dashboard/statistics/filtered")
async def filtered_statistics(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All")
):
    """Dashboard statistics - with dynamic filtering"""
    try:
        if df_global.empty:
            print("ERROR: DataFrame is empty!")
            return {
                "total_vehicles": 0, 
                "mechanical_count": 0, 
                "bodyshop_count": 0, 
                "accessories_count": 0,
                "total_landed_cost": 0.0
            }
        
        print(f"\n{'='*80}")
        print(f"FILTERED STATS REQUEST")
        print(f"{'='*80}")
        print(f"Filters: branch={branch}, ro_status={ro_status}, age_bucket={age_bucket}, mjob={mjob}")
        print(f"Total rows in df_global: {len(df_global)}")
        print(f"Total landed cost before filtering: {df_global['total_landed_cost'].sum()}")
        
        # Start with full dataframe
        filtered_df = df_global.copy()
        print(f"Starting filtered_df rows: {len(filtered_df)}")
        
        # Apply Branch filter
        if branch and branch != "All":
            filtered_df = filtered_df[filtered_df['Branch'] == branch]
            print(f"After BRANCH filter ({branch}): {len(filtered_df)} rows")
        
        # Apply RO Status filter
        if ro_status and ro_status != "All":
            filtered_df = filtered_df[filtered_df['RO Status'] == ro_status]
            print(f"After RO_STATUS filter ({ro_status}): {len(filtered_df)} rows")
        
        # Apply Age Bucket filter
        if age_bucket and age_bucket != "All":
            filtered_df = filtered_df[filtered_df['Age Bucket'] == age_bucket]
            print(f"After AGE_BUCKET filter ({age_bucket}): {len(filtered_df)} rows")
        
        # Apply MJob filter
        if mjob and mjob != "All":
            if mjob == "Not Categorized":
                filtered_df = filtered_df[filtered_df['RO Remarks'].apply(lambda x: extract_mjobs(x) is None)]
                print(f"After MJOB filter (Not Categorized): {len(filtered_df)} rows")
            else:
                search_mjob = mjob.upper()
                filtered_df = filtered_df[filtered_df['RO Remarks'].apply(
                    lambda x: any(m.upper() in [search_mjob, search_mjob.replace('/', '')] 
                                for m in (extract_mjobs(x) or []))
                )]
                print(f"After MJOB filter ({mjob}): {len(filtered_df)} rows")
        
        # Count by service category
        mechanical = filtered_df[filtered_df['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        bodyshop = filtered_df[filtered_df['SERVC_CATGRY_DESC'] == 'Bodyshop']
        accessories = filtered_df[filtered_df['SERVC_CATGRY_DESC'] == 'Accessories']
        
        # Calculate total landed cost for FILTERED data only
        total_cost = 0.0
        if 'total_landed_cost' in filtered_df.columns:
            total_cost = float(filtered_df['total_landed_cost'].sum())
            print(f"Filtered landed cost sum: {total_cost}")
        
        result = {
            "total_vehicles": int(len(filtered_df)),
            "mechanical_count": int(len(mechanical)),
            "bodyshop_count": int(len(bodyshop)),
            "accessories_count": int(len(accessories)),
            "total_landed_cost": float(total_cost)
        }
        
        print(f"FINAL RESULT: {result}")
        print(f"{'='*80}\n")
        
        return result
        
    except Exception as e:
        print(f"\nERROR in filtered_statistics: {str(e)}")
        import traceback
        traceback.print_exc()
        print(f"{'='*80}\n")
        return {
            "total_vehicles": 0, 
            "mechanical_count": 0, 
            "bodyshop_count": 0, 
            "accessories_count": 0,
            "total_landed_cost": 0.0
        }

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
        print(f"Error in mech_filters: {str(e)}")
        return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"]}

@app.get("/api/filter-options/bodyshop")
async def bs_filters():
    """Bodyshop filters - dynamically extracts MJob options"""
    try:
        if df_global.empty:
            return {"branches": ["All"], "ro_statuses": ["All"], "age_buckets": ["All"], "mjobs": ["All"]}
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        
        mjobs_set = set(['Not Categorized'])
        for remark in df['RO Remarks'].dropna():
            extracted = extract_mjobs(remark)
            if extracted:
                mjobs_set.update(extracted)
        
        mjobs_sorted = ['All', 'Not Categorized']
        for m in ['M1', 'M2', 'M3', 'M4', 'M/4']:
            if m in mjobs_set:
                mjobs_sorted.append(m)
        
        print(f"DEBUG: Found MJob options: {mjobs_sorted}")
        
        return {
            "branches": ["All"] + sorted([str(x) for x in df['Branch'].unique().tolist()]),
            "ro_statuses": ["All"] + sorted([str(x) for x in df['RO Status'].unique().tolist()]),
            "age_buckets": ["All"] + sorted([str(x) for x in df['Age Bucket'].unique().tolist()]),
            "mjobs": mjobs_sorted
        }
    except Exception as e:
        print(f"Error in bs_filters: {str(e)}")
        import traceback
        traceback.print_exc()
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
    except Exception as e:
        print(f"Error in acc_filters: {str(e)}")
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
        
        print(f"DEBUG: Mechanical - Total: {total}, Filtered: {filtered}, Returned: {len(vehicles)}")
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_mechanical: {str(e)}")
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
    """Get bodyshop vehicles with MJob filtering"""
    try:
        if df_global.empty:
            return {"total_count": 0, "filtered_count": 0, "vehicles": []}
        
        print(f"DEBUG: get_bodyshop called with mjob='{mjob}'")
        
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop'].copy()
        total = len(df)
        
        df = apply_filters(df, branch, ro_status, age_bucket, mjob)
        
        filtered = len(df)
        print(f"DEBUG: Bodyshop - Total: {total}, Filtered (after mjob): {filtered}")
        
        df = df.iloc[skip:skip + limit]
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        
        print(f"DEBUG: Bodyshop - Returned: {len(vehicles)}")
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_bodyshop: {str(e)}")
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
        
        print(f"DEBUG: Accessories - Total: {total}, Filtered: {filtered}, Returned: {len(vehicles)}")
        
        return {"total_count": total, "filtered_count": filtered, "vehicles": vehicles}
    except Exception as e:
        print(f"Error in get_accessories: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"total_count": 0, "filtered_count": 0, "vehicles": []}

@app.get("/api/export/mechanical")
async def export_mech(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export mechanical vehicles"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'].isin(['Repair', 'Paid Service', 'Free Service'])]
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_mech: {str(e)}")
        return {"vehicles": []}

@app.get("/api/export/bodyshop")
async def export_bs(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All"),
    mjob: Optional[str] = Query("All")
):
    """Export bodyshop vehicles with MJob filtering"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Bodyshop']
        df = apply_filters(df, branch, ro_status, age_bucket, mjob)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_bs: {str(e)}")
        return {"vehicles": []}

@app.get("/api/export/accessories")
async def export_acc(
    branch: Optional[str] = Query("All"),
    ro_status: Optional[str] = Query("All"),
    age_bucket: Optional[str] = Query("All")
):
    """Export accessories vehicles"""
    try:
        if df_global.empty:
            return {"vehicles": []}
        df = df_global[df_global['SERVC_CATGRY_DESC'] == 'Accessories']
        df = apply_filters(df, branch, ro_status, age_bucket)
        vehicles = [convert_row(row) for _, row in df.iterrows()]
        return {"vehicles": vehicles}
    except Exception as e:
        print(f"Error in export_acc: {str(e)}")
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
