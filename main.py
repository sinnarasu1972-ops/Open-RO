import os
import pandas as pd
import re
from io import BytesIO
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from datetime import datetime

# ==================== CONFIGURATION ====================

PORT = int(os.getenv('PORT', 8000))
HOST = '0.0.0.0'

# ==================== GLOBAL DATA ====================

df_global = None
df_landed_cost = None
df_billable_type = None
df_model_group = None
ro_remark_codes = []

# Known RO status codes to look for
KNOWN_RO_CODES = ['WIP', 'RBND', 'PNA', 'WCA', 'WNS', 'MGP', 'CNA', 'WFC', 'WFCA']

# ==================== HELPER FUNCTIONS (DEFINED FIRST) ====================

def extract_remark_code(remark):
    """Extract RO code from remarks - looks for known codes"""
    if pd.isna(remark) or remark == '-' or remark == '':
        return None
    
    remark_str = str(remark).strip().upper()
    
    # Look for any known RO status code in the remarks
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


def apply_filters(data, service_category, branch, ro_status, age_bucket, segment, 
                 billable_type, sa_name, service_type, ro_remark_code):
    """Apply all filters to dataframe"""
    df = data.copy()
    
    # Service category filter
    if service_category != 'All':
        if service_category == 'mechanical':
            df = df[df['SERVC_CATGRY_DESC'].str.contains('MECHANICAL', case=False, na=False)]
        elif service_category == 'bodyshop':
            df = df[df['SERVC_CATGRY_DESC'].str.contains('BODY', case=False, na=False)]
        elif service_category == 'accessories':
            df = df[df['SERVC_CATGRY_DESC'].str.contains('ACCESSORY', case=False, na=False)]
        elif service_category == 'presale':
            df = df[df['SERVC_CATGRY_DESC'].str.contains('PRE-SALE', case=False, na=False)]
    
    # Other filters
    if branch != 'All':
        df = df[df['Branch'] == branch]
    
    if ro_status != 'All':
        df = df[df['RO Status'] == ro_status]
    
    if age_bucket != 'All':
        df = df[df['Age Bucket'] == age_bucket]
    
    if segment != 'All':
        df = df[df['segment'] == segment]
    
    if billable_type != 'All':
        df = df[df['Billable Type'] == billable_type]
    
    if sa_name != 'All':
        df = df[df['Service Adviser Name'] == sa_name]
    
    if service_type != 'All':
        df = df[df['SERVC_TYPE_DESC'] == service_type]
    
    if ro_remark_code != 'All':
        df = df[df['ro_remark_code'] == ro_remark_code]
    
    return df


def convert_row(row, service_category='mechanical'):
    """Convert DataFrame row to API response format"""
    base_dict = {
        'ro_id': str(row.get('RO ID', '-')),
        'landed_cost': float(row.get('Landed Cost', 0)) if pd.notna(row.get('Landed Cost')) else 0,
        'ro_date': str(row.get('RO Date', '-')),
        'branch': str(row.get('Branch', '-')),
        'status': str(row.get('RO Status', '-')),
        'sa_name': str(row.get('Service Adviser Name', '-')),
        'model_group': str(row.get('model_group_name', row.get('Model Group', '-'))),
        'segment': str(row.get('segment', '-')),
        'chassis': str(row.get('VIN', '-')),
        'ro_remark_code': str(row.get('ro_remark_code', '-')) if pd.notna(row.get('ro_remark_code')) else '-',
        'remarks': str(row.get('RO Remarks', '-')),
        'pending_reason': str(row.get('PENDNCY_RESN_DESC', '-')),
    }
    
    # Add service category specific fields
    if service_category == 'bodyshop':
        base_dict['ro_number'] = str(row.get('RO ID', '-'))
        base_dict['reg_number'] = str(row.get('Reg. Number', '-'))
        base_dict['mjob'] = str(row.get('mjob', '-')) if pd.notna(row.get('mjob')) else '-'
        base_dict['tat'] = str(row.get('TAT', '-'))
    else:
        base_dict['reg_number'] = str(row.get('Reg. Number', '-'))
    
    return base_dict


# ==================== DATA LOADING ====================

def load_data():
    """Load and process all Excel files"""
    global df_global, df_landed_cost, df_billable_type, df_model_group, ro_remark_codes
    
    try:
        print(f"[OK] Initializing RO Remark code extraction with known codes: {KNOWN_RO_CODES}")
        
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
            print(f"[OK] Model Group columns: {list(df_model_group.columns)}")
        else:
            print("⚠ Model Group Excel file not found - Model Group and Segment will not be enriched")
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
        print(f"[OK] Loaded {len(df_global)} rows, {len(df_global.columns)} cols")
        print(f"[OK] Columns: {list(df_global.columns)}")
        
        # ===== EXTRACT RO REMARK CODES =====
        print(f"[OK] Extracting RO codes from remarks...")
        df_global['ro_remark_code'] = df_global['RO Remarks'].apply(extract_remark_code)
        
        # Extract MJob codes
        print(f"[OK] Extracting MJob codes from remarks...")
        df_global['mjob'] = df_global['RO Remarks'].apply(extract_mjobs)
        
        # Get unique codes found in data
        found_codes = df_global['ro_remark_code'].dropna().unique()
        ro_remark_codes = sorted([str(code) for code in found_codes])
        print(f"[OK] Found {len(ro_remark_codes)} unique RO codes in remarks: {ro_remark_codes}")
        
        # Count occurrences
        for code in ro_remark_codes:
            count = (df_global['ro_remark_code'] == code).sum()
            print(f"    {code}: {count} occurrences")
        
        # ===== MERGE MODEL GROUP DATA =====
        if not df_model_group.empty:
            print(f"[OK] Merging Model Group data...")
            print(f"  Model Group df shape: {df_model_group.shape}")
            print(f"  Open RO df shape: {df_global.shape}")
            
            try:
                if 'Model Group' in df_global.columns:
                    # Create mappings from Model_Group.xlsx
                    segment_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Segment']))
                    mg_mapping = dict(zip(df_model_group['Model Code'], df_model_group['Model Group']))
                    
                    print(f"  Created mappings:")
                    print(f"    - Model Code -> Segment: {len(segment_mapping)} entries")
                    print(f"    - Model Code -> Model Group Name: {len(mg_mapping)} entries")
                    
                    # Show sample mappings
                    sample_codes = list(segment_mapping.keys())[:5]
                    print(f"    Sample: {sample_codes}")
                    for code in sample_codes:
                        print(f"      {code} -> Segment: {segment_mapping[code]}, Name: {mg_mapping[code]}")
                    
                    # Apply mappings
                    print(f"  Applying mappings to Open RO 'Model Group' column...")
                    df_global['segment'] = df_global['Model Group'].map(segment_mapping)
                    df_global['model_group_name'] = df_global['Model Group'].map(mg_mapping)
                    
                    matched = df_global['segment'].notna().sum()
                    unmatched = df_global['segment'].isna().sum()
                    print(f"  [OK] Mapping complete: {matched} matched, {unmatched} unmatched")
                    
                    # Fill remaining nulls (using proper pandas syntax)
                    df_global['segment'] = df_global['segment'].fillna('Unknown')
                    df_global['model_group_name'] = df_global['model_group_name'].fillna(df_global['Model Group'])
                    
                    print(f"[OK] Model Group and Segment data enriched")
                    print(f"  Segment column exists: {('segment' in df_global.columns)}")
                    print(f"  Segment values: {sorted(df_global['segment'].dropna().unique().tolist())}")
                    
                    # Count segments
                    segment_counts = df_global['segment'].value_counts()
                    print(f"  Segment value counts:")
                    for seg, count in segment_counts.items():
                        print(f"    {seg}: {count}")
                        
            except Exception as e:
                print(f"⚠ Error merging Model Group: {e}")
                df_global['segment'] = 'Unknown'
                df_global['model_group_name'] = df_global['Model Group']
        else:
            df_global['segment'] = 'Unknown'
            df_global['model_group_name'] = df_global['Model Group']
        
        # ===== LOAD LANDED COST DATA =====
        parts_file = None
        for fn in ['Part Issue But Not Bill.xlsx', 'Part_Issue_But_Not_Bill.xlsx', 'part_issue_but_not_bill.xlsx']:
            if os.path.exists(fn):
                parts_file = fn
                break
        
        if parts_file is None:
            print("⚠ Part Issue file not found - Landed Cost and Billable Type will not be available")
            df_global['Landed Cost'] = 0
            df_global['Billable Type'] = '-'
            df_landed_cost = pd.DataFrame()
            df_billable_type = pd.DataFrame()
        else:
            print(f"[OK] Loading: {parts_file}")
            try:
                df_temp = pd.read_excel(parts_file)
                print(f"[OK] Loaded {len(df_temp)} part records")
                print(f"[DEBUG] Part Issue columns: {df_temp.columns.tolist()}")
                
                # Find the correct column names (handle variations)
                ro_col = None
                cost_col = None
                billable_col = None
                
                for col in df_temp.columns:
                    col_lower = col.lower().strip()
                    if 'ro' in col_lower and 'number' in col_lower:
                        ro_col = col
                    elif 'landed' in col_lower and 'cost' in col_lower:
                        cost_col = col
                    elif 'billable' in col_lower and 'type' in col_lower:
                        billable_col = col
                
                print(f"[DEBUG] Found columns: RO={ro_col}, Cost={cost_col}, Billable={billable_col}")
                
                # If we found the columns, use them
                if ro_col and cost_col:
                    df_landed_cost = df_temp.groupby(ro_col)[cost_col].sum().reset_index()
                    df_landed_cost.columns = ['RO Number', 'Landed Cost']
                    
                    if billable_col:
                        df_billable_type = df_temp.groupby(ro_col)[billable_col].first().reset_index()
                        df_billable_type.columns = ['RO Number', 'Billable Type']
                    else:
                        df_billable_type = pd.DataFrame({'RO Number': df_landed_cost['RO Number'], 'Billable Type': '-'})
                    
                    print(f"[OK] Aggregated {len(df_landed_cost)} unique RO Numbers with landed cost")
                    print(f"[OK] Extracted billable type for {len(df_billable_type)} RO Numbers")
                    
                    # Merge into main dataframe
                    df_global = df_global.merge(df_landed_cost, left_on='RO ID', right_on='RO Number', how='left')
                    df_global = df_global.merge(df_billable_type, left_on='RO ID', right_on='RO Number', how='left')
                    
                    # Fill NAs (using proper pandas syntax)
                    df_global['Landed Cost'] = df_global['Landed Cost'].fillna(0)
                    df_global['Billable Type'] = df_global['Billable Type'].fillna('-')
                    
                    print(f"[OK] Merged landed cost and billable type data into main dataframe")
                else:
                    print(f"⚠ Could not find required columns in Part Issue file")
                    print(f"⚠ Available columns: {df_temp.columns.tolist()}")
                    df_global['Landed Cost'] = 0
                    df_global['Billable Type'] = '-'
                    
            except Exception as e:
                print(f"⚠ Error processing Part Issue file: {e}")
                df_global['Landed Cost'] = 0
                df_global['Billable Type'] = '-'
        
        print("[OK] Data loading complete")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        df_global = pd.DataFrame()
        df_landed_cost = pd.DataFrame()
        df_billable_type = pd.DataFrame()
        ro_remark_codes = []


# ==================== INITIALIZE DATA ====================

load_data()

# ==================== FASTAPI APP ====================

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== API ENDPOINTS ====================

@app.get("/api/filter-options/{service_category}")
def get_filter_options(service_category: str, branch: str = "All"):
    """Get available filter options for a service category"""
    if df_global.empty:
        return {
            'branches': [],
            'statuses': [],
            'age_buckets': [],
            'segments': [],
            'billable_types': [],
            'sa_names': [],
            'service_types': [],
            'ro_remark_codes': []
        }
    
    df = df_global.copy()
    
    # Filter by service category first
    if service_category == 'mechanical':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('MECHANICAL', case=False, na=False)]
    elif service_category == 'bodyshop':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('BODY', case=False, na=False)]
    elif service_category == 'accessories':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('ACCESSORY', case=False, na=False)]
    elif service_category == 'presale':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('PRE-SALE', case=False, na=False)]
    
    # Then filter by branch if specified
    if branch != 'All':
        df = df[df['Branch'] == branch]
    
    return {
        'branches': ['All'] + sorted([str(x) for x in df['Branch'].dropna().unique().tolist()]),
        'statuses': ['All'] + sorted([str(x) for x in df['RO Status'].dropna().unique().tolist()]),
        'age_buckets': ['All'] + sorted([str(x) for x in df['Age Bucket'].dropna().unique().tolist()]),
        'segments': ['All'] + sorted([str(x) for x in df['segment'].dropna().unique().tolist()]),
        'billable_types': ['All'] + sorted([str(x) for x in df['Billable Type'].dropna().unique().tolist()]),
        'sa_names': ['All'] + sorted([str(x) for x in df['Service Adviser Name'].dropna().unique().tolist()]),
        'service_types': ['All'] + sorted([str(x) for x in df['SERVC_TYPE_DESC'].dropna().unique().tolist()]),
        'ro_remark_codes': ['All'] + sorted(ro_remark_codes)
    }


@app.get("/api/vehicles/{service_category}")
def get_vehicles(
    service_category: str,
    skip: int = 0,
    limit: int = 50,
    branch: str = "All",
    ro_status: str = "All",
    age_bucket: str = "All",
    segment: str = "All",
    billable_type: str = "All",
    sa_name: str = "All",
    service_type: str = "All",
    ro_remark_code: str = "All"
):
    """Get paginated vehicle list with filters"""
    if df_global.empty:
        return {'vehicles': [], 'total': 0}
    
    df = apply_filters(df_global, service_category, branch, ro_status, age_bucket, segment, 
                       billable_type, sa_name, service_type, ro_remark_code)
    
    total = len(df)
    vehicles = [convert_row(row, service_category) for _, row in df.iloc[skip:skip+limit].iterrows()]
    
    return {'vehicles': vehicles, 'total': total}


@app.get("/api/dashboard/statistics")
def get_statistics():
    """Get overall dashboard statistics"""
    if df_global.empty:
        return {
            'total_ros': 0,
            'mechanical': 0,
            'bodyshop': 0,
            'accessories': 0,
            'presale': 0,
            'part_issued_value': '₹0'
        }
    
    df = df_global.copy()
    
    return {
        'total_ros': len(df),
        'mechanical': len(df[df['SERVC_CATGRY_DESC'].str.contains('MECHANICAL', case=False, na=False)]),
        'bodyshop': len(df[df['SERVC_CATGRY_DESC'].str.contains('BODY', case=False, na=False)]),
        'accessories': len(df[df['SERVC_CATGRY_DESC'].str.contains('ACCESSORY', case=False, na=False)]),
        'presale': len(df[df['SERVC_CATGRY_DESC'].str.contains('PRE-SALE', case=False, na=False)]),
        'part_issued_value': f"₹{df['Landed Cost'].sum():,.2f}"
    }


@app.get("/api/dashboard/statistics/filtered")
def get_filtered_statistics(
    service_category: str = "All",
    branch: str = "All",
    ro_status: str = "All",
    age_bucket: str = "All",
    segment: str = "All",
    billable_type: str = "All",
    sa_name: str = "All",
    service_type: str = "All",
    ro_remark_code: str = "All"
):
    """Get filtered statistics"""
    if df_global.empty:
        return {'total': 0, 'part_issued_value': '₹0'}
    
    df = apply_filters(df_global, service_category, branch, ro_status, age_bucket, segment, 
                       billable_type, sa_name, service_type, ro_remark_code)
    
    return {
        'total': len(df),
        'part_issued_value': f"₹{df['Landed Cost'].sum():,.2f}"
    }


@app.get("/api/dashboard/division-stats-v2")
def get_division_stats(service_category: str = "All"):
    """Get division-wise statistics"""
    if df_global.empty:
        return {'branches': []}
    
    df = df_global.copy()
    
    # Filter by service category
    if service_category == 'mechanical':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('MECHANICAL', case=False, na=False)]
    elif service_category == 'bodyshop':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('BODY', case=False, na=False)]
    elif service_category == 'accessories':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('ACCESSORY', case=False, na=False)]
    elif service_category == 'presale':
        df = df[df['SERVC_CATGRY_DESC'].str.contains('PRE-SALE', case=False, na=False)]
    
    # Group by branch
    branch_stats = df.groupby('Branch').agg({
        'RO ID': 'count',
        'Landed Cost': 'sum'
    }).reset_index()
    
    branch_stats.columns = ['branch', 'count', 'value']
    
    return {
        'branches': branch_stats.to_dict('records')
    }


@app.get("/api/export/{service_category}")
def export_data(
    service_category: str,
    branch: str = "All",
    ro_status: str = "All",
    age_bucket: str = "All",
    segment: str = "All",
    billable_type: str = "All",
    sa_name: str = "All",
    service_type: str = "All",
    ro_remark_code: str = "All"
):
    """Export filtered data to CSV"""
    if df_global.empty:
        return {"message": "No data to export"}
    
    df = apply_filters(df_global, service_category, branch, ro_status, age_bucket, segment, 
                       billable_type, sa_name, service_type, ro_remark_code)
    
    if df.empty:
        return {"message": "No filtered data to export"}
    
    # Select relevant columns based on service category
    if service_category == 'bodyshop':
        columns = ['RO ID', 'Landed Cost', 'RO Date', 'Branch', 'RO Status', 'Service Adviser Name',
                  'Reg. Number', 'model_group_name', 'segment', 'VIN', 'ro_remark_code', 'mjob', 
                  'TAR', 'RO Remarks', 'PENDNCY_RESN_DESC']
    else:
        columns = ['RO ID', 'Landed Cost', 'RO Date', 'Branch', 'RO Status', 'Service Adviser Name',
                  'Reg. Number', 'model_group_name', 'segment', 'VIN', 'ro_remark_code', 'RO Remarks', 
                  'PENDNCY_RESN_DESC']
    
    # Keep only columns that exist
    columns = [col for col in columns if col in df.columns]
    export_df = df[columns].copy()
    
    # Create CSV
    csv_buffer = BytesIO()
    export_df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"RO_Export_{service_category}_{timestamp}.csv"
    
    return StreamingResponse(
        iter([csv_buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/")
def read_root():
    """Serve dashboard HTML"""
    if os.path.exists('dashboard.html'):
        return FileResponse('dashboard.html', media_type='text/html')
    return {"message": "Open RO Dashboard"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
