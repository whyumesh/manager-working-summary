import pandas as pd
import os
from datetime import datetime

# ==================== USER DEFINED PARAMETERS ====================
# Set your working folder path
working_folder = r'C:\Users\YourUsername\Documents\KPI_Work'  # UPDATE THIS PATH

# Input file name
input_file = 'AIL_Effort KPI_YTD 2025.xlsb'

# Optional: Template file name (set to None if not using)
template_file = None  # or 'manager_summary.xlsx'

# Output file name
output_file = 'ABM_Consolidated_Report.xlsx'
# ==================================================================

print("=" * 60)
print("ABM CONSOLIDATION SCRIPT")
print("=" * 60)

# Build full file paths
input_filepath = os.path.join(working_folder, input_file)
output_filepath = os.path.join(working_folder, output_file)

# Check if input file exists
if not os.path.exists(input_filepath):
    print(f"\n❌ ERROR: Input file not found!")
    print(f"   Looking for: {input_filepath}")
    print(f"\n   Please update the 'working_folder' path in the script.")
    exit()

print(f"\n📂 Working Folder: {working_folder}")
print(f"📄 Input File: {input_file}")

# Read the XLSB file - ABM sheet
print(f"\n🔄 Reading ABM sheet from {input_file}...")

try:
    # Try reading ABM sheet
    df = pd.read_excel(input_filepath, sheet_name='ABM', engine='pyxlsb')
    print(f"✓ Successfully read ABM sheet with {len(df)} rows")
except Exception as e:
    print(f"❌ ERROR reading ABM sheet: {str(e)}")
    print("\n💡 Available sheets in the file:")
    try:
        xl_file = pd.ExcelFile(input_filepath, engine='pyxlsb')
        for sheet in xl_file.sheet_names:
            print(f"   - {sheet}")
    except:
        pass
    exit()

# Display column names
print(f"\n📋 Columns found in ABM sheet:")
for i, col in enumerate(df.columns, 1):
    print(f"   {i}. {col}")

# Find ABM Name column
abm_name_col = None
for col in df.columns:
    col_str = str(col).upper().replace(' ', '-').replace('_', '-')
    if 'ABM' in col_str and 'NAME' in col_str:
        abm_name_col = col
        break

if not abm_name_col:
    print("\n❌ ERROR: Could not find 'ABM-Name' column!")
    print("   Please check the column name in your file.")
    exit()

print(f"\n✓ Found ABM Name column: '{abm_name_col}'")

# Function to find column flexibly
def find_column(df, keywords, exact_match=None):
    """Find column that matches keywords"""
    if exact_match and exact_match in df.columns:
        return exact_match
    
    for col in df.columns:
        col_normalized = str(col).upper().replace(' ', '-').replace('_', '-')
        for keyword in keywords:
            keyword_normalized = keyword.upper().replace(' ', '-').replace('_', '-')
            if keyword_normalized in col_normalized or col_normalized in keyword_normalized:
                return col
    return None

# Map required columns
print(f"\n🔍 Mapping columns...")
column_map = {
    'Call Days': find_column(df, ['Call Days', 'Call-Days', 'CallDays', 'Field Work']),
    'Actual DR Calls': find_column(df, ['Actual DR Calls', 'Actual-DR-Calls', 'Doctor Calls', 'DR Calls']),
    'Doctor Call Avg': find_column(df, ['Doctor Call Avg', 'Doctor-Call-Avg', 'Call Avg', 'Call Average']),
    '2PC Freq Cov %': find_column(df, ['2PC Freq Cov %', '2PC-Freq-Cov-%', '2PC Freq Cov', '2PC Cov']),
    'Total DR Cov %': find_column(df, ['Total DR Cov %', 'Total-DR-Cov-%', 'Total Cov', 'Coverage'])
}

print("\n📊 Column Mapping:")
for new_name, old_name in column_map.items():
    status = "✓" if old_name else "✗"
    print(f"   {status} {new_name:20s} -> {old_name}")

# Check for missing columns
missing_cols = [k for k, v in column_map.items() if v is None]
if missing_cols:
    print(f"\n⚠️  WARNING: Could not find these columns: {', '.join(missing_cols)}")
    print("   Proceeding with available columns...")

# Select and prepare data
print(f"\n🔧 Processing data...")
selected_cols = [abm_name_col] + [v for v in column_map.values() if v is not None]
working_df = df[selected_cols].copy()

# Rename columns
rename_dict = {v: k for k, v in column_map.items() if v is not None}
rename_dict[abm_name_col] = 'ABM Name'
working_df.rename(columns=rename_dict, inplace=True)

# Remove rows where ABM Name is null or empty
initial_count = len(working_df)
working_df = working_df[working_df['ABM Name'].notna()]
working_df = working_df[working_df['ABM Name'].astype(str).str.strip() != '']
removed_count = initial_count - len(working_df)

if removed_count > 0:
    print(f"   Removed {removed_count} rows with empty ABM Name")

# Convert numeric columns
numeric_cols = ['Call Days', 'Actual DR Calls', 'Doctor Call Avg', '2PC Freq Cov %', 'Total DR Cov %']
for col in numeric_cols:
    if col in working_df.columns:
        working_df[col] = pd.to_numeric(working_df[col], errors='coerce')

# Group by ABM Name and aggregate
print(f"\n📈 Consolidating data for {working_df['ABM Name'].nunique()} unique ABMs...")

agg_dict = {}
if 'Call Days' in working_df.columns:
    agg_dict['Call Days'] = 'sum'
if 'Actual DR Calls' in working_df.columns:
    agg_dict['Actual DR Calls'] = 'sum'
if 'Doctor Call Avg' in working_df.columns:
    agg_dict['Doctor Call Avg'] = 'mean'
if '2PC Freq Cov %' in working_df.columns:
    agg_dict['2PC Freq Cov %'] = 'mean'
if 'Total DR Cov %' in working_df.columns:
    agg_dict['Total DR Cov %'] = 'mean'

abm_summary = working_df.groupby('ABM Name', as_index=False).agg(agg_dict)

# Round numeric values
if 'Doctor Call Avg' in abm_summary.columns:
    abm_summary['Doctor Call Avg'] = abm_summary['Doctor Call Avg'].round(2)
if '2PC Freq Cov %' in abm_summary.columns:
    abm_summary['2PC Freq Cov %'] = abm_summary['2PC Freq Cov %'].round(2)
if 'Total DR Cov %' in abm_summary.columns:
    abm_summary['Total DR Cov %'] = abm_summary['Total DR Cov %'].round(2)

# Sort by ABM Name
abm_summary = abm_summary.sort_values('ABM Name').reset_index(drop=True)

# Display summary statistics
print(f"\n📊 CONSOLIDATION SUMMARY:")
print(f"   Total ABMs: {len(abm_summary)}")
if 'Call Days' in abm_summary.columns:
    print(f"   Total Call Days: {abm_summary['Call Days'].sum():.0f}")
if 'Actual DR Calls' in abm_summary.columns:
    print(f"   Total DR Calls: {abm_summary['Actual DR Calls'].sum():.0f}")
if 'Doctor Call Avg' in abm_summary.columns:
    print(f"   Avg Doctor Call: {abm_summary['Doctor Call Avg'].mean():.2f}")
if '2PC Freq Cov %' in abm_summary.columns:
    print(f"   Avg 2PC Coverage: {abm_summary['2PC Freq Cov %'].mean():.2f}%")
if 'Total DR Cov %' in abm_summary.columns:
    print(f"   Avg Total Coverage: {abm_summary['Total DR Cov %'].mean():.2f}%")

# Save output
print(f"\n💾 Saving output...")

# Check if template exists
use_template = False
if template_file:
    template_path = os.path.join(working_folder, template_file)
    if os.path.exists(template_path):
        use_template = True
        print(f"   Using template: {template_file}")
        try:
            # Copy template and write data
            import shutil
            shutil.copy(template_path, output_filepath)
            with pd.ExcelWriter(output_filepath, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                abm_summary.to_excel(writer, sheet_name='ABM Summary', index=False)
        except Exception as e:
            print(f"   ⚠️  Could not use template: {str(e)}")
            use_template = False

if not use_template:
    # Create new file
    with pd.ExcelWriter(output_filepath, engine='openpyxl') as writer:
        abm_summary.to_excel(writer, sheet_name='ABM Summary', index=False)

print(f"✓ Output saved to: {output_filepath}")

# Display sample data
print(f"\n📋 SAMPLE DATA (First 5 ABMs):")
print("=" * 60)
print(abm_summary.head().to_string(index=False))
print("=" * 60)

print("\n✅ CONSOLIDATION COMPLETE!")
print(f"📁 Check output file: {output_file}")
