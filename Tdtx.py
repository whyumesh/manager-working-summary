import pandas as pd
import os
from datetime import datetime

# ==================== USER DEFINED PARAMETERS ====================
# Set your input file path and month
input_path = 'Y:\\OLD DATA\\Python KPI Extraction\\AIL 2025\\Nov 25 10112025\\'
output_path = input_path  # Can change if you want different output location
month_year = 'Nov 2025'  # Should match the file naming convention

# Optional: Template file path (if using template)
template_file = 'manager_summary.xlsx'  # Set to None if not using template
# ==================================================================

print(f"Starting ABM Consolidation for {month_year}...")

# Find all KPI files in the directory
kpi_files = [f for f in os.listdir(input_path) if f.startswith('KPI') and f.endswith('.xlsx') and month_year in f]

print(f"Found {len(kpi_files)} KPI files")

# Initialize list to store all ABM data
all_abm_data = []

# Process each KPI file
for file in kpi_files:
    file_path = os.path.join(input_path, file)
    print(f"Processing: {file}")
    
    try:
        # Read the ABM sheet (try different possible names)
        df = None
        try:
            df = pd.read_excel(file_path, sheet_name='final_KPI_ABM')
        except:
            try:
                df = pd.read_excel(file_path, sheet_name='ABM')
            except:
                print(f"  Warning: No ABM sheet found in {file}")
                continue
        
        if df is not None and not df.empty:
            # Check if ABM Name column exists
            abm_col = None
            for col in df.columns:
                if 'ABM' in col.upper() and 'NAME' in col.upper():
                    abm_col = col
                    break
            
            if abm_col:
                all_abm_data.append(df)
                print(f"  ✓ Added {len(df)} records")
            else:
                print(f"  Warning: ABM Name column not found in {file}")
    
    except Exception as e:
        print(f"  Error reading {file}: {str(e)}")

# Combine all ABM data
if not all_abm_data:
    print("ERROR: No ABM data found in any files!")
    exit()

combined_df = pd.concat(all_abm_data, ignore_index=True)
print(f"\nTotal ABM records: {len(combined_df)}")

# Find the ABM Name column
abm_name_col = None
for col in combined_df.columns:
    if 'ABM' in col.upper() and 'NAME' in col.upper():
        abm_name_col = col
        break

if not abm_name_col:
    print("ERROR: ABM Name column not found!")
    print("Available columns:", combined_df.columns.tolist())
    exit()

print(f"Using ABM Name column: '{abm_name_col}'")

# Define column mappings (flexible matching)
def find_column(df, keywords):
    """Find column that contains any of the keywords"""
    for col in df.columns:
        col_upper = str(col).upper().replace(' ', '-').replace('_', '-')
        for keyword in keywords:
            if keyword.upper() in col_upper:
                return col
    return None

# Map required columns
column_map = {
    'Call Days': find_column(combined_df, ['Call Days', 'Call-Days', 'CallDays']),
    'Actual DR Calls': find_column(combined_df, ['Actual DR Calls', 'Actual-DR-Calls', 'Doctor Calls']),
    'Doctor Call Avg': find_column(combined_df, ['Doctor Call Avg', 'Doctor-Call-Avg', 'Call Avg']),
    '2PC Freq Cov %': find_column(combined_df, ['2PC Freq Cov %', '2PC-Freq-Cov-%', '2PC Freq Cov']),
    'Total DR Cov %': find_column(combined_df, ['Total DR Cov %', 'Total-DR-Cov-%', 'Total Cov'])
}

print("\nColumn mapping:")
for new_name, old_name in column_map.items():
    print(f"  {new_name}: {old_name}")

# Check for missing columns
missing_cols = [k for k, v in column_map.items() if v is None]
if missing_cols:
    print(f"\nWARNING: Could not find columns: {missing_cols}")
    print("Available columns:", combined_df.columns.tolist())

# Select and rename columns
selected_cols = [abm_name_col] + [v for v in column_map.values() if v is not None]
consolidated_df = combined_df[selected_cols].copy()

# Rename columns
rename_dict = {v: k for k, v in column_map.items() if v is not None}
rename_dict[abm_name_col] = 'ABM Name'
consolidated_df.rename(columns=rename_dict, inplace=True)

# Convert numeric columns to appropriate types
numeric_cols = ['Call Days', 'Actual DR Calls', 'Doctor Call Avg', '2PC Freq Cov %', 'Total DR Cov %']
for col in numeric_cols:
    if col in consolidated_df.columns:
        consolidated_df[col] = pd.to_numeric(consolidated_df[col], errors='coerce')

# Group by ABM Name and aggregate
print(f"\nConsolidating data for {consolidated_df['ABM Name'].nunique()} unique ABMs...")

abm_summary = consolidated_df.groupby('ABM Name').agg({
    'Call Days': 'sum',
    'Actual DR Calls': 'sum',
    'Doctor Call Avg': 'mean',  # Average of averages
    '2PC Freq Cov %': 'mean',   # Average coverage
    'Total DR Cov %': 'mean'    # Average coverage
}).reset_index()

# Round the values
abm_summary['Doctor Call Avg'] = abm_summary['Doctor Call Avg'].round(2)
abm_summary['2PC Freq Cov %'] = abm_summary['2PC Freq Cov %'].round(2)
abm_summary['Total DR Cov %'] = abm_summary['Total DR Cov %'].round(2)

# Sort by ABM Name
abm_summary = abm_summary.sort_values('ABM Name').reset_index(drop=True)

print(f"\nConsolidated Summary:")
print(f"  Total ABMs: {len(abm_summary)}")
print(f"  Total Call Days: {abm_summary['Call Days'].sum():.0f}")
print(f"  Total DR Calls: {abm_summary['Actual DR Calls'].sum():.0f}")

# Save output
output_filename = f'ABM_Consolidated_Report_{month_year.replace(" ", "_")}.xlsx'
output_filepath = os.path.join(output_path, output_filename)

# Check if template exists
use_template = False
if template_file:
    template_path = os.path.join(input_path, template_file)
    if os.path.exists(template_path):
        use_template = True
        print(f"\nUsing template: {template_file}")

if use_template:
    # Load template and write data
    with pd.ExcelWriter(output_filepath, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        abm_summary.to_excel(writer, sheet_name='ABM Summary', index=False, startrow=1)
else:
    # Create new file
    with pd.ExcelWriter(output_filepath, engine='openpyxl') as writer:
        abm_summary.to_excel(writer, sheet_name='ABM Summary', index=False)

print(f"\n✓ Output saved to: {output_filepath}")
print(f"\nSample data (first 5 rows):")
print(abm_summary.head())

print("\n=== Consolidation Complete ===")
