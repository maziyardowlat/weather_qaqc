import pandas as pd
import os

# File paths
file_2023 = 'data/02FW005_raw_CR350_1379_20231102.csv'
file_2024 = 'data/02FW005_raw_CR350_1379_20240524.csv'
file_2025 = 'data/02FW005_raw_CR350_1379_20250521.csv'
output_file = 'data/concatenated_all_years.csv'

# Mapping from 2023 to 2024/2025
column_mapping = {
    'battv': 'BattV_Avg',
    'Ptmp': 'PTemp_C_Avg',
    'stmp1': 'stmp_Avg',
    'dsws': 'SlrFD_W_Avg',
    'rtot': 'Rain_mm_Tot',
    'strike': 'Strikes_Tot',
    'strikeD': 'Dist_km_Avg',
    'wind': 'WS_ms_Avg',
    'wdir': 'WindDir',
    'windM': 'MaxWS_ms_Avg',
    'tmp': 'AirT_C_Avg',
    'vap': 'VP_mbar_Avg',
    'press': 'BP_mbar_Avg',
    'rh': 'RH',
    'tmp2': 'RHT_C_Avg',
    'tiltNS': 'TiltNS_deg_Avg',
    'tiltWE': 'TiltWE_deg_Avg',
    'dswt': 'SlrTF_MJ_Tot',
    'Invalid_Wind': 'Invalid_Wind_Avg',
    'dt': 'DT_Avg',
    'tcdt': 'TCDT_Avg',
    'snod': 'DBTCDT_Avg',
    'swin': 'SWin_Avg',
    'swout': 'SWout_Avg',
    'lwin': 'LWin_Avg',
    'lwout': 'LWout_Avg',
    'swnet': 'SWnet_Avg',
    'lwnet': 'LWnet_Avg',
    'swalbedo': 'SWalbedo_Avg',
    'nr': 'NR_Avg',
    'stmp2': 'gtmp_Avg'
}

def read_data(filepath):
    """
    Reads data skipping proper TOA5 rows:
    Row 0: Info (Skip)
    Row 1: Header (Use)
    Row 2: Units (Use for metadata, skip for reading)
    Row 3: Rate (Skip)
    """
    print(f"Reading {filepath}...")
    
    # Read Units row using pandas to handle CSV parsing correctly
    # header=None means row 0 is first row. Row 2 is units.
    units_df = pd.read_csv(filepath, header=None, skiprows=2, nrows=1)
    # The columns of units_df will be 0,1,2..., we just want the values
    units_list = units_df.iloc[0].tolist()
    
    # Read Header row to match units
    header_df = pd.read_csv(filepath, header=None, skiprows=1, nrows=1)
    header_list = header_df.iloc[0].tolist()
    
    # Create Name -> Unit map
    units_map = dict(zip(header_list, units_list))
    
    # Read Data
    # skiprows=[0, 2, 3] relative to file start
    # Row 1 becomes the header
    # Handle "NAN" strings, empty strings, and various placeholders as NaN
    df = pd.read_csv(filepath, skiprows=[0, 2, 3], na_values=['NAN', '"NAN"', ''], keep_default_na=True, skipinitialspace=True, low_memory=False)
    
    return df, units_map

def main():
    # 1. Load Data
    df_2023, units_2023 = read_data(file_2023)
    df_2024, units_2024 = read_data(file_2024)
    df_2025, units_2025 = read_data(file_2025)
    
    print(f"Loaded 2023: {df_2023.shape}")
    print(f"Loaded 2024: {df_2024.shape}")
    print(f"Loaded 2025: {df_2025.shape}")

    # 2. Normalize 2023
    print("Mapping 2023 columns...")
    df_2023 = df_2023.rename(columns=column_mapping)
    
    # Verify all mapped columns exist in new schema
    # We can check against df_2024 columns
    target_cols = set(df_2024.columns)
    current_cols = set(df_2023.columns)
    
    # For concatenation, pandas aligns by name.
    # Check for any 2023 columns that didn't map (optional, but good for debug)
    unmapped = current_cols - target_cols
    if unmapped:
        print(f"Warning: Columns in 2023 not in 2024 schema: {unmapped}")
        
    # 3. Concatenate
    print("Concatenating datasets...")
    # ignore_index=True because we will build a new time index
    df_all = pd.concat([df_2023, df_2024, df_2025], ignore_index=True)
    print(f"Total records: {len(df_all)}")
    
    # 4. Process Time
    print("Processing timestamps...")
    if 'TIMESTAMP' not in df_all.columns:
        raise ValueError("TIMESTAMP column missing!")
        
    df_all['TIMESTAMP'] = pd.to_datetime(df_all['TIMESTAMP'], format='mixed')
    df_all = df_all.sort_values('TIMESTAMP')
    
    # Deduplicate (Keep first)
    before_dedup = len(df_all)
    
    # DEBUG: Inspect duplicates
    dups = df_all[df_all.duplicated(subset=['TIMESTAMP'], keep=False)]
    if not dups.empty:
        print(f"\nWARNING: Found {len(dups)} duplicate records (showing all occurrences).")
        print("Sample of duplicates (first 10 rows):")
        # Print just TIMESTAMP and valid columns to keep output clean, or just head
        print(dups.sort_values('TIMESTAMP').head(10))
        
        dup_file = "data/duplicates_report.csv"
        dups.sort_values('TIMESTAMP').to_csv(dup_file)
        print(f"Full duplicate report saved to {dup_file}\n")

    df_all = df_all.drop_duplicates(subset=['TIMESTAMP'], keep='first')
    after_dedup = len(df_all)
    if before_dedup != after_dedup:
        print(f"Dropped {before_dedup - after_dedup} duplicate timestamps.")
        
    df_all = df_all.set_index('TIMESTAMP')
    
    # 5. Resample
    print("Resampling to 15T...")
    # asfreq() puts Nan where data is missing
    df_resampled = df_all.resample('15T').asfreq()
    
    print(f"Final shape after resampling: {df_resampled.shape}")
    
    # 6. Prepare Output
    # We need to construct the CSV with the Units row.
    # The target schema is 2024/2025.
    # We should use units_2024 (or 2025) for the columns.
    
    # Identify the column order of the final dataframe
    final_cols = df_resampled.columns.tolist()
    
    # Construct Units row
    output_units = []
    # Index is TIMESTAMP usually, but reset_index will bring it back as column
    df_final = df_resampled.reset_index()
    final_cols_with_ts = df_final.columns.tolist()
    
    for col in final_cols_with_ts:
        if col == 'TIMESTAMP':
            output_units.append('TS') # Standard TOA5 unit for Timestamp
        elif col == 'RECORD':
            output_units.append('RN')
        else:
            # Look up in 2024 units
            u = units_2024.get(col)
            if u is None:
                u = ""
            output_units.append(u)
            
            # --- NEW: Add Flag Column for every data column ---
            # If the column is a data column (not TIMESTAMP/RECORD/Index), add a Flag column
            flag_col_name = f"{col}_Flag"
            
            # Create the flag data
            # Initialize with empty string
            df_final[flag_col_name] = ""
            
            # Set 'M' where data is NaN
            # Note: We need to handle object types or pure numerics cautiously
            mask_nan = df_final[col].isna()
            df_final.loc[mask_nan, flag_col_name] = "M"
            
            # Add unit for flag (empty)
            output_units.append("") # Unit for the flag column
            
    # Reorder columns: Data, Data_Flag, Data2, Data2_Flag...
    # We built output_units in the desired order (Col, Flag, Col, Flag)
    # So we just need to reconstruct the column list for the dataframe
    ordered_cols = []
    for col in final_cols_with_ts:
        ordered_cols.append(col)
        if col not in ['TIMESTAMP', 'RECORD']:
            ordered_cols.append(f"{col}_Flag")
            
    df_final = df_final[ordered_cols]
            
    # Write to CSV
    print(f"Saving to {output_file}...")
    
    with open(output_file, 'w') as f:
        # 1. Header Row
        # Use df_final.columns to ensure we match the actual data columns
        f.write(",".join(df_final.columns) + "\n")
        # 2. Units Row
        # Make sure to handle NaNs or non-strings in units
        clean_units = [str(x) if pd.notna(x) else "" for x in output_units]
        f.write(",".join(clean_units) + "\n")
        
    # Append data
    # mode='a' (append)
    df_final.to_csv(output_file, mode='a', header=False, index=False, na_rep='NaN')
    
    print("Done!")

if __name__ == "__main__":
    main()
