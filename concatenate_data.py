import pandas as pd
import os
import csv
import warnings
import numpy as np

# Suppress ffill/bfill warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# File paths
file_2023 = 'data/02FW005_raw_CR350_1379_20231102.csv'
file_2024 = 'data/02FW005_raw_CR350_1379_20240524.csv'
file_2025 = 'data/02FW005_raw_CR350_1379_20250521.csv'
output_file = 'data/concatenated_all_years.csv'
duplicate_report_file = 'data/duplicates_report.csv'
STATION_ID = "02FW005"

Field_time_in_1 = "2023-11-02 14:33"
Field_time_out_1 = "2023-11-02 17:00"
Field_time_in_2 = "2024-05-24 08:19"
Field_time_out_2 = "2024-05-24 09:10"
Field_time_in_3 = "2025-05-21 09:48"
Field_time_out_3 = "2025-05-21 10:55"

# Config: Year -> (Data ID number, raw_file_path)
year_config = {
    2023: {'id_num': 222, 'path': file_2023},
    2024: {'id_num': 39,  'path': file_2024},
    2025: {'id_num': 244, 'path': file_2025}
}

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

column_rename = {
    'VP_mbar_Avg': 'VP_hPa_Avg',
    'BP_mbar_Avg': 'BP_hPa_Avg'
}

column_unit_rename = {
    "mbar": "hPa"
}

def parse_toa5_header(filepath):
    """
    Parses the first line of a TOA5 file to extract:
    Model, Serial, OS, ProgramName.
    Returns a dict with:
      logger_id: "{Model}-{Serial}"
      logger_script: "{Model}-{Serial}-{StationID}-{ProgramName}" (approx)
      logger_software: "{Model}-{Serial}-{OS}"
    """
    meta = {
        'logger_id': '-9999',
        'logger_script': '-9999',
        'logger_software': '-9999'
    }
    
    try:
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return meta

        with open(filepath, 'r') as f:
            line = f.readline().strip()
            # Split by comma, strip quotes
            parts = [p.strip().strip('"') for p in line.split(',')]
            
            # Expecting 8 parts for standard TOA5
            # 0: Format (TOA5)
            # 1: StationName (Logger)
            # 2: Model (CR350)
            # 3: Serial (1379)
            # 4: OS (CR350.Std.01.00)
            # 5: Program (CPU:...)
            # 6: Sig
            # 7: Table
            
            if len(parts) >= 6:
                model = parts[2]
                serial = parts[3]
                os_ver = parts[4]
                prog_name = parts[5]
                
                # Construct IDs
                meta['logger_id'] = f"{model}-{serial}"
                meta['logger_software'] = f"{model}-{serial}-{os_ver}"
                # Using Program Name as version for now as requested format implies station/version logic
                # Format requested: model-serial-station-version
                # We have STATION_ID global. 
                # Program name example: CPU:CheslattaLake_CR350_20230629.CRB
                # Just use the full prog name as 'version' component
                meta['logger_script'] = f"{model}-{serial}-{STATION_ID}-{prog_name}"
                
    except Exception as e:
        print(f"Warning: Could not parse header for {filepath}: {e}")
        
    return meta

def read_data(year, config_entry):
    filepath = config_entry['path']
    id_num = config_entry['id_num']
    
    print(f"Reading {filepath} (Year {year})...")
    
    if not os.path.exists(filepath):
        print(f"Skipping {year}: File not found {filepath}")
        return pd.DataFrame(), {}, {}

    # 1. Parse Metadata
    meta = parse_toa5_header(filepath)
    
    # 2. Read Units (Row 2, index 2 if 0-based lines)
    try:
        units_df = pd.read_csv(filepath, header=None, skiprows=2, nrows=1)
        units_list = units_df.iloc[0].mask(pd.isna, "").tolist()
    except Exception as e:
        print(f"Error reading units: {e}")
        units_list = []

    # 3. Read Header (Row 1, index 1)
    try:
        header_df = pd.read_csv(filepath, header=None, skiprows=1, nrows=1)
        header_list = header_df.iloc[0].tolist()
    except Exception as e:
        print(f"Error reading header: {e}")
        return pd.DataFrame(), {}, meta
    
    if len(units_list) < len(header_list):
        units_list += [""] * (len(header_list) - len(units_list))
    
    units_map = dict(zip(header_list, units_list))
    
    # 4. Read Data
    # Skip TOA5 header (0), Units (2), Type (3). Keep Header (1).
    df = pd.read_csv(filepath, skiprows=[0, 2, 3], 
                     na_values=['NAN', '"NAN"', '', '-7999', '7999'], 
                     keep_default_na=True, skipinitialspace=True, low_memory=False)
    
    # robustly parse timestamp
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    # 5. Add Metadata Columns
    # Data ID: Just the number (string) as requested
    df['Data_ID'] = str(id_num)
    df['Station_ID'] = STATION_ID
    df['Logger_ID'] = meta['logger_id']
    df['Logger_Script'] = meta['logger_script']
    df['Logger_Software'] = meta['logger_software']
    
    return df, units_map, meta

def main():
    # Store all dataframes and metadata
    dfs = []
    all_units = {} # catch-all units map
    latest_meta = {} 
    
    # 1. Process each file
    for year in sorted(year_config.keys()):
        cfg = year_config[year]
        df, units, meta = read_data(year, cfg)
        
        if df.empty:
            continue

        # Apply Mapping if 2023
        if year == 2023:
            df = df.rename(columns=column_mapping)
            # Update units map keys
            new_units = {}
            for k, v in units.items():
                new_key = column_mapping.get(k, k)
                new_units[new_key] = v
            units = new_units
            
        dfs.append(df)
        all_units.update(units)
        # Always update latest meta as we process in chronological order
        latest_meta = meta 

    if not dfs:
        print("No data loaded!")
        return

    # 2. Concatenate
    print("Concatenating datasets...")
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"Total records: {len(df_all)}")

    df_all = df_all.rename(columns=column_rename)
    
    # Update units map to reflect column renames and unit changes
    new_all_units = {}
    for col, unit in all_units.items():
        new_col = column_rename.get(col, col)
        new_unit = column_unit_rename.get(unit, unit)
        new_all_units[new_col] = new_unit
    all_units = new_all_units
    
    # 3. Process Time
    if 'TIMESTAMP' not in df_all.columns:
        raise ValueError("TIMESTAMP column missing!")
    
    # Already converted in read_data, but good to ensure
    # df_all['TIMESTAMP'] = pd.to_datetime(df_all['TIMESTAMP'])
    df_all = df_all.sort_values('TIMESTAMP')
    
    # Deduplicate
    df_all = df_all.drop_duplicates(subset=['TIMESTAMP'], keep='first')
    
    df_all = df_all.set_index('TIMESTAMP')
    
    # 4. Resample (Gap Filling)
    print("Resampling to 15T...")
    # Resample to 15T, insert NaNs
    df_resampled = df_all.resample('15T').asfreq()
    
    # 5. Construct Flags
    # Identify data columns (exclude metadata columns we just added)
    meta_cols = ['Data_ID', 'Station_ID', 'Logger_ID', 'Logger_Script', 'Logger_Software']
    data_cols = [c for c in df_resampled.columns if c not in meta_cols and c != 'RECORD']
    
    # Create final DataFrame with ordering
    df_final = df_resampled.reset_index()
    
    # Fill text metadata for gap-filled rows
    df_final['Station_ID'] = df_final['Station_ID'].fillna(STATION_ID)
    
    # Ffill metadata
    for mc in meta_cols:
        if mc in df_final.columns:
            df_final[mc] = df_final[mc].fillna(method='ffill')
            # If start of file has na (resampling before first record?), bfill
            df_final[mc] = df_final[mc].fillna(method='bfill')
    
    # Now construct column list
    final_col_order = ['TIMESTAMP', 'RECORD'] + data_cols + meta_cols
    # Ensure all exist
    final_col_order = [c for c in final_col_order if c in df_final.columns]
    
    df_final = df_final[final_col_order]
    
    # Add Flags
    cols_to_write = []
    
    # Preparing the header rows lists
    row_headers = [] # Row 1
    row_units = []   # Row 2

    for col in df_final.columns:
        if col == 'CVMeta':
            continue
        if col == "Invalid_Wind_Avg":
            continue

        cols_to_write.append(col)
        row_headers.append(col)
        
        # Units
        if col == 'TIMESTAMP': u = 'TS'
        elif col == 'RECORD': u = 'RN'
        else: u = all_units.get(col, "")
        row_units.append(u)
        
        # Flag Column
        if col not in ['TIMESTAMP'] + meta_cols:
            flag_col = f"{col}_Flag"
            df_final[flag_col] = ""
            cols_to_write.append(flag_col)
            
            # Set ERR flag logic
            # We want to flag ANY value that is present but not a valid number (INF, text, etc) as ERR
            # 1. Convert to numeric, coercing errors to NaN
            # Keep original to check what was there before
            original_series = df_final[col].copy()
            numeric_series = pd.to_numeric(df_final[col], errors='coerce')
            
            # 2. Identify values that became NaN but were NOT NaN/empty originally
            # These are "bad values" (Text, INF, Name?, etc.)
            # Note: pd.to_numeric handles "INF" by default depending on version, 
            # so we might need to explicitly check infinite if it doesn't coerce them to NaN.
            # But usually we want INF to be NaN + ERR too.
            
            # Check for infinity in the numeric series (if it wasn't coerced)
            is_infinite = np.isinf(numeric_series)
            
            # Check for coercion failures (was valid-ish string, became NaN)
            # We need to be careful with empty strings being "NaN" in pandas read_csv
            # If it was already NaN, it's Missing (M), not Error.
            was_not_nan = original_series.notna() & (original_series != "")
            became_nan = numeric_series.isna()
            
            # ERR condition: (Is Infinite) OR (Was Not NaN AND Became NaN when coerced)
            mask_err = is_infinite | (was_not_nan & became_nan)
            
            if mask_err.any():
                df_final.loc[mask_err, flag_col] = "ERR"
                # Standardize to NaN
                numeric_series[mask_err] = np.nan
            
            # Apply the numeric series to the dataframe
            df_final[col] = numeric_series

            # Set M flag
            # Only apply M to NaNs that are NOT already flagged as ERR
            mask_nan = df_final[col].isna()
            # If it's NaN now, and NOT ERR, then it's Missing
            mask_m = mask_nan & (df_final[flag_col] != "ERR")
            df_final.loc[mask_m, flag_col] = "M"

            # Set V flag (Field Visits)
            # Parse and round field visits
            # (Ideally define this list outside loop for efficiency, but let's keep it robust and simple here)
            field_visits = [
                (Field_time_in_1, Field_time_out_1),
                (Field_time_in_2, Field_time_out_2),
                (Field_time_in_3, Field_time_out_3)
            ]

            for start_str, end_str in field_visits:
                # Parse
                try:
                    t_start = pd.to_datetime(start_str)
                    t_end = pd.to_datetime(end_str)
                    
                    # Round down start to nearest 15T
                    t_start_rounded = t_start.floor('15T')
                    
                    # Round up end to nearest 15T
                    t_end_rounded = t_end.ceil('15T')
                    
                    # Apply mask
                    mask_visit = (df_final['TIMESTAMP'] >= t_start_rounded) & (df_final['TIMESTAMP'] <= t_end_rounded)
                    
                    # Append V flag instead of overwriting
                    # Identify rows with existing flags (e.g. "M") within the mask
                    mask_existing = mask_visit & (df_final[flag_col] != "")
                    df_final.loc[mask_existing, flag_col] = df_final.loc[mask_existing, flag_col] + ", V"
                    
                    # Identify rows with empty flags within the mask
                    mask_empty = mask_visit & (df_final[flag_col] == "")
                    df_final.loc[mask_empty, flag_col] = "V"
                        
                except Exception as e:
                    print(f"Warning: Could not process field visit time {start_str}-{end_str}: {e}")
                
            # Add headers for flag col
            row_headers.append(flag_col)
            row_units.append("") 
            
    # Reorder df_final
    df_final = df_final[cols_to_write]
    
    # Save
    print(f"Saving to {output_file}...")
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row_headers)
        writer.writerow(row_units)
        
    df_final.to_csv(output_file, mode='a', header=False, index=False, na_rep='NaN')
    print("Done!")

if __name__ == "__main__":
    main()
