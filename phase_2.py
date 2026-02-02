import pandas as pd
import numpy as np
import os
import csv
from datetime import datetime, timedelta, timezone
from suntime import Sun

INPUT_FILE = 'data/concatenated_one_year.csv'
OUTPUT_FILE = 'data/concatenated_one_year_phase2.csv'



# Threshold Configuration {Column: (Min, Max)}
THRESHOLDS = {
    'AirT_C_Avg': (-50, 50),
    'RHT_C_Avg': (-50, 50),
    'RH': (0, 100),
    'BP_mbar_Avg': (850, 1050),
    'WS_ms_Avg': (0, 50),
    'WindDir': (0, 360),
    'Rain_mm_Tot': (0, 50),
    'SWin_Avg': (0, 1350),
    'SWout_Avg': (0, 1350),
    'LWin_Avg': (0, 600),
    'LWout_Avg': (0, 600),
    'TiltNS_deg_Avg': (-1, 5),
    'TiltWE_deg_Avg': (-1, 5),
    'DBTCDT_Avg': (-5, 250), 
    'DT_Avg': (50, 1000),    
    'stmp_Avg': (-50, 50),   
    'gtmp_Avg': (-50, 50),
    'BattV_Avg': (10, 16),
    'VP_mbar_Avg': (0, 80)
}


Add_caution_flag = [
    'BattV_Avg', 'PTemp_C_Avg', 'SlrFD_W_Avg', 'Dist_km_Avg', 'WS_ms_Avg', 
    'MaxWS_ms_Avg', 'AirT_C_Avg', 'VP_mbar_Avg', 'BP_mbar_Avg', 'RHT_C_Avg', 
    'TiltNS_deg_Avg', 'TiltWE_deg_Avg', 'Invalid_Wind_Avg', 'DT_Avg', 
    'TCDT_Avg', 'DBTCDT_Avg', 'SWin_Avg', 'SWout_Avg', 'LWin_Avg', 
    'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 'SWalbedo_Avg', 'NR_Avg', 
    'stmp_Avg', 'gtmp_Avg'
]

SOLAR_COLUMNS = [
    'SlrFD_W_Avg', 'SWin_Avg', 'SWout_Avg', 'SWnet_Avg', 'SlrTF_MJ_Tot'
]

def load_data(filepath):
    print(f"Reading {filepath}...")
    
    # Read Header and Units separately to preserve them
    header_df = pd.read_csv(filepath, header=None, nrows=2)
    headers = header_df.iloc[0].tolist()
    units = header_df.iloc[1].tolist()
    
    # Read proper data
    # Row 0 is header, Row 1 is units. So skip=[1] (Units) relative to 0-based index?
    # pd.read_csv uses 0-based row indices.
    # header=0 means first row is header.
    # We want to skip the units row, which is row 1 (the second row).
    df = pd.read_csv(filepath, header=0, skiprows=[1], 
                     na_values=['NAN', '"NAN"', ''], keep_default_na=True, 
                     skipinitialspace=True, low_memory=False)
    
    return df, headers, units

def apply_uniquecases(df):
    # Check for 'RECORD' or 'Record' column
    found_col = None
    if "RECORD" in df.columns:
        found_col = "RECORD"
    elif "Record" in df.columns:
        found_col = "Record"

    if found_col:
        col = found_col
        # Check for 0 values. Coerce to numeric ensures we handle strings "0" correctly if needed.
        vals = pd.to_numeric(df[col], errors='coerce')
        mask_zero = (vals == 0)

        if mask_zero.any():
            flag_col = f"{col}_Flag"
            if flag_col not in df.columns:
                df[flag_col] = ""

            print(f"  - {col}: Flagging {mask_zero.sum()} records with value 0 as 'LR'")

            current_flags = df[flag_col].fillna("").astype(str)
            targets = current_flags.loc[mask_zero]
            # Add C flag, appending if existing flags present
            new_flags = np.where(targets == "", "LR", targets + ", LR")
            df.loc[mask_zero, flag_col] = new_flags

    return df

def apply_thresholds(df):
    print("Applying QC Thresholds...")
    
    for col, (min_v, max_v) in THRESHOLDS.items():
        if col not in df.columns:
            print(f"Warning: Column {col} not found in dataset.")
            continue
            
        flag_col = f"{col}_Flag"
        if flag_col not in df.columns:
            # Should have been created in Phase 1, but create if missing
            df[flag_col] = ""
            
        # Create numeric series for comparison, handling non-numerics if any
        vals = pd.to_numeric(df[col], errors='coerce')
        
        # Identify Violations
        mask_fail = (vals < min_v) | (vals > max_v)
        
        # Check Existing Flags for 'M'
        # ensure string
        current_flags = df[flag_col].fillna("").astype(str)
        mask_has_m = current_flags.str.contains('M')
        
        # Final Mask: Failed Threshold AND Not Missing
        mask_apply = mask_fail & (~mask_has_m)
        
        # Apply 'T'
        if mask_apply.any():
            count = mask_apply.sum()
            print(f"  - {col}: Flagging {count} records outside [{min_v}, {max_v}]")
            targets = current_flags.loc[mask_apply]
            new_flags = np.where(targets == "", "T", targets + ", T")
            df.loc[mask_apply, flag_col] = new_flags
            
    return df

def apply_legacy_flags(df, target_id="222"):
    print(f"Applying Legacy 'C' Flags for Data_ID {target_id}...")
    
    # Ensure Data_ID is string for comparison
    if 'Data_ID' not in df.columns:
        print("Warning: Data_ID column not found. Skipping legacy flags.")
        return df
        
    mask_legacy = (df['Data_ID'].astype(str) == target_id)
    
    if not mask_legacy.any():
        print(f"No records found with Data_ID {target_id}")
        return df
        
    count = mask_legacy.sum()
    print(f"Found {count} legacy records.")
    
    for col in Add_caution_flag:
        if col not in df.columns:
            continue
            
        flag_col = f"{col}_Flag"
        if flag_col not in df.columns:
            df[flag_col] = ""
            
        current_flags = df.loc[mask_legacy, flag_col].fillna("").astype(str)
        
        # Append 'C', handling existing flags
        new_flags = np.where(current_flags == "", "C", current_flags + ", C")
        
        df.loc[mask_legacy, flag_col] = new_flags
        
    return df

def apply_nighttime_flags(df):
    print("Applying Nighttime 'Z' Flags for Solar Data...")
    latitude = 53.7217
    longitude = -125.6417

    sun = Sun(latitude, longitude)
    
    # 1. Ensure TIMESTAMP is datetime
    if 'TIMESTAMP' not in df.columns:
        print("Error: TIMESTAMP column missing.")
        return df
        
    # Ensure datetime objects
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    # 2. Define Timezone: Fixed PDT (UTC-7)
    # The data is in PDT year-round.
    tz_pdt = timezone(timedelta(hours=-7))
    
    # 3. Iterate by unique dates to calculate sunrise/set
    # Create a column for just the date component to iterate
    temp_dates = df['TIMESTAMP'].dt.date
    unique_dates = temp_dates.unique()
    
    records_flagged = 0
    
    for d in unique_dates:
        # Convert date to datetime to satisfy suntime requirements
        current_date = datetime(d.year, d.month, d.day)
        
        # Calculate Sunrise/Sunset (Returns UTC datetime)
        try:
            rise_utc = sun.get_sunrise_time(current_date)
            set_utc = sun.get_sunset_time(current_date)
        except Exception as e:
            print(f"Warning: Could not calc sun time for {d}: {e}")
            continue
            
        # Convert to Fixed PDT
        rise_pdt = rise_utc.astimezone(tz_pdt)
        set_pdt = set_utc.astimezone(tz_pdt)
        
        # Make Naive to match CSV TIMESTAMPs (which are naive PDT)
        rise_naive = rise_pdt.replace(tzinfo=None)
        set_naive = set_pdt.replace(tzinfo=None)
        
        # Mask for this date
        mask_date = (temp_dates == d)
        
        # Get the timestamps for this date
        ts_values = df.loc[mask_date, 'TIMESTAMP']
        
        # Night Mask: Time < Rise OR Time > Set
        mask_night_time = (ts_values < rise_naive) | (ts_values > set_naive)
        
        # Get indices
        night_index_subset = ts_values[mask_night_time].index
        
        if len(night_index_subset) == 0:
            continue
            
        # Check Solar Columns for Non-Zero
        for col in SOLAR_COLUMNS:
            if col not in df.columns:
                continue
                
            # Get values for night rows
            vals = pd.to_numeric(df.loc[night_index_subset, col], errors='coerce').fillna(0)
            
            # Non-zero mask (using small epsilon)
            mask_nonzero = (vals.abs() > 0.0001)
            
            if mask_nonzero.any():
                target_indices = vals[mask_nonzero].index
                
                flag_col = f"{col}_Flag"
                if flag_col not in df.columns:
                    df[flag_col] = ""
                    
                current_flags = df.loc[target_indices, flag_col].fillna("").astype(str)
                new_flags = np.where(current_flags == "", "Z", current_flags + ", Z")
                
                df.loc[target_indices, flag_col] = new_flags
                records_flagged += len(target_indices)

    print(f"Flagged {records_flagged} solar records with 'Z'.")
    return df

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    # 1. Load
    df, headers, units = load_data(INPUT_FILE)
    print(f"Loaded {len(df)} rows.")
    
    # 2. Apply Thresholds
    df = apply_thresholds(df)
    df = apply_uniquecases(df)
    
    # 2b. Apply Legacy Flags
    df = apply_legacy_flags(df)
    
    # 3. Apply Nighttime Flags
    
    # 4. Cleanup Flags (Ensure empty flags are "" not NaN)
    flag_cols = [c for c in df.columns if c.endswith("_Flag")]
    df[flag_cols] = df[flag_cols].fillna("")

    # 5. Save
    print(f"Saving to {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(df.columns) # Header
        # Units row - Need to map current columns to units
        # The units list corresponds to the ORIGINAL headers
        # If columns haven't changed order, we can reuse 'units' list.
        # But safest is to map.
        
        orig_map = dict(zip(headers, units))
        
        current_units = []
        for col in df.columns:
            u = orig_map.get(col, "")
            current_units.append(u)
            
        writer.writerow(current_units)
        
    df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False, na_rep='NaN')
    print("Done!")

if __name__ == "__main__":
    main()
