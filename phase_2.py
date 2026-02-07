import pandas as pd
import numpy as np
import os
import csv
from datetime import datetime, timedelta, timezone
from suntime import Sun

INPUT_FILE = 'data/concatenated_all_years.csv'
OUTPUT_FILE = 'data/concatenated_all_years_phase2.csv'

# Threshold Configuration {Column: (Min, Max)}
SENSOR_HEIGHT = 166 # Default Sensor Height in cm (H).

# Threshold Configuration {Column: (Min, Max)}
THRESHOLDS = {
    'BattV_Avg': (10, 16),
    'PTemp_C_Avg': (-40, 70),
    'RHT_C_Avg': (-40, 50),
    'SlrFD_W_Avg': (0, 1350),
    'Rain_mm_Tot': (0, 33),
    'Strikes_tot': (0, 66635),
    'Dist_km_Avg': (0, 40),
    'WS_ms_Avg': (0, 30),
    'WindDir': (0, 360),
    'AirT_C_Avg': (-50, 60),
    'VP_hPa_Avg': (0, 470),
    'BP_hPa_Avg': (850, 1050),
    'RH': (0, 100),
    'TiltNS_deg_Avg': (-3, 3),
    'TiltWE_deg_Avg': (-3, 3),
    'SlrTF_MJ_Tot': (0, 1.215),
    'DT_Avg': (50, SENSOR_HEIGHT + 5), 
    'DBTCDT_Avg': (0, SENSOR_HEIGHT + 5),
    'SWin_Avg': (-5, 1350), # changed this from 0 to -1, it seems that at night, SWin is slightly negative (from around 22:00 to 04:45)
    'SWout_Avg': (0, 'SWin_Avg'), #changed this from 0 to -1, it seems that at night, SWout is slightly negative (from around 22:00 to 04:45)
    'LWin_Avg': (100, 550),
    'LWout_Avg': (150, 600),
    'SWnet_Avg': (-5, 1350), #changed this from 0 to -5
    'LWnet_Avg': (-300, 100),
    'SWalbedo_Avg': (0, 1),
    'NR_Avg': (-200, 1000),
    'stmp_Avg': (-50, 60),
    'gtmp_Avg': (-50, 60), 
}

# Dependency Configuration
DEPENDENCY_CONFIG = [
    # ClimaVue50
    {'target': 'SlrFD_W_Avg', 'sources': ['TiltNS_deg_Avg', 'TiltWE_deg_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'Rain_mm_Tot', 'sources': ['TiltNS_deg_Avg', 'TiltWE_deg_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'AirT_C_Avg', 'sources': ['SlrFD_W_Avg', 'WS_ms_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'VP_hPa_Avg', 'sources': ['RHT_C_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'RH', 'sources': ['VP_hPa_Avg', 'AirT_C_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'SlrTF_MJ_Tot', 'sources': ['SlrFD_W_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SlrTF_MJ_Tot', 'sources': ['SlrFD_W_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    
    # SR50
    {'target': 'TCDT_Avg', 'sources': ['DT_Avg'], 'trigger_flags': ['T'], 'set_flag': 'DF'},
    {'target': 'TCDT_Avg', 'sources': ['AirT_C_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'SU'},
    {'target': 'DBTCDT_Avg', 'sources': ['TCDT_Avg'], 'trigger_flags': ['T'], 'set_flag': 'DF'},
    {'target': 'DBTCDT_Avg', 'sources': ['TCDT_Avg'], 'trigger_flags': ['SU'], 'set_flag': 'SU'},
    
    # Net Radiometer
    {'target': 'SWnet_Avg', 'sources': ['SWin_Avg', 'SWout_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SWnet_Avg', 'sources': ['Swing_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'SWout_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'LWnet_Avg', 'sources': ['LWin_Avg', 'LWout_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SWalbedo_Avg', 'sources': ['SWin_Avg', 'SWout_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'SWalbedo_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'NR_Avg', 'sources': ['SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'NR_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},

]


Add_caution_flag = [
    'BattV_Avg', 'PTemp_C_Avg', 'SlrFD_W_Avg', 'Dist_km_Avg', 'WS_ms_Avg', 
    'MaxWS_ms_Avg', 'AirT_C_Avg', 'VP_hPa_Avg', 'BP_hPa_Avg', 'RHT_C_Avg', 
    'TiltNS_deg_Avg', 'TiltWE_deg_Avg', 'Invalid_Wind_Avg', 'DT_Avg', 
    'TCDT_Avg', 'DBTCDT_Avg', 'SWin_Avg', 'SWout_Avg', 'LWin_Avg', 
    'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 'SWalbedo_Avg', 'NR_Avg', 
    'stmp_Avg', 'gtmp_Avg'
]

SOLAR_COLUMNS = [
    'SlrFD_W_Avg', 'SWin_Avg'
]

def load_data(filepath):
    print(f"Reading {filepath}...")
    
    # Read Header and Units separately to preserve them
    header_df = pd.read_csv(filepath, header=None, nrows=2)
    headers = header_df.iloc[0].tolist()
    units = header_df.iloc[1].tolist()
    df = pd.read_csv(filepath, header=0, skiprows=[1], 
                     na_values=['NAN', '"NAN"', ''], keep_default_na=True, 
                     skipinitialspace=True, low_memory=False)
    return df, headers, units

def apply_uniquecases(df):
    # Checks the record column, and sees logger reset.
    found_col = "RECORD"
    if found_col:
        col = found_col
        vals = pd.to_numeric(df[col], errors='coerce')
        
        # get your previous value
        prev_vals = vals.shift(1)
        # see if your first value is "NaN" or if its missing.
        is_start = vals.shift(1).isna()
        # Mask where current < previous (Restart) or if the first value is 0.
        mask_restart = (vals < prev_vals) | (is_start & (vals == 0))
        if mask_restart.any():
            flag_col = f"{col}_Flag"
            if flag_col not in df.columns:
                df[flag_col] = ""

            print(f"  - {col}: Flagging {mask_restart.sum()} records as 'LR' (Sequence Drop)")

            # Get current flags
            current_flags = df[flag_col].fillna("").astype(str)
            # Get the rows that need to be flagged
            targets = current_flags.loc[mask_restart]
            # Add LR flag, if the flag is empty, add LR, if it has something, add , LR
            new_flags = np.where(targets == "", "LR", targets + ", LR")
            # Update the flags
            df.loc[mask_restart, flag_col] = new_flags

    return df

def apply_thresholds(df):
    print("Applying QC Thresholds...")
    
    for col, (min_v, max_v) in THRESHOLDS.items():
        if col not in df.columns:
            print(f"Warning: Column {col} not found in dataset.")
            continue
            
        flag_col = f"{col}_Flag"
        # Create numeric series for comparison, handling non-numerics if any
        vals = pd.to_numeric(df[col], errors='coerce')
        
        # Prepare Limit Values (Handle Dynamic Column References)
        if isinstance(min_v, str) and min_v in df.columns:
            limit_min = pd.to_numeric(df[min_v], errors='coerce')
        else:
            limit_min = min_v

        if isinstance(max_v, str) and max_v in df.columns:
            limit_max = pd.to_numeric(df[max_v], errors='coerce')
        else:
            limit_max = max_v
        
        # Identify Violations
        mask_fail = (vals < limit_min) | (vals > limit_max)
        
        # Check Existing Flags for 'M'
        # ensure string
        current_flags = df[flag_col].fillna("").astype(str)
        mask_has_m = current_flags.str.contains('M')
        
        # Final Mask: Failed Threshold AND Not Missing
        mask_apply = mask_fail & (~mask_has_m)
        
        # Apply 'T'
        if mask_apply.any():
            # Count the number of records to be flagged, only for print statement
            count = mask_apply.sum()
            print(f"  - {col}: Flagging {count} records outside [{min_v}, {max_v}]")
            # Get the rows that need to be flagged
            targets = current_flags.loc[mask_apply]
            # Add T flag, if the flag is empty, add T, if it has something, add , T
            new_flags = np.where(targets == "", "T", targets + ", T")
            # Update the flags
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

def apply_dynamic_thresholds(df):
    print("Applying Dynamic Thresholds & Logic Flags...")
    
    # Ensure TIMESTAMP for seasonal checks
    has_date = 'TIMESTAMP' in df.columns
    if has_date:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    limit = SENSOR_HEIGHT - 50
    
    # T Check for DBTCDT (Snow Depth)
    if 'DBTCDT_Avg' in df.columns:
        vals = pd.to_numeric(df['DBTCDT_Avg'], errors='coerce')
        
        # > H-50 is T (Physical Limit)
        mask_fail = (vals > limit)
        if mask_fail.any():
            col = 'DBTCDT_Avg'
            flag_col = f"{col}_Flag"
            print(f"  - {col}: Flagging {mask_fail.sum()} records > {limit} (H-50)")
            current_flags = df.loc[mask_fail, flag_col].fillna("").astype(str)
            new_flags = np.where(current_flags == "", "T", current_flags + ", T")
            df.loc[mask_fail, flag_col] = new_flags
     
        # Check to make sure that is betwen two dates than in a month
        if has_date:
            months = df['TIMESTAMP'].dt.month
            mask_summer = months.isin([6, 7, 8, 9])
            mask_snow = (vals > 0)
            mask_sf = mask_summer & mask_snow
            
            if mask_sf.any():
                col = 'DBTCDT_Avg'
                flag_col = f"{col}_Flag"
                print(f"  - {col}: Flagging {mask_sf.sum()} records with 'SF' (Summer Snow)")
                current_flags = df.loc[mask_sf, flag_col].fillna("").astype(str)
                new_flags = np.where(current_flags == "", "SF", current_flags + ", SF")
                df.loc[mask_sf, flag_col] = new_flags

    # 3. NV Flag: Wind Speed == 0 -> No Direction
    if 'WS_ms_Avg' in df.columns and 'WindDir' in df.columns:
        ws = pd.to_numeric(df['WS_ms_Avg'], errors='coerce').fillna(0)
        mask_calm = (ws == 0)
        
        if mask_calm.any():
            col = 'WindDir'
            flag_col = f"{col}_Flag"
            if flag_col not in df.columns: df[flag_col] = ""
            print(f"  - {col}: Flagging {mask_calm.sum()} records with 'NW' (No Wind)")
            current_flags = df.loc[mask_calm, flag_col].fillna("").astype(str)
            new_flags = np.where(current_flags == "", "NW", current_flags + ", NW")
            df.loc[mask_calm, flag_col] = new_flags

    # 5. SU Flag: Albedo (0.1 < Albedo < 0.95 is normal, outside is SU)
    # Thresholds T is < 0 OR > 1.
    # SU is < 0.1 OR > 0.95.
    if 'SWalbedo_Avg' in df.columns:
        alb = pd.to_numeric(df['SWalbedo_Avg'], errors='coerce')
        mask_su = (alb < 0.1) | (alb > 0.95)
        
        if mask_su.any():
            col = 'SWalbedo_Avg'
            flag_col = f"{col}_Flag"
            if flag_col not in df.columns: df[flag_col] = ""
            
            print(f"  - {col}: Flagging {mask_su.sum()} records with 'SU' (Extreme Albedo)")
            current_flags = df.loc[mask_su, flag_col].fillna("").astype(str)
            new_flags = np.where(current_flags == "", "SU", current_flags + ", SU")
            df.loc[mask_su, flag_col] = new_flags

    return df

def apply_critical_flags(df):
    print("Checking Critical Flags (PTemp)...")
    if 'PTemp_C_Avg' not in df.columns:
        return df
        
    # Check if PTemp has 'T' flag
    ptemp_flag_col = 'PTemp_C_Avg_Flag'
    if ptemp_flag_col not in df.columns:
        return df
        
    current_ptemp_flags = df[ptemp_flag_col].fillna("").astype(str)
    # Mask of rows where PTemp is T. (Using exact match for safety)
    mask_critical = current_ptemp_flags.str.contains(r'\bT\b', regex=True)
    
    if mask_critical.any():
        print(f"CRITICAL: Found {mask_critical.sum()} records with PTemp Failure. Flagging ALL columns with ERR.")
        
        # Apply ERR to ALL other parameters defined in THRESHOLDS
        for col in THRESHOLDS.keys():
            if col == 'PTemp_C_Avg': continue
            if col not in df.columns: continue
            
            flag_col = f"{col}_Flag"
            if flag_col not in df.columns: df[flag_col] = ""
            
            current_flags = df.loc[mask_critical, flag_col].fillna("").astype(str)
            new_flags = np.where(current_flags == "", "ERR", current_flags + ", ERR")
            df.loc[mask_critical, flag_col] = new_flags
            
    return df

def apply_dependencies(df):
    print("Applying Dependency Flags...")
    
    for config in DEPENDENCY_CONFIG:
        target_col = config['target']
        source_cols = config['sources']
        trigger_flags = config['trigger_flags'] 
        set_flag = config['set_flag']
        
        if target_col not in df.columns:
            continue
            
        target_flag_col = f"{target_col}_Flag"
        if target_flag_col not in df.columns:
             df[target_flag_col] = ""

        # Check sources
        dependency_fail_mask = pd.Series(False, index=df.index)
        
        for src in source_cols:
            if src not in df.columns:
                continue
            
            src_flag_col = f"{src}_Flag"
            if src_flag_col not in df.columns:
                continue
                
            current_src_flags = df[src_flag_col].fillna("").astype(str)
            
            # Construct regex for trigger flags
            pattern = "|".join([rf"\b{f}\b" for f in trigger_flags])
            
            has_error = current_src_flags.str.contains(pattern, regex=True)
            dependency_fail_mask = dependency_fail_mask | has_error
            
        if dependency_fail_mask.any():
            count = dependency_fail_mask.sum()
            # print(f"  - {target_col}: Flagging {count} records with {set_flag} (Dep)")
            
            current_flags = df.loc[dependency_fail_mask, target_flag_col].fillna("").astype(str)
            new_flags = np.where(current_flags == "", set_flag, current_flags + ", " + set_flag)
            df.loc[dependency_fail_mask, target_flag_col] = new_flags
            
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
        # Search for correct sunrise/sunset for this local date
        rise_naive = None
        set_naive = None
        
        # Check current day and next day to capture events that cross UTC midnight
        candidates = [datetime(d.year, d.month, d.day), datetime(d.year, d.month, d.day) + timedelta(days=1)]
        
        for cand in candidates:
            try:
                # Check Sunrise
                r_utc = sun.get_sunrise_time(cand)
                r_pdt = r_utc.astimezone(tz_pdt)
                if r_pdt.date() == d:
                    rise_naive = r_pdt.replace(tzinfo=None)
                
                # Check Sunset
                s_utc = sun.get_sunset_time(cand)
                s_pdt = s_utc.astimezone(tz_pdt)
                if s_pdt.date() == d:
                    set_naive = s_pdt.replace(tzinfo=None)
            except Exception as e:
                # Polar night/day or calculation error
                continue
        
        # Mask for this date
        mask_date = (temp_dates == d)
        
        # Get the timestamps for this date
        ts_values = df.loc[mask_date, 'TIMESTAMP']
        
        # Verify we found both times
        if rise_naive is None or set_naive is None:
            # print(f"Warning: Could not determine sunrise/sunset for {d} (Polar Day/Night?)")
            continue

        # Night Mask: Time < Rise OR Time > Set
        # User requested 15 minute padding from both ends (Z flag starts too early/ends too late).
        # This expands the "Day" window by 15 mins on each side.
        padding = timedelta(minutes=15)
        mask_night_time = (ts_values < (rise_naive - padding)) | (ts_values > (set_naive + padding))
        
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
    
    # 2. Apply Thresholds (Static Min/Max)
    df = apply_thresholds(df)
    
    # 3. Apply Dynamic Thresholds & Nighttime
    df = apply_dynamic_thresholds(df)
    df = apply_nighttime_flags(df)
    
    # 4. Critical Flags (PTemp Failure)
    df = apply_critical_flags(df)
    
    # 5. Apply Unique Cases & Legacy
    df = apply_uniquecases(df)
    df = apply_legacy_flags(df)

    # 6. Apply Dependencies (Checks T, ERR, Z set above)
    df = apply_dependencies(df)
    
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
