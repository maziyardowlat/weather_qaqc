import pandas as pd
import numpy as np
import os
import csv
import warnings

# Suppress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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
    'TiltNS_deg_Avg': (-5, 5),
    'TiltWE_deg_Avg': (-5, 5),
    'DBTCDT_Avg': (-5, 250), # Snow Depth
    'DT_Avg': (50, 1000),    # Distance to target
    'stmp_Avg': (-50, 50),   # Soil Temp
    'gtmp_Avg': (-50, 50),
    'BattV_Avg': (10, 16),
    'VP_mbar_Avg': (0, 80)
}

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

def apply_inheritance(df):
    print("Applying Flag Inheritance...")
    
    # helper for inheritance
    def inherit_flag(target_col, source_cols, flag_char='T'):
        # Target Flag Column
        t_flag_col = f"{target_col}_Flag"
        if t_flag_col not in df.columns:
            df[t_flag_col] = ""
            
        target_flags = df[t_flag_col].fillna("").astype(str)
        
        # Check if ANY source column has the flag 'T'
        # We need to look at source FLAG columns
        mask_source_has_flag = pd.Series(False, index=df.index)
        
        for sc in source_cols:
            s_flag_col = f"{sc}_Flag"
            if s_flag_col in df.columns:
                s_flags = df[s_flag_col].fillna("").astype(str)
                mask_source_has_flag |= s_flags.str.contains(flag_char)
        
        mask_target_clean = ~target_flags.str.contains('M')
        
        mask_apply = mask_source_has_flag & mask_target_clean & (~target_flags.str.contains(flag_char)) # Don't double add T
        
        if mask_apply.any():
            count = mask_apply.sum()
            print(f"  - {target_col}: Inheriting {flag_char} from sources for {count} records")
            
            targets = target_flags.loc[mask_apply]
            new_flags = np.where(targets == "", flag_char, targets + f", {flag_char}")
            df.loc[mask_apply, t_flag_col] = new_flags

    # 1. Albedo inherits from SWin, SWout
    inherit_flag('SWalbedo_Avg', ['SWin_Avg', 'SWout_Avg'], 'T')
    
    # 2. Net Rad inherits from SWin, SWout, LWin, LWout
    inherit_flag('NR_Avg', ['SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg'], 'T')
    # Use SWnet/LWnet? User said "see LW/SW". Usually NR depends on components.
    
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
    
    # 3. Apply Inheritance
    df = apply_inheritance(df)
    
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
