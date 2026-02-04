import pandas as pd
import numpy as np
import os
import csv
from datetime import datetime

# Input/Output
INPUT_FILE = 'data/concatenated_one_year_phase2.csv'
OUTPUT_FILE = 'data/concatenated_one_year_phase3.csv'

# Configuration Dictionary
# max_change (Tau) and no_change (NC steps)
# Invalid or No limit is None
QAQC_CONFIG = [
    # Logger
    {'column': 'BattV_Avg', 'max_change': 1, 'no_change': None},
    {'column': 'PTemp_C_Avg', 'max_change': 5, 'no_change': 4},
    # ClimaVue50
    {'column': 'RHT_C_Avg', 'max_change': 5, 'no_change': 4}, 
    {'column': 'SlrFD_W_Avg', 'max_change': None, 'no_change': None},
    {'column': 'Rain_mm_Tot', 'max_change': 20, 'no_change': None}, 
    {'column': 'Strikes_tot', 'max_change': None, 'no_change': None},
    {'column': 'Dist_km_Avg', 'max_change': None, 'no_change': None},
    {'column': 'WS_ms_Avg', 'max_change': None, 'no_change': 10},
    {'column': 'WindDir', 'max_change': None, 'no_change': 10},
    {'column': 'AirT_C_Avg', 'max_change': 5, 'no_change': 4},
    {'column': 'VP_hPa_Avg', 'max_change': 1, 'no_change': 10},
    {'column': 'RH', 'max_change': 20, 'no_change': 10},
    {'column': 'BP_hPa_Avg', 'max_change': 10, 'no_change': 10},
    {'column': 'TiltNS_deg_Avg', 'max_change': 1, 'no_change': None},
    {'column': 'TiltWE_deg_Avg', 'max_change': 1, 'no_change': None},
    {'column': 'SlrTF_MJ_Tot', 'max_change': None, 'no_change': None},
    # SR50
    {'column': 'DT_Avg', 'max_change': None, 'no_change': None},
    {'column': 'TCDT_Avg', 'max_change': None, 'no_change': None},
    {'column': 'DBTCDT_Avg', 'max_change': 10, 'no_change': None},
    # Net Radiometer
    {'column': 'SWin_Avg', 'max_change': None, 'no_change': None},
    {'column': 'SWout_Avg', 'max_change': None, 'no_change': None},
    {'column': 'LWin_Avg', 'max_change': None, 'no_change': None},
    {'column': 'LWout_Avg', 'max_change': None, 'no_change': None},
    {'column': 'SWnet_Avg', 'max_change': None, 'no_change': None},
    {'column': 'LWnet_Avg', 'max_change': None, 'no_change': None},
    {'column': 'SWalbedo_Avg', 'max_change': None, 'no_change': None},
    {'column': 'NR_Avg', 'max_change': None, 'no_change': None},
    # Ground Thermistors
    {'column': 'stmp_Avg', 'max_change': 5, 'no_change': 4},
    {'column': 'gtmp_Avg', 'max_change': 5, 'no_change': 4},
]

def load_data(filepath):
    print(f"Reading {filepath}...")
    # Read Header and Units separately
    header_df = pd.read_csv(filepath, header=None, nrows=2)
    headers = header_df.iloc[0].tolist()
    units = header_df.iloc[1].tolist()
    # Read data
    df = pd.read_csv(filepath, header=0, skiprows=[1], 
                     na_values=['NAN', '"NAN"', ''], keep_default_na=True, 
                     skipinitialspace=True, low_memory=False)
    return df, headers, units

def append_flag(df, mask, flag_col, flag_char):
    """
    Appends flag_char to flag_col where mask is True.
    """
    if not mask.any():
        return df
    
    # Ensure flag col exists
    if flag_col not in df.columns:
        df[flag_col] = ""
        
    current_flags = df.loc[mask, flag_col].fillna("").astype(str)
    
    # Append flag
    new_flags = np.where(current_flags == "", flag_char, current_flags + ", " + flag_char)
    df.loc[mask, flag_col] = new_flags
    return df

def apply_change_checks(df):
    print("Applying Phase 3 QA/QC (Jumps, Spikes, No Change)...")
    
    # Ensure TIMESTAMP sorted
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df.sort_values('TIMESTAMP', inplace=True)
    
    for config in QAQC_CONFIG:
        col = config['column']
        max_change = config['max_change'] # Tau
        no_change_limit = config['no_change'] # NC Steps

        if col not in df.columns:
            continue
            
        flag_col = f"{col}_Flag"
        if flag_col not in df.columns:
            df[flag_col] = ""
        
        # Calculate diffs if max_change is set or analysis needed
        vals = pd.to_numeric(df[col], errors='coerce')
        
        # diff_prev: Value - PrevValue (x_t - x_{t-1})
        diff_prev = vals - vals.shift(1)
        # diff_next: Value - NextValue (x_t - x_{t+1})
        diff_next = vals - vals.shift(-1)
        
        # 1. No Change (NC)
        if no_change_limit is not None:
            # Group by value change
            grp = (vals != vals.shift()).cumsum()
            grp_counts = df.groupby(grp)[col].transform('count')
            
            mask_nc = (grp_counts >= no_change_limit) & (vals.notna())
            
            if mask_nc.any():
                print(f"  - {col}: Flagging {mask_nc.sum()} records as 'NC'")
                df = append_flag(df, mask_nc, flag_col, 'NC')
                
        # 2. Spikes (S) and Jumps (J)
        if max_change is not None:
            
            mask_s_pos = (diff_prev > max_change) & (diff_next > max_change)
            mask_s_neg = (diff_prev < -max_change) & (diff_next < -max_change)
            mask_s = mask_s_pos | mask_s_neg
            
            if mask_s.any():
                print(f"  - {col}: Flagging {mask_s.sum()} records as 'S'")
                df = append_flag(df, mask_s, flag_col, 'S')
                
            # Jump (J)
            # Condition: abs(diff_prev) > tau
            mask_j = (diff_prev.abs() > max_change)
            
            if mask_j.any():
                # Note: S implies J usually, but J tracks basic rate of change violation
                # print(f"  - {col}: Flagging {mask_j.sum()} records as 'J'")
                df = append_flag(df, mask_j, flag_col, 'J')

    return df

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    df, headers, units = load_data(INPUT_FILE)
    print(f"Loaded {len(df)} rows.")
    
    df = apply_change_checks(df)
    
    # Cleanup flags (NaN -> "")
    flag_cols = [c for c in df.columns if c.endswith("_Flag")]
    df[flag_cols] = df[flag_cols].fillna("")

    print(f"Saving to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        
        # Map units
        orig_map = dict(zip(headers, units))
        current_units = []
        for col in df.columns:
            u = orig_map.get(col, "")
            current_units.append(u)
        writer.writerow(current_units)
        
    df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False, na_rep='NaN')
    print("Done.")

if __name__ == '__main__':
    main()