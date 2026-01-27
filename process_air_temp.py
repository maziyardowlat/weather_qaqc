import pandas as pd
import numpy as np

def process_air_temp(input_file, output_file):
    print(f"Reading {input_file}...")
    
    # Read data, skipping Units row (row 2, index 1 if 0-based, or row 3 if 1-based)
    # TOA5: 0=Info, 1=Header, 2=Units, 3=Rate. Data starts at 4.
    # We want headers from row 1.
    # We want to skip row 2 and 3.
    # Concatenated file has:
    # Row 0: Header
    # Row 1: Units
    # Row 2+: Data
    df = pd.read_csv(input_file, header=0, skiprows=[1], na_values=['NAN', '"NAN"'], low_memory=False)
    
    # Ensure TIMESTAMP is datetime
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='mixed')
    
    # Filter Data: Start to 2024-07-04 14:00:00 (Inclusive)
    end_date = '2024-07-04 14:00:00'
    print(f"Filtering data up to {end_date}...")
    df = df[df['TIMESTAMP'] <= end_date].copy()
    
    # Rename Column
    target_col = 'AirT_C_Avg'
    output_col = 'AirTC_C_Avg'
    
    if target_col not in df.columns:
        print(f"Error: {target_col} not found in columns: {df.columns}")
        return
        
    df.rename(columns={target_col: output_col}, inplace=True)
    
    # Sort
    df.sort_values('TIMESTAMP', inplace=True)
    
    # Calculate Differences for Checks
    # diff_prev: Value - PrevValue
    df['diff_prev'] = df[output_col].diff()
    # diff_next: Value - NextValue
    df['diff_next'] = df[output_col] - df[output_col].shift(-1)
    
    # Initialize Flag Column
    df['Flag'] = ''
    
    # --- QA/QC Logic ---
    
    # 1. Missing (M)
    mask_m = df[output_col].isna()
    df.loc[mask_m, 'Flag'] = 'M'
    
    # 2. Temperature Range (T): < -50 or > 50
    # Only check non-missing
    mask_t = (df[output_col] < -50) | (df[output_col] > 50)
    
    # Helper to append flags
    def append_flag(mask, flag_char):
        # Calculate masks based on current state BEFORE modification
        new_mask = mask & (df['Flag'] == '')
        existing_mask = mask & (df['Flag'] != '') & (df['Flag'] != 'M')
        
        # Apply edits
        df.loc[new_mask, 'Flag'] = flag_char
        df.loc[existing_mask, 'Flag'] += ', ' + flag_char

    append_flag(mask_t, 'T')

    # 3. Spike Check (S)
    # Abs diff > 5 from BOTH prev and next, AND same sign (consistent spike/dip)
    # (val - prev) * (val - next) > 0  implies same sign.
    mask_s = (df['diff_prev'].abs() > 5) & \
             (df['diff_next'].abs() > 5) & \
             ((df['diff_prev'] * df['diff_next']) > 0)
             
    append_flag(mask_s, 'S')
    
    # 4. Jump Check (J)
    # Abs diff from Prev > 5
    # Note: S condition implies J condition (large diff from prev).
    # So S implies J usually.
    mask_j = df['diff_prev'].abs() > 5
    append_flag(mask_j, 'J')
    
    # 5. Pass (P)
    # If Flag is still empty, valid value -> P
    mask_p = (df['Flag'] == '') & (df[output_col].notna())
    df.loc[mask_p, 'Flag'] = 'P'
    
    # Add Limit Columns
    df['Min_Limit'] = -50
    df['Max_Limit'] = 50
    df['Rate_Limit'] = 5
    
    # Select Columns
    final_cols = ['TIMESTAMP', output_col, 'Flag', 'Min_Limit', 'Max_Limit', 'Rate_Limit']
    df_out = df[final_cols]
    
    print(f"Saving {len(df_out)} rows to {output_file}...")
    print("Tail of output:")
    print(df_out.tail())
    
    df_out.to_csv(output_file, index=False, na_rep='NaN')
    print("Done.")

if __name__ == "__main__":
    input_csv = "data/concatenated_all_years.csv"
    output_csv = "data/processed_air_temp.csv"
    process_air_temp(input_csv, output_csv)
