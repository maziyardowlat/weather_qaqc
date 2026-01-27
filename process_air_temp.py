import pandas as pd
import numpy as np

def process_air_temp(input_file, output_file):
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file, header=0, skiprows=[1], na_values=['NAN', '"NAN"'], low_memory=False)

    #input in our max/min/rate of change values
    rate_of_change = 5
    max_temp = 50
    min_temp = -50
    
    # Ensure TIMESTAMP is datetime
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='mixed')
    
    # Filter Data: Start to 2024-07-04 14:00:00 (Inclusive)
    end_date = '2024-07-04 14:00:00'
    print(f"Filtering data up to {end_date}...")
    df = df[df['TIMESTAMP'] <= end_date].copy()
    
    # Rename Column
    target_col = 'AirT_C_Avg'
    
    if target_col not in df.columns:
        print(f"Error: {target_col} not found in columns: {df.columns}")
        return    
    # Sort
    df.sort_values('TIMESTAMP', inplace=True)
    
    # Calculate Differences for Checks
    # diff_prev: Value - PrevValue
    df['diff_prev'] = df[target_col] - df[target_col].shift(1)
    # diff_next: Value - NextValue
    df['diff_next'] = df[target_col] - df[target_col].shift(-1)
    
    # Initialize Flag Column
    df['Flag'] = ''
    
    # 1. Missing (M)
    mask_m = df[target_col].isna()
    df.loc[mask_m, 'Flag'] = 'M'
    
    # 2. Temperature Range (T): < -50 or > 50
    # Only check non-missing
    mask_t = (df[target_col] < min_temp) | (df[target_col] > max_temp)
    
    # Helper to append flags
    def append_flag(mask, flag_char):
        # Calculate masks based on current state BEFORE modification
        new_mask = mask & (df['Flag'] == '')
        existing_mask = mask & (df['Flag'] != '') & (df['Flag'] != 'M')
        
        # Apply edits
        df.loc[new_mask, 'Flag'] = flag_char
        df.loc[existing_mask, 'Flag'] += ', ' + flag_char

    append_flag(mask_t, 'T')

    # 3. No Change Check (NC)
    # Flag if 4 or more consecutive rows have the EXACT same numeric value
    # Filter out NaNs so we don't flag missing sequences as 'No Change'
    
    # Create group ID: increments every time value changes
    grp = (df[target_col] != df[target_col].shift()).cumsum()
    
    # Count size of each group
    grp_counts = df.groupby(grp)[target_col].transform('count')
    
    # Mask: count >= 4 AND value is not NaN
    mask_nc = (grp_counts >= 4) & (df[target_col].notna())
    
    append_flag(mask_nc, 'NC')


    # 3. Spike Check (S)
    # Abs diff > 5 from BOTH prev and next, AND same sign (consistent spike/dip)
    # (val - prev) * (val - next) > 0  implies same sign.

    mask_s = (df['diff_next'] > rate_of_change) & (df['diff_prev'] > rate_of_change)
    append_flag(mask_s, 'S')
    
    # 4. Jump Check (J)
    # Abs diff from Prev > 5
    # Note: S condition implies J condition (large diff from prev).
    # So S implies J usually.

    mask_j = df['diff_prev'].abs() > rate_of_change
    append_flag(mask_j, 'J')
    
    # 5. Pass (P)
    # If Flag is still empty, valid value -> P
    mask_p = (df['Flag'] == '') & (df[target_col].notna())
    df.loc[mask_p, 'Flag'] = 'P'
    
    # Add Limit Columns
    df['Min_Limit'] = min_temp
    df['Max_Limit'] = max_temp
    df['Rate_Limit'] = rate_of_change

    
    # Select Columns
    final_cols = ['TIMESTAMP', target_col, 'Flag', 'Min_Limit', 'Max_Limit', 'Rate_Limit']
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
