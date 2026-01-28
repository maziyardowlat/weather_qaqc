import pandas as pd
import numpy as np

def process_weather_column(df, target_col, min_val, max_val, rate_of_change):
    """
    Applies QA/QC flags to a specific column in the dataframe.
    
    Args:
        df (pd.DataFrame): The weather data dataframe (must contain 'TIMESTAMP' and target_col).
        target_col (str): The name of the column to process.
        min_val (float): Minimum valid value.
        max_val (float): Maximum valid value.
        rate_of_change (float): Maximum allowed change between consecutive records.
        
    Returns:
        pd.DataFrame: A new dataframe containing TIMESTAMP, target_col, Flag, and Limit columns.
    """
    # Work on a copy to avoid SettingWithCopy warnings on the original df
    # We only need specific columns for processing, but we keep TIMESTAMP for sorting/indexing
    if target_col not in df.columns:
        print(f"Error: {target_col} not found in columns.")
        return pd.DataFrame()

    # Create a working copy with just the necessary columns to ensure cleanliness
    # or just copy the whole thing if we want to return it all. 
    # The prompt implies we want to output a file specific to this column usually, 
    # but for a general function, let's keep the logic focused on the column.
    
    df_proc = df[['TIMESTAMP', target_col]].copy()
    
    # Ensure sorted by time
    df_proc.sort_values('TIMESTAMP', inplace=True)
    
    # Calculate Differences
    # diff_prev: Value - PrevValue
    df_proc['diff_prev'] = df_proc[target_col] - df_proc[target_col].shift(1)
    # diff_next: Value - NextValue
    df_proc['diff_next'] = df_proc[target_col] - df_proc[target_col].shift(-1)
    
    # Initialize Flag Column
    df_proc['Flag'] = ''
    
    # 1. Missing (M)
    mask_m = df_proc[target_col].isna()
    df_proc.loc[mask_m, 'Flag'] = 'M'
    
    # 2. Temperature/Range Check (T): < min or > max
    # Note: Using 'T' as requested to match original logic, even if generic.
    
    mask_t = (df_proc[target_col] < min_val) | (df_proc[target_col] > max_val)
    
    # Helper to append flags
    def append_flag(mask, flag_char):
        new_mask = mask & (df_proc['Flag'] == '')
        existing_mask = mask & (df_proc['Flag'] != '') & (df_proc['Flag'] != 'M')
        
        df_proc.loc[new_mask, 'Flag'] = flag_char
        df_proc.loc[existing_mask, 'Flag'] += ', ' + flag_char

    append_flag(mask_t, 'T')

    # 3. No Change Check (NC)
    # Flag if 4 or more consecutive rows have the EXACT same numeric value
    grp = (df_proc[target_col] != df_proc[target_col].shift()).cumsum()
    grp_counts = df_proc.groupby(grp)[target_col].transform('count')
    mask_nc = (grp_counts >= 4) & (df_proc[target_col].notna())
    append_flag(mask_nc, 'NC')

    # 4. Spike Check (S)
    # Original simplifed logic:
    # Abs diff > rate_of_change from BOTH prev and next
    # Note: original code calculated diff_next/prev as actual differences (signed).
    # mask_s = (df['diff_next'] > rate_of_change) & (df['diff_prev'] > rate_of_change)
    # This specifically targets upward spikes. 
    # To keep it EXACTLY the same as requested:
    
    mask_s = (df_proc['diff_next'] > rate_of_change) & (df_proc['diff_prev'] > rate_of_change)
    append_flag(mask_s, 'S')
    
    # 5. Jump Check (J)
    # Abs diff from Prev > rate
    mask_j = df_proc['diff_prev'].abs() > rate_of_change
    append_flag(mask_j, 'J')
    
    # 6. Pass (P)
    mask_p = (df_proc['Flag'] == '') & (df_proc[target_col].notna())
    df_proc.loc[mask_p, 'Flag'] = 'P'
    
    # Add Limit Columns for reference
    df_proc['Min_Limit'] = min_val
    df_proc['Max_Limit'] = max_val
    df_proc['Rate_Limit'] = rate_of_change
    
    return df_proc[['TIMESTAMP', target_col, 'Flag', 'Min_Limit', 'Max_Limit', 'Rate_Limit']]

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Process weather data column with QA/QC.")
    parser.add_argument("--col", required=True, help="Column name to process (e.g., RH, BattV_Avg)")
    parser.add_argument("--min", type=float, required=True, help="Minimum valid value")
    parser.add_argument("--max", type=float, required=True, help="Maximum valid value")
    parser.add_argument("--rate", type=float, required=True, help="Maximum rate of change")
    parser.add_argument("--input", default="data/concatenated_all_years.csv", help="Input CSV file")
    parser.add_argument("--output", help="Output file path (optional)")

    args = parser.parse_args()

    input_csv = args.input
    if not os.path.exists(input_csv):
        print(f"Error: Input file not found: {input_csv}")
        exit(1)

    # Determine output filename if not provided
    if args.output:
        output_csv = args.output
    else:
        # Default: data/processed_{col}.csv
        base_dir = os.path.dirname(input_csv)
        output_csv = os.path.join(base_dir, f"processed_{args.col}.csv")

    print(f"Reading {input_csv}...")
    # Load data (using settings from process_air_temp.py)
    df = pd.read_csv(input_csv, header=0, skiprows=[1], na_values=['NAN', '"NAN"'], low_memory=False)
    
    # Ensure TIMESTAMP is datetime
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='mixed')
    
    # Filter Data (using consistent end date from process_air_temp.py)
    # If filter logic varies, we might want to make this an argument too.
    end_date = '2024-07-04 14:00:00'
    print(f"Filtering data up to {end_date}...")
    df = df[df['TIMESTAMP'] <= end_date].copy()

    print(f"Processing {args.col} with Min={args.min}, Max={args.max}, Rate={args.rate}...")
    
    df_out = process_weather_column(df, args.col, args.min, args.max, args.rate)
    
    if df_out.empty:
        print("Processing failed (column not found?).")
    else:
        print(f"Saving {len(df_out)} rows to {output_csv}...")
        print("Tail of output:")
        print(df_out.tail())
        df_out.to_csv(output_csv, index=False, na_rep='NaN')
        print("Done.")
