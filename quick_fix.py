import pandas as pd
import os

def isolate_columns(file_path):
    """
    Ingests a CSV file (assuming TOA5 tidy format) and isolates
    TIMESTAMP, RECORD, AirTC, and AirTC_Flag columns.
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return None
        
        print(f"Reading file: {file_path}")
        df = pd.read_csv(file_path, skiprows=[0, 2, 3])
        
        # Define target columns
        target_cols = ['TIMESTAMP', 'RECORD', 'tmp', 'tmp_Flag']
        
        # Verify columns exist
        missing = [c for c in target_cols if c not in df.columns]
        if missing:
            print(f"Warning: The following columns were not found: {missing}")
            available_cols = [c for c in target_cols if c in df.columns]
            df_subset = df[available_cols].copy()
        else:
            df_subset = df[target_cols].copy()
            
        # --- NEW LOGIC: Calculate Max/Min for 'tmp' ---
        if 'tmp' in df_subset.columns:
            # Convert to numeric just in case
            df_subset['tmp'] = pd.to_numeric(df_subset['tmp'], errors='coerce')
            
            # --- Flaggging Logic ---
            if 'tmp_Flag' not in df_subset.columns:
                 df_subset['tmp_Flag'] = ''
            
            # 1. Missing/NaN Check ('M')
            # If tmp is NaN (non-numeric or missing), set flag to 'M'
            df_subset.loc[df_subset['tmp'].isna(), 'tmp_Flag'] = 'M'
            
            # 2. No Change Check ('NC')
            # Flag if 4 or more consecutive rows have the EXACT same numeric value
            # Calculate groups of consecutive identical values
            # (NaNs != NaNs, so they break groups naturally, which is what we want)
            # We filter out NaNs from the NC check to be safe (already flagged as M)
            
            # Create a group ID for consecutive values
            # (value != prev_value) -> True for new group. cumsum() makes unique IDs
            grp = (df_subset['tmp'] != df_subset['tmp'].shift()).cumsum()
            
            # Calculate size of each group
            grp_counts = df_subset.groupby(grp)['tmp'].transform('count')
            
            # Mask: count >= 4 AND value is not NaN
            # Note: We group on 'tmp', so NaNs are their own groups of size 1 usually, 
            # but explicit notna() check ensures we don't flag 4 consecutive NaNs as "No Change" (they are "M")
            mask_nc = (grp_counts >= 4) & (df_subset['tmp'].notna())
            
            # Apply 'NC' flag
            # Careful not to overwrite 'M' if there's overlap (there shouldn't be due to notna check)
            # Use 'NC' for these rows. 
            df_subset.loc[mask_nc, 'tmp_Flag'] = 'NC'
         
            
            max_val = df_subset['tmp'].max()
            min_val = df_subset['tmp'].min()

            # --- Step Change / Neighbor Logic ---
            # Calculate difference with next row (i vs i+1)
            # diff_next = current - next
            df_subset['diff_next'] = df_subset['tmp'] - df_subset['tmp'].shift(-1)
            
            # Calculate difference with prev row (i vs i-1)
            # diff_prev = current - prev
            df_subset['diff_prev'] = df_subset['tmp'] - df_subset['tmp'].shift(1)
            
            # Store neighbor values as requested
            df_subset['val_next'] = df_subset['tmp'].shift(-1)
            df_subset['val_prev'] = df_subset['tmp'].shift(1)
            
            # --- New Logic: Neighbor Differences & Percentiles ---
            # "val_next_val_prev" = val_next - val_prev
            df_subset['val_next_val_prev'] = df_subset['val_next'] - df_subset['val_prev']
                    
            # Calculate 99.5th and 0.5th percentiles of the difference column
            # (ignoring NaNs automatically by pandas)
            p99_5 = df_subset['val_next_val_prev'].quantile(0.995)
            p00_5 = df_subset['val_next_val_prev'].quantile(0.005)

            p99_5_tmp = df_subset['tmp'].quantile(0.995)
            p00_5_tmp = df_subset['tmp'].quantile(0.005)

            p99_5_diff_next = df_subset['diff_next'].quantile(0.995)
            p00_5_diff_next = df_subset['diff_next'].quantile(0.005)

            # 3. Extreme Value Check ('E')
            # Flag if tmp is outside the 0.5th - 99.5th percentile range
            # We use the previously calculated global percentiles
            mask_extreme = (df_subset['tmp'] > p99_5_tmp) | (df_subset['tmp'] < p00_5_tmp)
            df_subset.loc[mask_extreme, 'tmp_Flag'] = 'E'
            
            print(f"99.5th Percentile of diff: {p99_5}")
            print(f"0.5th Percentile of diff: {p00_5}")
            
            # Store these scalar values in new repeating columns
            df_subset['99.5'] = p99_5
            df_subset['0.5'] = p00_5

            df_subset['99.5_diff_next'] = p99_5_diff_next
            df_subset['0.5_diff_next'] = p00_5_diff_next

            df_subset['99.5_tmp'] = p99_5_tmp
            df_subset['0.5_tmp'] = p00_5_tmp

            # Also keep simple max/min columns as originally planned
            df_subset['tmp_Max'] = max_val
            df_subset['tmp_Min'] = min_val


            #add difference between val next and val_prv
            df_subset['val_next_val_prev'] = df_subset['val_next'] - df_subset['val_prev']
            
            print(f"Global Max tmp: {max_val}")
            print(f"Global Min tmp: {min_val}")
            

            
        print("\nIsolated DataFrame Head:")
        print(df_subset.head())
        
        return df_subset

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    # Path to the file mentioned by the user
    file_path = "data/Cheslata Lake_tidy (11).csv"
    
    df = isolate_columns(file_path)
    
    if df is not None:
        output_file = "quick_fix_output.csv"
        df.to_csv(output_file, index=False, na_rep='NaN')
        print(f"\n✅ Success! Isolated data saved to: {os.path.abspath(output_file)}")
        print("Ready for flag setting logic...")
