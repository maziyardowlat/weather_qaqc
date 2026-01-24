import pandas as pd
import matplotlib.pyplot as plt
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
            
            # Ensure tmp_Flag is string and no NaNs (critical for appending flags)
            df_subset['tmp_Flag'] = df_subset['tmp_Flag'].fillna('').astype(str)
            
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

            p99_5_diff_next = df_subset['diff_next'].quantile(0.995)
            # p00_5_diff_next = df_subset['diff_next'].quantile(0.005)

            #absolute 99.5
            p99_5_diff_next_abs = abs(df_subset['diff_next']).quantile(0.995)

            
            # Calculate difference with prev row (i vs i-1)
            # diff_prev = current - prev
            df_subset['diff_prev'] = df_subset['tmp'] - df_subset['tmp'].shift(1)

            p99_5_diff_prev = df_subset['diff_prev'].quantile(0.995)
            p00_5_diff_prev = df_subset['diff_prev'].quantile(0.005)
            
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
            mask_extreme = (df_subset['tmp'] > p99_5_tmp)
            
            # Calculate masks based on CURRENT state before modification
            mask_is_clean = (df_subset['tmp_Flag'] == '')
            mask_not_clean = (df_subset['tmp_Flag'] != '')
            
            # Apply 'E'
            # 1. New flags
            df_subset.loc[mask_extreme & mask_is_clean, 'tmp_Flag'] = 'E'
            
            # 2. Append to existing flags
            df_subset.loc[mask_extreme & mask_not_clean, 'tmp_Flag'] += ', E'
            
            # 4. Temperature Threshold Check ('T')
            temperature_threshold = (df_subset['tmp'] > 50) | (df_subset['tmp'] < -50)
            
            # Calculate masks based on CURRENT state (post-E updates)
            mask_is_clean = (df_subset['tmp_Flag'] == '')
            mask_not_clean = (df_subset['tmp_Flag'] != '')
            
            # Apply 'T'
            df_subset.loc[temperature_threshold & mask_is_clean, 'tmp_Flag'] = 'T'
            df_subset.loc[temperature_threshold & mask_not_clean, 'tmp_Flag'] += ', T'
            
            # 5. Spike Check ('S')
            spike_threshold = (df_subset['diff_next'])
            
            # Calculate masks based on CURRENT state (post-E, T updates)
            mask_is_clean = (df_subset['tmp_Flag'] == '')
            mask_not_clean = (df_subset['tmp_Flag'] != '')
            
            # Apply 'S'
            df_subset.loc[spike_threshold & mask_is_clean, 'tmp_Flag'] = 'S'
            df_subset.loc[spike_threshold & mask_not_clean, 'tmp_Flag'] += ', S'

            # 6. Jump Check ('J')
            # Check if absolute diff_next is greater than the p99.5 absolute diff_next
            jump_threshold = df_subset['diff_next'].abs() > p99_5_diff_next_abs
            
            # Calculate masks based on CURRENT state
            mask_is_clean = (df_subset['tmp_Flag'] == '')
            mask_not_clean = (df_subset['tmp_Flag'] != '')
            
            # Apply 'J'
            df_subset.loc[jump_threshold & mask_is_clean, 'tmp_Flag'] = 'J'
            df_subset.loc[jump_threshold & mask_not_clean, 'tmp_Flag'] += ', J'

            


            

            
            print(f"99.5th Percentile of diff: {p99_5}")
            print(f"0.5th Percentile of diff: {p00_5}")

            df_subset['p99_5_diff_next'] = p99_5_diff_next
            df_subset['p00_5_diff_next'] = p00_5_diff_next

            df_subset['p99_5_diff_next_abs'] = p99_5_diff_next_abs

            df_subset['p99_5_diff_prev'] = p99_5_diff_prev
            df_subset['p00_5_diff_prev'] = p00_5_diff_prev
            
            # Store these scalar values in new repeating columns
            df_subset['99.5_val_next_prev'] = p99_5
            df_subset['0.5_val_next_prev'] = p00_5

            df_subset['99.5_diff_next'] = p99_5_diff_next
            df_subset['0.5_diff_next'] = p00_5_diff_next

            df_subset['99.5_tmp'] = p99_5_tmp
            df_subset['0.5_tmp'] = p00_5_tmp

            # Also keep simple max/min columns as originally planned
            df_subset['tmp_Max'] = max_val
            df_subset['tmp_Min'] = min_val

            #add difference between val next and val_prv
            df_subset['val_next_val_prev'] = df_subset['val_next'] - df_subset['val_prev']
             
        print("\nIsolated DataFrame Head:")
        print(df_subset.head())
        
        return df_subset

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def plot_results(df):
    """
    Plots 'tmp' values over 'TIMESTAMP' with points colored by 'tmp_Flag'.
    """
    try:
        # Ensure TIMESTAMP is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['TIMESTAMP']):
            # Attempt to infer format or use specific format if known to speed up
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        
        # Sort by timestamp just in case
        df = df.sort_values('TIMESTAMP')
        
        plt.figure(figsize=(15, 8))
        
        # Plot lines first (neutral color) to show continuity
        # Drop NaNs from plotting line to avoid breaks if desired, or keep breaks
        # Convert to numpy to avoid multi-dimensional indexing issues
        x_vals = df['TIMESTAMP'].to_numpy()
        y_vals = df['tmp'].to_numpy()
        
        plt.plot(x_vals, y_vals, color='lightgray', linewidth=0.5, zorder=1)
        
        # Scatter for flags
        unique_flags = df['tmp_Flag'].unique()
        # Sort so '' (clean) is handled consistently
        unique_flags = sorted([str(f) for f in unique_flags])
        
        # Color map
        cm = plt.cm.get_cmap('rainbow')
        
        for i, flag in enumerate(unique_flags):
            # Select data
            mask = df['tmp_Flag'] == flag
            if flag == '':
               label = "Clean"
               color = 'blue' 
               alpha = 0.3
               s = 10
            elif flag == 'M':
                # Missing values usually have NaN tmp, so they won't plot.
                # If user wants to see them, we'd need to impute or plot rug.
                # For now, skip or plot if tmp exists (which it shouldn't for M)
                continue 
            else:
               label = flag
               # Pick color from rainbow
               # Avoid 0/1 extremes if they are hard to see?
               ratio = i / max(len(unique_flags)-1, 1)
               color = cm(ratio)
               alpha = 1.0
               s = 20
            
            # Scatter only valid tmp values
            valid_mask = mask & df['tmp'].notna()
            if valid_mask.any():
                # Use .to_numpy() explicitly
                subset_df = df[valid_mask]
                plt.scatter(subset_df['TIMESTAMP'].to_numpy(), subset_df['tmp'].to_numpy(), 
                            label=label, color=color, alpha=alpha, s=s, zorder=2)

        plt.title("Temperature Analysis: Tmp vs Timestamp (Colored by Flags)")
        plt.xlabel("Timestamp")
        plt.ylabel("Temperature")
        plt.legend(title="Flags")
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        output_img = "quick_fix_plot.png"
        plt.savefig(output_img)
        print(f"✅ Plot saved to: {os.path.abspath(output_img)}")
        
    except Exception as e:
        print(f"Plotting failed: {e}")

if __name__ == "__main__":
    # Path to the file mentioned by the user
    file_path = "data/Cheslata Lake_tidy (11).csv"
    
    df = isolate_columns(file_path)
    
    if df is not None:
        output_file = "quick_fix_output.csv"
        df.to_csv(output_file, index=False, na_rep='NaN')
        print(f"\n✅ Success! Isolated data saved to: {os.path.abspath(output_file)}")
        print("Ready for flag setting logic...")
        
        plot_results(df)
