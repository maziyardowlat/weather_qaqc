
import pandas as pd
import numpy as np

FILE = 'data/concatenated_one_year_phase3.csv'

def check_inf():
    print(f"Reading {FILE}...")
    # Skip the second header line (units)
    df = pd.read_csv(FILE, header=0, skiprows=[1], low_memory=False)
    
    # Check for -inf
    # -inf might be loaded as float -inf, or string.
    
    print("Checking for infinite values...")
    
    found_any = False
    
    for col in df.columns:
        if col.endswith('_Flag') or col == 'TIMESTAMP':
            continue
            
        # Convert to numeric to be sure, detecting inf
        vals = pd.to_numeric(df[col], errors='coerce')
        
        # Check for actual infs
        is_inf = np.isinf(vals)
        if is_inf.any():
            count = is_inf.sum()
            print(f"Column '{col}' has {count} infinite values.")
            found_any = True
            
            # Show a few examples
            example_indices = df.index[is_inf].tolist()[:5]
            for idx in example_indices:
                val = df.at[idx, col]
                flag_col = f"{col}_Flag"
                flag_val = df.at[idx, flag_col] if flag_col in df.columns else "N/A"
                print(f"  Row {idx}: Value={val}, Flag='{flag_val}'")
                
                # Also check surrounding values to see why Jump/Spike might check failed
                if idx > 0:
                    prev_val = df.at[idx-1, col]
                    print(f"    Prev: {prev_val}")
                if idx < len(df)-1:
                    next_val = df.at[idx+1, col]
                    print(f"    Next: {next_val}")

    if not found_any:
        print("No standard numpy infinite values found.")
        # Check for string representations just in case
        str_infs = df.isin(['-inf', 'inf', '-Inf', 'Inf']).sum()
        if str_infs.any():
            print("Found string representations of inf:")
            print(str_infs[str_infs > 0])
        else:
            print("No string 'inf' found either.")

if __name__ == '__main__':
    check_inf()
