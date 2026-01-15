import pandas as pd
import numpy as np

def apply_qc(df, station_config):
    """
    Applies QA/QC flags to the dataframe based on station_config thresholds.
    thresholds structure: {ColName: {min: val, max: val, rate: val}}
    """
    df_qc = df.copy()
    thresholds = station_config.get("thresholds", {})
    
    # Iterate over each column configured for QC
    for col, rules in thresholds.items():
        if col not in df_qc.columns:
            continue
            
        # Initialize Flag Column
        flag_col = f"{col}_Flag"
        df_qc[flag_col] = "" 
        
        # We need numeric data for checks
        # Force numeric, coercing errors to NaN
        series = pd.to_numeric(df_qc[col], errors='coerce')
        
        qc_codes = []
        
        # Vectorized checks are faster, but for complex string concatenation of multiple flags 
        # (e.g., "High, Rate"), iteration or clever apply is needed. 
        # For simplicity and readability in this prototype:
        
        # 1. Range Check (Min/Max)
        min_val = rules.get("min")
        max_val = rules.get("max")
        
        if min_val is not None:
             df_qc.loc[series < min_val, flag_col] += "Low "
             
        if max_val is not None:
             df_qc.loc[series > max_val, flag_col] += "High "

        # 2. Rate of Change Check
        rate_val = rules.get("rate_of_change")
        if rate_val is not None:
             # Calculate absolute difference
             diff = series.diff().abs()
             # Flag where diff exceeds rate
             df_qc.loc[diff > rate_val, flag_col] += "Rate "
             
        # Cleanup: Strip trailing spaces
        df_qc[flag_col] = df_qc[flag_col].str.strip()
        
    return df_qc
