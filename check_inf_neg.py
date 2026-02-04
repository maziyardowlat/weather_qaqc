
import pandas as pd
import numpy as np

FILE = 'data/concatenated_one_year_phase3.csv'
QAQC_CONFIG = [
    # Logger
    {'column': 'BattV_Avg', 'max_change': 1},
    {'column': 'PTemp_C_Avg', 'max_change': 5},
    # ClimaVue50
    {'column': 'RHT_C_Avg', 'max_change': 5}, 
    {'column': 'SlrFD_W_Avg', 'max_change': None},
    {'column': 'Rain_mm_Tot', 'max_change': 20}, 
    {'column': 'Strikes_tot', 'max_change': None},
    {'column': 'Dist_km_Avg', 'max_change': None},
    {'column': 'WS_ms_Avg', 'max_change': None},
    {'column': 'WindDir', 'max_change': None},
    {'column': 'AirT_C_Avg', 'max_change': 5},
    {'column': 'VP_hPa_Avg', 'max_change': 1},
    {'column': 'RH', 'max_change': 20},
    {'column': 'BP_hPa_Avg', 'max_change': 10},
    {'column': 'TiltNS_deg_Avg', 'max_change': 1},
    {'column': 'TiltWE_deg_Avg', 'max_change': 1},
    {'column': 'SlrTF_MJ_Tot', 'max_change': None},
    # SR50
    {'column': 'DT_Avg', 'max_change': None},
    {'column': 'TCDT_Avg', 'max_change': None},
    {'column': 'DBTCDT_Avg', 'max_change': 10},
    # Net Radiometer
    {'column': 'SWin_Avg', 'max_change': None},
    {'column': 'SWout_Avg', 'max_change': None},
    {'column': 'LWin_Avg', 'max_change': None},
    {'column': 'LWout_Avg', 'max_change': None},
    {'column': 'SWnet_Avg', 'max_change': None},
    {'column': 'LWnet_Avg', 'max_change': None},
    {'column': 'SWalbedo_Avg', 'max_change': None},
    {'column': 'NR_Avg', 'max_change': None},
    # Ground Thermistors
    {'column': 'stmp_Avg', 'max_change': 5},
    {'column': 'gtmp_Avg', 'max_change': 5},
]

def check_neg_inf():
    print(f"Reading {FILE}...")
    df = pd.read_csv(FILE, header=0, skiprows=[1], low_memory=False)
    
    print("Checking for NEGATIVE infinite values...")
    
    found_any = False
    
    # Map config to check if column *should* have flags
    config_map = {c['column']: c['max_change'] for c in QAQC_CONFIG}
    
    for col in df.columns:
        if col.endswith('_Flag') or col == 'TIMESTAMP':
            continue
            
        vals = pd.to_numeric(df[col], errors='coerce')
        is_neg_inf = np.isneginf(vals)
        
        if is_neg_inf.any():
            count = is_neg_inf.sum()
            max_change = config_map.get(col)
            should_flag = max_change is not None
            
            print(f"Column '{col}' has {count} -inf values. MaxChange={max_change}")
            found_any = True
            
            # Show examples
            example_indices = df.index[is_neg_inf].tolist()[:5]
            for idx in example_indices:
                flag_col = f"{col}_Flag"
                flag_val = df.at[idx, flag_col] if flag_col in df.columns else "N/A"
                print(f"  Row {idx}: Flag='{flag_val}'")
                
                if should_flag and 'S' not in flag_val and 'J' not in flag_val:
                     print(f"    WARNING: Should be flagged J/S but is not!")

    if not found_any:
        print("No negative infinite values found.")

if __name__ == '__main__':
    check_neg_inf()
