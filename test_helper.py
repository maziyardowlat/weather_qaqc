import pandas as pd
import json
import csv
import os

# Mock load_mapping for test context
def load_mapping():
    with open('column_mapping.json', 'r') as f:
        return json.load(f)

# Re-implement the helper function exactly as valid test
def write_csv_with_units(df, save_path):
    """
    Writes DataFrame to CSV with a second row containing units.
    Units are looked up from column_mapping.json.
    """
    # Helper to load mapping
    mapping = load_mapping()
    
    import csv
    with open(save_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # 1. Header
        writer.writerow(df.columns)
        
        # 2. Units
        units_row = []
        for col in df.columns:
            unit_val = "nan" # Default
            
            if col == 'TIMESTAMP': 
                unit_val = 'TS'
            elif col == 'RECORD': 
                unit_val = 'RN'
            elif col.endswith('_Flag'): 
                unit_val = 'nan'
            else:
                # Lookup in mapping
                if col in mapping:
                    info = mapping[col]
                    if isinstance(info, dict):
                        unit_val = info.get('unit', 'nan')
                        # If blank in JSON, use nan or empty? User sample had 'nan' for flags.
                        # Let's align with observed data "TS,RN,nan,Volts..."
                        if unit_val == "": unit_val = "nan"
            
            units_row.append(unit_val)
        
        writer.writerow(units_row)
        
    # 3. Data
    df.to_csv(save_path, mode='a', header=False, index=False, na_rep='NaN')

def test_helper_function():
    # Mock DF
    data = {
        'TIMESTAMP': ['2023-01-01 12:00:00'],
        'RECORD': [1],
        'BattV_Avg': [12.5],
        'BattV_Avg_Flag': [''],
        'Unknown_Col': [999]
    }
    df = pd.DataFrame(data)
    save_path = 'test_helper_output.csv'
    
    write_csv_with_units(df, save_path)
    
    print(f"Saved test file to {save_path}")
    
    # Read back and verify
    with open(save_path, 'r') as f:
        lines = f.readlines()
        
    print("\n-- File Content --")
    for line in lines:
        print(line.strip())
        
    # Check Row 2
    row2 = lines[1].strip().split(',')
    expected_units = ['TS', 'RN', 'Volts', 'nan', 'nan'] # Unknown_Col defaults to nan
    
    print(f"\nExpected Row 2: {expected_units}")
    print(f"Actual Row 2:   {row2}")
    
    if row2 == expected_units:
        print("SUCCESS: Units row matches expectations.")
    else:
        print("FAILURE: Units row mismatch.")

if __name__ == "__main__":
    test_helper_function()
