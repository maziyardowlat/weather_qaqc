import pandas as pd
import io

def test_load_skip_units():
    # Mock CSV content with units row
    csv_content = """TIMESTAMP,RECORD,BattV_Avg
TS,RN,Volts
2023-01-01 12:00:00,1,12.5
2023-01-01 12:15:00,2,12.6"""
    
    # Simulate loading
    df_qc = pd.read_csv(io.StringIO(csv_content), low_memory=False)
    
    print("Initial DF:")
    print(df_qc.head())
    
    # Apply logic from app.py
    if not df_qc.empty and 'TIMESTAMP' in df_qc.columns:
        first_val = str(df_qc.iloc[0]['TIMESTAMP'])
        print(f"First timestamp value: {first_val}")
        
        if first_val == 'TS':
            print("Detected units row. Skipping...")
            df_qc = df_qc.iloc[1:].reset_index(drop=True)
            
    print("\nProcessed DF:")
    print(df_qc.head())
    
    # Verify
    if len(df_qc) == 2:
        print("SUCCESS: Row count correct (2 data rows).")
    else:
        print(f"FAILURE: Row count incorrect ({len(df_qc)}).")
        
    # Verify TIMESTAMP is ready for conversion
    try:
        df_qc['TIMESTAMP'] = pd.to_datetime(df_qc['TIMESTAMP'])
        print("SUCCESS: Timestamp conversion successful.")
    except Exception as e:
        print(f"FAILURE: Timestamp conversion failed: {e}")

if __name__ == "__main__":
    test_load_skip_units()
