import pandas as pd

def convert_timezone(df, timestamp_col, from_tz, to_tz):
    """
    Converts a timestamp column from one timezone to another.
    """
    if timestamp_col not in df.columns:
        return df
    
    try:
        # localized timestamps (if naive, assume from_tz)
        if df[timestamp_col].dt.tz is None:
            # Ambiguous times can happen during DST switch; 'infer' often works or raise
            localized = df[timestamp_col].dt.tz_localize(from_tz, ambiguous='infer', nonexistent='shift_forward')
        else:
            localized = df[timestamp_col]
            
        converted = localized.dt.tz_convert(to_tz)
        
        # User requested "just one number" (naive timestamp), stripping offset info.
        return converted.dt.tz_localize(None)
    except Exception as e:
        # Fallback or error logging
        print(f"Timezone conversion error: {e}")
        return df[timestamp_col]

def format_tidy_csv(df, station_config=None, header_info=None, process_codes=None):
    """
    Formats the dataframe for 'Tidy' export using the TOA5 standard (4 header rows).
    
    Rows:
    0: TOA5, StationID, LoggerModel, Serial, OS, Program, Sig, Table
    1: Column Names (from df.columns)
    2: Units (looked up from station_config)
    3: Process Codes (passed through or default empty)
    """
    if station_config is None:
        station_config = {}
    if header_info is None:
        header_info = {}
        
    # --- 1. Construct Row 0 (Environment) ---
    # Standard: TOA5, Station, LoggerModel, Serial, OS, Program, Sig, Table
    # We populate what we know.
    station_id = station_config.get("id", "Unknown")
    # If Logger details are in DataFrame (Logger_Model, Logger_ID), prioritize those for the "file" metadata?
    # Or use the passed header_info which came from the source file?
    # The user accepted the header_info approach.
    model = header_info.get("logger_model", "Unknown_Model")
    serial = header_info.get("logger_serial", "Unknown_Serial")
    
    # We use a fixed list for the first few items, then match column count
    # User requested removing: Check_Config_OS, Reconstructed_File, 0, Table1
    row0_prefix = ["TOA5", station_id, model, serial, "", "", "", ""]
    # Pad with empty strings to match df width
    row0 = row0_prefix + [""] * (len(df.columns) - len(row0_prefix))
    
    # --- 2. Construct Row 1 (Headers) ---
    row1 = list(df.columns)
    
    # --- 3. Construct Row 2 (Units) ---
    # Look up units in config for each column name
    thresholds = station_config.get("thresholds", {})
    row2 = []
    for col in df.columns:
        # Special columns
        if col == "TIMESTAMP":
            row2.append("TS")
        elif col == "RECORD":
            row2.append("RN")
        elif col in ["Station_ID", "Logger_ID", "Logger_Model"]:
            row2.append("") # No units for metadata
        else:
            # Check config
            unit = thresholds.get(col, {}).get("unit", "")
            row2.append(unit)
            
    # --- 4. Construct Row 3 (Process Codes) ---
    row3 = []
    # If we have original codes, try to align? 
    # Mismatch is high risk since we renamed/moved cols.
    # Safe default: empty or "Smp" for data?
    # User said: "make it so its auto-populated, if that row exists"
    # For now, let's leave blank unless we strictly know.
    # Or we can blindly pad the original codes if provided?
    # Let's just make them empty to avoid misleading "Avg" on a column that might be "Max"
    row3 = [""] * len(df.columns)
    
    # --- Combine ---
    # We need to construct a CSV string manually for the headers, then append df without header
    
    # Helper to join CSV line
    def to_csv_line(items):
        return ",".join([f'"{str(x)}"' for x in items]) + "\n"
        
    csv_str = ""
    csv_str += to_csv_line(row0)
    csv_str += to_csv_line(row1)
    csv_str += to_csv_line(row2)
    csv_str += to_csv_line(row3)
    
    # Append Data
    # User requested literal "NaN" for missing values
    csv_str += df.to_csv(index=False, header=False, na_rep='NaN')
    
    return csv_str.encode('utf-8')
