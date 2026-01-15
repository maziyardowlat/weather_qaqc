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
        
        # If exporting, we might want to remove offset info to keep it simple, 
        # but standardized CSVs usually prefer ISO format or just local time.
        # The user requested switching it for the tidy file. 
        # We will return the series with tz info.
        return converted
    except Exception as e:
        # Fallback or error logging
        print(f"Timezone conversion error: {e}")
        return df[timestamp_col]

def format_tidy_csv(df):
    """
    Formats the dataframe for 'Tidy' export.
    - Flattens structure?
    - Ensures standard columns?
    For now, simply returns CSV string of the processed DF.
    """
    return df.to_csv(index=False).encode('utf-8')
