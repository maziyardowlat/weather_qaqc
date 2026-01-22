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

def format_tidy_csv(df):
    """
    Formats the dataframe for 'Tidy' export.
    - Flattens structure?
    - Ensures standard columns?
    For now, simply returns CSV string of the processed DF.
    """
    return df.to_csv(index=False).encode('utf-8')
