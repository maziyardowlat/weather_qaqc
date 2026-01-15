import pandas as pd
import streamlit as st

def parse_toa5(uploaded_file, skip_rows=None):
    """
    Parses a Campbell Scientific TOA5 file.
    Assumes standard 4-line header structure:
    Line 0: Info
    Line 1: Column Names (Header)
    Line 2: Units
    Line 3: Process Codes
    
    skip_rows: List of row indices to skip. Default is [0, 2, 3].
    
    Returns:
        df: Pandas DataFrame
        metadata: Dictionary with units {col_name: unit}
        error: Error message string or None
    """
    try:
        # Default behavior if not specified
        if skip_rows is None:
            # Skip 0 (info), 2 (units), 3 (codes)
            # Line 1 is header
            skip_rows = [0, 2, 3]
            
        # We need to extract Units (Line 2) before we skip it.
        # This requires reading the file twice or reading lines manually first.
        # Since uploaded_file is a stream, we must reset position.
        
        # 1. Extract Header Info
        uploaded_file.seek(0)
        # Read first 4 lines
        header_lines = [uploaded_file.readline().decode('utf-8').strip().replace('"','') for _ in range(4)]
        
        # Line 1: Columns
        columns = header_lines[1].split(',')
        # Line 2: Units
        units = header_lines[2].split(',')
        
        # Create metadata map {Column: Unit}
        # Handle cases where lengths might mismatch
        metadata = {}
        if len(columns) == len(units):
            metadata = dict(zip(columns, units))
        
        # 2. Parse DataFrame
        uploaded_file.seek(0) # Reset pointer
        df = pd.read_csv(uploaded_file, skiprows=skip_rows)
        
        # Convert TIMESTAMP to datetime
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
        
        return df, metadata, None
    except Exception as e:
        return None, {}, str(e)
