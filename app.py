
# ... (imports: streamlit, pandas, os, json, numpy, datetime)

import streamlit as st
import pandas as pd
import os
import json
import numpy as np
import numpy as np
from datetime import datetime, timedelta, timezone
try:
    from suntime import Sun
except ImportError:
    Sun = None # Handle missing dependency gracefully

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None


# Page Config
st.set_page_config(page_title="NHG Weather Pipeline", layout="wide")

# Constants
# Constants
MAPPING_FILE = "column_mapping.json"
GROUPS_FILE = "instrument_groups.json"
STATION_CONFIG_FILE = "station_configs.json"

# --- QC Configuration (Default) ---
# This serves as the "Base" checks if no instrument group is assigned
DEFAULT_THRESHOLDS = {
    'BattV_Avg': (10, 16),
    'PTemp_C_Avg': (-40, 70),
    'RHT_C_Avg': (-40, 50),
    'SlrFD_W_Avg': (0, 1350),
    'Rain_mm_Tot': (0, 33),
    'Strikes_Tot': (0, 66635),
    'Dist_km_Avg': (0, 40),
    'WS_ms_Avg': (0, 30),
    'WindDir': (0, 360),
    'AirT_C_Avg': (-50, 60),
    'VP_hPa_Avg': (0, 470),
    'BP_hPa_Avg': (850, 1050),
    'RH': (0, 100),
    'SlrTF_MJ_Tot': (0, 1.215),
    'DT_Avg': (50, 171), # Default H=166 + 5
    'DBTCDT_Avg': (0, 171),
    'SWin_Avg': (0, 1350), 
    'SWout_Avg': (0, 'SWin_Avg'),
    'LWin_Avg': (100, 550),
    'LWout_Avg': (150, 600),
    'SWnet_Avg': (0, 1350), 
    'LWnet_Avg': (-300, 100),
    'SWalbedo_Avg': (0, 1),
    'NR_Avg': (-200, 1000),
    'stmp_Avg': (-50, 60),
    'gtmp_Avg': (-50, 60), 
}

# Initial Instrument Definitions
# If instrument_groups.json doesn't exist, we build it from this list + DEFAULT_THRESHOLDS
INITIAL_INSTRUMENT_GROUPS = {
    'ClimaVue50': [
        'Rain_mm_Tot', 'Strikes_Tot', 'Dist_km_Avg', 'WS_ms_Avg', 'WindDir',
        'AirT_C_Avg', 'VP_hPa_Avg', 'BP_hPa_Avg', 'RH', 'SlrFD_W_Avg',
        'SlrTF_MJ_Tot', 'RHT_C_Avg'
    ],
    'SR50': [
        'DT_Avg', 'DBTCDT_Avg'
    ],
    'NetRadiometer': [
        'SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg',
        'LWnet_Avg', 'SWalbedo_Avg', 'NR_Avg', 'stmp_Avg', 'gtmp_Avg'
    ],
    'System': [
        'BattV_Avg', 'PTemp_C_Avg'
    ]
}

DEPENDENCY_CONFIG = [
    # ClimaVue50
    {'target': 'SlrFD_W_Avg', 'sources': ['TiltNS_deg_Avg', 'TiltWE_deg_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'Rain_mm_Tot', 'sources': ['TiltNS_deg_Avg', 'TiltWE_deg_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'AirT_C_Avg', 'sources': ['SlrFD_W_Avg', 'WS_ms_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'VP_hPa_Avg', 'sources': ['RHT_C_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'RH', 'sources': ['VP_hPa_Avg', 'AirT_C_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'SlrTF_MJ_Tot', 'sources': ['SlrFD_W_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SlrTF_MJ_Tot', 'sources': ['SlrFD_W_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    # SR50
    {'target': 'TCDT_Avg', 'sources': ['DT_Avg'], 'trigger_flags': ['T'], 'set_flag': 'DF'},
    {'target': 'TCDT_Avg', 'sources': ['AirT_C_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'SU'},
    {'target': 'DBTCDT_Avg', 'sources': ['TCDT_Avg'], 'trigger_flags': ['T'], 'set_flag': 'DF'},
    {'target': 'DBTCDT_Avg', 'sources': ['TCDT_Avg'], 'trigger_flags': ['SU'], 'set_flag': 'SU'},
    # Net Radiometer
    {'target': 'SWnet_Avg', 'sources': ['SWin_Avg', 'SWout_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SWnet_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'SWout_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'LWnet_Avg', 'sources': ['LWin_Avg', 'LWout_Avg'], 'trigger_flags': ['T', 'ERR'], 'set_flag': 'DF'},
    {'target': 'SWalbedo_Avg', 'sources': ['SWin_Avg', 'SWout_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'SWalbedo_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
    {'target': 'NR_Avg', 'sources': ['SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg'], 'trigger_flags': ['T', 'ERR', 'DF'], 'set_flag': 'DF'},
    {'target': 'NR_Avg', 'sources': ['SWin_Avg'], 'trigger_flags': ['Z'], 'set_flag': 'Z'},
]

SOLAR_COLUMNS = ['SlrFD_W_Avg', 'SWin_Avg']

ADD_CAUTION_FLAG = [
    'BattV_Avg', 'PTemp_C_Avg', 'SlrFD_W_Avg', 'Dist_km_Avg', 'WS_ms_Avg', 
    'MaxWS_ms_Avg', 'AirT_C_Avg', 'VP_hPa_Avg', 'BP_hPa_Avg', 'RHT_C_Avg', 
    'TiltNS_deg_Avg', 'TiltWE_deg_Avg', 'Invalid_Wind_Avg', 'DT_Avg', 
    'TCDT_Avg', 'DBTCDT_Avg', 'SWin_Avg', 'SWout_Avg', 'LWin_Avg', 
    'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 'SWalbedo_Avg', 'NR_Avg', 
    'stmp_Avg', 'gtmp_Avg'
]

# --- Helper Functions ---

def load_json_file(filepath, default=None):
    if default is None: default = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading {filepath}: {e}")
            return default
    return default

def save_json_file(filepath, data):
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
         st.error(f"Error saving {filepath}: {e}")

def load_mapping():
    return load_json_file(MAPPING_FILE, {})

def save_mapping(mapping):
    save_json_file(MAPPING_FILE, mapping)

def load_instrument_groups():
    groups = load_json_file(GROUPS_FILE, {})
    if not groups:
        # Initialize defaults if empty
        # Map list of cols to actual thresholds structure
        for grp_name, cols in INITIAL_INSTRUMENT_GROUPS.items():
            grp_data = {}
            for col in cols:
                # Use current defaults for initial values
                if col in DEFAULT_THRESHOLDS:
                    grp_data[col] = DEFAULT_THRESHOLDS[col]
            groups[grp_name] = grp_data
        
        # Save the initialized groups so user can edit them
        save_json_file(GROUPS_FILE, groups)
    return groups

def save_instrument_groups(groups):
    save_json_file(GROUPS_FILE, groups)

def load_station_configs():
    return load_json_file(STATION_CONFIG_FILE, {})

def save_station_configs(configs):
    save_json_file(STATION_CONFIG_FILE, configs)

def parse_toa5_header(file):
    """
    Parses the first line of a TOA5 file (StringIO or UploadedFile)
    Returns a dict with metadata, defaults to '999' if missing.
    """
    meta = {
        'logger_id': '999',
        'logger_script': '999',
        'logger_software': '999'
    }
    
    try:
        # Read first line without consuming the file permanently
        # For UploadedFile, we read, parse, then seek(0)
        line = file.readline().decode('utf-8').strip()
        parts = [p.strip().strip('"') for p in line.split(',')]
        
        # TOA5 Standard: Format, StationName, Model, Serial, OS, ProgramName, Sig, Table
        if len(parts) >= 6:
            model = parts[2]
            serial = parts[3]
            os_ver = parts[4]
            prog_name = parts[5]
            
            meta['logger_id'] = f"{model}-{serial}"
            meta['logger_software'] = f"{model}-{serial}-{os_ver}"
            meta['logger_script'] = f"{model}-{serial}-{prog_name}" # Version approx
            
    except Exception as e:
        st.warning(f"Could not parse TOA5 header: {e}")
        
    finally:
        file.seek(0)
        
    return meta

def load_csv_preview(file):
    """Loads header and first few rows for preview"""
    try:
        # Skip TOA5 header (0), Units (2), Type (3) usually.
        # But for preview we just want to see columns.
        # Let's try to detect header row. Usually row 1 (0-indexed).
        df = pd.read_csv(file, skiprows=[0, 2, 3], nrows=5, keep_default_na=False)
        file.seek(0)
        return df
    except Exception as e:
        return None

def parse_field_report(pdf_file):
    """
    Parses a Field Report PDF to extract Date, Time-in, and Time-out.
    Expects formats like:
    Date YYYY-MM-DD
    Time-in HH:MM ...
    Time-out HH:MM ...
    
    Returns a dict with 'field_in', 'field_out' found strings or None.
    """
    if not PdfReader:
        st.error("pypdf not installed. Cannot parse PDF.")
        return {}

    parsed_data = {}
    
    try:
        reader = PdfReader(pdf_file)
        full_text = ""
        
        # Read all pages (usually short reports) to be safe
        for page in reader.pages:
            full_text += page.extract_text() + "\n"
            
        import re
        
        # 1. Extract Date
        # Pattern: Date YYYY-MM-DD
        date_match = re.search(r'Date\s+(\d{4}-\d{2}-\d{2})', full_text)
        report_date = date_match.group(1) if date_match else None
        
        # 2. Extract Time-in
        # Pattern: Time-in HH:MM (ignore rest)
        in_match = re.search(r'Time-in\s+(\d{1,2}:\d{2})', full_text)
        time_in = in_match.group(1) if in_match else None
        
        # 3. Extract Time-out
        # Pattern: Time-out HH:MM
        out_match = re.search(r'Time-out\s+(\d{1,2}:\d{2})', full_text)
        time_out = out_match.group(1) if out_match else None
        
        if report_date and time_in:
            parsed_data['field_in'] = f"{report_date} {time_in}"
            
        if report_date and time_out:
            parsed_data['field_out'] = f"{report_date} {time_out}"
            
    except Exception as e:
        st.warning(f"Error parsing PDF: {e}")
        
    finally:
        pdf_file.seek(0)
        
    return parsed_data

def process_file_data(uploaded_file, mapping, metadata, data_id, station_id):
    """
    Reads the full CSV, applies mapping, adds metadata columns.
    Returns processed DataFrame.
    """
    try:
        # Read full file
        # Skip TOA5 header (0), Units (2), Type (3). Header is row 1.
        df = pd.read_csv(uploaded_file, skiprows=[0, 2, 3], 
                         na_values=['NAN', '"NAN"', '', '-7999', '7999'], 
                         keep_default_na=True, skipinitialspace=True, low_memory=False)
        
        # Reset file pointer for next read if needed (though Streamlit handles this usually)
        uploaded_file.seek(0)

        # Rename columns
        # Invert mapping? No, mapping is Source -> Target
        # df.rename(columns=mapping)
        # However, mapping might be incomplete, so only rename known keys
        # 1. Identify Drop Columns
        cols_to_drop = [k for k, v in mapping.items() if v == "REMOVE" and k in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            
        # 2. Rename remaining
        clean_mapping = {k: v for k, v in mapping.items() if k in df.columns and v != k and v != "REMOVE"}
        if clean_mapping:
            df = df.rename(columns=clean_mapping)
            
        # Standardize Timestamp
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        
        # Add Metadata Columns
        df['Data_ID'] = str(data_id)
        df['Station_ID'] = station_id
        df['Logger_ID'] = metadata.get('Logger_ID', '999')
        df['Logger_Script'] = metadata.get('Logger_Script', '999')
        df['Logger_Software'] = metadata.get('Logger_Software', '999')
        
        return df

    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}")
        return pd.DataFrame()

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

# --- UI Components ---

def mapping_editor_ui():
    """
    UI for editing the column mapping JSON using a table editor.
    """
    st.write("Edit the column mapping configuration below.")
    st.info("💡 **Tips:**\n- **Aliases**: Separate multiple values with a comma (e.g. `battv, BattV`).\n- **Add Row**: Click the `+` at the bottom to add a new variable.\n- **Remove Row**: Select rows and press `Delete`.")
    
    current_mapping = load_mapping()
    
    # 1. FLATTEN: Convert JSON Dict -> List of Dicts for DataFrame
    # Structure: Variable Name | Unit | Aliases (str) | Unit Aliases (str)
    table_data = []
    
    for var_name, info in current_mapping.items():
        # Handle cases where info might be malformed (though unlikely with our control)
        if not isinstance(info, dict): continue
        
        aliases_list = info.get('aliases', [])
        unit = info.get('unit', '')
        unit_aliases_list = info.get('unit_aliases', [])
        
        # Convert lists to comma-separated strings for easy editing
        aliases_str = ", ".join([str(a) for a in aliases_list])
        unit_aliases_str = ", ".join([str(ua) for ua in unit_aliases_list])
        
        table_data.append({
            "Variable Name": var_name,
            "Unit": unit,
            "Aliases": aliases_str,
            "Unit Aliases": unit_aliases_str
        })
        
    # Create DataFrame
    df = pd.DataFrame(table_data)
    
    # 2. EDIT: Show Data Editor
    # num_rows="dynamic" allows adding/deleting rows
    edited_df = st.data_editor(
        df, 
        num_rows="dynamic", 
        use_container_width=True,
        hide_index=True,
        column_config={
            "Variable Name": st.column_config.TextColumn("Variable Name", required=True, help="Canonical name of the variable (e.g. BattV_Avg)"),
            "Unit": st.column_config.TextColumn("Unit", help="Canonical unit (e.g. Volts)"),
            "Aliases": st.column_config.TextColumn("Aliases", help="Comma-separated list of alternative names found in raw files"),
            "Unit Aliases": st.column_config.TextColumn("Unit Aliases", help="Comma-separated list of alternative units found in raw files"),
        }
    )
    
    # 3. SAVE: Parse back to JSON
    if st.button("Save Changes"):
        try:
            new_mapping = {}
            for index, row in edited_df.iterrows():
                var_name = str(row.get("Variable Name", "")).strip()
                if not var_name: 
                    continue # Skip empty variable names
                
                # Parse Aliases
                aliases_raw = str(row.get("Aliases", ""))
                aliases = [a.strip() for a in aliases_raw.split(',') if a.strip()]
                
                # Parse Unit Aliases
                unit_aliases_raw = str(row.get("Unit Aliases", ""))
                unit_aliases = [ua.strip() for ua in unit_aliases_raw.split(',') if ua.strip()]
                
                unit = str(row.get("Unit", "")).strip()
                
                new_mapping[var_name] = {
                    "aliases": aliases,
                    "unit": unit,
                    "unit_aliases": unit_aliases
                }
                
            # Check if empty (safety check)
            if not new_mapping:
                st.warning("Mapping is empty. Are you sure?")
                if not st.button("Confirm Empty Save"):
                    return
            
            save_mapping(new_mapping)
            st.success("Configuration saved successfully!")
            st.rerun()
            
        except Exception as e:
            st.error(f"Error saving configuration: {e}")

# Check for st.dialog support (Streamlit >= 1.34.0)
if hasattr(st, "dialog"):
    @st.dialog("Global Settings: Column Mapping", width="large")
    def mapping_editor_dialog():
        mapping_editor_ui()
else:
    def mapping_editor_dialog():
        # Fallback for older Streamlit
        st.sidebar.markdown("---")
        st.sidebar.subheader("Edit Column Mapping")
        with st.sidebar.expander("Show Editor", expanded=True):
             mapping_editor_ui()

# --- Main App ---

def main():
    st.title("NHG Weather Data Pipeline")

    # --- Sidebar ---
    st.sidebar.header("Global Settings")
    
    # Column Mapping Editor Button
    if hasattr(st, "dialog"):
        if st.sidebar.button("Edit Column Mapping"):
            mapping_editor_dialog()
    else:
        # For fallback, we might just show it inline or use checkbox/expander logic
        # But for simplicity in fallback, let's just show the expander if they want
        mapping_editor_dialog()

    st.sidebar.divider()

    station_name = st.sidebar.text_input("Station Name", value="Station_Name")
    output_dir = st.sidebar.text_input("Output Directory", value="data")
    sensor_height = st.sidebar.number_input("Sensor Height (cm)", value=166)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # --- Tabs ---
    tab1, tab2 = st.tabs(["1. Ingestion & Concatenation", "2. QA/QC Processing"])

    # --- Tab 1: Ingestion ---
    with tab1:
        st.header("1. Ingestion & Standardization")
        
        uploaded_files = st.file_uploader("Upload Raw Logger Files (CSV/TOA5)", accept_multiple_files=True)
        
        processed_file_configs = [] # specific configs for each file

        if uploaded_files:
            st.divider()
            
            # Load current mapping to help with defaults
            mapping_ref = load_mapping()
            
            # --- Per File Configuration ---
            for i, file in enumerate(uploaded_files):
                with st.expander(f"File {i+1}: {file.name}", expanded=True):
                    col1, col2 = st.columns(2)
                    
                    # Metadata Auto-Parse
                    # Note: Need unique keys for widgets
                    # Reading file metadata
                    # We only parse if not already in session state? Or just parse every time for simplicity.
                    meta = parse_toa5_header(file)  
                    
                    with col1:
                        # Allow Empty Data ID -> Defaults to 999
                        did_val = st.text_input(f"Data ID ({file.name})", value="", key=f"did_{i}")
                        data_id = did_val if did_val.strip() else "999"

                        logger_id = st.text_input(f"Logger ID", value=meta['logger_id'], key=f"lid_{i}")
                        
                    with col2:
                        logger_script = st.text_input(f"Logger Script", value=meta['logger_script'], key=f"lsc_{i}")
                        logger_soft = st.text_input(f"Logger Software", value=meta['logger_software'], key=f"lsw_{i}")

                    # Caution Flag Option
                    add_caution = st.checkbox(f"Add Caution Flag (C) to all data columns", key=f"caution_{i}")

                    # Field Visits (Optional)
                    st.caption("Field Visit (Optional - Leave blank if none)")
                    
                    # PDF Uploader for Auto-fill
                    fv_pdf = st.file_uploader("Upload Field Report (PDF) to Auto-fill", type=["pdf"], key=f"fv_pdf_{i}")
                    
                    default_in = ""
                    default_out = ""
                    
                    if fv_pdf:
                        parsed = parse_field_report(fv_pdf)
                        if parsed.get('field_in'):
                            default_in = parsed['field_in']
                            st.success(f"Found Field In: {default_in}")
                        if parsed.get('field_out'):
                            default_out = parsed['field_out']
                            st.success(f"Found Field Out: {default_out}")
                            
                    fv_col1, fv_col2 = st.columns(2)
                    with fv_col1:
                        fv_in = st.text_input("Field In (YYYY-MM-DD HH:MM)", value=default_in, key=f"fi_{i}")
                    with fv_col2:
                        fv_out = st.text_input("Field Out (YYYY-MM-DD HH:MM)", value=default_out, key=f"fo_{i}")

                    
                    # Preview & Mapping
                    df_preview = load_csv_preview(file)
                    
                    final_mapping = {}
                    
                    if df_preview is not None:
                        st.dataframe(df_preview.head(), use_container_width=True)
                        
                        columns = df_preview.columns.tolist()
                        
                        # Auto-Map Logic
                        current_mapping = {}
                        for col in columns:
                            target = col 
                            # Try to find match in JSON
                            for std_name, info in mapping_ref.items():
                                # Handle new dict format (with units) or old list format
                                if isinstance(info, dict):
                                    aliases = info.get('aliases', [])
                                else:
                                    aliases = info # Fallback for old format

                                if col == std_name:
                                    target = std_name
                                    break
                                if col in aliases:
                                    target = std_name
                                    break
                            current_mapping[col] = target

                        st.caption("Column Mapping (Uncheck 'Include' to remove column)")
                        map_df = pd.DataFrame({
                            "Include": [True] * len(columns),
                            "Source": columns,
                            "Target": [current_mapping[c] for c in columns]
                        })
                        
                        # Editable dataframe
                        edited_map = st.data_editor(
                            map_df, 
                            key=f"map_editor_{i}",
                            num_rows="fixed",
                            column_config={
                                "Include": st.column_config.CheckboxColumn(
                                    label="Keep?",
                                    default=True
                                ),
                                "Source": st.column_config.TextColumn(disabled=True),
                                "Target": st.column_config.SelectboxColumn(
                                    label="Target Column",
                                    options=list(set(list(mapping_ref.keys()) + columns)), # Unique options
                                    required=True
                                )
                            },
                            use_container_width=True
                        )
                        
                        # Create dictionary from editor
                        # If Include is False, map to "REMOVE" which is handled in process_file_data
                        for index, row in edited_map.iterrows():
                            if row['Include']:
                                final_mapping[row['Source']] = row['Target']
                            else:
                                final_mapping[row['Source']] = "REMOVE"
                    
                    # Save Config
                    processed_file_configs.append({
                        "file": file,
                        "data_id": data_id,
                        "meta": {
                            "Logger_ID": logger_id, 
                            "Logger_Script": logger_script, 
                            "Logger_Software": logger_soft
                        },
                        "field_visit": (fv_in, fv_out) if fv_in and fv_out else None,
                        "field_visit": (fv_in, fv_out) if fv_in and fv_out else None,
                        "mapping": final_mapping,
                        "add_caution": add_caution
                    })

            # --- Processing Button ---
            if st.button("Process & Concatenate Datasets", type="primary"):
                with st.spinner("Processing Files..."):
                    all_dfs = []
                    
                    for cfg in processed_file_configs:
                        df = process_file_data(
                            cfg['file'], 
                            cfg['mapping'], 
                            cfg['meta'], 
                            cfg['data_id'], 
                            station_name
                        )
                        
                        if not df.empty:
                            # Apply Field Visits Here? Or after concat? Only matters for flagging.
                            # Better to store field visits and apply to final df
                            # But we need to know which file/period the visit applies to if we do it globally? 
                            # Actually, field visits are specific time ranges, so we can apply globally by range.
                            # Actually, field visits are specific time ranges, so we can apply globally by range.
                            
                            # Apply Manual Caution Flag if selected
                            if cfg.get('add_caution', False):
                                for col in ADD_CAUTION_FLAG:
                                    if col in df.columns:
                                        flag_col = f"{col}_Flag"
                                        if flag_col not in df.columns:
                                            df[flag_col] = "" # Init
                                        
                                        # Append "C"
                                        # Append "C" ensuring no duplicates
                                        curr = df[flag_col].fillna("").astype(str)
                                        # If "C" is already in the flag (e.g. "C", "C, T", "M, C"), don't add
                                        mask_has_c = curr.str.contains(r'\bC\b', regex=True)
                                        df.loc[~mask_has_c, flag_col] = np.where(curr[~mask_has_c] == "", "C", curr[~mask_has_c] + ", C")
                                        
                        all_dfs.append(df)
                    
                    if all_dfs:
                        # Concatenate
                        full_df = pd.concat(all_dfs, ignore_index=True)
                        
                        # Time processing
                        if 'TIMESTAMP' in full_df.columns:
                            full_df = full_df.sort_values('TIMESTAMP')
                            full_df = full_df.drop_duplicates(subset=['TIMESTAMP'], keep='first')
                            full_df = full_df.set_index('TIMESTAMP')
                            
                            # Resample 15T
                            full_df = full_df.resample('15T').asfreq()
                            
                            # Determine flags for Resampled
                            # Meta cols needed for rebuild
                            meta_cols = ['Data_ID', 'Station_ID', 'Logger_ID', 'Logger_Script', 'Logger_Software']
                            
                            # Reset index to get Timestamp back
                            df_final = full_df.reset_index()
                            
                            # Fill Metadata
                            # Station ID is constant
                            df_final['Station_ID'] = df_final['Station_ID'].fillna(station_name)
                            # Others ffill/bfill
                            for mc in meta_cols:
                                if mc in df_final.columns:
                                    df_final[mc] = df_final[mc].fillna(method='ffill').fillna(method='bfill')
                            
                            # Flags Logic
                            # 1. Create Flags
                            # 2. Check ERR (Infinity/NaN conversion)
                            # 3. Check M (Missing)
                            # 4. Check V (Field Visits)
                            
                            data_cols = [c for c in df_final.columns if c not in meta_cols and c != 'TIMESTAMP' and c != 'RECORD']
                            
                            # Collect all field visits
                            all_field_visits = []
                            for cfg in processed_file_configs:
                                if cfg['field_visit']:
                                    all_field_visits.append(cfg['field_visit'])
                                    
                            for col in data_cols:
                                # Skip metadata-like cols
                                if col.endswith('_Flag') or col == "RECORD": 
                                    continue
                                    
                                    continue
                                    
                                flag_col = f"{col}_Flag"
                                # Don't overwrite existing flags (e.g. C from file processing)
                                if flag_col not in df_final.columns:
                                    df_final[flag_col] = "" # Init
                                else:
                                    df_final[flag_col] = df_final[flag_col].fillna("").astype(str)
                                
                                # ERR Logic
                                # Convert to numeric
                                original = df_final[col].copy()
                                numeric = pd.to_numeric(df_final[col], errors='coerce')
                                
                                # Check for corruption (was present but became NaN) or Infinity
                                was_present = original.notna() & (original != "")
                                became_nan = numeric.isna()
                                is_inf = np.isinf(numeric)
                                
                                mask_err = is_inf | (was_present & became_nan)
                                
                                if mask_err.any():
                                    df_final.loc[mask_err, flag_col] = "ERR"
                                    numeric[mask_err] = np.nan
                                    
                                df_final[col] = numeric
                                
                                # M Logic (Missing)
                                mask_missing = df_final[col].isna()
                                # If missing and NOT ERR -> M
                                mask_m = mask_missing & (df_final[flag_col] != "ERR")
                                df_final.loc[mask_m, flag_col] = "M"
                                
                                # V Logic (Field Visits)
                                for f_in, f_out in all_field_visits:
                                    try:
                                        t_start = pd.to_datetime(f_in).floor('15T')
                                        t_end = pd.to_datetime(f_out).ceil('15T')
                                        
                                        mask_visit = (df_final['TIMESTAMP'] >= t_start) & (df_final['TIMESTAMP'] <= t_end)
                                        
                                        # Append V
                                        current_flags = df_final.loc[mask_visit, flag_col]
                                        new_flags = np.where(current_flags == "", "V", current_flags + ", V")
                                        df_final.loc[mask_visit, flag_col] = new_flags
                                        
                                    except Exception as e:
                                        st.warning(f"Invalid Field Visit Time: {f_in} - {f_out}")

                            # Reorder Columns
                            # Interleave Data and Flags
                            ordered_cols = ['TIMESTAMP']
                            
                            # Handle RECORD and RECORD_Flag
                            if 'RECORD' in df_final.columns:
                                ordered_cols.append('RECORD')
                                if 'RECORD_Flag' not in df_final.columns:
                                    df_final['RECORD_Flag'] = ""
                                ordered_cols.append('RECORD_Flag')

                            # Identify data columns (exclude reserved)
                            # Reserved: TIMESTAMP, RECORD, RECORD_Flag, Meta Cols, and ALL Flag columns (we add flags manually next to data)
                            reserved = set(['TIMESTAMP', 'RECORD', 'RECORD_Flag']) | set(meta_cols) | set([c for c in df_final.columns if c.endswith("_Flag")])
                            data_cols = [c for c in df_final.columns if c not in reserved]

                            # Add data columns and their flags
                            for col in data_cols:
                                ordered_cols.append(col)
                                flag_col = f"{col}_Flag"
                                if flag_col in df_final.columns:
                                    ordered_cols.append(flag_col)
                                        
                            # Add metadata columns at the END
                            for mc in meta_cols:
                                if mc in df_final.columns:
                                    ordered_cols.append(mc)
                            # Just ensure all columns are covered or dropped?
                            # For now, this covers the main requirement.
                            
                            df_final = df_final[ordered_cols]
                            
                            # Save
                            filename = f"{station_name}_concatenated_tidy.csv"
                            save_path = os.path.join(output_dir, filename)
                            
                            # Use helper to include units row
                            write_csv_with_units(df_final, save_path)
                            
                            st.success(f"Successfully processed {len(df_final)} records!")
                            st.success(f"Saved to: {save_path}")
                            st.dataframe(df_final.head(50))
                            
                        else:
                            st.error("Concatenation failed: Missing TIMESTAMP column.")


    # --- Tab 2: QA/QC ---
    with tab2:
        st.header("2. QA/QC Processing")

        # File Selection (Moved Up)
        if os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith("_concatenated_tidy.csv")]
        else:
            files = []
        selected_file = st.selectbox("Select File to Process", files)
        
        # Load File Metadata (Dates) if selected
        file_start_date = datetime.today().date()
        file_end_date = datetime.today().date()
        
        if selected_file:
            file_path = os.path.join(output_dir, selected_file)
            try:
                # Read timestamp column, skip units row (row index 1)
                df_dates = pd.read_csv(file_path, usecols=['TIMESTAMP'], skiprows=[1])
                df_dates['TIMESTAMP'] = pd.to_datetime(df_dates['TIMESTAMP'])
                file_start_date = df_dates['TIMESTAMP'].min().date()
                file_end_date = df_dates['TIMESTAMP'].max().date()
                st.success(f"📅 File date range: {file_start_date} to {file_end_date}")
            except Exception as e:
                st.warning(f"Could not load file dates: {e}. Using today's date as default.")

        st.divider()

        # --- Instrument Configuration ---
        with st.expander("🛠️ Configure Instruments & Deployments", expanded=True):
            tab_groups, tab_deploy = st.tabs(["Instrument Groups", "Deployment History"])
            
            # --- Tab A: Instrument Groups ---
            with tab_groups:
                st.info("Define sets of instruments (e.g., 'ClimaVue50', 'Winter Setup') and their specific thresholds.")
                
                # Load
                groups = load_instrument_groups()
                group_names = list(groups.keys())
                
                # Edit / Create
                col_grp1, col_grp2 = st.columns([1, 2])
                with col_grp1:
                    selected_group = st.selectbox("Select Group to Edit", ["<New Group>"] + group_names)
                    
                    if selected_group == "<New Group>":
                        new_grp_name = st.text_input("New Group Name")
                        grp_name = new_grp_name if new_grp_name else None
                        current_cols = []
                        current_thresholds = {}
                    else:
                        grp_name = selected_group
                        current_data = groups[selected_group]
                        current_cols = list(current_data.keys())
                        current_thresholds = current_data

                with col_grp2:
                    if grp_name:
                        # Select Columns
                        # Get all available columns from mapping for suggestions
                        mapping = load_mapping()
                        all_known_cols = list(mapping.keys()) + list(DEFAULT_THRESHOLDS.keys())
                        all_known_cols = sorted(list(set(all_known_cols)))
                        
                        # Multiselect
                        # Pre-select columns that are in the current group
                        default_sel = [c for c in current_cols if c in all_known_cols]
                        # Ensure we don't lose cols that might not be in "known"
                        extras = [c for c in current_cols if c not in all_known_cols]
                        
                        selected_cols = st.multiselect("Included Columns", all_known_cols + extras, default=default_sel + extras)
                        
                        # Threshold Editor for Selected Cols
                        if selected_cols:
                            st.caption("Set Thresholds for this Group:")
                            edit_data = []
                            for c in selected_cols:
                                # Get existing or default or empty
                                if c in current_thresholds:
                                    val = current_thresholds[c]
                                    if isinstance(val, list) or isinstance(val, tuple):
                                        cur_min, cur_max = val[0], val[1]
                                    else:
                                        cur_min, cur_max = 0, 0
                                elif c in DEFAULT_THRESHOLDS:
                                    cur_min, cur_max = DEFAULT_THRESHOLDS[c]
                                else:
                                    cur_min, cur_max = 0, 0
                                edit_data.append({"Column": c, "Min": cur_min, "Max": cur_max})
                            
                            grp_df = pd.DataFrame(edit_data)
                            edited_grp_df = st.data_editor(grp_df, key=f"editor_{grp_name}", use_container_width=True)
                            
                            if st.button("Save Group"):
                                new_grp_data = {}
                                for idx, row in edited_grp_df.iterrows():
                                    new_grp_data[row['Column']] = (row['Min'], row['Max'])
                                
                                groups[grp_name] = new_grp_data
                                save_instrument_groups(groups)
                                st.success(f"Saved group '{grp_name}'!")
                                st.rerun()

            # --- Tab B: Deployment History ---
            with tab_deploy:
                st.info(f"Assign Instrument Groups to specific time ranges for Station: **{station_name}**")
                
                configs = load_station_configs()
                st_cfg = configs.get(station_name, [])
                
                # Display Current Configs
                if st_cfg:
                    st.write("Current Deployments:")
                    cfg_df = pd.DataFrame(st_cfg)
                    st.dataframe(cfg_df)
                    
                    if st.button("Clear History"):
                        configs[station_name] = []
                        save_station_configs(configs)
                        st.rerun()
                else:
                    st.warning("No deployment history found. 'Base' thresholds will apply everywhere.")

                st.divider()
                st.write("Add Deployment:")
                c1, c2, c3 = st.columns(3)
                with c1:
                    d_start = st.date_input("Start Date", value=file_start_date)
                with c2:
                    d_end = st.date_input("End Date", value=file_end_date)
                with c3:
                    d_grp = st.selectbox("Instrument Group", group_names)
                    
                if st.button("Add Assignment"):
                    if d_start > d_end:
                        st.error("Start date must be before end date.")
                    else:
                        new_entry = {
                            "start": str(d_start),
                            "end": str(d_end),
                            "group": d_grp
                        }
                        if station_name not in configs:
                            configs[station_name] = []
                        configs[station_name].append(new_entry)
                        # Sort by start date
                        configs[station_name].sort(key=lambda x: x['start'])
                        save_station_configs(configs)
                        st.success("Added deployment!")
                        st.rerun()
                
                st.divider()
                st.subheader("🔍 Check Active Thresholds")
                check_date = st.date_input("Preview thresholds for date:", value=file_start_date)
                
                # Logic to find active group
                active_grp_name = "None (Using Defaults)"
                active_grp_data = {}
                
                start_check = str(check_date)
                # Check overlapping configs
                for cfg in st_cfg:
                    if cfg['start'] <= start_check <= cfg['end']:
                        active_grp_name = cfg['group']
                        active_grp_data = groups.get(active_grp_name, {})
                        break
                
                st.write(f"**Active Group:** {active_grp_name}")
                
                # Build Comparison Table
                preview_data = []
                for k, v in DEFAULT_THRESHOLDS.items():
                    # Default
                    def_min, def_max = v
                    
                    # Override
                    if k in active_grp_data:
                        act_min, act_max = active_grp_data[k]
                        source = "Instrument Group"
                    else:
                        act_min, act_max = def_min, def_max
                        source = "Default"
                        
                    preview_data.append({
                        "Column": k,
                        "Effective Min": str(act_min),
                        "Effective Max": str(act_max),
                        "Source": source
                    })
                
                st.dataframe(pd.DataFrame(preview_data), use_container_width=True)


        if selected_file:
            st.divider()

            # Use DEFAULT_THRESHOLDS directly as the base
            active_thresholds = DEFAULT_THRESHOLDS.copy()
            # Apply sensor height adjustments
            active_thresholds['DT_Avg'] = (50, sensor_height + 5)
            active_thresholds['DBTCDT_Avg'] = (0, sensor_height + 5)

            # --- Logic Functions (Embedded to access st context) ---
            def run_qc_pipeline(df):
                # Load Time-Varying Configs
                instr_groups = load_instrument_groups()
                st_configs = load_station_configs()
                current_deps = st_configs.get(station_name, [])
                
                # 1. Apply Thresholds
                # Identify columns to check: All vars in DataFrame that aren't flags/meta
                qc_cols = [c for c in df.columns if not c.endswith('_Flag') and c not in ['TIMESTAMP', 'RECORD', 'Data_ID', 'Station_ID', 'Logger_ID', 'Logger_Script', 'Logger_Software']]
                
                # Helper to resolve value/col reference
                def resolve_val(v, data_slice):
                    if isinstance(v, str) and v in data_slice.columns:
                        return pd.to_numeric(data_slice[v], errors='coerce')
                    return v

                for col in qc_cols:
                    # Determine Base Thresholds (from UI editor)
                    if col in active_thresholds:
                         base_min, base_max = active_thresholds[col]
                    else:
                        # If not in Base and not in any Group, skip
                        # Optimization: check if any group has it
                        relevant_deps = [d for d in current_deps if col in instr_groups.get(d['group'], {})]
                        if not relevant_deps:
                            continue
                        base_min, base_max = -np.inf, np.inf

                    # Initialize Limits with Base
                    # We use Series to handle row-by-row variations
                    vals = pd.to_numeric(df[col], errors='coerce')
                    
                    # Optimization: If no deployments override this column, use vector calc
                    relevant_deps = [d for d in current_deps if col in instr_groups.get(d['group'], {})]
                    
                    if not relevant_deps:
                         limit_min = resolve_val(base_min, df)
                         limit_max = resolve_val(base_max, df)
                         mask_fail = (vals < limit_min) | (vals > limit_max)
                    else:
                        # Complex: Time-Varying
                        limit_min_series = pd.Series(np.nan, index=df.index)
                        limit_max_series = pd.Series(np.nan, index=df.index)
                        
                        # fill Base
                        limit_min_series[:] = resolve_val(base_min, df)
                        limit_max_series[:] = resolve_val(base_max, df)
                        
                        for dep in current_deps:
                            grp = instr_groups.get(dep['group'], {})
                            if col in grp:
                                try:
                                    t_s = pd.to_datetime(dep['start'])
                                    t_e = pd.to_datetime(dep['end']) + timedelta(hours=23, minutes=59)
                                    
                                    mask_time = (df['TIMESTAMP'] >= t_s) & (df['TIMESTAMP'] <= t_e)
                                    if mask_time.any():
                                        g_min, g_max = grp[col]
                                        
                                        # Resolve values for this slice
                                        vals_slice = df.loc[mask_time]
                                        v_min = resolve_val(g_min, vals_slice)
                                        v_max = resolve_val(g_max, vals_slice)
                                        
                                        limit_min_series.loc[mask_time] = v_min
                                        limit_max_series.loc[mask_time] = v_max
                                except Exception as e:
                                    st.warning(f"Config Error ({dep}): {e}")
                                    
                        mask_fail = (vals < limit_min_series) | (vals > limit_max_series)

                    # Check Fail & Existing M
                    flag_col = f"{col}_Flag"
                    if flag_col not in df.columns: df[flag_col] = "" 

                    current_flags = df[flag_col].fillna("").astype(str)
                    mask_has_m = current_flags.str.contains('M')
                    mask_apply = mask_fail & (~mask_has_m)

                    if mask_apply.any():
                        target_indices = current_flags.index[mask_apply]
                        targets = current_flags.loc[mask_apply]
                        new_flags = np.where(targets == "", "T", targets + ", T")
                        df.loc[mask_apply, flag_col] = new_flags

                # 2. Dynamic/Logic Flags
                # Snow Depth Logic
                limit = sensor_height - 50
                if 'DBTCDT_Avg' in df.columns:
                     vals = pd.to_numeric(df['DBTCDT_Avg'], errors='coerce')
                     flag_col = 'DBTCDT_Avg_Flag'
                     # T > H-50
                     mask_t = (vals > limit)
                     if mask_t.any():
                         curr = df.loc[mask_t, flag_col].fillna("").astype(str)
                         df.loc[mask_t, flag_col] = np.where(curr == "", "T", curr + ", T")
                     # Summer Snow
                     if 'TIMESTAMP' in df.columns:
                         months = df['TIMESTAMP'].dt.month
                         mask_sf = months.isin([6,7,8,9]) & (vals > 0)
                         if mask_sf.any():
                             curr = df.loc[mask_sf, flag_col].fillna("").astype(str)
                             df.loc[mask_sf, flag_col] = np.where(curr == "", "SF", curr + ", SF")

                # Wind Logic (NW)
                if 'WS_ms_Avg' in df.columns and 'WindDir' in df.columns:
                    ws = pd.to_numeric(df['WS_ms_Avg'], errors='coerce').fillna(0)
                    mask_calm = (ws == 0)
                    if mask_calm.any():
                        fc = 'WindDir_Flag'
                        curr = df.loc[mask_calm, fc].fillna("").astype(str)
                        df.loc[mask_calm, fc] = np.where(curr == "", "NW", curr + ", NW")

                # Albedo Logic (SU)
                if 'SWalbedo_Avg' in df.columns:
                    alb = pd.to_numeric(df['SWalbedo_Avg'], errors='coerce')
                    mask_su = (alb < 0.1) | (alb > 0.95)
                    if mask_su.any():
                        fc = 'SWalbedo_Avg_Flag'
                        curr = df.loc[mask_su, fc].fillna("").astype(str)
                        df.loc[mask_su, fc] = np.where(curr == "", "SU", curr + ", SU")

                # Tilt Logic (T/SU) - moved from static
                for tcol in ['TiltNS_deg_Avg', 'TiltWE_deg_Avg']:
                    if tcol in df.columns:
                        vals = pd.to_numeric(df[tcol], errors='coerce')
                        fc = f"{tcol}_Flag"
                        # T > 10
                        mask_t = (vals.abs() > 10)
                        if mask_t.any():
                            curr = df.loc[mask_t, fc].fillna("").astype(str)
                            df.loc[mask_t, fc] = np.where(curr == "", "T", curr + ", T")
                        # SU > 3
                        mask_su = (vals.abs() > 3) & (vals.abs() <= 10)
                        if mask_su.any():
                            curr = df.loc[mask_su, fc].fillna("").astype(str)
                            df.loc[mask_su, fc] = np.where(curr == "", "SU", curr + ", SU")

                # 3. Nighttime Flags (Z)
                if Sun and 'TIMESTAMP' in df.columns:
                    # Using lat/lon hardcoded in phase_2.py: 53.7217, -125.6417
                    # Ideally this comes from Metadata/UI, but keeping compat with script
                    latitude = 53.7217
                    longitude = -125.6417
                    sun = Sun(latitude, longitude)
                    tz_pdt = timezone(timedelta(hours=-7))

                    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
                    temp_dates = df['TIMESTAMP'].dt.date
                    unique_dates = temp_dates.unique()

                    for d in unique_dates:
                        # Find rise/set
                        rise_naive = None
                        set_naive = None
                        candidates = [datetime(d.year, d.month, d.day), datetime(d.year, d.month, d.day) + timedelta(days=1)]

                        for cand in candidates:
                            try:
                                r_utc = sun.get_sunrise_time(cand)
                                s_utc = sun.get_sunset_time(cand)
                                r_pdt = r_utc.astimezone(tz_pdt)
                                s_pdt = s_utc.astimezone(tz_pdt)

                                if r_pdt.date() == d: rise_naive = r_pdt.replace(tzinfo=None)
                                if s_pdt.date() == d: set_naive = s_pdt.replace(tzinfo=None)
                            except: continue

                        if rise_naive and set_naive:
                            mask_date = (temp_dates == d)
                            ts_vals = df.loc[mask_date, 'TIMESTAMP']
                            padding = timedelta(minutes=15)

                            mask_night = (ts_vals < (rise_naive - padding)) | (ts_vals > (set_naive + padding))
                            night_indices = ts_vals[mask_night].index

                            if len(night_indices) > 0:
                                for scol in SOLAR_COLUMNS:
                                    if scol in df.columns:
                                        vals = pd.to_numeric(df.loc[night_indices, scol], errors='coerce').fillna(0)
                                        mask_nz = (vals.abs() > 0.0001)
                                        if mask_nz.any():
                                            idx = vals[mask_nz].index
                                            fc = f"{scol}_Flag"
                                            curr = df.loc[idx, fc].fillna("").astype(str)
                                            df.loc[idx, fc] = np.where(curr == "", "Z", curr + ", Z")

                # 4. Critical Flags (PTemp)
                if 'PTemp_C_Avg_Flag' in df.columns:
                   pf = df['PTemp_C_Avg_Flag'].fillna("").astype(str)
                   mask_crit = pf.str.contains(r'\bT\b', regex=True)
                   if mask_crit.any():
                       st.warning(f"Critical PTemp Failure found in {mask_crit.sum()} records.")
                       for col in active_thresholds.keys():
                           if col == 'PTemp_C_Avg' or col not in df.columns: continue
                           fc = f"{col}_Flag"
                           curr = df.loc[mask_crit, fc].fillna("").astype(str)
                           df.loc[mask_crit, fc] = np.where(curr == "", "ERR", curr + ", ERR")

                # 5. Legacy/Unique Cases
                # LR (Logger Reset)
                if "RECORD" in df.columns:
                     vals = pd.to_numeric(df["RECORD"], errors='coerce')
                     prev = vals.shift(1)
                     is_start = prev.isna()
                     mask_restart = (vals < prev) | (is_start & (vals==0))
                     if mask_restart.any():
                         fc = "RECORD_Flag"
                         if fc not in df.columns: df[fc] = ""
                         curr = df.loc[mask_restart, fc].fillna("").astype(str)
                         df.loc[mask_restart, fc] = np.where(curr == "", "LR", curr + ", LR")

                # Legacy C
                if "Data_ID" in df.columns:
                    mask_leg = (df['Data_ID'].astype(str) == "222") # Hardcoded in script
                    if mask_leg.any():
                        for col in ADD_CAUTION_FLAG:
                            if col in df.columns:
                                fc = f"{col}_Flag"
                                if fc not in df.columns: df[fc] = ""
                                curr = df.loc[mask_leg, fc].fillna("").astype(str)
                                mask_has_c = curr.str.contains(r'\bC\b', regex=True)
                                # Only apply to those without C
                                mask_apply = mask_leg & (~mask_has_c)
                                if mask_apply.any():
                                     idx = df.index[mask_apply]
                                     curr_subset = df.loc[idx, fc].fillna("").astype(str)
                                     df.loc[idx, fc] = np.where(curr_subset == "", "C", curr_subset + ", C")

                # 6. Dependencies
                for dep in DEPENDENCY_CONFIG:
                   target = dep['target']
                   if target not in df.columns: continue
                   tfc = f"{target}_Flag"

                   mask_fail = pd.Series(False, index=df.index)
                   for src in dep['sources']:
                       if src not in df.columns: continue
                       sfc = f"{src}_Flag"
                       curr_s = df[sfc].fillna("").astype(str)
                       pat = "|".join([rf"\b{f}\b" for f in dep['trigger_flags']])
                       mask_fail = mask_fail | curr_s.str.contains(pat, regex=True)

                   if mask_fail.any():
                       curr = df.loc[mask_fail, tfc].fillna("").astype(str)
                       df.loc[mask_fail, tfc] = np.where(curr == "", dep['set_flag'], curr + ", " + dep['set_flag'])

                # 7. Pass Flags (P)
                for col in df.columns:
                    if col.endswith("_Flag"):
                        dcol = col[:-5]
                        if dcol in df.columns:
                            curr_f = df[col].fillna("").astype(str).str.strip().replace('nan', '')
                            vals = df[dcol]
                            mask_p = (curr_f == "") & (vals.notna()) & (vals.astype(str).str.strip() != '') & (vals.astype(str).str.lower() != 'nan')
                            if mask_p.any():
                                df.loc[mask_p, col] = 'P'

                return df

            if st.button("Run QA/QC Pipeline", type="primary"):
                f_path = os.path.join(output_dir, selected_file)

                with st.spinner("Running QA/QC..."):
                    try:
                        # Load
                        df_qc = pd.read_csv(f_path, low_memory=False)
                        
                        # Handle Units Row (if present)
                        # Check if first row is units (e.g. TIMESTAMP is 'TS')
                        if not df_qc.empty and 'TIMESTAMP' in df_qc.columns:
                            first_val = str(df_qc.iloc[0]['TIMESTAMP'])
                            if first_val == 'TS':
                                df_qc = df_qc.iloc[1:].reset_index(drop=True)
                                
                        if 'TIMESTAMP' in df_qc.columns:
                            df_qc['TIMESTAMP'] = pd.to_datetime(df_qc['TIMESTAMP'])

                        # Process
                        df_qc = run_qc_pipeline(df_qc)

                        # Reorder Columns (Interleave)
                        # Reorder Columns (Interleave)
                        ordered_cols = ['TIMESTAMP']
                        
                        # Handle RECORD and RECORD_Flag
                        if 'RECORD' in df_qc.columns:
                            ordered_cols.append('RECORD')
                            # Ensure flag exists (QC might have added LR, but if not, create empty)
                            if 'RECORD_Flag' not in df_qc.columns:
                                df_qc['RECORD_Flag'] = ""
                            ordered_cols.append('RECORD_Flag')
                            
                        meta_cols = ['Data_ID', 'Station_ID', 'Logger_ID', 'Logger_Script', 'Logger_Software']

                        # Identify data columns (everything else)
                        reserved = set(['TIMESTAMP', 'RECORD', 'RECORD_Flag']) | set(meta_cols) | set([c for c in df_qc.columns if c.endswith("_Flag")])
                        data_cols = [c for c in df_qc.columns if c not in reserved]
                        
                        for col in data_cols:
                            ordered_cols.append(col)
                            flag_col = f"{col}_Flag"
                            if flag_col in df_qc.columns:
                                ordered_cols.append(flag_col)
                                
                        # Add metadata columns at the END
                        for mc in meta_cols:
                            if mc in df_qc.columns: ordered_cols.append(mc)
                                
                        df_qc = df_qc[ordered_cols]

                        # Save
                        out_name = selected_file.replace("_tidy.csv", "_tidy_QC.csv")
                        if "_QC" not in out_name: # prevent double QCQC
                             out_name = selected_file.replace(".csv", "_QC.csv")

                        save_path = os.path.join(output_dir, out_name)
                        
                        # Use helper to include units row
                        write_csv_with_units(df_qc, save_path)

                        st.success("QA/QC Complete!")
                        st.success(f"Saved to: {save_path}")
                        st.dataframe(df_qc.head(50))

                    except Exception as e:
                        st.error(f"QA/QC Failed: {e}")
                        st.exception(e)

if __name__ == "__main__":
    main()
