import streamlit as st
import pandas as pd
import os
import modules.station_manager as station_manager
import modules.parser as parser
import modules.qaqc as qaqc
import modules.utils as utils

# Set page config
st.set_page_config(
    page_title="NHG Weather Data QA/QC",
    page_icon="🌦️",
    layout="wide"
)

# App Title
st.title("Northern Hydrometeorology Group (NHG) - Weather Data QA/QC Pipeline")

# Sidebar for Navigation
st.sidebar.title("Navigation")
app_mode = st.sidebar.selectbox("Choose the Mode",
    ["Instructions", "Station Configuration", "Data Processing", "Data Compilation"]
)

if app_mode == "Instructions":
    st.markdown("""
    ## Welcome
    This application allows you to processing weather station data using the NHG QA/QC standards.

    ### Workflow:
    1. **Station Configuration**: Set up your station metadata and define quality control thresholds (e.g., Min/Max Temp).
    2. **Data Processing**: Upload raw logger files (TOA5), run the QA/QC engine, and download the "Tidy" data.
    3. **Data Compilation**: Merge multiple processed files into a master dataset.
    """)

elif app_mode == "Station Configuration":
    st.header("Station Configuration")
    
    # Load stations
    stations = station_manager.load_stations()
    station_names = list(stations.keys())
    
    # UI to select or add new station
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_station = st.selectbox("Select Station", ["Create New..."] + station_names)
    
    new_station_name = ""
    if selected_station == "Create New...":
        with col2:
            new_station_name = st.text_input("Enter New Station Name")
    
    # Determine current working station
    current_station = new_station_name if selected_station == "Create New..." else selected_station
    
    if current_station:
        st.subheader(f"Settings for: {current_station}")
        
        # Get existing config or init empty
        existing_config = stations.get(current_station, {"id": "", "thresholds": {}})
        
        # Metadata
        station_id = st.text_input("Station ID", value=existing_config.get("id", ""))

        station_logger = st.text_input("Station Logger", value=existing_config.get("logger", ""))
        
        # Threshold Editor
        st.markdown("### QC Thresholds")
        st.info("Define the specific thresholds for each column in your raw file.")
        
        # Helper: Populate from File
        with st.expander("Auto-fill Columns from Reference File"):
            ref_file = st.file_uploader("Upload a sample raw file to extract column names", type=["csv", "dat"])
            if ref_file:
                # We need to parse just the header. 
                # Assuming standard TOA5 skip logic for now, or just reading the first few lines?
                # Let's try to use the parser with default skip defaults
                try:
                    df_ref, metadata, _ = parser.parse_toa5(ref_file)
                    if df_ref is not None:
                        ref_cols = [c for c in df_ref.columns if c not in ["TIMESTAMP", "RECORD"]]
                        # Merge into existing thresholds if not present
                        current_thresholds = existing_config.get("thresholds", {})
                        
                        added_count = 0
                        for c in ref_cols:
                            unit = metadata.get(c, "")
                            if c not in current_thresholds:
                                defaults = qaqc.DEFAULT_THRESHOLDS.get(c, {})
                                current_thresholds[c] = {
                                    "min": defaults.get("min"), 
                                    "max": defaults.get("max"), 
                                    "rate_of_change": None, 
                                    "unit": unit
                                }
                                added_count += 1
                            else:
                                if "unit" not in current_thresholds[c] or not current_thresholds[c]["unit"]:
                                    current_thresholds[c]["unit"] = unit
                        
                        existing_config["thresholds"] = current_thresholds
                        if added_count > 0:
                            st.success(f"Added {added_count} new columns from file.")
                        else:
                            st.success("Refreshed units for existing columns.")
                except Exception as e:
                    st.error(f"Error reading reference file: {e}")

        # Convert thresholds dict to dataframe for editing
        # Structure: Column | Units | Min | Max | Rate of Change
        thresholds_data = []
        for col, rules in existing_config.get("thresholds", {}).items():
            thresholds_data.append({
                "Column Name": col,
                "Units": rules.get("unit", ""),
                "Min": rules.get("min", None),
                "Max": rules.get("max", None),
                "Rate of Change (Spikes)": rules.get("rate_of_change", None)
            })
        
        # If empty, provide a template row
        if not thresholds_data:
            thresholds_data = [{"Column Name": "AirTC_Avg", "Units": "Deg C", "Min": -40.0, "Max": 40.0, "Rate of Change (Spikes)": 5.0}]
            
        df_thresholds = pd.DataFrame(thresholds_data)
        
        # Configure column settings
        column_config = {
            "Column Name": st.column_config.TextColumn(disabled=True),
            "Units": st.column_config.TextColumn(disabled=True),
        }
        
        edited_df = st.data_editor(
            df_thresholds, 
            num_rows="dynamic", 
            use_container_width=True,
            column_config=column_config,
            key=f"editor_{current_station}"
        )
        
        # Save Button
        if st.button("Save Configuration"):
            if not current_station:
                st.error("Station Name is required.")
            else:
                # Reconstruct config dict
                new_thresholds = {}
                for index, row in edited_df.iterrows():
                    col_name = row["Column Name"]
                    if col_name and pd.notna(col_name):
                        new_thresholds[col_name] = {
                            "unit": row["Units"],
                            "min": float(row["Min"]) if pd.notna(row["Min"]) else None,
                            "max": float(row["Max"]) if pd.notna(row["Max"]) else None,
                            "rate_of_change": float(row["Rate of Change (Spikes)"]) if pd.notna(row["Rate of Change (Spikes)"]) else None
                        }
                
                # Update main dict
                stations[current_station] = {
                    "id": station_id,
                    "logger": station_logger,
                    "thresholds": new_thresholds
                }
                
                station_manager.save_stations(stations)
                if selected_station == "Create New...":
                    st.rerun() # Refresh to show new station in list

elif app_mode == "Data Processing":
    st.header("Data Ingestion & QA/QC")
    
    # 1. Select Station
    stations = station_manager.load_stations()
    station_names = list(stations.keys())
    
    if not station_names:
        st.warning("No stations found. Please go to 'Station Configuration' to create one.")
    else:
        selected_station = st.selectbox("Select Station for QC", station_names)
        
        # 2. Upload File
        uploaded_file = st.file_uploader("Upload Raw Logger File (TOA5/CSV)", type=["csv", "dat"])
        
        # Advanced Parsing Options
        with st.expander("Advanced Parsing Options (Row Skipping)"):
            st.caption("Standard TOA5 files use 4 header lines. Default skips lines 0, 2, and 3 (0-indexed).")
            custom_skip_input = st.text_input("Rows to Skip (comma separated)", value="0, 2, 3")
            
            try:
                skip_rows = [int(x.strip()) for x in custom_skip_input.split(",") if x.strip().isdigit()]
            except:
                skip_rows = [0, 2, 3] # fallback
        
        if uploaded_file and selected_station:
            st.info(f"Processing File: {uploaded_file.name} for Station: {selected_station}")
            
            # 3. Parse and Run
            # Pre-load to check columns
            if 'df_preview' not in st.session_state or st.session_state.get('last_uploaded') != uploaded_file.name:
                 with st.spinner("Analyzing file structure..."):
                    df_pre, _, err = parser.parse_toa5(uploaded_file, skip_rows=skip_rows)
                    if not err:
                        st.session_state['df_preview'] = df_pre
                        st.session_state['last_uploaded'] = uploaded_file.name
                    else:
                        st.error(f"Error reading file: {err}")

            if 'df_preview' in st.session_state:
                df_raw = st.session_state['df_preview']
                config = stations[selected_station]
                thresholds = config.get("thresholds", {})
                
                # Identify Mismatches
                expected_cols = [c for c in thresholds.keys()]
                file_cols = list(df_raw.columns)
                
                missing_cols = [c for c in expected_cols if c not in file_cols]
                
                column_mapping = {}
                
                if missing_cols:
                    st.warning(f"Column Mismatch Detected! The following columns expected by the station config are missing in the file: {', '.join(missing_cols)}")
                    st.write("Please map the file columns to the expected station columns:")
                    
                    with st.expander("Column Mapping", expanded=True):
                        for missing in missing_cols:
                            # Suggest close match? For now just select box
                            # Filter out columns that are already matched perfectly
                            available_options = ["(Select Mapping)"] + [c for c in file_cols if c not in expected_cols] 
                            selected_map = st.selectbox(f"Map file column to '{missing}'", available_options, key=f"map_{missing}")
                            
                            if selected_map != "(Select Mapping)":
                                column_mapping[missing] = selected_map
                    
                    if len(column_mapping) < len(missing_cols):
                        st.error("Please map all missing columns to proceed.")
                
                # Run Button (Conditioned on mapping)
                if not missing_cols or len(column_mapping) == len(missing_cols):
                    if st.button("Run QA/QC"):
                        with st.spinner("Processing..."):
                            # Apply Mapping
                            if column_mapping:
                                # Invert mapping: {Expected: File} -> {File: Expected} for rename
                                rename_dict = {v: k for k, v in column_mapping.items()}
                                df_raw = df_raw.rename(columns=rename_dict)
                                st.info(f"Applied column mapping: {rename_dict}")
                            
                            # Apply QA/QC
                            df_qc = qaqc.apply_qc(df_raw, config)

                        # Add Station ID Column
                        station_id = config.get("id", "")
                        if "Station_ID" not in df_qc.columns:
                            df_qc.insert(0, "Station_ID", station_id)
                        
                        logger_id = config.get("logger", "")
                        if "Logger_ID" not in df_qc.columns:
                            df_qc.insert(1, "Logger_ID", logger_id)
                            

                        
                        # Store in session state for export (Phase 5)
                        st.session_state["qc_result"] = df_qc
                        st.session_state["qc_station"] = selected_station
                        
                        # Display Results
                        st.subheader("QC Preview")
                        
                        # Highlight flags?
                        # Let's filter to show only rows with flags for better visibility
                        flag_cols = [c for c in df_qc.columns if "_Flag" in c]
                        
                        if flag_cols:
                            # Create a filtering mask
                            # Row has flag if any flag column is not empty string
                            has_flag = df_qc[flag_cols].apply(lambda x: x.str.len() > 0).any(axis=1)
                            n_flagged = has_flag.sum()
                            
                            st.write(f"Found **{n_flagged}** records with flags.")
                            
                            tab1, tab2 = st.tabs(["Flagged Data", "Full Data"])
                            
                            with tab1:
                                if n_flagged > 0:
                                    st.dataframe(df_qc[has_flag])
                                else:
                                    st.write("No flags found! Data is clean based on current thresholds.")
                            
                            with tab2:
                                st.dataframe(df_qc)
                        else:
                            st.write("No configured thresholds yielded flag columns. Check Configuration.")
                            st.dataframe(df_qc)

            # Export Section (outside the run button scope, uses session state)
            if "qc_result" in st.session_state and st.session_state["qc_station"] == selected_station:
                st.markdown("---")
                st.subheader("Export Options")
                
                col_tz1, col_tz2 = st.columns(2)
                
                # Source Timezone Map (User requested specific fixed offsets)
                source_tz_map = {
                    "US/PDT": "Etc/GMT+7", # UTC-7
                    "US/PST": "Etc/GMT+8", # UTC-8
                    "UTC": "UTC"
                }

                with col_tz1:
                    from_tz_label = st.selectbox("Source Timezone", list(source_tz_map.keys()), index=0)
                    from_tz = source_tz_map[from_tz_label]
                    
                with col_tz2:
                    to_tz = st.selectbox("Target Timezone for Tidy File", ["UTC", "US/Pacific", "America/Vancouver"], index=0)
                
                if st.button("Convert Timezone"):
                     # Apply conversion to TIMESTAMP
                     df_export = st.session_state["qc_result"].copy()
                     if 'TIMESTAMP' in df_export.columns:
                         df_export['TIMESTAMP'] = utils.convert_timezone(df_export, 'TIMESTAMP', from_tz, to_tz)
                         st.session_state["qc_export"] = df_export
                         st.success(f"Converted timestamps from {from_tz_label} to {to_tz}")
                     else:
                         st.warning("No TIMESTAMP column found.")
                
                # Download Button
                # Use converted DF if valid, else QC result
                df_to_download = st.session_state.get("qc_export", st.session_state["qc_result"])
                
                csv_data = utils.format_tidy_csv(df_to_download)
                st.download_button(
                    label="Download Tidy CSV",
                    data=csv_data,
                    file_name=f"{selected_station}_tidy.csv",
                    mime="text/csv"
                )

elif app_mode == "Data Compilation":
    st.header("Data Compilation")
    st.write("Merge multiple Tidy files into a single master record.")
    
    uploaded_files = st.file_uploader("Upload Tidy CSV files", accept_multiple_files=True, type=["csv"])
    
    if uploaded_files:
        if st.button("Compile Files"):
            try:
                dfs = []
                for f in uploaded_files:
                    dfs.append(pd.read_csv(f))
                
                if dfs:
                    compiled_df = pd.concat(dfs, ignore_index=True)
                    
                    # Sort by Timestamp if present
                    if 'TIMESTAMP' in compiled_df.columns:
                        compiled_df['TIMESTAMP'] = pd.to_datetime(compiled_df['TIMESTAMP'])
                        compiled_df = compiled_df.sort_values('TIMESTAMP')
                    
                    st.success(f"Compiled {len(uploaded_files)} files showing {len(compiled_df)} total records.")
                    st.dataframe(compiled_df.head())
                    
                    # Download
                    csv_compiled = utils.format_tidy_csv(compiled_df)
                    st.download_button(
                        label="Download Compiled Master File",
                        data=csv_compiled,
                        file_name="Compiled_Master_Dataset.csv",
                        mime="text/csv"
                    )
            except Exception as e:
                st.error(f"Error compiling files: {e}")

