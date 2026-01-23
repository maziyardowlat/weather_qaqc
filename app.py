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
        
        # Threshold Editor
        st.markdown("### QC Thresholds")
        st.info("Define the specific thresholds for each column. You can add new columns by clicking the '+' button or delete existing ones by selecting the row and hitting delete (or using the trash icon).")
        
        # Helper: Populate from File
        with st.expander("Auto-fill Columns from Reference File"):
            ref_file = st.file_uploader("Upload a sample raw file to extract column names", type=["csv", "dat"])
            if ref_file:
                # We need to parse just the header. 
                # Assuming standard TOA5 skip logic for now, or just reading the first few lines?
                # Let's try to use the parser with default skip defaults
                try:
                    df_ref, metadata, _, _ = parser.parse_toa5(ref_file)
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
            "Column Name": st.column_config.TextColumn(disabled=False),
            "Units": st.column_config.TextColumn(disabled=False),
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
#                    "logger": station_logger, # Removed from config
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
                    df_preview, metadata, header_info, proc_codes, err = parser.parse_toa5(uploaded_file, skip_rows=skip_rows)
                    if not err:
                        st.session_state['df_preview'] = df_preview
                        st.session_state['header_info'] = header_info
                        st.session_state['process_codes'] = proc_codes
                        st.session_state['file_meta'] = metadata
                        st.session_state['last_uploaded'] = uploaded_file.name
                    else:
                        st.error(f"Error reading file: {err}")

            if 'df_preview' in st.session_state:
                df_raw = st.session_state['df_preview']
                header_info = st.session_state.get('header_info', {})
                file_meta = st.session_state.get('file_meta', {})
                config = stations[selected_station]
                thresholds = config.get("thresholds", {})
                
                # Identify Mismatches
                expected_cols = [c for c in thresholds.keys()]
                file_cols = list(df_raw.columns)
                
                missing_cols = [c for c in expected_cols if c not in file_cols]
                
                column_mapping = {}
                unit_mapping = {} # Store user choice for units
                
                # Metadata Inputs (Data Ingestion)
                st.subheader("Data Metadata")
                col_meta1, col_meta2, col_meta3 = st.columns(3)
                with col_meta1:
                    logger_model_input = st.text_input("Logger Model", value=header_info.get("logger_model", "CR350"))
                with col_meta2:
                    logger_serial_input = st.text_input("Logger Serial", value=header_info.get("logger_serial", ""))
                with col_meta3:
                    data_id_input = st.text_input("Data ID", value=header_info.get("data_id", ""))
                
                # Update header info in session state if user edits (so export picks it up)
                header_info["logger_model"] = logger_model_input
                header_info["logger_serial"] = logger_serial_input
                header_info["data_id"] = data_id_input
                st.session_state['header_info'] = header_info
                
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
                
                # Unit Conflict Check
                # Check if mapped columns or existing columns have different units
                mismatched_units = []
                for col in expected_cols:
                    if col in missing_cols and col not in column_mapping:
                        continue # Skip unmapped
                    
                    # Determine source unit from file
                    file_col = column_mapping.get(col, col) # Use mapped name or same name
                    file_unit = file_meta.get(file_col, "")
                    
                    # Determine config unit
                    config_unit = thresholds.get(col, {}).get("unit", "")
                    
                    # Normalize? For now exact string match
                    if file_unit and config_unit and file_unit != config_unit:
                        mismatched_units.append((col, config_unit, file_unit))
                
                if mismatched_units:
                    st.warning(f"Unit Mismatch Detected for {len(mismatched_units)} columns.")
                    with st.expander("Resolve Unit Conflicts", expanded=True):
                        st.caption("Select which unit label to use for the final output.")
                        for col, c_unit, f_unit in mismatched_units:
                            # Dropdown
                            options = [f"Station Config: {c_unit}", f"File: {f_unit}"]
                            choice = st.selectbox(f"Unit for '{col}'", options, key=f"unit_{col}")
                            if "File:" in choice:
                                unit_mapping[col] = f_unit
                
                # Save unit decision to session state for export
                st.session_state['unit_mapping'] = unit_mapping

                # --- DUPLICATE & GAP ANALYSIS ---
                # We need to ensure TIMESTAMP is datetime for this check
                # It should have been converted in parser, but let's be safe
                ts_col = "TIMESTAMP"
                has_conflicts = False
                
                if ts_col in df_raw.columns:
                    # check for exact duplicates (drop silently/automatically?)
                    # User said: "if all values are identical... do not prompt"
                    n_exact = df_raw.duplicated().sum()
                    if n_exact > 0:
                        st.warning(f"Found {n_exact} exact duplicate rows. They will be automatically dropped.")
                        # We can drop them now for the "Run" stage context? 
                        # Actually we shouldn't modify df_raw in session state automatically without strict consent,
                        # but for flow, let's filter them for the view context.
                        # We will apply the drop inside the "Run" block to be destructive only then.
                    
                    # Check for Timestamp Collisions (Same TS, different content)
                    # We exclude exact duplicates from this check
                    df_dedup = df_raw.drop_duplicates()
                    dupe_mask = df_dedup.duplicated(subset=[ts_col], keep=False)
                    conflict_rows = df_dedup[dupe_mask]
                    
                    if not conflict_rows.empty:
                        has_conflicts = True
                        st.warning(f"⚠️ Found {len(conflict_rows)} rows with conflicting timestamps (Same time, different data).")
                        
                        # Show pairs
                        with st.expander("Review Conflicts", expanded=True):
                            st.write("We detected multiple rows for these timestamps.")
                            st.dataframe(conflict_rows.sort_values(by=ts_col))
                            
                            st.info("Resolution Policy: The QA/QC process will automatically keep the **first** occurrence and drop the duplicates.")
                            
                            resolve_conflicts = st.checkbox("I acknowledge these conflicts (Proceed with 'Keep First')", value=False)
                            
                            if resolve_conflicts:
                                has_conflicts = False # Override blocker
                
                # Run Button (Conditioned on mapping)
                if (not missing_cols or len(column_mapping) == len(missing_cols)) and not has_conflicts:
                    if st.button("Run QA/QC"):
                        with st.spinner("Processing..."):
                            # 1. Apply Mapping
                            if column_mapping:
                                rename_dict = {v: k for k, v in column_mapping.items()}
                                df_proc = df_raw.rename(columns=rename_dict)
                            else:
                                df_proc = df_raw.copy()
                            
                            # 2. Drop Exact Duplicates
                            df_proc = df_proc.drop_duplicates()
                            
                            # 3. Handle Conflicts (Keep First as per current logic default, or error?)
                            # User said "deletes 2nd row". So keep='first' works.
                            df_proc = df_proc.drop_duplicates(subset=[ts_col], keep='first')
                            
                            # 4. Gap Filling (15 MIN)
                            if ts_col in df_proc.columns:
                                df_proc[ts_col] = pd.to_datetime(df_proc[ts_col])
                                df_proc = df_proc.sort_values(ts_col)
                                
                                start_ts = df_proc[ts_col].iloc[0]
                                end_ts = df_proc[ts_col].iloc[-1]
                                
                                # Generate full range
                                full_range = pd.date_range(start=start_ts, end=end_ts, freq='15T')
                                
                                # Reindex
                                df_proc = df_proc.set_index(ts_col).reindex(full_range)
                                
                                # The reindex creates an index name shift suitable for reset
                                df_proc.index.name = ts_col
                                df_proc = df_proc.reset_index()
                                
                                # Now gaps are NaN rows
                            
                            # 5. Apply QA/QC
                            df_qc = qaqc.apply_qc(df_proc, config)

                        # Add Station ID Column
                        station_id = config.get("id", "")
                        if "Station_ID" not in df_qc.columns:
                            df_qc.insert(0, "Station_ID", station_id)
                        
                        # Add Logger Info from inputs
                        if "Logger_ID" not in df_qc.columns:
                            # User requested Logger Model and Serial. 
                            # Usually Logger_ID implies Serial, but request said:
                            # "logger type ... filled by row[0] 3 ... CR350x"
                            # "logger serial ... row[0] 4"
                            # Assuming we want both columns or just one?
                            # "Logger_ID" currently exists. 
                            # Let's add both.
                            df_qc.insert(1, "Logger_Model", logger_model_input)
                            df_qc.insert(2, "Logger_ID", logger_serial_input)
                        
                        # Add Data ID Column
                        if "Data_ID" not in df_qc.columns:
                             df_qc.insert(3, "Data_ID", data_id_input) # Insert after Logger_ID (index 2)
                            

                        
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
                         
                         # Add UTC Offset Column
                         # Logic: If we converted to UTC, offset is 0? Or is this describing the *original* offset?
                         # User said: "if we do not change it... -7 for every."
                         # "End timestamp being the tidy files timestamp (PDT)... see that it should be -7 as it is compared to UTC."
                         
                         # If Target is UTC:
                         if to_tz == "UTC":
                             df_export['UTC_offset'] = 0
                         else:
                             # Assume PDT fixed (-7)
                             df_export['UTC_offset'] = -7
                             
                         st.session_state["qc_export"] = df_export
                         st.success(f"Converted timestamps from {from_tz_label} to {to_tz}")
                     else:
                         st.warning("No TIMESTAMP column found.")
                
                # Checking 15 Minute Intervals (Post-Process Verification)
                # User asked for error message if rows are not 15 min apart.
                # Since we performed Gap Filling, this should be clean, but let's verify.
                df_check = st.session_state.get("qc_export", st.session_state["qc_result"])
                if 'TIMESTAMP' in df_check.columns and len(df_check) > 1:
                     # Calculate diff
                     ts_check = pd.to_datetime(df_check['TIMESTAMP'])
                     diffs = ts_check.diff().dropna()
                     # We expect 15 minutes (900 seconds)
                     # Allow small float tolerance?
                     bad_intervals = diffs[diffs != pd.Timedelta(minutes=15)]
                     
                     if not bad_intervals.empty:
                         st.warning(f"⚠️ Interval Check: Found {len(bad_intervals)} rows where the interval is not 15 minutes!")
                         # This might happen if gap filling failed or sorting was off.
                     else:
                         st.success("✅ Interval Check: All records are strictly 15 minutes apart.")

                # Download Button
                # Use converted DF if valid, else QC result
                df_to_download = st.session_state.get("qc_export", st.session_state["qc_result"])
                
                # Ensure UTC_offset exists if user didn't click Convert
                if 'UTC_offset' not in df_to_download.columns:
                     # Default to -7 (PDT)
                     df_to_download['UTC_offset'] = -7
                
                # Use format_tidy_csv with full metadata
                config = stations[selected_station]
                # Apply Unit Overrides
                unit_mapping = st.session_state.get('unit_mapping', {})
                if unit_mapping:
                    import copy
                    config = copy.deepcopy(config)
                    for col, unit in unit_mapping.items():
                        if col in config["thresholds"]:
                            config["thresholds"][col]["unit"] = unit
                
                header_info = st.session_state.get('header_info', {})
                proc_codes = st.session_state.get("process_codes", [])
                
                csv_data = utils.format_tidy_csv(df_to_download, station_config=config, header_info=header_info, process_codes=proc_codes)
                st.download_button(
                    label="Download Tidy CSV (TOA5 Standard)",
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
                    # Try to read TOA5 (skip rows) or regular CSV
                    # We can try reading with default settings first.
                    try:
                        # Try standard TOA5 skip
                        temp_df = pd.read_csv(f, skiprows=[0, 2, 3])
                    except:
                        # Fallback to normal
                        f.seek(0)
                        temp_df = pd.read_csv(f)
                        
                    dfs.append(temp_df)
                
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

