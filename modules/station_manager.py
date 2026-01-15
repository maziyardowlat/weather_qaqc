import json
import os
import streamlit as st

CONFIG_FILE = "stations_config.json"

def load_stations():
    """Loads station configurations from the JSON file."""
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        st.error(f"Error reading {CONFIG_FILE}. It might be corrupted.")
        return {}

def save_stations(stations):
    """Saves the station dictionary to the JSON file."""
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(stations, f, indent=4)
        st.success("Station configuration saved successfully!")
    except Exception as e:
        st.error(f"Error saving configuration: {e}")

def get_station_names():
    """Returns a list of available station names."""
    stations = load_stations()
    return list(stations.keys())

def get_station_config(station_name):
    """Returns the config for a specific station."""
    stations = load_stations()
    return stations.get(station_name, {})
