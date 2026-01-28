import pandas as pd
import matplotlib.pyplot as plt

import argparse
import os

def plot_weather_data(input_file, target_col):
    print(f"Reading data from {input_file}...")
    # 1. Read the CSV file
    df = pd.read_csv(input_file)
    
    # 2. Fix the Timestamp column so Python understands dates
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    colors = {
        'P': (0, 0.5, 0, 0.3),
        'M': 'red',
        'T': 'blue',
        'S': 'orange',
        'J': 'purple',
        'NC': 'cyan'
    }
    
    plt.figure(figsize=(12, 6))
    
    # For 'M' (Missing), replace NaN with a value out of range -> actually we use vlines now
    # We can skip the fillna step since we are plotting lines separately
    
    plt.grid(True, alpha=0.3)
    
    # Calculate global min/max for vlines
    data_min = df[target_col].min()
    data_max = df[target_col].max()
    
    # Add some padding
    y_min = data_min - (data_max - data_min) * 0.1
    y_max = data_max + (data_max - data_min) * 0.1
    
    for flag_name, color_name in colors.items():
        subset = df[df['Flag'].astype(str).str.contains(flag_name, na=False)]
        
        if flag_name == 'M':
            # Plot vertical lines for Missing values
            if not subset.empty:
                plt.vlines(x=subset['TIMESTAMP'], ymin=y_min, ymax=y_max, colors=color_name, label=flag_name, alpha=0.5)
        else:
            # Plot scatter points for other flags
            if target_col in subset.columns:
               plt.scatter(
                   subset['TIMESTAMP'], 
                   subset[target_col], 
                   color=color_name, 
                   label=flag_name,
                   s=15
               )
        
    # 6. Add labels and save
    plt.title(f"{target_col} with QA/QC Flags")
    plt.xlabel("Date")
    plt.ylabel(target_col)
    plt.legend(title="Flags")
    
    # Generate output filename
    base_name = os.path.basename(input_file).replace('.csv', '')
    output_plot = f"{base_name}_plot.png"
    
    plt.savefig(output_plot)
    print(f"Plot saved to {output_plot}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot processed weather data.")
    parser.add_argument("--input", default="data/processed_air_temp.csv", help="Input CSV file")
    parser.add_argument("--col", default="AirT_C_Avg", help="Target column name")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found.")
        exit(1)
        
    plot_weather_data(args.input, args.col)