import pandas as pd
import matplotlib.pyplot as plt

def plot_air_temp():
    print("Reading data...")
    # 1. Read the CSV file
    df = pd.read_csv("data/processed_air_temp.csv")
    
    # 2. Fix the Timestamp column so Python understands dates
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    colors = {
        'P': 'green',
        'M': 'red',
        'T': 'blue',
        'S': 'orange',
        'J': 'purple',
        'NC': 'cyan'
    }
    
    plt.figure(figsize=(12, 6))
    
    # For 'M' (Missing), replace NaN with -60 so they appear at the bottom
    mask_m = df['Flag'].astype(str).str.contains('M', na=False)
    # We use .copy() to avoid SettingWithCopy warning if applicable, though read_csv returns new df
    df.loc[mask_m, 'AirT_C_Avg'] = df.loc[mask_m, 'AirT_C_Avg'].fillna(-60)
    
    plt.grid(True, alpha=0.3)
    
    for flag_name, color_name in colors.items():
        
        # Find rows where the Flag column contains this letter
        # e.g. if looking for 'T', we find rows like "T" or "T, S"
        subset = df[df['Flag'].astype(str).str.contains(flag_name, na=False)]
        
        # Plot these pointsno
        plt.scatter(
            subset['TIMESTAMP'], 
            subset['AirT_C_Avg'], 
            color=color_name, 
            label=flag_name,
            s=15 # Size of dots
        )
        
    # 6. Add labels and save
    plt.title("Air Temperature with QA/QC Flags")
    plt.xlabel("Date")
    plt.ylabel("Temperature (C)")
    plt.legend(title="Flags")
    
    plt.savefig("air_temp_plot.png")
    print("Plot saved to air_temp_plot.png")

if __name__ == "__main__":
    plot_air_temp()