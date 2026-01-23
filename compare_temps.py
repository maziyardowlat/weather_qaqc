import pandas as pd
import matplotlib.pyplot as plt

# File Paths
files = {
    "2023": "data/02FW005_raw_CR350_1379_20231102.csv",
    "2024": "data/02FW005_raw_CR350_1379_20240524.csv",
    "2025": "data/02FW005_raw_CR350_1379_20250521.csv"
}

def read_toa5(filepath):
    """Reads a TOA5 file skipping metadata rows (0, 2, 3)."""
    try:
        df = pd.read_csv(filepath, skiprows=[0, 2, 3], low_memory=False)
        df.columns = [c.strip() for c in df.columns]
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def plot_combined(df23, df24, df25):
    plt.figure(figsize=(14, 8))
    
    # helper to ensure numeric
    def get_data(df, col):
        return pd.to_numeric(df[col], errors='coerce').values

    # 2023 Data
    if df23 is not None and 'tmp' in df23.columns:
        plt.plot(df23['TIMESTAMP'].values, get_data(df23, 'tmp'), label='2023 tmp (Air Temp?)', color='blue', alpha=0.6)
    if df23 is not None and 'tmp2' in df23.columns:
        plt.plot(df23['TIMESTAMP'].values, get_data(df23, 'tmp2'), label='2023 tmp2 (RH Temp?)', color='red', alpha=0.6)

    # 2024 Data
    if df24 is not None and 'AirT_C_Avg' in df24.columns:
        plt.plot(df24['TIMESTAMP'].values, get_data(df24, 'AirT_C_Avg'), label='2024 AirT_C_Avg', color='cyan', alpha=0.6)
    if df24 is not None and 'RHT_C_Avg' in df24.columns:
        plt.plot(df24['TIMESTAMP'].values, get_data(df24, 'RHT_C_Avg'), label='2024 RHT_C_Avg', color='orange', alpha=0.6)
        
    # 2025 Data
    if df25 is not None and 'AirT_C_Avg' in df25.columns:
        plt.plot(df25['TIMESTAMP'].values, get_data(df25, 'AirT_C_Avg'), label='2025 AirT_C_Avg', color='cyan', alpha=0.6)
    if df25 is not None and 'RHT_C_Avg' in df25.columns:
        plt.plot(df25['TIMESTAMP'].values, get_data(df25, 'RHT_C_Avg'), label='2025 RHT_C_Avg', color='orange', alpha=0.6)

    plt.title("Comparison of Temperature Columns across years")
    plt.xlabel("Timestamp")
    plt.ylabel("Temperature (C)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("temp_comparison.png")
    print("Plot saved to temp_comparison.png")

def main():
    print("Reading files...")
    df23 = read_toa5(files["2023"])
    df24 = read_toa5(files["2024"])
    df25 = read_toa5(files["2025"])

    plot_combined(df23, df24, df25)

if __name__ == "__main__":
    main()
