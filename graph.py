import pandas as pd
import matplotlib.pyplot as plt
import os

# File Paths
files = {
    "2023": "data/02FW005_raw_CR350_1379_20231102.csv",
    "2024": "data/02FW005_raw_CR350_1379_20240524.csv",
    "2025": "data/02FW005_raw_CR350_1379_20250521.csv"
}

def read_toa5(filepath):
    """Reads a TOA5 file skipping metadata rows (0, 2, 3)."""
    # TOA5 Structure:
    # 0: File Info
    # 1: Headers (Keep)
    # 2: Units
    # 3: Process Info
    try:
        df = pd.read_csv(filepath, skiprows=[0, 2, 3])
        # Clean columns
        df.columns = [c.strip() for c in df.columns]
        if 'TIMESTAMP' in df.columns:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def plot_2023(df):
    plt.figure(figsize=(10, 6))
    plt.plot(df['TIMESTAMP'], df['stmp1'], label='stmp1', alpha=0.7)
    plt.plot(df['TIMESTAMP'], df['stmp2'], label='stmp2', alpha=0.7)
    plt.title("2023 Data: stmp1 vs stmp2")
    plt.xlabel("Timestamp")
    plt.ylabel("Temperature")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_year_avg(df, year):
    plt.figure(figsize=(10, 6))
    plt.plot(df['TIMESTAMP'], df['stmp_Avg'], label='stmp_Avg', alpha=0.7)
    plt.plot(df['TIMESTAMP'], df['gtmp_Avg'], label='gtmp_Avg', alpha=0.7)
    plt.title(f"{year} Data: stmp_Avg vs gtmp_Avg")
    plt.xlabel("Timestamp")
    plt.ylabel("Temperature")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_combined(df23, df24, df25):
    plt.figure(figsize=(12, 8))
    
    # 2023
    plt.plot(df23['TIMESTAMP'], df23['stmp1'], label='2023 stmp1', linestyle='--', alpha=0.7)
    plt.plot(df23['TIMESTAMP'], df23['stmp2'], label='2023 stmp2', linestyle=':', alpha=0.7)
    
    # 2024
    plt.plot(df24['TIMESTAMP'], df24['stmp_Avg'], label='2024 stmp_Avg', alpha=0.7)
    plt.plot(df24['TIMESTAMP'], df24['gtmp_Avg'], label='2024 gtmp_Avg', alpha=0.7)

    # 2025
    plt.plot(df25['TIMESTAMP'], df25['stmp_Avg'], label='2025 stmp_Avg', alpha=0.7)
    plt.plot(df25['TIMESTAMP'], df25['gtmp_Avg'], label='2025 gtmp_Avg', alpha=0.7)

    plt.title("Combined Data (2023-2025)")
    plt.xlabel("Timestamp")
    plt.ylabel("Temperature")
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    print("Reading files...")
    df23 = read_toa5(files["2023"])
    df24 = read_toa5(files["2024"])
    df25 = read_toa5(files["2025"])

    if df23 is not None:
        print("Plotting 2023...")
        plot_2023(df23)
    
    if df24 is not None:
        print("Plotting 2024...")
        plot_year_avg(df24, "2024")

    if df25 is not None:
        print("Plotting 2025...")
        plot_year_avg(df25, "2025")

    if df23 is not None and df24 is not None and df25 is not None:
        print("Plotting Combined...")
        plot_combined(df23, df24, df25)

if __name__ == "__main__":
    main()
