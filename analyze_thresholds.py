import pandas as pd
import sys
import argparse

def analyze_files(file_paths):
    # Initialize a dictionary to store stats across all files
    # Structure: {col_name: {'min': val, 'max': val, 'max_rate': val}}
    aggregated_stats = {}

    for file_path in file_paths:
        try:
            print(f"Processing {file_path}...", file=sys.stderr)
            # Read CSV
            # Header is on line 2 (index 1).
            # We want to skip line 1 (index 0, the TOA5 line).
            # Then the next lines (units, type) become data rows 0 and 1, which we drop.
            df = pd.read_csv(file_path, header=1, encoding='ISO-8859-1', low_memory=False)
            
            # Drop the first two rows (Units and Aggregation Type)
            df = df.iloc[2:].reset_index(drop=True)

            # Convert TIMESTAMP to datetime
            if 'TIMESTAMP' in df.columns:
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
                # Sort by timestamp just in case, for rate of change
                df = df.sort_values('TIMESTAMP')
            
            # Identify numeric columns (everything except TIMESTAMP and string cols)
            # We try to convert everything to numeric
            for col in df.columns:
                if col == 'TIMESTAMP':
                    continue
                
                # Convert to numeric, turning errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # If column is all NaN after conversion, skip it
                if df[col].dropna().empty:
                    continue

                # Calculate stats
                col_min = df[col].min()
                col_max = df[col].max()
                
                # Rate of change: difference between consecutive valid measurements
                # We assume the time step is consistent-ish, or just care about the step change
                diffs = df[col].diff().abs()
                max_rate = diffs.max()

                # Update aggregated stats
                if col not in aggregated_stats:
                    aggregated_stats[col] = {
                        'min': col_min,
                        'max': col_max,
                        'max_rate': max_rate
                    }
                else:
                    aggregated_stats[col]['min'] = min(aggregated_stats[col]['min'], col_min)
                    aggregated_stats[col]['max'] = max(aggregated_stats[col]['max'], col_max)
                    aggregated_stats[col]['max_rate'] = max(aggregated_stats[col]['max_rate'], max_rate)

        except Exception as e:
            print(f"Error processing {file_path}: {e}", file=sys.stderr)

    # Output results as Markdown table
    print("| Column | Min | Max | Max Rate of Change |")
    print("| :--- | :--- | :--- | :--- |")
    
    for col, stats in aggregated_stats.items():
        # Format numbers to be nice (e.g., 3 decimals)
        def fmt(x):
            return f"{x:.3f}" if pd.notnull(x) else "NaN"
            
        print(f"| {col} | {fmt(stats['min'])} | {fmt(stats['max'])} | {fmt(stats['max_rate'])} |")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze TOA5 weather data files for QA/QC thresholds.")
    parser.add_argument('files', metavar='F', type=str, nargs='+', help='CSV files to analyze')
    args = parser.parse_args()

    analyze_files(args.files)
