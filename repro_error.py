import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def reproduce():
    try:
        # Create a dataframe with a string column containing a NaN (float)
        data = {
            'TIMESTAMP': ['2023-01-01', '2023-01-02', np.nan, '2023-01-04'],
            'tmp': [10, 12, 11, 13]
        }
        df = pd.DataFrame(data)
        
        print("Attempting to plot string column with NaN...")
        plt.plot(df['TIMESTAMP'], df['tmp'])
        plt.show()
        print("Plot success (unexpected)")
        
    except Exception as e:
        print(f"Caught expected error: {e}")

if __name__ == "__main__":
    reproduce()
