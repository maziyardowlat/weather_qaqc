import pandas as pd
import modules.qaqc as qaqc
import numpy as np

def test_defaults():
    # 1. Test standard default application
    df = pd.DataFrame({
        "RH": [50, 105, -5, 100],  # 105 and -5 should be flagged
        "AirTC": [20, 20, 20, 20]
    })
    
    # Empty config for RH, should trigger defaults
    config = {
        "thresholds": {
            "RH": {"min": None, "max": None}
        }
    }
    
    print("Testing apply_qc with defaults...")
    df_result = qaqc.apply_qc(df, config)
    
    rh_flags = df_result["RH_Flag"]
    print("RH Flags:", rh_flags.tolist())
    
    assert "High" in rh_flags[1], "Index 1 should be flagged High"
    assert "Low" in rh_flags[2], "Index 2 should be flagged Low"
    assert rh_flags[0] == "", "Index 0 should be clean"
    
    print("✅ apply_qc verification passed!")

    # 2. Test Override still works
    config_override = {
        "thresholds": {
            "RH": {"min": 0, "max": 110} # Override max to 110
        }
    }
    print("Testing override...")
    df_override = qaqc.apply_qc(df, config_override)
    rh_flags_ov = df_override["RH_Flag"]
    print("RH Flags (Override):", rh_flags_ov.tolist())
    
    assert rh_flags_ov[1] == "", "Index 1 should NOT be flagged High with override"
    print("✅ Override verification passed!")

if __name__ == "__main__":
    test_defaults()
