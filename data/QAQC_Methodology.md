# QA/QC Methodology: Seasonal & Diurnal Thresholds

## Overview

This document defines the strategy for generating and applying Quality Control (QC) thresholds to weather station data.
The goal is to move beyond simple "all-time" min/max values and instead use a **Context-Aware** approach that adapts to **Season (Month)** and **Time of Day (Day/Night)**.

---

## 2. Flagging Standards (The Output)

We move beyond a simple "Pass/Fail" binary to a nuanced flagging system that distinguishes between definite errors and suspicious anomalies.

| Flag    | Label            | Condition           | Definition                                                        | Action                                  |
| :------ | :--------------- | :------------------ | :---------------------------------------------------------------- | :-------------------------------------- |
| **P**   | **Pass**         | `Min < Val < Max`   | Data is within all seasonal limits and physics checks.            | **Keep**                                |
| **LMT** | **Limit**        | `Soft < Val < Hard` | Violates a **Seasonal Soft Limit** (e.g., unusually hot for Jan). | **Flag & Keep** (Preserve for analysis) |
| **SPK** | **Spike**        | `Delta > Threshold` | **Rate of Change** violation (Jump too large for timestep).       | **Flag & Keep** (Preserve for analysis) |
| **F**   | **Fail**         | `Val > Hard`        | Violates **Hard Physics Limit** (e.g. > +50°C, < 0 Rad).          | **Nullify** (Set Value to NaN)          |
| **INF** | **Infinity**     | `Val == inf`        | Sensor reported Infinity.                                         | **Nullify** (Set Value to NaN)          |
| **NAN** | **Not a Number** | `Val == NaN`        | Sensor reported NaN (Error code).                                 | **Nullify** (Already NaN)               |
| **M**   | **Missing**      | `Timestamp Gap`     | No record exists for this timestamp.                              | **Nullify** (Insert NaN Row)            |
| **FLT** | **Flatline**     | `Var(6h) == 0`      | Value has remained exactly constant (0.00 change) for 6+ hours.   | **Flag & Keep** (Sensor likely stuck)   |

---

## 3. Hierarchy of Checks

We apply a "Swiss Cheese" model of QC, where data must pass multiple layers of filters.

### Level 1: Hard Limits (Physics & Sensor Specs)

_from Official CAMnet Procedures_
These are absolute "Sanity Checks". If a value exceeds these, the sensor is broken or the data is garbage.

- **Temperature**: -50°C to +50°C
- **Humidity**: 0% to 100%
- **Pressure**: 700 to 1100 mbar
- **Solar Radiation**: Cannot be negative (< 0)

### Level 2: Seasonal Limits (Monthly)

Weather is seasonal. A temperature of +15°C is valid in July but suspicious in January.

- **Method**: Group historical data by **Month (1–12)**.
- **Calculation**:
  - **Soft Min**: 1st Percentile of that month's history.
  - **Soft Max**: 99th Percentile of that month's history.
- **Outcome**: Generates 12 unique min/max pairs per variable.

### Level 3: Diurnal Limits (Day/Night)

Some variables depend entirely on the sun.

- **Solar Radiation (Sw_in / SlrFD)**:
  - **Nighttime check**: Values _must_ be near zero (allowing for small sensor noise, e.g., < 5 W/m²).
  - **Logic**: Calculate accurate Sunrise/Sunset times (using Latitude/Longitude and Date) to enforce "Zero at Night".

---

## 4. Timezone Standardization (UTC vs. Local)

**Critical Consideration:** The raw data is in **Pacific Daylight Time (PDT, GMT-7)**, but scientific best practice uses **UTC**.

**Impact on QA/QC:**

- **Monthly bins**: Converting to UTC shifts the "start of the month" by 7-8 hours. Negligible impact.
- **Diurnal (Day/Night) logic**: **High Impact**. "Noon" in PDT is ~20:00 in UTC.

**Strategy:**

1.  **Standardize Early**: Convert all timestamps to **UTC** immediately.
2.  **Solar Calculation**: "Night" is defined by **Solar Elevation < 0°** (Geometrically calculated), not by clock hours.

---

## 5. Adaptive "Rolling Window" Strategy

To solve "Unknown History", we use an **Adaptive Threshold** model.

**Concept:** Thresholds are **dynamic**, recalculated annually using a moving window (e.g., last 3 years) of "Good" data.

1.  **Bootstrap**: Use 2023–2025 data to create "Version 1" baselines (clamped by Hard Limits).
2.  **Continuous Learning**: As new valid data arrives, the model learns the true climate range.

---

## 6. Implementation Strategy

### Step A: Data Parsing & Segmentation

The analysis script will:

1.  **Ingest** years of raw CSV data.
2.  **Calculate Solar Time**: Determine if each timestamp is "Day" or "Night" based on the station location.
3.  **Bin Data**: Create groups like `Jan_Day`, `Jan_Night`, `Feb_Day`, `Feb_Night`, etc.

### Step B: Statistical Profiling

For each bin (Month), calculate:

- **P01** (1st Percentile) → _Proposed Soft Min_
- **P99** (99th Percentile) → _Proposed Soft Max_
- **Max Rate of Change** → _Sudden spike detection_

### Step C: Configuration Generation

Output a logical configuration structure:

```json
"AirTC_Avg": {
    "hard_limits": { "min": -50, "max": 50 },
    "seasonal": {
        "January": { "min": -38.5, "max": 4.2 },
        "February": { "min": -35.0, "max": 6.1 },
        ...
        "July": { "min": 5.5, "max": 32.0 }
    }
},
"Solar_Rad": {
    "night_max": 5.0,
    "seasonal_day_max": {
        "January": 400,
        "July": 1100
    }
}
```

## 3. Benefits

- **Precision**: Catches winter sensor failures that would pass a generic "Yearly Min" check.
- **Accuracy**: Enforces physical laws (no sun at midnight) without manual intervention.
- **Robustness**: Using percentiles (1%/99%) ignores the `infinity` errors currently present in the raw data, preventing them from corrupting our thresholds.
