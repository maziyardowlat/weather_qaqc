# QA/QC Comparison: Current Methodology vs. Jan 26 Workshop

This document compares our developed `QAQC_Methodology.md` against the requirements outlined in `QAQCWorkshop_Jan26.docx`.

## Summary

**Alignment Score: High (90%)**
Our methodology aligns perfectly with the Workshop's "Phase 2" goals, specifically utilizing **historical statistical thresholds** (Percentiles) to define flags. Our "Rolling Window" approach effectively implements their desire for a system that learns from history.

## Key Differences & Recommendations

### 1. Flagging Codes (Mapping)

The Workshop proposes a slightly more granular set of flags. We can map them as follows:

| Concept             | Our Flag           | Workshop Flag                    | Note                                                                                                                |
| :------------------ | :----------------- | :------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **Pass**            | `P`                | `P`                              | Perfect match.                                                                                                      |
| **Outlier (Limit)** | `LMT`              | `E` (Extremes) / `T` (Threshold) | Workshop splits "Extreme" (Historical bounds) vs "Threshold" (Hard bounds). We use `LMT` for soft and `F` for hard. |
| **Spike/Jump**      | `SPK`              | `S` (Spike) / `J` (Jump)         | Workshop distinguishes 3-point check (`S`) vs 2-point check (`J`). We combined them, but can split if needed.       |
| **Flatline**        | `FLT`              | `NC` (No Change)                 | Perfect match.                                                                                                      |
| **Physics Fail**    | `F`                | `ER` (Error)                     | Limits like >50m/s Wind.                                                                                            |
| **Missing**         | `M`                | `M`                              | Perfect match.                                                                                                      |
| **Zero/Night**      | `F` (Hard)         | `Z`                              | Solar Radiation checks.                                                                                             |
| **Wind Dir Null**   | _New Check Needed_ | `NV`                             | **Gap Identified**: We need a specific check: "If Wind Speed == 0, Wind Dir should be Null/Flagged".                |

### 2. Threshold Values (Hard Limits)

The Workshop document provides specific **Hard Values** that we should adopt immediately into our logic:

| Parameter             | Proposed Hard Limit | Rate of Change Limit (15min) |
| :-------------------- | :------------------ | :--------------------------- |
| **Temperature**       | -50°C to +50°C      | **±5°C**                     |
| **Wind Speed**        | Max 50 m/s          | _No limit_                   |
| **Relative Humidity** | 0% to 100%          | **±20%**                     |
| **Pressure**          | 850\* to 1050 hPa   | **10 hPa**                   |
| **Rainfall**          | -                   | **20 mm** (max per 15min)    |
| **Snow Depth**        | Max 5m              | **±100 mm**                  |

_> Note: The Workshop suggests using **99.99% and 0.01%** for historical percentile checks, whereas we proposed **99% and 1%**. Moving to 99.99% will result in fewer "False Alarms" (LMT flags), making the system looser but less nagging._

### 3. Missing Logic Identified

1.  **The "NV" Check**: We need to add logic to flag _Wind Direction_ if _Wind Speed_ is zero.
2.  **Snow Free (`SF`)**: The Workshop suggests a specific flag for "Snow Depth recorded during summer". Our "Seasonal Monthly" logic naturally covers this (e.g., August Snow Depth threshold will be near 0), so no extra flag is strongly needed, `LMT` covers it.

## Recommendation

1.  **Adopt the Values**: Update our configuration to use the concrete Rate-of-Change limits (e.g., 5°C/15min) from the workshop.
2.  **Adopt 99.9%**: Tighten our percentiles from 99% to 99.9% (or 99.99%) to match their "Extreme" definition better.
3.  **Add Wind Logic**: Implement the `Speed == 0 -> Dir = Null` rule.
