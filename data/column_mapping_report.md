Weather Station Data Column Mapping & QA/QC Criteria
====================================================

This document outlines the mapping between the 2023 short-hand headers and the
standardized 2024/2025 headers, along with their associated Quality Control
(QA/QC) criteria.

**Files Analyzed:** \* **2023:** `02FW005_raw_CR350_1379_20231102.csv` \*
**2024/2025:** `02FW005_raw_CR350_1379_20240524.csv` / `...20250521.csv` \*
**QA/QC Doc:** `QAQC Procedure.docx`

Column Mapping & QA/QC Table
----------------------------

| Index | 2023 Header    | 2024/2025 Header   | QA/QC Criteria (Flag "F" if...)         |
|-------|----------------|--------------------|-----------------------------------------|
| 1     | `TIMESTAMP`    | `TIMESTAMP`        | N/A                                     |
| 2     | `RECORD`       | `RECORD`           | N/A                                     |
| 3     | `battv`        | `BattV_Avg`        |                                         |
| 4     | `Ptmp`         | `PTemp_C_Avg`      | \< -50°C or \> +50°C                    |
| 5     | `stmp1`        | `stmp_Avg`         | \< -50°C or \> +50°C                    |
| 6     | `dsws`         | `SlrFD_W_Avg`      | Negative values; Non-zero at night      |
| 7     | `rtot`         | `Rain_mm_Tot`      | Extreme values; Maintenance artifacts   |
| 8     | `strike`       | `Strikes_Tot`      |                                         |
| 9     | `strikeD`      | `Dist_km_Avg`      |                                         |
| 10    | `wind`         | `WS_ms_Avg`        | Extended zeros (Frozen); Extreme values |
| 11    | `wdir`         | `WindDir`          | \< 0 or \> 360; or if WS is flagged     |
| 12    | `windM`        | `MaxWS_ms_Avg`     | Extreme values                          |
| 13    | `tmp`          | `AirT_C_Avg`       | \< -50°C or \> +50°C                    |
| 14    | `vap`          | `VP_mbar_Avg`      |                                         |
| 15    | `press`        | `BP_mbar_Avg`      | \< 700 mb or \> 1100 mb; Spikes         |
| 16    | `rh`           | `RH`               | \< 0% or \> 100%                        |
| 17    | `tmp2`         | `RHT_C_Avg`        | \< -50°C or \> +50°C                    |
| 18    | `tiltNS`       | `TiltNS_deg_Avg`   |                                         |
| 19    | `tiltWE`       | `TiltWE_deg_Avg`   |                                         |
| 20    | `dswt`         | `SlrTF_MJ_Tot`     | Negative values                         |
| 21    | `CVMeta`       | `CVMeta`           |                                         |
| 22    | `Invalid_Wind` | `Invalid_Wind_Avg` |                                         |
| 23    | `dt`           | `DT_Avg`           |                                         |
| 24    | `tcdt`         | `TCDT_Avg`         |                                         |
| 25    | `snod`         | `DBTCDT_Avg`       |                                         |
| 26    | `swin`         | `SWin_Avg`         | Negative values; Non-zero at night      |
| 27    | `swout`        | `SWout_Avg`        | Negative values; Non-zero at night      |
| 28    | `lwin`         | `LWin_Avg`         |                                         |
| 29    | `lwout`        | `LWout_Avg`        |                                         |
| 30    | `swnet`        | `SWnet_Avg`        |                                         |
| 31    | `lwnet`        | `LWnet_Avg`        |                                         |
| 32    | `swalbedo`     | `SWalbedo_Avg`     |                                         |
| 33    | `nr`           | `NR_Avg`           |                                         |
| 34    | `stmp2`        | `gtmp_Avg`         | \< -50°C or \> +50°C                    |

Structural Changes Notes
------------------------

1.  `stmp_Avg` **Insertion:** The 2024 format introduces `stmp_Avg`, while in
    the original we have stmp1. Does stmp_1 correspond with stmpAvg?

2.  stmp2 corresponds to gtmp_Avg? Is that true?

3.  `gtmp_Avg` **Mapping:** The old `stmp2` maps to `gtmp_Avg`.

4.  **General QC Rules:**

    -   **"NaN" Check:** All columns should be flagged if values are "NaN" (Not
        a Number).

    -   **Timestamps:** Ensure consistent PDT/PST handling (GMT-07:00).

5.  **Processing & Data Integrity Rules:**

    -   **Missing Values:** Add "M" flags for columns with missing values.

    -   **Gap Filling:** If a timestamp is missing entirely (gap in record),
        insert a row with the correct timestamp and fill all data values with
        "M".

    -   **Unused Column Removal:** Implement the ability to remove unused or
        non-data columns (e.g., `CVMeta` which is static text) to clean up the
        dataset.

    -   **Column Aliasing:** Allow for renaming selected columns to ensure
        consistency across different file years (e.g., mapping 2023 headers to
        the 2024 standard).
