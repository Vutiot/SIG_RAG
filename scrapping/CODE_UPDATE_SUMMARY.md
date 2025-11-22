# Code Update Summary: Year-Merged File Architecture

**Date:** 2025-11-22
**Status:** ✅ **COMPLETED**
**Last Update:** Removed basin suffix (_04) from filenames

---

## Overview

Updated the scraping system to maintain year-merged files (keeping NO3 and TURB separated) instead of creating yearly combined files. This follows the user's requirement: "keep no3 and turb separated but merge years."

---

## Code Changes

### 1. **main.py** - Modified `run_task_t6()` function

**File:** `/home/alex/Documents/code/SIG_RAG/scrapping/main.py`

**Lines Modified:** 614-704

**Key Changes:**

#### Added pandas import (line 20):
```python
import pandas as pd
```

#### Replaced combined file logic with parameter-separated logic:

**OLD BEHAVIOR** (lines 614-671):
- Combined NO3 and TURB data into single yearly files
- Filename pattern: `qualite_nappes_monthly_no3_turb_2024_04.parquet`
- Created one file per year containing both parameters

**NEW BEHAVIOR** (lines 614-704, updated 2025-11-22):
- Separates NO3 and TURB data into different files
- Filename pattern: `qualite_nappes_monthly_no3.parquet` and `qualite_nappes_monthly_turb.parquet`
- Appends new data to existing merged files
- Creates new files only if they don't exist
- No region suffix in filenames (cleaner naming)

#### New Logic Flow:

1. **Process each parameter separately** (lines 628-682):
   ```python
   for param in parameters_to_save:
       param_code = param['code']
       param_name = param['name']

       # Filter data for this specific parameter
       param_filtered = [
           row for row in year_results
           if row.get('code_param') == param_code
       ]
   ```

2. **Check if merged file exists** (line 648):
   ```python
   if merged_output_path.exists():
       # Append to existing file
   else:
       # Create new file
   ```

3. **Append to existing file** (lines 655-664):
   - Load existing parquet file
   - Create DataFrame from new data
   - Concatenate with pandas
   - Sort by date for consistency
   - Save back to same file

4. **Create new file** (lines 674-682):
   - Use existing `export_to_parquet()` function
   - Log creation with record count

#### Improved Logging:

**Append operation:**
```python
self.logger.info(
    f"Appending {param_name.upper()} data",
    year=year,
    new_records=param_count,
    total_records=len(combined_df),
    output=str(merged_output_path)
)
```

**New file creation:**
```python
self.logger.info(
    f"Created new {param_name.upper()} merged file",
    year=year,
    records=param_count,
    output=str(merged_output_path)
)
```

---

## File Naming Convention

### Before:
```
qualite_nappes_monthly_no3_turb_2015_04.parquet
qualite_nappes_monthly_no3_turb_2016_04.parquet
...
qualite_nappes_monthly_no3_turb_2025_04.parquet
```
- 11 combined files (one per year)
- Both parameters in each file

### After (Updated 2025-11-22):
```
qualite_nappes_monthly_no3.parquet       (all years, NO3 only)
qualite_nappes_monthly_turb.parquet      (all years, TURB only)
```
- 2 merged files total
- Parameters properly separated
- All years in one file per parameter
- **No basin suffix** (cleaner filenames)

---

## Current Data Status (Updated 2025-11-22)

### Merged Files:

**`qualite_nappes_monthly_no3.parquet`**
- Records: 14,622
- Parameter: 1340 (NO3) only
- Date range: 2015-01-05 to 2025-09-25
- Size: 824 KB
- ✅ Checksum verified

**`qualite_nappes_monthly_turb.parquet`**
- Records: 6,058
- Parameter: 1295 (Turbidity) only
- Date range: 2015-01-05 to 2025-09-25
- Size: 411 KB
- ✅ Checksum verified

**Total:** 20,680 records across 2 files (1.2 MB)

---

## Benefits

1. **Reduced file count:** 11 files → 2 files (82% reduction)
2. **Proper separation:** NO3 and TURB data completely separated
3. **Append capability:** New scrapes add to existing files instead of creating new ones
4. **Easier analysis:** Single file per parameter for entire time series
5. **Cleaner directory:** Less clutter in `exports/parquet/`

---

## Future Scrapes

When running new scrapes (e.g., for 2026 data):

1. System will fetch all parameters as before
2. Filter by parameter code (1340 for NO3, 1295 for TURB)
3. Check if merged files exist
4. **Append** new data to existing files
5. Sort by date automatically
6. Log both new record count and total record count

### Example Future Run (2026):
```
Appending NO3 data
  year=2026
  new_records=1,200
  total_records=15,822
  output=exports/parquet/qualite_nappes_monthly_no3.parquet

Appending TURB data
  year=2026
  new_records=500
  total_records=6,558
  output=exports/parquet/qualite_nappes_monthly_turb.parquet
```

---

## Testing

### Test Playbook Created:
**`playbook_test_append.json`**
- Tests append functionality with October 2025 data
- Confirms code handles existing files correctly

### Verification:
✅ Pandas import added
✅ Parameter separation logic implemented
✅ File exists check working
✅ Append logic in place
✅ Logging updated
✅ Existing merged files intact
✅ No old combined files remaining

---

## Backward Compatibility

**Breaking Change:** YES

Old playbooks that expect yearly combined files will now create year-merged separated files instead.

**Migration:** All old yearly files have been merged and removed. System is now fully migrated to new architecture.

---

## Related Scripts

### `utils/merge_years.py`
Created to perform one-time migration from yearly files to year-merged files:
1. Split combined `no3_turb` files into separate NO3 and TURB
2. Merge all years for each parameter
3. Remove old yearly files

**Status:** Migration completed successfully

### `utils/merge_parquet_files.py`
Created initially to merge NO3 and TURB together (superseded):
- No longer used
- Kept for reference

### `utils/cleanup_old_parquet.py`
Cleaned up old unfiltered parquet files:
- Removed 789 MB of old files
- 99.6% space savings achieved

---

## Summary

The scraping system now maintains a clean, efficient file structure with:
- **2 files** instead of 11+ yearly files
- **Parameter separation** (NO3 and TURB in separate files)
- **Year merging** (all years in one file per parameter)
- **Append capability** (new data appended, not creating new files)
- **Automatic sorting** (by date for consistency)
- **Clean filenames** (no basin suffix - simpler naming convention)

**Production Status:** ✅ Ready for production use

---

## Update History

### 2025-11-22 - Removed Basin Suffix from Filenames
- **Change:** Removed `_04` (basin code) suffix from merged filenames
- **Files Affected:**
  - `qualite_nappes_monthly_no3_04.parquet` → `qualite_nappes_monthly_no3.parquet`
  - `qualite_nappes_monthly_turb_04.parquet` → `qualite_nappes_monthly_turb.parquet`
- **Code Updated:** `main.py` line 646 - removed region_suffix from filename
- **Verification:** ✅ Checksum verified (14,622 NO3 + 6,058 TURB records)
- **Reason:** Cleaner, simpler filenames; basin code not needed in filename

### 2025-11-16 - Initial Year-Merged Architecture
- Implemented year-merged files with parameter separation
- Added pandas import for append functionality
- Created utilities: merge_years.py, cleanup_old_parquet.py

---

**Last Updated:** 2025-11-22 10:26 UTC
**Author:** Automated scraping system update
