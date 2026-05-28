# Where's My Stuff - Developer Guide

## Project Overview

**Where's My Stuff** is a real-time inventory visibility dashboard for Walmart's Total Inventory Team. It provides current snapshot and historical trend analysis across all inventory channels.

**Deployed on:** Posit Connect  
**Backend:** Python FastAPI with BigQuery  
**Frontend:** Bootstrap 5 + Chart.js  

---

## Data Architecture

### BigQuery Tables

| Table | Purpose | Used By |
|-------|---------|---------|
| `WHERES_MY_STUFF_ROLLUP` | Current day inventory snapshot | Current View (index.html) |
| `R0C0JUG_WMUS_HIST_COMBINED` | 67+ weeks historical data at Category level | Historical View (all tabs except DC Drilldown) |
| `R0C0JUG_WMUS_HIST_DC_LEVEL` | DC-grain historical data with DC_TYPE column | Historical View → DC Drilldown tab only |

**Project:** `wmt-execution-intel-prod`  
**Dataset:** `WM_AD_HOC`

### Environment Variables (manifest.json)

```json
{
  "PROJECT_ID": "wmt-execution-intel-prod",
  "HIST_PROJECT_ID": "wmt-execution-intel-prod",
  "DATASET": "WM_AD_HOC",
  "TABLE": "WHERES_MY_STUFF_ROLLUP",
  "HIST_TABLE": "R0C0JUG_WMUS_HIST_COMBINED",
  "DC_HIST_TABLE": "R0C0JUG_WMUS_HIST_DC_LEVEL"
}
```

---

## Table Schema Differences (CRITICAL)

### HIST_COMBINED vs DC_LEVEL - Column Naming

The two historical tables have **different column structures**. This has caused multiple bugs.

#### R0C0JUG_WMUS_HIST_COMBINED

| Column | Description |
|--------|-------------|
| `FC_OH_UNITS` | FC (eComm) inventory |
| `ON_YARD_UNITS` | On Yard inventory |
| `STO_IN_TRANSIT_TO_DC_UNITS` | STO to DC |
| `DC_OH_REGIONAL_UNITS` | Pre-aggregated DC OH by type |
| `DC_OH_GROCERY_UNITS` | Pre-aggregated DC OH by type |
| `STO_TO_REGIONAL_UNITS` | Pre-aggregated STO by type |
| `ON_YARD_REGIONAL_UNITS` | Pre-aggregated On Yard by type |

**Note:** HIST_COMBINED does **NOT** have `DC_LABELED_REGIONAL_UNITS`, `DC_UNLABELED_REGIONAL_UNITS`, etc.

#### R0C0JUG_WMUS_HIST_DC_LEVEL

| Column | Description |
|--------|-------------|
| `DC_TYPE` | Column for filtering (values: 'Regional', 'Grocery', 'Fashion', 'Imports', etc.) |
| `STO_TO_DC_UNITS` | STO to DC (NOT `STO_IN_TRANSIT_TO_DC_UNITS`) |
| `DC_OH_UNITS` | Total DC OH (no pre-aggregated type columns) |
| `DC_LABELED_UNITS` | Total DC Labeled (no pre-aggregated type columns) |
| `DC_UNLABELED_UNITS` | Total DC Unlabeled (no pre-aggregated type columns) |
| `ON_YARD_UNITS` | Total On Yard (no pre-aggregated type columns) |

**Note:** DC_LEVEL does **NOT** have pre-aggregated type columns. Use `CASE WHEN DC_TYPE = '...'` to derive breakdowns.

---

## Common Issues & Fixes

### Issue 1: "Unrecognized name" BigQuery Error

**Symptom:** Historical data fails to load with error like:
```
Unrecognized name: DC_LABELED_REGIONAL_UNITS; Did you mean DC_OH_REGIONAL_UNITS?
```

**Cause:** Querying columns that don't exist in the target table.

**Fix:** Ensure queries only use columns that exist in the specific table:
- For `HIST_COMBINED`: Use pre-aggregated columns like `DC_OH_REGIONAL_UNITS`
- For `DC_LEVEL`: Use `CASE WHEN DC_TYPE = 'Regional' THEN DC_OH_UNITS ELSE 0 END`

### Issue 2: DC Drilldown Returns No Data

**Symptom:** DC Drilldown tab shows empty tables for dept/category views.

**Cause:** Wrong column names in DC_LEVEL queries (e.g., `STO_IN_TRANSIT_TO_DC_UNITS` instead of `STO_TO_DC_UNITS`).

**Fix:** Use correct column names for DC_LEVEL table:
```sql
-- CORRECT for DC_LEVEL
SUM(COALESCE(STO_TO_DC_UNITS, 0)) AS sto_to_dc

-- WRONG for DC_LEVEL (this column doesn't exist)
SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0)) AS sto_to_dc
```

### Issue 3: FC/On Yard Showing 0

**Symptom:** FC (eComm) and On Yard tiles show 0 on Historical View.

**Possible Causes:**
1. Query failing due to missing columns (check Posit logs for errors)
2. Source tables don't have data
3. HIST_COMBINED table needs rebuilding

**Debug:** Check Posit Connect logs for:
```
[HIST] Enterprise fc_oh sum: XXX
[HIST] Enterprise on_yard sum: XXX
```

### Issue 4: Department Breakdown Missing Columns

**Symptom:** Department Breakdown table only shows 3 columns instead of 14.

**Fix:** Ensure `updateDeptTable()` in index.html includes all channels:
- Floor, Backroom, Store OH, DC OH, In-Transit, DC Reserved
- DC Labeled, DC Unlabeled, STO to DC, FC, On Yard
- Total Network, Total Cost

---

## Code Architecture

### Server-Side (api/server.py)

```
DataCache (Current View)
├── Loads from: WHERES_MY_STUFF_ROLLUP
├── Aggregation: Enterprise → SBU → Dept → Category
└── Refresh: Hourly + on date change

HistoricalCache (Historical View)
├── Main data from: HIST_COMBINED
│   ├── enterprise_df (enterprise level)
│   ├── sbu_df (SBU level)
│   ├── dept_df (department level)
│   └── catg_df (category level)
│
└── DC Drilldown from: DC_LEVEL (separate table!)
    ├── dc_drill_sbu_df
    ├── dc_drill_dept_df
    └── dc_drill_catg_df
```

### Frontend Data Flow

```
historical.html
├── Total Trend      → RAW_DATA.enterprise
├── SBU Comparison   → RAW_DATA.by_sbu
├── DC Type Trend    → RAW_DATA.by_sbu (aggregated by DC type columns)
├── STO to DC        → RAW_DATA.by_sbu
├── Data Table       → RAW_DATA.by_sbu
└── DC Drilldown     → RAW_DATA.dc_drill_sbu / dc_drill_dept / dc_drill_catg
                       (with fallback to by_sbu / by_dept / by_catg)
```

---

## Deployment Checklist

### Before Deploying

1. **Test locally** with correct Python environment:
   ```bash
   python -m uvicorn api.server:app --host 127.0.0.1 --port 8001 --reload
   ```

2. **Check for BigQuery column errors** in console output

3. **Verify all historical data loads**:
   - Check for `[HIST]` log lines
   - Verify row counts for enterprise, sbu, dept, catg, dc_drill_*

### Deploying to Posit Connect

1. **Commit changes:**
   ```bash
   git add -A
   git commit -m "description"
   ```

2. **Push to GitHub:**
   ```bash
   git push origin main
   ```

3. **In Posit Connect:**
   - Click "Update from Git repository"
   - Click "Restart" after update completes

4. **Verify deployment:**
   - Check `/api/health` endpoint
   - Verify `historical_cache.ready = true`
   - Check all tabs load data

---

## Adding New Columns

### To HIST_COMBINED queries:

1. Verify column exists in `R0C0JUG_WMUS_HIST_COMBINED`
2. Add to `metric_sql` in `HistoricalCache.load()`
3. Add to `to_response()` if needed
4. Update frontend to display

### To DC Drilldown queries:

1. Verify column exists in `R0C0JUG_WMUS_HIST_DC_LEVEL`
2. If column is at DC level (not pre-aggregated), use `CASE WHEN DC_TYPE`:
   ```sql
   SUM(CASE WHEN DC_TYPE = 'Regional' THEN column ELSE 0 END) AS column_regional
   ```
3. Add to `dc_drill_metric_sql` in `HistoricalCache.load()`
4. Update frontend `loadDCDrilldownData()` if needed

---

## Rebuilding Historical Tables

If source data changes, rebuild tables by running the SQL scripts in BigQuery:

1. `historical_inventory_full_pipeline.sql` → Rebuilds `HIST_COMBINED`
2. `08_dc_level_historical.sql` → Rebuilds `DC_LEVEL`

**Warning:** These queries can take several minutes and will affect the live dashboard during rebuild.

---

## Contact

**Team:** Walmart Total Inventory Team  
**Primary Table Owner:** r0c0jug

---

*Last Updated: May 28, 2026*
