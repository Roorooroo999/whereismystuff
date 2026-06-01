# Where's My Stuff - Troubleshooting Guide

## Table of Contents
1. [Data Architecture](#data-architecture)
2. [Common Issues & Fixes](#common-issues--fixes)
3. [Debugging Checklist](#debugging-checklist)
4. [Historical Lessons Learned](#historical-lessons-learned)

---

## Data Architecture

### BigQuery Tables

| Table | Purpose | Used By |
|-------|---------|---------|
| `WHERES_MY_STUFF_ROLLUP` | Current day inventory snapshot | Current Snapshot view (index.html) |
| `R0C0JUG_WMUS_HIST_COMBINED` | 67+ weeks historical data at Category level | Historical Trends (ALL tabs except DC Drilldown) |
| `R0C0JUG_WMUS_HIST_DC_LEVEL` | DC-grain historical data with DC_TYPE | Historical Trends → DC Drilldown tab ONLY |

### HIST_COMBINED Columns (Source: historical_inventory_full_pipeline.sql)

**Store Inventory:**
- `STORE_OH_UNITS`, `STORE_OH_COST`
- `BACKROOM_UNITS`
- `ON_FLOOR_UNITS` (calculated: STORE_OH - BACKROOM)
- `DC_RESERVED_UNITS`, `DC_RESERVED_COST`
- `IN_TRANSIT_UNITS`, `IN_TRANSIT_COST`

**DC Inventory:**
- `DC_OH_UNITS`, `DC_OH_COST`
- `DC_LABELED_UNITS`, `DC_LABELED_COST`
- `DC_UNLABELED_UNITS`, `DC_UNLABELED_COST`
- DC OH by type: `DC_OH_REGIONAL_UNITS`, `DC_OH_GROCERY_UNITS`, `DC_OH_FASHION_UNITS`, `DC_OH_IMPORTS_UNITS`, etc.

**STO (Stock Transfer Orders):**
- `STO_IN_TRANSIT_TO_DC_UNITS`, `STO_IN_TRANSIT_TO_DC_COST`
- STO by destination: `STO_TO_REGIONAL_UNITS`, `STO_TO_GROCERY_UNITS`, etc.

**FC (Fulfillment Center):** ⚠️ ONLY in HIST_COMBINED
- `FC_OH_UNITS`, `FC_OH_COST`

**On Yard (Trailers at DC):**
- `ON_YARD_UNITS`, `ON_YARD_COST`
- On Yard by type: `ON_YARD_REGIONAL_UNITS`, `ON_YARD_GROCERY_UNITS`, etc.

**Totals:**
- `TOTAL_NETWORK_UNITS`, `TOTAL_NETWORK_COST`

### DC_LEVEL Columns (Different from HIST_COMBINED!)

| DC_LEVEL Column | HIST_COMBINED Equivalent | Notes |
|-----------------|-------------------------|-------|
| `DEPARTMENT` | `OMNI_DEPT_DESC` | Different column name! |
| `CATEGORY` | `OMNI_CATG_DESC` | Different column name! |
| `STO_TO_DC_UNITS` | `STO_IN_TRANSIT_TO_DC_UNITS` | Different column name! |
| `DC_TYPE` | N/A | Used for CASE WHEN aggregation |
| ❌ No FC_OH | ✅ `FC_OH_UNITS` | FC only in HIST_COMBINED |

---

## Common Issues & Fixes

### Issue 1: FC (eComm) and On Yard Showing 0

**Symptoms:**
- FC (eComm) KPI tile shows 0
- On Yard KPI tile shows 0
- Other metrics (DC OH, STO to DC, etc.) load correctly

**Root Cause Options:**
1. **Data not in BigQuery** - Verify with:
   ```sql
   SELECT BUS_DT, SUM(FC_OH_UNITS) AS fc_oh, SUM(ON_YARD_UNITS) AS on_yard
   FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_COMBINED`
   WHERE BUS_DT >= '2026-05-01'
   GROUP BY BUS_DT ORDER BY BUS_DT DESC LIMIT 5;
   ```

2. **Server query missing columns** - Check `metric_sql` in server.py includes:
   ```sql
   SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh,
   SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
   ```

3. **Frontend not reading values** - Check browser console for:
   ```
   🔍 fc_oh: XXX (source: ...)
   🔍 on_yard: XXX (source: ...)
   ```

4. **Cache issue** - Check `/api/health` endpoint for:
   ```json
   "debug": {
     "fc_oh_exists": true,
     "fc_oh_sum": 62481302,
     "on_yard_exists": true,
     "on_yard_sum": 643906
   }
   ```

**Fix:** Ensure the `metric_sql` in server.py includes both columns, and frontend's `updateKPIs()` reads from `weeks` array which aggregates `by_sbu` data.

---

### Issue 2: DC Drilldown Category Shows 0 / Empty

**Symptoms:**
- DC Drilldown → By Category view shows "No increases found"
- Current: 0, Prior: 0, Change: 0
- SBU and Dept views work fine

**Root Cause Options:**
1. **Week mismatch** - DC_LEVEL table may lag 1 week behind HIST_COMBINED
   - HIST_COMBINED has Week 18
   - DC_LEVEL only has up to Week 17
   - Frontend looks for Week 18 in DC_LEVEL → not found → 0

2. **Null categories** - DC_LEVEL has NULL CATEGORY values creating malformed keys

**Fix (June 2026):**
- Frontend now finds the latest week AVAILABLE in each data source
- Server query filters `WHERE CATEGORY IS NOT NULL AND TRIM(CATEGORY) != ''`
- Frontend filters null categories before aggregation

---

### Issue 3: API Returns 503 "Data is loading"

**Symptoms:**
- Dashboard shows loading spinner indefinitely
- `/api/health` shows `"loading": true` stuck

**Root Cause:**
- `is_loading` flag not reset after error during cache load
- Try/finally pattern missing

**Fix:** Ensure load() uses try/finally:
```python
def load(self):
    self.is_loading = True
    try:
        self._do_load(...)
    finally:
        self.is_loading = False
```

---

### Issue 4: Scheduled Query Returns 0 Rows → Empty Table

**Symptoms:**
- Current Snapshot shows no data after midnight
- Scheduled query runs but WHERES_MY_STUFF_ROLLUP is empty

**Root Cause:**
- Scheduled query uses T-1 date logic
- `CREATE OR REPLACE TABLE` deletes existing data if query returns 0 rows
- If source data not ready at scheduled time, table becomes empty

**Fix:** Delay scheduled query by 30+ minutes, or add safety check:
```sql
-- Only replace if staging has rows
CREATE OR REPLACE TABLE ... AS
SELECT * FROM staging WHERE (SELECT COUNT(*) FROM staging) > 0
```

---

### Issue 5: Race Condition - Data Shows 0 on First Load, Works on Refresh

**Symptoms:**
- FC (eComm), On Yard, or DC Drilldown Category show 0 on first page load
- User refreshes the page → data appears correctly
- Intermittent: sometimes works, sometimes doesn't

**Root Cause (CRITICAL - discovered June 2026):**

The `is_ready` property was **incomplete**. It only checked 4 DataFrames:
```python
# OLD (BUGGY):
return (
    self.enterprise_df is not None
    and self.sbu_df is not None
    and self.dept_df is not None
    and self.catg_df is not None
    and not self.is_loading
)
```

**Missing:** `dc_drill_sbu_df`, `dc_drill_dept_df`, `dc_drill_catg_df`

**Timeline:**
1. Cache starts loading → `is_loading = True`
2. Enterprise, SBU, Dept, Category queries run (~25-30 seconds)
3. `is_loading = False`, all 4 main DFs populated → `is_ready = True` ❌
4. User requests `/api/historical` → Gets 200 OK (not 503)
5. BUT DC Drilldown queries are **still running** → DFs are `None`
6. Response includes empty DC Drilldown data → Frontend shows 0

**Fix (Applied June 2026):**
```python
# NEW (CORRECT):
return (
    self.enterprise_df is not None and not self.enterprise_df.empty
    and self.sbu_df is not None
    and self.dept_df is not None
    and self.catg_df is not None
    # CRITICAL: Also check DC Drilldown DataFrames!
    and self.dc_drill_sbu_df is not None
    and self.dc_drill_dept_df is not None
    and self.dc_drill_catg_df is not None
    and not self.is_loading
)
```

Now `/api/historical` returns 503 until ALL data (including DC Drilldown) is loaded.

---

## Debugging Checklist

### When FC/On Yard = 0:

1. ✅ **Verify BigQuery data exists:**
   ```sql
   SELECT SUM(FC_OH_UNITS), SUM(ON_YARD_UNITS) FROM HIST_COMBINED WHERE BUS_DT = '2026-05-29'
   ```

2. ✅ **Check /api/health endpoint:**
   - `fc_oh_exists: true`
   - `fc_oh_sum: > 0`
   - `on_yard_exists: true`
   - `on_yard_sum: > 0`

3. ✅ **Check browser console:**
   - Look for `🔍 fc_oh:` and `🔍 on_yard:` logs
   - Verify `source:` shows data origin

4. ✅ **Check by_sbu JSON response:**
   - Open Network tab → Filter by `/api/historical`
   - Check response → `by_sbu` array → verify `fc_oh` and `on_yard` keys exist

### When DC Drilldown Category = 0:

1. ✅ **Check console for week mismatch:**
   ```
   ⚠️ HIST_COMBINED week 2026-WK18 not in DC_LEVEL! Using DC_LEVEL latest: 2026-WK17
   ```

2. ✅ **Verify DC_LEVEL has data:**
   ```sql
   SELECT WM_YEAR, WM_WEEK, COUNT(*) FROM R0C0JUG_WMUS_HIST_DC_LEVEL
   GROUP BY WM_YEAR, WM_WEEK ORDER BY 1 DESC, 2 DESC LIMIT 5
   ```

3. ✅ **Check for null categories:**
   ```sql
   SELECT COUNT(*) FROM R0C0JUG_WMUS_HIST_DC_LEVEL WHERE CATEGORY IS NULL
   ```

---

## Historical Lessons Learned

### June 2026: FC/On Yard and DC Drilldown Category Failures

**Problem:** FC and On Yard KPIs repeatedly showed 0. DC Drilldown Category filter kept failing while Dept worked.

**Root Causes Identified:**
1. **Week Synchronization:** DC_LEVEL table (for DC Drilldown) updates on different schedule than HIST_COMBINED. When HIST_COMBINED had Week 18, DC_LEVEL only had Week 17. Frontend looked for Week 18 in DC_LEVEL → not found.

2. **Null Categories:** DC_LEVEL had rows with NULL CATEGORY values, creating malformed aggregation keys like `"WMT|FOOD|null"`.

3. **Enterprise Match Logic:** Original code only did exact BUS_DT match. If formats differed or data was missing, it returned 0 instead of using aggregated data.

**Fixes Applied:**
1. DC Drilldown now finds the latest week AVAILABLE in each data source (dc_drill_sbu, dc_drill_dept, dc_drill_catg)
2. Server filters NULL categories: `WHERE CATEGORY IS NOT NULL`
3. Frontend filters empty categories before aggregation
4. Enterprise matching uses multiple strategies with fallback to aggregated data

**Key Insight:** HIST_COMBINED has FC_OH_UNITS and ON_YARD_UNITS. DC_LEVEL does NOT have FC_OH_UNITS (FC is a separate channel, not DC-level).

---

## Quick Reference

### API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/api/health` | Cache status + fc_oh/on_yard debug info |
| `/api/historical` | Full historical data JSON |
| `/api/refresh` | Force cache reload |

### Console Debug Logs

| Log Pattern | Meaning |
|-------------|---------|
| `🔍 fc_oh: X (source: BUS_DT)` | FC value from enterprise exact match |
| `🔍 fc_oh: X (source: AGGREGATED)` | FC value from by_sbu aggregation |
| `⚠️ HIST_COMBINED week X not in DC_LEVEL` | Week sync issue |
| `DC Drilldown Catg source: dc_drill_catg` | Using DC_LEVEL table |
| `DC Drilldown Catg source: by_catg` | Fallback to HIST_COMBINED |

---

*Last Updated: June 1, 2026*
