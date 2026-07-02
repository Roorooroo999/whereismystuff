# Where's My Stuff — Dashboard Changelog

**Project:** Where's My Stuff — Walmart Total Inventory Dashboard  
**Deployed on:** Posit Connect  
**Last Updated:** July 2, 2026

---

## Summary of Changes

### 1. Migrate In-Transit to DC KPI: STO → CDI_ATLAS (INTRANSIT_TO_DC)

**Commits:** `6579ad4`, `f3b5f9c`, `5fa729a`

**Problem:**  
The historical In-Transit to DC KPI was sourced from `STO_IN_TRANSIT_TO_DC_UNITS` (Store Transfer Orders), which only captured STO-originated in-transit inventory. The new source (`CDI_ATLAS_INV_ITEM_INVENTORY`) captures RDC + ICC + ACC in-transit, giving a more complete picture while excluding GDC.

**Changes:**
- `api/server.py` — All `metric_sql` variants updated: `STO_IN_TRANSIT_TO_DC_UNITS` → `INTRANSIT_TO_DC_UNITS`, `STO_IN_TRANSIT_TO_DC_COST` → `INTRANSIT_TO_DC_COST`
- `api/server.py` — `critical_cols` check updated: `'sto_to_dc'` → `'intransit_to_dc'`
- `dashboard/historical.html`, `inventory-buckets.html`, `unified_preview.html` — All `sto_to_dc` data keys replaced with `intransit_to_dc` (regex-safe, preserves `sto_to_dc_regional` etc.)
- `dashboard/historical.html` — Tab label, chart titles, table header: "STO to DC" → "In-Transit to DC"

**Backward Compatibility:**  
Added `_replace_intransit_with_sto()` fallback in `run_query` — if `INTRANSIT_TO_DC_UNITS` is not found in an older table, the query automatically retries with the old STO column names. No dashboard downtime.

---

### 2. Cube Calculation: WTAVG_CUBE → TOTAL_CUBE

**Commit:** `6579ad4`

**Problem:**  
HIST_COMBINED schema changed — cube is now stored as `TOTAL_CUBE` (pre-aggregated sum of `units × cube`). The old queries computed `SUM(units * WTAVG_CUBE)` per-row, which was incorrect for aggregated tables.

**Changes:**
- `api/server.py` — All cube metric SQL updated from `SUM(COALESCE(units,0) * COALESCE(WTAVG_CUBE,0))` pattern to `SUM(COALESCE(TOTAL_CUBE,0))` pattern across enterprise, SBU, dept, catg queries
- Frontend divides `TOTAL_CUBE` by units to display weighted average cube per item
- Added 2-stage BigQuery error fallback in `run_query`:
  1. First retry strips `TOTAL_CUBE` lines if BQ returns "Unrecognized name"
  2. Second retry swaps `INTRANSIT_TO_DC` → `STO_IN_TRANSIT_TO_DC` if needed

---

### 3. Fix HIST_INTRANSIT_DC Pipeline: ~58 Units → ~1M Units

**Commit:** `5fa729a` (pipeline SQL) + `f3b5f9c` (server fallback)

**Problem:**  
The BigQuery pipeline step `HIST_INTRANSIT_DC` was producing only 58 units instead of ~1M. Two root-cause bugs:

**Bug 1 — Wrong join key:**
```sql
-- BEFORE (wrong: IT_ITEM_NBR is ITEM_NBR, not MDS_FAM_ID)
cdi.IT_ITEM_NBR AS MDS_FAM_ID

-- AFTER (correct: use true MDS_FAM_ID from ITEM_CUR join)
itm.MDS_FAM_ID
```
Affected CTEs: `rdc_intransit`, `gdc_intransit`, `icc_acc_intransit`

**Bug 2 — NULL trap on LEFT JOIN:**
```sql
-- BEFORE (dropped all rows where OMNI or DC_DIM had no match)
WHERE OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
  AND DC_DIM.DC_TYPE_DESC IS NOT NULL

-- AFTER (NULL-safe: keeps unmatched rows, uses COALESCE for DC filter)
WHERE (OMNI.OMNI_SEG_DESC IS NULL OR OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES'))
  AND COALESCE(UPPER(DC_DIM.DC_TYPE_DESC), '') NOT LIKE '%ECOMM%'
  AND COALESCE(UPPER(DC_DIM.DC_TYPE_DESC), '') NOT LIKE '%FULFILLMENT%'
  AND COALESCE(UPPER(DC_DIM.DC_TYPE_DESC), '') NOT LIKE 'FC%'
```

**File:** `historical_inventory_full_pipeline.sql`

---

### 4. New Backroom Source in Pipeline

**Context:** Part of the new scheduled pipeline provided by the team.

**Change:**  
STEP 1.5 Backroom source updated from old table (`o0c046n_backroom_oh_inv_...`) to:
```
wmt-instockinventory-datamart.RS001_WM_AD_HOC.store_backroom_qty
```
Column: `TOTAL_BACKROOM_QTY`

**File:** `historical_inventory_full_pipeline.sql`

---

### 5. Health Endpoint — Last Error Tracking

**Commit:** `f3b5f9c`

**Change:**  
`/api/health` now returns `last_error` field showing the most recent BigQuery query failure. Useful for diagnosing schema mismatches without checking Posit logs directly.

```json
{
  "historical_cache": { "ready": true, "rows": 121109 },
  "last_error": null
}
```

---

### 6. On Order KPI Week — Derive from HIST_COMBINED

**Commit:** `4178851`

**Problem:**  
The On Order KPI tile was showing WK21 on Thursday because the client-side week calculation held the week as "incomplete" until Friday (Saturday–Thursday → WK N-1, Friday → WK N).

**Fix:**  
Replaced the hardcoded date arithmetic with `getAnchorWeek() % 100`, which reads the max `WM_YR_WK_NBR` directly from HIST_COMBINED data — the same source of truth used by all other inventory KPIs.

```javascript
// BEFORE: client-side date math (wrong on Thu)
const _calWk = Math.floor(_daysSinceFY26 / 7) + 1;
latestOrdWk = Math.max(1, _daysIntoWk === 6 ? _calWk : _calWk - 1);

// AFTER: from HIST_COMBINED max week
const latestOrdWk = getAnchorWeek() % 100;
```

**File:** `dashboard/inventory-buckets.html`

---

### 7. Label Cleanup — "STO to DC" → "In-Transit to DC"

**Commit:** `324e86e`

All user-visible text updated to reflect the CDI_ATLAS source:

| File | Location | Before | After |
|------|----------|--------|-------|
| `inventory-buckets.html` | Chart title | `DC In Transit (STO to DC) — YoY` | `DC In Transit — YoY` |
| `historical.html` | Tab nav | `STO to DC` | `In-Transit to DC` |
| `historical.html` | Chart title | `STO to DC — Total YoY Trend` | `In-Transit to DC — Total YoY Trend` |
| `historical.html` | Chart title | `STO to DC by SBU — YoY Comparison` | `In-Transit to DC by SBU — YoY Comparison` |
| `historical.html` | Chart title | `STO to DC by DC Type — YoY Comparison` | `In-Transit to DC by DC Type — YoY Comparison` |
| `historical.html` | Table header | `STO to DC` | `In-Transit to DC` |
| `historical.html` | Total formula | `... + STO + On Yard + ...` | `... + In-Transit to DC + On Yard + ...` |

---

## Deployment Notes

After any pipeline SQL change, the BigQuery scheduled query must be **re-run manually** before the dashboard reflects the new data. After Posit redeploys from git:

```javascript
// Force-refresh historical cache from browser DevTools console (while logged into Posit):
fetch('/content/5443b4be-549e-41a4-901c-d77727cc67ac/api/cache/refresh-historical?force=true', {method:'POST'}).then(r=>r.json()).then(console.log)
```

Expected response: `{status: 'refreshed', rows: ~121000, load_time_sec: ~3.6, forced: true}`

---

## Key Architecture Notes

- **HIST_COMBINED** (`R0C0JUG_WMUS_HIST_COMBINED`) — Category-level historical data, source of truth for week anchor
- **DC_LEVEL** (`R0C0JUG_WMUS_HIST_DC_LEVEL`) — DC-grain data for DC Drilldown tab only
- **CDI_ATLAS** sources: `US_SUPPLY_CHAIN_AMBIENT_RAW_VM` (RDC) + `US_SUPPLY_CHAIN_AMB_CC_IMPORTS_RAW_VM` (ICC/ACC) — GDC excluded
- **On Order** format: `12622` = FY126 (2026) WK22. HIST_COMBINED format: `WM_YR_WK_NBR = 202622`
- **`histWkToOrderWk()`** converts between the two: `(yr - 1900) * 100 + wk`
