# Where's My Stuff — Developer Guide
**Walmart Total Inventory Team** | Last Updated: June 8, 2026 | Tag: `v1.1.0`

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Structure](#file-structure)
4. [BigQuery Tables & Schemas](#bigquery-tables--schemas)
5. [API Endpoints](#api-endpoints)
6. [Caching Architecture](#caching-architecture)
7. [Dashboard Views](#dashboard-views)
8. [Local Development](#local-development)
9. [Deployment (Posit Connect)](#deployment-posit-connect)
10. [BigQuery SQL Scripts](#bigquery-sql-scripts)
11. [Common Issues & Fixes](#common-issues--fixes)
12. [Adding New Features](#adding-new-features)

---

## Overview

**Where's My Stuff** is a real-time inventory visibility dashboard for Walmart's Total Inventory Team. It shows current snapshot and historical trend data across all inventory channels.

| View | Description |
|------|-------------|
| **Current (LIVE)** | Today's inventory snapshot — refreshes hourly |
| **Historical (WEEKLY)** | 67+ weeks of trend data by channel, SBU, dept, category |
| **Buckets (PIPELINE)** | Supply chain flow view — On Order → On Yard → In DC → In Transit → FC → Backroom → On Floor |

**Stack:** Python FastAPI · Google BigQuery · Bootstrap 5 · Chart.js · Deployed on Posit Connect

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Posit Connect (Python FastAPI app)                     │
│                                                         │
│  api/server.py                                          │
│  ├── DataCache          ← WHERES_MY_STUFF_ROLLUP        │
│  ├── HistoricalCache    ← R0C0JUG_WMUS_HIST_COMBINED    │
│  └── OnOrderCache       ← R0C0JUG_WMUS_HIST_ONORDER     │
│                                                         │
│  dashboard/                                             │
│  ├── index.html         ← Current view (host page)      │
│  ├── historical.html    ← Historical view (iframe)      │
│  └── inventory-buckets.html  ← Buckets view (iframe)   │
└─────────────────────────────────────────────────────────┘
```

### Startup Sequence
1. Posit Connect starts Python FastAPI app
2. **DataCache** loads `WHERES_MY_STUFF_ROLLUP` from BigQuery (~5s)
3. **HistoricalCache** and **OnOrderCache** load in parallel background threads
   - Core queries (enterprise/SBU/dept) complete in ~10s → `is_ready = True`
   - Category queries (~110K / ~316K rows) load in background daemon thread (~40s)
   - On next restart: all data loads from disk parquet cache in ~2-5s (no BQ query)
4. Health check at `/api/health` returns `ready: true` once core queries finish

### Data Flow — Historical View
```
/api/historical  →  enterprise + by_sbu + by_dept  (lite response, ~500KB)
/api/historical/catg  →  by_catg  (lazy, ~5MB, fetched when user visits catg tab)
```

### Data Flow — Buckets View
```
/api/historical  →  same lite response as above
/api/on-order    →  enterprise + by_sbu + by_dept  (lite, no catg)
/api/historical/catg  →  preloaded in background on page open
/api/on-order/catg    →  lazy, only when catg filter selected
```

---

## File Structure

```
Total Inventory/
├── api/
│   ├── server.py              ← FastAPI app (main backend)
│   ├── __init__.py
│   ├── fetch_historical.py    ← Historical data fetch utilities
│   └── .cache/                ← Disk parquet cache (auto-created, not in git)
│       ├── enterprise.parquet
│       ├── sbu.parquet
│       ├── dept.parquet
│       ├── catg.parquet        ← Created after first BQ catg query
│       ├── metadata.json
│       ├── onorder_*.parquet   ← On-order disk cache
│       └── onorder_metadata.json
│
├── dashboard/
│   ├── index.html             ← Current view + view toggle host
│   ├── historical.html        ← Historical trend view (loaded in iframe)
│   └── inventory-buckets.html ← Buckets pipeline view (loaded in iframe)
│
├── manifest.json              ← Posit Connect deployment config
├── requirements.txt           ← Python dependencies
├── AGENTS.md                  ← Wibey AI coding assistant context
└── DEVELOPER_GUIDE.md         ← This file
```

### SQL Scripts (reference, not deployed)
| File | Purpose |
|------|---------|
| `historical_inventory_full_pipeline.sql` | Rebuilds `HIST_COMBINED` table |
| `08_dc_level_historical.sql` | Rebuilds `DC_LEVEL` table |
| `10_on_order_historical.sql` | On-order historical pipeline |
| `06_dashboard_queries.sql` | Current snapshot queries |

---

## BigQuery Tables & Schemas

**Project:** `wmt-execution-intel-prod`  **Dataset:** `WM_AD_HOC`

### `WHERES_MY_STUFF_ROLLUP` — Current Snapshot
Used by: `index.html` (Current view)

| Column | Description |
|--------|-------------|
| `BUS_DT` | Business date |
| `SBU` | Strategic Business Unit |
| `OMNI_DEPT_NBR` / `OMNI_DEPT_DESC` | Department |
| `OMNI_CATG_NBR` / `OMNI_CATG_DESC` | Category |
| `STORE_OH_UNITS` | Store On Hand |
| `ON_FLOOR_UNITS` | On Floor |
| `BACKROOM_UNITS` | Backroom |
| `IN_TRANSIT_UNITS` | In Transit |
| `DC_OH_UNITS` | DC On Hand |
| `DC_RESERVED_UNITS` | DC Reserved |
| `DC_LABELED_UNITS` | DC Labeled |
| `DC_UNLABELED_UNITS` | DC Unlabeled |
| `STO_IN_TRANSIT_TO_DC_UNITS` | STO to DC |
| `FC_OH_UNITS` | FC (eComm) OH |
| `ON_YARD_UNITS` | On Yard (RDC & FDC) |
| `TOTAL_NETWORK_UNITS` | Total Network |
| `TOTAL_INVENTORY_COST` | Total Cost |

---

### `R0C0JUG_WMUS_HIST_COMBINED` — Historical Weekly (67+ weeks)
Used by: `historical.html` and `inventory-buckets.html`

> ⚠️ Column names differ from `DC_LEVEL` table — use SQL aliases

| Column | SQL Alias | Description |
|--------|-----------|-------------|
| `BUS_DT` | `BUS_DT` | Week end date |
| `WM_YEAR` | `WM_YEAR` | Fiscal year |
| `WM_WEEK` | `WM_WEEK` | Fiscal week |
| `WM_YR_WK_NBR` | `WM_YR_WK_NBR` | YYYYWW format key |
| `SBU` | `SBU` | Division |
| `OMNI_DEPT_DESC` | `department` | Department name |
| `OMNI_CATG_DESC` | `category` | Category name |
| `STORE_OH_UNITS` | `store_oh` | Store OH |
| `ON_FLOOR_UNITS` | `on_floor` | On Floor |
| `BACKROOM_UNITS` | `backroom` | Backroom |
| `IN_TRANSIT_UNITS` | `in_transit` | In Transit |
| `DC_OH_REGIONAL_UNITS` | — | Pre-aggregated DC OH by type |
| `FC_OH_UNITS` | `fc_oh` | FC (eComm) |
| `ON_YARD_UNITS` | `on_yard` | On Yard |
| `STO_IN_TRANSIT_TO_DC_UNITS` | `sto_to_dc` | STO to DC |

> **Note:** This table does NOT have `DC_TYPE` column or `DC_LABELED_REGIONAL_UNITS` etc.

---

### `R0C0JUG_WMUS_HIST_ONORDER` — On-Order Historical
Used by: `inventory-buckets.html` (On Order tile)

| Column | Description |
|--------|-------------|
| `wm_week` | Fiscal week (format: `12618` = FY26 WK18) |
| `sbu` | Division |
| `OMNI_DEPT_NBR/DESC` | Department |
| `OMNI_CATG_NBR/DESC` | Category |
| `replen_ind` | REPLEN / NON-REPLEN |
| `channel_ind` | STORE / ECOMM |
| `dsd_ind` | DSD / NON-DSD |
| `units_ordered` | Units on order |
| `units_received` | Units received |
| `units_open` | Open units (ordered - received) |

> ⚠️ On-order week format is `12618` (5 digits), historical uses `202618` (6 digits). Conversion in frontend: `125xx → 2025xx`, `126xx → 2026xx`

---

## API Endpoints

### Current View (DataCache)
| Endpoint | Description |
|----------|-------------|
| `GET /api/summary` | Enterprise totals |
| `GET /api/by-sbu` | By SBU/division |
| `GET /api/by-department` | By department |
| `GET /api/by-dc-type` | By DC type |
| `GET /api/filters/departments` | Dept list for filters |
| `GET /api/filters/categories` | Catg list for filters |
| `GET /api/item/{item_id}` | Single item lookup |
| `GET /api/search/items` | Item search |

### Historical View (HistoricalCache)
| Endpoint | Description |
|----------|-------------|
| `GET /api/historical` | Lite response: enterprise + by_sbu + by_dept (no catg) |
| `GET /api/historical/catg` | Category data (lazy-loaded, ~5MB). Returns `{"loading": true}` if BQ query still running |
| `GET /api/dc-lookup` | DC-grain weekly trend by DC number |

### On-Order / Buckets (OnOrderCache)
| Endpoint | Description |
|----------|-------------|
| `GET /api/on-order` | Lite response: enterprise + by_sbu + by_dept |
| `GET /api/on-order/catg` | Category data (lazy-loaded). Returns `{"loading": true}` if pending |

### Health & Admin
| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Cache readiness, row counts, debug info |
| `POST /api/cache/refresh` | Force reload DataCache from BQ |
| `POST /api/cache/refresh-historical` | Force reload HistoricalCache from BQ |
| `POST /api/cache/refresh-onorder` | Force reload OnOrderCache from BQ |

### Dashboard Pages
| Endpoint | Description |
|----------|-------------|
| `GET /dashboard` | Current view (index.html) |
| `GET /dashboard/historical.html` | Historical view |
| `GET /dashboard/inventory-buckets.html` | Buckets view |

---

## Caching Architecture

### Three-Layer Cache

```
Layer 1: In-memory DataFrame (fastest)
    ↓ miss/stale
Layer 2: Disk parquet files at api/.cache/ (~2-5s load)
    ↓ miss/stale  
Layer 3: BigQuery query (~10-40s depending on table)
```

### Cache Classes (`api/server.py`)

| Class | Table | `is_ready` requires | Catg strategy |
|-------|-------|---------------------|---------------|
| `DataCache` | `WHERES_MY_STUFF_ROLLUP` | all data | N/A |
| `HistoricalCache` | `HIST_COMBINED` | enterprise + sbu + dept | Background thread |
| `OnOrderCache` | `HIST_ONORDER` | enterprise + sbu + dept | Background thread |

### Daily Auto-Refresh
- Scheduler checks current date every hour
- On date change: triggers fresh BQ reload for all three caches
- Disk parquet is stale-checked by date — will re-query BQ on new day

### Key Properties
```python
cache.is_ready      # True once core data is loaded (health check)
cache.catg_ready    # True once catg background thread finishes
cache.is_loading    # True during active BQ query
cache.loaded_date   # Date string of last successful load
```

---

## Dashboard Views

### Current View (`dashboard/index.html`)
- Hosts all three views via view toggle
- Loads Historical and Buckets in iframes (lazy, on first click)
- API base detection: strips `/dashboard/...` from pathname → `/content/<id>`
- Refreshes current data every 5 minutes via `setInterval`

### Historical View (`dashboard/historical.html`)
- Loaded as iframe from `index.html`
- Navbar hidden in iframe mode (`?iframe=1` not required — `window.self !== window.top` detected)
- Catg data lazy-loaded via `/api/historical/catg` when user clicks catg tab
- Retry logic: polls every 5s if server returns `{"loading": true}`

### Buckets View (`dashboard/inventory-buckets.html`)
- Loaded as iframe from `index.html` with `?iframe=1`
- `data-status` badge is in navbar — uses null-safe `setStatus()` helper (navbar removed in iframe)
- **Filter flow:**
  1. SBU selection → filters dept list → clears catg
  2. Dept selection → triggers `loadHistCatgBackground()` → populates catg once ready
  3. Catg selection → triggers `loadOnorderCatgBackground()` for on-order catg
- Hist catg preloaded immediately on page load (background, no blocking)
- `CACHED_BY_YEAR` uses `CACHED_BY_YEAR_KEY` (separate from `CACHED_FILTERS_KEY`) to prevent stale chart data

### Important JS Variables (Buckets)
| Variable | Purpose |
|----------|---------|
| `HIST_DATA` | Historical lite response |
| `ONORDER_DATA` | On-order lite response |
| `HIST_CATG_LOADED` | True once `/api/historical/catg` loaded |
| `ONORDER_CATG_LOADED` | True once `/api/on-order/catg` loaded |
| `CACHED_BY_YEAR` | Grouped weekly data by fiscal year |
| `CACHED_BY_YEAR_KEY` | Filter key for byYear cache (must be separate from `CACHED_FILTERS_KEY`) |
| `SELECTED_BUCKETS` | Set of active bucket KPI card filters |

---

## Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set BigQuery credentials
export GOOGLE_APPLICATION_CREDENTIALS=service-account.json
# OR
export GOOGLE_CREDENTIALS=$(cat service-account.json)

# 3. Start the server
python -m uvicorn api.server:app --host 127.0.0.1 --port 8001 --reload

# 4. Open dashboard
# http://127.0.0.1:8001/dashboard
```

> **Local dev note:** API base is `http://localhost:8001` when `window.location.protocol === 'file:'`

---

## Deployment (Posit Connect)

### Environment Variables (set in Posit Connect → Vars tab)
| Variable | Value |
|----------|-------|
| `PROJECT_ID` | `wmt-execution-intel-prod` |
| `HIST_PROJECT_ID` | `wmt-execution-intel-prod` |
| `DATASET` | `WM_AD_HOC` |
| `TABLE` | `WHERES_MY_STUFF_ROLLUP` |
| `HIST_TABLE` | `R0C0JUG_WMUS_HIST_COMBINED` |
| `DC_HIST_TABLE` | `R0C0JUG_WMUS_HIST_DC_LEVEL` |
| `ONORDER_TABLE` | `R0C0JUG_WMUS_HIST_ONORDER` |
| `GOOGLE_CREDENTIALS` | _(service account JSON, set as secret)_ |

### Deploy Steps
1. Commit and push changes to GitHub (`main` branch)
2. In Posit Connect: click **Update Now** (pulls latest from GitHub)
3. Click **Restart** after update completes
4. Verify at `/api/health` — both `historical_cache.ready` and `onorder_cache.ready` should be `true` within ~15s
5. Check Posit logs for `[HIST] Load finished. is_ready=True` and `[ONORDER] Core data ready`

### Git Tag Bookmarks
| Tag | Commit | Description |
|-----|--------|-------------|
| `v1.1.0` | `43e5f52` | Buckets fully working — filters, catg, fast load |

---

## BigQuery SQL Scripts

| Script | Purpose | Run when |
|--------|---------|----------|
| `historical_inventory_full_pipeline.sql` | Full rebuild of `HIST_COMBINED` | Weekly data is stale or schema changed |
| `08_dc_level_historical.sql` | Rebuild `DC_LEVEL` table | DC breakdown data needs refresh |
| `10_on_order_historical.sql` | On-order historical pipeline | On-order data pipeline changes |
| `06_dashboard_queries.sql` | Current snapshot queries reference | Debugging current view data |

> ⚠️ These take several minutes and affect the live dashboard during execution.

---

## Common Issues & Fixes

### Buckets page stuck on "Loading inventory pipeline data..."
**Cause:** Usually `statusEl` null crash (navbar removed in iframe) or cache not ready.  
**Fix (deployed):** `setStatus()` helper null-checks before writing. `is_ready` no longer requires slow catg queries.

### Historical catg shows no data
**Cause:** `/api/historical/catg` returns `{"loading": true}` if BQ catg query still running.  
**Check:** Posit logs for `[HIST] [BG] Catg ready:` message. Wait ~40s after restart on cold BQ load.  
**Subsequent restarts:** `catg.parquet` loaded from disk instantly.

### "Unrecognized name" BigQuery error
**Cause:** Wrong column name for the table being queried.  
**Fix:** `HIST_COMBINED` and `DC_LEVEL` have different column names — always use SQL aliases. See schema table above.

### Filters not updating charts in Buckets
**Cause (fixed):** `CACHED_BY_YEAR` used `CACHED_FILTERS_KEY` which is set by `buildBucketsData()` before the cache check, making the cache always hit.  
**Fix (deployed):** Separate `CACHED_BY_YEAR_KEY` variable.

### On-order data returns 503
**Normal behavior** when on-order cache is still loading. Frontend retries with graceful fallback (`by_sbu: [], by_dept: []`).

### Category filter empty when selecting dept
**Cause (fixed):** `HIST_DATA.by_catg = []` in lite response — catg must be lazy-loaded.  
**Fix (deployed):** `loadHistCatgBackground()` triggered on page load and on first dept selection.

---

## Adding New Features

### Add a new inventory channel column

1. **Verify column exists** in the relevant BQ table
2. **Add to SQL query** in `_do_load()` inside the appropriate cache class in `server.py`
3. **Add to `to_lite_response()`** (or `to_response()` if catg-level needed)
4. **Frontend:** Update `buildBucketsData()` or `renderDCDrilldown()` to include the new field
5. **Invalidate caches:** Set `self._cached_lite_response = None` and `self._cached_response = None` in `load()`

### Add a new API endpoint

```python
@app.get("/api/my-new-endpoint")
async def my_endpoint():
    if not hist_cache.is_ready:
        raise HTTPException(status_code=503, detail="Cache loading")
    # build response from cache DataFrames
    return SafeJSONResponse(content={...})
```

> Always use `SafeJSONResponse` — it's the only response type compatible with Posit Connect.

### Add a new dashboard tab

1. Add HTML tab/pane to the relevant `dashboard/*.html`
2. Add JS data-fetch logic (use lazy-loading pattern if data is large)
3. Register route in `api/server.py` if a new HTML file is added
4. Add file to `manifest.json` `files` section

---

## Contact & Ownership

| | |
|---|---|
| **Team** | Walmart Total Inventory Team |
| **Primary table owner** | r0c0jug |
| **GitHub repo** | `Roorooroo999/whereismystuff` |
| **Posit Connect** | Content ID 224, GUID `5443b4be-549e-41a4-901c-d77727cc67ac` |
| **Python version** | 3.11.10 |
| **Current tag** | `v1.1.0` (June 8, 2026) |
