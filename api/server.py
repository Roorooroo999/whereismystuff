"""
Where's My Stuff - Inventory Rollup API
FastAPI backend with in-memory cache (single BigQuery load → instant responses)
Deployed on Google Cloud Run / Posit Connect
"""

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import json
import logging
import asyncio
import time
import tempfile
from datetime import datetime, timezone
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path

logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="Where's My Stuff API",
    description="Inventory Rollup Dashboard API - Walmart Total Inventory Team",
    version="3.0.0"
)

# Enable CORS for dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# No-cache middleware
class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        return response

app.add_middleware(NoCacheMiddleware)

# BigQuery config
# DATA_PROJECT = where the table lives; CLIENT_PROJECT = where the job runs (must have bigquery.jobs.create)
DATA_PROJECT = os.environ.get("DATA_PROJECT", "wmt-instockinventory-datamart")
CLIENT_PROJECT = os.environ.get("CLIENT_PROJECT", "wmt-execution-intel-prod")
PROJECT_ID = DATA_PROJECT  # for backward compat in table references
DATASET = os.environ.get("DATASET", "WM_AD_HOC")
TABLE = os.environ.get("TABLE", "WHERES_MY_STUFF_ROLLUP")
REFRESH_INTERVAL = int(os.environ.get("CACHE_REFRESH_SECONDS", "3600"))  # 1 hour default


# ============================================================
# In-Memory Cache
# ============================================================

class DataCache:
    """
    Loads ONE pre-aggregated BigQuery query at startup.
    All API endpoints filter/group from this cached DataFrame.
    Auto-refreshes every hour or when the date changes.
    """

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None       # main fact data
        self.catg_df: Optional[pd.DataFrame] = None   # dept→category mapping
        self.loaded_at: Optional[float] = None
        self.loaded_date: Optional[str] = None        # BUS_DT date string
        self.load_time_sec: float = 0
        self.row_count: int = 0
        self.is_loading: bool = False

    @property
    def is_ready(self) -> bool:
        return self.df is not None and not self.df.empty

    def _get_client(self):
        # Priority: GOOGLE_CREDENTIALS env var (JSON string) → GOOGLE_APPLICATION_CREDENTIALS file → ADC
        # Uses CLIENT_PROJECT for running jobs (needs bigquery.jobs.create permission)
        creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=CLIENT_PROJECT, credentials=credentials)
        return bigquery.Client(project=CLIENT_PROJECT)

    def load(self):
        """Load all data from BigQuery in ONE query, pre-aggregated at the finest filter grain."""
        self.is_loading = True
        t0 = time.time()
        client = self._get_client()

        # Main fact query: pre-aggregated at SBU × Dept × DC_Type × Replen_Type grain
        # This is enough to serve all 7 dashboard endpoints via Python groupby
        query = f"""
        SELECT
            OMNI_DIVISION AS sbu,
            OMNI_DEPT_NBR AS dept_nbr,
            OMNI_DEPT_DESC AS department,
            COALESCE(DC_TYPE_DESC, '__NONE__') AS dc_type,
            COALESCE(REPLEN_TYPE, '__NONE__') AS replen_type,
            COUNT(DISTINCT MDS_FAM_ID) AS item_count,
            SUM(STORE_OH_UNITS) AS store_units,
            SUM(DC_OH_UNITS) AS dc_oh_units,
            SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
            SUM(IN_TRANSIT_UNITS) AS transit_units,
            SUM(DC_LABELED_UNITS) AS dc_labeled_units,
            SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
            ROUND(SUM(STORE_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS store_cost,
            ROUND(SUM(DC_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_oh_cost,
            ROUND(SUM(DC_RESERVED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_reserved_cost,
            ROUND(SUM(IN_TRANSIT_UNITS * COALESCE(UNIT_COST, 0)), 2) AS transit_cost,
            ROUND(SUM(DC_LABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_labeled_cost,
            ROUND(SUM(DC_UNLABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_unlabeled_cost
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND OMNI_DEPT_DESC IS NOT NULL
        GROUP BY sbu, dept_nbr, department, dc_type, replen_type
        """

        logger.info("[CACHE] Loading main data from BigQuery...")
        df = client.query(query).to_dataframe()

        # Compute derived columns once
        df["total_units"] = (
            df["store_units"] + df["dc_oh_units"] + df["dc_reserved_units"]
            + df["transit_units"] + df["dc_labeled_units"] + df["dc_unlabeled_units"]
        )
        df["total_cost"] = (
            df["store_cost"] + df["dc_oh_cost"] + df["dc_reserved_cost"]
            + df["transit_cost"] + df["dc_labeled_cost"] + df["dc_unlabeled_cost"]
        )

        # Category mapping (separate lighter query)
        catg_query = f"""
        SELECT DISTINCT
            OMNI_DIVISION AS sbu,
            OMNI_DEPT_DESC AS department,
            OMNI_CATG_DESC AS category
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND OMNI_CATG_DESC IS NOT NULL
        ORDER BY department, category
        """
        logger.info("[CACHE] Loading category mappings...")
        catg_df = client.query(catg_query).to_dataframe()

        self.df = df
        self.catg_df = catg_df
        self.loaded_at = time.time()
        self.loaded_date = datetime.now().strftime("%Y-%m-%d")
        self.load_time_sec = round(time.time() - t0, 1)
        self.row_count = len(df)
        self.is_loading = False

        logger.info(f"[CACHE] Loaded {len(df):,} rows + {len(catg_df):,} category mappings in {self.load_time_sec}s")

    def _apply_filters(self, df: pd.DataFrame, sbu=None, department=None, category=None,
                        dc_type=None, replen_type=None, **kwargs) -> pd.DataFrame:
        """Apply dashboard filters to a DataFrame."""
        mask = pd.Series(True, index=df.index)
        if sbu:
            mask &= df["sbu"].isin(sbu)
        if department:
            mask &= df["department"].isin(department)
        if dc_type:
            mask &= df["dc_type"].isin(dc_type)
        if replen_type:
            mask &= df["replen_type"] == replen_type
        # category filter requires catg_df join — handled at endpoint level
        return df[mask]

    # ---------- Aggregation helpers ----------

    UNIT_COLS = ["store_units", "dc_oh_units", "dc_reserved_units", "transit_units",
                 "dc_labeled_units", "dc_unlabeled_units"]
    COST_COLS = ["store_cost", "dc_oh_cost", "dc_reserved_cost", "transit_cost",
                 "dc_labeled_cost", "dc_unlabeled_cost"]
    SUM_COLS = UNIT_COLS + COST_COLS + ["item_count", "total_units", "total_cost"]

    def get_summary(self, **filters) -> dict:
        filtered = self._apply_filters(self.df, **filters)
        s = filtered[self.SUM_COLS].sum()
        return {
            "total_items": int(s["item_count"]),
            "store_units": int(s["store_units"]),
            "dc_reserved_units": int(s["dc_reserved_units"]),
            "transit_units": int(s["transit_units"]),
            "dc_units": int(s["dc_oh_units"]),
            "dc_labeled_units": int(s["dc_labeled_units"]),
            "dc_unlabeled_units": int(s["dc_unlabeled_units"]),
            "total_units": int(s["total_units"]),
            "store_cost": round(float(s["store_cost"]), 2),
            "dc_reserved_cost": round(float(s["dc_reserved_cost"]), 2),
            "transit_cost": round(float(s["transit_cost"]), 2),
            "dc_cost": round(float(s["dc_oh_cost"]), 2),
            "dc_labeled_cost": round(float(s["dc_labeled_cost"]), 2),
            "dc_unlabeled_cost": round(float(s["dc_unlabeled_cost"]), 2),
            "total_cost": round(float(s["total_cost"]), 2),
        }

    def get_by_sbu(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "dc_reserved_units", "transit_units",
                "dc_oh_units", "dc_labeled_units", "dc_unlabeled_units",
                "total_units", "store_cost", "total_cost"]
        grouped = filtered.groupby("sbu", as_index=False)[cols].sum()
        grouped = grouped.sort_values("total_units", ascending=False)
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "sbu": r["sbu"],
                "item_count": int(r["item_count"]),
                "store_units": int(r["store_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "total_units": int(r["total_units"]),
                "store_cost": round(float(r["store_cost"]), 2),
                "total_cost": round(float(r["total_cost"]), 2),
            })
        return result

    def get_by_sbu_dept(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "dc_reserved_units", "transit_units",
                "dc_oh_units", "dc_labeled_units", "dc_unlabeled_units",
                "total_units", "total_cost"]
        grouped = filtered.groupby(["sbu", "dept_nbr", "department"], as_index=False)[cols].sum()
        grouped = grouped.sort_values(["sbu", "total_units"], ascending=[True, False])
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "sbu": r["sbu"],
                "dept_nbr": int(r["dept_nbr"]),
                "department": r["department"],
                "item_count": int(r["item_count"]),
                "store_units": int(r["store_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "total_units": int(r["total_units"]),
                "total_cost": round(float(r["total_cost"]), 2),
            })
        return result

    def get_by_dc_type(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        # Only rows that actually have a DC type
        filtered = filtered[filtered["dc_type"] != "__NONE__"]
        cols = ["item_count", "dc_oh_units", "dc_labeled_units", "dc_unlabeled_units",
                "dc_oh_cost", "dc_labeled_cost", "dc_unlabeled_cost"]
        grouped = filtered.groupby("dc_type", as_index=False)[cols].sum()
        grouped["total_dc_units"] = grouped["dc_oh_units"] + grouped["dc_labeled_units"] + grouped["dc_unlabeled_units"]
        grouped["total_dc_cost"] = grouped["dc_oh_cost"] + grouped["dc_labeled_cost"] + grouped["dc_unlabeled_cost"]
        grouped = grouped.sort_values("total_dc_units", ascending=False)
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "dc_type": r["dc_type"],
                "item_count": int(r["item_count"]),
                "dc_oh_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "total_dc_units": int(r["total_dc_units"]),
                "dc_oh_cost": round(float(r["dc_oh_cost"]), 2),
                "dc_labeled_cost": round(float(r["dc_labeled_cost"]), 2),
                "dc_unlabeled_cost": round(float(r["dc_unlabeled_cost"]), 2),
                "total_dc_cost": round(float(r["total_dc_cost"]), 2),
            })
        return result

    def get_by_dc_type_sbu(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        filtered = filtered[filtered["dc_type"] != "__NONE__"]
        cols = ["item_count", "dc_oh_units", "dc_labeled_units", "dc_unlabeled_units",
                "dc_oh_cost", "dc_labeled_cost", "dc_unlabeled_cost"]
        grouped = filtered.groupby(["dc_type", "sbu"], as_index=False)[cols].sum()
        grouped["total_dc_units"] = grouped["dc_oh_units"] + grouped["dc_labeled_units"] + grouped["dc_unlabeled_units"]
        grouped["total_dc_cost"] = grouped["dc_oh_cost"] + grouped["dc_labeled_cost"] + grouped["dc_unlabeled_cost"]
        grouped = grouped.sort_values(["dc_type", "total_dc_units"], ascending=[True, False])
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "dc_type": r["dc_type"],
                "sbu": r["sbu"],
                "item_count": int(r["item_count"]),
                "dc_oh_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "total_dc_units": int(r["total_dc_units"]),
                "total_dc_cost": round(float(r["total_dc_cost"]), 2),
            })
        return result

    def get_by_department(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "dc_oh_units", "transit_units",
                "dc_reserved_units", "dc_labeled_units", "dc_unlabeled_units",
                "total_units", "total_cost"]
        grouped = filtered.groupby(["department", "dept_nbr"], as_index=False)[cols].sum()
        grouped = grouped.sort_values("total_units", ascending=False)
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "department": r["department"],
                "dept_nbr": int(r["dept_nbr"]),
                "item_count": int(r["item_count"]),
                "store_units": int(r["store_units"]),
                "dc_oh_units": int(r["dc_oh_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "total_units": int(r["total_units"]),
                "total_cost": round(float(r["total_cost"]), 2),
            })
        return result

    def get_dept_categories(self) -> list:
        if self.catg_df is None:
            return []
        result = []
        for _, r in self.catg_df.iterrows():
            result.append({"sbu": r["sbu"], "department": r["department"], "category": r["category"]})
        return result


# Global cache instance
cache = DataCache()


# ============================================================
# Background auto-refresh
# ============================================================

async def cache_refresh_loop():
    """Background task: refresh cache every REFRESH_INTERVAL seconds or when date changes."""
    while True:
        try:
            await asyncio.sleep(REFRESH_INTERVAL)
            current_date = datetime.now().strftime("%Y-%m-%d")
            if cache.loaded_date != current_date or not cache.is_ready:
                logger.info(f"[CACHE] Auto-refresh triggered (date={current_date}, last={cache.loaded_date})")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, cache.load)
        except Exception as e:
            logger.error(f"[CACHE] Auto-refresh failed: {e}")


@app.on_event("startup")
async def startup_event():
    """Load cache on server startup, then start background refresh loop."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cache.load)
    except Exception as e:
        logger.error(f"[CACHE] Initial load failed: {e}")
    asyncio.create_task(cache_refresh_loop())


# ============================================================
# Response Models
# ============================================================

class InventorySummary(BaseModel):
    total_items: int
    store_units: int
    dc_reserved_units: int
    transit_units: int
    dc_units: int
    dc_labeled_units: int
    dc_unlabeled_units: int
    total_units: int
    store_cost: float
    dc_reserved_cost: float
    transit_cost: float
    dc_cost: float
    dc_labeled_cost: float
    dc_unlabeled_cost: float
    total_cost: float


class SBUData(BaseModel):
    sbu: str
    item_count: int
    store_units: int
    dc_reserved_units: int
    transit_units: int
    dc_units: int
    dc_labeled_units: int
    dc_unlabeled_units: int
    total_units: int
    store_cost: float
    total_cost: float


class ItemDetail(BaseModel):
    mds_fam_id: int
    item_nbr: Optional[int]
    omni_seg_desc: str
    omni_division: str
    omni_dept_desc: str
    replen_type: str
    store_oh_units: int
    dc_reserved_units: int
    transit_units: int
    dc_oh_units: int
    dc_labeled_units: int
    dc_unlabeled_units: int
    total_units: int
    unit_cost: Optional[float]
    total_cost: Optional[float]


# ============================================================
# Helper: parse common filter params
# ============================================================

def parse_filters(sbu=None, division=None, department=None, category=None,
                  dc_type=None, replen_type=None, item_id=None) -> dict:
    """Normalize filter params into a dict for the cache."""
    filters = {}
    if sbu:
        filters["sbu"] = sbu
    if division:
        filters["sbu"] = division  # division maps to sbu column
    if department:
        filters["department"] = department
    if dc_type:
        filters["dc_type"] = dc_type
    if replen_type:
        filters["replen_type"] = replen_type
    # category and item_id are special — not in the cached grain
    return filters


def require_cache():
    if not cache.is_ready:
        raise HTTPException(status_code=503, detail="Data is loading, please retry in a few seconds")


# ============================================================
# API Endpoints (all served from in-memory cache)
# ============================================================

@app.get("/")
async def root():
    return {"status": "ok", "service": "Where's My Stuff API", "cache_ready": cache.is_ready}


@app.get("/api/summary")
async def get_summary(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_summary(**filters)


@app.get("/api/by-sbu")
async def get_by_sbu(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_by_sbu(**filters)


@app.get("/api/by-sbu-dept")
async def get_by_sbu_dept(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_by_sbu_dept(**filters)


@app.get("/api/by-dc-type")
async def get_by_dc_type(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_by_dc_type(**filters)


@app.get("/api/by-dc-type-sbu")
async def get_by_dc_type_sbu(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_by_dc_type_sbu(**filters)


@app.get("/api/by-department")
async def get_by_department(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division, department=department,
                            dc_type=dc_type, replen_type=replen_type)
    return cache.get_by_department(**filters)


@app.get("/api/dept-categories")
async def get_dept_categories():
    require_cache()
    return cache.get_dept_categories()


# Legacy endpoints (still work, served from cache)
@app.get("/api/filters/departments")
async def get_departments(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None)
):
    require_cache()
    df = cache.df
    if sbu:
        df = df[df["sbu"].isin(sbu)]
    if division:
        df = df[df["sbu"].isin(division)]
    depts = sorted(df["department"].dropna().unique().tolist())
    return depts


@app.get("/api/filters/categories")
async def get_categories(
    sbu: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None)
):
    require_cache()
    df = cache.catg_df
    if sbu:
        df = df[df["sbu"].isin(sbu)]
    if department:
        df = df[df["department"].isin(department)]
    cats = df[["category"]].drop_duplicates().sort_values("category")
    return [{"category_nbr": 0, "category": c} for c in cats["category"].tolist()]


@app.get("/api/by-division")
async def get_by_division(
    sbu: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, replen_type=replen_type)
    filtered = cache._apply_filters(cache.df, **filters)
    grouped = filtered.groupby("sbu", as_index=False)["total_units"].sum()
    grouped = grouped.sort_values("total_units", ascending=False)
    return [{"division": r["sbu"], "total_units": int(r["total_units"])} for _, r in grouped.iterrows()]


@app.get("/api/by-replen")
async def get_by_replen(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None)
):
    require_cache()
    filters = parse_filters(sbu=sbu, division=division)
    filtered = cache._apply_filters(cache.df, **filters)
    filtered = filtered[filtered["replen_type"] != "__NONE__"]
    cols = ["store_units", "dc_reserved_units", "transit_units",
            "dc_oh_units", "dc_labeled_units", "dc_unlabeled_units"]
    grouped = filtered.groupby("replen_type", as_index=False)[cols].sum()
    result = []
    for _, r in grouped.iterrows():
        result.append({
            "REPLEN_TYPE": r["replen_type"],
            "store_units": int(r["store_units"]),
            "dc_reserved_units": int(r["dc_reserved_units"]),
            "transit_units": int(r["transit_units"]),
            "dc_units": int(r["dc_oh_units"]),
            "dc_labeled_units": int(r["dc_labeled_units"]),
            "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
        })
    return result


# Item-level lookup still hits BigQuery directly (rare, specific item search)
@app.get("/api/item/{item_id}")
async def get_item(item_id: str):
    client = cache._get_client()
    query = f"""
    SELECT
        MDS_FAM_ID AS mds_fam_id, ITEM_NBR AS item_nbr,
        OMNI_SEG_DESC AS omni_seg_desc, OMNI_DIVISION AS omni_division,
        OMNI_DEPT_DESC AS omni_dept_desc, REPLEN_TYPE AS replen_type,
        SUM(STORE_OH_UNITS) AS store_oh_units, SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units, SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units, SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS
            + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units,
        MAX(UNIT_COST) AS unit_cost,
        ROUND(SUM((STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS
            + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
      AND (CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')
    GROUP BY MDS_FAM_ID, ITEM_NBR, OMNI_SEG_DESC, OMNI_DIVISION, OMNI_DEPT_DESC, REPLEN_TYPE
    """
    try:
        result = client.query(query).result()
        items = [ItemDetail(**dict(row)) for row in result]
        if not items:
            raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
        return items
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search/items")
async def search_items(
    q: str = Query(..., min_length=3),
    limit: int = Query(20, le=100)
):
    client = cache._get_client()
    query = f"""
    SELECT DISTINCT MDS_FAM_ID AS mds_fam_id, ITEM_NBR AS item_nbr,
        OMNI_DIVISION AS sbu, OMNI_DEPT_DESC AS department
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
      AND (CAST(MDS_FAM_ID AS STRING) LIKE '{q}%' OR CAST(ITEM_NBR AS STRING) LIKE '{q}%')
    LIMIT {limit}
    """
    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Cache status & manual refresh
# ============================================================

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Where's My Stuff API",
        "version": "3.0.0",
        "cache": {
            "ready": cache.is_ready,
            "rows": cache.row_count,
            "loaded_at": cache.loaded_date,
            "load_time_sec": cache.load_time_sec,
            "loading": cache.is_loading,
        }
    }


@app.post("/api/cache/refresh")
async def refresh_cache():
    """Manual cache refresh (POST to avoid accidental triggers)."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cache.load)
        return {"status": "refreshed", "rows": cache.row_count, "load_time_sec": cache.load_time_sec}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Static Dashboard Serving
# ============================================================

def get_dashboard_path():
    cloud_run_path = Path("/app/dashboard/index.html")
    if cloud_run_path.exists():
        return cloud_run_path
    cwd_path = Path.cwd() / "dashboard" / "index.html"
    if cwd_path.exists():
        return cwd_path
    local_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if local_path.exists():
        return local_path
    return Path(__file__).parent / "dashboard" / "index.html"


@app.get("/dashboard")
async def serve_dashboard():
    dashboard_file = get_dashboard_path()
    if dashboard_file.exists():
        return FileResponse(dashboard_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Dashboard not found")


# ============================================================
# Local:     uvicorn api.server:app --reload --port 8001
# Cloud Run: Automatically uses PORT env variable
# ============================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
