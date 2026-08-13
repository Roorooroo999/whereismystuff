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
import numpy as np
import os
import json
import logging
import asyncio
import time
import tempfile
import threading
from decimal import Decimal
from datetime import datetime, date, timezone
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path

logger = logging.getLogger("uvicorn.error")


def _df_records(df) -> list:
    """Convert DataFrame to records list with NaN/Inf replaced by None (valid JSON null).

    SAFE_DIVIDE in BigQuery returns NULL when denominator=0, which pandas reads as
    float NaN.  Python's json.dumps serialises NaN as the literal 'NaN' which is not
    valid JSON and breaks browser JSON.parse().

    NOTE: df.where(pd.notnull(df), None) does NOT work for float columns — pandas
    silently converts None back to NaN in float dtype.  Must post-process the records.
    """
    import math
    records = df.to_dict(orient="records")
    clean = []
    for rec in records:
        clean.append({
            k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
            for k, v in rec.items()
        })
    return clean


# Custom JSON encoder to handle BigQuery Decimal/numpy types
class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


class SafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, cls=SafeJSONEncoder, ensure_ascii=False).encode("utf-8")


def _serialize_json(data: dict) -> bytes:
    """Serialize dict to UTF-8 JSON bytes using SafeJSONEncoder."""
    return json.dumps(data, cls=SafeJSONEncoder, ensure_ascii=False).encode("utf-8")


app = FastAPI(
    title="Where's My Stuff API",
    description="Inventory Rollup Dashboard API - Walmart Total Inventory Team",
    version="3.0.0",
    default_response_class=SafeJSONResponse
)

# NOTE: GZipMiddleware removed — Posit Connect's reverse proxy handles compression.
# The cached dict approach (to_lite_response / to_catg_response) still provides
# the main performance benefit (DataFrame→dict conversion happens once per load).

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
        path = request.url.path
        if path.startswith("/api/") or path.startswith("/dashboard") or path == "/":
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        return response

app.add_middleware(NoCacheMiddleware)

# BigQuery config
# All tables are now in wmt-execution-intel-prod.WM_AD_HOC
CLIENT_PROJECT = os.environ.get("CLIENT_PROJECT", "wmt-execution-intel-prod")
PROJECT_ID = os.environ.get("PROJECT_ID", "wmt-execution-intel-prod")
HIST_PROJECT_ID = os.environ.get("HIST_PROJECT_ID", "wmt-execution-intel-prod")
DATASET = os.environ.get("DATASET", "WM_AD_HOC")
TABLE = os.environ.get("TABLE", "WHERES_MY_STUFF_ROLLUP")
HIST_TABLE = os.environ.get("HIST_TABLE", "R0C0JUG_WMUS_HIST_COMBINED")
# DC Drilldown uses a separate DC-level historical table with finer granularity
DC_HIST_TABLE = os.environ.get("DC_HIST_TABLE", "R0C0JUG_WMUS_HIST_DC_LEVEL")
# On-Order historical table for WOW trend analysis
ONORDER_TABLE = os.environ.get("ONORDER_TABLE", "R0C0JUG_WMUS_HIST_ONORDER")
# STO DC-level historical table (has DEST_DC_NBR, DEST_DC_TYPE per row)
STO_TABLE = os.environ.get("STO_TABLE", "R0C0JUG_WMUS_HIST_STO")
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
        print("[CACHE] load() called, getting BigQuery client...", flush=True)
        try:
            client = self._get_client()
            print("[CACHE] BigQuery client created successfully", flush=True)
        except Exception as e:
            print(f"[CACHE] Failed to create BigQuery client: {e}", flush=True)
            logger.error(f"[CACHE] Failed to create BigQuery client: {e}")
            self.is_loading = False
            return

        # Wrap entire load in try/finally to ALWAYS reset is_loading
        try:
            self._do_load(client, t0)
        except Exception as e:
            print(f"[CACHE] Load failed with error: {e}", flush=True)
            import traceback
            print(f"[CACHE] Traceback: {traceback.format_exc()}", flush=True)
        finally:
            self.is_loading = False
            print(f"[CACHE] Load finished. is_ready={self.is_ready}", flush=True)

    def _do_load(self, client, t0):
        """Internal load logic - called by load() with error handling."""
        # Main fact query: pre-aggregated at SBU × Dept × Category × Replen_Type grain
        # This enables category-level filtering in the dashboard
        # Uses MAX(BUS_DT) to always get the latest available data
        # NOTE: DC_TYPE_DESC is NOT in ROLLUP table (only in staging tables)
        fqn = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"
        query = f"""
        SELECT
            OMNI_DIVISION AS sbu,
            OMNI_DEPT_NBR AS dept_nbr,
            OMNI_DEPT_DESC AS department,
            OMNI_CATG_DESC AS category,
            COALESCE(REPLEN_TYPE, '__NONE__') AS replen_type,
            COUNT(DISTINCT MDS_FAM_ID) AS item_count,
            SUM(COALESCE(STORE_OH_UNITS, 0)) AS store_units,
            SUM(COALESCE(ON_FLOOR_UNITS, 0)) AS on_floor_units,
            SUM(COALESCE(BACKROOM_UNITS, 0)) AS backroom_units,
            SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh_units,
            SUM(COALESCE(DC_RESERVED_UNITS, 0)) AS dc_reserved_units,
            SUM(COALESCE(IN_TRANSIT_UNITS, 0)) AS transit_units,
            SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled_units,
            SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled_units,
            SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0)) AS sto_units,
            SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh_units,
            SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard_units,
            -- DC Type breakdown (Grocery, Regional, Fashion, Imports)
            SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
            SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
            SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
            SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
            SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
            SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
            SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports,
            SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_grocery,
            SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_regional,
            SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_fashion,
            SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_imports,
            SUM(COALESCE(DC_LABELED_GROCERY_UNITS, 0)) AS dc_labeled_grocery,
            SUM(COALESCE(DC_LABELED_REGIONAL_UNITS, 0)) AS dc_labeled_regional,
            SUM(COALESCE(DC_LABELED_FASHION_UNITS, 0)) AS dc_labeled_fashion,
            SUM(COALESCE(DC_LABELED_IMPORTS_UNITS, 0)) AS dc_labeled_imports,
            SUM(COALESCE(DC_UNLABELED_GROCERY_UNITS, 0)) AS dc_unlabeled_grocery,
            SUM(COALESCE(DC_UNLABELED_REGIONAL_UNITS, 0)) AS dc_unlabeled_regional,
            SUM(COALESCE(DC_UNLABELED_FASHION_UNITS, 0)) AS dc_unlabeled_fashion,
            SUM(COALESCE(DC_UNLABELED_IMPORTS_UNITS, 0)) AS dc_unlabeled_imports,
            ROUND(SUM(COALESCE(STORE_OH_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS store_cost,
            ROUND(SUM(COALESCE(ON_FLOOR_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS on_floor_cost,
            ROUND(SUM(COALESCE(BACKROOM_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS backroom_cost,
            ROUND(SUM(COALESCE(DC_OH_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS dc_oh_cost,
            ROUND(SUM(COALESCE(DC_RESERVED_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS dc_reserved_cost,
            ROUND(SUM(COALESCE(IN_TRANSIT_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS transit_cost,
            ROUND(SUM(COALESCE(DC_LABELED_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS dc_labeled_cost,
            ROUND(SUM(COALESCE(DC_UNLABELED_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS dc_unlabeled_cost,
            ROUND(SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS sto_cost,
            ROUND(SUM(COALESCE(FC_OH_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS fc_oh_cost,
            ROUND(SUM(COALESCE(ON_YARD_UNITS, 0) * COALESCE(UNIT_COST, 0)), 2) AS on_yard_cost
        FROM {fqn}
        WHERE BUS_DT = (SELECT MAX(BUS_DT) FROM {fqn})
          AND OMNI_DEPT_DESC IS NOT NULL
        GROUP BY sbu, dept_nbr, department, category, replen_type
        """

        print("[CACHE] Loading main data from BigQuery...", flush=True)
        print(f"[CACHE] Query table: {PROJECT_ID}.{DATASET}.{TABLE}", flush=True)
        logger.info("[CACHE] Loading main data from BigQuery...")
        try:
            df = client.query(query).to_dataframe()
            print(f"[CACHE] Query returned {len(df)} rows", flush=True)
        except Exception as e:
            print(f"[CACHE] BigQuery query failed: {e}", flush=True)
            logger.error(f"[CACHE] BigQuery query failed: {e}")
            raise  # Re-raise to be caught by load()'s try/finally

        # Compute derived columns once (includes all channels)
        df["total_units"] = (
            df["store_units"] + df["dc_oh_units"] + df["dc_reserved_units"]
            + df["transit_units"] + df["dc_labeled_units"] + df["dc_unlabeled_units"]
            + df["sto_units"] + df["fc_oh_units"] + df["on_yard_units"]
        )
        df["total_cost"] = (
            df["store_cost"] + df["dc_oh_cost"] + df["dc_reserved_cost"]
            + df["transit_cost"] + df["dc_labeled_cost"] + df["dc_unlabeled_cost"]
            + df["sto_cost"] + df["fc_oh_cost"] + df["on_yard_cost"]
        )

        # Category mapping (separate lighter query)
        catg_query = f"""
        SELECT DISTINCT
            OMNI_DIVISION AS sbu,
            OMNI_DEPT_DESC AS department,
            OMNI_CATG_DESC AS category
        FROM {fqn}
        WHERE BUS_DT = (SELECT MAX(BUS_DT) FROM {fqn})
          AND OMNI_CATG_DESC IS NOT NULL
        ORDER BY department, category
        """
        logger.info("[CACHE] Loading category mappings...")
        try:
            catg_df = client.query(catg_query).to_dataframe()
        except Exception as e:
            logger.error(f"[CACHE] Category query failed: {e}")
            catg_df = pd.DataFrame()

        # Get the actual BUS_DT from the data
        bus_dt_query = f"SELECT MAX(BUS_DT) AS max_dt FROM {fqn}"
        try:
            bus_dt_result = client.query(bus_dt_query).to_dataframe()
            actual_bus_dt = str(bus_dt_result["max_dt"].iloc[0]) if not bus_dt_result.empty else datetime.now().strftime("%Y-%m-%d")
        except Exception as e:
            logger.error(f"[CACHE] BUS_DT query failed: {e}")
            actual_bus_dt = datetime.now().strftime("%Y-%m-%d")

        self.df = df
        self.catg_df = catg_df
        self.loaded_at = time.time()
        self.loaded_date = actual_bus_dt
        self.load_time_sec = round(time.time() - t0, 1)
        self.row_count = len(df)
        # Note: is_loading is set to False in the finally block of load()

        logger.info(f"[CACHE] Loaded {len(df):,} rows + {len(catg_df):,} category mappings in {self.load_time_sec}s (BUS_DT={actual_bus_dt})")

    def _apply_filters(self, df: pd.DataFrame, sbu=None, department=None, category=None,
                        dc_type=None, replen_type=None, **kwargs) -> pd.DataFrame:
        """Apply dashboard filters to a DataFrame."""
        mask = pd.Series(True, index=df.index)
        if sbu:
            mask &= df["sbu"].isin(sbu)
        if department:
            mask &= df["department"].isin(department)
        if replen_type:
            mask &= df["replen_type"] == replen_type

        # Category filter - now directly filters by category column in main data
        if category and "category" in df.columns:
            mask &= df["category"].isin(category)

        filtered = df[mask].copy()

        # DC Type filter - show only DC inventory for selected types
        # DC types: Grocery, Regional, Fashion, Imports
        # When DC Type is selected, Store metrics should be 0 (stores aren't DCs)
        if dc_type:
            all_dc_types = ["Grocery", "Regional", "Fashion", "Imports"]
            selected_types = [t.lower() for t in dc_type]

            # Zero out Store metrics (stores aren't DCs)
            filtered["store_units"] = 0
            filtered["on_floor_units"] = 0
            filtered["backroom_units"] = 0
            filtered["store_cost"] = 0
            filtered["on_floor_cost"] = 0
            filtered["backroom_cost"] = 0

            # Zero out FC (Fulfillment Centers aren't traditional DCs)
            filtered["fc_oh_units"] = 0
            filtered["fc_oh_cost"] = 0

            # Zero out Transit and Reserved (no DC type breakdown available)
            filtered["transit_units"] = 0
            filtered["dc_reserved_units"] = 0
            filtered["transit_cost"] = 0
            filtered["dc_reserved_cost"] = 0

            # Zero out non-selected DC type columns
            for dt in all_dc_types:
                dt_lower = dt.lower()
                if dt_lower not in selected_types:
                    # Zero out this DC type's columns
                    for col_suffix in ["dc_oh_", "sto_", "on_yard_", "dc_labeled_", "dc_unlabeled_"]:
                        col_name = f"{col_suffix}{dt_lower}"
                        if col_name in filtered.columns:
                            filtered[col_name] = 0

            # Recalculate the aggregate DC columns based on remaining DC type columns
            filtered["dc_oh_units"] = (
                filtered.get("dc_oh_grocery", 0) + filtered.get("dc_oh_regional", 0) +
                filtered.get("dc_oh_fashion", 0) + filtered.get("dc_oh_imports", 0)
            )
            filtered["sto_units"] = (
                filtered.get("sto_grocery", 0) + filtered.get("sto_regional", 0) +
                filtered.get("sto_fashion", 0) + filtered.get("sto_imports", 0)
            )
            filtered["on_yard_units"] = (
                filtered.get("on_yard_grocery", 0) + filtered.get("on_yard_regional", 0) +
                filtered.get("on_yard_fashion", 0) + filtered.get("on_yard_imports", 0)
            )
            filtered["dc_labeled_units"] = (
                filtered.get("dc_labeled_grocery", 0) + filtered.get("dc_labeled_regional", 0) +
                filtered.get("dc_labeled_fashion", 0) + filtered.get("dc_labeled_imports", 0)
            )
            filtered["dc_unlabeled_units"] = (
                filtered.get("dc_unlabeled_grocery", 0) + filtered.get("dc_unlabeled_regional", 0) +
                filtered.get("dc_unlabeled_fashion", 0) + filtered.get("dc_unlabeled_imports", 0)
            )

            # Recalculate total_units (only DC metrics that have DC type breakdown)
            filtered["total_units"] = (
                filtered["dc_oh_units"] + filtered["dc_labeled_units"] + filtered["dc_unlabeled_units"]
                + filtered["sto_units"] + filtered["on_yard_units"]
            )
            filtered["total_cost"] = (
                filtered["dc_oh_cost"] + filtered["dc_labeled_cost"] + filtered["dc_unlabeled_cost"]
                + filtered["sto_cost"] + filtered["on_yard_cost"]
            )

        return filtered

    # ---------- Aggregation helpers ----------

    UNIT_COLS = ["store_units", "on_floor_units", "backroom_units", "dc_oh_units",
                 "dc_reserved_units", "transit_units", "dc_labeled_units", "dc_unlabeled_units",
                 "sto_units", "fc_oh_units", "on_yard_units"]
    COST_COLS = ["store_cost", "on_floor_cost", "backroom_cost", "dc_oh_cost",
                 "dc_reserved_cost", "transit_cost", "dc_labeled_cost", "dc_unlabeled_cost",
                 "sto_cost", "fc_oh_cost", "on_yard_cost"]
    SUM_COLS = UNIT_COLS + COST_COLS + ["item_count", "total_units", "total_cost"]

    def get_summary(self, **filters) -> dict:
        filtered = self._apply_filters(self.df, **filters)
        s = filtered[self.SUM_COLS].sum()
        return {
            "total_items": int(s["item_count"]),
            "store_units": int(s["store_units"]),
            "on_floor_units": int(s["on_floor_units"]),
            "backroom_units": int(s["backroom_units"]),
            "dc_reserved_units": int(s["dc_reserved_units"]),
            "transit_units": int(s["transit_units"]),
            "dc_units": int(s["dc_oh_units"]),
            "dc_labeled_units": int(s["dc_labeled_units"]),
            "dc_unlabeled_units": int(s["dc_unlabeled_units"]),
            "sto_units": int(s["sto_units"]),
            "fc_oh_units": int(s["fc_oh_units"]),
            "on_yard_units": int(s["on_yard_units"]),
            "total_units": int(s["total_units"]),
            "store_cost": round(float(s["store_cost"]), 2),
            "on_floor_cost": round(float(s["on_floor_cost"]), 2),
            "backroom_cost": round(float(s["backroom_cost"]), 2),
            "dc_reserved_cost": round(float(s["dc_reserved_cost"]), 2),
            "transit_cost": round(float(s["transit_cost"]), 2),
            "dc_cost": round(float(s["dc_oh_cost"]), 2),
            "dc_labeled_cost": round(float(s["dc_labeled_cost"]), 2),
            "dc_unlabeled_cost": round(float(s["dc_unlabeled_cost"]), 2),
            "sto_cost": round(float(s["sto_cost"]), 2),
            "fc_oh_cost": round(float(s["fc_oh_cost"]), 2),
            "on_yard_cost": round(float(s["on_yard_cost"]), 2),
            "total_cost": round(float(s["total_cost"]), 2),
        }

    def get_by_sbu(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "on_floor_units", "backroom_units",
                "dc_reserved_units", "transit_units", "dc_oh_units", "dc_labeled_units",
                "dc_unlabeled_units", "sto_units", "fc_oh_units", "on_yard_units",
                "total_units", "store_cost", "on_floor_cost", "backroom_cost",
                "dc_oh_cost", "dc_reserved_cost", "transit_cost", "dc_labeled_cost",
                "dc_unlabeled_cost", "sto_cost", "fc_oh_cost", "on_yard_cost", "total_cost"]
        grouped = filtered.groupby("sbu", as_index=False)[cols].sum()
        grouped = grouped.sort_values("total_units", ascending=False)
        result = []
        for _, r in grouped.iterrows():
            result.append({
                "sbu": r["sbu"],
                "item_count": int(r["item_count"]),
                "store_units": int(r["store_units"]),
                "on_floor_units": int(r["on_floor_units"]),
                "backroom_units": int(r["backroom_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "sto_units": int(r["sto_units"]),
                "fc_oh_units": int(r["fc_oh_units"]),
                "on_yard_units": int(r["on_yard_units"]),
                "total_units": int(r["total_units"]),
                "store_cost": round(float(r["store_cost"]), 2),
                "on_floor_cost": round(float(r["on_floor_cost"]), 2),
                "backroom_cost": round(float(r["backroom_cost"]), 2),
                "dc_cost": round(float(r["dc_oh_cost"]), 2),
                "dc_reserved_cost": round(float(r["dc_reserved_cost"]), 2),
                "transit_cost": round(float(r["transit_cost"]), 2),
                "dc_labeled_cost": round(float(r["dc_labeled_cost"]), 2),
                "dc_unlabeled_cost": round(float(r["dc_unlabeled_cost"]), 2),
                "sto_cost": round(float(r["sto_cost"]), 2),
                "fc_oh_cost": round(float(r["fc_oh_cost"]), 2),
                "on_yard_cost": round(float(r["on_yard_cost"]), 2),
                "total_cost": round(float(r["total_cost"]), 2),
            })
        return result

    def get_by_sbu_dept(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "on_floor_units", "backroom_units",
                "dc_reserved_units", "transit_units", "dc_oh_units", "dc_labeled_units",
                "dc_unlabeled_units", "sto_units", "fc_oh_units", "on_yard_units",
                "total_units", "store_cost", "on_floor_cost", "backroom_cost",
                "dc_oh_cost", "dc_reserved_cost", "transit_cost", "dc_labeled_cost",
                "dc_unlabeled_cost", "sto_cost", "fc_oh_cost", "on_yard_cost", "total_cost"]
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
                "on_floor_units": int(r["on_floor_units"]),
                "backroom_units": int(r["backroom_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_units": int(r["dc_oh_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "sto_units": int(r["sto_units"]),
                "fc_oh_units": int(r["fc_oh_units"]),
                "on_yard_units": int(r["on_yard_units"]),
                "total_units": int(r["total_units"]),
                "store_cost": round(float(r["store_cost"]), 2),
                "on_floor_cost": round(float(r["on_floor_cost"]), 2),
                "backroom_cost": round(float(r["backroom_cost"]), 2),
                "dc_cost": round(float(r["dc_oh_cost"]), 2),
                "dc_reserved_cost": round(float(r["dc_reserved_cost"]), 2),
                "transit_cost": round(float(r["transit_cost"]), 2),
                "dc_labeled_cost": round(float(r["dc_labeled_cost"]), 2),
                "dc_unlabeled_cost": round(float(r["dc_unlabeled_cost"]), 2),
                "sto_cost": round(float(r["sto_cost"]), 2),
                "fc_oh_cost": round(float(r["fc_oh_cost"]), 2),
                "on_yard_cost": round(float(r["on_yard_cost"]), 2),
                "total_cost": round(float(r["total_cost"]), 2),
            })
        return result

    def get_by_dc_type(self, **filters) -> list:
        """DC Type breakdown - Grocery, Regional, Fashion, Imports from ROLLUP columns."""
        filtered = self._apply_filters(self.df, **filters)

        # DC Types with their column mappings (including labeled/unlabeled by DC Type)
        dc_types = [
            {"name": "Grocery", "dc_oh_col": "dc_oh_grocery", "sto_col": "sto_grocery", "yard_col": "on_yard_grocery",
             "labeled_col": "dc_labeled_grocery", "unlabeled_col": "dc_unlabeled_grocery"},
            {"name": "Regional", "dc_oh_col": "dc_oh_regional", "sto_col": "sto_regional", "yard_col": "on_yard_regional",
             "labeled_col": "dc_labeled_regional", "unlabeled_col": "dc_unlabeled_regional"},
            {"name": "Imports", "dc_oh_col": "dc_oh_imports", "sto_col": "sto_imports", "yard_col": "on_yard_imports",
             "labeled_col": "dc_labeled_imports", "unlabeled_col": "dc_unlabeled_imports"},
            {"name": "Fashion", "dc_oh_col": "dc_oh_fashion", "sto_col": "sto_fashion", "yard_col": "on_yard_fashion",
             "labeled_col": "dc_labeled_fashion", "unlabeled_col": "dc_unlabeled_fashion"},
        ]

        # Total DC cost for ratio-based cost allocation
        total_dc_oh = int(filtered["dc_oh_units"].sum()) if "dc_oh_units" in filtered.columns else 1
        total_dc_cost = float(filtered["dc_oh_cost"].sum()) + float(filtered["dc_labeled_cost"].sum()) + \
                       float(filtered["dc_unlabeled_cost"].sum()) + float(filtered["sto_cost"].sum()) + \
                       float(filtered["on_yard_cost"].sum())

        result = []
        for dt in dc_types:
            dc_oh = int(filtered[dt["dc_oh_col"]].sum()) if dt["dc_oh_col"] in filtered.columns else 0
            sto = int(filtered[dt["sto_col"]].sum()) if dt["sto_col"] in filtered.columns else 0
            on_yard = int(filtered[dt["yard_col"]].sum()) if dt["yard_col"] in filtered.columns else 0

            # Use actual labeled/unlabeled by DC Type columns
            labeled = int(filtered[dt["labeled_col"]].sum()) if dt["labeled_col"] in filtered.columns else 0
            unlabeled = int(filtered[dt["unlabeled_col"]].sum()) if dt["unlabeled_col"] in filtered.columns else 0

            total_units = dc_oh + labeled + unlabeled + sto + on_yard
            if total_units == 0:
                continue

            # Estimate cost by DC OH ratio
            ratio = dc_oh / total_dc_oh if total_dc_oh > 0 else 0
            cost = total_dc_cost * ratio

            result.append({
                "dc_type": dt["name"],
                "dc_oh_units": dc_oh,
                "dc_labeled_units": labeled,
                "dc_unlabeled_units": unlabeled,
                "sto_units": sto,
                "on_yard_units": on_yard,
                "total_dc_units": total_units,
                "total_dc_cost": round(cost, 2),
            })

        result.sort(key=lambda x: x["total_dc_units"], reverse=True)
        return result

    def get_by_dc_type_sbu(self, **filters) -> list:
        """DC Type by SBU breakdown - Grocery, Regional, Fashion, Imports with SBU detail."""
        filtered = self._apply_filters(self.df, **filters)

        # DC Types with all column mappings (including labeled/unlabeled by DC Type)
        dc_types = [
            {"name": "Grocery", "dc_oh_col": "dc_oh_grocery", "sto_col": "sto_grocery", "yard_col": "on_yard_grocery",
             "labeled_col": "dc_labeled_grocery", "unlabeled_col": "dc_unlabeled_grocery"},
            {"name": "Regional", "dc_oh_col": "dc_oh_regional", "sto_col": "sto_regional", "yard_col": "on_yard_regional",
             "labeled_col": "dc_labeled_regional", "unlabeled_col": "dc_unlabeled_regional"},
            {"name": "Imports", "dc_oh_col": "dc_oh_imports", "sto_col": "sto_imports", "yard_col": "on_yard_imports",
             "labeled_col": "dc_labeled_imports", "unlabeled_col": "dc_unlabeled_imports"},
            {"name": "Fashion", "dc_oh_col": "dc_oh_fashion", "sto_col": "sto_fashion", "yard_col": "on_yard_fashion",
             "labeled_col": "dc_labeled_fashion", "unlabeled_col": "dc_unlabeled_fashion"},
        ]

        # All columns we need
        cols = ["dc_oh_units", "dc_labeled_units", "dc_unlabeled_units",
                "dc_oh_grocery", "dc_oh_regional", "dc_oh_fashion", "dc_oh_imports",
                "sto_grocery", "sto_regional", "sto_fashion", "sto_imports",
                "on_yard_grocery", "on_yard_regional", "on_yard_fashion", "on_yard_imports",
                "dc_labeled_grocery", "dc_labeled_regional", "dc_labeled_fashion", "dc_labeled_imports",
                "dc_unlabeled_grocery", "dc_unlabeled_regional", "dc_unlabeled_fashion", "dc_unlabeled_imports",
                "dc_oh_cost", "dc_labeled_cost", "dc_unlabeled_cost", "sto_cost", "on_yard_cost"]
        existing_cols = [c for c in cols if c in filtered.columns]
        grouped = filtered.groupby("sbu", as_index=False)[existing_cols].sum()

        result = []
        for dt in dc_types:
            for _, r in grouped.iterrows():
                dc_oh = int(r[dt["dc_oh_col"]]) if dt["dc_oh_col"] in r.index else 0
                sto = int(r[dt["sto_col"]]) if dt["sto_col"] in r.index else 0
                on_yard = int(r[dt["yard_col"]]) if dt["yard_col"] in r.index else 0

                # Use actual labeled/unlabeled by DC Type columns
                labeled = int(r[dt["labeled_col"]]) if dt["labeled_col"] in r.index else 0
                unlabeled = int(r[dt["unlabeled_col"]]) if dt["unlabeled_col"] in r.index else 0

                total_units = dc_oh + labeled + unlabeled + sto + on_yard
                if total_units == 0:
                    continue

                # Estimate cost by DC OH ratio
                sbu_total_dc_oh = int(r["dc_oh_units"]) if "dc_oh_units" in r.index else 1
                ratio = dc_oh / sbu_total_dc_oh if sbu_total_dc_oh > 0 else 0
                sbu_cost = (float(r.get("dc_oh_cost", 0)) + float(r.get("dc_labeled_cost", 0)) +
                           float(r.get("dc_unlabeled_cost", 0)) + float(r.get("sto_cost", 0)) +
                           float(r.get("on_yard_cost", 0)))
                cost = sbu_cost * ratio

                result.append({
                    "dc_type": dt["name"],
                    "sbu": r["sbu"],
                    "dc_oh_units": dc_oh,
                    "dc_labeled_units": labeled,
                    "dc_unlabeled_units": unlabeled,
                    "sto_units": sto,
                    "on_yard_units": on_yard,
                    "total_dc_units": total_units,
                    "total_dc_cost": round(cost, 2),
                })

        # Sort by DC Type order then by total units
        type_order = {"Grocery": 0, "Regional": 1, "Imports": 2, "Fashion": 3}
        result.sort(key=lambda x: (type_order.get(x["dc_type"], 99), -x["total_dc_units"]))
        return result

    def get_by_department(self, **filters) -> list:
        filtered = self._apply_filters(self.df, **filters)
        cols = ["item_count", "store_units", "on_floor_units", "backroom_units",
                "dc_oh_units", "transit_units", "dc_reserved_units", "dc_labeled_units",
                "dc_unlabeled_units", "sto_units", "fc_oh_units", "on_yard_units",
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
                "on_floor_units": int(r["on_floor_units"]),
                "backroom_units": int(r["backroom_units"]),
                "dc_oh_units": int(r["dc_oh_units"]),
                "transit_units": int(r["transit_units"]),
                "dc_reserved_units": int(r["dc_reserved_units"]),
                "dc_labeled_units": int(r["dc_labeled_units"]),
                "dc_unlabeled_units": int(r["dc_unlabeled_units"]),
                "sto_units": int(r["sto_units"]),
                "fc_oh_units": int(r["fc_oh_units"]),
                "on_yard_units": int(r["on_yard_units"]),
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
# Historical Inventory Cache
# ============================================================

class HistoricalCache:
    """
    Pre-aggregated historical inventory data from R0C0JUG_WMUS_HIST_COMBINED.
    Three grain levels cached as DataFrames — served instantly via pandas.
    Source table refreshed daily by BigQuery scheduled query.
    """

    # Columns to SUM at each aggregation level
    METRIC_COLS = [
        "store_oh", "backroom", "on_floor", "dc_reserved", "in_transit",
        "dc_oh", "dc_labeled", "dc_unlabeled", "intransit_to_dc", "total_network",
        "fc_oh", "fc_oh_cost", "total_network_cost",
        "dc_oh_regional", "dc_oh_grocery", "dc_oh_fashion", "dc_oh_imports",
        "dc_oh_gidc", "dc_oh_msc", "dc_oh_support", "dc_oh_other",
        "sto_to_regional", "sto_to_grocery", "sto_to_fashion", "sto_to_imports",
        "sto_to_gidc", "sto_to_msc", "sto_to_support", "sto_to_other",
    ]

    # Cache directory for parquet files (instant load on restart).
    # WMS_CACHE_DIR env var lets Posit Connect point to /tmp/wms_posit_cache which
    # survives redeploys (unlike api/.cache/ which is wiped when a new bundle is deployed).
    # Locally: env var not set → falls back to api/.cache/ (unchanged behaviour).
    CACHE_DIR = Path(os.environ.get("WMS_CACHE_DIR", str(Path(__file__).parent / ".cache")))

    def __init__(self):
        self.enterprise_df: Optional[pd.DataFrame] = None
        self.sbu_df: Optional[pd.DataFrame] = None
        self.dept_df: Optional[pd.DataFrame] = None
        self.catg_df: Optional[pd.DataFrame] = None
        self.loaded_at: Optional[float] = None
        self.loaded_date: Optional[str] = None
        self.load_time_sec: float = 0
        self.is_loading: bool = False
        self.last_load_error: str = ""   # last BQ error — visible in /api/health
        # Cached JSON response to avoid repeated DataFrame → dict conversion
        self._cached_response: Optional[dict] = None
        # Pre-serialized JSON bytes for the lite response (enterprise+sbu+dept)
        self._cached_lite_bytes: Optional[bytes] = None
        # Pre-serialized JSON bytes for category data (by_catg) — large, lazy-loaded
        self._cached_catg_bytes: Optional[bytes] = None
        # Ensure cache directory exists
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _start_catg_bg_load(self):
        """Start a background thread to load catg from BigQuery.
        Used when disk cache was loaded but catg parquet was absent.
        """
        import threading

        def _bg():
            try:
                client = self._get_client()
            except Exception as e:
                print(f"[HIST] [BG] Failed to get BQ client for catg: {e}", flush=True)
                return

            fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{HIST_TABLE}`"
            catg_metric_sql = """
                SUM(COALESCE(STORE_OH_UNITS, 0)) AS store_oh,
                SUM(COALESCE(BACKROOM_UNITS, 0)) AS backroom,
                SUM(COALESCE(ON_FLOOR_UNITS, 0)) AS on_floor,
                SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh,
                SUM(COALESCE(DC_RESERVED_UNITS, 0)) AS dc_reserved,
                SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled,
                SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled,
                SUM(COALESCE(INTRANSIT_TO_DC_UNITS, 0)) AS intransit_to_dc,
                SUM(COALESCE(INTRANSIT_TO_DC_COST, 0)) AS intransit_to_dc_cost,
                SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
                SUM(COALESCE(IN_TRANSIT_UNITS, 0)) AS in_transit,
                SUM(COALESCE(TOTAL_NETWORK_UNITS, 0)) AS total_network,
                SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh,
                SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
                SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
                SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
                SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
                SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_to_dc_regional,
                SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_to_dc_grocery,
                SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_to_dc_fashion,
                SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_to_dc_imports,
                SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
                SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
                SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
                SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports,
                -- Cube: HIST_COMBINED stores TOTAL_CUBE; derive wtavg by dividing by units
                SAFE_DIVIDE(SUM(COALESCE(STORE_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STORE_OH_UNITS,0)),0)) AS store_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(BACKROOM_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(BACKROOM_UNITS,0)),0)) AS backroom_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(ON_FLOOR_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_FLOOR_UNITS,0)),0)) AS on_floor_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(DC_RESERVED_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_RESERVED_UNITS,0)),0)) AS dc_reserved_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(TRANSIT_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(IN_TRANSIT_UNITS,0)),0)) AS transit_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(FC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(FC_OH_UNITS,0)),0)) AS fc_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(DC_OH_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_OH_UNITS,0)),0)) AS dc_oh_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(DC_LBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_LABELED_UNITS,0)),0)) AS dc_lbl_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(DC_UNLBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_UNLABELED_UNITS,0)),0)) AS dc_unlbl_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(INTRANSIT_DC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(INTRANSIT_TO_DC_UNITS,0)),0)) AS intransit_dc_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(STO_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS,0)),0)) AS sto_wtavg_cube,
                SAFE_DIVIDE(SUM(COALESCE(ON_YARD_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_YARD_UNITS,0)),0)) AS on_yard_wtavg_cube
            """
            q = f"""SELECT WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                           OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department,
                           OMNI_CATG_NBR AS catg_nbr, OMNI_CATG_DESC AS category, {catg_metric_sql}
                    FROM {fqn}
                    WHERE OMNI_CATG_NBR IS NOT NULL AND WM_YEAR >= 2024
                    GROUP BY WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department, catg_nbr, category
                    ORDER BY WM_YR_WK_NBR, SBU, dept_nbr, catg_nbr"""
            # Retry with exponential backoff — handles transient Posit/BQ failures
            _retry_delays = [30, 60, 120]
            for attempt in range(len(_retry_delays) + 1):
                try:
                    t0 = time.time()
                    df = client.query(q).to_dataframe()
                    self.catg_df = df
                    self._cached_catg_bytes = None
                    self._cached_response = None
                    print(f"[HIST] [BG] Catg ready: {len(df):,} rows in {round(time.time()-t0,1)}s", flush=True)
                    self._save_catg_to_disk()
                    break  # success
                except Exception as e:
                    if attempt < len(_retry_delays):
                        delay = _retry_delays[attempt]
                        print(f"[HIST] [BG] Catg attempt {attempt+1} failed ({e}), retrying in {delay}s...", flush=True)
                        time.sleep(delay)
                    else:
                        print(f"[HIST] [BG] Catg all retries exhausted: {e}", flush=True)

        threading.Thread(target=_bg, daemon=True, name="hist-catg-loader").start()

    def _save_to_disk(self):
        """Save core DataFrames to parquet for instant load on restart.
        Catg is saved separately by _save_catg_to_disk() once it finishes loading.
        """
        try:
            t0 = time.time()
            self.enterprise_df.to_parquet(self.CACHE_DIR / "enterprise.parquet")
            self.sbu_df.to_parquet(self.CACHE_DIR / "sbu.parquet")
            self.dept_df.to_parquet(self.CACHE_DIR / "dept.parquet")
            # Save metadata (catg may not be ready yet — that's OK)
            metadata = {
                "loaded_date": self.loaded_date,
                "loaded_at": self.loaded_at,
                "columns": list(self.enterprise_df.columns)  # used to detect schema changes
            }
            with open(self.CACHE_DIR / "metadata.json", "w") as f:
                json.dump(metadata, f)
            print(f"[HIST] [DISK] Saved core cache to disk in {round(time.time() - t0, 1)}s", flush=True)
        except Exception as e:
            print(f"[HIST] [WARN] Failed to save cache to disk: {e}", flush=True)

    def _save_catg_to_disk(self):
        """Save catg parquet after background load completes."""
        try:
            if self.catg_df is not None:
                self.catg_df.to_parquet(self.CACHE_DIR / "catg.parquet")
                print(f"[HIST] [DISK] Saved catg cache to disk ({len(self.catg_df):,} rows)", flush=True)
        except Exception as e:
            print(f"[HIST] [WARN] Failed to save catg to disk: {e}", flush=True)

    def _load_from_disk(self, stale_ok: bool = False) -> bool:
        """Load DataFrames from parquet cache. Returns True if core data loaded successfully.

        stale_ok=True: load even if cache date doesn't match today (caller will refresh in background).
        """
        try:
            metadata_file = self.CACHE_DIR / "metadata.json"
            if not metadata_file.exists():
                print("[HIST] No disk cache found", flush=True)
                return False

            with open(metadata_file) as f:
                metadata = json.load(f)
            cached_date = metadata.get("loaded_date")
            today = datetime.now().strftime("%Y-%m-%d")
            is_stale = cached_date != today

            if is_stale and not stale_ok:
                print(f"[HIST] Disk cache is stale ({cached_date} vs {today})", flush=True)
                return False

            # Invalidate cache if cube columns are missing (old cache pre-cube feature)
            # Note: empty list (old metadata without "columns" key) also triggers refresh
            cached_cols = metadata.get("columns", [])
            if "store_wtavg_cube" not in cached_cols:
                print("[HIST] Disk cache missing cube columns, will refresh from BQ", flush=True)
                return False

            t0 = time.time()
            self.enterprise_df = pd.read_parquet(self.CACHE_DIR / "enterprise.parquet")
            self.sbu_df = pd.read_parquet(self.CACHE_DIR / "sbu.parquet")
            self.dept_df = pd.read_parquet(self.CACHE_DIR / "dept.parquet")
            # Catg may not exist yet if server was restarted mid-load
            catg_file = self.CACHE_DIR / "catg.parquet"
            if catg_file.exists():
                self.catg_df = pd.read_parquet(catg_file)
            else:
                print("[HIST] [DISK] catg.parquet not found, will load from BQ in background", flush=True)

            self.loaded_date = cached_date
            self.loaded_at = metadata.get("loaded_at")
            elapsed = round(time.time() - t0, 1)
            stale_tag = " [STALE]" if is_stale else ""
            print(f"[HIST] [FAST{stale_tag}] Loaded from disk in {elapsed}s (cached {cached_date})"
                  f"{' [catg missing]' if self.catg_df is None else ''}", flush=True)
            return True
        except Exception as e:
            print(f"[HIST] [WARN] Failed to load from disk cache: {e}", flush=True)
            return False

    @property
    def is_ready(self) -> bool:
        # catg_df intentionally excluded — it's the slowest query (~35-40s) and is
        # served lazily via /api/historical/catg. Requiring it here blocks the health
        # check and keeps the Buckets page stuck loading for 40+ seconds.
        return (
            self.enterprise_df is not None and not self.enterprise_df.empty
            and self.sbu_df is not None
            and self.dept_df is not None
            and not self.is_loading
        )

    @property
    def catg_ready(self) -> bool:
        """True once the large category DataFrame has finished loading."""
        return self.catg_df is not None

    def _get_client(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=CLIENT_PROJECT, credentials=credentials)
        return bigquery.Client(project=CLIENT_PROJECT)

    def load(self, force_refresh: bool = False):
        """Load historical data - from disk cache first, then BigQuery.

        On restart: Loads from parquet cache in ~1s (instant user experience)
        Then refreshes from BigQuery in background if cache is stale.
        """
        self.is_loading = True
        self._cached_response = None  # Clear cache on reload
        self._cached_lite_bytes = None
        self._cached_catg_bytes = None
        t0 = time.time()

        # STEP 1: Try fresh disk cache (same-day, instant ~1s)
        if not force_refresh and self._load_from_disk(stale_ok=False):
            self.is_loading = False
            self.load_time_sec = round(time.time() - t0, 1)
            if self.catg_df is None:
                print(f"[HIST] [OK] Core ready from disk, catg missing — loading from BQ in background...", flush=True)
                self._start_catg_bg_load()
            else:
                print(f"[HIST] [OK] Ready to serve from disk cache!", flush=True)
            return

        # STEP 1b: Stale disk cache exists — serve immediately, refresh in background
        if not force_refresh and self._load_from_disk(stale_ok=True):
            self.is_loading = False
            self.load_time_sec = round(time.time() - t0, 1)
            print(f"[HIST] Serving stale disk cache; BQ refresh starting in background...", flush=True)

            def _hist_bg_refresh():
                try:
                    client = self._get_client()
                    self._do_load(client, time.time())
                except Exception as e:
                    print(f"[HIST] [BG] Refresh failed: {e}", flush=True)

            threading.Thread(target=_hist_bg_refresh, daemon=True, name="hist-stale-refresh").start()
            return

        # STEP 2: No disk cache — must query BQ
        print(f"[HIST] Loading fresh data from BigQuery...", flush=True)
        try:
            client = self._get_client()
        except Exception as e:
            print(f"[HIST] Failed to get BigQuery client: {e}", flush=True)
            self.is_loading = False
            return

        # Wrap entire load in try/finally to ALWAYS reset is_loading
        try:
            self._do_load(client, t0)
        except Exception as e:
            print(f"[HIST] Load failed with error: {e}", flush=True)
            import traceback
            print(f"[HIST] Traceback: {traceback.format_exc()}", flush=True)
        finally:
            self.is_loading = False
            print(f"[HIST] Load finished. is_ready={self.is_ready}", flush=True)

    def _do_load(self, client, t0):
        """Internal load logic - called by load() with error handling.

        PERFORMANCE: Queries run in PARALLEL using ThreadPoolExecutor.
        This reduces load time from ~50s (sequential) to ~25s (parallel).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Historical metrics - use COALESCE for columns that may not exist in older data
        # Includes ALL DC Type breakdown columns needed by DC Drilldown filtering
        metric_sql = """
            SUM(COALESCE(STORE_OH_UNITS, 0)) AS store_oh,
            SUM(COALESCE(BACKROOM_UNITS, 0)) AS backroom,
            SUM(COALESCE(ON_FLOOR_UNITS, 0)) AS on_floor,
            SUM(COALESCE(DC_RESERVED_UNITS, 0)) AS dc_reserved,
            SUM(COALESCE(IN_TRANSIT_UNITS, 0)) AS in_transit,
            SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh,
            SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled,
            SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled,
            SUM(COALESCE(INTRANSIT_TO_DC_UNITS, 0)) AS intransit_to_dc,
            SUM(COALESCE(INTRANSIT_TO_DC_COST, 0)) AS intransit_to_dc_cost,
            SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
            SUM(COALESCE(TOTAL_NETWORK_UNITS, 0)) AS total_network,
            SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh,
            SUM(COALESCE(FC_OH_COST, 0)) AS fc_oh_cost,
            SUM(COALESCE(TOTAL_NETWORK_COST, 0)) AS total_network_cost,
            -- DC OH by type
            SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
            SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
            SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
            SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
            SUM(COALESCE(DC_OH_GIDC_UNITS, 0)) AS dc_oh_gidc,
            SUM(COALESCE(DC_OH_MSC_UNITS, 0)) AS dc_oh_msc,
            SUM(COALESCE(DC_OH_SUPPORT_UNITS, 0)) AS dc_oh_support,
            SUM(COALESCE(DC_OH_OTHER_UNITS, 0)) AS dc_oh_other,
            -- STO by type (kept for DC drilldown breakdown, not KPI)
            SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_to_regional,
            SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_to_grocery,
            SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_to_fashion,
            SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_to_imports,
            SUM(COALESCE(STO_TO_GIDC_UNITS, 0)) AS sto_to_gidc,
            SUM(COALESCE(STO_TO_MSC_UNITS, 0)) AS sto_to_msc,
            SUM(COALESCE(STO_TO_SUPPORT_UNITS, 0)) AS sto_to_support,
            SUM(COALESCE(STO_TO_OTHER_UNITS, 0)) AS sto_to_other,
            -- On Yard by type (for DC Drilldown filtering)
            SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
            SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
            SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports,
            -- Cube: HIST_COMBINED stores TOTAL_CUBE (sum of units×cube); derive wtavg by dividing by units
            SAFE_DIVIDE(SUM(COALESCE(STORE_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STORE_OH_UNITS,0)),0)) AS store_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(BACKROOM_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(BACKROOM_UNITS,0)),0)) AS backroom_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(ON_FLOOR_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_FLOOR_UNITS,0)),0)) AS on_floor_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_RESERVED_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_RESERVED_UNITS,0)),0)) AS dc_reserved_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(TRANSIT_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(IN_TRANSIT_UNITS,0)),0)) AS transit_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(FC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(FC_OH_UNITS,0)),0)) AS fc_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_OH_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_OH_UNITS,0)),0)) AS dc_oh_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_LBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_LABELED_UNITS,0)),0)) AS dc_lbl_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_UNLBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_UNLABELED_UNITS,0)),0)) AS dc_unlbl_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(INTRANSIT_DC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(INTRANSIT_TO_DC_UNITS,0)),0)) AS intransit_dc_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(STO_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS,0)),0)) AS sto_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(ON_YARD_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_YARD_UNITS,0)),0)) AS on_yard_wtavg_cube
        """

        fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{HIST_TABLE}`"
        # Only load FY24+ (2024 onward) — dashboard shows max 67 weeks (≈1.3 FY).
        # This WHERE clause cuts BQ scan by ~30-40% vs full table scan.
        year_filter = "WHERE WM_YEAR >= 2024"

        # Enterprise level
        q1 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, {metric_sql}
                 FROM {fqn} {year_filter}
                 GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR ORDER BY BUS_DT"""

        # SBU level
        q2 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, {metric_sql}
                 FROM {fqn} {year_filter}
                 GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU ORDER BY BUS_DT, SBU"""

        # Dept level
        q3 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                        OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department, {metric_sql}
                 FROM {fqn} {year_filter}
                 GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department
                 ORDER BY BUS_DT, SBU, dept_nbr"""

        # Category level — HIST_COMBINED does NOT have DC_LABELED/UNLABELED type columns!
        # Only DC_OH_*, STO_TO_*, and ON_YARD_* type breakdowns exist in HIST_COMBINED
        # DC_LABELED/UNLABELED type breakdowns are only available in DC_LEVEL table via CASE WHEN
        catg_metric_sql = """
            SUM(COALESCE(STORE_OH_UNITS, 0)) AS store_oh,
            SUM(COALESCE(BACKROOM_UNITS, 0)) AS backroom,
            SUM(COALESCE(ON_FLOOR_UNITS, 0)) AS on_floor,
            SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh,
            SUM(COALESCE(DC_RESERVED_UNITS, 0)) AS dc_reserved,
            SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled,
            SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled,
            SUM(COALESCE(INTRANSIT_TO_DC_UNITS, 0)) AS intransit_to_dc,
            SUM(COALESCE(INTRANSIT_TO_DC_COST, 0)) AS intransit_to_dc_cost,
            SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
            SUM(COALESCE(IN_TRANSIT_UNITS, 0)) AS in_transit,
            SUM(COALESCE(TOTAL_NETWORK_UNITS, 0)) AS total_network,
            SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh,
            -- DC OH by type (these exist in HIST_COMBINED)
            SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
            SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
            SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
            SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
            -- STO by type (retained for DC-type drilldown, not primary KPI)
            SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_to_dc_regional,
            SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_to_dc_grocery,
            SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_to_dc_fashion,
            SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_to_dc_imports,
            -- On Yard by type (these exist in HIST_COMBINED)
            SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
            SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
            SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports,
            -- Cube: HIST_COMBINED stores TOTAL_CUBE; derive wtavg by dividing by units
            SAFE_DIVIDE(SUM(COALESCE(STORE_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STORE_OH_UNITS,0)),0)) AS store_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(BACKROOM_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(BACKROOM_UNITS,0)),0)) AS backroom_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(ON_FLOOR_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_FLOOR_UNITS,0)),0)) AS on_floor_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_RESERVED_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_RESERVED_UNITS,0)),0)) AS dc_reserved_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(TRANSIT_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(IN_TRANSIT_UNITS,0)),0)) AS transit_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(FC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(FC_OH_UNITS,0)),0)) AS fc_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_OH_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_OH_UNITS,0)),0)) AS dc_oh_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_LBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_LABELED_UNITS,0)),0)) AS dc_lbl_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(DC_UNLBL_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(DC_UNLABELED_UNITS,0)),0)) AS dc_unlbl_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(INTRANSIT_DC_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(INTRANSIT_TO_DC_UNITS,0)),0)) AS intransit_dc_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(STO_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS,0)),0)) AS sto_wtavg_cube,
            SAFE_DIVIDE(SUM(COALESCE(ON_YARD_TOTAL_CUBE,0)), NULLIF(SUM(COALESCE(ON_YARD_UNITS,0)),0)) AS on_yard_wtavg_cube
        """
        # Catg query: drop BUS_DT (frontend only needs WM_YR_WK_NBR; BUS_DT bloats row count).
        # Year filter matches the 67-week chart window (FY24 onward).
        q4 = f"""SELECT WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                        OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department,
                        OMNI_CATG_NBR AS catg_nbr, OMNI_CATG_DESC AS category, {catg_metric_sql}
                 FROM {fqn}
                 WHERE OMNI_CATG_NBR IS NOT NULL AND WM_YEAR >= 2024
                 GROUP BY WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department, catg_nbr, category
                 ORDER BY WM_YR_WK_NBR, SBU, dept_nbr, catg_nbr"""

        print(f"[HIST] Loading from table: {HIST_PROJECT_ID}.{DATASET}.{HIST_TABLE}", flush=True)

        # ============================================================
        # STEP A: Run core queries (enterprise/sbu/dept) in parallel.
        #         These finish in ~10s and make is_ready=True quickly.
        # STEP B: Run catg in a background thread (~35-40s).
        #         catg is served lazily via /api/historical/catg so it
        #         doesn't need to block the health check or Buckets page.
        # ============================================================
        core_queries = {'enterprise': q1, 'sbu': q2, 'dept': q3}
        results = {}

        def _strip_cube_columns(sql: str) -> str:
            """Remove SAFE_DIVIDE cube lines from SQL for tables without TOTAL_CUBE columns."""
            import re as _re
            lines = sql.split('\n')
            clean = [ln for ln in lines if 'TOTAL_CUBE' not in ln.upper()]
            result = '\n'.join(clean)
            # Remove any trailing comma left on the last column before FROM/GROUP/ORDER
            result = _re.sub(r',\s*\n(\s*(FROM|GROUP BY|ORDER BY))', r'\n\1', result, flags=_re.IGNORECASE)
            return result

        def _replace_intransit_with_sto(sql: str) -> str:
            """Fallback: swap INTRANSIT_TO_DC columns for STO equivalents.

            Used when HIST_COMBINED was built without the CDI_ATLAS join
            (i.e. pipeline ran on old schema without HIST_INTRANSIT_DC step).
            The alias names are preserved so the frontend still gets intransit_to_dc key.
            """
            sql = sql.replace('INTRANSIT_TO_DC_UNITS', 'STO_IN_TRANSIT_TO_DC_UNITS')
            sql = sql.replace('INTRANSIT_TO_DC_COST',  'STO_IN_TRANSIT_TO_DC_COST')
            return sql

        def run_query(name, sql):
            try:
                t_start = time.time()
                df = client.query(sql).to_dataframe()
                elapsed = round(time.time() - t_start, 1)
                print(f"[HIST] [OK] {name}: {len(df):,} rows in {elapsed}s", flush=True)
                return df
            except Exception as e:
                err_str = str(e)
                if "Unrecognized name" not in err_str:
                    self.last_load_error = f"{name}: {err_str[:400]}"
                    print(f"[HIST] [ERR] {name} FAILED: {e}", flush=True)
                    return None

                # ── Fallback 1: strip TOTAL_CUBE / WTAVG_CUBE cube lines ──────────
                if "TOTAL_CUBE" in err_str or "WTAVG_CUBE" in err_str:
                    print(f"[HIST] [WARN] {name}: cube cols missing, retrying without cube. BQ: {err_str[:200]}", flush=True)
                    sql = _strip_cube_columns(sql)
                    try:
                        df = client.query(sql).to_dataframe()
                        print(f"[HIST] [OK] {name} (no-cube): {len(df):,} rows", flush=True)
                        return df
                    except Exception as e2:
                        err_str = str(e2)
                        if "Unrecognized name" not in err_str:
                            print(f"[HIST] [ERR] {name} no-cube retry FAILED: {e2}", flush=True)
                            return None

                # ── Fallback 2: replace INTRANSIT_TO_DC with STO columns ──────────
                if "INTRANSIT" in err_str.upper():
                    print(f"[HIST] [WARN] {name}: INTRANSIT cols missing, falling back to STO. BQ: {err_str[:200]}", flush=True)
                    sql = _replace_intransit_with_sto(sql)
                    try:
                        df = client.query(sql).to_dataframe()
                        print(f"[HIST] [OK] {name} (sto-compat): {len(df):,} rows", flush=True)
                        return df
                    except Exception as e3:
                        print(f"[HIST] [ERR] {name} sto-compat retry FAILED: {e3}", flush=True)
                        return None

                self.last_load_error = f"{name}: {err_str[:400]}"
                print(f"[HIST] [ERR] {name} FAILED (unrecognized col): {err_str[:300]}", flush=True)
                return None

        # ── STEP A: All 4 queries in parallel (catg included from start) ────
        # catg used to start AFTER core queries (~10s delay). Now all 4 run
        # simultaneously so catg is ready ~10s sooner for WoW/YoY category view.
        all_queries = {**core_queries, 'catg': q4}
        print(f"[HIST] Starting 4 queries in parallel (enterprise/sbu/dept/catg)...", flush=True)
        core_start = time.time()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(run_query, name, sql): name for name, sql in all_queries.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    print(f"[HIST] [ERR] {name} exception: {e}", flush=True)
                    results[name] = None

        core_elapsed = round(time.time() - core_start, 1)
        print(f"[HIST] [FAST] All queries done in {core_elapsed}s", flush=True)

        # Assign core results
        self.enterprise_df = results.get('enterprise')
        self.sbu_df = results.get('sbu')
        self.dept_df = results.get('dept')

        # Assign catg immediately (preloaded in parallel — no background thread needed)
        if results.get('catg') is not None:
            self.catg_df = results['catg']
            print(f"[HIST] Catg preloaded: {len(self.catg_df):,} rows", flush=True)
        else:
            print(f"[HIST] [WARN] Catg query failed — category view unavailable", flush=True)

        # Validate
        if self.dept_df is not None and len(self.dept_df) > 0:
            unique_depts = self.dept_df['department'].nunique() if 'department' in self.dept_df.columns else 'N/A'
            print(f"[HIST] Dept: {len(self.dept_df):,} records, {unique_depts} departments", flush=True)
        else:
            print(f"[HIST] [WARN] No dept data loaded!", flush=True)

        if self.enterprise_df is not None and len(self.enterprise_df) > 0:
            critical_cols = ['fc_oh', 'on_yard', 'intransit_to_dc', 'dc_labeled', 'dc_unlabeled']
            missing_cols = [c for c in critical_cols if c not in self.enterprise_df.columns]
            if missing_cols:
                print(f"[HIST] [WARN] Missing critical columns: {missing_cols}", flush=True)
            latest = self.enterprise_df.iloc[-1]
            print(f"[HIST] Latest week: BUS_DT={latest.get('BUS_DT')}, "
                  f"fc_oh={latest.get('fc_oh', 'N/A')}, on_yard={latest.get('on_yard', 'N/A')}", flush=True)

        # Convert date columns for core DFs
        for df in [self.enterprise_df, self.sbu_df, self.dept_df]:
            if df is not None and "BUS_DT" in df.columns:
                df["BUS_DT"] = df["BUS_DT"].astype(str)

        self.loaded_at = time.time()
        self.loaded_date = datetime.now().strftime("%Y-%m-%d")
        self.load_time_sec = round(time.time() - t0, 1)
        # Note: is_loading=False is set in the finally block of load() right after _do_load returns.
        # Core data is ready — Buckets health check will pass.

        core_rows = (len(self.enterprise_df) if self.enterprise_df is not None else 0) + \
                    (len(self.sbu_df) if self.sbu_df is not None else 0) + \
                    (len(self.dept_df) if self.dept_df is not None else 0)
        logger.info(f"[HIST] Core loaded {core_rows:,} rows in {self.load_time_sec}s")

        # Diagnostic: confirm cube columns are present and have data
        if self.enterprise_df is not None and 'store_wtavg_cube' in self.enterprise_df.columns:
            sample = self.enterprise_df['store_wtavg_cube'].dropna()
            cube_val = round(float(sample.iloc[0]), 4) if len(sample) > 0 else 'all_null'
            print(f"[HIST] [CUBE_OK] store_wtavg_cube present, sample={cube_val}", flush=True)
        else:
            print(f"[HIST] [CUBE_MISSING] store_wtavg_cube NOT in enterprise df — cube will show 0", flush=True)

        # Save all data to disk (catg now included since it loaded in parallel)
        self._save_to_disk()
        if self.catg_df is not None:
            self._save_catg_to_disk()

    def to_lite_response(self) -> dict:
        """LITE response dict — no by_catg (80-90% of payload).

        Cached so DataFrame→dict conversion happens once per cache load.
        Catg data served separately via /api/historical/catg (lazy-loaded).
        Uses the same SafeJSONResponse path as other endpoints for Posit compat.
        """
        if self._cached_lite_bytes is not None:
            return self._cached_lite_bytes  # reusing field as dict cache

        logger.info("[HIST] Building lite response dict (first request after load)...")
        t0 = time.time()

        ent = self.enterprise_df
        dates = sorted(ent["BUS_DT"].unique())
        sbu_list = sorted(self.sbu_df["SBU"].unique()) if "SBU" in self.sbu_df.columns else []

        result = {
            "generated_at": datetime.now().isoformat(),
            "date_range": {
                "min": dates[0] if dates else None,
                "max": dates[-1] if dates else None,
                "weeks": len(dates)
            },
            "sbu_list": list(sbu_list),
            "enterprise": _df_records(self.enterprise_df) if self.enterprise_df is not None else [],
            "by_sbu": _df_records(self.sbu_df) if self.sbu_df is not None else [],
            "by_dept": _df_records(self.dept_df) if self.dept_df is not None else [],
            "by_catg": [],           # fetched separately via /api/historical/catg
        }

        self._cached_lite_bytes = result  # cache the dict (field repurposed)
        elapsed = round(time.time() - t0, 2)
        logger.info(f"[HIST] Lite response dict built in {elapsed}s")
        return result

    def to_catg_response(self) -> dict:
        """Catg-grain dict — by_catg (lazy-loaded by frontend).

        Cached so DataFrame→dict conversion happens once per cache load.
        """
        if self._cached_catg_bytes is not None:
            return self._cached_catg_bytes  # reusing field as dict cache

        logger.info("[HIST] Building catg response dict (first catg request after load)...")
        t0 = time.time()

        result = {
            "by_catg": _df_records(self.catg_df) if self.catg_df is not None else [],
        }

        self._cached_catg_bytes = result  # cache the dict (field repurposed)
        elapsed = round(time.time() - t0, 2)
        logger.info(f"[HIST] Catg response dict built in {elapsed}s")
        return result

    def to_response(self) -> dict:
        """Full response dict (all data including catg). Kept for compatibility."""
        if self._cached_response is not None:
            return self._cached_response

        logger.info("[HIST] Building full response dict (first request after load)...")
        t0 = time.time()

        ent = self.enterprise_df
        dates = sorted(ent["BUS_DT"].unique())
        sbu_list = sorted(self.sbu_df["SBU"].unique()) if "SBU" in self.sbu_df.columns else []

        self._cached_response = {
            "generated_at": datetime.now().isoformat(),
            "date_range": {
                "min": dates[0] if dates else None,
                "max": dates[-1] if dates else None,
                "weeks": len(dates)
            },
            "sbu_list": list(sbu_list),
            "enterprise": _df_records(self.enterprise_df) if self.enterprise_df is not None else [],
            "by_sbu": _df_records(self.sbu_df) if self.sbu_df is not None else [],
            "by_dept": _df_records(self.dept_df) if self.dept_df is not None else [],
            "by_catg": _df_records(self.catg_df) if self.catg_df is not None else [],
        }

        logger.info(f"[HIST] Full response dict built in {round(time.time() - t0, 2)}s")
        return self._cached_response


hist_cache = HistoricalCache()


# ============================================================
# On-Order Historical Cache
# ============================================================

class OnOrderCache:
    """
    On-Order historical data from R0C0JUG_WMUS_HIST_ONORDER.
    Shows units ordered, received, and open by WM Week with various dimensions.
    Supports WOW trend analysis separate from inventory data.
    """

    # Bump CACHE_VERSION to force ALL Posit worker processes to discard their
    # parquet and re-query BQ on next load — even if cached_date == today.
    # Increment whenever the source BQ table is rebuilt with schema/logic changes.
    CACHE_VERSION = 5  # bumped 2026-06-24: BQ table rebuilt with OMS self-join rescue (removes sandbox dependency) — force fresh load

    # Cache directory — same WMS_CACHE_DIR env var as HistoricalCache (see above).
    CACHE_DIR = Path(os.environ.get("WMS_CACHE_DIR", str(Path(__file__).parent / ".cache")))

    def __init__(self):
        self.enterprise_df: Optional[pd.DataFrame] = None  # Total by week
        self.sbu_df: Optional[pd.DataFrame] = None         # By SBU
        self.dept_df: Optional[pd.DataFrame] = None        # By Department
        self.catg_df: Optional[pd.DataFrame] = None        # By Category
        self.loaded_at: Optional[float] = None
        self.loaded_date: Optional[str] = None
        self.load_time_sec: float = 0
        self.is_loading: bool = False
        self._cached_response: Optional[dict] = None
        self._cached_lite_response: Optional[dict] = None
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _save_to_disk(self):
        """Save core DataFrames (enterprise/sbu/dept) to parquet — NOT catg (saved separately)."""
        try:
            t0 = time.time()
            if self.enterprise_df is not None:
                self.enterprise_df.to_parquet(self.CACHE_DIR / "onorder_enterprise.parquet")
            if self.sbu_df is not None:
                self.sbu_df.to_parquet(self.CACHE_DIR / "onorder_sbu.parquet")
            if self.dept_df is not None:
                self.dept_df.to_parquet(self.CACHE_DIR / "onorder_dept.parquet")
            # Save metadata (include columns + version so workers can detect stale caches)
            cols = list(self.enterprise_df.columns) if self.enterprise_df is not None else []
            metadata = {"loaded_date": self.loaded_date, "loaded_at": self.loaded_at,
                        "columns": cols, "cache_version": self.CACHE_VERSION}
            with open(self.CACHE_DIR / "onorder_metadata.json", "w") as f:
                json.dump(metadata, f)
            print(f"[ONORDER] [DISK] Saved core cache to disk in {round(time.time() - t0, 1)}s", flush=True)
        except Exception as e:
            print(f"[ONORDER] [WARN] Failed to save core cache to disk: {e}", flush=True)

    def _save_catg_to_disk(self):
        """Save catg DataFrame to parquet (called from background thread after catg query)."""
        try:
            if self.catg_df is not None:
                self.catg_df.to_parquet(self.CACHE_DIR / "onorder_catg.parquet")
                print(f"[ONORDER] [DISK] Saved catg cache to disk ({len(self.catg_df):,} rows)", flush=True)
        except Exception as e:
            print(f"[ONORDER] [WARN] Failed to save catg to disk: {e}", flush=True)

    def _load_from_disk(self, stale_ok: bool = False) -> bool:
        """Load DataFrames from parquet cache. Returns True if successful.

        stale_ok=True: load even if cache date doesn't match today (caller refreshes in background).
        """
        try:
            metadata_file = self.CACHE_DIR / "onorder_metadata.json"
            if not metadata_file.exists():
                print("[ONORDER] No disk cache found", flush=True)
                return False

            with open(metadata_file) as f:
                metadata = json.load(f)
            cached_date = metadata.get("loaded_date")
            today = datetime.now().strftime("%Y-%m-%d")
            is_stale = cached_date != today

            if is_stale and not stale_ok:
                print(f"[ONORDER] Disk cache is stale ({cached_date} vs {today})", flush=True)
                return False

            # Invalidate cache if version has changed (BQ table was rebuilt)
            cached_version = metadata.get("cache_version", 1)
            if cached_version != self.CACHE_VERSION:
                print(f"[ONORDER] Cache version mismatch ({cached_version} vs {self.CACHE_VERSION}), will refresh from BQ", flush=True)
                return False

            # Invalidate cache if required columns are missing
            cached_cols = metadata.get("columns", [])
            required_cols = ["cube_ordered", "import_ind", "in_store_wm_week"]
            missing = [c for c in required_cols if c not in cached_cols]
            if missing:
                print(f"[ONORDER] Disk cache missing columns {missing}, will refresh from BQ", flush=True)
                return False

            t0 = time.time()
            self.enterprise_df = pd.read_parquet(self.CACHE_DIR / "onorder_enterprise.parquet")
            self.sbu_df = pd.read_parquet(self.CACHE_DIR / "onorder_sbu.parquet")
            self.dept_df = pd.read_parquet(self.CACHE_DIR / "onorder_dept.parquet")

            catg_file = self.CACHE_DIR / "onorder_catg.parquet"
            if catg_file.exists():
                self.catg_df = pd.read_parquet(catg_file)
            else:
                self.catg_df = None

            self.loaded_date = cached_date
            self.loaded_at = metadata.get("loaded_at")
            elapsed = round(time.time() - t0, 1)
            stale_tag = " [STALE]" if is_stale else ""
            catg_info = f"{len(self.catg_df):,} catg rows" if self.catg_df is not None else "catg pending"
            print(f"[ONORDER] [FAST{stale_tag}] Loaded from disk in {elapsed}s ({catg_info})", flush=True)
            return True
        except Exception as e:
            print(f"[ONORDER] [WARN] Failed to load from disk cache: {e}", flush=True)
            return False

    def _start_catg_bg_load(self):
        """Start background thread to load catg from BigQuery (non-blocking)."""
        import threading
        print(f"[ONORDER] [BG] Starting background catg query (won't block health check)...", flush=True)

        def _load_catg_bg():
            try:
                client = self._get_client()
            except Exception as e:
                print(f"[ONORDER] [BG] Failed to get BQ client for catg: {e}", flush=True)
                return
            fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{ONORDER_TABLE}`"
            q_catg = f"""
            SELECT
                wm_week,
                sbu,
                OMNI_DEPT_NBR AS dept_nbr,
                OMNI_DEPT_DESC AS department,
                OMNI_CATG_NBR AS catg_nbr,
                OMNI_CATG_DESC AS category,
                import_ind,
                replen_ind,
                channel_ind,
                dsd_ind,
                SUM(COALESCE(vnpk_ordered, 0)) AS vnpk_ordered,
                SUM(COALESCE(units_ordered, 0)) AS units_ordered,
                SUM(COALESCE(vnpk_received, 0)) AS vnpk_received,
                SUM(COALESCE(units_received, 0)) AS units_received,
                SUM(COALESCE(vnpk_open, 0)) AS vnpk_open,
                SUM(COALESCE(units_open, 0)) AS units_open,
                SUM(COALESCE(cube_ordered, 0)) AS cube_ordered,
                SUM(COALESCE(cube_open, 0)) AS cube_open
            FROM {fqn}
            WHERE wm_week >= 12500 AND wm_week <= 12701 AND OMNI_CATG_NBR IS NOT NULL
            GROUP BY wm_week, sbu, dept_nbr, department, catg_nbr, category, import_ind, replen_ind, channel_ind, dsd_ind
            ORDER BY wm_week, sbu, dept_nbr, catg_nbr
            """
            _retry_delays = [30, 60, 120]
            for attempt in range(len(_retry_delays) + 1):
                try:
                    t_start = time.time()
                    df = client.query(q_catg).to_dataframe()
                    elapsed = round(time.time() - t_start, 1)
                    print(f"[ONORDER] [BG] Catg ready: {len(df):,} rows in {elapsed}s", flush=True)
                    self.catg_df = df
                    self._cached_response = None
                    self._save_catg_to_disk()
                    break  # success
                except Exception as e:
                    if attempt < len(_retry_delays):
                        delay = _retry_delays[attempt]
                        print(f"[ONORDER] [BG] Catg attempt {attempt+1} failed ({e}), retrying in {delay}s...", flush=True)
                        time.sleep(delay)
                    else:
                        print(f"[ONORDER] [BG] Catg all retries exhausted: {e}", flush=True)

        threading.Thread(target=_load_catg_bg, daemon=True, name="onorder-catg-loader").start()

    @property
    def is_ready(self) -> bool:
        # catg_df excluded — slow query (~40s), loaded in background thread
        return (
            self.enterprise_df is not None and not self.enterprise_df.empty
            and self.sbu_df is not None
            and self.dept_df is not None
            and not self.is_loading
        )

    @property
    def catg_ready(self) -> bool:
        return self.catg_df is not None

    def _get_client(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=CLIENT_PROJECT, credentials=credentials)
        return bigquery.Client(project=CLIENT_PROJECT)

    def load(self, force_refresh: bool = False):
        """Load on-order data - from disk cache first, then BigQuery."""
        self.is_loading = True
        self._cached_response = None
        self._cached_lite_response = None
        t0 = time.time()

        # STEP 1: Try fresh disk cache (same-day, instant ~0.5s)
        if not force_refresh and self._load_from_disk(stale_ok=False):
            self.is_loading = False
            self.load_time_sec = round(time.time() - t0, 1)
            if self.catg_df is None:
                self._start_catg_bg_load()
            else:
                print(f"[ONORDER] [OK] Ready to serve from disk cache!", flush=True)
            return

        # STEP 1b: Stale disk cache — serve immediately, refresh in background
        if not force_refresh and self._load_from_disk(stale_ok=True):
            self.is_loading = False
            self.load_time_sec = round(time.time() - t0, 1)
            print(f"[ONORDER] Serving stale disk cache; BQ refresh starting in background...", flush=True)

            def _onorder_bg_refresh():
                try:
                    client = self._get_client()
                    self._do_load(client, time.time())
                except Exception as e:
                    print(f"[ONORDER] [BG] Refresh failed: {e}", flush=True)

            threading.Thread(target=_onorder_bg_refresh, daemon=True, name="onorder-stale-refresh").start()
            return

        # STEP 2: No disk cache — load from BigQuery (first-ever run)
        print(f"[ONORDER] No disk cache; loading from BigQuery...", flush=True)
        try:
            client = self._get_client()
        except Exception as e:
            print(f"[ONORDER] Failed to get BigQuery client: {e}", flush=True)
            self.is_loading = False
            return

        try:
            self._do_load(client, t0)
        except Exception as e:
            print(f"[ONORDER] Load failed with error: {e}", flush=True)
            import traceback
            print(f"[ONORDER] Traceback: {traceback.format_exc()}", flush=True)
        finally:
            self.is_loading = False
            print(f"[ONORDER] Load finished. is_ready={self.is_ready}", flush=True)

    def _do_load(self, client, t0):
        """Internal load logic for On-Order data."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{ONORDER_TABLE}`"

        # Metrics to aggregate
        metric_sql = """
            SUM(COALESCE(vnpk_ordered, 0)) AS vnpk_ordered,
            SUM(COALESCE(units_ordered, 0)) AS units_ordered,
            SUM(COALESCE(vnpk_received, 0)) AS vnpk_received,
            SUM(COALESCE(units_received, 0)) AS units_received,
            SUM(COALESCE(vnpk_open, 0)) AS vnpk_open,
            SUM(COALESCE(units_open, 0)) AS units_open,
            SUM(COALESCE(cube_ordered, 0)) AS cube_ordered,
            SUM(COALESCE(cube_open, 0)) AS cube_open
        """

        # Enterprise level - total by week with indicator breakdowns
        # Filter: wm_week <= 12701 to exclude invalid future week values
        q1 = f"""
        SELECT
            wm_week,
            in_store_wm_week,
            import_ind,
            replen_ind,
            channel_ind,
            dsd_ind,
            {metric_sql}
        FROM {fqn}
        WHERE wm_week <= 12701
        GROUP BY wm_week, in_store_wm_week, import_ind, replen_ind, channel_ind, dsd_ind
        ORDER BY wm_week
        """

        # SBU level
        q2 = f"""
        SELECT
            wm_week,
            in_store_wm_week,
            sbu,
            import_ind,
            replen_ind,
            channel_ind,
            dsd_ind,
            {metric_sql}
        FROM {fqn}
        WHERE wm_week <= 12701
        GROUP BY wm_week, in_store_wm_week, sbu, import_ind, replen_ind, channel_ind, dsd_ind
        ORDER BY wm_week, sbu
        """

        # Department level
        q3 = f"""
        SELECT
            wm_week,
            in_store_wm_week,
            sbu,
            OMNI_DEPT_NBR AS dept_nbr,
            OMNI_DEPT_DESC AS department,
            import_ind,
            replen_ind,
            channel_ind,
            dsd_ind,
            {metric_sql}
        FROM {fqn}
        WHERE wm_week <= 12701
        GROUP BY wm_week, in_store_wm_week, sbu, dept_nbr, department, import_ind, replen_ind, channel_ind, dsd_ind
        ORDER BY wm_week, sbu, dept_nbr
        """

        # Category level
        q4 = f"""
        SELECT
            wm_week,
            in_store_wm_week,
            sbu,
            OMNI_DEPT_NBR AS dept_nbr,
            OMNI_DEPT_DESC AS department,
            OMNI_CATG_NBR AS catg_nbr,
            OMNI_CATG_DESC AS category,
            import_ind,
            replen_ind,
            channel_ind,
            dsd_ind,
            {metric_sql}
        FROM {fqn}
        WHERE wm_week >= 12500 AND wm_week <= 12701 AND OMNI_CATG_NBR IS NOT NULL
        GROUP BY wm_week, in_store_wm_week, sbu, dept_nbr, department, catg_nbr, category, import_ind, replen_ind, channel_ind, dsd_ind
        ORDER BY wm_week, sbu, dept_nbr, catg_nbr
        """

        print(f"[ONORDER] Loading from table: {HIST_PROJECT_ID}.{DATASET}.{ONORDER_TABLE}", flush=True)
        print(f"[ONORDER] Starting 3 core queries in parallel...", flush=True)

        # STEP A: 3 core queries in parallel (~5s)
        core_queries = {
            'enterprise': q1,
            'sbu': q2,
            'dept': q3,
        }

        results = {}

        def run_query(name, sql):
            try:
                t_start = time.time()
                df = client.query(sql).to_dataframe()
                elapsed = round(time.time() - t_start, 1)
                print(f"[ONORDER] [OK] {name}: {len(df):,} rows in {elapsed}s", flush=True)
                return df
            except Exception as e:
                err_str = str(e)
                if "Unrecognized name" in err_str and ("cube_ordered" in err_str.lower() or "cube_open" in err_str.lower()):
                    # cube_ordered/cube_open not yet in ONORDER table — retry without cube columns
                    print(f"[ONORDER] [WARN] {name}: cube columns not in table, retrying without cube...", flush=True)
                    import re as _re
                    lines = sql.split('\n')
                    clean = '\n'.join(ln for ln in lines if 'cube_ordered' not in ln.lower() and 'cube_open' not in ln.lower())
                    clean = _re.sub(r',(\s*\n\s*(?:FROM|GROUP|ORDER))', r'\1', clean)
                    try:
                        df = client.query(clean).to_dataframe()
                        print(f"[ONORDER] [OK] {name} (no cube): {len(df):,} rows", flush=True)
                        return df
                    except Exception as e2:
                        print(f"[ONORDER] [ERR] {name} retry failed: {e2}", flush=True)
                        return None
                print(f"[ONORDER] [ERR] {name} FAILED: {e}", flush=True)
                return None

        parallel_start = time.time()
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(run_query, name, sql): name for name, sql in core_queries.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    print(f"[ONORDER] [ERR] {name} exception: {e}", flush=True)
                    results[name] = None

        parallel_elapsed = round(time.time() - parallel_start, 1)
        print(f"[ONORDER] [FAST] Core queries done in {parallel_elapsed}s", flush=True)

        self.enterprise_df = results.get('enterprise')
        self.sbu_df = results.get('sbu')
        self.dept_df = results.get('dept')

        # Validate enterprise data
        if self.enterprise_df is not None and len(self.enterprise_df) > 0:
            weeks = self.enterprise_df['wm_week'].nunique()
            total_ordered = self.enterprise_df['units_ordered'].sum()
            print(f"[ONORDER] {weeks} weeks, total units_ordered: {total_ordered:,.0f}", flush=True)

        self.loaded_at = time.time()
        self.loaded_date = datetime.now().strftime("%Y-%m-%d")
        self.load_time_sec = round(time.time() - parallel_start, 1)

        core_rows = sum(len(df) for df in [self.enterprise_df, self.sbu_df, self.dept_df] if df is not None)
        print(f"[ONORDER] Core data ready: {core_rows:,} rows. is_ready=True", flush=True)

        # Diagnostic: confirm cube_ordered column is present and has non-zero data
        if self.enterprise_df is not None and 'cube_ordered' in self.enterprise_df.columns:
            non_zero = int((self.enterprise_df['cube_ordered'] > 0).sum())
            total_rows = len(self.enterprise_df)
            cube_sum = float(self.enterprise_df['cube_ordered'].sum())
            print(f"[ONORDER] [CUBE_OK] cube_ordered present: {non_zero}/{total_rows} non-zero rows, total={cube_sum:,.0f} cu-ft", flush=True)
        else:
            print(f"[ONORDER] [CUBE_MISSING] cube_ordered NOT in enterprise df — on-order cube will be blank", flush=True)

        # Save core data to disk (catg saved separately after bg thread)
        self._save_to_disk()

        # STEP B: catg in background thread (~40s, won't block health check)
        import threading
        def _load_catg_bg():
            _retry_delays = [30, 60, 120]
            for attempt in range(len(_retry_delays) + 1):
                try:
                    t_start = time.time()
                    df = client.query(q4).to_dataframe()
                    elapsed = round(time.time() - t_start, 1)
                    print(f"[ONORDER] [BG] Catg ready: {len(df):,} rows in {elapsed}s", flush=True)
                    self.catg_df = df
                    self._cached_response = None
                    self._cached_lite_response = None
                    self._save_catg_to_disk()
                    break  # success
                except Exception as e:
                    if attempt < len(_retry_delays):
                        delay = _retry_delays[attempt]
                        print(f"[ONORDER] [BG] Catg attempt {attempt+1} failed ({e}), retrying in {delay}s...", flush=True)
                        time.sleep(delay)
                    else:
                        print(f"[ONORDER] [BG] Catg all retries exhausted: {e}", flush=True)

        print(f"[ONORDER] Catg loading in background thread (won't block health check)...", flush=True)
        threading.Thread(target=_load_catg_bg, daemon=True, name="onorder-catg-loader").start()

    def to_lite_response(self) -> dict:
        """LITE response — enterprise, by_sbu, by_dept only (no catg).
        Cached. Catg (~316K rows) served separately via /api/on-order/catg.
        """
        if self._cached_lite_response is not None:
            return self._cached_lite_response

        weeks = sorted(self.enterprise_df["wm_week"].unique()) if self.enterprise_df is not None else []
        sbu_list = sorted(self.sbu_df["sbu"].unique()) if self.sbu_df is not None and "sbu" in self.sbu_df.columns else []

        self._cached_lite_response = {
            "generated_at": datetime.now().isoformat(),
            "week_range": {
                "min": int(weeks[0]) if weeks else None,
                "max": int(weeks[-1]) if weeks else None,
                "count": len(weeks)
            },
            "sbu_list": list(sbu_list),
            "filters": {
                "replen_options": ["REPLEN", "NON-REPLEN"],
                "channel_options": ["STORE", "ECOMM"],
                "dsd_options": ["DSD", "NON-DSD"]
            },
            "enterprise": _df_records(self.enterprise_df) if self.enterprise_df is not None else [],
            "by_sbu": _df_records(self.sbu_df) if self.sbu_df is not None else [],
            "by_dept": _df_records(self.dept_df) if self.dept_df is not None else [],
            "by_catg": [],  # fetched separately via /api/on-order/catg
        }
        logger.info("[ONORDER] Lite response built")
        return self._cached_lite_response

    def to_catg_response(self) -> dict:
        """Catg-grain response — lazy-loaded by frontend."""
        return {
            "by_catg": _df_records(self.catg_df) if self.catg_df is not None else [],
            "loading": self.catg_df is None,
        }

    def to_response(self) -> dict:
        """Full response dict (all data including catg). Kept for compatibility."""
        if self._cached_response is not None:
            return self._cached_response

        logger.info("[ONORDER] Building JSON response...")
        t0 = time.time()

        weeks = sorted(self.enterprise_df["wm_week"].unique()) if self.enterprise_df is not None else []
        sbu_list = sorted(self.sbu_df["sbu"].unique()) if self.sbu_df is not None and "sbu" in self.sbu_df.columns else []

        self._cached_response = {
            "generated_at": datetime.now().isoformat(),
            "week_range": {
                "min": int(weeks[0]) if weeks else None,
                "max": int(weeks[-1]) if weeks else None,
                "count": len(weeks)
            },
            "sbu_list": list(sbu_list),
            "filters": {
                "replen_options": ["REPLEN", "NON-REPLEN"],
                "channel_options": ["STORE", "ECOMM"],
                "dsd_options": ["DSD", "NON-DSD"]
            },
            "enterprise": _df_records(self.enterprise_df) if self.enterprise_df is not None else [],
            "by_sbu": _df_records(self.sbu_df) if self.sbu_df is not None else [],
            "by_dept": _df_records(self.dept_df) if self.dept_df is not None else [],
            "by_catg": _df_records(self.catg_df) if self.catg_df is not None else [],
        }

        logger.info(f"[ONORDER] JSON response built in {round(time.time() - t0, 2)}s")
        return self._cached_response


onorder_cache = OnOrderCache()


# ============================================================
# Background auto-refresh
# ============================================================

async def cache_refresh_loop():
    """Background task: refresh all caches from BigQuery every REFRESH_INTERVAL seconds (hourly).

    Hourly refresh ensures the dashboard picks up new BQ pipeline data within 1 hour of
    the pipeline completing (pipeline runs at 7AM Saturday). Max lag = 1 hour regardless
    of server restart timing — eliminates the same-day stale-parquet race condition.
    """
    while True:
        try:
            await asyncio.sleep(REFRESH_INTERVAL)
            loop = asyncio.get_event_loop()
            logger.info("[CACHE] Hourly refresh starting...")

            # Current snapshot
            await loop.run_in_executor(None, cache.load)

            # Historical — force BQ query (bypass disk cache) so new pipeline data is picked up
            await loop.run_in_executor(None, lambda: hist_cache.load(force_refresh=True))

            # On-order
            await loop.run_in_executor(None, onorder_cache.load)

            # STO-DC
            if not _sto_dc_loading:
                await loop.run_in_executor(None, _load_sto_dc_from_bq)

            logger.info("[CACHE] Hourly refresh complete")
        except Exception as e:
            logger.error(f"[CACHE] Hourly refresh failed: {e}")


async def _load_hist_cache_background():
    """Load historical cache in background so it doesn't block server startup."""
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, hist_cache.load)
        logger.info("[HIST] Background load complete")
    except Exception as e:
        logger.error(f"[HIST] Background load failed: {e}")


async def _load_onorder_cache_background():
    """Load on-order cache in background so it doesn't block server startup."""
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, onorder_cache.load)
        logger.info("[ONORDER] Background load complete")
    except Exception as e:
        logger.error(f"[ONORDER] Background load failed: {e}")


@app.on_event("startup")
async def startup_event():
    """Load main cache on startup, historical cache in background."""
    # Use print() for guaranteed output on Posit Connect
    print("[STARTUP] ========================================", flush=True)
    print(f"[STARTUP] Starting server initialization...", flush=True)
    print(f"[STARTUP] PROJECT_ID={PROJECT_ID}", flush=True)
    print(f"[STARTUP] DATASET={DATASET}", flush=True)
    print(f"[STARTUP] TABLE={TABLE}", flush=True)
    creds_set = "SET" if os.environ.get("GOOGLE_CREDENTIALS") else "NOT SET"
    print(f"[STARTUP] GOOGLE_CREDENTIALS env var is {creds_set}", flush=True)
    if os.environ.get("GOOGLE_CREDENTIALS"):
        creds_preview = os.environ.get("GOOGLE_CREDENTIALS", "")[:50]
        print(f"[STARTUP] GOOGLE_CREDENTIALS starts with: {creds_preview}...", flush=True)
    print("[STARTUP] ========================================", flush=True)

    # Current view (DataCache) is hidden — skip its blocking BQ load at startup.
    # Historical and On-Order caches load in background and are always available.
    print("[STARTUP] Current view disabled — skipping DataCache load", flush=True)

    # Load historical and on-order caches in background — don't block server startup
    asyncio.create_task(_load_hist_cache_background())
    asyncio.create_task(_load_onorder_cache_background())
    asyncio.create_task(cache_refresh_loop())
    # STO-DC: disabled along with Current view (only used by Current view endpoints)
    # await loop2.run_in_executor(None, _load_sto_dc_startup)


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
    if category:
        filters["category"] = category
    if dc_type:
        filters["dc_type"] = dc_type
    if replen_type:
        filters["replen_type"] = replen_type
    return filters


def require_cache():
    if not cache.is_ready:
        raise HTTPException(status_code=503, detail="Data is loading, please retry in a few seconds")


# ============================================================
# API Endpoints (all served from in-memory cache)
# ============================================================

@app.get("/")
async def root():
    """Redirect root to dashboard."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="./dashboard")


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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
                            category=category, dc_type=dc_type, replen_type=replen_type)
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
# Supports MDS_FAM_ID, ITEM_NBR, or REPL_GROUP_NBR (CID)
@app.get("/api/item/{item_id}")
async def get_item(item_id: str):
    client = cache._get_client()
    query = f"""
    SELECT
        MDS_FAM_ID AS mds_fam_id, ITEM_NBR AS item_nbr,
        REPL_GROUP_NBR AS cid,
        ITEM1_DESC AS item_desc,
        OMNI_SEG_DESC AS omni_seg_desc, OMNI_DIVISION AS omni_division,
        OMNI_DEPT_DESC AS omni_dept_desc, REPLEN_TYPE AS replen_type,
        SUM(STORE_OH_UNITS) AS store_oh_units,
        SUM(ON_FLOOR_UNITS) AS on_floor_units,
        SUM(BACKROOM_UNITS) AS backroom_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STO_IN_TRANSIT_TO_DC_UNITS) AS sto_units,
        SUM(FC_OH_UNITS) AS fc_oh_units,
        SUM(ON_YARD_UNITS) AS on_yard_units,
        SUM(TOTAL_NETWORK_UNITS) AS total_units,
        MAX(UNIT_COST) AS unit_cost,
        ROUND(SUM(TOTAL_NETWORK_COST), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
      AND (CAST(MDS_FAM_ID AS STRING) = '{item_id}'
           OR CAST(ITEM_NBR AS STRING) = '{item_id}'
           OR CAST(REPL_GROUP_NBR AS STRING) = '{item_id}')
    GROUP BY MDS_FAM_ID, ITEM_NBR, REPL_GROUP_NBR, ITEM1_DESC, OMNI_SEG_DESC, OMNI_DIVISION, OMNI_DEPT_DESC, REPLEN_TYPE
    ORDER BY total_units DESC
    """
    try:
        result = client.query(query).result()
        items = [dict(row) for row in result]
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
    """Search items by MDS_FAM_ID, ITEM_NBR, or CID (REPL_GROUP_NBR)"""
    client = cache._get_client()
    query = f"""
    SELECT DISTINCT MDS_FAM_ID AS mds_fam_id, ITEM_NBR AS item_nbr,
        REPL_GROUP_NBR AS cid,
        OMNI_DIVISION AS sbu, OMNI_DEPT_DESC AS department
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
      AND (CAST(MDS_FAM_ID AS STRING) LIKE '{q}%'
           OR CAST(ITEM_NBR AS STRING) LIKE '{q}%'
           OR CAST(REPL_GROUP_NBR AS STRING) LIKE '{q}%')
    LIMIT {limit}
    """
    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# WCNP Kubernetes probe endpoints
# /health  → liveness  (is the process alive?)
# /ready   → readiness (is the BQ cache loaded and ready to serve?)
# ============================================================

@app.get("/health")
async def liveness():
    """Kubernetes liveness probe — always returns 200 if the process is running."""
    return {"status": "UP"}


@app.get("/ready")
async def readiness():
    """Kubernetes readiness probe — returns 200 only when BQ caches are loaded."""
    if cache.is_ready and hist_cache.is_ready:
        return {"status": "READY"}
    from fastapi import Response
    return JSONResponse(
        status_code=503,
        content={"status": "NOT_READY", "cache": cache.is_ready, "hist_cache": hist_cache.is_ready}
    )


# ============================================================
# Cache status & manual refresh
# ============================================================

@app.get("/api/health")
async def health_check():
    hist_rows = 0
    hist_debug = {}
    if hist_cache.is_ready:
        catg_len = len(hist_cache.catg_df) if hist_cache.catg_df is not None else 0
        hist_rows = len(hist_cache.enterprise_df) + len(hist_cache.sbu_df) + len(hist_cache.dept_df) + catg_len

        # Debug: Check fc_oh and on_yard in enterprise_df
        ent = hist_cache.enterprise_df
        if ent is not None and not ent.empty:
            hist_debug["enterprise_columns"] = list(ent.columns)
            hist_debug["fc_oh_exists"] = "fc_oh" in ent.columns
            hist_debug["on_yard_exists"] = "on_yard" in ent.columns
            if "fc_oh" in ent.columns:
                hist_debug["fc_oh_sum"] = int(ent["fc_oh"].sum())
                hist_debug["fc_oh_latest"] = int(ent.iloc[-1]["fc_oh"]) if len(ent) > 0 else 0
            if "on_yard" in ent.columns:
                hist_debug["on_yard_sum"] = int(ent["on_yard"].sum())
                hist_debug["on_yard_latest"] = int(ent.iloc[-1]["on_yard"]) if len(ent) > 0 else 0
            if len(ent) > 0:
                hist_debug["latest_week"] = {
                    "BUS_DT": str(ent.iloc[-1].get("BUS_DT", "N/A")),
                    "WM_YEAR": int(ent.iloc[-1].get("WM_YEAR", 0)),
                    "WM_WEEK": int(ent.iloc[-1].get("WM_WEEK", 0))
                }

    # On-Order cache info
    onorder_rows = 0
    if onorder_cache.is_ready:
        onorder_rows = sum(len(df) for df in [
            onorder_cache.enterprise_df,
            onorder_cache.sbu_df,
            onorder_cache.dept_df,
            onorder_cache.catg_df
        ] if df is not None)

    return {
        "status": "healthy",
        "service": "Where's My Stuff API",
        "version": "3.1.0",
        "cache": {
            "ready": cache.is_ready,
            "rows": cache.row_count,
            "loaded_at": cache.loaded_date,
            "load_time_sec": cache.load_time_sec,
            "loading": cache.is_loading,
        },
        "historical_cache": {
            "ready": hist_cache.is_ready,
            "rows": hist_rows,
            "loaded_at": hist_cache.loaded_date,
            "load_time_sec": hist_cache.load_time_sec,
            "loading": hist_cache.is_loading,
            "last_error": hist_cache.last_load_error or None,
            "debug": hist_debug
        },
        "onorder_cache": {
            "ready": onorder_cache.is_ready,
            "rows": onorder_rows,
            "loaded_at": onorder_cache.loaded_date,
            "load_time_sec": onorder_cache.load_time_sec,
            "loading": onorder_cache.is_loading,
        }
    }


@app.get("/api/debug/filters")
async def debug_filters(
    sbu: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None)
):
    """Debug endpoint to verify filter logic."""
    require_cache()
    result = {
        "input_params": {
            "sbu": sbu,
            "department": department,
            "category": category,
            "dc_type": dc_type,
            "replen_type": replen_type
        },
        "catg_df_available": cache.catg_df is not None,
        "catg_df_rows": len(cache.catg_df) if cache.catg_df is not None else 0,
    }

    if category and cache.catg_df is not None:
        matching_rows = cache.catg_df[cache.catg_df["category"].isin(category)]
        result["category_matches"] = len(matching_rows)
        result["matched_departments"] = matching_rows["department"].unique().tolist()

    filters = parse_filters(sbu=sbu, department=department, category=category,
                            dc_type=dc_type, replen_type=replen_type)
    result["parsed_filters"] = filters

    filtered = cache._apply_filters(cache.df, **filters)
    result["filtered_rows"] = len(filtered)
    result["original_rows"] = len(cache.df)

    return result


@app.post("/api/cache/refresh")
async def refresh_cache():
    """Manual cache refresh (POST to avoid accidental triggers)."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cache.load)
        return {"status": "refreshed", "rows": cache.row_count, "load_time_sec": cache.load_time_sec}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/refresh-historical")
async def refresh_historical_cache(force: bool = False):
    """Manual historical cache refresh.

    Pass ?force=true to bypass disk cache and re-fetch from BigQuery even if
    disk cache appears current (e.g. BQ source table was updated after today's
    initial cache load).
    """
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: hist_cache.load(force_refresh=force))
        catg_len = len(hist_cache.catg_df) if hist_cache.catg_df is not None else 0
        hist_rows = len(hist_cache.enterprise_df) + len(hist_cache.sbu_df) + len(hist_cache.dept_df) + catg_len
        return {"status": "refreshed", "rows": hist_rows, "load_time_sec": hist_cache.load_time_sec, "forced": force}
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


@app.get("/dashboard/index.html")
async def serve_dashboard_index():
    """Serve index.html explicitly for /dashboard/index.html path."""
    dashboard_file = get_dashboard_path()
    if dashboard_file.exists():
        return FileResponse(dashboard_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Dashboard not found")


@app.get("/api/dc-lookup")
async def get_dc_lookup(dc_nbr: str = Query(..., description="DC number to look up")):
    """
    Return weekly inventory trend for a specific DC number.
    Queries R0C0JUG_WMUS_HIST_DC_OH (DC-grain historical table).
    Falls back to searching dept_df by dept_nbr if DC table is unavailable.
    """
    DC_TABLE = os.environ.get("DC_HIST_TABLE", "R0C0JUG_WMUS_HIST_DC_OH")
    fqn = f"`{PROJECT_ID}.{DATASET}.{DC_TABLE}`"

    # Try the DC-level table first
    try:
        client = hist_cache._get_client()
        q = f"""
            SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR,
                   DC_NBR, DC_DESC,
                   SUM(STORE_OH_UNITS) AS store_oh,
                   SUM(DC_OH_UNITS) AS dc_oh,
                   SUM(IN_TRANSIT_UNITS) AS in_transit,
                   SUM(FC_OH_UNITS) AS fc_oh,
                   SUM(TOTAL_NETWORK_UNITS) AS total_network
            FROM {fqn}
            WHERE CAST(DC_NBR AS STRING) = @dc_nbr
            GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, DC_NBR, DC_DESC
            ORDER BY BUS_DT DESC
            LIMIT 52
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("dc_nbr", "STRING", str(dc_nbr))]
        )
        df = client.query(q, job_config=job_config).to_dataframe()
        if not df.empty:
            if "BUS_DT" in df.columns:
                df["BUS_DT"] = df["BUS_DT"].astype(str)
            desc = df["DC_DESC"].iloc[0] if "DC_DESC" in df.columns else f"DC {dc_nbr}"
            rows = _df_records(df.drop(columns=["DC_NBR", "DC_DESC"], errors="ignore"))
            return SafeJSONResponse(content={"dc_nbr": dc_nbr, "label": desc, "source": "dc_table", "rows": rows})
    except Exception as e:
        logger.warning(f"[DC-LOOKUP] DC table query failed ({e}), falling back to dept_df")

    # Fallback: search dept_df by dept_nbr
    if hist_cache.is_ready and hist_cache.dept_df is not None:
        df = hist_cache.dept_df
        mask = df["dept_nbr"].astype(str) == str(dc_nbr)
        if not mask.any():
            # Try partial dept description match
            if "department" in df.columns:
                mask = df["department"].str.contains(dc_nbr, case=False, na=False)
        matches = df[mask].copy()
        if not matches.empty:
            grp = matches.groupby(["BUS_DT", "WM_YEAR", "WM_WEEK", "WM_YR_WK_NBR"])[
                ["store_oh", "dc_oh", "in_transit", "fc_oh", "total_network"]
            ].sum().reset_index().sort_values("BUS_DT", ascending=False).head(52)
            grp["BUS_DT"] = grp["BUS_DT"].astype(str)
            dept_name = matches["department"].iloc[0] if "department" in matches.columns else f"Dept {dc_nbr}"
            sbu = matches["SBU"].iloc[0] if "SBU" in matches.columns else None
            rows = _df_records(grp)
            return SafeJSONResponse(content={"dc_nbr": dc_nbr, "label": dept_name, "SBU": sbu, "source": "dept_df", "rows": rows})

    raise HTTPException(status_code=404, detail=f"No data found for DC/Dept {dc_nbr}")


@app.get("/api/historical")
async def get_historical_data():
    """Serve LITE historical data (enterprise + sbu + dept — no category rows).

    Category data (~80-90% of payload) is excluded for speed.
    Frontend lazy-loads it via /api/historical/catg when needed.
    Dict cached after first DataFrame→dict conversion so subsequent requests are fast.
    """
    if not hist_cache.is_ready:
        raise HTTPException(status_code=503, detail="Historical data is loading, please retry in a few seconds")
    return SafeJSONResponse(content=hist_cache.to_lite_response())


@app.get("/api/historical/catg")
async def get_historical_catg():
    """Serve category-grain data (by_catg).

    Large payload — lazy-loaded by the frontend only when the user drills to
    category level (Change Analysis → By Category, or SBU Comparison → Category filter).
    Dict cached after first DataFrame→dict conversion.
    Returns loading=true if catg is still being queried from BigQuery.
    """
    if not hist_cache.is_ready:
        raise HTTPException(status_code=503, detail="Historical data is loading, please retry in a few seconds")
    if not hist_cache.catg_ready:
        # Main cache is ready but catg query is still running (~35-40s)
        # Return a loading signal so the frontend can retry
        return SafeJSONResponse(content={"by_catg": [], "loading": True})
    return SafeJSONResponse(content=hist_cache.to_catg_response())


@app.get("/api/on-order")
async def get_onorder_data():
    """Serve LITE on-order data (enterprise + by_sbu + by_dept, no catg).
    Catg (~316K rows) fetched separately via /api/on-order/catg when needed.
    """
    if not onorder_cache.is_ready:
        raise HTTPException(status_code=503, detail="On-Order data is loading, please retry in a few seconds")
    return SafeJSONResponse(content=onorder_cache.to_lite_response())


@app.get("/api/on-order/catg")
async def get_onorder_catg():
    """Serve on-order catg data (lazy-loaded by frontend when user filters to category)."""
    if not onorder_cache.is_ready:
        raise HTTPException(status_code=503, detail="On-Order data is loading, please retry in a few seconds")
    if not onorder_cache.catg_ready:
        return SafeJSONResponse(content={"by_catg": [], "loading": True})
    return SafeJSONResponse(content=onorder_cache.to_catg_response())


@app.api_route("/api/cache/refresh-onorder", methods=["GET", "POST"])
async def refresh_onorder_cache(force: bool = False):
    """Manual on-order cache refresh.

    Use ?force=true to bypass the parquet disk cache and re-query BigQuery directly.
    Required after rebuilding the source BQ table on the same day (otherwise the
    same-day parquet would be served instead of fresh data).
    """
    try:
        if force:
            # Delete on-order parquet files so load() goes straight to BQ
            import shutil
            for fname in ["onorder_enterprise.parquet", "onorder_sbu.parquet",
                          "onorder_dept.parquet", "onorder_catg.parquet", "onorder_metadata.json"]:
                p = onorder_cache.CACHE_DIR / fname
                if p.exists():
                    p.unlink()
                    print(f"[ONORDER] Deleted stale parquet: {fname}", flush=True)
            print("[ONORDER] Force-refresh: parquet cleared, will query BQ", flush=True)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, onorder_cache.load)
        return {"status": "success", "force": force,
                "rows": len(onorder_cache.enterprise_df) if onorder_cache.enterprise_df is not None else 0}
    except Exception as e:
        logger.error(f"[ONORDER] Manual refresh failed: {e}")
        return {"status": "error", "message": str(e)}


# ============================================================
# STO DC-level cache (R0C0JUG_WMUS_HIST_STO)
# Loaded eagerly at startup in background thread (same cadence as hist/onorder).
# Returns weekly STO units by destination DC × DC Type × SBU.
# ============================================================

_sto_dc_cache: Optional[dict] = None
_sto_dc_cache_date: Optional[str] = None
_sto_dc_loading: bool = False
STO_DC_CACHE_DIR = Path(os.environ.get("WMS_CACHE_DIR", str(Path(__file__).parent / ".cache")))


def _sto_dc_get_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=HIST_PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=HIST_PROJECT_ID)


def _load_sto_dc_from_bq():
    """Query BQ and populate _sto_dc_cache. Runs in a background thread."""
    global _sto_dc_cache, _sto_dc_cache_date, _sto_dc_loading
    today = datetime.now().strftime("%Y-%m-%d")
    _sto_dc_loading = True
    try:
        client = _sto_dc_get_client()
        fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{STO_TABLE}`"
        q = f"""
        SELECT
          WM_YR_WK_NBR, WM_YEAR, WM_WEEK,
          DEST_DC_NBR, COALESCE(DEST_DC_TYPE, 'UNKNOWN') AS DEST_DC_TYPE, SBU,
          SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END) AS sto_units,
          SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_COST  ELSE 0 END) AS sto_cost
        FROM {fqn}
        WHERE BUS_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 MONTH)
          AND DEST_DC_NBR IS NOT NULL
          AND STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC')
        GROUP BY WM_YR_WK_NBR, WM_YEAR, WM_WEEK, DEST_DC_NBR, DEST_DC_TYPE, SBU
        ORDER BY WM_YR_WK_NBR DESC, sto_units DESC
        """
        t0 = time.time()
        print(f"[STO-DC] Querying {STO_TABLE}...", flush=True)
        df = client.query(q).to_dataframe()
        elapsed = round(time.time() - t0, 1)
        print(f"[STO-DC] Loaded {len(df):,} rows in {elapsed}s", flush=True)
        payload = {"generated_at": datetime.now().isoformat(), "rows": len(df), "by_dc_sbu": _df_records(df)}
        _sto_dc_cache = payload
        _sto_dc_cache_date = today
        try:
            import pickle
            STO_DC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(STO_DC_CACHE_DIR / "sto_dc_cache.pkl", "wb") as f:
                pickle.dump({"date": today, "data": payload}, f)
            print("[STO-DC] Saved to disk cache", flush=True)
        except Exception as e:
            print(f"[STO-DC] Disk save failed: {e}", flush=True)
    except Exception as e:
        print(f"[STO-DC] BQ load failed: {e}", flush=True)
    finally:
        _sto_dc_loading = False


def _load_sto_dc_startup():
    """Try disk cache first; fall back to BQ. Called at server startup."""
    global _sto_dc_cache, _sto_dc_cache_date
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        import pickle
        path = STO_DC_CACHE_DIR / "sto_dc_cache.pkl"
        if path.exists():
            with open(path, "rb") as f:
                saved = pickle.load(f)
            if saved.get("date") == today:
                _sto_dc_cache = saved["data"]
                _sto_dc_cache_date = today
                print(f"[STO-DC] Loaded from disk ({saved['data']['rows']:,} rows)", flush=True)
                return
            # Stale — serve it immediately, refresh in background
            _sto_dc_cache = saved["data"]
            _sto_dc_cache_date = saved.get("date")
            print(f"[STO-DC] Stale disk cache; refreshing from BQ in background...", flush=True)
    except Exception as e:
        print(f"[STO-DC] Disk load failed: {e}", flush=True)
    threading.Thread(target=_load_sto_dc_from_bq, daemon=True, name="sto-dc-loader").start()


@app.get("/api/sto-dc")
async def get_sto_dc_data():
    """Weekly STO-to-DC data by destination DC number, DC type, and SBU.
    Served from background-loaded cache. Returns 503 while still loading.
    """
    global _sto_dc_cache, _sto_dc_cache_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _sto_dc_cache is not None:
        if _sto_dc_cache_date != today and not _sto_dc_loading:
            threading.Thread(target=_load_sto_dc_from_bq, daemon=True, name="sto-dc-refresh").start()
        return SafeJSONResponse(content=_sto_dc_cache)
    if _sto_dc_loading:
        raise HTTPException(status_code=503, detail="STO-DC data still loading, retry shortly")
    threading.Thread(target=_load_sto_dc_from_bq, daemon=True, name="sto-dc-loader").start()
    raise HTTPException(status_code=503, detail="STO-DC data loading started, retry in ~15 seconds")


@app.get("/dashboard/historical.html")
async def serve_historical():
    """Serve the historical trends dashboard (loaded via iframe toggle)."""
    dashboard_dir = get_dashboard_path().parent
    hist_file = dashboard_dir / "historical.html"
    if hist_file.exists():
        return FileResponse(hist_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Historical dashboard not found")


@app.get("/dashboard/inventory-buckets.html")
async def serve_inventory_buckets():
    """Serve the inventory buckets pipeline dashboard (loaded via iframe toggle)."""
    dashboard_dir = get_dashboard_path().parent
    buckets_file = dashboard_dir / "inventory-buckets.html"
    if buckets_file.exists():
        return FileResponse(buckets_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Inventory buckets dashboard not found")


@app.get("/dashboard/historical_data.json")
async def serve_historical_data():
    """Serve the pre-computed historical inventory JSON."""
    dashboard_dir = get_dashboard_path().parent
    data_file = dashboard_dir / "historical_data.json"
    if data_file.exists():
        return FileResponse(data_file, media_type="application/json")
    raise HTTPException(status_code=404, detail="Historical data not found")


# ============================================================
# Local:     uvicorn api.server:app --reload --port 8001
# Cloud Run: Automatically uses PORT env variable
# ============================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
