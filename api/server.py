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
from decimal import Decimal
from datetime import datetime, date, timezone
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path

logger = logging.getLogger("uvicorn.error")


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


app = FastAPI(
    title="Where's My Stuff API",
    description="Inventory Rollup Dashboard API - Walmart Total Inventory Team",
    version="3.0.0",
    default_response_class=SafeJSONResponse
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
            self.is_loading = False
            return

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
        self.is_loading = False

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
        "dc_oh", "dc_labeled", "dc_unlabeled", "sto_to_dc", "total_network",
        "fc_oh", "fc_oh_cost", "total_network_cost",
        "dc_oh_regional", "dc_oh_grocery", "dc_oh_fashion", "dc_oh_imports",
        "dc_oh_gidc", "dc_oh_msc", "dc_oh_support", "dc_oh_other",
        "sto_to_regional", "sto_to_grocery", "sto_to_fashion", "sto_to_imports",
        "sto_to_gidc", "sto_to_msc", "sto_to_support", "sto_to_other",
    ]

    def __init__(self):
        self.enterprise_df: Optional[pd.DataFrame] = None
        self.sbu_df: Optional[pd.DataFrame] = None
        self.dept_df: Optional[pd.DataFrame] = None
        self.catg_df: Optional[pd.DataFrame] = None
        # DC Drilldown uses separate DC-level table for SBU/Dept/Category views
        self.dc_drill_sbu_df: Optional[pd.DataFrame] = None
        self.dc_drill_dept_df: Optional[pd.DataFrame] = None
        self.dc_drill_catg_df: Optional[pd.DataFrame] = None
        self.loaded_at: Optional[float] = None
        self.loaded_date: Optional[str] = None
        self.load_time_sec: float = 0
        self.is_loading: bool = False
        # Cached JSON response to avoid repeated DataFrame → dict conversion
        self._cached_response: Optional[dict] = None

    @property
    def is_ready(self) -> bool:
        return self.enterprise_df is not None and not self.enterprise_df.empty

    def _get_client(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=CLIENT_PROJECT, credentials=credentials)
        return bigquery.Client(project=CLIENT_PROJECT)

    def load(self):
        """Load 3 aggregation levels from BigQuery in parallel-ish queries."""
        self.is_loading = True
        self._cached_response = None  # Clear cache on reload
        t0 = time.time()
        client = self._get_client()

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
            SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0)) AS sto_to_dc,
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
            -- DC Labeled by type (for DC Drilldown filtering)
            SUM(COALESCE(DC_LABELED_REGIONAL_UNITS, 0)) AS dc_labeled_regional,
            SUM(COALESCE(DC_LABELED_GROCERY_UNITS, 0)) AS dc_labeled_grocery,
            SUM(COALESCE(DC_LABELED_FASHION_UNITS, 0)) AS dc_labeled_fashion,
            SUM(COALESCE(DC_LABELED_IMPORTS_UNITS, 0)) AS dc_labeled_imports,
            -- DC Unlabeled by type (for DC Drilldown filtering)
            SUM(COALESCE(DC_UNLABELED_REGIONAL_UNITS, 0)) AS dc_unlabeled_regional,
            SUM(COALESCE(DC_UNLABELED_GROCERY_UNITS, 0)) AS dc_unlabeled_grocery,
            SUM(COALESCE(DC_UNLABELED_FASHION_UNITS, 0)) AS dc_unlabeled_fashion,
            SUM(COALESCE(DC_UNLABELED_IMPORTS_UNITS, 0)) AS dc_unlabeled_imports,
            -- STO by type
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
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports
        """

        fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{HIST_TABLE}`"

        # Enterprise level
        q1 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, {metric_sql}
                 FROM {fqn} GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR ORDER BY BUS_DT"""

        # SBU level
        q2 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, {metric_sql}
                 FROM {fqn} GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU ORDER BY BUS_DT, SBU"""

        # Dept level
        q3 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                        OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department, {metric_sql}
                 FROM {fqn} GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department
                 ORDER BY BUS_DT, SBU, dept_nbr"""

        # Category level — include ALL DC metrics needed for DC Drilldown (matching metric_sql)
        # Must include all DC Type breakdown columns for filtering to work properly
        catg_metric_sql = """
            SUM(COALESCE(STORE_OH_UNITS, 0)) AS store_oh,
            SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh,
            SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled,
            SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled,
            SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0)) AS sto_to_dc,
            SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
            SUM(COALESCE(IN_TRANSIT_UNITS, 0)) AS in_transit,
            SUM(COALESCE(TOTAL_NETWORK_UNITS, 0)) AS total_network,
            SUM(COALESCE(FC_OH_UNITS, 0)) AS fc_oh,
            -- DC OH by type
            SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
            SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
            SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
            SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
            -- DC Labeled by type
            SUM(COALESCE(DC_LABELED_REGIONAL_UNITS, 0)) AS dc_labeled_regional,
            SUM(COALESCE(DC_LABELED_GROCERY_UNITS, 0)) AS dc_labeled_grocery,
            SUM(COALESCE(DC_LABELED_FASHION_UNITS, 0)) AS dc_labeled_fashion,
            SUM(COALESCE(DC_LABELED_IMPORTS_UNITS, 0)) AS dc_labeled_imports,
            -- DC Unlabeled by type
            SUM(COALESCE(DC_UNLABELED_REGIONAL_UNITS, 0)) AS dc_unlabeled_regional,
            SUM(COALESCE(DC_UNLABELED_GROCERY_UNITS, 0)) AS dc_unlabeled_grocery,
            SUM(COALESCE(DC_UNLABELED_FASHION_UNITS, 0)) AS dc_unlabeled_fashion,
            SUM(COALESCE(DC_UNLABELED_IMPORTS_UNITS, 0)) AS dc_unlabeled_imports,
            -- STO by type (using sto_to_dc naming for frontend compatibility)
            SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_to_dc_regional,
            SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_to_dc_grocery,
            SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_to_dc_fashion,
            SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_to_dc_imports,
            -- On Yard by type
            SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
            SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
            SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports
        """
        q4 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                        OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department,
                        OMNI_CATG_NBR AS catg_nbr, OMNI_CATG_DESC AS category, {catg_metric_sql}
                 FROM {fqn}
                 WHERE OMNI_CATG_NBR IS NOT NULL
                 GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department, catg_nbr, category
                 ORDER BY BUS_DT, SBU, dept_nbr, catg_nbr"""

        print(f"[HIST] Loading from table: {HIST_PROJECT_ID}.{DATASET}.{HIST_TABLE}", flush=True)

        print("[HIST] Loading enterprise-level historical data...", flush=True)
        logger.info("[HIST] Loading enterprise-level historical data...")
        try:
            self.enterprise_df = client.query(q1).to_dataframe()
            print(f"[HIST] Enterprise: {len(self.enterprise_df):,} rows", flush=True)
            if 'fc_oh' in self.enterprise_df.columns:
                print(f"[HIST] Enterprise fc_oh sum: {self.enterprise_df['fc_oh'].sum()}", flush=True)
            if 'on_yard' in self.enterprise_df.columns:
                print(f"[HIST] Enterprise on_yard sum: {self.enterprise_df['on_yard'].sum()}", flush=True)
        except Exception as e:
            print(f"[HIST] Enterprise query FAILED: {e}", flush=True)
            raise

        print("[HIST] Loading SBU-level historical data...", flush=True)
        logger.info("[HIST] Loading SBU-level historical data...")
        self.sbu_df = client.query(q2).to_dataframe()
        print(f"[HIST] SBU: {len(self.sbu_df):,} rows", flush=True)

        print("[HIST] Loading Dept-level historical data...", flush=True)
        logger.info("[HIST] Loading Dept-level historical data...")
        self.dept_df = client.query(q3).to_dataframe()
        print(f"[HIST] Dept: {len(self.dept_df):,} rows", flush=True)

        print("[HIST] Loading Category-level historical data...", flush=True)
        logger.info("[HIST] Loading Category-level historical data...")
        self.catg_df = client.query(q4).to_dataframe()
        print(f"[HIST] Category: {len(self.catg_df):,} rows", flush=True)

        # ============================================================
        # DC Drilldown - Load from separate DC_HIST_TABLE
        # Provides SBU/Dept/Category views with full DC Type breakdown
        # ============================================================
        dc_fqn = f"`{HIST_PROJECT_ID}.{DATASET}.{DC_HIST_TABLE}`"
        dc_drill_metric_sql = """
            SUM(COALESCE(DC_OH_UNITS, 0)) AS dc_oh,
            SUM(COALESCE(DC_LABELED_UNITS, 0)) AS dc_labeled,
            SUM(COALESCE(DC_UNLABELED_UNITS, 0)) AS dc_unlabeled,
            SUM(COALESCE(STO_IN_TRANSIT_TO_DC_UNITS, 0)) AS sto_to_dc,
            SUM(COALESCE(ON_YARD_UNITS, 0)) AS on_yard,
            -- DC OH by type
            SUM(COALESCE(DC_OH_REGIONAL_UNITS, 0)) AS dc_oh_regional,
            SUM(COALESCE(DC_OH_GROCERY_UNITS, 0)) AS dc_oh_grocery,
            SUM(COALESCE(DC_OH_FASHION_UNITS, 0)) AS dc_oh_fashion,
            SUM(COALESCE(DC_OH_IMPORTS_UNITS, 0)) AS dc_oh_imports,
            -- DC Labeled by type
            SUM(COALESCE(DC_LABELED_REGIONAL_UNITS, 0)) AS dc_labeled_regional,
            SUM(COALESCE(DC_LABELED_GROCERY_UNITS, 0)) AS dc_labeled_grocery,
            SUM(COALESCE(DC_LABELED_FASHION_UNITS, 0)) AS dc_labeled_fashion,
            SUM(COALESCE(DC_LABELED_IMPORTS_UNITS, 0)) AS dc_labeled_imports,
            -- DC Unlabeled by type
            SUM(COALESCE(DC_UNLABELED_REGIONAL_UNITS, 0)) AS dc_unlabeled_regional,
            SUM(COALESCE(DC_UNLABELED_GROCERY_UNITS, 0)) AS dc_unlabeled_grocery,
            SUM(COALESCE(DC_UNLABELED_FASHION_UNITS, 0)) AS dc_unlabeled_fashion,
            SUM(COALESCE(DC_UNLABELED_IMPORTS_UNITS, 0)) AS dc_unlabeled_imports,
            -- STO by type
            SUM(COALESCE(STO_TO_REGIONAL_UNITS, 0)) AS sto_to_dc_regional,
            SUM(COALESCE(STO_TO_GROCERY_UNITS, 0)) AS sto_to_dc_grocery,
            SUM(COALESCE(STO_TO_FASHION_UNITS, 0)) AS sto_to_dc_fashion,
            SUM(COALESCE(STO_TO_IMPORTS_UNITS, 0)) AS sto_to_dc_imports,
            -- On Yard by type
            SUM(COALESCE(ON_YARD_REGIONAL_UNITS, 0)) AS on_yard_regional,
            SUM(COALESCE(ON_YARD_GROCERY_UNITS, 0)) AS on_yard_grocery,
            SUM(COALESCE(ON_YARD_FASHION_UNITS, 0)) AS on_yard_fashion,
            SUM(COALESCE(ON_YARD_IMPORTS_UNITS, 0)) AS on_yard_imports
        """

        print(f"[HIST] Loading DC Drilldown data from: {HIST_PROJECT_ID}.{DATASET}.{DC_HIST_TABLE}", flush=True)
        try:
            # DC Drilldown - SBU level
            dc_q1 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, {dc_drill_metric_sql}
                        FROM {dc_fqn} GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU ORDER BY BUS_DT, SBU"""
            self.dc_drill_sbu_df = client.query(dc_q1).to_dataframe()
            print(f"[HIST] DC Drilldown SBU: {len(self.dc_drill_sbu_df):,} rows", flush=True)

            # DC Drilldown - Dept level
            dc_q2 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                               OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department, {dc_drill_metric_sql}
                        FROM {dc_fqn} GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department
                        ORDER BY BUS_DT, SBU, dept_nbr"""
            self.dc_drill_dept_df = client.query(dc_q2).to_dataframe()
            print(f"[HIST] DC Drilldown Dept: {len(self.dc_drill_dept_df):,} rows", flush=True)

            # DC Drilldown - Category level
            dc_q3 = f"""SELECT BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU,
                               OMNI_DEPT_NBR AS dept_nbr, OMNI_DEPT_DESC AS department,
                               OMNI_CATG_NBR AS catg_nbr, OMNI_CATG_DESC AS category, {dc_drill_metric_sql}
                        FROM {dc_fqn}
                        WHERE OMNI_CATG_NBR IS NOT NULL
                        GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department, catg_nbr, category
                        ORDER BY BUS_DT, SBU, dept_nbr, catg_nbr"""
            self.dc_drill_catg_df = client.query(dc_q3).to_dataframe()
            print(f"[HIST] DC Drilldown Category: {len(self.dc_drill_catg_df):,} rows", flush=True)

            # Convert date columns
            for df in [self.dc_drill_sbu_df, self.dc_drill_dept_df, self.dc_drill_catg_df]:
                if df is not None and "BUS_DT" in df.columns:
                    df["BUS_DT"] = df["BUS_DT"].astype(str)
        except Exception as e:
            print(f"[HIST] DC Drilldown load FAILED: {e}", flush=True)
            logger.warning(f"[HIST] DC Drilldown load failed: {e}. DC Drilldown tab will use fallback data.")
            # Fallback: use existing HIST_TABLE data for DC Drilldown
            self.dc_drill_sbu_df = None
            self.dc_drill_dept_df = None
            self.dc_drill_catg_df = None

        # Convert date columns to strings for JSON serialization
        for df in [self.enterprise_df, self.sbu_df, self.dept_df, self.catg_df]:
            if "BUS_DT" in df.columns:
                df["BUS_DT"] = df["BUS_DT"].astype(str)

        self.loaded_at = time.time()
        self.loaded_date = datetime.now().strftime("%Y-%m-%d")
        self.load_time_sec = round(time.time() - t0, 1)
        self.is_loading = False

        dc_drill_rows = (len(self.dc_drill_sbu_df) if self.dc_drill_sbu_df is not None else 0) + \
                        (len(self.dc_drill_dept_df) if self.dc_drill_dept_df is not None else 0) + \
                        (len(self.dc_drill_catg_df) if self.dc_drill_catg_df is not None else 0)
        total_rows = len(self.enterprise_df) + len(self.sbu_df) + len(self.dept_df) + len(self.catg_df) + dc_drill_rows
        logger.info(f"[HIST] Loaded {total_rows:,} total rows in {self.load_time_sec}s")

    def to_response(self) -> dict:
        """Build the same JSON structure as historical_data.json. Uses caching for performance."""
        # Return cached response if available (DataFrame → dict conversion is slow)
        if self._cached_response is not None:
            return self._cached_response

        logger.info("[HIST] Building JSON response (first request after load)...")
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
            "enterprise": self.enterprise_df.to_dict(orient="records"),
            "by_sbu": self.sbu_df.to_dict(orient="records"),
            "by_dept": self.dept_df.to_dict(orient="records"),
            "by_catg": self.catg_df.to_dict(orient="records") if self.catg_df is not None else [],
            # DC Drilldown data from separate DC_HIST_TABLE (with full DC Type breakdown)
            "dc_drill_sbu": self.dc_drill_sbu_df.to_dict(orient="records") if self.dc_drill_sbu_df is not None else [],
            "dc_drill_dept": self.dc_drill_dept_df.to_dict(orient="records") if self.dc_drill_dept_df is not None else [],
            "dc_drill_catg": self.dc_drill_catg_df.to_dict(orient="records") if self.dc_drill_catg_df is not None else [],
        }

        logger.info(f"[HIST] JSON response built in {round(time.time() - t0, 2)}s")
        return self._cached_response


hist_cache = HistoricalCache()


# ============================================================
# Background auto-refresh
# ============================================================

async def cache_refresh_loop():
    """Background task: refresh both caches every REFRESH_INTERVAL seconds or when date changes."""
    while True:
        try:
            await asyncio.sleep(REFRESH_INTERVAL)
            current_date = datetime.now().strftime("%Y-%m-%d")
            loop = asyncio.get_event_loop()
            if cache.loaded_date != current_date or not cache.is_ready:
                logger.info(f"[CACHE] Auto-refresh triggered (date={current_date}, last={cache.loaded_date})")
                await loop.run_in_executor(None, cache.load)
            if hist_cache.loaded_date != current_date or not hist_cache.is_ready:
                logger.info(f"[HIST] Auto-refresh triggered (date={current_date}, last={hist_cache.loaded_date})")
                await loop.run_in_executor(None, hist_cache.load)
        except Exception as e:
            logger.error(f"[CACHE] Auto-refresh failed: {e}")


async def _load_hist_cache_background():
    """Load historical cache in background so it doesn't block server startup."""
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, hist_cache.load)
        logger.info("[HIST] Background load complete")
    except Exception as e:
        logger.error(f"[HIST] Background load failed: {e}")


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

    loop = asyncio.get_event_loop()
    try:
        print("[STARTUP] Beginning cache load...", flush=True)
        await loop.run_in_executor(None, cache.load)
        print("[STARTUP] Cache load function completed", flush=True)
        print(f"[STARTUP] cache.is_ready = {cache.is_ready}", flush=True)
        print(f"[STARTUP] cache.df is None = {cache.df is None}", flush=True)
        if cache.df is not None:
            print(f"[STARTUP] cache.df rows = {len(cache.df)}", flush=True)
        else:
            print("[STARTUP] WARNING: cache.df is None - load likely failed!", flush=True)
    except Exception as e:
        print(f"[CACHE] Initial load failed: {e}", flush=True)
        import traceback
        print(f"[CACHE] Traceback: {traceback.format_exc()}", flush=True)
    # Load historical cache in background — don't block server startup
    asyncio.create_task(_load_hist_cache_background())
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
# Cache status & manual refresh
# ============================================================

@app.get("/api/health")
async def health_check():
    hist_rows = 0
    if hist_cache.is_ready:
        catg_len = len(hist_cache.catg_df) if hist_cache.catg_df is not None else 0
        hist_rows = len(hist_cache.enterprise_df) + len(hist_cache.sbu_df) + len(hist_cache.dept_df) + catg_len
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
        },
        "historical_cache": {
            "ready": hist_cache.is_ready,
            "rows": hist_rows,
            "loaded_at": hist_cache.loaded_date,
            "load_time_sec": hist_cache.load_time_sec,
            "loading": hist_cache.is_loading,
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
async def refresh_historical_cache():
    """Manual historical cache refresh."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, hist_cache.load)
        catg_len = len(hist_cache.catg_df) if hist_cache.catg_df is not None else 0
        hist_rows = len(hist_cache.enterprise_df) + len(hist_cache.sbu_df) + len(hist_cache.dept_df) + catg_len
        return {"status": "refreshed", "rows": hist_rows, "load_time_sec": hist_cache.load_time_sec}
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
            rows = df.drop(columns=["DC_NBR", "DC_DESC"], errors="ignore").to_dict(orient="records")
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
            rows = grp.to_dict(orient="records")
            return SafeJSONResponse(content={"dc_nbr": dc_nbr, "label": dept_name, "SBU": sbu, "source": "dept_df", "rows": rows})

    raise HTTPException(status_code=404, detail=f"No data found for DC/Dept {dc_nbr}")


@app.get("/api/historical")
async def get_historical_data():
    """Serve pre-aggregated historical inventory data from in-memory cache."""
    if not hist_cache.is_ready:
        raise HTTPException(status_code=503, detail="Historical data is loading, please retry in a few seconds")
    return SafeJSONResponse(content=hist_cache.to_response())


@app.get("/dashboard/historical.html")
async def serve_historical():
    """Serve the historical trends dashboard (loaded via iframe toggle)."""
    dashboard_dir = get_dashboard_path().parent
    hist_file = dashboard_dir / "historical.html"
    if hist_file.exists():
        return FileResponse(hist_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Historical dashboard not found")


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
