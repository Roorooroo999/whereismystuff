"""
Where's My Stuff - Inventory Rollup API
FastAPI backend for BigQuery-powered dashboard filtering
Deployed on Google Cloud Run
"""

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from google.cloud import bigquery
import os
import logging
from typing import Optional, List
from pydantic import BaseModel
from pathlib import Path

logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="Where's My Stuff API",
    description="Inventory Rollup Dashboard API - Walmart Total Inventory Team",
    version="2.0.0"
)

# Enable CORS for dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# No-cache middleware to prevent stale filtered results
class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            # Log query params for debugging
            if request.query_params:
                logger.info(f"{request.url.path} filters: {dict(request.query_params)}")
        return response

app.add_middleware(NoCacheMiddleware)

# BigQuery client (uses ADC - Application Default Credentials)
# In Cloud Run, ADC automatically uses the service account attached to the instance
PROJECT_ID = os.environ.get("PROJECT_ID", "wmt-instockinventory-datamart")
DATASET = os.environ.get("DATASET", "WM_AD_HOC")
TABLE = os.environ.get("TABLE", "WHERES_MY_STUFF_ROLLUP")

def get_bq_client():
    """Get BigQuery client using ADC"""
    return bigquery.Client(project=PROJECT_ID)


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
# API Endpoints
# ============================================================

@app.get("/")
async def root():
    """Health check"""
    return {"status": "ok", "service": "Where's My Stuff API"}


@app.get("/api/summary", response_model=InventorySummary)
async def get_summary(
    sbu: Optional[List[str]] = Query(None, description="Filter by SBU(s)"),
    division: Optional[List[str]] = Query(None, description="Filter by Division(s)"),
    department: Optional[List[str]] = Query(None, description="Filter by Department(s)"),
    category: Optional[List[str]] = Query(None, description="Filter by Category(s)"),
    dc_type: Optional[List[str]] = Query(None, description="Filter by DC Type(s)"),
    replen_type: Optional[str] = Query(None, description="Filter by Replen Type"),
    item_id: Optional[str] = Query(None, description="Filter by MDS_FAM_ID or WALMART_NBR")
):
    """
    Get inventory summary with optional filters.
    Returns aggregated KPI data for the dashboard.
    """
    client = get_bq_client()

    # Build WHERE clause
    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        # Search by MDS_FAM_ID or ITEM_NBR (WALMART_NBR)
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        COUNT(DISTINCT MDS_FAM_ID) AS total_items,
        SUM(STORE_OH_UNITS) AS store_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units,
        ROUND(SUM(STORE_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS store_cost,
        ROUND(SUM(DC_RESERVED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_reserved_cost,
        ROUND(SUM(IN_TRANSIT_UNITS * COALESCE(UNIT_COST, 0)), 2) AS transit_cost,
        ROUND(SUM(DC_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_cost,
        ROUND(SUM(DC_LABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_labeled_cost,
        ROUND(SUM(DC_UNLABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_unlabeled_cost,
        ROUND(SUM((STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
    """

    try:
        result = client.query(query).result()
        row = list(result)[0]

        return InventorySummary(
            total_items=row.total_items or 0,
            store_units=row.store_units or 0,
            dc_reserved_units=row.dc_reserved_units or 0,
            transit_units=row.transit_units or 0,
            dc_units=row.dc_units or 0,
            dc_labeled_units=row.dc_labeled_units or 0,
            dc_unlabeled_units=row.dc_unlabeled_units or 0,
            total_units=row.total_units or 0,
            store_cost=row.store_cost or 0,
            dc_reserved_cost=row.dc_reserved_cost or 0,
            transit_cost=row.transit_cost or 0,
            dc_cost=row.dc_cost or 0,
            dc_labeled_cost=row.dc_labeled_cost or 0,
            dc_unlabeled_cost=row.dc_unlabeled_cost or 0,
            total_cost=row.total_cost or 0
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/by-sbu", response_model=List[SBUData])
async def get_by_sbu(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None),
    category: Optional[List[str]] = Query(None),
    dc_type: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None),
    item_id: Optional[str] = Query(None)
):
    """Get inventory breakdown by SBU"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        OMNI_DIVISION AS sbu,
        COUNT(DISTINCT MDS_FAM_ID) AS item_count,
        SUM(STORE_OH_UNITS) AS store_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units,
        ROUND(SUM(STORE_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS store_cost,
        ROUND(SUM((STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
    GROUP BY OMNI_DIVISION
    ORDER BY total_units DESC
    """

    try:
        result = client.query(query).result()
        return [SBUData(**dict(row)) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    """Get inventory breakdown by SBU with Department rollup (hierarchical view)"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        OMNI_DIVISION AS sbu,
        OMNI_DEPT_NBR AS dept_nbr,
        OMNI_DEPT_DESC AS department,
        COUNT(DISTINCT MDS_FAM_ID) AS item_count,
        SUM(STORE_OH_UNITS) AS store_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units,
        ROUND(SUM((STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
      AND OMNI_DEPT_DESC IS NOT NULL
    GROUP BY OMNI_DIVISION, OMNI_DEPT_NBR, OMNI_DEPT_DESC
    ORDER BY OMNI_DIVISION, total_units DESC
    """

    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/by-division")
async def get_by_division(
    sbu: Optional[List[str]] = Query(None),
    replen_type: Optional[str] = Query(None)
):
    """Get inventory breakdown by Division"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        OMNI_DIVISION AS division,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
    GROUP BY OMNI_DIVISION
    ORDER BY total_units DESC
    """

    try:
        result = client.query(query).result()
        return [{"division": row.division, "total_units": row.total_units} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/by-replen")
async def get_by_replen(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None)
):
    """Get inventory breakdown by Replen Type"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        REPLEN_TYPE,
        SUM(STORE_OH_UNITS) AS store_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
    GROUP BY REPLEN_TYPE
    ORDER BY REPLEN_TYPE
    """

    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/item/{item_id}", response_model=List[ItemDetail])
async def get_item(item_id: str):
    """
    Get detailed inventory for a specific item.
    Accepts MDS_FAM_ID or WALMART_NBR (ITEM_NBR).
    """
    client = get_bq_client()

    query = f"""
    SELECT
        MDS_FAM_ID AS mds_fam_id,
        ITEM_NBR AS item_nbr,
        OMNI_SEG_DESC AS omni_seg_desc,
        OMNI_DIVISION AS omni_division,
        OMNI_DEPT_DESC AS omni_dept_desc,
        REPLEN_TYPE AS replen_type,
        SUM(STORE_OH_UNITS) AS store_oh_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) AS total_units,
        MAX(UNIT_COST) AS unit_cost,
        ROUND(SUM((STORE_OH_UNITS + DC_RESERVED_UNITS + IN_TRANSIT_UNITS + DC_OH_UNITS + COALESCE(DC_LABELED_UNITS, 0) + COALESCE(DC_UNLABELED_UNITS, 0)) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
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
    q: str = Query(..., min_length=3, description="Search query (min 3 chars)"),
    limit: int = Query(20, le=100, description="Max results")
):
    """
    Search items by MDS_FAM_ID or WALMART_NBR prefix.
    Returns matching items for autocomplete.
    """
    client = get_bq_client()

    query = f"""
    SELECT DISTINCT
        MDS_FAM_ID AS mds_fam_id,
        ITEM_NBR AS item_nbr,
        OMNI_DIVISION AS sbu,
        OMNI_DEPT_DESC AS department
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


@app.get("/api/filters/departments")
async def get_departments(
    sbu: Optional[List[str]] = Query(None),
    division: Optional[List[str]] = Query(None)
):
    """Get available departments based on current filters"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT DISTINCT OMNI_DEPT_DESC AS department
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
      AND OMNI_DEPT_DESC IS NOT NULL
    ORDER BY department
    """

    try:
        result = client.query(query).result()
        return [row.department for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/filters/categories")
async def get_categories(
    sbu: Optional[List[str]] = Query(None),
    department: Optional[List[str]] = Query(None)
):
    """Get available categories based on current filters (cascades from department)"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    where_sql = " AND ".join(where_clauses)

    # Optimized: add LIMIT to prevent slow queries
    query = f"""
    SELECT DISTINCT
        OMNI_CATG_NBR AS category_nbr,
        OMNI_CATG_DESC AS category
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
      AND OMNI_CATG_DESC IS NOT NULL
    ORDER BY category
    LIMIT 150
    """

    try:
        result = client.query(query).result()
        return [{"category_nbr": row.category_nbr, "category": row.category} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    """Get inventory breakdown by DC Type (Regional, Grocery, Fashion, etc.)"""
    client = get_bq_client()

    where_clauses = [
        "BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)",
        "DC_TYPE_DESC IS NOT NULL"
    ]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        DC_TYPE_DESC AS dc_type,
        COUNT(DISTINCT MDS_FAM_ID) AS item_count,
        SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(DC_OH_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) AS total_dc_units,
        ROUND(SUM(DC_OH_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_oh_cost,
        ROUND(SUM(DC_LABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_labeled_cost,
        ROUND(SUM(DC_UNLABELED_UNITS * COALESCE(UNIT_COST, 0)), 2) AS dc_unlabeled_cost,
        ROUND(SUM((DC_OH_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) * COALESCE(UNIT_COST, 0)), 2) AS total_dc_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
    GROUP BY DC_TYPE_DESC
    ORDER BY total_dc_units DESC
    """

    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    """Get inventory breakdown by DC Type with SBU detail (hierarchical)"""
    client = get_bq_client()

    where_clauses = [
        "BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)",
        "DC_TYPE_DESC IS NOT NULL"
    ]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        DC_TYPE_DESC AS dc_type,
        OMNI_DIVISION AS sbu,
        COUNT(DISTINCT MDS_FAM_ID) AS item_count,
        SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(DC_OH_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) AS total_dc_units,
        ROUND(SUM((DC_OH_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) * COALESCE(UNIT_COST, 0)), 2) AS total_dc_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
      AND OMNI_DIVISION != 'UNASSIGNED'
    GROUP BY DC_TYPE_DESC, OMNI_DIVISION
    ORDER BY DC_TYPE_DESC, total_dc_units DESC
    """

    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    """Get inventory breakdown by Department"""
    client = get_bq_client()

    where_clauses = ["BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)"]

    if sbu:
        sbu_list = ", ".join([f"'{s}'" for s in sbu])
        where_clauses.append(f"OMNI_DIVISION IN ({sbu_list})")

    if division:
        div_list = ", ".join([f"'{d}'" for d in division])
        where_clauses.append(f"OMNI_DIVISION IN ({div_list})")

    if department:
        dept_list = ", ".join([f"'{d}'" for d in department])
        where_clauses.append(f"OMNI_DEPT_DESC IN ({dept_list})")

    if category:
        cat_list = ", ".join([f"'{c}'" for c in category])
        where_clauses.append(f"OMNI_CATG_DESC IN ({cat_list})")

    if dc_type:
        dc_list = ", ".join([f"'{d}'" for d in dc_type])
        where_clauses.append(f"DC_TYPE_DESC IN ({dc_list})")

    if replen_type:
        where_clauses.append(f"REPLEN_TYPE = '{replen_type}'")

    if item_id:
        where_clauses.append(f"(CAST(MDS_FAM_ID AS STRING) = '{item_id}' OR CAST(ITEM_NBR AS STRING) = '{item_id}')")

    where_sql = " AND ".join(where_clauses)

    query = f"""
    SELECT
        OMNI_DEPT_DESC AS department,
        OMNI_DEPT_NBR AS dept_nbr,
        COUNT(DISTINCT MDS_FAM_ID) AS item_count,
        SUM(STORE_OH_UNITS) AS store_units,
        SUM(DC_OH_UNITS) AS dc_oh_units,
        SUM(IN_TRANSIT_UNITS) AS transit_units,
        SUM(DC_RESERVED_UNITS) AS dc_reserved_units,
        SUM(DC_LABELED_UNITS) AS dc_labeled_units,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled_units,
        SUM(STORE_OH_UNITS + DC_OH_UNITS + IN_TRANSIT_UNITS + DC_RESERVED_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) AS total_units,
        ROUND(SUM((STORE_OH_UNITS + DC_OH_UNITS + IN_TRANSIT_UNITS + DC_RESERVED_UNITS + DC_LABELED_UNITS + DC_UNLABELED_UNITS) * COALESCE(UNIT_COST, 0)), 2) AS total_cost
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_sql}
      AND OMNI_DEPT_DESC IS NOT NULL
    GROUP BY OMNI_DEPT_DESC, OMNI_DEPT_NBR
    ORDER BY total_units DESC
    """

    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Static Dashboard Serving
# ============================================================

def get_dashboard_path():
    """Get dashboard path - works in local dev, Cloud Run, and Posit Connect"""
    # Cloud Run: dashboard copied to /app/dashboard
    cloud_run_path = Path("/app/dashboard/index.html")
    if cloud_run_path.exists():
        return cloud_run_path

    # Posit Connect / CWD-based: dashboard/ in current working directory
    cwd_path = Path.cwd() / "dashboard" / "index.html"
    if cwd_path.exists():
        return cwd_path

    # Local dev: dashboard in ../dashboard relative to api/
    local_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if local_path.exists():
        return local_path

    # Fallback: same directory
    fallback_path = Path(__file__).parent / "dashboard" / "index.html"
    return fallback_path

@app.get("/dashboard")
async def serve_dashboard():
    """Serve the main dashboard HTML"""
    dashboard_file = get_dashboard_path()
    if dashboard_file.exists():
        return FileResponse(dashboard_file, media_type="text/html")
    raise HTTPException(status_code=404, detail="Dashboard not found. Checked paths: /app/dashboard, ../dashboard")


@app.get("/api/health")
async def health_check():
    """Health check endpoint for Cloud Run"""
    return {
        "status": "healthy",
        "service": "Where's My Stuff API",
        "version": "2.0.0",
        "project": PROJECT_ID,
        "dataset": DATASET,
        "table": TABLE
    }


# ============================================================
# Local:     uvicorn server:app --reload --port 8000
# Cloud Run: Automatically uses PORT env variable
# ============================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
