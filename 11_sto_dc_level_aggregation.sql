-- ══════════════════════════════════════════════════════════════════════════════
-- STO DC-Level Aggregation — by Destination DC Number
-- ══════════════════════════════════════════════════════════════════════════════
-- Purpose: Aggregate STO units from R0C0JUG_WMUS_HIST_STO at the DC grain.
--          Adds DEST_DC_NBR and DEST_DC_TYPE to the GROUP BY so each destination
--          DC gets its own row (previously only grouped by BUS_DT, OMNI_CATG_NBR).
--
-- Source:  wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO
--          (run CREATE TABLE script first — R0C0JUG_WMUS_HIST_STO must exist)
--
-- Use:     Read by dashboard DC Drilldown / STO-by-DC views
--          Can be used as-is for ad-hoc BQ exploration or fed into a scheduled table.
--
-- Updated: June 17, 2026 — added DEST_DC_NBR, DEST_DC_TYPE to GROUP BY
-- Author:  r0c0jug
-- ══════════════════════════════════════════════════════════════════════════════

-- Option A: Ad-hoc query (use directly in BQ or dashboard API)
SELECT
  BUS_DT,
  WM_YEAR,
  WM_WEEK,
  WM_YR_WK_NBR,
  OMNI_DEPT_NBR,
  OMNI_DEPT_DESC,
  OMNI_CATG_NBR,
  OMNI_CATG_DESC,
  SBU,

  -- ── Destination DC identity ────────────────────────────────────────────────
  DEST_DC_NBR,
  DEST_DC_TYPE,

  -- ── STO in transit TO this DC (DC_TO_DC + STORE_TO_DC flows) ─────────────
  SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_UNITS,
  SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_COST  ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_COST,

  -- ── Cube (weighted avg across items flowing to this DC) ───────────────────
  SAFE_DIVIDE(
    SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS * ITEM_CUBE ELSE 0 END),
    NULLIF(SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END), 0)
  ) AS STO_WTAVG_CUBE

FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`
GROUP BY
  BUS_DT,
  WM_YEAR,
  WM_WEEK,
  WM_YR_WK_NBR,
  OMNI_DEPT_NBR,
  OMNI_DEPT_DESC,
  OMNI_CATG_NBR,
  OMNI_CATG_DESC,
  SBU,
  DEST_DC_NBR,
  DEST_DC_TYPE
ORDER BY
  BUS_DT DESC,
  DEST_DC_NBR,
  OMNI_CATG_NBR
;


-- ══════════════════════════════════════════════════════════════════════════════
-- Option B: Persist to a scheduled table for dashboard use
-- ══════════════════════════════════════════════════════════════════════════════
/*
CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_STO_DC_GRAIN`
PARTITION BY BUS_DT
CLUSTER BY DEST_DC_NBR, OMNI_CATG_NBR
AS (
  SELECT
    BUS_DT,
    WM_YEAR,
    WM_WEEK,
    WM_YR_WK_NBR,
    OMNI_DEPT_NBR,
    OMNI_DEPT_DESC,
    OMNI_CATG_NBR,
    OMNI_CATG_DESC,
    SBU,
    DEST_DC_NBR,
    DEST_DC_TYPE,
    SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_UNITS,
    SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_COST  ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_COST,
    SAFE_DIVIDE(
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS * ITEM_CUBE ELSE 0 END),
      NULLIF(SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END), 0)
    ) AS STO_WTAVG_CUBE
  FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`
  GROUP BY ALL
);
*/
