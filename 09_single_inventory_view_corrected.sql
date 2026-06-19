-- ============================================================
-- SCRIPT: 09_single_inventory_view_corrected.sql
-- PURPOSE: ONE SINGLE VIEW - All Inventory Stages in One Table
-- VERSION: 3.0 (Added WTAVG_CUBE per stage; current-week non-Friday date)
-- ============================================================
--
-- CHANGES IN v3.0:
--   1. friday_dates: Adds yesterday's date when today is Mon–Thu so the
--      current (incomplete) WM week shows the most recent day's snapshot
--      instead of waiting for Friday (e.g., WK26 Wednesday → Tuesday data)
--   2. WTAVG_CUBE: Added to every stage as weighted-average item cube
--      (PKG_HT * PKG_WDTH * PKG_LEN / 1728 ft³) weighted by UNITS.
--      Uses item_dims CTE (already defined) via LEFT JOIN.
--
-- CHANGES IN v2.9:
--   1. On Order: Changed to NEXT week's MABD (not same week)
--   2. Cost/Retail: Added fd_omni_cost_lookup CTE for stages without native cost
--   3. Stage 1: Cost/Retail from OMS (VNPK_COST_AMT, BASE_UNIT_RTL_AMT)
--   4. Stages 2,3,5,5.5,7.5: Cost/Retail from fd_omni_cost_lookup
--   5. Stage 4: Added retail from lookup
--   6. Stage 6.5: Added retail from lookup (column doesn't exist in source)
--   7. DSD_IND: Maintained for Stage 1 filtering
--   8. Stage 9: Added Store Staged (online orders awaiting pickup, from 202501)
--
-- STAGES:
--   1_ON_ORDER           - POs by NEXT week's MABD - WITH DSD_IND
--   2_IN_TRANSIT_TO_DC   - DC-to-DC in-transit (RDC/FDC + GDC)
--   3_ON_YARD            - Trailers at DC yard (cumulative)
--   4_DC_ON_HAND         - DC warehouse inventory
--   5_DC_LABELED_STAGED  - DC labeled + staged inventory
--   5.5_DC_UNLABELED     - DC unlabeled inventory
--   6_IN_TRANSIT_TO_STORE - DC->Store shipments
--   6.5_DC_RESERVED      - Store-allocated DC inventory
--   7_STORE_ON_HAND      - Store floor + backroom
--   7.5_BACKROOM         - Backroom portion of store OH
--   8_FC_ON_HAND         - Fulfillment center inventory
--   9_STORE_STAGED       - Online orders in store awaiting pickup (not in OH)
--
-- ============================================================

DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_SINGLE_INVENTORY_VIEW`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_SINGLE_INVENTORY_VIEW`
PARTITION BY BUS_DT
CLUSTER BY INVENTORY_STAGE, NODE_TYPE, SBU
AS (

-- ══════════════════════════════════════════════════════════════
-- STEP 0: ITEM_CUR (Consistent with Historical Pipeline)
-- Apply standard Walmart exclusions across ALL stages
-- ══════════════════════════════════════════════════════════════
WITH ITEM_CUR AS (
  SELECT *
  FROM (
    SELECT DISTINCT
      MDS_FAM_ID,
      ITEM_NBR,
      ITEM_REPLENISHABLE_IND,
      ACCTG_DEPT_NBR AS DEPT_NBR,
      WHPK_QTY,
      VENDOR_NBR,
      VENDOR_SEQ_NBR,
      VENDOR_DEPT_NBR,
      -- SBU mapping (matches historical pipeline)
      CASE
        WHEN ACCTG_DEPT_NBR IN (92,95) THEN 'PANTRY'
        WHEN ACCTG_DEPT_NBR IN (2,4,8,13,40,46,79) THEN 'CONSUMABLES'
        WHEN ACCTG_DEPT_NBR IN (3,9,10,11,12,16,19,56) THEN 'HARDLINES'
        WHEN ACCTG_DEPT_NBR IN (90,91,1,82,96) THEN 'CAC'
        WHEN ACCTG_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
        WHEN ACCTG_DEPT_NBR IN (14,17,20,22,71,74) THEN 'HOME'
        WHEN ACCTG_DEPT_NBR IN (80,81,93,94,97,98) THEN 'FRESH'
        WHEN ACCTG_DEPT_NBR IN (5,6,7,18,21,67,72,87) THEN 'ETS'
        ELSE 'OTHER'
      END AS SBU,
      -- Item exception flag
      CASE
        WHEN FINELINE_NBR = 8023 AND DEPT_NBR = 42 THEN 1
        WHEN ACCTG_DEPT_NBR = 19  AND DEPT_CATEGORY_NBR = 3072 THEN 1
        WHEN ACCTG_DEPT_NBR = 91  AND DEPT_CATEGORY_NBR = 3201 THEN 1
        WHEN ACCTG_DEPT_NBR = 95  AND FINELINE_NBR IN (9225,9226,9998,2353) THEN 1
        WHEN ACCTG_DEPT_NBR = 40  AND DEPT_CATEGORY_NBR IN (1808,1809,1788) THEN 1
        WHEN ACCTG_DEPT_NBR = 8   AND DEPT_CATEGORY_NBR IN (676) THEN 1
        WHEN ACCTG_DEPT_NBR = 2   AND DEPT_CATEGORY_NBR IN (546,542) THEN 1
        WHEN ACCTG_DEPT_NBR = 95  AND VENDOR_NAME = 'MCLANE COMPANY' THEN 1
        WHEN ITEM1_DESC IN (
          'PRICE ADJ','BR 15LB EXCHANGE','AMERIG PROPANE EXCH','0','1','4','42','72',
          '123','1408','1800','1843','1865','1917','1984','2067','71239','436166','453092',
          '9509766','133538321','36881149149','36881431565','36881455820','70616024240',
          '72286839404','323900040953','719410785109','861368000220',
          '*** DISCONTINUED***','***DISCONTINUED BY C','***DISCONTINUED BY V','***DISCONTINUED***',
          '***DISCONTINUED*** B','***DISCONTINUED*** L','***DISCONTINUED***BD',
          '***DISCONTINUED***CO','***DISCONTINUED***GL','***DISCONTINUED***HA',
          '***DISCONTINUED***LA','***DISCONTINUED***LY','***DISCONTINUED***MA',
          '***DISCONTINUED***MR','***DISCONTINUED***RU','***DISCONTINUED***TO',
          '**DISCONTINUED BY VE','**DISCONTINUED**','**DISCONTINUED** ARR',
          'DIESEL','REG GAS UNLEADED','REUSABLE BAGS','REUSABLE BAGS ASST 2',
          'FUEL PREPAY','HI-GRADE PREMIUM GAS','UNLEADED CLEAR','CAROUSEL REUSABLE BG'
        ) THEN 1
        ELSE 0
      END AS ITEM_EXCEPTIONS
    FROM `wmt-edw-prod.US_WM_VM.ITEM_CUR`
    WHERE ACCTG_DEPT_NBR NOT IN (99,39,37,38,49,85,60,83,86,88)
      AND MDSE_SEGMENT_DESC NOT IN ('HEALTH AND WELLNESS','WALMART SERVICES')
      AND VENDOR_NBR <> 481890
      AND REPL_SUBTYPE_CODE NOT IN (99, 16, 89, 21, 6)
      AND DEPT_CATEGORY_NBR IS NOT NULL
      AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(DEPT_CATEGORY_NBR AS STRING)) NOT IN ('821425','821435','872873')
      AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(DEPT_CATG_GRP_NBR AS STRING)) NOT IN ('826379')
      AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(REPL_SUBTYPE_CODE AS STRING)) NOT IN ('8021','8014','9821')
      -- Exclude PFS vendors
      AND (VENDOR_SEQ_NBR + 10 * VENDOR_DEPT_NBR + 1000 * VENDOR_NBR) NOT IN (
          SELECT (VENDOR_SEQ_NBR + 10 * DEPT_NBR + 1000 * VENDOR_NBR)
          FROM `wmt-edw-prod.US_WM_VM.PFS_VENDOR_STORE`
          WHERE PFS_PRODUCTION_IND = 'P'
          GROUP BY VENDOR_NBR, DEPT_NBR, VENDOR_SEQ_NBR
      )
  )
  WHERE ITEM_EXCEPTIONS = 0
),

-- ══════════════════════════════════════════════════════════════
-- Snapshot dates:
--   - Fridays (standard WM week-end snapshot; Sat/Sun naturally get last Friday)
--   - OR yesterday (current_date - 1) when run Mon–Thu for any mid-week ad-hoc refresh
--     NOTE: Friday data pipelines don't complete until Saturday, so on Friday
--     the most recent available data is Thursday's — use < CURRENT_DATE() (not <=)
--
-- Day-by-day behavior:
--   Mon–Thu  → last Friday + yesterday (current_date - 1)
--   Fri      → last Friday only (today's data not yet available; < excludes today)
--   Sat/Sun  → last Friday (yesterday/2-days-ago qualifies via EXTRACT=6)
-- ══════════════════════════════════════════════════════════════
friday_dates AS (
  SELECT DISTINCT
    CAL_DT AS BUS_DT,
    WM_FULL_YR_NBR AS WM_YEAR,
    WM_WK_NBR AS WM_WEEK,
    WM_YR_WK_NBR
  FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
  WHERE (
    -- Standard: Friday snapshots for all completed weeks
    EXTRACT(DAYOFWEEK FROM CAL_DT) = 6
    -- Mid-week: any Mon–Thu refresh uses yesterday's snapshot
    -- DAYOFWEEK: 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat
    OR (
      CAL_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
      AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 5
    )
  )
    AND CAL_DT >= '2025-02-07'                 -- FY25 WK01
    AND CAL_DT < CURRENT_DATE('America/Chicago')  -- excludes today (Friday data not ready until Sat)
),

-- OMNI mapping (Walmart US only)
omni_mapping AS (
  SELECT DISTINCT
    MDS_FAM_ID,
    OMNI_DEPT_NBR,
    OMNI_DEPT_DESC,
    OMNI_CATG_NBR,
    OMNI_CATG_DESC
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
  WHERE VAL_IND = 1
    AND OP_CMPNY_CD = 'WMT-US'
    AND OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND OMNI_DEPT_NBR IS NOT NULL
),

-- DC dimension with consistent type mapping
dc_dim AS (
  SELECT
    DC_NBR,
    DC_TYPE_DESC,
    CASE
      WHEN DC_TYPE_DESC = 'Regional' THEN 'RDC'
      WHEN DC_TYPE_DESC = 'Fashion' THEN 'FDC'
      WHEN DC_TYPE_DESC IN ('Grocery', 'Grocery Flow') THEN 'GDC'
      WHEN DC_TYPE_DESC IN ('Imports', 'Import Flow', 'GIDC', 'IMFC') THEN 'IDC'
      WHEN DC_TYPE_DESC LIKE '%Dotcom%' OR DC_TYPE_DESC LIKE '%Sortable%'
           OR DC_TYPE_DESC LIKE '%Fulfillment%' THEN 'FC'
      WHEN DC_TYPE_DESC = 'Market Service Center' THEN 'MSC'
      WHEN DC_TYPE_DESC = 'Support' THEN 'SUPPORT'
      ELSE 'OTHER_DC'
    END AS DC_CATEGORY
  FROM `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR`
  WHERE COUNTRY_CODE = 'US'
    AND CURRENT_IND = 'Y'
    AND DC_TYPE_DESC IS NOT NULL
    AND DC_TYPE_DESC NOT IN ('Sams Import', 'Sams Club')
    AND DC_TYPE_DESC NOT LIKE 'Sam%'
    AND UPPER(DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')
),

-- ══════════════════════════════════════════════════════════════
-- COST/RETAIL LOOKUP from fd_omni (for stages without native cost)
-- ══════════════════════════════════════════════════════════════
fd_omni_cost_lookup AS (
  SELECT
    FD.BUS_DT,
    FD.MDS_FAM_ID,
    SAFE_DIVIDE(
      SUM(COALESCE(FD.TY_ON_HAND_COST_AMT, 0) + COALESCE(FD.TY_FC_ON_HAND_COST_AMT, 0) + COALESCE(FD.TY_IN_TRNST_COST_AMT, 0)),
      NULLIF(SUM(COALESCE(FD.TY_ON_HAND_QTY, 0) + COALESCE(FD.TY_FC_ON_HAND_UNIT_QTY, 0) + COALESCE(FD.TY_IN_TRNST_QTY, 0)), 0)
    ) AS AVG_COST_PER_UNIT,
    SAFE_DIVIDE(
      SUM(COALESCE(FD.TY_ON_HAND_RTL_AMT, 0) + COALESCE(FD.TY_FC_ON_HAND_RTL_AMT, 0) + COALESCE(FD.TY_IN_TRNST_RTL_AMT, 0)),
      NULLIF(SUM(COALESCE(FD.TY_ON_HAND_QTY, 0) + COALESCE(FD.TY_FC_ON_HAND_UNIT_QTY, 0) + COALESCE(FD.TY_IN_TRNST_QTY, 0)), 0)
    ) AS AVG_RETAIL_PER_UNIT
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` FD
  JOIN friday_dates F ON FD.BUS_DT = F.BUS_DT
  WHERE FD.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND (
      COALESCE(FD.TY_ON_HAND_QTY, 0) > 0
      OR COALESCE(FD.TY_FC_ON_HAND_UNIT_QTY, 0) > 0
      OR COALESCE(FD.TY_IN_TRNST_QTY, 0) > 0
    )
  GROUP BY FD.BUS_DT, FD.MDS_FAM_ID
),

-- Calendar for MABD mapping
calendar AS (
  SELECT DISTINCT
    CAL_DT AS GREGORIAN_DATE,
    WM_YR_WK_NBR
  FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
  WHERE CAL_DT >= '2025-02-07'
),

-- ══════════════════════════════════════════════════════════════
-- Item cube dimensions (cubic feet per unit)
-- WTAVG_CUBE = SAFE_DIVIDE(SUM(UNITS * ITEM_CUBE), NULLIF(SUM(UNITS),0))
-- ══════════════════════════════════════════════════════════════
item_dims AS (
  SELECT
    MDS_FAM_ID,
    SAFE_DIVIDE(PKG_HT_IN_QTY * PKG_WDTH_IN_QTY * PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
  WHERE VAL_IND     = 1
    AND OP_CMPNY_CD = 'WMT-US'
    AND MDS_FAM_ID <> -1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MDS_FAM_ID ORDER BY MDS_FAM_ID) = 1
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 1: ON ORDER (by CURRENT week's MABD)
-- Shows POs whose Must-Arrive-By Date falls in the SAME WM week
-- as the snapshot date (F.BUS_DT). This aligns on-order with the
-- current inventory position — WK20 snapshot shows WK20 arriving POs.
-- ══════════════════════════════════════════════════════════════
stage_1_on_order AS (
  SELECT
    F.BUS_DT,
    F.WM_YEAR,
    F.WM_WEEK,
    F.WM_YR_WK_NBR,
    '1_ON_ORDER' AS INVENTORY_STAGE,
    'ON_ORDER' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR,
    OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CASE WHEN pol.CHANNEL_MTHD_DESC = 'DSD' THEN 'DSD' ELSE 'NON-DSD' END AS DSD_IND,
    SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY) AS UNITS,
    SUM(pol.VNPK_ORD_QTY * COALESCE(pol.VNPK_COST_AMT, 0)) AS COST_AMT,
    SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY * COALESCE(pol.BASE_UNIT_RTL_AMT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` po
  JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` pol ON pol.OMS_PO_NBR = po.OMS_PO_NBR
  CROSS JOIN friday_dates F
  -- Match POs where MABD falls in the CURRENT (same) WM week as snapshot
  JOIN calendar cal ON cal.GREGORIAN_DATE = pol.MABD_DATE
    AND cal.WM_YR_WK_NBR = F.WM_YR_WK_NBR
  INNER JOIN ITEM_CUR ITM ON ITM.ITEM_NBR = pol.ITEM_NBR
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = ITM.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = ITM.MDS_FAM_ID
  WHERE po.COUNTRY_CODE = 'US'
    AND pol.PO_LINE_STATUS_CD NOT IN (1300)
    AND pol.VNPK_ORD_QTY > 0
    AND cal.WM_YR_WK_NBR BETWEEN 202501 AND 202752
  GROUP BY
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CASE WHEN pol.CHANNEL_MTHD_DESC = 'DSD' THEN 'DSD' ELSE 'NON-DSD' END
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 2: IN TRANSIT TO DC (from CDI_ATLAS)
-- ══════════════════════════════════════════════════════════════
stage_2_rdc_intransit AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '2_IN_TRANSIT_TO_DC' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(cdi.IT_INTRANSIT_QTY) AS UNITS,
    SUM(cdi.IT_INTRANSIT_QTY * COALESCE(FCL.AVG_COST_PER_UNIT, 0)) AS COST_AMT,
    SUM(cdi.IT_INTRANSIT_QTY * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(cdi.IT_INTRANSIT_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(cdi.IT_INTRANSIT_QTY), 0)
    ) AS WTAVG_CUBE
  FROM friday_dates F
  JOIN `wmt-edw-prod.US_SUPPLY_CHAIN_AMBIENT_RAW_VM.CDI_ATLAS_INV_ITEM_INVENTORY` cdi
    ON DATE(cdi.MERGE_TS) = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.ITEM_NBR = cdi.IT_ITEM_NBR
  LEFT JOIN dc_dim dc ON dc.DC_NBR = cdi.IT_DC_NUM
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = ITM.MDS_FAM_ID
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = ITM.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = ITM.MDS_FAM_ID
  WHERE cdi.IT_INTRANSIT_QTY > 0
    AND dc.DC_CATEGORY IS NOT NULL
  GROUP BY ALL
),

stage_2_gdc_intransit AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '2_IN_TRANSIT_TO_DC' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(cdi.IT_INTRANSIT_QTY) AS UNITS,
    SUM(cdi.IT_INTRANSIT_QTY * COALESCE(FCL.AVG_COST_PER_UNIT, 0)) AS COST_AMT,
    SUM(cdi.IT_INTRANSIT_QTY * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(cdi.IT_INTRANSIT_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(cdi.IT_INTRANSIT_QTY), 0)
    ) AS WTAVG_CUBE
  FROM friday_dates F
  JOIN `wmt-edw-prod.US_SUPPLY_CHAIN_GROCERY_RAW_VM.CDI_ATLAS_INV_ITEM_INVENTORY` cdi
    ON DATE(cdi.MERGE_TS) = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.ITEM_NBR = cdi.IT_ITEM_NBR
  LEFT JOIN dc_dim dc ON dc.DC_NBR = cdi.FACILITY_NUMBER
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = ITM.MDS_FAM_ID
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = ITM.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = ITM.MDS_FAM_ID
  WHERE cdi.IT_INTRANSIT_QTY > 0
    AND dc.DC_CATEGORY IS NOT NULL
  GROUP BY ALL
),

stage_2_intransit_to_dc AS (
  SELECT * FROM stage_2_rdc_intransit
  UNION ALL
  SELECT * FROM stage_2_gdc_intransit
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 3: ON YARD (Cumulative position AS OF snapshot date)
-- ══════════════════════════════════════════════════════════════
po_first_gate AS (
  SELECT
    PO_NBR,
    WHSE_NBR AS DC_NBR,
    MIN(DATE(LEFT(GATE_IN_DATE, 10))) AS first_gate_date
  FROM `wmt-cp-prod.TUP.RDC_FDC_EVENT_POS_HIST`
  WHERE TRAILER_STATUS_CODE IN ('IN', 'XI', 'CI', 'XD')
    AND PO_NBR IS NOT NULL
    AND GATE_IN_DATE IS NOT NULL
  GROUP BY PO_NBR, WHSE_NBR
),

daily_on_yard AS (
  SELECT
    pfg.first_gate_date AS GATE_DATE,
    ITM.MDS_FAM_ID,
    pfg.DC_NBR,
    SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY) AS ON_YARD_UNITS,
    SUM(pol.VNPK_ORD_QTY * COALESCE(pol.VNPK_COST_AMT, 0)) AS ON_YARD_COST,
    SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY * COALESCE(pol.BASE_UNIT_RTL_AMT, 0)) AS ON_YARD_RETAIL
  FROM `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` po
  JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` pol ON pol.OMS_PO_NBR = po.OMS_PO_NBR
  INNER JOIN (
    SELECT DISTINCT ic.ITEM_NBR, ic.MDS_FAM_ID
    FROM ITEM_CUR ic
    WHERE ic.MDS_FAM_ID IS NOT NULL
  ) ITM ON ITM.ITEM_NBR = pol.ITEM_NBR
  JOIN po_first_gate pfg ON pfg.PO_NBR = po.PO_NBR
  WHERE po.COUNTRY_CODE = 'US'
    AND pol.PO_LINE_STATUS_CD NOT IN (1300)
    AND pfg.first_gate_date >= '2025-02-07'
    AND pol.VNPK_ORD_QTY > 0
  GROUP BY pfg.first_gate_date, ITM.MDS_FAM_ID, pfg.DC_NBR
),

stage_3_on_yard AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '3_ON_YARD' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(DOY.ON_YARD_UNITS) AS UNITS,
    SUM(DOY.ON_YARD_COST) AS COST_AMT,
    SUM(DOY.ON_YARD_RETAIL) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(DOY.ON_YARD_UNITS * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(DOY.ON_YARD_UNITS), 0)
    ) AS WTAVG_CUBE
  FROM friday_dates F
  JOIN daily_on_yard DOY ON DOY.GATE_DATE = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = DOY.MDS_FAM_ID
  LEFT JOIN dc_dim dc ON dc.DC_NBR = DOY.DC_NBR
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = DOY.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = DOY.MDS_FAM_ID
  WHERE dc.DC_CATEGORY IN ('RDC', 'FDC', 'GDC', 'IDC')
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 4: DC ON-HAND
-- ══════════════════════════════════════════════════════════════
stage_4_dc_on_hand AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '4_DC_ON_HAND' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY) AS UNITS,
    SUM(WDS.ON_HAND_QTY * COALESCE(WDS.WHPK_COST_AMT, 0)) AS COST_AMT,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-edw-prod.US_WM_VM.WHSE_DLY_SKU` WDS
  JOIN friday_dates F ON WDS.INVENTORY_DATE = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = WDS.ITEM_NBR
  LEFT JOIN dc_dim dc ON dc.DC_NBR = WDS.WHSE_NBR
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = WDS.ITEM_NBR
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = WDS.ITEM_NBR
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = WDS.ITEM_NBR
  WHERE WDS.ON_HAND_QTY > 0
    AND dc.DC_CATEGORY NOT IN ('FC')
    AND dc.DC_CATEGORY IS NOT NULL
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 5: DC LABELED + STAGED
-- ══════════════════════════════════════════════════════════════
stage_5_dc_labeled AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '5_DC_LABELED_STAGED' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1)) AS UNITS,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(FCL.AVG_COST_PER_UNIT, 0)) AS COST_AMT,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1)), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-edw-prod.US_WM_VM.DC_DLY_INVT_TYPE` DIT
  JOIN friday_dates F ON DIT.INVENTORY_DATE = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN dc_dim dc ON dc.DC_NBR = DIT.DC_NBR
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = DIT.ITEM_NBR
  WHERE DIT.INVT_TYPE_CODE IN ('L', 'S')
    AND DIT.WHPK_INVT_QTY > 0
    AND dc.DC_CATEGORY NOT IN ('FC')
    AND dc.DC_CATEGORY IS NOT NULL
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 5.5: DC UNLABELED
-- ══════════════════════════════════════════════════════════════
stage_5_5_dc_unlabeled AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '5.5_DC_UNLABELED' AS INVENTORY_STAGE,
    dc.DC_CATEGORY AS NODE_TYPE,
    dc.DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1)) AS UNITS,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(FCL.AVG_COST_PER_UNIT, 0)) AS COST_AMT,
    SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1) * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(DIT.WHPK_INVT_QTY * COALESCE(ITM.WHPK_QTY, 1)), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-edw-prod.US_WM_VM.DC_DLY_INVT_TYPE` DIT
  JOIN friday_dates F ON DIT.INVENTORY_DATE = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN dc_dim dc ON dc.DC_NBR = DIT.DC_NBR
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = DIT.ITEM_NBR
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = DIT.ITEM_NBR
  WHERE DIT.INVT_TYPE_CODE = 'U'
    AND DIT.WHPK_INVT_QTY > 0
    AND dc.DC_CATEGORY NOT IN ('FC')
    AND dc.DC_CATEGORY IS NOT NULL
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 6: IN-TRANSIT DC -> STORE
-- ══════════════════════════════════════════════════════════════
stage_6_in_transit AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '6_IN_TRANSIT_TO_STORE' AS INVENTORY_STAGE,
    'DC->STORE' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    FD.OMNI_DEPT_NBR, FD.OMNI_DEPT_DESC,
    FD.OMNI_CATG_NBR, FD.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(FD.TY_IN_TRNST_QTY) AS UNITS,
    SUM(COALESCE(FD.TY_IN_TRNST_COST_AMT, 0)) AS COST_AMT,
    SUM(COALESCE(FD.TY_IN_TRNST_RTL_AMT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(FD.TY_IN_TRNST_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(FD.TY_IN_TRNST_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` FD
  JOIN friday_dates F ON F.BUS_DT = FD.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = FD.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = FD.MDS_FAM_ID
  WHERE FD.FULFMT_CHNL_NM = 'Store'
    AND FD.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND FD.TY_IN_TRNST_QTY > 0
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 6.5: DC RESERVED
-- ══════════════════════════════════════════════════════════════
stage_6_5_dc_reserved AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '6.5_DC_RESERVED' AS INVENTORY_STAGE,
    'DC_RESERVED' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    FD.OMNI_DEPT_NBR, FD.OMNI_DEPT_DESC,
    FD.OMNI_CATG_NBR, FD.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(FD.TY_IN_WHSE_QTY) AS UNITS,
    SUM(COALESCE(FD.TY_IN_WHSE_COST_AMT, 0)) AS COST_AMT,
    SUM(FD.TY_IN_WHSE_QTY * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(FD.TY_IN_WHSE_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(FD.TY_IN_WHSE_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` FD
  JOIN friday_dates F ON F.BUS_DT = FD.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = FD.MDS_FAM_ID
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = FD.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = FD.MDS_FAM_ID
  WHERE FD.FULFMT_CHNL_NM = 'Store'
    AND FD.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND FD.TY_IN_WHSE_QTY > 0
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 7: STORE ON-HAND
-- ══════════════════════════════════════════════════════════════
stage_7_store_oh AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '7_STORE_ON_HAND' AS INVENTORY_STAGE,
    'STORE' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    FD.OMNI_DEPT_NBR, FD.OMNI_DEPT_DESC,
    FD.OMNI_CATG_NBR, FD.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(FD.TY_ON_HAND_QTY) AS UNITS,
    SUM(COALESCE(FD.TY_ON_HAND_COST_AMT, 0)) AS COST_AMT,
    SUM(COALESCE(FD.TY_ON_HAND_RTL_AMT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(FD.TY_ON_HAND_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(FD.TY_ON_HAND_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` FD
  JOIN friday_dates F ON FD.BUS_DT = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = FD.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = FD.MDS_FAM_ID
  WHERE FD.FULFMT_CHNL_NM = 'Store'
    AND FD.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND FD.TY_ON_HAND_QTY > 0
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 7.5: BACKROOM
-- ══════════════════════════════════════════════════════════════
stage_7_5_backroom AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '7.5_BACKROOM' AS INVENTORY_STAGE,
    'BACKROOM' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    CAST(BKRM.omni_dept_nbr AS INT64) AS OMNI_DEPT_NBR,
    BKRM.omni_dept_desc AS OMNI_DEPT_DESC,
    CAST(BKRM.omni_catg_nbr AS INT64) AS OMNI_CATG_NBR,
    BKRM.omni_catg_desc AS OMNI_CATG_DESC,
    BKRM.SBU_NM AS SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(
      CASE
        WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
        WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
        ELSE 0
      END
    ) AS UNITS,
    SUM(
      CASE
        WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
        WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
        ELSE 0
      END * COALESCE(FCL.AVG_COST_PER_UNIT, 0)
    ) AS COST_AMT,
    SUM(
      CASE
        WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
        WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
        ELSE 0
      END * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)
    ) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(
        CASE
          WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
          WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
          ELSE 0
        END * COALESCE(dims.ITEM_CUBE, 0)
      ),
      NULLIF(SUM(
        CASE
          WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
          WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
          ELSE 0
        END
      ), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-instockinventory-datamart.WM_AD_HOC.o0c046n_backroom_oh_inv_inv_sol_reporting_pre_20260126` BKRM
  JOIN friday_dates F
    ON (BKRM.WM_YEAR + 2000) = F.WM_YEAR
    AND BKRM.WM_WEEK = F.WM_WEEK
  LEFT JOIN fd_omni_cost_lookup FCL
    ON FCL.BUS_DT = F.BUS_DT
    AND FCL.MDS_FAM_ID = BKRM.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = BKRM.MDS_FAM_ID
  WHERE BKRM.WM_YEAR IN (25, 26)
    AND (
      (BKRM.WM_YEAR = 25 AND BKRM.BACKROOM_QTY_LY > 0)
      OR (BKRM.WM_YEAR = 26 AND BKRM.BACKROOM_QTY_TY > 0)
    )
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 8: FC ON-HAND (Walmart-owned only)
-- ══════════════════════════════════════════════════════════════
stage_8_fc_oh AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '8_FC_ON_HAND' AS INVENTORY_STAGE,
    'FC' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    FD.OMNI_DEPT_NBR, FD.OMNI_DEPT_DESC,
    FD.OMNI_CATG_NBR, FD.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(FD.TY_FC_ON_HAND_UNIT_QTY) AS UNITS,
    SUM(COALESCE(FD.TY_FC_ON_HAND_COST_AMT, 0)) AS COST_AMT,
    SUM(COALESCE(FD.TY_FC_ON_HAND_RTL_AMT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(FD.TY_FC_ON_HAND_UNIT_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(FD.TY_FC_ON_HAND_UNIT_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` FD
  JOIN friday_dates F ON FD.BUS_DT = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = FD.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = FD.MDS_FAM_ID
  WHERE FD.FULFMT_CHNL_NM = 'OWNED'
    AND FD.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND FD.TY_FC_ON_HAND_UNIT_QTY > 0
  GROUP BY ALL
),

-- ══════════════════════════════════════════════════════════════
-- STAGE 9: STORE STAGED INVENTORY
-- ══════════════════════════════════════════════════════════════
stage_9_store_staged AS (
  SELECT
    F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
    '9_STORE_STAGED' AS INVENTORY_STAGE,
    'STORE' AS NODE_TYPE,
    CAST(NULL AS STRING) AS DC_TYPE_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    ITM.SBU,
    CAST(NULL AS STRING) AS DSD_IND,
    SUM(STO.ORDER_QTY) AS UNITS,
    SUM(STO.ORDER_QTY * COALESCE(FCL.AVG_COST_PER_UNIT, 0)) AS COST_AMT,
    SUM(STO.ORDER_QTY * COALESCE(FCL.AVG_RETAIL_PER_UNIT, 0)) AS RETAIL_AMT,
    SAFE_DIVIDE(
      SUM(STO.ORDER_QTY * COALESCE(dims.ITEM_CUBE, 0)),
      NULLIF(SUM(STO.ORDER_QTY), 0)
    ) AS WTAVG_CUBE
  FROM `wmt-edw-prod.WW_MB_DL_VM.STORE_FULFMT_ORDER_ITEM_360` STO
  JOIN friday_dates F ON DATE(STO.RPT_DT) = F.BUS_DT
  INNER JOIN ITEM_CUR ITM ON ITM.MDS_FAM_ID = STO.MDS_FAM_ID
  LEFT JOIN omni_mapping OMNI ON OMNI.MDS_FAM_ID = STO.MDS_FAM_ID
  LEFT JOIN fd_omni_cost_lookup FCL ON FCL.BUS_DT = F.BUS_DT AND FCL.MDS_FAM_ID = STO.MDS_FAM_ID
  LEFT JOIN item_dims dims ON dims.MDS_FAM_ID = STO.MDS_FAM_ID
  WHERE UPPER(STO.ORDER_STATUS_DESC) IN ('CREATED', 'PO PICK COMPLETE', 'AWAITING_AUTHORIZATION')
    AND F.WM_YR_WK_NBR >= 202501
  GROUP BY ALL
)

-- ══════════════════════════════════════════════════════════════
-- FINAL UNION: All Stages Combined
-- Columns: BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, INVENTORY_STAGE,
--          NODE_TYPE, DC_TYPE_DESC, OMNI_DEPT_NBR, OMNI_DEPT_DESC,
--          OMNI_CATG_NBR, OMNI_CATG_DESC, SBU, DSD_IND,
--          UNITS, COST_AMT, RETAIL_AMT, WTAVG_CUBE
-- ══════════════════════════════════════════════════════════════
SELECT * FROM stage_1_on_order
UNION ALL
SELECT * FROM stage_2_intransit_to_dc
UNION ALL
SELECT * FROM stage_3_on_yard
UNION ALL
SELECT * FROM stage_4_dc_on_hand
UNION ALL
SELECT * FROM stage_5_dc_labeled
UNION ALL
SELECT * FROM stage_5_5_dc_unlabeled
UNION ALL
SELECT * FROM stage_6_in_transit
UNION ALL
SELECT * FROM stage_6_5_dc_reserved
UNION ALL
SELECT * FROM stage_7_store_oh
UNION ALL
SELECT * FROM stage_7_5_backroom
UNION ALL
SELECT * FROM stage_8_fc_oh
UNION ALL
SELECT * FROM stage_9_store_staged

);
