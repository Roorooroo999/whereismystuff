-- ══════════════════════════════════════════════════════════════════════════════
-- On-Order by SBU, OMNI Dept/Category, and WM Week
-- ══════════════════════════════════════════════════════════════════════════════
-- Purpose: On-order units by SBU and OMNI Dept/Category, grouped by WM fiscal week
--          Includes ordered, received, and open quantities
--          MABD starting from WM Week 12501
--          Excludes service items, discontinued, PFS vendors, etc.
--
-- Target Table: wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ONORDER
-- Scheduled Query: Daily refresh
--
-- Schema:
--   wm_week         INT64    - WM fiscal week (e.g., 12501 = FY25 Week 1)
--   sbu             STRING   - SBU (CONSUMABLES, FRESH, CAC, PANTRY, FASHION, ETS, HARDLINES, HOME, OTHER)
--   OMNI_DEPT_NBR   INT64    - OMNI Department number
--   OMNI_DEPT_DESC  STRING   - OMNI Department description
--   OMNI_CATG_NBR   INT64    - OMNI Category number
--   OMNI_CATG_DESC  STRING   - OMNI Category description
--   PO_TYPE_CD      INT64    - Raw PO type code from OMS_PURCHASE_ORDER
--   import_ind      STRING   - IMPORT (PO type 40,42,43) or DOMESTIC
--   replen_ind      STRING   - REPLEN or NON-REPLEN (based on ITEM_REPLENISHABLE_IND)
--   channel_ind     STRING   - ECOMM or STORE (based on PO_EVENT_ABBR)
--   dsd_ind         STRING   - DSD or NON-DSD (based on CHANNEL_MTHD_DESC)
--   vnpk_ordered    INT64    - Vendor packs ordered
--   units_ordered   INT64    - Units ordered (vnpk * pack qty)
--   vnpk_received   INT64    - Vendor packs received
--   units_received  INT64    - Units received
--   vnpk_open       INT64    - Vendor packs open (ordered - received)
--   units_open      INT64    - Units open
--   cube_ordered    FLOAT64  - Total cubic feet ordered (units_ordered × item_cube)
--   cube_open       FLOAT64  - Total cubic feet open (units_open × item_cube)
--
-- Indicators:
--   - import_ind:   IMPORT if PO_TYPE_CD IN (40,42,43), else DOMESTIC
--   - replen_ind:   REPLEN/NON-REPLEN based on ITEM_REPLENISHABLE_IND
--   - channel_ind:  ECOMM if PO_EVENT_ABBR starts with 'OL' or 'ONLINE', else STORE
--   - dsd_ind:      DSD if CHANNEL_MTHD_DESC = 'DSD', else NON-DSD
--
-- PO Type exclusions (applied in WHERE):
--   - 77, 73, 53 excluded (per business rules)
--
-- Import PO types:
--   - 40 = Import (Direct Import)
--   - 42 = Import (Agent Import)
--   - 43 = Import (Consolidation)
--
-- Sources:
--   - wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER
--   - wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE
--   - wmt-edw-prod.US_WM_VM.ITEM_CUR
--   - wmt-edw-prod.US_WM_VM.PFS_VENDOR_STORE
--   - wmt-edw-prod.US_WM_VM.CALENDAR_DAY
--   - wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly
--   - wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw (cube dims)
--
-- Exclusions:
--   - Service items (photo, tire, pharmacy, pet, salon services)
--   - Discontinued items
--   - PFS vendors (production indicator = 'P')
--   - Health & Wellness, Walmart Services segments
--   - Cancelled PO lines (status 1300)
--   - Environmental/deposit fee vendors (481890)
--   - Specific repl subtypes: PFS (99), production (16,89), non-PI (21), DSD type 7 (6)
--   - PO types: 77, 73, 53
--
-- Created: June 1, 2026
-- Updated: June 17, 2026 — added import_ind (PO_TYPE_CD IN (40,42,43) = IMPORT)
-- Updated: June 23, 2026 — added in_store_wm_week (IN_STORE_DATE → WM fiscal week)
-- Updated: June 23, 2026 — include cancelled lines (1300) where PO_BASE_DATA shows supplier shipped (1000/1100) [TEMP: sandbox]
-- Updated: June 24, 2026 — fix omni_mapping fanout: QUALIFY ROW_NUMBER() keeps 1 row per MDS_FAM_ID (was 120–200× inflation)
-- Updated: June 24, 2026 — fix EXISTS rescue: CAST(BOOKING_PO_NBR AS INT64) matches OMS INT64 (leading-zero STRING mismatch closed WK 12520 ~59M gap)
-- Updated: June 24, 2026 — replace sandbox PO_BASE_DATA with OMS self-join on distributed child POs — removes wmt-edw-sandbox dependency
-- Author: r0c0jug
-- ══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ONORDER` AS

WITH

-- ── PFS Vendors to exclude ───────────────────────────────────────────────────
pfs_vendors AS (
  SELECT DISTINCT
    (VENDOR_SEQ_NBR + 10 * DEPT_NBR + 1000 * VENDOR_NBR) AS pfs_key
  FROM `wmt-edw-prod.US_WM_VM.PFS_VENDOR_STORE`
  WHERE PFS_PRODUCTION_IND = 'P'
),

-- ── OMNI Category/Dept mapping from fd_omni_chnl_item_dly ────────────────────
-- 730-day window (table requires bus_dt partition filter — full scan blocked).
-- Covers FY25 (started ~Jan 2024) through today, including discontinued items.
-- QUALIFY keeps only the most recent row per MDS_FAM_ID — 1 row per item, no fanout.
-- (30-day window was dropping historical POs for discontinued items; 120–200× fanout
--  was inflating units when items spanned multiple category rows in the window.)
omni_mapping AS (
  SELECT
    MDS_FAM_ID,
    OMNI_DEPT_NBR,
    OMNI_DEPT_DESC,
    OMNI_CATG_NBR,
    OMNI_CATG_DESC
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
  WHERE OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND OMNI_DEPT_NBR IS NOT NULL
    AND BUS_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MDS_FAM_ID ORDER BY BUS_DT DESC) = 1
),

-- ── Item master with SBU mapping and exclusions ──────────────────────────────
item_base AS (
  SELECT
    ITEM_NBR,
    MDS_FAM_ID,
    ACCTG_DEPT_NBR,
    DEPT_NBR,
    FINELINE_NBR,
    DEPT_CATEGORY_NBR,
    DEPT_CATG_GRP_NBR,
    REPL_SUBTYPE_CODE,
    VENDOR_NBR,
    VENDOR_SEQ_NBR,
    VENDOR_DEPT_NBR,
    VENDOR_NAME,
    ITEM1_DESC,
    ITEM_REPLENISHABLE_IND,
    -- SBU mapping (dept 81 under FRESH)
    CASE
      WHEN ACCTG_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN ACCTG_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN ACCTG_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN ACCTG_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN ACCTG_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN ACCTG_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      WHEN ACCTG_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN ACCTG_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      ELSE 'OTHER'
    END AS sbu,
    -- Item exception flag
    CASE
      WHEN FINELINE_NBR = 8023 AND DEPT_NBR = 42 THEN 1                              -- photo service
      WHEN ACCTG_DEPT_NBR = 19  AND DEPT_CATEGORY_NBR = 3072 THEN 1                  -- tire service
      WHEN ACCTG_DEPT_NBR = 91  AND DEPT_CATEGORY_NBR = 3201 THEN 1                  -- DISPLAY
      WHEN ACCTG_DEPT_NBR = 95  AND FINELINE_NBR IN (9225,9226,9998,2353) THEN 1     -- DSD OR SPECIAL VENDOR PROGRAM
      WHEN ACCTG_DEPT_NBR = 40  AND DEPT_CATEGORY_NBR IN (1808,1809,1788) THEN 1     -- PHARMACY SERVICES
      WHEN ACCTG_DEPT_NBR = 8   AND DEPT_CATEGORY_NBR IN (676) THEN 1                -- PET SERVICE
      WHEN ACCTG_DEPT_NBR = 2   AND DEPT_CATEGORY_NBR IN (546,542) THEN 1            -- SALON SERVICE
      WHEN ACCTG_DEPT_NBR = 95  AND VENDOR_NAME = 'MCLANE COMPANY' THEN 1            -- DSD convenience items
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
    END AS item_exception
  FROM `wmt-edw-prod.US_WM_VM.ITEM_CUR`
  WHERE ITEM_NBR IS NOT NULL
    -- Exclude departments
    AND ACCTG_DEPT_NBR NOT IN (99,39,37,38,49,85,60,83,86,88)
    -- Exclude segments
    AND MDSE_SEGMENT_DESC NOT IN ('HEALTH AND WELLNESS','WALMART SERVICES')
    -- Exclude environmental/deposit fees vendor
    AND VENDOR_NBR <> 481890
    -- Exclude repl subtypes: PFS (99), production (16,89), non-PI (21), DSD type 7 (6)
    AND REPL_SUBTYPE_CODE NOT IN (99, 16, 89, 21, 6)
    -- Must have category
    AND DEPT_CATEGORY_NBR IS NOT NULL
    -- Exclude specific dept+category combinations
    AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(DEPT_CATEGORY_NBR AS STRING)) NOT IN ('821425','821435','872873')
    AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(DEPT_CATG_GRP_NBR AS STRING)) NOT IN ('826379')
    AND CONCAT(CAST(ACCTG_DEPT_NBR AS STRING), CAST(REPL_SUBTYPE_CODE AS STRING)) NOT IN ('8021','8014','9821')
),

-- ── Final item list excluding PFS vendors and item exceptions ────────────────
item AS (
  SELECT
    i.ITEM_NBR,
    i.MDS_FAM_ID,
    i.ACCTG_DEPT_NBR,
    i.sbu,
    i.ITEM_REPLENISHABLE_IND
  FROM item_base i
  LEFT JOIN pfs_vendors pfs
    ON (i.VENDOR_SEQ_NBR + 10 * i.VENDOR_DEPT_NBR + 1000 * i.VENDOR_NBR) = pfs.pfs_key
  WHERE pfs.pfs_key IS NULL           -- Exclude PFS vendors
    AND i.item_exception = 0          -- Exclude item exceptions
),

-- ── Item cube dimensions (pkg height × width × length / 1728 = cubic feet) ───
item_dims AS (
  SELECT
    MDS_FAM_ID,
    SAFE_DIVIDE(PKG_HT_IN_QTY * PKG_WDTH_IN_QTY * PKG_LEN_IN_QTY, 1728) AS item_cube
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
  WHERE VAL_IND     = 1
    AND OP_CMPNY_CD = 'WMT-US'
    AND MDS_FAM_ID <> -1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MDS_FAM_ID ORDER BY MDS_FAM_ID) = 1
),

-- ── Calendar for WM fiscal week ──────────────────────────────────────────────
calendar AS (
  SELECT DISTINCT
    GREGORIAN_DATE,
    WM_YR_WK
  FROM `wmt-edw-prod.US_WM_VM.CALENDAR_DAY`
  WHERE WM_YR_WK >= 12501
),

-- ── Calendar for IN_STORE_DATE → WM fiscal week ──────────────────────────────
calendar_ins AS (
  SELECT DISTINCT
    GREGORIAN_DATE,
    WM_YR_WK
  FROM `wmt-edw-prod.US_WM_VM.CALENDAR_DAY`
  WHERE WM_YR_WK >= 12501
)

-- ══════════════════════════════════════════════════════════════════════════════
-- FINAL OUTPUT: On-Order by WM Week, SBU, OMNI Dept/Category with Indicators
-- ══════════════════════════════════════════════════════════════════════════════
SELECT
  cal.WM_YR_WK                AS wm_week,
  cal_ins.WM_YR_WK            AS in_store_wm_week,
  itm.sbu,
  omni.OMNI_DEPT_NBR,
  omni.OMNI_DEPT_DESC,
  omni.OMNI_CATG_NBR,
  omni.OMNI_CATG_DESC,
  po.PO_TYPE_CD,

  -- Indicators
  CASE WHEN po.PO_TYPE_CD IN (40,42,43) THEN 'IMPORT' ELSE 'DOMESTIC' END AS import_ind,
  CASE WHEN itm.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS replen_ind,
  CASE WHEN UPPER(pol.PO_EVENT_ABBR) LIKE 'OL%' OR UPPER(pol.PO_EVENT_ABBR) LIKE 'ONLINE%' THEN 'ECOMM' ELSE 'STORE' END AS channel_ind,
  CASE WHEN pol.CHANNEL_MTHD_DESC = 'DSD' THEN 'DSD' ELSE 'NON-DSD' END AS dsd_ind,

  -- Ordered
  SUM(pol.VNPK_ORD_QTY) AS vnpk_ordered,
  SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY) AS units_ordered,

  -- Received
  SUM(COALESCE(pol.VNPK_RCV_QTY, 0)) AS vnpk_received,
  SUM(COALESCE(pol.VNPK_RCV_QTY, 0) * pol.VNPK_QTY) AS units_received,

  -- Open (Ordered - Received)
  SUM(pol.VNPK_ORD_QTY - COALESCE(pol.VNPK_RCV_QTY, 0)) AS vnpk_open,
  SUM((pol.VNPK_ORD_QTY - COALESCE(pol.VNPK_RCV_QTY, 0)) * pol.VNPK_QTY) AS units_open,

  -- Total cubic feet ordered / open
  SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY * COALESCE(dims.item_cube, 0))                                    AS cube_ordered,
  SUM((pol.VNPK_ORD_QTY - COALESCE(pol.VNPK_RCV_QTY, 0)) * pol.VNPK_QTY * COALESCE(dims.item_cube, 0)) AS cube_open

FROM `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` po
JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` pol
  ON pol.OMS_PO_NBR = po.OMS_PO_NBR
JOIN item itm
  ON itm.ITEM_NBR = pol.ITEM_NBR
JOIN omni_mapping omni
  ON omni.MDS_FAM_ID = itm.MDS_FAM_ID
JOIN calendar cal
  ON cal.GREGORIAN_DATE = pol.MABD_DATE
LEFT JOIN calendar_ins cal_ins
  ON cal_ins.GREGORIAN_DATE = po.IN_STORE_DATE
LEFT JOIN item_dims dims
  ON dims.MDS_FAM_ID = itm.MDS_FAM_ID
WHERE po.COUNTRY_CODE = 'US'
  AND (
    pol.PO_LINE_STATUS_CD NOT IN (1300)   -- Normal: exclude cancelled lines
    OR (
      pol.PO_LINE_STATUS_CD = 1300        -- Exception: cancelled in OMS after supplier shipped
      -- Rescue via OMS self-join: find distributed child POs of this booking PO that were
      -- shipped/received (status 1000/1100). Replaces sandbox PO_BASE_DATA dependency.
      -- Use EXISTS (not JOIN) to avoid fanout — each booking PO line can have many child PO rows.
      AND EXISTS (
        SELECT 1
        FROM `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` dist_po
        JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` dist_pol
          ON dist_pol.OMS_PO_NBR = dist_po.OMS_PO_NBR
        WHERE dist_po.BOOKING_PO_NBR          = po.OMS_PO_NBR
          AND dist_po.OMS_PO_NBR             <> dist_po.BOOKING_PO_NBR
          AND dist_pol.OMS_PO_LINE_NBR        = pol.OMS_PO_LINE_NBR
          AND dist_pol.PO_LINE_STATUS_CD IN (1000, 1100)
      )
    )
  )
  AND po.PO_TYPE_CD NOT IN (77,73,53)      -- Exclude per business rules
  AND (
    COALESCE(CAST(po.BOOKING_PO_NBR AS STRING), '0') = '0'                -- domestic POs (BOOKING_PO_NBR = 0 or NULL)
    OR CAST(po.OMS_PO_NBR AS STRING) = CAST(po.BOOKING_PO_NBR AS STRING)  -- imports: original booking PO only (excludes distributed child POs)
  )

GROUP BY
  cal.WM_YR_WK,
  cal_ins.WM_YR_WK,
  itm.sbu,
  omni.OMNI_DEPT_NBR,
  omni.OMNI_DEPT_DESC,
  omni.OMNI_CATG_NBR,
  omni.OMNI_CATG_DESC,
  po.PO_TYPE_CD,
  CASE WHEN po.PO_TYPE_CD IN (40,42,43) THEN 'IMPORT' ELSE 'DOMESTIC' END,
  CASE WHEN itm.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END,
  CASE WHEN UPPER(pol.PO_EVENT_ABBR) LIKE 'OL%' OR UPPER(pol.PO_EVENT_ABBR) LIKE 'ONLINE%' THEN 'ECOMM' ELSE 'STORE' END,
  CASE WHEN pol.CHANNEL_MTHD_DESC = 'DSD' THEN 'DSD' ELSE 'NON-DSD' END
;
