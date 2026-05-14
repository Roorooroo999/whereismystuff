-- ============================================================
-- SCRIPT : wheres_my_stuff_invt_rollup_vw.sql
-- DESC   : "Where's My Stuff" — Enterprise Inventory Rollup
--          Breaks down inventory position by store-network SKU across:
--            • Store      (repl_sku_ei_invt_dly → on_hand_qty + STORE_INFO_SNE)
--            • In-Transit (repl_sku_ei_invt_dly → in_trnst_qty)
--            • DC         (WHSE_DLY_SKU + DC_DIM_CUR with DC type breakdown)
--          Store and In-Transit share the same source table for consistency.
--          FC is built as a separate staging table (R0C0JUG_WMUS_FC_INVT)
--          but is not included in the rollup — FC uses a different item key
--          (CATLG_ITEM_ID) and is out of scope for the store-network view.
-- ============================================================
--
-- EXECUTION ORDER:
--   1.  ITEM_CUR                                  (TEMP)  — item exclusion base
--   2.  OMNI_STORE_HIERARCHY                      (TEMP)  — store merch hierarchy (MDS_FAM_ID)
--   3.  OMNI_ECOMM_HIERARCHY                      (TEMP)  — FC merch hierarchy (CATLG_ITEM_ID)
--   4.  R0C0JUG_WMUS_STORE_INVT          (TABLE) — store OH  [your section]
--   5.  R0C0JUG_WMUS_IN_TRANSIT_INVT     (TABLE) — in-transit  [your section]
--   6.  R0C0JUG_WMUS_DC_INVT             (TABLE) — DC OH by type  [DC team section]
--   7.  R0C0JUG_WMUS_FC_INVT             (TABLE) — FC OH (staging only — not in rollup)
--   8.  WHERES_MY_STUFF_ROLLUP           (TABLE) — final rollup (Store + Transit + DC)
-- ============================================================
--
-- ITEM KEY:
--   All three rollup networks key on MDS_FAM_ID (internal merch family ID).
--   WALMART_NBR = ITEM_NBR from OMNI_STORE_HIERARCHY (human-facing item number).
--
-- REPLEN TYPE:
--   ITEM_REPLENISHABLE_IND sourced from ITEM_CUR (wmt-edw-prod.US_WM_VM.ITEM_CUR).
--   All three networks (Store, Transit, DC) join to ITEM_CUR and use this column.
--   REPLEN_TYPE = 'REPLEN' when ITEM_REPLENISHABLE_IND = 'Y', else 'NON-REPLEN'.
-- ============================================================


/*--------------------------------------------------------------------
  TEMP: ITEM_CUR
  Source: wmt-edw-prod.US_WM_VM.ITEM_CUR
  Purpose: Clean item base with all standard Walmart exclusions.
  REPL_SUBTYPE_CODE is used in WHERE filters only (item exclusions).
  ITEM_REPLENISHABLE_IND is selected and used to derive REPLEN_TYPE in staging tables.
--------------------------------------------------------------------*/
CREATE OR REPLACE TEMP TABLE ITEM_CUR
CLUSTER BY MDS_FAM_ID
AS (
  SELECT *
  FROM (
    SELECT DISTINCT
      MDS_FAM_ID,
      ITEM_NBR,
      ITEM_REPLENISHABLE_IND,
      ACCTG_DEPT_NBR                                AS DEPT_NBR,
      WHPK_QTY,
      -- Flag items that should be excluded from reporting
      CASE
        WHEN FINELINE_NBR = 8023 AND DEPT_NBR = 42 THEN 1   -- photo service
        WHEN ACCTG_DEPT_NBR = 19  AND DEPT_CATEGORY_NBR = 3072 THEN 1  --tire service
        WHEN ACCTG_DEPT_NBR = 91  AND DEPT_CATEGORY_NBR = 3201 THEN 1 --DISPLAY
        WHEN ACCTG_DEPT_NBR = 95  AND FINELINE_NBR IN (9225,9226,9998,2353) THEN 1   --DSD OR SPECIAL VENDOR PROGRAM
        WHEN ACCTG_DEPT_NBR = 40  AND DEPT_CATEGORY_NBR IN (1808,1809,1788) THEN 1 --PHARMACY SERVICES
        WHEN ACCTG_DEPT_NBR = 8   AND DEPT_CATEGORY_NBR IN (676) THEN 1  --PET SERVICE
        WHEN ACCTG_DEPT_NBR = 2   AND DEPT_CATEGORY_NBR IN (546,542) THEN 1  --SALON SERVICE
        WHEN ACCTG_DEPT_NBR = 95  AND VENDOR_NAME = 'MCLANE COMPANY' THEN 1 --DSD convenience items - tracked differently
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
      AND VENDOR_NBR <> 481890                              -- environmental/deposit fees
      AND REPL_SUBTYPE_CODE <> 99                           -- no pay-from-scan
      AND REPL_SUBTYPE_CODE NOT IN (16,89)                  -- no production
      AND REPL_SUBTYPE_CODE <> 21                           -- no non-PI
      AND REPL_SUBTYPE_CODE <> 6                            -- no guaranteed-sales DSD type 7
      AND DEPT_CATEGORY_NBR IS NOT NULL
      AND CONCAT(ACCTG_DEPT_NBR, DEPT_CATEGORY_NBR)  NOT IN ('821425','821435','872873')
      AND CONCAT(ACCTG_DEPT_NBR, DEPT_CATG_GRP_NBR)  NOT IN ('826379')
      AND CONCAT(ACCTG_DEPT_NBR, REPL_SUBTYPE_CODE)  NOT IN ('8021','8014','9821')
      -- exclude PFS vendors
      AND (VENDOR_SEQ_NBR + 10 * VENDOR_DEPT_NBR + 1000 * VENDOR_NBR) NOT IN (
          SELECT (VENDOR_SEQ_NBR + 10 * DEPT_NBR + 1000 * VENDOR_NBR)
          FROM `wmt-edw-prod.US_WM_VM.PFS_VENDOR_STORE`
          WHERE PFS_PRODUCTION_IND = 'P'
          GROUP BY VENDOR_NBR, DEPT_NBR, VENDOR_SEQ_NBR
      )
  )
  WHERE ITEM_EXCEPTIONS <> 1
);


/*--------------------------------------------------------------------
  TEMP: OMNI_STORE_HIERARCHY
  Source: mdse_omni_item_vw  (MDS_FAM_ID grain)
  Purpose: Merch division/dept hierarchy for store-network items.
  ITEM_NBR here is the human-facing Walmart item number (WALMART_NBR).
  VAL_IND = 1 means the item record is valid/active in the hierarchy.
  item_cost_amt (aliased as UNIT_COST) is pulled for cost calculations.
  REPLEN_TYPE is derived from ITEM_CUR.ITEM_REPLENISHABLE_IND (joined separately).

  Added: OMNI_CATG_NBR and OMNI_CATG_DESC for category filtering in dashboard.
--------------------------------------------------------------------*/
CREATE OR REPLACE TEMP TABLE OMNI_STORE_HIERARCHY
AS (
  SELECT DISTINCT
    MDS_FAM_ID,
    ITEM_NBR,
    item_cost_amt                                      AS UNIT_COST,
    OMNI_SEG_DESC,
    CASE
      WHEN OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI_DEPT_NBR IN (80,81,93,94,97,98)           THEN 'FRESH'
      WHEN OMNI_DEPT_NBR IN (90,91,1,82,96)               THEN 'CAC'
      WHEN OMNI_DEPT_NBR IN (92,95)                       THEN 'PANTRY'
      WHEN OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34)  THEN 'FASHION'
      WHEN OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)        THEN 'ETS'
      WHEN OMNI_DEPT_NBR IN (3,9,10,11,12,16,56)          THEN 'HARDLINES'
      WHEN OMNI_DEPT_NBR IN (14,17,19,20,22,71,74)        THEN 'HOME'
      ELSE 'UNASSIGNED'
    END                                                    AS OMNI_DIVISION,
    OMNI_SUBGROUP_DESC,
    OMNI_DEPT_NBR,
    OMNI_DEPT_DESC,
    OMNI_CATG_NBR,                                        -- Category number for filtering
    OMNI_CATG_DESC                                        -- Category description for display
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
  WHERE VAL_IND   = 1    -- VAL_IND = 1: valid/active item record in the hierarchy
    AND MDS_FAM_ID <> -1
);


/*--------------------------------------------------------------------
  TEMP: OMNI_ECOMM_HIERARCHY
  Source: mdse_omni_item_vw  (CATLG_ITEM_ID grain)
  Purpose: Merch hierarchy for FC/eComm items.
  Used only by R0C0JUG_WMUS_FC_INVT staging table.
--------------------------------------------------------------------*/
-- CREATE OR REPLACE TEMP TABLE OMNI_ECOMM_HIERARCHY
-- AS (
--   SELECT DISTINCT
--     CATLG_ITEM_ID,
--     ITEM_NBR,
--     OMNI_SEG_DESC,
--     CASE
--       WHEN OMNI_SEG_DESC IN ('FASHION','APPAREL')                    THEN 'FASHION'
--       WHEN OMNI_SEG_DESC IN ('ENTERTAINMENT',
--                              'ENTERTAINMENT TOYS AND SEASONAL')       THEN 'ETS'
--       WHEN OMNI_DEPT_NBR IN (1,82,90,91,96)                         THEN 'CAC'
--       WHEN OMNI_DEPT_NBR IN (80,81,93,94,97,98)                     THEN 'FRESH'
--       WHEN OMNI_DEPT_NBR IN (92,95)                                  THEN 'PANTRY'
--       ELSE OMNI_SEG_DESC
--     END                                                               AS OMNI_DIVISION,
--     OMNI_SUBGROUP_DESC,
--     OMNI_DEPT_NBR,
--     OMNI_DEPT_DESC
--   FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
--   WHERE VAL_IND             = 1
--     AND CATLG_ITEM_ID        <> -1
--     AND OMNI_DEPT_NBR        IS NOT NULL
--     AND ITEM_STATUS_CD       <> 'D'
--     AND (ITEM_DEL_IND = 0    OR ITEM_DEL_IND IS NULL)
--     AND OP_CMPNY_CD          = 'WMT-US'
--     AND BUYG_REGION_CD       IN (0,6)
--     AND (SUPPLIER_PRTNR_TYPE_CD NOT IN ('WFS','DSV') OR SUPPLIER_PRTNR_TYPE_CD IS NULL)
--     AND (OBSOLETE_DT = '2500-01-01' OR OBSOLETE_DT IS NULL)
-- );


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_STORE_INVT   (YOUR SECTION)
  Source: repl_sku_ei_invt_dly
  Purpose: Store floor on-hand inventory at item x store x date.

  Notes:
  - Matches KPI_REPORT_ITEM_LVL_STORE_OH pattern exactly
  - STORE_INFO_SNE inner join removes FCs, Sam's Clubs, closed/
    relocating stores (OPEN_STATUS 0/6/7), and special sub-division
    codes (X=Sam's, S=Supercenter variant, Z, W, G)
  - ON_HAND_QTY guard: BETWEEN 1 AND 10000 (reference pattern)
  - In-transit is intentionally excluded here — it is sourced
    separately from repl_sku_ei_invt_dly (see next table)
  - ITEM_REPLENISHABLE_IND from ITEM_CUR (joined via MDS_FAM_ID)
  - REPLEN_TYPE = 'REPLEN' when ITEM_REPLENISHABLE_IND = 'Y', else 'NON-REPLEN'
  - WALMART_NBR = ITEM_NBR from OMNI_STORE_HIERARCHY
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STORE_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    INVT.BUS_DT,
    OMNI.OMNI_SEG_DESC,
    OMNI.OMNI_DIVISION,
    OMNI.OMNI_SUBGROUP_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR,                              -- Category number
    OMNI.OMNI_CATG_DESC,                             -- Category description
    'STORE'                                          AS NODE_TYPE,
    INVT.MDS_FAM_ID,
    OMNI.ITEM_NBR                                    AS WALMART_NBR,
    OMNI.UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    COUNT(DISTINCT INVT.STORE_NBR)                   AS STORE_COUNT,
    SUM(COALESCE(INVT.ON_HAND_QTY, 0))               AS STORE_OH_UNITS,
    SUM(COALESCE(INVT.IN_WHSE_QTY, 0))               AS DC_RESERVED_UNITS

  FROM `wmt-gdap-dl-sec-merch-bq-prod.ww_repl_dl_secure.repl_sku_ei_invt_dly` INVT

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON INVT.BUS_DT = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON INVT.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID

  -- INNER JOIN ensures only store items (items in store hierarchy) are included
  INNER JOIN OMNI_STORE_HIERARCHY OMNI
    ON INVT.MDS_FAM_ID = OMNI.MDS_FAM_ID

  -- Remove FCs, closed stores, Sam's Clubs, and special sub-division types
  INNER JOIN `wmt-edw-prod.US_WM_VM.STORE_INFO_SNE` STR_ALIGN
    ON STR_ALIGN.STORE_NBR                = INVT.STORE_NBR
    AND STR_ALIGN.ALIGN_SUB_DIVISION_NBR  NOT IN ('X','S','Z','W','G')
    AND STR_ALIGN.OPEN_STATUS             NOT IN ('0','6','7')

  WHERE 1=1
    AND INVT.BUS_DT           IN (DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY))
    AND INVT.ON_HAND_QTY       BETWEEN 1 AND 10000
    AND OMNI.OMNI_SEG_DESC     NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')

  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_IN_TRANSIT_INVT   (YOUR SECTION)
  Source: repl_sku_ei_invt_dly
  Purpose: Inventory in-transit to stores, at item x date.

  Notes:
  - in_trnst_qty represents inventory currently en route (shipped
    from DC, not yet received at store)
  - Same STORE_INFO_SNE filter applied so transit is scoped to the
    same valid store universe as store OH
  - RPT_EXCL_IND = 0: data pipeline quality flag — excludes rows
    marked as bad/excluded by the reporting pipeline
  - FULFMT_VALID_IND = 1: active store-item relationship —
    only count transit for live replenishment-active store/item pairs
  - ITEM_REPLENISHABLE_IND from ITEM_CUR (joined via MDS_FAM_ID)
  - REPLEN_TYPE = 'REPLEN' when ITEM_REPLENISHABLE_IND = 'Y', else 'NON-REPLEN'
  - WALMART_NBR = ITEM_NBR from OMNI_STORE_HIERARCHY
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_IN_TRANSIT_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    INVT.BUS_DT,
    OMNI.OMNI_SEG_DESC,
    OMNI.OMNI_DIVISION,
    OMNI.OMNI_SUBGROUP_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR,                              -- Category number
    OMNI.OMNI_CATG_DESC,                             -- Category description
    'IN_TRANSIT'                                     AS NODE_TYPE,
    INVT.MDS_FAM_ID,
    OMNI.ITEM_NBR                                    AS WALMART_NBR,
    OMNI.UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    SUM(COALESCE(INVT.IN_TRNST_QTY, 0))              AS IN_TRANSIT_UNITS

  FROM `wmt-gdap-dl-sec-merch-bq-prod.ww_repl_dl_secure.repl_sku_ei_invt_dly` INVT

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON INVT.BUS_DT = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON INVT.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID

  -- INNER JOIN ensures only store items (items in store hierarchy) are included
  INNER JOIN OMNI_STORE_HIERARCHY OMNI
    ON INVT.MDS_FAM_ID = OMNI.MDS_FAM_ID

  INNER JOIN `wmt-edw-prod.US_WM_VM.STORE_INFO_SNE` STR_ALIGN
    ON STR_ALIGN.STORE_NBR                = INVT.STORE_NBR
    AND STR_ALIGN.ALIGN_SUB_DIVISION_NBR  NOT IN ('X','S','Z','W','G')
    AND STR_ALIGN.OPEN_STATUS             NOT IN ('0','6','7')

  WHERE 1=1
    AND INVT.BUS_DT           IN (DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY))
    AND INVT.RPT_EXCL_IND      = 0
    AND INVT.FULFMT_VALID_IND  = 1
    AND INVT.IN_TRNST_QTY      > 0
    AND OMNI.OMNI_SEG_DESC     NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')

  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_DC_INVT
  Source: WHSE_DLY_SKU (On-Hand) + DC_DLY_INVT_TYPE (Labeled + Unlabeled)
  Purpose: DC inventory combining On-Hand + Labeled + Unlabeled inventory.

  INVT_TYPE_CODE breakdown:
    O = On-Hand (from WHSE_DLY_SKU)
    L = Distribution Labeled - Not Invoiced (from DC_DLY_INVT_TYPE)
    S = Staple Stock Labeled - Not Invoiced (from DC_DLY_INVT_TYPE)
    U = Unlabeled Distribution (from DC_DLY_INVT_TYPE)

  DC_TYPE_DESC comes from DC_DIM_CUR.
  Filtered to STORE-REPLENISHING DCs only.
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_DC_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  -- ═══════════════════════════════════════════════════════════════
  -- PART 1: ON-HAND INVENTORY (INVT_TYPE_CODE = 'O')
  -- Source: WHSE_DLY_SKU
  -- ═══════════════════════════════════════════════════════════════
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    WDS.INVENTORY_DATE                               AS BUS_DT,
    OMNI.OMNI_SEG_DESC,
    OMNI.OMNI_DIVISION,
    OMNI.OMNI_SUBGROUP_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR,                              -- Category number
    OMNI.OMNI_CATG_DESC,                             -- Category description
    'DC'                                             AS NODE_TYPE,
    'O'                                              AS INVT_TYPE_CODE,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN')         AS DC_TYPE_DESC,
    WDS.WHSE_NBR                                     AS DC_NBR,
    WDS.ITEM_NBR                                     AS MDS_FAM_ID,
    OMNI.ITEM_NBR                                    AS WALMART_NBR,
    OMNI.UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY)             AS DC_OH_UNITS

  FROM `wmt-edw-prod.US_WM_VM.WHSE_DLY_SKU` WDS

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON  DC_DIM.COUNTRY_CODE  = 'US'
    AND DC_DIM.DC_NBR         = WDS.WHSE_NBR
    AND DC_DIM.CURRENT_IND    = 'Y'

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON WDS.INVENTORY_DATE = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON WDS.ITEM_NBR = ITEM_CUR.MDS_FAM_ID

  INNER JOIN OMNI_STORE_HIERARCHY OMNI
    ON WDS.ITEM_NBR = OMNI.MDS_FAM_ID

  WHERE 1=1
    AND WDS.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND WDS.ON_HAND_QTY    > 0
    AND OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    -- Exclude eComm / fulfillment DC types
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%DOTCOM%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%SORTABLE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%E-COMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%ECOMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%FULFILLMENT%'
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')

  GROUP BY ALL

  UNION ALL

  -- ═══════════════════════════════════════════════════════════════
  -- PART 2: LABELED + UNLABELED INVENTORY (INVT_TYPE_CODE IN ('L', 'S', 'U'))
  -- Source: DC_DLY_INVT_TYPE
  -- L = Distribution Labeled - Not Invoiced
  -- S = Staple Stock Labeled - Not Invoiced
  -- U = Unlabeled Distribution
  -- ═══════════════════════════════════════════════════════════════
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    DC.INVENTORY_DATE                                AS BUS_DT,
    OMNI.OMNI_SEG_DESC,
    OMNI.OMNI_DIVISION,
    OMNI.OMNI_SUBGROUP_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    OMNI.OMNI_CATG_NBR,                              -- Category number
    OMNI.OMNI_CATG_DESC,                             -- Category description
    'DC'                                             AS NODE_TYPE,
    DC.INVT_TYPE_CODE,                               -- 'L', 'S', or 'U'
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN')         AS DC_TYPE_DESC,
    DC.DC_NBR,
    DC.ITEM_NBR                                      AS MDS_FAM_ID,
    OMNI.ITEM_NBR                                    AS WALMART_NBR,
    OMNI.UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    SUM(DC.WHPK_INVT_QTY * ITEM_CUR.WHPK_QTY)       AS DC_OH_UNITS

  FROM `wmt-edw-prod.US_WM_VM.DC_DLY_INVT_TYPE` DC

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON  DC_DIM.COUNTRY_CODE  = 'US'
    AND DC_DIM.DC_NBR         = DC.DC_NBR
    AND DC_DIM.CURRENT_IND    = 'Y'

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON DC.INVENTORY_DATE = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON DC.ITEM_NBR = ITEM_CUR.MDS_FAM_ID

  INNER JOIN OMNI_STORE_HIERARCHY OMNI
    ON DC.ITEM_NBR = OMNI.MDS_FAM_ID

  WHERE 1=1
    AND DC.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND DC.INVT_TYPE_CODE IN ('L', 'S', 'U')         -- Labeled + Unlabeled inventory
    AND DC.WHPK_INVT_QTY   > 0
    AND OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    -- Exclude eComm / fulfillment DC types
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%DOTCOM%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%SORTABLE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%E-COMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%ECOMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%FULFILLMENT%'
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')

  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_FC_INVT
  Source: repl_fc_item_invt_dly + mdse_omni_item_vw
  Purpose: FC (fulfillment center) on-hand at item x date.
  NOTE: This staging table is built for reference but is NOT included
  in WHERES_MY_STUFF_ROLLUP. FC uses CATLG_ITEM_ID (a different item
  key from MDS_FAM_ID used by the store network). A crosswalk would
  be needed to combine FC with Store/Transit/DC in a single rollup.
--------------------------------------------------------------------*/
-- CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_FC_INVT`
-- CLUSTER BY CATLG_ITEM_ID
-- AS (
--   SELECT
--     CAL.WM_FULL_YR_NBR                            AS WM_YEAR,
--     CAL.WM_WK_NBR                                 AS WM_WEEK,
--     CAL.WM_YR_WK_NBR,
--     OH.RPT_DT                                     AS BUS_DT,
--     OMNI.OMNI_SEG_DESC,
--     OMNI.OMNI_DIVISION,
--     OMNI.OMNI_SUBGROUP_DESC,
--     OMNI.OMNI_DEPT_NBR,
--     OMNI.OMNI_DEPT_DESC,
--     'FC'                                           AS NODE_TYPE,
--     OH.CATLG_ITEM_ID,
--     OMNI.ITEM_NBR                                  AS WALMART_NBR,
--     OH.SHIP_NODE_ORG_CD                            AS FC_ID,
--     SUM(OH.ON_HAND_UNIT_QTY)                       AS FC_OH_UNITS,
--     SUM(OH.ON_HAND_COST_AMT)                       AS FC_OH_COST

--   FROM `wmt-gdap-dl-sec-merch-bq-prod.us_repl_cnsm_secure.repl_fc_item_invt_dly` OH

--   INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
--     ON OH.RPT_DT = CAL.CAL_DT

--   -- FC item dimension (CATLG_ITEM_ID grain — different from store MDS_FAM_ID)
--   LEFT JOIN OMNI_ECOMM_HIERARCHY OMNI
--     ON OH.CATLG_ITEM_ID = OMNI.CATLG_ITEM_ID

--   WHERE 1=1
--     AND OH.RPT_DT          = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
--     AND OH.ON_HAND_UNIT_QTY > 0
--     AND OH.ON_HAND_UNIT_QTY < 500000       -- guard: exclude outlier/corrupt records

--   GROUP BY ALL
-- );


/*--------------------------------------------------------------------
  TABLE: WHERES_MY_STUFF_ROLLUP
  Purpose: Final "Where's My Stuff" — store-network item rollup
           combining Store OH, DC Reserved, In-Transit, and DC inventory.
           Share % and total calculations are handled in the dashboard.

  Grain:   MDS_FAM_ID x BUS_DT x REPLEN_TYPE x DC_TYPE_DESC
    • Store / Transit rows:  DC_TYPE_DESC = NULL  (one row per item per date)
    • DC rows: one row per item x DC type (REGIONAL DC, NATIONAL DC, etc.)
      To collapse DC types to a single total, remove DC_TYPE_DESC from
      DC_AGG GROUP BY and the SPINE / final SELECT.

  Item identifiers in output:
    MDS_FAM_ID   — internal merch family ID (join key across all 3 networks)
    WALMART_NBR  — human-facing Walmart ITEM_NBR
    UNIT_COST    — item_cost_amt from mdse_omni_item_vw (via OMNI_STORE_HIERARCHY)
    ITEM_REPLENISHABLE_IND  — from ITEM_CUR (all networks join to ITEM_CUR)
    REPLEN_TYPE  — 'REPLEN' when ITEM_REPLENISHABLE_IND = 'Y', else 'NON-REPLEN'

  Inventory columns:
    STORE_OH_UNITS      — on_hand_qty from repl_sku_ei_invt_dly (floor inventory)
    DC_RESERVED_UNITS   — in_whse_qty from repl_sku_ei_invt_dly (DC allocated, not shipped)
    IN_TRANSIT_UNITS    — in_trnst_qty from repl_sku_ei_invt_dly (shipped, not received)
    DC_OH_UNITS         — on_hand_qty from WHSE_DLY_SKU (INVT_TYPE_CODE = 'O')
    DC_LABELED_UNITS    — DC_DLY_INVT_TYPE where INVT_TYPE_CODE IN ('L', 'S')
    DC_UNLABELED_UNITS  — DC_DLY_INVT_TYPE where INVT_TYPE_CODE = 'U'
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP`
AS (
  WITH

  -- ── Store OH + DC Reserved  [your section — repl_sku_ei_invt_dly] ──────────
  STORE_AGG AS (
    SELECT
      BUS_DT,
      WM_YEAR,
      WM_WEEK,
      WM_YR_WK_NBR,
      OMNI_SEG_DESC,
      OMNI_DIVISION,
      OMNI_SUBGROUP_DESC,
      OMNI_DEPT_NBR,
      OMNI_DEPT_DESC,
      OMNI_CATG_NBR,                                  -- Category number
      OMNI_CATG_DESC,                                 -- Category description
      MDS_FAM_ID,
      WALMART_NBR,
      UNIT_COST,
      ITEM_REPLENISHABLE_IND,
      REPLEN_TYPE,
      SUM(STORE_OH_UNITS)           AS STORE_OH_UNITS,
      SUM(DC_RESERVED_UNITS)        AS DC_RESERVED_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STORE_INVT`
    GROUP BY ALL
  ),

  -- ── In-Transit  [your section — repl_sku_ei_invt_dly] ─────────────
  TRANSIT_AGG AS (
    SELECT
      BUS_DT,
      WM_YEAR,
      WM_WEEK,
      WM_YR_WK_NBR,
      OMNI_SEG_DESC,
      OMNI_DIVISION,
      OMNI_SUBGROUP_DESC,
      OMNI_DEPT_NBR,
      OMNI_DEPT_DESC,
      OMNI_CATG_NBR,                                  -- Category number
      OMNI_CATG_DESC,                                 -- Category description
      MDS_FAM_ID,
      WALMART_NBR,
      UNIT_COST,
      ITEM_REPLENISHABLE_IND,
      REPLEN_TYPE,
      SUM(IN_TRANSIT_UNITS)         AS IN_TRANSIT_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_IN_TRANSIT_INVT`
    GROUP BY ALL
  ),

  -- ── DC  [DC team section — WHSE_DLY_SKU + DC_DLY_INVT_TYPE] ───────────────
  -- DC_TYPE_DESC grain: one row per item x date x DC type.
  -- Store / Transit rows will have DC_TYPE_DESC = NULL in the SPINE.
  -- Split by inventory type: O=On-Hand, L/S=Labeled, U=Unlabeled
  DC_AGG AS (
    SELECT
      BUS_DT,
      WM_YEAR,
      WM_WEEK,
      WM_YR_WK_NBR,
      OMNI_SEG_DESC,
      OMNI_DIVISION,
      OMNI_SUBGROUP_DESC,
      OMNI_DEPT_NBR,
      OMNI_DEPT_DESC,
      OMNI_CATG_NBR,                                  -- Category number
      OMNI_CATG_DESC,                                 -- Category description
      DC_TYPE_DESC,
      MDS_FAM_ID,
      WALMART_NBR,
      UNIT_COST,
      ITEM_REPLENISHABLE_IND,
      REPLEN_TYPE,
      -- Split by inventory type
      SUM(CASE WHEN INVT_TYPE_CODE = 'O' THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L', 'S') THEN DC_OH_UNITS ELSE 0 END) AS DC_LABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U' THEN DC_OH_UNITS ELSE 0 END) AS DC_UNLABELED_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_DC_INVT`
    GROUP BY ALL
  ),

  -- ── Spine: full set of item x date x replen-type x DC-type combos ────
  -- Store / Transit rows: DC_TYPE_DESC = NULL
  -- DC rows: one entry per DC_TYPE_DESC value
  -- ITEM_REPLENISHABLE_IND and REPLEN_TYPE are item-level attributes —
  -- they are consistent per MDS_FAM_ID across all three networks.
  SPINE AS (
    SELECT DISTINCT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, UNIT_COST,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_DEPT_NBR, OMNI_CATG_NBR, CAST(NULL AS STRING) AS DC_TYPE_DESC
    FROM STORE_AGG
    UNION DISTINCT
    SELECT DISTINCT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, UNIT_COST,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_DEPT_NBR, OMNI_CATG_NBR, CAST(NULL AS STRING) AS DC_TYPE_DESC
    FROM TRANSIT_AGG
    UNION DISTINCT
    SELECT DISTINCT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, UNIT_COST,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_DEPT_NBR, OMNI_CATG_NBR, DC_TYPE_DESC
    FROM DC_AGG
  )

  -- ── Final Rollup ──────────────────────────────────────────────────────
  -- Tightened joins to include all item attributes from SPINE
  --         to prevent duplicate rows if staging tables have multiple matches.
  -- Added ROLLUP_ROW_TYPE helper field to clarify grain for dashboard users.
  SELECT
    SP.BUS_DT,
    -- COALESCE(S.WM_YEAR,  T.WM_YEAR,  D.WM_YEAR)   AS WM_YEAR,
    -- COALESCE(S.WM_WEEK,  T.WM_WEEK,  D.WM_WEEK)   AS WM_WEEK,
    -- COALESCE(S.WM_YR_WK_NBR, T.WM_YR_WK_NBR,
    --          D.WM_YR_WK_NBR)                       AS WM_YR_WK_NBR,

    -- ── Item identifiers ──────────────────────────────────────────────
    SP.MDS_FAM_ID,
    SP.WALMART_NBR AS ITEM_NBR,
    SP.UNIT_COST,
    SP.ITEM_REPLENISHABLE_IND,
    SP.REPLEN_TYPE,

    -- ── Merch hierarchy ───────────────────────────────────────────────
    SP.OMNI_DEPT_NBR,
    COALESCE(S.OMNI_DEPT_DESC, T.OMNI_DEPT_DESC,
             D.OMNI_DEPT_DESC)                     AS OMNI_DEPT_DESC,
    SP.OMNI_CATG_NBR,                              -- Category number
    COALESCE(S.OMNI_CATG_DESC, T.OMNI_CATG_DESC,
             D.OMNI_CATG_DESC)                     AS OMNI_CATG_DESC,
    COALESCE(S.OMNI_SEG_DESC,  T.OMNI_SEG_DESC,
             D.OMNI_SEG_DESC)                      AS OMNI_SEG_DESC,
    COALESCE(S.OMNI_DIVISION,  T.OMNI_DIVISION,
             D.OMNI_DIVISION)                      AS OMNI_DIVISION,
    COALESCE(S.OMNI_SUBGROUP_DESC, T.OMNI_SUBGROUP_DESC,
             D.OMNI_SUBGROUP_DESC)                 AS OMNI_SUBGROUP_DESC,

    -- ── DC type (NULL on Store / Transit rows) ────────────────────────
    SP.DC_TYPE_DESC,

    -- ── Row type indicator ────────────────────────────────────────────
    -- Helps dashboard users understand the grain:
    --   STORE_TRANSIT: Store OH + DC Reserved + In-Transit on this row (DC_TYPE_DESC IS NULL)
    --   DC: DC inventory by type on this row (DC_TYPE_DESC has value)
    CASE
      WHEN SP.DC_TYPE_DESC IS NULL THEN 'STORE_TRANSIT'
      ELSE 'DC'
    END                                            AS ROLLUP_ROW_TYPE,

    -- ── Inventory by network ──────────────────────────────────────────
    -- Store on-hand (repl_sku_ei_invt_dly → on_hand_qty)
    -- Only populate on DC_TYPE_DESC IS NULL rows to avoid duplication
    CASE WHEN SP.DC_TYPE_DESC IS NULL
         THEN COALESCE(S.STORE_OH_UNITS, 0)
         ELSE 0
    END                                            AS STORE_OH_UNITS,

    -- DC Reserved (repl_sku_ei_invt_dly → in_whse_qty)
    -- Inventory allocated at DC for store, not yet shipped
    -- Only populate on DC_TYPE_DESC IS NULL rows to avoid duplication
    CASE WHEN SP.DC_TYPE_DESC IS NULL
         THEN COALESCE(S.DC_RESERVED_UNITS, 0)
         ELSE 0
    END                                            AS DC_RESERVED_UNITS,

    -- In-transit to stores (repl_sku_ei_invt_dly → in_trnst_qty)
    -- Only populate on DC_TYPE_DESC IS NULL rows to avoid duplication
    CASE WHEN SP.DC_TYPE_DESC IS NULL
         THEN COALESCE(T.IN_TRANSIT_UNITS, 0)
         ELSE 0
    END                                            AS IN_TRANSIT_UNITS,

    -- DC on-hand (WHSE_DLY_SKU - INVT_TYPE_CODE = 'O')
    COALESCE(D.DC_OH_UNITS, 0)                     AS DC_OH_UNITS,

    -- DC Labeled (DC_DLY_INVT_TYPE - INVT_TYPE_CODE IN ('L', 'S'))
    COALESCE(D.DC_LABELED_UNITS, 0)                AS DC_LABELED_UNITS,

    -- DC Unlabeled (DC_DLY_INVT_TYPE - INVT_TYPE_CODE = 'U')
    COALESCE(D.DC_UNLABELED_UNITS, 0)              AS DC_UNLABELED_UNITS,

    CURRENT_TIMESTAMP()                            AS REFRESH_TS

  FROM SPINE SP

  -- Store: join on ALL item attributes + ensure DC_TYPE_DESC IS NULL
  LEFT JOIN STORE_AGG   S  ON  SP.BUS_DT                 = S.BUS_DT
                           AND SP.MDS_FAM_ID              = S.MDS_FAM_ID
                           AND SP.WALMART_NBR             = S.WALMART_NBR
                           AND SP.OMNI_DEPT_NBR           = S.OMNI_DEPT_NBR
                           AND SP.OMNI_CATG_NBR           = S.OMNI_CATG_NBR
                           AND SP.DC_TYPE_DESC IS NULL

  -- Transit: join on ALL item attributes + ensure DC_TYPE_DESC IS NULL
  LEFT JOIN TRANSIT_AGG T  ON  SP.BUS_DT                 = T.BUS_DT
                           AND SP.MDS_FAM_ID              = T.MDS_FAM_ID
                           AND SP.WALMART_NBR             = T.WALMART_NBR
                           AND SP.OMNI_DEPT_NBR           = T.OMNI_DEPT_NBR
                           AND SP.OMNI_CATG_NBR           = T.OMNI_CATG_NBR
                           AND SP.DC_TYPE_DESC IS NULL

  -- DC: join on ALL item attributes + DC_TYPE_DESC
  LEFT JOIN DC_AGG      D  ON  SP.BUS_DT                 = D.BUS_DT
                           AND SP.MDS_FAM_ID              = D.MDS_FAM_ID
                           AND SP.WALMART_NBR             = D.WALMART_NBR
                           AND SP.OMNI_DEPT_NBR           = D.OMNI_DEPT_NBR
                           AND SP.OMNI_CATG_NBR           = D.OMNI_CATG_NBR
                           AND SP.DC_TYPE_DESC            = D.DC_TYPE_DESC

  ORDER BY
    SP.BUS_DT            DESC,
    SP.OMNI_DEPT_NBR     ASC,
    SP.MDS_FAM_ID        ASC,
    SP.REPLEN_TYPE       ASC,
    SP.DC_TYPE_DESC      ASC
);
