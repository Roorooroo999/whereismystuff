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
      REPL_GROUP_NBR,                                     -- CID (Replenishment Group)
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
  TABLE: R0C0JUG_WMUS_STORE_INVT
  Source: fd_omni_chnl_item_dly (ALIGNED WITH HIST_COMBINED)
  Purpose: Store OH + DC Reserved at item x date.

  Notes:
  - ALIGNED with HIST_COMBINED data source for consistency
  - fd_omni_chnl_item_dly has native cost columns
  - NO hierarchy join to avoid duplication - aggregate directly
  - WALMART_NBR from ITEM_CUR (single row per MDS_FAM_ID)
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STORE_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    INVT.BUS_DT,
    INVT.OMNI_SEG_DESC,
    CASE
      WHEN INVT.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN INVT.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN INVT.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN INVT.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN INVT.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN INVT.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      WHEN INVT.OMNI_DEPT_NBR IN (3,9,10,11,12,16,56)         THEN 'HARDLINES'
      WHEN INVT.OMNI_DEPT_NBR IN (14,17,19,20,22,71,74)       THEN 'HOME'
      ELSE 'UNASSIGNED'
    END                                              AS OMNI_DIVISION,
    CAST(NULL AS STRING)                             AS OMNI_SUBGROUP_DESC,
    INVT.OMNI_DEPT_NBR,
    INVT.OMNI_DEPT_DESC,
    INVT.OMNI_CATG_NBR,
    INVT.OMNI_CATG_DESC,
    'STORE'                                          AS NODE_TYPE,
    INVT.MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR                                AS WALMART_NBR,   -- From ITEM_CUR (1:1)
    ITEM_CUR.REPL_GROUP_NBR,                         -- CID
    SAFE_DIVIDE(
      SUM(COALESCE(INVT.TY_ON_HAND_COST_AMT, 0)),
      NULLIF(SUM(COALESCE(INVT.TY_ON_HAND_QTY, 0)), 0)
    )                                                AS UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    SUM(COALESCE(INVT.TY_ON_HAND_QTY, 0))           AS STORE_OH_UNITS,
    SUM(COALESCE(INVT.TY_ON_HAND_COST_AMT, 0))      AS STORE_OH_COST,
    SUM(COALESCE(INVT.TY_IN_WHSE_QTY, 0))           AS DC_RESERVED_UNITS,
    SUM(COALESCE(INVT.TY_IN_WHSE_COST_AMT, 0))      AS DC_RESERVED_COST

  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` INVT

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON INVT.BUS_DT = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON INVT.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID

  WHERE 1=1
    AND INVT.BUS_DT           = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND INVT.FULFMT_CHNL_NM   = 'Store'
    AND INVT.OMNI_SEG_DESC    NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')

  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_IN_TRANSIT_INVT
  Source: fd_omni_chnl_item_dly (ALIGNED WITH HIST_COMBINED)
  Purpose: Inventory in-transit to stores, at item x date.

  Notes:
  - ALIGNED with HIST_COMBINED data source for consistency
  - NO hierarchy join to avoid duplication - aggregate directly
  - WALMART_NBR from ITEM_CUR (single row per MDS_FAM_ID)
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_IN_TRANSIT_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR                              AS WM_YEAR,
    CAL.WM_WK_NBR                                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    INVT.BUS_DT,
    INVT.OMNI_SEG_DESC,
    CASE
      WHEN INVT.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN INVT.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN INVT.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN INVT.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN INVT.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN INVT.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      WHEN INVT.OMNI_DEPT_NBR IN (3,9,10,11,12,16,56)         THEN 'HARDLINES'
      WHEN INVT.OMNI_DEPT_NBR IN (14,17,19,20,22,71,74)       THEN 'HOME'
      ELSE 'UNASSIGNED'
    END                                              AS OMNI_DIVISION,
    CAST(NULL AS STRING)                             AS OMNI_SUBGROUP_DESC,
    INVT.OMNI_DEPT_NBR,
    INVT.OMNI_DEPT_DESC,
    INVT.OMNI_CATG_NBR,
    INVT.OMNI_CATG_DESC,
    'IN_TRANSIT'                                     AS NODE_TYPE,
    INVT.MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR                                AS WALMART_NBR,   -- From ITEM_CUR (1:1)
    ITEM_CUR.REPL_GROUP_NBR,                         -- CID
    SAFE_DIVIDE(
      SUM(COALESCE(INVT.TY_IN_TRNST_COST_AMT, 0)),
      NULLIF(SUM(COALESCE(INVT.TY_IN_TRNST_QTY, 0)), 0)
    )                                                AS UNIT_COST,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE
      WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y'     THEN 'REPLEN'
      ELSE 'NON-REPLEN'
    END                                              AS REPLEN_TYPE,
    SUM(COALESCE(INVT.TY_IN_TRNST_QTY, 0))          AS IN_TRANSIT_UNITS,
    SUM(COALESCE(INVT.TY_IN_TRNST_COST_AMT, 0))     AS IN_TRANSIT_COST

  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` INVT

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON INVT.BUS_DT = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON INVT.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID

  WHERE 1=1
    AND INVT.BUS_DT           = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND INVT.FULFMT_CHNL_NM   = 'Store'
    AND INVT.TY_IN_TRNST_QTY  > 0
    AND INVT.OMNI_SEG_DESC    NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')

  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_DC_INVT
  Source: WHSE_DLY_SKU (On-Hand) + DC_DLY_INVT_TYPE (Labeled + Unlabeled)
  Purpose: DC inventory combining On-Hand + Labeled + Unlabeled inventory.
  NOTE: NO hierarchy join to avoid duplication. Hierarchy comes from STORE_INVT.
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_DC_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  -- PART 1: ON-HAND (INVT_TYPE_CODE = 'O')
  SELECT
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    WDS.INVENTORY_DATE AS BUS_DT,
    'DC' AS NODE_TYPE, 'O' AS INVT_TYPE_CODE,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE_DESC,
    WDS.ITEM_NBR AS MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY) AS DC_OH_UNITS
  FROM `wmt-edw-prod.US_WM_VM.WHSE_DLY_SKU` WDS
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US' AND DC_DIM.DC_NBR = WDS.WHSE_NBR AND DC_DIM.CURRENT_IND = 'Y'
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL ON WDS.INVENTORY_DATE = CAL.CAL_DT
  INNER JOIN ITEM_CUR ON WDS.ITEM_NBR = ITEM_CUR.MDS_FAM_ID
  WHERE WDS.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND WDS.ON_HAND_QTY > 0
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%DOTCOM%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%SORTABLE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%E-COMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%ECOMMERCE%'
    AND UPPER(COALESCE(DC_DIM.DC_TYPE_DESC, '')) NOT LIKE '%FULFILLMENT%'
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')
  GROUP BY ALL

  UNION ALL

  -- PART 2: LABELED + UNLABELED (L, S, U)
  SELECT
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    DC.INVENTORY_DATE AS BUS_DT,
    'DC' AS NODE_TYPE, DC.INVT_TYPE_CODE,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE_DESC,
    DC.ITEM_NBR AS MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(DC.WHPK_INVT_QTY * ITEM_CUR.WHPK_QTY) AS DC_OH_UNITS
  FROM `wmt-edw-prod.US_WM_VM.DC_DLY_INVT_TYPE` DC
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US' AND DC_DIM.DC_NBR = DC.DC_NBR AND DC_DIM.CURRENT_IND = 'Y'
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL ON DC.INVENTORY_DATE = CAL.CAL_DT
  INNER JOIN ITEM_CUR ON DC.ITEM_NBR = ITEM_CUR.MDS_FAM_ID
  WHERE DC.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND DC.INVT_TYPE_CODE IN ('L', 'S', 'U')
    AND DC.WHPK_INVT_QTY > 0
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
  Source: fd_omni_chnl_item_dly (FULFMT_CHNL_NM = 'OWNED')
  Purpose: FC (fulfillment center) on-hand at item x date.
  NOTE: NO hierarchy join to avoid duplication.
--------------------------------------------------------------------*/
DROP TABLE IF EXISTS `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_FC_INVT`;

CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_FC_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    INVT.BUS_DT,
    INVT.OMNI_SEG_DESC,
    INVT.OMNI_DEPT_NBR, INVT.OMNI_DEPT_DESC,
    INVT.OMNI_CATG_NBR, INVT.OMNI_CATG_DESC,
    'FC' AS NODE_TYPE,
    INVT.MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(COALESCE(INVT.TY_FC_ON_HAND_UNIT_QTY, 0)) AS FC_OH_UNITS,
    SUM(COALESCE(INVT.TY_FC_ON_HAND_COST_AMT, 0)) AS FC_OH_COST
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly` INVT
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL ON INVT.BUS_DT = CAL.CAL_DT
  INNER JOIN ITEM_CUR ON INVT.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID

  WHERE INVT.BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND INVT.FULFMT_CHNL_NM = 'OWNED'
    AND INVT.TY_FC_ON_HAND_UNIT_QTY > 0
    AND INVT.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_BACKROOM_INVT
  Source: o0c046n_backroom_oh_inv_inv_sol_reporting_pre_20260126
  NOTE: NO hierarchy join to avoid duplication.
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_BACKROOM_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  WITH latest_week AS (
    SELECT WM_YEAR, WM_WEEK, (WM_YEAR * 100 + WM_WEEK) AS WM_YR_WK_SORT
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.o0c046n_backroom_oh_inv_inv_sol_reporting_pre_20260126`
    WHERE WM_YEAR IN (25, 26)
    GROUP BY WM_YEAR, WM_WEEK
    ORDER BY WM_YR_WK_SORT DESC
    LIMIT 1
  )
  SELECT
    DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY) AS BUS_DT,
    'BACKROOM' AS NODE_TYPE,
    BKRM.MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(CASE
      WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
      WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
      ELSE 0
    END) AS BACKROOM_UNITS
  FROM `wmt-instockinventory-datamart.WM_AD_HOC.o0c046n_backroom_oh_inv_inv_sol_reporting_pre_20260126` BKRM
  INNER JOIN latest_week LW ON BKRM.WM_YEAR = LW.WM_YEAR AND BKRM.WM_WEEK = LW.WM_WEEK
  INNER JOIN ITEM_CUR ON BKRM.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID
  WHERE BKRM.WM_YEAR IN (25, 26)
    AND ((BKRM.WM_YEAR = 25 AND BKRM.BACKROOM_QTY_LY > 0) OR (BKRM.WM_YEAR = 26 AND BKRM.BACKROOM_QTY_TY > 0))
  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_STO_INVT
  Source: ALLOC_ORDER_FACT + STOCK_TRANSFER_ORDER + ITEM
  NOTE: NO hierarchy join to avoid duplication.
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STO_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  SELECT
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    DATE(AOF.ALLOC_CREATE_TS) AS BUS_DT,
    'STO' AS NODE_TYPE,
    COALESCE(DEST_DC.DC_TYPE_DESC, 'UNKNOWN') AS DEST_DC_TYPE,
    ITEM.ITEM_NBR AS MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(AOF.WHPK_ORD_QTY * AOF.WHPK_QTY) AS STO_IN_TRANSIT_TO_DC_UNITS
  FROM `wmt-edw-prod.US_WM_OMS_VM.ALLOC_ORDER_FACT` AOF
  INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.STOCK_TRANSFER_ORDER` STO
    ON AOF.STOCK_TRANSFER_ORD_NBR = STO.STOCK_TRANSFER_ORD_NBR
  INNER JOIN `wmt-edw-prod.US_WM_VM.ITEM` ITEM
    ON ITEM.OLD_NBR = AOF.ITEM_NBR AND ITEM.OBSOLETE_DATE IS NULL
  INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_STO_PO_XREF` STO_XREF
    ON STO_XREF.STOCK_TRANSFER_ORD_NBR = AOF.STOCK_TRANSFER_ORD_NBR
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON DATE(AOF.ALLOC_CREATE_TS) = CAL.CAL_DT
  INNER JOIN ITEM_CUR ON ITEM.ITEM_NBR = ITEM_CUR.MDS_FAM_ID
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DEST_DC
    ON DEST_DC.COUNTRY_CODE = 'US' AND DEST_DC.DC_NBR = AOF.TO_STORE_NBR AND DEST_DC.CURRENT_IND = 'Y'
  WHERE DATE(AOF.ALLOC_CREATE_TS) = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND AOF.WHPK_ORD_QTY > 0
    AND DEST_DC.DC_TYPE_DESC IS NOT NULL
  GROUP BY ALL
);


/*--------------------------------------------------------------------
  TABLE: R0C0JUG_WMUS_ON_YARD_INVT
  Source: TUP.RDC_FDC_EVENT_POS_HIST + OMS_PURCHASE_ORDER + OMS_PO_LINE
  NOTE: NO hierarchy join to avoid duplication.
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_ON_YARD_INVT`
CLUSTER BY MDS_FAM_ID
AS (
  WITH po_first_gate AS (
    SELECT PO_NBR, WHSE_NBR AS DC_NBR, MIN(LEFT(GATE_IN_DATE, 10)) AS first_gate_date
    FROM `wmt-cp-prod.TUP.RDC_FDC_EVENT_POS_HIST`
    WHERE TRAILER_STATUS_CODE IN ('IN', 'XI', 'CI', 'XD') AND PO_NBR IS NOT NULL AND GATE_IN_DATE IS NOT NULL
    GROUP BY PO_NBR, WHSE_NBR
  )
  SELECT
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    DATE(pfg.first_gate_date) AS BUS_DT,
    'ON_YARD' AS NODE_TYPE,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE,
    pfg.DC_NBR,
    itm.MDS_FAM_ID,
    ITEM_CUR.ITEM_NBR AS WALMART_NBR,
    ITEM_CUR.REPL_GROUP_NBR,
    ITEM_CUR.ITEM_REPLENISHABLE_IND,
    CASE WHEN ITEM_CUR.ITEM_REPLENISHABLE_IND = 'Y' THEN 'REPLEN' ELSE 'NON-REPLEN' END AS REPLEN_TYPE,
    SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY) AS ON_YARD_UNITS
  FROM `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` po
  INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` pol ON pol.OMS_PO_NBR = po.OMS_PO_NBR
  INNER JOIN (
    SELECT DISTINCT ITEM_NBR, MDS_FAM_ID
    FROM `wmt-edw-prod.US_WM_VM.ITEM_CUR`
    WHERE ITEM_NBR IS NOT NULL AND MDS_FAM_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ITEM_NBR ORDER BY OBSOLETE_DATE DESC) = 1
  ) itm ON itm.ITEM_NBR = pol.ITEM_NBR
  INNER JOIN po_first_gate pfg ON pfg.PO_NBR = po.PO_NBR
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL ON DATE(pfg.first_gate_date) = CAL.CAL_DT
  INNER JOIN ITEM_CUR ON itm.MDS_FAM_ID = ITEM_CUR.MDS_FAM_ID
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US' AND DC_DIM.DC_NBR = pfg.DC_NBR AND DC_DIM.CURRENT_IND = 'Y'
  WHERE pol.PO_LINE_STATUS_CD <> 1300
    AND po.COUNTRY_CODE = 'US'
    AND DATE(pfg.first_gate_date) = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
    AND pol.VNPK_ORD_QTY > 0
  GROUP BY ALL
);


/*--------------------------------------------------------------------
  LEGACY: R0C0JUG_WMUS_FC_INVT (CATLG_ITEM_ID version - DEPRECATED)
  Kept for reference only - uses different item key than store network.
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
           combining ALL inventory channels:
           • Store OH, DC Reserved, In-Transit
           • DC OH, DC Labeled, DC Unlabeled
           • FC OH (Fulfillment Center)
           • Backroom
           • STO (Stock Transfer Orders to DC)
           • On Yard (Trailers at DC yards)

  Grain:   MDS_FAM_ID x BUS_DT x REPLEN_TYPE (collapsed DC types)

  Item identifiers in output:
    MDS_FAM_ID   — internal merch family ID (join key across all networks)
    WALMART_NBR  — human-facing Walmart ITEM_NBR
    UNIT_COST    — item_cost_amt from mdse_omni_item_vw (via OMNI_STORE_HIERARCHY)
    ITEM_REPLENISHABLE_IND  — from ITEM_CUR
    REPLEN_TYPE  — 'REPLEN' when ITEM_REPLENISHABLE_IND = 'Y', else 'NON-REPLEN'

  Inventory columns (all channels):
    STORE_OH_UNITS        — Store floor on-hand
    BACKROOM_UNITS        — Store backroom inventory
    DC_RESERVED_UNITS     — DC allocated for store, not shipped
    IN_TRANSIT_UNITS      — Shipped from DC, not received at store
    DC_OH_UNITS           — DC on-hand (INVT_TYPE_CODE = 'O')
    DC_LABELED_UNITS      — DC labeled (INVT_TYPE_CODE IN ('L', 'S'))
    DC_UNLABELED_UNITS    — DC unlabeled (INVT_TYPE_CODE = 'U')
    STO_IN_TRANSIT_TO_DC_UNITS — Stock Transfer Orders to DC
    FC_OH_UNITS           — Fulfillment Center on-hand
    ON_YARD_UNITS         — Trailers at DC yards
    TOTAL_NETWORK_UNITS   — Sum of all channels
--------------------------------------------------------------------*/
CREATE OR REPLACE TABLE `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP`
AS (
  WITH

  -- ── Store OH + DC Reserved (with native cost from fd_omni_chnl_item_dly) ──
  STORE_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR, UNIT_COST,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_SEG_DESC, OMNI_DIVISION, OMNI_SUBGROUP_DESC,
      OMNI_DEPT_NBR, OMNI_DEPT_DESC, OMNI_CATG_NBR, OMNI_CATG_DESC,
      SUM(STORE_OH_UNITS) AS STORE_OH_UNITS,
      SUM(STORE_OH_COST) AS STORE_OH_COST,           -- Native cost
      SUM(DC_RESERVED_UNITS) AS DC_RESERVED_UNITS,
      SUM(DC_RESERVED_COST) AS DC_RESERVED_COST      -- Native cost
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STORE_INVT`
    GROUP BY ALL
  ),

  -- ── In-Transit (with native cost from fd_omni_chnl_item_dly) ────────
  TRANSIT_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR, UNIT_COST,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_SEG_DESC, OMNI_DIVISION, OMNI_SUBGROUP_DESC,
      OMNI_DEPT_NBR, OMNI_DEPT_DESC, OMNI_CATG_NBR, OMNI_CATG_DESC,
      SUM(IN_TRANSIT_UNITS) AS IN_TRANSIT_UNITS,
      SUM(IN_TRANSIT_COST) AS IN_TRANSIT_COST        -- Native cost
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_IN_TRANSIT_INVT`
    GROUP BY ALL
  ),

  -- ── DC (collapsed to item level - no OMNI columns in staging table) ──
  DC_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      SUM(CASE WHEN INVT_TYPE_CODE = 'O' THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L', 'S') THEN DC_OH_UNITS ELSE 0 END) AS DC_LABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U' THEN DC_OH_UNITS ELSE 0 END) AS DC_UNLABELED_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_DC_INVT`
    GROUP BY ALL
  ),

  -- ── FC (Fulfillment Center - has partial OMNI columns) ──────────────
  FC_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      OMNI_SEG_DESC, OMNI_DEPT_NBR, OMNI_DEPT_DESC,
      OMNI_CATG_NBR, OMNI_CATG_DESC,
      SUM(FC_OH_UNITS) AS FC_OH_UNITS,
      SUM(FC_OH_COST) AS FC_OH_COST
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_FC_INVT`
    GROUP BY ALL
  ),

  -- ── Backroom (no OMNI columns in staging table) ─────────────────────
  BACKROOM_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      SUM(BACKROOM_UNITS) AS BACKROOM_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_BACKROOM_INVT`
    GROUP BY ALL
  ),

  -- ── STO (Stock Transfer Orders - no OMNI columns) ───────────────────
  STO_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      SUM(STO_IN_TRANSIT_TO_DC_UNITS) AS STO_IN_TRANSIT_TO_DC_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_STO_INVT`
    GROUP BY ALL
  ),

  -- ── On Yard (no OMNI columns) ────────────────────────────────────────
  ON_YARD_AGG AS (
    SELECT
      BUS_DT, MDS_FAM_ID, WALMART_NBR, REPL_GROUP_NBR,
      ITEM_REPLENISHABLE_IND, REPLEN_TYPE,
      SUM(ON_YARD_UNITS) AS ON_YARD_UNITS
    FROM `wmt-instockinventory-datamart.WM_AD_HOC.R0C0JUG_WMUS_ON_YARD_INVT`
    GROUP BY ALL
  ),

  -- ── Spine: All unique item x date (simple key for reliable joins) ───
  SPINE AS (
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM STORE_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM TRANSIT_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM DC_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM FC_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM BACKROOM_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM STO_AGG
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, MDS_FAM_ID FROM ON_YARD_AGG
  )

  -- ── Final Rollup with ALL channels ──────────────────────────────────
  SELECT
    SP.BUS_DT,

    -- ── Item identifiers (from Store/Transit which have full info) ────
    SP.MDS_FAM_ID,
    COALESCE(S.WALMART_NBR, T.WALMART_NBR, D.WALMART_NBR, FC.WALMART_NBR,
             BR.WALMART_NBR, STO.WALMART_NBR, OY.WALMART_NBR) AS ITEM_NBR,
    COALESCE(S.REPL_GROUP_NBR, T.REPL_GROUP_NBR, D.REPL_GROUP_NBR, FC.REPL_GROUP_NBR,
             BR.REPL_GROUP_NBR, STO.REPL_GROUP_NBR, OY.REPL_GROUP_NBR) AS REPL_GROUP_NBR,
    COALESCE(S.UNIT_COST, T.UNIT_COST)               AS UNIT_COST,
    COALESCE(S.ITEM_REPLENISHABLE_IND, T.ITEM_REPLENISHABLE_IND, D.ITEM_REPLENISHABLE_IND,
             FC.ITEM_REPLENISHABLE_IND, BR.ITEM_REPLENISHABLE_IND) AS ITEM_REPLENISHABLE_IND,
    COALESCE(S.REPLEN_TYPE, T.REPLEN_TYPE, D.REPLEN_TYPE,
             FC.REPLEN_TYPE, BR.REPLEN_TYPE)         AS REPLEN_TYPE,

    -- ── Merch hierarchy (from Store/Transit/FC which have OMNI columns) ─
    COALESCE(S.OMNI_DEPT_NBR, T.OMNI_DEPT_NBR, FC.OMNI_DEPT_NBR) AS OMNI_DEPT_NBR,
    COALESCE(S.OMNI_DEPT_DESC, T.OMNI_DEPT_DESC, FC.OMNI_DEPT_DESC) AS OMNI_DEPT_DESC,
    COALESCE(S.OMNI_CATG_NBR, T.OMNI_CATG_NBR, FC.OMNI_CATG_NBR) AS OMNI_CATG_NBR,
    COALESCE(S.OMNI_CATG_DESC, T.OMNI_CATG_DESC, FC.OMNI_CATG_DESC) AS OMNI_CATG_DESC,
    COALESCE(S.OMNI_SEG_DESC, T.OMNI_SEG_DESC, FC.OMNI_SEG_DESC) AS OMNI_SEG_DESC,
    COALESCE(S.OMNI_DIVISION, T.OMNI_DIVISION)       AS OMNI_DIVISION,
    COALESCE(S.OMNI_SUBGROUP_DESC, T.OMNI_SUBGROUP_DESC) AS OMNI_SUBGROUP_DESC,

    -- ── Store Inventory (native cost from fd_omni_chnl_item_dly) ───────
    COALESCE(S.STORE_OH_UNITS, 0)                    AS STORE_OH_UNITS,
    COALESCE(S.STORE_OH_COST, 0)                     AS STORE_OH_COST,
    COALESCE(BR.BACKROOM_UNITS, 0)                   AS BACKROOM_UNITS,
    COALESCE(S.STORE_OH_UNITS, 0) - COALESCE(BR.BACKROOM_UNITS, 0) AS ON_FLOOR_UNITS,
    COALESCE(S.DC_RESERVED_UNITS, 0)                 AS DC_RESERVED_UNITS,
    COALESCE(S.DC_RESERVED_COST, 0)                  AS DC_RESERVED_COST,

    -- ── In-Transit (native cost from fd_omni_chnl_item_dly) ───────────
    COALESCE(T.IN_TRANSIT_UNITS, 0)                  AS IN_TRANSIT_UNITS,
    COALESCE(T.IN_TRANSIT_COST, 0)                   AS IN_TRANSIT_COST,

    -- ── DC Inventory (cost calc uses Store/Transit UNIT_COST) ─────────
    COALESCE(D.DC_OH_UNITS, 0)                       AS DC_OH_UNITS,
    COALESCE(D.DC_OH_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0) AS DC_OH_COST,
    COALESCE(D.DC_LABELED_UNITS, 0)                  AS DC_LABELED_UNITS,
    COALESCE(D.DC_LABELED_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0) AS DC_LABELED_COST,
    COALESCE(D.DC_UNLABELED_UNITS, 0)                AS DC_UNLABELED_UNITS,
    COALESCE(D.DC_UNLABELED_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0) AS DC_UNLABELED_COST,

    -- ── STO (Stock Transfer Orders) ───────────────────────────────────
    COALESCE(STO.STO_IN_TRANSIT_TO_DC_UNITS, 0)      AS STO_IN_TRANSIT_TO_DC_UNITS,
    COALESCE(STO.STO_IN_TRANSIT_TO_DC_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0) AS STO_IN_TRANSIT_TO_DC_COST,

    -- ── FC (Fulfillment Center) ───────────────────────────────────────
    COALESCE(FC.FC_OH_UNITS, 0)                      AS FC_OH_UNITS,
    COALESCE(FC.FC_OH_COST, 0)                       AS FC_OH_COST,

    -- ── On Yard ───────────────────────────────────────────────────────
    COALESCE(OY.ON_YARD_UNITS, 0)                    AS ON_YARD_UNITS,
    COALESCE(OY.ON_YARD_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0) AS ON_YARD_COST,

    -- ── Total Network (all channels) ──────────────────────────────────
    (COALESCE(S.STORE_OH_UNITS, 0)
     + COALESCE(S.DC_RESERVED_UNITS, 0)
     + COALESCE(T.IN_TRANSIT_UNITS, 0)
     + COALESCE(D.DC_OH_UNITS, 0)
     + COALESCE(D.DC_LABELED_UNITS, 0)
     + COALESCE(D.DC_UNLABELED_UNITS, 0)
     + COALESCE(STO.STO_IN_TRANSIT_TO_DC_UNITS, 0)
     + COALESCE(FC.FC_OH_UNITS, 0)
     + COALESCE(OY.ON_YARD_UNITS, 0)
    )                                                AS TOTAL_NETWORK_UNITS,

    (COALESCE(S.STORE_OH_COST, 0)
     + COALESCE(S.DC_RESERVED_COST, 0)
     + COALESCE(T.IN_TRANSIT_COST, 0)
     + COALESCE(D.DC_OH_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0)
     + COALESCE(D.DC_LABELED_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0)
     + COALESCE(D.DC_UNLABELED_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0)
     + COALESCE(STO.STO_IN_TRANSIT_TO_DC_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0)
     + COALESCE(FC.FC_OH_COST, 0)
     + COALESCE(OY.ON_YARD_UNITS, 0) * COALESCE(S.UNIT_COST, T.UNIT_COST, 0)
    )                                                AS TOTAL_NETWORK_COST,

    CURRENT_TIMESTAMP()                              AS REFRESH_TS

  FROM SPINE SP

  LEFT JOIN STORE_AGG S
    ON SP.BUS_DT = S.BUS_DT AND SP.MDS_FAM_ID = S.MDS_FAM_ID

  LEFT JOIN TRANSIT_AGG T
    ON SP.BUS_DT = T.BUS_DT AND SP.MDS_FAM_ID = T.MDS_FAM_ID

  LEFT JOIN DC_AGG D
    ON SP.BUS_DT = D.BUS_DT AND SP.MDS_FAM_ID = D.MDS_FAM_ID

  LEFT JOIN FC_AGG FC
    ON SP.BUS_DT = FC.BUS_DT AND SP.MDS_FAM_ID = FC.MDS_FAM_ID

  LEFT JOIN BACKROOM_AGG BR
    ON SP.BUS_DT = BR.BUS_DT AND SP.MDS_FAM_ID = BR.MDS_FAM_ID

  LEFT JOIN STO_AGG STO
    ON SP.BUS_DT = STO.BUS_DT AND SP.MDS_FAM_ID = STO.MDS_FAM_ID

  LEFT JOIN ON_YARD_AGG OY
    ON SP.BUS_DT = OY.BUS_DT AND SP.MDS_FAM_ID = OY.MDS_FAM_ID

  ORDER BY SP.BUS_DT DESC, COALESCE(S.OMNI_DEPT_NBR, T.OMNI_DEPT_NBR, FC.OMNI_DEPT_NBR) ASC, SP.MDS_FAM_ID ASC
);
