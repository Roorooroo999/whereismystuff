-- ============================================================
-- STEP 0: ITEM_CUR (TEMP TABLE)
-- Filter: item_validity logic from mdse_omni_item (EI-aligned)
-- Fields: from wmt-edw-prod.US_WM_VM.ITEM_CUR (ITEM_NBR, REPL_GROUP_NBR, WHPK_QTY)
-- All downstream steps (1.5 → 8) reference ITEM_CUR unchanged
-- ============================================================
CREATE OR REPLACE TEMP TABLE ITEM_CUR
CLUSTER BY MDS_FAM_ID
AS (
  WITH item_validity AS (
    SELECT dim.mds_fam_id
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_dl_secure.mdse_omni_item` dim
    WHERE dim.geo_region_cd = 'US'
      AND dim.op_cmpny_cd   = 'WMT-US'
      AND dim.val_ind        = 1
      -- (a) Repl sub-type exclusions
      AND (dim.repl_sub_type_cd NOT IN (99,16,89,6,21) OR dim.repl_sub_type_cd IS NULL)
      -- (b) Vendor exclusions
      AND dim.vendor_nbr <> 481890
      -- (c) Dept / category combo exclusions
      AND NOT (
           (dim.omni_dept_nbr = 82 AND dim.omni_catg_nbr     = 1425)  -- Gift & Prepaid Cards
        OR (dim.omni_dept_nbr = 82 AND dim.omni_catg_nbr     = 1435)  -- Collectibles
        OR (dim.omni_dept_nbr = 82 AND dim.omni_catg_grp_nbr = 6379)  -- D82 catg group
        OR (dim.omni_dept_nbr = 87 AND dim.omni_catg_nbr     = 2873)  -- Prepaid Wireless
        OR (dim.omni_dept_nbr = 80 AND dim.repl_sub_type_cd IN (21,14)) -- Service Deli DSD
        OR (dim.omni_dept_nbr = 98 AND dim.repl_sub_type_cd  = 21)    -- Bakery Non-PI
      )
      -- (d) PFS vendor exclusion
      AND (dim.vendor_seq_nbr + 10 * dim.vendor_dept_nbr + 1000 * dim.vendor_nbr)
          NOT IN (
            SELECT (vendor_seq_nbr + 10 * dept_nbr + 1000 * vendor_nbr)
            FROM `wmt-edw-prod.US_WM_VM.PFS_VENDOR_STORE`
            WHERE pfs_production_ind = 'P'
            GROUP BY vendor_nbr, dept_nbr, vendor_seq_nbr
          )
      -- (e) Item description exclusions
      AND dim.item_desc_1 NOT IN (
          'PRICE ADJ','BR 15LB EXCHANGE','AMERIG PROPANE EXCH','0','1','4','42','72',
          '123','1408','1800','1843','1865','1917','1984','2067','71239','436166','453092',
          '9509766','133538321','DIESEL','REG GAS UNLEADED','REUSABLE BAGS',
          'REUSABLE BAGS ASST 2','FUEL PREPAY','HI-GRADE PREMIUM GAS',
          'UNLEADED CLEAR','CAROUSEL REUSABLE BG'
      )
      AND dim.item_desc_1 NOT LIKE '%DISCONTINUED%'
      -- (f) Fineline / dept / category exclusions
      AND NOT (dim.fineline_nbr  = 8023 AND dim.dept_nbr = 42)
      -- AND NOT (dim.omni_dept_nbr = 19   AND dim.omni_catg_nbr = 3072)
      AND NOT (dim.omni_dept_nbr = 91   AND dim.omni_catg_nbr = 3201)
      AND NOT (dim.omni_dept_nbr = 95   AND dim.fineline_nbr IN (9225,9226,9998,2353))
      AND NOT (dim.omni_dept_nbr = 40   AND dim.omni_catg_nbr IN (1808,1809,1788))
      AND NOT (dim.omni_dept_nbr =  8   AND dim.omni_catg_nbr IN (676))
      AND NOT (dim.omni_dept_nbr =  2   AND dim.omni_catg_nbr IN (546,542))
      AND NOT (dim.omni_dept_nbr = 95   AND dim.vendor_nm = 'MCLANE COMPANY')
      -- (g) Segment / dept exclusions — including full H&W exclusion
      AND dim.omni_seg_desc IS NOT NULL
      AND dim.omni_seg_desc <> 'OTHER'
      AND NOT (dim.omni_seg_desc = 'HEALTH AND WELLNESS'
               AND dim.omni_dept_nbr IN (38,50,75))
      AND dim.omni_dept_nbr NOT IN (39,60,86,88,99,38,37,83,85)
  )
  SELECT DISTINCT
    ic.MDS_FAM_ID,
    ic.ITEM_NBR,
    ic.ITEM_REPLENISHABLE_IND,
    ic.ACCTG_DEPT_NBR  AS DEPT_NBR,
    ic.WHPK_QTY,
    ic.REPL_GROUP_NBR
  FROM `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
  INNER JOIN item_validity iv ON ic.MDS_FAM_ID = iv.mds_fam_id
);


-- ============================================================
-- STEP 1: STORE + FC CONSOLIDATED INVENTORY
-- Store OH/InWhse/InTransit → NEW: repl_sku_ei_invt_dly_vw
-- FC OH                     → UNCHANGED: fd_omni_chnl_item_dly
-- H&W                       → excluded via ITEM_CUR + OV filter
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH

  -- ── Store validity ────────────────────────────────────────────────────────
  store_validity AS (
    SELECT DISTINCT store_nbr
    FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_STORE_CLUB_DIM`
    WHERE TRIM(op_cmpny_cd) = 'WMT-US'
      AND geo_region_cd      = 'US'
      AND UPPER(banner_cd)   NOT IN ('F4')
      AND TRIM(subdiv_nbr)   IN ('A','B','C','D','E','F','I','M','N','O','X','Z')
  ),

  -- ── OMNI hierarchy: item → catg/dept ─────────────────────────────────────
  omni_validity AS (
    SELECT
      mds_fam_id,
      omni_seg_desc,
      omni_catg_nbr,
      omni_catg_desc,
      omni_dept_nbr,
      omni_dept_desc
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_dl_secure.mdse_omni_item`
    WHERE op_cmpny_cd = 'WMT-US'
      AND val_ind     = 1
  ),

  -- ── Unit cost: unchanged — derived from old table ─────────────────────────
  item_unit_cost AS (
    SELECT
      BUS_DT,
      MDS_FAM_ID,
      SAFE_DIVIDE(
        SUM(COALESCE(TY_ON_HAND_COST_AMT, 0)),
        NULLIF(SUM(COALESCE(TY_ON_HAND_QTY, 0)), 0)
      ) AS UNIT_COST
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
    WHERE BUS_DT >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM BUS_DT) = 6
        OR (BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND BUS_DT < CURRENT_DATE('America/Chicago')
      AND FULFMT_CHNL_NM = 'Store'
      AND TY_ON_HAND_QTY > 0
    GROUP BY BUS_DT, MDS_FAM_ID
  ),

  -- ── Store channel: NEW EI source ──────────────────────────────────────────
  store_channel AS (
    SELECT
      INVT.bus_dt                                                       AS BUS_DT,
      OV.mds_fam_id                                                     AS MDS_FAM_ID,
      SUM(COALESCE(INVT.on_hand_qty,  0))                               AS STORE_OH_UNITS,
      SUM(COALESCE(INVT.on_hand_qty,  0) * COALESCE(UC.UNIT_COST, 0))  AS STORE_OH_COST,
      SUM(COALESCE(INVT.in_whse_qty,  0))                               AS DC_RESERVED_UNITS,
      SUM(COALESCE(INVT.in_whse_qty,  0) * COALESCE(UC.UNIT_COST, 0))  AS DC_RESERVED_COST,
      SUM(COALESCE(INVT.in_trnst_qty, 0))                               AS IN_TRANSIT_UNITS,
      SUM(COALESCE(INVT.in_trnst_qty, 0) * COALESCE(UC.UNIT_COST, 0))  AS IN_TRANSIT_COST,
      OV.omni_catg_nbr                                                   AS OMNI_CATG_NBR,
      OV.omni_catg_desc                                                  AS OMNI_CATG_DESC,
      OV.omni_dept_nbr                                                   AS OMNI_DEPT_NBR,
      OV.omni_dept_desc                                                  AS OMNI_DEPT_DESC
    FROM `wmt-gdap-dl-sec-merch-bq-prod.ww_repl_dl_secure.repl_sku_ei_invt_dly_vw` INVT
    INNER JOIN store_validity  SV ON INVT.STORE_NBR  = SV.store_nbr
    INNER JOIN ITEM_CUR        IC ON INVT.mds_fam_id = IC.MDS_FAM_ID  -- ITEM_CUR now carries item_validity filter
    INNER JOIN omni_validity   OV ON INVT.mds_fam_id = OV.mds_fam_id
    LEFT JOIN  item_unit_cost  UC ON INVT.bus_dt      = UC.BUS_DT
                                 AND INVT.mds_fam_id  = UC.MDS_FAM_ID
    WHERE INVT.bus_dt >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM INVT.bus_dt) = 6
        OR (INVT.bus_dt = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND INVT.bus_dt < CURRENT_DATE('America/Chicago')
      AND INVT.rpt_excl_ind = 0
      AND INVT.STORE_NBR NOT IN (2670,7611,2489,5169,3704,5997,7638,3971)
      AND NOT (OV.omni_seg_desc = 'HEALTH AND WELLNESS' AND OV.omni_dept_nbr IN (38, 50, 75))
      AND OV.omni_seg_desc <> 'OTHER'
      AND OV.omni_seg_desc IS NOT NULL
    GROUP BY
      INVT.bus_dt, OV.mds_fam_id,
      OV.omni_catg_nbr, OV.omni_catg_desc,
      OV.omni_dept_nbr, OV.omni_dept_desc
  ),

  -- ── FC channel: UNCHANGED ─────────────────────────────────────────────────
-- ── FC Sort/Nonsort attribute lookup ─────────────────────────────────────
fc_attr AS (
  SELECT DISTINCT
    CAST(fc_id AS STRING) AS fc_id,
    is_sortable
  FROM `wmt-instockinventory-datamart.WM_AD_HOC.sce2e_aims_lkup_fc_attr`
),

-- ── FC item bridge: catlg_item_id → MDS_FAM_ID + OMNI hierarchy ──────────
-- Using mdse_omni_item_vw (authoritative item dimension — no partition filter needed)
fc_item_bridge AS (
  SELECT DISTINCT
    catlg_item_id,
    mds_fam_id,
    omni_catg_nbr,
    omni_catg_desc,
    omni_dept_nbr,
    omni_dept_desc,
    omni_seg_desc
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
  WHERE catlg_item_id IS NOT NULL
    AND mds_fam_id    IS NOT NULL
    AND omni_seg_desc NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND val_ind     = 1
    AND op_cmpny_cd = 'WMT-US'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY catlg_item_id ORDER BY mds_fam_id) = 1
),

-- ── FC channel: NEW source + Sort/Nonsort breakdown ───────────────────────
fc_channel AS (
  SELECT
    INVT.RPT_DT                                                        AS BUS_DT,
    BRG.MDS_FAM_ID,
    SUM(COALESCE(INVT.ON_HAND_UNIT_QTY, 0))                           AS FC_OH_UNITS,
    SUM(COALESCE(INVT.ON_HAND_COST_AMT, 0))                           AS FC_OH_COST,
    SUM(CASE WHEN FA.is_sortable = 1 THEN COALESCE(INVT.ON_HAND_UNIT_QTY, 0) ELSE 0 END) AS FC_OH_SORT_UNITS,
    SUM(CASE WHEN FA.is_sortable = 1 THEN COALESCE(INVT.ON_HAND_COST_AMT, 0) ELSE 0 END) AS FC_OH_SORT_COST,
    SUM(CASE WHEN FA.is_sortable = 0 THEN COALESCE(INVT.ON_HAND_UNIT_QTY, 0) ELSE 0 END) AS FC_OH_NONSORT_UNITS,
    SUM(CASE WHEN FA.is_sortable = 0 THEN COALESCE(INVT.ON_HAND_COST_AMT, 0) ELSE 0 END) AS FC_OH_NONSORT_COST,
    BRG.OMNI_CATG_NBR,
    BRG.OMNI_CATG_DESC,
    BRG.OMNI_DEPT_NBR,
    BRG.OMNI_DEPT_DESC
  FROM `wmt-gdap-dl-sec-merch-bq-prod.us_repl_cnsm_secure.repl_fc_item_invt_dly` INVT
  INNER JOIN fc_item_bridge BRG
    ON INVT.CATLG_ITEM_ID = BRG.CATLG_ITEM_ID
  LEFT JOIN fc_attr FA
    ON CAST(INVT.SHIP_NODE_ORG_CD AS STRING) = FA.fc_id
  WHERE INVT.RPT_DT >= '2025-02-07'
    AND (
      EXTRACT(DAYOFWEEK FROM INVT.RPT_DT) = 6
      OR (INVT.RPT_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
    )
    AND INVT.RPT_DT < CURRENT_DATE('America/Chicago')
    AND INVT.ON_HAND_UNIT_QTY  > 0
    AND INVT.ON_HAND_UNIT_QTY  < 500000
  GROUP BY
    INVT.RPT_DT, BRG.MDS_FAM_ID,
    BRG.OMNI_CATG_NBR, BRG.OMNI_CATG_DESC,
    BRG.OMNI_DEPT_NBR, BRG.OMNI_DEPT_DESC
)

  SELECT
    COALESCE(S.BUS_DT, F.BUS_DT)                     AS BUS_DT,
    CAL.WM_FULL_YR_NBR                                 AS WM_YEAR,
    CAL.WM_WK_NBR                                      AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    COALESCE(S.MDS_FAM_ID, F.MDS_FAM_ID)              AS MDS_FAM_ID,
    SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE,
    COALESCE(S.STORE_OH_UNITS, 0)                      AS STORE_OH_UNITS,
    COALESCE(S.STORE_OH_COST, 0)                       AS STORE_OH_COST,
    COALESCE(S.DC_RESERVED_UNITS, 0)                   AS DC_RESERVED_UNITS,
    COALESCE(S.DC_RESERVED_COST, 0)                    AS DC_RESERVED_COST,
    COALESCE(S.IN_TRANSIT_UNITS, 0)                    AS IN_TRANSIT_UNITS,
    COALESCE(S.IN_TRANSIT_COST, 0)                     AS IN_TRANSIT_COST,
    COALESCE(F.FC_OH_UNITS, 0)                         AS FC_OH_UNITS,
    COALESCE(F.FC_OH_COST, 0)                          AS FC_OH_COST,
    COALESCE(F.FC_OH_SORT_UNITS, 0)                    AS FC_OH_SORT_UNITS,
    COALESCE(F.FC_OH_SORT_COST, 0)                     AS FC_OH_SORT_COST,
    COALESCE(F.FC_OH_NONSORT_UNITS, 0)                 AS FC_OH_NONSORT_UNITS,
    COALESCE(F.FC_OH_NONSORT_COST, 0)                  AS FC_OH_NONSORT_COST,
    COALESCE(S.OMNI_CATG_NBR,  F.OMNI_CATG_NBR)        AS OMNI_CATG_NBR,
    COALESCE(S.OMNI_CATG_DESC, F.OMNI_CATG_DESC)       AS OMNI_CATG_DESC,
    COALESCE(S.OMNI_DEPT_NBR,  F.OMNI_DEPT_NBR)        AS OMNI_DEPT_NBR,
    COALESCE(S.OMNI_DEPT_DESC, F.OMNI_DEPT_DESC)       AS OMNI_DEPT_DESC,
    CASE
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (92,95)                      THEN 'PANTRY'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (90,91,1,82,96)              THEN 'CAC'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END AS SBU

  FROM store_channel S
  FULL OUTER JOIN fc_channel F
    ON S.BUS_DT         = F.BUS_DT
    AND S.MDS_FAM_ID    = F.MDS_FAM_ID
    AND S.OMNI_CATG_NBR = F.OMNI_CATG_NBR

  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON COALESCE(S.BUS_DT, F.BUS_DT) = CAL.CAL_DT

  INNER JOIN ITEM_CUR
    ON COALESCE(S.MDS_FAM_ID, F.MDS_FAM_ID) = ITEM_CUR.MDS_FAM_ID

  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON COALESCE(S.MDS_FAM_ID, F.MDS_FAM_ID) = OMNI.MDS_FAM_ID
    AND OMNI.VAL_IND      = 1
    AND OMNI.OP_CMPNY_CD  = 'WMT-US'
    AND OMNI.MDS_FAM_ID  <> -1

  WHERE (
    S.STORE_OH_UNITS     > 0
    OR S.DC_RESERVED_UNITS > 0
    OR S.IN_TRANSIT_UNITS  > 0
    OR F.FC_OH_UNITS       > 0
  )
  -- AND COALESCE(S.OMNI_DEPT_NBR, F.OMNI_DEPT_NBR) NOT IN (38,50,75)  -- H&W depts
);


-- ============================================================
-- LEGACY VIEWS (updated to expose ITEM_CUBE)
-- ============================================================

DROP VIEW IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_INVT`;
CREATE OR REPLACE VIEW `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_INVT` AS
SELECT
  BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, MDS_FAM_ID,
  ITEM_CUBE,
  STORE_OH_UNITS, STORE_OH_COST,
  DC_RESERVED_UNITS, DC_RESERVED_COST,
  OMNI_CATG_NBR, OMNI_CATG_DESC, OMNI_DEPT_NBR, OMNI_DEPT_DESC, SBU
FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`;

DROP VIEW IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_TRANSIT_INVT`;
CREATE OR REPLACE VIEW `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_TRANSIT_INVT` AS
SELECT
  BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, MDS_FAM_ID,
  ITEM_CUBE,
  IN_TRANSIT_UNITS, IN_TRANSIT_COST,
  OMNI_CATG_NBR, OMNI_CATG_DESC, OMNI_DEPT_NBR, OMNI_DEPT_DESC, SBU
FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`
WHERE IN_TRANSIT_UNITS > 0;

DROP VIEW IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_FC_INVT`;
CREATE OR REPLACE VIEW `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_FC_INVT` AS
SELECT
  BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, MDS_FAM_ID,
  ITEM_CUBE,
  FC_OH_UNITS, FC_OH_COST,
  OMNI_CATG_NBR, OMNI_CATG_DESC, OMNI_DEPT_NBR, OMNI_DEPT_DESC, SBU
FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`
WHERE FC_OH_UNITS > 0;


-- ============================================================
-- STEP 1.5: BACKROOM INVENTORY (no ITEM_CUBE - no OMNI join in source)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_BACKROOM`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_BACKROOM`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH FRIDAY_DATES AS (
    SELECT DISTINCT
      WM_FULL_YR_NBR AS WM_YEAR,
      WM_WK_NBR AS WM_WEEK,
      WM_YR_WK_NBR,
      CAL_DT AS BUS_DT
    FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
    WHERE (
      EXTRACT(DAYOFWEEK FROM CAL_DT) = 6
      OR (CAL_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
    )
      AND CAL_DT >= '2025-02-07'
      AND CAL_DT < CURRENT_DATE('America/Chicago')
  ),

  repl_item_map AS (
  SELECT rim.REPL_GROUP_NBR, rim.MDS_FAM_ID
  FROM (
    SELECT
      ic.REPL_GROUP_NBR,
      ic.MDS_FAM_ID,
      CASE WHEN omni.MDS_FAM_ID IS NOT NULL THEN 0 ELSE 1 END AS omni_missing
    FROM ITEM_CUR ic
    LEFT JOIN (
      SELECT DISTINCT MDS_FAM_ID
      FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
      WHERE VAL_IND = 1 AND OP_CMPNY_CD = 'WMT-US'
    ) omni ON omni.MDS_FAM_ID = ic.MDS_FAM_ID
    WHERE ic.REPL_GROUP_NBR IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ic.REPL_GROUP_NBR
      ORDER BY
        CASE WHEN omni.MDS_FAM_ID IS NOT NULL THEN 0 ELSE 1 END,  
        ic.MDS_FAM_ID                                               
    ) = 1
  ) rim
  )
    SELECT
        FRI.BUS_DT,
        FRI.WM_YEAR,
        FRI.WM_WEEK,
        FRI.WM_YR_WK_NBR,
        RIM.MDS_FAM_ID,
        SUM(COALESCE(BKRM.TOTAL_BACKROOM_QTY, 0))                                AS BACKROOM_UNITS,
        SAFE_DIVIDE(
          SUM(COALESCE(BKRM.TOTAL_BACKROOM_QTY, 0)
              * SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728)),
          NULLIF(SUM(COALESCE(BKRM.TOTAL_BACKROOM_QTY, 0)), 0)
        )                                                                         AS BACKROOM_WTAVG_CUBE,
        COALESCE(OMNI.OMNI_CATG_NBR,  0)         AS OMNI_CATG_NBR,
        COALESCE(OMNI.OMNI_CATG_DESC, 'OTHER')   AS OMNI_CATG_DESC,
        COALESCE(OMNI.OMNI_DEPT_NBR,  0)         AS OMNI_DEPT_NBR,
        COALESCE(OMNI.OMNI_DEPT_DESC, 'OTHER')   AS OMNI_DEPT_DESC,
        CASE
          WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
          WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
          WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
          WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
          WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
          WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
          WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
          WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
          ELSE 'OTHER'
        END                                                                       AS SBU
      FROM `wmt-instockinventory-datamart.RS001_WM_AD_HOC.store_backroom_qty` BKRM
      INNER JOIN FRIDAY_DATES FRI
        ON BKRM.CAL_DATE = FRI.BUS_DT
      LEFT JOIN repl_item_map RIM
        ON BKRM.REPL_GRP_NBR = RIM.REPL_GROUP_NBR
      LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
        ON RIM.MDS_FAM_ID = OMNI.MDS_FAM_ID
        AND OMNI.VAL_IND = 1
        AND OMNI.OP_CMPNY_CD = 'WMT-US'
        AND OMNI.MDS_FAM_ID <> -1
      WHERE COALESCE(BKRM.TOTAL_BACKROOM_QTY, 0) > 0
      GROUP BY ALL
    );

--   SELECT
--     FRI.BUS_DT,
--     FRI.WM_YEAR,
--     FRI.WM_WEEK,
--     FRI.WM_YR_WK_NBR,
--     BKRM.MDS_FAM_ID,
--     SUM(
--       CASE
--         WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
--         WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0)
--         ELSE 0
--       END
--     ) AS BACKROOM_UNITS,
--     SUM(
--       CASE
--         WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.LY_OH, 0)
--         WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.TY_OH, 0)
--         ELSE 0
--       END
--     ) AS STORE_OH_UNITS,
--     SAFE_DIVIDE(
--       SUM(
--         CASE WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
--              WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0) ELSE 0 END
--         * SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728)
--       ),
--       NULLIF(SUM(
--         CASE WHEN BKRM.WM_YEAR = 25 THEN COALESCE(BKRM.BACKROOM_QTY_LY, 0)
--              WHEN BKRM.WM_YEAR = 26 THEN COALESCE(BKRM.BACKROOM_QTY_TY, 0) ELSE 0 END
--       ), 0)
--     ) AS BACKROOM_WTAVG_CUBE,
--     CAST(BKRM.omni_catg_nbr AS INT64)  AS OMNI_CATG_NBR,
--     BKRM.omni_catg_desc                AS OMNI_CATG_DESC,
--     CAST(BKRM.omni_dept_nbr AS INT64)  AS OMNI_DEPT_NBR,
--     BKRM.omni_dept_desc                AS OMNI_DEPT_DESC,
--     BKRM.SBU_NM                        AS SBU
--   FROM `wmt-instockinventory-datamart.WM_AD_HOC.o0c046n_backroom_oh_inv_inv_sol_reporting_pre_20260126` BKRM
--   INNER JOIN FRIDAY_DATES FRI
--     ON (BKRM.WM_YEAR + 2000) = FRI.WM_YEAR
--     AND BKRM.WM_WEEK = FRI.WM_WEEK
--   LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
--     ON BKRM.MDS_FAM_ID = OMNI.MDS_FAM_ID
--     AND OMNI.VAL_IND = 1
--     AND OMNI.OP_CMPNY_CD = 'WMT-US'
--   WHERE BKRM.WM_YEAR IN (25, 26)
--     AND (
--       (BKRM.WM_YEAR = 25 AND BKRM.BACKROOM_QTY_LY > 0)
--       OR
--       (BKRM.WM_YEAR = 26 AND BKRM.BACKROOM_QTY_TY > 0)
--     )
--   GROUP BY
--     FRI.BUS_DT, FRI.WM_YEAR, FRI.WM_WEEK, FRI.WM_YR_WK_NBR,
--     BKRM.MDS_FAM_ID, BKRM.omni_catg_nbr, BKRM.omni_catg_desc,
--     BKRM.omni_dept_nbr, BKRM.omni_dept_desc, BKRM.SBU_NM
-- );

-- ============================================================
-- STEP 2: IN TRANSIT TO DC (CDI_ATLAS — RDC + GDC inbound)
-- ============================================================
CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_INTRANSIT_DC`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH item_unit_cost AS (
    SELECT BUS_DT, MDS_FAM_ID,
      SAFE_DIVIDE(SUM(COALESCE(TY_ON_HAND_COST_AMT,0)), NULLIF(SUM(COALESCE(TY_ON_HAND_QTY,0)),0)) AS UNIT_COST
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
    WHERE BUS_DT >= '2025-02-07'
      AND (EXTRACT(DAYOFWEEK FROM BUS_DT) = 6
           OR (BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
               AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6))
      AND BUS_DT < CURRENT_DATE('America/Chicago')
      AND FULFMT_CHNL_NM = 'Store' AND TY_ON_HAND_QTY > 0
    GROUP BY BUS_DT, MDS_FAM_ID
  ),

  -- ── Friday date spine ─────────────────────────────────────
  friday_spine AS (
    SELECT DISTINCT
      CAL_DT AS BUS_DT,
      WM_FULL_YR_NBR AS WM_YEAR,
      WM_WK_NBR      AS WM_WEEK,
      WM_YR_WK_NBR
    FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
    WHERE (EXTRACT(DAYOFWEEK FROM CAL_DT) = 6
           OR (CAL_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
               AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6))
      AND CAL_DT >= '2025-02-07'
      AND CAL_DT < CURRENT_DATE('America/Chicago')
  ),

  rdc_intransit AS (
    SELECT
      F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
      itm.MDS_FAM_ID,          
      cdi.IT_DC_NUM AS DC_NBR,
      SUM(cdi.IT_INTRANSIT_QTY) AS INTRANSIT_QTY
    FROM friday_spine F
    JOIN `wmt-edw-prod.US_SUPPLY_CHAIN_AMBIENT_RAW_VM.CDI_ATLAS_INV_ITEM_INVENTORY` cdi
      ON DATE(cdi.MERGE_TS) = F.BUS_DT
    INNER JOIN ITEM_CUR itm ON itm.ITEM_NBR = cdi.IT_ITEM_NBR
    WHERE cdi.IT_INTRANSIT_QTY > 0
    GROUP BY ALL
  ),
  gdc_intransit AS (
    SELECT DATE(cdi.MERGE_TS) AS BUS_DT, itm.MDS_FAM_ID,  
           cdi.FACILITY_NUMBER AS DC_NBR, SUM(cdi.IT_INTRANSIT_QTY) AS INTRANSIT_QTY
    FROM `wmt-edw-prod.US_SUPPLY_CHAIN_GROCERY_RAW_VM.CDI_ATLAS_INV_ITEM_INVENTORY` cdi
    INNER JOIN ITEM_CUR itm ON itm.ITEM_NBR = cdi.IT_ITEM_NBR
    WHERE cdi.IT_INTRANSIT_QTY > 0
      AND DATE(cdi.MERGE_TS) >= '2025-02-07'
      AND (EXTRACT(DAYOFWEEK FROM DATE(cdi.MERGE_TS)) = 6
           OR (DATE(cdi.MERGE_TS) = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
               AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6))
      AND DATE(cdi.MERGE_TS) < CURRENT_DATE('America/Chicago')
    GROUP BY ALL
  ),
  icc_acc_intransit AS (
    SELECT
      F.BUS_DT, F.WM_YEAR, F.WM_WEEK, F.WM_YR_WK_NBR,
      itm.MDS_FAM_ID,          -- ← TRUE MDS_FAM_ID (was cdi.IT_ITEM_NBR — BUG)
      cdi.IT_DC_NUM AS DC_NBR,
      SUM(cdi.IT_INTRANSIT_QTY) AS INTRANSIT_QTY
    FROM friday_spine F
    JOIN `wmt-edw-prod.US_SUPPLY_CHAIN_AMB_CC_IMPORTS_RAW_VM.CDI_ATLAS_INV_ITEM_INVENTORY` cdi
      ON DATE(cdi.MERGE_TS) = F.BUS_DT
    INNER JOIN ITEM_CUR itm ON itm.ITEM_NBR = cdi.IT_ITEM_NBR
    WHERE cdi.IT_INTRANSIT_QTY > 0
    GROUP BY ALL
  ),
  combined AS (
    SELECT * FROM rdc_intransit
    UNION ALL
    -- SELECT * FROM gdc_intransit
    -- UNION ALL
    SELECT * FROM icc_acc_intransit   
  )
  SELECT
    c.BUS_DT,
    CAL.WM_FULL_YR_NBR AS WM_YEAR, CAL.WM_WK_NBR AS WM_WEEK, CAL.WM_YR_WK_NBR,
    c.MDS_FAM_ID, c.DC_NBR,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE,
    SUM(c.INTRANSIT_QTY)                                     AS INTRANSIT_TO_DC_UNITS,
    SUM(c.INTRANSIT_QTY * COALESCE(UC.UNIT_COST, 0))         AS INTRANSIT_TO_DC_COST,
    OMNI.OMNI_CATG_NBR, OMNI.OMNI_CATG_DESC,
    OMNI.OMNI_DEPT_NBR, OMNI.OMNI_DEPT_DESC,
    SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE,
    CASE
      WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END AS SBU
  FROM combined c
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL ON c.BUS_DT = CAL.CAL_DT
  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON c.MDS_FAM_ID = OMNI.MDS_FAM_ID AND OMNI.VAL_IND = 1 AND OMNI.OP_CMPNY_CD = 'WMT-US'
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US' AND DC_DIM.DC_NBR = c.DC_NBR AND DC_DIM.CURRENT_IND = 'Y'
  LEFT JOIN item_unit_cost UC ON c.BUS_DT = UC.BUS_DT AND c.MDS_FAM_ID = UC.MDS_FAM_ID
  WHERE OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT LIKE '%ECOMM%'
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT LIKE '%FULFILLMENT%'
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT LIKE 'FC%'
  GROUP BY ALL
);



-- ============================================================
-- STEP 3: DC ON-HAND (with ITEM_CUBE)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_OH`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_OH`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  SELECT
    WDS.INVENTORY_DATE                AS BUS_DT,
    CAL.WM_FULL_YR_NBR                AS WM_YEAR,
    CAL.WM_WK_NBR                     AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    WDS.ITEM_NBR                      AS MDS_FAM_ID,
    WDS.WHSE_NBR                      AS DC_NBR,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_QTY)      AS DC_OH_UNITS,
    SUM(WDS.ON_HAND_QTY * WDS.WHPK_COST_AMT) AS DC_OH_COST,
    OMNI.OMNI_CATG_NBR,
    OMNI.OMNI_CATG_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE,
    CASE
      WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END AS SBU
  FROM `wmt-edw-prod.US_WM_VM.WHSE_DLY_SKU` WDS
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON WDS.INVENTORY_DATE = CAL.CAL_DT
  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON WDS.ITEM_NBR = OMNI.MDS_FAM_ID
    AND OMNI.VAL_IND = 1
    AND OMNI.OP_CMPNY_CD = 'WMT-US'
  INNER JOIN ITEM_CUR
    ON WDS.ITEM_NBR = ITEM_CUR.MDS_FAM_ID
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US'
    AND DC_DIM.DC_NBR = WDS.WHSE_NBR
    AND DC_DIM.CURRENT_IND = 'Y'
  WHERE WDS.INVENTORY_DATE >= '2025-02-07'
    AND (
      EXTRACT(DAYOFWEEK FROM WDS.INVENTORY_DATE) = 6
      OR (WDS.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
    )
    AND WDS.INVENTORY_DATE < CURRENT_DATE('America/Chicago')
    AND WDS.ON_HAND_QTY > 0
    AND OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')
  GROUP BY ALL
);


-- ============================================================
-- STEP 4: DC LABELED + UNLABELED (with ITEM_CUBE)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LBL`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LBL`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH item_unit_cost AS (
    SELECT
      BUS_DT,
      MDS_FAM_ID,
      SAFE_DIVIDE(
        SUM(COALESCE(TY_ON_HAND_COST_AMT, 0)),
        NULLIF(SUM(COALESCE(TY_ON_HAND_QTY, 0)), 0)
      ) AS UNIT_COST
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
    WHERE BUS_DT >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM BUS_DT) = 6
        OR (BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND BUS_DT < CURRENT_DATE('America/Chicago')
      AND FULFMT_CHNL_NM = 'Store'
      AND TY_ON_HAND_QTY > 0
    GROUP BY BUS_DT, MDS_FAM_ID
  )
  SELECT
    DC.INVENTORY_DATE               AS BUS_DT,
    CAL.WM_FULL_YR_NBR              AS WM_YEAR,
    CAL.WM_WK_NBR                   AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    DC.ITEM_NBR                     AS MDS_FAM_ID,
    DC.DC_NBR,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN') AS DC_TYPE,
    DC.INVT_TYPE_CODE,
    SUM(DC.WHPK_INVT_QTY * ITEM.WHPK_QTY)                                   AS DC_LBL_UNLBL_UNITS,
    SUM(DC.WHPK_INVT_QTY * ITEM.WHPK_QTY * COALESCE(UC.UNIT_COST, 0))       AS DC_LBL_UNLBL_COST,
    OMNI.OMNI_CATG_NBR,
    OMNI.OMNI_CATG_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE,
    CASE
      WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END AS SBU
  FROM `wmt-edw-prod.US_WM_VM.DC_DLY_INVT_TYPE` DC
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON DC.INVENTORY_DATE = CAL.CAL_DT
  LEFT JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ITEM
    ON DC.ITEM_NBR = ITEM.MDS_FAM_ID
  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON DC.ITEM_NBR = OMNI.MDS_FAM_ID
    AND OMNI.VAL_IND = 1
    AND OMNI.OP_CMPNY_CD = 'WMT-US'
  INNER JOIN ITEM_CUR ITEM_EXCL
    ON DC.ITEM_NBR = ITEM_EXCL.MDS_FAM_ID
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US'
    AND DC_DIM.DC_NBR = DC.DC_NBR
    AND DC_DIM.CURRENT_IND = 'Y'
  LEFT JOIN item_unit_cost UC
    ON DC.INVENTORY_DATE = UC.BUS_DT
    AND DC.ITEM_NBR = UC.MDS_FAM_ID
  WHERE DC.INVENTORY_DATE >= '2025-02-07'
    AND (
      EXTRACT(DAYOFWEEK FROM DC.INVENTORY_DATE) = 6
      OR (DC.INVENTORY_DATE = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
          AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
    )
    AND DC.INVENTORY_DATE < CURRENT_DATE('America/Chicago')
    AND DC.INVT_TYPE_CODE IN ('L', 'S', 'U')
    AND DC.WHPK_INVT_QTY > 0
    AND OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
    AND DC_DIM.DC_TYPE_DESC IS NOT NULL
    AND UPPER(DC_DIM.DC_TYPE_DESC) NOT IN ('PHARMACY', 'ADMINISTRATION ACCOUNT')
  GROUP BY ALL
);


-- ============================================================
-- STEP 5: STOCK TRANSFER ORDERS (with ITEM_CUBE)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH item_unit_cost AS (
    SELECT
      BUS_DT,
      MDS_FAM_ID,
      SAFE_DIVIDE(
        SUM(COALESCE(TY_ON_HAND_COST_AMT, 0)),
        NULLIF(SUM(COALESCE(TY_ON_HAND_QTY, 0)), 0)
      ) AS UNIT_COST
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
    WHERE BUS_DT >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM BUS_DT) = 6
        OR (BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND BUS_DT < CURRENT_DATE('America/Chicago')
      AND FULFMT_CHNL_NM = 'Store'
      AND TY_ON_HAND_QTY > 0
    GROUP BY BUS_DT, MDS_FAM_ID
  ),

  STO_BASE AS (
    SELECT
      DATE(AOF.ALLOC_CREATE_TS)      AS BUS_DT,
      ITEM.ITEM_NBR                   AS MDS_FAM_ID,
      AOF.TO_STORE_NBR                AS DEST_WHSE_NBR,
      AOF.FROM_STORE_NBR              AS SRC_WHSE_NBR,
      SUM(AOF.WHPK_ORD_QTY * AOF.WHPK_QTY) AS STO_UNITS
    FROM `wmt-edw-prod.US_WM_OMS_VM.ALLOC_ORDER_FACT` AOF
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.STOCK_TRANSFER_ORDER` STO
      ON AOF.STOCK_TRANSFER_ORD_NBR = STO.STOCK_TRANSFER_ORD_NBR
    INNER JOIN `wmt-edw-prod.US_WM_VM.ITEM` ITEM
      ON ITEM.OLD_NBR = AOF.ITEM_NBR
      AND ITEM.OBSOLETE_DATE IS NULL
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_STO_PO_XREF` STO_XREF
      ON STO_XREF.STOCK_TRANSFER_ORD_NBR = AOF.STOCK_TRANSFER_ORD_NBR
    WHERE DATE(AOF.ALLOC_CREATE_TS) >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM DATE(AOF.ALLOC_CREATE_TS)) = 6
        OR (DATE(AOF.ALLOC_CREATE_TS) = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND DATE(AOF.ALLOC_CREATE_TS) < CURRENT_DATE('America/Chicago')
      AND AOF.WHPK_ORD_QTY > 0
    GROUP BY ALL
  )
  SELECT
    S.BUS_DT,
    CAL.WM_FULL_YR_NBR                AS WM_YEAR,
    CAL.WM_WK_NBR                     AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    S.MDS_FAM_ID,
    S.DEST_WHSE_NBR                    AS DEST_DC_NBR,
    COALESCE(DEST_DC.DC_TYPE_DESC, 'UNKNOWN') AS DEST_DC_TYPE,
    S.SRC_WHSE_NBR                     AS SRC_DC_NBR,
    COALESCE(SRC_DC.DC_TYPE_DESC, 'UNKNOWN')  AS SRC_DC_TYPE,
    CASE
      WHEN DEST_DC.DC_TYPE_DESC IS NOT NULL AND SRC_DC.DC_TYPE_DESC IS NOT NULL THEN 'DC_TO_DC'
      WHEN DEST_DC.DC_TYPE_DESC IS NULL     AND SRC_DC.DC_TYPE_DESC IS NOT NULL THEN 'DC_TO_STORE'
      WHEN DEST_DC.DC_TYPE_DESC IS NOT NULL AND SRC_DC.DC_TYPE_DESC IS NULL     THEN 'STORE_TO_DC'
      ELSE 'OTHER'
    END AS STO_FLOW_TYPE,
    SUM(S.STO_UNITS)                                         AS STO_UNITS,
    SUM(S.STO_UNITS * COALESCE(UC.UNIT_COST, 0))             AS STO_COST,
    OMNI.OMNI_CATG_NBR,
    OMNI.OMNI_CATG_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    SAFE_DIVIDE(OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY, 1728) AS ITEM_CUBE,
    CASE
      WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END AS SBU
  FROM STO_BASE S
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON S.BUS_DT = CAL.CAL_DT
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DEST_DC
    ON DEST_DC.COUNTRY_CODE = 'US'
    AND DEST_DC.DC_NBR = S.DEST_WHSE_NBR
    AND DEST_DC.CURRENT_IND = 'Y'
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` SRC_DC
    ON SRC_DC.COUNTRY_CODE = 'US'
    AND SRC_DC.DC_NBR = S.SRC_WHSE_NBR
    AND SRC_DC.CURRENT_IND = 'Y'
  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON S.MDS_FAM_ID = OMNI.MDS_FAM_ID
    AND OMNI.VAL_IND = 1
    AND OMNI.OP_CMPNY_CD = 'WMT-US'
  LEFT JOIN item_unit_cost UC
    ON S.BUS_DT = UC.BUS_DT
    AND S.MDS_FAM_ID = UC.MDS_FAM_ID
  WHERE OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
  GROUP BY ALL
);


-- ============================================================
-- STEP 5.5: ON YARD INVENTORY (TUP-based — replaces RDC_FDC_EVENT_POS_HIST)
-- Trailer source : wmt-cp-prod.TUP.TRAILER_STATUS_CHANGES
-- Unit paths     : OMS (PO_NBR join) + STO (XREF → ALLOC_ORDER_FACT)
-- Inflation fixes: V4 (multi-alloc), V5 (multi-delivery per PO), V6 (sched dups)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ON_YARD`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ON_YARD`
PARTITION BY BUS_DT
CLUSTER BY OMNI_CATG_NBR
AS (
  WITH

  -- ── Unit cost: aligned with all other steps ──────────────────
  item_unit_cost AS (
    SELECT
      BUS_DT,
      MDS_FAM_ID,
      SAFE_DIVIDE(
        SUM(COALESCE(TY_ON_HAND_COST_AMT, 0)),
        NULLIF(SUM(COALESCE(TY_ON_HAND_QTY, 0)), 0)
      ) AS UNIT_COST
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_fd_app_secure.fd_omni_chnl_item_dly`
    WHERE BUS_DT >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM BUS_DT) = 6
        OR (BUS_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND BUS_DT < CURRENT_DATE('America/Chicago')
      AND FULFMT_CHNL_NM = 'Store'
      AND TY_ON_HAND_QTY > 0
    GROUP BY BUS_DT, MDS_FAM_ID
  ),

  -- ── Friday date spine × TUP DC list ──────────────────────────
  -- TUP_DCS_CONFIG auto-includes all active RDC/FDC + provides timezone
  DATES AS (
    SELECT DISTINCT
      DC.DC_NUMBER,
      CAL.CAL_DT                    AS REPORT_DATE,
      DC.TIMEZONE,
      TIMESTAMP_TRUNC(
        TIMESTAMP(
          DATETIME_ADD(DATETIME(CAL.CAL_DT), INTERVAL 6 HOUR),
          DC.TIMEZONE
        ),
        SECOND
      )                             AS SNAPSHOT_TIME_UTC
    FROM `wmt-cp-prod.TUP.TUP_DCS_CONFIG` DC
    CROSS JOIN (
      SELECT DISTINCT CAL_DT
      FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
      WHERE (
        EXTRACT(DAYOFWEEK FROM CAL_DT) = 6
        OR (CAL_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
        AND CAL_DT >= '2025-02-07'
        AND CAL_DT < CURRENT_DATE('America/Chicago')
    ) CAL
    WHERE DC.RDC_IND = 1 OR DC.FDC_IND = 1
  ),

  -- ── Trailer layer: latest status per trip before 6 AM snapshot ─
  on_yard_raw AS (
    SELECT
      D.DC_NUMBER,
      D.REPORT_DATE,
      S.TRIP_ID,
      T.DELIVERY_NUMBER,
      S.TRAILER_STATUS_CODE
    FROM DATES D
    INNER JOIN `wmt-cp-prod.TUP.TRAILER_STATUS_CHANGES` S
      ON D.DC_NUMBER = S.DC_NUMBER
      AND S.ENTITY_OPERATION_TS <= D.SNAPSHOT_TIME_UTC
    INNER JOIN `wmt-edw-prod.US_SUPPLY_CHAIN_SCT_NONCAT_VM.TRAILER` T
      ON (D.DC_NUMBER = CAST(T.DC_NUMBER AS INT64)
       OR D.DC_NUMBER = CAST(T.PRIMARY_DC_NUMBER AS INT64))
      AND S.TRIP_ID = T.TRIP_ID
      AND (TIMESTAMP_TRUNC(T.GATE_OUT_TS, SECOND) >= TIMESTAMP_TRUNC(D.SNAPSHOT_TIME_UTC, SECOND)
           OR T.GATE_OUT_TS IS NULL)
      AND TIMESTAMP_TRUNC(T.ARRIVAL_TIME, SECOND) <= TIMESTAMP_TRUNC(D.SNAPSHOT_TIME_UTC, SECOND)
      AND T.SOURCE_SYSTEM = 'yms2'
      AND T.ARRIVAL_TIME IS NOT NULL
      AND DATE(T.ENTITY_OPERATION_TS) >= DATE(D.SNAPSHOT_TIME_UTC) - 21  -- reduced from 60; trailers rarely on yard >3 weeks
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY D.DC_NUMBER, S.TRIP_ID, D.SNAPSHOT_TIME_UTC
      ORDER BY S.ENTITY_OPERATION_TS DESC
    ) = 1
  ),

  on_yard AS (
    SELECT * FROM on_yard_raw
    WHERE TRAILER_STATUS_CODE IN ('IN','CI','XI','XD','CS','XR')
  ),

  -- ── Scheduler dedup — fixes V6 duplicate rows ────────────────
  sched_dedup AS (
    SELECT DISTINCT
      CAST(dc_nbr AS INT64) AS dc_nbr,
      delivery_number,
      po_number,
      po_type
    FROM `wmt-supply-chain-ais.IVCM.scheduler_ais_deliveries_src`
    WHERE po_number IS NOT NULL
      AND po_number <> '9999999999'
  ),

  -- ── OMS path: DISTINCT POs per day per DC — fixes V5 ─────────
  -- Corrected join key: PO_NBR (not OMS_PO_NBR)
  oms_pos_on_yard AS (
    SELECT DISTINCT
      oy.REPORT_DATE,
      oy.DC_NUMBER,
      po.OMS_PO_NBR
    FROM on_yard oy
    INNER JOIN sched_dedup del
      ON del.dc_nbr        = oy.DC_NUMBER
      AND del.delivery_number = oy.DELIVERY_NUMBER
      AND del.po_type NOT IN (23, 28, 29)
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PURCHASE_ORDER` po
      ON del.po_number     = po.PO_NBR
      AND po.COUNTRY_CODE  = 'US'
  ),

  -- OMS → item units (ITEM_CUR from STEP 0 applies item exclusions)
  oms_units AS (
    SELECT
      p.REPORT_DATE,
      p.DC_NUMBER,
      ic.MDS_FAM_ID,
      SUM(pol.VNPK_ORD_QTY * pol.VNPK_QTY)  AS ON_YARD_UNITS
    FROM oms_pos_on_yard p
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_PO_LINE` pol
      ON pol.OMS_PO_NBR         = p.OMS_PO_NBR
      AND pol.PO_LINE_STATUS_CD <> 1300
      AND pol.VNPK_ORD_QTY      > 0
    INNER JOIN ITEM_CUR ic ON ic.ITEM_NBR = pol.ITEM_NBR
    GROUP BY 1, 2, 3
  ),

  -- ── STO path: DISTINCT STOs per day per DC — fixes V5 + V4 ───
  sto_nbrs_on_yard AS (
    SELECT DISTINCT
      oy.REPORT_DATE,
      oy.DC_NUMBER,
      sx.STOCK_TRANSFER_ORD_NBR
    FROM on_yard oy
    INNER JOIN sched_dedup del
      ON del.dc_nbr           = oy.DC_NUMBER
      AND del.delivery_number = oy.DELIVERY_NUMBER
      AND del.po_type IN (23, 28, 29)
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.OMS_STO_PO_XREF` sx
      ON del.po_number = CAST(sx.XREF_PO_NBR AS STRING)
  ),

  -- STO → item units (GROUP BY collapses multi-alloc rows — fixes V4)
  sto_units AS (
    SELECT
      s.REPORT_DATE,
      s.DC_NUMBER,
      ic.MDS_FAM_ID,
      SUM(aof.WHPK_ORD_QTY * aof.WHPK_QTY)  AS ON_YARD_UNITS
    FROM sto_nbrs_on_yard s
    INNER JOIN `wmt-edw-prod.US_WM_OMS_VM.ALLOC_ORDER_FACT` aof
      ON aof.STOCK_TRANSFER_ORD_NBR = s.STOCK_TRANSFER_ORD_NBR
    INNER JOIN ITEM_CUR ic ON ic.ITEM_NBR = aof.ITEM_NBR
    GROUP BY 1, 2, 3
  ),

  -- ── Combine both paths ───────────────────────────────────────
  all_units AS (
    SELECT * FROM oms_units
    UNION ALL
    SELECT * FROM sto_units
  )

  -- ── Final output — same schema as old STEP 5.5 ───────────────
  -- MDS_FAM_ID × DC_NBR grain so STEP 7's ITEM_CUBE multiplication is correct
  SELECT
    u.REPORT_DATE                                                       AS BUS_DT,
    CAL.WM_FULL_YR_NBR                                                  AS WM_YEAR,
    CAL.WM_WK_NBR                                                       AS WM_WEEK,
    CAL.WM_YR_WK_NBR,
    u.MDS_FAM_ID,
    u.DC_NUMBER                                                         AS DC_NBR,
    COALESCE(DC_DIM.DC_TYPE_DESC, 'UNKNOWN')                            AS DC_TYPE,
    SUM(u.ON_YARD_UNITS)                                                AS ON_YARD_UNITS,
    SUM(u.ON_YARD_UNITS * COALESCE(UC.UNIT_COST, 0))                    AS ON_YARD_COST,
    OMNI.OMNI_CATG_NBR,
    OMNI.OMNI_CATG_DESC,
    OMNI.OMNI_DEPT_NBR,
    OMNI.OMNI_DEPT_DESC,
    SAFE_DIVIDE(
      OMNI.PKG_HT_IN_QTY * OMNI.PKG_WDTH_IN_QTY * OMNI.PKG_LEN_IN_QTY,
      1728
    )                                                                   AS ITEM_CUBE,
    CASE
      WHEN OMNI.OMNI_DEPT_NBR IN (92,95)                      THEN 'PANTRY'
      WHEN OMNI.OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
      WHEN OMNI.OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
      WHEN OMNI.OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
      WHEN OMNI.OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
      WHEN OMNI.OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
      WHEN OMNI.OMNI_DEPT_NBR IN (80,81,93,94,97,98)          THEN 'FRESH'
      WHEN OMNI.OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
      ELSE 'OTHER'
    END                                                                 AS SBU
  FROM all_units u
  INNER JOIN `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM` CAL
    ON CAL.CAL_DT = u.REPORT_DATE
  LEFT JOIN `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw` OMNI
    ON u.MDS_FAM_ID      = OMNI.MDS_FAM_ID
    AND OMNI.VAL_IND     = 1
    AND OMNI.OP_CMPNY_CD = 'WMT-US'
  LEFT JOIN `wmt-edw-prod.WW_CORE_DIM_VM.DC_DIM_CUR` DC_DIM
    ON DC_DIM.COUNTRY_CODE = 'US'
    AND DC_DIM.DC_NBR      = u.DC_NUMBER
    AND DC_DIM.CURRENT_IND = 'Y'
  LEFT JOIN item_unit_cost UC
    ON UC.BUS_DT      = u.REPORT_DATE
    AND UC.MDS_FAM_ID = u.MDS_FAM_ID
  WHERE OMNI.OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS','OTHER','WALMART SERVICES')
  GROUP BY ALL

);


-- ============================================================
-- STEP 7: COMBINED INVENTORY TABLE (with AVG_ITEM_CUBE)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_COMBINED`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_COMBINED`
PARTITION BY BUS_DT
CLUSTER BY OMNI_DEPT_NBR, OMNI_CATG_NBR
AS (

  WITH store_fc_agg AS (
    SELECT
      BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR,
      OMNI_CATG_NBR, OMNI_CATG_DESC,
      OMNI_DEPT_NBR, OMNI_DEPT_DESC,
      SBU,
      SUM(STORE_OH_UNITS)    AS STORE_OH_UNITS,
      SUM(STORE_OH_COST)     AS STORE_OH_COST,
      SUM(DC_RESERVED_UNITS) AS DC_RESERVED_UNITS,
      SUM(DC_RESERVED_COST)  AS DC_RESERVED_COST,
      SUM(IN_TRANSIT_UNITS)  AS IN_TRANSIT_UNITS,
      SUM(IN_TRANSIT_COST)   AS IN_TRANSIT_COST,
      SUM(FC_OH_UNITS)       AS FC_OH_UNITS,
      SUM(FC_OH_COST)        AS FC_OH_COST,
      SUM(FC_OH_SORT_UNITS)        AS FC_OH_SORT_UNITS,
      SUM(FC_OH_SORT_COST)         AS FC_OH_SORT_COST,
      SUM(FC_OH_NONSORT_UNITS)     AS FC_OH_NONSORT_UNITS,
      SUM(FC_OH_NONSORT_COST)      AS FC_OH_NONSORT_COST,
      SUM(STORE_OH_UNITS * ITEM_CUBE) AS STORE_TOTAL_CUBE,
      SUM(DC_RESERVED_UNITS * ITEM_CUBE) AS DC_RESERVED_TOTAL_CUBE,
      SUM(IN_TRANSIT_UNITS  * ITEM_CUBE)  AS TRANSIT_TOTAL_CUBE,
      SUM(FC_OH_UNITS       * ITEM_CUBE)     AS FC_TOTAL_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STORE_FC_INVT`
    GROUP BY ALL
  ),


    backroom_agg AS (
      SELECT
        BUS_DT,
        OMNI_CATG_NBR,
        SUM(BACKROOM_UNITS)                          AS BACKROOM_UNITS,
        SUM(BACKROOM_UNITS * COALESCE(b.BACKROOM_WTAVG_CUBE, 0))    AS BACKROOM_TOTAL_CUBE
      FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_BACKROOM` b
      WHERE OMNI_CATG_NBR <> 0   -- matched rows join to store_fc normally
      GROUP BY BUS_DT, OMNI_CATG_NBR
    ),

    backroom_other_agg AS (
  SELECT
    b.BUS_DT, b.WM_YEAR, b.WM_WEEK, b.WM_YR_WK_NBR,
    b.OMNI_CATG_NBR, b.OMNI_CATG_DESC,
    b.OMNI_DEPT_NBR, b.OMNI_DEPT_DESC,
    b.SBU,
    SUM(b.BACKROOM_UNITS)                          AS BACKROOM_UNITS,
    SUM(b.BACKROOM_UNITS * COALESCE(b.BACKROOM_WTAVG_CUBE, 0)) AS BACKROOM_TOTAL_CUBE
  FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_BACKROOM` b
  WHERE NOT EXISTS (
    SELECT 1 FROM store_fc_agg sfc
    WHERE sfc.BUS_DT = b.BUS_DT
      AND sfc.OMNI_CATG_NBR = b.OMNI_CATG_NBR
  )
  GROUP BY ALL
),

  intransit_dc_agg AS (
  SELECT
    BUS_DT, OMNI_CATG_NBR,
    SUM(INTRANSIT_TO_DC_UNITS)  AS INTRANSIT_TO_DC_UNITS,
    SUM(INTRANSIT_TO_DC_COST)   AS INTRANSIT_TO_DC_COST,
    SUM(INTRANSIT_TO_DC_UNITS * ITEM_CUBE) AS INTRANSIT_DC_TOTAL_CUBE
  FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_INTRANSIT_DC`
  GROUP BY BUS_DT, OMNI_CATG_NBR
  ),

  dc_oh_agg AS (
    SELECT
      BUS_DT,
      OMNI_CATG_NBR,
      SUM(DC_OH_UNITS)  AS DC_OH_UNITS,
      SUM(DC_OH_COST)   AS DC_OH_COST,
      SUM(DC_OH_UNITS * ITEM_CUBE) AS DC_OH_TOTAL_CUBE,
      SUM(CASE WHEN DC_TYPE = 'Regional'              THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_REGIONAL_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Grocery'               THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_GROCERY_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Fashion'               THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_FASHION_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Imports'               THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_IMPORTS_UNITS,
      SUM(CASE WHEN DC_TYPE = 'GIDC'                  THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_GIDC_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Market Service Center' THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_MSC_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Support'               THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_SUPPORT_UNITS,
      SUM(CASE WHEN DC_TYPE NOT IN ('Regional','Grocery','Fashion','Imports','GIDC','Market Service Center','Support')
               THEN DC_OH_UNITS ELSE 0 END) AS DC_OH_OTHER_UNITS
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_OH`
    GROUP BY BUS_DT, OMNI_CATG_NBR
  ),

  dc_lbl_agg AS (
    SELECT
      BUS_DT,
      OMNI_CATG_NBR,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_UNITS ELSE 0 END) AS DC_LABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_COST  ELSE 0 END) AS DC_LABELED_COST,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U'        THEN DC_LBL_UNLBL_UNITS ELSE 0 END) AS DC_UNLABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U'        THEN DC_LBL_UNLBL_COST  ELSE 0 END) AS DC_UNLABELED_COST,

        SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_UNITS * ITEM_CUBE ELSE 0 END) AS DC_LBL_TOTAL_CUBE,
        SUM(CASE WHEN INVT_TYPE_CODE = 'U' THEN DC_LBL_UNLBL_UNITS * ITEM_CUBE ELSE 0 END) AS DC_UNLBL_TOTAL_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LBL`
    GROUP BY BUS_DT, OMNI_CATG_NBR
  ),

  sto_agg AS (
    SELECT
      BUS_DT,
      OMNI_CATG_NBR,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_COST  ELSE 0 END) AS STO_IN_TRANSIT_TO_DC_COST,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Regional'              THEN STO_UNITS ELSE 0 END) AS STO_TO_REGIONAL_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Grocery'               THEN STO_UNITS ELSE 0 END) AS STO_TO_GROCERY_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Fashion'               THEN STO_UNITS ELSE 0 END) AS STO_TO_FASHION_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Imports'               THEN STO_UNITS ELSE 0 END) AS STO_TO_IMPORTS_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'GIDC'                  THEN STO_UNITS ELSE 0 END) AS STO_TO_GIDC_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Market Service Center' THEN STO_UNITS ELSE 0 END) AS STO_TO_MSC_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') AND DEST_DC_TYPE = 'Support'               THEN STO_UNITS ELSE 0 END) AS STO_TO_SUPPORT_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC')
               AND DEST_DC_TYPE NOT IN ('Regional','Grocery','Fashion','Imports','GIDC','Market Service Center','Support')
               THEN STO_UNITS ELSE 0 END) AS STO_TO_OTHER_UNITS,
      SUM(CASE WHEN STO_FLOW_TYPE IN ('DC_TO_DC','STORE_TO_DC') THEN STO_UNITS * ITEM_CUBE ELSE 0 END) AS STO_TOTAL_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`
    GROUP BY BUS_DT, OMNI_CATG_NBR
  ),

  on_yard_agg AS (
    SELECT
      BUS_DT,
      OMNI_CATG_NBR,
      SUM(ON_YARD_UNITS)  AS ON_YARD_UNITS,
      SUM(ON_YARD_COST)   AS ON_YARD_COST,
      SUM(ON_YARD_UNITS * ITEM_CUBE) AS ON_YARD_TOTAL_CUBE,
      SUM(CASE WHEN DC_TYPE = 'Regional'              THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_REGIONAL_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Grocery'               THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_GROCERY_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Fashion'               THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_FASHION_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Imports'               THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_IMPORTS_UNITS,
      SUM(CASE WHEN DC_TYPE = 'GIDC'                  THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_GIDC_UNITS,
      SUM(CASE WHEN DC_TYPE = 'Market Service Center' THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_MSC_UNITS,
      SUM(CASE WHEN DC_TYPE NOT IN ('Regional','Grocery','Fashion','Imports','GIDC','Market Service Center')
               THEN ON_YARD_UNITS ELSE 0 END) AS ON_YARD_OTHER_UNITS
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ON_YARD`
    GROUP BY BUS_DT, OMNI_CATG_NBR
  )

  SELECT
    sfc.BUS_DT,
    sfc.WM_YEAR,
    sfc.WM_WEEK,
    sfc.WM_YR_WK_NBR,
    sfc.OMNI_CATG_NBR,
    sfc.OMNI_CATG_DESC,
    sfc.OMNI_DEPT_NBR,
    sfc.OMNI_DEPT_DESC,
    sfc.SBU,

    sfc.STORE_TOTAL_CUBE,
    COALESCE(b.BACKROOM_TOTAL_CUBE, 0)   AS BACKROOM_TOTAL_CUBE,

    idc.INTRANSIT_DC_TOTAL_CUBE,

    sfc.STORE_TOTAL_CUBE - COALESCE(b.BACKROOM_TOTAL_CUBE, 0)   AS ON_FLOOR_TOTAL_CUBE,
    sfc.DC_RESERVED_TOTAL_CUBE,
    sfc.TRANSIT_TOTAL_CUBE,
    sfc.FC_TOTAL_CUBE,
    d.DC_OH_TOTAL_CUBE,
    l.DC_LBL_TOTAL_CUBE,
    l.DC_UNLBL_TOTAL_CUBE,
    st.STO_TOTAL_CUBE,
    oy.ON_YARD_TOTAL_CUBE,

    sfc.STORE_OH_UNITS,
    sfc.STORE_OH_COST,
    COALESCE(b.BACKROOM_UNITS, 0)   AS BACKROOM_UNITS,
    sfc.STORE_OH_UNITS - COALESCE(b.BACKROOM_UNITS, 0)    AS ON_FLOOR_UNITS,
    sfc.DC_RESERVED_UNITS,
    sfc.DC_RESERVED_COST,
    sfc.IN_TRANSIT_UNITS,
    sfc.IN_TRANSIT_COST,
    COALESCE(d.DC_OH_UNITS, 0)         AS DC_OH_UNITS,
    COALESCE(d.DC_OH_COST, 0)          AS DC_OH_COST,
    COALESCE(l.DC_LABELED_UNITS, 0)    AS DC_LABELED_UNITS,
    COALESCE(l.DC_LABELED_COST, 0)     AS DC_LABELED_COST,
    COALESCE(l.DC_UNLABELED_UNITS, 0)  AS DC_UNLABELED_UNITS,
    COALESCE(l.DC_UNLABELED_COST, 0)   AS DC_UNLABELED_COST,
    COALESCE(d.DC_OH_REGIONAL_UNITS, 0) AS DC_OH_REGIONAL_UNITS,
    COALESCE(d.DC_OH_GROCERY_UNITS, 0)  AS DC_OH_GROCERY_UNITS,
    COALESCE(d.DC_OH_FASHION_UNITS, 0)  AS DC_OH_FASHION_UNITS,
    COALESCE(d.DC_OH_IMPORTS_UNITS, 0)  AS DC_OH_IMPORTS_UNITS,
    COALESCE(d.DC_OH_GIDC_UNITS, 0)     AS DC_OH_GIDC_UNITS,
    COALESCE(d.DC_OH_MSC_UNITS, 0)      AS DC_OH_MSC_UNITS,
    COALESCE(d.DC_OH_SUPPORT_UNITS, 0)  AS DC_OH_SUPPORT_UNITS,
    COALESCE(d.DC_OH_OTHER_UNITS, 0)    AS DC_OH_OTHER_UNITS,
    COALESCE(st.STO_IN_TRANSIT_TO_DC_UNITS, 0) AS STO_IN_TRANSIT_TO_DC_UNITS,
    COALESCE(st.STO_IN_TRANSIT_TO_DC_COST, 0)  AS STO_IN_TRANSIT_TO_DC_COST,
    COALESCE(idc.INTRANSIT_TO_DC_UNITS, 0) AS INTRANSIT_TO_DC_UNITS,
    COALESCE(idc.INTRANSIT_TO_DC_COST, 0)  AS INTRANSIT_TO_DC_COST, 
    COALESCE(st.STO_TO_REGIONAL_UNITS, 0)       AS STO_TO_REGIONAL_UNITS,
    COALESCE(st.STO_TO_GROCERY_UNITS, 0)        AS STO_TO_GROCERY_UNITS,
    COALESCE(st.STO_TO_FASHION_UNITS, 0)        AS STO_TO_FASHION_UNITS,
    COALESCE(st.STO_TO_IMPORTS_UNITS, 0)        AS STO_TO_IMPORTS_UNITS,
    COALESCE(st.STO_TO_GIDC_UNITS, 0)           AS STO_TO_GIDC_UNITS,
    COALESCE(st.STO_TO_MSC_UNITS, 0)            AS STO_TO_MSC_UNITS,
    COALESCE(st.STO_TO_SUPPORT_UNITS, 0)        AS STO_TO_SUPPORT_UNITS,
    COALESCE(st.STO_TO_OTHER_UNITS, 0)          AS STO_TO_OTHER_UNITS,
    sfc.FC_OH_UNITS,
    sfc.FC_OH_COST,
    sfc.FC_OH_SORT_UNITS,
    sfc.FC_OH_SORT_COST,
    sfc.FC_OH_NONSORT_UNITS,
    sfc.FC_OH_NONSORT_COST,
    COALESCE(oy.ON_YARD_UNITS, 0)          AS ON_YARD_UNITS,
    COALESCE(oy.ON_YARD_COST, 0)           AS ON_YARD_COST,
    COALESCE(oy.ON_YARD_REGIONAL_UNITS, 0) AS ON_YARD_REGIONAL_UNITS,
    COALESCE(oy.ON_YARD_GROCERY_UNITS, 0)  AS ON_YARD_GROCERY_UNITS,
    COALESCE(oy.ON_YARD_FASHION_UNITS, 0)  AS ON_YARD_FASHION_UNITS,
    COALESCE(oy.ON_YARD_IMPORTS_UNITS, 0)  AS ON_YARD_IMPORTS_UNITS,
    COALESCE(oy.ON_YARD_GIDC_UNITS, 0)     AS ON_YARD_GIDC_UNITS,
    COALESCE(oy.ON_YARD_MSC_UNITS, 0)      AS ON_YARD_MSC_UNITS,
    COALESCE(oy.ON_YARD_OTHER_UNITS, 0)    AS ON_YARD_OTHER_UNITS,

    ( sfc.STORE_OH_UNITS
    + sfc.DC_RESERVED_UNITS
    + sfc.IN_TRANSIT_UNITS
    + COALESCE(d.DC_OH_UNITS, 0)
    + COALESCE(l.DC_LABELED_UNITS, 0)
    + COALESCE(l.DC_UNLABELED_UNITS, 0)
    -- + COALESCE(st.STO_IN_TRANSIT_TO_DC_UNITS, 0)
    + sfc.FC_OH_UNITS
    + COALESCE(oy.ON_YARD_UNITS, 0)
    + COALESCE(idc.INTRANSIT_TO_DC_UNITS, 0)
    ) AS TOTAL_NETWORK_UNITS,

    ( sfc.STORE_OH_COST
    + sfc.DC_RESERVED_COST
    + sfc.IN_TRANSIT_COST
    + COALESCE(d.DC_OH_COST, 0)
    + COALESCE(l.DC_LABELED_COST, 0)
    + COALESCE(l.DC_UNLABELED_COST, 0)
    -- + COALESCE(st.STO_IN_TRANSIT_TO_DC_COST, 0)
    + sfc.FC_OH_COST
    + COALESCE(oy.ON_YARD_COST, 0)
    + COALESCE(idc.INTRANSIT_TO_DC_COST, 0)
    ) AS TOTAL_NETWORK_COST

  FROM store_fc_agg sfc
  LEFT JOIN backroom_agg b  ON sfc.BUS_DT = b.BUS_DT  AND sfc.OMNI_CATG_NBR = b.OMNI_CATG_NBR
  -- LEFT JOIN backroom_other_agg bo ON sfc.BUS_DT = bo.BUS_DT
  LEFT JOIN dc_oh_agg d     ON sfc.BUS_DT = d.BUS_DT  AND sfc.OMNI_CATG_NBR = d.OMNI_CATG_NBR
  LEFT JOIN dc_lbl_agg l    ON sfc.BUS_DT = l.BUS_DT  AND sfc.OMNI_CATG_NBR = l.OMNI_CATG_NBR
  LEFT JOIN intransit_dc_agg idc ON sfc.BUS_DT = idc.BUS_DT AND sfc.OMNI_CATG_NBR = idc.OMNI_CATG_NBR
  LEFT JOIN sto_agg st      ON sfc.BUS_DT = st.BUS_DT AND sfc.OMNI_CATG_NBR = st.OMNI_CATG_NBR
  LEFT JOIN on_yard_agg oy  ON sfc.BUS_DT = oy.BUS_DT AND sfc.OMNI_CATG_NBR = oy.OMNI_CATG_NBR
  -- Append unmapped backroom units as OTHER rows — zero for all other channels
UNION ALL

SELECT
  bo.BUS_DT,
  bo.WM_YEAR,
  bo.WM_WEEK,
  bo.WM_YR_WK_NBR,
  bo.OMNI_CATG_NBR,
  bo.OMNI_CATG_DESC,
  bo.OMNI_DEPT_NBR,
  bo.OMNI_DEPT_DESC,
  bo.SBU,
  -- Cube columns (all other channels zero/null)
  0               AS STORE_TOTAL_CUBE,
  bo.BACKROOM_TOTAL_CUBE,
  NULL            AS INTRANSIT_DC_TOTAL_CUBE,
   0               AS ON_FLOOR_TOTAL_CUBE,
  0               AS DC_RESERVED_TOTAL_CUBE,
  0               AS TRANSIT_TOTAL_CUBE,
  0               AS FC_TOTAL_CUBE,
  NULL            AS DC_OH_TOTAL_CUBE,
  NULL            AS DC_LBL_TOTAL_CUBE,
  NULL            AS DC_UNLBL_TOTAL_CUBE,
  NULL            AS STO_TOTAL_CUBE,
  NULL            AS ON_YARD_TOTAL_CUBE,
  -- Units: only backroom is non-zero
  0               AS STORE_OH_UNITS,
  0               AS STORE_OH_COST,
  bo.BACKROOM_UNITS,
  0               AS ON_FLOOR_UNITS,
  0               AS DC_RESERVED_UNITS,    0 AS DC_RESERVED_COST,
  0               AS IN_TRANSIT_UNITS,     0 AS IN_TRANSIT_COST,
  0               AS DC_OH_UNITS,          0 AS DC_OH_COST,
  0               AS DC_LABELED_UNITS,     0 AS DC_LABELED_COST,
  0               AS DC_UNLABELED_UNITS,   0 AS DC_UNLABELED_COST,
  0 AS DC_OH_REGIONAL_UNITS, 0 AS DC_OH_GROCERY_UNITS,
  0 AS DC_OH_FASHION_UNITS,  0 AS DC_OH_IMPORTS_UNITS,
  0 AS DC_OH_GIDC_UNITS,     0 AS DC_OH_MSC_UNITS,
  0 AS DC_OH_SUPPORT_UNITS,  0 AS DC_OH_OTHER_UNITS,
  0 AS STO_IN_TRANSIT_TO_DC_UNITS, 0 AS STO_IN_TRANSIT_TO_DC_COST,
  0 AS INTRANSIT_TO_DC_UNITS,      0 AS INTRANSIT_TO_DC_COST,
  0 AS STO_TO_REGIONAL_UNITS, 0 AS STO_TO_GROCERY_UNITS,
  0 AS STO_TO_FASHION_UNITS,  0 AS STO_TO_IMPORTS_UNITS,
  0 AS STO_TO_GIDC_UNITS,     0 AS STO_TO_MSC_UNITS,
  0 AS STO_TO_SUPPORT_UNITS,  0 AS STO_TO_OTHER_UNITS,
  0               AS FC_OH_UNITS,          0 AS FC_OH_COST,
  0 AS FC_OH_SORT_UNITS,    0 AS FC_OH_SORT_COST,
  0 AS FC_OH_NONSORT_UNITS, 0 AS FC_OH_NONSORT_COST,
  0               AS ON_YARD_UNITS,        0 AS ON_YARD_COST,
  0 AS ON_YARD_REGIONAL_UNITS, 0 AS ON_YARD_GROCERY_UNITS,
  0 AS ON_YARD_FASHION_UNITS,  0 AS ON_YARD_IMPORTS_UNITS,
  0 AS ON_YARD_GIDC_UNITS,     0 AS ON_YARD_MSC_UNITS,
  0 AS ON_YARD_OTHER_UNITS,
  0               AS TOTAL_NETWORK_UNITS,
  0               AS TOTAL_NETWORK_COST,

FROM backroom_other_agg bo
);


-- ============================================================
-- STEP 8: DC-LEVEL HISTORICAL TABLE (with AVG_ITEM_CUBE)
-- ============================================================
DROP TABLE IF EXISTS `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LEVEL`;

CREATE OR REPLACE TABLE `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LEVEL`
PARTITION BY BUS_DT
CLUSTER BY DC_NBR, SBU
AS (

  WITH
  cal AS (
    SELECT DISTINCT
      CAL_DT AS BUS_DT,
      WM_FULL_YR_NBR AS WM_YEAR,
      WM_WK_NBR AS WM_WEEK,
      WM_YR_WK_NBR
    FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM`
    WHERE CAL_DT >= '2025-02-07'
      AND (
        EXTRACT(DAYOFWEEK FROM CAL_DT) = 6
        OR (CAL_DT = DATE_SUB(CURRENT_DATE('America/Chicago'), INTERVAL 1 DAY)
            AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE('America/Chicago')) BETWEEN 2 AND 6)
      )
      AND CAL_DT < CURRENT_DATE('America/Chicago')
  ),

  omni_hier AS (
    SELECT DISTINCT
      OMNI_CATG_NBR,
      OMNI_CATG_DESC,
      OMNI_DEPT_NBR,
      OMNI_DEPT_DESC,
      CASE
        WHEN OMNI_DEPT_NBR IN (2,4,8,13,40,46,79)          THEN 'CONSUMABLES'
        WHEN OMNI_DEPT_NBR IN (80,93,94,97,98)             THEN 'FRESH'
        WHEN OMNI_DEPT_NBR IN (90,91,1,82,96)              THEN 'CAC'
        WHEN OMNI_DEPT_NBR IN (81,92,95)                   THEN 'PANTRY'
        WHEN OMNI_DEPT_NBR IN (23,24,25,26,29,31,32,33,34) THEN 'FASHION'
        WHEN OMNI_DEPT_NBR IN (5,6,7,18,21,67,72,87)       THEN 'ETS'
        WHEN OMNI_DEPT_NBR IN (3,9,10,11,12,16,19,56)      THEN 'HARDLINES'
        WHEN OMNI_DEPT_NBR IN (14,17,20,22,71,74)          THEN 'HOME'
        ELSE 'UNASSIGNED'
      END AS SBU
    FROM `wmt-gdap-dl-sec-merch-bq-prod.us_mdse_omni_vm.mdse_omni_item_vw`
    WHERE OMNI_SEG_DESC NOT IN ('HEALTH AND WELLNESS', 'OTHER', 'WALMART SERVICES')
      AND OMNI_CATG_NBR IS NOT NULL
  ),

  dc_oh AS (
    SELECT
      BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR,
      SUM(DC_OH_UNITS)  AS DC_OH_UNITS,
      SUM(DC_OH_COST)   AS DC_OH_COST,
      SAFE_DIVIDE(SUM(DC_OH_UNITS * ITEM_CUBE), NULLIF(SUM(DC_OH_UNITS), 0)) AS DC_OH_WTAVG_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_OH`
    WHERE UPPER(DC_TYPE) NOT LIKE '%ECOMM%'
      AND UPPER(DC_TYPE) NOT LIKE '%FULFILLMENT%'
      AND UPPER(DC_TYPE) NOT LIKE '%E-COMMERCE%'
      AND UPPER(DC_TYPE) NOT LIKE 'FC%'
    GROUP BY BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR
  ),

  dc_lbl AS (
    SELECT
      BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_UNITS ELSE 0 END) AS DC_LABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_COST  ELSE 0 END) AS DC_LABELED_COST,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U'        THEN DC_LBL_UNLBL_UNITS ELSE 0 END) AS DC_UNLABELED_UNITS,
      SUM(CASE WHEN INVT_TYPE_CODE = 'U'        THEN DC_LBL_UNLBL_COST  ELSE 0 END) AS DC_UNLABELED_COST,
      SAFE_DIVIDE(
        SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_UNITS * ITEM_CUBE ELSE 0 END),
        NULLIF(SUM(CASE WHEN INVT_TYPE_CODE IN ('L','S') THEN DC_LBL_UNLBL_UNITS ELSE 0 END), 0)
      ) AS DC_LBL_WTAVG_CUBE,
      SAFE_DIVIDE(
        SUM(CASE WHEN INVT_TYPE_CODE = 'U' THEN DC_LBL_UNLBL_UNITS * ITEM_CUBE ELSE 0 END),
        NULLIF(SUM(CASE WHEN INVT_TYPE_CODE = 'U' THEN DC_LBL_UNLBL_UNITS ELSE 0 END), 0)
      ) AS DC_UNLBL_WTAVG_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_DC_LBL`
    WHERE UPPER(DC_TYPE) NOT LIKE '%ECOMM%'
      AND UPPER(DC_TYPE) NOT LIKE '%FULFILLMENT%'
      AND UPPER(DC_TYPE) NOT LIKE '%E-COMMERCE%'
      AND UPPER(DC_TYPE) NOT LIKE 'FC%'
    GROUP BY BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR
  ),

  sto_to_dc AS (
    SELECT
      BUS_DT,
      DEST_DC_NBR    AS DC_NBR,
      DEST_DC_TYPE   AS DC_TYPE,
      OMNI_CATG_NBR,
      SUM(STO_UNITS) AS STO_TO_DC_UNITS,
      SUM(STO_COST)  AS STO_TO_DC_COST,
      SAFE_DIVIDE(SUM(STO_UNITS * ITEM_CUBE), NULLIF(SUM(STO_UNITS), 0)) AS STO_WTAVG_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_STO`
    WHERE STO_FLOW_TYPE IN ('DC_TO_DC', 'STORE_TO_DC')
      AND UPPER(DEST_DC_TYPE) NOT LIKE '%ECOMM%'
      AND UPPER(DEST_DC_TYPE) NOT LIKE '%FULFILLMENT%'
      AND UPPER(DEST_DC_TYPE) NOT LIKE '%E-COMMERCE%'
      AND UPPER(DEST_DC_TYPE) NOT LIKE 'FC%'
    GROUP BY BUS_DT, DEST_DC_NBR, DEST_DC_TYPE, OMNI_CATG_NBR
  ),

  on_yard AS (
    SELECT
      BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR,
      SUM(ON_YARD_UNITS) AS ON_YARD_UNITS,
      SUM(ON_YARD_COST)  AS ON_YARD_COST,
      SAFE_DIVIDE(SUM(ON_YARD_UNITS * ITEM_CUBE), NULLIF(SUM(ON_YARD_UNITS), 0)) AS ON_YARD_WTAVG_CUBE
    FROM `wmt-execution-intel-prod.WM_AD_HOC.R0C0JUG_WMUS_HIST_ON_YARD`
    WHERE UPPER(DC_TYPE) NOT LIKE '%ECOMM%'
      AND UPPER(DC_TYPE) NOT LIKE '%FULFILLMENT%'
      AND UPPER(DC_TYPE) NOT LIKE '%E-COMMERCE%'
      AND UPPER(DC_TYPE) NOT LIKE 'FC%'
    GROUP BY BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR
  ),

  all_dc_keys AS (
    SELECT DISTINCT BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR FROM dc_oh
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR FROM dc_lbl
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR FROM sto_to_dc
    UNION DISTINCT
    SELECT DISTINCT BUS_DT, DC_NBR, DC_TYPE, OMNI_CATG_NBR FROM on_yard
  )

  SELECT
    k.BUS_DT,
    c.WM_YEAR,
    c.WM_WEEK,
    c.WM_YR_WK_NBR,
    k.DC_NBR,
    k.DC_TYPE,
    h.SBU,
    h.OMNI_DEPT_NBR,
    h.OMNI_DEPT_DESC  AS DEPARTMENT,
    k.OMNI_CATG_NBR,
    h.OMNI_CATG_DESC  AS CATEGORY,
    oh.DC_OH_WTAVG_CUBE,
    lbl.DC_LBL_WTAVG_CUBE,
    lbl.DC_UNLBL_WTAVG_CUBE,
    sto.STO_WTAVG_CUBE,
    oy.ON_YARD_WTAVG_CUBE,
    COALESCE(oh.DC_OH_UNITS, 0)          AS DC_OH_UNITS,
    COALESCE(oh.DC_OH_COST, 0)           AS DC_OH_COST,
    COALESCE(lbl.DC_LABELED_UNITS, 0)    AS DC_LABELED_UNITS,
    COALESCE(lbl.DC_LABELED_COST, 0)     AS DC_LABELED_COST,
    COALESCE(lbl.DC_UNLABELED_UNITS, 0)  AS DC_UNLABELED_UNITS,
    COALESCE(lbl.DC_UNLABELED_COST, 0)   AS DC_UNLABELED_COST,
    COALESCE(sto.STO_TO_DC_UNITS, 0)     AS STO_TO_DC_UNITS,
    COALESCE(sto.STO_TO_DC_COST, 0)      AS STO_TO_DC_COST,
    COALESCE(oy.ON_YARD_UNITS, 0)        AS ON_YARD_UNITS,
    COALESCE(oy.ON_YARD_COST, 0)         AS ON_YARD_COST,

    COALESCE(oh.DC_OH_UNITS, 0)
      + COALESCE(lbl.DC_LABELED_UNITS, 0)
      + COALESCE(lbl.DC_UNLABELED_UNITS, 0)
      + COALESCE(sto.STO_TO_DC_UNITS, 0)
      + COALESCE(oy.ON_YARD_UNITS, 0) AS TOTAL_DC_UNITS,

    COALESCE(oh.DC_OH_COST, 0)
      + COALESCE(lbl.DC_LABELED_COST, 0)
      + COALESCE(lbl.DC_UNLABELED_COST, 0)
      + COALESCE(sto.STO_TO_DC_COST, 0)
      + COALESCE(oy.ON_YARD_COST, 0) AS TOTAL_DC_COST

  FROM all_dc_keys k
  LEFT JOIN cal c       ON c.BUS_DT = k.BUS_DT
  LEFT JOIN omni_hier h ON h.OMNI_CATG_NBR = k.OMNI_CATG_NBR
  LEFT JOIN dc_oh oh
    ON oh.BUS_DT = k.BUS_DT AND oh.DC_NBR = k.DC_NBR AND oh.OMNI_CATG_NBR = k.OMNI_CATG_NBR
  LEFT JOIN dc_lbl lbl
    ON lbl.BUS_DT = k.BUS_DT AND lbl.DC_NBR = k.DC_NBR AND lbl.OMNI_CATG_NBR = k.OMNI_CATG_NBR
  LEFT JOIN sto_to_dc sto
    ON sto.BUS_DT = k.BUS_DT AND sto.DC_NBR = k.DC_NBR AND sto.OMNI_CATG_NBR = k.OMNI_CATG_NBR
  LEFT JOIN on_yard oy
    ON oy.BUS_DT = k.BUS_DT AND oy.DC_NBR = k.DC_NBR AND oy.OMNI_CATG_NBR = k.OMNI_CATG_NBR

  WHERE h.SBU IS NOT NULL
);