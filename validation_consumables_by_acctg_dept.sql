-- ============================================================
-- VALIDATION: Consumables using ACCTG_DEPT_NBR (Merch One Definition)
-- Purpose: Compare rollup DC numbers to Merch One using specific dept numbers
-- Merch One Consumables = ACCTG_DEPT_NBR IN (2, 4, 8, 13, 40, 46, 79)
--   Dept 2:  PERSONAL CARE
--   Dept 4:  HOUSEHOLD PAPER
--   Dept 8:  PETS AND SUPPLIES
--   Dept 13: HOUSEHOLD CLEANING
--   Dept 40: OTC PHARMACY
--   Dept 46: BEAUTY
--   Dept 79: BABY CONSUMABLES HARDLINES
-- ============================================================

-- ============================================================
-- QUERY 1: DC Inventory using ACCTG_DEPT_NBR filter
-- This matches Merch One's Consumables definition
-- ============================================================
SELECT
  'DC_BY_ACCTG_DEPT' AS SOURCE,
  SUM(DC_OH_UNITS) AS total_dc_units
FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP` r
JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
  ON r.MDS_FAM_ID = ic.MDS_FAM_ID
WHERE ic.ACCTG_DEPT_NBR IN (2, 4, 8, 13, 40, 46, 79)
;

-- ============================================================
-- QUERY 2: Compare OMNI_SEG_DESC='CONSUMABLES' vs ACCTG_DEPT_NBR list
-- Shows item overlap and unit differences
-- ============================================================
WITH
by_omni_seg AS (
  SELECT
    MDS_FAM_ID,
    SUM(STORE_OH_UNITS) AS store_units,
    SUM(IN_TRANSIT_UNITS) AS transit_units,
    SUM(DC_OH_UNITS) AS dc_units
  FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP`
  WHERE SBU = 'CONSUMABLES'
  GROUP BY MDS_FAM_ID
),
by_acctg_dept AS (
  SELECT DISTINCT
    r.MDS_FAM_ID,
    r.STORE_OH_UNITS,
    r.IN_TRANSIT_UNITS,
    r.DC_OH_UNITS
  FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP` r
  JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
    ON r.MDS_FAM_ID = ic.MDS_FAM_ID
  WHERE ic.ACCTG_DEPT_NBR IN (2, 4, 8, 13, 40, 46, 79)
)
SELECT
  'OMNI_SEG = CONSUMABLES' AS method,
  COUNT(DISTINCT MDS_FAM_ID) AS item_count,
  SUM(store_units) AS store_units,
  SUM(transit_units) AS transit_units,
  SUM(dc_units) AS dc_units,
  SUM(store_units) + SUM(transit_units) + SUM(dc_units) AS total_units
FROM by_omni_seg

UNION ALL

SELECT
  'ACCTG_DEPT_NBR (2,4,8,13,40,46,79)' AS method,
  COUNT(DISTINCT MDS_FAM_ID) AS item_count,
  SUM(STORE_OH_UNITS) AS store_units,
  SUM(IN_TRANSIT_UNITS) AS transit_units,
  SUM(DC_OH_UNITS) AS dc_units,
  SUM(STORE_OH_UNITS) + SUM(IN_TRANSIT_UNITS) + SUM(DC_OH_UNITS) AS total_units
FROM by_acctg_dept
;

-- ============================================================
-- QUERY 3: Breakdown by ACCTG_DEPT_NBR for Consumables
-- Shows which departments contribute what inventory
-- ============================================================
SELECT
  ic.ACCTG_DEPT_NBR,
  MAX(ic.DEPT_DESC) AS dept_desc,
  COUNT(DISTINCT r.MDS_FAM_ID) AS item_count,
  SUM(r.STORE_OH_UNITS) AS store_units,
  SUM(r.IN_TRANSIT_UNITS) AS transit_units,
  SUM(r.DC_OH_UNITS) AS dc_units,
  SUM(r.STORE_OH_UNITS) + SUM(r.IN_TRANSIT_UNITS) + SUM(r.DC_OH_UNITS) AS total_units
FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP` r
JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
  ON r.MDS_FAM_ID = ic.MDS_FAM_ID
WHERE ic.ACCTG_DEPT_NBR IN (2, 4, 8, 13, 40, 46, 79)
GROUP BY ic.ACCTG_DEPT_NBR
ORDER BY dc_units DESC
;

-- ============================================================
-- QUERY 4: Find items in ACCTG_DEPT but NOT in OMNI_SEG=CONSUMABLES
-- These are items that Merch One counts but our OMNI_SEG filter misses
-- ============================================================
SELECT
  ic.ACCTG_DEPT_NBR,
  ic.DEPT_DESC,
  r.SBU AS omni_seg_desc,
  COUNT(DISTINCT r.MDS_FAM_ID) AS item_count,
  SUM(r.DC_OH_UNITS) AS dc_units
FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP` r
JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
  ON r.MDS_FAM_ID = ic.MDS_FAM_ID
WHERE ic.ACCTG_DEPT_NBR IN (2, 4, 8, 13, 40, 46, 79)
  AND (r.SBU <> 'CONSUMABLES' OR r.SBU IS NULL)
GROUP BY ic.ACCTG_DEPT_NBR, ic.DEPT_DESC, r.SBU
ORDER BY dc_units DESC
;

-- ============================================================
-- QUERY 5: Find items in OMNI_SEG=CONSUMABLES but NOT in ACCTG_DEPT list
-- These are items our rollup counts but Merch One excludes
-- ============================================================
SELECT
  ic.ACCTG_DEPT_NBR,
  ic.DEPT_DESC,
  COUNT(DISTINCT r.MDS_FAM_ID) AS item_count,
  SUM(r.DC_OH_UNITS) AS dc_units
FROM `wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP` r
JOIN `wmt-edw-prod.US_WM_VM.ITEM_CUR` ic
  ON r.MDS_FAM_ID = ic.MDS_FAM_ID
WHERE r.SBU = 'CONSUMABLES'
  AND ic.ACCTG_DEPT_NBR NOT IN (2, 4, 8, 13, 40, 46, 79)
GROUP BY ic.ACCTG_DEPT_NBR, ic.DEPT_DESC
ORDER BY dc_units DESC
;
