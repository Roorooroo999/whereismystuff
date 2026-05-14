-- ============================================================
-- FILE  : dc_invt_cte_stub.sql
-- DESC  : DC Inventory CTE stub for "Where's My Stuff" rollup
--         For DC team to complete with DC-type breakdown.
--         Drop this CTE into wheres_my_stuff_invt_rollup_vw.sql
--         replacing the existing dc_invt CTE stub.
-- SOURCE: wmt-gdap-dl-sec-merch-bq-prod.ww_repl_dl_secure.repl_sku_ei_invt_dly_vw
-- OWNER : DC Inventory team
-- DATE  : 2026-05-13
-- ============================================================
--
-- INSTRUCTIONS FOR DC TEAM:
-- 1. Join base CTE to your location dim to get dc_type_cd
-- 2. Replace <your_project.your_dataset.location_dim> below
--    with the actual location/store dim table path
-- 3. Confirm store_nbr >= 6000 is the correct DC range,
--    or replace with your own dc_type_cd IS NOT NULL filter
-- 4. Uncomment the dc_type_cd CASE columns as needed
-- 5. Drop into the main rollup view and uncomment the
--    per-type columns in the SELECT block
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- OPTION A: Join to location dimension (recommended)
-- ────────────────────────────────────────────────────────────
dc_invt AS (
  SELECT
    b.item_nbr,
    b.bus_dt,
    -- l.dc_type_cd,                              -- e.g. RDC, NDC, FDC, GDC
    SUM(b.in_whse_qty)                             AS dc_total_whse_qty,
    SUM(CASE WHEN l.dc_type_cd = 'RDC' THEN b.in_whse_qty ELSE 0 END)  AS rdc_whse_qty,
    SUM(CASE WHEN l.dc_type_cd = 'NDC' THEN b.in_whse_qty ELSE 0 END)  AS ndc_whse_qty,
    SUM(CASE WHEN l.dc_type_cd = 'FDC' THEN b.in_whse_qty ELSE 0 END)  AS fdc_whse_qty,
    SUM(CASE WHEN l.dc_type_cd = 'GDC' THEN b.in_whse_qty ELSE 0 END)  AS gdc_whse_qty,
    SUM(CASE WHEN l.dc_type_cd NOT IN ('RDC','NDC','FDC','GDC')
             THEN b.in_whse_qty ELSE 0 END)                              AS other_dc_whse_qty
  FROM base AS b
  INNER JOIN `<your_project.your_dataset.location_dim>` AS l   -- TODO: replace
    ON b.store_nbr = l.store_nbr
  WHERE l.location_type_cd = 'DC'                -- TODO: confirm flag name/value
  GROUP BY b.item_nbr, b.bus_dt
  -- , l.dc_type_cd                              -- uncomment if keeping dc_type grain
),

-- ────────────────────────────────────────────────────────────
-- OPTION B: No dimension join — range-based stub only
-- Use this if you don't have a location dim handy yet.
-- Replace the dc_invt CTE in the main view with this block.
-- ────────────────────────────────────────────────────────────
-- dc_invt AS (
--   SELECT
--     item_nbr,
--     bus_dt,
--     SUM(in_whse_qty)  AS dc_total_whse_qty
--   FROM base
--   WHERE store_nbr >= 6000   -- TODO: confirm DC store_nbr range
--   GROUP BY item_nbr, bus_dt
-- ),

-- ────────────────────────────────────────────────────────────
-- FINAL SELECT ADDITIONS (paste into rollup SELECT block)
-- ────────────────────────────────────────────────────────────
-- Replace:
--   COALESCE(d.dc_total_whse_qty, 0)  AS dc_total_whse_qty,
--
-- With:
--   COALESCE(d.dc_total_whse_qty, 0)  AS dc_total_whse_qty,
--   COALESCE(d.rdc_whse_qty,      0)  AS rdc_whse_qty,
--   COALESCE(d.ndc_whse_qty,      0)  AS ndc_whse_qty,
--   COALESCE(d.fdc_whse_qty,      0)  AS fdc_whse_qty,
--   COALESCE(d.gdc_whse_qty,      0)  AS gdc_whse_qty,
--   COALESCE(d.other_dc_whse_qty, 0)  AS other_dc_whse_qty,
