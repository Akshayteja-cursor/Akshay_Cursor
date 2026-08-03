-- Natalia / Rajani example: DBX vs Synapse near-duplication on UNRESTRICTEDSTOCK
-- Material: 026404-01-150, plant FR04, model PDM - SUBS FRANCE - TEST INVENTORY
USE CATALOG IDENTIFIER(:catalog);

-- =============================================================================
-- 1) Assortment view fan-out check (France cube joins ONLY Material + CompanyCode)
--    Any row_count > 1 will duplicate InventoryUnits in DBX partition.
-- =============================================================================
SELECT
    MATERIAL,
    CompanyCode,
    COUNT(*) AS assortment_rows,
    COLLECT_SET(PDUCode) AS pdu_codes,
    COLLECT_SET(IntPupMasterId) AS pup_ids
FROM s3_gold.vw_inventory_material_assortment_semester
WHERE CompanyCode = 'FR10'
  AND MATERIAL IN ('026404-01-150')
GROUP BY MATERIAL, CompanyCode;

-- Broad scan: materials that would fan-out in France cube
SELECT
    COUNT(*) AS material_company_keys,
    SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS keys_with_fanout,
    MAX(cnt) AS max_rows_per_key
FROM (
    SELECT MATERIAL, CompanyCode, COUNT(*) AS cnt
    FROM s3_gold.vw_inventory_material_assortment_semester
    WHERE CompanyCode = 'FR10'
    GROUP BY MATERIAL, CompanyCode
) s;

-- =============================================================================
-- 2) Cost overlap fan-out at FR04 (bridge joins cost by plant/material/date)
-- =============================================================================
SELECT
    plant,
    material,
    COUNT(*) AS overlapping_cost_rows,
    COLLECT_LIST(named_struct(
        'valid_from', valid_from_date,
        'valid_to', valid_to_date,
        'data_source', data_source,
        'map', moving_average_price,
        'std', standard_price
    )) AS cost_rows
FROM s3_gold.dim_plant_material_cost
WHERE plant = 'FR04'
  AND material = '026404-01-150'
  AND DATE('2026-08-03') BETWEEN valid_from_date AND valid_to_date
GROUP BY plant, material;

-- =============================================================================
-- 3) Gold snapshot (no bridge joins) vs bridge QTY for Natalia example
-- =============================================================================
SELECT
    'snapshot' AS layer,
    plant,
    material,
    stock_segment,
    storage_location,
    special_stock_indicator,
    unrestricted_use_stock AS qty
FROM s3_gold.fact_inventory_snapshot
WHERE snapshot_date = DATE('2026-08-03')
  AND plant = 'FR04'
  AND material = '026404-01-150';

SELECT
    'bridge' AS layer,
    PLANT,
    MATERIAL,
    STOCKSEGMENT,
    STORAGELOCATION,
    SPECIALSTOCKINDICATOR,
    QTYTYPE,
    IntInventoryStatusCode,
    SUM(QTY) AS qty,
    COUNT(*) AS row_count
FROM s3_gold.fact_inventory_snapshot_bridge_eu
WHERE POSTINGDATE = DATE('2026-08-03')
  AND PLANT = 'FR04'
  AND MATERIAL = '026404-01-150'
  AND QTYTYPE = 'UNRESTRICTEDSTOCK'
GROUP BY ALL
ORDER BY IntInventoryStatusCode, STOCKSEGMENT, STORAGELOCATION;

SELECT
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' THEN QTY ELSE 0 END) AS bridge_unrestricted_qty,
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' AND IntInventoryStatusCode = 10 THEN QTY ELSE 0 END) AS bridge_unrestricted_status10
FROM s3_gold.fact_inventory_snapshot_bridge_eu
WHERE POSTINGDATE = DATE('2026-08-03')
  AND PLANT = 'FR04'
  AND MATERIAL = '026404-01-150';

-- =============================================================================
-- 4) Simulate France cube DBX partition join (the exact fan-out risk)
-- =============================================================================
SELECT
    EU.PLANT,
    EU.MATERIAL,
    EU.QTYTYPE,
    EU.IntInventoryStatusCode,
    COUNT(*) AS rows_after_assortment_join,
    SUM(EU.QTY) AS qty_after_join,
    COUNT(DISTINCT SS.PDUCode) AS distinct_pdu_from_ss
FROM s3_gold.fact_inventory_snapshot_bridge_eu EU
LEFT JOIN s3_gold.vw_inventory_material_assortment_semester SS
    ON EU.MATERIAL = SS.MATERIAL
   AND EU.COMPANYCODE = SS.CompanyCode
WHERE EU.POSTINGDATE = DATE('2026-08-03')
  AND EU.COMPANYCODE = 'FR10'
  AND EU.PLANT = 'FR04'
  AND EU.MATERIAL = '026404-01-150'
  AND EU.QTYTYPE = 'UNRESTRICTEDSTOCK'
GROUP BY ALL;
