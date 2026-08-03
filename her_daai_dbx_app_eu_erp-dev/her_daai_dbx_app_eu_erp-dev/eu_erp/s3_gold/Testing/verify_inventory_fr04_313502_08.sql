-- Verification pack for Torsten UAT: article 313502-08 / plant FR04
-- Run in Databricks against the UAT/dev catalog used by the France inventory cube.
-- Replace :catalog and :snapshot_date as needed.

USE CATALOG IDENTIFIER(:catalog);

-- =============================================================================
-- 0) Confirm material typo: message said 313504-08-210, screenshots show 313502-08-210
-- =============================================================================
SELECT material_number, style_number, color_number, size_number
FROM s2_silver.dim_materials
WHERE material_number LIKE '313502-08%'
   OR material_number LIKE '313504-08%'
ORDER BY material_number;

-- =============================================================================
-- 1) Gold snapshot (PHYS unrestricted etc.) for FR04 / article family
-- =============================================================================
SELECT
    snapshot_date,
    company_code,
    plant,
    material,
    storage_location,
    stock_segment,
    special_stock_indicator,
    unrestricted_use_stock,
    restricted_use_stock,
    allocated_to_stock,
    allocated_to_delivery,
    allocated_on_hold,
    sto_not_shipped,
    sto_in_transit,
    atp_wb_plant_stock,
    atp_la_shipping,
    atp_be_order_items
FROM s3_gold.fact_inventory_snapshot
WHERE snapshot_date = CAST(:snapshot_date AS DATE)
  AND plant = 'FR04'
  AND material LIKE '313502-08%'
ORDER BY material, storage_location, stock_segment;

-- Article rollup from snapshot
SELECT
    snapshot_date,
    SUM(unrestricted_use_stock) AS unrestricted_units,
    SUM(restricted_use_stock)   AS restricted_units,
    SUM(allocated_to_stock + allocated_to_delivery + allocated_on_hold + allocated_to_po) AS allocated_units,
    SUM(sto_not_shipped + sto_in_transit) AS sto_units,
    SUM(atp_wb_plant_stock) AS atp_wb,
    SUM(atp_la_shipping) AS atp_la,
    SUM(atp_be_order_items) AS atp_be
FROM s3_gold.fact_inventory_snapshot
WHERE snapshot_date = CAST(:snapshot_date AS DATE)
  AND plant = 'FR04'
  AND material LIKE '313502-08%'
GROUP BY snapshot_date;

-- =============================================================================
-- 2) MATDOC rebuild from silver movements (must match unrestricted_use_stock)
--    Compare this to SAP: SUM(signed MENGE) for LBBSA_SID = '01', not SUM(STOCK_QTY)
-- =============================================================================
SELECT
    material,
    plant,
    storage_location,
    COALESCE(NULLIF(TRIM(stock_segment), ''), NULLIF(TRIM(issg_or_rcvg_stock_segment), '')) AS stock_segment,
    inventory_special_stock_type,
    inventory_stock_type,
    SUM(matl_stk_change_qty_in_base_unit) AS signed_qty
FROM s2_silver.fact_material_movements
WHERE plant = 'FR04'
  AND material LIKE '313502-08%'
  AND CAST(posting_date AS DATE) <= CAST(:snapshot_date AS DATE)
GROUP BY ALL
ORDER BY material, inventory_stock_type, stock_segment;

SELECT
    material,
    SUM(CASE WHEN inventory_stock_type = '01' THEN matl_stk_change_qty_in_base_unit ELSE 0 END) AS unrestricted_01,
    SUM(CASE WHEN inventory_stock_type = '08' THEN matl_stk_change_qty_in_base_unit ELSE 0 END) AS restricted_08,
    SUM(matl_stk_change_qty_in_base_unit) AS all_stock_types
FROM s2_silver.fact_material_movements
WHERE plant = 'FR04'
  AND material LIKE '313502-08%'
  AND CAST(posting_date AS DATE) <= CAST(:snapshot_date AS DATE)
GROUP BY material
ORDER BY material;

-- Size called out by Torsten (screenshot material)
SELECT
    SUM(CASE WHEN inventory_stock_type = '01' THEN matl_stk_change_qty_in_base_unit ELSE 0 END) AS unrestricted_01
FROM s2_silver.fact_material_movements
WHERE plant = 'FR04'
  AND material = '313502-08-210'
  AND CAST(posting_date AS DATE) <= CAST(:snapshot_date AS DATE);

-- =============================================================================
-- 3) Bridge output as consumed by Power BI — check Unrestricted pollution
--    QTYTYPE = UNRESTRICTEDSTOCK is also used by ATP_* source_flow rows.
-- =============================================================================
SELECT
    QTYTYPE,
    IntInventoryStatusCode,
    STOCKSEGMENT,
    SPECIALSTOCKINDICATOR,
    MATERIAL,
    SUM(QTY) AS qty
FROM s3_gold.fact_inventory_snapshot_bridge_eu
WHERE PLANT = 'FR04'
  AND MATERIAL LIKE '313502-08%'
  AND POSTINGDATE = CAST(:snapshot_date AS DATE)
GROUP BY ALL
ORDER BY MATERIAL, QTYTYPE, IntInventoryStatusCode;

-- What a "Stock type = Unrestricted" filter likely returns
SELECT
    MATERIAL,
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' THEN QTY ELSE 0 END) AS qty_by_qtytype_unrestricted,
    SUM(CASE WHEN IntInventoryStatusCode = 10 THEN QTY ELSE 0 END) AS qty_by_status_10,
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' AND IntInventoryStatusCode = 10 THEN QTY ELSE 0 END) AS qty_unrestricted_and_status_10
FROM s3_gold.fact_inventory_snapshot_bridge_eu
WHERE PLANT = 'FR04'
  AND MATERIAL LIKE '313502-08%'
  AND POSTINGDATE = CAST(:snapshot_date AS DATE)
GROUP BY MATERIAL
ORDER BY MATERIAL;

SELECT
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' THEN QTY ELSE 0 END) AS article_qtytype_unrestricted,
    SUM(CASE WHEN IntInventoryStatusCode = 10 THEN QTY ELSE 0 END) AS article_status_10,
    SUM(CASE WHEN QTYTYPE = 'UNRESTRICTEDSTOCK' AND IntInventoryStatusCode = 10 THEN QTY ELSE 0 END) AS article_both
FROM s3_gold.fact_inventory_snapshot_bridge_eu
WHERE PLANT = 'FR04'
  AND MATERIAL LIKE '313502-08%'
  AND POSTINGDATE = CAST(:snapshot_date AS DATE);

-- =============================================================================
-- 4) Allocated / restricted legs for Natalia's question
-- =============================================================================
SELECT *
FROM s3_gold.fact_inventory_allocated
WHERE plant = 'FR04'
  AND material LIKE '313502-08%';
