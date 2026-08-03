-- View: vw_inventory_material_assortment_semester
-- Purpose: FirstAssortment / LatestAssortment per material per company
-- Usage:   France cube joins this to fact_inventory_snapshot_bridge_eu ON Material + CompanyCode
-- IMPORTANT: Grain MUST be exactly one row per (Material, CompanyCode).
--            Multiple s4hana_interfaces rows per company previously produced fan-out
--            and duplicated InventoryUnits in PDM - SUBS FRANCE - TEST INVENTORY.
use catalog identifier(:catalog);

CREATE OR REPLACE VIEW s3_gold.vw_inventory_material_assortment_semester AS
SELECT
      X.MATERIAL
    , X.CompanyCode
    , MAX(X.IntPupMasterId)     AS IntPupMasterId
    , MAX(X.PDUCode)            AS PDUCode
    , MAX(X.FirstAssortment)    AS FirstAssortment
    , MAX(X.LatestAssortment)   AS LatestAssortment
FROM (
    SELECT
          A.Product                         AS MATERIAL
        , A.ProductSalesOrg                 AS SalesOrgCode
        , A.ProductDistributionChnl         AS DistChannelCode
        , A.FirstAssortment
        , A.LatestAssortment
        , B.company_code                    AS CompanyCode
        , C.int_put_pup_master_id           AS IntPupMasterId
        , C.pdu_code                        AS PDUCode
    FROM s2_silver.materialsalesattributes A
    INNER JOIN s2_silver.dim_sales_org_to_company_code B
            ON A.ProductSalesOrg = B.Code
    INNER JOIN s2_silver.s4hana_interfaces C
            ON B.company_code = C.company_code
           AND C.interface = 'EBP'
    QUALIFY ROW_NUMBER() OVER (
                PARTITION BY A.Product, A.ProductSalesOrg
                ORDER BY CASE WHEN A.ProductDistributionChnl = '40' THEN '00'
                              ELSE A.ProductDistributionChnl END
           ) = 1
) X
GROUP BY
      X.MATERIAL
    , X.CompanyCode
;
