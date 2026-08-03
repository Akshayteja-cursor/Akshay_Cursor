use catalog identifier(:catalog);


CREATE OR REPLACE VIEW s3_gold.fact_inventory_snapshot_bridge_eu_by_postingdate AS

WITH unpivoted AS (
    SELECT
        src.snapshot_date,
        src.posting_date,
        src.data_source,
        src.company_code,
        src.plant,
        src.material,
        src.storage_location,
        src.stock_segment,
        src.special_stock_indicator,
        src.batch_id,
        src.material_base_unit,
        src.cost_currency,
        src.cost_valid_from_date,
        src.cost_valid_to_date,
        src.gross,
        src.load_timestamp,

        stacked.qtytype,
        stacked.source_flow,
        stacked.kpi_value,
        stacked.amt_value,
        stacked.running_total_value
    FROM s3_gold.fact_inventory_snapshot_by_postingdate src
    LATERAL VIEW STACK(
        19,
        'UNRESTRICTEDSTOCK',                'PHYS',   src.unrestricted_use_stock,        src.amt_unrestricted_use_stock,        src.running_total_unrestricted_use_stock,
        'STOCKINQUALITYINSPECTION',         'PHYS',   src.stock_in_quality_inspection,   src.amt_stock_in_quality_inspection,   src.running_total_stock_in_quality_inspection,
        'BLOCKEDSTOCKRETURNS',              'PHYS',   src.returns,                       src.amt_returns,                       src.running_total_returns,
        'STOCKINTRANSFERSTLOC',             'PHYS',   src.stock_transfer_storage_loc,    src.amt_stock_transfer_storage_loc,    src.running_total_stock_transfer_storage_loc,
        'STOCKINTRANSFERPLANT',             'PHYS',   src.stock_transfer_plant,          src.amt_stock_transfer_plant,          src.running_total_stock_transfer_plant,
        'STOCKINTRANSIT',                   'PHYS',   src.stock_in_transit,              src.amt_stock_in_transit,              src.running_total_stock_in_transit,
        'BLOCKEDSTOCK',                     'PHYS',   src.blocked_stock,                 src.amt_blocked_stock,                 src.running_total_blocked_stock,
        'TOTALSTOCKRESTRICTEDBATCHES',      'PHYS',   src.restricted_use_stock,          src.amt_restricted_use_stock,          src.running_total_restricted_use_stock,
        'TIEDEMPTIES',                      'PHYS',   src.tied_empties,                  src.amt_tied_empties,                  src.running_total_tied_empties,
        'VALUATEDGOODSRECEIPTBLOCKEDSTOCK', 'PHYS',   src.valuated_gr_blocked_stock,     src.amt_valuated_gr_blocked_stock,     src.running_total_valuated_gr_blocked_stock,
        -- Keep source_flow aligned with fact_inventory_snapshot_bridge_eu:
        -- ALLOCATED* => ALLOC, STO* => STO (not PHYS). PHYS is only physical MATDOC stock types.
        'ALLOCATEDTOPO',                    'ALLOC',  src.allocated_to_po,               src.amt_allocated_to_po,               CAST(NULL AS DECIMAL(38,6)),
        'ALLOCATEDTOSTOCK',                 'ALLOC',  src.allocated_to_stock,            src.amt_allocated_to_stock,            CAST(NULL AS DECIMAL(38,6)),
        'ALLOCATEDTODELIVERY',              'ALLOC',  src.allocated_to_delivery,         src.amt_allocated_to_delivery,         CAST(NULL AS DECIMAL(38,6)),
        'ALLOCATEDONHOLD',                  'ALLOC',  src.allocated_on_hold,             src.amt_allocated_on_hold,             CAST(NULL AS DECIMAL(38,6)),
        'STONOTSHIPPED',                    'STO',    src.sto_not_shipped,               src.amt_sto_not_shipped,               CAST(NULL AS DECIMAL(38,6)),
        'STOINTRANSIT',                     'STO',    src.sto_in_transit,                src.amt_sto_in_transit,                CAST(NULL AS DECIMAL(38,6)),
        'UNRESTRICTEDSTOCK',                'ATP_WB', src.atp_wb_plant_stock,            src.amt_atp_wb_plant_stock,            CAST(NULL AS DECIMAL(38,6)),
        'UNRESTRICTEDSTOCK',                'ATP_LA', src.atp_la_shipping,               src.amt_atp_la_shipping,               CAST(NULL AS DECIMAL(38,6)),
        'UNRESTRICTEDSTOCK',                'ATP_BE', src.atp_be_order_items,            src.amt_atp_be_order_items,            CAST(NULL AS DECIMAL(38,6))
    ) stacked AS qtytype, source_flow, kpi_value, amt_value, running_total_value
   WHERE kpi_value <> 0

      AND (source_flow NOT IN ('ATP_LA', 'ATP_WB', 'ATP_BE') OR src.stock_segment = 'WHS')
      and src.company_code in ('FR10','ES10','IT10','DE19')
),

-- FAS (ATP_WB) only: explode into debit/credit legs (ratio -1/+1) before
-- joins, so the row-doubling stays scoped to this small subset instead of
-- inflating the full unpivoted set.
exploded AS (
    SELECT u.*, ratio_sign
    FROM unpivoted u
    LATERAL VIEW EXPLODE(ARRAY(-1, 1)) ratio_tbl AS ratio_sign
    WHERE u.source_flow = 'ATP_WB'

    UNION ALL

    SELECT u.*, CAST(1 AS INT) AS ratio_sign
    FROM unpivoted u
    WHERE u.source_flow <> 'ATP_WB'
)

SELECT
    CASE
        WHEN pl.plant_category_code = 'A' OR COALESCE(u.stock_segment,'') = ''
            THEN CONCAT(u.plant, '|',
                        CASE WHEN u.company_code = 'PT10' THEN 'ES10' ELSE u.company_code END)
        ELSE CONCAT(u.plant, '_', u.stock_segment, '|',
                    CASE WHEN u.company_code = 'PT10' THEN 'ES10' ELSE u.company_code END)
    END                                                     AS NatStoreDC_BK,           

    u.company_code                                          AS COMPANYCODE,
    u.plant                                                 AS PLANT,
    u.material                                              AS MATERIAL,
    u.storage_location                                      AS STORAGELOCATION,
    u.stock_segment                                         AS STOCKSEGMENT,
    u.batch_id                                              AS BATCHID,
    u.qtytype                                               AS QTYTYPE,
    u.material_base_unit                                    AS MATERIALBASEUNIT,
    CASE WHEN u.source_flow = 'ATP_WB' THEN u.kpi_value * ratio_sign ELSE u.kpi_value END AS QTY,
    u.running_total_value                                   AS RunningTotal,

    COALESCE( nullif( MP.moving_average_price,0),MP.standard_price, 0.00) AS PRICE,

    CASE WHEN u.source_flow = 'ATP_WB' THEN u.amt_value * ratio_sign ELSE u.amt_value END AS VALUE,
    u.cost_currency                                         AS PRICECURRENCY,
    u.cost_valid_from_date                                  AS VALIDFROMDAT,
    u.cost_valid_to_date                                    AS VALIDTODAT,
    u.posting_date                                          AS POSTINGDATE,
    u.snapshot_date                                         AS SNAPSHOTDATE,

    CAST(CASE WHEN pl.distribution_channel_code = '20' THEN 2 ELSE 1 END AS INT) AS BusinessTypeCode,

    CAST(NULL AS INT)                                       AS INSERTTYPE,
    u.load_timestamp                                        AS ModifiedOn,

    CONCAT(u.material, '|',
           CASE WHEN u.company_code = 'PT10' THEN 'ES10' ELSE u.company_code END) AS NatArticle_BK,

    intf.pdu_code                                           AS PDUCode,
    intf.int_put_pup_master_id                              AS IntPupMasterId,

    CONCAT(TRIM(mat.style_number), ' ', TRIM(mat.color_number)) AS ArticleNumber_BK,
    CONCAT(TRIM(mat.style_number), ' ', TRIM(mat.color_number), ' ',
           TRIM(COALESCE(mat.size_number, '0')))            AS ArticleSize_BK,
    mat.brand_code                                          AS BrandCode,
    COALESCE(TRY_CAST(mat.reporting_business_unit_code AS INT), 0) AS RBUCode,
    CAST(CASE WHEN mat.retail_dcsdep_code IN ('31','32','33','34')
                   AND mat.product_division_code = '2' THEN '3'
              ELSE mat.product_division_code END AS INT)    AS ProdDivCode,
    COALESCE(TRY_CAST(mat.style_number AS INT), 0)          AS StyleNumber_BK,

    CAST(50 AS INT)                                         AS IntAgeingCode,

    
    CAST(
        CASE
            WHEN u.source_flow = 'ATP_WB' AND ratio_sign = -1 THEN 10
            WHEN u.source_flow = 'ATP_WB' AND ratio_sign =  1 THEN 20
            WHEN u.source_flow = 'ATP_BE'        THEN -20
            WHEN u.source_flow = 'ATP_LA'        THEN -10
            WHEN u.source_flow = 'STO'           THEN 50
            WHEN u.special_stock_indicator = 'W' THEN 30
            WHEN u.stock_segment = 'CON'         THEN 30
            WHEN pl.plant_category_code = 'A'         THEN 30
            ELSE 10
        END AS INT)                                         AS IntInventoryStatusCode, 
    CAST(CASE WHEN pl.plant_category_code = 'B' THEN 30 ELSE 40 END AS INT) AS IntInventoryTypeCode,

    u.special_stock_indicator                              AS SPECIALSTOCKINDICATOR,
    u.gross                                                 AS GROSS,
    u.load_timestamp                                       AS _LDTS,

    
    CAST(LEFT(mat.first_pid_season, 5) AS INT)             AS FirstAssortmentSemester,
    CAST(LEFT(mat.last_pid_season, 5) AS INT)              AS LatestAssortmentSemester

FROM exploded u
LEFT JOIN s2_silver.dim_plants pl
    ON pl.store_code = u.plant
LEFT JOIN s2_silver.s4hana_interfaces intf
    ON  intf.sales_org = pl.sales_organization_code
    AND intf.interface = 'EBP'
LEFT JOIN s2_silver.dim_materials mat
    ON mat.material_number = u.material

LEFT JOIN s3_gold.dim_plant_material_cost MP
        ON u.material = MP.material
        AND u.plant = MP.plant
        AND CAST(u.posting_date AS DATE)
            BETWEEN CAST(MP.valid_from_date AS DATE)
                AND CAST(MP.valid_to_date AS DATE)

-- Guard against cost-validity overlap fan-out (would duplicate QTY in the cube).
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        u.snapshot_date,
        u.posting_date,
        u.company_code,
        u.plant,
        u.material,
        u.storage_location,
        u.stock_segment,
        u.batch_id,
        u.special_stock_indicator,
        u.qtytype,
        u.source_flow,
        ratio_sign
    ORDER BY MP.valid_from_date DESC NULLS LAST,
             MP.load_timestamp DESC NULLS LAST
) = 1
;