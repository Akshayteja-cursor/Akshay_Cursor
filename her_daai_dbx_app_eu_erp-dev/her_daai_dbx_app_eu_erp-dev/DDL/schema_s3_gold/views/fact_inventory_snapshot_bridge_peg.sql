use catalog identifier(:catalog);

CREATE OR REPLACE VIEW s3_gold.fact_inventory_snapshot_bridge_peg AS

WITH 

unpivoted AS (
    SELECT
        src.snapshot_date,
        src.company_code,
        src.plant,
        src.material,
        src.storage_location,
        src.stock_segment,
        src.special_stock_indicator,
        src.batch_id,
        src.material_base_unit,
        src.cost_currency,
        src.load_timestamp,

        stacked.qtytype,
        stacked.source_flow,
        stacked.kpi_value,
        stacked.amt_value
    FROM s3_gold.fact_inventory_snapshot src
    LATERAL VIEW STACK(
        16,
        'UNRESTRICTEDSTOCK',                'PHYS',   src.unrestricted_use_stock,        src.amt_unrestricted_use_stock,
        'STOCKINQUALITYINSPECTION',         'PHYS',   src.stock_in_quality_inspection,   src.amt_stock_in_quality_inspection,
        'BLOCKEDSTOCKRETURNS',              'PHYS',   src.returns,                       src.amt_returns,
        'STOCKINTRANSFERSTLOC',             'PHYS',   src.stock_transfer_storage_loc,    src.amt_stock_transfer_storage_loc,
        'STOCKINTRANSFERPLANT',             'PHYS',   src.stock_transfer_plant,          src.amt_stock_transfer_plant,
        'STOCKINTRANSIT',                   'PHYS',   src.stock_in_transit,              src.amt_stock_in_transit,
        'BLOCKEDSTOCK',                     'PHYS',   src.blocked_stock,                 src.amt_blocked_stock,
        'TOTALSTOCKRESTRICTEDBATCHES',      'PHYS',   src.restricted_use_stock,          src.amt_restricted_use_stock,
        'TIEDEMPTIES',                      'PHYS',   src.tied_empties,                  src.amt_tied_empties,
        'VALUATEDGOODSRECEIPTBLOCKEDSTOCK', 'PHYS',   src.valuated_gr_blocked_stock,     src.amt_valuated_gr_blocked_stock,
        -- Align with EU bridge inventory-type logic:
        -- ALLOCATED* => ALLOC, STO* => STO (not PHYS). PHYS is only physical MATDOC stock types.
        'ALLOCATEDTOPO',                    'ALLOC',  src.allocated_to_po,               src.amt_allocated_to_po,
        'ALLOCATEDTOSTOCK',                 'ALLOC',  src.allocated_to_stock,            src.amt_allocated_to_stock,
        'ALLOCATEDTODELIVERY',              'ALLOC',  src.allocated_to_delivery,         src.amt_allocated_to_delivery,
        'ALLOCATEDONHOLD',                  'ALLOC',  src.allocated_on_hold,             src.amt_allocated_on_hold,
        'STONOTSHIPPED',                    'STO',    src.sto_not_shipped,               src.amt_sto_not_shipped,
        'STOINTRANSIT',                     'STO',    src.sto_in_transit,                src.amt_sto_in_transit
      
    ) stacked AS qtytype, source_flow, kpi_value, amt_value
    WHERE kpi_value <> 0
      
      AND src.company_code = 'DE11'
),

base AS (
    SELECT
        u.snapshot_date                                         AS POSTINGDATE,
        u.storage_location                                      AS STORAGELOCATION,
        u.stock_segment                                         AS STOCKSEGMENT,
        u.cost_currency                                         AS PRICECURRENCY,
        u.qtytype                                               AS QTYTYPE,
        u.kpi_value                                             AS QTY,
        u.amt_value                                             AS AMT_VALUE,

        CONCAT(u.material, '|',
               CASE WHEN u.company_code = 'PT10' THEN 'ES10' ELSE u.company_code END) AS NatArticle_BK,
        intf.pdu_code                                           AS PDUCode,
        intf.int_put_pup_master_id                              AS IntPupMasterId,
        mat.brand_code                                          AS BrandCode,
        COALESCE(TRY_CAST(mat.reporting_business_unit_code AS INT), 0) AS RBUCode,
        CAST(CASE WHEN mat.retail_dcsdep_code IN ('31','32','33','34')
                       AND mat.product_division_code = '2' THEN '3'
                  ELSE mat.product_division_code END AS INT)    AS ProdDivCode,

        CAST(
            CASE
                WHEN u.source_flow = 'ATP_BE'        THEN -20
                WHEN u.source_flow = 'ATP_LA'        THEN -10
                WHEN u.source_flow = 'STO'           THEN 50
                WHEN u.special_stock_indicator = 'W' THEN 30
                WHEN u.stock_segment = 'CON'         THEN 30
                WHEN pl.plant_category_code = 'A'    THEN 30
                ELSE 10
            END AS INT)                                         AS IntInventoryStatusCode,
        CAST(CASE WHEN pl.distribution_channel_code = '20' THEN 2 ELSE 1 END AS INT) AS BusinessTypeCode,

        CASE WHEN TRY_CAST(REPLACE(CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number)),' ','') AS DOUBLE) IS NOT NULL
             THEN CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number)) ELSE '000000 00' END AS ArticleNumber_BK,
        CASE WHEN TRY_CAST(REPLACE(CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number),' ',TRIM(COALESCE(mat.size_number,'0'))),' ','') AS DOUBLE) IS NOT NULL
             THEN CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number),' ',TRIM(COALESCE(mat.size_number,'0'))) ELSE '000000 00 0' END AS ArticleSize_BK,
        CASE WHEN TRY_CAST(mat.style_number AS DOUBLE) IS NOT NULL
             THEN RIGHT(CONCAT('000000', CAST(CAST(TRY_CAST(mat.style_number AS INT) AS STRING) AS STRING)), 6)
             ELSE '000000' END                                  AS StyleNumber_BK,


        CASE WHEN nsd.int_dc_type_code = 60 THEN nsd.gsm_code
             ELSE CONCAT(
                CASE WHEN pl.plant_category_code = 'A' OR COALESCE(u.stock_segment,'') = ''
                     THEN u.plant ELSE CONCAT(u.plant,'_',u.stock_segment) END, '|EU') END AS NatStoreDC_BK,

        CONCAT(
            CASE WHEN pl.plant_category_code = 'A' OR COALESCE(u.stock_segment,'') = ''
                 THEN u.plant ELSE CONCAT(u.plant,'_',u.stock_segment) END, '|EU',
            '|', u.storage_location, '|', u.stock_segment)      AS NatStoreDC_Location_Segment_BK,

        nsd.country_code,
        COALESCE(vat.vat, 0)                                    AS VAT,
        rp.price                                                  AS RRP,

        mat.business_segment_code,
        mat.age_grp_code,
        mat.gender_code
    FROM unpivoted u
    LEFT JOIN s2_silver.dim_plants pl
        ON pl.store_code = u.plant
    LEFT JOIN s2_silver.s4hana_interfaces intf
        ON  intf.sales_org = pl.sales_organization_code
        AND intf.interface = 'EBP'
    LEFT JOIN s2_silver.dim_materials mat
        ON mat.material_number = u.material
    INNER JOIN s2_silver.peg_natstoredcs nsd
        ON nsd.natstoredc_bk = CONCAT(
                CASE WHEN pl.plant_category_code = 'A' OR COALESCE(u.stock_segment,'') = ''
                     THEN u.plant ELSE CONCAT(u.plant,'_',u.stock_segment) END, '|EU')
    LEFT JOIN s2_silver.vat_rates vat
        ON  vat.country_code = nsd.country_code
    LEFT JOIN  s3_gold.dim_retail_price_901 rp
        ON  rp.article_number_bk = CASE
                WHEN TRY_CAST(REPLACE(CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number)),' ','') AS DOUBLE) IS NOT NULL
                THEN CONCAT(TRIM(mat.style_number),' ',TRIM(mat.color_number)) ELSE '000000 00' END
        AND u.snapshot_date BETWEEN rp.validity_start_date AND rp.validity_end_date
        and COALESCE(rp.release_status, '')    = ''
        AND COALESCE(rp.processing_status, '') = ''
        AND rp.sales_organisation = '6000'
        AND rp.is_generic_material  =true 
)

SELECT
    POSTINGDATE                              AS BookingDate,
    ArticleNumber_BK,
    ArticleSize_BK,
    NatArticle_BK,
    StyleNumber_BK,
    BrandCode,
    CAST(ProdDivCode AS INT)                 AS ProductDivisionCode,
    CAST(RBUCode AS INT)                     AS RBUCode,

    CAST(COALESCE(TRY_CAST(business_segment_code AS INT), 0) AS INT) AS BusinessSegmentCode,
    CAST(COALESCE(TRY_CAST(age_grp_code AS INT), 0) AS INT)          AS ReportingAgeGroupCode,
    CAST(COALESCE(TRY_CAST(gender_code AS INT), 0) AS INT)           AS ReportingGenderCode,
    CAST(0 AS INT)                           AS ProductCreationModelCode,

    CAST(10 AS INT)                          AS BusinessClassCode,
    CAST(BusinessTypeCode AS INT)            AS BusinessTypeCode,
    CAST(IntInventoryStatusCode AS INT)      AS InventoryStatusCode,
    IntPupMasterId                           AS PupMasterId,
    PDUCode,
    PRICECURRENCY                            AS CurrencyCode,
    NatStoreDC_BK,
    STORAGELOCATION                          AS StorageLocation,
    NatStoreDC_Location_Segment_BK,
    QTYTYPE                                  AS InventoryStockType,

    SUM(QTY)                                 AS InventoryUnits,
    CAST(SUM(AMT_VALUE) AS DECIMAL(38,2))    AS InventoryGrossValue,
    CAST(0 AS DECIMAL(38,2))                 AS InventoryNetValue,

    RRP                                                          AS RRP_incl_VAT,
    RRP * (100.0 / (100.0 + MAX(VAT)))                          AS RRP_excl_VAT,
    RRP * SUM(QTY)                                              AS GrossValue,
    RRP * (100.0 / (100.0 + MAX(VAT))) * SUM(QTY)              AS NetRRP

FROM base
GROUP BY all
HAVING SUM(QTY) <> 0
;