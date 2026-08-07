-- Databricks SQL
--
-- Prerequisites:
--   1. Load the Excel tabs ('Amperwave Pacing Report', 'Spotify Pacing Report')
--      into temp views named:
--        report_amperwave_sheet
--        report_megaphone_sheet
--   2. Set report_ts below to the report date used to generate the workbook.
--
-- A zero-row result from podcast_report_mismatches means the two report tabs
-- match the table-derived expected output at the report-generation grain.
--
-- UAT 07/28/2026 expectations baked into the expected output:
--   * Both OSI columns (R & S) carry the Ad Ops podcast formula:
--     (delivered-to-date / contracted) / (days live / total flight days).
--   * The Megaphone tab is labeled 'Spotify'.
--   * Goal/Contracted Quantity come straight from the deal-line grain of the
--     gold tables (AOS already rolls extension order lines into the deal
--     line, e.g. deal 569046 carries quantity 2,000,000 / production
--     2,100,000 with the extended flight 06/01 - 08/14).

CREATE OR REPLACE TEMP VIEW podcast_report_mismatches AS
WITH params AS (
    SELECT TIMESTAMP '2026-07-23 00:00:00' AS report_ts
),
source_rows AS (
    SELECT
        'Amperwave' AS instance,
        deal_line_item_id,
        advertiser_name,
        deal_name,
        deal_line_item_name,
        deal_line_item_start_date,
        deal_line_item_end_date,
        net_unit_cost_amt,
        production_quantity,
        quantity,
        account_executive,
        delivered_impressions,
        event_date
    FROM fox_bi_qa.gold_ad_sales.ft_amperwave_campaign_delivery

    UNION ALL

    SELECT
        'Spotify' AS instance,
        deal_line_item_id,
        advertiser_name,
        deal_name,
        deal_line_item_name,
        deal_line_item_start_date,
        deal_line_item_end_date,
        net_unit_cost_amt,
        production_quantity,
        quantity,
        account_executive,
        delivered_impressions,
        event_date
    FROM fox_bi_qa.gold_ad_sales.ft_megaphone_campaign_delivery
),
source_agg AS (
    SELECT
        s.instance,
        s.deal_line_item_id,
        FIRST(s.advertiser_name, TRUE)              AS advertiser,
        FIRST(s.deal_name, TRUE)                    AS order_name,
        FIRST(s.deal_line_item_name, TRUE)          AS line_item_name,
        FIRST(s.deal_line_item_start_date, TRUE)    AS line_item_start_ts,
        FIRST(s.deal_line_item_end_date, TRUE)      AS line_item_end_ts,
        FIRST(s.net_unit_cost_amt, TRUE)             AS rate_raw,
        FIRST(s.production_quantity, TRUE)           AS goal_quantity_raw,
        FIRST(s.quantity, TRUE)                      AS contracted_quantity_raw,
        FIRST(s.account_executive, TRUE)             AS salesperson,
        SUM(TRY_CAST(s.delivered_impressions AS DOUBLE)) AS delivered_raw
    FROM source_rows s
    CROSS JOIN params p
    WHERE YEAR(s.event_date) = YEAR(p.report_ts)
    GROUP BY s.instance, s.deal_line_item_id
),
typed_source AS (
    SELECT
        instance,
        deal_line_item_id,
        advertiser,
        order_name,
        line_item_name,
        TRY_CAST(line_item_start_ts AS TIMESTAMP) AS line_item_start_ts,
        TRY_CAST(line_item_end_ts AS TIMESTAMP)   AS line_item_end_ts,
        COALESCE(TRY_CAST(rate_raw AS DOUBLE), 0.0)                AS rate,
        COALESCE(TRY_CAST(goal_quantity_raw AS DOUBLE), 0.0)       AS goal_quantity,
        COALESCE(TRY_CAST(contracted_quantity_raw AS DOUBLE), 0.0) AS contracted_quantity,
        salesperson,
        COALESCE(TRY_CAST(delivered_raw AS DOUBLE), 0.0)            AS ad_server_impressions
    FROM source_agg
),
expected_raw AS (
    SELECT
        s.instance,
        s.advertiser,
        s.order_name,
        s.line_item_name,
        s.line_item_start_ts,
        s.line_item_end_ts,
        s.rate,
        s.goal_quantity,
        s.contracted_quantity,
        CAST('' AS STRING) AS delivery_indicator,
        s.salesperson,
        s.ad_server_impressions,
        CAST(0.0 AS DOUBLE) AS third_party_impressions,
        CAST(0.0 AS DOUBLE) AS third_party_clicks,
        CAST(0.0 AS DOUBLE) AS third_party_ctr,
        CASE
            WHEN s.contracted_quantity = 0 THEN 0.0
            ELSE (s.goal_quantity - s.contracted_quantity) / s.contracted_quantity
        END AS buffer,
        CAST(0.0 AS DOUBLE) AS discrepancy,
        -- Podcast OSI (Ad Ops formula), used for BOTH OSI columns:
        -- (delivered / contracted) / (days live / total flight days)
        CASE
            WHEN s.ad_server_impressions <= 0 OR s.contracted_quantity <= 0 THEN 0.0
            WHEN s.line_item_start_ts IS NULL OR s.line_item_end_ts IS NULL THEN 0.0
            WHEN s.line_item_end_ts < s.line_item_start_ts THEN 0.0
            WHEN p.report_ts < s.line_item_start_ts THEN 0.0
            ELSE
                (s.ad_server_impressions / s.contracted_quantity)
                /
                (
                    (DATEDIFF(DAY, s.line_item_start_ts, LEAST(p.report_ts, s.line_item_end_ts)) + 1.0)
                    /
                    (DATEDIFF(DAY, s.line_item_start_ts, s.line_item_end_ts) + 1.0)
                )
        END AS current_first_party_osi,
        CASE
            WHEN s.ad_server_impressions <= 0 OR s.contracted_quantity <= 0 THEN 0.0
            WHEN s.line_item_start_ts IS NULL OR s.line_item_end_ts IS NULL THEN 0.0
            WHEN s.line_item_end_ts < s.line_item_start_ts THEN 0.0
            WHEN p.report_ts < s.line_item_start_ts THEN 0.0
            ELSE
                (s.ad_server_impressions / s.contracted_quantity)
                /
                (
                    (DATEDIFF(DAY, s.line_item_start_ts, LEAST(p.report_ts, s.line_item_end_ts)) + 1.0)
                    /
                    (DATEDIFF(DAY, s.line_item_start_ts, s.line_item_end_ts) + 1.0)
                )
        END AS current_third_party_osi,
        CAST(0.0 AS DOUBLE) AS total_error_count,
        CAST(0.0 AS DOUBLE) AS total_error_rate
    FROM typed_source s
    CROSS JOIN params p
),
actual_raw AS (
    SELECT
        TRY_CAST(`Instance` AS STRING)                    AS instance,
        TRY_CAST(`Advertiser` AS STRING)                  AS advertiser,
        TRY_CAST(`Order` AS STRING)                       AS order_name,
        TRY_CAST(`Line Item Name` AS STRING)              AS line_item_name,
        TRY_CAST(`Line Item Start Date` AS TIMESTAMP)     AS line_item_start_ts,
        TRY_CAST(`Line Item End Date` AS TIMESTAMP)       AS line_item_end_ts,
        TRY_CAST(`Rate` AS DOUBLE)                        AS rate,
        TRY_CAST(`Goal Quantity` AS DOUBLE)               AS goal_quantity,
        TRY_CAST(`Contracted Quantity` AS DOUBLE)         AS contracted_quantity,
        TRY_CAST(`Delivery Indicator` AS STRING)          AS delivery_indicator,
        TRY_CAST(`Salesperson` AS STRING)                 AS salesperson,
        TRY_CAST(`Ad Server Impressions` AS DOUBLE)       AS ad_server_impressions,
        TRY_CAST(`Impressions (3rd Party)` AS DOUBLE)     AS third_party_impressions,
        TRY_CAST(`Clicks (3rd Party)` AS DOUBLE)          AS third_party_clicks,
        TRY_CAST(`3rd Party CTR` AS DOUBLE)               AS third_party_ctr,
        TRY_CAST(`Buffer` AS DOUBLE)                      AS buffer,
        TRY_CAST(`Discrepancy` AS DOUBLE)                 AS discrepancy,
        TRY_CAST(`Current First Party OSI` AS DOUBLE)     AS current_first_party_osi,
        TRY_CAST(`Current Third Party OSI` AS DOUBLE)     AS current_third_party_osi,
        TRY_CAST(`Total Error Count` AS DOUBLE)           AS total_error_count,
        TRY_CAST(`Total Error Rate` AS DOUBLE)            AS total_error_rate
    FROM report_amperwave_sheet

    UNION ALL

    SELECT
        TRY_CAST(`Instance` AS STRING),
        TRY_CAST(`Advertiser` AS STRING),
        TRY_CAST(`Order` AS STRING),
        TRY_CAST(`Line Item Name` AS STRING),
        TRY_CAST(`Line Item Start Date` AS TIMESTAMP),
        TRY_CAST(`Line Item End Date` AS TIMESTAMP),
        TRY_CAST(`Rate` AS DOUBLE),
        TRY_CAST(`Goal Quantity` AS DOUBLE),
        TRY_CAST(`Contracted Quantity` AS DOUBLE),
        TRY_CAST(`Delivery Indicator` AS STRING),
        TRY_CAST(`Salesperson` AS STRING),
        TRY_CAST(`Ad Server Impressions` AS DOUBLE),
        TRY_CAST(`Impressions (3rd Party)` AS DOUBLE),
        TRY_CAST(`Clicks (3rd Party)` AS DOUBLE),
        TRY_CAST(`3rd Party CTR` AS DOUBLE),
        TRY_CAST(`Buffer` AS DOUBLE),
        TRY_CAST(`Discrepancy` AS DOUBLE),
        TRY_CAST(`Current First Party OSI` AS DOUBLE),
        TRY_CAST(`Current Third Party OSI` AS DOUBLE),
        TRY_CAST(`Total Error Count` AS DOUBLE),
        TRY_CAST(`Total Error Rate` AS DOUBLE)
    FROM report_megaphone_sheet
),
expected AS (
    SELECT
        TRIM(COALESCE(instance, ''))                     AS instance,
        TRIM(COALESCE(advertiser, ''))                   AS advertiser,
        TRIM(COALESCE(order_name, ''))                   AS order_name,
        TRIM(COALESCE(line_item_name, ''))               AS line_item_name,
        line_item_start_ts,
        line_item_end_ts,
        CAST(ROUND(COALESCE(rate, 0.0), 8) AS DECIMAL(38,8)) AS rate,
        CAST(ROUND(COALESCE(goal_quantity, 0.0), 6) AS DECIMAL(38,6)) AS goal_quantity,
        CAST(ROUND(COALESCE(contracted_quantity, 0.0), 6) AS DECIMAL(38,6)) AS contracted_quantity,
        TRIM(COALESCE(delivery_indicator, ''))           AS delivery_indicator,
        TRIM(COALESCE(salesperson, ''))                  AS salesperson,
        CAST(ROUND(COALESCE(ad_server_impressions, 0.0), 6) AS DECIMAL(38,6)) AS ad_server_impressions,
        CAST(ROUND(COALESCE(third_party_impressions, 0.0), 6) AS DECIMAL(38,6)) AS third_party_impressions,
        CAST(ROUND(COALESCE(third_party_clicks, 0.0), 6) AS DECIMAL(38,6)) AS third_party_clicks,
        CAST(ROUND(COALESCE(third_party_ctr, 0.0), 10) AS DECIMAL(38,10)) AS third_party_ctr,
        CAST(ROUND(COALESCE(buffer, 0.0), 10) AS DECIMAL(38,10)) AS buffer,
        CAST(ROUND(COALESCE(discrepancy, 0.0), 10) AS DECIMAL(38,10)) AS discrepancy,
        CAST(ROUND(COALESCE(current_first_party_osi, 0.0), 10) AS DECIMAL(38,10)) AS current_first_party_osi,
        CAST(ROUND(COALESCE(current_third_party_osi, 0.0), 10) AS DECIMAL(38,10)) AS current_third_party_osi,
        CAST(ROUND(COALESCE(total_error_count, 0.0), 6) AS DECIMAL(38,6)) AS total_error_count,
        CAST(ROUND(COALESCE(total_error_rate, 0.0), 10) AS DECIMAL(38,10)) AS total_error_rate
    FROM expected_raw
),
actual AS (
    SELECT
        TRIM(COALESCE(instance, ''))                     AS instance,
        TRIM(COALESCE(advertiser, ''))                   AS advertiser,
        TRIM(COALESCE(order_name, ''))                   AS order_name,
        TRIM(COALESCE(line_item_name, ''))               AS line_item_name,
        line_item_start_ts,
        line_item_end_ts,
        CAST(ROUND(COALESCE(rate, 0.0), 8) AS DECIMAL(38,8)) AS rate,
        CAST(ROUND(COALESCE(goal_quantity, 0.0), 6) AS DECIMAL(38,6)) AS goal_quantity,
        CAST(ROUND(COALESCE(contracted_quantity, 0.0), 6) AS DECIMAL(38,6)) AS contracted_quantity,
        TRIM(COALESCE(delivery_indicator, ''))           AS delivery_indicator,
        TRIM(COALESCE(salesperson, ''))                  AS salesperson,
        CAST(ROUND(COALESCE(ad_server_impressions, 0.0), 6) AS DECIMAL(38,6)) AS ad_server_impressions,
        CAST(ROUND(COALESCE(third_party_impressions, 0.0), 6) AS DECIMAL(38,6)) AS third_party_impressions,
        CAST(ROUND(COALESCE(third_party_clicks, 0.0), 6) AS DECIMAL(38,6)) AS third_party_clicks,
        CAST(ROUND(COALESCE(third_party_ctr, 0.0), 10) AS DECIMAL(38,10)) AS third_party_ctr,
        CAST(ROUND(COALESCE(buffer, 0.0), 10) AS DECIMAL(38,10)) AS buffer,
        CAST(ROUND(COALESCE(discrepancy, 0.0), 10) AS DECIMAL(38,10)) AS discrepancy,
        CAST(ROUND(COALESCE(current_first_party_osi, 0.0), 10) AS DECIMAL(38,10)) AS current_first_party_osi,
        CAST(ROUND(COALESCE(current_third_party_osi, 0.0), 10) AS DECIMAL(38,10)) AS current_third_party_osi,
        CAST(ROUND(COALESCE(total_error_count, 0.0), 6) AS DECIMAL(38,6)) AS total_error_count,
        CAST(ROUND(COALESCE(total_error_rate, 0.0), 10) AS DECIMAL(38,10)) AS total_error_rate
    FROM actual_raw
),
missing_from_report AS (
    SELECT * FROM expected
    EXCEPT ALL
    SELECT * FROM actual
),
unexpected_in_report AS (
    SELECT * FROM actual
    EXCEPT ALL
    SELECT * FROM expected
)
SELECT 'TABLE_ROW_NOT_IN_REPORT' AS validation_result, m.*
FROM missing_from_report m

UNION ALL

SELECT 'REPORT_ROW_NOT_IN_TABLE' AS validation_result, u.*
FROM unexpected_in_report u
;

-- Detailed mismatches. Expected result: zero rows.
SELECT *
FROM podcast_report_mismatches
ORDER BY instance, advertiser, order_name, line_item_name, validation_result;

-- Summary. Expected result: zero rows. If rows exist, each count is a mismatch count.
SELECT validation_result, instance, COUNT(*) AS mismatch_rows
FROM podcast_report_mismatches
GROUP BY validation_result, instance
ORDER BY instance, validation_result;

-- Optional source-quality check: the report uses FIRST(..., TRUE), which is
-- nondeterministic when a deal_line_item_id has more than one non-null value
-- for a descriptive field.
WITH params AS (
    SELECT TIMESTAMP '2026-07-23 00:00:00' AS report_ts
),
source_rows AS (
    SELECT
        'Amperwave' AS instance,
        deal_line_item_id, advertiser_name, deal_name, deal_line_item_name,
        deal_line_item_start_date, deal_line_item_end_date, net_unit_cost_amt,
        production_quantity, quantity, account_executive, event_date
    FROM fox_bi_qa.gold_ad_sales.ft_amperwave_campaign_delivery

    UNION ALL

    SELECT
        'Spotify' AS instance,
        deal_line_item_id, advertiser_name, deal_name, deal_line_item_name,
        deal_line_item_start_date, deal_line_item_end_date, net_unit_cost_amt,
        production_quantity, quantity, account_executive, event_date
    FROM fox_bi_qa.gold_ad_sales.ft_megaphone_campaign_delivery
)
SELECT
    instance,
    deal_line_item_id,
    CONCAT_WS(', ',
        CASE WHEN COUNT(DISTINCT advertiser_name) > 1 THEN 'advertiser_name' END,
        CASE WHEN COUNT(DISTINCT deal_name) > 1 THEN 'deal_name' END,
        CASE WHEN COUNT(DISTINCT deal_line_item_name) > 1 THEN 'deal_line_item_name' END,
        CASE WHEN COUNT(DISTINCT deal_line_item_start_date) > 1 THEN 'deal_line_item_start_date' END,
        CASE WHEN COUNT(DISTINCT deal_line_item_end_date) > 1 THEN 'deal_line_item_end_date' END,
        CASE WHEN COUNT(DISTINCT net_unit_cost_amt) > 1 THEN 'net_unit_cost_amt' END,
        CASE WHEN COUNT(DISTINCT production_quantity) > 1 THEN 'production_quantity' END,
        CASE WHEN COUNT(DISTINCT quantity) > 1 THEN 'quantity' END,
        CASE WHEN COUNT(DISTINCT account_executive) > 1 THEN 'account_executive' END
    ) AS fields_with_multiple_values
FROM source_rows s
CROSS JOIN params p
WHERE YEAR(s.event_date) = YEAR(p.report_ts)
GROUP BY instance, deal_line_item_id
HAVING
       COUNT(DISTINCT advertiser_name) > 1
    OR COUNT(DISTINCT deal_name) > 1
    OR COUNT(DISTINCT deal_line_item_name) > 1
    OR COUNT(DISTINCT deal_line_item_start_date) > 1
    OR COUNT(DISTINCT deal_line_item_end_date) > 1
    OR COUNT(DISTINCT net_unit_cost_amt) > 1
    OR COUNT(DISTINCT production_quantity) > 1
    OR COUNT(DISTINCT quantity) > 1
    OR COUNT(DISTINCT account_executive) > 1
ORDER BY instance, deal_line_item_id;
