# Databricks notebook source
from typing import TYPE_CHECKING, List, Any, Dict, Tuple, Union, Optional, Callable, cast
import re
import math
from datetime import date, datetime, time, timedelta

import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

WHITELISTED_TEST_ORDERS = {10720, 10727, 10723, 10729, 10730, 10724, 10731, 10732}

global drop_bucket, environment, catalog, schema, aws_profile
# drop_bucket = 'ad-tech-drop-dev'
# aws_profile = 'databricks-cpe-dev'
environment = ''
catalog = 'fox_bi_prod'
schema = 'gold_ad_sales'


from pyspark.sql.functions import *

# COMMAND ----------

global file_to_table_fields
file_to_table_fields = {
    'OMS_Template_Finance Invoice Line Item.csv': {'source_column_names': ['invoice_organization_name', 'agency_id', 'agency_name', 'advertiser_id', 'advertiser_name', 'sales_order_id', 'sales_order_name', 'sales_order_start_date', 'sales_order_end_date', 'sales_order_status', 'parent_sales_line_item_id', 'parent_sales_line_item_name', 'fw_migration_placement_id', 'ps_line_item_id', 'sales_line_item_id', 'sales_line_item_name', 'sales_line_item_start_date', 'sales_line_item_end_date', 'placement_property_derived', 'buy_type', 'product_name', 'product_type', 'billable_third_party_server', 'migration_invoice_line_id', 'p2_plus_cpm', 'hh_cpm', 'hh_imps', 'duration', 'equivalize', 'demo_band', 'fw_gross_impression_cap', 'fw_target_demographics_nielsen_dar', 'fw_target_demographics_comscore_vce', 'fw_rbp_advanced', 'est_demo_comp_vpvh', 'is_makegood', 'is_added_value', 'viewability', 'viewability_vendor', 'deal_id', 'deal_type', 'market_place_type', 'sales_region', 'programmatic_type', 'external_po_number', 'vp_sales', 'primary_sales_person_name', 'campaign_manager_trafficker', 'account_coordinator', 'order_owner_full_name', 'cost_type', 'quantity', 'net_unit_cost', 'net_cost', 'total_net', 'opportunity_unique_id', 'suggested_invoice_amount_terms', 'include_on_invoice', 'deal_year', 'linear_liability', 'liability_type', 'product_billing_id', 'fw_budget_model', 'net_invoice_amount', 'remaining_net_invoice_amount'], 'target_column_names': ['Invoice Organization Name', 'Agency ID', 'Agency Name', 'Advertiser ID', 'Advertiser Name', 'Sales Order ID', 'Sales Order Name', 'Sales Order Start Date', 'Sales Order End Date', 'Sales Order Status', 'Parent Sales Line Item ID', 'Parent Sales Line Item Name', 'FW Migration Placement ID', 'PS Line Item IDs', 'Sales Line Item ID', 'Sales Line Item Name', 'Sales Line Item Start Date', 'Sales Line Item End Date', 'Placement Property', 'Buy Type', 'Product Name', 'Product Type', 'Billable Third Party Server', 'Migration Invoice Line ID', 'P2+ CPM', 'HH CPM', 'HH Imps', 'Duration', 'Equivalize?', 'Demo Band', 'FW - Gross Impression Cap', 'FW - Target Demographics - Nielsen DAR', 'FW - Target Demographics - comScore vCE', 'FW - RBP Advanced', 'Planned Demo Comp', 'Makegood', 'Added Value', 'Viewability', 'Viewability Vendor', 'Deal ID', 'Deal Type', 'Marketplace Type', 'Sales Region', 'Programmatic Type', 'External PO Ref', 'VP Sales', 'Primary Salesperson Full Name', 'Trafficker/Campaign Manager', 'Account Coordinator', 'Order Owner Full Name', 'Cost Method', 'Quantity', 'Net Unit Cost', 'Net Cost', 'Total Net', 'Opportunity Unique ID', 'Suggested Invoice Amount Terms', 'Include on Invoice', 'Deal Year', 'Linear Liability', 'Liability Type', 'Product Billing ID', 'FW - Budget Model', 'Net Invoice Amount', 'Remaining Net Invoice Amount'], 'metric_cols': ['net_invoice_amount'], 'date_cols': ['sales_order_start_date', 'sales_order_start_date', 'sales_line_item_start_date', 'sales_line_item_end_date'], 'table': 'drop_digital_operative_line_detail'},
    'Operative_OMS_AdOps_PLI.parquet': {'source_column_names': ['sales_line_item_id', 'reason_pended_flg', 'primary_earliest_delivery_date', 'third_party_earliest_delivery_date', 'push_qty'], 'target_column_names': ['Sales Line Item ID', 'Pending Message Name', 'Primary Earliest Delivery Date', 'Third-Party Earliest Delivery Date', 'Push Quantity'], 'metric_cols': [], 'date_cols': ['primary_earliest_delivery_date', 'third_party_earliest_delivery_date'], 'table': 'drop_digital_operative_line_detail'},
    'Production_Systems.csv': {'source_column_names': ['sales_line_item_id', 'production_system_name', 'fw_deal_type', 'production_line_item_status'], 'target_column_names': ['Sales Line Item ID', 'Production System Name', 'FW - Deal Type', 'Production Line Item Status'], 'metric_cols': [], 'date_cols': [], 'table': 'drop_digital_operative_line_detail'},
    'FinanceExport_Template.csv': {'source_column_names': ['invoice_id', 'sales_line_item_id', 'placement_property_derived', 'net_invoice_amount_terms', 'billing_notes', 'net_invoice_amount', 'net_cost', 'billing_period_name'], 'target_column_names': ['Invoice ID', 'Sales Line Item ID', 'Placement Property', 'Net Invoice Amount Terms', 'Billing Notes', 'Net Invoice Amount', 'Net Line Item Cost', 'Billing Period Name'], 'metric_cols': ['net_invoice_amount'], 'date_cols': [], 'table': 'drop_digital_operative_line_detail'},
    'Allocation_Details.parquet': {'source_column_names': ['sales_order_line_item_id', 'date', 'is_live_flg', 'is_dark_flg'], 'target_column_names': ['sales_order_line_item_id', 'date', 'is_live_flg', 'is_dark_flg'], 'metric_cols': [], 'date_cols': ['date'], 'table': 'drop_oo_aos_allocation_details'},
    'Sales Line Item_Working Orders.csv': {'source_column_names': ['invoice_organization_name','agency_name', 'advertiser_name', 'sales_order_id', 'sales_order_name', 'sales_order_status', 'sales_stage_name', 'sales_region', 'sales_line_item_id', 'sales_line_item_name', 'sales_line_item_start_date', 'sales_line_item_end_date', 'placement_property_derived', 'product_name', 'billable_third_party_server', 'p2_plus_cpm', 'hh_imps', 'duration', 'demo_band', 'fw_gross_impression_cap', 'est_demo_comp_vpvh', 'viewability', 'deal_year', 'campaign_manager_trafficker', 'account_coordinator', 'order_owner_full_name', 'cost_type', 'quantity', 'opportunity_unique_id', 'unit_type'], 'target_column_names': ['Invoice Organization Name','Agency Name', 'Advertiser Name', 'Sales Order ID', 'Sales Order Name', 'Sales Order Status', 'Sales Stage', 'Sales Region', 'Sales Line Item ID', 'Sales Line Item Name', 'Sales Line Item Start Date', 'Sales Line Item End Date', 'Placement Property', 'Product Name', 'Billable Third Party Server', 'P2+ CPM', 'HH Imps', 'Duration', 'Demo Band', 'FW - Gross Impression Cap', 'Planned Demo Comp', 'Viewability', 'Deal Year', 'Trafficker/Campaign Manager', 'Account Coordinator', 'Order Owner Full Name', 'Cost Method', 'Quantity', 'Opportunity Unique ID', 'Unit Type'], 'metric_cols': [], 'date_cols': ['sales_line_item_start_date', 'sales_line_item_end_date'], 'table': 'drop_digital_operative_line_detail'},
    'Sales_Order.parquet': {'source_column_names': ['sales_order_id', 'invoice_organization_name'], 'target_column_names': ['Sales_Order_ID', 'Invoice_Organization_Name'], 'metric_cols': [], 'date_cols': [], 'table': 'drop_digital_operative_line_detail'}
}

# COMMAND ----------

def cast_to_boolean(row: pd.Series, col) -> bool:
    if row[col].lower() == 'false':
        return False
    elif row[col].lower() == 'true':
        return True
    return None

# COMMAND ----------

def write_xcom_value(script: str, output: str) -> None:
    xcom_path = f'dbfs:/FileStore/adtech/drop/{script}/xcom.txt'
    dbutils.fs.put(xcom_path, output, overwrite=True)

def file_to_df(file: str):
    # Create a Spark session (this is usually done automatically in Databricks)
    spark = SparkSession.builder.appName("DROP").getOrCreate()
    
    global catalog, schema, source

    global file_to_table_fields
    table = file_to_table_fields[file]['table']
    source_column_names = file_to_table_fields[file]['source_column_names']
    target_column_names = file_to_table_fields[file]['target_column_names']
    metric_cols = file_to_table_fields[file]['metric_cols']
    date_cols = file_to_table_fields[file]['date_cols']

    assert len(source_column_names) == len(target_column_names), "source and target column names must be the same length"
    
    # Load a table from Unity Catalog
    query = "Select "
    for index, col in enumerate(source_column_names):
        if col == 'invoice_organization_name':
            query += """case when invoice_organization_name = 'FOX Sports & Entertainment Programmatic' then 'FOX Corp Programmatic Guaranteed'
            when invoice_organization_name = 'Fox Sports & Entertainment' then 'FOX Sports & Entertainment' else invoice_organization_name end as invoice_organization_name"""
        elif col == 'cost_type':
            query += """case when cost_type like '%SOV-Flat Rate%' then 'SOV-Flat Rate' else cost_type end as cost_type"""
        elif col == 'deal_id':
            query += """trim(deal_id) as deal_id"""
        elif col == 'placement_property_derived':
            query += """case when source = 'AOS' then case when placement_property = 'FOX Sports' then case when buy_type in ('Direct LFV', 'Programmatic LFV', 'Programmatic LFV Backfill', 'Programmatic LFV PMP') then 'FOX Sports Streaming' when buy_type in ('Direct SFV', 'Programmatic SFV', 'Programmatic SFV Backfill', 'Programmatic SFV PMP') then 'FOX Sports Clips' when buy_type = 'Social Facebook/Instagram' then 'FOX Sports - Facebook/Instagram' when buy_type = 'Social TikTok' then 'FOX Sports - TikTok' when buy_type = 'Social X' then 'FOX Sports - Twitter' when buy_type = 'Sponsorship' then 'FOX Sports - Custom Sponsorship' else 'FOX Sports' end when placement_property = 'FOXNow' then case when buy_type = 'Social Facebook/Instagram' then 'FOX - Facebook/Instagram' when buy_type = 'Social TikTok' then 'FOX - TikTok' when buy_type = 'Social X' then 'FOX - Twitter' else 'FOXNow' end else placement_property end else placement_property end as placement_property_derived"""
        elif col == 'billable_third_party_server':
            query += """CASE
                            WHEN billable_third_party_server = 'DCM' THEN 'FOX DCM'
                            WHEN billable_third_party_server = 'DFA' THEN 'FOX DCM'
                            WHEN billable_third_party_server ilike '%Doubleverify%' THEN 'FOX Doubleverify'
                            WHEN billable_third_party_server = 'Innovid' THEN 'FOX Innovid'
                            WHEN billable_third_party_server = 'FOX Innovid' THEN 'FOX Innovid'
                            WHEN billable_third_party_server ilike '%Flashtalking%' THEN 'FOX Flashtalking'
                            WHEN billable_third_party_server ilike '%Extreme Reach%' THEN 'FOX Extreme Reach'
                            WHEN billable_third_party_server ilike '%Sizmek%' THEN 'FOX Sizmek'
                            WHEN billable_third_party_server ilike '%MediaMind%' THEN 'FOX Sizmek'
                            WHEN billable_third_party_server ilike '%1st Party%' THEN '1st Party'
                            ELSE billable_third_party_server
                        END AS billable_third_party_server"""
        elif col == 'est_demo_comp_vpvh':
            query += """case when replace(est_demo_comp_vpvh, '%') regexp '[a-zA-Z]' then 0 else replace(est_demo_comp_vpvh, '%') end as est_demo_comp_vpvh"""
        elif col in metric_cols:
            query += f" SUM({col}) as {col}"
        elif col in date_cols:
            query += f"{col}::date"
        else:
            query += f"{col}"
        if index != len(source_column_names) - 1:
            query += f", "

    query += f" FROM {catalog}.{schema}.{table}"

    query += " GROUP BY "
    group_by_cols = [col for col in source_column_names if col not in metric_cols]
    for index, col in enumerate(group_by_cols):
        query += f"{col}"
        if index != len(group_by_cols) - 1:
            query += f", "

    df = spark.sql(query)

    for s,t in zip(source_column_names, target_column_names):
        df = df.withColumnRenamed(s, t)

    return df.toPandas()

# COMMAND ----------

def list_all_s3_keys(client: 'S3Client', bucket: str, prefix: str = '', suffix: str = '') -> List[str]:
    """
    Retrieve all keys in S3 for a given bucket matching a prefix and suffix

    Keyword arguments:
    client -- a S3 client provided with appropriate credentials to use
    bucket -- a S3 bucket accessible by the client
    prefix -- a prefix to match keys on
    suffix -- a suffix to match keys on
    """
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    )
    try:
        keys = [
            content['Key']
            for response in response_iterator
            for content in response['Contents']
            if content['Key'].endswith(suffix)
        ]
    except KeyError:
        keys = []
    return keys

# COMMAND ----------

def filter_op_orders(op_oms: pd.DataFrame, start_date: datetime, end_date: datetime, *, remove_non_ad_served: bool = False) -> pd.DataFrame:
    parent_columns = ['Parent Line Item Total', 'Parent Line Item Run Rate', 'Parent Line Item Earned Revenue', 'Parent Line Item Revenue at Risk']

    if any(parent_column not in op_oms.columns for parent_column in parent_columns):
        op_oms = op_oms[op_oms['Sales Line Item End Date'] > end_date]
        op_oms = op_oms[op_oms['Sales Line Item Start Date'] <= start_date]
        return op_oms

    original_parent_values = op_oms.groupby('Parent Sales Line Item ID').aggregate({
        c: 'min' for c in parent_columns
    })

    op_oms = op_oms[op_oms['Sales Line Item End Date'] > end_date]
    op_oms = op_oms[op_oms['Sales Line Item Start Date'] <= start_date]

    if remove_non_ad_served:
        op_oms = op_oms[~(op_oms['Sales Order Name'].str.contains('Non Ad-Served') | op_oms['Sales Line Item Name'].str.contains('Non Ad-Served'))]

    min_line_item_ids_per_parent = op_oms[~op_oms['Is Future Start']].groupby('Parent Sales Line Item ID').aggregate({'Sales Line Item ID': 'min'})
    min_line_item_ids_per_parent = min_line_item_ids_per_parent.rename(columns={'Sales Line Item ID': 'Min Sales Line Item ID in Group'})

    op_oms_joined = op_oms.merge(min_line_item_ids_per_parent, on=['Parent Sales Line Item ID'], how='left')
    op_oms_joined['Missing Parent Values'] = (op_oms_joined['Sales Line Item ID'] == op_oms_joined['Min Sales Line Item ID in Group']) \
        & op_oms_joined['Parent Line Item Revenue at Risk'].isna() \
        & ~op_oms_joined['Is Future Start']
    op_oms_joined = op_oms_joined.merge(original_parent_values, on=['Parent Sales Line Item ID'], how='left', suffixes=[None, ' Original'])

    for column in parent_columns:
        def select(row: pd.Series) -> Any:
            if row['Missing Parent Values']:
                return row[f'{column} Original']
            return row[column]
        op_oms_joined[column] = op_oms_joined.apply(select, axis=1)

    op_oms = op_oms_joined.drop(columns=['Min Sales Line Item ID in Group', 'Missing Parent Values'])
    op_oms = op_oms.drop(columns=[f'{column} Original' for column in parent_columns])

    return op_oms

# COMMAND ----------

# Function to convert different month formats to 'YYYY-M'
def format_billing_period_name(row: pd.Series) -> Any:
    if row['Billing Period Name'] is None:
        return None

    # Case 1: Format like "NOV'26"
    if "'" in row['Billing Period Name']:
        month_abbr = row['Billing Period Name'][:3].upper()
        year = '20' + row['Billing Period Name'].split("'")[1]  # Assuming year is 2026, change logic for different century
        date_str = f"{month_abbr} {year}"
        date_obj = pd.to_datetime(date_str, format="%b %Y")
    
    # Case 2: Format like "June-2023"
    elif '-' in row['Billing Period Name'] and row['Billing Period Name'][0].isalpha():
        date_obj = pd.to_datetime(row['Billing Period Name'], format="%B-%Y")
    
    # Case 3: Format like "08-2021"
    elif '-' in row['Billing Period Name'] and row['Billing Period Name'][0].isdigit() and 'Q' not in row['Billing Period Name']:
        date_obj = pd.to_datetime(row['Billing Period Name'], format="%m-%Y")
    
    # Case 4: Format like "2021-Q1"
    elif '-' in row['Billing Period Name'] and row['Billing Period Name'][0].isdigit() and 'Q' in row['Billing Period Name']:
        quarter_to_last_month_in_quarter = {1: 9, 2: 12, 3: 3, 4: 6}
        quarter = int(row['Billing Period Name'][-1])
        month = quarter_to_last_month_in_quarter[quarter]
        date_str = f'{month} {row["Billing Period Name"][:4]}'
        date_obj = pd.to_datetime(date_str, format="%m %Y")

    return date_obj


def read_op_invoices(invoice_filename: str, report_date: datetime) -> pd.DataFrame:
    op_finance = file_to_df('FinanceExport_Template.csv')

    op_finance = op_finance.dropna(
        subset=['Sales Line Item ID'],
    ).astype(
        {
            'Invoice ID': 'Int64',
            'Sales Line Item ID': 'int64',
        },
    ).set_index(
        'Sales Line Item ID',
    )

    op_finance.loc[:, 'Billing Period Name'] = op_finance.apply(format_billing_period_name, axis=1)
    op_finance.loc[:, 'Billing Period End Date'] = op_finance.loc[:, 'Billing Period Name'] + MonthEnd(1)
    op_finance['Billing Period End Date'] = op_finance['Billing Period End Date'].astype('datetime64[ns]')

    inv_terms_dict = {
        'thirdparty_performance': '3rd Party Performance',
        'straightline': 'Straightline',
        'publisher_performance': 'Primary Performance',
        'manual': 'Manual Entry',
        'prorated': 'Pro-rated',
        'single_month': 'Single Period',
        'imported': 'Imported',
        'performance': '3rd Party Performance',
    }

    op_finance.loc[:, 'Net Invoice Amount Terms'] = op_finance.loc[:, 'Net Invoice Amount Terms'].map(inv_terms_dict)
    op_finance.loc[:, 'In Current Quarter'] = op_finance['Billing Period Name'].dt.quarter == pd.Series(report_date).dt.quarter[0]
    op_finance.loc[:, 'In Current Month'] = op_finance['Billing Period Name'].dt.month == report_date.month

    return op_finance

# COMMAND ----------

def format_currency(cell: float) -> str:
    if math.isnan(float(cell)):
        return ''
    return '{:.2f}'.format(float(cell))


def set_col_fmt(fmt: Any, worksheet: Any, df: pd.DataFrame, cols: Any, offset: int = 0) -> List[Any]:
    return [
        worksheet.set_column(
            df.columns.get_loc(col) + offset,
            df.columns.get_loc(col) + offset,
            12,
            fmt,
        ) for col in cols
        if col in df.columns
    ]

# COMMAND ----------

def drop_freewheel_inactive(fw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all rows that are considered inactive. This includes:
    * Rows that end before 2020
    * Rows that are PROMO
    * Rows that are placeholder, BTN, test, or Do Not Use campaigns
    * Rows that are placeholder, BTN, test, or Do Not Use placement properties
    """
    filtered = fw_df.loc[fw_df['Sales Order End Date'] >= datetime(2019, 12, 31, 0, 0), :]
    filtered = filtered.loc[filtered['Agency Name'] != 'FNG IN-HOUSE MARKETING & PROMOTIONS', :]

    filtered = filtered.loc[
        ~filtered['Campaign Name'].str.contains(
            # '.*Test.*|'
            # '.*BTN.*|'
            '.*Do Not Use.*|'
            '.*T3ST.*|'
            '.*Fox Broadcasting.*|'
            '.*DoNotUse.*|'
            '.*Cancel.*',
            regex=True,
            flags=re.IGNORECASE,
            na=False,
        ),
        :,
    ]

    filtered = filtered.loc[
        ~filtered['Placement Property'].str.contains(
            # '.*Test.*|'
            # '.*BTN.*|'
            '.*Do Not Use.*|'
            '.*T3ST.*|'
            '.*FBC.*|'
            '.*Fox Broadcasting.*|'
            '.*DoNotUse.*|'
            '.*Cancel.*',
            regex=True,
            flags=re.IGNORECASE,
            na=False,
        ),
        :,
    ]

    return filtered

# COMMAND ----------

def read_fw_demo(
    fw_demo_monthly: str,
    fw_demo_daily_path: str,
    report_date: datetime,
    monthly: bool = False,
    skiprows: int = 0,
) -> pd.DataFrame:
    report_date = datetime.combine(
        report_date.date(),
        datetime.min.time(),
    )
    demo_comp_date = report_date - timedelta(days=4)
    demo_comp_month_start = demo_comp_date.replace(day=1)

    fw_demo = pd.read_csv(
        fw_demo_monthly,
        skiprows=skiprows,
        usecols={
            'Placement ID',
            'Event Month',
            'On-Target Net Delivered Impressions',
            'Gross Counted Ads',
        },
        dtype={
            'Placement ID': 'int64',
            'On-Target Net Delivered Impressions': 'object',
            'Gross Counted Ads': 'object',
        },
        parse_dates=[
            'Event Month',
        ],
        index_col='Placement ID',
        encoding='utf-8',
        storage_options={'profile': aws_profile},
    ).rename(
        {
            'Gross Counted Ads': 'Gross Counted Ads (Demo)',
        },
        axis=1,
    )

    fw_demo['On-Target Net Delivered Impressions'] = fw_demo['On-Target Net Delivered Impressions'].str.replace(',', '').fillna(0).astype('int64')
    fw_demo['Gross Counted Ads (Demo)'] = fw_demo['Gross Counted Ads (Demo)'].str.replace(',', '').fillna(0).astype('int64')


    fw_demo_daily = pd.read_csv(
        fw_demo_daily_path,
        skiprows=skiprows,
        index_col='Placement ID',
        parse_dates=['Event Date'],
        usecols=[
            'Placement ID',
            'Event Date',
            'On-Target Net Delivered Impressions',
            'Gross Counted Ads',
        ],
        dtype={
            'Placement ID': 'int64',
            'On-Target Net Delivered Impressions': 'object',
            'Gross Counted Ads': 'object',
        },
        storage_options={'profile': aws_profile},
    ).rename(
        {
            'Gross Counted Ads': 'Gross Counted Ads (Demo)',
        },
        axis=1,
    )

    fw_demo_daily['On-Target Net Delivered Impressions'] = fw_demo_daily['On-Target Net Delivered Impressions'].str.replace(',', '').fillna(0).astype('int64')
    fw_demo_daily['Gross Counted Ads (Demo)'] = fw_demo_daily['Gross Counted Ads (Demo)'].str.replace(',', '').fillna(0).astype('int64')

    # Only use monthly data if requested
    if monthly:
        report_date = datetime.combine(
            report_date.replace(day=1).date(),
            datetime.min.time(),
        )
        fw_demo = fw_demo[fw_demo['Event Month'] == report_date]
        fw_demo_monthly_agg = fw_demo.groupby(fw_demo.index).agg('sum', numeric_only=True)
        return fw_demo_monthly_agg

    fw_demo = fw_demo[fw_demo['Event Month'] < demo_comp_month_start]
    fw_demo_monthly_agg = fw_demo.groupby(fw_demo.index).agg('sum', numeric_only=True)

    fw_demo_daily_current = fw_demo_daily[fw_demo_daily['Event Date'] >= demo_comp_month_start]
    fw_demo_daily_current = fw_demo_daily_current[fw_demo_daily_current['Event Date'] <= demo_comp_date]
    fw_demo_daily_agg = fw_demo_daily_current.groupby(fw_demo_daily_current.index).agg('sum', numeric_only=True)

    fw_demo_agg = fw_demo_monthly_agg.combine(
        fw_demo_daily_agg,
        lambda ps_to_month, month_to_date: ps_to_month + month_to_date,
        fill_value=0,
    ).astype(
        {
            'On-Target Net Delivered Impressions': 'int64',
            'Gross Counted Ads (Demo)': 'int64',
        },
    )
    return fw_demo_agg
  
def read_fw_analytics_lifetime(fw_analytics_filename: str, report_date: datetime, skiprows: int = 0) -> pd.DataFrame:
    fw_analytics = pd.read_csv(
        fw_analytics_filename,
        skiprows=skiprows,
        usecols={
            'Ad Unit ID',
            'Placement ID',
            'Campaign ID',
            'Campaign Name',
            'Series Name',
            'Budget Model',
            'Net Counted Ads',
            'Gross Counted Ads',
            'Booked On-Target Impression Goal',
            'Impression Goal',
            'FFDR (%)',
            'Forced Over Delivery Percent (%)',
            'Creative Duration',
            'Ad Unit Price',
        },
        dtype={
            'Ad Unit ID': 'int64',
            'Placement ID': 'int64',
            'Campaign ID': 'int64',
            'Campaign Name': 'object',
            'Series Name': 'object',
            'Budget Model': 'object',
            'Net Counted Ads': 'object',
            'Gross Counted Ads': 'object',
            'Booked On-Target Impression Goal': 'object',
            'Impression Goal': 'object',
            'FFDR (%)': 'float64',
            'Forced Over Delivery Percent (%)': 'object',
            'Creative Duration': 'object',
            'Ad Unit Price': 'object',
        },
        na_values={
            'Forced Over Delivery Percent (%)': [
                'Network Default',
            ],
        },
        encoding='utf-8',
        storage_options={'profile': aws_profile}
    )

    int_cols = ['Net Counted Ads', 'Gross Counted Ads', 'Booked On-Target Impression Goal']
    float_cols = ['Creative Duration', 'Impression Goal', 'Ad Unit Price']

    for col in int_cols:
        fw_analytics[col] = fw_analytics[col].str.replace(',', '').fillna(0).astype('int64')
    for col in float_cols:
        fw_analytics[col] = fw_analytics[col].str.replace(',', '').fillna(0).astype('float64')

    return fw_analytics

def read_fw_analytics_monthly(fw_analytics_filename: str, report_date: datetime, skiprows: int = 0) -> pd.DataFrame:
    fw_analytics = pd.read_csv(
        fw_analytics_filename,
        skiprows=skiprows,
        usecols={
            'Ad Unit ID',
            'Placement ID',
            'Campaign ID',
            'Campaign Name',
            'Series Name',
            'Budget Model',
            'Net Counted Ads',
            'Gross Counted Ads',
            'Booked On-Target Impression Goal',
            'Impression Goal',
            'FFDR (%)',
            'Forced Over Delivery Percent (%)',
            'Creative Duration',
            'Ad Unit Price',
            'Event Month',
        },
        dtype={
            'Ad Unit ID': 'int64',
            'Placement ID': 'int64',
            'Campaign ID': 'int64',
            'Campaign Name': 'object',
            'Series Name': 'object',
            'Budget Model': 'object',
            'Net Counted Ads': 'object',
            'Gross Counted Ads': 'object',
            'Booked On-Target Impression Goal': 'object',
            'Impression Goal': 'object',
            'FFDR (%)': 'float64',
            'Forced Over Delivery Percent (%)': 'object',
            'Creative Duration': 'object',
            'Ad Unit Price': 'object',
        },
        na_values={
            'Forced Over Delivery Percent (%)': [
                'Network Default',
            ],
        },
        parse_dates=[
            'Event Month',
        ],
        encoding='utf-8',
        storage_options={'profile': aws_profile}
    )

    int_cols = ['Net Counted Ads', 'Gross Counted Ads', 'Booked On-Target Impression Goal']
    float_cols = ['Creative Duration', 'Impression Goal', 'Ad Unit Price']

    for col in int_cols:
        fw_analytics[col] = fw_analytics[col].str.replace(',', '').fillna(0).astype('int64')
    for col in float_cols:
        fw_analytics[col] = fw_analytics[col].str.replace(',', '').fillna(0).astype('float64')

    current_quarter = pd.Series(report_date).dt.quarter[0]

    fw_analytics.loc[:, 'In Current Quarter'] = (
        fw_analytics['Event Month'].dt.quarter == current_quarter
    ) & (
        fw_analytics['Event Month'].dt.year == report_date.year
    )
    fw_analytics.loc[:, 'In Current Month'] = (
        fw_analytics['Event Month'].dt.month == report_date.month
    ) & (
        fw_analytics['Event Month'].dt.year == report_date.year
    )

    return fw_analytics

# COMMAND ----------

def merge_vpvh(fw_analytics: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    s3_dir = f's3://{drop_bucket}/'
    vpvh_filename = 'VPVHS - MASTER.csv'
    vpvh_file = s3_dir + 'lookup/' + vpvh_filename
    series_lookup_filename = 'Series Lookup.csv'
    series_lookup_file = s3_dir + 'lookup/' + series_lookup_filename

    vpvh_master = pd.read_csv(
        vpvh_file,
        parse_dates=['Interval Start'],
        thousands=',',
        na_values=[0],
        storage_options={'profile': aws_profile},
    )

    vpvh_master = vpvh_master[vpvh_master['Interval Start'] <= report_date]

    series_lookup = pd.read_csv(
        series_lookup_file,
        index_col='FW Series Name',
        storage_options={'profile': aws_profile},
    )

    demo_to_band = {
        'Females 12 - 34': 'W12-34',
        'Females 12 - 17': 'W12-17',
        'Females 12 - 24': 'W12-24',
        'Females 18 - 24': 'W18-24',
        'Females 18 - 34': 'W18-34',
        'Females 18 - 49': 'W18-49',
        'Females 18 - 99': 'W18+',
        'Females 21 - 34': 'W21-34',
        'Females 21 - 49': 'W21-49',
        'Females 25 - 49': 'W25-49',
        'Females 25 - 54': 'W25-54',
        'Females 35 - 54': 'W35-54',
        'Females 35 - 64': 'W35-64',
        'Males 12 - 17': 'M12-17',
        'Males 12 - 24': 'M12-24',
        'Males 12 - 34': 'M12-34',
        'Males 18 - 24': 'M18-24',
        'Males 18 - 34': 'M18-34',
        'Males 18 - 49': 'M18-49',
        'Males 21 - 34': 'M21-34',
        'Males 21 - 49': 'M21-49',
        'Males 21 - 99': 'M21+',
        'Males 25 - 34': 'M25-34',
        'Males 25 - 49': 'M25-49',
        'Males 25 - 54': 'M25-54',
        'Males 25 - 99': 'M25+',
        'Males 35 - 54': 'M35-54',
        'Males 35 - 64': 'M35-64',
        'Persons 12 - 17': 'P12-17',
        'Persons 12 - 24': 'P12-24',
        'Persons 12 - 34': 'P12-34',
        'Persons 13 - 49': 'P13-49',
        'Persons 18 - 24': 'P18-24',
        'Persons 18 - 34': 'P18-34',
        'Persons 18 - 49': 'P18-49',
        'Persons 18 - 99': 'P18+',
        'Persons 2 - 99': 'P2+',
        'Persons 21 - 34': 'P21-34',
        'Persons 21 - 49': 'P21-49',
        'Persons 21 - 99': 'P21+',
        'Persons 25 - 34': 'P25-34',
        'Persons 25 - 49': 'P25-49',
        'Persons 25 - 54': 'P25-54',
        'Persons 25 - 64': 'P25-64',
        'Persons 25 - 99': 'P25+',
        'Persons 30 - 99': 'P30+',
        'Persons 35 - 54': 'P35-54',
        'Persons 35 - 64': 'P35-64',
        'Persons 50 - 99': 'P50+',
        'Persons 35 - 99': 'P35+',
        'Persons 13 - 34': 'P13-34',
        'Persons 21 - 54': 'P21-54',
    }

    vpvh_master.rename(
        demo_to_band,
        axis=1,
        inplace=True,
    )

    vpvh_master = vpvh_master[vpvh_master['P2+'] >= 1000]
    series_fw_to_nielsen = series_lookup.to_dict()['Nielsen Series Name']
    fw_analytics.loc[:, 'Nielsen Series Name'] = fw_analytics['Series Name'].map(series_fw_to_nielsen)
    band_to_vpvh_mean = vpvh_master.mean(axis=0).fillna(1000).astype('int64').to_dict()
    vpvh_cols = set(vpvh_master.columns).intersection(set(demo_to_band.values()))
    vpvh_master = vpvh_master.sort_values('Interval Start', ascending=False).groupby('Program Name').agg('first')

    fw_analytics_vpvh = pd.merge(
        fw_analytics,
        vpvh_master,
        how='left',
        left_on=['Nielsen Series Name'],
        right_on=['Program Name'],
    )

    fw_analytics_vpvh.set_index('Ad Unit ID', inplace=True)

    vpvh_agg_dict = {}
    for band in vpvh_cols:
        vpvh_agg_dict[band] = 'mean'
        fw_analytics_vpvh[band] = fw_analytics_vpvh[band].fillna(band_to_vpvh_mean[band])
        fw_analytics_vpvh[f'weighted_{band}'] = fw_analytics_vpvh['Net Counted Ads'] * fw_analytics_vpvh[band]
        vpvh_agg_dict[f'weighted_{band}'] = 'sum'

    return (fw_analytics_vpvh, vpvh_agg_dict, band_to_vpvh_mean)

# COMMAND ----------

def read_adjuster(adjuster_file: str, skiprows: int = 6) -> pd.DataFrame:
    ad_juster = pd.read_csv(
        adjuster_file,
        skiprows=skiprows,
        usecols=[
            'Ad Unit Id',
            '3rd Party Server',
            'Impressions (3rd Party)',
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions',
            '3rd Party Valid Impressions',
            'Impressions Analyzed',
            '3rd Party Valid, Audible and Fully On-Screen'
            ' for Half of the Duration Impressions (15 sec)',
            '3rd Party Valid and Viewable Impressions',
            'Report End Date',
        ],
        dtype={
            'Ad Unit Id': 'float64',
            '3rd Party Server': 'object',
            'Impressions (3rd Party)': 'int64',
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions': 'int64',
            '3rd Party Valid Impressions': 'int64',
            'Impressions Analyzed': 'int64',
            '3rd Party Valid, Audible and Fully On-Screen'
            ' for Half of the Duration Impressions (15 sec)': 'int64',
            '3rd Party Valid and Viewable Impressions': 'int64',
        },
        parse_dates=[
            'Report End Date',
        ],
        index_col=False,
        encoding='utf-8',
        storage_options={'profile': aws_profile}
    ).dropna(subset=['Ad Unit Id']).astype({'Ad Unit Id': 'int64'})

    ad_juster.set_index('Ad Unit Id', inplace=True)

    return ad_juster

def extract_sales_line_item_id(row: pd.Series, col: str) -> str:
    pattern = '^\d+(?=-1_)'
    matches = re.findall(pattern, str(row[col]))
    if len(matches)>0:
        return matches[0]
    else:
        return np.nan

def read_adjuster_news(adjuster_news_file: str, op_oms: pd.DataFrame, skiprows: int = 0) -> pd.DataFrame:
    adjuster_news = pd.read_csv(
        adjuster_news_file,
        skiprows=skiprows,
        usecols=[
            #'Primary Trafficker',
            'Advertiser',
            #'Order Name',
            #'Order ID',
            'Campaign Name',
            'Campaign Identifier',
            '3rd Party Server',
            '3rd Party Creative Name',
            '3rd Party Creative Id',
            'Impressions',
            'Impressions (3rd Party)',
            'Clicks',
            'Clicks (3rd Party)',
            #'Report Start Date',
            #'Report End Date',
            #'Report Start Date (3rd Party)',
            #'Report End Date (3rd Party)',
            'Campaign Start',
            'Campaign End',
        ],
        dtype={
            #'Primary Trafficker': 'str',
            'Advertiser': 'str',
            #'Order Name': 'str',
            #'Order ID': 'str',
            'Campaign Name': 'str',
            'Campaign Identifier': 'str',
            '3rd Party Server': 'str',
            '3rd Party Creative Name': 'str',
            '3rd Party Creative Id': 'str',
        },
        parse_dates=[
            #'Report Start Date',
            #'Report End Date',
            #'Report Start Date (3rd Party)',
            #'Report End Date (3rd Party)',
            'Campaign Start',
            'Campaign End',
        ],
        index_col=False,
        encoding='utf-8',
        storage_options={'profile': aws_profile}
        )
    
    int_cols = ['Impressions', 'Impressions (3rd Party)', 'Clicks', 'Clicks (3rd Party)']
    for col in int_cols:
        adjuster_news[col] = adjuster_news[col].fillna(0).astype('int64')

    adjuster_news['Campaign Identifier'] = pd.to_numeric(adjuster_news['Campaign Identifier'], errors='coerce')
    op_oms['Placement ID'] = pd.to_numeric(op_oms['Placement ID'], errors='coerce')

    adjuster_news.loc[:, 'Sales Line Item ID'] = adjuster_news.apply(extract_sales_line_item_id, args=('Campaign Name',), axis=1)
    adjuster_news = pd.merge(adjuster_news, op_oms.reset_index()[['Placement ID', 'Sales Line Item ID']], left_on='Campaign Identifier', right_on='Placement ID', how='left', suffixes=('_x', '_y'))
    adjuster_news['Sales Line Item ID'] = adjuster_news['Sales Line Item ID_x'].combine_first(adjuster_news['Sales Line Item ID_y'])
    adjuster_news = adjuster_news.drop(['Placement ID', 'Sales Line Item ID_x', 'Sales Line Item ID_y'], axis=1)

    adjuster_news = adjuster_news.fillna({
        '3rd Party Server': '',
    })

    adjuster_news = adjuster_news.dropna(subset=['Sales Line Item ID'])
    adjuster_news['Sales Line Item ID'] = adjuster_news['Sales Line Item ID'].astype('int64')
    
    return adjuster_news

def read_staq(staq_file: str, skiprows: int = 0) -> pd.DataFrame:
    staq = pd.read_csv(
        staq_file,
        skiprows=skiprows,
        usecols=[
            'Ad Unit ID',
            '3rd Party Server',
            'Impressions (3rd Party)',
        ],
        dtype={
            'Ad Unit ID': 'float64',
            '3rd Party Server': 'object',
            'Impressions (3rd Party)': 'object',
        },
        index_col=False,
        encoding='utf-8',
        storage_options={'profile': aws_profile}
    ).dropna(subset=['Ad Unit ID']).astype({'Ad Unit ID': 'int64'})

    staq['Impressions (3rd Party)'] = staq['Impressions (3rd Party)'].fillna(0).astype('int64')

    missing_staq_columns = [
        '3rd Party Audible and Fully On-Screen'
        ' for Half of Duration Impressions',
        '3rd Party Valid Impressions',
        'Impressions Analyzed',
        '3rd Party Valid, Audible and Fully On-Screen'
        ' for Half of the Duration Impressions (15 sec)',
        '3rd Party Valid and Viewable Impressions',
        'Report End Date',
    ]

    for col in missing_staq_columns:
        staq[col] = None
    
    staq = staq.rename(columns={'Ad Unit ID': 'Ad Unit Id'})

    staq.set_index('Ad Unit Id', inplace=True)

    return staq

# COMMAND ----------

def read_gam_lifetime(gam_file: str, op_oms: pd.DataFrame) -> pd.DataFrame:
    gam = pd.read_parquet(
        gam_file,
        columns=[
            'Date',
            'Advertiser Name',
            'Order Name',
            'Line Item ID',
            'Line Item Name',
            'Line Item Start Date',
            'Line Item End Date',
            'Rate',
            'Goal Quantity',
            'Contracted Quantity',
            'Delivery Indicator',
            'Salesperson',
            'Total Impressions',
            'Ad Server Impressions',
            'Ad Server Clicks',
            'Total Error Count'
        ],
        storage_options={'profile': aws_profile}
    )

    gam['Goal Quantity'] = gam.groupby('Line Item ID')['Goal Quantity'].transform(lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan)
    gam['Contracted Quantity'] = gam.groupby('Line Item ID')['Contracted Quantity'].transform(lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan)
    gam = gam.drop(columns=['Date'])

    gam.loc[:, 'Rate'] = gam['Rate']/1000000
    gam.loc[:, 'Total Error Count'] = gam['Total Error Count'].str.replace('-', '0', regex=False).astype('int64')
    gam.loc[:, 'Contracted Quantity'] = gam['Contracted Quantity'].str.replace('-', '0', regex=False).astype('int64')
    gam.loc[:, 'Goal Quantity'] = gam['Goal Quantity'].astype('str')
    gam.loc[:, 'Sales Line Item ID'] = gam.apply(extract_sales_line_item_id, args=('Line Item Name',), axis=1)

    gam['Line Item ID'] = pd.to_numeric(gam['Line Item ID'], errors='coerce')
    op_oms['Placement ID'] = pd.to_numeric(op_oms['Placement ID'], errors='coerce')

    gam.loc[:, 'Sales Line Item ID'] = gam.apply(extract_sales_line_item_id, args=('Line Item Name',), axis=1)
    gam = pd.merge(gam, op_oms.reset_index()[['Placement ID', 'Sales Line Item ID']], left_on='Line Item ID', right_on='Placement ID', how='left', suffixes=('_x', '_y'))
    gam['Sales Line Item ID'] = gam['Sales Line Item ID_x'].combine_first(gam['Sales Line Item ID_y'])
    gam = gam.drop(['Line Item ID', 'Placement ID', 'Sales Line Item ID_x', 'Sales Line Item ID_y'], axis=1)

    gam = gam.dropna(subset=['Sales Line Item ID'])
    gam['Sales Line Item ID'] = gam['Sales Line Item ID'].astype('int64')

    gam = gam.rename(
        {
            'Advertiser Name': 'GAM Advertiser Name',
            'Order Name': 'GAM Order Name',
            'Salesperson': 'GAM Salesperson'
        },
        axis=1
    )

    return gam

# COMMAND ----------

def min(x):
    minimum = x[0]
    for i in x:
        if i < minimum:
            minimum = i
    return minimum

def round(row: pd.Series, col: str, digits: int = 0) -> Any:
    factor = 10 ** digits
    try:
        return int(row[col] * factor + 0.5 if row[col] >= 0 else row[col] * factor - 0.5) / factor
    except:
        return 0

# COMMAND ----------

def billable_ad_unit(fw_analytics: pd.DataFrame, op_oms: pd.DataFrame) -> pd.DataFrame:
    billable_3p = pd.merge(
        fw_analytics[['Placement ID', 'Ad Unit ID']],
        op_oms[['Billable Third Party Server']],
        left_on='Placement ID',
        right_index=True,
        how='left',
    ).drop_duplicates()

    ad_unit_to_3p = billable_3p.set_index(
        'Ad Unit ID',
    ).to_dict()['Billable Third Party Server']
    return ad_unit_to_3p


def billable_campaign(gam_3p: pd.DataFrame, op_oms: pd.DataFrame) -> pd.DataFrame:
    billable_3p = pd.merge(
        gam_3p[['Sales Line Item ID', 'Campaign Name']],
        op_oms[['Sales Line Item ID', 'Billable Third Party Server']],
        left_on='Sales Line Item ID',
        right_on='Sales Line Item ID',
        how='left',
    ).drop_duplicates()

    campaign_to_3p = billable_3p.set_index(
        'Campaign Name',
    ).to_dict()['Billable Third Party Server']
    return campaign_to_3p


def billable_adj_server(row: pd.Series, col_to_3p: Dict[int, str], col: str) -> Any:
    if row[col] in col_to_3p:
        op_server = col_to_3p[row[col]]
    else:
        return math.nan
    op_server_to_adj = {
        'Moat': ['Moat'],
        'DCM': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
            'DCM',
        ],
        'DFA': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
        ],
        'DCM - DAR (Nielsen)': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
            'DCM',
        ],
        'Innovid': ['Innovid 3rd Party', 'Innovid',],
        'Flashtalking': ['FlashTalking', 'FlashTalking Email Reader', 'Flashtalking',],
        'Extreme Reach': [
            'ExtremeReachAPI',
            'Extreme Reach Email',
            'ExtremeReachAPI, Extreme Reach Email',
            'Extreme Reach Email, ExtremeReachAPI',
        ],
        'Sizmek': ['MediaMind', 'MediaMind Report Reader'],
        'TubeMogul': ['TubeMogul'],
        'FOX DCM': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
            'DCM',
            'FOX DCM',
        ],
        'FOX DFA': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
            'FOX DFA',
        ],
        'FOX DCM - DAR (Nielsen)': [
            'DFA by Google',
            'Dart Report Reader',
            'Dart Report Reader, DFA by Google',
            'DCM',
            'FOX DCM - DAR (Nielsen)',
        ],
        'FOX Innovid': [
            'Innovid 3rd Party',
            'Innovid',
            'FOX Innovid',
        ],
        'Tubi Innovid': [
            'Innovid 3rd Party',
            'Tubi Innovid',
        ],
        'FOX Flashtalking': [
            'FlashTalking', 'FlashTalking Email Reader', 'Flashtalking', 'FOX Flashtalking',
        ],
        'Tubi Flashtalking': [
            'FlashTalking', 'FlashTalking Email Reader', 'Tubi Flashtalking',
        ],
        'FOX Extreme Reach': [
            'ExtremeReachAPI',
            'Extreme Reach Email',
            'ExtremeReachAPI, Extreme Reach Email',
            'Extreme Reach Email, ExtremeReachAPI',
            'FOX Extreme Reach',
        ],
        'Tubi Extreme Reach': [
            'ExtremeReachAPI',
            'Extreme Reach Email',
            'ExtremeReachAPI, Extreme Reach Email',
            'Extreme Reach Email, ExtremeReachAPI',
            'Tubi Extreme Reach',
        ],
        'FOX Sizmek': [
            'MediaMind', 'MediaMind Report Reader', 'FOX Sizmek',
        ],
    }
    if op_server in op_server_to_adj:
        adj_servers = op_server_to_adj[op_server]
        for adj_server in adj_servers:
            if row['3rd Party Server'] == adj_server:
                return adj_server
    return math.nan


def merge_delivery_data(
    op_oms: pd.DataFrame,
    fw_analytics: pd.DataFrame,
    ad_juster: pd.DataFrame,
    fw_analytics_vpvh: pd.DataFrame,
    vpvh_agg_dict: Dict[str, Any],
) -> pd.DataFrame:
    fw_ad_unit_agg = {
        'Placement ID': 'max',
        'FFDR (%)': 'max',
        'Forced Over Delivery Percent (%)': 'max',
        'Campaign ID': 'max',
        'Campaign Name': 'max',
        'Series Name': 'max',
        'Budget Model': 'max',
        'Creative Duration': 'max',
        'Impression Goal': 'max',
        'Net Counted Ads': 'sum',
        'Gross Counted Ads': 'sum',
        'Booked On-Target Impression Goal': 'max',
        'Ad Unit Price': 'max',
    }

    fw_ad_unit_agg.update(vpvh_agg_dict)
    fw_analytics_agg = fw_analytics_vpvh.groupby(fw_analytics_vpvh.index).aggregate(fw_ad_unit_agg)

    # MOAT data is used to calculate Viewability
    ad_juster_moat = ad_juster.loc[ad_juster['3rd Party Server'] == 'Moat', :]
    ad_juster_moat_agg = ad_juster_moat.groupby(level=0).aggregate(
        {
            '3rd Party Valid, Audible and Fully On-Screen'
            ' for Half of the Duration Impressions (15 sec)': 'sum',
            'Impressions Analyzed': 'sum',
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions': 'sum',
            '3rd Party Valid Impressions': 'sum',
            '3rd Party Valid and Viewable Impressions': 'sum',
        },
    )

    ad_juster_all = ad_juster[[
        '3rd Party Server',
        'Impressions (3rd Party)',
    ]].reset_index().set_index(['Ad Unit Id', '3rd Party Server'])

    ad_juster_all_agg = ad_juster_all.dropna().groupby(level=[0, 1]).aggregate(
        {
            'Impressions (3rd Party)': 'sum',
        },
    ).reset_index()

    ad_unit_to_3p = billable_ad_unit(fw_analytics, op_oms)

    ad_juster_all_agg.loc[
        :,
        'Billable 3rd Party Server',
    ] = ad_juster_all_agg.apply(
        lambda row: billable_adj_server(row, ad_unit_to_3p, 'Ad Unit Id'),
        axis=1,
    )
    ad_juster_all_agg = ad_juster_all_agg.dropna().set_index('Ad Unit Id')

    ad_juster_joined = pd.merge(
        ad_juster_all_agg,
        ad_juster_moat_agg,
        left_index=True,
        right_index=True,
        how='outer',
    )

    ad_juster_joined_agg_dict = {
        'Impressions (3rd Party)': 'sum',
        '3rd Party Valid, Audible and Fully On-Screen for Half of the Duration Impressions (15 sec)': 'sum',
        'Impressions Analyzed': 'sum',
        '3rd Party Audible and Fully On-Screen for Half of Duration Impressions': 'sum',
        '3rd Party Valid Impressions': 'sum',
        '3rd Party Valid and Viewable Impressions': 'sum',
    }

    ad_juster_joined = ad_juster_joined.groupby(ad_juster_joined.index).aggregate(ad_juster_joined_agg_dict)

    fw_adj = fw_analytics_agg.join(ad_juster_joined)

    fw_adj = fw_adj.fillna(
        {
            'Impressions (3rd Party)': fw_adj['Net Counted Ads'],
        },
    )

    fw_adj_placement = fw_adj.reset_index().set_index('Placement ID')

    fw_placement_agg = {
        'FFDR (%)': 'max',
        'Forced Over Delivery Percent (%)': 'max',
        'Campaign ID': 'max',
        'Campaign Name': 'max',
        'Series Name': 'max',
        'Budget Model': 'max',
        'Impression Goal': 'max',
        'Booked On-Target Impression Goal': 'max',
        'Creative Duration': 'max',
        'Ad Unit Price': 'max',
        'Net Counted Ads': 'sum',
        'Gross Counted Ads': 'sum',
        'Impressions (3rd Party)': 'sum',
        '3rd Party Valid, Audible and Fully On-Screen'
        ' for Half of the Duration Impressions (15 sec)': 'sum',
        'Impressions Analyzed': 'sum',
        '3rd Party Audible and Fully On-Screen'
        ' for Half of Duration Impressions': 'sum',
        '3rd Party Valid Impressions': 'sum',
        '3rd Party Valid and Viewable Impressions': 'sum',
    }

    fw_placement_agg.update(vpvh_agg_dict)
    fw_adj_agg = fw_adj_placement.groupby(fw_adj_placement.index).aggregate(fw_placement_agg)

    for band in vpvh_agg_dict.keys():
        if not band.startswith('weighted_'):
            fw_adj_agg[band] = fw_adj_agg[f'weighted_{band}'] / fw_adj_agg['Net Counted Ads']

    return fw_adj_agg

def merge_news_delivery_data(
    op_oms: pd.DataFrame,
    gam: pd.DataFrame,
    adj: pd.DataFrame,
    op_fw_adj: pd.DataFrame
) -> pd.DataFrame:
    gam_line_item_agg = {
        'GAM Advertiser Name': 'max',
        'GAM Order Name': 'max',
        'Line Item Name': 'max',
        'Line Item Start Date': 'max',
        'Line Item End Date': 'max',
        'Rate': 'max',
        'Goal Quantity': 'max',
        'Contracted Quantity': 'max',
        'Delivery Indicator': 'max',
        'GAM Salesperson': 'max',
        'Total Impressions': 'sum',
        'Ad Server Impressions': 'sum',
        'Ad Server Clicks': 'sum',
        'Total Error Count': 'sum'
    }

    gam = gam[[
        'Sales Line Item ID',
        'GAM Advertiser Name',
        'GAM Order Name',
        'Line Item Name',
        'Line Item Start Date',
        'Line Item End Date',
        'Rate',
        'Goal Quantity',
        'Contracted Quantity',
        'Delivery Indicator',
        'GAM Salesperson',
        'Total Impressions',
        'Ad Server Impressions',
        'Ad Server Clicks',
        'Total Error Count'
    ]].reset_index(drop=True).set_index('Sales Line Item ID')

    gam = gam.groupby(level=0).aggregate(gam_line_item_agg)

    gam = gam.rename(
        {
            'GAM Advertiser Name': 'Advertiser',
            'GAM Order Name': 'Order',
            'GAM Salesperson': 'Salesperson',
        },
        axis=1
    )

    ad_juster_agg = {
        '3rd Party Server': 'max',
        'Campaign Start': 'max',
        'Campaign End': 'max',
        'Impressions': 'sum',
        'Impressions (3rd Party)': 'sum',
        'Clicks': 'sum',
        'Clicks (3rd Party)': 'sum'
    }
    
    adj = adj[[
        'Sales Line Item ID',
        '3rd Party Server',
        'Campaign Start',
        'Campaign End',
        'Impressions',
        'Impressions (3rd Party)',
        'Clicks',
        'Clicks (3rd Party)'
    ]].reset_index(drop=True).set_index('Sales Line Item ID')

    adj = adj.groupby(level=0).aggregate(ad_juster_agg)

    fw_agg = {
        'Advertiser Name': 'max',
        #'Sales Order Name': 'max',
        'Campaign Name': 'max',
        'Sales Line Item Name': 'max',
        'Sales Line Item Start Date': 'max',
        'Sales Line Item End Date': 'max',
        'Net Unit Cost': 'max',
        'Quantity': 'max',
        'Push Quantity': 'max',
        'Primary Salesperson Full Name': 'max',
        'Gross Counted Ads': 'sum',
        'Net Counted Ads': 'sum',
    }

    # Filter only news orders
    news_business_orgs = ['FOX News & Business', 'FOX News & Business Programmatic', 'FOX News & Business Programmatic - GAM', 'Outkick', 'TMZ and TooFab']
    op_fw_adj = op_fw_adj[op_fw_adj['Invoice Organization Name'].isin(news_business_orgs)]

    fw = op_fw_adj[[
        'Sales Line Item ID',
        'Advertiser Name',
        #'Sales Order Name',
        'Campaign Name',
        'Sales Line Item Name',
        'Sales Line Item Start Date',
        'Sales Line Item End Date',
        'Net Unit Cost',
        'Quantity', 
        'Push Quantity',
        'Primary Salesperson Full Name',
        'Gross Counted Ads',
        'Net Counted Ads',
    ]].reset_index(drop=True).set_index('Sales Line Item ID')

    fw = fw.groupby(level=0).aggregate(fw_agg)

    fw = fw.rename(
        {
            'Advertiser Name': 'Advertiser',
            'Campaign Name': 'Order',
            'Sales Line Item Name': 'Line Item Name',
            'Sales Line Item Start Date': 'Line Item Start Date',
            'Sales Line Item End Date': 'Line Item End Date',
            'Net Unit Cost': 'Rate',
            'Quantity': 'Goal Quantity',
            'Push Quantity': 'Contracted Quantity',
            'Primary Salesperson Full Name': 'Salesperson',
            'Gross Counted Ads': 'Total Impressions',
            'Net Counted Ads': 'Ad Server Impressions',
        },
        axis=1
    )

    # Filter out lines not served in freewheel
    fw = fw[fw['Ad Server Impressions'] != 0]
    fw.loc[:, 'Goal Quantity'] = fw['Goal Quantity'].astype('str')

    gam_fw = pd.concat([gam, fw])
    gam_fw_adj = pd.merge(gam_fw, adj, on=['Sales Line Item ID'], how='outer')
    gam_fw_adj['Line Item Start Date'] = gam_fw_adj['Line Item Start Date'].combine_first(gam_fw_adj['Campaign Start'])
    gam_fw_adj['Line Item End Date'] = gam_fw_adj['Line Item End Date'].combine_first(gam_fw_adj['Campaign End'])
    gam_fw_adj['Ad Server Impressions'] = gam_fw_adj['Impressions'].combine_first(gam_fw_adj['Ad Server Impressions'])

    op_gam_fw_adj = pd.merge(op_oms, gam_fw_adj, on=['Sales Line Item ID'], how='outer').reset_index(drop=True).set_index('Placement ID')

    op_gam_fw_adj = op_gam_fw_adj.fillna(
        {
            'Contracted Quantity': 0,
            'Total Impressions': 0,
            'Ad Server Clicks': 0,
            'Total Error Count': 0,
            'Impressions (3rd Party)': gam_fw_adj['Impressions'],
            'Clicks (3rd Party)': gam_fw_adj['Clicks'],
        }
    )

    return op_gam_fw_adj

def sales_line_type(row: pd.Series) -> str:
    if row['Makegood']:
        return 'Make Good'
    if row['Added Value']:
        return 'Added Value'
    return 'Guaranteed'


def billing_scenario(row: pd.Series) -> str:
    if not isinstance(row['Net Invoice Amount Terms'], str):
        return 'Bill on Actuals - 3rd Party Performance'
    if row['Is Bill on Contract']:
        return 'Bill on Contract - ' + row['Net Invoice Amount Terms']
    return 'Bill on Actuals - ' + row['Net Invoice Amount Terms']


def demo_source(row: pd.Series) -> Union[float, str]:
    if isinstance(row['FW - Target Demographics - Nielsen DAR'], str):
        return 'Nielsen DAR'
    if isinstance(row['FW - Target Demographics - comScore vCE'], str):
        return 'comScore vCE'
    return math.nan


def property_deal(row: pd.Series) -> Union[float, str]:
    if not isinstance(row['Invoice Organization Name'], str):
        return math.nan
    if row['Placement Property'] == 'BTN - Digital':
        return 'BTN - Digital'
    elif row['Invoice Organization Name'] == 'FOX Sports & Entertainment':
        deal_type = 'Cash'
    elif row['Invoice Organization Name'] == 'FOX Sports & Entertainment Fluidity':
        deal_type = 'Fluidity'
    else:
        return math.nan
    sports_products = {
        'LFV - FOX - Pre/Midroll - WWE VOD P2+',
        'LFV - FOX - Pre/Midroll - WWE VOD P2+ Preemptible',
        'LFV - FOX - Pre/Midroll - WWE VOD Demo',
        'LFV - FOX - Pre/Midroll - WWE VOD Demo Preemptible',
    }
    if row['Product Name'] in sports_products:
        return 'Sports' + ' ' + deal_type
    prop_type_rollup = {
        'FOXNow': 'Ent',
        'FOX - Twitter': 'Ent',
        'FOX - Facebook/Instagram': 'Ent',
        'FOX DAI VOD': 'Ent',
        'FOX on Hulu': 'Ent',
        'FOX on Tubi': 'Ent',
        'FOX Sports Streaming': 'Sports',
        'FOX SPORTS DAI VOD': 'Sports',
        'FOX Sports - Facebook/Instagram': 'Sports',
        'FOX Sports - Twitter': 'Sports',
        'FOX Sports - TikTok': 'Sports',
        'FOX Sports - Custom Sponsorship': 'Sports',
        'FOX Sports Clips': 'Sports',
        'Deportes': 'Sports',
        'Deportes Streaming': 'Sports',
        'Deportes Clips': 'Sports',
        'Deportes Display': 'Sports',
        'Deportes - Twitter': 'Sports',
        'Deportes - Facebook/Instagram': 'Sports',
        'SendToNews': 'Sports',
        'FOX News': 'News',
        'FOX Business': 'News',
        'FOX News - Digital': 'News',
        'FOX Business - Digital': 'News',
        'FOX Sports on Tubi': 'Sports',
        'Tubi': 'Tubi',
        'Minute Media': 'Sports',
        'Transmit': 'Sports',
        'RON - Ent': 'Ent',
        'FOXNews.com': 'News',
        'FOXBusiness.com': 'News',
        'Fox Sold Tubi': 'Tubi',
        'TMZ - Digital': 'News',
        'Outkick': 'News',
        'FoxWeather.com': 'News',
        'Deportes - Digital': 'Sports',
        'YouTube - Ent': 'Ent',
        'Caffeine TV': 'Ent',
        'Fox Sports - Production': 'Sports',
        'FOX Nation': 'News',
        'FOX Soul': 'Ent',
        'fox weather': 'News',
        'FOXBusiness.com - Facebook/Instagram': "News",
        'FOXBusiness.com - LinkedIn': 'News',
        'FOXNews.com - Facebook/Instagram': 'News',
        'FOXWeather.com - Facebook/Instagram': 'News',
        'fxw': 'News',
        'Outkick - Facebook/Instagram': 'News',
        'Outkick on Tubi': 'News',
        'RON - News': 'News',
        'TMZ': 'News',
        'TMZ - Facebook/Instagram': 'News',
        'TooFab': 'News',
        'YouTube - News': 'News',
        'FOXWeather.com - TikTok': 'News',
        'FSEN': 'Sports',
        'FOX Sports': 'Sports'
    }
    if row['Placement Property'] not in prop_type_rollup:
        return 'Placement Property not mapped'
    if row['Invoice Organization Name'] == 'FOX Sports & Entertainment Fluidity':
        deal_type = 'Fluidity'
    elif row['Invoice Organization Name'] == 'FOX Sports & Entertainment':
        deal_type = 'Cash'
    else:
        print('Unknown invoicing organization', 'organization=', row['Invoice Organization Name'])
        deal_type = row['Invoice Organization Name']
    return prop_type_rollup[row['Placement Property']] + ' ' + deal_type


def identify_placement_types(df: pd.DataFrame, report_date: datetime, end_of_period_date: Optional[datetime] = None) -> pd.DataFrame:
    if end_of_period_date is None:
        end_of_period_date = report_date

    contract_terms = {
        'Manual Entry',
        'Straightline',
        'Pro-rated',
        'Single Period',
    }

    df.loc[:, 'Is Bill on Contract'] = df['Net Invoice Amount Terms'].isin(contract_terms)

    vod_props = {
        'FOX DAI VOD',
        'FOX SPORTS DAI VOD',
    }

    df.loc[:, 'Is VOD Placement'] = df['Placement Property'].isin(vod_props)
    df.loc[:, 'Is Demo Placement'] = (~df['Is VOD Placement']) & (df['Demo Band'] != 'P2+')
    df.loc[:, 'Is 3P'] = df['Billable Third Party Server'].fillna('NA') != '1st Party'

    df.loc[:, 'Is Future Start'] = (df['Sales Line Item Start Date'] >= report_date) | (df['Sales Line Item Start Date'] >= end_of_period_date)
    df.loc[:, 'Is Absolute A'] = df['Product Name'].str.contains('.*Absolute A.*')

    sab_products = {
        'LFV - FOH - trueX SAB Midroll - ROS DT P2+',
        'LFV - FOH - trueX SAB Midroll - ROS CTV P2+',
    }

    df.loc[:, 'Is SAB Engagement'] = df['Product Name'].isin(sab_products)
    df.loc[:, 'Sales Line Item Type'] = df.apply(sales_line_type, axis=1)
    df.loc[:, 'Billing Scenario'] = df.apply(billing_scenario, axis=1)
    df.loc[:, 'Property Deal Rollup'] = df.apply(property_deal, axis=1)
    df.loc[:, 'Demographic Data Source'] = df.apply(demo_source, axis=1)
    df_1p = df.fillna({'Net Counted Ads': 0}).astype({'Net Counted Ads': 'int64'})
    df_1p.loc[:, 'No Delivery'] = df_1p['Net Counted Ads'].fillna(0) == 0

    # New Logic for Non-Ad Served
    # Based on Non-Ad Served Production Systems and No Delivery
    non_ad_served_production_systems ={
        'Social Production System',
        'Beeswax Production System',
        'Programmatic Guaranteed Production System',
        'Open API Production System',
        'Social Production System, Beeswax Production System',
        'Beeswax Production System, Social Production System',
    }
    sponsor_products = {
        'Custom Sponsorship - FOX Sports - Placeholder',
        'Custom Sponsorship - FOX Entertainment - Placeholder',
        'EVT - FOX Sports - FSLS - Super Bowl',
        'LFV - FSLS - TNF - Non-Ad Served',
        'FEE - FOX Sports - Production',
        'AUD - FSLS - Live Host Reads - Sponsorship',
        'LFV - MLBtv - Mid - MLB - DT/MB - Non Ad-Served',
        'LFV - FSLS - Pre/Mid - Super Bowl - Non Ad-Served',
        'FEE - FOX Ent - Talent',
        'LFV - FSLS - Mid - ROS - Linear - Makegood - Non Ad-Served',
    }
    social_buy_types = {
        'Podcasts',
        'Social',
        'Social Facebook/Instagram',
        'Social LinkedIn',
        'Social TikTok',
        'Social TikTok Pulse',
        'Social X',
        'Social Snapchat'
    }

    df_1p.loc[:, 'Is Non-Ad Served'] = np.where(df_1p['Production System Name'].isin(non_ad_served_production_systems),
                                                np.where(df_1p['Is 3P'] == True,
                                                         np.where((df_1p['Impressions (3rd Party)'].isna()) | (df_1p['Impressions (3rd Party)'] == 0),
                                                                  True, False),
                                                         np.where(df_1p['No Delivery'] == True,
                                                                  True, False)),
                                                np.where(df_1p['Operative Product Type'].isin({'Package'}) | df_1p['Buy Type'].isin(social_buy_types) | df_1p['Product Name'].isin(sponsor_products),
                                                         True, False))
    
    df_1p.loc[:, 'Is Non-Ad Served'] = np.where(df_1p['Product Name'].str.startswith('SOC', na=False) | df_1p['Product Name'].str.startswith('DIS', na=False),
                                                True, df_1p['Is Non-Ad Served'])


    return df_1p


def billable_metric(row: pd.Series, implied: bool = False) -> str:
    if not implied:
        if row['Is SAB Engagement']:
            return 'SAB Engagement'
        if row['Is Absolute A']:
            return 'Absolute A'
        if row['Is Bill on Contract']:
            return 'Bill on Contract'

    should_equivalize = row['Equivalize?'] and (row['Creative Duration'] >= 30)
    is_cov = is_cov_row(row)

    # if row['FW - Deal Type'] is not None and 'programmatic guaranteed' in row['FW - Deal Type'].lower():
    #     return 'Programmatic Guaranteed Imps'
    # if row['Buy Type'] is not None and row['Buy Type'] == 'Programmatic BF':
    #     return 'Programmatic Reseller Imps'

    if row['Is VOD Placement']:
        if row['Equivalize?']:
            return 'Equivalized and VPVH Imps'
        return 'VPVH Imps'

    if row['Is Demo Placement']:
        if row['Is 3P']:
            if row['Is Viewability Placement']:
                if should_equivalize:
                    return '3P Equivalized Viewability and Demo Imps'
                else:
                    return '3P Viewability and Demo Imps'
            if should_equivalize:
                return '3P Equivalized Demo Imps'
            else:
                return '3P Demo Imps'
        if row['Is Viewability Placement']:
            return '1P Viewability and Demo Imps'
        if should_equivalize:
            return '1P Equivalized Demo Imps'
        else:
            return '1P Demo Imps'

    if row['Is 3P']:
        if row['Is SAB Engagement']:
            return '3P SAB Engagement Imps'
        if row['Is Viewability Placement'] and is_cov and implied:
            return '3P Viewability and Demo Imps'
        if row['Is Viewability Placement']:
            return '3P Viewability Imps'
        if should_equivalize:
            return '3P Equivalized Imps'
        if is_cov and implied:
            return '3P Demo Imps'
        return '3P Imps'

    if row['Is SAB Engagement']:
        return '1P SAB Engagement Imps'
    if row['Is Viewability Placement'] and is_cov and implied:
        return '1P Viewability and Demo Imps'
    if row['Is Viewability Placement']:
        return '1P Viewability Imps'

    if should_equivalize:
        return '1P Equivalized Imps'
    if is_cov and implied:
        return '1P Demo Imps'
    return '1P Imps'

def redefining_billable_metric(df: pd.DataFrame) -> pd.DataFrame:
    """
        if row['Is 3P']:
            if row['Is Viewability Placement'] and is_cov and implied:
                return '3P Viewability and CoView Imps'
            if is_cov and implied:
                return '3P CoView Imps'
        
        if row['Is Viewability Placement'] and is_cov and implied:
            return '1P Viewability and CoView Imps'

        if is_cov and implied:
            return '1P Demo Imps'
    """

    #get if a row is CoViewing
    df.loc[:,'Is CoView'] = df.apply(is_cov_row, axis=1)

    df['Billable Metric'] = np.where(df['Is Audience Target'] == True,
                                     np.where(df['Billable Third Party Server'] == '1st Party',
                                              '1P Audience Target', '3P Audience Target'),
                                     df['Billable Metric'])

    df['Implied Billable Metric'] = np.where(df['Is CoView'] == True,
                                             np.where(df['Is Viewability Placement'] == True,
                                                      np.where(df['Is 3P'] == True,
                                                               '3P Viewability and CoView Imps', '1P Viewability and CoView Imps'),
                                                      np.where(df['Is 3P'] == True,
                                                               '3P CoView Imps', '1P CoView Imps')),
                                             df['Implied Billable Metric'])
    
    return df

def identify_billable_metric(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Billable Metric'] = df.apply(
        billable_metric,
        axis=1,
    )

    df.loc[:, 'Implied Billable Metric'] = df.apply(
        lambda row: billable_metric(row, implied=True),
        axis=1,
    )
    return df


def vod_eq_factor(row: pd.Series) -> float:
    if row['Is VOD Placement'] or row['Equivalize?']:
        if not math.isnan(row['Creative Duration']):
            return cast(float, row['Creative Duration']) / 30
    return 0


def lookup_vpvh(row: pd.Series, band_to_vpvh_mean: Dict[str, float]) -> float:
    if row['Is VOD Placement']:
        demo_band = row['Demo Band']
        if demo_band in row.index:
            if math.isnan(row[demo_band]):
                return band_to_vpvh_mean[demo_band] / 1000
            return cast(float, row[demo_band]) / 1000
    return 0


def calculate_vpvh(df: pd.DataFrame, band_to_vpvh_mean: Dict[str, float]) -> pd.DataFrame:
    df.loc[:, 'Equivalization Factor'] = df.apply(
        vod_eq_factor,
        axis=1,
    )
    df.loc[:, 'VPVH'] = df.apply(
        lambda row: lookup_vpvh(row, band_to_vpvh_mean),
        axis=1,
    ).astype('float64')
    return df


def calc_demo_band(row: pd.Series) -> str:
    if row['Is Audience Target']:
        return 'Audience Target'
    return str(row['Demo Band'])


def demo_comp(row: pd.Series) -> float:
    if row['Gross Counted Ads (Demo)'] == 0:
        dc = 0
    else:
        dc = row['On-Target Net Delivered Impressions'] / row['Gross Counted Ads (Demo)']
    if row['Impression Goal'] == 0:
        below_threshold_dc = 0
    else:
        below_threshold_dc = row['Booked On-Target Impression Goal'] / row['Impression Goal']
    if row['Placement Property'] in ['Minute Media', 'SendToNews', 'Transmit']:
        if row['Demo Band'] != 'P2+':
            return below_threshold_dc
    if row['Is Absolute A']:
        return 1.2
    if (dc == 0 or math.isnan(dc)) and (row['Is Demo Placement'] or is_cov_row(row)):
        if row['Booked On-Target Impression Goal'] == 0:
            return math.nan
        return cast(float, below_threshold_dc)
    return dc


def inherit_max_demo_comp(df: pd.DataFrame) -> Callable[[pd.Series], float]:
    max_demo_comp = df.groupby(
        ['Parent Sales Line Item ID', 'Placement Property', 'Demo Band'],
        sort=False,
    ).agg({'Demo Comp': 'max'}).reset_index().dropna().drop_duplicates().set_index(
        ['Parent Sales Line Item ID', 'Placement Property', 'Demo Band'],
    )
    order_to_demo_comp = max_demo_comp.to_dict('index')

    def inherit_demo_comp(row: pd.Series) -> float:
        if row['No Delivery']:
            return math.nan

        properties = {
            'FOX on Hulu',
            'FOX on Tubi',
            'FOXNow',
            'FOX Sports Streaming',
            'Deportes',
            'FOX Sports Clips',
            'Deportes Display',
            'Deportes Clips',
            'Deportes Streaming',
            'SendToNews',
            'FOX Sports on Tubi',
        }

        if not math.isnan(row['Demo Comp']):
            return math.nan

        if row['Placement Property'] not in properties:
            return math.nan

        if (
            row['Parent Sales Line Item ID'],
            row['Placement Property'],
            row['Demo Band'],
        ) in order_to_demo_comp:
            return cast(
                float, order_to_demo_comp[
                    row['Parent Sales Line Item ID'],
                    row['Placement Property'],
                    row['Demo Band'],
                ]['Demo Comp'],
            )
        if row['Is Demo Placement']:
            try:
                manual_comp = float(row['Planned Demo Comp'])
                return manual_comp
            except (TypeError, ValueError):
                return math.nan
        return math.nan
    return inherit_demo_comp


def calculate_dc(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Demo Comp'] = df.apply(demo_comp, axis=1).astype('float64')
    max_demo_comp = inherit_max_demo_comp(df)
    df.loc[:, 'Inherited Demo Comp'] = df.apply(max_demo_comp, axis=1)
    df = df.fillna({'Demo Comp': df['Inherited Demo Comp']})
    df.loc[:, 'Is CTV Placement'] = ~df['Inherited Demo Comp'].isna()
    return df


def calculate_viewability(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Viewability (GroupM)'] = df.apply(viewability_gm, axis=1)
    df.loc[:, 'Viewability (MRC)'] = df.apply(viewability_mrc, axis=1)
    df.loc[:, 'Viewability'] = df.apply(viewability_used, axis=1)
    dt_viewability = inherit_dt_viewability(df)
    df.loc[:, 'Inherited Viewability'] = df.apply(dt_viewability, axis=1)
    df.loc[:, 'Viewability'] = df.loc[:, 'Inherited Viewability'].fillna(df.loc[:, 'Viewability'])
    df.loc[df['Viewability Vendor'] == 'MOAT', 'Viewability'] = 1
    return df


def viewability_gm(row: pd.Series) -> float:
    if row['Impressions Analyzed'] <= 0:
        return 1
    if math.isnan(row['Impressions Analyzed']):
        return 1
    gm_view = row[
        '3rd Party Valid, Audible and Fully On-Screen'
        ' for Half of the Duration Impressions (15 sec)'
    ] / row['Impressions Analyzed']
    if gm_view == 0:
        gm_view_net = row[
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions'
        ] / row['Impressions Analyzed']
        if gm_view_net > 0:
            return cast(float, gm_view_net)
        return 1
    return cast(float, gm_view)


def viewability_mrc(row: pd.Series) -> float:
    if row['Impressions Analyzed'] <= 0:
        return 1
    if math.isnan(row['Impressions Analyzed']):
        return 1
    view_mrc = row[
        '3rd Party Valid and Viewable Impressions'
    ] / row['Impressions Analyzed']
    if view_mrc > 0:
        return cast(float, view_mrc)
    return 1


def viewability_used(row: pd.Series) -> Any:
    if row['Placement Property'] == 'FOX on Hulu' and row['Is CTV Placement']:
        return 1
    if row['Viewability MRC']:
        return row['Viewability (MRC)']
    return row['Viewability (GroupM)']


def inherit_dt_viewability(df: pd.DataFrame) -> Callable[[pd.Series], Any]:
    dt_viewability = df[~df['Is CTV Placement'] & (df['Placement Property'].isin({'FOXNow', 'FOX Sports Streaming', 'FOX Sports Clips', 'SendToNews'}))].groupby(
        ['Parent Sales Line Item ID', 'Placement Property'],
        sort=False,
    ).agg({'Viewability': 'max'}).drop_duplicates().dropna()
    order_to_dt_view = dt_viewability.to_dict('index')

    def inherit_viewability(row: pd.Series) -> Any:
        if row['Placement Property'] in {'FOXNow', 'FOX Sports Streaming', 'FOX Sports Clips', 'SendToNews'}:
            if not row['Is CTV Placement']:
                if (row['Parent Sales Line Item ID'], row['Placement Property']) in order_to_dt_view:
                    return order_to_dt_view[row['Parent Sales Line Item ID'], row['Placement Property']]['Viewability']
        return math.nan

    return inherit_viewability


def inherit_3p_imps(row: pd.Series) -> Any:
    if row['Is VOD Placement']:
        return row['Net Counted Ads']
    if row['Is 3P']:
        if row['Impressions (3rd Party)'] == 0.0:
            return row['Net Counted Ads']
        return row['Impressions (3rd Party)']
    return row['Net Counted Ads']


def demo_imps_1p(row: pd.Series) -> Any:
    imps = row['Demo Comp'] * row['Net Counted Ads']
    if row['Is Absolute A']:
        imps = 1.2 * row['1P Imps']
        return imps
    if (row['Is Demo Placement'] or is_cov_row(row)) and imps > 0 and not math.isinf(imps):
        return imps
    return 0


def demo_imps_3p(row: pd.Series) -> Any:
    imps = row['Demo Comp'] * row['Impressions (3rd Party)']
    if row['Is Absolute A']:
        imps = 1.2 * row['3P Imps']
        return imps
    if (row['Is Demo Placement'] or is_cov_row(row)) and imps > 0 and not math.isinf(imps):
        return imps
    return 0


def is_cov_row(row: pd.Series) -> bool:
    # is_p2plus = row['Demo Band'] == 'P2+'
    # is_dit = row['FW - Budget Model'] == 'Demographic Impression Target'
    if type(row['FW - RBP Advanced']) == float:
        if np.isnan(row['FW - RBP Advanced']):
            return False
    if row['FW - RBP Advanced'] is None:
        has_cov = False
    else:
        has_cov = any(pair[0].strip() == 'FW - Apply Co-Viewing' and pair[1].strip() == 'Yes' for pair in row['FW - RBP Advanced'])
    return has_cov


def var_1p_3p(row: pd.Series) -> Any:
    if row['Net Counted Ads'] > 0:
        return (row['Impressions (3rd Party)'] - row['Net Counted Ads']) / row['Net Counted Ads']
    return 0


# def is_cov_row(row) -> bool:
#     # is_p2plus = row['Demo Band'] == 'P2+'
#     # is_dit = row['FW - Budget Model'] == 'Demographic Impression Target'
#     if row['FW - RBP Advanced'] is None:
#         has_cov = False
#     else:
#         has_cov = any(pair[0] == 'FW - Apply Co-Viewing' and pair[1] == 'Yes' for pair in row['FW - RBP Advanced'])
#     return has_cov


def imps_1p(row) -> float:
    return row['Net Counted Ads']


def imps_3p(row) -> float:
    return row['Impressions (3rd Party)']


def calculate_imps(df: pd.DataFrame) -> pd.DataFrame:
    pd.options.mode.use_inf_as_na = True

    # 1P and 3P Impressions
    df.loc[:, 'Impressions (3rd Party)'] = df.apply(inherit_3p_imps, axis=1)
    df.loc[:, '1P vs 3P Var'] = df.apply(var_1p_3p, axis=1)
    df.loc[:, '1P Imps'] = df.apply(imps_1p, axis=1).fillna(0).astype('int64')
    df.loc[:, '3P Imps'] = df.apply(imps_3p, axis=1).fillna(0).astype('int64')
    df = df.fillna(
        {
            '1P Imps': 0,
            '3P Imps': 0,
        },
    ).astype(
        {
            '1P Imps': 'int64',
            '3P Imps': 'int64',
        },
    )

    # SAB Engagement Impressions
    def sab_engagement_imps(row: pd.Series) -> int:
        if row['Net Unit Cost'] == 0:
            return 0
        else:
            return (row['Ad Unit Price'] / row['Net Unit Cost'] / 1000) * row['Net Counted Ads']
        
    def sab_engagement_imps_3p(row: pd.Series) -> int:
        if row['Net Unit Cost'] == 0:
            return 0
        else:
            return (row['Ad Unit Price'] / row['Net Unit Cost'] / 1000) * row['Impressions (3rd Party)']
    
    df.loc[:, '1P SAB Engagement Imps'] = df.apply(sab_engagement_imps, axis=1)
    df.loc[:, '3P SAB Engagement Imps'] = df.apply(sab_engagement_imps_3p, axis=1)
    df.loc[:, '1P SAB Engagement Imps'] = df['1P SAB Engagement Imps'].fillna(0).astype('int64')
    df.loc[:, '3P SAB Engagement Imps'] = df['3P SAB Engagement Imps'].fillna(0).astype('int64')

    # VOD Impressions
    df.loc[:, 'VPVH Imps'] = (df['VPVH'] * df['Net Counted Ads']).fillna(0).astype('int64')
    df.loc[:, 'Equivalized Imps'] = (df['Equivalization Factor'] * df['Net Counted Ads']).fillna(0).astype('int64')
    df.loc[:, 'Equivalized and VPVH Imps'] = (df['Equivalization Factor'] * df['VPVH Imps']).fillna(0).astype('int64')
    df.loc[:, '1P VPVH Imps'] = (df['VPVH'] * df['Net Counted Ads']).fillna(0).astype('int64')
    df.loc[:, '1P Equivalized Imps'] = (df['Equivalization Factor'] * df['Net Counted Ads']).fillna(0).astype('int64')
    df.loc[:, '1P Equivalized and VPVH Imps'] = (df['Equivalization Factor'] * df['1P VPVH Imps']).fillna(0).astype('int64')
    df.loc[:, '3P VPVH Imps'] = (df['VPVH'] * df['Impressions (3rd Party)']).fillna(0).astype('int64')
    df.loc[:, '3P Equivalized Imps'] = (df['Equivalization Factor'] * df['Impressions (3rd Party)']).fillna(0).astype('int64')
    df.loc[:, '3P Equivalized and VPVH Imps'] = (df['Equivalization Factor'] * df['3P VPVH Imps']).fillna(0).astype('int64')

    # Demo Impressions
    df.loc[:, '1P Demo Imps'] = df.apply(demo_imps_1p, axis=1).astype('int64')
    df.loc[:, '1P Equivalized Demo Imps'] = df['Equivalization Factor'] * df['1P Demo Imps']
    df.loc[:, '3P Demo Imps'] = df.apply(demo_imps_3p, axis=1).astype('int64')
    df.loc[:, '3P Equivalized Demo Imps'] = df['Equivalization Factor'] * df['3P Demo Imps']

    # Viewability Impressions
    df.loc[:, '1P Viewability Imps'] = (df['Viewability'] * df['1P Imps']).astype('int64')
    df.loc[:, '3P Viewability Imps'] = (df['Viewability'] * df['3P Imps']).astype('int64')
    df.loc[:, '1P Viewability and Demo Imps'] = (df['Viewability'] * df['1P Demo Imps']).astype('int64')
    df.loc[:, '3P Viewability and Demo Imps'] = (df['Viewability'] * df['3P Demo Imps']).astype('int64')
    df.loc[:, '3P Equivalized Viewability and Demo Imps'] = df['Equivalization Factor'] * df['3P Viewability and Demo Imps']

    #Viewability and CoView Impressions
    df.loc[:, '1P Viewability and CoView Imps'] = df['1P Viewability and Demo Imps'] 
    df.loc[:, '3P Viewability and CoView Imps'] = df['3P Viewability and Demo Imps']

    #CoView Imps
    df.loc[:, '1P CoView Imps'] = df['1P Demo Imps']
    df.loc[:, '3P CoView Imps'] = df['3P Demo Imps']

    #Programmatic Impressions
    # df.loc[:, 'Programmatic Guaranteed Imps'] = df['1P Imps']
    # df.loc[:, 'Programmatic Reseller Imps'] = df['1P Imps']

    # Billable/Earned Quantity
    df.loc[:, 'Billable Quantity'] = df.lookup(df.index, df['Implied Billable Metric']).astype('int64')

    """
        Additional Logic for Absolute A
        
        Billable Qty = 1.2*(1P/3P Imps) #Business wants the comp % to always be 120%
    """

    df['Billable Quantity'] = np.where(df['Is Absolute A'] == True,
                                       np.where(df['Billable Third Party Server'] == '1st Party',
                                                1.2*df['1P Imps'], 1.2*df['3P Imps']),
                                       df['Billable Quantity'])
    
    '''
        Additional Logic for Billing Scenario Based on 3P Imps in AOS

        If Net Invoice Amount Terms = 'Performance':
            If 3P Imps > 0:
                Billing Scenario = 'Bill on Actuals - 3rd Party Performance'
            Else:
                Billing Scenario = 'Bill on Actuals - Primary Performance'
    '''

    # df['Billing Scenario'] = np.where(df['Net Invoice Amount Terms'] == 'Performance',
    #                                    np.where(df['3P Imps'] > 0, 
    #                                             'Bill on Actuals - 3rd Party Performance', 'Bill on Actuals - Primary Performance'),
    #                                    df['Billing Scenario'])
    
    return df


def capped_date(
    flight_days: int,
    start_date: datetime,
    cap_date: datetime,
    flight: Optional[List[Tuple[datetime, datetime, str]]] = None) -> int:
    if not isinstance(flight, list):
        datediff = cap_date - start_date
        if datediff < timedelta(0):
            return 0
        if datediff.days >= flight_days:
            return flight_days
        return datediff.days
    capped_days = 0
    for day in flight:
        if day[2] == 1 and start_date <= day[0] <= cap_date.date():
            capped_days += 1
    return capped_days
    # return sum(1 for day in flight if day[2] == '1' and start_date <= day[0] <= cap_date.date())


def launch_date(row: pd.Series) -> Any:
    if row['Is 3P']:
        late_date = row['Third-Party Earliest Delivery Date']
    else:
        late_date = row['Primary Earliest Delivery Date']
    if pd.isnull(late_date):
        return row['Sales Line Item Start Date']
    return late_date


def earned_rev(row: pd.Series) -> Any:
    """
        Additional Logic for Absolute A
        if Cost Method = SOV-Flat Rate:
            Billable Rev = Net Unit Cost
        else:
            Billable Rev = (Billable Qty/1000)*Net Unit Cost
    """
    if row['Is Absolute A']:
        if row['Cost Method'] == 'SOV-Flat Rate':
            return row['Net Unit Cost']
        else:
            return row['Net Unit Cost'] * row['Billable Quantity'] / 1000
    if row['Is SAB Engagement']:
        return row['Net Unit Cost'] * row['Billable Quantity']
    return row['Net Unit Cost'] * row['Billable Quantity'] / 1000


def delivery_ratio(row: pd.Series) -> Any:
    if row['Is Non-Ad Served']:
        return 0
    if row['Quantity'] == 0:
        return 0
    return row['Billable Quantity'] / row['Quantity']


def days_in_period(row: Any, period_start: date, period_end: date) -> int:
    if isinstance(row['Flight'], list):
        days_in_period_count = 0
        for day in row['Flight']:
            if period_start <= day[0] <= period_end and day[2] == 1:
                days_in_period_count += 1
        return days_in_period_count
        # return sum(1 for day in row['Flight'] if period_start <= day[0] <= period_end and day[2] == '1')

    if row['Sales Line Item Start Date'] < period_start:
        start_date = period_start
    else:
        start_date = row['Sales Line Item Start Date'].date()
    if row['Sales Line Item End Date'] > period_end:
        end_date = period_end
    else:
        end_date = row['Sales Line Item End Date'].date()
    days_in_period = (end_date - start_date + timedelta(1)).days
    if days_in_period < 0:
        return 0
    return days_in_period


def get_allocation_details() -> pd.DataFrame:
    df = file_to_df('Allocation_Details.parquet')
    df = df.rename(columns={'sales_order_line_item_id': 'Sales Line Item ID'})
    df.loc[:, 'date'] = df.loc[:, 'date'].astype('datetime64[us]')
    df = df.set_index('Sales Line Item ID')
    df = df[~df['is_live_flg'].isna() | ~df['is_dark_flg'].isna()]
    df['Flight'] = df[['date', 'is_dark_flg', 'is_live_flg']].apply(tuple, axis=1)
    df = df.drop(columns=['date', 'is_dark_flg', 'is_live_flg'])
    return df.groupby(df.index).aggregate(list)


def days_in_flight(start_date: datetime, end_date: datetime, flight: Union[float, List[Tuple[datetime, datetime, str]]]) -> int:
    if not isinstance(flight, list):
        return (end_date - start_date + timedelta(1)).days
    flight_days = 0
    for day in flight:
        if day[2] == 1:
            flight_days += 1
    return flight_days
    # return sum(1 for day in flight if day[2] == '1')

def calculate_flight(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    quarter_start = pd.to_datetime(report_date.replace(day=15) - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date()
    quarter_end = pd.to_datetime(report_date.replace(day=15) + pd.tseries.offsets.QuarterEnd(startingMonth=0)).date()
    month_end = pd.to_datetime(report_date.replace(day=15) + pd.tseries.offsets.MonthEnd()).date()
    month_start = month_end.replace(day=1)

    allocation_details = get_allocation_details()

    df = df.join(allocation_details, on=['Sales Line Item ID'])
    df.loc[:, 'Days in Flight'] = df.apply(lambda row: days_in_flight(row['Sales Line Item Start Date'], row['Sales Line Item End Date'], row['Flight']), axis=1)

    df.loc[:, 'Days Elapsed'] = df.apply(
        lambda row: capped_date(
            row['Days in Flight'],
            row['Sales Line Item Start Date'],
            report_date,
            row['Flight'],
        ),
        axis=1,
    )

    df.loc[:, 'Flight Days in Month'] = df.apply(lambda row: days_in_period(row, month_start, month_end), axis=1)
    df.loc[:, 'Flight Days in Quarter'] = df.apply(lambda row: days_in_period(row, quarter_start, quarter_end), axis=1)
    df.loc[:, 'Remaining Days in Flight'] = df['Days in Flight'] - df['Days Elapsed']
    df.loc[:, 'Remaining Days until EOM'] = (datetime.combine(month_end, time()) - report_date + timedelta(1)).days
    df.loc[:, 'Remaining Days until EOQ'] = (datetime.combine(quarter_end, time()) - report_date + timedelta(1)).days
    df.loc[:, 'Remaining Flight Days in Lifetime'] = df.loc[:, 'Remaining Days in Flight']
    df.loc[:, 'Remaining Flight Days in Month'] = [min(days) for days in zip(df['Remaining Days in Flight'], df['Flight Days in Month'], df['Remaining Days until EOM'])]
    df.loc[:, 'Remaining Flight Days in Quarter'] = [min(days) for days in zip(df['Remaining Days in Flight'], df['Flight Days in Quarter'], df['Remaining Days until EOQ'])]
    df.loc[:, 'Flight %'] = df['Days Elapsed'] / df['Days in Flight']
    df.loc[:, 'Launch Date'] = df.apply(launch_date, axis=1)
    df.loc[:, 'Launch Days in Flight'] = df.apply(lambda row: days_in_period(row, row['Launch Date'].to_pydatetime().date(), row['Sales Line Item End Date']), axis=1)
    df.loc[:, 'Launch Days Elapsed'] = df.apply(lambda row: capped_date(row['Launch Days in Flight'], row['Launch Date'], report_date, row['Flight']), axis=1)
    df.loc[:, 'Launch Flight %'] = df['Launch Days Elapsed'] / df['Launch Days in Flight']
    df.loc[:, 'Earned Revenue'] = df.apply(earned_rev, axis=1)
    df.loc[:, 'Pacing %'] = df['Billable Quantity'] / df['Quantity'] / df['Launch Flight %']
    df.loc[:, 'Delivered %'] = df.apply(delivery_ratio, axis=1)
    df.loc[:, 'Daily Budget'] = df['Net Cost'] / df['Days in Flight']
    df.loc[:, 'Daily Imps Budget'] = df['Quantity'] / df['Days in Flight']
    df.loc[:, 'Straight Line Booked Revenue to EOM'] = df['Daily Budget'] * df['Flight Days in Month']
    df.loc[:, 'Straight Line Booked Revenue to EOQ'] = df['Daily Budget'] * df['Flight Days in Quarter']
    df.loc[:, 'Straight Line Booked Impressions to EOM'] = df['Daily Imps Budget'] * df['Flight Days in Month']
    df.loc[:, 'Straight Line Booked Impressions to EOQ'] = df['Daily Imps Budget'] * df['Flight Days in Quarter']
    df.loc[:, 'Straight Line Booked Impressions to EOM'] = df.apply(round, args=('Straight Line Booked Impressions to EOM', ), axis=1)
    df.loc[:, 'Straight Line Booked Impressions to EOQ'] = df.apply(round, args=('Straight Line Booked Impressions to EOQ', ), axis=1)
    # df.loc[:, 'Straight Line Booked Impressions to EOM'] = round(df['Daily Imps Budget'] * df['Flight Days in Month'])
    # df.loc[:, 'Straight Line Booked Impressions to EOQ'] = round(df['Daily Imps Budget'] * df['Flight Days in Quarter'])

    if environment == '':
        df = df.drop(columns=['Flight'])
    else:
        def flight_days(flight: Any) -> Any:
            if isinstance(flight, list):
                return [day[0] for day in flight if day[2] == 1]
            return flight
        df['Live Days'] = df['Flight'].apply(flight_days)
        df = df.drop(columns=['Flight'])

    return df


def run_rate(row: pd.Series) -> Any:
    if row['Is Absolute A']:
        return row['Net Cost']
    if row['Is Non-Ad Served']:
        return row['Net Cost'] * row['Pacing %']
    if row['Is Future Start']:
        return row['Net Cost'] * row['Pacing %']
    if row['Quantity'] == 0:
        return row['Net Cost'] * row['Pacing %']
    programmatic_guarantees = {
        'Programmatic Guaranteed – Freewheel',
        'Programmatic Guaranteed – SpotX',
        'Quasi Programmatic – Agency',
        'Quasi Programmatic (GAM) – GAM',
        'Programmatic Guaranteed – Telaria',
    }
    if row['Programmatic Type'] in programmatic_guarantees:
        return 0
    if row['No Delivery']:
        return row['Net Cost'] * (1-row['Flight %'])
    if 'cancel' in row['Sales Line Item Name'].lower():
        return 0
    return row['Net Cost'] * row['Pacing %']


def pacing_rate(row: pd.Series) -> Any:
    if row['Is Future Start']:
        return row['Daily Budget']
    if row['Remaining Days in Flight'] <= 0:
        return 0
    pacing_rate = (row['Run Rate']-row['Earned Revenue']) / row['Remaining Days in Flight']
    if pacing_rate < 0:
        return 0
    return pacing_rate


def capped_run_rate(row: pd.Series) -> Any:
    if row['Pacing %'] >= 1:
        return row['Net Cost']
    return row['Run Rate']


def daily_pacing_quantity(row: pd.Series) -> Any:
    if row['Is Future Start']:
        return row['Daily Imps Budget']
    if row['Launch Days Elapsed'] <= 0:
        return row['Billable Quantity']
    return row['Billable Quantity']/row['Launch Days Elapsed']


def daily_pacing_1p(row: pd.Series) -> Any:
    if row['Is Future Start']:
        return row['Daily Imps Budget']
    if row['Launch Days Elapsed'] <= 0:
        return row['1P Imps']
    return row['1P Imps']/row['Launch Days Elapsed']


def calculate_projected(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Run Rate'] = df.apply(run_rate, axis=1)
    df.loc[:, 'Sales Line Item Run Rate (Capped)'] = df.apply(capped_run_rate, axis=1)
    df.loc[:, 'Daily Pacing Quantity'] = df.apply(daily_pacing_quantity, axis=1)
    df.loc[:, 'Daily Pacing 1P Imps'] = df.apply(daily_pacing_1p, axis=1)
    df.loc[:, 'Daily Pacing Rate'] = df.apply(pacing_rate, axis=1)
    return df


def parent_risk_rev(row: pd.Series) -> Any:
    if math.isnan(row['Parent Line Item Total']):
        return None
    if row['Is Absolute A']:
        return 0
    if row['Is Non-Ad Served']:
        return 0
    if row['Is Future Start']:
        return 0
    risk_rev = row['Parent Line Item Total'] - row['Parent Line Item Run Rate']
    if risk_rev <= 0:
        return 0
    return risk_rev


def cap_risk_rev(row: pd.Series, risk_col: str, cap_col: str) -> Any:
    if row['Is Absolute A']:
        return 0
    if row['Is Non-Ad Served']:
        return 0
    if row['Is Future Start']:
        return 0
    if row['Quantity'] == 0:
        return 0
    if row[risk_col] <= 0:
        return 0
    if row[risk_col] >= row[cap_col]:
        return row[cap_col]
    return row[risk_col]


def categorize_risk(row: pd.Series, report_date: datetime, threshold: float = 1.1) -> Any:
    if row['Is Future Start']:
        return 'Future Start'
    if row['Is Non-Ad Served']:
        return 'Non-Ad Served'
    elif row['Sales Line Item End Date'] >= report_date:
        return categorize_active(row, threshold)
    return categorize_complete(row)


def categorize_active(row: pd.Series, threshold: Any) -> Any:
    if row['No Delivery']:
        return 'Inactive, Late Creative'
    if row['Pacing %'] > threshold:
        return 'Active, Over-Pacing'
    elif row['Pacing %'] >= 1:
        return 'Active, On-Schedule'
    return 'Active, Under-Pacing'


def categorize_complete(row: pd.Series) -> str:
    if row['Billable Quantity'] >= row['Quantity']:
        return 'Complete, Delivered in Full'
    return 'Complete, Under-Delivered'


def is_cancelled(row: pd.Series) -> bool:
    if 'cancel' in row['Sales Line Item Name'].lower():
        return True
    return False


def calculate_risk(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    def mask_sales_line_item_id(row: pd.Series) -> Any:
        if type(row['Sales Line Item Name']) == str:
            if 'cancel' in row['Sales Line Item Name'].lower():
                return np.nan
        return row['Sales Line Item ID']

    df.loc[:, 'Sales Line Item Revenue at Risk'] = df['Net Cost'] - df['Run Rate']
    df.loc[:, 'Is Cancelled'] = df.apply(is_cancelled, axis=1)
    temp_df = df[~(df['Is Future Start'] | df['Is Non-Ad Served'] | df['Is Cancelled'] | df['Sales Line Item Name'].str.contains('Non Ad-Served'))]
    temp_df['Sales Line Item ID'] = temp_df.apply(mask_sales_line_item_id, axis=1)

    line_item_total = temp_df.groupby(
        'Parent Sales Line Item ID',
        as_index=False,
    ).agg(
        {
            'Sales Line Item ID': 'min',
            'Net Cost': 'sum',
            'Sales Line Item Run Rate (Capped)': 'sum',
            'Earned Revenue': 'sum',
        },
    ).rename(
        {
            'Net Cost': 'Parent Line Item Total',
            'Sales Line Item Run Rate (Capped)': 'Parent Line Item Run Rate',
            'Earned Revenue': 'Parent Line Item Earned Revenue',
        },
        axis=1,
    )

    df['Sales Line Item ID'] = df['Sales Line Item ID'].astype('float64')
    df = pd.merge(
        df,
        line_item_total,
        on=['Parent Sales Line Item ID', 'Sales Line Item ID'],
        how='left',
    )

    df.loc[:, 'Parent Line Item Revenue at Risk'] = df.apply(
        parent_risk_rev,
        axis=1,
    )

    df.loc[:, 'Sales Line Item Revenue at Risk'] = df.apply(
        lambda row: cap_risk_rev(
            row,
            'Sales Line Item Revenue at Risk',
            'Net Cost',
        ),
        axis=1,
    )

    df.loc[:, 'Sales Line Item Risk Category'] = df.apply(
        lambda row: categorize_risk(row, report_date),
        axis=1,
    )

    return df

def rounded_invoice_amt(group: pd.Series, col: str) -> pd.DataFrame:
    # round function needs to be redefined maybe
    cents = round(round(group['Net Invoice Amount'].mean(), 2) - group[col].sum(), 2)
    max_inv = group.sort_values(by=col, ascending=False).head(1)
    adj_id = max_inv['Sales Line Item ID'].min()
    adj_inv_amt = (max_inv[col] + cents).max()
    return pd.DataFrame({'adj_id': adj_id, 'adj_inv_amt': adj_inv_amt}, index=[0])


def billable_imps_active(row: pd.Series) -> Any:
    def round(number: int, digits: int = 0) -> Any:
        factor = 10 ** digits
        return int(number * factor + 0.5 if number >= 0 else number * factor - 0.5) / factor

    if row['Cost Method'] == 'SOV-Flat Rate':
        return 0
    if row['Is Bill on Contract']:
        if row['Net Unit Cost'] == 0:
            return 0
        if math.isnan(row['Rounded Scaled Active Net Invoice Amount']):
            return 0
        return round(1000 * row['Rounded Scaled Active Net Invoice Amount'] / row['Net Unit Cost'])

    if row['Invoice Organization Name'] == 'FOX Sports & Entertainment Fluidity':
        return row['Billable Quantity']
    if row['Billable Quantity'] >= row['Quantity']:
        return row['Quantity']
    return row['Billable Quantity']


def billable_imps_lifetime(row: pd.Series) -> Any:
    def round(number: int, digits: int = 0) -> Any:
        factor = 10 ** digits
        return int(number * factor + 0.5 if number >= 0 else number * factor - 0.5) / factor

    if row['Cost Method'] == 'SOV-Flat Rate':
        return 0
    if row['Is Bill on Contract']:
        if row['Net Unit Cost'] == 0:
            return 0
        if math.isnan(row['Rounded Scaled Net Invoice Amount']):
            return 0
        return round(1000 * row['Rounded Scaled Net Invoice Amount'] / row['Net Unit Cost'])
    if row['Invoice Organization Name'] == 'FOX Sports & Entertainment Fluidity':
        return row['Billable Quantity']
    if row['Billable Quantity'] >= row['Quantity']:
        return row['Quantity']
    return row['Billable Quantity']


def billable_rev_active(row: pd.Series, capped: bool = True) -> Any:
    if row['Is Bill on Contract']:
        return row['Rounded Scaled Active Net Invoice Amount']
    if capped and (row['Earned Revenue'] >= row['Net Cost']):
        return row['Net Cost']
    return row['Earned Revenue']


def billable_rev_lifetime(row: pd.Series, capped: bool = True) -> Any:
    if row['Is Bill on Contract']:
        return row['Rounded Scaled Net Invoice Amount']
    if capped and (row['Earned Revenue'] >= row['Net Cost']):
        return row['Net Cost']
    return row['Earned Revenue']


def active_net_cost_ratio(row: pd.Series) -> Any:
    if row['Total Active Net Cost'] > 0:
        return row['Net Cost'] / row['Total Active Net Cost']
    return 0


def net_cost_ratio(row: pd.Series) -> Any:
    if row['Total Net Cost'] > 0:
        return row['Net Cost'] / row['Total Net Cost']
    return 0


def calculate_billable(df: pd.DataFrame) -> pd.DataFrame:
    # Package rows carry the parent Net Cost and are dropped later in format_billing.
    # Exclude them from Net Cost ratio / Net Invoice scaling so children receive the
    # full AOS finance Net Invoice Amount (otherwise BAR is ~50% of AOS).
    non_package = df[~df['Operative Product Type'].isin({'Package'})]
    total_active_net_cost = non_package[~(non_package['Is Future Start'])].groupby(
        'Parent Sales Line Item ID',
        as_index=False,
    ).agg({'Net Cost': 'sum'}).rename({'Net Cost': 'Total Active Net Cost'}, axis=1)
    total_net_cost = non_package.groupby('Parent Sales Line Item ID', as_index=False).agg({'Net Cost': 'sum'}).rename({'Net Cost': 'Total Net Cost'}, axis=1)
    df = pd.merge(df, total_net_cost, on=['Parent Sales Line Item ID'], how='left')
    df = pd.merge(df, total_active_net_cost, on=['Parent Sales Line Item ID'], how='left')
    df.loc[:, 'Active Net Cost Ratio'] = df.apply(active_net_cost_ratio, axis=1)
    df.loc[:, 'Net Cost Ratio'] = df.apply(net_cost_ratio, axis=1)
    df.loc[:, 'Scaled Active Net Invoice Amount'] = df['Net Invoice Amount'] * df['Active Net Cost Ratio']
    df.loc[:, 'Scaled Net Invoice Amount'] = df['Net Invoice Amount'] * df['Net Cost Ratio']
    df.loc[:, 'Scaled Active Net Invoice Amount'] = df.apply(round, args=('Scaled Active Net Invoice Amount', 2,), axis=1)
    df.loc[:, 'Scaled Net Invoice Amount']  = df.apply(round, args=('Scaled Net Invoice Amount', 2,), axis=1)
    non_package = df[~df['Operative Product Type'].isin({'Package'})]
    order_to_active_amt_df = non_package[~(non_package['Is Future Start'])].groupby('Parent Sales Line Item ID', as_index=False).apply(
        lambda group: rounded_invoice_amt(group, 'Scaled Active Net Invoice Amount'))
    order_to_active_amt = {row['adj_id']: row['adj_inv_amt'] for _idx, row in order_to_active_amt_df.iterrows()}
    df.loc[:, 'Rounded Scaled Active Net Invoice Amount'] = df['Sales Line Item ID'].map(
        order_to_active_amt).fillna(df['Scaled Active Net Invoice Amount'])
    order_to_amt_df = non_package.groupby('Parent Sales Line Item ID', as_index=False).apply(
        lambda group: rounded_invoice_amt(group, 'Scaled Net Invoice Amount'))
    order_to_amt = {row['adj_id']: row['adj_inv_amt'] for _idx, row in order_to_amt_df.iterrows()}
    df.loc[:, 'Rounded Scaled Net Invoice Amount'] = df['Sales Line Item ID'].map(order_to_amt).fillna(df['Scaled Net Invoice Amount'])
    df.loc[:, 'Billable Impressions Active'] = df.apply(billable_imps_active, axis=1)
    df.loc[:, 'Billable Impressions Lifetime'] = df.apply(billable_imps_lifetime, axis=1)
    df.loc[:, 'Billable Revenue Active'] = df.apply(lambda row: billable_rev_active(row, capped=True), axis=1)
    df.loc[:, 'Billable Revenue Active - Uncapped'] = df.apply(lambda row: billable_rev_active(row, capped=False), axis=1)
    df.loc[:, 'Billable Revenue Lifetime'] = df.apply(lambda row: billable_rev_lifetime(row, capped=True), axis=1)
    df.loc[:, 'Billable Revenue Lifetime - Uncapped'] = df.apply(lambda row: billable_rev_lifetime(row, capped=False), axis=1)
    return df


def billing_instructions(row: pd.Series) -> Any:
    ioi = row['Include on Invoice']
    ioi = 'Show Only Net' if ioi == 'Only Net' else ioi
    if row['Billing Notes']:
        return ioi + '; ' + row['Billing Notes']
    return ioi


def merge_order_delivery(op_oms: pd.DataFrame, fw_adj_agg: pd.DataFrame, fw_demo_agg: pd.DataFrame) -> pd.DataFrame:
    op_fw_adj = op_oms.join(
        fw_adj_agg.join(
        fw_demo_agg,
        ).reset_index().set_index('Placement ID'),
    )
    op_fw_adj = drop_freewheel_inactive(op_fw_adj)
    op_fw_adj.reset_index(inplace=True)
    return op_fw_adj


def calculate_metrics(op_fw_adj: pd.DataFrame, band_to_vpvh_mean: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    op_fw_adj = identify_placement_types(op_fw_adj, report_date)
    op_fw_adj = identify_billable_metric(op_fw_adj)
    op_fw_adj = redefining_billable_metric(op_fw_adj)
    op_fw_adj = calculate_vpvh(op_fw_adj, band_to_vpvh_mean)
    op_fw_adj = calculate_dc(op_fw_adj)
    op_fw_adj = calculate_viewability(op_fw_adj)
    op_fw_adj = calculate_imps(op_fw_adj)
    op_fw_adj = calculate_flight(op_fw_adj, report_date)
    op_fw_adj = calculate_billable(op_fw_adj)
    op_fw_adj = calculate_projected(op_fw_adj)
    op_fw_adj = calculate_risk(op_fw_adj, report_date)
    op_fw_adj.reset_index(inplace=True)
    return op_fw_adj

# COMMAND ----------

def equivalize(df: pd.DataFrame, report_type: str) -> pd.DataFrame:

    def equivalize_billable_metric(row: pd.Series) -> str:
        if report_type != 'operative_3p' and row['is_pg_deal']:
            return row['Billable Metric']
        if row['Is Absolute A'] or row['Is Audience Target']:
            return row['Billable Metric']
        if row['Equivalize?']:
            if row['Is Demo Placement']:
                if row['Is Viewability Placement']:
                    if row['Is 3P']:
                        return '3P Equivalized Viewability and Demo Imps'
                    return '1P Equivalized Viewability and Demo Imps'
                if row['Is 3P']:
                    return '3P Equivalized and Demo Imps'
                return '1P Equivalized and Demo Imps'
            if row['Is 3P']:
               return '3P Equivalized Imps'
            return '1P Equivalized Imps'
        return row['Billable Metric']
    
    def equivalize_billable_quantity(row: pd.Series) -> int:
        if row['Billable Metric'] == '3P Equivalized Viewability and Demo Imps':
            return row['Equivalization Factor']*row['Viewability']*row['3P Demo Imps']
        if row['Billable Metric'] == '1P Equivalized Viewability and Demo Imps':
            return row['Equivalization Factor']*row['Viewability']*row['1P Demo Imps']
        if row['Billable Metric'] == '3P Equivalized and Demo Imps':
            return row['Equivalization Factor']*row['3P Demo Imps']
        if row['Billable Metric'] == '1P Equivalized and Demo Imps':
            return row['Equivalization Factor']*row['1P Demo Imps']
        if row['Billable Metric'] == '3P Equivalized Imps':
            return row['Equivalization Factor']*row['3P Imps']
        if row['Billable Metric'] == '1P Equivalized Imps':
            return row['Equivalization Factor']*row['1P Imps']
        if report_type == 'bar':
            if row['Billable Metric'] == 'Bill on Contract' or row['Billable Metric'] == 'SOV' or ('Audience' in row['Billable Metric']):
                return row['Billable Impressions']
        return row['Billable Quantity']
    
    def equivalize_billable_revenue(row: pd.Series) -> float:
        if row['Billable Metric'] == '3P Equivalized Viewability and Demo Imps':
            return ((row['Equivalization Factor']*row['Viewability']*row['3P Demo Imps'])/1000)*row['Net Unit Cost']
        if row['Billable Metric'] == '1P Equivalized Viewability and Demo Imps':
            return ((row['Equivalization Factor']*row['Viewability']*row['1P Demo Imps'])/1000)*row['Net Unit Cost']
        if row['Billable Metric'] == '3P Equivalized and Demo Imps':
            return ((row['Equivalization Factor']*row['3P Demo Imps'])/1000)*row['Net Unit Cost']
        if row['Billable Metric'] == '1P Equivalized and Demo Imps':
            return ((row['Equivalization Factor']*row['1P Demo Imps'])/1000)*row['Net Unit Cost']
        if row['Billable Metric'] == '3P Equivalized Imps':
            return ((row['Equivalization Factor']*row['3P Imps'])/1000)*row['Net Unit Cost']
        if row['Billable Metric'] == '1P Equivalized Imps':
            return ((row['Equivalization Factor']*row['1P Imps'])/1000)*row['Net Unit Cost']
        if report_type == 'pacing':
            return row['Earned Revenue']
        if report_type == 'bar':
            return row['Billable Revenue']
    
    if report_type == 'pacing':
        df.loc[:, 'Billable Metric'] = df.apply(equivalize_billable_metric, axis=1)
        df.loc[:, 'Billable Quantity'] = df.apply(equivalize_billable_quantity, axis=1)
        df.loc[:, 'Earned Revenue'] = df.apply(equivalize_billable_revenue, axis=1)
    
    if report_type == 'bar':
        df.loc[:, 'Billable Metric'] = df.apply(equivalize_billable_metric, axis=1)
        df.loc[:, 'Billable Impressions'] = df.apply(equivalize_billable_quantity, axis=1)
        df.loc[:, 'Billable Revenue'] = df.apply(equivalize_billable_revenue, axis=1)
    
    if report_type == 'operative_1p' or report_type == 'operative_3p':
        df.loc[:, 'Billable Metric'] = df.apply(equivalize_billable_metric, axis=1)
        if '1p' in report_type:
            df.loc[:, '1P Equivalized Demo Imps'] = df.apply(lambda row: ((row['Equivalization Factor']*row['1P Demo Imps'])/1000)*row['Net Unit Cost'] if row['Billable Metric'] == '1P Equivalized and Demo Imps' else 0, axis=1)
        if '3p' in report_type:
            df.loc[:, '3P Equivalized Demo Imps'] = df.apply(lambda row: ((row['Equivalization Factor']*row['3P Demo Imps'])/1000)*row['Net Unit Cost'] if row['Billable Metric'] == '3P Equivalized and Demo Imps' else 0, axis=1)

    return df