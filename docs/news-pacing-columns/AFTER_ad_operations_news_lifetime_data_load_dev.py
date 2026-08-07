# Databricks notebook source
# MAGIC %run "/Workspace/Users/prajwal.harikishor@fox.com/fdp-di-adtech-databricks/src/dev/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/prajwal.harikishor@fox.com/fdp-di-adtech-databricks/src/dev/data_reporting_and_operational_processing/alert"

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
import io
import boto3
import pandas as pd
import numpy as np
import traceback


# COMMAND ----------

def find_staq_file(report_date: datetime) -> str:
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')

    staq = 'STAQ_Adjuster_Pacing_YTD_All_Lines_' + report_date.strftime('%Y-%m-%d')

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=staq_bucket, Prefix=staq)

    latest_file = None
    latest_modified = None

    for page in page_iterator:
        for obj in page.get('Contents', []):
            if staq in obj['Key']:
                if latest_modified is None or obj['LastModified'] > latest_modified:
                    latest_modified = obj['LastModified']
                    latest_file = obj['Key']

    if latest_file:
        return latest_file

# COMMAND ----------

def apply_pg_billing_news(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    """Overlay Freewheel PG lifetime delivery onto News PG invoice orgs.

    News pacing reads a thin column set from AdOps_Reporting_Lifetime_Delivery
    (unlike the main lifetime job), so this path must not assume PG / parent /
    pacing columns already exist on the frame.
    """
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    fw_pg_filename = 'FW_PG_Lifetime_Delivery.parquet'
    fw_pg_file = s3_dir + 'processed/' + fw_pg_filename

    if 'Placement ID' not in df.columns:
        raise KeyError(
            "Placement ID is required to join FW_PG_Lifetime_Delivery.parquet "
            "for News Freewheel PG billing"
        )

    # Thin news read never includes PG Impressions; ignore if already present
    # from a prior lifetime overlay so the FW PG merge can re-apply cleanly.
    df = df.drop(columns=['PG Impressions'], errors='ignore')

    pg_billing = pd.read_parquet(fw_pg_file, storage_options={'profile': aws_profile})

    pg_billing.drop(['Event Date'], axis=1, inplace=True)

    pg_billing = pg_billing.groupby(['Deal ID']).sum().reset_index()

    pg_billing = pg_billing.rename({'Net Counted Ads': 'PG Impressions'}, axis=1)

    df = pd.merge(df, pg_billing, how='left', left_on='Placement ID', right_on='Deal ID')

    # Only News Programmatic orgs get FW PG overlay. Regular News/Outkick/TMZ
    # must keep AdOps Gross/Net Counted Ads (prod news path does not wipe them).
    news_pg_orgs = {
        'FOX News & Business Programmatic',
        'FOX News & Business Programmatic - GAM',
    }

    def is_pg_deal(row: pd.Series) -> bool:
        return row['Invoice Organization Name'] in news_pg_orgs

    if 'Programmatic Type' not in df.columns:
        df['Programmatic Type'] = None
    if 'Billable Metric' not in df.columns:
        df['Billable Metric'] = None

    def billable_metric(row: pd.Series) -> str:
        sales_order = row['Sales Order Name']
        prog_type = row['Programmatic Type']
        invoice_org = row['Invoice Organization Name']
        if row['is_pg_deal'] and isinstance(sales_order, str) and 'evergreen' in sales_order.lower():
            return 'Programmatic Reseller Imps'
        if row['is_pg_deal'] and isinstance(prog_type, str) and 'programmatic guaranteed' in prog_type.lower():
            return "Programmatic Guaranteed Imps"
        if row['is_pg_deal'] and isinstance(invoice_org, str) and 'programmatic guaranteed' in invoice_org.lower():
            return "Programmatic Guaranteed Imps"
        return row['Billable Metric']

    df.loc[:, 'is_pg_deal'] = df.apply(is_pg_deal, axis=1)

    df.loc[:, 'Billable Metric'] = df.apply(billable_metric, axis=1)

    pg_df = df[df['is_pg_deal'] == True].copy()
    other_df = df[df['is_pg_deal'] == False].copy()

    # Only overwrite when FW PG delivery matched; leave unmatched PG rows alone
    has_pg = pg_df['PG Impressions'].notna()
    pg_df.loc[has_pg, '1P Imps'] = pg_df.loc[has_pg, 'PG Impressions']
    pg_df.loc[has_pg, 'Net Counted Ads'] = pg_df.loc[has_pg, 'PG Impressions']
    pg_df.loc[has_pg, 'Gross Counted Ads'] = pg_df.loc[has_pg, 'PG Impressions']
    pg_df.loc[has_pg, 'No Delivery'] = pg_df.loc[has_pg, 'PG Impressions'] == 0
    pg_df.loc[has_pg, 'Billable Quantity'] = pg_df.loc[has_pg, 'PG Impressions']
    if 'Launch Flight %' in pg_df.columns:
        pg_df.loc[has_pg, 'Pacing %'] = (
            pg_df.loc[has_pg, 'Billable Quantity']
            / pg_df.loc[has_pg, 'Quantity']
            / pg_df.loc[has_pg, 'Launch Flight %']
        )
    pg_df.loc[has_pg, 'Earned Revenue'] = (
        pg_df.loc[has_pg, 'Net Unit Cost'] * pg_df.loc[has_pg, 'Billable Quantity'] / 1000
    )
    pg_df.drop(
        ['Parent Line Item Total', 'Parent Line Item Run Rate', 'Parent Line Item Earned Revenue'],
        inplace=True,
        axis=1,
        errors='ignore',
    )

    df = pd.concat([other_df, pg_df])

    df.fillna(
        {
            '1P Imps': 0,
            'Billable Quantity': 0,
            'Earned Revenue': 0,
        },
        inplace=True,
    )

    return df

# COMMAND ----------

def osi(row: pd.Series, report_date: datetime, type: str) -> float:
    if type == '1p':
        if row['Ad Server Impressions'] == 0 or row['Contracted Quantity'] == 0:
            return 0
        qty = row['Ad Server Impressions']/row['Contracted Quantity']
    else:
        if row['Impressions (3rd Party)'] == 0 or row['Contracted Quantity'] == 0:
            return 0
        qty = row['Impressions (3rd Party)']/row['Contracted Quantity']
    days_elapsed = report_date - row['Line Item Start Date'] + timedelta(days=1)
    flight = row['Line Item End Date'] - row['Line Item Start Date'] + timedelta(days=1)

    if report_date < row['Line Item End Date']:
        return qty/(days_elapsed/flight)
    else:
        return qty
    
def podcast_osi(row: pd.Series, report_date: datetime) -> float:


    delivered_impressions = pd.to_numeric(
        row.get('Ad Server Impressions', 0),
        errors='coerce'
    )

    contracted_impressions = pd.to_numeric(
        row.get('Contracted Quantity', 0),
        errors='coerce'
    )

    start_date = pd.to_datetime(
        row.get('Line Item Start Date'),
        errors='coerce'
    )

    end_date = pd.to_datetime(
        row.get('Line Item End Date'),
        errors='coerce'
    )

    if pd.isna(delivered_impressions):
        return 0

    if pd.isna(contracted_impressions) or contracted_impressions <= 0:
        return 0

    if pd.isna(start_date) or pd.isna(end_date):
        return 0


    if end_date < start_date:
        return 0

    if report_date < start_date:
        return 0


    total_flight_days = (
        end_date - start_date
    ).days + 1

    if total_flight_days <= 0:
        return 0

    effective_date = report_date if report_date < end_date else end_date

    days_live = (
        effective_date - start_date
    ).days + 1

    if days_live <= 0:
        return 0

    delivery_progress = (
        delivered_impressions / contracted_impressions
    )

    flight_progress = (
        days_live / total_flight_days
    )

    if flight_progress <= 0:
        return 0

    return delivery_progress / flight_progress

    
def metrics_calculaition(row: pd.Series, calculation: str) -> float:
    if calculation == 'Total Error Rate':
        if row['Total Impressions'] + row['Total Error Count'] == 0:
            return 0
        return row['Total Error Count']/(row['Total Impressions']+row['Total Error Count'])
    
    if calculation == '3rd Party CTR':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return row['Clicks (3rd Party)']/row['Impressions (3rd Party)']
        #return 0
    
    if calculation == 'Buffer':
        if row['Contracted Quantity'] == 0:
            return 0
        return (row['Ad Server Booked Impressions']-row['Contracted Quantity'])/row['Contracted Quantity']
    
    if calculation == 'Discrepancy':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return (row['Impressions (3rd Party)']-row['Ad Server Impressions'])/row['Impressions (3rd Party)']

def calculate_metrics(df: pd.DataFrame, report_date: datetime, is_podcast: bool = False) -> pd.DataFrame:
    df.loc[:, 'Total Error Rate'] = df.apply(metrics_calculaition, args=('Total Error Rate', ), axis=1)
    df.loc[:, '3rd Party CTR'] = df.apply(metrics_calculaition, args=('3rd Party CTR', ), axis=1)
    df.loc[:, 'Ad Server Booked Impressions'] = df['Quantity']
    df.loc[:, 'Buffer'] = df.apply(metrics_calculaition, args=('Buffer', ), axis=1)
    df.loc[:, 'Discrepancy'] = df.apply(metrics_calculaition, args=('Discrepancy', ), axis=1)
    if is_podcast:
        podcast_osi_values = df.apply(podcast_osi, args=(report_date, ), axis=1)
        df.loc[:, 'Current First Party OSI'] = podcast_osi_values
        df.loc[:, 'Current Third Party OSI'] = podcast_osi_values
    else:
        df.loc[:, 'Current First Party OSI'] = df.apply(osi, args=(report_date, '1p'), axis=1)
        df.loc[:, 'Current Third Party OSI'] = df.apply(osi, args=(report_date, '3p'), axis=1)
    
    metric_columns = [
        'Total Error Rate',
        '3rd Party CTR',
        'Ad Server Booked Impressions',
        'Buffer',
        'Discrepancy',
        'Current First Party OSI',
        'Current Third Party OSI',
    ]

    df = df.fillna({col:0 for col in metric_columns})

    for col in metric_columns:
        df[col] = df[col].replace([np.inf,  -np.inf], 0)

    return df



def normalize_external_system(value, instance: str = '') -> str:
    """Map AOS Production System Name (or sheet instance) to Ad Ops labels."""
    raw = '' if value is None or (isinstance(value, float) and np.isnan(value)) else str(value).strip()
    key = raw.lower()
    mapping = {
        'google ad manager': 'GAM',
        'google ad manager - fox deportes': 'GAM',
        'gam': 'GAM',
        'freewheel': 'Freewheel',
        'free wheel': 'Freewheel',
        'megaphone': 'Megaphone',
        'amperwave': 'Amperwave',
        'spotify': 'Megaphone',
    }
    for needle, label in mapping.items():
        if needle in key:
            return label

    instance_key = (instance or '').strip().lower()
    instance_fallback = {
        'news': 'GAM',
        'sports': 'GAM',
        'gam pg': 'GAM',
        'fw pg': 'Freewheel',
        'amperwave': 'Amperwave',
        'spotify': 'Megaphone',
    }
    if instance_key in instance_fallback:
        return instance_fallback[instance_key]
    return raw


def format_news_pacing(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    # Filter the lines for the current year
    df = df[df['Line Item End Date'] >= report_date.replace(month=1, day=1)].copy()

    if 'Placement ID' not in df.columns and df.index.name == 'Placement ID':
        df = df.reset_index()

    # AOS fields arrive via Operative_OMS merge in merge_news_delivery_data
    if 'AOS Deal ID' not in df.columns:
        df['AOS Deal ID'] = df['Sales Order ID'] if 'Sales Order ID' in df.columns else None
    if 'AOS Deal Line ID' not in df.columns:
        df['AOS Deal Line ID'] = df['Sales Line Item ID'] if 'Sales Line Item ID' in df.columns else None
    if 'External Ad ID' not in df.columns:
        df['External Ad ID'] = df['Placement ID'] if 'Placement ID' in df.columns else None
    if 'Campaign Manager' not in df.columns:
        df['Campaign Manager'] = df['Trafficker/Campaign Manager'] if 'Trafficker/Campaign Manager' in df.columns else ''
    if 'Line Item Type' not in df.columns:
        df['Line Item Type'] = ''
    if 'External System' not in df.columns:
        prod_sys = df['Production System Name'] if 'Production System Name' in df.columns else ''
        instance = df['Instance'] if 'Instance' in df.columns else ''
        df['External System'] = [
            normalize_external_system(ps, inst)
            for ps, inst in zip(
                prod_sys if isinstance(prod_sys, pd.Series) else [prod_sys] * len(df),
                instance if isinstance(instance, pd.Series) else [instance] * len(df),
            )
        ]
    if 'Billable Third Party Server' not in df.columns:
        df['Billable Third Party Server'] = ''
    if 'Salesperson' not in df.columns and 'Primary Salesperson Full Name' in df.columns:
        df['Salesperson'] = df['Primary Salesperson Full Name']

    df = df.rename(columns={
        'Order': 'Ad Server Deal Name',
        'Line Item Name': 'Ad Server Line Item Name',
    })

    df = df.loc[:, REPORT_COLUMNS]

    df = df.dropna(subset=['Ad Server Line Item Name'])
    return df

PODCAST_COLUMN_MAP = {
    'advertiser_name': 'Advertiser',
    'deal_id': 'AOS Deal ID',
    'deal_name': 'Ad Server Deal Name',
    'deal_line_item_id': 'AOS Deal Line ID',
    'external_ad_id': 'External Ad ID',
    'deal_line_item_name': 'Ad Server Line Item Name',
    'deal_line_item_start_date': 'Line Item Start Date',
    'deal_line_item_end_date': 'Line Item End Date',
    'billable_third_party_descr': 'Billable Third Party Server',
    'net_unit_cost_amt': 'Rate',
    'production_quantity': 'Goal Quantity',
    'quantity': 'Contracted Quantity',
    'account_executive': 'Salesperson',
    'delivered_impressions': 'Ad Server Impressions',
 }

REPORT_COLUMNS = [
    'Instance',
    'Advertiser',
    'AOS Deal ID',
    'Ad Server Deal Name',
    'AOS Deal Line ID',
    'External Ad ID',
    'Ad Server Line Item Name',
    'Line Item Type',
    'External System',
    'Line Item Start Date',
    'Line Item End Date',
    'Billable Third Party Server',
    'Rate',
    'Goal Quantity',
    'Delivery Indicator',
    'Salesperson',
    'Campaign Manager',
    # Rest of columns stay the same
    'Contracted Quantity',
    'Ad Server Impressions',
    'Impressions (3rd Party)',
    'Clicks (3rd Party)',
    '3rd Party CTR',
    'Buffer',
    'Discrepancy',
    'Current First Party OSI',
    'Current Third Party OSI',
    'Total Error Count',
    'Total Error Rate',
 ]


def format_podcast_pacing(feed_df: pd.DataFrame, instance_label: str) -> pd.DataFrame:
    # Amperwave / Megaphone (Spotify) gold tables already join AOS attributes
    df = feed_df.rename(columns=PODCAST_COLUMN_MAP)
    for col in ['Rate', 'Goal Quantity', 'Contracted Quantity', 'Ad Server Impressions']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0
    for col in ['AOS Deal ID', 'AOS Deal Line ID', 'External Ad ID', 'Billable Third Party Server',
                'Ad Server Deal Name', 'Ad Server Line Item Name', 'Advertiser', 'Salesperson']:
        if col not in df.columns:
            df[col] = ''
    df['Line Item Start Date'] = pd.to_datetime(df['Line Item Start Date'], errors='coerce')
    df['Line Item End Date'] = pd.to_datetime(df['Line Item End Date'], errors='coerce')
    df['Instance'] = instance_label
    df['Delivery Indicator'] = ''
    df['Line Item Type'] = ''  # GAM-only
    df['Campaign Manager'] = ''  # not on amperwave/megaphone gold tables today
    df['External System'] = normalize_external_system(None, instance_label)
    df['Quantity'] = df['Goal Quantity']
    df['Total Impressions'] = df['Ad Server Impressions']
    df['Impressions (3rd Party)'] = 0
    df['Clicks (3rd Party)'] = 0
    df['Total Error Count'] = 0
    return df

    
def news_lifetime_delivery(ad_juster_pacing_news: str, report_date: datetime) -> pd.DataFrame:
    global drop_bucket,amperwave_delta_table, megaphone_delta_table
    s3_dir = f's3://{drop_bucket}/'
    operative_filename = 'Operative_OMS.parquet'
    operative = s3_dir + 'processed/' + operative_filename
    gam_file_name = 'GAM_Lifetime_Delivery.parquet'
    sports_gam_file_name = 'Sports_GAM_Lifetime_Delivery.parquet'
    gam_file = s3_dir + 'processed/' + gam_file_name
    sports_gam_file = s3_dir + 'processed/' + sports_gam_file_name
    adops_lifetime_filename = 'AdOps_Reporting_Lifetime_Delivery.parquet'
    adops_lifetime = s3_dir + 'processed/' + adops_lifetime_filename

    op_oms = pd.read_parquet(operative, storage_options={'profile': aws_profile})
    op_fw_adj = pd.read_parquet(
        adops_lifetime,
        columns=[
            'Placement ID',  # required to join FW_PG_Lifetime_Delivery (Deal ID)
            'Sales Line Item ID',
            'Invoice Organization Name',
            'Advertiser Name',
            'Campaign Name',
            'Sales Order Name',
            'Sales Line Item Name',
            'Sales Line Item Start Date',
            'Sales Line Item End Date',
            'Net Unit Cost',
            'Quantity',
            'Push Quantity',
            'Primary Salesperson Full Name',
            'Gross Counted Ads',
            'Net Counted Ads',
        ],
        storage_options={'profile': aws_profile}
    )
   


    # Gold tables already join AOS (deal_id, external_ad_id, billable_third_party_descr, etc.)
    PODCAST_AGG_COLUMNS = [
    F.first('advertiser_name', ignorenulls=True).alias('advertiser_name'),
    F.first('deal_id', ignorenulls=True).alias('deal_id'),
    F.first('deal_name', ignorenulls=True).alias('deal_name'),
    F.first('deal_line_item_id', ignorenulls=True).alias('deal_line_item_id'),
    F.first('external_ad_id', ignorenulls=True).alias('external_ad_id'),
    F.first('deal_line_item_name', ignorenulls=True).alias('deal_line_item_name'),
    F.first('deal_line_item_start_date', ignorenulls=True).alias('deal_line_item_start_date'),
    F.first('deal_line_item_end_date', ignorenulls=True).alias('deal_line_item_end_date'),
    F.first('billable_third_party_descr', ignorenulls=True).alias('billable_third_party_descr'),
    F.first('net_unit_cost_amt', ignorenulls=True).alias('net_unit_cost_amt'),
    F.first('production_quantity', ignorenulls=True).alias('production_quantity'),
    F.first('quantity', ignorenulls=True).alias('quantity'),
    F.first('account_executive', ignorenulls=True).alias('account_executive'),
    F.sum(F.col('delivered_impressions').cast('double')).alias('delivered_impressions'),
    ]

    amperwave_data = (
        spark.read.table(amperwave_delta_table)
        .filter(F.year(F.col('event_date')) == report_date.year)
        .groupBy('deal_line_item_id')
        .agg(*PODCAST_AGG_COLUMNS)
        .toPandas()
    )
    megaphone_data = (
        spark.read.table(megaphone_delta_table)
        .filter(F.year(F.col('event_date')) == report_date.year)
        .groupBy('deal_line_item_id')
        .agg(*PODCAST_AGG_COLUMNS)
        .toPandas()
    )
    gam = read_gam_lifetime(gam_file, sports_gam_file, op_oms)
    adjuster_news = read_adjuster_news(ad_juster_pacing_news, op_oms, skiprows=0)

    # Append PG freewheel data (drop is inside apply_pg_billing_news; thin read has no PG Impressions)
    op_fw_adj = apply_pg_billing_news(op_fw_adj, report_date)

    ###########################################################################
    # Write the processed into its own year file
    with io.BytesIO() as output:
        adjuster_news.to_parquet(output, index=False)
        adjuster_news_data = output.getvalue()

    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    s3_bucket = drop_bucket
    s3_key = 'processed/' + f'adjuster_news_data_{report_date.year}.parquet'
    s3_client.put_object(Bucket=s3_bucket, Body=adjuster_news_data, Key=s3_key,)
    ###########################################################################

    amperwave_report = calculate_metrics(format_podcast_pacing(amperwave_data, 'Amperwave'), report_date,is_podcast = True).loc[:, REPORT_COLUMNS]
    megaphone_report = calculate_metrics(format_podcast_pacing(megaphone_data, 'Spotify'), report_date, is_podcast = True).loc[:, REPORT_COLUMNS]

    op_gam_fw_adj = merge_news_delivery_data(op_oms, gam, adjuster_news, op_fw_adj)

    op_gam_fw_adj = calculate_metrics(op_gam_fw_adj, report_date)

    return op_gam_fw_adj, amperwave_report, megaphone_report

def export_news_lifetime_delivery(op_gam_fw_adj: pd.DataFrame, report_date: datetime) -> str:
    with io.BytesIO() as output:
        op_gam_fw_adj.to_parquet(output, index=False)
        op_gam_fw_adj_data = output.getvalue()

    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    global drop_bucket
    s3_bucket = drop_bucket
    s3_key = 'processed/' + f'AdOps_News_Reporting_Lifetime_Delivery_{report_date.year}.parquet'
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=op_gam_fw_adj_data,
        Key=s3_key,
    )

    return s3_key

def export_news_pacing_report(op_gam_fw_adj: pd.DataFrame,amperwave_report,megaphone_report, report_date: datetime) -> str:
    news_pacing_report_filename = 'News_Pacing_Report_' + report_date.strftime('%Y%m%d')
    global drop_bucket
    op_gam_fw_adj = format_news_pacing(op_gam_fw_adj, report_date)

    news_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'News'].drop(columns=['Instance'])
    sports_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'Sports'].drop(columns=['Instance'])
    # gam_pg_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'GAM PG'].drop(columns=['Instance'])
    fw_pg_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'FW PG'].drop(columns=['Instance'])
    amperwave_report = amperwave_report.drop(columns=['Instance'], errors='ignore')
    megaphone_report = megaphone_report.drop(columns=['Instance'], errors='ignore')

    with io.BytesIO() as output:
        with pd.ExcelWriter(
            output,
            datetime_format='MM/dd/yyyy',
            engine='xlsxwriter',
            options={'strings_to_numbers': True},
        ) as writer:
            workbook = writer.book
            pct_fmt = workbook.add_format({'num_format': '0.00%'})
            report_pct_cols = ['3rd Party CTR', 'Buffer', 'Discrepancy', 'Current First Party OSI', 'Current Third Party OSI']

            news_report.to_excel(writer, sheet_name='News GAM Pacing Report', index=False)
            worksheet = writer.sheets['News GAM Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                news_report,
                report_pct_cols
            )

            sports_report.to_excel(writer, sheet_name='Sports GAM Pacing Report', index=False)
            worksheet = writer.sheets['Sports GAM Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                news_report,
                report_pct_cols
            )
            
            amperwave_report.to_excel(writer, sheet_name='Amperwave Pacing Report', index=False)
            worksheet = writer.sheets['Amperwave Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                amperwave_report,
                report_pct_cols
            )

            megaphone_report.to_excel(writer, sheet_name='Spotify Pacing Report', index=False)
            worksheet = writer.sheets['Spotify Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                megaphone_report,
                report_pct_cols
            )

            # gam_pg_report.to_excel(writer, sheet_name='GAM PG Pacing Report', index=False)
            # worksheet = writer.sheets['GAM PG Pacing Report']
            # set_col_fmt(
            #     pct_fmt,
            #     worksheet,
            #     gam_pg_report,
            #     report_pct_cols
            # )

            fw_pg_report.to_excel(writer, sheet_name='FW PG Pacing Report', index=False)
            worksheet = writer.sheets['FW PG Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                fw_pg_report,
                report_pct_cols
            )
        news_pacing_report_data = output.getvalue()

    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    s3_bucket = drop_bucket
    s3_key = 'reports/' + news_pacing_report_filename + '.xlsx'
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=news_pacing_report_data,
        Key=s3_key,
    )

    return s3_key

# COMMAND ----------

def adops_news_lifetime_delivery(date: datetime, staq_file: str) -> None:
    global drop_bucket
    data_dir = f's3://{staq_bucket}/'
    ad_juster_pacing_news = data_dir + staq_file
    # ad_juster_pacing_news = f's3://fdp-staq-web-identity-bucket-test/STAQ_Adjuster_Pacing_YTD_All_Lines_2026-03-02_13-05-48.csv'
    report_date = datetime.combine(date, datetime.min.time())
    news_lifetime_delivery_data, amperwave_report, megaphone_report = news_lifetime_delivery(ad_juster_pacing_news, report_date)
    export_news_lifetime_delivery(news_lifetime_delivery_data, report_date)
    out = export_news_pacing_report(
        news_lifetime_delivery_data,
        amperwave_report,
        megaphone_report,
        report_date,
    )
    write_xcom_value('AdOps News Lifetime Delivery', str(out))

# COMMAND ----------

if __name__ == '__main__':
    date = dbutils.widgets.get('date')
    global drop_bucket, aws_profile, amperwave_delta_table, megaphone_delta_table
    drop_bucket = dbutils.widgets.get('drop_bucket')
    aws_profile = dbutils.widgets.get('aws_profile')
    staq_bucket = dbutils.widgets.get('staq_bucket')
    amperwave_delta_table = dbutils.widgets.get('amperwave_delta_table')
    megaphone_delta_table = dbutils.widgets.get('megaphone_delta_table')

    def parse_date(date: str) -> bool:
        format = "%Y-%m-%d"
        try:
            res = bool(datetime.strptime(date, format))
        except ValueError:
            res = False
        return res

    assert parse_date(date), "Invalid date format, should be in YYYY-MM-DD format"

    report_date = datetime.strptime(date, "%Y-%m-%d")

    #adjuster_file = 'adjuster/Adjuster_Pacing_News_' + report_date.strftime("%Y%m%d") + '.csv.gz'
    staq_file = find_staq_file(report_date)
    print(staq_file)

    try:
        adops_news_lifetime_delivery(report_date, staq_file)
    except Exception as error:
        alert = Alert()
        #alert.send('AdOps News Lifetime Delivery', f'{type(error).__name__}: {error}')
        #dbutils.notebook.exit(f'ERROR!!! - {error}')
        error_details = traceback.format_exc()
        dbutils.notebook.exit(error_details)