# Databricks notebook source
# MAGIC %skip
# MAGIC %run "/Workspace/Users/prajwal.harikishor@fox.com/fdp-di-adtech-databricks/src/dev/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/prajwal.harikishor@fox.com/fdp-di-adtech-databricks/src/dev/data_reporting_and_operational_processing/alert"

# COMMAND ----------

# MAGIC
# MAGIC %pip install s3fs
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/kranthi.kommineni@fox.com/fdp-di-adtech-databricks-dev/src/dev/data_reporting_and_operational_processing/core"
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
import io
import boto3
import pandas as pd
import numpy as np
import traceback
import s3fs  # enables pandas/fsspec access to s3:// paths


# COMMAND ----------

dbutils.widgets.text("date", datetime.today().strftime("%Y-%m-%d"), "1. Report date (YYYY-MM-DD)")
dbutils.widgets.text("drop_bucket", "", "2. Drop bucket")
dbutils.widgets.text("staq_bucket", "", "3. STAQ bucket")
dbutils.widgets.text("aws_profile", "", "4. AWS profile")
dbutils.widgets.text("amperwave_delta_table", "fox_bi_qa.gold_ad_sales.ft_amperwave_campaign_delivery", "5. Amperwave table")
dbutils.widgets.text("megaphone_delta_table", "fox_bi_qa.gold_ad_sales.ft_megaphone_campaign_delivery", "6. Megaphone table")

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

# DBTITLE 1,Cell 9
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

# Calculate OSI for all platforms except podcasts :07/28/2026
#They would like the OSI fields (Columns R & S) populated, but it’s not like FW or GAM where the percentage can be pulled from the UI. Therefore, they provided us with the formula they would like implemented specifically for Amperwave and Spotify.

def podcast_osi(row: pd.Series, report_date: datetime) -> float:
    """
    Calculate OSI specifically for Amperwave and Spotify.

    Formula:
        (Delivered-to-date impressions / Contracted impressions)
        /
        (Days flight has been live / Total flight days)

    Current First Party OSI and Current Third Party OSI will both
    use this calculated value for podcast platforms.
    """

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

    # Return 0 when required values are missing or invalid.
    if pd.isna(delivered_impressions):
        return 0

    if pd.isna(contracted_impressions) or contracted_impressions <= 0:
        return 0

    if pd.isna(start_date) or pd.isna(end_date):
        return 0

    # Invalid flight date range.
    if end_date < start_date:
        return 0

    # Flight has not started yet.
    if report_date < start_date:
        return 0

    # Total flight days, including start and end dates.
    total_flight_days = (
        end_date - start_date
    ).days + 1

    if total_flight_days <= 0:
        return 0

    # Completed flights should stop counting at the end date.
    effective_date = min(report_date, end_date)

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
        # Amperwave / Spotify have no 3rd-party measurement, so both OSI
        # columns (R & S) are populated with the Ad Ops provided formula.
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

def format_news_pacing(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    # Filter the lines for the current year
    df = df[df['Line Item End Date'] >= report_date.replace(month=1, day=1)]
    df = df.loc[:,
                [
                    'Instance',
                    'Advertiser',
                    'Order',
                    'Line Item Name',
                    'Line Item Start Date',
                    'Line Item End Date',
                    'Rate',
                    'Goal Quantity',
                    'Contracted Quantity',
                    'Delivery Indicator',
                    'Primary Salesperson Full Name',
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
                ]]
    
    df = df.dropna(subset=['Line Item Name'])
    df = df.rename(columns={'Primary Salesperson Full Name': 'Salesperson'})

    return df
#==========================Verify=====================================================
PODCAST_COLUMN_MAP = {
    'advertiser_name': 'Advertiser',
    'deal_name': 'Order',
    'deal_line_item_name': 'Line Item Name',
    'deal_line_item_start_date': 'Line Item Start Date',
    'deal_line_item_end_date': 'Line Item End Date',
    'net_unit_cost_amt': 'Rate',
    'production_quantity': 'Goal Quantity',
    'quantity': 'Contracted Quantity',
    'account_executive': 'Salesperson',
    'delivered_impressions': 'Ad Server Impressions',
 }

REPORT_COLUMNS = [
    'Instance', 'Advertiser', 'Order', 'Line Item Name',
    'Line Item Start Date', 'Line Item End Date', 'Rate',
    'Goal Quantity', 'Contracted Quantity', 'Delivery Indicator', 'Salesperson',
    'Ad Server Impressions', 'Impressions (3rd Party)', 'Clicks (3rd Party)',
    '3rd Party CTR', 'Buffer', 'Discrepancy',
    'Current First Party OSI', 'Current Third Party OSI',
    'Total Error Count', 'Total Error Rate',
 ]


def format_podcast_pacing(feed_df: pd.DataFrame, instance_label: str) -> pd.DataFrame:
    df = feed_df.rename(columns=PODCAST_COLUMN_MAP)
    for col in ['Rate', 'Goal Quantity', 'Contracted Quantity', 'Ad Server Impressions']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['Line Item Start Date'] = pd.to_datetime(df['Line Item Start Date'], errors='coerce')
    df['Line Item End Date'] = pd.to_datetime(df['Line Item End Date'], errors='coerce')
    df['Instance'] = instance_label
    df['Delivery Indicator'] = ''
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
   

# Read Amperwave and Megaphone data from data from delta table
    # Check with noor for the table names

    PODCAST_AGG_COLUMNS = [
    F.first('advertiser_name', ignorenulls=True).alias('advertiser_name'),
    F.first('deal_name', ignorenulls=True).alias('deal_name'),
    F.first('deal_line_item_name', ignorenulls=True).alias('deal_line_item_name'),
    F.first('deal_line_item_start_date', ignorenulls=True).alias('deal_line_item_start_date'),
    F.first('deal_line_item_end_date', ignorenulls=True).alias('deal_line_item_end_date'),
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

    ########################report generation using new helper function###################################################
    amperwave_report = calculate_metrics(format_podcast_pacing(amperwave_data, 'Amperwave'), report_date, is_podcast=True).loc[:, REPORT_COLUMNS]
    megaphone_report = calculate_metrics(format_podcast_pacing(megaphone_data, 'Spotify'), report_date, is_podcast=True).loc[:, REPORT_COLUMNS]

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

def export_news_pacing_report(op_gam_fw_adj: pd.DataFrame,amperwave_report,megaphone_report,report_date: datetime) -> str:
    news_pacing_report_filename = 'News_Pacing_Report_' + report_date.strftime('%Y%m%d')
    global drop_bucket
    op_gam_fw_adj = format_news_pacing(op_gam_fw_adj, report_date)

    news_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'News'].drop(columns=['Instance'])
    sports_report = op_gam_fw_adj[op_gam_fw_adj['Instance'] == 'Sports'].drop(columns=['Instance'])


    with io.BytesIO() as output:
        with pd.ExcelWriter(
            output,
            datetime_format='MM/dd/yyyy',
            engine='xlsxwriter',
            # options={'strings_to_numbers': True},
            engine_kwargs={'options': {'strings_to_numbers': True}}, #localrun#
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
    news_lifetime_delivery_data,amperwave_report,megaphone_report = news_lifetime_delivery(ad_juster_pacing_news, report_date)
    export_news_lifetime_delivery(news_lifetime_delivery_data, report_date)
    out = export_news_pacing_report(news_lifetime_delivery_data, amperwave_report, megaphone_report, report_date)
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
        # alert = Alert()
        #alert.send('AdOps News Lifetime Delivery', f'{type(error).__name__}: {error}')
        #dbutils.notebook.exit(f'ERROR!!! - {error}')
        # error_details = traceback.format_exc()
        # dbutils.notebook.exit(error_details)
        print(error)