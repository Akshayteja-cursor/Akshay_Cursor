# Databricks notebook source
# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/alert"

# COMMAND ----------

from datetime import datetime, timedelta
import io
import boto3
import pandas as pd
import numpy as np

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
    
def metrics_calculaition(row: pd.Series, calculation: str) -> float:
    if calculation == 'Total Error Rate':
        if row['Total Impressions'] + row['Total Error Count'] == 0:
            return 0
        return row['Total Error Count']/(row['Total Impressions']+row['Total Error Count'])
    
    if calculation == '3rd Party CTR':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return row['Clicks (3rd Party)']/row['Impressions (3rd Party)']
    
    if calculation == 'Buffer':
        if row['Contracted Quantity'] == 0:
            return 0
        return (row['Ad Server Booked Impressions']-row['Contracted Quantity'])/row['Contracted Quantity']
    
    if calculation == 'Discrepancy':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return (row['Impressions (3rd Party)']-row['Ad Server Impressions'])/row['Impressions (3rd Party)']


def calculate_metrics(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    df.loc[:, 'Total Error Rate'] = df.apply(metrics_calculaition, args=('Total Error Rate', ), axis=1)
    df.loc[:, '3rd Party CTR'] = df.apply(metrics_calculaition, args=('3rd Party CTR', ), axis=1)
    df.loc[:, 'Ad Server Booked Impressions'] = df['Quantity']
    df.loc[:, 'Buffer'] = df.apply(metrics_calculaition, args=('Buffer', ), axis=1)
    df.loc[:, 'Discrepancy'] = df.apply(metrics_calculaition, args=('Discrepancy', ), axis=1)
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


REPORT_COLUMNS = [
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
        prod_sys = df['Production System Name'] if 'Production System Name' in df.columns else pd.Series([''] * len(df), index=df.index)
        instance = df['Instance'] if 'Instance' in df.columns else pd.Series([''] * len(df), index=df.index)
        df['External System'] = [
            normalize_external_system(ps, inst)
            for ps, inst in zip(prod_sys, instance)
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
 
def news_lifetime_delivery(ad_juster_pacing_news: str, report_date: datetime) -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    operative_filename = 'Operative_OMS.parquet'
    operative = s3_dir + 'processed/' + operative_filename
    gam_file_name = 'GAM_Lifetime_Delivery.parquet'
    gam_file = s3_dir + 'processed/' + gam_file_name
    adops_lifetime_filename = 'AdOps_Reporting_Lifetime_Delivery.parquet'
    adops_lifetime = s3_dir + 'processed/' + adops_lifetime_filename

    gam = read_gam_lifetime(gam_file)
    adjuster_news = read_adjuster_news(ad_juster_pacing_news, skiprows=8)
    op_oms = pd.read_parquet(operative, storage_options={'profile': aws_profile})
    op_fw_adj = pd.read_parquet(adops_lifetime, storage_options={'profile': aws_profile})

    op_gam_fw_adj = merge_news_delivery_data(op_oms, gam, adjuster_news, op_fw_adj)
    

    op_gam_fw_adj = calculate_metrics(op_gam_fw_adj, report_date)

    return op_gam_fw_adj

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

def export_news_pacing_report(op_gam_fw_adj: pd.DataFrame, report_date: datetime) -> str:
    news_pacing_report_filename = 'News_Pacing_Report_' + report_date.strftime('%Y%m%d')

    op_gam_fw_adj = format_news_pacing(op_gam_fw_adj, report_date)

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

            op_gam_fw_adj.to_excel(writer, sheet_name='News Pacing Report', index=False)
            worksheet = writer.sheets['News Pacing Report']
            set_col_fmt(
                pct_fmt,
                worksheet,
                op_gam_fw_adj,
                report_pct_cols
            )

        news_pacing_report_data = output.getvalue()

    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    global drop_bucket
    s3_bucket = drop_bucket
    s3_key = 'reports/' + news_pacing_report_filename + '.xlsx'
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=news_pacing_report_data,
        Key=s3_key,
    )

    return s3_key

# COMMAND ----------

def adops_news_lifetime_delivery(date: datetime, adjuster_file: str) -> None:
    global drop_bucket
    data_dir = f's3://{drop_bucket}/'
    ad_juster_pacing_news = data_dir + adjuster_file
    report_date = datetime.combine(date, datetime.min.time())
    news_lifetime_delivery_data = news_lifetime_delivery(ad_juster_pacing_news, report_date)
    export_news_lifetime_delivery(news_lifetime_delivery_data, report_date)
    out = export_news_pacing_report(news_lifetime_delivery_data, report_date)
    write_xcom_value('AdOps News Lifetime Delivery', str(out))

# COMMAND ----------

if __name__ == '__main__':
    date = dbutils.widgets.get('date')
    global drop_bucket, aws_profile
    drop_bucket = dbutils.widgets.get('drop_bucket')
    aws_profile = dbutils.widgets.get('aws_profile')

    def parse_date(date: str) -> bool:
        format = "%Y-%m-%d"
        try:
            res = bool(datetime.strptime(date, format))
        except ValueError:
            res = False
        return res

    assert parse_date(date), "Invalid date format, should be in YYYY-MM-DD format"

    report_date = datetime.strptime(date, "%Y-%m-%d")

    adjuster_file = 'adjuster/Adjuster_Pacing_News_' + report_date.strftime("%Y%m%d") + '.csv.gz'

    try:
        adops_news_lifetime_delivery(report_date, adjuster_file)
    except Exception as error:
        alert = Alert()
        alert.send('AdOps News Lifetime Delivery', f'{type(error).__name__}: {error}')
        dbutils.notebook.exit(f'ERROR!!! - {error}')