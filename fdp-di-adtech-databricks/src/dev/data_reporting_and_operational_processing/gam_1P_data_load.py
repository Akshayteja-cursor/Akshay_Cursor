# Databricks notebook source
# COMMAND ----------

# MAGIC %run "/Workspace/Users/prajwal.harikishor@fox.com/fdp-di-adtech-databricks/src/dev/data_reporting_and_operational_processing/alert"

# COMMAND ----------

from typing import Any
from googleads import ad_manager, oauth2, errors
import json
import tempfile
import os
from datetime import datetime, timedelta
import boto3
import pandas as pd
import numpy as np
import s3fs

# COMMAND ----------

APPLICATION_NAME = 'DROP - GAM Ingestion'
NETWORK_CODE_NEWS = 4145
NETWORK_CODE_SPORTS = 20893548

def get_ad_manager_client(secret_arn: str, NETWORK_CODE: int) -> Any:
    session = boto3.Session(profile_name=aws_profile)
    resp = session.client('secretsmanager').get_secret_value(SecretId=secret_arn)
    secret = json.loads(resp['SecretString'])

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as secret_file:
        json.dump(secret, secret_file, indent=2)
        secret_file.close()

    oauth2_client = oauth2.GoogleServiceAccountClient(secret_file.name, oauth2.GetAPIScope('ad_manager'))
    os.unlink(secret_file.name)
    ad_manager_client = ad_manager.AdManagerClient(oauth2_client, APPLICATION_NAME, NETWORK_CODE)

    return ad_manager_client

def pull_gam_report(client: Any, report_id: str, start_date: datetime, end_date: datetime, output_date:datetime, instance: str) -> str:
    filename = instance + '_GAM_Report' + output_date.strftime('_%Y%m%d') + '.csv.gz'

    # Initialize appropriate service.
    report_service = client.GetService('ReportService', version='v202602')
    
    # Initialize a DataDownloader.
    report_downloader = client.GetDataDownloader(version='v202602')
    
    # Create statement object to filter for an order.
    statement = (ad_manager.StatementBuilder(version='v202602')
                 .Where('id = :id')
                 .WithBindVariable('id', int(report_id)))
    
    response = report_service.getSavedQueriesByStatement(statement.ToStatement())

    print(response)
    report_downloaded = False

    if 'results' in response and len(response['results']):
        saved_query = response['results'][0]
    
        if saved_query['isCompatibleWithApiVersion']:
            report_job = {}

            report_job['reportQuery'] = saved_query['reportQuery']

            report_job['reportQuery']['startDate']['year'] = start_date.year
            report_job['reportQuery']['startDate']['month'] = start_date.month
            report_job['reportQuery']['startDate']['day'] = start_date.day
            report_job['reportQuery']['endDate']['year'] = end_date.year
            report_job['reportQuery']['endDate']['month'] = end_date.month
            report_job['reportQuery']['endDate']['day'] = end_date.day

            try:
                # Run the report and wait for it to finish.
                report_job_id = report_downloader.WaitForReport(report_job)
            except errors.AdManagerReportError as e:
                print('Failed to generate report. Error was: %s' % e)
        
            # Change to your preferred export format.
            export_format = 'CSV_DUMP'

            # Download report data.
            global drop_bucket
            s3 = s3fs.S3FileSystem(profile=aws_profile)
            s3_bucket = drop_bucket
            report_dir = 'gam/' if instance == 'News' else 'gam_sports/'
            

            with s3.open(f'{s3_bucket}/{report_dir}{filename}', 'wb') as report_file:
                report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)

            print('File write completed')

            report_downloaded = True
    
    if report_downloaded:
        return filename
    else:
        raise Exception('Unable to download gam report')

def update_daily_breakouts(filename: str, instance: str) -> list:
    global drop_bucket

    if instance == 'News':
        gam = pd.read_csv(f's3://{drop_bucket}/gam/{filename}', storage_options={'profile': aws_profile})
        daily_file_path = 's3://{drop_bucket}/gam/daily/dt={dt}/gam_daily_{dt}.parquet'
    else:
        gam = pd.read_csv(f's3://{drop_bucket}/gam_sports/{filename}', storage_options={'profile': aws_profile})
        gam.loc[:, 'CF[376]_Value'] = ''
        daily_file_path = 's3://{drop_bucket}/gam_sports/daily/dt={dt}/gam_daily_{dt}.parquet'

    to_be_renamed_dict ={
        'Dimension.ADVERTISER_NAME': 'Advertiser Name',
        'Dimension.DATE': 'Date',
        'Dimension.LINE_ITEM_NAME': 'Line Item Name',
        'Dimension.ORDER_NAME': 'Order Name',
        'Dimension.ADVERTISER_ID': 'Advertiser ID',
        'Dimension.LINE_ITEM_ID': 'Line Item ID',
        'Dimension.ORDER_ID': 'Order ID',
        'DimensionAttribute.LINE_ITEM_START_DATE_TIME': 'Line Item Start Date',
        'DimensionAttribute.LINE_ITEM_END_DATE_TIME': 'Line Item End Date',
        'DimensionAttribute.LINE_ITEM_COST_PER_UNIT': 'Rate',
        'DimensionAttribute.LINE_ITEM_GOAL_QUANTITY': 'Goal Quantity',
        'DimensionAttribute.LINE_ITEM_CONTRACTED_QUANTITY': 'Contracted Quantity',
        'DimensionAttribute.LINE_ITEM_DELIVERY_INDICATOR': 'Delivery Indicator',
        'DimensionAttribute.ORDER_SALESPERSON': 'Salesperson',
        'DimensionAttribute.ORDER_TRAFFICKER': 'Trafficker',
        'Dimension.LINE_ITEM_TYPE': 'Line Item Type',
        'CF[376]_Value': 'Name Comments',
        'Column.AD_SERVER_IMPRESSIONS': 'Ad Server Impressions',
        'Column.AD_SERVER_CLICKS': 'Ad Server Clicks',
        'Column.VIDEO_VIEWERSHIP_COMPLETE': 'Complete',
        'Column.VIDEO_VIEWERSHIP_COMPLETION_RATE': 'Completion Rate',
        'Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT': 'Total Error Count',
        'Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE': 'Total Error Rate',
        'Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS': 'Total Impressions'
    }

    gam = gam.rename(to_be_renamed_dict, axis=1)
    gam = gam[gam['Line Item ID'] != -1]
    gam['Date'] = pd.to_datetime(gam['Date'], format='%Y-%m-%d')
    gam.loc[:, 'Line Item Start Date'] = np.where(gam['Line Item Start Date'] == '-',
                                                  None, gam['Line Item Start Date'].str[:10])
    gam.loc[:, 'Line Item End Date'] = np.where(gam['Line Item End Date'] == 'Unlimited',
                                                '2100-01-01',
                                                np.where(gam['Line Item End Date'] == '-',
                                                         None, gam['Line Item End Date'].str[:10]))
    gam['Line Item Start Date'] = pd.to_datetime(gam['Line Item Start Date'], format='%Y-%m-%d')
    gam['Line Item End Date'] = pd.to_datetime(gam['Line Item End Date'], format='%Y-%m-%d')
    gam['Delivery Indicator'] = gam['Delivery Indicator'].astype(str)
    gam['Goal Quantity'] = gam['Goal Quantity'].fillna('')

    dates=[]

    for date, df in gam.groupby('Date'):
        dt = date.strftime('%Y-%m-%d')
        dates.append(dt)
        df.to_parquet(daily_file_path.format(drop_bucket=drop_bucket, dt=dt), compression='gzip', storage_options={'profile': aws_profile})
        print(f'Updated partition dt={dt}')

    return dates

def update_lifetime(partitions, instance) -> None:
    global drop_bucket

    if instance == 'News':
        lifetime_path = f's3://{drop_bucket}/processed/GAM_Lifetime_Delivery.parquet'
        daily_file_path = 's3://{drop_bucket}/gam/daily/dt={partition}/gam_daily_{partition}.parquet'
    else:
        lifetime_path = f's3://{drop_bucket}/processed/Sports_GAM_Lifetime_Delivery.parquet'
        daily_file_path = 's3://{drop_bucket}/gam_sports/daily/dt={partition}/gam_daily_{partition}.parquet'

    updated = []

    for partition in partitions:
        latest = pd.read_parquet(daily_file_path.format(drop_bucket=drop_bucket, partition=partition), storage_options={'profile': aws_profile})
        latest = latest.set_index(['Date', 'Line Item ID', 'Order ID'])
        updated.append(latest)

    updated_data = pd.concat(updated)

    try:
        lifetime_file = pd.read_parquet(lifetime_path, storage_options={'profile': aws_profile})
        lifetime_file = lifetime_file.set_index(['Date', 'Line Item ID', 'Order ID'])
        lifetime_file = pd.concat([lifetime_file[~lifetime_file.index.isin(updated_data.index)], updated_data])
        lifetime_file = lifetime_file.reset_index()
        lifetime_file.to_parquet(lifetime_path, storage_options={'profile': aws_profile})
    except:
        updated_data = updated_data.reset_index()
        updated_data.to_parquet(lifetime_path, storage_options={'profile': aws_profile})

    print(f'Updated for dates - ', partitions)

# COMMAND ----------

def gam_update_daily(gam_service_account_creds_secret_arn: str, report_id: str, start_date: datetime, end_date: datetime, output_date: datetime, instance: str) -> None:
    NETWORK_CODE = NETWORK_CODE_NEWS if instance == 'News' else NETWORK_CODE_SPORTS
    client = get_ad_manager_client(gam_service_account_creds_secret_arn, NETWORK_CODE)
    curr_filename = pull_gam_report(client, report_id, start_date, end_date, output_date, instance)
    updated_partitions = update_daily_breakouts(curr_filename, instance)
    update_lifetime(updated_partitions, instance)

# COMMAND ----------

if __name__ == '__main__':
    report_date = dbutils.widgets.get('date')
    global drop_bucket, aws_profile
    drop_bucket = dbutils.widgets.get('drop_bucket')
    aws_profile = dbutils.widgets.get('aws_profile')
    report_id = dbutils.widgets.get('report_id')
    gam_service_account_creds_secret_arn = dbutils.widgets.get('gam_service_account_creds_secret_arn')
    instance = dbutils.widgets.get('instance')
    full_year_load = dbutils.widgets.get('full_year_load')

    def parse_date(date: str) -> bool:
        format = "%Y-%m-%d"
        try:
            res = bool(datetime.strptime(date, format))
        except ValueError:
            res = False
        return res

    assert parse_date(report_date), "Invalid date format, should be in YYYY-MM-DD format"
    assert report_id != '', "Report ID is required"
    assert gam_service_account_creds_secret_arn != '', "Invalid gam_service_account_creds_secret_arn"
    assert instance != '', "Invalid instance"
    assert full_year_load != '', "Invalid full_year_load"

    report_date = datetime.strptime(report_date, "%Y-%m-%d")
    if full_year_load == 'true':
        start_date = (report_date.replace(month=1).replace(day=1))
    else:
        if report_date.day <= 5:
            start_date = (report_date.replace(month=report_date.month-1).replace(day=1) if report_date.month > 1 else report_date.replace(year=report_date.year-1).replace(month=12).replace(day=1))
        else:
            start_date = (report_date.replace(day=1)) 

    end_date = report_date - timedelta(days=1)

    try:
        gam_update_daily(gam_service_account_creds_secret_arn, report_id, start_date, end_date, report_date, instance)
    except Exception as error:
        alert = Alert()
        alert.send('GAM Update Daily', f'{type(error).__name__}: {error}')
        dbutils.notebook.exit(f'ERROR!!! - {error}')