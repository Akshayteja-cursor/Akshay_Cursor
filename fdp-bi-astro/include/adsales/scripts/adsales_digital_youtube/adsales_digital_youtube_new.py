import logging
import argparse, sys
import email
import os
from airflow.models import Variable
import zipfile
from io import BytesIO, StringIO
import re
import pandas as pd
from dags.config.adsales.adsales_digital_youtube import adsales_digital_youtube_config
from include.adsales.scripts.common.dbt_cloud_utils import DBTCloudUtils
from include.adsales.scripts.common.s3_service import *
import boto3
import json
from airflow.hooks.base import BaseHook

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

try:
    env = 'dev' if Variable.get('env') == 'local' else Variable.get('env')
except Exception as e:
    logging.warning(f"Could not get env from Airflow Variable: {e}, defaulting to 'dev'")
    env = 'dev'
config = adsales_digital_youtube_config[env]


def process_news_views_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"News views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read news views daily zip object")

    logging.info("Unzipping the news views daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the news views daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"News views daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"News views daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_news_views_daily')
    logging.info(f"News views daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the news views daily file under unzipped folder.")
    logging.info("Successfully process news views daily file.")


def process_news_views_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"News views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read news views monthly zip object")

    logging.info("Unzipping the news views monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the news views monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"News views monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"News views monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is news views monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is news views monthly file, successfully read the unzipped file.")
    file_df = pd.read_csv(file_obj['Body'])
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {'Channel': 'Unknown', 'Channel title': 'Unknown',
               'Views': total['Views'][0] - file_df.loc[file_df['Channel'] != 'Total', 'Views'].sum(),
               'Watch time (hours)': total['Watch time (hours)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Watch time (hours)'].sum(),
               'Average view duration': total['Average view duration'][0],
               'Estimated partner revenue (USD)': total['Estimated partner revenue (USD)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Estimated partner revenue (USD)'].sum()
               }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)

    logging.info("This file is news views monthly file, successfully appended new row.")
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is news views monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is news views monthly file, successfully added date & new row to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_news_views_monthly')
    logging.info(f"News views monthly loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the news views monthly file under unzipped folder.")
    logging.info("Successfully process news views monthly file.")


def process_entertainment_views_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Entertainment views monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read entertainment views monthly zip object")

    logging.info("Unzipping the entertainment views monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the entertainment views monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Entertainment views monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Entertainment views monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is entertainment views monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is entertainment views monthly file, successfully read the unzipped file.")
    file_df = pd.read_csv(file_obj['Body'])
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {'Channel': 'Unknown', 'Channel title': 'Unknown',
               'Views': total['Views'][0] - file_df.loc[file_df['Channel'] != 'Total', 'Views'].sum(),
               'Watch time (hours)': total['Watch time (hours)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Watch time (hours)'].sum(),
               'Average view duration': total['Average view duration'][0],
               'Estimated partner revenue (USD)': total['Estimated partner revenue (USD)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Estimated partner revenue (USD)'].sum()
               }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    logging.info("This file is entertainment views monthly file, successfully appended new row.")
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is entertainment views monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is entertainment views monthly file, successfully added date & new row to the file and uploaded"
        " into unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'],
                                             step_name='dbt_entertainment_views_monthly')
    logging.info(f"Entertainment views monthly loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the entertainment views monthly file under unzipped folder.")
    logging.info("Successfully process entertainment views monthly file.")


def process_entertainment_views_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Entertainment views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read entertainment views daily zip object")

    logging.info("Unzipping the entertainment views daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the entertainment views daily zipped object.")
    logging.info(f"Entertainment views daily z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Entertainment views daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Entertainment views daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'],
                                             step_name='dbt_entertainment_views_daily')
    logging.info(f"Entertainment views daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the entertainment views daily file under unzipped folder.")
    logging.info("Successfully processed entertainment views daily file.")


def process_sports_views_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Sports views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read sports views daily zip object")

    logging.info("Unzipping the sports views daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the sports views daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Sports views daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Sports views daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_sports_views_daily')
    logging.info(f"Sports views daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the sports views daily file under unzipped folder.")
    logging.info("Successfully process sports views daily file.")


def process_sports_views_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Sports views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read sports views monthly zip object")

    logging.info("Unzipping the sports views monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the sports views monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Sports views monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Sports views monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is sports views monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is sports views monthly file, successfully read the unzipped file.")
    file_df = pd.read_csv(file_obj['Body'])
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {'Channel': 'Unknown', 'Channel title': 'Unknown',
               'Views': total['Views'][0] - file_df.loc[file_df['Channel'] != 'Total', 'Views'].sum(),
               'Watch time (hours)': total['Watch time (hours)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Watch time (hours)'].sum(),
               'Average view duration': total['Average view duration'][0],
               'Estimated partner revenue (USD)': total['Estimated partner revenue (USD)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Estimated partner revenue (USD)'].sum()
               }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    logging.info("This file is sports views monthly file, successfully appended new row.")
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is sports views monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is sports views monthly file, successfully added date & new row to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_sports_views_monthly')
    logging.info(f"Sports views monthly loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the sports views monthly file under unzipped folder.")
    logging.info("Successfully process sports views monthly file.")


def process_outkick_views_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Outkick views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read outkick views daily zip object")

    logging.info("Unzipping the outkick views daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the outkick views daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Outkick views daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Outkick views daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_outkick_views_daily')
    logging.info(f"Outkick views daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the outkick views daily file under unzipped folder.")
    logging.info("Successfully process outkick views daily file.")


def process_outkick_views_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Outkick views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read outkick views monthly zip object")

    logging.info("Unzipping the outkick views monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the outkick views monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Outkick views monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Outkick views monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is outkick views monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is outkick views monthly file, successfully read the unzipped file.")
    file_df = pd.read_csv(file_obj['Body'])
    file_df = file_df.drop(columns='Date')
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is sports views monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is outkick views monthly file, successfully added date file and uploaded into unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_outkick_views_monthly')
    logging.info(f"Outkick views monthly loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the outkick views monthly file under unzipped folder.")
    logging.info("Successfully process outkick views monthly file.")


def process_tmz_views_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"TMZ views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read TMZ views daily zip object")

    logging.info("Unzipping the TMZ views daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the TMZ views daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"TMZ views daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"TMZ views daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_tmz_views_daily')
    logging.info(f"TMZ views daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the TMZ views daily file under unzipped folder.")
    logging.info("Successfully process TMZ views daily file.")


def process_tmz_views_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"TMZ views daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read TMZ views monthly zip object")

    logging.info("Unzipping the TMZ views monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the TMZ views monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"TMZ views monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"TMZ views monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is TMZ views monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is TMZ views monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {'Channel': 'Unknown', 'Channel title': 'Unknown',
               'Views': total['Views'][0] - file_df.loc[file_df['Channel'] != 'Total', 'Views'].sum(),
               'Watch time (hours)': total['Watch time (hours)'][0] - file_df.loc[
                   file_df['Channel'] != 'Total', 'Watch time (hours)'].sum(),
               'Average view duration': total['Average view duration'][0]
               }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    logging.info("This file is TMZ views monthly file, successfully appended new row.")
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is TMZ views monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is TMZ views monthly file, successfully added date & new row to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_tmz_views_monthly')
    logging.info(f"TMZ views monthly loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the TMZ views monthly file under unzipped folder.")
    logging.info("Successfully process TMZ views monthly file.")


def process_news_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"News revenue daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read news revenue daily zip object")

    logging.info("Unzipping the news revenue daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the news revenue daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"News revenue daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"News revenue daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_news_revenue_daily')
    logging.info(f"News revenue daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the news revenue daily file under unzipped folder.")
    logging.info("Successfully process news revenue daily file.")


def process_outkick_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Outkick revenue daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read outkick revenue daily zip object")

    logging.info("Unzipping the outkick revenue daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the outkick revenue daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Outkick revenue daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Outkick revenue daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_outkick_revenue_daily')
    logging.info(f"Outkick revenue daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Outkick revenue daily file under unzipped folder.")
    logging.info("Successfully process Outkick revenue daily file.")


def process_news_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"News revenue monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read News revenue monthly zip object")

    logging.info("Unzipping the News revenue monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the News revenue monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"News revenue monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"News revenue monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is News revenue monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is News revenue monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    cols = list(file_df.columns)
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {
        cols[0]: 'Unknown', cols[1]: 'Unknown', cols[2]: total[cols[2]][0],
        cols[3]: total[cols[3]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[3]].sum(),
        cols[4]: total[cols[4]][0], cols[5]: total[cols[5]][0],
        cols[6]: total[cols[6]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[6]].sum(),
        cols[7]: total[cols[7]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[7]].sum(),
        cols[8]: total[cols[8]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[8]].sum(),
        cols[9]: total[cols[9]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[9]].sum(),
        cols[10]: total[cols[10]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[10]].sum(),
        cols[11]: total[cols[11]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[11]].sum(),
        cols[12]: total[cols[12]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[12]].sum(),
        cols[13]: total[cols[13]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[13]].sum(),
        cols[14]: total[cols[14]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[14]].sum(),
        cols[15]: total[cols[15]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[15]].sum(),
        cols[16]: total[cols[16]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[16]].sum(),
        cols[17]: total[cols[17]][0],
        cols[18]: total[cols[18]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[18]].sum()
    }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    file_df.insert(0, 'date', file_date[0])
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    logging.info("This file is News revenue monthly file, successfully added date to the file.")

    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is News revenue monthly file, successfully appended a new row &added date to the file and "
        "uploaded into unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_news_revenue_monthly')
    logging.info(f"News revenue monthly file loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the News revenue monthly file under unzipped folder.")
    logging.info("Successfully process News revenue monthly file.")


def process_outkick_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Outkick revenue monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read Outkick revenue monthly zip object")

    logging.info("Unzipping the Outkick revenue monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the Outkick revenue monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Outkick revenue monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Outkick revenue monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is Outkick revenue monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is Outkick revenue monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    file_df = file_df.drop(columns='Date')
    file_df.insert(0, 'date', file_date[0])
    logging.info("This file is Outkick revenue monthly file, successfully added date to the file.")
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is Outkick revenue monthly file, successfully added date to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'],
                                             step_name='dbt_outkick_revenue_monthly')
    logging.info(f"Outkick revenue monthly file loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Outkick revenue monthly file under unzipped folder.")
    logging.info("Successfully process Outkick revenue monthly file.")


def process_entertainment_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Entertainment revenue daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read Entertainment revenue daily zip object")

    logging.info("Unzipping the Entertainment revenue daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the Entertainment revenue daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Entertainment revenue daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Entertainment revenue daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'],
                                             step_name='dbt_entertainment_revenue_daily')
    logging.info(f"Entertainment revenue daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Entertainment revenue daily file under unzipped folder.")
    logging.info("Successfully process Entertainment revenue daily file.")


def process_entertainment_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Entertainment revenue monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read Entertainment revenue monthly zip object")

    logging.info("Unzipping the Entertainment revenue monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the Entertainment revenue monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Entertainment revenue monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Entertainment revenue monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is Entertainment revenue monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is Entertainment revenue monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    cols = list(file_df.columns)
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {cols[0]: 'Unknown', cols[1]: 'Unknown', cols[2]: total[cols[2]][0],
               cols[3]: total[cols[3]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[3]].sum(),
               cols[4]: total[cols[4]][0], cols[5]: total[cols[5]][0],
               cols[6]: total[cols[6]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[6]].sum(),
               cols[7]: total[cols[7]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[7]].sum(),
               cols[8]: total[cols[8]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[8]].sum(),
               cols[9]: total[cols[9]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[9]].sum(),
               cols[10]: total[cols[10]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[10]].sum(),
               cols[11]: total[cols[11]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[11]].sum(),
               cols[12]: total[cols[12]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[12]].sum(),
               cols[13]: total[cols[13]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[13]].sum(),
               cols[14]: total[cols[14]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[14]].sum(),
               cols[15]: total[cols[15]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[15]].sum(),
               cols[16]: total[cols[16]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[16]].sum(),
               cols[17]: total[cols[17]][0],
               cols[18]: total[cols[18]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[18]].sum()
               }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    logging.info("This file is Entertainment views monthly file, successfully appended new row.")
    file_df.insert(0, 'date', file_date[0])
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    logging.info("This file is Entertainment revenue monthly file, successfully added new row and date to the file.")

    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is Entertainment revenue monthly file, successfully added date to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'],
                                             step_name='dbt_entertainment_revenue_monthly')
    logging.info(f"Entertainment revenue monthly file loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Entertainment revenue monthly file under unzipped folder.")
    logging.info("Successfully process Entertainment revenue monthly file.")


def process_sports_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Sports revenue daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read Sports revenue daily zip object")

    logging.info("Unzipping the Sports revenue daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the Sports revenue daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Sports revenue daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Sports revenue daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_sports_revenue_daily')
    logging.info(f"Sports revenue daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Sports revenue daily file under unzipped folder.")
    logging.info("Successfully process Sports revenue daily file.")


def process_sports_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"Sports revenue monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read Sports revenue monthly zip object")

    logging.info("Unzipping the Sports revenue monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the Sports revenue monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"Sports revenue monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"Sports revenue monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is Sports revenue monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is Sports revenue monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    cols = list(file_df.columns)
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {
        cols[0]: 'Unknown', cols[1]: 'Unknown', cols[2]: total[cols[2]][0],
        cols[3]: total[cols[3]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[3]].sum(),
        cols[4]: total[cols[4]][0], cols[5]: total[cols[5]][0],
        cols[6]: total[cols[6]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[6]].sum(),
        cols[7]: total[cols[7]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[7]].sum(),
        cols[8]: total[cols[8]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[8]].sum(),
        cols[9]: total[cols[9]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[9]].sum(),
        cols[10]: total[cols[10]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[10]].sum(),
        cols[11]: total[cols[11]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[11]].sum(),
        cols[12]: total[cols[12]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[12]].sum(),
        cols[13]: total[cols[13]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[13]].sum(),
        cols[14]: total[cols[14]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[14]].sum(),
        cols[15]: total[cols[15]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[15]].sum(),
        cols[16]: total[cols[16]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[16]].sum(),
        cols[17]: total[cols[17]][0],
        cols[18]: total[cols[18]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[18]].sum()
    }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    file_df.insert(0, 'date', file_date[0])
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    logging.info("This file is Sports revenue monthly file, successfully added new row & date field.")

    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is sports revenue monthly file, successfully added date to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_sports_revenue_monthly')
    logging.info(f"Sports revenue monthly file loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the Sports revenue monthly file under unzipped folder.")
    logging.info("Successfully process sports revenue monthly file.")


def process_tmz_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"TMZ revenue daily zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read TMZ revenue daily zip object")

    logging.info("Unzipping the TMZ revenue daily zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the TMZ revenue daily zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"TMZ revenue daily file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"TMZ revenue daily unzip_files: {str(unzip_files)}")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_tmz_revenue_daily')
    logging.info(f"TMZ revenue daily loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )
    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the TMZ revenue daily file under unzipped folder.")
    logging.info("Successfully process TMZ revenue daily file.")


def process_tmz_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type):
    pattern = '\d{4}-\d{2}-\d{2}'
    file_date = re.findall(pattern, file_name)

    s3_res = get_s3_conn(role_arn_to_assume=None)
    zip_obj = s3_res.Bucket(data_s3_bucket).Object(key + file_name)

    logging.info(f"TMZ revenue monthly zip_obj: {str(zip_obj)}")
    buffer = BytesIO(zip_obj.get()["Body"].read())
    logging.info("Successfully read TMZ revenue monthly zip object")

    logging.info("Unzipping the TMZ revenue monthly zipped object.")
    z = zipfile.ZipFile(buffer)
    logging.info("Successfully unzipped the TMZ revenue monthly zipped object.")
    logging.info(f"{youtube_file_type} z.namelist(): {str(z.namelist())}")

    if z.namelist()[0].startswith('Table'):
        filename = z.namelist()[0]
    else:
        filename = z.namelist()[1]
    file_info = z.getinfo(filename)
    logging.info(f"TMZ revenue monthly file_info: {str(file_info)}")

    leaf_file_name = f"{youtube_file_type}_{file_date[0]}_{file_date[1]}.csv"

    logging.info(
        f"Deleting files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")
    delete_s3_file(
        's3://' + data_s3_bucket + '/' + f"{config['data_s3_key']}/{youtube_file_type}/unzipped",
        recursive=1
    )
    logging.info(
        f"Successfully deleted files under s3://{data_s3_bucket}/{config['data_s3_key']}/{youtube_file_type}/unzipped/.")

    s3_res.meta.client.upload_fileobj(
        z.open(filename),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name,
        ExtraArgs={'ACL': 'bucket-owner-full-control'}
    )
    logging.info(
        f"Uploaded the unzipped file - {leaf_file_name} to s3://{config['data_s3_bucket']}/{youtube_file_type}/unzipped/")

    unzip_files = s3_list_file(
        f"s3://{config['data_s3_bucket']}/{config['data_s3_key']}/{youtube_file_type}/unzipped/{leaf_file_name}",
        role_arn_to_assume=None
    )
    logging.info(f"TMZ revenue monthly unzip_files: {str(unzip_files)}")

    logging.info("This file is TMZ revenue monthly file, so reading the unzipped file.")
    s3_cli = get_s3_client(role_arn_to_assume=None)
    file_obj = s3_cli.get_object(
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info("This file is TMZ revenue monthly file, successfully read the unzipped file.")

    file_df = pd.read_csv(file_obj['Body'])
    cols = list(file_df.columns)
    total = file_df.loc[file_df['Channel'] == 'Total']
    new_row = {
        cols[0]: 'Unknown', cols[1]: 'Unknown', cols[2]: total[cols[2]][0],
        cols[3]: total[cols[3]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[3]].sum(),
        cols[4]: total[cols[4]][0], cols[5]: total[cols[5]][0],
        cols[6]: total[cols[6]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[6]].sum(),
        cols[7]: total[cols[7]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[7]].sum(),
        cols[8]: total[cols[8]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[8]].sum(),
        cols[9]: total[cols[9]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[9]].sum(),
        cols[10]: total[cols[10]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[10]].sum(),
        cols[11]: total[cols[11]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[11]].sum(),
        cols[12]: total[cols[12]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[12]].sum(),
        cols[13]: total[cols[13]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[13]].sum(),
        cols[14]: total[cols[14]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[14]].sum(),
        cols[15]: total[cols[15]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[15]].sum(),
        cols[16]: total[cols[16]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[16]].sum(),
        cols[17]: total[cols[17]][0],
        cols[18]: total[cols[18]][0] - file_df.loc[file_df[cols[0]] != 'Total', cols[18]].sum()
    }
    file_df = pd.concat([file_df, pd.DataFrame([new_row])], ignore_index=True)
    file_df.insert(0, 'date', file_date[0])
    csv_buf = StringIO()
    file_df.to_csv(csv_buf, index=False)
    logging.info("This file is TMZ views monthly file, successfully added new row & date field.")

    s3_cli.put_object(
        ACL='bucket-owner-full-control',
        Body=csv_buf.getvalue(),
        Bucket=config['data_s3_bucket'],
        Key=config['data_s3_key'] + f"/{youtube_file_type}/unzipped/" + leaf_file_name
    )
    logging.info(
        "This file is TMZ revenue monthly file, successfully added date to the file and uploaded into "
        "unzipped path.")

    DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_silver'], step_name='dbt_tmz_revenue_monthly')
    logging.info(f"TMZ revenue monthly file loaded via DBT into Silver table: {str(unzip_files)}")

    copy_source = {
        'Bucket': config['data_s3_bucket'],
        'Key': config['data_s3_key'] + f"/{youtube_file_type}/unzipped/{leaf_file_name}"
    }

    s3_res.meta.client.copy(
        copy_source,
        config['data_s3_bucket'],
        config['data_s3_key'] + f"/{youtube_file_type}/archive/{leaf_file_name}"
    )

    logging.info(f"Archived the file from "
                 f"s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/unzipped/{leaf_file_name}"
                 f"to s3://{config['data_s3_bucket']}/config['data_s3_key']/{youtube_file_type}/archive/{leaf_file_name}")

    unzipped_s3_object = s3_res.Bucket(data_s3_bucket).Object(key.replace("unprocessed", "unzipped") + leaf_file_name)
    unzipped_s3_object.delete()
    logging.info("Successfully deleted the TMZ revenue monthly file under unzipped folder.")
    logging.info("Successfully process TMZ revenue monthly file.")


def process_file(file_name, key, data_s3_bucket, youtube_file_type):
    if 'news_views_daily' in youtube_file_type:
        process_news_views_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'entertainment_views_daily' in youtube_file_type:
        process_entertainment_views_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'sports_views_daily' in youtube_file_type:
        process_sports_views_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'outkick_views_daily' in youtube_file_type:
        process_outkick_views_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'tmz_views_daily' in youtube_file_type:
        process_tmz_views_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'news_views_monthly' in youtube_file_type:
        process_news_views_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'entertainment_views_monthly' in youtube_file_type:
        process_entertainment_views_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'sports_views_monthly' in youtube_file_type:
        process_sports_views_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'outkick_views_monthly' in youtube_file_type:
        process_outkick_views_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'tmz_views_monthly' in youtube_file_type:
        process_tmz_views_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'news_revenue_daily' in youtube_file_type:
        process_news_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'outkick_revenue_daily' in youtube_file_type:
        process_outkick_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'sports_revenue_daily' in youtube_file_type:
        process_sports_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'entertainment_revenue_daily' in youtube_file_type:
        process_entertainment_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'tmz_revenue_daily' in youtube_file_type:
        process_tmz_revenue_daily(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'news_revenue_monthly' in youtube_file_type:
        process_news_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'outkick_revenue_monthly' in youtube_file_type:
        process_outkick_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'sports_revenue_monthly' in youtube_file_type:
        process_sports_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'entertainment_revenue_monthly' in youtube_file_type:
        process_entertainment_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type)
    elif 'tmz_revenue_monthly' in youtube_file_type:
        process_tmz_revenue_monthly(file_name, key, data_s3_bucket, youtube_file_type)


def process_emails():
    email_s3_bucket = config["email_s3_bucket"]
    email_s3_key = config["email_s3_key"]
    data_s3_bucket = config["data_s3_bucket"]
    data_s3_key = config["data_s3_key"]
    file_list = config["file_list"]
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    sts_client = boto3.client('sts')
    aws_conn_id = config['aws_conn_id']
    conn = BaseHook.get_connection(aws_conn_id)
    cpe_role_arn = json.loads(conn.get_extra())['role_arn']
    logging.info(
        f"Assuming the CPE role with aws connection id: {aws_conn_id} for the cpe role: {cpe_role_arn}")
    assumed_cpe_role_object = sts_client.assume_role(
        RoleArn=cpe_role_arn,
        RoleSessionName=f"AdSalesAssumeCPERoleS3Access"
    )
    logging.info(
        f"Successfully assumed the CPE role with aws connection id: {aws_conn_id} for the cpe role: {cpe_role_arn}")
    cpe_role_credentials = assumed_cpe_role_object['Credentials']
    cpe_role_session = boto3.Session(
        aws_access_key_id=cpe_role_credentials['AccessKeyId'],
        aws_secret_access_key=cpe_role_credentials['SecretAccessKey'],
        aws_session_token=cpe_role_credentials['SessionToken']
    )
    s3conn = cpe_role_session.client('s3')

    processed_file_types = []
    skipped_file_types = []

    for f in file_list:
        logging.info(f"- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        response = s3conn.list_objects_v2(Bucket=email_s3_bucket, Prefix=f"{email_s3_key}/{f}", MaxKeys=1)

        # list_objects_v2 omits 'Contents' when the prefix is empty. Using
        # response['Contents'] KeyErrors and aborts the silver loop mid-way, so
        # later file types never load — a common Prod vs QA drift source when QA
        # only receives a subset of the 20 youtube file prefixes.
        if response.get('Contents'):
            logging.info(f"New files found for {f}. Processing . . .")
            objs = s3conn.list_objects_v2(
                Bucket=email_s3_bucket,
                Prefix=f"{email_s3_key}/{f}/"
            )['Contents']

            logging.info(f"Extracted all files - {str(objs)}")
            unprocessed_files = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)]
            logging.info(f"Unprocessed files - {str(unprocessed_files)}")

            for unprocessed_mime_file in unprocessed_files:
                s3_resource = get_s3_conn()
                if 'AMAZON_SES_SETUP_NOTIFICATION' not in unprocessed_mime_file:
                    logging.info(f"Retrieving email object.")
                    email_object = s3_resource.Bucket(email_s3_bucket).Object(
                        unprocessed_mime_file)
                    email_msg = email.message_from_bytes(email_object.get()['Body'].read())
                    email_sub = email_msg['Subject']
                    logging.info(f"Found email subject - {email_sub}")
                    attachments = email_msg.get_payload()

                    if len(attachments) > 0:
                        for atchmnt in attachments:
                            if atchmnt.get('Content-Disposition'):
                                file_name = atchmnt.get('Content-Disposition').split('=')[1].replace('\"', '').replace(
                                    ";", "").replace('\r', '').replace('\n', '').replace('\t', '')[:-4].strip()
                                logging.info(f"Available File: {str(file_name)}")

                                logging.info("Creating /usr/local/airflow/adsales/tmp/mime folder.")
                                os.makedirs("/usr/local/airflow/adsales/tmp/mime", exist_ok=True)
                                logging.info("Successfully created /usr/local/airflow/adsales/tmp/mime folder.")

                                logging.info(f"Writing attachment to /usr/local/airflow/adsales/tmp/mime/{file_name}.")
                                open("/usr/local/airflow/adsales/tmp/mime/" + file_name, 'wb').write(
                                    atchmnt.get_payload(decode=True))
                                logging.info(
                                    f"Successfully dumped the attachment to /usr/local/airflow/adsales/tmp/mime/{file_name}.")

                                logging.info(
                                    f"Deleting files under s3://{data_s3_bucket}/{data_s3_key}/{f}/unprocessed/.")
                                delete_s3_file(
                                    's3://' + data_s3_bucket + '/' + f"{data_s3_key}/{f}/unprocessed",
                                    recursive=1
                                )
                                logging.info(
                                    f"Successfully deleted files under s3://{data_s3_bucket}/{data_s3_key}/{f}/unprocessed.")

                                logging.info(
                                    f"Uploading the new attachment into s3://{data_s3_bucket}/{data_s3_key}/{f}/unprocessed/{file_name}.")
                                upload_to_s3(
                                    "/usr/local/airflow/adsales/tmp/mime/" + file_name,
                                    f"s3://{data_s3_bucket}/{data_s3_key}/{f}/unprocessed/{file_name}",
                                    0,
                                    role_arn_to_assume=None
                                )
                                logging.info(
                                    f"Successfully uploaded the new attachment into s3://{data_s3_bucket}/{data_s3_key}/{f}/unprocessed/{file_name}.")
                                process_file(
                                    file_name,
                                    f"{data_s3_key}/{f}/unprocessed/",
                                    data_s3_bucket,
                                    f
                                )
                                processed_file_types.append(f)

                                logging.info(
                                    f"Deleting processed file - s3://{email_s3_bucket}/{unprocessed_mime_file}.")
                                delete_s3_file(
                                    f's3://{email_s3_bucket}/{unprocessed_mime_file}',
                                    recursive=1
                                )
                                logging.info(
                                    f"Successfully deleted the file - s3://{email_s3_bucket}/{unprocessed_mime_file}.")
        else:
            skipped_file_types.append(f)
            logging.info(f"No new file found for {f}. Skipping . . .")

    logging.info(
        f"Silver email processing summary | processed={processed_file_types} "
        f"| skipped_empty_prefix={skipped_file_types} "
        f"| data_landing=s3://{data_s3_bucket}/{data_s3_key}/"
    )


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--layer", help="Data model Layer - silver or gold")
        args = parser.parse_args()
        
        server_hostname = config['server_hostname']
        http_path = config['http_path']

        if args.layer == 'silver':
            logging.info("Running Silver task to load all the youtube files to staging table . . .")
            process_emails()
            logging.info("Successfully loaded all the youtube files to staging table and processed silver table.")
        else:
            logging.info("Triggering GOLD DBT job.")
            DBTCloudUtils(config['dbt_conn_id']).run(job_id=config['dbt_job_id_gold'], step_name='dbt_youtube_gold')
            logging.info("Successfully finished GOLD DBT job.")
    except Exception as e:
        logging.exception(f"Task failed: {e}")
        print(str(e))
        raise
       
