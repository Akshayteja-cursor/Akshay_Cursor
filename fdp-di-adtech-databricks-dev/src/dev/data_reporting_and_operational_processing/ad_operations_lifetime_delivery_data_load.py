# Databricks notebook source
# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/alert"

# COMMAND ----------

import io
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import boto3
from typing import Any

# COMMAND ----------

def find_staq_file(report_date: datetime) -> str:
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')

    staq = 'STAQ_Adjuster_Pacing_YTD_All_Lines_' + report_date.strftime('%Y-%m-%d')

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=staq_bucket)

    for page in page_iterator:
        for obj in page.get('Contents', []):
            if staq in obj['Key']:
                return obj['Key']

# COMMAND ----------

def format_comp(cell) -> float:
    try:
        if str(cell).contains('%'):
            try:
                return float(cell[:-1]/100)
            except:
                return 0.0
    except:
        try:
            return float(cell)
        except:
            return 0.0

def apply_vpvh_elimination(adops_lifetime: pd.DataFrame) -> pd.DataFrame:
    adops_lifetime.fillna(
        {
            'Demo Comp': 0,
            'Impression Goal':0,
        },
        inplace=True,
    )

    adops_lifetime['Billable Metric 1'] = np.where(adops_lifetime['Billable Metric'] == 'Bill on Contract',
                                                   adops_lifetime['Implied Billable Metric'], adops_lifetime['Billable Metric'])
    
    adops_lifetime['Demo Comp Temp'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                                -1, adops_lifetime['Demo Comp'])
    
    adops_lifetime['Demo Comp 1'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             adops_lifetime.groupby('Parent Sales Line Item ID')['Demo Comp Temp'].transform('max'), adops_lifetime['Demo Comp'])
    
    adops_lifetime['Demo Comp 2'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             np.where(adops_lifetime['Parent Sales Line Item ID'] == adops_lifetime['Sales Line Item ID'],
                                                      adops_lifetime['Planned Demo Comp'], adops_lifetime['Demo Comp 1']),
                                             adops_lifetime['Demo Comp 1'])
    
    adops_lifetime['count_1'] = adops_lifetime.groupby(['Parent Sales Line Item ID', 'Placement Property'])['Placement Property'].transform(len)
    adops_lifetime['count_2'] = adops_lifetime.groupby(['Parent Sales Line Item ID'])['Parent Sales Line Item ID'].transform(len)

    adops_lifetime['Demo Comp 3'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             np.where(adops_lifetime['count_1']>1,
                                                      np.where(adops_lifetime['count_1'] == adops_lifetime['count_2'],
                                                               adops_lifetime['Planned Demo Comp'], adops_lifetime['Demo Comp 2']),
                                                      adops_lifetime['Demo Comp 2']),
                                             adops_lifetime['Demo Comp 2'])
    
    adops_lifetime['Demo Comp 4'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             np.where(adops_lifetime['Billable Metric 1'] == 'Absolute A',
                                                      1.2, adops_lifetime['Demo Comp 3']),
                                             adops_lifetime['Demo Comp 3'])
    
    adops_lifetime['Demo Comp New'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             np.where(adops_lifetime['Is Audience Target'] == True,
                                                      0, adops_lifetime['Demo Comp 4']),
                                             adops_lifetime['Demo Comp 4'])
    
    adops_lifetime.loc[:,'Demo Comp New'] = adops_lifetime['Demo Comp New'].apply(format_comp)

    adops_lifetime['Billable Metric New'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                                     np.where(adops_lifetime['Is Audience Target'] == True,
                                                              np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                       '1P Audience Target', '3P Audience Target'),
                                                              np.where(adops_lifetime['Billable Metric 1'] == 'Absolute A',
                                                                       'Absolute A',
                                                                       np.where(adops_lifetime['Demo Band'] != 'P2+',
                                                                                np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                                         '1P Demo Imps', '3P Demo Imps'),
                                                                                np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                                         '1P CoView Imps', '3P CoView Imps')))),
                                                     adops_lifetime['Billable Metric 1'])
    
    adops_lifetime['Billable Metric New'] = np.where(adops_lifetime['Implied Billable Metric'].str.contains('CoView', regex=True),
                                                     adops_lifetime['Implied Billable Metric'], adops_lifetime['Billable Metric New'])
    
    adops_lifetime['1P Demo Imps'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                              np.where(adops_lifetime['Demo Comp New'] != 0,
                                                       adops_lifetime['1P Imps']*adops_lifetime['Demo Comp New'], adops_lifetime['1P Imps']),
                                              adops_lifetime['1P Demo Imps'])
    
    adops_lifetime['3P Demo Imps'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                              np.where(adops_lifetime['Demo Comp New'] != 0,
                                                       adops_lifetime['3P Imps']*adops_lifetime['Demo Comp New'], adops_lifetime['3P Imps']),
                                              adops_lifetime['3P Demo Imps'])
    
    adops_lifetime['Billable Quantity New'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                                       np.where(adops_lifetime['Is Audience Target'] == True,
                                                                np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                         adops_lifetime['1P Imps'], adops_lifetime['3P Imps']),
                                                                np.where(adops_lifetime['Billable Metric New'] == 'Absolute A',
                                                                         np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                                  1.2*adops_lifetime['1P Imps'], 1.2*adops_lifetime['3P Imps']),
                                                                         np.where(adops_lifetime['Demo Comp New'] != 0,
                                                                                  np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                                           adops_lifetime['1P Imps']*adops_lifetime['Demo Comp New'], adops_lifetime['3P Imps']*adops_lifetime['Demo Comp New']),
                                                                                  np.where(adops_lifetime['Billable Third Party Server'] == '1st Party',
                                                                                           adops_lifetime['1P Imps'], adops_lifetime['3P Imps'])))),
                                                       adops_lifetime['Billable Quantity'])
    
    adops_lifetime['Billable Metric New'] = np.where(adops_lifetime['Product Name'].str.contains('SOV', regex=True),
                                                     'SOV', adops_lifetime['Billable Metric New'])
    
    adops_lifetime['Billable Quantity New'] = np.where(adops_lifetime['Product Name'].str.contains('SOV', regex=True),
                                                       np.where(adops_lifetime['Impression Goal'] != 0,
                                                                adops_lifetime['Impression Goal'], adops_lifetime['Billable Quantity New']),
                                                       adops_lifetime['Billable Quantity New'])
    
    adops_lifetime['Delivered %'] = np.where(adops_lifetime['Product Name'].str.contains('SOV', regex=True),
                                             np.where(adops_lifetime['Estimated Impression Goal'] != 0,
                                                      adops_lifetime['Billable Quantity New']/adops_lifetime['Estimated Impression Goal'], 0),
                                             adops_lifetime['Delivered %'])
    
    adops_lifetime['Earned Revenue'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                                adops_lifetime['Net Unit Cost']*adops_lifetime['Billable Quantity New']/1000,
                                                np.where(adops_lifetime['Billable Metric New'] == 'SOV',
                                                         adops_lifetime['Net Cost'],
                                                         adops_lifetime['Earned Revenue']))
    
    adops_lifetime['Delivered %'] = np.where(adops_lifetime['Placement Property'] == 'FOX DAI VOD',
                                             adops_lifetime['Billable Quantity New']/adops_lifetime['Quantity'], adops_lifetime['Delivered %'])
    
    columns_to_drop = ['Demo Comp',
                       'Demo Comp Temp',
                       'Demo Comp 1',
                       'Demo Comp 2',
                       'Demo Comp 3',
                       'Demo Comp 4',
                       'Billable Metric',
                       'Billable Metric 1',
                       'count_1',
                       'count_2',
                       'Billable Quantity'
    ]
    
    adops_lifetime.drop(columns=columns_to_drop, inplace=True, axis=1)

    adops_lifetime.rename(
        {
            'Demo Comp New': 'Demo Comp',
            'Billable Metric New': 'Billable Metric',
            'Billable Quantity New': 'Billable Quantity'
        },
        inplace=True,
        axis=1
    )

    adops_lifetime.fillna(
        {
            'Billable Quantity': 0,
            'Earned Revenue': 0,
        },
        inplace=True,
    )

    adops_lifetime = adops_lifetime.astype(
        {
            'Billable Quantity': 'int64',
            'Earned Revenue': 'float64',
        },
    )
    
    return adops_lifetime

def recapped_run_rate(row: pd.Series) -> Any:
    if row['Pacing %'] >= 1:
        return row['Net Cost']
    return row['Run Rate']

def recalculate_projected(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'Run Rate'] = df.apply(run_rate, axis=1)
    df.loc[:, 'Sales Line Item Run Rate (Capped)'] = df.apply(recapped_run_rate, axis=1)
    return df


def apply_pg_billing(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    fw_pg_filename = 'FW_PG_Lifetime_Delivery.parquet'
    fw_pg_file = s3_dir + 'processed/' + fw_pg_filename

    pg_billing = pd.read_parquet(fw_pg_file, storage_options={'profile': aws_profile})

    pg_billing.drop(['Event Date'], axis=1, inplace=True)

    pg_billing = pg_billing.groupby(['Deal ID']).sum()

    pg_billing = pg_billing.rename({'Net Counted Ads': 'PG Impressions'}, axis=1)

    df = pd.merge(df, pg_billing, how='left', left_on='Placement ID', right_on='Deal ID')

    def is_pg_deal(row: pd.Series) -> bool:
        if row['Invoice Organization Name'] in {'FOX Corp Programmatic Guaranteed', 'FOX Corp Programmatic/Reseller Buys', 'FOX Corp Programmatic - Magnite', 'FOX Corp Programmatic - PubMatic'}:
            return True
        return False
    
    def billable_metric(row: pd.Series) -> str:
        if row['is_pg_deal'] and row['Sales Order Name'] is not None and 'evergreen' in row['Sales Order Name'].lower():
            return 'Programmatic Reseller Imps'
        if row['is_pg_deal'] and row['Programmatic Type'] is not None and 'programmatic guaranteed' in row['Programmatic Type'].lower():
            return "Programmatic Guaranteed Imps"
        if row['is_pg_deal'] and row['Invoice Organization Name'] is not None and 'programmatic guaranteed' in row['Invoice Organization Name'].lower():
            return "Programmatic Guaranteed Imps"
        return row['Billable Metric']
    
    df.loc[:, 'is_pg_deal'] = df.apply(is_pg_deal, axis=1)

    df.loc[:, 'Billable Metric'] = df.apply(billable_metric, axis=1)

    pg_df = df[df['is_pg_deal'] == True]

    other_df = df[df['is_pg_deal'] == False]

    pg_df.loc[:, '1P Imps'] = pg_df['PG Impressions']
    pg_df.loc[:, 'No Delivery'] = pg_df['PG Impressions'] == 0
    pg_df.loc[:, 'Billable Quantity'] = pg_df['PG Impressions']
    pg_df.loc[:, 'Pacing %'] = pg_df['Billable Quantity'] / pg_df['Quantity'] / pg_df['Launch Flight %']
    pg_df.loc[:, 'Earned Revenue'] = pg_df['Net Unit Cost'] * pg_df['Billable Quantity']/1000
    pg_df.drop(['Parent Line Item Total', 'Parent Line Item Run Rate', 'Parent Line Item Earned Revenue'], inplace=True, axis=1)
    pg_df = recalculate_projected(pg_df)
    pg_df = calculate_risk(pg_df, report_date)
    
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


def apply_magnite_billing(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    magnite_filename = 'Magnite_Lifetime_Delivery.parquet'
    magnite_file = s3_dir + 'processed/' + magnite_filename

    magnite_billing = pd.read_parquet(magnite_file, storage_options={'profile': aws_profile})
    magnite_billing.loc[:, 'Deal ID'] = magnite_billing['Deal ID'].str.strip()

    magnite_billing.drop(['Date'], axis=1, inplace=True)

    magnite_billing = magnite_billing.groupby(['Deal ID', 'Network']).sum().reset_index()

    magnite_billing.rename({'Deal ID':'Magnite Deal ID', 'Impressions':'Magnite Impressions', 'Total Gross Revenue':'Magnite Revenue'}, axis=1, inplace=True)

    def primary_placement_map(row: pd.Series) -> str:
        network_primary_placement_property_map = {
            'FOX Sports Streaming':'FOX Sports Streaming',
            'Tubi - One Fox':'Fox Sold Tubi',
            'FOX on Hulu':'FOX on Hulu',
            'FOXNow':'FOXNow',
            'FOX Business':'FOXBusiness.com',
            'FOX on Tubi':'FOX on Tubi',
            'FOX News':'FOXNews.com',
            'Fox SpringServe oRTB': '',
            'OneFox SpringServe oRTB brand': '',
            'Fox - Fox Television Stations - OneFox': 'Fox Sold FTS',
            'Fox - Weather - OneFox': 'FoxWeather.com',
            'FOX Weather': 'FoxWeather.com',
            'Tubi - One Fox- AAT Segments': '',
            'Fox Enterainment - OneFox - Segments': '',
            'OneFOX_AAT Segments': '',
        }

        try:
            return network_primary_placement_property_map[row['Network']]
        except:
            print('Unmapped Network - ',row['Network'])
            return ''
    
    def secondary_placement_map(row: pd.Series) -> str:
        network_secondary_placement_property_map = {
            'FOX Sports Streaming':'FOX Sports Clips',
            'Tubi - One Fox':'',
            'FOX on Hulu':'RON - Ent',
            'FOXNow':'RON - Ent',
            'FOX Business':'FOXNews.com',
            'FOX on Tubi':'FOX on Tubi',
            'FOX News':'RON - Ent',
            'Fox SpringServe oRTB': '',
            'OneFox SpringServe oRTB brand': '',
            'Fox - Fox Television Stations - OneFox': '',
            'Fox - Weather - OneFox': '',
            'FOX Weather': '',
            'Tubi - One Fox- AAT Segments': '',
            'Fox Enterainment - OneFox - Segments': '',
            'OneFOX_AAT Segments': '',
        }

        try:
            return network_secondary_placement_property_map[row['Network']]
        except:
            print('Unmapped Network - ',row['Network'])
            return ''

    magnite_billing.loc[:, 'Primary Placement Property'] = magnite_billing.apply(primary_placement_map, axis=1)
    magnite_billing.loc[:, 'Secondary Placement Property'] = magnite_billing.apply(secondary_placement_map, axis=1)

    def is_magnite_deal(row: pd.Series) -> bool:
        if row['Invoice Organization Name'] == 'FOX Corp Programmatic - Magnite':
            return True
        if 'PRG' in row['Product Name'] and 'Non Ad Served' in row['Product Name']:
            return True
        if row['Advertiser Name'] == 'MAGNITE':
            return True
        # if 'PRG' in row['Product Name'] and 'Non-Ad Served' in row['Product Name']:
        #     return True
        return False
    
    df.loc[:, 'is_magnite_deal'] = df.apply(is_magnite_deal, axis=1)

    def join_on_placement(row: pd.Series) -> bool:
        if row['is_magnite_deal'] == False:
            return False
        if row['Magnite Deal ID'] == None:
            return True
        if row['Primary Placement Property'] == '' and row['Secondary Placement Property'] == '':
            return True
        if row['Primary Placement Property'] == row['Placement Property']:
            return True
        if row['Secondary Placement Property'] == row['Placement Property']:
            return True
        return False
    
    magnite_billing = pd.merge(magnite_billing, df, how='left', left_on='Magnite Deal ID', right_on='Deal ID').reset_index(drop=True)
    magnite_billing.loc[:, 'join_on_placement'] = magnite_billing.apply(join_on_placement, axis=1)
    magnite_billing = magnite_billing[magnite_billing['join_on_placement'] == True]
    magnite_billing.rename({'Placement Property':'Magnite Placement Property'}, axis=1, inplace=True)
    magnite_billing = magnite_billing.loc[:, ['Magnite Deal ID', 'Magnite Placement Property', 'Magnite Impressions', 'Magnite Revenue']]
    magnite_billing = magnite_billing.groupby(['Magnite Deal ID', 'Magnite Placement Property']).sum().reset_index()

    df = pd.merge(df, magnite_billing, how='left', left_on=['Deal ID', 'Placement Property'], right_on=['Magnite Deal ID', 'Magnite Placement Property'])

    def billable_metric(row: pd.Series) -> str:
        if row['is_magnite_deal']:
            return 'Programmatic Guaranteed Imps'
        return row['Billable Metric']

    df.loc[:, 'Billable Metric'] = df.apply(billable_metric, axis=1)

    magnite_df = df[df['is_magnite_deal'] == True]

    other_df = df[df['is_magnite_deal'] == False]

    magnite_df.loc[:, 'PG Impressions'] = magnite_df['Magnite Impressions']
    magnite_df.loc[:, '1P Imps'] = magnite_df['Magnite Impressions']
    magnite_df.loc[:, 'No Delivery'] = magnite_df['Magnite Impressions'] == 0
    magnite_df.loc[:, 'Billable Quantity'] = magnite_df['Magnite Impressions']
    magnite_df.loc[:, 'Pacing %'] = magnite_df['Billable Quantity'] / magnite_df['Quantity'] / magnite_df['Launch Flight %']
    magnite_df.loc[:, 'Earned Revenue'] = magnite_df['Net Unit Cost'] * magnite_df['Billable Quantity']/1000
    magnite_df.loc[:, 'Is Non-Ad Served'] = magnite_df['Deal ID'].isin([None])
    magnite_df.drop(['Parent Line Item Total', 'Parent Line Item Run Rate', 'Parent Line Item Earned Revenue'], inplace=True, axis=1)
    magnite_df = recalculate_projected(magnite_df)
    magnite_df = calculate_risk(magnite_df, report_date)

    df = pd.concat([other_df, magnite_df])

    df.fillna(
        {
            '1P Imps': 0,
            'Billable Quantity': 0,
            'Earned Revenue': 0,
        },
        inplace=True,
    )

    return df


def lifetime_delivery(staq_pacing: str, report_date: datetime) -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    operative_filename = 'Operative_OMS.parquet'
    operative = s3_dir + 'processed/' + operative_filename

    pacing_name = 'Ad Ops Pacing (Analytics) Fox New FW Placement Agg.csv'
    monthly_delivery_name = 'Ad Ops Pacing (Analytics) Fox New FW QTD Monthly Agg.csv'
    demo_monthly_name = 'Ad Ops Pacing (Analytics) Fox New FW Demo Monthly Agg.csv'
    demo_daily_name = 'Ad Ops Pacing (Analytics) Fox New FW Demo Daily Agg.csv'

    ad_ops_pacing = s3_dir + 'freewheel/' + pacing_name
    ad_ops_pacing_monthly = s3_dir + 'freewheel/' + monthly_delivery_name
    ad_ops_pacing_demo = s3_dir + 'freewheel/' + demo_monthly_name
    ad_ops_pacing_demo_daily = s3_dir + 'freewheel/' + demo_daily_name
    try:
        fw_demo_by_month = read_fw_demo_by_month(ad_ops_pacing_demo, ad_ops_pacing_demo_daily, report_date)
        fw_analytics = read_fw_analytics_lifetime(ad_ops_pacing, report_date)
        fw_monthly_delivery = read_fw_analytics_monthly(ad_ops_pacing_monthly, report_date)
    except:
        # Manually uploaded Freewheel files if SFTP cannot be accessed
        fw_demo_by_month = read_fw_demo_by_month(ad_ops_pacing_demo, ad_ops_pacing_demo_daily, report_date, skiprows=4)
        fw_analytics = read_fw_analytics_lifetime(ad_ops_pacing, report_date, skiprows=4)
        try:
            fw_monthly_delivery = read_fw_analytics_monthly(ad_ops_pacing_monthly, report_date, skiprows=4)
        except Exception:
            fw_monthly_delivery = pd.DataFrame(columns=['Placement ID', 'Event Month', 'Net Counted Ads'])

    # BAR-aligned demo: one monthly comp applied to each month's overall delivery,
    # then rolled to placement so pacing matches BAR (not one lifetime/campaign comp).
    monthly_delivery = fw_monthly_delivery.copy()
    if monthly_delivery.index.name == 'Placement ID' and 'Placement ID' not in monthly_delivery.columns:
        monthly_delivery = monthly_delivery.reset_index()
    if (
        not monthly_delivery.empty
        and 'Placement ID' in monthly_delivery.columns
        and 'Event Month' in monthly_delivery.columns
        and 'Net Counted Ads' in monthly_delivery.columns
    ):
        fw_demo_agg = delivery_weighted_monthly_demo_comp(
            fw_demo_by_month,
            monthly_delivery[['Placement ID', 'Event Month', 'Net Counted Ads']],
        )
    else:
        # Fallback: prior behavior if monthly delivery file is unavailable
        fw_demo_agg = read_fw_demo(ad_ops_pacing_demo, ad_ops_pacing_demo_daily, report_date)

    (fw_analytics_vpvh, vpvh_agg_dict, band_to_vpvh_mean) = merge_vpvh(fw_analytics, report_date)

    op_oms = pd.read_parquet(operative, storage_options={'profile': aws_profile}).set_index('Placement ID')
    staq_lifetime = read_staq(staq_pacing, skiprows=0)
    fw_adj_agg = merge_delivery_data(op_oms, fw_analytics, staq_lifetime, fw_analytics_vpvh, vpvh_agg_dict)
    op_fw_adj = merge_order_delivery(op_oms, fw_adj_agg, fw_demo_agg)
    op_fw_adj = calculate_metrics(op_fw_adj, band_to_vpvh_mean, report_date)

    op_fw_adj_vpvh_elim = apply_vpvh_elimination(op_fw_adj)

    op_fw_adj_pg = apply_pg_billing(op_fw_adj_vpvh_elim, report_date)

    op_fw_adj_pg_mag = apply_magnite_billing(op_fw_adj_pg, report_date)

    op_fw_adj_pg_mag = equivalize(op_fw_adj_pg_mag, 'pacing')

    return op_fw_adj_pg_mag


def export_lifetime_delivery(op_fw_adj: pd.DataFrame) -> str:
    with io.BytesIO() as output:
        op_fw_adj.to_parquet(output, index=False)
        op_fw_adj_data = output.getvalue()

    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    global drop_bucket
    s3_bucket = drop_bucket
    s3_key = 'processed/' + 'AdOps_Reporting_Lifetime_Delivery.parquet'
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=op_fw_adj_data,
        Key=s3_key,
    )

    return s3_key

# COMMAND ----------

def adops_lifetime_delivery(date: datetime, staq: str) -> None:
    global drop_bucket
    staq_data_dir = f's3://{staq_bucket}/'
    staq_pacing = staq_data_dir + staq
    report_date = datetime.combine(date, datetime.min.time())
    op_fw_adj_pg_mag = lifetime_delivery(staq_pacing, report_date)
    out = export_lifetime_delivery(op_fw_adj_pg_mag)
    write_xcom_value('AdOps Lifetime Delivery', out)

# COMMAND ----------

if __name__ == '__main__':
    report_date = dbutils.widgets.get('date')
    global drop_bucket, aws_profile, staq_bucket
    drop_bucket = dbutils.widgets.get('drop_bucket')
    aws_profile = dbutils.widgets.get('aws_profile')
    staq_bucket = dbutils.widgets.get('staq_bucket')

    def parse_date(date: str) -> bool:
        format = "%Y-%m-%d"
        try:
            res = bool(datetime.strptime(date, format))
        except ValueError:
            res = False
        return res
    
    staq = find_staq_file(datetime.strptime(report_date, "%Y-%m-%d"))

    assert parse_date(report_date), "Invalid date format, should be in YYYY-MM-DD format"
    assert staq, "Staq file not found"

    try:
        adops_lifetime_delivery(datetime.strptime(report_date, "%Y-%m-%d"), staq)
    except Exception as error:
        alert = Alert()
        alert.send('AdOps Lifetime Delivery', f'{type(error).__name__}: {error}')
        dbutils.notebook.exit(f'ERROR!!! - {error}')