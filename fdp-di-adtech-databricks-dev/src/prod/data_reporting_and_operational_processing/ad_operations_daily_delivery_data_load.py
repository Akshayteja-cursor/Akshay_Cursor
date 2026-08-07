# Databricks notebook source
# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/alert"

# COMMAND ----------

from typing import TYPE_CHECKING, Any, Dict
import math
import io
from datetime import datetime, timedelta
import boto3
import pandas as pd
import numpy as np

# COMMAND ----------

PROGRAMMATIC_TYPES = {'Programmatic Guaranteed – Freewheel', 'Programmatic Guaranteed – Magnite', 'Programmatic Guaranteed – SpotX', 'Reseller', 'Programmatic Guaranteed - Freewheel', 'Programmatic Guaranteed - Magnite', 'Programmatic Guaranteed - Freewheel (Paid Directly to Buyer)', 'Programmatic Guaranteed - SpotX'}

def operative_adj_server(row: pd.Series) -> Any:
    if source == 'OP1':
        adj_to_op_server = {
            'Moat': 'Moat',
            'DFA by Google': 'DCM',
            'Dart Report Reader': 'DCM',
            'Dart Report Reader, DFA by Google': 'DCM',
            'DoubleVerify': 'Doubleverify',
            'DFP by Google': 'DCM',
            'Innovid 3rd Party': 'Innovid',
            'FlashTalking': 'Flashtalking',
            'FlashTalking Email Reader': 'Flashtalking',
            'Extreme Reach Email': 'Extreme Reach',
            'ExtremeReachAPI': 'Extreme Reach',
            'MediaMind': 'Sizmek',
            'MediaMind Report Reader': 'Sizmek',
            'Sizmek SAS Report Reader': 'Sizmek',
            'MediaMind Report Reader, Sizmek SAS Report Reader': 'Sizmek',
            'Sizmek SAS Report Reader, MediaMind Report Reader': 'Sizmek',
            'TubeMogul': 'TubeMogul',
        }
    else:
        adj_to_op_server = {
            'FOX DCM': 'FOX DCM',
            'FOX Innovid': 'FOX Innovid',
            'FOX Flashtalking': 'FOX Flashtalking',
            'FOX Extreme Reach': 'FOX Extreme Reach',
            'DFA by Google': 'FOX DCM',
            'Dart Report Reader': 'FOX DCM',
            'Dart Report Reader, DFA by Google': 'FOX DCM',
            'DoubleVerify': 'FOX Doubleverify',
            'DFP by Google': 'FOX DCM',
            'Innovid 3rd Party': 'FOX Innovid',
            'FlashTalking': 'FOX Flashtalking',
            'FlashTalking Email Reader': 'FOX Flashtalking',
            'Flashtalking': 'FOX Flashtalking',
            'Extreme Reach Email': 'FOX Extreme Reach',
            'ExtremeReachAPI': 'FOX Extreme Reach',
            'MediaMind': 'FOX Sizmek',
            'MediaMind Report Reader': 'FOX Sizmek',
            'Sizmek SAS Report Reader': 'FOX Sizmek',
            'MediaMind Report Reader, Sizmek SAS Report Reader': 'FOX Sizmek',
            'Sizmek SAS Report Reader, MediaMind Report Reader': 'FOX Sizmek',
        }
    if row['3rd Party Server'] in adj_to_op_server:
        adj_server = adj_to_op_server[row['3rd Party Server']]
        return adj_server
    return math.nan


def read_3p_daily(current_month_file: str, previous_month_file: str, source: str) -> pd.DataFrame:
    if source == 'staq':
        current_month = read_staq_file(current_month_file)
        previous_month = read_staq_file(previous_month_file)
    else:
        current_month = read_adjuster_file(current_month_file)
        previous_month = read_adjuster_file(previous_month_file)
    third_party_data = pd.concat([current_month, previous_month])
    return third_party_data


def read_staq_file(staq_filename: str) -> pd.DataFrame:
    staq = pd.read_parquet(
        staq_filename,
        columns=[
            'Ad Unit Id',
            '3rd Party Billable Server',
            '3rd Party Billable Impressions',
            'Campaign Name',
            'Campaign Identifier',
            'Campaign Start',
            'Campaign End',
            '3rd Party Server',
            # 'Impressions (3rd Party)',
            'Clicks',
            'Clicks (3rd Party)',
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions',
            'Impressions Analyzed',
            '3rd Party Valid Impressions',
            '3rd Party Valid, Audible and Fully On-Screen'
            ' for Half of the Duration Impressions (15 sec)',
            '3rd Party Valid and Viewable Impressions',
            'Report End Date',
            '3P Line Item ID',
            '3P Line Item Name',
            '3P Advertiser ID',
            '3P Advertiser Name',
            '3P Order ID',
            '3P Order Name',
            'Associated Production System Name',
            'Associated Line Item Name',
            'Start Date',
            'End Date'
        ],
        storage_options={'profile': aws_profile},
    )
    staq['Campaign Identifier'] = pd.to_numeric(staq['Campaign Identifier'], errors='coerce')
    staq.fillna(
        {
            'Campaign Identifier': -1,
        },
        inplace=True
    )
    staq['3rd Party Billable Server'] = staq['3rd Party Billable Server'].combine_first(staq['3rd Party Server'])
    staq['3rd Party Billable Server'] = np.where(staq['3rd Party Billable Server'] == '',
                                                 staq['3rd Party Server'],
                                                 np.where(staq['3rd Party Billable Server'] == '\xa0',
                                                          staq['3rd Party Server'],
                                                          staq['3rd Party Billable Server']))
    staq.drop(columns=['3rd Party Server'], inplace=True)

    staq.rename(
        {
            '3rd Party Billable Server': '3rd Party Server',
            '3rd Party Billable Impressions': 'Impressions (3rd Party)',
        },
        axis=1,
        inplace=True
    )

    staq = staq.dropna(subset=['3rd Party Server'])

    return staq


def read_fw_daily(fw_daily_filename: str, skiprows: int = 0) -> pd.DataFrame:
    fw_analytics_daily = pd.read_csv(
        fw_daily_filename,
        skiprows=skiprows,
        usecols={
            'Ad Unit ID',
            'Placement ID',
            'Placement Name',
            'Advertiser ID',
            'Advertiser Name',
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
            'Event Date',
            'Placement Start Date',
            'Placement End Date',
            'Insertion Order ID',
            'Insertion Order Name',
        },
        dtype={
            'Ad Unit ID': 'int64',
            'Placement ID': 'int64',
            'Placement Name': 'object',
            'Advertiser ID': 'float64',
            'Advertiser Name': 'object',
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
            'Creative Duration': 'float64',
            'Ad Unit Price': 'object',
            'Insertion Order ID': 'int64',
            'Insertion Order Name': 'object',
        },
        na_values={
            'Forced Over Delivery Percent (%)': [
                'Network Default',
            ],
        },
        parse_dates=[
            'Event Date',
            'Placement Start Date',
            'Placement End Date',
        ],
        encoding='utf-8',
        storage_options={'profile': aws_profile},
    )

    int_cols = ['Net Counted Ads', 'Gross Counted Ads', 'Booked On-Target Impression Goal', 'Impression Goal']
    float_cols = ['Ad Unit Price']
    for col in int_cols:
        fw_analytics_daily[col] = fw_analytics_daily[col].str.replace(',', '').fillna(0).astype('int64')
    for col in float_cols:
        fw_analytics_daily[col] = fw_analytics_daily[col].str.replace(',', '').fillna(0).astype('float64')

    return fw_analytics_daily


def read_adjuster_file(adj_filename: str) -> pd.DataFrame:
    ad_juster = pd.read_parquet(
        adj_filename,
        columns=[
            'Ad Unit Id',
            '3rd Party Billable Server',
            '3rd Party Billable Impressions',
            'Campaign Name',
            'Campaign Identifier',
            'Campaign Start',
            'Campaign End',
            '3rd Party Server',
            # 'Impressions (3rd Party)',
            'Clicks',
            'Clicks (3rd Party)',
            '3rd Party Audible and Fully On-Screen'
            ' for Half of Duration Impressions',
            'Impressions Analyzed',
            '3rd Party Valid Impressions',
            '3rd Party Valid, Audible and Fully On-Screen'
            ' for Half of the Duration Impressions (15 sec)',
            '3rd Party Valid and Viewable Impressions',
            'Report End Date',
            '3P Line Item ID',
            '3P Line Item Name',
            '3P Advertiser ID',
            '3P Advertiser Name',
            '3P Order ID',
            '3P Order Name',
            'Associated Production System Name',
            'Associated Line Item Name',
            'Start Date',
            'End Date'
        ],
        storage_options={'profile': aws_profile},
    )
    ad_juster['Campaign Identifier'] = pd.to_numeric(ad_juster['Campaign Identifier'], errors='coerce')
    ad_juster.fillna(
        {
            'Campaign Identifier': -1,
        },
        inplace=True
    )
    ad_juster['3rd Party Billable Server'] = ad_juster['3rd Party Billable Server'].combine_first(ad_juster['3rd Party Server'])
    ad_juster['3rd Party Billable Server'] = np.where(ad_juster['3rd Party Billable Server'] == '',
                                                      ad_juster['3rd Party Server'],
                                                      np.where(ad_juster['3rd Party Billable Server'] == '\xa0',
                                                               ad_juster['3rd Party Server'],
                                                               ad_juster['3rd Party Billable Server']))
    ad_juster.drop(columns=['3rd Party Server'], inplace=True)

    ad_juster.rename(
        {
            '3rd Party Billable Server': '3rd Party Server',
            '3rd Party Billable Impressions': 'Impressions (3rd Party)',
        },
        axis=1,
        inplace=True
    )

    ad_juster = ad_juster.dropna(subset=['3rd Party Server'])

    return ad_juster


def merge_daily_delivery(
    op_oms: pd.DataFrame,
    fw_analytics: pd.DataFrame,
    staq: pd.DataFrame,
    fw_analytics_vpvh: pd.DataFrame,
    filter_date_range: pd.DatetimeIndex,
    vpvh_agg_dict: Dict[str, Any]) -> pd.DataFrame:

    # Create a copy for GAM 3P Delivery
    gam_3p = staq.copy(deep=True)

    combined_3p = staq.copy(deep=True)
    combined_3p = combined_3p.set_index('Ad Unit Id')

    fw_ad_unit_agg = {
        'Placement ID': lambda x: x.iloc[0],
        'FFDR (%)': 'max',
        'Forced Over Delivery Percent (%)': 'max',
        'Advertiser ID': 'max',
        'Advertiser Name': 'max',
        'Campaign ID': 'max',
        'Campaign Name': 'max',
        'Placement Name': 'max',
        'Series Name': 'max',
        'Budget Model': 'max',
        'Creative Duration': 'max',
        'Impression Goal': 'max',
        'Net Counted Ads': 'sum',
        'Gross Counted Ads': 'sum',
        'Booked On-Target Impression Goal': 'max',
        'Ad Unit Price': 'max',
        'Placement Start Date': 'min',
        'Placement End Date': 'max',
        'Insertion Order ID': 'max',
        'Insertion Order Name': 'max',
    }

    fw_ad_unit_agg.update(vpvh_agg_dict)

    fw_analytics_agg = fw_analytics_vpvh.reset_index().rename(
        {
            'Ad Unit ID': 'Ad Unit Id',
            'Event Date': 'Report End Date',
        },
        axis=1,
    ).groupby(
        [
            'Ad Unit Id',
            'Report End Date',
        ],
    ).aggregate(
        fw_ad_unit_agg,
    )

    # MOAT data is used to calculate Viewability
    combined_3p_moat = combined_3p.loc[combined_3p['3rd Party Server'] == 'Moat', :]
    combined_3p_moat_agg = combined_3p_moat.groupby(['Ad Unit Id', 'Report End Date']).aggregate(
        {
            '3rd Party Valid, Audible and Fully On-Screen for Half of the Duration Impressions (15 sec)': 'sum',
            '3rd Party Valid Impressions': 'sum',
            '3rd Party Audible and Fully On-Screen for Half of Duration Impressions': 'sum',
            'Impressions Analyzed': 'sum',
            '3rd Party Valid and Viewable Impressions': 'sum',
        },
    )

    combined_3p_all = combined_3p[[
        'Report End Date',
        '3rd Party Server',
        'Impressions (3rd Party)',
        'Clicks',
        'Clicks (3rd Party)',
        '3P Line Item ID',
        '3P Line Item Name',
        '3P Advertiser ID',
        '3P Advertiser Name',
        '3P Order ID',
        '3P Order Name',
    ]].reset_index().set_index(['Ad Unit Id', 'Report End Date', '3rd Party Server'])

    combined_3p_all_agg = combined_3p_all.dropna(subset=['Impressions (3rd Party)']).groupby(level=[0, 1, 2]).aggregate(
        {
            'Impressions (3rd Party)': 'sum',
            'Clicks': 'sum',
            'Clicks (3rd Party)': 'sum',
            '3P Line Item ID': 'max',
            '3P Line Item Name': 'max',
            '3P Advertiser ID': 'max',
            '3P Advertiser Name': 'max',
            '3P Order ID': 'max',
            '3P Order Name': 'max',
        },
    ).reset_index()

    ad_unit_to_3p = billable_ad_unit(fw_analytics, op_oms)

    combined_3p_all_agg.loc[:, 'Billable 3rd Party Server'] = combined_3p_all_agg.apply(
        lambda row: billable_adj_server(row, ad_unit_to_3p, 'Ad Unit Id'),
        axis=1,
    )

    combined_3p_all_agg.loc[:, 'Operative 3rd Party Server'] = combined_3p_all_agg.apply(
        operative_adj_server,
        axis=1,
    )

    combined_3p_all_agg = combined_3p_all_agg.set_index(
        ['Ad Unit Id', 'Report End Date'],
    )

    combined_3p_joined = pd.merge(
        combined_3p_all_agg, combined_3p_moat_agg, left_index=True, right_index=True, how='outer',
    )

    fw_3p = fw_analytics_agg.join(combined_3p_joined)

    fw_3p.fillna(
        {
            'Impressions (3rd Party)': 0,
            'Clicks': 0,
            'Clicks (3rd Party)': 0,
            '3P Line Item ID': fw_3p['Placement ID'],
            '3P Line Item Name': fw_3p['Placement Name'],
            '3P Advertiser ID': fw_3p['Advertiser ID'],
            '3P Advertiser Name': fw_3p['Advertiser Name'],
            '3P Order ID': fw_3p['Insertion Order ID'],
            '3P Order Name': fw_3p['Insertion Order Name'],
            'Operative 3rd Party Server': '1st Party',
        },
        inplace=True,
    )

    fw_3p = fw_3p.astype(
        {
            '3P Line Item ID': 'str',
            '3P Advertiser ID': 'str',
        },
    )

    fw_3p.reset_index(inplace=True)

    fw_3p_filtered = fw_3p.loc[fw_3p['Report End Date'].isin(filter_date_range), :]

    fw_3p_placement = fw_3p_filtered.set_index(
        [
            'Placement ID',
            'Report End Date',
            'Operative 3rd Party Server',
        ],
    )

    fw_placement_agg = {
        'FFDR (%)': 'max',
        'Forced Over Delivery Percent (%)': 'max',
        'Advertiser ID': 'max',
        'Advertiser Name': 'max',
        'Campaign ID': 'max',
        'Campaign Name': 'max',
        'Placement Name': lambda x: x.iloc[0],
        'Series Name': 'max',
        'Budget Model': 'max',
        'Impression Goal': 'max',
        'Booked On-Target Impression Goal': 'max',
        'Creative Duration': 'max',
        'Ad Unit Price': 'max',
        'Net Counted Ads': 'sum',
        'Gross Counted Ads': 'sum',
        'Impressions (3rd Party)': 'sum',
        'Clicks': 'sum',
        'Clicks (3rd Party)': 'sum',
        '3rd Party Valid, Audible and Fully On-Screen for Half of the Duration Impressions (15 sec)': 'sum',
        '3rd Party Valid Impressions': 'sum',
        '3rd Party Audible and Fully On-Screen for Half of Duration Impressions': 'sum',
        'Impressions Analyzed': 'sum',
        '3rd Party Valid and Viewable Impressions': 'sum',
        'Placement Start Date': 'min',
        'Placement End Date': 'max',
        '3P Line Item ID': 'max',
        '3P Line Item Name': 'max',
        '3P Advertiser ID': 'max',
        '3P Advertiser Name': 'max',
        '3P Order ID': 'max',
        '3P Order Name': 'max',
        'Insertion Order ID': 'max',
        'Insertion Order Name': 'max',
    }

    fw_placement_agg.update(vpvh_agg_dict)
    fw_3p_agg = fw_3p_placement.groupby(level=[0, 1, 2]).aggregate(fw_placement_agg)
    fw_3p_agg = fw_3p_agg.reset_index()
    global fw_placements
    fw_placements = list(set(fw_3p_agg['Placement ID']))
    fw_3p_agg = fw_3p_agg.set_index(['Placement ID'])

    # Remove the freewheel placements from the GAM 3P dataframe
    gam_3p = gam_3p.dropna(subset=['Campaign Identifier'])
    gam_3p['Campaign Identifier'] = gam_3p['Campaign Identifier'].astype('int64')
    gam_3p = gam_3p[~gam_3p['Campaign Identifier'].isin(fw_placements)]
    gam_3p.set_index('Campaign Identifier', inplace=True)

    gam_3p = gam_3p.loc[gam_3p['Report End Date'].isin(filter_date_range), :]

    # MOAT data is used to calculate Viewability
    gam_3p_moat = gam_3p.loc[gam_3p['3rd Party Server'] == 'Moat', :]
    gam_3p_moat_agg = gam_3p_moat.groupby(['Campaign Identifier', 'Report End Date']).aggregate(
        {
            '3rd Party Valid, Audible and Fully On-Screen for Half of the Duration Impressions (15 sec)': 'sum',
            '3rd Party Valid Impressions': 'sum',
            '3rd Party Audible and Fully On-Screen for Half of Duration Impressions': 'sum',
            'Impressions Analyzed': 'sum',
            '3rd Party Valid and Viewable Impressions': 'sum',
        },
    )

    gam_3p_all = gam_3p[[
        'Ad Unit Id',
        'Report End Date',
        '3rd Party Server',
        'Impressions (3rd Party)',
        'Clicks',
        'Clicks (3rd Party)',
        '3P Line Item ID',
        '3P Line Item Name',
        '3P Advertiser ID',
        '3P Advertiser Name',
        '3P Order ID',
        '3P Order Name',
        'Campaign Name',
        'Campaign Start',
        'Campaign End',
    ]].reset_index().set_index(['Campaign Identifier', 'Report End Date', '3rd Party Server'])

    gam_3p_all_agg = gam_3p_all.dropna(subset=['Impressions (3rd Party)']).groupby(level=[0, 1, 2]).aggregate(
        {
            'Ad Unit Id': 'max',
            'Impressions (3rd Party)': 'sum',
            'Clicks': 'sum',
            'Clicks (3rd Party)': 'sum',
            '3P Line Item ID': 'max',
            '3P Line Item Name': 'max',
            '3P Advertiser ID': 'max',
            '3P Advertiser Name': 'max',
            '3P Order ID': 'max',
            '3P Order Name': 'max',
            'Campaign Name': 'max',
            'Campaign Start': 'max',
            'Campaign End': 'max',
        },
    ).reset_index()

    gam_3p_all_agg = gam_3p_all_agg.set_index(
        ['Campaign Identifier', 'Report End Date'],
    )

    gam_3p_joined = pd.merge(
        gam_3p_all_agg, gam_3p_moat_agg, left_index=True, right_index=True, how='outer',
    )

    gam_3p_joined = gam_3p_joined.reset_index()
    gam_3p_joined.loc[:, 'Sales Line Item ID'] = gam_3p_joined.apply(extract_sales_line_item_id, args=('Campaign Name',), axis=1)
    gam_3p_joined = pd.merge(gam_3p_joined, op_oms.reset_index()[['Placement ID', 'Sales Line Item ID']], left_on='Campaign Identifier', right_on='Placement ID', how='left', suffixes=('_x', '_y'))
    gam_3p_joined['Sales Line Item ID'] = gam_3p_joined['Sales Line Item ID_x'].combine_first(gam_3p_joined['Sales Line Item ID_y'])
    gam_3p_joined = gam_3p_joined.drop(['Placement ID', 'Sales Line Item ID_x', 'Sales Line Item ID_y'], axis=1)
    gam_3p_joined = gam_3p_joined.dropna(subset=['Sales Line Item ID'])
    gam_3p_joined['Sales Line Item ID'] = gam_3p_joined['Sales Line Item ID'].astype('int64')
    gam_3p_agg = gam_3p_joined.set_index(['Sales Line Item ID', 'Report End Date'])

    campaign_to_3p = billable_campaign(gam_3p_agg.reset_index(), op_oms)

    gam_3p_agg.loc[:, 'Billable 3rd Party Server'] = gam_3p_agg.apply(
        lambda row: billable_adj_server(row, campaign_to_3p, 'Campaign Name'),
        axis=1,
    )

    gam_3p_agg.loc[:, 'Operative 3rd Party Server'] = gam_3p_agg.apply(
        operative_adj_server,
        axis=1,
    )

    # We are only interested in GAM 3P delivery data
    # 1P Delivery is directly ingested from the GAM API
    gam_3p_agg.loc[:, 'Net Counted Ads'] = gam_3p_agg['Impressions (3rd Party)']

    # Temporary solution for Demo Comp and Equivalization
    gam_3p_agg.loc[:, 'Creative Duration'] = 0
    gam_3p_agg.loc[:, 'Gross Counted Ads (Demo)'] = 0
    gam_3p_agg.loc[:, 'On-Target Net Delivered Impressions'] = 0
    gam_3p_agg.loc[:, 'Booked On-Target Impression Goal'] = 0
    gam_3p_agg.loc[:, 'Impression Goal'] = 0
    gam_3p_agg.loc[:, 'Ad Unit Price'] = 0

    gam_3p_agg = gam_3p_agg.reset_index().set_index(['Sales Line Item ID'])

    # gam sports for associated production name = Google Ad Manager - Fox Deportes

    gam_3p_sports = pd.merge(staq, op_oms[['Production System Name']], left_on='Campaign Identifier', right_index=True, how='left')
    gam_3p_sports = gam_3p_sports[
        gam_3p_sports['Associated Production System Name'] == 'Google Ad Manager - Fox Deportes'
    ]


    # Remove the freewheel placements from the GAM 3P dataframe
    gam_3p_sports = gam_3p_sports.dropna(subset=['Campaign Identifier'])
    gam_3p_sports['Campaign Identifier'] = gam_3p_sports['Campaign Identifier'].astype('int64')
    # gam_3p_sports = gam_3p_sports[~gam_3p_sports['Campaign Identifier'].isin(fw_placements)]  ################################
    gam_3p_sports.set_index('Campaign Identifier', inplace=True)

    gam_3p_sports = gam_3p_sports.loc[gam_3p_sports['Report End Date'].isin(filter_date_range), :]

    # MOAT data is used to calculate Viewability
    gam_3p_sports_moat = gam_3p_sports.loc[gam_3p_sports['3rd Party Server'] == 'Moat', :]
    gam_3p_sports_moat_agg = gam_3p_sports_moat.groupby(['Campaign Identifier', 'Report End Date']).aggregate(
        {
            '3rd Party Valid, Audible and Fully On-Screen for Half of the Duration Impressions (15 sec)': 'sum',
            '3rd Party Valid Impressions': 'sum',
            '3rd Party Audible and Fully On-Screen for Half of Duration Impressions': 'sum',
            'Impressions Analyzed': 'sum',
            '3rd Party Valid and Viewable Impressions': 'sum',
        },
    )

    gam_3p_sports_all = gam_3p_sports[[
        'Ad Unit Id',
        'Report End Date',
        '3rd Party Server',
        'Impressions (3rd Party)',
        'Clicks',
        'Clicks (3rd Party)',
        '3P Line Item ID',
        '3P Line Item Name',
        '3P Advertiser ID',
        '3P Advertiser Name',
        '3P Order ID',
        '3P Order Name',
        'Campaign Name',
        'Campaign Start',
        'Campaign End',
        'Associated Line Item Name',
        'Start Date',
        'End Date'
    ]].reset_index().set_index(['Campaign Identifier', 'Report End Date', '3rd Party Server'])

    gam_3p_sports_all_agg = gam_3p_sports_all.dropna(subset=['Impressions (3rd Party)']).groupby(level=[0, 1, 2]).aggregate(
        {
            'Ad Unit Id': 'max',
            'Impressions (3rd Party)': 'sum',
            'Clicks': 'sum',
            'Clicks (3rd Party)': 'sum',
            '3P Line Item ID': 'max',
            '3P Line Item Name': 'max',
            '3P Advertiser ID': 'max',
            '3P Advertiser Name': 'max',
            '3P Order ID': 'max',
            '3P Order Name': 'max',
            'Campaign Name': 'max',
            'Campaign Start': 'max',
            'Campaign End': 'max',
            'Associated Line Item Name': 'max',
            'Start Date': 'max',
            'End Date': 'max',
        },
    ).reset_index()

    gam_3p_sports_all_agg = gam_3p_sports_all_agg.set_index(
        ['Campaign Identifier', 'Report End Date'],
    )

    gam_3p_sports_joined = pd.merge(
        gam_3p_sports_all_agg, gam_3p_sports_moat_agg, left_index=True, right_index=True, how='outer',
    )

    gam_3p_sports_joined = gam_3p_sports_joined.reset_index()
    gam_3p_sports_joined.loc[:, 'Sales Line Item ID'] = gam_3p_sports_joined.apply(extract_sales_line_item_id, args=('Campaign Name',), axis=1)
    gam_3p_sports_joined = pd.merge(gam_3p_sports_joined, op_oms.reset_index()[['Placement ID', 'Sales Line Item ID']], left_on='Campaign Identifier', right_on='Placement ID', how='left', suffixes=('_x', '_y'))
    gam_3p_sports_joined['Sales Line Item ID'] = gam_3p_sports_joined['Sales Line Item ID_x'].combine_first(gam_3p_sports_joined['Sales Line Item ID_y'])
    gam_3p_sports_joined = gam_3p_sports_joined.drop(['Placement ID', 'Sales Line Item ID_x', 'Sales Line Item ID_y'], axis=1)
    gam_3p_sports_joined = gam_3p_sports_joined.dropna(subset=['Sales Line Item ID'])
    gam_3p_sports_joined['Sales Line Item ID'] = gam_3p_sports_joined['Sales Line Item ID'].astype('int64')
    gam_3p_sports_agg = gam_3p_sports_joined.set_index(['Sales Line Item ID', 'Report End Date'])

    campaign_to_3p = billable_campaign(gam_3p_sports_agg.reset_index(), op_oms)

    gam_3p_sports_agg.loc[:, 'Billable 3rd Party Server'] = gam_3p_sports_agg.apply(
        lambda row: billable_adj_server(row, campaign_to_3p, 'Campaign Name'),
        axis=1,
    )

    gam_3p_sports_agg.loc[:, 'Operative 3rd Party Server'] = gam_3p_sports_agg.apply(
        operative_adj_server,
        axis=1,
    )

    # We are only interested in GAM 3P delivery data
    # 1P Delivery is directly ingested from the GAM API
    gam_3p_sports_agg.loc[:, 'Net Counted Ads'] = gam_3p_sports_agg['Impressions (3rd Party)']

    # Temporary solution for Demo Comp and Equivalization
    gam_3p_sports_agg.loc[:, 'Creative Duration'] = 0
    gam_3p_sports_agg.loc[:, 'Gross Counted Ads (Demo)'] = 0
    gam_3p_sports_agg.loc[:, 'On-Target Net Delivered Impressions'] = 0
    gam_3p_sports_agg.loc[:, 'Booked On-Target Impression Goal'] = 0
    gam_3p_sports_agg.loc[:, 'Impression Goal'] = 0
    gam_3p_sports_agg.loc[:, 'Ad Unit Price'] = 0

    gam_3p_sports_agg = gam_3p_sports_agg.reset_index().set_index(['Sales Line Item ID'])

    # fw_3p_agg.loc[:, 'Delivery Source'] = 'Freewheel + Adjuster'
    gam_3p_sports_agg.loc[:, 'Delivery Source'] = 'Staq(Fox Deportes)'
    fw_3p_agg.loc[:, 'Delivery Source'] = 'Freewheel + Adjuster'
    gam_3p_agg.loc[:, 'Delivery Source'] = 'Adjuster'

    return fw_3p_agg, gam_3p_agg, gam_3p_sports_agg


def merge_order_delivery_daily_freewheel(op_oms: pd.DataFrame, fw_adj_agg: pd.DataFrame, fw_demo_by_month: pd.DataFrame) -> pd.DataFrame:
    op_oms = op_oms.drop(['Advertiser ID', 'Advertiser Name'], axis=1)
    # Keep daily grain (Placement ID + Report End Date), then attach that month's
    # single monthly demo rate so AOS matches BAR (not day-varying comps).
    op_fw_adj = fw_adj_agg.reset_index().set_index('Placement ID').join(op_oms, how='outer')
    op_fw_adj = op_fw_adj.reset_index()
    if 'Report End Date' in op_fw_adj.columns:
        op_fw_adj = attach_monthly_demo_to_daily(op_fw_adj, fw_demo_by_month, date_col='Report End Date')
    # Select either the rows that have a matching line in the FW analytics data, or the rows that are Programmatic (these get
    # delivery assigned later so they don't show up in the FW delivery data).
    if source == 'OP1':
        op_fw_adj = op_fw_adj[~op_fw_adj['Placement Name'].isna() | op_fw_adj['Programmatic Type'].isin(PROGRAMMATIC_TYPES)]
        op_fw_adj = drop_freewheel_inactive(op_fw_adj)
    op_fw_adj['Sales Line Item Name'] = op_fw_adj['Sales Line Item Name'].fillna('').astype('str')
    return op_fw_adj

def merge_order_delivery_daily_gam(op_oms: pd.DataFrame, gam_adj_agg: pd.DataFrame) -> pd.DataFrame:
    op_oms = op_oms.reset_index().set_index('Sales Line Item ID')
    op_oms = op_oms[~op_oms['Placement ID'].isin(fw_placements)]
    op_gam_adj = (gam_adj_agg.reset_index().set_index('Sales Line Item ID')).join(op_oms, how='outer', lsuffix='x', rsuffix='y')
    op_gam_adj['Sales Line Item Name'] = op_gam_adj['Sales Line Item Name'].fillna('').astype('str')
    op_gam_adj.reset_index(inplace=True)
    return op_gam_adj


def fix_3p_advertiser_gam(op_gam_adj_daily: pd.DataFrame) -> pd.DataFrame:
    def fix_advertiser_id(row: pd.Series) -> str:
        if row['3P Advertiser ID'] is None or (isinstance(row['3P Advertiser ID'], float) and math.isnan(row['3P Advertiser ID'])):
            return row['Advertiser ID']
        elif not row['3P Advertiser ID'].isnumeric():
            return row['Advertiser ID']
        return row['3P Advertiser ID']

    def fix_advertiser_name(row: pd.Series) -> str:
        if row['3P Advertiser ID'] is None or (isinstance(row['3P Advertiser ID'], float) and math.isnan(row['3P Advertiser ID'])):
            return row['Advertiser Name']
        elif not row['3P Advertiser ID'].isnumeric():
            return row['Advertiser Name']
        return row['3P Advertiser Name']

    op_gam_adj_daily.loc[:, '3P Advertiser ID temp'] = op_gam_adj_daily.apply(fix_advertiser_id, axis=1)
    op_gam_adj_daily.loc[:, '3P Advertiser Name temp'] = op_gam_adj_daily.apply(fix_advertiser_name, axis=1)

    op_gam_adj_daily.drop(['3P Advertiser ID', '3P Advertiser Name'], axis=1, inplace=True)
    op_gam_adj_daily = op_gam_adj_daily.rename(
        {
            '3P Advertiser ID temp': '3P Advertiser ID',
            '3P Advertiser Name temp': '3P Advertiser Name'
        },
        axis=1,
    ).astype(
        {
            '3P Advertiser ID': 'float64',
            '3P Advertiser Name': 'str'
        }
    )

    return op_gam_adj_daily


def fix_3p_order_gam(op_gam_adj_daily: pd.DataFrame) -> pd.DataFrame:
    op_gam_adj_daily.fillna(
        {
            '3P Order ID': op_gam_adj_daily['Sales Order ID'],
            '3P Order Name': op_gam_adj_daily['Sales Order Name']
        },
        inplace=True,
    )

    return op_gam_adj_daily


def prod_sys_lookup() -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    if source == 'OP1':
        prod_sys_filename = 'Production System Mapping.xlsx'
    else:
        prod_sys_filename = 'Production System Mapping AOS.xlsx'
    prod_sys_file = s3_dir + 'lookup/' + prod_sys_filename
    prod_sys_mapping = pd.read_excel(
        prod_sys_file,
        usecols=[
            'Production System ID',
            'Name',
        ],
        dtype={
            'Production System ID': 'object',
            'Name': 'object',
        },
        index_col='Name',
        storage_options={'profile': aws_profile},
    ).to_dict()['Production System ID']
    return prod_sys_mapping


def prod_sys_name(row: pd.Series) -> Any:
    if source == 'AOS':
        if row['Production System Name'] == '1st Party':
            return 'FOX 1st Party'
    if row['Production System Name'] == 'Programmatic Guaranteed Production System':
        return row['Production System Name']
    if row['Operative 3rd Party Server'] == 'DFA':
        return 'DCM'
    return row['Operative 3rd Party Server']


def prod_sys_id(row: pd.Series, prod_sys_mapping: Dict[Any, Any], name_col: str) -> Any:
    if row[name_col] in prod_sys_mapping:
        return prod_sys_mapping[row[name_col]]
    return '-1'


def daily_delivery(staq_daily_file: str, staq_daily_pm_file: str, report_date: datetime) -> pd.DataFrame:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    operative_filename = 'Operative_OMS.parquet'
    operative = s3_dir + 'processed/' + operative_filename

    fw_daily_mtd_name = 'Ad Ops Pacing (Analytics) Fox New FW Daily Agg Current Month.csv'
    fw_daily_pm_name = 'Ad Ops Pacing (Analytics) Fox New FW Daily Agg Last Month.csv'
    demo_monthly_name = 'Ad Ops Pacing (Analytics) Fox New FW Demo Monthly Agg.csv'
    demo_daily_name = 'Ad Ops Pacing (Analytics) Fox New FW Demo Daily Agg.csv'

    fw_daily_mtd = s3_dir + 'freewheel/' + fw_daily_mtd_name
    fw_daily_pm = s3_dir + 'freewheel/' + fw_daily_pm_name
    ad_ops_pacing_demo = s3_dir + 'freewheel/' + demo_monthly_name
    ad_ops_pacing_demo_daily = s3_dir + 'freewheel/' + demo_daily_name
    try:
        fw_demo_by_month = read_fw_demo_by_month(ad_ops_pacing_demo, ad_ops_pacing_demo_daily, report_date)
        fw_analytics_daily_mtd = read_fw_daily(fw_daily_mtd)
        fw_analytics_daily_pm = read_fw_daily(fw_daily_pm)
    except:
        # Manually uploaded Freewheel files if SFTP cannot be accessed
        fw_demo_by_month = read_fw_demo_by_month(ad_ops_pacing_demo, ad_ops_pacing_demo_daily, report_date, skiprows=4)
        fw_analytics_daily_mtd = read_fw_daily(fw_daily_mtd, skiprows=4)
        fw_analytics_daily_pm = read_fw_daily(fw_daily_pm, skiprows=4)

    fw_analytics_daily = fw_analytics_daily_mtd.append(fw_analytics_daily_pm)

    staq_daily = read_3p_daily(staq_daily_file, staq_daily_pm_file, 'staq')
    op_oms = pd.read_parquet(operative, storage_options={'profile': aws_profile},).set_index('Placement ID')
    (fw_daily_vpvh, vpvh_agg_dict, band_to_vpvh_mean) = merge_vpvh(fw_analytics_daily, report_date)

    # Calculate the date range from start of previous month and today
    prev_month = report_date.month - 1 if report_date.month != 1 else 12
    bill_year = report_date.year - 1 if prev_month == 12 else report_date.year
    filter_date_range = pd.date_range(
        start=datetime.combine(
            report_date,
            datetime.min.time(),
        ).replace(
            month=prev_month,
            year=bill_year,
            day=1,
        ),
        end=datetime.combine(
            report_date,
            datetime.min.time(),
        ),
    )

    fw_3p_agg_daily, gam_adj_agg_daily,gam_3p_sports_agg_daily = merge_daily_delivery(op_oms, fw_analytics_daily, staq_daily, fw_daily_vpvh, filter_date_range, vpvh_agg_dict)
    op_fw_3p_daily = merge_order_delivery_daily_freewheel(op_oms, fw_3p_agg_daily, fw_demo_by_month)
    op_gam_adj_daily = merge_order_delivery_daily_gam(op_oms, gam_adj_agg_daily)
    op_fw_3p_daily = calculate_metrics(op_fw_3p_daily, band_to_vpvh_mean, report_date)
    op_gam_adj_daily = calculate_metrics(op_gam_adj_daily, band_to_vpvh_mean, report_date)
    op_gam_adj_daily = fix_3p_advertiser_gam(op_gam_adj_daily)
    op_gam_adj_daily = fix_3p_order_gam(op_gam_adj_daily)
    ##
    gam_3p_sports_daily = merge_order_delivery_daily_gam(op_oms, gam_3p_sports_agg_daily)
    gam_3p_sports_daily = calculate_metrics(gam_3p_sports_daily, band_to_vpvh_mean, report_date)
    gam_3p_sports_daily = fix_3p_advertiser_gam(gam_3p_sports_daily)
    gam_3p_sports_daily = fix_3p_order_gam(gam_3p_sports_daily)
    gam_3p_sports_daily.reset_index(inplace=True)
    ##
    op_fw_3p_daily.reset_index(inplace=True)
    op_gam_adj_daily.reset_index(inplace=True)

    ps_lookup = prod_sys_lookup()
    op_fw_3p_daily.loc[:, 'Production System Name'] = op_fw_3p_daily.apply(prod_sys_name, axis=1)
    op_fw_3p_daily.loc[:, 'Production System ID'] = op_fw_3p_daily.apply(
        lambda row: prod_sys_id(row, ps_lookup, 'Production System Name'),
        axis=1,
    )
    op_gam_adj_daily.loc[:, 'Production System Name'] = op_gam_adj_daily.apply(prod_sys_name, axis=1)
    op_gam_adj_daily.loc[:, 'Production System ID'] = op_gam_adj_daily.apply(
        lambda row: prod_sys_id(row, ps_lookup, 'Production System Name'),
        axis=1,
    )

    gam_3p_sports_daily.loc[:, 'Production System Name'] = gam_3p_sports_daily.apply(prod_sys_name, axis=1)
    gam_3p_sports_daily.loc[:, 'Production System ID'] = gam_3p_sports_daily.apply(
        lambda row: prod_sys_id(row, ps_lookup, 'Production System Name'),
        axis=1,
    )

    return op_fw_3p_daily, op_gam_adj_daily,gam_3p_sports_daily


def export_daily_delivery(op_fw_adj_daily: pd.DataFrame, source: str) -> str:
    with io.BytesIO() as output:
        op_fw_adj_daily.to_parquet(
            output,
            index=False,
        )
        op_fw_adj_daily_data = output.getvalue()

    session = boto3.Session(profile_name = aws_profile)
    s3_client = session.client('s3')
    global drop_bucket
    s3_bucket = drop_bucket
    s3_key = 'processed/' + source + '_AdOps_Reporting_Daily_Delivery.parquet'
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=op_fw_adj_daily_data,
        Key=s3_key,
    )

    return s3_key

# COMMAND ----------

def adops_daily_delivery(date: datetime) -> None:
    global drop_bucket

    current_month_dt = date.strftime('%Y-%m')
    staq_daily = f's3://{drop_bucket}/staq/monthly/dt={current_month_dt}/staq_monthly_{current_month_dt}.parquet'

    previous_month_dt = (date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    staq_daily_pm = f's3://{drop_bucket}/staq/monthly/dt={previous_month_dt}/staq_monthly_{previous_month_dt}.parquet'

    report_date = datetime.combine(date.date(), datetime.min.time())

    fw, gam, gam_sports = daily_delivery(staq_daily, staq_daily_pm, report_date)
    out1 = export_daily_delivery(fw, 'freewheel')
    out2 = export_daily_delivery(gam, 'gam')
    out3 = export_daily_delivery(gam_sports,'gam_sports')

    write_xcom_value('AdOps Daily Delivery Freewheel', out1)
    write_xcom_value('AdOps Daily Delivery GAM', out2)
    write_xcom_value('AdOps Daily Delivery GAM Sports', out3)



# COMMAND ----------

if __name__ == '__main__':
    report_date = dbutils.widgets.get('date')
    global drop_bucket, source, aws_profile
    drop_bucket = dbutils.widgets.get('drop_bucket')
    aws_profile = dbutils.widgets.get('aws_profile')
    staq_bucket = dbutils.widgets.get('staq_bucket')
    source = dbutils.widgets.get('source')

    def parse_date(date: str) -> bool:
        format = "%Y-%m-%d"
        try:
            res = bool(datetime.strptime(date, format))
        except ValueError:
            res = False
        return res

    assert parse_date(report_date), "Invalid date format, should be in YYYY-MM-DD format"
    assert source != '', "Source is required"

    try:
        adops_daily_delivery(datetime.strptime(report_date, "%Y-%m-%d"))
    except Exception as error:
        alert = Alert()
        alert.send('AdOps Daily Delivery', f'{type(error).__name__}: {error}')
        dbutils.notebook.exit(f'ERROR!!! - {error}')