# Databricks notebook source
# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/core"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/adtech/fdp-di-adtech-databricks/src/prod/data_reporting_and_operational_processing/alert"

# COMMAND ----------

from typing import Any
import re
import io
from datetime import datetime, timedelta
import pandas as pd
import boto3
import math

# COMMAND ----------

def find_staq_files(report_date: datetime) -> str:
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')

    staq1 = 'STAQ_Adjuster_Exec_Summary_MTD_This_Month_' + report_date.strftime('%Y-%m-%d')
    staq2 = 'STAQ_Adjuster_Exec_Summary_QTD_This_Quarter_' + report_date.strftime('%Y-%m-%d')

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=staq_bucket)

    file_1 = ''
    file_2 = ''

    for page in page_iterator:
        for obj in page.get('Contents', []):
            if staq1 in obj['Key']:
                file_1 = obj['Key']
            if staq2 in obj['Key']:
                file_2 = obj['Key']

    return file_1, file_2

# COMMAND ----------

def financial_period_agg(fw_analytics: pd.Series, adj_period_filename: str, op_fw_adj: str, period_name: str) -> pd.DataFrame:
    if period_name == 'Quarter':
        period_abbrv = 'EOQ'
    elif period_name == 'Month':
        period_abbrv = 'EOM'
    else:
        raise ValueError
    fw_analytics_period = fw_analytics[
        fw_analytics[
            'In Current ' + period_name
        ]
    ][
        [
            'Ad Unit ID',
            'Placement ID',
            'Net Counted Ads'
        ]
    ]
    fw_period_agg = fw_analytics_period.groupby('Ad Unit ID').agg(
        {
            'Placement ID': 'max',
            'Net Counted Ads': 'sum'
        }
    )

    try:
        adj_period = pd.read_csv(
            adj_period_filename,
            skiprows=0,
            usecols=[
                'Ad Unit ID',
                '3rd Party Server',
                'Impressions (3rd Party)'
            ],
            dtype={
                'Ad Unit ID': 'float64',
                '3rd Party Server': 'object',
                'Impressions (3rd Party)': 'object'
            },
            encoding='utf-8',
            storage_options={'profile': aws_profile}
        ).dropna(
            subset=[
                'Ad Unit ID',
                'Impressions (3rd Party)'
            ]
        ).astype(
            {
                'Ad Unit ID': 'int64',
            }
        )
        adj_period = adj_period.rename(
             {
                 'Ad Unit ID': 'Ad Unit Id'
             },
             axis=1
         )

        adj_period['Impressions (3rd Party)'] = adj_period['Impressions (3rd Party)'].str.replace(',', '').astype('int64')
    except:
        print('Exception occurred')
        adj_period = pd.DataFrame(
            columns=[
                'Impressions (3rd Party)',
                '3rd Party Server',
                'Ad Unit ID'
            ]
        )
        adj_period = adj_period.rename(
             {
                 'Ad Unit ID': 'Ad Unit Id'
             },
             axis=1
         )

    ad_unit_to_3p = billable_ad_unit(fw_analytics, op_fw_adj.set_index('Placement ID'))

    if adj_period.empty:
        adj_period = adj_period.reindex(
            adj_period.columns.to_list() + ["Billable 3rd Party Server"], axis=1)
    else:
        adj_period.loc[:, 'Billable 3rd Party Server'] = adj_period.apply(
            lambda row: billable_adj_server(row, ad_unit_to_3p, 'Ad Unit Id'), axis=1)

    adj_period_agg = adj_period.dropna().groupby('Ad Unit Id').agg(
        {
            'Impressions (3rd Party)': 'sum'
        }
    )

    fw_adj_period = fw_period_agg.join(
        adj_period_agg).set_index('Placement ID')

    fw_adj_period = fw_adj_period.fillna(
        {'Impressions (3rd Party)': fw_adj_period['Net Counted Ads']})

    fw_adj_period_agg = fw_adj_period.reset_index().groupby('Placement ID').agg('sum')

    return fw_adj_period_agg


def financial_risk_rev(row: pd.Series, risk_col: str) -> float:
    if row['Is Bill on Contract']:
        return 0
    return float(row[risk_col])


def financial_op_risk_rev(row: pd.Series, period_name: str, period_abbrv: str) -> float:
    if row['Is Future Start']:
        return 0
    if row['No Delivery']:
        if row['Flight Days in ' + period_name] > 0:
            return float(row['Daily Budget'] * (1 - row['Remaining Flight Days in ' + period_name] / row['Flight Days in ' + period_name]))
        return 0
    return float(row['Straight Line Booked Revenue to ' + period_abbrv] - row['Total Projected Billable Revenue in Period'])


def projected_rev_period(row: pd.Series, period_name: str) -> float:
    proj_rev = row['Daily Pacing Rate'] * row['Remaining Flight Days in ' + period_name ]
    return cap_overdelivery(proj_rev, row['Net Cost'])


def total_projected_rev_period(row: pd.Series) -> float:
    total_proj_rev = row['Earned Revenue'] + row['Projected Revenue in Period']
    capped_amt = cap_overdelivery(total_proj_rev, row['Net Cost'])
    if capped_amt < row['Earned Revenue']:
        return float(row['Earned Revenue'])
    return capped_amt


def total_projected_imps_period(row: pd.Series) -> float:
    if row['Net Unit Cost'] == 0:
        return 0
    elif math.isnan(row['Total Projected Revenue in Period']):
        return 0
    else:
        return (row['Total Projected Revenue in Period']*1000)/row['Net Unit Cost']


def proj_1p_imps_period(row: pd.Series, period_name: str) -> float:
    proj_1p_imps = row['Daily Pacing 1P Imps'] * row['Remaining Flight Days in ' + period_name]
    return cap_overdelivery(proj_1p_imps, row['Quantity'])


def proj_imps_period(row: pd.Series, period_name: str) -> float:
    proj_imps = row['Daily Pacing Quantity'] * row['Remaining Flight Days in ' + period_name]
    return cap_overdelivery(proj_imps, row['Quantity'])


def total_proj_1p_imps_period(row: pd.Series) -> float:
    total_proj_1p_imps = row['1P Imps'] + row['Projected 1P Imps in Period']
    capped_amt = cap_overdelivery(total_proj_1p_imps, row['Quantity'])
    if capped_amt < row['1P Imps']:
        return float(row['1P Imps'])
    return capped_amt


def cap_overdelivery(amount: float, capped_amount: float) -> float:
    # cap at 10% above Net Cost or Quantity
    overdelivery_cap = 1.1 * capped_amount
    if amount > overdelivery_cap:
        return overdelivery_cap
    return amount


def total_projected_billable(row: pd.Series, period: str) -> float:
    if row['Is Bill on Contract']:
        return float(row['Billable Revenue'])
    total_proj_bill_rev = float(row['Billable Revenue']) + float(row['Projected Revenue in ' + period])
    if total_proj_bill_rev >= row['Net Cost']:
        return float(row['Net Cost'])
    return total_proj_bill_rev


def _apply_period_monthly_demo_comp(op_fw_adj_period: pd.DataFrame, report_date: datetime, period_name: str) -> pd.DataFrame:
    """Overwrite Demo Comp using BAR-style monthly comps for the selected period."""
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    demo_monthly = s3_dir + 'freewheel/Ad Ops Pacing (Analytics) Fox New FW Demo Monthly Agg.csv'
    demo_daily = s3_dir + 'freewheel/Ad Ops Pacing (Analytics) Fox New FW Demo Daily Agg.csv'
    monthly_delivery_path = s3_dir + 'freewheel/Ad Ops Pacing (Analytics) Fox New FW QTD Monthly Agg.csv'

    try:
        demo_by_month = read_fw_demo_by_month(demo_monthly, demo_daily, report_date)
        monthly_delivery = read_fw_analytics_monthly(monthly_delivery_path, report_date)
    except Exception:
        try:
            demo_by_month = read_fw_demo_by_month(demo_monthly, demo_daily, report_date, skiprows=4)
            monthly_delivery = read_fw_analytics_monthly(monthly_delivery_path, report_date, skiprows=4)
        except Exception:
            return op_fw_adj_period

    delivery = monthly_delivery.copy()
    if delivery.index.name == 'Placement ID' and 'Placement ID' not in delivery.columns:
        delivery = delivery.reset_index()
    if delivery.empty or 'Event Month' not in delivery.columns:
        return op_fw_adj_period

    delivery['Event Month'] = pd.to_datetime(delivery['Event Month'])
    if period_name == 'Month':
        delivery = delivery[
            (delivery['Event Month'].dt.month == report_date.month)
            & (delivery['Event Month'].dt.year == report_date.year)
        ]
    elif period_name == 'Quarter':
        if 'In Current Quarter' in delivery.columns:
            delivery = delivery[delivery['In Current Quarter'] == True]
        else:
            current_quarter = (report_date.month - 1) // 3 + 1
            delivery = delivery[
                (delivery['Event Month'].dt.quarter == current_quarter)
                & (delivery['Event Month'].dt.year == report_date.year)
            ]
    else:
        return op_fw_adj_period

    if delivery.empty or 'Net Counted Ads' not in delivery.columns:
        return op_fw_adj_period

    weighted = delivery_weighted_monthly_demo_comp(
        demo_by_month,
        delivery[['Placement ID', 'Event Month', 'Net Counted Ads']],
    )
    if weighted.empty:
        return op_fw_adj_period

    # Caller passes Placement ID as index (from financial_period_calc)
    out = op_fw_adj_period.copy()
    out = out.drop(columns=['On-Target Net Delivered Impressions', 'Gross Counted Ads (Demo)'], errors='ignore')
    out = out.join(weighted, how='left')
    out = out.fillna(
        {
            'On-Target Net Delivered Impressions': 0,
            'Gross Counted Ads (Demo)': 0,
        },
    )
    out['Demo Comp'] = out.apply(demo_comp, axis=1).astype('float64')
    return out


def financial_period_calc(op_fw_adj: pd.DataFrame, fw_adj_period: pd.DataFrame, op_finance: pd.DataFrame, period_name: str, report_date: datetime) -> pd.DataFrame:
    quarter_start = pd.to_datetime(report_date.replace(day=15) - pd.tseries.offsets.QuarterBegin(startingMonth=1)).date()
    quarter_end = pd.to_datetime(report_date.replace(day=15) + pd.tseries.offsets.QuarterEnd(startingMonth=0)).date()
    month_end = pd.to_datetime(report_date.replace(day=15) + pd.tseries.offsets.MonthEnd()).date()
    month_start = month_end.replace(day=1)

    if period_name == 'Quarter':
        period_abbrv = 'EOQ'
        placement_filter_date = datetime.combine(quarter_start - timedelta(days=1), datetime.min.time())
    elif period_name == 'Month':
        period_abbrv = 'EOM'
        placement_filter_date = datetime.combine(month_start - timedelta(days=1), datetime.min.time())
    else:
        raise ValueError

    end_of_period_date = datetime.combine(quarter_end, datetime.min.time())

    op_fw_adj['Deal Type Finance'] = op_fw_adj.apply(deal_type_finance, axis=1)
    op_fw_adj_filtered = op_fw_adj[op_fw_adj['Sales Line Item End Date'] > placement_filter_date]
    op_fw_adj_filtered = op_fw_adj_filtered[op_fw_adj_filtered['Sales Line Item Start Date'] <= end_of_period_date]

    op_subset = op_fw_adj_filtered[
        [
            'Sales Line Item ID',
            'Placement ID',
            'Demo Comp',
            'Cost Method',
            'Viewability',
            'Sales Line Item Start Date',
            'Sales Line Item End Date',
            'Sales Line Item Type',
            'VPVH',
            'Equivalization Factor',
            'Ad Unit Price',
            'Net Unit Cost',
            'Is VOD Placement',
            'Is Demo Placement',
            'Is Absolute A',
            'Is Non-Ad Served',
            'Is 3P',
            'Is SAB Engagement',
            'Is Future Start',
            'Is Bill on Contract',
            'No Delivery',
            'Buy Type',
            'Net Cost',
            'Days in Flight',
            'Parent Sales Line Item ID',
            'Flight Days in Month',
            'Flight Days in Quarter',
            'Quantity',
            'Remaining Flight Days in Month',
            'Remaining Flight Days in Quarter',
            'Daily Pacing Rate',
            'Daily Budget',
            'Daily Pacing Quantity',
            'Daily Pacing 1P Imps',
            'Implied Billable Metric',
            'Straight Line Booked Revenue to EOM',
            'Straight Line Booked Impressions to EOM',
            'Straight Line Booked Revenue to EOQ',
            'Straight Line Booked Impressions to EOQ',
            'Invoice Organization Name',
            'Deal Type Finance',
            'Demo Band',
            'FW - Budget Model',
            'FW - RBP Advanced',
            'Billable Third Party Server',
        ]
    ].set_index('Placement ID')

    op_fw_adj_period = op_subset.join(fw_adj_period)

    op_fw_adj_period = op_fw_adj_period.fillna(
        {
            'Net Counted Ads': 0,
        },
    ).astype(
        {
            'Net Counted Ads': 'int64',
        },
    )
    # Recompute Demo Comp for the period using one monthly rate applied to overall
    # period delivery (BAR-aligned), instead of the lifetime/campaign-level Demo Comp.
    op_fw_adj_period = _apply_period_monthly_demo_comp(op_fw_adj_period, report_date, period_name)
    op_fw_adj_period = calculate_imps(op_fw_adj_period.reset_index()).drop(columns=['Demo Band', 'FW - Budget Model', 'FW - RBP Advanced', 'Billable Third Party Server'])
    op_fw_adj_period.loc[:, 'Projected Impressions in Period'] = op_fw_adj_period.apply(
        lambda row: proj_imps_period(
            row,
            period_name,
        ),
        axis=1,
    )
    op_fw_adj_period.loc[:, 'Projected 1P Imps in Period'] = op_fw_adj_period.apply(
        lambda row: proj_1p_imps_period(
            row,
            period_name,
        ),
        axis=1,
    )
    op_fw_adj_period.loc[:, 'Total Projected 1P Imps in Period - Uncapped'] = op_fw_adj_period.apply(
        total_proj_1p_imps_period,
        axis=1,
    )
    op_fw_adj_period.loc[
        :,
        'Projected Revenue in Period',
    ] = op_fw_adj_period.apply(
        lambda row: projected_rev_period(
            row,
            period_name,
        ),
        axis=1,
    )
    op_fw_adj_period.loc[:, 'Earned Revenue'] = op_fw_adj_period.apply(earned_rev, axis=1)
    op_fw_adj_period.loc[:, 'Total Projected Revenue in Period'] = op_fw_adj_period.apply(
        total_projected_rev_period,
        axis=1,
    )

    op_fw_adj_period.loc[:, 'Total Projected Impressions in Period'] = op_fw_adj_period.apply(
        total_projected_imps_period,
        axis=1,
    )

    op_fw_adj_period.loc[:, 'Total Projected Impressions in Period'] = op_fw_adj_period.apply(round, args=('Total Projected Impressions in Period',), axis=1)

    # Merge in bill on contract revenue for the period
    op_fin_period = op_finance.loc[
        op_finance['In Current ' + period_name],
        :,
    ]
    op_fin_period_agg = op_fin_period.groupby(level=0).agg(
        {'Net Invoice Amount': 'sum'},
    )
    op_fw_adj_period = pd.merge(
        op_fw_adj_period,
        op_fin_period_agg,
        left_on='Parent Sales Line Item ID',
        right_index=True,
        how='left',
    )
    op_fw_adj_period = calculate_billable(
        op_fw_adj_period,
    ).rename(
        {
            'Billable Impressions Lifetime': 'Billable Impressions',
            'Billable Revenue Lifetime': 'Billable Revenue',
            'Billable Revenue Lifetime - Uncapped': 'Billable Revenue - Uncapped',
        },
        axis=1,
    )

    op_fw_adj_period.loc[:, 'Total Projected Billable Revenue in Period'] = op_fw_adj_period.apply(
        lambda row: total_projected_billable(
            row,
            'Period',
        ),
        axis=1,
    )

    op_fw_adj_period.loc[
        :,
        'Financial Operational Revenue at Risk in Period',
    ] = op_fw_adj_period.apply(
        lambda row: financial_op_risk_rev(
            row,
            period_name,
            period_abbrv,
        ),
        axis=1,
    )

    op_fw_adj_period.loc[
        :,
        'Financial Operational Revenue at Risk in Period'
    ] = op_fw_adj_period.apply(
        lambda row: cap_risk_rev(
            row,
            'Financial Operational Revenue at Risk in Period',
            'Straight Line Booked Revenue to ' + period_abbrv,
        ),
        axis=1,
    )

    op_fw_adj_period.loc[
        :,
        'Financial Revenue at Risk in Period',
    ] = op_fw_adj_period.apply(
        lambda row: financial_risk_rev(
            row,
            'Financial Operational Revenue at Risk in Period',
        ),
        axis=1,
    )

    # Rollup to parent line
    parent_line_projected = op_fw_adj_period.groupby('Parent Sales Line Item ID').agg(
        {
            'Sales Line Item ID': 'min',
            'Projected Impressions in Period': 'sum',
            'Projected Revenue in Period': 'sum',
            'Earned Revenue': 'sum',
            'Billable Revenue - Uncapped': 'sum',
            'Straight Line Booked Revenue to ' + period_abbrv: 'sum',
        },
    ).rename(
        {
            'Projected Impressions in Period': 'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period': 'Projected Revenue in Period (Parent Line Item)',
            'Earned Revenue': 'Earned Revenue in Period (Parent Line Item)',
            'Billable Revenue - Uncapped': 'Billable Revenue - Uncapped in Period (Parent Line Item)',
            'Straight Line Booked Revenue to ' + period_abbrv: 'Total Net Cost in Period',
        },
        axis=1,
    )

    # parent_line_projected.loc[:, 'Projected Impressions in Period (Parent Line Item)',] = round(parent_line_projected.loc[:, 'Projected Impressions in Period (Parent Line Item)'])
    parent_line_projected.loc[:, 'Projected Impressions in Period (Parent Line Item)'] = parent_line_projected.apply(round, args=('Projected Impressions in Period (Parent Line Item)',), axis=1)


    parent_line_projected.loc[
        :,
        'Total Projected Revenue in Period (Parent Line Item)',
    ] = parent_line_projected[
        'Earned Revenue in Period (Parent Line Item)'
    ] + parent_line_projected[
        'Projected Revenue in Period (Parent Line Item)'
    ]

    parent_line_projected.reset_index(inplace=True)

    op_fw_adj_period = pd.merge(
        op_fw_adj_period,
        parent_line_projected,
        on=['Parent Sales Line Item ID', 'Sales Line Item ID'],
        how='left',
    )

    op_fw_adj_period_subset = op_fw_adj_period[
        [
            'Sales Line Item ID',
            '1P Imps',
            '3P Imps',
            '1P vs 3P Var',
            '1P Demo Imps',
            '3P Demo Imps',
            'Billable Impressions',
            'Billable Revenue',
            'Billable Revenue - Uncapped',
            'Billable Quantity',
            'Earned Revenue',
            'Projected Impressions in Period',
            'Projected 1P Imps in Period',
            'Total Projected 1P Imps in Period - Uncapped',
            'Projected Revenue in Period',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Earned Revenue in Period (Parent Line Item)',
            'Total Net Cost in Period',
            'Total Projected Revenue in Period (Parent Line Item)',
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
        ]
    ].set_index(
        'Sales Line Item ID',
    )

    ret = pd.merge(
        op_fw_adj[op_fw_adj['Flight Days in ' + period_name] > 0],
        op_fw_adj_period_subset,
        left_on='Sales Line Item ID',
        right_index=True,
        suffixes=(' in Lifetime', ' in Period'),
        how='left',
    )

    return ret


def rq_projected_billable_rev(row: pd.Series, report_date: datetime) -> float:
    if row['Rolling Quarter Billable Revenue Cap'] <= 0:
        return 0
    if row['Billing Scenario'].startswith('Bill on Contract') and row['Sales Line Item End Date'] < report_date.replace(day=1):
        return 0
    if row['Billing Scenario'].startswith('Bill on Contract'):
        return float(row['Total Projected Billable Revenue in Period'])
    if row['Total Projected Billable Revenue in Period'] > row['Rolling Quarter Billable Revenue Cap']:
        return float(row['Rolling Quarter Billable Revenue Cap'])
    return float(row['Total Projected Billable Revenue in Period'])


def rq_projected_rev_total(row: pd.Series) -> float:
    if row['Rolling Quarter Revenue Cap'] <= 0:
        return 0
    if row['Total Projected Revenue in Period'] > row['Rolling Quarter Revenue Cap']:
        return float(row['Rolling Quarter Revenue Cap'])
    return float(row['Total Projected Revenue in Period'])


def rq_projected_rev(row: pd.Series) -> float:
    if row['Rolling Quarter Revenue Cap'] <= 0:
        return 0
    if row['Projected Revenue in Period'] > row['Rolling Quarter Revenue Cap']:
        return float(row['Rolling Quarter Revenue Cap'])
    return float(row['Projected Revenue in Period'])


def rq_projected_imps(row: pd.Series) -> float:
    if row['Rolling Quarter Impressions Cap'] <= 0:
        return 0
    if row['Projected Impressions in Period'] > row['Rolling Quarter Impressions Cap']:
        return float(row['Rolling Quarter Impressions Cap'])
    return float(row['Projected Impressions in Period'])


def rq_projected_imps_total(row: pd.Series) -> float:
    if row['Rolling Quarter Impressions Cap'] <= 0:
        return 0
    if row['Total Projected Impressions in Period'] > row['Rolling Quarter Impressions Cap']:
        return float(row['Rolling Quarter Impressions Cap'])
    return float(row['Total Projected Impressions in Period'])


def rq_projected_1p_imps(row: pd.Series) -> float:
    if row['Rolling Quarter Impressions Cap'] <= 0:
        return 0
    if row['Projected 1P Imps in Period'] > row['Rolling Quarter Impressions Cap']:
        return float(row['Rolling Quarter Impressions Cap'])
    return float(row['Projected 1P Imps in Period'])


def rolling_quarter_cap(rq_df: pd.DataFrame, q_df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    rq_df.loc[
        :,
        [
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
            'Sales Line Item Revenue at Risk (Lifetime)',
        ]
    ] = q_df.loc[
        :,
        [
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
            'Sales Line Item Revenue at Risk (Lifetime)',
        ]
    ].fillna(0)

    df = rq_df.copy().drop(
        [
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
        ],
        axis=1,
    )

    df['Rolling Quarter Billable Revenue Cap'] = df['Net Cost'] - (q_df['Billable Revenue in Period'] - df['Billable Revenue in Period'])
    df['Rolling Quarter Revenue Cap'] = df['Net Cost'] * 1.1 - (q_df['Billable Revenue in Period'] - df['Billable Revenue in Period'])
    df['Rolling Quarter Impressions Cap'] = (df['Quantity'] * 1.1 - (q_df['Billable Impressions in Period'] - df['Billable Impressions in Period']))
    df['Rolling Quarter Impressions Cap'] = df.apply(round, args=('Rolling Quarter Impressions Cap',), axis=1)

    df['Total Projected Billable Revenue in Period'] = df.apply(lambda row: rq_projected_billable_rev(row, report_date), axis=1)
    df['Total Projected Revenue in Period'] = df.apply(rq_projected_rev_total, axis=1)
    df['Projected Revenue in Period'] = df.apply(rq_projected_rev, axis=1)
    df['Total Projected Impressions in Period'] = df.apply(rq_projected_imps_total, axis=1)
    df['Projected Impressions in Period'] = (df.apply(rq_projected_imps, axis=1))
    df['Projected 1P Imps in Period'] = (df.apply(rq_projected_1p_imps, axis=1))
    df['Projected Impressions in Period'] = df.apply(round, args=('Projected Impressions in Period',), axis=1)
    df['Projected 1P Imps in Period'] = df.apply(round, args=('Projected 1P Imps in Period',), axis=1)

    parent_line_projected = df.groupby('Parent Sales Line Item ID').agg(
        {
            'Sales Line Item ID': 'min',
            'Projected Impressions in Period': 'sum',
            'Projected Revenue in Period': 'sum',
            'Earned Revenue - Uncapped (Period)': 'sum',
        },
    ).rename(
        {
            'Projected Impressions in Period': 'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period': 'Projected Revenue in Period (Parent Line Item)',
            'Earned Revenue - Uncapped (Period)': 'Earned Revenue in Period (Parent Line Item)',
        },
        axis=1,
    )

    parent_line_projected.loc[:, 'Projected Impressions in Period (Parent Line Item)'] = \
        parent_line_projected.apply(round, args=('Projected Impressions in Period (Parent Line Item)',), axis=1)

    parent_line_projected.loc[:, 'Total Projected Revenue in Period (Parent Line Item)'] = \
        parent_line_projected['Earned Revenue in Period (Parent Line Item)'] + \
            parent_line_projected['Projected Revenue in Period (Parent Line Item)']

    parent_line_projected.reset_index(inplace=True)

    df = pd.merge(
        df,
        parent_line_projected,
        on=['Parent Sales Line Item ID', 'Sales Line Item ID'],
        how='left',
    ).reset_index(drop=True)

    rq_df = rq_df.drop('Parent Sales Line Item ID', axis=1).reset_index(drop=True)

    rq_df.loc[
        :,
        [
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Projected Impressions in Period',
            'Projected Revenue in Period',
            'Projected 1P Imps in Period',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
        ]
    ] = df.loc[
        :,
        [
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Projected Impressions in Period',
            'Projected Revenue in Period',
            'Projected 1P Imps in Period',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
        ]
    ].fillna(0)

    rq_df = rq_df.astype(
        {
            'Projected Impressions in Period': 'int64',
            'Projected 1P Imps in Period': 'int64',
        },
    )

    return rq_df


def deal_type_finance(row: Any) -> str:
    if row['Invoice Organization Name'] is not None and 'Fluidity' in row['Invoice Organization Name']:
        return 'Fluidity'
    if row['Buy Type'] is not None and ('Backfill' in row['Buy Type'] or 'Programmatic BF' in row['Buy Type']):
        return 'Programmatic Backfill'
    if row['Buy Type'] is not None and ('Programmatic LFV PMP' in row['Buy Type'] or 'Programmatic SFV PMP' in row['Buy Type']):
        return 'PMP'
    if row['Buy Type'] is not None and ('Programmatic Video' in row['Buy Type'] or 'Programmatic LFV' in row['Buy Type'] or 'Programmatic SFV' in row['Buy Type'] or 'Programmatic ROV' in row['Buy Type']):
        return 'Programmatic Guaranteed'
    return 'Cash'

# COMMAND ----------

def do_executive_summary(exec_sum_adj_mtd: pd.DataFrame, exec_sum_adj_qtd: pd.DataFrame, report_date: datetime) -> str:
    global drop_bucket
    s3_dir = f's3://{drop_bucket}/'
    operative_filename = 'FinanceExport_Template.csv'
    lifetime_filename = 'AdOps_Reporting_Lifetime_Delivery.parquet'
    operative_invoice = s3_dir + 'operative/' + operative_filename
    lifetime_path = s3_dir + 'processed/' + lifetime_filename
    fw_name = 'Ad Ops Pacing (Analytics) Fox New FW QTD Monthly Agg.csv'
    fw_report = s3_dir + 'freewheel/' + fw_name

    try:
        fw_analytics = read_fw_analytics_monthly(fw_report, report_date)
    except:
        # Manually uploaded Freewheel files if SFTP cannot be accessed
        fw_analytics = read_fw_analytics_monthly(fw_report, report_date, skiprows=4)
    op_finance = read_op_invoices(operative_invoice, report_date)

    exec_sum_file = 'Executive Summary' + report_date.strftime('_%Y%m%d') + '.xlsx'

    op_fw_adj = pd.read_parquet(
        lifetime_path,
        storage_options={'profile': aws_profile}
    ).rename(
        {
            'Billable Impressions Lifetime': 'Billable Impressions',
            'Billable Revenue Lifetime': 'Billable Revenue',
            'Billable Revenue Lifetime - Uncapped': 'Billable Revenue - Uncapped',
        },
        axis=1,
    )

    fw_adj_qtd = financial_period_agg(fw_analytics, exec_sum_adj_qtd, op_fw_adj, 'Quarter')
    fw_adj_mtd = financial_period_agg(fw_analytics, exec_sum_adj_mtd, op_fw_adj, 'Month')

    op_fw_adj_m = financial_period_calc(op_fw_adj, fw_adj_mtd, op_finance, 'Month', report_date)
    op_fw_adj_rq = financial_period_calc(op_fw_adj, fw_adj_mtd, op_finance, 'Quarter', report_date)
    op_fw_adj_q = financial_period_calc(op_fw_adj, fw_adj_qtd, op_finance, 'Quarter', report_date)

    exec_sum_m = op_fw_adj_m.loc[
        :,
        [
            'Invoice Organization Name',
            'VP Sales',
            'Primary Salesperson Full Name',
            'Agency Name',
            'Agency ID',
            'Trafficker/Campaign Manager',
            'Advertiser Name',
            'Product Type',
            'Marketplace Type',
            'Sales Line Item Type',
            'Opportunity Unique ID',
            'Campaign ID',
            'Campaign Name',
            'Sales Order ID',
            'Sales Order Name',
            'Sales Order Start Date',
            'Sales Order End Date',
            'Total Net',
            'Total Net Cost in Period',
            'Placement Property',
            'Buy Type',
            'Sales Line Item ID',
            'Placement ID',
            'Sales Line Item Name',
            'Sales Line Item Start Date',
            'Sales Line Item End Date',
            'Demo Band',
            'Quantity',
            'Net Unit Cost',
            'Net Cost',
            'Billable Third Party Server',
            'Billing Scenario',
            'Billable Metric',
            'Launch Date',
            'Days in Flight',
            'Days Elapsed',
            'Remaining Days in Flight',
            'Remaining Days until EOM',
            'Flight %',
            '1P Imps in Period',
            '3P Imps in Period',
            '1P vs 3P Var in Period',
            'Demo Comp',
            '1P Demo Imps in Period',
            '3P Demo Imps in Period',
            'Viewability',
            'Billable Impressions in Lifetime',
            'Billable Revenue in Lifetime',
            'Billable Revenue - Uncapped in Lifetime',
            'Billable Quantity in Lifetime',
            'Earned Revenue in Lifetime',
            'Billable Impressions in Period',
            'Billable Revenue in Period',
            'Billable Revenue - Uncapped in Period',
            'Billable Quantity in Period',
            'Earned Revenue in Period',
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Projected Impressions in Period',
            'Projected Revenue in Period',
            'Projected 1P Imps in Period',
            'Total Projected 1P Imps in Period - Uncapped',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
            'Straight Line Booked Impressions to EOM',
            'Straight Line Booked Revenue to EOM',
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
            'Sales Line Item Revenue at Risk',
            'Sales Line Item Risk Category',
            'Product Name',
            'Deal Type Finance',
            'Liability Type',
        ],
    ]

    exec_sum_m['Billable Forecasted Revenue in Period'] = exec_sum_m['Total Projected Billable Revenue in Period'] - exec_sum_m['Billable Revenue in Period']
    exec_sum_m['Total 1P Impressions in Period'] = exec_sum_m['1P Imps in Period'] + exec_sum_m['Projected 1P Imps in Period']

    exec_sum_m = exec_sum_m.loc[
        ~exec_sum_m['Sales Line Item Name'].str.contains(
            '.*CANCEL.*',
            regex=True,
            flags=re.IGNORECASE,
            na=False,
        ),
        :,
    ]

    exec_sum_m.rename(
        {
            'Invoice Organization Name': 'Organization',
            'Primary Salesperson Full Name': 'Primary Salesperson',
            'Agency Name': 'Agency',
            'Advertiser Name': 'Advertiser',
            'Campaign Name': 'Campaign',
            'Sales Order Name': 'Sales Order',
            'Total Net': 'Sales Order Total Net Cost',
            'Total Net Cost in Period': 'Sales Line Item Net Cost',
            'Placement ID': 'PS Line Item ID',
            'Sales Line Item Name': 'Sales Line Item',
            '1P Imps in Period': '1P Imps (Period)',
            '3P Imps in Period': '3P Imps (Period)',
            '1P vs 3P Var in Period': '1P Vs 3P Var',
            'Demo Comp': 'Demo Comp (Lifetime)',
            '1P Demo Imps in Period': '1P Demo Imps (Period)',
            '3P Demo Imps in Period': '3P Demo Imps (Period)',
            'Viewability': 'Viewability (Lifetime)',
            'Billable Revenue - Uncapped in Lifetime': 'Billable Revenue - Uncapped (Lifetime)',
            'Billable Quantity in Lifetime': 'Earned Impressions - Uncapped (Lifetime)',
            'Earned Revenue in Lifetime': 'Earned Revenue - Uncapped (Lifetime)',
            'Billable Revenue - Uncapped in Period': 'Billable Revenue - Uncapped (Period)',
            'Billable Quantity in Period': 'Earned Impressions - Uncapped (Period)',
            'Earned Revenue in Period': 'Earned Revenue - Uncapped (Period)',
            'Sales Line Item Revenue at Risk': 'Sales Line Item Revenue at Risk (Lifetime)',
            'Sales Line Item Risk Category': 'Sales Line Item Risk Category (Lifetime)',
            'Straight Line Booked Impressions to EOM': 'Straight Line Booked Impressions in Period',
            'Straight Line Booked Revenue to EOM': 'Straight Line Booked Revenue in Period',
        },
        axis=1,
        inplace=True,
    )

    exec_sum_m.fillna(
        {
            'Campaign ID': exec_sum_m['Sales Order ID'],
            'Campaign': exec_sum_m['Sales Order'],
            'Demo Comp (Lifetime)': 0,
            'Viewability (Lifetime)': 1,
            'Billable Impressions in Lifetime': 0,
            'Earned Impressions - Uncapped (Lifetime)': 0,
            'Billable Impressions in Period': 0,
            'Earned Impressions - Uncapped (Period)': 0,
            'Projected Impressions in Period': 0,
            'Straight Line Booked Impressions in Period': 0,
            'Projected 1P Imps in Period': 0,
            'Total Projected 1P Imps in Period - Uncapped': 0,
        },
        inplace=True,
    )

    exec_sum_m = exec_sum_m.astype(
        {
            'Campaign ID': 'int64',
            'Billable Impressions in Lifetime': 'int64',
            'Earned Impressions - Uncapped (Lifetime)': 'int64',
            'Billable Impressions in Period': 'int64',
            'Earned Impressions - Uncapped (Period)': 'int64',
            'Projected Impressions in Period': 'int64',
            'Straight Line Booked Impressions in Period': 'int64',
            'Projected 1P Imps in Period': 'int64',
            'Total Projected 1P Imps in Period - Uncapped': 'int64',
            'Total 1P Impressions in Period': 'int64',
        },
    )

    exec_sum_rq = op_fw_adj_rq.loc[
        :,
        [
            'Invoice Organization Name',
            'Parent Sales Line Item ID',
            'VP Sales',
            'Primary Salesperson Full Name',
            'Agency Name',
            'Agency ID',
            'Trafficker/Campaign Manager',
            'Advertiser Name',
            'Product Type',
            'Marketplace Type',
            'Sales Line Item Type',
            'Opportunity Unique ID',
            'Campaign ID',
            'Campaign Name',
            'Sales Order ID',
            'Sales Order Name',
            'Sales Order Start Date',
            'Sales Order End Date',
            'Total Net',
            'Total Net Cost in Period',
            'Placement Property',
            'Buy Type',
            'Sales Line Item ID',
            'Placement ID',
            'Sales Line Item Name',
            'Sales Line Item Start Date',
            'Sales Line Item End Date',
            'Demo Band',
            'Quantity',
            'Net Unit Cost',
            'Net Cost',
            'Billable Third Party Server',
            'Billing Scenario',
            'Billable Metric',
            'Launch Date',
            'Days in Flight',
            'Days Elapsed',
            'Remaining Days in Flight',
            'Remaining Days until EOQ',
            'Flight %',
            '1P Imps in Period',
            '3P Imps in Period',
            '1P vs 3P Var in Period',
            'Demo Comp',
            '1P Demo Imps in Period',
            '3P Demo Imps in Period',
            'Viewability',
            'Billable Impressions in Lifetime',
            'Billable Revenue in Lifetime',
            'Billable Revenue - Uncapped in Lifetime',
            'Billable Quantity in Lifetime',
            'Earned Revenue in Lifetime',
            'Billable Impressions in Period',
            'Billable Revenue in Period',
            'Billable Revenue - Uncapped in Period',
            'Billable Quantity in Period',
            'Earned Revenue in Period',
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Projected Impressions in Period',
            'Projected Revenue in Period',
            'Projected 1P Imps in Period',
            'Total Projected 1P Imps in Period - Uncapped',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
            'Straight Line Booked Impressions to EOQ',
            'Straight Line Booked Revenue to EOQ',
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
            'Sales Line Item Revenue at Risk',
            'Sales Line Item Risk Category',
            'Product Name',
            'Deal Type Finance',
            'Liability Type',
        ],
    ]

    exec_sum_rq['Billable Forecasted Revenue in Period'] = exec_sum_rq['Total Projected Billable Revenue in Period'] - exec_sum_rq['Billable Revenue in Period']
    exec_sum_rq['Total 1P Impressions in Period'] = exec_sum_rq['1P Imps in Period'] + exec_sum_rq['Projected 1P Imps in Period']

    exec_sum_rq = exec_sum_rq.loc[
        ~exec_sum_rq['Sales Line Item Name'].str.contains(
            '.*CANCEL.*',
            regex=True,
            flags=re.IGNORECASE,
            na=False,
        ),
        :,
    ]

    exec_sum_rq.rename(
        {
            'Invoice Organization Name': 'Organization',
            'Primary Salesperson Full Name': 'Primary Salesperson',
            'Agency Name': 'Agency',
            'Advertiser Name': 'Advertiser',
            'Campaign Name': 'Campaign',
            'Sales Order Name': 'Sales Order',
            'Total Net': 'Sales Order Total Net Cost',
            'Total Net Cost in Period': 'Sales Line Item Net Cost',
            'Placement ID': 'PS Line Item ID',
            'Sales Line Item Name': 'Sales Line Item',
            '1P Imps in Period': '1P Imps (Period)',
            '3P Imps in Period': '3P Imps (Period)',
            '1P vs 3P Var in Period': '1P Vs 3P Var',
            'Demo Comp': 'Demo Comp (Lifetime)',
            '1P Demo Imps in Period': '1P Demo Imps (Period)',
            '3P Demo Imps in Period': '3P Demo Imps (Period)',
            'Viewability': 'Viewability (Lifetime)',
            'Billable Revenue - Uncapped in Lifetime': 'Billable Revenue - Uncapped (Lifetime)',
            'Billable Quantity in Lifetime': 'Earned Impressions - Uncapped (Lifetime)',
            'Earned Revenue in Lifetime': 'Earned Revenue - Uncapped (Lifetime)',
            'Billable Revenue - Uncapped in Period': 'Billable Revenue - Uncapped (Period)',
            'Billable Quantity in Period': 'Earned Impressions - Uncapped (Period)',
            'Earned Revenue in Period': 'Earned Revenue - Uncapped (Period)',
            'Sales Line Item Revenue at Risk': 'Sales Line Item Revenue at Risk (Lifetime)',
            'Sales Line Item Risk Category': 'Sales Line Item Risk Category (Lifetime)',
            'Straight Line Booked Impressions to EOQ': 'Straight Line Booked Impressions in Period',
            'Straight Line Booked Revenue to EOQ': 'Straight Line Booked Revenue in Period',
        },
        axis=1,
        inplace=True,
    )

    exec_sum_rq.fillna(
        {
            'Campaign ID': exec_sum_rq['Sales Order ID'],
            'Campaign': exec_sum_rq['Sales Order'],
            'Demo Comp (Lifetime)': 0,
            'Viewability (Lifetime)': 1,
            'Billable Impressions in Lifetime': 0,
            'Earned Impressions - Uncapped (Lifetime)': 0,
            'Billable Impressions in Period': 0,
            'Earned Impressions - Uncapped (Period)': 0,
            'Projected Impressions in Period': 0,
            'Straight Line Booked Impressions in Period': 0,
            'Projected 1P Imps in Period': 0,
            'Total Projected 1P Imps in Period - Uncapped': 0,
        },
        inplace=True,
    )

    exec_sum_rq = exec_sum_rq.astype(
        {
            'Campaign ID': 'int64',
            'Billable Impressions in Lifetime': 'int64',
            'Earned Impressions - Uncapped (Lifetime)': 'int64',
            'Billable Impressions in Period': 'int64',
            'Earned Impressions - Uncapped (Period)': 'int64',
            'Projected Impressions in Period': 'int64',
            'Straight Line Booked Impressions in Period': 'int64',
            'Projected 1P Imps in Period': 'int64',
            'Total Projected 1P Imps in Period - Uncapped': 'int64',
            'Total 1P Impressions in Period': 'int64',
        },
    )

    exec_sum_q = op_fw_adj_q.loc[
        :,
        [
            'Invoice Organization Name',
            'VP Sales',
            'Primary Salesperson Full Name',
            'Agency Name',
            'Agency ID',
            'Trafficker/Campaign Manager',
            'Advertiser Name',
            'Product Type',
            'Marketplace Type',
            'Sales Line Item Type',
            'Opportunity Unique ID',
            'Campaign ID',
            'Campaign Name',
            'Sales Order ID',
            'Sales Order Name',
            'Sales Order Start Date',
            'Sales Order End Date',
            'Total Net',
            'Total Net Cost in Period',
            'Placement Property',
            'Buy Type',
            'Sales Line Item ID',
            'Placement ID',
            'Sales Line Item Name',
            'Sales Line Item Start Date',
            'Sales Line Item End Date',
            'Demo Band',
            'Quantity',
            'Net Unit Cost',
            'Net Cost',
            'Billable Third Party Server',
            'Billing Scenario',
            'Billable Metric',
            'Launch Date',
            'Days in Flight',
            'Days Elapsed',
            'Remaining Days in Flight',
            'Remaining Days until EOQ',
            'Flight %',
            '1P Imps in Period',
            '3P Imps in Period',
            '1P vs 3P Var in Period',
            'Demo Comp',
            '1P Demo Imps in Period',
            '3P Demo Imps in Period',
            'Viewability',
            'Billable Impressions in Lifetime',
            'Billable Revenue in Lifetime',
            'Billable Revenue - Uncapped in Lifetime',
            'Billable Quantity in Lifetime',
            'Earned Revenue in Lifetime',
            'Billable Impressions in Period',
            'Billable Revenue in Period',
            'Billable Revenue - Uncapped in Period',
            'Billable Quantity in Period',
            'Earned Revenue in Period',
            'Projected Impressions in Period (Parent Line Item)',
            'Projected Revenue in Period (Parent Line Item)',
            'Projected Impressions in Period',
            'Projected Revenue in Period',
            'Projected 1P Imps in Period',
            'Total Projected 1P Imps in Period - Uncapped',
            'Total Projected Revenue in Period',
            'Total Projected Impressions in Period',
            'Total Projected Billable Revenue in Period',
            'Straight Line Booked Impressions to EOQ',
            'Straight Line Booked Revenue to EOQ',
            'Financial Revenue at Risk in Period',
            'Financial Operational Revenue at Risk in Period',
            'Sales Line Item Revenue at Risk',
            'Sales Line Item Risk Category',
            'Product Name',
            'Deal Type Finance',
            'Liability Type',
        ],
    ]

    exec_sum_q['Billable Forecasted Revenue in Period'] = exec_sum_q['Total Projected Billable Revenue in Period'] - exec_sum_q['Billable Revenue in Period']
    exec_sum_q['Total 1P Impressions in Period'] = exec_sum_q['1P Imps in Period'] + exec_sum_q['Projected 1P Imps in Period']

    exec_sum_q = exec_sum_q.loc[
        ~exec_sum_q['Sales Line Item Name'].str.contains(
            '.*CANCEL.*',
            regex=True,
            flags=re.IGNORECASE,
            na=False,
        ),
        :,
    ]

    exec_sum_q.rename(
        {
            'Invoice Organization Name': 'Organization',
            'Primary Salesperson Full Name': 'Primary Salesperson',
            'Agency Name': 'Agency',
            'Advertiser Name': 'Advertiser',
            'Campaign Name': 'Campaign',
            'Sales Order Name': 'Sales Order',
            'Total Net': 'Sales Order Total Net Cost',
            'Total Net Cost in Period': 'Sales Line Item Net Cost',
            'Placement ID': 'PS Line Item ID',
            'Sales Line Item Name': 'Sales Line Item',
            '1P Imps in Period': '1P Imps (Period)',
            '3P Imps in Period': '3P Imps (Period)',
            '1P vs 3P Var in Period': '1P Vs 3P Var',
            'Demo Comp': 'Demo Comp (Lifetime)',
            '1P Demo Imps in Period': '1P Demo Imps (Period)',
            '3P Demo Imps in Period': '3P Demo Imps (Period)',
            'Viewability': 'Viewability (Lifetime)',
            'Billable Revenue - Uncapped in Lifetime': 'Billable Revenue - Uncapped (Lifetime)',
            'Billable Quantity in Lifetime': 'Earned Impressions - Uncapped (Lifetime)',
            'Earned Revenue in Lifetime': 'Earned Revenue - Uncapped (Lifetime)',
            'Billable Revenue - Uncapped in Period': 'Billable Revenue - Uncapped (Period)',
            'Billable Quantity in Period': 'Earned Impressions - Uncapped (Period)',
            'Earned Revenue in Period': 'Earned Revenue - Uncapped (Period)',
            'Sales Line Item Revenue at Risk': 'Sales Line Item Revenue at Risk (Lifetime)',
            'Sales Line Item Risk Category': 'Sales Line Item Risk Category (Lifetime)',
            'Straight Line Booked Impressions to EOQ': 'Straight Line Booked Impressions in Period',
            'Straight Line Booked Revenue to EOQ': 'Straight Line Booked Revenue in Period',
        },
        axis=1,
        inplace=True,
    )

    exec_sum_q.fillna(
        {
            'Campaign ID': exec_sum_q['Sales Order ID'],
            'Campaign': exec_sum_q['Sales Order'],
            'Demo Comp (Lifetime)': 0,
            'Viewability (Lifetime)': 1,
            'Billable Impressions in Lifetime': 0,
            'Earned Impressions - Uncapped (Lifetime)': 0,
            'Billable Impressions in Period': 0,
            'Earned Impressions - Uncapped (Period)': 0,
            'Projected Impressions in Period': 0,
            'Straight Line Booked Impressions in Period': 0,
            'Projected 1P Imps in Period': 0,
            'Total Projected 1P Imps in Period - Uncapped': 0,
            'Total 1P Impressions in Period': 0,
            'Billable Forecasted Revenue in Period': 0,
        },
        inplace=True,
    )

    exec_sum_q = exec_sum_q.astype(
        {
            'Campaign ID': 'int64',
            'Billable Impressions in Lifetime': 'int64',
            'Earned Impressions - Uncapped (Lifetime)': 'int64',
            'Billable Impressions in Period': 'int64',
            'Earned Impressions - Uncapped (Period)': 'int64',
            'Projected Impressions in Period': 'int64',
            'Straight Line Booked Impressions in Period': 'int64',
            'Projected 1P Imps in Period': 'int64',
            'Total Projected Impressions in Period': 'int64',
            'Total Projected 1P Imps in Period - Uncapped': 'int64',
            'Billable Forecasted Revenue in Period': 'int64',
            'Total 1P Impressions in Period': 'int64',
        },
    )

    exec_sum_rq = rolling_quarter_cap(exec_sum_rq, exec_sum_q, report_date)

    exec_sum_money_cols = [
        'Sales Order Total Net Cost',
        'Sales Line Item Net Cost',
        'Net Unit Cost',
        'Net Cost',
        'Billable Revenue in Lifetime',
        'Billable Revenue - Uncapped (Lifetime)',
        'Earned Revenue - Uncapped (Lifetime)',
        'Billable Revenue in Period',
        'Billable Revenue - Uncapped (Period)',
        'Earned Revenue - Uncapped (Period)',
        'Projected Revenue in Period (Parent Line Item)',
        'Projected Revenue in Period',
        'Total Projected Revenue in Period',
        'Total Projected Billable Revenue in Period',
        'Straight Line Booked Revenue in Period',
        'Financial Revenue at Risk in Period',
        'Financial Operational Revenue at Risk in Period',
        'Sales Line Item Revenue at Risk (Lifetime)',
        'Billable Forecasted Revenue in Period',
    ]

    exec_sum_pct_cols = [
        'Flight %',
        '1P Vs 3P Var',
        'Demo Comp (Lifetime)',
        'Viewability (Lifetime)',
    ]
    with io.BytesIO() as output:
        with pd.ExcelWriter(
            output,
            datetime_format='MM/dd/yyyy',
            engine='xlsxwriter',
            options={'strings_to_numbers': True},
        ) as writer:
            workbook = writer.book
            money_fmt = workbook.add_format({'num_format': '$#,##0.00'})
            pct_fmt = workbook.add_format({'num_format': '0%'})
            exec_sum_m.to_excel(writer, sheet_name='Month', index=False)
            exec_sum_rq.to_excel(writer, sheet_name='Rolling Quarter', index=False)
            exec_sum_q.to_excel(writer, sheet_name='Quarter', index=False)
            worksheet = writer.sheets['Month']
            set_col_fmt(
                money_fmt,
                worksheet,
                exec_sum_m,
                exec_sum_money_cols,
            )
            set_col_fmt(
                pct_fmt,
                worksheet,
                exec_sum_m,
                exec_sum_pct_cols,
            )
            worksheet = writer.sheets['Rolling Quarter']
            set_col_fmt(
                money_fmt,
                worksheet,
                exec_sum_rq,
                exec_sum_money_cols,
            )
            set_col_fmt(
                pct_fmt,
                worksheet,
                exec_sum_rq,
                exec_sum_pct_cols,
            )
            worksheet = writer.sheets['Quarter']
            set_col_fmt(
                money_fmt,
                worksheet,
                exec_sum_q,
                exec_sum_money_cols,
            )
            set_col_fmt(
                pct_fmt,
                worksheet,
                exec_sum_q,
                exec_sum_pct_cols,
            )
        exec_sum_data = output.getvalue()
    
    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client('s3')
    s3_bucket = drop_bucket
    s3_key = 'reports/' + exec_sum_file
    s3_client.put_object(
        Bucket=s3_bucket,
        Body=exec_sum_data,
        Key=s3_key,
    )

    return s3_key

# COMMAND ----------

def executive_summary(date: datetime, mtd: str, qtd: str) -> None:
    report_date = datetime.combine(date, datetime.min.time())
    exec_sum_adj_mtd = f's3://{staq_bucket}/{mtd}'
    exec_sum_adj_qtd = f's3://{staq_bucket}/{qtd}'
    output_file = do_executive_summary(
        exec_sum_adj_mtd,
        exec_sum_adj_qtd,
        report_date,
    )
    write_xcom_value('Executive Summary', output_file)

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
    
    mtd, qtd = find_staq_files(datetime.strptime(report_date, "%Y-%m-%d"))
    assert parse_date(report_date), "Invalid date format, should be in YYYY-MM-DD format"
    assert mtd, "MTD file not found"
    assert qtd, "QTD file not found"

    try:
        executive_summary(datetime.strptime(report_date, "%Y-%m-%d"), mtd, qtd)
    except Exception as error:
        alert = Alert()
        alert.send('Executive Summary', f'{type(error).__name__}: {error}')
        dbutils.notebook.exit(f'ERROR!!! - {error}')