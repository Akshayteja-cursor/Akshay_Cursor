"""Pure-pandas helpers for News/Podcast pacing and lifetime delivery reports.

Extracted from ad_operations_news_lifetime_data_load so the logic can be
imported by both the notebook and the unit-test suite.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PODCAST_COLUMN_MAP: dict[str, str] = {
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

REPORT_COLUMNS: list[str] = [
    'Instance', 'Advertiser', 'Order', 'Line Item Name',
    'Line Item Start Date', 'Line Item End Date', 'Rate',
    'Goal Quantity', 'Contracted Quantity', 'Delivery Indicator', 'Salesperson',
    'Ad Server Impressions', 'Impressions (3rd Party)', 'Clicks (3rd Party)',
    '3rd Party CTR', 'Buffer', 'Discrepancy',
    'Current First Party OSI', 'Current Third Party OSI',
    'Total Error Count', 'Total Error Rate',
]

# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def osi(row: pd.Series, report_date: datetime, type: str) -> float:
    """On-Schedule Index: ratio of actual delivery pace to contracted pace.

    Parameters
    ----------
    row         : Series with Ad Server Impressions / Impressions (3rd Party),
                  Contracted Quantity, Line Item Start Date, Line Item End Date.
    report_date : Date of the report (used to measure elapsed flight days).
    type        : '1p' uses Ad Server Impressions; '3p' uses Impressions (3rd Party).
    """
    if type == '1p':
        if row['Ad Server Impressions'] == 0 or row['Contracted Quantity'] == 0:
            return 0
        qty = row['Ad Server Impressions'] / row['Contracted Quantity']
    else:
        if row['Impressions (3rd Party)'] == 0 or row['Contracted Quantity'] == 0:
            return 0
        qty = row['Impressions (3rd Party)'] / row['Contracted Quantity']

    days_elapsed = report_date - row['Line Item Start Date'] + timedelta(days=1)
    flight = row['Line Item End Date'] - row['Line Item Start Date'] + timedelta(days=1)

    if report_date < row['Line Item End Date']:
        return qty / (days_elapsed / flight)
    return qty


def podcast_osi(row: pd.Series, report_date: datetime) -> float:
    """OSI for Amperwave and Spotify (Megaphone) pacing lines.

    Ad Ops provided formula (UAT 07/28/2026):
        (Delivered-to-date impressions / Contracted impressions)
        /
        (Days flight has been live / Total flight days)

    Current First Party OSI and Current Third Party OSI both use this
    calculated value for podcast platforms because there is no 3rd-party
    measurement for them.
    """
    delivered_impressions = pd.to_numeric(row.get('Ad Server Impressions', 0), errors='coerce')
    contracted_impressions = pd.to_numeric(row.get('Contracted Quantity', 0), errors='coerce')
    start_date = pd.to_datetime(row.get('Line Item Start Date'), errors='coerce')
    end_date = pd.to_datetime(row.get('Line Item End Date'), errors='coerce')

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
    total_flight_days = (end_date - start_date).days + 1
    if total_flight_days <= 0:
        return 0

    # Completed flights stop counting at the end date.
    # Note: written without built-in min() because core.py does
    # 'from pyspark.sql.functions import *', which shadows min/max
    # in the notebook session with the 1-argument PySpark versions.
    effective_date = report_date if report_date < end_date else end_date
    days_live = (effective_date - start_date).days + 1
    if days_live <= 0:
        return 0

    delivery_progress = delivered_impressions / contracted_impressions
    flight_progress = days_live / total_flight_days
    if flight_progress <= 0:
        return 0

    return delivery_progress / flight_progress


def metrics_calculaition(row: pd.Series, calculation: str) -> float:
    """Compute a single named metric for one row."""
    if calculation == 'Total Error Rate':
        if row['Total Impressions'] + row['Total Error Count'] == 0:
            return 0
        return row['Total Error Count'] / (row['Total Impressions'] + row['Total Error Count'])

    if calculation == '3rd Party CTR':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return row['Clicks (3rd Party)'] / row['Impressions (3rd Party)']

    if calculation == 'Buffer':
        if row['Contracted Quantity'] == 0:
            return 0
        return (row['Ad Server Booked Impressions'] - row['Contracted Quantity']) / row['Contracted Quantity']

    if calculation == 'Discrepancy':
        if row['Impressions (3rd Party)'] == 0:
            return 0
        return (row['Impressions (3rd Party)'] - row['Ad Server Impressions']) / row['Impressions (3rd Party)']

    return 0


def calculate_metrics(df: pd.DataFrame, report_date: datetime, is_podcast: bool = False) -> pd.DataFrame:
    """Append all derived metric columns to *df* and return it.

    When *is_podcast* is True the Amperwave/Spotify OSI formula is used and
    both OSI columns (report columns R & S) carry the same value.
    """
    df.loc[:, 'Total Error Rate'] = df.apply(metrics_calculaition, args=('Total Error Rate',), axis=1)
    df.loc[:, '3rd Party CTR'] = df.apply(metrics_calculaition, args=('3rd Party CTR',), axis=1)
    df.loc[:, 'Ad Server Booked Impressions'] = df['Quantity']
    df.loc[:, 'Buffer'] = df.apply(metrics_calculaition, args=('Buffer',), axis=1)
    df.loc[:, 'Discrepancy'] = df.apply(metrics_calculaition, args=('Discrepancy',), axis=1)
    if is_podcast:
        podcast_osi_values = df.apply(podcast_osi, args=(report_date,), axis=1)
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
    df = df.fillna({col: 0 for col in metric_columns})
    for col in metric_columns:
        df[col] = df[col].replace([np.inf, -np.inf], 0)
    return df


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_podcast_pacing(feed_df: pd.DataFrame, instance_label: str) -> pd.DataFrame:
    """Rename and enrich a raw Amperwave / Megaphone feed DataFrame.

    Renames columns per PODCAST_COLUMN_MAP, coerces numeric/date types,
    and adds the constant fields required by the pacing report.
    """
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


def format_news_pacing(df: pd.DataFrame, report_date: datetime) -> pd.DataFrame:
    """Filter to current-year lines and select pacing report output columns."""
    df = df[df['Line Item End Date'] >= report_date.replace(month=1, day=1)]
    df = df.loc[:, [
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
