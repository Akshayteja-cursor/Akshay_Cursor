"""Pure helpers for BAR Net Invoice Amount scaling.

Package parent rows must be excluded from Net Cost denominators when allocating
AOS finance Net Invoice Amount across child sales line items. Otherwise the
package Net Cost (equal to the sum of children) doubles the denominator and BAR
reports ~50% of the AOS finance export.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def rounded_invoice_amt(group: pd.DataFrame, col: str) -> pd.DataFrame:
    cents = round(round(group["Net Invoice Amount"].mean(), 2) - group[col].sum(), 2)
    max_inv = group.sort_values(by=col, ascending=False).head(1)
    adj_id = max_inv["Sales Line Item ID"].min()
    adj_inv_amt = (max_inv[col] + cents).max()
    return pd.DataFrame({"adj_id": adj_id, "adj_inv_amt": adj_inv_amt}, index=[0])


def active_net_cost_ratio(row: pd.Series) -> Any:
    if row["Total Active Net Cost"] > 0:
        return row["Net Cost"] / row["Total Active Net Cost"]
    return 0


def net_cost_ratio(row: pd.Series) -> Any:
    if row["Total Net Cost"] > 0:
        return row["Net Cost"] / row["Total Net Cost"]
    return 0


def scale_net_invoice_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Allocate parent Net Invoice Amount to children by Net Cost ratio.

    Excludes Operative Product Type == 'Package' from ratio denominators and from
    rounding reconciliation, matching the BAR format_billing output filter.
    """
    df = df.copy()
    non_package = df[~df["Operative Product Type"].isin({"Package"})]

    total_active_net_cost = (
        non_package[~non_package["Is Future Start"]]
        .groupby("Parent Sales Line Item ID", as_index=False)
        .agg({"Net Cost": "sum"})
        .rename({"Net Cost": "Total Active Net Cost"}, axis=1)
    )
    total_net_cost = (
        non_package.groupby("Parent Sales Line Item ID", as_index=False)
        .agg({"Net Cost": "sum"})
        .rename({"Net Cost": "Total Net Cost"}, axis=1)
    )

    df = pd.merge(df, total_net_cost, on=["Parent Sales Line Item ID"], how="left")
    df = pd.merge(df, total_active_net_cost, on=["Parent Sales Line Item ID"], how="left")
    df.loc[:, "Active Net Cost Ratio"] = df.apply(active_net_cost_ratio, axis=1)
    df.loc[:, "Net Cost Ratio"] = df.apply(net_cost_ratio, axis=1)
    df.loc[:, "Scaled Active Net Invoice Amount"] = (
        df["Net Invoice Amount"] * df["Active Net Cost Ratio"]
    ).round(2)
    df.loc[:, "Scaled Net Invoice Amount"] = (
        df["Net Invoice Amount"] * df["Net Cost Ratio"]
    ).round(2)

    non_package = df[~df["Operative Product Type"].isin({"Package"})]
    order_to_active_amt_df = (
        non_package[~non_package["Is Future Start"]]
        .groupby("Parent Sales Line Item ID", as_index=False)
        .apply(lambda group: rounded_invoice_amt(group, "Scaled Active Net Invoice Amount"))
    )
    order_to_active_amt = {
        row["adj_id"]: row["adj_inv_amt"] for _idx, row in order_to_active_amt_df.iterrows()
    }
    df.loc[:, "Rounded Scaled Active Net Invoice Amount"] = df["Sales Line Item ID"].map(
        order_to_active_amt
    ).fillna(df["Scaled Active Net Invoice Amount"])

    order_to_amt_df = non_package.groupby("Parent Sales Line Item ID", as_index=False).apply(
        lambda group: rounded_invoice_amt(group, "Scaled Net Invoice Amount")
    )
    order_to_amt = {row["adj_id"]: row["adj_inv_amt"] for _idx, row in order_to_amt_df.iterrows()}
    df.loc[:, "Rounded Scaled Net Invoice Amount"] = df["Sales Line Item ID"].map(
        order_to_amt
    ).fillna(df["Scaled Net Invoice Amount"])
    return df


def scale_net_invoice_amounts_buggy(df: pd.DataFrame) -> pd.DataFrame:
    """Pre-fix behavior: Package rows included in Net Cost denominators."""
    df = df.copy()
    total_active_net_cost = (
        df[~df["Is Future Start"]]
        .groupby("Parent Sales Line Item ID", as_index=False)
        .agg({"Net Cost": "sum"})
        .rename({"Net Cost": "Total Active Net Cost"}, axis=1)
    )
    total_net_cost = (
        df.groupby("Parent Sales Line Item ID", as_index=False)
        .agg({"Net Cost": "sum"})
        .rename({"Net Cost": "Total Net Cost"}, axis=1)
    )
    df = pd.merge(df, total_net_cost, on=["Parent Sales Line Item ID"], how="left")
    df = pd.merge(df, total_active_net_cost, on=["Parent Sales Line Item ID"], how="left")
    df.loc[:, "Active Net Cost Ratio"] = df.apply(active_net_cost_ratio, axis=1)
    df.loc[:, "Scaled Active Net Invoice Amount"] = (
        df["Net Invoice Amount"] * df["Active Net Cost Ratio"]
    ).round(2)
    return df
