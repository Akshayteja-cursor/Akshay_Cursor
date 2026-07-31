"""Regression tests for BAR vs AOS Net Invoice Amount alignment."""

from __future__ import annotations

import unittest

import pandas as pd

from bar_net_invoice_scaling import (
    scale_net_invoice_amounts,
    scale_net_invoice_amounts_buggy,
)


def _so_258950_parent_581030() -> pd.DataFrame:
    """Constellation MWC Social package from June '26 BAR Net Check.xlsx.

    AOS finance parent Net Invoice Amount: 183673.4694
    Three child lines each Net Cost 100000; package row also Net Cost 300000.
    """
    parent_id = 581030
    aos_net_invoice = 183673.4694
    rows = [
        # Package parent row (dropped from BAR output, must not affect scaling)
        {
            "Parent Sales Line Item ID": parent_id,
            "Sales Line Item ID": parent_id,
            "Operative Product Type": "Package",
            "Net Cost": 300000.0,
            "Net Invoice Amount": aos_net_invoice,
            "Is Future Start": False,
        },
        {
            "Parent Sales Line Item ID": parent_id,
            "Sales Line Item ID": 581031,
            "Operative Product Type": "Standard",
            "Net Cost": 100000.0,
            "Net Invoice Amount": aos_net_invoice,
            "Is Future Start": False,
        },
        {
            "Parent Sales Line Item ID": parent_id,
            "Sales Line Item ID": 581033,
            "Operative Product Type": "Standard",
            "Net Cost": 100000.0,
            "Net Invoice Amount": aos_net_invoice,
            "Is Future Start": False,
        },
        {
            "Parent Sales Line Item ID": parent_id,
            "Sales Line Item ID": 581034,
            "Operative Product Type": "Standard",
            "Net Cost": 100000.0,
            "Net Invoice Amount": aos_net_invoice,
            "Is Future Start": False,
        },
    ]
    return pd.DataFrame(rows)


class TestBarNetInvoiceScaling(unittest.TestCase):
    def test_buggy_behavior_is_half_of_aos(self) -> None:
        df = _so_258950_parent_581030()
        out = scale_net_invoice_amounts_buggy(df)
        children = out[out["Operative Product Type"] != "Package"]
        bar_sum = children["Scaled Active Net Invoice Amount"].sum()
        aos = 183673.4694
        self.assertAlmostEqual(bar_sum / aos, 0.5, places=5)
        # Matches observed BAR Cash sheet values (~30612.24 each)
        self.assertTrue(((children["Scaled Active Net Invoice Amount"] - 30612.24).abs() < 0.02).all())

    def test_fix_aligns_children_sum_to_aos_net_invoice(self) -> None:
        df = _so_258950_parent_581030()
        out = scale_net_invoice_amounts(df)
        children = out[out["Operative Product Type"] != "Package"]
        bar_sum = children["Rounded Scaled Active Net Invoice Amount"].sum()
        aos = 183673.4694
        self.assertAlmostEqual(bar_sum, round(aos, 2), places=2)

    def test_single_child_package_no_longer_halved(self) -> None:
        # Parent 581025 pattern: one child with same Net Cost as package
        aos = 95374.17
        df = pd.DataFrame(
            [
                {
                    "Parent Sales Line Item ID": 581025,
                    "Sales Line Item ID": 581025,
                    "Operative Product Type": "Package",
                    "Net Cost": 275000.0,
                    "Net Invoice Amount": aos,
                    "Is Future Start": False,
                },
                {
                    "Parent Sales Line Item ID": 581025,
                    "Sales Line Item ID": 7310117465,
                    "Operative Product Type": "Standard",
                    "Net Cost": 275000.0,
                    "Net Invoice Amount": aos,
                    "Is Future Start": False,
                },
            ]
        )
        buggy = scale_net_invoice_amounts_buggy(df)
        fixed = scale_net_invoice_amounts(df)
        buggy_child = buggy.loc[buggy["Operative Product Type"] != "Package", "Scaled Active Net Invoice Amount"].iloc[0]
        fixed_child = fixed.loc[
            fixed["Operative Product Type"] != "Package", "Rounded Scaled Active Net Invoice Amount"
        ].iloc[0]
        self.assertAlmostEqual(buggy_child, aos / 2, places=2)
        self.assertAlmostEqual(fixed_child, aos, places=2)


if __name__ == "__main__":
    unittest.main()
