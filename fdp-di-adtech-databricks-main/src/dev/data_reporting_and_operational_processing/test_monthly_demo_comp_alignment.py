"""Tests for BAR-aligned monthly demo comp application."""
from __future__ import annotations
import unittest
import numpy as np
import pandas as pd

def delivery_weighted_monthly_demo_comp(demo_by_month, monthly_delivery):
    demo = demo_by_month.copy()
    demo["Event Month"] = pd.to_datetime(demo["Event Month"]).dt.to_period("M").dt.to_timestamp()
    demo["Monthly Demo Comp"] = np.where(
        demo["Gross Counted Ads (Demo)"] > 0,
        demo["On-Target Net Delivered Impressions"] / demo["Gross Counted Ads (Demo)"],
        0.0,
    )
    delivery = monthly_delivery.copy()
    delivery["Event Month"] = pd.to_datetime(delivery["Event Month"]).dt.to_period("M").dt.to_timestamp()
    delivery = delivery.groupby(["Placement ID", "Event Month"], as_index=False).agg({"Net Counted Ads": "sum"})
    joined = delivery.merge(demo[["Placement ID", "Event Month", "Monthly Demo Comp"]], on=["Placement ID", "Event Month"], how="left")
    joined["Monthly Demo Comp"] = joined["Monthly Demo Comp"].fillna(0.0)
    joined["Weighted Demo Imps"] = joined["Monthly Demo Comp"] * joined["Net Counted Ads"]
    placement = joined.groupby("Placement ID", as_index=True).agg({"Net Counted Ads": "sum", "Weighted Demo Imps": "sum"})
    placement["Gross Counted Ads (Demo)"] = placement["Net Counted Ads"]
    placement["On-Target Net Delivered Impressions"] = placement["Weighted Demo Imps"].round().astype("int64")
    return placement[["On-Target Net Delivered Impressions", "Gross Counted Ads (Demo)"]]

def attach_monthly_demo_to_daily(df, demo_by_month, date_col="Report End Date"):
    out = df.copy()
    out["Event Month"] = pd.to_datetime(out[date_col]).dt.to_period("M").dt.to_timestamp()
    demo = demo_by_month.copy()
    demo["Event Month"] = pd.to_datetime(demo["Event Month"]).dt.to_period("M").dt.to_timestamp()
    out = out.drop(columns=["On-Target Net Delivered Impressions", "Gross Counted Ads (Demo)"], errors="ignore")
    out = out.merge(demo, on=["Placement ID", "Event Month"], how="left")
    out = out.fillna({"On-Target Net Delivered Impressions": 0, "Gross Counted Ads (Demo)": 0})
    return out.drop(columns=["Event Month"], errors="ignore")

class TestMonthlyDemoCompAlignment(unittest.TestCase):
    def test_daily_rows_same_monthly_comp_in_august(self):
        demo_by_month = pd.DataFrame([{
            "Placement ID": 1, "Event Month": "2025-08-01",
            "On-Target Net Delivered Impressions": 742536, "Gross Counted Ads (Demo)": 1000000,
        }])
        daily = pd.DataFrame([
            {"Placement ID": 1, "Report End Date": "2025-08-01", "Net Counted Ads": 1000},
            {"Placement ID": 1, "Report End Date": "2025-08-15", "Net Counted Ads": 2000},
            {"Placement ID": 1, "Report End Date": "2025-08-31", "Net Counted Ads": 15984796},
        ])
        out = attach_monthly_demo_to_daily(daily, demo_by_month)
        comps = out["On-Target Net Delivered Impressions"] / out["Gross Counted Ads (Demo)"]
        self.assertEqual(comps.nunique(), 1)
        self.assertAlmostEqual(float(comps.iloc[0]), 0.742536, places=6)
        # Ticket-style BAR billable approx: 15984796 * 0.742536 ≈ 11869288
        billable = int(15984796 * float(comps.iloc[0]))
        self.assertTrue(abs(billable - 11869288) < 50)

    def test_weighted_monthly_matches_bar_sum_not_old_blend(self):
        demo_by_month = pd.DataFrame([
            {"Placement ID": 10, "Event Month": "2026-04-01", "On-Target Net Delivered Impressions": 50, "Gross Counted Ads (Demo)": 100},
            {"Placement ID": 10, "Event Month": "2026-05-01", "On-Target Net Delivered Impressions": 80, "Gross Counted Ads (Demo)": 100},
        ])
        monthly_delivery = pd.DataFrame([
            {"Placement ID": 10, "Event Month": "2026-04-01", "Net Counted Ads": 1000},
            {"Placement ID": 10, "Event Month": "2026-05-01", "Net Counted Ads": 2000},
        ])
        bar_sum = 0.5 * 1000 + 0.8 * 2000  # 2100
        old_blend = ((50 + 80) / 200) * 3000  # 1950
        weighted = delivery_weighted_monthly_demo_comp(demo_by_month, monthly_delivery)
        encoded = weighted.loc[10, "On-Target Net Delivered Impressions"] / weighted.loc[10, "Gross Counted Ads (Demo)"]
        self.assertAlmostEqual(encoded * 3000, bar_sum, places=5)
        self.assertNotAlmostEqual(old_blend, bar_sum)

if __name__ == "__main__":
    unittest.main()
