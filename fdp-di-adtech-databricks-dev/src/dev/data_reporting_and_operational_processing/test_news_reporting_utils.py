"""Unit tests for news_reporting_utils.

Run with:
    pytest /Workspace/Users/kranthi.kommineni@fox.com/fdp-di-adtech-databricks-dev/\
           src/dev/data_reporting_and_operational_processing/test_news_reporting_utils.py -v
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta

# Ensure the module directory is on the path when running via pytest
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__))
        if '__file__' in dir()
        else '/Workspace/Users/kranthi.kommineni@fox.com/'
               'fdp-di-adtech-databricks-dev/src/dev/data_reporting_and_operational_processing'
    )
)

from news_reporting_utils import (  # noqa: E402
    PODCAST_COLUMN_MAP,
    REPORT_COLUMNS,
    base_line_item_name,
    calculate_metrics,
    combine_extension_lines,
    format_podcast_pacing,
    metrics_calculaition,
    osi,
    podcast_osi,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _feed_row(**overrides) -> pd.DataFrame:
    """One-row DataFrame matching the gold Amperwave / Megaphone schema."""
    defaults = {
        'advertiser_name': 'Test Advertiser',
        'deal_name': 'Test Deal',
        'deal_line_item_name': 'Test Line Item',
        'deal_line_item_start_date': '2026-01-01',
        'deal_line_item_end_date': '2026-12-31',
        'net_unit_cost_amt': '10.50',
        'production_quantity': '1000000',
        'quantity': '900000',
        'account_executive': 'Jane AE',
        'delivered_impressions': '500000',
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


def _metric_row(**overrides) -> pd.Series:
    """pd.Series with all columns consumed by metrics_calculaition."""
    defaults = {
        'Total Impressions': 1_000,
        'Total Error Count': 0,
        'Impressions (3rd Party)': 0,
        'Clicks (3rd Party)': 0,
        'Contracted Quantity': 1_000,
        'Ad Server Booked Impressions': 1_000,
        'Ad Server Impressions': 900,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


def _osi_row(start: str, end: str, delivered: float, contracted: float) -> pd.Series:
    return pd.Series({
        'Ad Server Impressions': delivered,
        'Contracted Quantity': contracted,
        'Line Item Start Date': pd.Timestamp(start),
        'Line Item End Date': pd.Timestamp(end),
    })


# ---------------------------------------------------------------------------
# format_podcast_pacing
# ---------------------------------------------------------------------------

class TestFormatPodcastPacing:
    """Covers the rename + type-coerce + constant-fill logic."""

    def test_output_columns_renamed(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        for src, dst in PODCAST_COLUMN_MAP.items():
            assert dst in result.columns, f"Expected '{dst}' after rename"

    def test_amperwave_instance_label(self):
        assert format_podcast_pacing(_feed_row(), 'Amperwave')['Instance'].iloc[0] == 'Amperwave'

    def test_spotify_instance_label(self):
        assert format_podcast_pacing(_feed_row(), 'Spotify')['Instance'].iloc[0] == 'Spotify'

    def test_numeric_columns_cast_correctly(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        assert result['Rate'].iloc[0] == pytest.approx(10.50)
        assert result['Goal Quantity'].iloc[0] == 1_000_000
        assert result['Contracted Quantity'].iloc[0] == 900_000
        assert result['Ad Server Impressions'].iloc[0] == 500_000

    def test_date_columns_are_datetime64(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        assert pd.api.types.is_datetime64_any_dtype(result['Line Item Start Date'])
        assert pd.api.types.is_datetime64_any_dtype(result['Line Item End Date'])

    def test_quantity_equals_goal_quantity(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        assert result['Quantity'].iloc[0] == result['Goal Quantity'].iloc[0]

    def test_total_impressions_equals_ad_server_impressions(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        assert result['Total Impressions'].iloc[0] == result['Ad Server Impressions'].iloc[0]

    def test_3p_fields_are_zero(self):
        result = format_podcast_pacing(_feed_row(), 'Amperwave')
        assert result['Impressions (3rd Party)'].iloc[0] == 0
        assert result['Clicks (3rd Party)'].iloc[0] == 0
        assert result['Total Error Count'].iloc[0] == 0

    def test_delivery_indicator_empty_string(self):
        assert format_podcast_pacing(_feed_row(), 'Amperwave')['Delivery Indicator'].iloc[0] == ''

    def test_invalid_numeric_coerced_to_zero(self):
        result = format_podcast_pacing(
            _feed_row(net_unit_cost_amt='N/A', production_quantity=None, delivered_impressions='bad'),
            'Amperwave',
        )
        assert result['Rate'].iloc[0] == 0
        assert result['Goal Quantity'].iloc[0] == 0
        assert result['Ad Server Impressions'].iloc[0] == 0

    def test_empty_dataframe_returns_empty(self):
        empty = pd.DataFrame(columns=list(PODCAST_COLUMN_MAP.keys()))
        assert len(format_podcast_pacing(empty, 'Amperwave')) == 0

    def test_multiple_rows_all_get_instance_label(self):
        feed = pd.concat([_feed_row(), _feed_row(advertiser_name='Another')], ignore_index=True)
        result = format_podcast_pacing(feed, 'Spotify')
        assert (result['Instance'] == 'Spotify').all()


# ---------------------------------------------------------------------------
# osi
# ---------------------------------------------------------------------------

class TestOsi:
    """Covers OSI for 1p vs 3p impressions, zero guards, in-flight vs ended."""

    def _row(self, start: str, end: str, ad_imps: int, contracted: int, p3_imps: int = 0) -> pd.Series:
        return pd.Series({
            'Ad Server Impressions': ad_imps,
            'Impressions (3rd Party)': p3_imps,
            'Contracted Quantity': contracted,
            'Line Item Start Date': pd.Timestamp(start),
            'Line Item End Date': pd.Timestamp(end),
        })

    def test_1p_on_pace_mid_flight(self):
        # Day 182 of 365-day flight, 182 of 365 delivered → OSI ≈ 1.0
        row = self._row('2026-01-01', '2026-12-31', ad_imps=182, contracted=365)
        assert osi(row, datetime(2026, 7, 1), '1p') == pytest.approx(1.0, abs=0.05)

    def test_1p_zero_impressions_returns_zero(self):
        row = self._row('2026-01-01', '2026-12-31', ad_imps=0, contracted=1_000)
        assert osi(row, datetime(2026, 7, 1), '1p') == 0

    def test_1p_zero_contracted_returns_zero(self):
        row = self._row('2026-01-01', '2026-12-31', ad_imps=500, contracted=0)
        assert osi(row, datetime(2026, 7, 1), '1p') == 0

    def test_3p_uses_3p_impressions(self):
        # 1p impressions are 0 but 3p are set — OSI should be > 0
        row = self._row('2026-01-01', '2026-12-31', ad_imps=0, contracted=365, p3_imps=182)
        assert osi(row, datetime(2026, 7, 1), '3p') > 0

    def test_3p_zero_3p_impressions_returns_zero(self):
        row = self._row('2026-01-01', '2026-12-31', ad_imps=500, contracted=365, p3_imps=0)
        assert osi(row, datetime(2026, 7, 1), '3p') == 0

    def test_after_flight_end_returns_delivery_ratio(self):
        # Campaign ended March 31; report date July → no pacing divisor
        row = self._row('2026-01-01', '2026-03-31', ad_imps=90, contracted=90)
        assert osi(row, datetime(2026, 7, 1), '1p') == pytest.approx(1.0, abs=0.01)

    def test_over_pacing_osi_exceeds_one(self):
        row = self._row('2026-01-01', '2026-12-31', ad_imps=500, contracted=365)
        assert osi(row, datetime(2026, 7, 1), '1p') > 1.0

    def test_under_pacing_osi_below_one(self):
        # Only 10 of 365 delivered, halfway through flight → very low OSI
        row = self._row('2026-01-01', '2026-12-31', ad_imps=10, contracted=365)
        assert osi(row, datetime(2026, 7, 1), '1p') < 1.0


# ---------------------------------------------------------------------------
# podcast_osi  (Amperwave / Spotify formula from Ad Ops — UAT 07/28/2026)
# ---------------------------------------------------------------------------

class TestPodcastOsi:
    """(delivered / contracted) / (days live / total flight days)."""

    def test_on_pace_mid_flight_returns_one(self):
        # 100-day flight (01/01 - 04/10), report on day 50, 50% delivered.
        row = _osi_row('2026-01-01', '2026-04-10', delivered=500, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 2, 19)) == pytest.approx(1.0)

    def test_formula_value_matches_hand_calculation(self):
        # Chevron-style flight: 06/01 - 08/14 (75 days), report 06/30 (day 30).
        # delivery progress = 600k / 2.0M = 0.30; flight progress = 30/75 = 0.40
        row = _osi_row('2026-06-01', '2026-08-14', delivered=600_000, contracted=2_000_000)
        assert podcast_osi(row, datetime(2026, 6, 30)) == pytest.approx(0.30 / 0.40)

    def test_over_pacing_exceeds_one(self):
        row = _osi_row('2026-01-01', '2026-04-10', delivered=800, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 2, 19)) == pytest.approx(1.6)

    def test_completed_flight_returns_delivery_ratio(self):
        # Flight over → flight progress capped at 100%, OSI = delivered/contracted.
        row = _osi_row('2026-01-01', '2026-03-31', delivered=900, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 7, 1)) == pytest.approx(0.9)

    def test_flight_not_started_returns_zero(self):
        row = _osi_row('2026-08-01', '2026-09-30', delivered=100, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 7, 1)) == 0

    def test_zero_contracted_returns_zero(self):
        row = _osi_row('2026-01-01', '2026-12-31', delivered=500, contracted=0)
        assert podcast_osi(row, datetime(2026, 7, 1)) == 0

    def test_zero_delivered_returns_zero(self):
        row = _osi_row('2026-01-01', '2026-12-31', delivered=0, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 7, 1)) == 0

    def test_missing_dates_return_zero(self):
        row = pd.Series({
            'Ad Server Impressions': 500,
            'Contracted Quantity': 1_000,
            'Line Item Start Date': pd.NaT,
            'Line Item End Date': pd.NaT,
        })
        assert podcast_osi(row, datetime(2026, 7, 1)) == 0

    def test_end_before_start_returns_zero(self):
        row = _osi_row('2026-06-01', '2026-05-01', delivered=500, contracted=1_000)
        assert podcast_osi(row, datetime(2026, 7, 1)) == 0


# ---------------------------------------------------------------------------
# base_line_item_name / combine_extension_lines  (UAT 07/28/2026)
# ---------------------------------------------------------------------------

class TestBaseLineItemName:

    def test_strips_extension_suffix(self):
        assert base_line_item_name('DIO - AUD - FNC - Mid - Hourly Update - Extension') == \
            'DIO - AUD - FNC - Mid - Hourly Update'

    def test_strips_numbered_extension_suffix(self):
        assert base_line_item_name('DIO - AUD - FNC - Pre/Mid - Rundown - Extension 2') == \
            'DIO - AUD - FNC - Pre/Mid - Rundown'

    def test_case_insensitive(self):
        assert base_line_item_name('Some Line - EXTENSION') == 'Some Line'

    def test_name_without_suffix_unchanged(self):
        assert base_line_item_name('DIO - AUD - FNC - Mid - Hourly Update') == \
            'DIO - AUD - FNC - Mid - Hourly Update'

    def test_extension_mid_name_unchanged(self):
        assert base_line_item_name('DIO - Extension Cord Ads - Mid') == \
            'DIO - Extension Cord Ads - Mid'

    def test_extension_without_dash_separator_unchanged(self):
        assert base_line_item_name('Homepage Extension') == 'Homepage Extension'

    def test_non_string_passthrough(self):
        assert base_line_item_name(None) is None
        assert pd.isna(base_line_item_name(np.nan))


class TestCombineExtensionLines:
    """AOS extension order lines roll up into their base line."""

    CHEVRON_ORDER = 'Chevron / 26-27 / Scatter / News Digital / A250'

    def _chevron_feed(self) -> pd.DataFrame:
        """The four AOS order lines from the UAT feedback (order 14807 / deal 263391)."""
        rows = [
            # base lines (06/01 - 08/02 after modification)
            dict(deal_line_item_name='DIO - AUD - FNC - Pre/Mid - Rundown',
                 deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-02',
                 quantity='950000', production_quantity='997500', delivered_impressions='600000'),
            dict(deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update',
                 deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-02',
                 quantity='1900000', production_quantity='1995000', delivered_impressions='1200000'),
            # extension lines (08/04 - 08/14)
            dict(deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update - Extension',
                 deal_line_item_start_date='2026-08-04', deal_line_item_end_date='2026-08-14',
                 quantity='100000', production_quantity='105000', delivered_impressions='0'),
            dict(deal_line_item_name='DIO - AUD - FNC - Pre/Mid - Rundown - Extension',
                 deal_line_item_start_date='2026-08-04', deal_line_item_end_date='2026-08-14',
                 quantity='50000', production_quantity='52500', delivered_impressions='0'),
        ]
        frames = [
            _feed_row(
                advertiser_name='Chevron',
                deal_name=self.CHEVRON_ORDER,
                net_unit_cost_amt='20',
                **row,
            )
            for row in rows
        ]
        return pd.concat(frames, ignore_index=True)

    def _combined_chevron(self) -> pd.DataFrame:
        formatted = format_podcast_pacing(self._chevron_feed(), 'Spotify')
        return combine_extension_lines(formatted)

    def test_four_lines_collapse_to_two(self):
        assert len(self._combined_chevron()) == 2

    def test_hourly_update_quantities_match_uat_expectation(self):
        result = self._combined_chevron()
        row = result[result['Line Item Name'] == 'DIO - AUD - FNC - Mid - Hourly Update'].iloc[0]
        assert row['Goal Quantity'] == 2_100_000
        assert row['Contracted Quantity'] == 2_000_000

    def test_rundown_quantities_match_uat_expectation(self):
        result = self._combined_chevron()
        row = result[result['Line Item Name'] == 'DIO - AUD - FNC - Pre/Mid - Rundown'].iloc[0]
        assert row['Goal Quantity'] == 1_050_000
        assert row['Contracted Quantity'] == 1_000_000

    def test_flight_window_spans_base_start_to_extension_end(self):
        result = self._combined_chevron()
        for _, row in result.iterrows():
            assert row['Line Item Start Date'] == pd.Timestamp('2026-06-01')
            assert row['Line Item End Date'] == pd.Timestamp('2026-08-14')

    def test_delivered_impressions_are_summed(self):
        result = self._combined_chevron()
        row = result[result['Line Item Name'] == 'DIO - AUD - FNC - Mid - Hourly Update'].iloc[0]
        assert row['Ad Server Impressions'] == 1_200_000
        assert row['Total Impressions'] == 1_200_000

    def test_quantity_recomputed_from_merged_goal(self):
        result = self._combined_chevron()
        assert (result['Quantity'] == result['Goal Quantity']).all()

    def test_rate_taken_from_base_line(self):
        feed = self._chevron_feed()
        # Give the extension a different rate: the base (earliest start) must win.
        feed.loc[feed['deal_line_item_name'].str.endswith('Extension'), 'net_unit_cost_amt'] = '25'
        result = combine_extension_lines(format_podcast_pacing(feed, 'Spotify'))
        assert (result['Rate'] == 20).all()

    def test_lines_without_extension_pass_through(self):
        formatted = format_podcast_pacing(_feed_row(), 'Amperwave')
        result = combine_extension_lines(formatted)
        assert len(result) == 1
        assert result['Goal Quantity'].iloc[0] == 1_000_000
        assert result['Contracted Quantity'].iloc[0] == 900_000

    def test_same_line_name_in_different_orders_not_merged(self):
        feed = pd.concat(
            [
                _feed_row(deal_name='Order A', deal_line_item_name='Same Line'),
                _feed_row(deal_name='Order B', deal_line_item_name='Same Line - Extension'),
            ],
            ignore_index=True,
        )
        result = combine_extension_lines(format_podcast_pacing(feed, 'Spotify'))
        assert len(result) == 2

    def test_empty_dataframe_returns_empty(self):
        empty = format_podcast_pacing(pd.DataFrame(columns=list(PODCAST_COLUMN_MAP.keys())), 'Spotify')
        assert len(combine_extension_lines(empty)) == 0

    def test_rolled_up_duplicate_rows_are_not_double_counted(self):
        """Gold-table shape where OMS already rolls extensions into the deal
        line: multiple delivering campaigns repeat the same combined totals
        (quantity 2,000,000 / production 2,100,000). Quantities must be
        counted once; delivered impressions still sum across the rows."""
        feed = pd.concat(
            [
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update',
                          deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-14',
                          quantity='2000000', production_quantity='2100000',
                          net_unit_cost_amt='20', delivered_impressions='700000'),
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update',
                          deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-14',
                          quantity='2000000', production_quantity='2100000',
                          net_unit_cost_amt='20', delivered_impressions='500000'),
            ],
            ignore_index=True,
        )
        result = combine_extension_lines(format_podcast_pacing(feed, 'Spotify'))
        assert len(result) == 1
        row = result.iloc[0]
        assert row['Goal Quantity'] == 2_100_000
        assert row['Contracted Quantity'] == 2_000_000
        assert row['Ad Server Impressions'] == 1_200_000
        assert row['Total Impressions'] == 1_200_000
        assert row['Quantity'] == 2_100_000

    def test_mixed_duplicates_and_extension_line(self):
        """Duplicated base rows plus a real extension row: base counted once,
        extension quantity added, all delivery summed."""
        feed = pd.concat(
            [
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update',
                          deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-02',
                          quantity='1900000', production_quantity='1995000',
                          net_unit_cost_amt='20', delivered_impressions='700000'),
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update',
                          deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-02',
                          quantity='1900000', production_quantity='1995000',
                          net_unit_cost_amt='20', delivered_impressions='500000'),
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='DIO - AUD - FNC - Mid - Hourly Update - Extension',
                          deal_line_item_start_date='2026-08-04', deal_line_item_end_date='2026-08-14',
                          quantity='100000', production_quantity='105000',
                          net_unit_cost_amt='20', delivered_impressions='0'),
            ],
            ignore_index=True,
        )
        result = combine_extension_lines(format_podcast_pacing(feed, 'Spotify'))
        assert len(result) == 1
        row = result.iloc[0]
        assert row['Goal Quantity'] == 2_100_000
        assert row['Contracted Quantity'] == 2_000_000
        assert row['Ad Server Impressions'] == 1_200_000
        assert row['Line Item Start Date'] == pd.Timestamp('2026-06-01')
        assert row['Line Item End Date'] == pd.Timestamp('2026-08-14')

    def test_two_distinct_extensions_with_equal_quantities_both_counted(self):
        """'Extension' and 'Extension 2' with identical quantities are
        different contributions (different original names) and must both add."""
        feed = pd.concat(
            [
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='Base Line',
                          deal_line_item_start_date='2026-06-01', deal_line_item_end_date='2026-08-02',
                          quantity='1800000', production_quantity='1890000',
                          delivered_impressions='100000'),
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='Base Line - Extension',
                          deal_line_item_start_date='2026-08-04', deal_line_item_end_date='2026-08-14',
                          quantity='100000', production_quantity='105000',
                          delivered_impressions='0'),
                _feed_row(deal_name=self.CHEVRON_ORDER,
                          deal_line_item_name='Base Line - Extension 2',
                          deal_line_item_start_date='2026-08-04', deal_line_item_end_date='2026-08-14',
                          quantity='100000', production_quantity='105000',
                          delivered_impressions='0'),
            ],
            ignore_index=True,
        )
        result = combine_extension_lines(format_podcast_pacing(feed, 'Spotify'))
        assert len(result) == 1
        row = result.iloc[0]
        assert row['Contracted Quantity'] == 2_000_000
        assert row['Goal Quantity'] == 2_100_000

    def test_no_helper_columns_leak_into_output(self):
        result = self._combined_chevron()
        assert '_original_line_item_name' not in result.columns


# ---------------------------------------------------------------------------
# metrics_calculaition
# ---------------------------------------------------------------------------

class TestMetricsCalculaition:
    """Tests each named calculation branch individually."""

    def test_total_error_rate_no_errors(self):
        assert metrics_calculaition(_metric_row(), 'Total Error Rate') == 0

    def test_total_error_rate_calculated(self):
        row = _metric_row(**{'Total Impressions': 900, 'Total Error Count': 100})
        assert metrics_calculaition(row, 'Total Error Rate') == pytest.approx(0.1)

    def test_total_error_rate_zero_denominator(self):
        row = _metric_row(**{'Total Impressions': 0, 'Total Error Count': 0})
        assert metrics_calculaition(row, 'Total Error Rate') == 0

    def test_3rd_party_ctr_zero_impressions(self):
        row = _metric_row(**{'Impressions (3rd Party)': 0})
        assert metrics_calculaition(row, '3rd Party CTR') == 0

    def test_3rd_party_ctr_calculated(self):
        row = _metric_row(**{'Impressions (3rd Party)': 1_000, 'Clicks (3rd Party)': 50})
        assert metrics_calculaition(row, '3rd Party CTR') == pytest.approx(0.05)

    def test_buffer_zero_contracted(self):
        row = _metric_row(**{'Contracted Quantity': 0})
        assert metrics_calculaition(row, 'Buffer') == 0

    def test_buffer_on_pace(self):
        row = _metric_row(**{'Ad Server Booked Impressions': 1_000, 'Contracted Quantity': 1_000})
        assert metrics_calculaition(row, 'Buffer') == pytest.approx(0.0)

    def test_buffer_over_delivery(self):
        row = _metric_row(**{'Ad Server Booked Impressions': 1_100, 'Contracted Quantity': 1_000})
        assert metrics_calculaition(row, 'Buffer') == pytest.approx(0.1)

    def test_discrepancy_zero_3p_impressions(self):
        row = _metric_row(**{'Impressions (3rd Party)': 0})
        assert metrics_calculaition(row, 'Discrepancy') == 0

    def test_discrepancy_calculated(self):
        row = _metric_row(**{'Impressions (3rd Party)': 1_000, 'Ad Server Impressions': 900})
        assert metrics_calculaition(row, 'Discrepancy') == pytest.approx(0.1)


# ---------------------------------------------------------------------------
# calculate_metrics + format_podcast_pacing  (full Amperwave / Spotify pipeline)
# ---------------------------------------------------------------------------

class TestCalculateMetricsPodcast:
    """End-to-end: raw feed → format → combine extensions → calculate_metrics."""

    REPORT_DATE = datetime(2026, 7, 13)

    def _pipeline(self, feed_df: pd.DataFrame, label: str = 'Amperwave') -> pd.DataFrame:
        return calculate_metrics(
            combine_extension_lines(format_podcast_pacing(feed_df, label).copy()),
            self.REPORT_DATE,
            is_podcast=True,
        )

    def test_all_report_columns_present(self):
        result = self._pipeline(_feed_row()).loc[:, REPORT_COLUMNS]
        assert list(result.columns) == REPORT_COLUMNS

    def test_no_infinities_in_any_numeric_column(self):
        result = self._pipeline(_feed_row())
        num_cols = result.select_dtypes(include=[np.number]).columns
        assert not result[num_cols].isin([np.inf, -np.inf]).any().any()

    def test_zero_inputs_produce_zero_metrics_not_errors(self):
        feed = _feed_row(net_unit_cost_amt='0', production_quantity='0', quantity='0', delivered_impressions='0')
        result = self._pipeline(feed)
        assert result['Buffer'].iloc[0] == 0
        assert result['Discrepancy'].iloc[0] == 0
        assert result['Current First Party OSI'].iloc[0] == 0
        assert result['Current Third Party OSI'].iloc[0] == 0

    def test_osi_positive_for_active_campaign(self):
        feed = _feed_row(
            deal_line_item_start_date='2026-01-01',
            deal_line_item_end_date='2026-12-31',
            delivered_impressions='500000',
            quantity='1000000',
        )
        result = self._pipeline(feed)
        assert result['Current First Party OSI'].iloc[0] > 0

    def test_both_osi_columns_populated_with_podcast_formula(self):
        """UAT: columns R & S must both carry the Amperwave/Spotify OSI value."""
        feed = _feed_row(
            deal_line_item_start_date='2026-06-01',
            deal_line_item_end_date='2026-08-14',
            delivered_impressions='1200000',
            quantity='2000000',
        )
        result = self._pipeline(feed)
        # 07/13 is day 43 of the 75-day flight: (0.6) / (43/75)
        expected = (1_200_000 / 2_000_000) / (43 / 75)
        assert result['Current First Party OSI'].iloc[0] == pytest.approx(expected)
        assert result['Current Third Party OSI'].iloc[0] == pytest.approx(expected)

    def test_third_party_osi_no_longer_zero_for_podcasts(self):
        """Regression: the generic 3p OSI returned 0 because podcasts have no 3p imps."""
        result = self._pipeline(_feed_row())
        assert result['Current Third Party OSI'].iloc[0] > 0

    def test_chevron_uat_scenario_end_to_end(self):
        """Exact UAT case: merged quantities and OSI on the merged flight."""
        chevron = TestCombineExtensionLines()
        report_date = datetime(2026, 7, 23)
        result = calculate_metrics(
            combine_extension_lines(format_podcast_pacing(chevron._chevron_feed(), 'Spotify')),
            report_date,
            is_podcast=True,
        ).loc[:, REPORT_COLUMNS]

        assert len(result) == 2
        hourly = result[result['Line Item Name'] == 'DIO - AUD - FNC - Mid - Hourly Update'].iloc[0]
        assert hourly['Goal Quantity'] == 2_100_000
        assert hourly['Contracted Quantity'] == 2_000_000
        assert hourly['Line Item Start Date'] == pd.Timestamp('2026-06-01')
        assert hourly['Line Item End Date'] == pd.Timestamp('2026-08-14')
        # 07/23 is day 53 of the 75-day merged flight
        expected_osi = (1_200_000 / 2_000_000) / (53 / 75)
        assert hourly['Current First Party OSI'] == pytest.approx(expected_osi)
        assert hourly['Current Third Party OSI'] == pytest.approx(expected_osi)

    def test_amperwave_and_spotify_instances_distinguished(self):
        amp = self._pipeline(_feed_row(), 'Amperwave')
        meg = self._pipeline(_feed_row(), 'Spotify')
        assert amp['Instance'].iloc[0] == 'Amperwave'
        assert meg['Instance'].iloc[0] == 'Spotify'

    def test_na_nulls_filled_in_metric_columns(self):
        """NaN in metric columns must be replaced with 0, not propagated."""
        feed = _feed_row(delivered_impressions='0', quantity='0')
        result = self._pipeline(feed)
        metric_cols = [
            'Total Error Rate', '3rd Party CTR', 'Buffer',
            'Discrepancy', 'Current First Party OSI', 'Current Third Party OSI',
        ]
        assert not result[metric_cols].isna().any().any()

    def test_report_columns_subset_matches_constant(self):
        result = self._pipeline(_feed_row()).loc[:, REPORT_COLUMNS]
        assert set(result.columns) == set(REPORT_COLUMNS)
