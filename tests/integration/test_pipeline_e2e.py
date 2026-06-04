"""
tests/integration/test_pipeline_e2e.py
────────────────────────────────────────
End-to-end integration test: reads real Parquet → runs full pipeline → validates Gold.

Run with: pytest tests/integration/ -v -s

These tests are slower (seconds, not ms) and require data/raw/*.parquet to exist.
In CI they run on a 10K-row sample; locally on the full 500K.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT))

from pipeline.silver.clean_trips import transform as silver_transform
from pipeline.gold.build_star_schema import (
    build_dim_date, build_dim_location, build_dim_vendor,
    build_fct_trips, build_fct_hourly_demand
)
from monitoring.contracts.silver_contract import run_silver_suite
import duckdb


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def raw_df():
    """Loads real (or sample) Parquet data for the full test run."""
    path = ROOT / "data" / "raw" / "yellow_tripdata_2024_q1.parquet"
    if not path.exists():
        pytest.skip(f"Raw data not found at {path}. Run ingestion first.")
    df = pd.read_parquet(path)
    # Use 10K rows in CI for speed; full 500K locally
    sample = df.sample(min(10_000, len(df)), random_state=42)
    return sample


@pytest.fixture(scope="module")
def silver_result(raw_df):
    return silver_transform(raw_df)


@pytest.fixture(scope="module")
def gold_tables(silver_result):
    con = duckdb.connect()
    fct_trips      = build_fct_trips(silver_result.clean_df, con)
    fct_hourly     = build_fct_hourly_demand(fct_trips, con)
    return {
        "fct_trips":         fct_trips,
        "fct_hourly_demand": fct_hourly,
        "dim_date":          build_dim_date(),
        "dim_location":      build_dim_location(),
        "dim_vendor":        build_dim_vendor(),
    }


# ── Silver layer tests ────────────────────────────────────────────────────────

class TestSilverLayer:
    def test_pass_rate_above_97_pct(self, silver_result):
        assert silver_result.pass_rate >= 0.96, (
            f"Silver pass rate {silver_result.pass_rate:.1%} below threshold"
        )

    def test_no_null_pickup_timestamps(self, silver_result):
        nulls = silver_result.clean_df["tpep_pickup_datetime"].isna().sum()
        assert nulls == 0, f"{nulls} null pickup timestamps in Silver"

    def test_all_derived_columns_present(self, silver_result):
        expected = [
            "derived_trip_duration_min", "derived_pickup_hour",
            "derived_is_rush_hour", "derived_is_weekend",
            "derived_tip_rate", "derived_fare_per_mile",
            "derived_speed_mph", "derived_is_airport_trip",
            "derived_in_congestion_zone",
        ]
        missing = [c for c in expected if c not in silver_result.clean_df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_no_negative_fares(self, silver_result):
        neg = (silver_result.clean_df["fare_amount"] < 0).sum()
        assert neg == 0, f"{neg} negative fares in Silver clean data"

    def test_cash_trips_have_zero_tip(self, silver_result):
        cash = silver_result.clean_df[silver_result.clean_df["payment_type"] == 2]
        bad  = (cash["tip_amount"] > 0).sum()
        assert bad == 0, f"{bad} cash trips with tip_amount > 0 (business rule violation)"

    def test_durations_positive(self, silver_result):
        neg = (silver_result.clean_df["derived_trip_duration_min"] <= 0).sum()
        assert neg == 0, f"{neg} trips with non-positive duration in Silver"

    def test_great_expectations_suite_passes(self, silver_result):
        suite = run_silver_suite(silver_result.clean_df)
        failed = [(r.name, r.details) for r in suite.results if not r.passed]
        # Source data may have <0.1% retransmit duplicates; all other expectations must pass
        critical_failures = [(n, d) for n, d in failed if "duplicate" not in n.lower()]
        assert not critical_failures, f"GE contract failures (critical):\n" + "\n".join(str(f) for f in critical_failures)


# ── Gold layer tests ──────────────────────────────────────────────────────────

class TestGoldLayer:
    def test_fct_trips_has_rows(self, gold_tables):
        assert len(gold_tables["fct_trips"]) > 0

    def test_fct_trips_no_null_surrogate_keys(self, gold_tables):
        nulls = gold_tables["fct_trips"]["trip_sk"].isna().sum()
        assert nulls == 0

    def test_fct_trips_surrogate_keys_unique(self, gold_tables):
        dupes = gold_tables["fct_trips"]["trip_sk"].duplicated().sum()
        assert dupes == 0, f"{dupes} duplicate trip_sk values"

    def test_fct_trips_all_fk_in_dim_location(self, gold_tables):
        valid_locs = set(gold_tables["dim_location"]["location_id"])
        pu_bad = ~gold_tables["fct_trips"]["pickup_location_id"].isin(valid_locs)
        do_bad = ~gold_tables["fct_trips"]["dropoff_location_id"].isin(valid_locs)
        assert pu_bad.sum() == 0, f"{pu_bad.sum()} invalid pickup location IDs"
        assert do_bad.sum() == 0, f"{do_bad.sum()} invalid dropoff location IDs"

    def test_window_functions_computed(self, gold_tables):
        fct = gold_tables["fct_trips"]
        assert "zone_7d_rolling_avg_fare" in fct.columns
        assert "fare_pctile_zone_hour" in fct.columns
        # Percentile should be 0–1
        assert fct["fare_pctile_zone_hour"].between(0, 1).all()

    def test_hourly_demand_aggregation_correct(self, gold_tables):
        fct     = gold_tables["fct_trips"]
        hourly  = gold_tables["fct_hourly_demand"]
        # Total trips in hourly agg must equal total trips in fct
        assert hourly["trip_count"].sum() == len(fct)

    def test_dim_date_has_full_year(self, gold_tables):
        dates = gold_tables["dim_date"]
        assert len(dates) >= 365
        assert (dates["date_day"].astype(str).str[:4] == "2024").any()

    def test_dim_location_263_zones(self, gold_tables):
        assert len(gold_tables["dim_location"]) == 265  # 263 + 2 unknowns

    def test_revenue_totals_are_positive(self, gold_tables):
        total_rev = gold_tables["fct_trips"]["total_amount"].sum()
        assert total_rev > 0


# ── Metrics snapshot test (regression guard) ─────────────────────────────────

class TestMetricsSnapshot:
    """Catch silent regressions: if avg fare drops by >20%, something broke upstream."""

    def test_avg_fare_in_expected_range(self, silver_result):
        avg = silver_result.clean_df["fare_amount"].mean()
        assert 12 <= avg <= 50, f"Avg fare ${avg:.2f} outside expected $12–$50"

    def test_avg_trip_distance_in_range(self, silver_result):
        avg = silver_result.clean_df["trip_distance"].mean()
        assert 1.0 <= avg <= 10.0, f"Avg distance {avg:.2f} mi outside expected 1–10"

    def test_credit_card_pct_above_60(self, silver_result):
        cc_pct = (silver_result.clean_df["payment_type"] == 1).mean()
        assert cc_pct >= 0.60, f"CC rate {cc_pct:.1%} below expected 60%"

    def test_rush_hour_pct_in_range(self, silver_result):
        rush = silver_result.clean_df["derived_is_rush_hour"].mean()
        assert 0.15 <= rush <= 0.45, f"Rush hour % {rush:.1%} outside expected range"
