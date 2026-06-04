"""
tests/unit/test_silver_transforms.py
──────────────────────────────────────
Unit tests for silver transform functions.

Philosophy:
  - Test business rules, not implementation details
  - Each test has ONE reason to fail
  - Edge cases (zeros, nulls, extremes) tested explicitly
  - Use realistic data fixtures, not toy examples
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
from pipeline.silver.clean_trips import (
    cast_types,
    derive_financial_features,
    derive_geo_features,
    derive_time_features,
    parse_timestamps,
    remove_outliers,
    transform,
    ZONE_JFK,
    ZONE_LGA,
    RATECODE_JFK,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_trip() -> dict:
    """Single valid trip record."""
    return {
        "vendorid":              1,
        "tpep_pickup_datetime":  "2024-01-15 08:30:00",
        "tpep_dropoff_datetime": "2024-01-15 08:52:00",
        "passenger_count":       1,
        "trip_distance":         3.2,
        "ratecodeid":            1,
        "store_and_fwd_flag":    "N",
        "pulocationid":          50,
        "dolocationid":          79,
        "payment_type":          1,
        "fare_amount":           15.50,
        "extra":                 2.50,
        "mta_tax":               0.50,
        "tip_amount":            3.10,
        "tolls_amount":          0.00,
        "improvement_surcharge": 1.00,
        "congestion_surcharge":  2.50,
        "total_amount":          25.10,
        "airport_fee":           0.00,
    }


@pytest.fixture
def standard_df(minimal_trip) -> pd.DataFrame:
    """100-row DataFrame of similar trips."""
    rows = [minimal_trip.copy() for _ in range(100)]
    return pd.DataFrame(rows)


@pytest.fixture
def mixed_df(minimal_trip) -> pd.DataFrame:
    """
    DataFrame with realistic variation:
    - Rush hour vs. overnight
    - Credit card vs. cash
    - Airport vs. standard
    - Various fare amounts
    """
    import random
    random.seed(42)
    rng = np.random.default_rng(42)
    rows = []
    for i in range(200):
        r = minimal_trip.copy()
        r["tpep_pickup_datetime"]  = f"2024-01-{(i % 28)+1:02d} {(i % 24):02d}:00:00"
        r["tpep_dropoff_datetime"] = f"2024-01-{(i % 28)+1:02d} {(i % 24):02d}:{(i % 50)+5:02d}:00"
        r["fare_amount"]   = round(float(rng.uniform(4, 80)), 2)
        r["trip_distance"] = round(float(rng.uniform(0.5, 15)), 2)
        r["payment_type"]  = int(rng.choice([1, 2], p=[0.7, 0.3]))
        r["tip_amount"]    = round(float(r["fare_amount"] * rng.uniform(0.15, 0.25)), 2) if r["payment_type"] == 1 else 0.0
        r["total_amount"]  = round(r["fare_amount"] + r["extra"] + r["mta_tax"] + r["improvement_surcharge"] + r["tip_amount"], 2)
        rows.append(r)
    return pd.DataFrame(rows)


# ── Type casting ──────────────────────────────────────────────────────────────

class TestCastTypes:
    def test_vendorid_is_int8(self, standard_df):
        result = cast_types(standard_df)
        assert result["vendorid"].dtype == pd.Int8Dtype()

    def test_location_ids_are_int16(self, standard_df):
        result = cast_types(standard_df)
        assert result["pulocationid"].dtype == pd.Int16Dtype()
        assert result["dolocationid"].dtype == pd.Int16Dtype()

    def test_fare_is_float32(self, standard_df):
        result = cast_types(standard_df)
        assert result["fare_amount"].dtype == np.float32

    def test_nullable_int_preserves_nulls(self, standard_df):
        standard_df.loc[0, "passenger_count"] = None
        result = cast_types(standard_df)
        assert pd.isna(result.loc[0, "passenger_count"])


# ── Timestamps ───────────────────────────────────────────────────────────────

class TestParseTimestamps:
    def test_duration_computed(self, standard_df):
        df = parse_timestamps(standard_df)
        # 08:30 → 08:52 = 22 minutes
        assert df["derived_trip_duration_min"].iloc[0] == pytest.approx(22.0, abs=0.1)

    def test_negative_duration_survives_for_downstream_filtering(self, minimal_trip):
        """parse_timestamps does NOT filter; remove_outliers does. Separation of concerns."""
        row = minimal_trip.copy()
        row["tpep_dropoff_datetime"] = "2024-01-15 08:00:00"  # before pickup
        df = pd.DataFrame([row])
        result = parse_timestamps(df)
        assert result["derived_trip_duration_min"].iloc[0] < 0  # negative, not dropped

    def test_timestamps_are_datetime_dtype(self, standard_df):
        df = parse_timestamps(standard_df)
        assert pd.api.types.is_datetime64_any_dtype(df["tpep_pickup_datetime"])


# ── Time features ─────────────────────────────────────────────────────────────

class TestDeriveTimeFeatures:
    def test_rush_hour_morning(self, minimal_trip):
        minimal_trip["tpep_pickup_datetime"] = "2024-01-15 08:00:00"  # Monday
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_time_features(df)
        assert df["derived_is_rush_hour"].iloc[0] == True

    def test_rush_hour_evening(self, minimal_trip):
        minimal_trip["tpep_pickup_datetime"] = "2024-01-15 17:00:00"  # Monday PM
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_time_features(df)
        assert df["derived_is_rush_hour"].iloc[0] == True

    def test_not_rush_hour_midday(self, minimal_trip):
        minimal_trip["tpep_pickup_datetime"] = "2024-01-15 13:00:00"
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_time_features(df)
        assert df["derived_is_rush_hour"].iloc[0] == False

    def test_weekend_detection(self, minimal_trip):
        minimal_trip["tpep_pickup_datetime"] = "2024-01-20 14:00:00"  # Saturday
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_time_features(df)
        assert df["derived_is_weekend"].iloc[0] == True

    def test_time_of_day_categories(self, minimal_trip):
        for hour, expected in [(2, "overnight"), (9, "morning"), (14, "afternoon"),
                                (18, "evening"), (22, "night")]:
            minimal_trip["tpep_pickup_datetime"] = f"2024-01-15 {hour:02d}:00:00"
            df = parse_timestamps(pd.DataFrame([minimal_trip]))
            df = derive_time_features(df)
            assert str(df["derived_time_of_day"].iloc[0]) == expected, f"Hour {hour}"


# ── Financial features ────────────────────────────────────────────────────────

class TestDeriveFinancialFeatures:
    def test_tip_rate_credit_card(self, minimal_trip):
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_financial_features(df)
        expected_tip_rate = round(3.10 / 15.50, 4)
        assert df["derived_tip_rate"].iloc[0] == pytest.approx(expected_tip_rate, abs=0.0001)

    def test_tip_rate_cash_is_zero(self, minimal_trip):
        minimal_trip["payment_type"] = 2
        minimal_trip["tip_amount"]   = 0.0
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_financial_features(df)
        assert df["derived_tip_rate"].iloc[0] == 0.0

    def test_fare_per_mile(self, minimal_trip):
        # fare=15.50, dist=3.2 → $/mi = 4.84
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_financial_features(df)
        assert df["derived_fare_per_mile"].iloc[0] == pytest.approx(15.50 / 3.2, abs=0.01)

    def test_zero_distance_doesnt_raise(self, minimal_trip):
        minimal_trip["trip_distance"] = 0.0
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_financial_features(df)
        assert df["derived_fare_per_mile"].iloc[0] == 0.0  # not NaN, not error


# ── Geo features ──────────────────────────────────────────────────────────────

class TestDeriveGeoFeatures:
    def test_jfk_trip_flagged(self, minimal_trip):
        minimal_trip["ratecodeid"]   = RATECODE_JFK
        minimal_trip["dolocationid"] = ZONE_JFK
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_geo_features(df)
        assert df["derived_is_jfk_trip"].iloc[0] == True
        assert df["derived_is_airport_trip"].iloc[0] == True

    def test_lga_airport_trip_flagged(self, minimal_trip):
        minimal_trip["dolocationid"] = ZONE_LGA
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_geo_features(df)
        assert df["derived_is_airport_trip"].iloc[0] == True

    def test_standard_trip_not_airport(self, minimal_trip):
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_geo_features(df)
        assert df["derived_is_airport_trip"].iloc[0] == False

    def test_same_zone_detected(self, minimal_trip):
        minimal_trip["pulocationid"] = 50
        minimal_trip["dolocationid"] = 50
        df = parse_timestamps(pd.DataFrame([minimal_trip]))
        df = derive_geo_features(df)
        assert df["derived_is_same_zone"].iloc[0] == True


# ── Outlier removal ───────────────────────────────────────────────────────────

class TestRemoveOutliers:
    def test_extreme_fare_quarantined(self, mixed_df):
        df = parse_timestamps(mixed_df)
        df = derive_time_features(df)
        df = derive_financial_features(df)
        df = derive_geo_features(df)
        # Inject obvious outlier
        df.loc[0, "fare_amount"] = 99999.0
        clean, outliers = remove_outliers(df)
        assert 0 not in clean.index or clean.loc[0, "fare_amount"] != 99999.0

    def test_valid_data_passes_through(self, mixed_df):
        df = parse_timestamps(mixed_df)
        df = derive_time_features(df)
        df = derive_financial_features(df)
        df = derive_geo_features(df)
        clean, outliers = remove_outliers(df)
        total = len(clean) + len(outliers)
        assert total == len(df)  # no rows lost

    def test_quarantine_has_reason_column(self, mixed_df):
        df = parse_timestamps(mixed_df)
        df = derive_time_features(df)
        df = derive_financial_features(df)
        df = derive_geo_features(df)
        df.loc[0, "fare_amount"] = 99999.0
        _, outliers = remove_outliers(df)
        if len(outliers) > 0:
            assert "_quarantine_reason" in outliers.columns


# ── End-to-end transform ─────────────────────────────────────────────────────

class TestTransform:
    def test_transform_returns_silver_result(self, mixed_df):
        result = transform(mixed_df)
        assert hasattr(result, "clean_df")
        assert hasattr(result, "quarantine_df")
        assert hasattr(result, "metrics")

    def test_pass_rate_over_95_pct(self, mixed_df):
        result = transform(mixed_df)
        assert result.pass_rate >= 0.95

    def test_derived_columns_present(self, mixed_df):
        result = transform(mixed_df)
        expected_cols = [
            "derived_trip_duration_min",
            "derived_is_rush_hour",
            "derived_tip_rate",
            "derived_fare_per_mile",
            "derived_is_airport_trip",
        ]
        for col in expected_cols:
            assert col in result.clean_df.columns, f"Missing: {col}"

    def test_metrics_have_expected_keys(self, mixed_df):
        result = transform(mixed_df)
        for key in ["rows_in", "rows_clean", "pass_rate", "avg_fare"]:
            assert key in result.metrics, f"Missing metric: {key}"
