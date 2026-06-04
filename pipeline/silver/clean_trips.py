"""
pipeline/silver/clean_trips.py
───────────────────────────────
Silver layer: Bronze validated Parquet → cleaned, enriched, analytics-ready.

Senior-level engineering decisions documented in-line:
  1. All transforms are PURE FUNCTIONS — no side effects, fully testable
  2. Business rules are NAMED constants — not magic numbers
  3. Quarantine path handles bad data; valid data is never silently mutated
  4. Column lineage is explicit: new derived columns prefixed with derived_
  5. Outlier strategy: IQR-fence with configurable multiplier, not hard cutoffs
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import duckdb
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ── Business constants (sourced from TLC data dictionary) ─────────────────────

# TLC zone IDs for airports
ZONE_JFK     = 132
ZONE_LGA     = 138
ZONE_EWR     = 1
AIRPORT_ZONES = {ZONE_JFK, ZONE_LGA, ZONE_EWR}

# Rate codes
RATECODE_JFK      = 2
RATECODE_NEWARK   = 3

# Congestion surcharge applies to trips originating in Manhattan (approx zones)
# Source: NYC Congestion Pricing Zone (central business district)
MANHATTAN_ZONES = set(range(4, 154))  # approximate; full list in dim_location

# Fare caps per TLC rules
MIN_FARE       = 3.00
MAX_FARE_STD   = 300.00
JFK_FLAT_FARE  = 70.00
NEWARK_MIN_FARE = 100.00

# Outlier fence multiplier (IQR × k)
IQR_K = 3.0

# Payment type labels
PAYMENT_LABELS = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}


# ── Transform functions ───────────────────────────────────────────────────────

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce canonical dtypes.
    Using nullable Int types (Int8, Int16) preserves nullability
    without the ambiguity of float-encoded integers.
    """
    return df.astype({
        "vendorid":             "Int8",
        "passenger_count":      "Int8",
        "ratecodeid":           "Int8",
        "pulocationid":         "Int16",
        "dolocationid":         "Int16",
        "payment_type":         "Int8",
        "fare_amount":          "float32",
        "extra":                "float32",
        "mta_tax":              "float32",
        "tip_amount":           "float32",
        "tolls_amount":         "float32",
        "improvement_surcharge":"float32",
        "total_amount":         "float32",
        "congestion_surcharge": "float32",
        "trip_distance":        "float32",
    })


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Parse datetimes, derive duration, enforce temporal sanity."""
    df = df.copy()
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"],  utc=False)
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], utc=False)
    df["derived_trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2).astype("float32")
    return df


def derive_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive business-relevant time features used by all downstream models.
    These avoid repeated EXTRACT calls in every SQL model.
    """
    df = df.copy()
    pu = df["tpep_pickup_datetime"]
    df["derived_pickup_hour"]       = pu.dt.hour.astype("Int8")
    df["derived_pickup_dow"]        = pu.dt.dayofweek.astype("Int8")  # 0=Mon
    df["derived_pickup_date"]       = pu.dt.date
    df["derived_pickup_month"]      = pu.dt.month.astype("Int8")
    df["derived_is_weekend"]        = (pu.dt.dayofweek >= 5).astype("bool")
    df["derived_is_rush_hour"]      = (
        ((pu.dt.hour >= 7) & (pu.dt.hour <= 9) & (pu.dt.dayofweek < 5)) |
        ((pu.dt.hour >= 16) & (pu.dt.hour <= 19) & (pu.dt.dayofweek < 5))
    ).astype("bool")
    df["derived_time_of_day"] = pd.cut(
        pu.dt.hour,
        bins=[-1, 5, 11, 16, 20, 23],
        labels=["overnight","morning","afternoon","evening","night"],
    )
    return df


def derive_financial_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived financial metrics used in the finance mart.
    All division protected against zero-division.
    """
    df = df.copy()

    # Revenue components
    df["derived_revenue_excl_tips"] = (
        df["fare_amount"] + df["extra"] + df["mta_tax"] +
        df["improvement_surcharge"] + df["tolls_amount"] + df["congestion_surcharge"]
    ).astype("float32")

    df["derived_tip_rate"] = (
        df["tip_amount"] / df["fare_amount"].replace(0, np.nan)
    ).fillna(0).round(4).astype("float32")

    df["derived_fare_per_mile"] = (
        df["fare_amount"] / df["trip_distance"].replace(0, np.nan)
    ).fillna(0).clip(0, 50).round(2).astype("float32")

    df["derived_fare_per_minute"] = (
        df["fare_amount"] / df["derived_trip_duration_min"].replace(0, np.nan)
    ).fillna(0).clip(0, 20).round(2).astype("float32")

    df["derived_speed_mph"] = (
        df["trip_distance"] / (df["derived_trip_duration_min"] / 60).replace(0, np.nan)
    ).fillna(0).clip(0, 80).round(1).astype("float32")

    # Payment label
    df["derived_payment_label"] = df["payment_type"].map(PAYMENT_LABELS).fillna("unknown")

    return df


def derive_geo_features(df: pd.DataFrame) -> pd.DataFrame:
    """Flag airport trips, congestion zone, and same-zone trips."""
    df = df.copy()
    df["derived_is_airport_trip"]    = (
        df["pulocationid"].isin(AIRPORT_ZONES) | df["dolocationid"].isin(AIRPORT_ZONES)
    ).astype("bool")
    df["derived_is_jfk_trip"]        = (df["ratecodeid"] == RATECODE_JFK).astype("bool")
    df["derived_is_newark_trip"]     = (df["ratecodeid"] == RATECODE_NEWARK).astype("bool")
    df["derived_is_same_zone"]       = (df["pulocationid"] == df["dolocationid"]).astype("bool")
    df["derived_in_congestion_zone"] = (df["pulocationid"].isin(MANHATTAN_ZONES)).astype("bool")
    return df


def remove_outliers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separate outliers into quarantine using IQR fence.
    Returns (clean_df, outlier_df).

    Senior engineering note: Hard cutoffs (e.g. fare > $500 = bad) encode
    assumptions that break silently when data distributions shift. IQR fences
    adapt to the actual distribution of each batch.

    Rate-code awareness: JFK ($70) and Newark ($108+) are flat-rate trips by
    TLC regulation — their fares legitimately exceed the standard IQR fence.
    We apply fare outlier logic only to rate_code == 1 (standard metered fare).
    Non-standard rate codes have their own validity rules.
    """
    outlier_mask = pd.Series(False, index=df.index)

    # ── Duration outlier: applies to all rate codes ─────────────────────────
    if "derived_trip_duration_min" in df.columns:
        s   = df["derived_trip_duration_min"].dropna()
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        lower = max(1.0, q1 - IQR_K * iqr)
        upper = q3 + IQR_K * iqr
        dur_outlier = (df["derived_trip_duration_min"] < lower) | (df["derived_trip_duration_min"] > upper)
        outlier_mask |= dur_outlier.fillna(False)

    # ── Distance outlier: applies to all rate codes ─────────────────────────
    if "trip_distance" in df.columns:
        s   = df["trip_distance"].dropna()
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        upper = q3 + IQR_K * iqr
        dist_outlier = (df["trip_distance"] < 0) | (df["trip_distance"] > upper)
        outlier_mask |= dist_outlier.fillna(False)

    # ── Fare outlier: ONLY for standard rate (code=1) ───────────────────────
    # JFK (2) = $70, Newark (3) = $108+ are regulated flat rates, not outliers
    if "fare_amount" in df.columns and "ratecodeid" in df.columns:
        standard_mask = df["ratecodeid"] == 1
        s   = df.loc[standard_mask, "fare_amount"].dropna()
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        lower = max(MIN_FARE, q1 - IQR_K * iqr)
        upper = q3 + IQR_K * iqr
        fare_outlier_std = standard_mask & ((df["fare_amount"] < lower) | (df["fare_amount"] > upper))
        outlier_mask |= fare_outlier_std.fillna(False)

    clean_df   = df[~outlier_mask].copy()
    outlier_df = df[outlier_mask].copy()
    if not outlier_df.empty:
        outlier_df["_quarantine_reason"] = "outlier_iqr_fence"

    logger.debug("Outlier removal: %d clean, %d quarantined", len(clean_df), len(outlier_df))
    return clean_df, outlier_df


@dataclass
class SilverResult:
    clean_df:     pd.DataFrame
    quarantine_df: pd.DataFrame
    metrics:      dict

    @property
    def pass_rate(self) -> float:
        total = len(self.clean_df) + len(self.quarantine_df)
        return len(self.clean_df) / total if total else 0.0


def transform(df: pd.DataFrame) -> SilverResult:
    """
    Full silver transform pipeline. Composable, ordered, logged.

    Call order matters:
      1. types → timestamps → time features (timestamps needed first)
      2. financial features (duration needed for fare/min)
      3. geo features (ratecodeid needed)
      4. outlier removal (all derived cols available)
    """
    logger.info("Silver transform: %d rows input", len(df))
    n_in = len(df)

    df = cast_types(df)
    df = parse_timestamps(df)
    df = derive_time_features(df)
    df = derive_financial_features(df)
    df = derive_geo_features(df)

    clean_df, outlier_df = remove_outliers(df)

    metrics = {
        "rows_in":        n_in,
        "rows_clean":     len(clean_df),
        "rows_outlier":   len(outlier_df),
        "pass_rate":      round(len(clean_df) / n_in, 4) if n_in else 0,
        "avg_fare":       round(float(clean_df["fare_amount"].mean()), 2),
        "avg_distance":   round(float(clean_df["trip_distance"].mean()), 2),
        "avg_duration":   round(float(clean_df["derived_trip_duration_min"].mean()), 1),
        "avg_speed_mph":  round(float(clean_df["derived_speed_mph"].mean()), 1),
        "pct_airport":    round(clean_df["derived_is_airport_trip"].mean() * 100, 2),
        "pct_rush_hour":  round(clean_df["derived_is_rush_hour"].mean() * 100, 2),
        "pct_credit_card":round((clean_df["payment_type"] == 1).mean() * 100, 2),
    }

    logger.info(
        "Silver complete: %d clean (%.1f%%), %d outliers | "
        "avg_fare=$%.2f avg_dist=%.2f mi",
        metrics["rows_clean"], metrics["pass_rate"] * 100,
        metrics["rows_outlier"], metrics["avg_fare"], metrics["avg_distance"],
    )

    return SilverResult(clean_df, outlier_df, metrics)
