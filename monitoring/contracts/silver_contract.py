"""
monitoring/contracts/silver_contract.py
─────────────────────────────────────────
Programmatic Great Expectations suite for the Silver layer.

A "data contract" is the formal agreement between producers and consumers
about what data looks like. This file IS the contract — version-controlled,
reviewed like code, enforced in CI.

Why GE over dbt tests alone?
  dbt tests run on Gold (post-transform). GE runs on Silver (pre-transform).
  Catching problems closer to the source means less corrupt data propagating.

Run:
    python monitoring/contracts/silver_contract.py --path data/lakehouse/silver/
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ExpectationResult:
    name:    str
    column:  str | None
    passed:  bool
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SuiteResult:
    suite_name:   str
    results:      list[ExpectationResult] = field(default_factory=list)
    rows_checked: int = 0

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def success_rate(self) -> float:
        return self.passed / len(self.results) if self.results else 0.0

    def print_report(self) -> None:
        print(f"\n{'═'*60}")
        print(f"  Data Contract: {self.suite_name}")
        print(f"  Rows checked : {self.rows_checked:,}")
        print(f"  Results      : {self.passed} passed / {self.failed} failed")
        print(f"{'═'*60}")
        for r in self.results:
            icon = "✓" if r.passed else "✗"
            col  = f"[{r.column}]" if r.column else ""
            print(f"  {icon}  {r.name} {col}")
            if not r.passed:
                print(f"       → {r.details}")
        print()


# ── Individual expectation functions ──────────────────────────────────────────

def expect_column_not_null(df: pd.DataFrame, column: str, max_null_pct: float = 0.0) -> ExpectationResult:
    null_pct = df[column].isna().mean()
    passed   = null_pct <= max_null_pct
    return ExpectationResult(
        name=f"expect_column_not_null (max_null={max_null_pct:.0%})",
        column=column, passed=passed,
        details={"null_pct": round(null_pct, 4), "threshold": max_null_pct}
    )


def expect_column_values_in_set(df: pd.DataFrame, column: str, allowed: set) -> ExpectationResult:
    bad = df[column].dropna()
    bad = bad[~bad.isin(allowed)]
    passed = len(bad) == 0
    return ExpectationResult(
        name="expect_column_values_in_set",
        column=column, passed=passed,
        details={"unexpected_count": len(bad), "unexpected_sample": bad.unique()[:5].tolist()}
    )


def expect_column_values_between(
    df: pd.DataFrame, column: str,
    min_val: float | None = None, max_val: float | None = None,
    max_violation_pct: float = 0.001
) -> ExpectationResult:
    s = df[column].dropna()
    violations = pd.Series(False, index=s.index)
    if min_val is not None:
        violations |= (s < min_val)
    if max_val is not None:
        violations |= (s > max_val)
    vio_pct = violations.mean()
    passed  = vio_pct <= max_violation_pct
    return ExpectationResult(
        name=f"expect_column_values_between [{min_val}, {max_val}]",
        column=column, passed=passed,
        details={
            "violation_pct": round(vio_pct, 4),
            "threshold": max_violation_pct,
            "min": float(s.min()), "max": float(s.max()),
        }
    )


def expect_column_mean_between(df, column, min_mean=None, max_mean=None) -> ExpectationResult:
    mean = float(df[column].mean())
    passed = (min_mean is None or mean >= min_mean) and (max_mean is None or mean <= max_mean)
    return ExpectationResult(
        name=f"expect_column_mean_between [{min_mean}, {max_mean}]",
        column=column, passed=passed,
        details={"actual_mean": round(mean, 4)}
    )


def expect_column_pair_temporal(df, pickup_col, dropoff_col, max_violation_pct=0.0) -> ExpectationResult:
    bad = df[dropoff_col] <= df[pickup_col]
    vio_pct = bad.mean()
    return ExpectationResult(
        name=f"expect_temporal_order ({pickup_col} < {dropoff_col})",
        column=None, passed=vio_pct <= max_violation_pct,
        details={"violation_pct": round(vio_pct, 4)}
    )


def expect_row_count_between(df, min_rows=None, max_rows=None) -> ExpectationResult:
    n = len(df)
    passed = (min_rows is None or n >= min_rows) and (max_rows is None or n <= max_rows)
    return ExpectationResult(
        name=f"expect_row_count_between [{min_rows}, {max_rows}]",
        column=None, passed=passed,
        details={"actual_count": n}
    )


def expect_no_duplicate_trips(df, cols=None) -> ExpectationResult:
    if cols is None:
        cols = ["vendorid", "tpep_pickup_datetime", "pulocationid"]
    available = [c for c in cols if c in df.columns]
    dupes = df.duplicated(subset=available).sum()
    return ExpectationResult(
        name=f"expect_no_duplicates ({available})",
        column=None, passed=dupes == 0,
        details={"duplicate_count": int(dupes)}
    )


def expect_tip_zero_for_cash(df) -> ExpectationResult:
    """Cash payments (type 2) should not have tip_amount > 0."""
    cash_with_tip = ((df["payment_type"] == 2) & (df["tip_amount"] > 0)).sum()
    return ExpectationResult(
        name="expect_tip_zero_for_cash (business rule)",
        column=None, passed=cash_with_tip == 0,
        details={"cash_trips_with_tip": int(cash_with_tip)}
    )


# ── The full suite ──────────────────────────────────────────────────────────

def run_silver_suite(df: pd.DataFrame) -> SuiteResult:
    """
    Runs the complete Silver data contract.
    All expectations are documented with the business reason.
    """
    suite = SuiteResult(suite_name="silver.yellow_trips", rows_checked=len(df))
    E = suite.results.append   # shorthand

    # ── Volume ─────────────────────────────────────────────────────────────
    E(expect_row_count_between(df, min_rows=1000))

    # ── Temporal ───────────────────────────────────────────────────────────
    E(expect_column_not_null(df, "tpep_pickup_datetime"))
    E(expect_column_not_null(df, "tpep_dropoff_datetime"))
    E(expect_column_pair_temporal(df, "tpep_pickup_datetime", "tpep_dropoff_datetime"))

    # ── Geography ──────────────────────────────────────────────────────────
    E(expect_column_values_between(df, "pulocationid", 1, 265))
    E(expect_column_values_between(df, "dolocationid", 1, 265))

    # ── Fares ──────────────────────────────────────────────────────────────
    E(expect_column_values_between(df, "fare_amount",   3.0,  350.0, max_violation_pct=0.001))
    E(expect_column_values_between(df, "tip_amount",    0.0,  None))
    E(expect_column_values_between(df, "total_amount",  3.0,  None))
    E(expect_column_values_between(df, "trip_distance", 0.0,  200.0, max_violation_pct=0.001))

    # Average fare should be $12–$50 for standard NYC taxi trips
    E(expect_column_mean_between(df, "fare_amount", 12.0, 50.0))

    # ── Categorical ────────────────────────────────────────────────────────
    E(expect_column_values_in_set(df, "vendorid",     {1, 2}))
    E(expect_column_values_in_set(df, "payment_type", {1, 2, 3, 4, 5, 6}))

    # ── Business rules ─────────────────────────────────────────────────────
    E(expect_tip_zero_for_cash(df))
    E(expect_no_duplicate_trips(df))

    # ── Derived columns (if silver transform has run) ───────────────────────
    if "derived_trip_duration_min" in df.columns:
        E(expect_column_values_between(df, "derived_trip_duration_min", 1.0, 300.0, max_violation_pct=0.001))
    if "derived_speed_mph" in df.columns:
        E(expect_column_values_between(df, "derived_speed_mph", 0.0, 80.0, max_violation_pct=0.005))

    return suite


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()

    import glob
    files = list(args.path.rglob("*.parquet"))
    if not files:
        logger.error("No Parquet files found in %s", args.path)
        sys.exit(1)

    df = pd.read_parquet(files[0])
    logger.info("Loaded %d rows from %s", len(df), files[0])

    result = run_silver_suite(df)
    result.print_report()

    if args.fail_on_error and result.failed > 0:
        sys.exit(1)
