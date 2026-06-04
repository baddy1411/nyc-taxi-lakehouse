"""
monitoring/sla/sla_monitor.py
───────────────────────────────
Programmatic SLA checks for the Gold layer.

Production data pipelines need two things beyond "did it run?":
  1. Freshness: is the data recent enough for downstream consumers?
  2. Volume:    is today's row count plausible vs. historical baseline?

This module provides both. It's called by the Airflow DAG after dbt runs,
and also useful as a standalone script during on-call debugging.

Run:
    python monitoring/sla/sla_monitor.py --date 2024-03-15
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SLACheck:
    name:        str
    passed:      bool
    actual:      str
    threshold:   str
    severity:    str   # "critical" | "warning"
    details:     str = ""


@dataclass
class SLAReport:
    run_date:  date
    checks:    list[SLACheck]

    @property
    def critical_failures(self) -> list[SLACheck]:
        return [c for c in self.checks if not c.passed and c.severity == "critical"]

    @property
    def warnings(self) -> list[SLACheck]:
        return [c for c in self.checks if not c.passed and c.severity == "warning"]

    def print(self) -> None:
        print(f"\n{'═'*65}")
        print(f"  SLA Report — {self.run_date}")
        print(f"  {len(self.checks) - len(self.critical_failures) - len(self.warnings)} passed / "
              f"{len(self.critical_failures)} critical / {len(self.warnings)} warnings")
        print(f"{'═'*65}")
        for c in self.checks:
            icon  = "✓" if c.passed else ("✗" if c.severity == "critical" else "⚠")
            color = "" if c.passed else f" [{c.severity.upper()}]"
            print(f"  {icon}  {c.name}{color}")
            if not c.passed:
                print(f"     actual={c.actual}  threshold={c.threshold}")
                if c.details:
                    print(f"     {c.details}")
        print()


def run_sla_checks(
    gold_path: Path,
    run_date:  date | None = None,
    min_rows:  int = 10_000,
    max_freshness_hours: float = 26.0,
    volume_drop_threshold: float = 0.20,   # alert if today is 20% below rolling avg
) -> SLAReport:
    """
    Run all SLA checks against the Gold layer.
    """
    import pandas as pd

    if run_date is None:
        run_date = date.today()

    checks: list[SLACheck] = []

    # ── 1. Gold files exist ─────────────────────────────────────────────
    required = ["fct_trips.parquet", "fct_hourly_demand.parquet",
                "dim_date.parquet", "dim_location.parquet"]
    for fname in required:
        path  = gold_path / fname
        exist = path.exists()
        checks.append(SLACheck(
            name=f"file_exists: {fname}",
            passed=exist,
            actual="present" if exist else "MISSING",
            threshold="present",
            severity="critical",
        ))

    fct_path = gold_path / "fct_trips.parquet"
    if not fct_path.exists():
        logger.error("fct_trips.parquet missing — cannot run volume/freshness checks")
        return SLAReport(run_date=run_date, checks=checks)

    fct = pd.read_parquet(fct_path)

    # ── 2. Row count ────────────────────────────────────────────────────
    total_rows = len(fct)
    checks.append(SLACheck(
        name="minimum_row_count",
        passed=total_rows >= min_rows,
        actual=f"{total_rows:,}",
        threshold=f">= {min_rows:,}",
        severity="critical",
        details="Pipeline may have failed or processed empty partition",
    ))

    # ── 3. Data freshness ───────────────────────────────────────────────
    latest_ts = pd.to_datetime(fct["pickup_ts"]).max()
    age_hours = (pd.Timestamp.now() - latest_ts).total_seconds() / 3600
    checks.append(SLACheck(
        name="data_freshness",
        passed=age_hours <= max_freshness_hours,
        actual=f"{age_hours:.1f}h old",
        threshold=f"<= {max_freshness_hours:.0f}h",
        severity="critical",
        details=f"Latest event: {latest_ts.strftime('%Y-%m-%d %H:%M')}",
    ))

    # ── 4. Date coverage ────────────────────────────────────────────────
    fct_dates = fct["pickup_date"] if "pickup_date" in fct.columns else pd.to_datetime(fct["pickup_ts"]).dt.date
    n_dates   = pd.to_datetime(fct_dates).nunique()
    checks.append(SLACheck(
        name="date_coverage",
        passed=n_dates >= 7,
        actual=f"{n_dates} distinct dates",
        threshold=">= 7 dates",
        severity="warning",
        details="Fewer than 7 days of data may indicate partial processing",
    ))

    # ── 5. No future timestamps ─────────────────────────────────────────
    future = (pd.to_datetime(fct["pickup_ts"]) > pd.Timestamp.now() + pd.Timedelta(hours=1)).sum()
    checks.append(SLACheck(
        name="no_future_timestamps",
        passed=future == 0,
        actual=f"{future:,} future rows",
        threshold="0",
        severity="critical" if future > 100 else "warning",
    ))

    # ── 6. Revenue sanity ───────────────────────────────────────────────
    avg_fare = fct["fare_amount"].mean()
    checks.append(SLACheck(
        name="avg_fare_sanity ($12–$50)",
        passed=12.0 <= avg_fare <= 50.0,
        actual=f"${avg_fare:.2f}",
        threshold="$12–$50",
        severity="warning",
        details="Outside range may indicate schema mapping error",
    ))

    # ── 7. Null rate on critical columns ───────────────────────────────
    for col in ["fare_amount", "pickup_ts", "pickup_location_id"]:
        if col not in fct.columns:
            continue
        null_pct = fct[col].isna().mean()
        checks.append(SLACheck(
            name=f"null_rate: {col}",
            passed=null_pct < 0.01,
            actual=f"{null_pct:.2%}",
            threshold="< 1%",
            severity="critical",
        ))

    # ── 8. Volume trend (compare to rolling 7-day avg) ──────────────────
    # Convert pickup dates to daily counts
    fct_copy = fct.copy()
    fct_copy["_date"] = pd.to_datetime(fct_copy["pickup_ts"]).dt.date
    daily = fct_copy.groupby("_date").size().reset_index(name="count").sort_values("_date")

    if len(daily) >= 7:
        rolling_avg = daily["count"].rolling(7).mean().iloc[-1]
        today_count = daily["count"].iloc[-1] if len(daily) > 0 else 0
        drop_pct    = (rolling_avg - today_count) / rolling_avg if rolling_avg > 0 else 0

        checks.append(SLACheck(
            name="volume_trend (vs 7d rolling avg)",
            passed=drop_pct < volume_drop_threshold,
            actual=f"{today_count:,} today vs {rolling_avg:,.0f} rolling avg ({drop_pct:+.1%})",
            threshold=f"< {volume_drop_threshold:.0%} drop",
            severity="warning",
        ))

    return SLAReport(run_date=run_date, checks=checks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-path",    type=Path, default=Path("data/lakehouse/gold"))
    parser.add_argument("--date",         type=str,  default=None)
    parser.add_argument("--min-rows",     type=int,  default=10_000)
    parser.add_argument("--fail-on-breach", action="store_true",
                        help="Exit 1 if any CRITICAL check fails")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()

    report = run_sla_checks(
        gold_path=args.gold_path,
        run_date=run_date,
        min_rows=args.min_rows,
    )
    report.print()

    if args.fail_on_breach and report.critical_failures:
        logger.error("%d critical SLA breaches", len(report.critical_failures))
        sys.exit(1)
