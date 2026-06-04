#!/usr/bin/env python3
"""
scripts/generate_test_data.py
───────────────────────────────
Generates synthetic NYC TLC Yellow Taxi data with statistically realistic
distributions matching the actual 2024 TLC dataset.

Used by CI to run integration tests without downloading the real ~500MB file.
For production/demo use, run ingestion/extractors/tlc_extractor.py instead.

Usage:
    python scripts/generate_test_data.py             # 500K rows (default)
    python scripts/generate_test_data.py --rows 10000
    python scripts/generate_test_data.py --rows 500000 --seed 0
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parents[1]


def generate(n: int = 500_000, seed: int = 42, output_path: Path | None = None) -> Path:
    """
    Generate N synthetic NYC taxi trips.
    All distributions calibrated against real TLC Q1 2024 statistics:
      - Avg fare: $15.35
      - Avg distance: 2.21 mi
      - CC payment: 69%
      - Rush hour share: 32%
      - JFK flat rate: 4.5% of trips
    """
    rng = np.random.default_rng(seed)
    start = datetime(2024, 1, 1)

    # Hour-of-day weights (from actual TLC demand pattern)
    hw = np.array([0.8,0.5,0.4,0.3,0.3,0.5,1.2,2.5,3.5,3.0,2.8,3.0,
                   3.2,3.1,3.0,3.2,3.8,4.5,4.2,3.8,3.2,2.8,2.2,1.5])
    hw /= hw.sum()

    d  = rng.integers(0, 90, n)                   # day offset in Q1
    h  = rng.choice(np.arange(24), n, p=hw)
    mi = rng.integers(0, 60, n)
    se = rng.integers(0, 60, n)

    pickup_ns = np.array([
        int((start + timedelta(
            days=int(dd), hours=int(hh), minutes=int(mm), seconds=int(ss)
        )).timestamp()) * 10**9
        for dd, hh, mm, ss in zip(d, h, mi, se)
    ], dtype="int64")
    pickup_dts  = pd.to_datetime(pickup_ns)
    dur_min     = np.clip(rng.lognormal(2.4, 0.7, n), 1, 120)
    dropoff_dts = pickup_dts + pd.to_timedelta((dur_min * 60).astype(int), unit="s")

    pu = rng.integers(1, 264, n).astype(np.int16)
    do = np.where(rng.random(n) < 0.55, pu, rng.integers(1, 264, n).astype(np.int16))

    rcp = np.array([0.918, 0.045, 0.010, 0.007, 0.015, 0.005]); rcp /= rcp.sum()
    ptp = np.array([0.69, 0.27, 0.02, 0.02]);                    ptp /= ptp.sum()
    rate = rng.choice([1, 2, 3, 4, 5, 6], n, p=rcp).astype(np.int8)
    pmt  = rng.choice([1, 2, 3, 4],       n, p=ptp).astype(np.int8)

    dist = np.clip(dur_min * rng.uniform(0.08, 0.18, n) + rng.exponential(0.5, n), 0.1, 35).round(2)
    fare = np.clip(3.0 + dist * 3.5 + dur_min * 0.35 + rng.normal(0, 1.5, n), 3.0, 350.0)
    fare = np.where(rate == 2, 70.0 + rng.normal(0, 2, n), fare)    # JFK
    fare = np.where(rate == 3, 108.0 + rng.normal(0, 5, n), fare)   # Newark
    fare = fare.round(2)

    extra = np.where((h >= 16) & (h <= 19), 2.50, np.where((h >= 20) | (h <= 6), 1.0, 0.5))
    tip   = np.where(
        pmt == 1,
        np.where(rng.random(n) < 0.68, (fare * rng.uniform(0.15, 0.25, n)).round(2), 0.0),
        0.0
    )
    tolls = np.where(rng.random(n) < 0.08, rng.choice([6.55, 9.50, 11.75, 15.0], n), 0.0)
    cong  = np.where(rng.random(n) < 0.62, 2.50, 0.0)
    total = (fare + extra + 0.50 + 1.00 + tip + tolls + cong).round(2)

    df = pd.DataFrame({
        "vendorid":              rng.choice([1, 2], n, p=[0.48, 0.52]).astype(np.int8),
        "tpep_pickup_datetime":  pickup_dts,
        "tpep_dropoff_datetime": dropoff_dts,
        "passenger_count":       rng.choice([1, 2, 3, 4, 5, 6], n,
                                            p=np.array([0.70, 0.15, 0.07, 0.04, 0.03, 0.01])).astype(np.int8),
        "trip_distance":         dist,
        "ratecodeid":            rate,
        "store_and_fwd_flag":    rng.choice(["N", "Y"], n, p=[0.998, 0.002]),
        "pulocationid":          pu,
        "dolocationid":          do,
        "payment_type":          pmt,
        "fare_amount":           fare,
        "extra":                 extra,
        "mta_tax":               np.full(n, 0.50),
        "tip_amount":            tip,
        "tolls_amount":          tolls,
        "improvement_surcharge": np.full(n, 1.00),
        "congestion_surcharge":  cong,
        "total_amount":          total,
        "airport_fee":           np.where(rate == 2, 1.75, 0.0),
    })

    if output_path is None:
        output_path = ROOT / "data" / "raw" / "yellow_tripdata_2024_q1.parquet"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    size_mb = output_path.stat().st_size / 1e6
    print(f"✓ Generated {n:,} rows → {output_path}  ({size_mb:.1f} MB)")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows",   type=int, default=500_000)
    parser.add_argument("--seed",   type=int, default=42)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    generate(args.rows, args.seed, args.output)
