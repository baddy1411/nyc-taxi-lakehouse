"""
ingestion/extractors/tlc_extractor.py
──────────────────────────────────────
Downloads NYC TLC Yellow Taxi Parquet files from the official source
(or reads local files for dev) and validates them against the schema contract.

Real data URL pattern:
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

Usage:
    # Download from real source
    python -m ingestion.extractors.tlc_extractor --year 2024 --month 01

    # Use local file (dev / CI)
    python -m ingestion.extractors.tlc_extractor --local-file data/raw/yellow_tripdata_2024_q1.parquet
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).parents[2]))
from ingestion.schemas.tlc_schema import validate_batch

logger = logging.getLogger(__name__)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Canonical column ordering for the 2024 schema
EXPECTED_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
    "pulocationid", "dolocationid", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]


def download_tlc(year: int, month: int, dest_dir: Path) -> Path:
    """Download one month of TLC data. Returns local path."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url      = f"{TLC_BASE_URL}/{filename}"
    dest     = dest_dir / filename
    dest_dir.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        logger.info("Already cached: %s", dest)
        return dest

    logger.info("Downloading %s ...", url)
    t0 = time.monotonic()

    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB chunks
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {pct:.1f}%  ({downloaded/1e6:.0f}/{total/1e6:.0f} MB)", end="", flush=True)

    elapsed = time.monotonic() - t0
    size_mb = dest.stat().st_size / 1e6
    logger.info("\n✓ Downloaded %s (%.0f MB) in %.1fs", filename, size_mb, elapsed)
    return dest


def extract(
    source_path: Path,
    output_dir: Path,
    batch_size: int = 100_000,
) -> dict:
    """
    Read Parquet, validate schema, write Bronze (valid) + quarantine partitions.

    Returns extraction metadata dict.
    """
    logger.info("Extracting: %s", source_path)

    # --- Read with PyArrow (row-group level parallelism) ---
    pf = pq.ParquetFile(source_path)
    total_rows = pf.metadata.num_rows
    logger.info("Source: %d rows, %d row groups, %.0f MB",
                total_rows, pf.metadata.num_row_groups,
                source_path.stat().st_size / 1e6)

    # Check schema compatibility
    source_cols = {c.lower() for c in pf.schema_arrow.names}
    expected    = set(EXPECTED_COLUMNS)
    missing = expected - source_cols
    extra   = source_cols - expected
    if missing:
        logger.warning("Missing columns in source: %s", missing)
    if extra:
        logger.debug("Extra columns (will be kept): %s", extra)

    bronze_path      = output_dir / "bronze"
    quarantine_path  = output_dir / "quarantine"
    bronze_path.mkdir(parents=True, exist_ok=True)
    quarantine_path.mkdir(parents=True, exist_ok=True)

    total_valid  = 0
    total_bad    = 0
    all_errors: dict[str, int] = {}

    for batch in pf.iter_batches(batch_size=batch_size):
        import pandas as pd
        df = batch.to_pandas()

        # Normalise column names to lower
        df.columns = [c.lower() for c in df.columns]

        # Validate
        result = validate_batch(df)
        total_valid += len(result.valid_df)
        total_bad   += len(result.quarantine_df)

        for k, v in result.error_counts.items():
            all_errors[k] = all_errors.get(k, 0) + v

        # Write valid → bronze (partitioned by pickup date)
        if not result.valid_df.empty:
            _write_partitioned(result.valid_df, bronze_path, "tpep_pickup_datetime")

        # Write invalid → quarantine
        if not result.quarantine_df.empty:
            result.quarantine_df.to_parquet(
                quarantine_path / f"quarantine_{total_bad}.parquet",
                index=False
            )

    pass_rate = total_valid / (total_valid + total_bad) if (total_valid + total_bad) > 0 else 0
    meta = {
        "source": str(source_path),
        "total_rows":    total_valid + total_bad,
        "valid_rows":    total_valid,
        "quarantined":   total_bad,
        "pass_rate":     round(pass_rate, 4),
        "error_summary": dict(sorted(all_errors.items(), key=lambda x: -x[1])[:10]),
    }
    logger.info(
        "Extraction complete: %d valid (%.1f%%), %d quarantined",
        total_valid, pass_rate * 100, total_bad
    )
    return meta


def _write_partitioned(df, base_path: Path, dt_col: str) -> None:
    """Write DataFrame partitioned by year/month/day."""
    import pandas as pd
    df = df.copy()
    dt = pd.to_datetime(df[dt_col])
    df["_year"]  = dt.dt.year
    df["_month"] = dt.dt.month
    df["_day"]   = dt.dt.day

    for (y, m, d), group in df.groupby(["_year", "_month", "_day"]):
        part_dir = base_path / f"year={y}" / f"month={m:02d}" / f"day={d:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = part_dir / "data.parquet"
        # Append-safe: don't overwrite, generate unique file names
        if out.exists():
            import uuid
            out = part_dir / f"data_{uuid.uuid4().hex[:8]}.parquet"
        group.drop(columns=["_year","_month","_day"]).to_parquet(out, index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--year",       type=int, default=2024)
    parser.add_argument("--month",      type=int, default=1)
    parser.add_argument("--local-file", type=Path, help="Skip download, use local Parquet")
    parser.add_argument("--output-dir", type=Path, default=Path("data/lakehouse"))
    args = parser.parse_args()

    if args.local_file:
        source = args.local_file
    else:
        source = download_tlc(args.year, args.month, Path("data/raw"))

    meta = extract(source, args.output_dir)
    import json
    print(json.dumps(meta, indent=2))
