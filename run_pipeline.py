#!/usr/bin/env python3
"""
run_pipeline.py
───────────────
Single entrypoint for the NYC Taxi Lakehouse pipeline.
Runs Bronze → Silver → Gold locally; each stage is idempotent.

Usage:
    python run_pipeline.py                        # full pipeline, default source
    python run_pipeline.py --stage silver         # only silver transform
    python run_pipeline.py --source data/raw/yellow_tripdata_2024_q1.parquet
    python run_pipeline.py --dry-run              # validate config, no writes
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# ── Logging setup ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

ROOT    = Path(__file__).parent
DATA    = ROOT / "data"
RAW     = DATA / "raw"
BRONZE  = DATA / "lakehouse" / "bronze"
SILVER  = DATA / "lakehouse" / "silver"
GOLD    = DATA / "lakehouse" / "gold"
QUARANTINE = DATA / "lakehouse" / "quarantine"


def banner(text: str) -> None:
    logger.info("─" * 55)
    logger.info("  %s", text)
    logger.info("─" * 55)


def run_bronze(source_path: Path, dry_run: bool = False) -> dict:
    """Stage 1: Schema-validate source → partitioned Bronze Parquet."""
    banner("STAGE 1 · BRONZE  (extract + schema validate)")
    if dry_run:
        logger.info("DRY RUN — skipping write")
        return {"status": "dry_run"}

    import pandas as pd
    from ingestion.schemas.tlc_schema import validate_batch

    df = pd.read_parquet(source_path)
    logger.info("Loaded %d rows from %s", len(df), source_path)

    # Sample validation (full Pydantic scan is slow; 10K is representative)
    sample = df.sample(min(10_000, len(df)), random_state=42)
    result = validate_batch(sample)
    logger.info("Schema validation: %s", result)

    if result.pass_rate < 0.90:
        logger.error("Pass rate %.1f%% below 90%% threshold — aborting", result.pass_rate * 100)
        sys.exit(1)

    # Write Bronze (partition year/month for Hive-compatible layout)
    BRONZE.mkdir(parents=True, exist_ok=True)
    import pyarrow.parquet as pq, pyarrow as pa
    df = df.copy()
    dt = pd.to_datetime(df["tpep_pickup_datetime"])
    df["year"]  = dt.dt.year.astype(str)
    df["month"] = dt.dt.month.apply(lambda x: f"{x:02d}")
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=str(BRONZE), partition_cols=["year", "month"])

    return {
        "source":     str(source_path),
        "rows":       len(df),
        "pass_rate":  result.pass_rate,
    }


def run_silver(source_path: Path, dry_run: bool = False) -> dict:
    """Stage 2: Bronze → cleaned, enriched, typed Silver Parquet."""
    banner("STAGE 2 · SILVER  (clean + enrich + validate)")
    t0 = time.monotonic()

    import pandas as pd
    from pipeline.silver.clean_trips import transform
    from monitoring.contracts.silver_contract import run_silver_suite

    df = pd.read_parquet(source_path)
    logger.info("Input: %d rows", len(df))

    result = transform(df)
    logger.info(
        "Transform: %d clean (%.1f%%), %d quarantined",
        result.metrics["rows_clean"],
        result.metrics["pass_rate"] * 100,
        result.metrics["rows_outlier"],
    )

    # Run GE contract — log warnings, don't abort (quarantine handles bad data)
    suite = run_silver_suite(result.clean_df)
    logger.info("GE contract: %d/%d passed", suite.passed, len(suite.results))
    for r in suite.results:
        if not r.passed:
            logger.warning("  GE FAIL: %s → %s", r.name, r.details)

    if not dry_run:
        SILVER.mkdir(parents=True, exist_ok=True)
        out = SILVER / "clean_trips.parquet"
        result.clean_df.to_parquet(out, index=False)
        logger.info("Wrote %s  (%.1f MB)", out, out.stat().st_size / 1e6)

        if not result.quarantine_df.empty:
            QUARANTINE.mkdir(parents=True, exist_ok=True)
            q_out = QUARANTINE / "silver_outliers.parquet"
            result.quarantine_df.to_parquet(q_out, index=False)
            logger.info("Quarantine: %d rows → %s", len(result.quarantine_df), q_out)

    elapsed = time.monotonic() - t0
    return {**result.metrics, "elapsed_s": round(elapsed, 2)}


def run_gold(source_path: Path, dry_run: bool = False) -> dict:
    """Stage 3: Silver → Star schema Gold tables."""
    banner("STAGE 3 · GOLD   (star schema + window analytics)")
    t0 = time.monotonic()

    import pandas as pd
    import duckdb
    from pipeline.gold.build_star_schema import (
        build_dim_date, build_dim_location, build_dim_vendor,
        build_fct_trips, build_fct_hourly_demand,
    )

    df = pd.read_parquet(source_path)
    con = duckdb.connect()

    tables = {
        "dim_date":          build_dim_date(),
        "dim_location":      build_dim_location(),
        "dim_vendor":        build_dim_vendor(),
        "fct_trips":         build_fct_trips(df, con),
        "fct_hourly_demand": build_fct_hourly_demand(build_fct_trips(df, con), con),
    }

    if not dry_run:
        GOLD.mkdir(parents=True, exist_ok=True)
        for name, tbl in tables.items():
            path = GOLD / f"{name}.parquet"
            tbl.to_parquet(path, index=False)
            logger.info("  %-22s %8d rows  (%.0f KB)", name, len(tbl), path.stat().st_size / 1024)

    elapsed = time.monotonic() - t0
    return {name: len(tbl) for name, tbl in tables.items()} | {"elapsed_s": round(elapsed, 2)}


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="NYC Taxi Lakehouse pipeline runner")
    parser.add_argument("--stage",    choices=["bronze", "silver", "gold", "all"],
                        default="all", help="Pipeline stage to run")
    parser.add_argument("--source",   type=Path,
                        default=RAW / "yellow_tripdata_2024_q1.parquet",
                        help="Source Parquet file")
    parser.add_argument("--date",     type=str, default=None,
                        help="Optional Airflow execution date (YYYY-MM-DD), used for logs/metadata")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Validate config without writing data")
    parser.add_argument("--json",     action="store_true",
                        help="Output results as JSON")
    args = parser.parse_args()

    if not args.source.exists():
        logger.error("Source file not found: %s", args.source)
        logger.error("Run: python ingestion/extractors/tlc_extractor.py --year 2024 --month 01")
        sys.exit(1)

    results = {}
    total_t0 = time.monotonic()

    if args.stage in ("bronze", "all"):
        results["bronze"] = run_bronze(args.source, args.dry_run)

    silver_input = args.source  # fallback if bronze not run
    if args.stage in ("silver", "all"):
        results["silver"] = run_silver(args.source, args.dry_run)
        silver_input = SILVER / "clean_trips.parquet"

    if args.stage in ("gold", "all"):
        gold_input = silver_input if (SILVER / "clean_trips.parquet").exists() else args.source
        results["gold"] = run_gold(gold_input, args.dry_run)

    total_elapsed = round(time.monotonic() - total_t0, 1)
    results["total_elapsed_s"] = total_elapsed

    banner(f"PIPELINE COMPLETE  ({total_elapsed}s)")
    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        logger.info("Results: %s", json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
