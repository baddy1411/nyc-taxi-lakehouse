# NYC Taxi Lakehouse

Analytics engineering project for NYC TLC Yellow Taxi trip data. The repo implements a local lakehouse-style pipeline with ingestion, medallion transformations, data contracts, dbt marts, CI, and a Streamlit dashboard.

The goal is not to pretend this is a fully managed enterprise platform. It is a reproducible portfolio project that demonstrates the engineering judgment expected in a modern data engineering role: schema enforcement, idempotent processing, testable transformations, analytical modeling, data quality gates, and operational monitoring.

## Why This Project Is Relevant

Current data engineering hiring signals are moving away from simple ETL notebooks and toward production-minded systems. This project is designed to show:

- Batch ingestion from a public Parquet source with schema validation.
- Bronze, Silver, and Gold layers with clear ownership boundaries.
- Deterministic transformation functions with unit and integration tests.
- Data contracts on the Silver layer and dbt tests on analytical marts.
- DuckDB-first local development, with SQL models structured for warehouse migration.
- CI workflow covering linting, tests, dbt validation, and data quality checks.
- Business-facing Gold tables for finance, operations, and geospatial analysis.

## Architecture

```text
NYC TLC Parquet
      |
      v
Bronze: raw validated Parquet, partitioned by pickup date
      |
      v
Silver: typed, cleaned, enriched trips plus quarantine output
      |
      v
Gold: dimensional marts and pre-aggregated demand tables
      |
      v
Streamlit dashboard / dbt documentation / SLA checks
```

## Technology

| Layer | Tools | Purpose |
| --- | --- | --- |
| Ingestion | Python, Requests, PyArrow | Download/read TLC Parquet and validate source schema |
| Storage | Parquet | Columnar local lakehouse format for reproducible demos |
| Transformation | Pandas, DuckDB, dbt-duckdb | Python transforms for Silver, SQL models for marts |
| Quality | Pydantic-style schema validation, custom contract runner, dbt tests | Catch schema, range, nullability, and business-rule failures |
| Orchestration | Airflow DAG | Shows dependency design, retries, sensors, and SLA callbacks |
| CI/CD | GitHub Actions | Lint, unit tests, integration tests, dbt validation, contract checks |
| Dashboard | Streamlit, Plotly | Gold-layer analytics for portfolio demonstration |

## Project Structure

```text
ingestion/          Source extraction and schema validation
pipeline/           Bronze, Silver, and Gold transformation code
models/             dbt project for staging, intermediate, and marts
monitoring/         Data contracts and SLA checks
orchestration/      Airflow DAG for production-style scheduling
dashboard/          Streamlit analytics app
scripts/            Synthetic data generator for CI and demos
tests/              Unit and integration tests
.github/workflows/  CI pipeline
```

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Generate a reproducible local dataset
python scripts/generate_test_data.py --rows 10000

# Run the local pipeline
python run_pipeline.py --stage all --source data/raw/yellow_tripdata_2024_q1.parquet

# Run tests
pytest tests/unit -q
pytest tests/integration -q

# Validate dbt models
cd models
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Launch dashboard
streamlit run dashboard/app.py
```

## What To Highlight On A CV

Use one tight bullet, not a long list of tools:

> Built a reproducible NYC Taxi lakehouse pipeline processing 500K+ trips with PyArrow/Pandas/DuckDB, dbt marts, contract-based data quality checks, Airflow orchestration design, GitHub Actions CI, and Streamlit analytics dashboard.

If you want a more senior framing:

> Designed a production-style batch data platform for NYC TLC trip data: schema-validated ingestion, medallion transformations, quarantine handling, dimensional Gold marts, dbt tests, CI quality gates, SLA monitoring, and dashboard-ready analytics.

## Portfolio Notes

This repo is strongest when presented as a local, production-minded lakehouse simulation. Do not overclaim managed cloud deployment unless Terraform, object storage, catalog integration, secrets, and deployment scripts are actually added.

Recommended next upgrades:

- Add Docker Compose for one-command local execution.
- Add real Great Expectations or Soda Core suites if you want to claim those tools directly.
- Add Terraform for S3, Glue/Athena, and IAM if you want to claim cloud lakehouse deployment.
- Add a small benchmark report comparing raw Parquet scans versus Gold pre-aggregations.
- Publish dbt docs screenshots and dashboard screenshots in `docs/`.

## Dataset

The project targets NYC TLC Yellow Taxi trip records:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

For CI and demos, `scripts/generate_test_data.py` creates a statistically shaped synthetic dataset so tests do not depend on a large external download.
