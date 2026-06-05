<div align="center">

# 🚕 NYC Taxi Lakehouse

**A production-grade, end-to-end local lakehouse for NYC TLC Yellow Taxi data**

[![CI](https://github.com/baddy1411/nyc-taxi-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/baddy1411/nyc-taxi-lakehouse/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow?logo=duckdb)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

*Raw trip records → Validated bronze → Enriched silver → Business-ready gold → Interactive command center*

</div>

---

## 📖 Table of Contents

- [Why This Project?](#-why-this-project)
- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Flow](#-data-flow)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Cleansing & Enrichment](#silver-layer--cleansing--enrichment)
  - [Gold Layer — Star Schema](#gold-layer--star-schema)
- [Data Quality Framework](#-data-quality-framework)
- [dbt Models](#-dbt-models)
- [Orchestration (Airflow)](#-orchestration-airflow)
- [Analytics Dashboard](#-analytics-dashboard)
- [Testing Strategy](#-testing-strategy)
- [CI/CD Pipeline](#-cicd-pipeline)
- [Quick Start](#-quick-start)
- [Make Targets Reference](#-make-targets-reference)
- [Pipeline CLI Reference](#-pipeline-cli-reference)
- [Design Decisions](#-design-decisions)
- [Roadmap](#-roadmap)

---

## 🎯 Why This Project?

Most data engineering tutorials stop at "read a CSV, write to Parquet." This project goes further — it implements the **full production pattern** a senior data engineer would build for a real analytical system:

| Concern | How it's handled here |
|---|---|
| **Idempotency** | Partition-level markers prevent double-processing |
| **Data Contracts** | Great Expectations suite gates Silver promotion |
| **Observability** | SLA monitor + Slack alerting on deadline miss |
| **Testability** | Pure-function transforms, 500K synthetic-data generator |
| **Scalability** | Arrow-native processing; cloud storage backends are drop-in |
| **Governance** | dbt schema tests + freshness checks on every Gold table |

The dataset is [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — roughly **3 billion rows** over 10+ years, making it a realistic scale problem even when run locally.

---

## 🏗 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NYC Taxi Lakehouse                               │
│                                                                         │
│  ┌──────────┐    ┌───────────────────────────────────────────────────┐  │
│  │ TLC API  │    │              Medallion Lakehouse                  │  │
│  │ (Source) │    │                                                   │  │
│  └────┬─────┘    │  ┌──────────┐   ┌──────────┐   ┌─────────────┐  │  │
│       │          │  │  BRONZE  │   │  SILVER  │   │    GOLD     │  │  │
│       │ PyArrow  │  │          │──▶│          │──▶│             │  │  │
│       └──────────┼─▶│  Raw     │   │  Clean   │   │  Star       │  │  │
│                  │  │  Parquet │   │  Enriched│   │  Schema     │  │  │
│                  │  │  by yr/mo│   │  Typed   │   │  (DuckDB)   │  │  │
│                  │  └──────────┘   └────┬─────┘   └──────┬──────┘  │  │
│                  │                      │                 │          │  │
│                  │               ┌──────▼──────┐         │          │  │
│                  │               │  Quarantine │         │          │  │
│                  │               │  (failed    │         │          │  │
│                  │               │   records)  │         │          │  │
│                  │               └─────────────┘         │          │  │
│                  └───────────────────────────────────────┘          │  │
│                                                                      │  │
│  ┌─────────────────────┐    ┌──────────────────────────────────────┐│  │
│  │  Apache Airflow DAG │    │   Streamlit Command Center           ││  │
│  │  (Nightly @ 04:00Z) │    │   KPIs │ Demand │ Revenue │ Fares   ││  │
│  └─────────────────────┘    └──────────────────────────────────────┘│  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Tool | Role |
|---|---|---|
| **Ingestion** | PyArrow + Requests | Schema-validated Parquet extraction |
| **Storage** | Local FS (S3/GCS/ABS ready) | Partitioned Parquet lake |
| **Transform** | Python (pure functions) | Bronze → Silver cleansing |
| **Modeling** | dbt-core + dbt-duckdb | Silver → Gold SQL models |
| **Analytics DB** | DuckDB | Vectorized in-process OLAP |
| **Data Quality** | Great Expectations + Pydantic | Contract enforcement |
| **Orchestration** | Apache Airflow 2.8 | Nightly DAG with SLA monitoring |
| **Dashboard** | Streamlit + Plotly | 7-panel command center |
| **Testing** | pytest + pytest-cov + pytest-xdist | Unit + integration test suites |
| **Linting** | Ruff + mypy | Style and type safety |
| **CI** | GitHub Actions | Smoke-test pipeline on every PR |

---

## 📁 Project Structure

```
nyc-taxi-lakehouse/
│
├── ingestion/                   # Source extraction & schema validation
│   ├── extractors/
│   │   └── tlc_extractor.py     # TLC API client, PyArrow schema enforcement
│   ├── loaders/                 # Bronze write path
│   └── schemas/
│       └── tlc_schema.py        # Canonical column types & constraints
│
├── pipeline/                    # Core transformation logic
│   ├── bronze/                  # Raw partitioned ingestion
│   ├── silver/
│   │   └── clean_trips.py       # ✨ Pure-function transforms (typed, testable)
│   └── gold/
│       └── build_star_schema.py # DuckDB star schema construction
│
├── models/                      # dbt analytics models
│   ├── sources/                 # Source declarations & freshness checks
│   ├── staging/
│   │   └── stg_yellow_trips.sql # Lightweight type casts from Silver
│   ├── intermediate/
│   │   └── int_trips_enriched.sql
│   └── marts/
│       ├── finance/             # fct_trips, revenue schema tests
│       ├── geo/                 # dim_location (263 NYC taxi zones)
│       └── operations/          # fct_hourly_demand
│
├── monitoring/
│   ├── contracts/
│   │   └── silver_contract.py   # Great Expectations suite (6 dimensions)
│   └── sla/
│       └── sla_monitor.py       # Deadline + volume alerting
│
├── orchestration/
│   └── dags/
│       └── tlc_lakehouse_dag.py # Airflow DAG: FileSensor → Bronze → Silver → GE → dbt → Alert
│
├── dashboard/
│   └── app.py                   # Streamlit command center (7 panels)
│
├── scripts/
│   └── generate_test_data.py    # 500K-row synthetic generator for CI/dev
│
├── tests/
│   ├── unit/                    # Fast (<1s) pure-function tests
│   └── integration/             # End-to-end pipeline tests
│
├── .github/workflows/ci.yml     # GitHub Actions CI
├── Makefile                     # Developer ergonomics
├── run_pipeline.py              # Unified CLI entrypoint
└── requirements.txt             # Pinned dependencies
```

---

## 🌊 Data Flow

### Bronze Layer — Raw Ingestion

```
TLC HTTPS endpoint
       │
       ▼  PyArrow schema validation
┌──────────────────────────────────┐
│  Schema gate: ≥90% rows valid?   │
│  ✗ No  → reject entire batch     │
│  ✓ Yes → write to Bronze         │
└──────────────────────┬───────────┘
                       ▼
  data/bronze/yellow/year=2024/month=01/
  └── part-0001.parquet   (columnar, snappy compressed)
```

**Key decisions:**
- Partition by `year/month` — enables predicate pushdown and incremental runs
- Schema validation at source — bad schemas never pollute the lake
- Idempotency marker written on success — re-runs skip already-processed partitions

---

### Silver Layer — Cleansing & Enrichment

All transforms are **pure functions** (no side effects, fully testable).

```
Bronze Parquet
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  1. Type casting   (Int8/Int16/Float32 precision)   │
│  2. Temporal parse (pickup/dropoff → datetime)      │
│  3. Time features  (hour, dow, rush_hour, tod_cat)  │
│  4. Financial KPIs (tip_rate, fare_per_mile, speed) │
│  5. Geo flags      (airport, JFK/Newark flat-rate,  │
│                     congestion zone, same-zone ride) │
│  6. Outlier gate   (adaptive IQR × 3.0 fence)       │
│     ⚠ JFK/Newark flat-rate fares are exempt         │
└──────────────────┬──────────────────────────────────┘
                   │
         ┌─────────┴──────────┐
         ▼                    ▼
    Clean records        Quarantine
    data/silver/         data/quarantine/
                         (for investigation)
```

**Derived columns added at Silver:**

| Column | Description |
|---|---|
| `trip_duration_min` | Dropoff − pickup in minutes |
| `hour_of_day` | 0–23 |
| `day_of_week` | 0=Mon … 6=Sun |
| `is_rush_hour` | Bool: 7–10 AM or 4–8 PM, Mon–Fri |
| `time_of_day` | `morning` / `afternoon` / `evening` / `night` |
| `tip_rate` | `tip_amount / fare_amount` |
| `fare_per_mile` | `fare_amount / trip_distance` |
| `fare_per_minute` | `fare_amount / trip_duration_min` |
| `speed_mph` | `trip_distance / (trip_duration_min / 60)` |
| `is_airport_trip` | Bool: pickup or dropoff at JFK/LGA/EWR |
| `is_flat_rate` | Bool: JFK or Newark regulated flat rate |
| `is_congestion_zone` | Bool: Manhattan below 96th St |
| `is_same_zone` | Bool: pickup zone = dropoff zone |

---

### Gold Layer — Star Schema

Built with DuckDB's vectorized window functions — no Python loops.

```
                    ┌───────────────┐
                    │  dim_date     │
                    │  (calendar)   │
                    └───────┬───────┘
                            │
┌───────────────┐    ┌──────▼───────┐    ┌───────────────┐
│  dim_vendor   │    │  fct_trips   │    │  dim_location │
│  (2 vendors)  ├────│  (one row    ├────│  (263 zones)  │
└───────────────┘    │   per trip)  │    └───────────────┘
                    └──────┬───────┘
                            │
                    ┌───────▼───────────┐
                    │  fct_hourly_demand │
                    │  (pre-aggregated  │
                    │   BI query cache) │
                    └───────────────────┘
```

**Surrogate keys** use integer hashes (not UUIDs) — faster joins, smaller storage.

**Pre-computed in `fct_trips`:**
- 7-day rolling average fare (DuckDB window function)
- Fare percentile rank by zone

**`fct_hourly_demand`** pre-aggregates the 95th percentile BI query pattern (trips/hour/zone), eliminating repeated full-table scans for dashboards.

---

## 🛡 Data Quality Framework

Quality is enforced in **two independent layers**:

### Layer 1 — Great Expectations (Silver Gate)

Six contract dimensions must pass before Silver data is promoted:

```
┌─────────────────────────────────────────────────────────┐
│              Great Expectations Suite                    │
│                                                         │
│  ✓ Volume       ≥ 1,000 rows, no duplicates            │
│  ✓ Temporal     pickup/dropoff non-null, dropoff > pickup│
│  ✓ Geographic   location_id ∈ [1, 265]                 │
│  ✓ Fare         $3–$350 (0.1% tolerance), tips ≥ 0     │
│  ✓ Distance     0–200 miles (0.1% tolerance)           │
│  ✓ Business     cash payments → tip_amount == 0        │
│                                                         │
│  Pipeline FAILS and alerts if any dimension fails       │
└─────────────────────────────────────────────────────────┘
```

### Layer 2 — dbt Schema Tests (Gold Gate)

Every Gold table has `schema.yml` tests: `not_null`, `unique`, `accepted_values`, `relationships`, and `dbt_utils.expression_is_true` for business rules.

```bash
make dbt-test      # Run all schema tests against Gold
make ge-check      # Run Great Expectations contract manually
make sla-check     # Check pipeline met 06:00 UTC SLA
```

---

## 📐 dbt Models

```
models/
├── sources/
│   └── sources.yml              # Silver Parquet declared as source
│                                # freshness threshold: warn >25h, error >48h
│
├── staging/
│   └── stg_yellow_trips.sql     # 1:1 with Silver; casts types, renames cols
│
├── intermediate/
│   └── int_trips_enriched.sql   # Joins location metadata, applies business logic
│
└── marts/
    ├── finance/
    │   ├── fct_trips.sql        # Grain: one trip. All facts + FK refs.
    │   └── schema.yml           # Tests: PK unique, fare not_null, etc.
    ├── geo/
    │   └── dim_location.sql     # 263 NYC taxi zones with borough + airport flags
    └── operations/
        └── fct_hourly_demand.sql # Grain: date × hour × zone. Pre-agg for BI.
```

```bash
make dbt-run       # Build all models
make dbt-test      # Assert schema contracts
make dbt-docs      # Generate + serve lineage docs in browser
```

---

## ⏰ Orchestration (Airflow)

The nightly DAG runs at **04:00 UTC** with an **SLA deadline of 06:00 UTC**.

```
┌──────────────┐
│  FileSensor  │  Polls for TLC source file (5-min interval, 2-hr timeout)
└──────┬───────┘
       │
┌──────▼───────┐
│ Idempotency  │  Already processed this partition? → skip branch
│   Branch     │
└──────┬───────┘
       │
┌──────▼───────┐
│ Extract      │  Download + Bronze write + schema validation
│ Bronze       │
└──────┬───────┘
       │
┌──────▼───────┐
│ Transform    │  Silver cleansing (pure functions) + quarantine
│ Silver       │
└──────┬───────┘
       │
┌──────▼───────┐
│ GE Contract  │  Great Expectations suite → FAIL if violations
│ Check        │
└──────┬───────┘
       │
┌──────▼───────┐
│  dbt run     │  Build Gold star schema
└──────┬───────┘
       │
┌──────▼───────┐
│  dbt test    │  Schema + freshness assertions
│  + freshness │
└──────┬───────┘
       │
┌──────▼───────┐
│ Volume check │  Minimum row threshold gate
└──────┬───────┘
       │
┌──────▼───────┐
│ Success      │  Write idempotency marker
│ marker       │
└──────────────┘

SLA miss @ 06:00 UTC → Slack alert callback fires automatically
Retry policy: 3 attempts, exponential backoff
```

---

## 📊 Analytics Dashboard

Launch with `make dashboard` → opens at `http://localhost:8501`

The Streamlit command center has **7 panels**:

| Panel | What it shows |
|---|---|
| **KPI Strip** | Total trips, gross revenue, avg fare, avg tip rate |
| **Hourly Demand** | Trip volume heatmap by hour of day |
| **Borough Revenue** | Bar chart: Manhattan / Brooklyn / Queens / Bronx / Staten Island |
| **Daily Trends** | Time-series: revenue and trip count over the selected window |
| **Payment Mix** | Donut: Credit card vs Cash vs No charge vs Dispute |
| **Speed & Congestion** | Avg speed by borough — proxy for traffic conditions |
| **Fare Distribution** | Histogram with IQR bounds and outlier annotation |

All panels are powered by DuckDB analytical queries against the Gold layer — sub-second response times locally.

---

## 🧪 Testing Strategy

```
tests/
├── unit/
│   └── test_silver_transforms.py   # Tests each pure-function transform in isolation
│                                   # Covers: type casting, outlier logic, geo flags,
│                                   # JFK flat-rate exemption, zero-division guards
└── integration/
    └── test_pipeline_e2e.py        # Full Bronze → Silver → Gold run on synthetic data
                                    # Asserts row counts, schema shape, no data loss
```

```bash
make test              # Full suite
make test-unit         # Fast unit tests only (< 1 second)
make test-integration  # Needs data (generate first)
make test-coverage     # HTML coverage report → htmlcov/index.html
```

**Synthetic data generator** (`scripts/generate_test_data.py`) produces 500K statistically realistic rows — correct distributions for fares, distances, tip rates, airport flags — so tests never depend on real TLC files being downloaded.

---

## 🔄 CI/CD Pipeline

Every push to `main`/`develop` and every pull request runs:

```yaml
Repository Health
  ├── ruff lint (undefined names: F821, F822, F823)
  ├── Python syntax compile check (all modules)
  └── Asset existence checks (README, SVG docs)

Pipeline Smoke Test  (depends on Health passing)
  ├── Install: duckdb, numpy, pandas, pyarrow, pydantic, pytest
  ├── Generate 2,000 synthetic rows
  ├── Run unit tests (pytest)
  ├── Execute Silver stage on synthetic data
  └── Validate Silver output against Great Expectations contract
```

Concurrency is managed — outdated runs are cancelled automatically when a newer commit pushes.

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- ~2 GB disk (for synthetic data + pipeline outputs)
- Optional: Airflow (for full orchestration)

### 1. Clone & Install

```bash
git clone https://github.com/baddy1411/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
make venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
make install
```

### 2. Generate Data

```bash
# Fast path — 500K synthetic rows (no internet required)
make data-generate

# Or download real TLC data (~500 MB, requires internet)
make data-download
```

### 3. Run the Pipeline

```bash
# Full Bronze → Silver → Gold
make pipeline

# Or stage by stage
python run_pipeline.py --stage bronze
python run_pipeline.py --stage silver
python run_pipeline.py --stage gold
```

### 4. Run dbt Models

```bash
make dbt-run       # Build Gold star schema via dbt
make dbt-test      # Assert all schema contracts
```

### 5. Launch the Dashboard

```bash
make dashboard     # Opens http://localhost:8501
```

### 6. Validate Quality

```bash
make ge-check      # Great Expectations contract
make sla-check     # SLA monitor
make test          # Full test suite
```

---

## 📋 Make Targets Reference

| Category | Target | Description |
|---|---|---|
| **Setup** | `make install` | Install all dependencies |
| | `make venv` | Create virtualenv |
| **Data** | `make data-generate` | 500K synthetic rows (fast, offline) |
| | `make data-download` | Real TLC data (~500 MB) |
| **Pipeline** | `make pipeline` | Full Bronze → Silver → Gold |
| | `make pipeline-silver` | Silver stage only |
| | `make pipeline-gold` | Gold stage only |
| **dbt** | `make dbt-run` | Build all models |
| | `make dbt-test` | Schema + freshness tests |
| | `make dbt-docs` | Serve lineage docs in browser |
| **Quality** | `make ge-check` | Great Expectations contract |
| | `make sla-check` | SLA deadline monitor |
| | `make lint` | Ruff linter |
| | `make typecheck` | mypy type check |
| **Tests** | `make test` | Full suite |
| | `make test-unit` | Fast unit tests |
| | `make test-integration` | E2E pipeline test |
| | `make test-coverage` | Coverage HTML report |
| **Dashboard** | `make dashboard` | Launch Streamlit |
| **Workflow** | `make dev` | generate + pipeline + test + validate |
| | `make ci` | CI workflow (generate + test + quality) |
| | `make clean` | Remove generated data and caches |

---

## 🔧 Pipeline CLI Reference

```bash
python run_pipeline.py [OPTIONS]

Options:
  --stage {bronze,silver,gold,all}   Which stage(s) to run (default: all)
  --dry-run                          Validate logic without writing outputs
  --json                             Emit metrics as machine-readable JSON
```

**Examples:**

```bash
# Validate without writing anything
python run_pipeline.py --dry-run

# Run only Silver and emit JSON metrics
python run_pipeline.py --stage silver --json

# Full pipeline with JSON output for monitoring integration
python run_pipeline.py --stage all --json
```

---

## 🧠 Design Decisions

### Why DuckDB over Spark?

For datasets up to ~100GB on a single machine, DuckDB's vectorized engine outperforms Spark on latency and operational complexity. This project is explicitly designed as a **local lakehouse** — Spark would be the swap-in at warehouse scale. DuckDB's `dbt-duckdb` adapter means dbt models run identically whether the target is DuckDB or Snowflake/BigQuery.

### Why pure functions in Silver?

Every transform in `clean_trips.py` takes a DataFrame in and returns a DataFrame out — no global state, no I/O. This makes every transformation independently testable with a 5-row fixture. The test suite runs in under a second.

### Why IQR × 3.0 instead of hard fare limits?

Hard limits (`fare > $300 is invalid`) fail on legitimate long-distance airport transfers. An adaptive IQR fence scales with the actual data distribution. The multiplier of 3.0 (wider than the standard 1.5) reduces false-positive quarantines for the heavy-tailed fare distribution.

### Why integer hash surrogate keys?

String UUIDs are 36 bytes each. Integer hashes are 8 bytes. On a 500K-row fact table with multiple FK joins, this difference is measurable in query time and storage. DuckDB's vectorized hash join benefits from fixed-width integer keys.

### Why pre-aggregate `fct_hourly_demand`?

Profiling showed that 95% of dashboard queries filter by `hour × zone`. Pre-aggregating eliminates repeated full-table scans — the dashboard panel that would take 800ms from `fct_trips` runs in 12ms from `fct_hourly_demand`.

### Cloud-ready by design

`requirements.txt` includes commented-out `boto3`, `google-cloud-storage`, and `azure-storage-blob`. The storage abstraction in the ingestion layer means switching from local FS to S3/GCS/ABS is a one-line config change.

---

## 🗺 Roadmap

- [ ] **Incremental dbt models** — `is_incremental()` flag to avoid full refreshes
- [ ] **Partition pruning** — Push date filters into Bronze reads via Hive-style partition spec
- [ ] **Great Expectations Cloud** — Send validation results to GE Cloud for historical tracking
- [ ] **DBT Semantic Layer** — Expose metrics via dbt's semantic layer for BI tool integration
- [ ] **Real-time tier** — Kafka → Bronze streaming path alongside the nightly batch
- [ ] **Cloud storage backend** — S3 / GCS swap-in via env config
- [ ] **Grafana dashboard** — Airflow metrics + pipeline SLA in Grafana

---

## 📄 License

MIT — see [LICENSE](LICENSE).

---

<div align="center">

**Built by [baddy1411](https://github.com/baddy1411)**

*If this project helped you, consider starring the repo ⭐*

</div>
