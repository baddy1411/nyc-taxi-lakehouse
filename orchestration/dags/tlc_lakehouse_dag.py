"""
orchestration/dags/tlc_lakehouse_dag.py
─────────────────────────────────────────
Production Airflow DAG for the NYC Taxi Lakehouse pipeline.

Design decisions:
  - Sensor-gated: waits for upstream file, doesn't blindly assume availability
  - Incremental: skips already-processed partitions (idempotent)
  - SLA callbacks: Slack alert if pipeline misses the 06:00 UTC SLA
  - Explicit task dependencies with clear failure isolation
  - Data quality gate: GE contract must pass before Gold is built
  - Separate tasks = separate retries = better debuggability
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

# ── Default task config ───────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                    "data-engineering",
    "depends_on_past":          True,          # don't run if yesterday failed
    "wait_for_downstream":      True,
    "email":                    ["data-alerts@yourcompany.com"],
    "email_on_failure":         True,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff":True,
    "max_retry_delay":          timedelta(minutes=30),
    "execution_timeout":        timedelta(hours=2),
}


# ── SLA miss callback ─────────────────────────────────────────────────────────

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called by Airflow when the pipeline hasn't completed by the SLA deadline.
    Sends Slack alert; could also page PagerDuty.
    """
    import logging
    logging.getLogger("airflow.task").warning(
        "SLA MISS: %s | Tasks: %s",
        dag.dag_id, [t.task_id for t in blocking_task_list]
    )
    # Production: send to Slack / PagerDuty here
    # slack_hook.send(f"🚨 SLA miss: {dag.dag_id}")


# ── Branch logic ─────────────────────────────────────────────────────────────

def check_already_processed(**context) -> str:
    """
    Check if this partition was already successfully processed.
    If yes, skip to 'already_done'. This makes the DAG idempotent.
    """
    from pathlib import Path
    ds = context["ds"]  # YYYY-MM-DD
    year, month, day = ds.split("-")
    silver_marker = Path(
        f"data/lakehouse/silver/year={year}/month={month}/day={day}/_SUCCESS"
    )
    if silver_marker.exists():
        return "already_done"
    return "extract_bronze"


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="tlc_lakehouse_daily",
    description="NYC TLC Yellow Taxi: raw → Bronze → Silver → Gold → Quality check",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *",   # 04:00 UTC — TLC publishes ~03:00 UTC
    start_date=datetime(2024, 1, 2),
    catchup=True,                    # Backfill from start_date
    max_active_runs=3,               # Up to 3 concurrent backfill runs
    sla_miss_callback=sla_miss_callback,
    tags=["lakehouse", "tlc", "production"],
    doc_md="""
## NYC TLC Lakehouse DAG

Runs nightly at 04:00 UTC. Processes the previous day's trip data.

### Pipeline
1. **File sensor** — waits for TLC to publish the file
2. **Branch** — skip if partition already processed (idempotent)
3. **Extract → Bronze** — download + schema validate + partition write
4. **Silver transform** — clean, enrich, outlier-remove
5. **GE contract** — runs Silver data contract (gate: fails pipeline if contract broken)
6. **dbt run** — build Gold star schema
7. **dbt test** — enforce data contracts on Gold
8. **SLA check** — verify freshness + volume

### SLA
Pipeline must complete by 06:00 UTC (2h after start).
    """,
) as dag:

    # ── T0: Sensor — wait for TLC file ────────────────────────────────────
    wait_for_source = FileSensor(
        task_id="wait_for_tlc_file",
        filepath="data/raw/yellow_tripdata_{{ ds_nodash[:6] }}.parquet",
        poke_interval=300,       # check every 5 min
        timeout=7200,            # give up after 2 hours
        mode="reschedule",       # release worker slot while waiting
        sla=timedelta(hours=1),
    )

    # ── T1: Branch — skip already-processed partitions ────────────────────
    branch = BranchPythonOperator(
        task_id="check_already_processed",
        python_callable=check_already_processed,
    )

    already_done = EmptyOperator(task_id="already_done")

    # ── T2: Bronze extraction + schema validation ─────────────────────────
    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command=(
            "python -m ingestion.extractors.tlc_extractor "
            "--local-file data/raw/yellow_tripdata_{{ ds_nodash[:6] }}.parquet "
            "--output-dir data/lakehouse "
            "| tee /tmp/extract_{{ ds }}.log"
        ),
        sla=timedelta(minutes=20),
    )

    # ── T3: Silver transform ──────────────────────────────────────────────
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            "python run_pipeline.py "
            "--stage silver "
            "--date {{ ds }} "
            "| tee /tmp/silver_{{ ds }}.log"
        ),
        sla=timedelta(minutes=30),
    )

    # ── T4: Great Expectations quality gate ──────────────────────────────
    ge_contract = BashOperator(
        task_id="ge_data_contract",
        bash_command=(
            "python monitoring/contracts/silver_contract.py "
            "--path data/lakehouse/silver "
            "--fail-on-error"
        ),
        sla=timedelta(minutes=10),
        # This task MUST succeed before Gold is built
        # If it fails, downstream Gold tasks are skipped
    )

    # ── T5: dbt Gold layer ────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd models && dbt run "
            "--target prod "
            "--select marts.finance marts.operations marts.geo "
            "--vars '{\"execution_date\": \"{{ ds }}\"}' "
            "--no-partial-parse "
        ),
        sla=timedelta(minutes=20),
    )

    # ── T6: dbt tests ─────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd models && dbt test --target prod",
        sla=timedelta(minutes=15),
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd models && dbt source freshness",
    )

    # ── T7: SLA monitoring ────────────────────────────────────────────────
    sla_check = BashOperator(
        task_id="sla_volume_check",
        bash_command=(
            "python monitoring/sla/sla_monitor.py "
            "--date {{ ds }} "
            "--min-rows 10000 "
            "--fail-on-breach"
        ),
    )

    # ── T8: Write success marker (enables idempotency check) ─────────────
    write_success = BashOperator(
        task_id="write_success_marker",
        bash_command=(
            "mkdir -p data/lakehouse/silver/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:] }} && "
            "touch data/lakehouse/silver/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:] }}/_SUCCESS"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── T9: Notify ────────────────────────────────────────────────────────
    notify = BashOperator(
        task_id="notify_completion",
        bash_command=(
            'echo "Pipeline {{ ds }} complete at $(date -u). '
            'Rows processed: $(cat /tmp/silver_{{ ds }}.log | grep rows_clean | tail -1)"'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependency graph ──────────────────────────────────────────────────
    (
        wait_for_source
        >> branch
        >> [already_done, extract_bronze]
    )
    (
        extract_bronze
        >> transform_silver
        >> ge_contract
        >> dbt_run
        >> dbt_test
        >> dbt_freshness
        >> sla_check
        >> write_success
        >> notify
    )
