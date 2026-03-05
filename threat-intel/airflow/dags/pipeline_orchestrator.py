# airflow/dags/pipeline_orchestrator.py
# -----------------------------------------------------------------------
# Master orchestrator for the daily Medallion pipeline.
#
# KEY FIX (2026-03-05): The Airflow scheduler's zombie_task_threshold
# defaults to 300s (5 min). TriggerDagRunOperator tasks that poll
# wait_for_completion=True look idle to the heartbeat monitor and get
# killed as "zombies" after 5 min — even when the child DAG is still
# running fine. This was the #1 cause of the cascade failures.
#
# Solution: Set AIRFLOW__SCHEDULER__SCHEDULER_ZOMBIE_TASK_THRESHOLD=86400
# in docker-compose AND use poke_interval=30 so the poller heartbeats
# frequently enough to never be mistaken for a zombie.
# -----------------------------------------------------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State

default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline_orchestrator",
    description="Daily pipeline: bronze → silver → union → lookups (+ MISP side branch).",
    start_date=datetime(2025, 11, 3),
    schedule="30 2 * * *",           # run the WHOLE pipeline at 02:30 UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args=default_args,
    tags=["orchestration", "graph"],
) as dag:

    # Use UTC-normalized values; children expect naive UTC strings.
    ds_utc = "{{ data_interval_end.in_timezone('UTC').to_date_string() }}"
    ts_utc = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%d %H:%M:%S') }}"

    # Common kwargs for every TriggerDagRunOperator — short poke_interval
    # keeps the task heartbeating so the scheduler never marks it as a zombie.
    TRIGGER_DEFAULTS = dict(
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=False,
        poke_interval=30,          # heartbeat every 30s (was 60s)
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED],
    )

    start = EmptyOperator(task_id="start")

    # ----- MISP side branch (runs first, lightweight) -----
    with TaskGroup("misp") as misp:
        misp_osint = TriggerDagRunOperator(
            task_id="misp_osint",
            trigger_dag_id="misp_osint_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(hours=1),
            **TRIGGER_DEFAULTS,
        )

    # ----- Bronze (ingest) — sequential to limit RAM -----
    with TaskGroup("bronze") as bronze:
        openphish_bronze = TriggerDagRunOperator(
            task_id="openphish_bronze",
            trigger_dag_id="openphish_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(minutes=15),
            **TRIGGER_DEFAULTS,
        )
        urlhaus_bronze = TriggerDagRunOperator(
            task_id="urlhaus_bronze",
            trigger_dag_id="urlhaus_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(minutes=15),
            **TRIGGER_DEFAULTS,
        )
        openphish_bronze >> urlhaus_bronze

    # ----- Silver (normalize) — sequential -----
    with TaskGroup("silver") as silver:
        openphish_silver = TriggerDagRunOperator(
            task_id="openphish_silver",
            trigger_dag_id="openphish_silver",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(minutes=15),
            **TRIGGER_DEFAULTS,
        )
        urlhaus_silver = TriggerDagRunOperator(
            task_id="urlhaus_silver",
            trigger_dag_id="urlhaus_silver",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(minutes=15),
            **TRIGGER_DEFAULTS,
        )
        openphish_silver >> urlhaus_silver

    # ----- Union (merge silvers) -----
    union = TriggerDagRunOperator(
        task_id="labels_union",
        trigger_dag_id="labels_union",
        conf={"ds": ds_utc, "ts": ts_utc},
        execution_timeout=timedelta(minutes=15),
        **TRIGGER_DEFAULTS,
    )

    # ----- Lookups — WHOIS then DNS, sequential -----
    with TaskGroup("lookups") as lookups:
        whois = TriggerDagRunOperator(
            task_id="whois_rdap",
            trigger_dag_id="whois_rdap_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(hours=4),  # WHOIS can be slow
            **TRIGGER_DEFAULTS,
        )
        dns_geo = TriggerDagRunOperator(
            task_id="dns_ip_geo",
            trigger_dag_id="dns_ip_geo_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            execution_timeout=timedelta(hours=2),
            **TRIGGER_DEFAULTS,
        )
        whois >> dns_geo

    end = EmptyOperator(task_id="end")

    # Strictly linear graph: prevents Docker OOM from concurrent heavy jobs
    start >> misp >> bronze >> silver >> union >> lookups >> end
