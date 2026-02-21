# airflow/dags/pipeline_orchestrator.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State  # <-- add this

default_args = {"retries": 0}

# Toggle this to True if you want WHOIS -> DNS sequentially for lower resource spikes
SERIALIZE_LOOKUPS = False

with DAG(
    dag_id="pipeline_orchestrator",
    description="Daily pipeline: bronze → silver → union → lookups (+ MISP side branch).",
    start_date=datetime(2025, 11, 3),
    schedule="30 2 * * *",           # run the WHOLE pipeline at 02:30 UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    tags=["orchestration", "graph"],
) as dag:

    # Use UTC-normalized values; children expect naive UTC strings.
    ds_utc = "{{ data_interval_end.in_timezone('UTC').to_date_string() }}"
    ts_utc = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%d %H:%M:%S') }}"

    start = EmptyOperator(task_id="start")

    # ----- Bronze (ingest) -----
    with TaskGroup("bronze") as bronze:
        openphish_bronze = TriggerDagRunOperator(
            task_id="openphish_bronze",
            trigger_dag_id="openphish_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            # deferrable=True,  # optional later
            deferrable=False,   # <-- stability first
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )
        urlhaus_bronze = TriggerDagRunOperator(
            task_id="urlhaus_bronze",
            trigger_dag_id="urlhaus_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )

    # ----- Silver (normalize) -----
    with TaskGroup("silver") as silver:
        openphish_silver = TriggerDagRunOperator(
            task_id="openphish_silver",
            trigger_dag_id="openphish_silver",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )
        urlhaus_silver = TriggerDagRunOperator(
            task_id="urlhaus_silver",
            trigger_dag_id="urlhaus_silver",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )

    # ----- Union (merge silvers) -----
    union = TriggerDagRunOperator(
        task_id="labels_union",
        trigger_dag_id="labels_union",
        conf={"ds": ds_utc, "ts": ts_utc},
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=False,
        poke_interval=60,
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED],
    )

    # ----- Lookups (depend on union) -----
    with TaskGroup("lookups") as lookups:
        whois = TriggerDagRunOperator(
            task_id="whois_rdap",
            trigger_dag_id="whois_rdap_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,          # was True; turn off to avoid deferral flakiness
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )
        dns_geo = TriggerDagRunOperator(
            task_id="dns_ip_geo",
            trigger_dag_id="dns_ip_geo_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )

        if SERIALIZE_LOOKUPS:
            whois >> dns_geo  # run WHOIS then DNS to lower resource spikes

    # ----- MISP side branch (independent) -----
    with TaskGroup("misp") as misp:
        misp_osint = TriggerDagRunOperator(
            task_id="misp_osint",
            trigger_dag_id="misp_osint_ingest",
            conf={"ds": ds_utc, "ts": ts_utc},
            wait_for_completion=True,
            reset_dag_run=True,
            deferrable=False,
            poke_interval=60,
            allowed_states=[State.SUCCESS],
            failed_states=[State.FAILED],
        )

    end = EmptyOperator(task_id="end")

    # Graph
    start >> bronze >> silver >> union >> lookups
    start >> misp
    [lookups, misp] >> end
