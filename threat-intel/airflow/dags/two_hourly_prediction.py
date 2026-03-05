# airflow/dags/two_hourly_prediction.py
# -----------------------------------------------------------------------
# Triggers the full pipeline_orchestrator then runs predict_and_export.
#
# FIX: Added poke_interval=30 and allowed_states/failed_states so the
# TriggerDagRunOperator heartbeats frequently enough to avoid being
# killed as a zombie by the scheduler.
# -----------------------------------------------------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.state import State

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "two_hourly_prediction",
    default_args=default_args,
    description="Run ingestion, lookups, prediction & export every 2 hours",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=7),
    tags=["ml", "scoring"],
) as dag:

    # Trigger the pipeline orchestrator — use short poke_interval to keep
    # heartbeating and avoid being killed as a zombie while waiting.
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_pipeline_orchestrator",
        trigger_dag_id="pipeline_orchestrator",
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=False,
        poke_interval=30,          # heartbeat every 30s (was 60s)
        execution_timeout=timedelta(hours=6),
        allowed_states=[State.SUCCESS],
        failed_states=[State.FAILED],
    )

    # After ingestion completes, run the prediction & export job
    run_prediction = BashOperator(
        task_id="predict_and_export_task",
        bash_command="python /opt/airflow/ml/ct/predict_and_export.py {{ ds }}",
        execution_timeout=timedelta(minutes=30),
    )

    trigger_ingestion >> run_prediction
