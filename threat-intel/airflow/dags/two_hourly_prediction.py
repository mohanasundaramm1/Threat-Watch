# airflow/dags/two_hourly_prediction.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

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
    tags=["ml", "scoring"],
) as dag:

    # Trigger the pipeline orchestrator to run the Medallion Bronze->Silver->Lookups for the current day
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_pipeline_orchestrator",
        trigger_dag_id="pipeline_orchestrator",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    # After ingestion completes, run the prediction & export job
    # {{ ds }} gives the execution date string (YYYY-MM-DD)
    run_prediction = BashOperator(
        task_id="predict_and_export_task",
        bash_command="python /opt/airflow/ml/ct/predict_and_export.py {{ ds }}",
    )
    
    trigger_ingestion >> run_prediction
