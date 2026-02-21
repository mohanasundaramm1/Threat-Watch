# airflow/dags/biweekly_ml_retraining.py
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "biweekly_ml_retraining",
    default_args=default_args,
    description="Retrain LightGBM model every 14 days",
    schedule_interval="0 0 */14 * *",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["ml", "training"],
) as dag:
    
    # We execute the script directly in the container since ml folder is now mapped
    run_retraining = BashOperator(
        task_id="retrain_model_task",
        bash_command="python /opt/airflow/ml/ct/retrain_model.py 14",
    )
    
    run_retraining
