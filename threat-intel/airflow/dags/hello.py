from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello from Airflow! DAG is alive.")

default_args = {"owner": "you"}

with DAG(
    dag_id="hello",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["bootstrap"],
) as dag:
    PythonOperator(task_id="say_hello", python_callable=hello)
