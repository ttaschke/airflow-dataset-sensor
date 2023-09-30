import airflow
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator

with DAG(
    "producing_dag",
    default_args={"start_date": airflow.utils.dates.days_ago(0)},
    description="producing_dag",
    schedule="0 * * * *",
) as dag:
    create_dataset_task = EmptyOperator(
        task_id="create_hourly_dataset", outlets=[Dataset("hourly_dataset")]
    )
