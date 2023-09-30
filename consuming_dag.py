import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.sensors.dataset_sensor import DatasetSensor

with DAG(
    "consuming_dag",
    default_args={"start_date": airflow.utils.dates.days_ago(0)},
    schedule="0 * * * *",
) as dag:
    # Sensor waiting for the dataset being created for the same data_interval_start as this DagRun
    dataset_sensor_hourly_task = DatasetSensor(
        task_id="dataset_sensor_hourly",
        dataset_uri="hourly_dataset",
        execution_date="{{data_interval_start}}",
        poke_interval=60,
    )

    # Task that will execute once the dataset was created
    use_dataset_task = EmptyOperator(task_id="use_dataset")

    dataset_sensor_hourly_task >> use_dataset_task
