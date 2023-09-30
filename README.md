# Airflow DatasetSensor

Airflow sensor that can be used to wait for a dataset being produced for a given execution_date.
This is a workaround for the limitation that currently data-aware scheduling and time-based scheduling can not be used together for a consuming DAG.

Trade-off: Datasets are not directly connected to their consumer DAGs and thus their relationship is not displayed in the Datasets UI.
Limitations: Works for the newest created one-to-one dataset.

Tested with Airflow 2.6.3