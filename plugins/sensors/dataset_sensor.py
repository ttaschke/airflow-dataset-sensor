import datetime

from airflow.models.dagrun import DagRun
from airflow.models.dataset import (
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm import Session


class DatasetSensor(BaseSensorOperator):
    """
    Waits for a dataset being produced for the given execution_date

    :param dataset_uri: The URI of the dataset (templated)
    :param execution_date: Execution date for which to check the dataset's status as ISO-formatted string or datetime.datetime (templated)
    """

    template_fields = ["dataset_uri", "execution_date"]

    def __init__(
        self,
        *,
        dataset_uri: str,
        execution_date: str | datetime.datetime,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dataset_uri = dataset_uri

        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, str):
            self.execution_date = execution_date
        else:
            raise TypeError(
                f"execution_date should be passed either as a str or datetime.datetime, not {type(execution_date)}"
            )

    @provide_session
    def poke(self, context: Context, session: Session = NEW_SESSION) -> bool:
        dataset, task_outlet = (
            session.query(DatasetModel, TaskOutletDatasetReference)
            .filter_by(uri=self.dataset_uri)
            .join(
                TaskOutletDatasetReference,
                isouter=True,
            )
            .order_by(TaskOutletDatasetReference.updated_at.desc())
            .first()
        )

        dag_runs = DagRun.find(
            dag_id=task_outlet.dag_id,
            execution_date=timezone.parse(self.execution_date),
        )

        if not dag_runs:
            self.log.info(
                "Could not find a DagRun of DAG %s for execution date %s",
                task_outlet.dag_id,
                self.execution_date,
            )
            return False

        dag_run = dag_runs[0]

        self.log.info(
            "Poking for dataset %s produced by task %s in dag %s for run_id %s ... ",
            self.dataset_uri,
            task_outlet.task_id,
            task_outlet.dag_id,
            dag_run.run_id,
        )

        dataset_event = (
            session.query(DatasetEvent)
            .filter_by(
                dataset_id=dataset.id,
                source_dag_id=task_outlet.dag_id,
                source_task_id=task_outlet.task_id,
                source_run_id=dag_run.run_id,
            )
            .one_or_none()
        )

        if dataset_event is not None:
            self.log.info(
                "Found dataset %s produced by task %s in dag %s for execution_date %s",
                self.dataset_uri,
                task_outlet.task_id,
                task_outlet.dag_id,
                self.execution_date,
            )
            return True

        return False
