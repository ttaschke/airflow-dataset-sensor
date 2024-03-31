import datetime

from airflow.configuration import conf
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
from plugins.triggers.dataset_event import DatasetEventTrigger


class DatasetSensor(BaseSensorOperator):
    """
    Waits for a dataset being produced for the given execution_date

    :param dataset_uri: The URI of the dataset (templated)
    :param execution_date: Execution date for which to check the dataset's status as ISO-formatted string or datetime.datetime (templated)
    :param poll_interval: Polling interval to check for the dataset's status
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields = ["dataset_uri", "execution_date"]

    def __init__(
        self,
        *,
        dataset_uri: str,
        execution_date: str | datetime.datetime,
        poll_interval: float = 10.0,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
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

        self.poll_interval = poll_interval
        self.deferrable = deferrable

    @provide_session
    def poke(self, context: Context, session: Session = NEW_SESSION) -> bool:
        dataset_info = (
            session.query(DatasetModel, TaskOutletDatasetReference)
            .filter_by(uri=self.dataset_uri)
            .join(
                TaskOutletDatasetReference,
                isouter=True,
            )
            .order_by(TaskOutletDatasetReference.updated_at.desc())
            .first()
        )

        if dataset_info is not None:
            dataset, task_outlet = dataset_info
        else:
            self.log.info(
                "Could not find a dataset with dataset uri %s",
                self.dataset_uri,
            )
            return False

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

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=DatasetEventTrigger(
                    dataset_uri=self.dataset_uri,
                    execution_date=self.execution_date,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None) -> None:
        if event is not None:
            self.log.info(
                "Found dataset %s produced by task %s in dag %s for execution_date %s",
                self.dataset_uri,
                self.task_id,
                self.dag_id,
                self.execution_date,
            )
        return
