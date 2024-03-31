import asyncio
import typing
from typing import Tuple

from airflow.models.dagrun import DagRun
from airflow.models.dataset import (
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from asgiref.sync import sync_to_async
from sqlalchemy.orm import Session


class DatasetEventTrigger(BaseTrigger):
    """
    Waits asynchronously for a dataset being produced for the given execution_date

    :param dataset_uri: The URI of the dataset (templated)
    :param execution_date: Execution date for which to check the dataset's status as ISO-formatted string or datetime.datetime (templated)
    :param poll_interval: Polling interval to check for the dataset's status
    """

    def __init__(
        self, dataset_uri: str, execution_date: str, poll_interval: float
    ) -> None:
        super().__init__()
        self.dataset_uri = dataset_uri
        self.execution_date = execution_date
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        return (
            "triggers.dataset_event.DatasetEventTrigger",
            {
                "dataset_uri": self.dataset_uri,
                "execution_date": self.execution_date,
                "poll_interval": self.poll_interval,
            },
        )

    @provide_session
    async def run(self, session: Session = NEW_SESSION):
        num_runs = 0

        while True:
            if num_runs > 0:
                self.log.info(
                    "Sleep %s seconds until polling dataset status again. Number of runs: %s",
                    self.poll_interval,
                    num_runs,
                )
                await asyncio.sleep(self.poll_interval)

            num_runs = num_runs + 1

            dataset_info = await self.get_dataset_info()

            if dataset_info is not None:
                dataset, task_outlet = dataset_info
            else:
                self.log.info(
                    "Could not find a dataset with dataset uri %s",
                    self.dataset_uri,
                )
                continue

            dag_runs = await self.get_dag_run(
                dag_id=task_outlet.dag_id,
                execution_date=timezone.parse(self.execution_date),
            )

            if not dag_runs:
                self.log.info(
                    "Could not find a DagRun of DAG %s for execution date %s",
                    task_outlet.dag_id,
                    self.execution_date,
                )
                continue

            dag_run = dag_runs[0]

            self.log.info(
                "Poking for dataset %s produced by task %s in dag %s for run_id %s ... ",
                self.dataset_uri,
                task_outlet.task_id,
                task_outlet.dag_id,
                dag_run.run_id,
            )

            dataset_event = await self.get_dataset_event(
                dataset_id=dataset.id,
                dag_id=task_outlet.dag_id,
                task_id=task_outlet.task_id,
                run_id=dag_run.run_id,
            )

            if dataset_event is not None:
                self.log.info(
                    "Found dataset %s produced by task %s in dag %s for execution_date %s",
                    self.dataset_uri,
                    task_outlet.task_id,
                    task_outlet.dag_id,
                    self.execution_date,
                )
                yield TriggerEvent(dataset_event)

    @sync_to_async
    @provide_session
    def get_dataset_info(
        self, session: Session
    ) -> Tuple[DatasetModel, TaskOutletDatasetReference] | None:
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

        return dataset_info

    @sync_to_async
    def get_dag_run(self, dag_id: str, execution_date: str) -> DagRun | None:
        dag_run = DagRun.find(
            dag_id=dag_id,
            execution_date=execution_date,
        )

        return dag_run

    @sync_to_async
    @provide_session
    def get_dataset_event(
        self,
        dataset_id: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        session: Session,
    ) -> DatasetEvent | None:
        dataset_event = (
            session.query(DatasetEvent)
            .filter_by(
                dataset_id=dataset_id,
                source_dag_id=dag_id,
                source_task_id=task_id,
                source_run_id=run_id,
            )
            .one_or_none()
        )

        return dataset_event
