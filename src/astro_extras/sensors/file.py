# Astro SDK Extras project
# (c) kol, 2023

""" FileChanged sensor """

import os
import time
import asyncio
from airflow.models import XCom
from functools import cached_property
from airflow.configuration import conf
from datetime import datetime, timedelta
from airflow.utils.context import Context
from airflow.hooks.filesystem import FSHook
from airflow.sensors.filesystem import FileSensor
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowSkipException, AirflowFailException
from typing import Tuple

class FileChangedTrigger(BaseTrigger):
    """
    A trigger that fires after it finds the requested file and this file has been changed since previous check.
    """
    def __init__(
        self,
        filepath: str,
        poll_interval: float = 5.0,
        dag_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
    ):
        super().__init__()
        assert not any([s in filepath for s in ('*', '?')]), 'Wildcards are not supported'

        self.filepath = filepath
        self.poll_interval = poll_interval
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id

        self._file_key = f'{os.path.split(filepath)[1]}__last_modified'
        self._last_modified = None

        if self.dag_id:
            xcom_val = XCom.get_one(
                key=self._file_key, 
                task_id=self.task_id, 
                dag_id=self.dag_id,
                run_id=self.run_id,
                include_prior_dates=True)
            if xcom_val:
                self._last_modified = datetime.fromisoformat(xcom_val)

            self.log.info(f'FileChangedTrigger started for {self.filepath} with xcom key {self._file_key}, last modified from xcom: {self._last_modified}')

        if not os.path.exists(self.filepath):
            self.log.info(f'File {self.filepath} was not found at trigger\'s start')

    def serialize(self):
        return (
            "astro_extras.sensors.file.FileChangedTrigger",
            {
                "filepath": self.filepath,
                "poll_interval": self.poll_interval,
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
            },
        )

    def check_once(self) -> Tuple[str, datetime] | None:
        mod_time = datetime.fromtimestamp(os.path.getmtime(self.filepath))
        self.log.info("Found file %s, last modified: %s", self.filepath, mod_time.isoformat())
        if not self._file_key:
            return (self.filepath, mod_time)

        if self._last_modified and mod_time <= self._last_modified:
            self.log.info("File %s was not changed since last check", self.filepath)
            return None

        self.log.info("Triggering file %s change event", self.filepath)
        self._last_modified = mod_time
        XCom.set(
            key=self._file_key, 
            value=mod_time.isoformat(),
            task_id=self.task_id,
            dag_id=self.dag_id,
            run_id=self.run_id)
        
        return (self.filepath, mod_time)

    async def run(self):
        while True:
            if os.path.exists(self.filepath):
                if (result := self.check_once()):
                    yield TriggerEvent(result)
            else:
                self.log.info(f'File {self.filepath} does not exist')

            await asyncio.sleep(self.poll_interval)

    def run_sync(self) -> Tuple[str, datetime] | None:
        while True:
            if os.path.exists(self.filepath):
                if (result := self.check_once()):
                    return result
            else:
                self.log.info(f'File {self.filepath} does not exist')

            time.sleep(self.poll_interval)


class FileChangedSensor(FileSensor):
    """
    Extension of standard ``FileSensor`` which waits for a file to appear on a filesystem,
    and to be modified since last check.

    Args:
        fs_conn_id: reference to the File (path) connection id
        filepath: file name to look after (wildcards/directories not allowed)
        deferrable: triggers Airflow task deferral mode

    Returns:
        2-element tuple with file name and file modification time as isoformatted string
    """
    def __init__(
            self,
            *,
            deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=True),
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.deferrable = deferrable

    @cached_property
    def path(self) -> str:
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        return full_path

    def execute(self, context: Context):
        if not self.deferrable:
            # Non-deferrable mode
            trigger = FileChangedTrigger(
                filepath=self.path,
                poll_interval=self.poke_interval,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=context["ti"].run_id,
            )
            self.execute_complete(context, trigger.run_sync())
        else:
            # Defer execution to the triggerer
            self.defer(
                trigger=FileChangedTrigger(
                    filepath=self.path,
                    poll_interval=self.poke_interval,
                    dag_id=self.dag_id,
                    task_id=self.task_id,
                    run_id=context["ti"].run_id,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout),
            )

    def execute_complete(self, context: Context, event: Tuple[str, datetime]) -> Tuple[str, str]:
        if not event and self.soft_fail:
            raise AirflowSkipException("File %s was not changed since last check", self.filepath)
        elif not event:
            raise AirflowFailException("File %s was not changed since last check", self.filepath)
        else:
            self.log.info("%s completed successfully, file %s modified at %s found", self.task_id, event[0], event[1])
            return event[0], event[1].isoformat()
