# Astro SDK Extras project
# (c) kol, 2023

""" FileChanged sensor """

import os
import asyncio
from datetime import datetime, timedelta
from functools import cached_property
from airflow.models import XCom
from airflow.configuration import conf
from airflow.hooks.filesystem import FSHook
from airflow.exceptions import AirflowException
from airflow.sensors.filesystem import FileSensor
from airflow.utils.context import Context
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.triggers.base import TriggerEvent
from typing import Tuple

class FileChangedTrigger(BaseTrigger):
    """
    A trigger that fires after it finds the requested file and this file has been changed since previous check.

    :param filepath: File name (relative to the base path set within the connection). Wildcards are not allowed.
    :param poll_interval: File check interval
    :param xcom_key: XCom key (``dag_id|task_id|exec_date``) to store file modification time (for internal use).
    """
    def __init__(
        self,
        filepath: str,
        poll_interval: float = 5.0,
        xcom_key: str = None,
    ):
        super().__init__()
        assert not any([s in filepath for s in ('*', '?')]), 'Wildcards are not supported'

        self.filepath = filepath
        self.poll_interval = poll_interval
        self.xcom_key = xcom_key
        self._file_key = None
        self._last_modified = None

        if xcom_key:
            self._dag_id, self._task_id, self._exec_date = xcom_key.split('|')
            self._exec_date = datetime.fromisoformat(self._exec_date)
            self._file_key = f'{os.path.split(filepath)[1]}_last_modified'

            xcom_val = XCom.get_one(
                execution_date=self._exec_date, 
                key=self._file_key, 
                task_id=self._task_id, 
                dag_id=self._dag_id, 
                include_prior_dates=True)
            if xcom_val:
                self._last_modified = datetime.fromisoformat(xcom_val)

            self.log.info(f'FileChangedTrigger started for xcom key {xcom_key}|{self._file_key}, last modified from xcom: {self._last_modified}')

    def serialize(self):
        return (
            "astro_extras.sensors.file.FileChangedTrigger",
            {
                "filepath": self.filepath,
                "poll_interval": self.poll_interval,
                "xcom_key": self.xcom_key,
            },
        )

    async def run(self):
        while True:
            if os.path.exists(self.filepath):
                mod_time = datetime.fromtimestamp(os.path.getmtime(self.filepath))
                self.log.info("Found file %s, last modified: %s", self.filepath, mod_time.isoformat())
                if not self._file_key:
                    yield TriggerEvent((self.filepath, mod_time))
                else:
                    if self._last_modified and mod_time <= self._last_modified:
                        self.log.info("File %s was not changed since last check", self.filepath)
                    else:
                        self.log.info("Triggering file %s change event", self.filepath)
                        self._last_modified = mod_time
                        XCom.set(
                            key=self._file_key, 
                            value=mod_time.isoformat(), 
                            task_id=self._task_id, 
                            dag_id=self._dag_id, 
                            execution_date=self._exec_date)
                        yield TriggerEvent((self.filepath, mod_time))

            await asyncio.sleep(self.poll_interval)

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
            deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        if not self.poke(context=context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=FileChangedTrigger(
                    filepath=self.path,
                    poll_interval=self.poke_interval,
                    xcom_key=f'{self.dag_id}|{self.task_id}|{context["ti"].execution_date.isoformat()}'
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: Tuple[str, datetime]) -> Tuple[str, str]:
        if not event:
            raise AirflowException("%s task failed as %s not found", self.task_id, self.filepath)
        self.log.info("%s completed successfully, file %s modified at %s found", self.task_id, event[0], event[1])
        return event[0], event[1].isoformat()
