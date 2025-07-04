# Astro SDK Extras project 2025

"""
Airflow operators integrating ExcelService:
    BaseExcelOperator,
    ExcelReadOperator,
    ExcelWriteOperator,
    ExcelTransformOperator,
    ExcelCleanupOperator
"""

import time
from typing import Any, Callable, Collection

from pandas import DataFrame

from airflow import Dataset
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.models.xcom_arg import XComArg
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from ..constants.excel import ExcelXcomKey
from ..services.excel import ExcelService
from ..types.excel import (
    ExcelData,
    ExcelReadEngine,
    ExcelWriteEngine,
)
from ..utils.airflow import (
    collect_excel_files,
    get_file_path_from_conn,
    get_file_path_from_dataset,
    safe_xcom_push,
)
from ..utils.fs import move_file_to_subdir


class BaseExcelOperator(BaseOperator):
    """
    Base Airflow operator for Excel tasks.

    Attributes:
        service (ExcelService): Ready-to-use ExcelService (either injected or default).
    """

    def __init__(
        self,
        *,
        service: ExcelService | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.service: ExcelService = service or ExcelService()

    @staticmethod
    def _get_file_path(
        conn_id: str | None,
        dataset: Dataset | None,
    ) -> str:
        """
        Get local file path from Airflow connection OR Dataset.

        Args:
            conn_id: Airflow Connection ID.
            dataset: Airflow Dataset.

        Returns:
            str: Local file path

        Raises:
            AirflowException: If both or neither of conn_id/dataset are set, or path cannot be resolved.
        """
        if (conn_id is None and dataset is None) or (conn_id and dataset):
            raise AirflowException(
                "Exactly one of conn_id or dataset must be provided."
            )
        if conn_id:
            return get_file_path_from_conn(conn_id)
        return get_file_path_from_dataset(dataset)


class ExcelReadOperator(BaseExcelOperator):
    """
    Airflow operator to read Excel and push result to XCom.
    Accepts either `conn_id` or `dataset` (mutually exclusive).

    Returns:
        ExcelData: pandas DataFrame or dict of DataFrames.
    """
    template_fields: Collection[str] = (
        *getattr(BaseExcelOperator, "template_fields", ()),
        "conn_id",
        "dataset",
        "sheets",
        "sheet_mask",
        "sheet_regex",
        "include_columns",
        "exclude_columns",
        "pandas_read_args",
        "read_engine",
    )

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        dataset: Dataset | None = None,
        sheets: str | int | list[str | int] | None = None,
        sheet_mask: str | None = None,
        sheet_regex: str | None = None,
        include_columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        pandas_read_args: dict[str, Any] | None = None,
        read_engine: ExcelReadEngine = "openpyxl",
        task_id: str = "read_excel_file",
        service: ExcelService | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, service=service, **kwargs)
        self.conn_id = conn_id
        self.dataset = dataset
        self.sheets = sheets
        self.sheet_mask = sheet_mask
        self.sheet_regex = sheet_regex
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        self.pandas_read_args = pandas_read_args or {}
        self.read_engine: ExcelReadEngine = read_engine

    def execute(self, context: Context) -> ExcelData:
        """
        Read Excel file and push result to XCom.

        Reads Excel data from file or dataset and returns it as a DataFrame or dictionary of DataFrames.
        """
        file_path = self._get_file_path(self.conn_id, self.dataset)
        safe_xcom_push(context, {ExcelXcomKey.INPUT.value: file_path})
        return self.service.read(
            file_path=file_path,
            sheets=self.sheets,
            sheet_mask=self.sheet_mask,
            sheet_regex=self.sheet_regex,
            include_columns=self.include_columns,
            exclude_columns=self.exclude_columns,
            pandas_args=self.pandas_read_args,
            engine=self.read_engine,
        )


class ExcelWriteOperator(BaseExcelOperator):
    """
    Airflow operator to write DataFrame(s) to an Excel file.
    Accepts either `conn_id` or `dataset` (mutually exclusive).

    Returns:
        str: Path to the written Excel file.
    """
    template_fields: Collection[str] = (
        *getattr(BaseExcelOperator, "template_fields", ()),
        "conn_id",
        "dataset",
        "sheet_name",
        "pandas_write_args",
        "write_engine",
    )

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        dataset: Dataset | None = None,
        data: ExcelData | XComArg | None = None,
        sheet_name: str | None = None,
        pandas_write_args: dict[str, Any] | None = None,
        write_engine: ExcelWriteEngine = "openpyxl",
        task_id: str = "write_excel_file",
        service: ExcelService | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, service=service, **kwargs)
        self.conn_id = conn_id
        self.dataset = dataset
        self.sheet_name = sheet_name
        self.data = data
        self.pandas_write_args = pandas_write_args or {}
        self.write_engine: ExcelWriteEngine = write_engine

    def execute(self, context: Context) -> str:
        """
        Write data to Excel file.

        Writes provided DataFrame(s) to an Excel file or dataset and returns the output file path.
        """
        actual = self.data.resolve(context) if isinstance(self.data, XComArg) else self.data

        if actual is None:
            raise ValueError("No data provided to write")

        output_file = self._get_file_path(self.conn_id, self.dataset)
        safe_xcom_push(context, {ExcelXcomKey.OUTPUT.value: output_file})
        self.service.write(
            data=actual,
            output_file=output_file,
            default_sheet=self.sheet_name,
            pandas_args=self.pandas_write_args,
            engine=self.write_engine,
        )
        return output_file


class ExcelTransformOperator(BaseExcelOperator):
    """
    Airflow operator to read, transform, and write Excel data in a single task.
    Accepts either `conn_id`/`dataset` for input, и `output_conn_id`/`output_dataset` for output (mutually exclusive pairs).

    Returns:
        str: Path to the output Excel file.
    """
    template_fields: Collection[str] = (
        *getattr(BaseExcelOperator, "template_fields", ()),
        "conn_id",
        "dataset",
        "output_conn_id",
        "output_dataset",
        "sheets",
        "sheet_mask",
        "sheet_regex",
        "include_columns",
        "exclude_columns",
        "sheet_name",
        "pandas_read_args",
        "read_engine",
        "pandas_write_args",
        "write_engine",
    )

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        dataset: Dataset | None = None,
        output_conn_id: str | None = None,
        output_dataset: Dataset | None = None,
        transform_func: Callable[[ExcelData, Context], ExcelData],
        sheets: str | int | list[str | int] | None = None,
        sheet_mask: str | None = None,
        sheet_regex: str | None = None,
        include_columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        sheet_name: str | None = None,
        pandas_read_args: dict[str, Any] | None = None,
        read_engine: ExcelReadEngine = "openpyxl",
        pandas_write_args: dict[str, Any] | None = None,
        write_engine: ExcelWriteEngine = "openpyxl",
        task_id: str = "transform_excel_file",
        service: ExcelService | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, service=service, **kwargs)
        self.conn_id = conn_id
        self.dataset = dataset
        self.output_conn_id = output_conn_id
        self.output_dataset = output_dataset
        self.transform_func = transform_func
        self.sheets = sheets
        self.sheet_mask = sheet_mask
        self.sheet_regex = sheet_regex
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        self.sheet_name = sheet_name
        self.pandas_read_args = pandas_read_args or {}
        self.read_engine: ExcelReadEngine = read_engine
        self.pandas_write_args = pandas_write_args or {}
        self.write_engine: ExcelWriteEngine = write_engine

    def execute(self, context: Context) -> str:
        """
        Read, transform, and write Excel data.

        Reads Excel data, applies transformation, writes result to file or dataset, and returns output path.
        """
        input_file = self._get_file_path(self.conn_id, self.dataset)
        safe_xcom_push(context, {ExcelXcomKey.INPUT.value: input_file})
        data = self.service.read(
            file_path=input_file,
            sheets=self.sheets,
            sheet_mask=self.sheet_mask,
            sheet_regex=self.sheet_regex,
            include_columns=self.include_columns,
            exclude_columns=self.exclude_columns,
            pandas_args=self.pandas_read_args,
            engine=self.read_engine,
        )
        
        transformed = self.transform_func(data, context)
        if transformed is None or not (isinstance(transformed, dict) or isinstance(transformed, DataFrame)):
            raise TypeError(
                "transform_func must return a DataFrame or dict of DataFrames, "
                f"but got {type(transformed).__name__}"
            )
        output_file = self._get_file_path(self.output_conn_id, self.output_dataset)
        safe_xcom_push(context, {ExcelXcomKey.OUTPUT.value: output_file})
        self.service.write(
            data=transformed,
            output_file=output_file,
            default_sheet=self.sheet_name,
            pandas_args=self.pandas_write_args,
            engine=self.write_engine,
        )
        return output_file


class ExcelCleanupOperator(BaseOperator):
    """
    Airflow operator to move all Excel-related files to either the 'success' or 'error' subdirectory
    (created in each file's original parent directory) depending on the DAG run result.

    Algorithm:
        - If run_sync_mode is True (default), the operator will wait for all DAG tasks to finish before cleanup.
        - If any task in the DAG run has failed, all Excel files are moved to their 'error' subdirectory,
          and the operator fails (raises AirflowException).
        - If there are no failed tasks, all Excel files are moved to their 'success' subdirectory.
        - If timeout is set (seconds), and waiting for DAG completion exceeds timeout,
          an AirflowException is raised.

    Args:
        success_dir (str): Name of the subdirectory for successful files (default: "success").
        error_dir (str): Name of the subdirectory for error files (default: "error").
        run_sync_mode (bool): If True, wait for all DAG tasks to finish before cleanup (default: True).
        check_interval (int): Seconds to wait between checks for DAG completion (default: 5).
        timeout (int | None): Maximum time to wait for DAG completion in seconds (default: 3000).
        task_id (str): Task identifier (default: "cleanup_excel_files").
        trigger_rule (str): Airflow trigger rule (default: TriggerRule.ALL_DONE).
    """

    def __init__(
        self,
        *,
        success_dir: str = "success",
        error_dir: str = "error",
        run_sync_mode: bool = True,
        check_interval: int = 5,
        timeout: int | None = 3000,
        task_id: str = "cleanup_excel_files",
        trigger_rule: str = TriggerRule.ALL_DONE,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, trigger_rule=trigger_rule, **kwargs)
        self.success_dir = success_dir
        self.error_dir = error_dir
        self.run_sync_mode = run_sync_mode
        self.check_interval = check_interval
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """
        Perform cleanup of Excel files after DAG run.

        Moves all Excel files to either 'success' or 'error' subdirectories based on the overall DAG run status.
        """
        dag_run: DagRun = context.get("dag_run")
        if not dag_run:
            raise AirflowException("ExcelCleanupOperator: 'dag_run' not found in context.")

        if self.run_sync_mode:
            self._wait_for_dag_to_finish(dag_run)

        all_tis: list[TaskInstance] = dag_run.get_task_instances()
        self.log.info("ExcelCleanupOperator: starting Excel file cleanup after DAG run.")

        any_failed: bool = any(
            ti.task_id != self.task_id and ti.state == TaskInstanceState.FAILED
            for ti in all_tis
        )
        target_subdir: str = self.error_dir if any_failed else self.success_dir
        self.log.info(f"ExcelCleanupOperator: files will be moved to subdirectory '{target_subdir}'.")

        files_to_move: set[str] = collect_excel_files(all_tis)
        if not files_to_move:
            self.log.warning("ExcelCleanupOperator: no files found to move (no XCom entries present).")
            if any_failed:
                raise AirflowException("ExcelCleanupOperator: DAG failed and no files to move.")
            return

        for src_path_str in files_to_move:
            move_file_to_subdir(src_path_str, target_subdir)

        if any_failed:
            raise AirflowException("ExcelCleanupOperator: DAG failed, operator marked as failed after moving files.")

    def _wait_for_dag_to_finish(self, dag_run: DagRun) -> None:
        """
        Block until all other tasks in the DAG run have finished, or until the timeout is exceeded.

        Checks the states of all task instances except itself and waits until they are all in a terminal state.
        If the timeout (in seconds) is set and reached, raises an AirflowException.
        Logs running tasks and re-checks every `self.check_interval` seconds before proceeding.
        """
        start_time = time.monotonic()
        while True:
            running_tasks = [
                ti.task_id
                for ti in dag_run.get_task_instances()
                if ti.task_id != self.task_id
                and ti.state not in (
                    TaskInstanceState.SUCCESS,
                    TaskInstanceState.FAILED,
                    TaskInstanceState.SKIPPED,
                    TaskInstanceState.UPSTREAM_FAILED,
                    TaskInstanceState.REMOVED,
                )
            ]

            if not running_tasks:
                self.log.info("ExcelCleanupOperator: All DAG tasks have finished.")
                break

            if self.timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > self.timeout:
                    raise AirflowException(
                        f"ExcelCleanupOperator: Timeout exceeded "
                        f"({self.timeout} seconds) while waiting for DAG tasks: {running_tasks}."
                    )

            self.log.info(
                f"ExcelCleanupOperator: Waiting for tasks {running_tasks} to finish. "
                f"Next check in {self.check_interval} seconds."
            )
            time.sleep(self.check_interval)
