# Astro SDK Extras project 2025

""" Utility functions for Airflow operations """

import logging
from typing import Any

from airflow import Dataset
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context

from ..constants.excel import EXCEL_OPERATOR_NAMES, ExcelXcomKey

logger = logging.getLogger(__name__)


def get_file_path_from_conn(conn_id: str) -> str:
    """
    Retrieve the file path from Airflow Connection by conn_id.

    Expects that the path is stored in extras (e.g., {"path": "/path/to/file.xlsx"}).

    Args:
        conn_id (str): The Airflow connection ID.

    Returns:
        str: Path to the Excel file.

    Raises:
        ValueError: If 'path' is not found in connection extras.
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    file_path = extra.get("path")
    if not file_path:
        raise ValueError(f"'path' not found in connection extras for conn_id={conn_id}")
    return file_path


def get_file_path_from_dataset(dataset: Dataset) -> str:
    """
    Resolve local file path from Airflow Dataset.

    Args:
        dataset: Airflow Dataset object.

    Returns:
        str: Local file path.

    Raises:
        AirflowException: If dataset does not have a valid string URI.
    """
    uri = getattr(dataset, "uri", None)
    if not uri or not isinstance(uri, str):
        raise AirflowException("Dataset must have a string 'uri' property.")
    return uri.removeprefix("file://")


def safe_xcom_push(context: Context, values: dict[str, Any]) -> None:
    """
    Safely push multiple key-value pairs to XCom, if TaskInstance ('ti') is present in context.

    Args:
        context (Context): Airflow task context.
        values (dict[str, Any]): Dictionary of key-value pairs to push to XCom.
                                 Only non-None values will be pushed.

    Notes:
        If 'ti' is not present in context, or if values is empty, the function does nothing.
    """
    ti = context.get("ti")
    if not ti:
        logger.debug("XCom push skipped: 'ti' (TaskInstance) not found in context.")
        return
    if not values:
        logger.debug("XCom push skipped: values dict is empty.")
        return

    for key, value in values.items():
        if value is None:
            logger.debug(f"XCom push for key '{key}' skipped: value is None.")
            continue
        logger.info(f"Pushing key '{key}' to XCom (task_id='{getattr(ti, 'task_id', None)}')")
        ti.xcom_push(key=str(key), value=value)


def is_excel_operator(ti: TaskInstance) -> bool:
    """
    Check if a given TaskInstance belongs to a supported Excel operator.

    Args:
        ti (TaskInstance): Airflow TaskInstance.

    Returns:
        bool: True if the operator is an Excel operator, False otherwise.
    """
    return getattr(ti, "operator", None) in EXCEL_OPERATOR_NAMES


def collect_excel_files(tis: list[TaskInstance]) -> set[str]:
    """
    Collect all unique file paths from XCom for Excel operators.

    Args:
        tis (list[TaskInstance]): List of task instances.

    Returns:
        set[str]: Set of unique file paths to move.
    """
    files: set[str] = set()
    for ti in filter(is_excel_operator, tis):
        for key in ExcelXcomKey:
            try:
                path = ti.xcom_pull(key=key.value, task_ids=ti.task_id)
                if path and isinstance(path, str):
                    files.add(path)
                    logger.debug(f"Found file for moving (task_id='{ti.task_id}', key='{key.value}'): {path}")
            except Exception as e:
                logger.debug(f"Failed to pull '{key.value}' from XCom for task '{ti.task_id}': {e}")
    return files
