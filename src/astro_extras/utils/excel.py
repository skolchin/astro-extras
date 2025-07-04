# Astro SDK Extras project 2025

"""
Utility functions for Excel operations:
    file validation,
    sheet selection,
    column filtering,
    reading,
    writing
"""

import fnmatch
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame

from ..exceptions.excel import ExcelReadError, ExcelWriteError
from ..types.excel import ExcelData, ExcelReadEngine, ExcelWriteEngine
from ..constants.excel import DEFAULT_SHEET_NAME

logger = logging.getLogger(__name__)


def get_sheet_names(
    file_path: Path,
    engine: ExcelReadEngine,
) -> list[str]:
    """
    Retrieve all sheet names from an Excel file using a specific read engine.

    Args:
        file_path: Path to the Excel file.
        engine: Name of the pandas Excel read engine to use.

    Returns:
        List of sheet names present in the Excel file.

    Raises:
        ExcelReadError: For any error encountered while opening or reading.
    """
    try:
        logger.debug(f"Reading sheets from {file_path} using engine {engine}")
        with pd.ExcelFile(file_path, engine=engine) as xls:
            names = [str(sheet) for sheet in xls.sheet_names]
        logger.info(f"Retrieved sheet names: {names} from {file_path}")
        return names

    except Exception as exc:
        logger.error(f"Failed to retrieve sheet names from {file_path}: {exc}")
        raise ExcelReadError(f"Failed to retrieve sheet names: {exc}") from exc

def select_sheets(
    all_sheets: list[str],
    sheets: str | int | list[str | int] | None = None,
    sheet_mask: str | None = None,
    sheet_regex: str | None = None,
) -> list[str]:
    """
    Filter sheet names by explicit list, glob-style mask, or regex.

    Args:
        all_sheets: Complete list of sheet names available.
        sheets: Single name/index or list of names/indices to select explicitly.
        sheet_mask: Glob pattern to filter sheet names.
        sheet_regex: Regular expression to match sheet names.

    Returns:
        Filtered list of sheet names based on provided criteria.

    Notes:
        - If both sheets and sheet_mask/regex are None, returns all_sheets.
        - Explicit sheets list takes precedence over mask/regex.
    """
    logger.debug(
        f"Selecting sheets from {all_sheets} with sheets={sheets}, "
        f"mask={sheet_mask}, regex={sheet_regex}"
    )

    if sheets is not None:
        sel_list = sheets if isinstance(sheets, list) else [sheets]
        result: list[str] = []
        for item in sel_list:
            if isinstance(item, int) and 0 <= item < len(all_sheets):
                result.append(all_sheets[item])
            elif isinstance(item, str) and item in all_sheets:
                result.append(item)
        logger.info(f"Explicitly selected sheets: {result}")
        return result

    if sheet_mask is not None:
        matched = [name for name in all_sheets if fnmatch.fnmatch(name, sheet_mask)]
        logger.info(f"Selected by mask '{sheet_mask}': {matched}")
        return matched

    if sheet_regex is not None:
        pattern = re.compile(sheet_regex)
        matched = [name for name in all_sheets if pattern.match(name)]
        logger.info(f"Selected by regex '{sheet_regex}': {matched}")
        return matched

    logger.info(f"No specific selection provided, using all sheets: {all_sheets}")
    return all_sheets

def filter_columns(
    df: DataFrame,
    include_columns: list[str] | None,
    exclude_columns: list[str] | None,
) -> DataFrame:
    """
    Filter DataFrame columns by inclusion or exclusion lists.

    Args:
        df: pandas DataFrame to be filtered.
        include_columns: List of column names to retain or None.
        exclude_columns: List of column names to drop or None.

    Returns:
        Filtered pandas DataFrame.

    Raises:
        ValueError: If any columns from include_columns are not found in the DataFrame,
                    or if include_columns is an empty list.
    """
    logger.debug(f"Filtering columns include={include_columns}, exclude={exclude_columns}")
    original_cols = list(df.columns)

    if include_columns is not None:
        if not include_columns:
            raise ValueError(
                "include_columns is an empty list. At least one column must be specified."
            )
        not_found = [c for c in include_columns if c not in df.columns]
        if not_found:
            raise ValueError(
                f"Columns from include_columns not found in DataFrame: {not_found}. "
                f"Available columns: {original_cols}"
            )
        filtered_df = df[include_columns]
        logger.info(
            f"Applied include_columns filter: {include_columns}, before: {original_cols}, "
            f"after: {list(filtered_df.columns)}"
        )
        return filtered_df

    if exclude_columns is not None:
        not_found = [c for c in exclude_columns if c not in df.columns]
        if not_found:
            logger.warning(
                f"Some columns from exclude_columns not found in DataFrame: {not_found}. "
                f"Available columns: {original_cols}"
            )
        filtered_df = df.drop(columns=[c for c in exclude_columns if c in df.columns], errors="ignore")
        logger.info(
            f"Applied exclude_columns filter: {exclude_columns}, before: {original_cols}, "
            f"after: {list(filtered_df.columns)}"
        )
        return filtered_df

    return df

def read_excel_sheets_raw(
    file_path: Path,
    sheets: list[str],
    engine: ExcelReadEngine,
    pandas_args: dict[str, Any],
) -> ExcelData:
    """
    Read one or multiple sheets from an Excel file.

    Args:
        file_path: Path to validated Excel file.
        sheets: Names of sheets to read.
        engine: Name of the pandas Excel read engine.
        pandas_args: Keyword arguments for pandas.read_excel.

    Returns:
        DataFrame if one sheet, or dict of DataFrames for multiple sheets.

    Raises:
        ExcelReadError: On any I/O or parsing failure.
    """
    try:
        if len(sheets) == 1:
            name = sheets[0]
            logger.debug(f"Reading single sheet '{name}' from {file_path}")
            df = pd.read_excel(file_path, sheet_name=name, engine=engine, **pandas_args)
            row_count = len(df)
            logger.info(f"Read {row_count} records from sheet '{name}' in file '{file_path}'")
            if df.empty:
                logger.warning(f"Sheet '{name}' is empty in file {file_path}")
            return df

        result: dict[str, DataFrame] = {}
        with pd.ExcelFile(file_path, engine=engine) as xls:
            missing_sheets = [name for name in sheets if name not in xls.sheet_names]
            if missing_sheets:
                logger.error(f"Sheets not found in '{file_path}': {missing_sheets}")
                raise ExcelReadError(f"Sheets not found: {missing_sheets}")
            for name in sheets:
                logger.debug(f"Reading sheet '{name}' from {file_path} using shared ExcelFile object")
                df = pd.read_excel(xls, sheet_name=name, **pandas_args)
                row_count = len(df)
                logger.info(f"Read {row_count} records from sheet '{name}' in file '{file_path}'")
                if df.empty:
                    logger.warning(f"Sheet '{name}' is empty in file {file_path}")
                result[name] = df
        logger.info(
            f"Read {len(result)} sheets from '{file_path}': " +
            ", ".join(f"{k} ({len(v)} rows)" for k, v in result.items())
        )
        return result

    except Exception as exc:
        logger.error(f"Error reading '{file_path}': {exc}")
        raise ExcelReadError(f"Error reading Excel: {exc}") from exc

def apply_column_filters(
    data: ExcelData,
    include_columns: list[str] | None,
    exclude_columns: list[str] | None,
) -> ExcelData:
    """
    Apply column inclusion/exclusion filters to ExcelData returned by reading.

    Args:
        data: DataFrame or dict of DataFrames read from Excel.
        include_columns: Columns to retain or None.
        exclude_columns: Columns to drop or None.

    Returns:
        Filtered DataFrame or dict of DataFrames.
    """
    if isinstance(data, dict):
        logger.debug(f"Applying column filters to {len(data)} sheets")
        filtered = {
            sheet: filter_columns(df, include_columns, exclude_columns)
            for sheet, df in data.items()
        }
        for sheet, df in filtered.items():
            logger.info(f"After column filter: {sheet} has {len(df.columns)} columns and {len(df)} rows")
        return filtered
    else:
        filtered_df = filter_columns(data, include_columns, exclude_columns)
        logger.info(f"After column filter: DataFrame has {len(filtered_df.columns)} columns and {len(filtered_df)} rows")
        return filtered_df

def write_excel(
    data: ExcelData,
    output_path: Path,
    engine: ExcelWriteEngine,
    default_sheet: str | None,
    pandas_args: dict[str, Any],
) -> None:
    """
    Write DataFrame(s) to an Excel file.

    Args:
        data: Single DataFrame or dict of DataFrames keyed by sheet name.
        output_path: Destination file path for the Excel output.
        engine: Name of the pandas Excel write engine.
        default_sheet: Sheet name to use if data is a single DataFrame or None.
        pandas_args: Keyword arguments for DataFrame.to_excel.

    Raises:
        ExcelWriteError: On any I/O or serialization failure.
    """
    try:
        with pd.ExcelWriter(output_path, engine=engine) as writer:
            if isinstance(data, dict):
                logger.info(f"Writing {len(data)} sheets to {output_path}")
                for sheet_name, df in data.items():
                    logger.info(f"Writing sheet '{sheet_name}' with {len(df)} records")
                    df.to_excel(writer, sheet_name=sheet_name, **pandas_args)
            else:
                sheet = default_sheet or DEFAULT_SHEET_NAME
                logger.info(f"Writing single sheet '{sheet}' with {len(data)} records to {output_path}")
                data.to_excel(writer, sheet_name=sheet, **pandas_args)
        logger.info(f"Successfully wrote Excel file to '{output_path}'")

    except Exception as exc:
        logger.error(f"Error writing '{output_path}': {exc}")
        raise ExcelWriteError(f"Error writing Excel: {exc}") from exc
