# Astro SDK Extras project 2025

"""
High-level stateless service for Excel file operations.
All read/write arguments and engines are passed per-method
"""

import logging
from pathlib import Path
from typing import Any

from ..constants.excel import SUPPORTED_INPUT_EXTENSIONS, SUPPORTED_OUTPUT_EXTENSIONS
from ..exceptions.excel import ExcelReadError
from ..types.excel import (
    ExcelData,
    ExcelReadEngine,
    ExcelWriteEngine,
)
from ..types.fs import FilePath
from ..utils.excel import (
    apply_column_filters,
    get_sheet_names,
    read_excel_sheets_raw,
    select_sheets,
    write_excel,
)
from ..validators.fs import validate_extension, validate_file_exists

logger = logging.getLogger(__name__)


class ExcelService:
    """
    High-level stateless service for Excel file operations.

    All read/write arguments and engines are passed per-method.

    Methods:
        read: Read and filter sheets from an Excel file.
        write: Write DataFrame(s) to an Excel file.
    """

    def read(
        self,
        file_path: FilePath,
        sheets: str | int | list[str | int] | None = None,
        sheet_mask: str | None = None,
        sheet_regex: str | None = None,
        include_columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        pandas_args: dict[str, Any] | None = None,
        engine: ExcelReadEngine = "openpyxl",
    ) -> ExcelData:
        """
        Read and filter sheets from an Excel file.

        Args:
            file_path: Path or string to the Excel input file.
            sheets: Specific sheet(s) by name or index.
            sheet_mask: Glob pattern to match sheet names.
            sheet_regex: Regex pattern to match sheet names.
            include_columns: List of columns to retain or None.
            exclude_columns: List of columns to drop or None.
            pandas_args: Options for pandas.read_excel or None.
            engine: Name of the pandas Excel read engine.

        Returns:
            Single DataFrame or dict of DataFrames.

        Raises:
            FileNotFoundError: If file not found.
            ExtensionError: If extension invalid.
            ExcelReadError: If no sheets match or read fails.
        """
        if sum(arg is not None for arg in (sheets, sheet_mask, sheet_regex)) > 1:
            raise ExcelReadError("Only one of 'sheets', 'sheet_mask' or 'sheet_regex' allowed.")

        if sum(arg is not None for arg in (include_columns, exclude_columns)) > 1:
            raise ExcelReadError("Only one of 'include_columns' or 'exclude_columns' allowed.")

        path = validate_file_exists(file_path)
        validate_extension(path, SUPPORTED_INPUT_EXTENSIONS)

        sheet_names = get_sheet_names(path, engine)
        selections = select_sheets(sheet_names, sheets, sheet_mask, sheet_regex)

        if not selections:
            raise ExcelReadError(
                f"No sheets match selection criteria: "
                f"sheets={sheets}, mask={sheet_mask}, regex={sheet_regex} "
                f"in file: {path} (available sheets: {sheet_names})"
            )

        logger.info(f"Starting read: file={path}, sheets={selections}, engine={engine}")
        raw = read_excel_sheets_raw(
            file_path=path,
            sheets=selections,
            engine=engine,
            pandas_args=pandas_args or {},
        )

        filtered = apply_column_filters(raw, include_columns, exclude_columns)

        if isinstance(filtered, dict):
            empty_by_columns = [name for name, df in filtered.items() if len(df.columns) == 0]
            empty_by_rows = [name for name, df in filtered.items() if df.empty]
            if len(empty_by_columns) == len(filtered):
                raise ExcelReadError(
                    f"After filtering, all sheets have no columns: {empty_by_columns}"
                )
            if empty_by_columns:
                logger.warning(
                    f"After filtering, the following sheets have no columns: {empty_by_columns}"
                )
            if empty_by_rows:
                logger.warning(
                    f"After filtering, the following sheets are empty (no rows): {empty_by_rows}"
                )
            logger.info(f"Read operation complete: {len(filtered)} sheets processed from '{path}'")
        else:
            if len(filtered.columns) == 0:
                raise ExcelReadError("After filtering, DataFrame has no columns.")
            if filtered.empty:
                logger.warning("After filtering, DataFrame is empty (no rows).")
            logger.info(f"Read operation complete: {len(filtered)} records from DataFrame")

        return filtered

    def write(
        self,
        data: ExcelData,
        output_file: FilePath,
        default_sheet: str | None = None,
        pandas_args: dict[str, Any] | None = None,
        engine: ExcelWriteEngine = "openpyxl",
    ) -> None:
        """
        Write DataFrame(s) to an Excel file.

        Args:
            data: Single DataFrame or dict of DataFrames.
            output_file: Path or string for the Excel output file.
            default_sheet: Sheet name for single DataFrame or None.
            pandas_args: Options for DataFrame.to_excel or None.
            engine: Name of the pandas Excel write engine.

        Raises:
            ExtensionError: If extension unsupported.
            ExcelWriteError: If write operation fails.
        """
        path = Path(output_file)
        validate_extension(path, SUPPORTED_OUTPUT_EXTENSIONS)
        path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting write to file '{output_file}' with engine '{engine}'")
        write_excel(
            data=data,
            output_path=path,
            engine=engine,
            default_sheet=default_sheet,
            pandas_args=pandas_args or {},
        )
