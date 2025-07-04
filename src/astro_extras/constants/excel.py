# Astro SDK Extras project 2025

""" Common constants for Excel operators """

from enum import Enum

SUPPORTED_INPUT_EXTENSIONS: set[str] = {".xlsx", ".xls", ".xlsm"}
SUPPORTED_OUTPUT_EXTENSIONS: set[str] = {".xlsx", ".xls", ".xlsm"}

DEFAULT_SHEET_NAME: str = "Sheet1"

EXCEL_OPERATOR_NAMES: set[str] = {
    "ExcelReadOperator",
    "ExcelWriteOperator",
    "ExcelTransformOperator",
}


class ExcelXcomKey(str, Enum):
    INPUT = "input_file"
    OUTPUT = "output_file"
