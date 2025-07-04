# Astro SDK Extras project 2025

""" Common type aliases for Excel utilities and services """

from typing import Literal
from pandas import DataFrame

ExcelData = DataFrame | dict[str, DataFrame]
ExcelReadEngine = Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"]
ExcelWriteEngine = Literal["openpyxl", "odf", "xlsxwriter"]
