# Astro SDK Extras project 2025

""" Custom exceptions for Excel file operations """


class ExcelError(Exception):
    """Base exception for all Excel-related errors."""
    pass


class ExcelReadError(ExcelError):
    """Error raised when reading from Excel fails."""
    pass


class ExcelWriteError(ExcelError):
    """Error raised when writing to Excel fails."""
    pass
