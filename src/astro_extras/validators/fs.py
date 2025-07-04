# Astro SDK Extras project 2025

""" File system validators """

import logging
from pathlib import Path

from ..exceptions.fs import ExtensionError
from ..types.fs import FilePath

logger = logging.getLogger(__name__)


def validate_file_exists(path_input: FilePath) -> Path:
    """
    Check that the given file exists on disk.

    Args:
        path_input (FilePath): Path or string pointing to the file.

    Returns:
        Path: A pathlib.Path object for the existing file.

    Raises:
        FileNotFoundError: If the file does not exist at the given path.
    """
    path = Path(path_input)
    if not path.exists():
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: '{path}'")
    logger.debug(f"File exists: {path}")
    return path

def validate_extension(path_input: FilePath, allowed_exts: set[str]) -> None:
    """
    Ensure that the file has an allowed extension.

    Args:
        path_input (FilePath): A pathlib.Path object or string for the file.
        allowed_exts (set[str]): A set of permitted extensions (including the dot).

    Raises:
        ExtensionError: If the file's extension is not in allowed_exts.
    """
    path = Path(path_input)
    suffix = path.suffix.lower()
    if suffix not in allowed_exts:
        logger.error(f"Unsupported extension '{suffix}'; allowed: {allowed_exts}")
        raise ExtensionError(f"Unsupported extension '{path.suffix}'")
    logger.debug(f"Extension '{suffix}' is supported")
