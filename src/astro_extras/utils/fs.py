# Astro SDK Extras project 2025

""" Utility functions for file system operations """

import logging
import shutil
from pathlib import Path

from ..types.fs import FilePath

logger = logging.getLogger(__name__)


def move_file_to_subdir(src_path: FilePath, target_subdir: str) -> None:
    """
    Move a file to a subdirectory of its current directory.

    Args:
        src_path (str | Path): Source file path.
        target_subdir (str): Subdirectory name.
    """
    src_path = Path(src_path)
    if not src_path.exists():
        logger.info(f"File '{src_path}' does not exist. Skipping move.")
        return

    dst_dir = src_path.parent / target_subdir
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_path = dst_dir / src_path.name

    if src_path.resolve() == dst_path.resolve():
        logger.info(f"Source and destination are the same for '{src_path}'. Skipping move.")
        return

    if dst_path.exists():
        logger.warning(f"Destination file '{dst_path}' already exists and will be overwritten.")

    try:
        shutil.move(str(src_path), str(dst_path))
        logger.info(f"Moved file: '{src_path}' → '{dst_path}'")
    except Exception as e:
        logger.warning(f"Failed to move '{src_path}' → '{dst_path}': {e}")
