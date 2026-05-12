"""
ZIP utilities for safe extraction (prevents ZIP Slip / path traversal).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Union
import zipfile


def safe_extractall(zip_ref: zipfile.ZipFile, extract_dir: Union[str, Path]) -> None:
    """
    Safely extract a ZIP file by validating every member path.

    Prevents malicious ZIPs from writing outside `extract_dir` via `../` or
    absolute paths.
    """
    extract_dir_path = Path(extract_dir).resolve()

    for member in zip_ref.infolist():
        member_name = member.filename.replace("\\", "/")

        # Reject absolute paths and Windows drive-letter paths.
        if member_name.startswith("/") or member_name.startswith("../") or member_name.startswith("..\\"):
            raise ValueError(f"Refusing to extract suspicious ZIP entry: {member_name}")
        if os.path.isabs(member_name) or (len(member_name) > 1 and member_name[1] == ":"):
            raise ValueError(f"Refusing to extract suspicious ZIP entry: {member_name}")

        # Resolve the final destination and ensure it stays under extract_dir.
        dest_path = (extract_dir_path / member_name).resolve()
        try:
            dest_path.relative_to(extract_dir_path)
        except ValueError:
            raise ValueError(f"Refusing to extract ZIP entry outside target dir: {member_name}")

    # All entries validated.
    zip_ref.extractall(extract_dir_path)

