"""
ZIP Adapter - Handles ingestion from local Canvas export ZIP files.
"""

import tempfile
import zipfile
import shutil
from pathlib import Path
from typing import Dict, Any, Optional

from core.stages.package_validator import PackageValidator
from utils.format_detector import FormatDetector, ExportFormat
from parsers.imscc_parser import IMSCCParser
from parsers.canvas_export_parser import CanvasExportParser
from models.canvas_models import CanvasCourse
from observability.logger import get_logger

logger = get_logger(__name__)

class ZipAdapter:
    """
    Adapter for processing local Canvas ZIP exports (IMSCC/ZIP).
    """

    def __init__(self):
        self.validator = PackageValidator()

    def load(self, payload: Dict[str, Any]) -> CanvasCourse:
        """
        Loads and parses a course from a local ZIP file.
        Payload expected: {"zip_path": Path}
        """
        zip_path = Path(payload["zip_path"])
        # 1. Check if it's already a directory
        cleanup_required = False
        if zip_path.is_dir():
            extract_dir = zip_path
        else:
            # 2. Validation
            is_valid, msg = self.validator.validate_zip(zip_path)
            if not is_valid:
                raise ValueError(f"Invalid ZIP package: {msg}")

            # 3. Extract to temp
            cleanup_required = True
            extract_dir = Path(tempfile.mkdtemp(prefix="lms_zip_extract_"))
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                
            # Traverse into a single top-level directory if present
            root_items = list(extract_dir.iterdir())
            if len(root_items) == 1 and root_items[0].is_dir():
                extract_dir = root_items[0]
                logger.info(f"Traversing into nested top-level directory: {extract_dir.name}")

        try:
            # 4. Detect format just for validation
            fmt = FormatDetector.detect(extract_dir)
            if fmt == ExportFormat.UNKNOWN:
                raise ValueError(f"Unknown export format in {zip_path.name}.")

            # Use the unified core Parser stage to build the CanvasCourse model
            from core.stages.parser import Parser
            parser = Parser(extract_dir)
            canvas_course, parse_report = parser.parse()
            
            if not canvas_course:
                # If the manifest is missing or completely broken
                raise ValueError(f"Failed to parse extract dir {extract_dir}: {parse_report.errors}")
                
            # Record the source directory for asset uploader
            canvas_course.source_directory = str(extract_dir)
                
            return canvas_course

        except Exception as e:
            if cleanup_required and extract_dir.exists():
                shutil.rmtree(extract_dir)
            raise e
        # Note: extract_dir cleanup should happen after the whole pipeline runs 
        # because AssetUploader needs the files. 
        # We'll need to handle cleanup in the IngestionWorker.
